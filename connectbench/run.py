"""Each sample runs in a fresh subprocess; fixture construction is untimed."""
import argparse
import gc
import importlib.metadata
import json
import platform
import resource
import statistics
import subprocess
import sys
import time

from .workloads import FAMILIES


def worker(args):
    import psutil
    from .backend import ReplayStub, responses, session
    from .workloads import build
    from pyspark.sql.connect import proto

    spark = session()
    client = spark.client
    fn = lambda: build(spark, args.family, args.size)
    expected_rows = None
    request_size = None
    if args.stage in ("protobuf", "serialize"):
        df = fn()
        fn = lambda: df._plan.to_proto(client)
        if args.stage == "serialize":
            plan = fn()
            fn = plan.SerializeToString
    elif args.stage == "outbound":
        def fn():
            plan = build(spark, args.family, args.size)._plan.to_proto(client)
            req = proto.ExecutePlanRequest(session_id=client._session_id)
            req.plan.CopyFrom(plan)
            return req.SerializeToString()
    elif args.stage in ("decode", "return-arrow"):
        frames = responses(client._session_id, args.rows, args.width, args.batch_rows, args.kind)
        expected_rows = args.rows
        if args.stage == "decode":
            fn = lambda: [proto.ExecutePlanResponse.FromString(f) for f in frames]
        else:
            client._stub = ReplayStub(client._session_id, frames)
            req = proto.ExecutePlanRequest(session_id=client._session_id)
            req.plan.CopyFrom(spark.range(args.rows)._plan.to_proto(client))
            fn = lambda: list(client._execute_and_fetch_as_iterator(req, {}))

    gc.collect()
    baseline = psutil.Process().memory_info().rss
    started = time.perf_counter_ns()
    result = fn()  # Keep output alive through memory measurement.
    elapsed = time.perf_counter_ns() - started
    rss = psutil.Process().memory_info().rss
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak *= 1 if sys.platform == "darwin" else 1024
    # Validation is deliberately outside timing.
    if args.stage in ("outbound", "serialize"):
        cls = proto.ExecutePlanRequest if args.stage == "outbound" else proto.Plan
        decoded = cls.FromString(result)
        plan = decoded.plan if args.stage == "outbound" else decoded
        assert plan.HasField("root")
        request_size = len(result)
    elif args.stage == "protobuf":
        assert result.HasField("root")
    elif args.stage == "decode":
        assert sum(r.arrow_batch.row_count for r in result) == expected_rows
    elif args.stage == "return-arrow":
        import pyarrow as pa
        batches = [b for b in result if isinstance(b, pa.RecordBatch)]
        assert sum(b.num_rows for b in batches) == expected_rows
        assert all(b.num_columns == args.width for b in batches)
        if expected_rows:
            assert batches[0].column(0)[0].as_py() == (0 if args.kind == "long" else "value-00000000")
    del result
    gc.collect()
    retained = psutil.Process().memory_info().rss
    client._channel.close()
    return dict(elapsed_ns=elapsed, baseline_rss_bytes=baseline,
                end_rss_bytes=rss, process_peak_rss_bytes=peak,
                retained_rss_bytes=retained, request_bytes=request_size)


def main():
    if sys.version_info < (3, 10):
        raise SystemExit("Spark 4 requires Python 3.10 or newer")
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--stage", choices=("construct", "protobuf", "serialize", "outbound", "decode", "return-arrow"), default="outbound")
    parser.add_argument("--family", choices=FAMILIES, default="wide")
    for name, default in (("size", 10), ("rows", 10000), ("width", 4), ("batch-rows", 1000), ("samples", 5)):
        parser.add_argument("--" + name, type=int, default=default)
    parser.add_argument("--kind", choices=("long", "string"), default="long")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.size, args.width, args.batch_rows, args.samples) < 1 or args.rows < 0 or args.timeout <= 0:
        parser.error("sizes/samples/timeout must be positive; rows may be zero")
    if args.worker:
        print(json.dumps(worker(args)))
        return
    samples = []
    failures = []
    for _ in range(args.samples):
        try:
            proc = subprocess.run([sys.executable, "-m", "connectbench.run", *sys.argv[1:], "--worker"],
                                  capture_output=True, text=True, timeout=args.timeout)
            if proc.returncode:
                failures.append(dict(status="failed", stderr=proc.stderr[-8000:]))
            else:
                samples.append(json.loads(proc.stdout.splitlines()[-1]))
        except subprocess.TimeoutExpired:
            failures.append(dict(status="timeout"))
    versions = {}
    for package in ("pyspark-client", "pyspark", "pyarrow", "pandas", "protobuf", "grpcio", "psutil"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            pass
    print(json.dumps(dict(parameters=vars(args), python=sys.version, platform=platform.platform(),
                          versions=versions, samples=samples, failures=failures,
                          median_ns=statistics.median(s["elapsed_ns"] for s in samples) if samples else None), indent=2))
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
