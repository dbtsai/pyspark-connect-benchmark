"""Identical public API workloads for Python and Rust-backed clients."""
import argparse
import gc
import importlib.metadata
import json
import resource
import sys
import time
import platform


def main():
    import psutil
    import pyarrow  # Import outside timing in both environments.
    import pandas
    import numpy
    from pyspark.sql import SparkSession, functions, Window
    from pyspark.sql.types import StructType, StructField, LongType
    from .workloads import build
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', choices=('python', 'rust'), required=True)
    parser.add_argument('--endpoint', default='sc://localhost:1')
    parser.add_argument('--stage', choices=('construct', 'outbound-rpc', 'return-rpc'), required=True)
    parser.add_argument('--family', default='wide')
    parser.add_argument('--size', type=int, default=10)
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--width', type=int, default=4)
    parser.add_argument('--kind', default='long')
    parser.add_argument('--action', choices=('toArrow', 'collect', 'toPandas', 'toLocalIterator'), default='toArrow')
    parser.add_argument('--scan-values', action='store_true')
    args = parser.parse_args()
    # Identical public API setup for both clients. No client internals patched.
    spark = (SparkSession.builder.remote(args.endpoint)
             .config('spark.python.sql.dataFrameDebugging.enabled', 'false').getOrCreate())
    def action(df):
        value = getattr(df, args.action)()
        value = list(value) if args.action == 'toLocalIterator' else value
        if args.scan_values:
            assert args.action in ('collect', 'toLocalIterator')
            checksum = sum(v if isinstance(v, int) else len(v) for row in value for v in row)
            return value, checksum
        return value
    if args.stage == 'construct':
        fn = lambda: build(spark, args.family, args.size)
    elif args.stage == 'outbound-rpc':
        fn = lambda: action(build(spark, args.family, args.size))
    else:
        df = spark.range(args.rows)
        fn = lambda: action(df)
    # Warm up the channel, schema/config code, and action path outside timing.
    # Each measured sample is still a fresh process with exactly one warmup action.
    if args.stage != 'construct':
        action(spark.range(1))
    gc.collect()
    process = psutil.Process()
    baseline = process.memory_info().rss
    start = time.perf_counter_ns()
    result = fn()
    elapsed = time.perf_counter_ns() - start
    end = process.memory_info().rss
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != 'darwin':
        peak *= 1024
    if args.stage != 'construct':
        expected = args.rows if args.stage == 'return-rpc' else 0
        if args.scan_values:
            result, checksum = result
            expected_checksum = (args.width * expected * (expected - 1) // 2
                                 if args.kind == 'long' else args.width * expected * 14)
            assert checksum == expected_checksum, (checksum, expected_checksum)
        if args.action == 'toArrow':
            count, width = result.num_rows, result.num_columns
            value = lambda row: result.column(0)[row].as_py()
        elif args.action == 'toPandas':
            count, width = result.shape
            value = lambda row: result.iloc[row, 0]
        else:
            count = len(result)
            width = len(result[0]) if count else args.width
            value = lambda row: result[row][0]
        assert count == expected, (count, expected)
        assert width == args.width
        if expected:
            assert value(0) == (0 if args.kind == 'long' else 'value-00000000')
            last = expected - 1 if args.kind == 'long' else f'value-{expected - 1:08d}'
            assert value(-1) == last
    else:
        from pyspark.sql import DataFrame
        assert isinstance(result, DataFrame)
    print(json.dumps(dict(parameters=vars(args), elapsed_ns=elapsed,
        baseline_rss_bytes=baseline, end_rss_bytes=end, process_peak_rss_bytes=peak,
        version=importlib.metadata.version('pyspark-client' if args.engine == 'python' else 'pyspark-client-rust'),
        python=sys.version, platform=platform.platform(),
        dependencies={name: importlib.metadata.version(name) for name in ('pandas', 'pyarrow', 'numpy', 'psutil')})), flush=True)
    # Avoid release-session RPCs: fake backend intentionally doesn't implement them.
    # The OS reclaims each sample's session/channel on process exit.


if __name__ == '__main__':
    main()
