"""One expanded-suite sample, with public workloads and external RSS observation."""
import argparse
from contextlib import contextmanager
import gc
import importlib.metadata
import json
import os
import platform
import resource
import sys
import time
import tempfile
import traceback
from urllib.request import build_opener, ProxyHandler


@contextmanager
def captured_stdout():
    """Capture both Python print and native Rust stdout, outside timer setup."""
    with tempfile.TemporaryFile() as output:
        sys.stdout.flush()
        saved = os.dup(1)
        try:
            os.dup2(output.fileno(), 1)
            yield
        finally:
            sys.stdout.flush()
            os.dup2(saved, 1)
            os.close(saved)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', choices=('python', 'rust'), required=True)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--case', required=True)
    parser.add_argument('--stats-endpoint', required=True)
    args = parser.parse_args()
    case = json.loads(args.case)
    phase = 'prepare'
    try:
        # Spark timestamps materialize as local naive Python datetimes in some paths.
        # Pin both client process time zone and scripted session time zone to UTC.
        os.environ['TZ'] = 'UTC'
        if hasattr(time, 'tzset'):
            time.tzset()
        import numpy
        import pandas
        import psutil
        import pyarrow
        from pyspark.sql import SparkSession
        from .catalog import prepare
        spark = (SparkSession.builder.remote(args.endpoint)
                 .config('spark.python.sql.dataFrameDebugging.enabled', 'false').getOrCreate())
        # Explicitly bypass corporate HTTP proxies for this localhost-only observer.
        opener = build_opener(ProxyHandler({}))

        def stats():
            with opener.open(args.stats_endpoint, timeout=10) as response:
                return json.load(response)

        # Keep explain's public printing inside its measured action, off the JSON protocol.
        with captured_stdout():
            operation, validate, warmups = prepare(spark, case)
            phase = 'warmup'
            for _ in range(warmups):
                warm = operation()
                del warm
        gc.collect()
        process = psutil.Process()
        print(json.dumps({'status': 'ready', 'warmups': warmups}), flush=True)
        assert sys.stdin.readline().strip() == 'go'
        before_stats = stats()
        baseline = process.memory_info().rss
        phase = 'measure'
        with captured_stdout():
            cpu_start = time.process_time_ns()
            start = time.perf_counter_ns()
            for _ in range(case['iterations']):
                result = operation()
            elapsed = time.perf_counter_ns() - start
            cpu_elapsed = time.process_time_ns() - cpu_start
        held = process.memory_info().rss
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform != 'darwin':
            peak *= 1024
        after_stats = stats()
        # The runner stops external sampling here, before validation and GC.
        print(json.dumps({'status': 'measured', 'elapsed_ns': elapsed,
            'cpu_ns': cpu_elapsed, 'baseline_rss_bytes': baseline,
            'held_rss_bytes': held, 'process_peak_rss_bytes': peak,
            'rpc_delta': {k: after_stats[k] - before_stats[k] for k in before_stats},
            'warmups': warmups}), flush=True)
        assert sys.stdin.readline().strip() == 'validate'
        phase = 'validate'
        validate(result)
        for key, expected in case.get('expected_rpc', {}).items():
            actual = after_stats[key] - before_stats[key]
            assert actual == expected, f'{key}: expected {expected}, observed {actual}'
        del result
        gc.collect()
        print(json.dumps({'status': 'ok', 'post_gc_rss_bytes': process.memory_info().rss,
            'case': case, 'version': importlib.metadata.version(
                'pyspark-client' if args.engine == 'python' else 'pyspark-client-rust'),
            'python': sys.version, 'platform': platform.platform(), 'timezone': 'UTC',
            'dependencies': {name: importlib.metadata.version(name)
                for name in ('pandas', 'pyarrow', 'numpy', 'psutil')}}), flush=True)
        return 0
    except Exception:
        print(json.dumps({'status': 'failed', 'phase': phase,
                          'error': traceback.format_exc()}), flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
