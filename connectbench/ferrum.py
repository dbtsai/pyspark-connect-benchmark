"""Three-client runner for the additional Ferrum-inspired server-free workloads."""
import argparse
import hashlib
import json
from pathlib import Path
import selectors
import statistics
import subprocess
import sys
import threading
import time


def read_message(process, timeout):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(timeout):
            raise subprocess.TimeoutExpired(process.args, timeout)
    line = process.stdout.readline()
    if not line:
        raise RuntimeError('Worker exited before its next protocol message')
    return json.loads(line)


def sample(command, timeout, interval):
    import psutil
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True, bufsize=1) as process:
        stop = threading.Event()
        observations = []

        def observe():
            target = psutil.Process(process.pid)
            while not stop.is_set():
                try:
                    observations.append((time.monotonic_ns(), target.memory_info().rss))
                except psutil.NoSuchProcess:
                    break
                stop.wait(interval)

        thread = None
        try:
            first = read_message(process, timeout)
            if first['status'] != 'ready':
                return dict(status='failed', detail=first)
            thread = threading.Thread(target=observe, daemon=True)
            thread.start()
            process.stdin.write('go\n')
            process.stdin.flush()
            measured = read_message(process, timeout)
            stop.set()
            thread.join()
            if measured['status'] != 'measured':
                return dict(status='failed', detail=measured)
            process.stdin.write('validate\n')
            process.stdin.flush()
            final = read_message(process, timeout)
            process.wait(timeout=timeout)
            stderr = process.stderr.read()
            if final['status'] != 'ok' or process.returncode:
                return dict(status='failed', detail=final, stderr=stderr[-8000:], measurement=measured)
            gaps = [b[0] - a[0] for a, b in zip(observations, observations[1:])]
            return dict(final, **{k: v for k, v in measured.items() if k != 'status'},
                sampled_peak_rss_bytes=max([measured['baseline_rss_bytes'], measured['held_rss_bytes']]
                                           + [rss for _, rss in observations]),
                rss_observations=len(observations), rss_target_interval_ms=interval * 1000,
                rss_largest_observed_gap_ms=max(gaps, default=0) / 1e6,
                stderr=stderr[-8000:])
        except (subprocess.TimeoutExpired, RuntimeError, json.JSONDecodeError) as error:
            process.kill()
            out, err = process.communicate()
            return dict(status='timeout' if isinstance(error, subprocess.TimeoutExpired) else 'failed',
                        error=str(error), stdout=out[-2000:], stderr=err[-8000:])
        finally:
            stop.set()
            if thread:
                thread.join()
            if process.poll() is None:
                process.kill()
                process.wait()


def summarize(raw):
    values = [s for s in raw['samples'] if s['status'] == 'ok']
    failed = len(raw['samples']) - len(values)
    row = dict(case=raw['case']['name'], group=raw['case']['group'], engine=raw['engine'],
               passed=len(values), failed=failed, iterations=raw['case']['iterations'])

    def med(key):
        return statistics.median(s[key] for s in values) if values else None

    for key in ('elapsed_ns', 'cpu_ns', 'baseline_rss_bytes', 'held_rss_bytes',
                'post_gc_rss_bytes', 'sampled_peak_rss_bytes', 'process_peak_rss_bytes'):
        row[key] = med(key)
    row['median_ms'] = med('elapsed_ns') / raw['case']['iterations'] / 1e6 if values else None
    row['cpu_ms'] = med('cpu_ns') / raw['case']['iterations'] / 1e6 if values else None
    times = [s['elapsed_ns'] / raw['case']['iterations'] / 1e6 for s in values]
    row['min_ms'], row['max_ms'] = (min(times), max(times)) if times else (None, None)
    row['rpc_delta'] = {k: statistics.median(s['rpc_delta'][k] for s in values)
                        for k in values[0]['rpc_delta']} if values else None
    return row


def main(catalog=None):
    from .ferrum_workloads import cases
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference-python', default='.venv/bin/python')
    parser.add_argument('--baseline-rust-python', default='.venv-rust-baseline/bin/python')
    parser.add_argument('--rust-python', default='.venv-rust/bin/python')
    parser.add_argument('--server-python', default='.venv/bin/python')
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--timeout', type=float, default=60)
    parser.add_argument('--rss-interval-ms', type=float, default=2)
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--group', action='append', help='Select group; repeat for multiple groups')
    parser.add_argument('--list', action='store_true', help='List cases without running clients')
    if catalog is not None:
        parser.add_argument('--suite', action='append', choices=('core', 'api', 'all'))
    parser.add_argument('--case', action='append', help='Select case by name; repeat for multiple cases')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--output', default='results/benchmark-unified' if catalog else 'results/ferrum-expanded')
    args = parser.parse_args()
    if args.samples < 1 or args.rss_interval_ms <= 0:
        parser.error('samples and RSS interval must be positive')
    matrix = catalog(args.smoke, args.suite or ('all',)) if catalog else cases(args.smoke)
    if args.group:
        unknown = set(args.group) - {c['group'] for c in matrix}
        if unknown:
            parser.error(f'Unknown groups: {sorted(unknown)}')
        matrix = [c for c in matrix if c['group'] in args.group]
    if args.case:
        unknown = set(args.case) - {c['name'] for c in matrix}
        if unknown:
            parser.error(f'Unknown cases in selection: {sorted(unknown)}')
        matrix = [c for c in matrix if c['name'] in args.case]
    if not matrix:
        parser.error('No matching cases')
    if args.list:
        print(json.dumps(matrix, indent=2))
        return 0
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    clients = [('python', 'python', args.reference_python),
               ('rust-baseline', 'rust', args.baseline_rust_python), ('rust', 'rust', args.rust_python)]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in Path('connectbench').glob('*.py')}
    provenance = dict(arguments=vars(args), clients=clients, cases=matrix, harness_sha256=hashes,
                      command=sys.argv, rss_observer='separate runner process; samples bracket measurement')
    path = root / 'run.json'
    if args.resume and path.exists():
        old = json.loads(path.read_text())
        assert old['harness_sha256'] == hashes, 'Harness changed: use a new output directory'
        assert old['clients'] == [list(c) for c in clients], 'Clients changed'
        assert old['cases'] == matrix, 'Case matrix changed: use a new output directory'
        for key in ('samples', 'rss_interval_ms', 'timeout', 'server_python'):
            assert old['arguments'][key] == vars(args)[key], f'{key} changed'
    else:
        assert not path.exists(), 'Output already exists: use --resume or a new directory'
        path.write_text(json.dumps(provenance, indent=2) + '\n')
        snapshot = root / 'harness'
        snapshot.mkdir(exist_ok=True)
        for source in Path('connectbench').glob('*.py'):
            (snapshot / source.name).write_bytes(source.read_bytes())
    summary = []
    for index, case in enumerate(matrix):
        paths = {label: root / f"{label}-{case['name']}.json" for label, _, _ in clients}
        if args.resume and all(p.exists() for p in paths.values()):
            records = [json.loads(paths[label].read_text()) for label, _, _ in clients]
            if all(len(r['samples']) == args.samples for r in records):
                summary.extend(summarize(r) for r in records)
                continue
        server_command = [args.server_python, '-m', 'connectbench.fake_grpc']
        for key in ('rows', 'width', 'kind', 'batch_rows'):
            server_command += ['--' + key.replace('_', '-'), str(case[key])]
        for key in ('fixture_script', 'local_cache_threshold'):
            if key in case:
                server_command += ['--' + key.replace('_', '-'), str(case[key])]
        server = subprocess.Popen(server_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            ready = read_message(server, args.timeout)
            records = {label: dict(case=case, engine=label, interpreter=python,
                       fixture=ready, samples=[]) for label, _, python in clients}
            for number in range(args.samples):
                # Rotate all three positions, reversing direction on alternate cases.
                offset = (index + number) % len(clients)
                order = clients[offset:] + clients[:offset]
                if index % 2:
                    order.reverse()
                for label, engine, python in order:
                    command = [python, '-m', 'connectbench.ferrum_worker', '--engine', engine,
                        '--endpoint', f"sc://127.0.0.1:{ready['port']}", '--case', json.dumps(case),
                        '--stats-endpoint', ready['stats_endpoint']]
                    result = sample(command, args.timeout, args.rss_interval_ms / 1000)
                    result['order'] = [c[0] for c in order]
                    records[label]['samples'].append(result)
            for label, _, _ in clients:
                raw = records[label]
                paths[label].write_text(json.dumps(raw, indent=2) + '\n')
                row = summarize(raw)
                summary.append(row)
                print(json.dumps(row), flush=True)
        finally:
            server.terminate()
            out, err = server.communicate(timeout=5)
            (root / f"server-{case['name']}.log").write_text(out + err)
        (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    if catalog is not None:
        from .benchmark_report import write_report
        write_report(root)
    return int(any(r['failed'] for r in summary))


if __name__ == '__main__':
    sys.exit(main())
