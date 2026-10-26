"""Run isolated Python/Rust workloads against a separate local fixture service."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--reference-python', default='.venv/bin/python')
    parser.add_argument('--server-python', default='.venv/bin/python')
    parser.add_argument('--rust-python', default='.venv-rust/bin/python')
    parser.add_argument('--baseline-rust-python', help='Also measure an unpatched Rust build')
    parser.add_argument('--alternate-client-order', action='store_true')
    parser.add_argument('--family', help='Only run this plan family (return cases excluded)')
    parser.add_argument('--stage', choices=('construct', 'outbound-rpc', 'return-rpc'))
    parser.add_argument('--actions', nargs='+', choices=('toArrow', 'collect', 'toPandas', 'toLocalIterator'), default=['toArrow'])
    parser.add_argument('--scan-values', action='store_true')
    parser.add_argument('--output', default='results/rust-comparison')
    args = parser.parse_args()
    clients = [('python', 'python', args.reference_python), ('rust', 'rust', args.rust_python)]
    if args.baseline_rust_python:
        clients.insert(1, ('rust-baseline', 'rust', args.baseline_rust_python))
    if args.scan_values and any(action not in ('collect', 'toLocalIterator') for action in args.actions):
        parser.error('--scan-values requires collect/toLocalIterator actions')
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    matrix = []
    for stage in ('construct', 'outbound-rpc'):
        for family, sizes in [('deep', [10, 100]), ('wide', [100, 1000]),
                              ('expression', [5, 10]), ('joins', [5, 25]),
                              ('unions', [10, 100]), ('mixed', [10, 100]),
                              ('local', [1000, 100000])]:
            for size in sizes:
                matrix.append(dict(stage=stage, family=family, size=size, rows=0, width=1,
                                   kind='long', batch_rows=1000))
    for kind, width, batch in [('long', 4, 1000), ('long', 50, 10000),
                               ('string', 4, 1000), ('string', 4, 10000)]:
        matrix.append(dict(stage='return-rpc', family='wide', size=10, rows=100000,
                           width=width, kind=kind, batch_rows=batch))
    if args.smoke:
        matrix = [dict(stage='return-rpc', family='wide', size=3, rows=17,
                       width=2, kind='long', batch_rows=5)]
    if args.family:
        matrix = [case for case in matrix if case['family'] == args.family and case['stage'] != 'return-rpc']
    if args.stage:
        matrix = [case for case in matrix if case['stage'] == args.stage]
    if args.scan_values:
        matrix = [case for case in matrix if case['stage'] == 'return-rpc']
    matrix = [dict(case, action=action) for case in matrix
              for action in (args.actions if case['stage'] == 'return-rpc' else ['toArrow'])]
    summary = []
    for case in matrix:
        name = '-'.join(str(case[k]) for k in ('stage', 'family', 'size', 'rows', 'width', 'kind', 'batch_rows', 'action'))
        if args.scan_values:
            name += '-scan'
        server = subprocess.Popen([args.server_python, '-m', 'connectbench.fake_grpc',
            '--rows', str(case['rows']), '--width', str(case['width']),
            '--kind', case['kind'], '--batch-rows', str(case['batch_rows'])],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            line = server.stdout.readline()
            if not line:
                raise RuntimeError(server.stderr.read())
            ready = json.loads(line)
            measurements = {label: dict(samples=[], failures=[]) for label, _, _ in clients}
            for sample in range(args.samples):
                ordered_clients = list(clients)
                if args.alternate_client_order and (sample + len(summary) // len(clients)) % 2:
                    ordered_clients.reverse()
                for label, engine, python in ordered_clients:
                    results = measurements[label]['samples']
                    failures = measurements[label]['failures']
                    command = [python, '-m', 'connectbench.compare_worker', '--engine', engine,
                        '--endpoint', f"sc://127.0.0.1:{ready['port']}"]
                    if args.scan_values:
                        command.append('--scan-values')
                    for key in ('stage', 'family', 'size', 'rows', 'width', 'kind', 'action'):
                        command += ['--' + key, str(case[key])]
                    try:
                        p = subprocess.run(command, capture_output=True, text=True, timeout=30)
                        if p.returncode:
                            failures.append(dict(status='failed', stderr=p.stderr[-8000:], stdout=p.stdout[-2000:]))
                        else:
                            results.append(json.loads(p.stdout.splitlines()[-1]))
                    except subprocess.TimeoutExpired:
                        failures.append(dict(status='timeout'))
            for label, engine, python in clients:
                results = measurements[label]['samples']
                failures = measurements[label]['failures']
                raw = dict(case=case, engine=label, worker_engine=engine, interpreter=python,
                           alternate_client_order=args.alternate_client_order,
                           fixture_bytes=ready['fixture_bytes'],
                           samples=results, failures=failures)
                (root / f'{label}-{name}.json').write_text(json.dumps(raw, indent=2))
                def median(key):
                    return statistics.median(r[key] for r in results) if results else None
                row = dict(case=name, engine=label, passed=len(results), failed=len(failures),
                           median_ms=median('elapsed_ns') / 1e6 if results else None,
                           rss_growth_mib=statistics.median((r['end_rss_bytes']-r['baseline_rss_bytes'])/2**20 for r in results) if results else None,
                           peak_mib=median('process_peak_rss_bytes')/2**20 if results else None)
                summary.append(row)
                print(json.dumps(row), flush=True)
        finally:
            server.terminate()
            out, err = server.communicate(timeout=5)
            (root / f'server-{name}.log').write_text(out + err)
        (root / 'summary.json').write_text(json.dumps(summary, indent=2))
    return int(any(r['failed'] for r in summary))


if __name__ == '__main__':
    sys.exit(main())
