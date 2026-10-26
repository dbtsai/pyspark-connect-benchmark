"""Combine complete case runs, keeping source provenance for every selected case.

Later directories replace whole cases (all clients/samples) only. Workload
parameters and measurement settings must agree; old raw results are preserved.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

# Allow direct invocation from the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from connectbench.ferrum import summarize
from connectbench.benchmark_report import write_report


def merge(directories, output):
    inputs = [Path(d).resolve() for d in directories]
    runs = [json.loads((d / 'run.json').read_text()) for d in inputs]
    base = runs[0]
    clients = [c[0] for c in base['clients']]
    chosen, definitions = {}, {}
    for index, (directory, run) in enumerate(zip(inputs, runs)):
        assert run['clients'] == base['clients'], 'Client identities differ'
        for key in ('samples', 'rss_interval_ms', 'timeout', 'server_python'):
            assert run['arguments'][key] == base['arguments'][key], f'{key} differs'
        for path, digest in run['harness_sha256'].items():
            archived = directory / 'harness' / Path(path).name
            assert hashlib.sha256(archived.read_bytes()).hexdigest() == digest, archived
        for case in run['cases']:
            name = case['name']
            if name in definitions:
                assert definitions[name] == case, f'Workload changed: {name}'
            definitions[name] = case
            for label in clients:
                raw = json.loads((directory / f'{label}-{name}.json').read_text())
                assert raw['case'] == case and raw['engine'] == label
                assert len(raw['samples']) == run['arguments']['samples'], name
            chosen[name] = index
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    (root / 'provenance').mkdir()
    (root / 'harness').mkdir()
    for index, (directory, run) in enumerate(zip(inputs, runs)):
        shutil.copytree(directory / 'harness', root / 'harness' / f'run-{index + 1}')
        for filename in ('run.json', 'summary.json'):
            shutil.copyfile(directory / filename, root / 'provenance' / f'run-{index + 1}-{filename}')
    summary = []
    for name in definitions:
        directory = inputs[chosen[name]]
        for label in clients:
            filename = f'{label}-{name}.json'
            shutil.copyfile(directory / filename, root / filename)
            summary.append(summarize(json.loads((root / filename).read_text())))
        shutil.copyfile(directory / f'server-{name}.log', root / f'server-{name}.log')
    provenance = dict(base, arguments=dict(base['arguments'], output=str(root)),
        harness_sha256=None, command=None,
        source_runs=[dict(directory=str(d), harness_sha256=r['harness_sha256']) for d, r in zip(inputs, runs)],
        case_source_run={name: index + 1 for name, index in chosen.items()},
        merge_command=sys.argv, merge_tool_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (root / 'run.json').write_text(json.dumps(provenance, indent=2) + '\n')
    (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    write_report(root)
    report = root / 'REPORT.md'
    text = report.read_text()
    note = ('This report combines complete case runs. Later runs replace all clients and samples '
            'for overlapping cases; parameters and measurement settings are checked for equality. '
            'Each case maps to its source run in run.json. Original run manifests and summaries '
            'are in provenance/ and each run’s measured source is archived separately under '
            'harness/run-N/. No independent samples are pooled across source versions.\n\n')
    text = text.replace('\n\n', '\n\n' + note, 1)
    report.write_text(text)
    return root


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    merge(args.directories, args.output)
