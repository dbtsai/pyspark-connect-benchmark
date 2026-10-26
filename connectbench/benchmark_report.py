"""Report any selection from the unified catalog; preserve all failed samples."""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import shlex
import sys
from .ferrum_report import LABELS, fmt, timing


def write_report(directory):
    root = Path(directory)
    run = json.loads((root / 'run.json').read_text())
    rows = json.loads((root / 'summary.json').read_text())
    by_case, failures = {}, []
    for row in rows:
        by_case.setdefault(row['case'], {})[row['engine']] = row
        raw = json.loads((root / f"{row['engine']}-{row['case']}.json").read_text())
        for number, sample in enumerate(raw['samples']):
            if sample['status'] != 'ok':
                detail = sample.get('detail', sample)
                error = detail.get('error', sample.get('stderr', 'See raw JSON'))
                failures.append(dict(case=row['case'], engine=row['engine'], sample=number,
                    phase=detail.get('phase', sample['status']),
                    error=error.strip().splitlines()[-1] if error.strip() else sample['status']))
    with (root / 'comparison.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(dict(row, rpc_delta=json.dumps(row['rpc_delta'], sort_keys=True)) for row in rows)
    (root / 'failures.json').write_text(json.dumps(failures, indent=2) + '\n')
    passed, failed = (sum(r[k] for r in rows) for k in ('passed', 'failed'))
    lines = ['# Unified Spark Connect client benchmark', '',
        f"{len(by_case)} cases, three clients, {run['arguments']['samples']} independent fresh-process "
        f'samples per case/client: **{passed} successful, {failed} failed samples**.', '',
        'All clients execute identical public Python workloads against a separate localhost '
        'fixture service. The three positions are official Python, baseline Rust and candidate '
        'Rust. The selected matrix, interpreters and source hashes are in run.json; package '
        'versions are in successful raw samples and executed Python sources are archived in '
        'harness/. Source-built wheel revisions and compiler settings must be recorded '
        'separately by the person running the benchmark.', '',
        '| Suite / group | Cases |', '|---|---:|']
    counts = Counter((c.get('suite', 'api'), c['group']) for c in run['cases'])
    lines += [f'| {suite} / {group} | {n} |' for (suite, group), n in counts.items()]
    lines += ['', '## Boundaries', '',
        '- Imports, session creation, prepared inputs/plans and validation are excluded. '
        'Whole-chain/projection construction cases build their plans inside timing. Increment '
        'cases add one call to an existing plan. Prepared outbound cases time the action only.',
        '- Timing is elapsed milliseconds per operation: median (minimum–maximum). Small '
        'cases time a loop and divide by its iteration count; loop iterations are not '
        'independent samples. CPU time includes client native threads. Three samples are exploratory.',
        '- Memory cells are absolute MiB: baseline / externally sampled peak / result held / '
        'after result deletion and GC. Inputs and existing plans stay alive. Target polling '
        f"interval is {run['arguments']['rss_interval_ms']} ms; short peaks may be missed. "
        'Lifetime peak, observation gaps and counts are separately retained in JSON.',
        '- Ordinary return cases warm the full same-action fixture once. allocation-first '
        'cases warm only the channel. First/repeated schema, inspection and prepared-UDF '
        'cases use zero/one warmup on the same object. Input-cache construction and artifact '
        'uploads use no warmup. Other case-specific warmups are recorded in raw samples.',
        '- collect-scan reads all fields and retains the row list. iterator-consume reads '
        'all fields while retaining only a checksum. toLocalIterator in the core suite '
        'materializes the iterator as a list, preserving the earlier boundary.',
        '- Request counters bracket only the measured loop. Preparation/warmup uploads and '
        'queries are excluded from these deltas; raw server logs also contain lifetime totals. '
        'Artifacts are checked for CRC, byte length and chunk count; fixtures do not persist '
        'files or execute local relations, UDFs, SQL, catalog changes or writes.',
        '- Inspection checks that repr/dir/completion includes the fixture column. Failing '
        'that check is excluded from performance medians: omitted work is not a speedup. '
        'Expected missing-column errors must preserve the error class and column name.',
        '- Fixture responses are canned. These runs test client work and transport, not '
        'server correctness or serverless authentication. See COVERAGE.md for implemented '
        'coverage and remaining integration scenarios.', '',
        '## Timing and CPU', '',
        '| Case | Python ms (range) | Baseline Rust ms (range) | Candidate Rust ms (range) | Python CPU ms | Baseline CPU ms | Candidate CPU ms |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for case, engines in by_case.items():
        ordered = [engines.get(k) for k in LABELS]
        values = [timing(r) for r in ordered] + [fmt(r['cpu_ms']) if r else 'missing' for r in ordered]
        lines.append('| ' + case + ' | ' + ' | '.join(values) + ' |')
    lines += ['', '## Memory', '', '| Case | Python MiB | Baseline Rust MiB | Candidate Rust MiB |',
              '|---|---:|---:|---:|']
    for case, engines in by_case.items():
        values = []
        for label in LABELS:
            row = engines.get(label)
            keys = ('baseline_rss_bytes', 'sampled_peak_rss_bytes', 'held_rss_bytes', 'post_gc_rss_bytes')
            values.append(' / '.join(fmt(row[k] / 2**20, 2) for k in keys)
                          if row and row['passed'] else 'failed')
        lines.append('| ' + case + ' | ' + ' | '.join(values) + ' |')
    lines += ['', '## Observed requests', '',
        'Counters are totals for the timed loop. CSV/JSON include every counter, including '
        'request bytes, release, config, schema, enriched-error fetch and upload-status calls.', '',
        '| Case | Client | Iterations | Execute | Analyze | Upload RPC | Uploaded bytes | Request bytes |',
        '|---|---|---:|---:|---:|---:|---:|---:|']
    for row in rows:
        if row['rpc_delta'] is not None:
            keys = ('execute_calls', 'analyze_calls', 'artifact_calls', 'artifact_payload_bytes', 'rpc_request_bytes')
            cells = [str(row['iterations'])] + [str(row['rpc_delta'].get(k, 0)) for k in keys]
            lines.append('| ' + row['case'] + ' | ' + row['engine'] + ' | ' + ' | '.join(cells) + ' |')
    lines += ['', '## Failures', '',
        'Failures remain visible and are excluded from successful timing medians. A fixture '
        'failure alone does not prove real-server API incompatibility.', '',
        '| Case | Client | Phase | Samples | Error |', '|---|---|---|---:|---|']
    for (case, engine, phase, error), n in Counter((f['case'], f['engine'], f['phase'], f['error']) for f in failures).items():
        lines.append(f"| {case} | {engine} | {phase} | {n} | {error.replace('|', '/')} |")
    arguments = run['arguments']
    command = [sys.executable, '-m', 'connectbench.benchmark']
    for key in ('server_python', 'reference_python', 'baseline_rust_python', 'rust_python',
                'samples', 'timeout', 'rss_interval_ms'):
        command += ['--' + key.replace('_', '-'), str(arguments[key])]
    if arguments.get('smoke'):
        command.append('--smoke')
    for key in ('suite', 'group', 'case'):
        for value in arguments.get(key) or ():
            command += ['--' + key, value]
    command += ['--output', str(root) + '-rerun']
    lines += ['', '## Reproduce', '',
        'Run from the repository root with the same source and client environments. '
        'Interpreter paths below reflect this run; adjust them on another machine.', '',
        '```sh', shlex.join(command), '```', '',
        'Use --suite, repeated --group or repeated --case to select coverage. '
        '--resume requires unchanged source hashes, clients, case matrix, sample count, '
        'server interpreter, timeout and RSS interval. A failed sample makes the runner exit nonzero.']
    (root / 'REPORT.md').write_text('\n'.join(lines) + '\n')
    coverage = Path('COVERAGE.md')
    if coverage.exists():
        (root / coverage.name).write_text(coverage.read_text())
    print(json.dumps(dict(report=str(root / 'REPORT.md'), cases=len(by_case), passed=passed, failed=failed)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    write_report(parser.parse_args().directory)
