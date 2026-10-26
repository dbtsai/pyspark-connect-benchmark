"""Create a reviewable report without hiding unsupported/failed samples."""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path


LABELS = ('python', 'rust-baseline', 'rust')


def fmt(value, digits=4):
    return 'failed' if value is None else f'{value:.{digits}f}'


def timing(row):
    if not row or row['median_ms'] is None:
        return 'failed'
    return f"{row['median_ms']:.4f} ({row['min_ms']:.4f}–{row['max_ms']:.4f})"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    args = parser.parse_args()
    root = Path(args.directory)
    run = json.loads((root / 'run.json').read_text())
    rows = json.loads((root / 'summary.json').read_text())
    groups = {}
    failures = []
    for row in rows:
        groups.setdefault(row['case'], {})[row['engine']] = row
        raw = json.loads((root / f"{row['engine']}-{row['case']}.json").read_text())
        for sample in raw['samples']:
            if sample['status'] != 'ok':
                detail = sample.get('detail', sample)
                error = detail.get('error', sample.get('stderr', 'See raw JSON'))
                last = error.strip().splitlines()[-1] if error.strip() else sample['status']
                failures.append(dict(case=row['case'], engine=row['engine'],
                    phase=detail.get('phase', sample['status']), error=last))
    with (root / 'comparison.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(dict(row, rpc_delta=json.dumps(row['rpc_delta'], sort_keys=True)) for row in rows)
    (root / 'failures.json').write_text(json.dumps(failures, indent=2) + '\n')
    passed, failed = (sum(r[key] for r in rows) for key in ('passed', 'failed'))
    coverage = Counter(c['group'] for c in run['cases'])
    lines = [
        '# Additional server-free benchmarks from the Ferrum report', '',
        f"{len(groups)} added cases, three clients, {run['arguments']['samples']} fresh-process "
        f'samples per case/client: **{passed} successful and {failed} failed samples**. '
        'All clients execute the same public Python workload code. A separate localhost fixture '
        'service supplies identical Arrow/schema/config/version/explain responses; no Spark '
        'execution server is used.', '',
        'The runner uses the official Python, baseline Rust and candidate Rust environments '
        'selected by the command line. Interpreter paths and harness hashes are recorded '
        'in run.json and versions in successful raw samples. Source-built wheel revisions '
        'and compiler settings must be recorded separately by the person running the benchmark.', '',
        '## Added coverage', '',
        '| Group | Cases | Measured boundary |', '|---|---:|---|',
        f"| Warm APIs and incremental calls | {coverage['api']} | withColumn, withColumns, select, "
        'filter, aggregation, join, union, sort, window; one call added to prepared depths 0/16/64/256 |',
        f"| Expression scaling | {coverage['expression']} | Linear arithmetic depths 4/12/32/64; isin lists of 1,000/10,000 |",
        f"| Prepared outbound | {coverage['prebuilt']} | First schema, warm explain, limit(0).collect on prebuilt depths 16/64/256 |",
        f"| Small calls and caching | {coverage['metadata']} | First/repeated schema/version; config get/set; columns/dtypes/column access; 20-query SQL/DataFrame loops |",
        f"| Local input | {coverage['input']} | Prepared tuples, Row lists, pandas, Arrow; explicit/inferred/DDL schemas; 10,000×3 mixed and 10,000×32 long |",
        f"| Scalar UDF | {coverage['udf']} | Construct versus construct+send; captured closure 0/64 KiB/1 MiB; useArrow=False |",
        f"| Return and allocation | {coverage['return']} | 12 added types × Arrow/collect+scan/pandas; 2,097,152-row and 512-column shapes; streaming field consumption; first full-size allocation controls |", '',
        'Added output types: double, decimal(18,2), microsecond timestamp, date, boolean, binary, '
        'nullable long, array with nulls, struct, map, nested struct/array/map, and 1,024-byte string. '
        'Batch contents/types are round-trip tested. Result validation checks dimensions and all '
        'first/last-row fields; collect-scan and streaming consumers also validate an all-field '
        'checksum that forces nested/native conversion.', '',
        '## Timing and CPU', '',
        'Milliseconds **per operation**; median (observed minimum–maximum). Small construction '
        'and metadata cases time a loop of 5–200 calls, then divide the total by the iteration '
        'count. Each process supplies one sample of that mean, not hundreds of independent '
        'samples. Loop overhead and ordinary release of the previous result are included. '
        'Three samples are exploratory; small or overlapping differences are inconclusive. '
        'CPU is process_time_ns across client threads, including conversion during reception; '
        'it is not the Ferrum report’s instrumented “client time.”', '',
        '| Case | Official Python ms (range) | Baseline Rust ms (range) | Candidate Rust ms (range) | Python CPU ms | Baseline CPU ms | Candidate CPU ms |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    for case, group in groups.items():
        ordered = [group.get(k) for k in LABELS]
        values = [timing(r) for r in ordered] + [fmt(r['cpu_ms']) if r else 'missing' for r in ordered]
        lines.append('| ' + case + ' | ' + ' | '.join(values) + ' |')
    lines += ['', '## Memory', '',
        'Absolute MiB per sample; each cell gives **baseline / sampled peak / result held / '
        'after result deletion and GC**. Prepared inputs/plans and the session remain alive. '
        'The runner observes the separate client process at a target 2 ms interval while '
        'the operation runs. This is a lower bound: brief peaks can be missed. Raw JSON '
        'records actual observation counts and maximum observed gaps. The observation window '
        'brackets the timed call and includes untimed request-counter snapshots and protocol '
        'messages; it is not an exact allocation profiler. Sampling runs outside the measured '
        'client, so its CPU is excluded.', '',
        'Default return cases perform one full-size same-action warmup. **allocation-first** '
        'cases warm only the channel, leaving the first full-size result allocation timed. '
        'Neither imports nor session creation are timed. Lifetime peak RSS is separately '
        'available in CSV/JSON and includes imports/setup/warmup; it must not be relabeled '
        'operation peak. Warm allocator reuse can make additional RSS small. Post-GC retention '
        'is not proof of a leak. Streaming consumers retain only row-count/checksum output, '
        'although the client may still buffer batches internally.', '',
        '| Case | Official Python MiB | Baseline Rust MiB | Candidate Rust MiB |',
        '|---|---:|---:|---:|']
    for case, group in groups.items():
        cells = []
        for label in LABELS:
            row = group.get(label)
            keys = ('baseline_rss_bytes', 'sampled_peak_rss_bytes', 'held_rss_bytes', 'post_gc_rss_bytes')
            cells.append(' / '.join(fmt(row[k] / 2**20, 2) for k in keys)
                         if row and row['passed'] else 'failed')
        lines.append('| ' + case + ' | ' + ' | '.join(cells) + ' |')
    lines += ['', '## Observed requests', '',
        'Counters are snapshotted immediately before/after the measured loop. Preparation, '
        'warmup and validation are excluded. Values below are totals for the loop, not '
        'per-operation counts; the iteration count is shown. HTTP observation requests are '
        'not Spark requests. Arrow payload bytes count serialized Arrow-bearing protobuf '
        'fragments, excluding per-response/session headers. Responses are fixtures, so these '
        'counts describe client behavior against this protocol script. Asynchronous cleanup '
        'that arrives outside the snapshots may not be included.', '',
        '| Case | Client | Iterations | Execute | Analyze | Config | Release | Schema | Version | Arrow batches | Request bytes | Arrow-bearing bytes |',
        '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for row in rows:
        stats = row['rpc_delta']
        if stats is None:
            continue
        keys = ('execute_calls', 'analyze_calls', 'config_calls', 'release_calls', 'schema_calls',
                'version_calls', 'arrow_batches', 'rpc_request_bytes', 'arrow_payload_bytes')
        cells = [str(row['iterations'])] + [str(stats[k]) for k in keys]
        lines.append('| ' + row['case'] + ' | ' + row['engine'] + ' | ' + ' | '.join(cells) + ' |')
    lines += ['', '## Support and failures', '']
    if failures:
        lines += ['Failed samples are excluded from performance medians and retained here and '
                  'in raw JSON, including failures during preparation, warmup and validation. '
                  'A failure against the fixture alone is not proof that the API is unsupported '
                  'against a real Spark server.', '',
                  '| Case | Client | Phase | Samples | Error |', '|---|---|---|---:|---|']
        counted = Counter((r['case'], r['engine'], r['phase'], r['error']) for r in failures)
        for (case, engine, phase, error), count in counted.items():
            lines.append(f"| {case} | {engine} | {phase} | {count} | {error.replace('|', '/')} |")
    else:
        lines.append('All measured samples passed their fixture checks.')
    lines += ['', '## Measurement boundaries and remaining scope', '',
        '- DataFrame debugging is explicitly false for all clients. Process and fixture-session '
        'time zones are UTC; aware timestamps are normalized to the same instant for validation. Imports/session setup, '
        'input generation, existing-plan construction and validation are outside timing.',
        '- Warm API cases warm the exact operation three times; input, UDF and most return '
        'cases warm it once. First-schema/version cases do not warm that property. Prepared '
        'schema cases warm the channel using a separate frame. Repeated schema/version cases '
        'warm the property once and time 50 more reads of the same object/session.',
        '- Construction API cases receive prepared expressions where practical; aggregation, '
        'join condition, sort and window cases also build their short expression/group wrapper '
        'inside the call. Depth controls the existing filter chain; incremental tests do not '
        'construct that chain inside timing.',
        '- Prebuilt outbound includes public API overhead, request encoding, localhost transport '
        'and scripted response handling. Explain includes printing to a temporary file. '
        'The fixture does not parse/optimize the submitted plan or prove server acceptance.',
        '- Local input construction excludes input allocation and leaves the local-relation '
        'upload thresholds very high. It measures inline conversion/construction; it does not '
        'validate query results, upload behavior or full schema inference semantics.',
        '- Scalar UDF construction may serialize at different points in each implementation; '
        'outbound cases force encoding/sending the closure. The service never executes the UDF.',
        '- Return calls include request/transport/client reception/conversion. collect-scan '
        'retains the collected result and reads every field. iterator-consume iterates and '
        'checksums every field without retaining the full Python row list.',
        '- This is selected coverage inspired by the supplied PDF, not a reproduction of its '
        '113 real-server workloads or a claim of every Spark API. Existing branching-expression, '
        'mixed-plan, join/union and full-scan cases remain in the earlier suite.',
        '- Real query correctness/server analysis, catalog/write semantics, uploads/artifacts, '
        'cancellation/retry/reattachment, auth/idle/token behavior, notebooks, cold imports, '
        'concurrency and default diagnostic-capture comparisons remain outside this addition.', '',
        '## Reproduce', '',
        '```sh',
        '.venv/bin/python -m unittest discover -s tests -v',
        '.venv/bin/python -m connectbench.ferrum --samples 3 --output results/ferrum-expanded-new',
        '.venv/bin/python -m connectbench.ferrum_report results/ferrum-expanded-new',
        '```', '',
        'Interpreter defaults follow the virtual-environment layout in README.md; '
        'override them with --reference-python, --baseline-rust-python and --rust-python. '
        'Use --smoke, --group, or repeated --case selectors for smaller runs. --resume requires '
        'the same harness hashes and interpreters. The runner returns nonzero on any measured '
        'failure, while preserving completed cases. Raw client JSON, server logs, run.json, '
        'summary.json, comparison.csv and failures.json accompany this report.',
    ]
    (root / 'REPORT.md').write_text('\n'.join(lines) + '\n')
    print(json.dumps(dict(report=str(root / 'REPORT.md'), cases=len(groups), passed=passed, failed=failed)))


if __name__ == '__main__':
    main()
