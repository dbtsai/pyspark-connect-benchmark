"""Build a comparison report from preserved benchmark JSON."""
import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    parser.add_argument('--scan-directory')
    args = parser.parse_args()
    root = Path(args.directory)
    directories = [root] + ([Path(args.scan_directory)] if args.scan_directory else [])
    groups = {}
    rows = []
    for directory in directories:
        for row in json.loads((directory / 'summary.json').read_text()):
            row = dict(row)
            raw = json.loads((directory / f"{row['engine']}-{row['case']}.json").read_text())
            samples = raw['samples']
            times = [s['elapsed_ns'] / 1e6 for s in samples]
            row['min_ms'] = min(times) if times else None
            row['max_ms'] = max(times) if times else None
            row['source'] = str(directory / f"{row['engine']}-{row['case']}.json")
            rows.append(row)
            groups.setdefault(row['case'], {})[row['engine']] = row
    (root / 'summary-combined.json').write_text(json.dumps(rows, indent=2))
    with (root / 'comparison.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = ['# Shared public Python API comparison', '',
        'Client and dependency versions, interpreter selection and platform details are retained in raw samples. '
        'Same public imports, SparkSession builder, DataFrame workload function and actions. No client internals patched. '
        'No real Spark server; a separate localhost process replays the same fixtures.', '',
        f"{len(groups)} paired cases; {sum(r['passed'] for r in rows)} successful samples; "
        f"{sum(r['failed'] for r in rows)} failed samples. "
        'Treat small differences as preliminary rather than statistically significant.', '',
        '## Findings', '',
        '- The exact same public Python workload can run on both packages; the engine switch only selects the interpreter/package environment and labels metadata. Session construction and measured API calls have no client-specific branch.',
        '- Compare collect-only and collect-plus-field-reading separately: Rust native Row wrappers defer Python value conversion. The scan cases include that caller-visible work.',
        '- Smaller end RSS growth does not imply a smaller memory footprint: inspect absolute lifetime peaks, particularly for wide collected results. These peaks include warmup, so operation-only peak attribution needs further instrumentation.', '',
        '## Timing', '',
        '| Case | Python median ms (range) | Rust median ms (range) | Python / Rust |',
        '|---|---:|---:|---:|']
    for name, pair in groups.items():
        p, r = pair['python'], pair['rust']
        if p['median_ms'] is None or r['median_ms'] is None:
            lines.append(f'| {name} | failed/see JSON | failed/see JSON | — |')
            continue
        lines.append(f"| {name} | {p['median_ms']:.3f} ({p['min_ms']:.3f}–{p['max_ms']:.3f}) | "
                     f"{r['median_ms']:.3f} ({r['min_ms']:.3f}–{r['max_ms']:.3f}) | {p['median_ms']/r['median_ms']:.2f}× |")
    lines += ['', 'A ratio above 1 means Rust was faster in this run. No overall speedup is calculated across unrelated workloads.', '',
        '## Memory', '',
        '| Case | Python end RSS growth MiB | Rust end RSS growth MiB | Python lifetime peak MiB | Rust lifetime peak MiB |',
        '|---|---:|---:|---:|---:|']
    for name, pair in groups.items():
        p, r = pair['python'], pair['rust']
        if p['peak_mib'] is None or r['peak_mib'] is None:
            continue
        lines.append(f"| {name} | {p['rss_growth_mib']:.2f} | {r['rss_growth_mib']:.2f} | {p['peak_mib']:.2f} | {r['peak_mib']:.2f} |")
    lines += ['', '## Measurement boundaries', '',
        '- Construction: identical public API commands, after dependency imports and session setup. First-use dispatch/config lookups remain in timing. Local-data creation may request config; that behavior is part of the public API path.',
        '- Outbound: construct the plan and call toArrow(), receiving an empty table. Includes plan encoding, schema/config operations, default execution/reattachment behavior, localhost RPC and public return processing. This is not isolated serialization timing.',
        '- Return: prebuild a range plan, then call toArrow, collect, toPandas or fully consume toLocalIterator. Includes request overhead and transport as well as conversion. Fixture parameters appear in raw JSON and case names.',
        '- RPC cases perform one untimed warmup of the same action. The service returns the configured fixture for warmup too, irrespective of the range size. Thus warmup uses the full result shape. Construction has no operation warmup.',
        '- A case ending in -scan includes collect() followed by reading every field and computing a checksum, with the collected result retained. Both clients execute the same Python loop. This includes native-to-Python value conversion in Rust row wrappers.',
        '- RSS growth is end-of-operation RSS minus baseline, with the result retained. Lifetime peak includes imports and warmup. Neither is an isolated operation transient peak; allocator reuse can make end growth small despite a large peak. Fake-server RSS is excluded.',
        '- Validation checks row counts, widths, first/last values and scan checksums outside timing. The service returns canned results and does not execute query semantics or prove real-server acceptance of plans. Nested request payloads are skipped to avoid a decoder recursion limit.',
        '- DataFrame debugging is explicitly false on both clients. Raw JSON records dependency versions; preserve environment freezes and source-built wheel provenance separately.',
        '- Public-action measurements are not interchangeable with the isolated private-interface stages in connectbench.run.', '',
        '## Historical source observations (Rust v4.2.0)', '',
        'The following observations describe the Rust v4.2.0 source tag (ce1fa080149fab85dbbe6db1bb6b8b2490f7031e). '
        'They may not apply to the selected client builds; they are not profiler attribution or proof of the share of runtime each step consumes.', '',
        '- **Arrow return bridge:** Rust collects decoded batches, writes a new Arrow IPC file, creates Python bytes, then opens that file through PyArrow. This adds encoding/copying work for large toArrow/toPandas results. '
        '[Core to_arrow](https://github.com/apache/spark-connect-rust/blob/v4.2.0/crates/spark-connect/src/dataframe.rs#L1623), '
        '[IPC writer](https://github.com/apache/spark-connect-rust/blob/v4.2.0/crates/spark-connect/src/dataframe.rs#L2149), '
        '[Python binding](https://github.com/apache/spark-connect-rust/blob/v4.2.0/crates/pyspark-rs/src/dataframe.rs#L1171).',
        '- **Local rows:** the binding converts every Python row into native values and clones field names per row. The core creates another coerced row collection before Arrow IPC serialization. This offers plausible targets for the large local-row time/memory cost. '
        '[Python input path](https://github.com/apache/spark-connect-rust/blob/v4.2.0/crates/pyspark-rs/src/session.rs#L334), '
        '[Arrow construction](https://github.com/apache/spark-connect-rust/blob/v4.2.0/crates/spark-connect/src/session.rs#L601).',
        '- **Collected rows:** Rust exposes native Row wrappers and converts values when accessed. Official Python rows already hold Python values. collect() timing alone therefore measures different materialization boundaries; the -scan cases include actual access to every value. '
        '[Row binding](https://github.com/apache/spark-connect-rust/blob/v4.2.0/crates/pyspark-rs/src/row.rs).', '',
        '## Reproduce', '', '```sh',
        '.venv/bin/python -m connectbench.compare --samples 3 \\',
        '  --reference-python .venv/bin/python \\',
        '  --actions toArrow collect toPandas toLocalIterator --output results/public-api42',
        '.venv/bin/python -m connectbench.compare --samples 3 \\',
        '  --reference-python .venv/bin/python --stage return-rpc \\',
        '  --actions collect --scan-values --output results/public-api42-scan',
        '.venv/bin/python -m connectbench.report results/public-api42 \\',
        '  --scan-directory results/public-api42-scan', '```', '',
        'Raw samples, failures and fixture-server logs are preserved. comparison.csv is convenient for further analysis.']
    (root / 'REPORT.md').write_text('\n'.join(lines) + '\n')
    print(f"Report: {root / 'REPORT.md'}; {len(groups)} paired cases")


if __name__ == '__main__':
    main()
