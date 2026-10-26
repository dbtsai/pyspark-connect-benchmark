# Spark Connect client benchmarks

Measure Python API performance and memory use in the official Spark Connect client
and the Rust-backed Python client, without a Spark execution server or JVM. Both
clients run identical public Python workloads against a separate localhost gRPC
fixture service.

The unified catalog contains **353 cases** covering DataFrame construction,
prepared outbound plans, local data conversion, metadata and caching, UDF
serialization, artifacts, errors, and result reception/conversion. See
[COVERAGE.md](COVERAGE.md) for the inventory and limits. This is selected coverage,
not every Spark API or a test of server execution correctness.

## Setup

Use Python 3.12 on macOS or Linux. Run commands from the repository root. The
clients both install the `pyspark` namespace, so use separate virtual environments.

```sh
git clone https://github.com/dbtsai/pyspark-connect-benchmark.git
cd pyspark-connect-benchmark

python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

python3.12 -m venv .venv-rust-baseline
.venv-rust-baseline/bin/python -m pip install -r requirements-rust.txt

python3.12 -m venv .venv-rust
.venv-rust/bin/python -m pip install -r requirements-rust.txt
```

These files select official `pyspark-client==4.2.0` and
`pyspark-client-rust==4.2.0`, with matching NumPy, pandas, PyArrow and psutil versions.
The official-client environment also runs the fixture service and RSS observer.
Use your organization's package index configuration if required.

The setup installs the same Rust release into both Rust environments as a setup
check. For a source-change comparison, replace them with baseline and candidate
wheels built with the same Python version, toolchain, release settings and
dependencies. Save source revisions, patches, compiler versions, build commands
and environment freezes with results. The runner records package versions but
cannot infer the source revision of a wheel.

## Run

```sh
# Inspect all cases without starting clients.
.venv/bin/python -m connectbench.benchmark --list

# 16 representative cases, three clients, one sample each.
.venv/bin/python -m connectbench.benchmark --smoke --samples 1 \
  --output results/smoke

# Full catalog; three independent fresh-process samples per case/client.
.venv/bin/python -m connectbench.benchmark --samples 3 \
  --output results/full
```

Defaults use `.venv/bin/python` for the official client and fixture service,
`.venv-rust-baseline/bin/python` for the baseline, and `.venv-rust/bin/python` for
the candidate. Override interpreters for existing environments:

```sh
.venv/bin/python -m connectbench.benchmark \
  --server-python .venv/bin/python \
  --reference-python .venv/bin/python \
  --baseline-rust-python /path/to/baseline/bin/python \
  --rust-python /path/to/candidate/bin/python \
  --samples 5 --output results/comparison
```

Select `--suite core` for the 48 construction and return cases, or `--suite api`
for the 305 API, protocol, data conversion and scaling cases. Omit `--suite` for
all 353 cases. Repeat `--suite`, `--group` or `--case` to select multiple entries;
use `--list` to find names.

```sh
.venv/bin/python -m connectbench.benchmark --group artifact --samples 3 \
  --output results/artifacts
.venv/bin/python -m connectbench.benchmark --case chain-withColumn-64 \
  --case return-nested3-collect-scan --samples 3 --output results/selected
```

Each run writes `REPORT.md`, `comparison.csv`, `summary.json`, `failures.json`, raw
samples, fixture logs, and `run.json` with the matrix, interpreter paths, settings
and harness hashes. Executed Python sources are copied into `harness/`. Results
are ignored by Git.

Use a new output directory per run. Continue an interrupted run with the same
command plus `--resume`; sources, clients, matrix, sample count, timeout, server
interpreter and RSS interval must match. Completed cases are reused. Any failed
sample gives a nonzero exit code and stays in the report. Some cases expose client
API gaps or protocol depth limits, so the full catalog is not expected to pass on
every client release. Failed samples are excluded from successful timing medians.

## Measurement boundaries

Imports, session setup, prepared inputs/plans and validation are outside the timer.
Whole-plan cases construct plans inside timing; incremental cases add one operation
to a prepared plan. Outbound cases include encoding, localhost transport and canned
response handling. Return cases prebuild a simple plan and measure reception and
conversion of scripted Arrow data through a public action.

Most return cases warm the same full-size action once. `allocation-first` cases
warm only the channel. Metadata and UDF cases have separately recorded warmups.
All clients disable DataFrame debugging and use UTC.

| Metric | Meaning |
|---|---|
| Wall time | Elapsed time per operation, median and observed range |
| CPU time | Client process CPU, including native threads |
| Baseline RSS | Resident memory immediately before the timed loop |
| Sampled peak RSS | Maximum observed client RSS around the operation, sampled externally |
| Held RSS | Resident memory while the returned result is held |
| Post-GC RSS | Resident memory after deleting the result and running garbage collection |
| Process lifetime peak RSS | High-water mark through the operation, including imports, setup and warmup |
| Request counters | Calls and bytes during the measured loop, including Arrow and artifact counters |

RSS means **resident set size**: resident physical memory of the client process,
including Python, Rust and Arrow allocations. Fixture server memory is excluded.
Every successful unified sample collects these metrics; failures may have only
partial measurements. The default peak sampling target is 2 ms, so short spikes
may be missed. Raw data records observation counts and the largest polling gap.
The observation window also includes untimed counter snapshots and worker messages.
These are process memory measurements, not exact allocation counts. Warm allocator
reuse can reduce RSS growth; post-GC retention alone does not establish a leak.

Small calls time a loop and divide by its iteration count; iterations are not
independent samples. Client order rotates across samples and reverses across
cases. Three samples are exploratory; use more samples and controlled conditions
before claiming regressions or speedups. Large plans and fixtures can consume
substantial memory; the runner has no memory limit. `--timeout` sets the worker
protocol timeout (default 60 seconds).

## Fixture validation and limits

The localhost fixture scripts ExecutePlan, AnalyzePlan, Config, ReleaseExecute,
artifact uploads/status, and enriched errors. Artifact checks cover bytes, chunk
lengths/counts, CRC and session identity. Result checks cover types, dimensions
and values. `collect-scan` forces all-field conversion and retains rows;
`iterator-consume` checksums all fields while retaining only a summary. Core
`toLocalIterator` cases materialize the iterator into a list.

The service skips nested plan decoding to support deep plans. It does not analyze
or execute queries, UDFs, writes or catalog changes, or establish real-server plan
acceptance. Results include localhost transport rather than production network
costs. Cancellation, retry/reattachment, cold startup, authentication, idle sessions,
notebooks and concurrency remain outside the suite.

## Tests and utilities

```sh
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m connectbench.benchmark_report results/comparison
```

`tools/merge_benchmark_runs.py` combines complete cases from compatible runs,
preserving per-case source provenance. Later directories replace overlapping
cases in full; independent samples are not pooled across source versions.

Earlier entry points remain available: `connectbench.run` isolates official-client
construction/protobuf/serialization/replay using private interfaces;
`connectbench.compare` runs the earlier public API matrix; `connectbench.ferrum`
runs the 117-case subset. Prefer `connectbench.benchmark` for the full catalog and
consistent timing, memory and request measurements. Private-interface stages may
need adjustments for other Spark client versions.
