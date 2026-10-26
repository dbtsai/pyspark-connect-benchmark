# Benchmark coverage

`python -m connectbench.benchmark --list` is the authoritative case inventory.
There are **353 cases**: 48 construction and return cases in `core`, and 305
API, protocol, data conversion and scaling cases in `api`. All three client
positions run identical public Python workload implementations. Successful
samples collect timing, CPU and memory metrics for every case.

| Area | Coverage |
|---|---|
| Individual construction calls | Nine warm APIs; incremental calls on filter-chain depths 16/64/256; `api-select-width*`: one select on prepared 1/16/128/512 expressions; `increment-withColumn-chain-*`: one call on existing withColumn chains |
| Whole construction and expressions | Deep filters, wide projections, reused branching expressions, joins, balanced unions and mixed plans; linear arithmetic and large isin; `chain-withColumn-*` at 16/64/256; `projection-*` at widths 1/16/128/512; `aggregate-64-sums`; single-depth linear expression and singleton isin |
| Prepared outbound plans; first/repeated schema | Prepared filter-chain schema/explain/limit(0).collect; `plan-*`: withColumn chain depths 1/16/64/256, widths 16/128/512, expression depths 4/12/32, filter, 64-aggregate, 8-join, window, SQL, parameterized SQL; each first schema, repeated schema and empty collect |
| Small requests and commands | Version, time-zone config, column access, schema/columns/dtypes, 20 empty queries; `query-*-one-value`: 20/50 SQL and DataFrame queries with one returned scalar; shuffle config get/set; `rpc-*`: currentDatabase, tableExists, empty listTables, tempview, cache, persist, saveAsTable and a scripted enriched missing-column exception |
| Fetch methods | Arrow/collect/pandas/materialized iterator and full-field streaming consumption; `query-take-10`, `query-show-20`; show prints the fixture-provided string to a temporary file |
| Fetch scaling | 100k rows, 2,097,152 rows, wide 512 columns, many batch shapes; `return-sweep-*`: 1/1k/4k/16k/256k/1m rows for Arrow and 1/8/64/512 columns for collect+scan; `return-buffer-*`: 64 KiB/1 MiB/16 MiB/128 MiB of numeric cells, plus IPC overhead |
| Fetch types | Long, double, string/1KiB string, decimal, timestamp/date, boolean, binary, nullable20%, array/struct/map/nested; `return-nullable50-*`, `return-nested3-*`: 50% nulls and three nested struct levels, each Arrow/collect+scan/pandas/full-field streaming |
| Local input types, representations and schemas | Tuples/Row/pandas/Arrow; explicit/inferred/DDL; 10k×3 mixed and 10k×32 long; `input-scale-*`: tuples/pandas 100/1k/100k/1m×3 long; `input-type-*`: string, timestamp, array, date, decimal; `input-inline-*`: separate first/repeated query on prepared 100k×3 inputs |
| Local-relation cache threshold and uploads | Earlier input cases deliberately inline; `input-cache1MiB-*`: pandas 30k×3 versus 60k×3 numeric inputs (cell buffers 720,000/1,440,000 bytes), construction versus prepared first/repeated query. Threshold is scripted to 1 MiB; cache status is scoped to session and artifact name |
| UDF definition and first/repeated send | Scalar useArrow=False; construct versus reconstruct+send; 0/64KiB/1MiB captures; `udf-*-construct/first-query/repeat-query`: scalar, scalar Arrow, pandas scalar, UDTF, mapInPandas, grouped applyInPandas; prepared scalar closures 1/10MiB and 10MiB construction; register+SQL construction and prepared first/repeated query |
| Upload files | `artifact-*`: .py, .zip as pyfile, .zip as archive, 10KiB/1MiB/100MiB plain files, 10×10KiB files; validate all uploaded bytes, files, framing, chunk count and CRC |
| Notebook schema interactions | columns/dtypes and string column access on simple frames; `inspection-*`: repr, dir, key completion, positional access and printSchema, first at depths 1/16/64/256; repeated at 256. repr/dir/completion must expose the fixture column to pass |

These are selected one-variable sweeps rather than a Cartesian product of every
API, shape, type and size. Construction, prepared outbound and return boundaries
remain separate. The fixtures exclude server execution and production network effects. See the
measurement boundaries in [README.md](README.md) when interpreting results.

## What a fixture proves

The service supplies identical Arrow/schema/version/config responses and records
request counts and bytes. It checks artifact CRCs, chunk lengths and session cache
identity. A missing-column script returns a structured gRPC error with a follow-up
FetchErrorDetails response. Unsupported APIs and validation failures stay in results;
no alternate client-specific workload is substituted.

Plans and UDFs are **not executed**. Canned responses do not prove inference, analysis,
UDF output, upload-cache reuse on a real server, catalog state, persistence, writes,
SQL parameters or real-server acceptance. listTables is the empty-result case; its
nonempty Table-object conversion is not covered. show uses a canned string, so it
measures request/response handling and printing, not server formatting.

The error case requires preservation of the error class and column name; it does
not require identical exception types or stack traces. Inspection failures indicate
missing fixture schema detail, not general incompatibility with every notebook.

## Remaining coverage

- Cancellation while server work is running or a result is streaming. This needs
  an explicit delay/backpressure/interrupt script and cleanup checks, or an execution
  server. Immediate successful responses cannot measure cancellation latency.
- Cold import, lazy first-feature import, warm-process/new-session setup and close.
  The current measured worker intentionally preimports dependencies and creates the
  session before timing. Its fresh-process samples must not be labeled cold-start tests.
- Actual notebook kernel startup/rendering, serverless authentication, session
  attachment, 5/15/45-minute idle intervals and expired-token behavior.
- Nonempty catalog results, changing catalog/write state, server UDF execution and
  processing-row sweeps, real analyzer failures and execution correctness.
- Concurrent queries, retries, reattachment, cancellation races and production
  network throughput. The fixture has no simulated server execution delay.
- Default DataFrame diagnostic capture versus disabled diagnostics. All current
  measured clients explicitly disable DataFrame debugging for comparability.
- Complete cross-products: for example all input representations
  across every type/size and every return method across every row/buffer size.

These gaps are explicit; they are not counted as implemented benchmark cases.

For setup, selection and output details, see [README.md](README.md).
