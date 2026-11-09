# STATUS (append-only checkpoint log)

## 2026-11-09 — M1 done

**Setup to reproduce**
```
cd ../ASAPWorkloads && git fetch origin pull/10/head:pr-10
git archive pr-10 traces/holmesgpt-sregym/run-0{1,2,3} | tar -x -C ../TrajectoryDB/data --strip-components=2
cd ../TrajectoryDB && cargo build --release
./target/release/tdb ingest data && ./target/release/tdb bench
```

**What was built**
- `src/main.rs` is a single binary with `ingest | sql | bench [--update]`. It uses DataFusion 54.
- Ingest flattens ATIF into NDJSON staging files. DataFusion then applies the typed schema, sorts the rows, and writes `db/*.parquet` via `COPY`. Sort orders are declared when the files are registered.
- Tables are `sessions` (with outcomes joined from `run-*/results.csv`), `steps`, `tool_calls` (with promoted `query`, `command`, `kind`, `namespace`), and `observations`. Placeholder tools are kept.

**Numbers**
- 601 sessions, 14,576 steps, 17,437 calls and observations. The 190 MB of JSON becomes 7.5 MB of Parquet. Ingest takes 0.7 s.
- Outcomes are known for 525 of 601 sessions (run-01: 114/190).
- T1: in diagnosis, failed sessions call `get_metrics` about 3× as often as passed ones (4.97 vs 1.68 per session) and `fetch_pod_logs` about 2× as often. In mitigation there is no difference.
- T5a: only 5.1% of `get_metrics` calls repeat an exact query string within the same session (141 of 2,738, across 93 of the 301 sessions that use the tool).
- T5c: across runs of the same problem, most PromQL strings are unique to one run. For problems attempted in 3 runs, 1,977 query strings appear in just one run and 43 in all three. Exact-string matching understates overlap, so M2 normalization should raise this.
- Goldens were cross-checked against an independent Python pass over the raw JSON (repeat count, cross-run buckets, one T1 cell).

**Next (M2)**: add the `promql-parser` crate and UDFs (`promql_metrics`, `promql_funcs`, `promql_matchers`, `promql_range`, `promql_normalize`). Then write T2, T3 (URL-decoded PromQL from bash curl calls), and T5 rerun on normalized queries. Report parse coverage.

**Open questions**
- Should the T5 normalized version strip label values, or only reorder and canonicalize them? This decides what counts as "the same query" for ASAP caching.
