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

## 2026-11-09 — M2 done

**What was built**
- `src/promql.rs` holds the extraction UDFs, built on `promql-parser` 0.11.1 with one AST walk per call:
  - list functions: `promql_metrics`, `promql_funcs`, `promql_aggs` ("sum by (a,b)"), `promql_matchers` ("label op value"), `promql_ranges`, `url_promql(text)`
  - scalar functions: `promql_error(q)` and `promql_normalize(q, mode)`. `mode` only accepts `'exact'`; other modes go in the `match` there. Unit test: `cargo test`.
- A `promql` view, registered in `ctx()`, collects every PromQL string the agent sent: `get_metrics.query` plus `query=` parameters found inside bash curl calls. Columns: session_id, step_id, call_idx, call_id, via, query.
- New queries: T2a–T2g and T3. T5a–c now read `promql_normalize(query, 'exact')` over the `promql` view.

**Numbers**
- There are 2,741 PromQL strings: 2,738 from `get_metrics` and 3 from bash (2 sessions). 99.6% parse.
- All 10 strings the parser rejects also got HTTP 400 from Prometheus (T2g), so the parser agrees with the backend on this data. Typical agent mistakes are `offset` placed after `rate()`, scalar comparisons without `bool`, and a comma between two queries.
- Shape (T2a): 34% use functions, 32% aggregations, 68% label matchers, 38% range windows. 427 have no metric name, and `__name__=~` appears 360 times, which is metric discovery by regex.
- The common pattern is `sum(rate(x[1m|2m|5m]))`. Top metrics are locust failures, span-metric calls, pg_stat_activity, probe_success, and redis memory.
- The T5 shifts from M1 are entirely the 3 bash queries.
- Cross-check: 767 `rate()` calls in T2c reconciles with a regex count over the raw JSON (773 total = 766 parsed + 7 inside unparseable queries; +1 from bash).

**Data gotchas found**
- `observations.status` is unreliable. 13 `get_metrics` results marked `success` are backend 400 errors. *(Corrected in M3: an earlier "109" came from a loose `%error%` match on metric names.)* Use the `metric_results` view, which classifies results from `content`.
- `sessions.application` is blank for 170 of 601 sessions, about 28% in every run, because SREGym extra metadata is missing. It could be derived from problem_id or namespace if needed.

**Next (M3)**: linear walks. W2 (sequence pattern vs. the viewer oracle), T4 (failed/empty metric result, then the next call), W3 (refinement chains), and W4 (diagnosis vs. mitigation). Plain SQL with window functions first.

## 2026-11-09 — M3 done (linear walks, plain SQL)

**What was built**
- Views moved to `src/views.sql`, which is loaded on every context:
  - `promql`
  - `metric_results`: each get_metrics call with its result kind (data / empty / refused_duplicate / too_large / backend_error / timeout), classified from content.
  - `promql_steps`: per-step metric/func/agg/range/matcher sets, `linked` to the previous PromQL step, and `chain_id` via gaps-and-islands.
- `oracle/check.sh` diffs the W2 goldens against the HolmesGPT viewer's `analysis.md` from PR #10 (the copy is in `oracle/`).
- New queries: W2a/b, T4a/b, W3a–c, W4a–c. 21 queries in total, and bench is green. Every query runs in ≤ 0.2 s at this scale.
- Walks are built from window functions (`row_number`, `lag`, running `sum`) plus self-joins on position or step, with no custom operators. This is the baseline for M5.

**Numbers**
- **W2 = oracle.** The tool-step pair patterns at gap 0 and gap ≤2 reproduce the viewer's top-20 tables exactly (pattern, occurrences, attempts, problems).
- **T4.** get_metrics results break down as data 81%, empty 13.5%, refused duplicate 3.5%, too large 1.5%, backend error 0.5%, timeout 0.1%. All `too_large` results come from metric-discovery queries (`{__name__=~...}`). In 68% of those cases the next PromQL is another discovery query, usually narrowed; in 24% it is a named metric. After an empty result, the agent stops querying PromQL in that stage 37% of the time, against 23.5% after data.
- **W3.** 1,755 PromQL steps form 1,226 chains, and 80% of chains are a single step. Inside chains in diagnosis, matchers and ranges change most often (about 76% of linked pairs). In mitigation, 23% of linked pairs are unchanged, which looks like verification polling. The longest diagnosis chain tracks lower diagnosis pass rates: 77% with no diagnosis PromQL → 57% (1) → 40% (2) → 31% (3) → 20% (4+). This is correlation, not causation; harder problems likely drive both.
- **W4.** 76% (132 of 173) of sessions that use PromQL in both stages re-query at least one diagnosis metric in mitigation. **HolmesGPT's exact-duplicate guard is scoped per stage.** All 95 refusals are same-stage repeats, no same-stage repeat ever runs, and the 46 cross-stage exact repeats all ran. So of T5's 141 get_metrics exact repeats, only 46 actually reached Prometheus.
- Cross-checks done in Python against the raw JSON: T4 kinds, the `none` and `same_query` shares, the W3 step total and W3c bucket 0. W4c's sum matches T5.

**Gotchas**
- DataFusion 54 errors (`DuplicateUnqualifiedField`) when two `count(*) FILTER (WHERE EXISTS ...)` appear in one SELECT. Use joins instead.
- `promql_steps` uses `metric_results` (get_metrics only), so the 3 bash PromQL queries are not in the walks.

**Next (M4)**: tree and cross walks. W1 (observation → ancestry plus context replay up to a call) and W5 (PromQL common across runs of a problem vs. specific to one run). After that, M5 adds walk operators (`seq_match`, `walk`) that must match these baselines, with timings at synthetic scale.
