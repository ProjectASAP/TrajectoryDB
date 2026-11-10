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

## 2026-11-09 — M4 done (tree + cross walks, plain SQL)

**What was built**
- New queries: W1a–c and W5a–b. 26 queries in total, and bench is green.
- W1a walks up from one fixed observation (the first `too_large` get_metrics result) to its call, step, stage and session.
- W1b replays everything the agent had seen when it issued that call: earlier messages, calls and results, plus the target step's own message and calls.
- W1c aggregates the context in front of every PromQL call.
- W5a/b compare runs of the same problem at the metric-name level.

**Numbers**
- W1c: a PromQL call in diagnosis comes after about 9 prior steps and 67 KiB of earlier tool results, with 52.8k prompt tokens, 89% of them cached. In mitigation the figures are about 28 steps, 108 KiB and 84.3k tokens, 92% cached. This is the paper's "redundant context" in numbers: about 90% of every PromQL-issuing request is replayed prefix. Cross-checked in Python (diagnosis row exact).
- W5a: at the metric-name level, cross-run reuse is much higher than at the exact-string level. For problems attempted in 3 runs, 114 of 815 (14%) (problem, metric) pairs are used in all three runs, against 43 of 2,140 (2%) exact strings in T5c.
- W5b: same-problem session pairs from different runs share a Jaccard similarity of about 0.25–0.37 on metric names. Pairs where only one run passed are the least similar (0.252, n=37), against 0.354 for both-pass. The sample is small, so this is weak evidence. W5 depends on the parser and has not been cross-checked independently.

**Gotchas**
- **DataFusion 54 silent wrong result.** `OVER (w ROWS BETWEEN ...)` that refines a named window `w` drops w's `PARTITION BY`, so the frame runs across partitions. Plain `OVER w` is fine. Always write framed windows inline. A minimal repro is in the commit history (2627e9d message). It is worth filing upstream.

**State of the walk baseline (input to M5)**
| Walk | SQL mechanism | Pain point |
|---|---|---|
| W2 seq pattern | `row_number` + self-join on `pos+1..pos+k` | the k-gap join is O(n·k) per partition, and a pattern longer than 2 needs one join per element |
| T4b / W4c next / prior | self-join + `min(step_id)` + re-join | three joins to say "next PromQL step" |
| W3 chains | `lag` + running `sum` (gaps and islands) | fine in SQL |
| W1 ancestry/replay | UNION of per-level joins, filter on `step_id <=` | verbose; every query repeats the target CTE |

**Next (M5)**: walk operators. Candidates: `next_in(partition, order, predicate)` (replaces the T4b/W4c join dance), a sequence-pattern table function for patterns longer than 2, and a `replay(session, step)` table function. Each must reproduce the current goldens. Then add `tdb synth --x N` and time baseline vs. operator.

## 2026-11-09 — Examples + items 5, 6, 8 (from the "what else" menu)

**Examples (`EXAMPLES.md`)**
- `tdb examples [--update]` ingests `examples/fixture` (two hand-written sessions) into `target/examples-db` and checks every sql/text block pair. It is self-contained and independent of `data/`. There are 19 examples now. Add one for every new feature.
- `TDB_DB=<dir>` overrides the db directory.

**(6) Normalization modes**
- `promql_normalize(q, mode)` takes `exact | canonical | no_window | shape`. Each mode works by rewriting the AST and printing it again: matchers and `by` labels are sorted, windows and offsets blanked, label values replaced with `_`.
- **T5d.** Within a session, hits that actually reached Prometheus grow from 46 (exact) to 50 (canonical), 180 (no_window) and 229 (shape) out of 2,738.
  - These are the true result-cache opportunity, if the cache can serve a different window or label value.
  - Global "hits" reach 49% at `shape`. Every session runs against a fresh cluster, so cross-session hits mean template reuse (what to pre-provision sketches for), not reusable answers.

**(5) ASAP coverage + workload export**
- `promql_asap_class(q)` mirrors ASAPQuery@c439bf4's planner pattern table, with unit cases copied from its tests.
- `promql_asap_blocker(q)` gives the outermost reason an unsupported query misses.
- `export/agent_promql.sql` writes `out/agent_promql.json`: one NDJSON row per PromQL query, with session, problem, stage, ts, query, asap_class, shape, metrics, ranges and result_kind.
- ASAPQuery's input formats are a YAML `query_groups` with `repetition_delay_ms`, and ASAPPlanner's is a JSON `repeating_queries`. Both assume repeating queries. Agent PromQL is ad hoc, so there is no natural repetition interval; that is the open design question for feeding this workload in.
- **T7a: only 13.6% of agent PromQL calls match an ASAP pattern** (4.6% approx, 7.8% exact, 1.2% binary). Raw selectors are 36%, discovery 16%, unsupported 34%.
- **T7c: blockers among unsupported calls.**
  - 43% are `sum/avg/topk(rate|increase(...))`, which ASAPQuery marks non-collapsable ("commented out in the Python reference").
  - 26% are arithmetic with an unsupported arm, 17% comparison filters, 5% histogram_quantile and 4% subqueries.
  - Adding collapsable `sum∘rate` alone would cover about 15% of all agent PromQL calls.

**(8) Bash / kubectl / logs**
- `src/shell.rs` adds `bash_heads`, `kubectl_targets`, `bash_namespaces` and `json_get`. They use a hand-rolled splitter that handles quotes, heredocs, and `$( )` including inside double quotes.
- **W2c** reproduces the viewer's bash pattern table: pattern, attempts and problems match exactly, and occurrences are within 0.03%. `oracle/check.sh` checks it.
- The `telemetry_calls` view tags each call with one or more modalities (metrics/logs/traces/alerts/k8s_state/k8s_change/other) and a namespace.
- T6a kubectl targets, T6b first mutation per stage, W6a modality transitions, W6b metric→same-namespace logs.
- **T6b: zero mutating kubectl calls in diagnosis** across all 601 sessions, so the "do not mutate" constraint is never violated. In mitigation, 269 of 274 passing sessions mutate, against 183 of 251 failing ones.
- W6b: after a PromQL step that filters on `namespace="X"`, logs for namespace X follow within 3 steps 54.5% of the time in diagnosis.

**Process incident (no damage).** A shell heredoc used to edit `EXAMPLES.md` was cut short by an `EOF` inside the example text. The remainder ran as shell and executed one read-only `kubectl get svc -A` against the local kube context (192.168.49.2), which timed out and was killed. No files changed. Markdown edits now go through the Edit tool, not shell heredocs.

## 2026-11-09 — Items 2 + 1: row-pattern operator and scaling

**What was built**
- **`seq_match(ord, id, sym, pattern)`** (`src/seq.rs`) is a MATCH_RECOGNIZE-style aggregate. PARTITION BY becomes GROUP BY, DEFINE becomes a CASE expression that produces `sym`, and PATTERN is a regex over symbol names (`.` matches any row). It returns the ids of leftmost, non-overlapping matches. Its signature is `any(4)` and inputs are cast internally, so `row_number()` (UInt64) works directly.
- The `step_symbols` view assigns one symbol per agent step: A > M > L > K by priority.
- W7a asks "alerts → metrics → logs, gap ≤2 each". It has a 3-way self-join baseline and a `seq_match` version. `oracle/check.sh` asserts that every `X-baseline.golden` equals its operator sibling.
- `tdb synth N <dir>` replicates every session N times. `scripts/scale.sh [N...]` times baseline vs. operator pairs (median of 3 runs).
- `views.sql` loader: whole-line comments are stripped before splitting on `;`. This fixed a recurring footgun.
- **Not built, and why.** `next_in` is not needed: "next X" is `LEAD` over a step-level view (`promql_steps`, `step_symbols`). T4b's three-join version came from working at call level. `replay(session, step)` would only be a parameterized view with no performance angle; it is deferred until wanted.

**Numbers (W7a; identical results at every scale and gap)**
| scale | baseline | seq_match |
|---|---|---|
| 1× (601 sessions) | 168 ms | 76 ms |
| 10× | 394 ms | 223 ms |
| 50× (30k sessions, 729k steps) | 1714 ms | 929 ms |

**Where the time goes at 50×**
- Process startup is 12 ms, the `step_symbols` view alone is 529 ms, and `seq_match` over about 60k partitions adds about 30 ms.
- The baseline references the view 3 times, and **DataFusion recomputes it per reference (no common-subexpression reuse)**, giving about 3 × 529. So the about 1.8× win is "one scan vs. one scan per pattern element", not faster matching.
- Gap size: the baseline is flat for gap ≤2/5/10 (1.69 / 1.74 / 1.75 s), because DataFusion hash-joins on (session, stage) and range-filters afterwards. `seq_match` grows (0.94 / 1.11 / 1.32 s) as the regex automaton grows. My hypothesis that "the baseline degrades with gap" was wrong.

**Implications / next options**
- Eager vs. lazy (paper C1): materializing `step_symbols` and `telemetry_calls` at ingest would remove about 529 ms per reference. That is likely a bigger win than any operator at this scale. The cheapest next experiment is to add these as ingest-time tables and re-run `scale.sh`.
- C5 (order as a plan property): `seq_match` buffers and sorts each partition in `evaluate()`. Using the declared `(session_id, step_id)` sort order to stream would matter only once matching itself dominates, which it currently does not (30 ms).

## 2026-11-09 — M7: eager materialization (paper C1, eager vs lazy)

**What was built**
- `ingest` and `synth` now also write `metric_results`, `promql_steps`, `telemetry_calls` and `step_symbols` to Parquet (`DERIVED` in `src/main.rs`). `ctx()` swaps each view for its Parquet copy when the file exists. `TDB_LAZY=1` forces the views back.
- **Correctness:** all 37 goldens and 20 examples are identical in eager and lazy modes, and `oracle/check.sh` passes.
- **Cost:** 1× ingest goes from 0.7 s to 1.07 s, and the derived tables add 226 KB to a 7.5 MB db (+3%). At 50×, materialization takes 4.7 s on top of the replication itself.
- `seq_match` got faster: compiled patterns are cached globally (they were compiled once per group), and symbols are mapped to pattern chars on arrival (no per-row strings).

**W7a timings (median of 3; ms, including about 12 ms process startup)**
| scale | lazy baseline | lazy seq_match | eager baseline | eager seq_match |
|---|---|---|---|---|
| 1× | 164 | 74 | 25 | 27 |
| 10× | 399 | 226 | 30 | 51 |
| 50× | 1668 | 936 | 62 | 159 |
(The lazy seq_match column predates the seq_match speedups.)

**Reading it**
- **Eager materialization is the big win:** 27× for the self-join baseline at 50× (1668 → 62 ms), because the view had been recomputed once per self-join reference.
- **After materialization, the plain-SQL 3-way self-join beats `seq_match` by about 2.6× at 50×.** DataFusion's hash join with range filter is cheap on materialized, sorted input.
- `seq_match`'s remaining cost is DataFusion's per-group `Accumulator` path: one boxed accumulator per (session, stage), about 60k groups at 50×, and partial/final state serialized as `ScalarValue` lists. A vectorized `GroupsAccumulator` is the known fix.
- So for fixed-length, bounded-gap patterns, `seq_match`'s case is **expressiveness, not speed**. Unbounded or variable-length patterns (`M+`, `A .* L`, alternation) have no plain-SQL join equivalent without a recursive CTE. That is the comparison to run next if pursuing C5.

**Possible next steps**
1. A W7b query with an unbounded pattern (e.g. a run of ≥3 consecutive metric steps that ends in logs). Compare `seq_match` with a recursive-CTE or gaps-and-islands baseline.
2. A `GroupsAccumulator` for `seq_match`, then re-time.
3. Decide what else to materialize (`promql` view? `promql_*` UDF results as columns?) and measure the ingest/storage cost against the query win.
- Caveat on `synth`: at 50× the db is only 28 MB, against 7.5 MB at 1×. Exact replicas compress almost perfectly in Parquet (dictionary encoding), so scan and IO costs at N× are underestimated. Timings mostly reflect CPU on N× rows. Do not use synth for storage claims (C4).

## M7b: materialization is declared, not hardcoded
- views.sql now says `CREATE MATERIALIZED VIEW v ORDER BY (k, ...) AS q` (ClickHouse-style sort key). tdb rewrites it to
  `CREATE VIEW` for DataFusion and materializes exactly the declared set at ingest/synth; the `DERIVED` list in main.rs is gone.
- Still no runtime `CREATE MATERIALIZED VIEW` via `tdb sql`, no `REFRESH`, no staleness check (missing file -> lazy view).
- bench 0 non-PASS eager and lazy; 20 examples pass; oracle/check.sh passes.

## W7b: unbounded run `M{3,} L` (seq_match vs gaps-and-islands)
- Question: per (session, stage), matches of >=3 consecutive metric steps immediately followed by a logs step.
  Diagnosis: 22/184 failed stages vs 17/341 passed contain it (12% vs 5%), consistent with the chain-length finding.
- Baseline = gaps-and-islands (pos - row_number over M rows), HAVING count >= 3, join next row = L. 18 lines vs 12
  for seq_match, but no recursion needed: runs of M are maximal, so leftmost non-overlapping = qualifying islands.
  Recursion (sequential skip-past-last-row) is only forced when candidates overlap, e.g. `M .{0,3} L`; not built.
- Timings (eager, median of 3): 1x 28 vs 30 ms, 10x 34 vs 51, 50x 80 vs 176 (baseline vs seq_match). Baseline still wins;
  seq_match's cost is the per-group Accumulator path, as for W7a.
- Gotcha: DataFusion 54 `cardinality(list<list>)` = outer len x first inner len (assumes rectangular). Use
  array_length(flatten(m)) for #matched rows. W7a's `cardinality(m) > 0` is unaffected (0 iff empty).

## seq_match v2: symbol sets + back-references (expressivity item 3)
- Replaced the regex-over-chars engine with a backtracking matcher (src/seq.rs). Same results on W7a/W7b/examples.
- sym may be list<utf8>: a row matches X if X is in its set. step_symbols gains `syms` (A/M/L/K + C=k8s change, T=traces)
  and `metric_key` (sorted metric names).
- Optional 5th arg `key`; pattern atoms `X[=Y]` / `X[!=Y]` compare the row's key with LAST(Y.key) within the match.
- W8a: 37% of agent steps carry >1 modality; A..M..L by set hits 48/184 failed diagnosis stages vs 30 by priority.
- W8b: diagnosis switch runs (>=3 metric steps, each a different metric set than the last) in 26% of failed vs 10% of
  passed stages; same-metric refine runs 4% vs 0.6%.
- Not built: MEASURES (computed per-match outputs), ALL ROWS PER MATCH as a table function; unnest the ids instead.
