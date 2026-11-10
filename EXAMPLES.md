# TrajectoryDB user guide

TrajectoryDB loads agent trajectories (ATIF v1.7) into SQL tables and adds functions for questions SQL alone handles badly: what did the agent ask its tools, in what order, how do two runs differ, and what did a step mean.

```
tdb ingest <trace-root>    # trajectories + run-*/results.csv -> db/*.parquet
tdb sql "<query>"          # DataFusion SQL over db/
```

Every example below is a runnable query with its real output. `tdb examples` re-runs them all against `examples/fixture`, two hand-written sessions of problem `p1`:

- **s1** (run-01, diagnosis passed). Diagnosis: alerts and `rate(http_requests_total{job="api"}[5m])` (data), the same query with `[1m]` (empty), `kubectl get pods`, then the final answer "root cause: api crashloop". Mitigation: a rollout restart, the `[5m]` query again (data), once more (refused as a duplicate), and a curl to Prometheus for `up{job="api"}`.
- **s2** (run-02, diagnosis failed). A discovery query `{__name__=~"http.*"}` (too large), then `up{job="api"}` and pod logs in the same step.

Queries over the full corpus live in `queries/` (checked by `tdb bench`); section headings below name the relevant ones.

Contents: [1 Data model](#1-data-model) · [2 Tool calls](#2-what-did-the-agent-ask-its-tools) · [3 Walks](#3-walking-a-trajectory) · [4 Sequence patterns](#4-sequence-patterns) · [5 Comparing runs](#5-comparing-runs-of-the-same-problem) · [6 LLM analysis](#6-semantic-analysis-with-an-llm) · [7 Your own functions](#7-your-own-functions) · [8 Physical design](#8-physical-design)

## 1 Data model

| Table | One row per | Key columns |
|---|---|---|
| `sessions` | trajectory | `run`, `problem_id`, `application`, `diagnosis_passed`, `mitigation_passed` |
| `steps` | step (user, system or agent) | `step_id`, `stage` (diagnosis/mitigation), `source`, token counts, `message` |
| `tool_calls` | tool call | `step_id`, `call_idx`, `call_id`, `fn`, `args` (JSON), `query`, `command` |
| `observations` | tool result | `call_id`, `content`, `status` |

All tables are sorted by `(session_id, step_id, ...)`. Derived views (`promql`, `metric_results`, `promql_steps`, `telemetry_calls`, `step_symbols`) are introduced where they are used.

```sql
SELECT session_id, run, problem_id, application, diagnosis_passed, mitigation_passed FROM sessions ORDER BY session_id
```
```text
+------------+--------+------------+-------------+------------------+-------------------+
| session_id | run    | problem_id | application | diagnosis_passed | mitigation_passed |
+------------+--------+------------+-------------+------------------+-------------------+
| s1         | run-01 | p1         | Shop        | true             | false             |
| s2         | run-02 | p1         | Shop        | false            | false             |
+------------+--------+------------+-------------+------------------+-------------------+
```

```sql
SELECT session_id, stage, source, count(*) AS steps FROM steps GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
```
```text
+------------+------------+--------+-------+
| session_id | stage      | source | steps |
+------------+------------+--------+-------+
| s1         | diagnosis  | agent  | 4     |
| s1         | diagnosis  | system | 1     |
| s1         | diagnosis  | user   | 1     |
| s1         | mitigation | agent  | 4     |
| s1         | mitigation | user   | 1     |
| s2         | diagnosis  | agent  | 2     |
| s2         | diagnosis  | system | 1     |
| s2         | diagnosis  | user   | 1     |
+------------+------------+--------+-------+
```

```sql
SELECT step_id, call_idx, fn, query, command FROM tool_calls WHERE session_id = 's2' ORDER BY step_id, call_idx
```
```text
+---------+----------+----------------+----------------------+---------+
| step_id | call_idx | fn             | query                | command |
+---------+----------+----------------+----------------------+---------+
| 3       | 0        | get_metrics    | {__name__=~"http.*"} |         |
| 4       | 0        | get_metrics    | up{job="api"}        |         |
| 4       | 1        | fetch_pod_logs |                      |         |
+---------+----------+----------------+----------------------+---------+
```

## 2 What did the agent ask its tools?

### PromQL: what was queried, and did it parse?

`promql_metrics`, `promql_funcs`, `promql_aggs`, `promql_matchers` and `promql_ranges` take apart one query string. Corpus: `T2a`–`T2g`.

```sql
SELECT promql_metrics(q) AS metrics, promql_funcs(q) AS funcs, promql_aggs(q) AS aggs, promql_matchers(q) AS matchers, promql_ranges(q) AS ranges
FROM (VALUES ('sum by (pod) (rate(http_requests_total{job="api",code=~"5.."}[5m]))')) t(q)
```
```text
+-----------------------+--------+----------------+----------------------+--------+
| metrics               | funcs  | aggs           | matchers             | ranges |
+-----------------------+--------+----------------+----------------------+--------+
| [http_requests_total] | [rate] | [sum by (pod)] | [job=api, code=~5..] | [5m]   |
+-----------------------+--------+----------------+----------------------+--------+
```

A discovery query names no metric. `promql_error` returns the parser's message, or NULL.

```sql
SELECT promql_metrics('{__name__=~"http.*"}') AS discovery, promql_metrics('{__name__="up"}') AS name_matcher, promql_error('rate(x[5m]') AS err
```
```text
+-----------+--------------+---------------------------+
| discovery | name_matcher | err                       |
+-----------+--------------+---------------------------+
| []        | [up]         | unclosed left parenthesis |
+-----------+--------------+---------------------------+
```

Agents also query Prometheus through bash. `url_promql` pulls `query=` parameters out of URLs, and the `promql` view unions both paths. Corpus: `T3`.

```sql
SELECT url_promql('curl "http://p:9090/api/v1/query_range?query=sum%28up%29&step=30"') AS q
```
```text
+-----------+
| q         |
+-----------+
| [sum(up)] |
+-----------+
```

```sql
SELECT session_id, step_id, via, query FROM promql ORDER BY session_id, step_id
```
```text
+------------+---------+-------------+------------------------------------------+
| session_id | step_id | via         | query                                    |
+------------+---------+-------------+------------------------------------------+
| s1         | 3       | get_metrics | rate(http_requests_total{job="api"}[5m]) |
| s1         | 4       | get_metrics | rate(http_requests_total{job="api"}[1m]) |
| s1         | 9       | get_metrics | rate(http_requests_total{job="api"}[5m]) |
| s1         | 10      | get_metrics | rate(http_requests_total{job="api"}[5m]) |
| s1         | 11      | bash_url    | up{job="api"}                            |
| s2         | 3       | get_metrics | {__name__=~"http.*"}                     |
| s2         | 4       | get_metrics | up{job="api"}                            |
+------------+---------+-------------+------------------------------------------+
```

### Did the call return anything useful?

`metric_results` classifies each `get_metrics` result from its content: data, empty, too_large, timeout, backend_error or refused_duplicate. Corpus: `T4a`, `T4b`.

```sql
SELECT session_id, step_id, stage, kind FROM metric_results ORDER BY session_id, step_id
```
```text
+------------+---------+------------+-------------------+
| session_id | step_id | stage      | kind              |
+------------+---------+------------+-------------------+
| s1         | 3       | diagnosis  | data              |
| s1         | 4       | diagnosis  | empty             |
| s1         | 9       | mitigation | data              |
| s1         | 10      | mitigation | refused_duplicate |
| s2         | 3       | diagnosis  | too_large         |
| s2         | 4       | diagnosis  | data              |
+------------+---------+------------+-------------------+
```

### How often does the agent repeat itself? (caching potential)

`promql_normalize(q, mode)` gives a key for counting repeats. Each mode is coarser than the one before. `exact` returns q unchanged. `canonical` re-prints it with matchers and `by` labels sorted. `no_window` also blanks windows and offsets. `shape` also blanks label values. Corpus: `T5a`–`T5d`.

```sql
SELECT mode, promql_normalize('sum by (b,a) (rate(x{z="1",a="2"}[5m] offset 1m))', mode) AS key
FROM (VALUES (1, 'exact'), (2, 'canonical'), (3, 'no_window'), (4, 'shape')) t(o, mode) ORDER BY o
```
```text
+-----------+----------------------------------------------------+
| mode      | key                                                |
+-----------+----------------------------------------------------+
| exact     | sum by (b,a) (rate(x{z="1",a="2"}[5m] offset 1m))  |
| canonical | sum by (a, b) (rate(x{a="2",z="1"}[5m] offset 1m)) |
| no_window | sum by (a, b) (rate(x{a="2",z="1"}[0s]))           |
| shape     | sum by (a, b) (rate(x{a="_",z="_"}[0s]))           |
+-----------+----------------------------------------------------+
```

```sql
SELECT promql_normalize(query, 'no_window') AS key, count(*) AS sent, count(DISTINCT session_id) AS sessions
FROM promql GROUP BY 1 ORDER BY 2 DESC, 1
```
```text
+------------------------------------------+------+----------+
| key                                      | sent | sessions |
+------------------------------------------+------+----------+
| rate(http_requests_total{job="api"}[0s]) | 4    | 1        |
| up{job="api"}                            | 2    | 2        |
| {__name__=~"http.*"}                     | 1    | 1        |
+------------------------------------------+------+----------+
```

### Could a sketch-based engine answer it? (ASAP)

`promql_asap_class` mirrors ASAPQuery's planner patterns. `promql_asap_blocker` gives the reason a query is unsupported. Corpus: `T7a`–`T7c`; `export/agent_promql.sql` writes the whole workload out.

```sql
SELECT q, promql_asap_class(q) AS class, promql_asap_blocker(q) AS blocker
FROM (VALUES (1, 'quantile_over_time(0.99, x[5m])'), (2, 'rate(x[5m])'), (3, 'sum(rate(x[5m]))'), (4, 'sum(x) > 0'), (5, 'x')) t(o, q) ORDER BY o
```
```text
+---------------------------------+--------------+--------------------+
| q                               | class        | blocker            |
+---------------------------------+--------------+--------------------+
| quantile_over_time(0.99, x[5m]) | asap_approx  |                    |
| rate(x[5m])                     | asap_exact   |                    |
| sum(rate(x[5m]))                | unsupported  | agg_over_rate      |
| sum(x) > 0                      | unsupported  | comparison_on_asap |
| x                               | raw_selector |                    |
+---------------------------------+--------------+--------------------+
```

### Shell and kubectl

`bash_heads` lists each simple command, with kubectl expanded to its verb. `kubectl_targets` gives verb + resource. `bash_namespaces` gives `-n` values, with `-A` → `*`. Corpus: `T6a`, `T6b`, `W2c`.

```sql
SELECT bash_heads(c) AS heads, kubectl_targets(c) AS targets, bash_namespaces(c) AS namespaces
FROM (VALUES ('sleep 5; kubectl get pods -n shop | grep api && kubectl -n shop rollout restart deploy/api')) t(c)
```
```text
+---------------------------------------------+-----------------------------+--------------+
| heads                                       | targets                     | namespaces   |
+---------------------------------------------+-----------------------------+--------------+
| [sleep, kubectl get, grep, kubectl rollout] | [get pods, rollout restart] | [shop, shop] |
+---------------------------------------------+-----------------------------+--------------+
```

Heredoc bodies and `$(...)`, including inside double quotes, are handled. `chr(10)` is a newline.

```sql
SELECT bash_heads('cat <<END | kubectl apply -f -' || chr(10) || 'spec: {}' || chr(10) || 'END' || chr(10) || 'echo "$(kubectl get svc -A)"') AS heads
```
```text
+-----------------------------------------+
| heads                                   |
+-----------------------------------------+
| [cat, kubectl apply, echo, kubectl get] |
+-----------------------------------------+
```

`json_get(args, key)` reads a top-level argument of any tool call.

```sql
SELECT json_get('{"pod_name":"api-1","limit":40}', 'pod_name') AS pod, json_get('{"limit":40}', 'limit') AS lim, json_get('{"filter":null}', 'filter') AS f
```
```text
+-------+-----+---+
| pod   | lim | f |
+-------+-----+---+
| api-1 | 40  |   |
+-------+-----+---+
```

### Which telemetry modality, and where?

`telemetry_calls` has one row per tool call and modality (metrics, logs, traces, alerts, k8s_state, k8s_change), with the target namespace when known. Corpus: `T1`, `W6a`.

```sql
SELECT session_id, step_id, call_idx, stage, modality, namespace FROM telemetry_calls ORDER BY 1, 2, 3, 5
```
```text
+------------+---------+----------+------------+------------+-----------+
| session_id | step_id | call_idx | stage      | modality   | namespace |
+------------+---------+----------+------------+------------+-----------+
| s1         | 3       | 0        | diagnosis  | metrics    |           |
| s1         | 3       | 1        | diagnosis  | alerts     |           |
| s1         | 4       | 0        | diagnosis  | metrics    |           |
| s1         | 5       | 0        | diagnosis  | k8s_state  | shop      |
| s1         | 8       | 0        | mitigation | k8s_change | shop      |
| s1         | 9       | 0        | mitigation | metrics    |           |
| s1         | 10      | 0        | mitigation | metrics    |           |
| s1         | 11      | 0        | mitigation | metrics    |           |
| s2         | 3       | 0        | diagnosis  | metrics    |           |
| s2         | 4       | 0        | diagnosis  | metrics    |           |
| s2         | 4       | 1        | diagnosis  | logs       | shop      |
+------------+---------+----------+------------+------------+-----------+
```

## 3 Walking a trajectory

### Linear: what came next?

Window functions over `(session_id, stage)` ordered by `step_id`. `promql_steps` has one row per PromQL step. A step is `linked` when it shares a metric with the previous PromQL step, and `chain_id` numbers each run of linked steps (refinement chains). Corpus: `W2a`, `W2b`, `W3a`–`W3c`.

```sql
SELECT session_id, stage, step_id, metrics, linked, chain_id,
  lead(step_id) OVER (PARTITION BY session_id, stage ORDER BY step_id) AS next_promql_step
FROM promql_steps ORDER BY session_id, stage, step_id
```
```text
+------------+------------+---------+-----------------------+--------+----------+------------------+
| session_id | stage      | step_id | metrics               | linked | chain_id | next_promql_step |
+------------+------------+---------+-----------------------+--------+----------+------------------+
| s1         | diagnosis  | 3       | [http_requests_total] | false  | 1        | 4                |
| s1         | diagnosis  | 4       | [http_requests_total] | true   | 1        |                  |
| s1         | mitigation | 9       | [http_requests_total] | false  | 1        | 10               |
| s1         | mitigation | 10      | [http_requests_total] | true   | 1        |                  |
| s2         | diagnosis  | 3       | []                    | false  | 1        | 4                |
| s2         | diagnosis  | 4       | [up]                  | false  | 2        |                  |
+------------+------------+---------+-----------------------+--------+----------+------------------+
```

Write window specs inline. In DataFusion 54, `OVER (w ROWS BETWEEN ...)` silently drops the named window's PARTITION BY.

### Across stages: did mitigation reuse diagnosis?

These are exact repeats of an earlier query, with whether they ran. Corpus: `W4a`–`W4c`.

```sql
SELECT r.session_id, r.step_id, r.stage, p.stage AS earlier_stage, r.kind
FROM metric_results r JOIN metric_results p ON p.session_id = r.session_id AND p.query = r.query AND p.step_id < r.step_id
ORDER BY 1, 2, 4
```
```text
+------------+---------+------------+---------------+-------------------+
| session_id | step_id | stage      | earlier_stage | kind              |
+------------+---------+------------+---------------+-------------------+
| s1         | 9       | mitigation | diagnosis     | data              |
| s1         | 10      | mitigation | diagnosis     | refused_duplicate |
| s1         | 10      | mitigation | mitigation    | refused_duplicate |
+------------+---------+------------+---------------+-------------------+
```

### Tree: where did this result come from, and what had the agent seen?

Observation → call → step → session, plus the cumulative prompt tokens up to that step. Corpus: `W1a`–`W1c`.

```sql
SELECT o.content, t.fn, st.step_id, st.stage, s.run,
  sum(st2.prompt_tok) AS prompt_tok_so_far
FROM observations o
JOIN tool_calls t ON t.session_id = o.session_id AND t.call_id = o.call_id
JOIN steps st ON st.session_id = t.session_id AND st.step_id = t.step_id
JOIN sessions s ON s.session_id = st.session_id
JOIN steps st2 ON st2.session_id = st.session_id AND st2.step_id <= st.step_id
WHERE o.content LIKE 'Refusing%'
GROUP BY 1, 2, 3, 4, 5
```
```text
+-----------------------------------------------------------------------------------------------------------------------+-------------+---------+------------+--------+-------------------+
| content                                                                                                               | fn          | step_id | stage      | run    | prompt_tok_so_far |
+-----------------------------------------------------------------------------------------------------------------------+-------------+---------+------------+--------+-------------------+
| Refusing to run this tool call because it has already been called during this session with the exact same parameters. | get_metrics | 10      | mitigation | run-01 | 2800              |
+-----------------------------------------------------------------------------------------------------------------------+-------------+---------+------------+--------+-------------------+
```

## 4 Sequence patterns

`seq_match(ord, id, sym, pattern [, key])` is an aggregate, used like MATCH_RECOGNIZE: GROUP BY gives the partition, `ord` the order, and `sym` the row's symbol (or set of symbols). It returns the `id`s of each match, in order. Matches are leftmost, greedy and non-overlapping.

- **Pattern:** symbols are identifiers, and `.` is any row. Also `( ) | * + ? {n} {m,} {m,n}`.
- **Counting:** `array_length(m)` is the number of matches; `array_length(flatten(m))` is the number of matched rows. Not `cardinality(m)`: DataFusion treats nested lists as rectangular.

### Order matters: one symbol per step

`step_symbols` numbers agent steps within (session, stage). Its `sym` column takes one symbol by priority: A(lerts) > M(etrics) > L(ogs) > K(8s state), else NULL. Corpus: `W7a` (vs. a self-join) and `W7b` (vs. gaps-and-islands).

```sql
SELECT session_id, stage, array_agg(coalesce(sym, '_') ORDER BY pos) AS seq,
  seq_match(pos, step_id, sym, 'A M') AS a_then_m,
  seq_match(pos, step_id, sym, 'M .{0,1} K') AS m_gap_k
FROM step_symbols GROUP BY 1, 2 ORDER BY 1, 2
```
```text
+------------+------------+--------------+----------+----------+
| session_id | stage      | seq          | a_then_m | m_gap_k  |
+------------+------------+--------------+----------+----------+
| s1         | diagnosis  | [A, M, K, _] | [[3, 4]] | [[4, 5]] |
| s1         | mitigation | [_, M, M, M] | []       | []       |
| s2         | diagnosis  | [M, M]       | []       | []       |
+------------+------------+--------------+----------+----------+
```

### A step can be several things: symbol sets

When `sym` is a list, a row matches X if X is in it. `step_symbols.syms` is the full set, adding C (k8s change) and T (traces). With priority symbols, s2's metrics+logs step is just M. Corpus: `W8a`.

```sql
SELECT session_id, stage, seq_match(pos, step_id, sym, 'M L') AS by_priority, seq_match(pos, step_id, syms, 'M L') AS by_set
FROM step_symbols GROUP BY 1, 2 ORDER BY 1, 2
```
```text
+------------+------------+-------------+----------+
| session_id | stage      | by_priority | by_set   |
+------------+------------+-------------+----------+
| s1         | diagnosis  | []          | []       |
| s1         | mitigation | []          | []       |
| s2         | diagnosis  | []          | [[3, 4]] |
+------------+------------+-------------+----------+
```

### Conditions on earlier rows: back-references

With a 5th argument `key`, the atom `X[=Y]` matches a row with symbol X whose key equals the key of the last row this match bound to Y. `X[!=Y]` requires the keys to differ. This is MATCH_RECOGNIZE's `DEFINE X AS key = LAST(Y.key)`. A NULL key never matches. Typical uses are "kept refining the same metric" vs "kept switching" (corpus: `W8b`) and "touched the namespace the alert named".

```sql
SELECT seq_match(o, o, s, 'M M[=M]+', k) AS refine, seq_match(o, o, s, 'M M[!=M]', k) AS switch,
  seq_match(o, o, s, 'M (. M[=M])+', k) AS refine_gap1
FROM (VALUES (1, 'M', 'x'), (2, 'M', 'x'), (3, 'M', 'y'), (4, 'L', NULL), (5, 'M', 'y'), (6, 'M', 'z')) t(o, s, k)
```
```text
+----------+------------------+-------------+
| refine   | switch           | refine_gap1 |
+----------+------------------+-------------+
| [[1, 2]] | [[2, 3], [5, 6]] | [[3, 4, 5]] |
+----------+------------------+-------------+
```

On the fixture, the key is the step's sorted metric names:

```sql
SELECT session_id, stage, seq_match(pos, step_id, syms, 'M M[=M]', metric_key) AS same_metric_twice
FROM step_symbols GROUP BY 1, 2 ORDER BY 1, 2
```
```text
+------------+------------+-------------------+
| session_id | stage      | same_metric_twice |
+------------+------------+-------------------+
| s1         | diagnosis  | [[3, 4]]          |
| s1         | mitigation | [[9, 10]]         |
| s2         | diagnosis  | []                |
+------------+------------+-------------------+
```

## 5 Comparing runs of the same problem

### Sets: what did both runs look at?

Corpus: `W5a`, `W5b` (Jaccard over metric sets).

```sql
SELECT unnest(array_intersect(a.m, b.m)) AS shared_metric FROM
  (SELECT array_distinct(flatten(array_agg(promql_metrics(query)))) AS m FROM promql WHERE session_id = 's1') a,
  (SELECT array_distinct(flatten(array_agg(promql_metrics(query)))) AS m FROM promql WHERE session_id = 's2') b
```
```text
+---------------+
| shared_metric |
+---------------+
| up            |
+---------------+
```

### Sequences: where did they diverge?

`seq_prefix(a, b)` is the length of the common prefix. `seq_edit_distance(a, b)` is the Levenshtein distance. `seq_align(a, b)` is one optimal alignment, where `x` = same, `x>y` = a did x where b did y, `x>` = only in a and `>y` = only in b. (`_` below is a step with no telemetry call.) They work on any `list<utf8>`: step symbols, tool names or PromQL shapes. Corpus: `X1a` (similarity by outcome), `X1b` (what each did at the divergence) and `X1c` (aligned differences).

```sql
WITH q AS (SELECT session_id, array_agg(coalesce(sym, '_') ORDER BY pos) AS q FROM step_symbols WHERE stage = 'diagnosis' GROUP BY 1)
SELECT a.q AS s1, b.q AS s2, seq_prefix(a.q, b.q) AS prefix, seq_edit_distance(a.q, b.q) AS dist, seq_align(a.q, b.q) AS alignment
FROM q a, q b WHERE a.session_id = 's1' AND b.session_id = 's2'
```
```text
+--------------+--------+--------+------+------------------+
| s1           | s2     | prefix | dist | alignment        |
+--------------+--------+--------+------+------------------+
| [A, M, K, _] | [M, M] | 0      | 3    | [A>M, M, K>, _>] |
+--------------+--------+--------+------+------------------+
```

Tool names make a finer alphabet:

```sql
WITH q AS (SELECT session_id, array_agg(fn ORDER BY step_id, call_idx) AS q FROM tool_calls t JOIN steps USING (session_id, step_id) WHERE stage = 'diagnosis' GROUP BY 1)
SELECT seq_align(a.q, b.q) AS alignment FROM q a, q b WHERE a.session_id = 's1' AND b.session_id = 's2'
```
```text
+--------------------------------------------------------------+
| alignment                                                    |
+--------------------------------------------------------------+
| [get_metrics, get_alerts>, get_metrics, bash>fetch_pod_logs] |
+--------------------------------------------------------------+
```

## 6 Semantic analysis with an LLM

`llm(prompt)` returns Claude's text reply. You build the prompt in SQL and post-process the reply in SQL. Use it for questions no parser answers: which workload the diagnosis blames, whether a result was used, whether a claim is supported.

- **Cache:** every (model, prompt) is cached in `TDB_LLM_CACHE` (default `llm_cache.jsonl`), so reruns are free and reproducible. These examples read `examples/llm_cache.jsonl`, which holds hand-written responses, and make no calls.
- **Cost guard:** `TDB_LLM_DRY=1` counts the distinct uncached prompts a query needs without calling. `TDB_LLM_MAX_CALLS` (default 200) caps calls per command.
- **Model:** `TDB_LLM_MODEL` (default `claude-opus-5-5`) and `TDB_LLM_EFFORT` (default `low`).
- **Credentials:** `ANTHROPIC_API_KEY` or `ANTHROPIC_AUTH_TOKEN`.

Corpus: `queries/llm/` (S1a, S1b, S2). These are not part of `tdb bench`, because they need the API.

Extraction, then a walk: name the blamed workload, then find the first tool call that touched it.

```sql
WITH blamed AS (
  SELECT session_id, llm('Name the workload this diagnosis blames, in lowercase, nothing else: ' || message) AS w
  FROM steps WHERE source = 'agent' AND stage = 'diagnosis' AND length(message) > 0
)
SELECT b.session_id, b.w AS blamed, min(t.step_id) AS first_touch_step
FROM blamed b JOIN tool_calls t ON t.session_id = b.session_id AND strpos(t.args, b.w) > 0
GROUP BY 1, 2
```
```text
+------------+--------+------------------+
| session_id | blamed | first_touch_step |
+------------+--------+------------------+
| s1         | api    | 3                |
+------------+--------+------------------+
```

`llm` is volatile on purpose. Otherwise the optimizer may push a filter on its output below a join and call the model on rows the join would have dropped.

## 7 Your own functions

`CREATE FUNCTION name(arg TYPE, ...) [RETURNS TYPE] RETURN <expr>` defines a scalar function in SQL, no rebuild. Arguments are named or positional (`$1`). Each call is inlined into the query, so the optimizer sees the plain expression. The body may call any function, including `llm()` and other SQL functions. Definitions last one session: put them before the query (`tdb sql "CREATE FUNCTION ...; SELECT ..."`) or in `defs.sql` (or `$TDB_DEFS`), which runs at startup and may also hold `CREATE VIEW`s. Scalar only; aggregates like `seq_match` are Rust.

```sql
CREATE OR REPLACE FUNCTION norm_dist(a VARCHAR[], b VARCHAR[]) RETURN seq_edit_distance(a, b) / greatest(array_length(a), array_length(b), 1)::double;
CREATE OR REPLACE FUNCTION similar(a VARCHAR[], b VARCHAR[]) RETURNS BOOLEAN RETURN norm_dist(a, b) <= 0.34;
SELECT norm_dist(['A', 'M', 'L'], ['A', 'K', 'L']) AS d, similar(['A', 'M', 'L'], ['A', 'K', 'L']) AS sim
```
```text
+--------------------+------+
| d                  | sim  |
+--------------------+------+
| 0.3333333333333333 | true |
+--------------------+------+
```

A prompt template is just a function over `llm()`:

```sql
CREATE OR REPLACE FUNCTION blamed_workload(diagnosis VARCHAR) RETURNS VARCHAR
  RETURN llm('Name the workload this diagnosis blames, in lowercase, nothing else: ' || diagnosis);
SELECT session_id, blamed_workload(message) AS blamed FROM steps WHERE source = 'agent' AND stage = 'diagnosis' AND length(message) > 0
```
```text
+------------+--------+
| session_id | blamed |
+------------+--------+
| s1         | api    |
+------------+--------+
```

## 8 Physical design

`src/views.sql` declares the built-in derived views (compiled in; your own go in `defs.sql`). A view written `CREATE MATERIALIZED VIEW v ORDER BY (k, ...) AS q` is computed at `ingest` (and `synth`) into `db/v.parquet`, sorted by `k`, and queries read that copy. `TDB_LAZY=1` evaluates every view from the base tables instead. Results are identical either way, and `tdb bench` passes in both modes.

```
TDB_LAZY=1 tdb sql "..."       # lazy: views over base tables
tdb synth 50 /tmp/db50         # 50x replicated sessions, for timing (compresses unrealistically well)
scripts/scale.sh 1 10 50       # times every *-baseline / *-seqmatch query pair
```
