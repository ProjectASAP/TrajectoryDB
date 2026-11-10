# Examples

One small example per feature. Each one runs against `examples/fixture`, two hand-written sessions of problem `p1`:

- **s1** (run-01): diagnosis queries `rate(http_requests_total{job="api"}[5m])` (data) and `[1m]` (empty), then kubectl. Mitigation restarts the app, re-runs the `[5m]` query (data), repeats it (refused), and curls `up{job="api"}`.
- **s2** (run-02): a discovery query `{__name__=~"http.*"}` (too large), then `up{job="api"}` and pod logs in the same step.

Check every example with `cargo run --release -- examples`. Pass `--update` to rewrite the outputs, then review the diff.

## Tables

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

## PromQL UDFs

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

`promql_normalize(q, mode)`: each mode is coarser than the one before. `exact` returns q unchanged. `canonical` re-prints it with matchers and `by` labels sorted. `no_window` also blanks windows and offsets. `shape` also blanks label values.

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
SELECT url_promql('curl "http://p:9090/api/v1/query_range?query=sum%28up%29&step=30"') AS q
```
```text
+-----------+
| q         |
+-----------+
| [sum(up)] |
+-----------+
```

`promql_asap_class` mirrors ASAPQuery's planner patterns. `promql_asap_blocker` gives the reason a query is unsupported.

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

## Shell and argument UDFs

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

## Views

`promql`: every PromQL string sent, whether through `get_metrics` or as a `query=` parameter inside bash.

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

`metric_results`: each `get_metrics` result classified from its content.

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

`promql_steps`: a step is `linked` when it shares a metric name with the previous PromQL step in its stage. `chain_id` numbers each run of linked steps.

```sql
SELECT session_id, stage, step_id, metrics, linked, chain_id FROM promql_steps ORDER BY session_id, stage, step_id
```
```text
+------------+------------+---------+-----------------------+--------+----------+
| session_id | stage      | step_id | metrics               | linked | chain_id |
+------------+------------+---------+-----------------------+--------+----------+
| s1         | diagnosis  | 3       | [http_requests_total] | false  | 1        |
| s1         | diagnosis  | 4       | [http_requests_total] | true   | 1        |
| s1         | mitigation | 9       | [http_requests_total] | false  | 1        |
| s1         | mitigation | 10      | [http_requests_total] | true   | 1        |
| s2         | diagnosis  | 3       | []                    | false  | 1        |
| s2         | diagnosis  | 4       | [up]                  | false  | 2        |
+------------+------------+---------+-----------------------+--------+----------+
```

`telemetry_calls`: one row per tool call and modality, with the target namespace when known.

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

## Walks

Linear: the next PromQL step after each one, within the same session and stage.

```sql
SELECT session_id, stage, step_id, lead(step_id) OVER (PARTITION BY session_id, stage ORDER BY step_id) AS next_promql_step
FROM promql_steps ORDER BY session_id, stage, step_id
```
```text
+------------+------------+---------+------------------+
| session_id | stage      | step_id | next_promql_step |
+------------+------------+---------+------------------+
| s1         | diagnosis  | 3       | 4                |
| s1         | diagnosis  | 4       |                  |
| s1         | mitigation | 9       | 10               |
| s1         | mitigation | 10      |                  |
| s2         | diagnosis  | 3       | 4                |
| s2         | diagnosis  | 4       |                  |
+------------+------------+---------+------------------+
```

Cross-stage: exact repeats of an earlier query, and whether they ran.

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

Tree: the ancestry of one result, plus the cumulative context before its step.

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

Cross-run: metrics queried by both runs of `p1`. This uses the `promql` view, so s1's bash `up` query counts.

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

## Row-pattern matching (`seq_match`)

`step_symbols` gives each agent step a position within its (session, stage) and one symbol: A(lerts) > M(etrics) > L(ogs) > K(8s state), else NULL. `seq_match(ord, id, sym, pattern)` is an aggregate that returns the ids of every match. Matches are leftmost and non-overlapping. In the pattern, `.` matches any row, including a NULL symbol.

```sql
SELECT session_id, stage, array_agg(coalesce(sym, '-') ORDER BY pos) AS syms,
  seq_match(pos, step_id, sym, 'A M') AS a_then_m,
  seq_match(pos, step_id, sym, 'M .{0,1} K') AS m_gap_k
FROM step_symbols GROUP BY 1, 2 ORDER BY 1, 2
```
```text
+------------+------------+--------------+----------+----------+
| session_id | stage      | syms         | a_then_m | m_gap_k  |
+------------+------------+--------------+----------+----------+
| s1         | diagnosis  | [A, M, K, -] | [[3, 4]] | [[4, 5]] |
| s1         | mitigation | [-, M, M, M] | []       | []       |
| s2         | diagnosis  | [M, M]       | []       | []       |
+------------+------------+--------------+----------+----------+
```
