-- Views registered on every tdb context. Whole-line comments are stripped, then statements are split on semicolons.
-- CREATE MATERIALIZED VIEW v ORDER BY (k, ...) AS q: ingest/synth also write q to <db>/v.parquet sorted by k,
-- and contexts read that copy (declared sort order) unless TDB_LAZY=1. Declare in dependency order.

-- promql: every PromQL string the agent sent: native get_metrics + query= params inside bash (curl to /api/v1/query*).
CREATE VIEW promql AS
SELECT session_id, step_id, call_idx, call_id, 'get_metrics' AS via, query FROM tool_calls WHERE fn = 'get_metrics'
UNION ALL
SELECT session_id, step_id, call_idx, call_id, 'bash_url' AS via, unnest(url_promql(command)) AS query
FROM tool_calls WHERE fn = 'bash' AND command LIKE '%query=%';

-- metric_results: each get_metrics call with its result classified from content (observations.status is unreliable).
CREATE MATERIALIZED VIEW metric_results ORDER BY (session_id, step_id, call_idx) AS
SELECT t.session_id, t.step_id, t.call_idx, t.call_id, st.stage, t.query,
  CASE
    WHEN o.content LIKE 'Refusing to run this tool call because it has already been called%' THEN 'refused_duplicate'
    WHEN o.content LIKE 'The tool call result is too large%' THEN 'too_large'
    WHEN o.content LIKE 'Timed out%' THEN 'timeout'
    WHEN o.content LIKE '%Error querying%' THEN 'backend_error'
    WHEN o.content LIKE '%''result'': []%' THEN 'empty'
    WHEN o.content LIKE '{''resultType''%' THEN 'data'
    ELSE 'other'
  END AS kind
FROM tool_calls t
JOIN steps st USING (session_id, step_id)
LEFT JOIN observations o ON o.session_id = t.session_id AND o.call_id = t.call_id
WHERE t.fn = 'get_metrics';

-- promql_steps: one row per agent step that issued get_metrics, with the union of what its queries touch.
-- linked = shares a metric name with the previous PromQL step of the same session+stage (other tools may sit between).
-- chain_id numbers maximal runs of linked steps (gaps-and-islands). Discovery-only steps (no metric name) never link.
CREATE MATERIALIZED VIEW promql_steps ORDER BY (session_id, stage, step_id) AS
WITH s AS (
  SELECT session_id, stage, step_id, count(*) AS calls,
    array_distinct(flatten(array_agg(promql_metrics(query))  FILTER (WHERE promql_error(query) IS NULL))) AS metrics,
    array_distinct(flatten(array_agg(promql_funcs(query))    FILTER (WHERE promql_error(query) IS NULL))) AS funcs,
    array_distinct(flatten(array_agg(promql_aggs(query))     FILTER (WHERE promql_error(query) IS NULL))) AS aggs,
    array_distinct(flatten(array_agg(promql_ranges(query))   FILTER (WHERE promql_error(query) IS NULL))) AS ranges,
    array_distinct(flatten(array_agg(promql_matchers(query)) FILTER (WHERE promql_error(query) IS NULL))) AS matchers
  FROM metric_results GROUP BY session_id, stage, step_id
), l AS (
  SELECT *, row_number() OVER w AS pos,
    lag(metrics) OVER w AS prev_metrics, lag(funcs) OVER w AS prev_funcs, lag(aggs) OVER w AS prev_aggs,
    lag(ranges) OVER w AS prev_ranges, lag(matchers) OVER w AS prev_matchers
  FROM s WINDOW w AS (PARTITION BY session_id, stage ORDER BY step_id)
), k AS (
  SELECT *, coalesce(array_has_any(metrics, prev_metrics), false) AS linked FROM l
)
SELECT *, sum(CASE WHEN linked THEN 0 ELSE 1 END) OVER (PARTITION BY session_id, stage ORDER BY step_id) AS chain_id
FROM k;

-- telemetry_calls: one row per (tool call, modality) with the namespace(s) it targets when known.
-- A bash call can carry several modalities (e.g. kubectl logs | grep, then curl a PromQL URL).
-- k8s_change = mutating kubectl verbs, which the diagnosis prompt forbids.
CREATE MATERIALIZED VIEW telemetry_calls ORDER BY (session_id, step_id, call_idx) AS
WITH b AS (
  SELECT session_id, step_id, call_idx, unnest(kubectl_targets(command)) AS tgt, bash_namespaces(command) AS ns
  FROM tool_calls WHERE fn = 'bash'
), c AS (
  SELECT session_id, step_id, call_idx, 'metrics' AS modality, NULL AS ns FROM tool_calls WHERE fn = 'get_metrics'
  UNION ALL SELECT session_id, step_id, call_idx, 'metrics', NULL FROM tool_calls WHERE fn = 'bash' AND cardinality(url_promql(command)) > 0
  UNION ALL SELECT session_id, step_id, call_idx, 'logs', json_get(args, 'namespace') FROM tool_calls WHERE fn = 'fetch_pod_logs'
  UNION ALL SELECT session_id, step_id, call_idx, 'traces', NULL FROM tool_calls WHERE fn IN ('get_traces', 'get_operations', 'get_dependency_graph')
  UNION ALL SELECT session_id, step_id, call_idx, 'alerts', NULL FROM tool_calls WHERE fn = 'get_alerts'
  UNION ALL SELECT session_id, step_id, call_idx, 'k8s_state', namespace FROM tool_calls
    WHERE fn LIKE 'kubernetes_%' OR fn LIKE 'kubectl_%' OR fn = 'get_services'
  UNION ALL SELECT session_id, step_id, call_idx,
    CASE WHEN split_part(tgt, ' ', 1) = 'logs' THEN 'logs'
         WHEN split_part(tgt, ' ', 1) IN ('apply', 'patch', 'delete', 'scale', 'edit', 'label', 'annotate', 'set', 'create',
              'replace', 'cordon', 'drain', 'uncordon', 'taint') OR tgt IN ('rollout restart', 'rollout undo') THEN 'k8s_change'
         WHEN split_part(tgt, ' ', 1) IN ('get', 'describe', 'top', 'events', 'rollout', 'api-resources', 'auth', 'wait') THEN 'k8s_state'
         ELSE 'other' END,
    ns[1]
    FROM b
)
SELECT DISTINCT c.session_id, c.step_id, c.call_idx, st.stage, c.modality, c.ns AS namespace
FROM c JOIN steps st USING (session_id, step_id);

-- step_symbols: every agent step with its position in (session, stage) and one symbol by priority
-- A(lerts) > M(etrics) > L(ogs) > K(8s state); NULL for other/no tools. Input for sequence-pattern walks (W7).
-- syms: the full symbol set of the step (also C = k8s change, T = traces), for set-valued seq_match.
-- metric_key: the step's sorted metric names joined by ',' (NULL without named metrics), a seq_match back-reference key.
CREATE MATERIALIZED VIEW step_symbols ORDER BY (session_id, stage, step_id) AS
WITH m AS (
  SELECT session_id, step_id,
    CASE WHEN bool_or(modality = 'alerts') THEN 'A' WHEN bool_or(modality = 'metrics') THEN 'M'
         WHEN bool_or(modality = 'logs') THEN 'L' WHEN bool_or(modality = 'k8s_state') THEN 'K' END AS sym,
    array_sort(array_distinct(array_agg(CASE modality WHEN 'alerts' THEN 'A' WHEN 'metrics' THEN 'M' WHEN 'logs' THEN 'L'
      WHEN 'k8s_state' THEN 'K' WHEN 'k8s_change' THEN 'C' WHEN 'traces' THEN 'T' END))) AS syms
  FROM telemetry_calls GROUP BY 1, 2
)
SELECT st.session_id, st.stage, st.step_id,
  row_number() OVER (PARTITION BY st.session_id, st.stage ORDER BY st.step_id) AS pos, m.sym, coalesce(m.syms, []) AS syms,
  nullif(array_to_string(array_sort(p.metrics), ','), '') AS metric_key
FROM steps st LEFT JOIN m USING (session_id, step_id) LEFT JOIN promql_steps p USING (session_id, step_id)
WHERE st.source = 'agent';
