-- Agent PromQL workload export (one JSON object per line). Run from the repo root:
--   ./target/release/tdb sql "$(cat export/agent_promql.sql)"
-- Each row is one PromQL string the agent sent, in issue order, with what it touched and how it went.
COPY (
  SELECT p.session_id, s.run, s.problem_id, s.application, st.stage, p.step_id, p.call_idx, st.ts, p.via, p.query,
    promql_asap_class(p.query) AS asap_class, promql_normalize(p.query, 'shape') AS shape,
    promql_metrics(p.query) AS metrics, promql_ranges(p.query) AS ranges, m.kind AS result_kind
  FROM promql p
  JOIN sessions s USING (session_id)
  JOIN steps st USING (session_id, step_id)
  LEFT JOIN metric_results m ON m.session_id = p.session_id AND m.call_id = p.call_id
  ORDER BY st.ts, p.session_id, p.step_id, p.call_idx
) TO 'out/agent_promql.json' STORED AS JSON;
