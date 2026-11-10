-- T5d: caching potential of get_metrics under each promql_normalize mode (coarser = more hits).
-- A call is a hit in a scope if an earlier call (by step timestamp) in that scope had the same normalized key:
--   session: same session; problem: any session of the same problem (other runs); global: anything earlier.
-- session_hits_ran: session hits that actually reached Prometheus (not refused by HolmesGPT's exact-dup guard).
-- Unparseable queries (10) are dropped in non-exact modes.
WITH k AS (
  SELECT md.mode, m.session_id, m.step_id, m.call_idx, m.kind, st.ts, s.problem_id,
         promql_normalize(m.query, md.mode) AS key
  FROM metric_results m
  JOIN steps st USING (session_id, step_id)
  JOIN sessions s USING (session_id)
  CROSS JOIN (VALUES (1, 'exact'), (2, 'canonical'), (3, 'no_window'), (4, 'shape')) md(ord, mode)
), r AS (
  SELECT *,
    row_number() OVER (PARTITION BY mode, session_id, key ORDER BY step_id, call_idx) AS rn_session,
    row_number() OVER (PARTITION BY mode, problem_id, key ORDER BY ts, session_id, step_id, call_idx) AS rn_problem,
    row_number() OVER (PARTITION BY mode, key ORDER BY ts, session_id, step_id, call_idx) AS rn_global
  FROM k WHERE key IS NOT NULL
)
SELECT mode, count(*) AS calls, count(DISTINCT key) AS distinct_keys,
  sum(CASE WHEN rn_session > 1 THEN 1 ELSE 0 END) AS session_hits,
  sum(CASE WHEN rn_session > 1 AND kind <> 'refused_duplicate' THEN 1 ELSE 0 END) AS session_hits_ran,
  sum(CASE WHEN rn_problem > 1 THEN 1 ELSE 0 END) AS problem_hits,
  sum(CASE WHEN rn_global > 1 THEN 1 ELSE 0 END) AS global_hits,
  round(sum(CASE WHEN rn_global > 1 THEN 100.0 ELSE 0 END) / count(*), 1) AS global_hit_pct
FROM r
GROUP BY mode
ORDER BY min(CASE mode WHEN 'exact' THEN 1 WHEN 'canonical' THEN 2 WHEN 'no_window' THEN 3 ELSE 4 END);
