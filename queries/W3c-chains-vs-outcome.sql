-- W3c: longest diagnosis-stage refinement chain per session vs diagnosis outcome (sessions with known outcome only).
-- Sessions with no diagnosis PromQL are bucket '0'.
WITH m AS (
  SELECT session_id, max(len) AS longest FROM (
    SELECT session_id, chain_id, count(*) AS len FROM promql_steps WHERE stage = 'diagnosis' GROUP BY 1, 2
  ) GROUP BY 1
)
SELECT CASE WHEN coalesce(m.longest, 0) >= 4 THEN '4+' ELSE CAST(coalesce(m.longest, 0) AS VARCHAR) END AS longest_chain,
  count(*) AS sessions,
  round(avg(CASE WHEN s.diagnosis_passed THEN 100.0 ELSE 0 END), 1) AS diag_pass_pct
FROM sessions s LEFT JOIN m USING (session_id)
WHERE s.diagnosis_passed IS NOT NULL
GROUP BY 1
ORDER BY 1;
