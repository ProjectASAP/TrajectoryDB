-- W4b: mitigation-stage get_metrics calls whose exact query string was already sent in the same session's
-- diagnosis stage, and how those re-sent calls turned out (HolmesGPT refuses exact duplicates).
SELECT g.kind, count(*) AS mitigation_calls,
  count(*) FILTER (WHERE d.query IS NOT NULL) AS seen_in_diagnosis
FROM metric_results g
LEFT JOIN (SELECT DISTINCT session_id, query FROM metric_results WHERE stage = 'diagnosis') d
  ON d.session_id = g.session_id AND d.query = g.query
WHERE g.stage = 'mitigation'
GROUP BY g.kind
ORDER BY mitigation_calls DESC, g.kind;
