-- T2b: most-queried metric names (top 20), with spread across sessions, problems and applications.
SELECT m.metric, count(*) AS uses, count(DISTINCT m.session_id) AS sessions,
  count(DISTINCT s.problem_id) AS problems, count(DISTINCT s.application) AS apps
FROM (SELECT session_id, unnest(promql_metrics(query)) AS metric FROM promql) m JOIN sessions s USING (session_id)
GROUP BY m.metric
ORDER BY uses DESC, m.metric
LIMIT 20;
