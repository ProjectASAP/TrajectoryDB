-- W5a: cross walk. Like T5c but at metric-name level: for each (problem, metric), in how many of the runs
-- that attempted the problem was the metric queried? Bucketed by runs_used / runs_attempted.
WITH att AS (SELECT problem_id, count(DISTINCT run) AS runs_attempted FROM sessions GROUP BY 1),
pm AS (
  SELECT s.problem_id, m.metric, count(DISTINCT s.run) AS runs_used
  FROM (SELECT session_id, unnest(metrics) AS metric FROM promql_steps) m JOIN sessions s USING (session_id)
  GROUP BY 1, 2
)
SELECT a.runs_attempted, pm.runs_used, count(*) AS problem_metric_pairs
FROM pm JOIN att a USING (problem_id)
GROUP BY 1, 2
ORDER BY 1, 2;
