-- T5c: for each problem, PromQL issued (exact string) in how many of the runs that attempted it.
-- Summary: distinct (problem, query) pairs bucketed by runs_issued / runs_attempted.
WITH att AS (
  SELECT problem_id, count(DISTINCT run) AS runs_attempted FROM sessions GROUP BY problem_id
), pq AS (
  SELECT s.problem_id, promql_normalize(t.query, 'exact') AS query, count(DISTINCT s.run) AS runs_issued
  FROM promql t JOIN sessions s USING (session_id)
  GROUP BY 1, 2
)
SELECT a.runs_attempted, pq.runs_issued, count(*) AS problem_query_pairs
FROM pq JOIN att a USING (problem_id)
GROUP BY a.runs_attempted, pq.runs_issued
ORDER BY a.runs_attempted, pq.runs_issued;
