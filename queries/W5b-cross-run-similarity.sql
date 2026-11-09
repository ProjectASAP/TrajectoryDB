-- W5b: cross walk. Pairs of sessions on the same problem from different runs: Jaccard similarity of the
-- metric-name sets they queried (all stages), split by the pair's diagnosis outcomes.
-- Pairs where neither session named any metric are excluded.
WITH ms AS (SELECT session_id, array_distinct(flatten(array_agg(metrics))) AS m FROM promql_steps GROUP BY 1),
ss AS (SELECT s.session_id, s.problem_id, s.run, s.diagnosis_passed AS ok, ms.m FROM sessions s JOIN ms USING (session_id)),
pairs AS (
  SELECT a.ok AS a_ok, b.ok AS b_ok,
    cardinality(array_intersect(a.m, b.m)) * 1.0 / cardinality(array_union(a.m, b.m)) AS j
  FROM ss a JOIN ss b ON a.problem_id = b.problem_id AND a.run < b.run
  WHERE cardinality(array_union(a.m, b.m)) > 0
)
SELECT CASE WHEN a_ok IS NULL OR b_ok IS NULL THEN 'unknown'
            WHEN a_ok AND b_ok THEN 'both_pass'
            WHEN NOT a_ok AND NOT b_ok THEN 'both_fail'
            ELSE 'one_pass' END AS outcomes,
  count(*) AS pairs, round(avg(j), 3) AS avg_jaccard,
  round(avg(CASE WHEN j = 0 THEN 100.0 ELSE 0 END), 1) AS pct_disjoint
FROM pairs
GROUP BY 1
ORDER BY 1;
