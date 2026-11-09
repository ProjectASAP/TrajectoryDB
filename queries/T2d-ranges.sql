-- T2d: range / subquery window sizes the agent asks for.
SELECT r AS window_, count(*) AS uses
FROM (SELECT unnest(promql_ranges(query)) AS r FROM promql)
GROUP BY r
ORDER BY uses DESC, r;
