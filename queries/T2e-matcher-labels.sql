-- T2e: which labels the agent filters on, and with which operator (top 20).
SELECT regexp_replace(mt, '(=~|!~|!=|=).*$', '') AS label,
       (regexp_match(mt, '(=~|!~|!=|=)'))[1] AS op, count(*) AS uses
FROM (SELECT unnest(promql_matchers(query)) AS mt FROM promql)
GROUP BY 1, 2
ORDER BY uses DESC, label, op
LIMIT 20;
