-- T7c: why unsupported agent PromQL misses ASAPQuery's patterns (promql_asap_blocker, outermost construct).
-- comparison_on_asap = a filter like `sum(x) > 0` around a query ASAP could serve: strip the filter and it fits.
SELECT promql_asap_blocker(query) AS blocker, count(*) AS calls,
  round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct_of_unsupported,
  count(DISTINCT promql_normalize(query, 'shape')) AS distinct_shapes
FROM promql
WHERE promql_asap_class(query) = 'unsupported'
GROUP BY 1
ORDER BY calls DESC, blocker;
