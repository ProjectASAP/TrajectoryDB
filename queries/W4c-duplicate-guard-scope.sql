-- W4c: scope of HolmesGPT's exact-duplicate guard. For every get_metrics call whose exact query was already sent
-- earlier in the same session, where was the earlier copy (same stage / other stage only), and did it run or get refused?
-- Finding: refusals happen only for same-stage repeats, all same-stage repeats are refused, and cross-stage repeats run.
WITH prior AS (
  SELECT r.session_id, r.call_id,
    max(CASE WHEN p.stage = r.stage THEN 1 ELSE 0 END) AS same_stage
  FROM metric_results r JOIN metric_results p
    ON p.session_id = r.session_id AND p.query = r.query AND p.step_id < r.step_id
  GROUP BY r.session_id, r.call_id
)
SELECT r.stage, CASE WHEN pr.same_stage = 1 THEN 'same_stage' ELSE 'other_stage_only' END AS earlier_copy,
  count(*) AS repeat_calls,
  count(*) FILTER (WHERE r.kind = 'refused_duplicate') AS refused,
  count(*) FILTER (WHERE r.kind <> 'refused_duplicate') AS ran
FROM metric_results r JOIN prior pr ON pr.session_id = r.session_id AND pr.call_id = r.call_id
GROUP BY 1, 2
ORDER BY 1, 2;
