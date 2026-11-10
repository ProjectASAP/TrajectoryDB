-- X1a: cross-run alignment. For every pair of runs of the same problem, compare their diagnosis-stage step-symbol
-- sequences: common prefix, edit distance, and distance / longer length. Order-aware, unlike W5b's metric-set Jaccard.
WITH seqs AS (
  SELECT s.session_id, s.problem_id, s.run, s.diagnosis_passed AS ok, array_agg(coalesce(y.sym, '-') ORDER BY y.pos) AS q
  FROM sessions s JOIN step_symbols y ON y.session_id = s.session_id AND y.stage = 'diagnosis'
  GROUP BY 1, 2, 3, 4
), pairs AS (
  SELECT CASE WHEN a.ok AND b.ok THEN 'both_pass' WHEN NOT a.ok AND NOT b.ok THEN 'both_fail' ELSE 'one_pass' END AS outcomes,
    seq_prefix(a.q, b.q) AS pre, seq_edit_distance(a.q, b.q) AS d, greatest(cardinality(a.q), cardinality(b.q)) AS len
  FROM seqs a JOIN seqs b ON a.problem_id = b.problem_id AND a.run < b.run
  WHERE a.ok IS NOT NULL AND b.ok IS NOT NULL
)
SELECT outcomes, count(*) AS pairs, round(avg(pre), 2) AS avg_prefix, round(avg(d), 2) AS avg_dist,
  round(avg(d * 1.0 / len), 3) AS avg_norm_dist, round(avg(len), 1) AS avg_len
FROM pairs GROUP BY 1 ORDER BY 1;
