-- W6a: cross-tool linear walk. Between consecutive tool-calling agent steps (same session+stage), which telemetry
-- modality follows which? Steps with several modalities contribute every from×to pair. Top 25; pct_of_from is the
-- share of all transitions leaving that modality.
WITH sm AS (SELECT DISTINCT session_id, stage, step_id, modality FROM telemetry_calls),
seq AS (
  SELECT session_id, stage, step_id, dense_rank() OVER (PARTITION BY session_id, stage ORDER BY step_id) AS pos
  FROM (SELECT DISTINCT session_id, stage, step_id FROM sm)
),
x AS (SELECT sm.*, seq.pos FROM sm JOIN seq USING (session_id, stage, step_id)),
t AS (SELECT a.modality AS from_m, b.modality AS to_m FROM x a JOIN x b
      ON a.session_id = b.session_id AND a.stage = b.stage AND b.pos = a.pos + 1)
SELECT from_m, to_m, count(*) AS transitions,
  round(count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY from_m), 1) AS pct_of_from
FROM t GROUP BY 1, 2
ORDER BY transitions DESC, from_m, to_m
LIMIT 25;
