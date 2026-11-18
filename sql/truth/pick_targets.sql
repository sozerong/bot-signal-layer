-- 생존 확인 대상 (implementation_design.md 6절 pick_targets). 한 행 = 확인할 저장소
-- suspect: 그 판정 구간의 fake_star_repo (low_activity AND (lockstep_ratio OR burst_5m), Gold와 같은 식)
-- control: 같은 날 star 20개 이상인 비의심 저장소에서 해시 순으로 suspect와 같은 수.
--   star 수 상한을 suspect의 최댓값으로 두는 것은 사전 실험 02_detect.py 그대로 (아주 큰 저장소가 대조군을 채우지 않게).
--   해시는 Spark xxhash64라 사전 실험(DuckDB hash)과 고른 대조군이 다르다.
WITH ev AS (
  SELECT entity_key,
         bool_or(evidence_id = 'low_activity' AND hit) AS low_act,
         bool_or(evidence_id IN ('lockstep_ratio', 'burst_5m') AND hit) AS coord
  FROM bot.silver.evidence
  WHERE entity_type = 'gh_repo' AND window_end = DATE(:window_end)
    AND evidence_id IN ('low_activity', 'lockstep_ratio', 'burst_5m')
  GROUP BY entity_key
),
f AS (
  SELECT f.entity_key, f.entity_name, f.stars, coalesce(ev.low_act AND ev.coord, false) AS suspect
  FROM bot.silver.entity_features f LEFT JOIN ev ON f.entity_key = ev.entity_key
  WHERE f.entity_type = 'gh_repo' AND f.event_date = DATE(:window_end) AND f.feature_version = 'f1'
),
s AS (SELECT * FROM f WHERE suspect),
c AS (
  SELECT entity_key, entity_name, stars,
         row_number() OVER (ORDER BY xxhash64(entity_key), entity_key) AS rn
  FROM f WHERE NOT suspect AND stars >= 20 AND stars <= (SELECT max(stars) FROM s)
)
SELECT entity_key, entity_name, stars, 'suspect' AS grp FROM s
UNION ALL
SELECT entity_key, entity_name, stars, 'control' FROM c WHERE rn <= (SELECT count(*) FROM s)
