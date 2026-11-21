-- 생존 확인 대상 (implementation_design.md 6절 pick_targets). 한 행 = 확인할 저장소
-- suspect: 그 판정 구간의 fake_star_repo (low_activity AND (lockstep_ratio OR burst_5m), Gold와 같은 식).
--   근거마다 지금 규칙 버전(:v_low_activity 등)만 본다. repo_alive가 rules/registry.yml에서 읽어 넘긴다 (F-13, 결정 16).
--   전에는 버전을 거르지 않아 lockstep_ratio v2와 v3이 함께 있는 날 v3에만 걸린 저장소가 섞였다
-- control: 같은 날 star 20개 이상인 비의심 저장소에서 해시 순으로 suspect와 같은 수.
--   star 수 상한을 suspect의 최댓값으로 두는 것은 사전 실험 02_detect.py 그대로 (아주 큰 저장소가 대조군을 채우지 않게).
--   해시는 Spark xxhash64라 사전 실험(DuckDB hash)과 고른 대조군이 다르다.
--   해시 순서라 suspect가 줄면 옛 대조군은 같은 순서의 앞부분이 남는다. 다만 suspect에서 빠진 저장소가
--   비의심이 되어 대조군 후보로 끼어들 수 있다 (M-21: 버전을 거르자 하루 0~4개)
-- 순서: 의심과 대조군을 번갈아 (순번, 묶음). 묻다가 막혀도 두 묶음이 같이 줄게 (F-15, 결정 16)
WITH ev AS (
  SELECT entity_key,
         bool_or(evidence_id = 'low_activity' AND hit) AS low_act,
         bool_or(evidence_id IN ('lockstep_ratio', 'burst_5m') AND hit) AS coord
  FROM bot.silver.evidence
  WHERE entity_type = 'gh_repo' AND window_end = DATE(:window_end)
    AND ((evidence_id = 'low_activity' AND rule_version = :v_low_activity)
      OR (evidence_id = 'lockstep_ratio' AND rule_version = :v_lockstep_ratio)
      OR (evidence_id = 'burst_5m' AND rule_version = :v_burst_5m))
  GROUP BY entity_key
),
f AS (
  SELECT f.entity_key, f.entity_name, f.stars, coalesce(ev.low_act AND ev.coord, false) AS suspect
  FROM bot.silver.entity_features f LEFT JOIN ev ON f.entity_key = ev.entity_key
  WHERE f.entity_type = 'gh_repo' AND f.event_date = DATE(:window_end) AND f.feature_version = 'f1'
),
s AS (
  SELECT entity_key, entity_name, stars,
         row_number() OVER (ORDER BY xxhash64(entity_key), entity_key) AS rn
  FROM f WHERE suspect
),
c AS (
  SELECT entity_key, entity_name, stars,
         row_number() OVER (ORDER BY xxhash64(entity_key), entity_key) AS rn
  FROM f WHERE NOT suspect AND stars >= 20 AND stars <= (SELECT max(stars) FROM s)
)
SELECT entity_key, entity_name, stars, grp FROM (
  SELECT entity_key, entity_name, stars, 'suspect' AS grp, rn FROM s
  UNION ALL
  SELECT entity_key, entity_name, stars, 'control', rn FROM c WHERE rn <= (SELECT count(*) FROM s)
)
ORDER BY rn, grp DESC  -- 같은 순번에서 suspect 먼저
