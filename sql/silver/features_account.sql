-- events 그날 -> entity_features 계정 행. 한 행 = 계정 x 날짜 x 피처 버전 (4.2절)
-- 저장소 컬럼은 NULL. features_repo.sql과 컬럼 순서와 형을 맞춘다 (같은 표에 쓴다)
SELECT
  'gh_account'                                AS entity_type,
  actor_key                                   AS entity_key,
  event_date,
  'f1'                                        AS feature_version,
  max(actor_name)                             AS entity_name,      -- max: 다시 돌려도 같은 값 (any_value는 매번 다를 수 있다)
  count(*)                                    AS n_events,
  count(DISTINCT hour(event_ts))              AS active_hours,
  count(DISTINCT action)                      AS n_types,
  -- 그날 WatchEvent만 함. 사전 실험은 star한 계정 중 n_types = 1이었다. star 안 한 계정까지 넓혀도 같은 뜻이 되게 종류를 확인한다
  count(DISTINCT action) = 1 AND max(action) = 'WatchEvent' AS is_low_activity,
  CAST(NULL AS BIGINT)                        AS stars,
  CAST(NULL AS BIGINT)                        AS low_act_stars,
  CAST(NULL AS BIGINT)                        AS max_5m_stars,
  CAST(NULL AS BIGINT)                        AS max_co_star
FROM bot.silver.events
WHERE source = 'gh' AND event_date = DATE(:window_end)  -- ponytail: 판정 구간은 지금 하루. 여러 날이면 날짜별로 돈다
GROUP BY actor_key, event_date
