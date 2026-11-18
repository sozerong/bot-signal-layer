-- events 그날 -> entity_features 계정 행. 한 행 = 계정 x 날짜 x 피처 버전 (4.2절)
-- 저장소 컬럼은 NULL. features_repo.sql과 컬럼 이름과 형을 맞춘다 (같은 표에 쓴다)
-- 두 단계로 센다: 안쪽은 (계정, 시간, 행동), 바깥은 계정. 무거운 집계를 여기서 한 번 하고 근거는 문턱만 본다 (F-12, 결정 13)
-- max_repos_per_hour를 더할 때 기존 컬럼 값은 그대로라 피처 버전은 f1 그대로다 (결정 9와 같은 판단, M-17 지문)
SELECT
  'gh_account'                                AS entity_type,
  actor_key                                   AS entity_key,
  event_date,
  'f1'                                        AS feature_version,
  max(actor_name)                             AS entity_name,      -- max: 다시 돌려도 같은 값 (any_value는 매번 다를 수 있다)
  sum(n)                                      AS n_events,
  count(DISTINCT h)                           AS active_hours,
  count(DISTINCT action)                      AS n_types,
  -- 그날 WatchEvent만 함. 사전 실험은 star한 계정 중 n_types = 1이었다. star 안 한 계정까지 넓혀도 같은 뜻이 되게 종류를 확인한다
  count(DISTINCT action) = 1 AND max(action) = 'WatchEvent' AS is_low_activity,
  CAST(NULL AS BIGINT)                        AS stars,
  CAST(NULL AS BIGINT)                        AS low_act_stars,
  CAST(NULL AS BIGINT)                        AS max_5m_stars,
  CAST(NULL AS BIGINT)                        AS max_co_star,
  max(n_repos)                                AS max_repos_per_hour  -- 같은 행동을 한 시간 안에 한 서로 다른 저장소 수의 최댓값
FROM (
  SELECT actor_key, event_date, hour(event_ts) AS h, action,
         max(actor_name) AS actor_name, count(*) AS n, count(DISTINCT target) AS n_repos
  FROM bot.silver.events
  WHERE source = 'gh' AND event_date = DATE(:window_end)  -- ponytail: 판정 구간은 지금 하루. 여러 날이면 날짜별로 돈다
  GROUP BY actor_key, event_date, hour(event_ts), action
)
GROUP BY actor_key, event_date
