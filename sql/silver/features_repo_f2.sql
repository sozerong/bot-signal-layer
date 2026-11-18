-- features_repo f2: f1(features_repo.sql)에서 저활동 계정당 star 상한만 50 -> 200. 나머지는 같다 (implementation_design.md 8절 L8-3)
-- 저활동 판정은 계정 피처(features_account.sql)의 것을 쓴다. 정의를 한 곳에만 둔다. 그래서 features_account 다음에 돈다
-- 식은 사전 실험 02_detect.py의 star, pair, burst, repo 표를 옮긴 것이다
WITH acct AS (
  SELECT entity_key AS actor_key, is_low_activity, endswith(entity_name, '[bot]') AS declared_bot
  FROM bot.silver.entity_features
  WHERE entity_type = 'gh_account' AND event_date = DATE(:window_end) AND feature_version = 'f1'
),
star AS (  -- 선언 봇을 뺀 WatchEvent
  SELECT e.actor_key, e.target, e.target_name, e.event_ts, a.is_low_activity AS low_act
  FROM bot.silver.events e JOIN acct a ON e.actor_key = a.actor_key
  WHERE e.source = 'gh' AND e.event_date = DATE(:window_end) AND e.action = 'WatchEvent' AND NOT a.declared_bot
),
burst AS (  -- 저장소마다 가장 몰린 5분 구간(그 star부터 300초 뒤까지)의 star 수
  SELECT target, max(c) AS max_5m_stars FROM (
    SELECT target, count(*) OVER (PARTITION BY target ORDER BY unix_seconds(event_ts)
                                  RANGE BETWEEN CURRENT ROW AND 300 FOLLOWING) AS c
    FROM star) GROUP BY target
),
la AS (  -- 저활동 계정의 star. 계정당 star 2~200개만. 상한을 50에서 올렸다 (f2). 쌍이 늘어 자기 조인이 무거워질 것으로 본다
  SELECT * FROM star
  WHERE low_act AND actor_key IN (SELECT actor_key FROM star WHERE low_act GROUP BY actor_key HAVING count(*) BETWEEN 2 AND 200)
),
pair AS (  -- 같은 저활동 계정이 1시간 안에 같이 star한 저장소 쌍. 이 프로젝트에서 가장 무거운 계산 (자기 조인)
  -- 공통 계정 10명 미만인 쌍은 버린다 (f1과 같다). stars >= 20에서 비율 0.5를 넘으려면 어차피 10명 이상이다
  SELECT a.target AS r1, b.target AS r2, count(DISTINCT a.actor_key) AS common
  FROM la a JOIN la b ON a.actor_key = b.actor_key AND a.target < b.target
   AND abs(unix_seconds(a.event_ts) - unix_seconds(b.event_ts)) <= 3600
  GROUP BY a.target, b.target HAVING count(DISTINCT a.actor_key) >= 10
),
co AS (
  SELECT target, max(common) AS max_co_star FROM (
    SELECT r1 AS target, common FROM pair UNION ALL SELECT r2, common FROM pair) GROUP BY target
)
SELECT
  'gh_repo'                         AS entity_type,
  s.target                          AS entity_key,
  DATE(:window_end)                 AS event_date,
  'f2'                              AS feature_version,
  max(s.target_name)                AS entity_name,
  CAST(NULL AS BIGINT)              AS n_events,
  CAST(NULL AS BIGINT)              AS active_hours,
  CAST(NULL AS BIGINT)              AS n_types,
  CAST(NULL AS BOOLEAN)             AS is_low_activity,
  count(*)                          AS stars,
  count_if(s.low_act)               AS low_act_stars,
  max(b.max_5m_stars)               AS max_5m_stars,
  coalesce(max(c.max_co_star), 0)   AS max_co_star,
  CAST(NULL AS BIGINT)              AS max_repos_per_hour  -- 계정 컬럼
FROM star s
JOIN burst b ON s.target = b.target
LEFT JOIN co c ON s.target = c.target
GROUP BY s.target
