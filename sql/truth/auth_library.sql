-- bot.silver.events(auth_lib) -> truth auth_library. 한 행 = web_client x 날짜 (criteria.md 3절: 자기 신고 라벨, 고정)
-- 라벨 = 원 논문 라벨을 만든 방식: UA가 크롤러 목록(monperrus)에 걸리거나 robots.txt를 요청 (02_detect.py 머리 주석, REPORT.md 3절 L1)
--   대상·날짜에 robots.txt 요청이 하나라도 있으면 bot. 원 논문의 사람 검토 라벨은 공개 파일에 없어 목록 판정까지만 따른다
-- 그래서 이 정답은 declared_ua와 같은 신호다. 시드 truth_independence(auth_library,declared_ua,false)가 그 채점을 막는다(순환).
-- assetless_browser는 자기 신고 UA를 빼고 걸므로, 이 정답에서 그 근거의 양성은 robots.txt를 다른 세션에서 요청한 대상만 남는다
-- known_at = 적재 시각. 고정 라벨이라 "판정 뒤에 알게 됨"은 빌드 순서(근거 -> 정답)로만 맞춘다. raw_ref = 목록 적재일
WITH d AS (SELECT max(load_date) AS list_date FROM bot.silver.ref_ua_patterns WHERE list = 'crawler_ua'),
p AS (SELECT r.pattern FROM bot.silver.ref_ua_patterns r JOIN d ON r.load_date = d.list_date WHERE r.list = 'crawler_ua'),
a AS (
  SELECT actor_key, event_date, max(client_ua) AS client_ua, bool_or(action = 'robots') AS robots
  FROM bot.silver.events WHERE source = 'auth_lib' GROUP BY actor_key, event_date
),
crawler AS (
  SELECT DISTINCT u.client_ua FROM (SELECT DISTINCT client_ua FROM a WHERE client_ua IS NOT NULL) u JOIN p ON u.client_ua RLIKE p.pattern
)
SELECT
  'web_client'                                                        AS entity_type,
  a.actor_key                                                         AS entity_key,
  CAST(NULL AS STRING)                                                AS entity_name,
  'auth_library'                                                      AS truth_source,
  CASE WHEN c.client_ua IS NOT NULL OR a.robots THEN 'bot' ELSE 'human' END AS label,
  'crawler_ua_or_robots_txt'                                          AS label_method,
  CAST(NULL AS TIMESTAMP)                                             AS observed_at,
  current_timestamp()                                                 AS known_at,
  concat('raw/auth_lib/public_v2.json; crawler_ua ', CAST(d.list_date AS STRING)) AS raw_ref,
  a.event_date                                                        AS target_window
FROM a CROSS JOIN d LEFT JOIN crawler c ON c.client_ua = a.client_ua
