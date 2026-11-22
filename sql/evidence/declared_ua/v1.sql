-- declared_ua v1 (자기 신고, 웹): 그날 UA가 크롤러 목록이나 AI 봇 목록 패턴에 걸리거나, 그날 robots.txt를 요청 (REPORT.md 3절 L1)
-- 고유 UA만 패턴과 맞춘 뒤 대상에 다시 붙인다 (요청마다 맞추는 것보다 검사가 크게 준다, handoff_v5_wp1.md 1-3)
-- 대상 = web_client(ip, ua 해시)라 대상 하나에 UA는 하나다. 목록 판 = ref_ua_patterns의 가장 최근 적재일 (metrics.list_date)
-- ponytail: 판을 SQL에 적지 않았다. 목록을 새로 적재하면 같은 SQL이 다른 판정을 낸다(sql_sha256은 같다). 판 고정이 필요해지면 v2에 날짜를 적는다
WITH e AS (
  SELECT actor_key, client_ua, action FROM bot.silver.events
  WHERE source = 'auth_lib' AND event_date = DATE(:window_end)
),
d AS (SELECT max(load_date) AS list_date FROM bot.silver.ref_ua_patterns),
p AS (
  SELECT r.pattern, r.list FROM bot.silver.ref_ua_patterns r JOIN d ON r.load_date = d.list_date
  WHERE r.list IN ('crawler_ua', 'ai_robots')
),
ua AS (
  SELECT u.client_ua, array_sort(collect_set(p.list)) AS lists
  FROM (SELECT DISTINCT client_ua FROM e WHERE client_ua IS NOT NULL) u
  JOIN p ON u.client_ua RLIKE p.pattern
  GROUP BY u.client_ua
),
a AS (
  SELECT actor_key, max(client_ua) AS client_ua, bool_or(action = 'robots') AS robots, count(*) AS n
  FROM e GROUP BY actor_key
)
SELECT 'web_client' AS entity_type, a.actor_key AS entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       ua.client_ua IS NOT NULL OR a.robots AS hit,
       CAST(NULL AS DOUBLE) AS score,
       to_json(named_struct('lists', ua.lists, 'robots', a.robots, 'n_requests', a.n, 'list_date', d.list_date)) AS metrics
FROM a CROSS JOIN d LEFT JOIN ua ON ua.client_ua = a.client_ua
