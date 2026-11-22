-- assetless_browser v1 (행동, strict만): 자기 신고 UA가 아닌데, 그날 세션 하나라도 요청 20개 이상이면서
-- 정적 파일(/themes), AJAX, 표지(/Cover)를 하나도 안 받았고 robots.txt도 요청하지 않음 (사전 실험 L2 규칙, 02_detect.py, REPORT.md 3절 69세션)
-- 자기 신고 여부는 declared_ua와 같은 목록, 같은 판으로 여기서 다시 맞춘다 (근거끼리 서로의 결과를 읽지 않는다, 결정 9의 독립)
-- 세션은 그날 요청만으로 센다: 자정(UTC)을 넘는 세션은 날마다 나뉜다
-- UA가 없거나 '', '-'인 대상은 브라우저가 아니라서 걸지 않는다 (사전 실험도 세션에서 뺐다. 요청 없이 끊긴 408 줄이 여기 든다, F-23)
WITH e AS (
  SELECT actor_key, session_key, client_ua, action FROM bot.silver.events
  WHERE source = 'auth_lib' AND event_date = DATE(:window_end)
),
d AS (SELECT max(load_date) AS list_date FROM bot.silver.ref_ua_patterns),
p AS (
  SELECT r.pattern FROM bot.silver.ref_ua_patterns r JOIN d ON r.load_date = d.list_date
  WHERE r.list IN ('crawler_ua', 'ai_robots')
),
declared AS (
  SELECT DISTINCT u.client_ua
  FROM (SELECT DISTINCT client_ua FROM e WHERE client_ua IS NOT NULL) u
  JOIN p ON u.client_ua RLIKE p.pattern
),
s AS (
  SELECT actor_key, session_key, count(*) AS n,
         count_if(action IN ('static', 'ajax', 'cover')) AS assets, count_if(action = 'robots') AS robots
  FROM e GROUP BY actor_key, session_key
),
a AS (
  SELECT e1.actor_key, max(e1.client_ua) AS client_ua FROM e e1 GROUP BY e1.actor_key
),
c AS (
  SELECT actor_key, count_if(n >= 20 AND assets = 0 AND robots = 0) AS n_sess_hit, max(n) AS max_sess_requests
  FROM s GROUP BY actor_key
)
SELECT 'web_client' AS entity_type, a.actor_key AS entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       x.client_ua IS NULL AND c.n_sess_hit > 0 AND a.client_ua IS NOT NULL AND a.client_ua NOT IN ('', '-') AS hit,
       CAST(c.max_sess_requests AS DOUBLE) AS score,
       to_json(named_struct('n_sess_hit', c.n_sess_hit, 'max_sess_requests', c.max_sess_requests, 'declared_ua', x.client_ua IS NOT NULL)) AS metrics
FROM a JOIN c ON c.actor_key = a.actor_key LEFT JOIN declared x ON x.client_ua = a.client_ua
