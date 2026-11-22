-- bronze_auth(load_web.py가 만든 임시 뷰, 한 행 = 요청 하나) -> events (4.2절 계약 그대로, 새 컬럼 없음)
-- 세션: 같은 actor(ip, ua), 시각 순으로 직전 요청과 30분(1,800초) 넘게 떨어지면 새 세션 (REPORT.md 3절, 원 논문 정의)
-- action = 경로 종류 (experiments/p0_bot_detection/web/02_detect.py와 같은 접두어)
WITH r AS (
  SELECT id, to_timestamp(timestamp) AS ts, ip, useragent AS ua, resource, referrer, response,
         substr(sha2(concat_ws('|', ip, useragent), 256), 1, 16) AS actor_key, bronze_file
  FROM bronze_auth
),
g AS (
  SELECT *, CASE WHEN unix_timestamp(ts) - unix_timestamp(lag(ts) OVER (PARTITION BY actor_key ORDER BY ts, id)) <= 1800
                 THEN 0 ELSE 1 END AS new_sess
  FROM r
),
s AS (
  SELECT *, sum(new_sess) OVER (PARTITION BY actor_key ORDER BY ts, id ROWS UNBOUNDED PRECEDING) AS sess_no
  FROM g
)
SELECT
  'auth_lib'                          AS source,
  id                                  AS event_id,
  ts                                  AS event_ts,
  to_date(ts)                         AS event_date,
  actor_key,
  CAST(NULL AS STRING)                AS actor_name,
  concat(actor_key, '-', sess_no)     AS session_key,
  CASE WHEN resource IS NULL           THEN 'no_request'  -- 응답 408/400, 요청 줄 "-" (F-23)
       WHEN resource LIKE '/themes%'   THEN 'static'
       WHEN resource LIKE '/AJAX%'     THEN 'ajax'
       WHEN resource LIKE '/Cover%'    THEN 'cover'
       WHEN resource LIKE '/Record%'   THEN 'detail'
       WHEN resource LIKE '/Search%'   THEN 'search'
       WHEN resource = '/robots.txt'   THEN 'robots'
       ELSE 'other' END               AS action,
  resource                            AS target,
  CAST(NULL AS STRING)                AS target_name,
  ua                                  AS client_ua,
  ip                                  AS client_ip,
  referrer,
  response                            AS status,
  bronze_file,
  :run_id                             AS run_id
FROM s
