-- bronze(load_events.py가 만든 임시 뷰) -> events. 한 행 = 이벤트 하나 (4.2절)
-- 날짜로 거르지 않는다: 그날 파일 24개의 줄을 모두 넣는다 (사전 실험 491만 행과 같은 기준).
-- 그날 파일에 다른 날짜 이벤트가 있으면 그 날짜 파티션을 덮어쓴다(F-16). load_events.py가 쓰기 전에 세어 막는다 (결정 22)
SELECT
  'gh'                               AS source,
  id                                 AS event_id,
  to_timestamp(created_at)           AS event_ts,
  to_date(to_timestamp(created_at))  AS event_date,
  CAST(actor.id AS STRING)           AS actor_key,
  actor.login                        AS actor_name,
  CAST(NULL AS STRING)               AS session_key,
  type                               AS action,
  CAST(repo.id AS STRING)            AS target,
  repo.name                          AS target_name,
  CAST(NULL AS STRING)               AS client_ua,
  CAST(NULL AS STRING)               AS client_ip,
  CAST(NULL AS STRING)               AS referrer,
  CAST(NULL AS STRING)               AS status,
  bronze_file,
  :run_id                            AS run_id
FROM bronze
