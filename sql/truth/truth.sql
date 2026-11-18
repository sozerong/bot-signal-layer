-- truth_raw(build_truth.py가 만든 임시 뷰, repo_alive의 jsonl) -> truth. 한 행 = 대상 x 정답 출처 x 확인 (4.2절)
-- 확인 실패(200, 301, 404가 아닌 응답)는 정답이 아니라서 넣지 않는다.
-- 같은 날 같은 구간의 같은 저장소를 여러 번 확인했으면 그날 처음 확인한 것 하나만 (MERGE 키와 같은 단위)
-- target_window: 확인 결과 줄의 window_end (repo_alive가 고른 구간). 다른 구간을 위해 같은 저장소를 고르면 행이 따로 남는다 (결정 14)
SELECT
  'gh_repo'                                             AS entity_type,
  entity_key,
  entity_name,
  'repo_deleted'                                        AS truth_source,
  CASE WHEN http = 404 THEN 'deleted' ELSE 'alive' END  AS label,
  'http_head'                                           AS label_method,
  CAST(NULL AS TIMESTAMP)                               AS observed_at,  -- 언제 지워졌는지는 GitHub가 알려 주지 않는다
  known_at,
  raw_ref,
  CAST(window_end AS DATE)                              AS target_window
FROM (
  SELECT *, row_number() OVER (PARTITION BY entity_key, window_end, to_date(known_at) ORDER BY known_at, raw_ref) AS rn
  FROM (SELECT *, to_timestamp(checked_at) AS known_at, raw_file AS raw_ref FROM truth_raw WHERE http IN (200, 301, 404))
)
WHERE rn = 1
