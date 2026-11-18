-- multi_repo_burst v2 (행동): v1과 같은 판정 식을 계정 피처(max_repos_per_hour)에서 읽는다.
-- v1은 events 491만 행을 근거 태스크에서 다시 집계해 4g 컨테이너에서 죽었다 (F-12). 무거운 집계는 피처에서 한 번 (결정 13)
-- hit, score, metrics가 v1과 같아야 한다 (M-17 지문). 점수 = 그 시간 저장소 수 / 100
SELECT 'gh_account' AS entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       max_repos_per_hour >= 100 AS hit,
       max_repos_per_hour / 100 AS score,
       to_json(named_struct('max_repos_1h', max_repos_per_hour)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_account' AND event_date = DATE(:window_end) AND feature_version = 'f1'
