-- declared_app v1 (자기 신고): login이 [bot]으로 끝나는 GitHub App 계정
SELECT entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       endswith(entity_name, '[bot]') AS hit,
       CAST(NULL AS DOUBLE) AS score,
       to_json(named_struct('login', entity_name)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_account' AND event_date = DATE(:window_end) AND feature_version = 'f1'
