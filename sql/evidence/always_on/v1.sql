-- always_on v1 (행동): 사람은 잔다. 선언 봇이 아니면서 하루 20개 시간대 이상, 100건 이상 활동
SELECT entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       NOT endswith(entity_name, '[bot]') AND active_hours >= 20 AND n_events >= 100 AS hit,
       active_hours / 24 AS score,
       to_json(named_struct('n_events', n_events, 'active_hours', active_hours)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_account' AND event_date = DATE(:window_end) AND feature_version = 'f1'
