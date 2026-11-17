-- low_activity v1 (조직): 계정은 그날 star만 함, 저장소는 star 20개 이상 중 절반 이상이 그런 계정
SELECT entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       is_low_activity AS hit,
       CAST(NULL AS DOUBLE) AS score,
       to_json(named_struct('n_events', n_events, 'n_types', n_types)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_account' AND event_date = DATE(:window_end) AND feature_version = 'f1'
UNION ALL
SELECT entity_type, entity_key,
       DATE(:window_start), DATE(:window_end),
       stars >= 20 AND low_act_stars / stars >= 0.5,
       low_act_stars / stars,
       to_json(named_struct('stars', stars, 'low_act_stars', low_act_stars))
FROM bot.silver.entity_features
WHERE entity_type = 'gh_repo' AND event_date = DATE(:window_end) AND feature_version = 'f1'
