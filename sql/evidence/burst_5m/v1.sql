-- burst_5m v1 (조직): 받은 star 중 가장 몰린 5분 구간의 비중.
-- 하루 100개 넘게 star하는 계정 무리는 lockstep(계정당 50개 상한)에서 빠지므로 따로 잡는다 (사전 실험 v3)
SELECT entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       max_5m_stars / stars >= 0.5 AS hit,
       max_5m_stars / stars AS score,
       to_json(named_struct('stars', stars, 'max_5m_stars', max_5m_stars)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_repo' AND event_date = DATE(:window_end) AND feature_version = 'f1'
