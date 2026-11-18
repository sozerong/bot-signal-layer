-- lockstep_ratio v3 (조직): v2와 같은 비율, 문턱값만 0.5 -> 0.4 (implementation_design.md 8절 L8-3)
-- 피처 버전은 거르지 않는다. 저장소 피처 파티션에는 한 버전만 있고, 읽은 버전을 metrics에 남긴다 (decisions.md 결정 9)
SELECT entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       max_co_star / stars >= 0.4 AS hit,
       max_co_star / stars AS score,
       to_json(named_struct('stars', stars, 'max_co_star', max_co_star, 'feature_version', feature_version)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_repo' AND event_date = DATE(:window_end)
