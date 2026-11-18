-- lockstep_ratio v2 (조직): 내 stargazer 중 다른 저장소와 1시간 안에 같이 움직인 저활동 계정의 비율
-- v1(절대 수 >= 10)은 트렌딩 저장소 오탐으로 버렸다 (사전 실험). 그래서 v2로 시작한다
-- 저장소 피처는 버전을 거르지 않는다. 파티션에 한 버전만 있고, 읽은 버전을 metrics에 남긴다 (decisions.md 결정 9)
SELECT entity_type, entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       max_co_star / stars >= 0.5 AS hit,
       max_co_star / stars AS score,
       to_json(named_struct('stars', stars, 'max_co_star', max_co_star, 'feature_version', feature_version)) AS metrics
FROM bot.silver.entity_features
WHERE entity_type = 'gh_repo' AND event_date = DATE(:window_end)
