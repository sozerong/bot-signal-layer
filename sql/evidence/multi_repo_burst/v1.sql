-- multi_repo_burst v1 (행동): 한 계정이 한 시간 안에 같은 행동을 서로 다른 저장소 100개 넘게에 했다.
-- M-6의 빈 곳: 세 계정이 15분 동안 저장소 1,606개에 릴리스를 냈는데 star와 24시간 활동만 보는 다섯 근거에 안 걸렸다.
-- 선언 봇도 그대로 판정한다 (걸러 내는 것은 정책 몫). 점수 = 그 시간 저장소 수 / 100
SELECT 'gh_account' AS entity_type, actor_key AS entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       max(n_repos) >= 100 AS hit,
       max(n_repos) / 100 AS score,
       to_json(named_struct('max_repos_1h', max(n_repos))) AS metrics
FROM (
  SELECT actor_key, hour(event_ts) AS h, action, count(DISTINCT target) AS n_repos
  FROM bot.silver.events
  WHERE source = 'gh' AND event_date = DATE(:window_end)
  GROUP BY actor_key, hour(event_ts), action
)
GROUP BY actor_key
