-- 저장소 x 날짜: 원래 star 수와 정책별 실유저 star 수, 인기 순위 변화 (4.3절)
with s as (
  select target as repo_key, max(target_name) as repo_name, event_date,
         count(*)                                 as stars_all,
         count(*) filter (where is_real_lenient)  as stars_lenient,
         count(*) filter (where is_real_balanced) as stars_balanced,
         count(*) filter (where is_real_strict)   as stars_strict
  from {{ ref('real_user_events') }}
  where action = 'WatchEvent'
  group by target, event_date
)
select repo_key, repo_name, event_date, stars_all, stars_lenient, stars_balanced, stars_strict,
       rank() over (partition by event_date order by stars_all desc)      as rank_all,
       rank() over (partition by event_date order by stars_balanced desc) as rank_balanced
from s
