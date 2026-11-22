-- M-36 (c), docs/review_logic.md R-3: star 상위 100이 날마다 얼마나 그대로 남는가(자연 교체)와, balanced와 strict 상위 100이 같은 저장소인가.
-- metric_policy_proposal은 남은 "개수"만 비교한다. 순위 동점이면 100개를 넘을 수 있어 개수도 함께 찍는다.
-- 실행: python -m scripts.query scripts/m36/c_top100_overlap.sql
with t as (
  select event_date, repo_key, rank_all, rank_balanced,
         rank() over (partition by event_date order by stars_strict desc) as rank_strict
  from impact
),
top as (select event_date, repo_key, rank_all <= 100 as in_all, rank_balanced <= 100 as in_b, rank_strict <= 100 as in_s from t),
prev as (
  select x.event_date, count(*) as all_same_as_prev_day
  from top x join top y on y.event_date + 1 = x.event_date and y.repo_key = x.repo_key
  where x.in_all and y.in_all group by x.event_date
)
select top.event_date,
       count(*) filter (where in_all) as n_all, max(prev.all_same_as_prev_day) as all_same_as_prev_day,
       count(*) filter (where in_b) as n_balanced, count(*) filter (where in_s) as n_strict,
       count(*) filter (where in_b and in_s) as balanced_strict_same,
       count(*) filter (where in_all and in_b) as all_balanced_same
from top left join prev using (event_date)
group by top.event_date order by top.event_date
