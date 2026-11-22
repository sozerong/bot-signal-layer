-- M-36 (b), docs/review_logic.md R-4: balanced에서는 실유저인데 strict에서만 빠진 계정이 몇이고, 그중 그날 이벤트 100건 이상이 몇인가,
-- 어떤 근거로 빠졌나. 결과 지표(계정당 이벤트)와 같은 값으로 자르는 몫을 잰다.
-- 실행: python -m scripts.query scripts/m36/b_strict_only.sql
with s as (
  select entity_key, event_date,
         bool_or(policy = 'balanced' and is_real) as real_balanced,
         bool_or(policy = 'strict' and not is_real) as out_strict,
         max(case when policy = 'strict' then excluded_by end) as by_strict
  from actor_policy_status where source = 'gh' and entity_type = 'gh_account' group by entity_key, event_date
),
n as (select actor_key, event_date, count(*) as n from real_user_events where source = 'gh' group by actor_key, event_date)
select s.event_date, count(*) as strict_only_accounts,
       count(*) filter (where n.n >= 100) as with_100_or_more_events,
       count(*) filter (where n.n >= 100) * 1.0 / count(*) as share_100_or_more,
       count(*) filter (where s.by_strict like '%always_on%') as by_always_on,
       count(*) filter (where s.by_strict like '%multi_repo_burst%') as by_multi_repo_burst,
       sum(n.n) as their_events
from s join n on n.actor_key = s.entity_key and n.event_date = s.event_date
where s.real_balanced and s.out_strict
group by s.event_date order by s.event_date
