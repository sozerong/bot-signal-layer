-- 대상 x 날짜 x 정책 -> 범주와 걸린 근거 (4.3절). 조합 규칙은 여기에만 있다. 근거 계산과 따로 바뀐다
--   declared_bot : declared_app (자기 신고는 모든 정책에 있다)
--   suspected_bot: 계정은 (행동 계열이 든 정책에서 always_on 또는 multi_repo_burst) 또는 (조직 계열이 든 정책에서 그날 저활동이고 fake_star_repo에 star)
--                  저장소는 조직 계열이 든 정책에서 fake_star_repo
with pf as (
  select policy,
         bool_or(family = 'behavior')     as has_behavior,
         bool_or(family = 'coordination') as has_coordination
  from {{ ref('policy_family') }} group by policy
),
fake_acct as (
  select distinct ev.actor_key as entity_key, ev.event_date
  from {{ ref('stg_events') }} ev
  join {{ ref('fake_star_repo') }} f on ev.target = f.repo_key and ev.event_date = f.event_date
  join {{ ref('evidence_flags') }} a
    on a.entity_type = 'gh_account' and a.entity_key = ev.actor_key and a.event_date = ev.event_date and a.low_activity
  where ev.action = 'WatchEvent'
),
hits as (
  select e.entity_type, e.entity_key, e.window_end as event_date, p.policy,
         string_agg(distinct e.evidence_id, ',' order by e.evidence_id) as hit_evidence
  from {{ ref('stg_evidence') }} e join {{ ref('policy_family') }} p on e.family = p.family
  where e.hit
  group by e.entity_type, e.entity_key, e.window_end, p.policy
)
select f.entity_type, f.entity_key, f.event_date, pf.policy,
       case
         when f.declared_app then 'declared_bot'
         when f.entity_type = 'gh_account' and pf.has_behavior and (f.always_on or f.multi_repo_burst) then 'suspected_bot'
         when f.entity_type = 'gh_account' and pf.has_coordination and fa.entity_key is not null then 'suspected_bot'
         when f.entity_type = 'gh_repo' and pf.has_coordination and f.low_activity and (f.lockstep_ratio or f.burst_5m) then 'suspected_bot'
         else 'human'
       end as category,
       h.hit_evidence,
       f.judged_at
from {{ ref('evidence_flags') }} f
cross join pf
left join fake_acct fa on f.entity_type = 'gh_account' and fa.entity_key = f.entity_key and fa.event_date = f.event_date
left join hits h on h.entity_type = f.entity_type and h.entity_key = f.entity_key and h.event_date = f.event_date and h.policy = pf.policy
