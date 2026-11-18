-- 이벤트 + 정책별 실유저 여부 (4.3절). 근거는 대상 전부에 행을 내므로 판정 없는 계정은 없어야 한다.
-- ponytail: 혹시 없으면 실유저로 둔다. 시험(policy_account_sum)이 계정 수를 따로 맞춘다
with v as (
  select entity_key, event_date,
         bool_or(policy = 'lenient'  and category = 'human') as real_lenient,
         bool_or(policy = 'balanced' and category = 'human') as real_balanced,
         bool_or(policy = 'strict'   and category = 'human') as real_strict
  from {{ ref('entity_verdict') }}
  where entity_type = 'gh_account'
  group by entity_key, event_date
)
select e.source, e.event_id, e.event_ts, e.event_date, e.actor_key, e.actor_name, e.action, e.target, e.target_name,
       coalesce(v.real_lenient, true)  as is_real_lenient,
       coalesce(v.real_balanced, true) as is_real_balanced,
       coalesce(v.real_strict, true)   as is_real_strict
from {{ ref('stg_events') }} e
left join v on v.entity_key = e.actor_key and v.event_date = e.event_date
