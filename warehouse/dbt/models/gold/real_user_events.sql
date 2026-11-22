-- 이벤트 + 정책별 실유저 여부 (4.3절). 근거는 대상 전부에 행을 내므로 판정 없는 계정은 없어야 한다.
-- 판정이 없으면 실유저가 아닌 것으로 둔다: real_users()와 같은 동작이고, 실패가 "거르지 않음과 같은 숫자"로 숨지 않고
-- 숫자 감소로 드러난다. 판정 빠짐 자체는 시험 every_account_judged가 잡는다 (docs/review_logic.md R-6)
with v as (
  select entity_key, event_date,
         bool_or(policy = 'lenient'  and category = 'human') as real_lenient,
         bool_or(policy = 'balanced' and category = 'human') as real_balanced,
         bool_or(policy = 'strict'   and category = 'human') as real_strict
  from {{ ref('entity_verdict') }}
  where entity_type in ('gh_account', 'web_client')  -- 원천마다 이벤트를 내는 대상 (sources/registry.yml entity)
  group by entity_key, event_date
)
select e.source, e.event_id, e.event_ts, e.event_date, e.actor_key, e.actor_name, e.action, e.target, e.target_name,
       coalesce(v.real_lenient, false)  as is_real_lenient,
       coalesce(v.real_balanced, false) as is_real_balanced,
       coalesce(v.real_strict, false)   as is_real_strict
from {{ ref('stg_events') }} e
left join v on v.entity_key = e.actor_key and v.event_date = e.event_date
