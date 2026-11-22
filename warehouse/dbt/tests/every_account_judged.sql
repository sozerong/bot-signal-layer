-- 보증: 그날 이벤트를 낸 대상은 정책마다 판정이 하나 있다 (docs/review_logic.md R-6, handoff_v7.md 1-1)
-- 대상 = GitHub는 계정(gh_account), 웹(auth_lib)은 web_client (sources/registry.yml entity, 결정 26)
-- policy_account_sum은 entity_verdict와 evidence_flags를 비교해 식으로 늘 같다. 이 시험은 원본 이벤트에서 출발한다.
-- 행이 나오면 근거가 그날 일부 대상을 빠뜨린 것이다(근거가 안 돈 날, 규칙 버전을 바꾸고 rule_backfill을 빠뜨린 날 등)
with p as (select distinct policy from {{ ref('policy_family') }}),
acct as (
  select distinct source, case when source = 'gh' then 'gh_account' else 'web_client' end as entity_type, actor_key, event_date
  from {{ ref('stg_events') }}
)
select a.source, a.event_date, p.policy, count(*) as accounts_without_verdict
from acct a
cross join p
left join {{ ref('entity_verdict') }} v
  on v.entity_type = a.entity_type and v.entity_key = a.actor_key and v.event_date = a.event_date and v.policy = p.policy
where v.entity_key is null
group by all
