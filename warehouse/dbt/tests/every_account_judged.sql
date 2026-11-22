-- 보증: 그날 이벤트를 낸 GitHub 계정은 정책마다 판정이 하나 있다 (docs/review_logic.md R-6, handoff_v7.md 1-1)
-- policy_account_sum은 entity_verdict와 evidence_flags를 비교해 식으로 늘 같다. 이 시험은 원본 이벤트에서 출발한다.
-- 행이 나오면 근거가 그날 일부 계정을 빠뜨린 것이다(근거가 안 돈 날, 규칙 버전을 바꾸고 rule_backfill을 빠뜨린 날 등)
-- 웹 원천은 대상 종류가 달라 여기서 보지 않는다. 웹 원천을 붙일 때(WP1-b) 그 대상 종류로 같은 시험을 더한다
with p as (select distinct policy from {{ ref('policy_family') }}),
acct as (select distinct source, actor_key, event_date from {{ ref('stg_events') }} where source = 'gh')
select a.source, a.event_date, p.policy, count(*) as accounts_without_verdict
from acct a
cross join p
left join {{ ref('entity_verdict') }} v
  on v.entity_type = 'gh_account' and v.entity_key = a.actor_key and v.event_date = a.event_date and v.policy = p.policy
where v.entity_key is null
group by all
