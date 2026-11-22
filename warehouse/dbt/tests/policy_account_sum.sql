-- 보증: 정책마다 대상 범주를 더하면 그날 대상 수와 같다 (범주가 겹치거나 빠지지 않는다). GitHub 계정과 웹 대상 따로
with acct as (
  select entity_type, event_date, count(*) as n from {{ ref('evidence_flags') }}
  where entity_type in ('gh_account', 'web_client') group by entity_type, event_date
),
v as (
  select entity_type, event_date, policy, count(*) as n, count(distinct entity_key) as n_distinct
  from {{ ref('entity_verdict') }} where entity_type in ('gh_account', 'web_client') group by entity_type, event_date, policy
)
select v.*, acct.n as accounts
from v join acct using (entity_type, event_date)
where v.n <> acct.n or v.n_distinct <> acct.n
