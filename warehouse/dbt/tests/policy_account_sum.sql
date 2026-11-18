-- 보증: 정책마다 계정 범주를 더하면 그날 계정 수와 같다 (범주가 겹치거나 빠지지 않는다)
with acct as (
  select event_date, count(*) as n from {{ ref('evidence_flags') }} where entity_type = 'gh_account' group by event_date
),
v as (
  select event_date, policy, count(*) as n, count(distinct entity_key) as n_distinct
  from {{ ref('entity_verdict') }} where entity_type = 'gh_account' group by event_date, policy
)
select v.*, acct.n as accounts
from v join acct using (event_date)
where v.n <> acct.n or v.n_distinct <> acct.n
