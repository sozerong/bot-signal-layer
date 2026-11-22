-- 보증: 대상 근거마다 그날 이벤트를 낸 대상 전부에 행이 있고, 지금 쓰는 근거는 날마다 행이 있다 (docs/failure_analysis.md F-18)
-- every_account_judged는 근거가 하나라도 돈 대상을 판정 있음으로 본다(evidence_flags가 근거들을 한 행으로 모은다).
-- 그래서 always_on 하나만 안 돈 날은 초록이고, strict가 빠진 근거를 false로 읽어 봇을 덜 뺀다. 이 시험이 그날을 잡는다.
-- 원천마다 그 원천 대상의 근거만 본다: GitHub 날짜는 gh_ 근거, 웹 날짜는 web_client 근거 (evidence_current.entity, 결정 26)
-- ponytail: 대상 근거 목록은 stg_evidence에 한 번이라도 나온 (근거, 대상 종류)다. 어느 날에도 안 돈 근거는 아래 둘째 부분이 잡는다
with acct as (
  select distinct source, case when source = 'gh' then 'gh_account' else 'web_client' end as entity_type, actor_key, event_date
  from {{ ref('stg_events') }}
),
ev as (select distinct evidence_id, entity_type from {{ ref('stg_evidence') }} where entity_type in ('gh_account', 'web_client')),
have as (select distinct evidence_id, entity_key, window_end from {{ ref('stg_evidence') }} where entity_type in ('gh_account', 'web_client')),
days as (select distinct source, event_date from acct)
select a.event_date, ev.evidence_id, count(*) as accounts_without_row
from acct a
join ev on ev.entity_type = a.entity_type
left join have h on h.evidence_id = ev.evidence_id and h.entity_key = a.actor_key and h.window_end = a.event_date
where h.entity_key is null
group by all
union all
select d.event_date, c.evidence_id, null
from days d
join {{ ref('evidence_current') }} c on (d.source = 'gh') = (c.entity like 'gh_%')
where not exists (select 1 from {{ ref('stg_evidence') }} e where e.evidence_id = c.evidence_id and e.window_end = d.event_date)
