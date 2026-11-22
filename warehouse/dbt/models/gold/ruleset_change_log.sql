-- 규칙 묶음 변경 기록 (결정 24): 기본 묶음(가장 최근 공개)과 policy_compare에 있는 다른 묶음을 지표 x 날짜 x 처리 방식마다 비교한다.
-- 새 묶음을 공개하기 전에 이 표로 무엇이 얼마나 바뀌는지 본다. 공개 뒤에는 새 묶음이 기본이 되어 옛 묶음이 비교 대상으로 남는다
{{ config(materialized='view') }}
with base as (select ruleset_id from {{ ref('ruleset_release') }} order by seq desc limit 1)
select o.source, o.event_date, o.metric, o.kind, o.treatment,
       o.ruleset_id as base_ruleset, n.ruleset_id as other_ruleset,
       o.value as base_value, n.value as other_value,
       n.value - o.value as diff,
       n.value / nullif(o.value, 0) - 1 as rel_change
from {{ ref('policy_compare') }} o
join {{ ref('policy_compare') }} n
  on n.source = o.source and n.event_date = o.event_date and n.metric = o.metric and n.treatment = o.treatment
where o.ruleset_id = (select ruleset_id from base) and n.ruleset_id <> o.ruleset_id
