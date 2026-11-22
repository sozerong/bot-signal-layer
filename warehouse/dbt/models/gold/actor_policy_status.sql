-- 가져가는 표면 (지시서 1-5 (3)): 원천 x 대상 x 날짜 x 정책 -> 실유저 여부, 빠지게 한 근거, AI 에이전트 여부, 규칙 묶음 id.
-- 판정 조합은 entity_verdict 한 곳에 있다. 이 뷰는 이름과 꼴만 정한다 (계약).
-- is_ai_agent: GH는 UA가 없어 알 수 없다(NULL). 웹 원천의 ai_agent_ua(1-3)가 붙으면 채운다
{{ config(materialized='view') }}
select 'gh' as source, entity_type, entity_key, event_date, policy,
       category = 'human' as is_real,
       category,
       case when category <> 'human' then hit_evidence end as excluded_by,
       cast(null as boolean) as is_ai_agent,
       '{{ var("ruleset_id") }}' as ruleset_id
from {{ ref('entity_verdict') }}
