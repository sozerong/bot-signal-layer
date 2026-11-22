-- 가져가는 표면 (지시서 1-5 (3)): 원천 x 대상 x 날짜 x 정책 -> 실유저 여부, 빠지게 한 근거, AI 에이전트 여부, 규칙 묶음 id.
-- 판정 조합은 entity_verdict 한 곳에 있다. 이 뷰는 이름과 꼴만 정한다 (계약).
-- is_ai_agent: GH는 UA가 없어 알 수 없다(NULL). 웹은 ai_agent_ua 근거 (entity_verdict.is_ai_agent)
{{ config(materialized='view') }}
-- ponytail: 원천 둘이라 대상 종류로 원천을 가른다. 셋째 원천이 오면 대상 종류 -> 원천 시드로
select case when entity_type = 'web_client' then 'auth_lib' else 'gh' end as source, entity_type, entity_key, event_date, policy,
       category = 'human' as is_real,
       category,
       case when category <> 'human' then hit_evidence end as excluded_by,
       is_ai_agent,
       '{{ var("ruleset_id") }}' as ruleset_id
from {{ ref('entity_verdict') }}
