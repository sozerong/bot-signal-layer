-- 가져가는 표면 (지시서 1-5 (3)): 이 대상은 왜 빠졌나. 대상 x 날짜 x 정책 x 근거마다, 그 정책에서 실유저가 아닌 대상의 hit 근거와
-- 규칙 버전, 점수, metrics, 판정 시각, SQL 지문 (Silver evidence 그대로). 점수 + 규칙 id를 함께 주는 방식은 Cloudflare 봇 관리와 같다
{{ config(materialized='view') }}
select case when v.entity_type = 'web_client' then 'auth_lib' else 'gh' end as source, v.entity_type, v.entity_key, v.event_date, v.policy, v.category,
       e.evidence_id, e.rule_version, e.family, e.score, e.metrics, e.judged_at, e.sql_sha256,
       '{{ var("ruleset_id") }}' as ruleset_id
from {{ ref('entity_verdict') }} v
join {{ ref('policy_family') }} p on p.policy = v.policy
join {{ ref('stg_evidence') }} e
  on e.entity_type = v.entity_type and e.entity_key = v.entity_key and e.window_end = v.event_date
 and e.family = p.family and e.hit
where v.category <> 'human'
