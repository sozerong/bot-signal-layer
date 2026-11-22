-- ② 이 대상은 왜 빠졌나: 07-15 계정 계정 A (M-4에서 한 시간에 저장소 579개에 star). 정책마다 걸린 근거와 규칙 버전, 점수
select w.policy, w.category, w.evidence_id, w.rule_version, w.score, w.metrics, w.judged_at, left(w.sql_sha256, 12) as sql_sha
from {{ ref('why_excluded') }} w
where w.entity_type = 'gh_account' and w.event_date = date '2024-07-15'
  and w.entity_key = (select max(actor_key) from {{ ref('real_user_events') }}
                      where actor_name = '계정 A' and event_date = date '2024-07-15')
order by w.policy, w.evidence_id
