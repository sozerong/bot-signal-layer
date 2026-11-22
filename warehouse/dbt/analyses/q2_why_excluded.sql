-- ② 이 대상은 왜 빠졌나: 07-15 자기 신고가 아닌 계정 중 한 시간 안에 가장 많은 저장소에 같은 행동을 한 계정(multi_repo_burst 점수 최대).
--    07-15 실행 결과는 한 시간에 저장소 590개(max_repos_1h). M-6의 579개 릴리스 계정과는 다른 계정이다. 정책마다 걸린 근거와 규칙 버전, 점수
-- 계정 이름은 쓰지 않는다: 대상별 봇 라벨은 공개하지 않는다 (direction_v5.md 3-1). 분석가는 entity_key를 자기 대상으로 바꿔 쓴다
select w.policy, w.category, w.evidence_id, w.rule_version, w.score, w.metrics, w.judged_at, left(w.sql_sha256, 12) as sql_sha
from {{ ref('why_excluded') }} w
where w.entity_type = 'gh_account' and w.event_date = date '2024-07-15'
  and w.entity_key = (select entity_key from {{ ref('why_excluded') }}
                      where entity_type = 'gh_account' and event_date = date '2024-07-15' and evidence_id = 'multi_repo_burst'
                        and entity_key not in (select entity_key from {{ ref('why_excluded') }}
                                               where event_date = date '2024-07-15' and evidence_id = 'declared_app')
                      order by score desc limit 1)
order by w.policy, w.evidence_id
