-- 근거 x 버전 x 정답 출처: 판정 수, 정답이 있는 수, 정밀도, 대조군 양성률 (4.3절)
-- 정답이 repo_deleted면 양성 = 삭제됨. 대조군 = 이 근거에 안 걸렸는데 정답이 있는 대상(생존 확인 대조군)
-- 재현율은 아직 없다: 생존 확인은 의심 대상과 표본 대조군만 한다
with n as (
  select evidence_id, rule_version, count(*) filter (where hit) as n_hit
  from (
    select evidence_id, rule_version, hit from {{ ref('stg_evidence') }}
    union all
    select 'fake_star_repo', 'combo', low_activity and (lockstep_ratio or burst_5m)
    from {{ ref('evidence_flags') }} where entity_type = 'gh_repo'
  ) x
  group by evidence_id, rule_version
),
p as (
  select evidence_id, rule_version, truth_source,
         count(*) filter (where hit)                                     as n_hit_with_truth,
         count(*) filter (where hit and label in ('deleted', 'bot'))     as n_hit_positive,
         count(*) filter (where not hit)                                 as n_control_with_truth,
         count(*) filter (where not hit and label in ('deleted', 'bot')) as n_control_positive
  from {{ ref('eval_pairs') }}
  group by evidence_id, rule_version, truth_source
)
select p.evidence_id, p.rule_version, p.truth_source, n.n_hit,
       p.n_hit_with_truth, p.n_hit_positive,
       round(p.n_hit_positive / nullif(p.n_hit_with_truth, 0), 4)         as precision,
       p.n_control_with_truth, p.n_control_positive,
       round(p.n_control_positive / nullif(p.n_control_with_truth, 0), 4) as control_positive_rate
from p join n on n.evidence_id = p.evidence_id and n.rule_version = p.rule_version
