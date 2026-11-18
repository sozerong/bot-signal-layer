-- 근거 x 버전 x 정답 출처 x 판정 구간: 판정 수, 정답이 있는 수, 정밀도, 대조군 양성률 (4.3절)
-- 구간마다 한 행, 그리고 구간을 합친 전체 행(window_end가 NULL). 짝은 같은 구간끼리라 전체 행도 구간별 행의 합이다 (결정 14)
-- 정답이 repo_deleted면 양성 = 삭제됨. 대조군 = 이 근거에 안 걸렸는데 정답이 있는 대상(생존 확인 대조군)
-- 재현율은 아직 없다: 생존 확인은 의심 대상과 표본 대조군만 한다
with ev as (
  select evidence_id, rule_version, window_end, hit from {{ ref('stg_evidence') }}
  union all
  select 'fake_star_repo', 'combo', event_date, low_activity and (lockstep_ratio or burst_5m)
  from {{ ref('evidence_flags') }} where entity_type = 'gh_repo'
),
n as (
  select evidence_id, rule_version, window_end, count(*) filter (where hit) as n_hit
  from ev
  group by grouping sets ((evidence_id, rule_version, window_end), (evidence_id, rule_version))
),
p as (
  select evidence_id, rule_version, truth_source, window_end,
         count(*) filter (where hit)                                     as n_hit_with_truth,
         count(*) filter (where hit and label in ('deleted', 'bot'))     as n_hit_positive,
         count(*) filter (where not hit)                                 as n_control_with_truth,
         count(*) filter (where not hit and label in ('deleted', 'bot')) as n_control_positive
  from {{ ref('eval_pairs') }}
  group by grouping sets ((evidence_id, rule_version, truth_source, window_end), (evidence_id, rule_version, truth_source))
)
select p.evidence_id, p.rule_version, p.truth_source, p.window_end, n.n_hit,
       p.n_hit_with_truth, p.n_hit_positive,
       round(p.n_hit_positive / nullif(p.n_hit_with_truth, 0), 4)         as precision,
       p.n_control_with_truth, p.n_control_positive,
       round(p.n_control_positive / nullif(p.n_control_with_truth, 0), 4) as control_positive_rate
from p join n on n.evidence_id = p.evidence_id and n.rule_version = p.rule_version
             and n.window_end is not distinct from p.window_end
