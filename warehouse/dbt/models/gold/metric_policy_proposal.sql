-- 지표별 기본 정책 제안 (지시서 handoff_v5_wp1.md 1-5 (1), direction_v5.md 3.1). 사람이 보고 seeds/metric_policy.csv에 옮긴다.
-- 규칙 (D10 (나), 결정 24 덧붙임): 잣대는 채점된 정책 balanced다. lenient와 balanced의 차이(절댓값, 7일 중앙값)가
--       그 지표의 자연 변동보다 크면 balanced, 아니면 lenient. 자연 변동 = lenient 값의 날짜 사이 변화율 절댓값의 중앙값.
--       strict는 제안하지 않는다: strict가 더 빼는 근거(always_on, multi_repo_burst)는 정답이 없어 채점되지 않았다(review_logic.md R-2).
--       step_strict 칸은 "보고용 상한"으로만 남긴다. 실험 지표는 D10 (다)대로 배정 전 행동으로 거르는 정책을 따로 둔다(설계안, 구현 전).
--       중앙값인 이유: 평균은 주말 하루(이벤트 -19.6%)에 끌려간다.
-- 기본 규칙 묶음(가장 최근 공개)으로만 계산한다. detection 지표(봇 판정 수 자체)는 규칙 대상이 아니라 제안이 비어 있다.
-- 중앙값은 엔진마다 같게 직접 센다 (가운데 한두 값의 평균)
with pc as (
  select * from {{ ref('policy_compare') }}
  where source is not null and kind in ('count', 'mean', 'ratio')
    and ruleset_id = (select ruleset_id from {{ ref('ruleset_release') }} order by seq desc limit 1)
),
day_change as (  -- lenient 값의 날짜 사이 변화
  select source, metric, abs(value / nullif(lag(value) over (partition by source, metric order by event_date), 0) - 1) as x
  from pc where treatment = 'lenient'
),
wide as (
  select source, metric, event_date,
         max(case when treatment = 'lenient'  then value end) as l,
         max(case when treatment = 'balanced' then value end) as b,
         max(case when treatment = 'strict'   then value end) as s
  from pc group by source, metric, event_date
),
xs as (
  select source, metric, 'natural' as what, x from day_change where x is not null
  union all select source, metric, 'step_balanced', abs(b / nullif(l, 0) - 1) from wide
  union all select source, metric, 'step_strict',   abs(s / nullif(b, 0) - 1) from wide
),
ranked as (
  select source, metric, what, x,
         row_number() over (partition by source, metric, what order by x) as rn,
         count(*) over (partition by source, metric, what) as cnt
  from xs where x is not null
),
med as (
  select source, metric,
         avg(case when what = 'natural'       then x end) as natural_var,
         avg(case when what = 'step_balanced' then x end) as step_balanced,
         avg(case when what = 'step_strict'   then x end) as step_strict,
         max(case when what = 'step_balanced' then cnt end) as n_days
  from ranked
  where rn in (floor((cnt + 1) / 2.0), ceil((cnt + 1) / 2.0))
  group by source, metric
)
select source, metric, natural_var, step_balanced, step_strict, n_days,
       case when step_balanced > natural_var then 'balanced' else 'lenient' end as proposed_policy
from med
