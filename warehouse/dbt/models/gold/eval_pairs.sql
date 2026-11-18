-- 판정 한 행 x 정답 한 행. evaluation이 모으기 전의 행 단위 표 (시험이 행마다 본다)
-- 1) 정답 출처마다 평가해도 되는 근거만: truth_independence의 allowed = true 행(계열 또는 evidence_id)에 맞는 것.
--    금지 목록(allowed = false)은 여기서 쓰지 않는다. 시험(independence_forbidden)이 따로 대조한다 (두 목록이 엇갈리면 실패)
-- 2) 판정 뒤에 알게 된 정답만 (known_at >= judged_at). 같은 대상의 정답이 여러 번이면 그중 가장 최근 것
with ev as (
  select evidence_id, rule_version, family, entity_type, entity_key, window_end, hit, judged_at
  from {{ ref('stg_evidence') }}
  union all
  select 'fake_star_repo', 'combo', 'coordination', entity_type, entity_key, event_date,
         low_activity and (lockstep_ratio or burst_5m), judged_at
  from {{ ref('evidence_flags') }}
  where entity_type = 'gh_repo'
),
allowed as (
  select distinct truth_source, target from {{ ref('truth_independence') }} where allowed
),
t as (
  select *, row_number() over (partition by truth_source, entity_type, entity_key order by known_at desc) as rn
  from {{ ref('stg_truth') }}
)
select ev.evidence_id, ev.rule_version, ev.family, ev.entity_type, ev.entity_key, ev.window_end, ev.hit, ev.judged_at,
       t.truth_source, t.label, t.known_at
from ev
join t on t.entity_type = ev.entity_type and t.entity_key = ev.entity_key and t.rn = 1 and t.known_at >= ev.judged_at
join allowed a on a.truth_source = t.truth_source and a.target in (ev.family, ev.evidence_id)
