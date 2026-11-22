-- 최소 관측 (경고): 정책별 봇 비율 ÷ 직전 7일 중앙값이 0.5 이하이거나 2 이상 (handoff_v6.md Q12, direction_v6.md Q12)
-- 경계를 포함한다: 지시서는 "0.5 미만"이지만 시험은 "하루 행 절반 -> 경고"라 정확히 절반(비 0.5)도 잡아야 한다
-- 봇 비율 = 그날 이벤트를 낸 계정 중 그 정책에서 실유저가 아닌 계정의 몫. real_user_events에서 센다:
-- 판정이 없는 계정도 실유저가 아님으로 들어가므로(handoff_v7.md 1-1의 기본값 false) 판정이 빠진 날은 비율이 뛰어 여기 걸린다.
-- 직전 7일 창(1 ~ 7일 전)에 날이 없거나 중앙값이 0이면 "비교값 없음"이라 행을 내지 않는다
{{ config(severity='warn') }}
with a as (
  select source, event_date, actor_key,
         bool_or(is_real_lenient) as lenient, bool_or(is_real_balanced) as balanced, bool_or(is_real_strict) as strict
  from {{ ref('real_user_events') }}
  group by source, event_date, actor_key
),
s as (
  select source, event_date, 'lenient' as policy, avg(case when lenient then 0.0 else 1.0 end) as bot_share from a group by 1, 2
  union all
  select source, event_date, 'balanced', avg(case when balanced then 0.0 else 1.0 end) from a group by 1, 2
  union all
  select source, event_date, 'strict', avg(case when strict then 0.0 else 1.0 end) from a group by 1, 2
),
c as (
  select s.source, s.event_date, s.policy, s.bot_share, count(p.bot_share) as n_prev, median(p.bot_share) as prev_median
  from s
  left join s p on p.source = s.source and p.policy = s.policy and date_diff('day', p.event_date, s.event_date) between 1 and 7
  group by s.source, s.event_date, s.policy, s.bot_share
)
select source, event_date, policy, bot_share, n_prev, prev_median, bot_share / prev_median as ratio
from c
where n_prev > 0 and prev_median > 0 and (bot_share / prev_median <= 0.5 or bot_share / prev_median >= 2)
