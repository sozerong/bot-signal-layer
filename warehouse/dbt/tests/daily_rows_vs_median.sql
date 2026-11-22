-- 최소 관측 (경고): 날짜별 이벤트 행 수 ÷ 직전 7일 중앙값이 0.5 이하이거나 2 이상 (handoff_v6.md Q12, direction_v6.md Q12)
-- 경계를 포함한다: 지시서는 "0.5 미만"이지만 시험은 "하루 행 절반 -> 경고"라 정확히 절반(비 0.5)도 잡아야 한다
-- 원천마다 따로 본다. 직전 7일 창(1 ~ 7일 전)에 적재된 날이 하루도 없으면 "비교값 없음"이라 행을 내지 않는다.
-- 창 안의 날이 7일보다 적어도 있는 날만으로 중앙값을 낸다(n_prev 칸에 며칠로 냈는지 남는다). 결정 28 옆 Q12 기록 참고
-- 잡는 것: 파일 일부만 받은 날, 원천이 조용히 줄어든 날. events_match_manifest는 manifest와의 차이만 보므로 manifest부터 짧으면 못 잡는다
{{ config(severity='warn') }}
with d as (
  select source, event_date, count(*) as n
  from {{ ref('stg_events') }}
  group by source, event_date
),
c as (
  select d.source, d.event_date, d.n, count(p.n) as n_prev, median(p.n) as prev_median
  from d
  left join d p on p.source = d.source and date_diff('day', p.event_date, d.event_date) between 1 and 7
  group by d.source, d.event_date, d.n
)
select source, event_date, n, n_prev, prev_median, n / prev_median as ratio
from c
where n_prev > 0 and prev_median > 0 and (n / prev_median <= 0.5 or n / prev_median >= 2)
