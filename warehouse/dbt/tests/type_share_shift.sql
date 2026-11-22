-- 데이터 품질 (경고): 이벤트 종류별 비율이 전날보다 크게 변했나 (GH 설계 D10). 원천 형식이 조용히 바뀌는 것을 잡는다
-- 기준: 비율 1% 이상인 종류가 전날 대비 절반 넘게 늘거나 줄면. 첫날은 전날이 없어 통과한다
{{ config(severity='warn') }}
with s as (
  select source, event_date, action, count(*) * 1.0 / sum(count(*)) over (partition by source, event_date) as share
  from {{ ref('stg_events') }}
  group by source, event_date, action
),
d as (
  select *, lag(share) over (partition by source, action order by event_date) as prev_share
  from s
)
select * from d
where prev_share >= 0.01 and abs(share - prev_share) > 0.5 * prev_share
