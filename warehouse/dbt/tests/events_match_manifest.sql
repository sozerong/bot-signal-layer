-- 데이터 품질: 그날 events 행 수 = 그날 manifest 줄 수 합 (GH 설계 D10). 오류 없이 줄이 빠지는 원천을 잡는다
-- 날짜는 manifest의 hour(UTC 'YYYY-MM-DDTHH')에서 자른다. events에 있는 날짜만 본다 (아직 적재하지 않은 날은 빼고)
with m as (
  select cast(left(hour, 10) as date) as event_date, sum(lines) as lines
  from {{ source('bronze', 'manifest') }}
  where status = 'ok'
  group by 1
),
e as (
  select event_date, count(*) as n from {{ ref('stg_events') }} group by event_date
)
select e.event_date, e.n, m.lines
from e left join m using (event_date)
where m.lines is null or e.n <> m.lines
