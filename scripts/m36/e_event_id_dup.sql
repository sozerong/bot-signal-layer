-- M-36 (e): GH Archive event_id가 날짜 안에서 겹치는가. 예측 0. 0이 아니면 모든 이벤트 수가 부풀려진 것이다.
-- 실행: python -m scripts.query scripts/m36/e_event_id_dup.sql
select event_date, count(*) as events, count(*) - count(distinct event_id) as duplicate_event_ids
from real_user_events where source = 'gh' group by event_date order by event_date
