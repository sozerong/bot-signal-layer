-- G1 #2: 날짜별 이벤트 1위 계정이 7일 내내 같은 계정인지 (이름은 찍지 않는다)
with a as (select event_date, actor_key, count(*) n from real_user_events where source='gh' group by 1,2),
r as (select *, row_number() over (partition by event_date order by n desc) rk from a),
t as (select event_date, actor_key from r where rk=1)
select count(distinct actor_key) as distinct_top1_accounts, count(*) as days,
       min(event_date) as d0, max(event_date) as d1 from t
