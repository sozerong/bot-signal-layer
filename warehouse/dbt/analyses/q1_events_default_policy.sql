-- depends_on: {{ ref('metric_policy') }}
-- ① 지표의 기본 정책으로 실유저만: 날짜별 이벤트 수. 정책을 고르지 않는다 (seeds/metric_policy.csv의 events 기본 정책)
select e.event_date, '{{ default_policy("events") }}' as policy, count(*) as events
from {{ ref('real_user_events') }} e
join {{ real_users(default_policy('events'), 'gh') }} u
  on u.entity_key = e.actor_key and u.event_date = e.event_date
group by e.event_date
order by e.event_date
