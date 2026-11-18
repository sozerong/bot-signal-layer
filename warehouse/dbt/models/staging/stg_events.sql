-- Silver events 그대로. 웹 소스용 빈 컬럼은 뺀다
select source, event_id, event_ts, event_date, actor_key, actor_name, action, target, target_name
from {{ source('silver', 'events') }}
