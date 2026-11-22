-- Silver events 그대로. 웹 원천의 세션(session_key)은 policy_compare 웹 지표가 쓴다. 나머지 웹 컬럼(UA, IP 등)은 뺀다
select source, event_id, event_ts, event_date, actor_key, actor_name, action, target, target_name, session_key
from {{ source('silver', 'events') }}
