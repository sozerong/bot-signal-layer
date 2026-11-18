-- 분석팀용: strict 정책(근거 하나라도 걸리면 뺌)의 실유저 이벤트 (4.3절)
{{ config(materialized='view') }}
select source, event_id, event_ts, event_date, actor_key, actor_name, action, target, target_name
from {{ ref('real_user_events') }}
where is_real_strict
