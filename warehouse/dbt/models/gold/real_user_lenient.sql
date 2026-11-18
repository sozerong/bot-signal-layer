-- 분석팀용: lenient 정책(자기 신고만 뺌)의 실유저 이벤트 (4.3절)
{{ config(materialized='view') }}
select source, event_id, event_ts, event_date, actor_key, actor_name, action, target, target_name
from {{ ref('real_user_events') }}
where is_real_lenient
