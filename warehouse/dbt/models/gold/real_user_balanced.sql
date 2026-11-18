-- 분석팀용: balanced 정책(자기 신고 + 인프라 + 조직)의 실유저 이벤트 (4.3절)
{{ config(materialized='view') }}
select source, event_id, event_ts, event_date, actor_key, actor_name, action, target, target_name
from {{ ref('real_user_events') }}
where is_real_balanced
