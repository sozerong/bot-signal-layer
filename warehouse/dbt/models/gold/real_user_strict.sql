-- 분석팀용: strict 정책(balanced에 더해 행동 근거 always_on, multi_repo_burst로 걸린 계정까지 뺌. low_activity 하나로는 빼지 않음)의 실유저 이벤트 (4.3절)
{{ config(materialized='view') }}
select source, event_id, event_ts, event_date, actor_key, actor_name, action, target, target_name
from {{ ref('real_user_events') }}
where is_real_strict
