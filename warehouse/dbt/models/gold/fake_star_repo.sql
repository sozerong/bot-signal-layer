-- 가짜 star 저장소: low_activity(저장소) AND (lockstep_ratio OR burst_5m). 사전 실험 v3와 같은 식 (4.3절)
select entity_key as repo_key, event_date, lockstep_ratio as lockstep_hit, burst_5m as burst_hit, judged_at
from {{ ref('evidence_flags') }}
where entity_type = 'gh_repo' and low_activity and (lockstep_ratio or burst_5m)
