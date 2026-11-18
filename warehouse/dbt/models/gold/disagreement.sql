-- 근거끼리 엇갈린 저장소: 저활동 star가 많은데 burst와 lockstep 중 하나만 걸림 (4.3절). 사람이 확인할 표본의 후보
select entity_key as repo_key, event_date,
       case when burst_5m then 'burst_only' else 'lockstep_only' end as kind,
       judged_at
from {{ ref('evidence_flags') }}
where entity_type = 'gh_repo' and low_activity and (burst_5m <> lockstep_ratio)
