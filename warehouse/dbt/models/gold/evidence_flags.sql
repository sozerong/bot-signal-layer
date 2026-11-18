-- 대상 x 날짜마다 근거별 hit를 한 행에. 조합 규칙(fake_star_repo, 정책)은 이 표만 본다
select entity_type, entity_key, window_end as event_date,
       bool_or(evidence_id = 'declared_app'   and hit) as declared_app,
       bool_or(evidence_id = 'always_on'      and hit) as always_on,
       bool_or(evidence_id = 'low_activity'   and hit) as low_activity,
       bool_or(evidence_id = 'lockstep_ratio' and hit) as lockstep_ratio,
       bool_or(evidence_id = 'burst_5m'       and hit) as burst_5m,
       bool_or(evidence_id = 'multi_repo_burst' and hit) as multi_repo_burst,
       max(judged_at) as judged_at
from {{ ref('stg_evidence') }}
group by entity_type, entity_key, window_end
