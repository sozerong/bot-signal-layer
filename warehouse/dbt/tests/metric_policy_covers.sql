-- 보증: policy_compare에 나오는 지표는 모두 seeds/metric_policy.csv에 기본 정책이 있다 (지시서 1-5 (1)).
-- 시드에 없는 지표를 만들면 빌드가 실패한다. 기본 정책은 정책 시드에 있는 이름이어야 한다
select distinct pc.metric, mp.default_policy
from {{ ref('policy_compare') }} pc
left join {{ ref('metric_policy') }} mp on mp.metric = pc.metric
where mp.metric is null
   or mp.default_policy not in (select distinct policy from {{ ref('policy_family') }})
