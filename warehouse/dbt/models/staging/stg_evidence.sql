-- Silver evidence에서 지금 쓰는 규칙 버전만 남기고 계열을 붙인다 (evidence_current는 rules/registry.yml에서 만든다)
select e.entity_type, e.entity_key, e.evidence_id, e.rule_version, c.family,
       e.window_start, e.window_end, e.hit, e.score, e.metrics, e.judged_at, e.sql_sha256
from {{ source('silver', 'evidence') }} e
join {{ ref('evidence_current') }} c on e.evidence_id = c.evidence_id and e.rule_version = c.rule_version
