-- 보증: 순환 쌍(truth_independence의 allowed = false)으로 평가한 행이 0건이다 (4.3절, criteria.md 3절)
-- eval_pairs는 허용 목록만 보고 고른다. 여기서는 금지 목록과 대조한다. 두 목록이 엇갈리면 이 시험이 실패한다
select p.truth_source, p.evidence_id, p.family, count(*) as n
from {{ ref('eval_pairs') }} p
join {{ ref('truth_independence') }} i
  on i.truth_source = p.truth_source and not i.allowed and i.target in (p.family, p.evidence_id)
group by p.truth_source, p.evidence_id, p.family
