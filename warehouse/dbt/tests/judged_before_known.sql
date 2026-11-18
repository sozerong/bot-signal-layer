-- 보증: 판정 뒤에 알게 된 정답만 채점에 쓴다. 모든 행에서 judged_at <= known_at (4.3절)
select * from {{ ref('eval_pairs') }} where judged_at > known_at
