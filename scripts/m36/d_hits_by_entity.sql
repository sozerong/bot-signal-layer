-- M-36 (d), docs/review_logic.md R-8: 근거마다 hit 수를 계정과 저장소로 나눈다. evaluation의 n_hit은 둘을 섞어 센다.
-- 실행: python -m scripts.query scripts/m36/d_hits_by_entity.sql
select event_date, entity_type,
       count(*) filter (where declared_app) as declared_app, count(*) filter (where always_on) as always_on,
       count(*) filter (where low_activity) as low_activity, count(*) filter (where lockstep_ratio) as lockstep_ratio,
       count(*) filter (where burst_5m) as burst_5m, count(*) filter (where multi_repo_burst) as multi_repo_burst
from evidence_flags group by event_date, entity_type order by event_date, entity_type
