-- G3 (M-48): features_repo 자기 조인의 키 쏠림을 데이터로. Silver 읽기만 (DuckDB iceberg_scan, 날짜별).
-- la = features_repo.sql la와 같은 거르기: 그날 저활동, 선언 봇 아님, 계정당 저활동 star 2 ~ 50.
-- 조인 키는 actor_key(계정). 계정마다 C(n,2)가 조인 전 위 한계, 1시간 조건을 건 실제 출력도 계정마다 센다.
-- 저장소 쪽: star CTE(선언 봇 뺀 star 전부)의 저장소별 star 계정 수, 그리고 la 기준 상위 10개 저장소가 낀 쌍의 몫.
-- 셔플 칸 모의: hash(actor_key) % 200 칸마다 출력 행 (DuckDB 해시라 Spark Murmur3과 칸 배치는 다르다. 모양만 본다)
with acct as (
  select entity_key as actor_key, is_low_activity, event_date, ends_with(entity_name, '[bot]') as declared_bot
  from iceberg_scan('/work/warehouse_local/silver/entity_features')
  where entity_type = 'gh_account' and feature_version = 'f1'
),
star as (
  select e.event_date, e.actor_key, e.target, e.event_ts, a.is_low_activity as low_act
  from iceberg_scan('/work/warehouse_local/silver/events') e
  join acct a on e.actor_key = a.actor_key and e.event_date = a.event_date
  where e.source = 'gh' and e.action = 'WatchEvent' and not a.declared_bot
),
la as (
  select * from star s
  where low_act and (event_date, actor_key) in (
    select (event_date, actor_key) from star where low_act group by event_date, actor_key having count(*) between 2 and 50)
),
n_acct as (select event_date, actor_key, count(*) as n, count(*) * (count(*) - 1) / 2 as cn2 from la group by 1, 2),
j as (
  select a.event_date, a.actor_key, a.target as r1, b.target as r2
  from la a join la b on a.event_date = b.event_date and a.actor_key = b.actor_key and a.target < b.target
   and abs(epoch(a.event_ts) - epoch(b.event_ts)) <= 3600
),
j_acct as (select event_date, actor_key, count(*) as out_rows from j group by 1, 2),
acct_rank as (select *, row_number() over (partition by event_date order by out_rows desc) as rk from j_acct),
bucket as (select event_date, hash(actor_key) % 200 as b, sum(out_rows) as rows_ from j_acct group by 1, 2),
repo_la as (select event_date, target, count(distinct actor_key) as la_accts from la group by 1, 2),
repo_top as (select event_date, target from (select *, row_number() over (partition by event_date order by la_accts desc) as rk from repo_la) where rk <= 10),
repo_star as (select event_date, target, count(*) as stars from star group by 1, 2),
d as (
  select event_date,
    (select count(*) from star s where s.event_date = d0.event_date) as star_rows,
    (select count(*) from la l where l.event_date = d0.event_date) as la_rows
  from (select distinct event_date from n_acct) d0
)
select d.event_date, d.star_rows, d.la_rows,
  (select count(*) from n_acct n where n.event_date = d.event_date) as la_accounts,
  (select median(n) from n_acct n where n.event_date = d.event_date) as n_med,
  (select quantile_cont(n, 0.99) from n_acct n where n.event_date = d.event_date) as n_p99,
  (select max(n) from n_acct n where n.event_date = d.event_date) as n_max,
  (select sum(cn2) from n_acct n where n.event_date = d.event_date) as cn2_sum,
  (select sum(out_rows) from j_acct x where x.event_date = d.event_date) as join_rows,
  (select max(out_rows) from j_acct x where x.event_date = d.event_date) as join_max_acct,
  (select round(100.0 * sum(out_rows) filter (where rk <= 10) / sum(out_rows), 2) from acct_rank x where x.event_date = d.event_date) as top10_acct_pct,
  (select round(max(rows_) / median(rows_), 2) from bucket x where x.event_date = d.event_date) as bucket_max_over_med,
  (select median(stars) from repo_star r where r.event_date = d.event_date) as repo_stars_med,
  (select quantile_cont(stars, 0.99) from repo_star r where r.event_date = d.event_date) as repo_stars_p99,
  (select max(stars) from repo_star r where r.event_date = d.event_date) as repo_stars_max,
  (select round(100.0 * count(*) filter (where r1 in (select target from repo_top t where t.event_date = d.event_date)
                                         or r2 in (select target from repo_top t where t.event_date = d.event_date)) / count(*), 2)
   from j where j.event_date = d.event_date) as top10_repo_pair_pct
from d order by 1
