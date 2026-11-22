-- 원천 x 날짜 x 지표 x 처리 방식 (지시서 handoff_v5_wp1.md 1-4). 지표마다 정책이 숫자를 얼마나 바꾸나.
-- 처리 방식: none(거르지 않음), lenient, balanced, strict, cap_top1(거르지 않고 계정당 이벤트를 그날 상위 1% 값으로 자르기)
-- kind: count(셈) / mean(평균, n·분산·MDE를 같이) / ratio(분자·분모를 따로) / detection(봇 판정 수 자체, 기본 정책 규칙 대상 아님)
-- 규칙 묶음마다 행을 쌓는다: 같은 ruleset_id를 다시 만들면 그 묶음 행만 바꾸고, 다른 묶음의 행은 남는다 (결정 24)
-- 웹 원천(auth_lib, 결정 26): 세션 수, 상세 조회 수(count), 검색 후 상세 전환율(ratio, 분자·분모 따로). 처리 방식은 none과 세 정책.
--   ponytail: 웹에는 cap_top1이 없다(자를 "계정당 이벤트" 지표가 웹 지표에 없다). 필요하면 세션당 상세 조회 mean과 함께 더한다
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='ruleset_id') }}
with acct as (  -- 계정 x 날짜: 이벤트 수와 정책별 실유저 여부 (실유저 여부는 계정·날짜마다 하나)
  select event_date, actor_key, count(*) as n,
         bool_and(is_real_lenient) as l, bool_and(is_real_balanced) as b, bool_and(is_real_strict) as s
  from {{ ref('real_user_events') }}
  where source = 'gh'
  group by event_date, actor_key
),
ranked as (
  select event_date, n,
         row_number() over (partition by event_date order by n) as rn,
         count(*) over (partition by event_date) as cnt
  from acct
),
cap as (  -- 그날 계정당 이벤트의 99번째 백분위 (가장 가까운 순위 방식, 엔진마다 같은 값이 나오게 직접 센다)
  select event_date, max(n) as p99 from ranked where rn <= ceil(0.99 * cnt) group by event_date
),
t as (  -- 처리 방식마다 남는 계정과 그 계정의 이벤트 수
  select 'none' as treatment, event_date, n * 1.0 as n from acct
  union all select 'lenient',  event_date, n * 1.0 from acct where l
  union all select 'balanced', event_date, n * 1.0 from acct where b
  union all select 'strict',   event_date, n * 1.0 from acct where s
  union all select 'cap_top1', a.event_date, least(a.n, c.p99) * 1.0 from acct a join cap c on a.event_date = c.event_date
),
acct_metrics as (
  select event_date, treatment, 'active_accounts' as metric, 'count' as kind, count(*) * 1.0 as value,
         cast(null as bigint) as n, cast(null as double) as mean, cast(null as double) as variance
  from t group by event_date, treatment
  union all
  select event_date, treatment, 'events', 'count', sum(n), null, null, null from t group by event_date, treatment
  union all
  select event_date, treatment, 'events_per_account', 'mean', avg(n), count(*), avg(n), var_samp(n) from t group by event_date, treatment
),
r as (  -- 저장소 순위: 처리 방식마다 그날 star 수로 다시 매긴다 (impact와 같은 star 수)
  select event_date, rank_all,
         rank() over (partition by event_date order by stars_lenient desc)  as r_lenient,
         rank() over (partition by event_date order by stars_balanced desc) as r_balanced,
         rank() over (partition by event_date order by stars_strict desc)   as r_strict
  from {{ ref('impact') }}
),
top_kept as (  -- 원래 100위 안 저장소 중 처리 뒤에도 100위 안에 남는 수 (M-8: balanced에서 57개가 밖으로)
  select event_date, 'none' as treatment, count(*) * 1.0 as value from r where rank_all <= 100 group by event_date
  union all select event_date, 'lenient',  count(*) * 1.0 from r where rank_all <= 100 and r_lenient <= 100 group by event_date
  union all select event_date, 'balanced', count(*) * 1.0 from r where rank_all <= 100 and r_balanced <= 100 group by event_date
  union all select event_date, 'strict',   count(*) * 1.0 from r where rank_all <= 100 and r_strict <= 100 group by event_date
),
flagged as (  -- 봇으로 판정된 저장소 수 (fake_star_repo). 거르지 않음은 0
  select d.event_date, p.policy as treatment, count(v.entity_key) * 1.0 as value
  from (select distinct event_date from acct) d
  cross join (select distinct policy from {{ ref('policy_family') }}) p
  left join {{ ref('entity_verdict') }} v
    on v.event_date = d.event_date and v.policy = p.policy and v.entity_type = 'gh_repo' and v.category = 'suspected_bot'
  group by d.event_date, p.policy
  union all select distinct event_date, 'none', 0.0 from acct
),
m as (
  select * from acct_metrics
  union all select event_date, treatment, 'top100_kept', 'count', value, null, null, null from top_kept
  union all select event_date, treatment, 'repos_flagged', 'detection', value, null, null, null from flagged
),
w as (
  select m.*,
         max(case when treatment = 'none'    then value end) over (partition by event_date, metric) as v_none,
         max(case when treatment = 'lenient' then value end) over (partition by event_date, metric) as v_lenient,
         -- 반반 배정 상대 MDE: (1.96 + 0.84) x sqrt(2 x 분산 / (n/2)) / 평균 (alpha 0.05 양측, 검정력 80%)
         2.8 * sqrt(2 * variance / (n / 2.0)) / nullif(mean, 0) as rel_mde
  from m
)
, web as (  -- 웹 요청 + 세션. 실유저 여부는 real_user_events 한 곳에서 (대상 x 날짜)
  select r.event_date, e.session_key, r.action, r.is_real_lenient as l, r.is_real_balanced as b, r.is_real_strict as s
  from {{ ref('real_user_events') }} r
  join {{ ref('stg_events') }} e on e.source = r.source and e.event_id = r.event_id
  where r.source = 'auth_lib' and e.source = 'auth_lib'
),
ws as (  -- 세션 x 날짜. 자정(UTC)을 넘는 세션은 날마다 따로 센다. 대상 = (ip, ua)라 세션 안 실유저 여부는 하나다
  select event_date, session_key, bool_and(l) as l, bool_and(b) as b, bool_and(s) as s,
         count(*) filter (where action = 'detail') as n_detail,
         bool_or(action = 'search') as did_search,
         bool_or(action = 'search') and bool_or(action = 'detail') as did_convert
  from web group by event_date, session_key
),
wt as (
  select 'none' as treatment, event_date, n_detail, did_search, did_convert from ws
  union all select 'lenient',  event_date, n_detail, did_search, did_convert from ws where l
  union all select 'balanced', event_date, n_detail, did_search, did_convert from ws where b
  union all select 'strict',   event_date, n_detail, did_search, did_convert from ws where s
),
wm as (
  select event_date, treatment, 'sessions' as metric, 'count' as kind, count(*) * 1.0 as value,
         cast(null as double) as num_value, cast(null as double) as den_value
  from wt group by event_date, treatment
  union all
  select event_date, treatment, 'detail_views', 'count', sum(n_detail) * 1.0, null, null from wt group by event_date, treatment
  union all
  select event_date, treatment, 'search_to_detail', 'ratio',
         count(*) filter (where did_convert) * 1.0 / nullif(count(*) filter (where did_search), 0),
         count(*) filter (where did_convert) * 1.0, count(*) filter (where did_search) * 1.0
  from wt group by event_date, treatment
),
ww as (
  select wm.*,
         max(case when treatment = 'none'    then value end)     over (partition by event_date, metric) as v_none,
         max(case when treatment = 'lenient' then value end)     over (partition by event_date, metric) as v_lenient,
         max(case when treatment = 'none'    then num_value end) over (partition by event_date, metric) as num_none,
         max(case when treatment = 'none'    then den_value end) over (partition by event_date, metric) as den_none
  from wm
)
select 'gh' as source, event_date, metric, kind, treatment, value,
       value / nullif(v_none, 0) - 1    as change_vs_none,
       value / nullif(v_lenient, 0) - 1 as extra_vs_lenient,
       cast(null as double) as num_value, cast(null as double) as den_value,
       cast(null as double) as num_change_vs_none, cast(null as double) as den_change_vs_none,
       n, mean, variance, rel_mde,
       -- 같은 MDE를 얻는 데 드는 트래픽 배수 = (MDE 비)^2 (MDE는 1/sqrt(n)에 비례)
       power(rel_mde / nullif(max(case when treatment = 'none' then rel_mde end) over (partition by event_date, metric), 0), 2) as traffic_x_vs_none,
       '{{ var("ruleset_id") }}' as ruleset_id
from w
union all
select 'auth_lib', event_date, metric, kind, treatment, value,
       value / nullif(v_none, 0) - 1, value / nullif(v_lenient, 0) - 1,
       num_value, den_value, num_value / nullif(num_none, 0) - 1, den_value / nullif(den_none, 0) - 1,
       null, null, null, null, null,
       '{{ var("ruleset_id") }}'
from ww
