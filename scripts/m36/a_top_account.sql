-- M-36 (a), docs/review_logic.md R-4: 날짜마다 이벤트가 가장 많은 GitHub 계정 하나의 몫, 자기 신고 여부, 그 계정을 뺀 MDE.
-- 계정 이름은 찍지 않는다(대상별 라벨 비공개, direction_v5.md 3-1). MDE 식은 policy_compare.sql 70행과 같다.
-- 예측(handoff_v7.md 1-4): 상위 계정은 lenient에서 빠지고, 빼면 MDE가 lenient 값(7 ~ 11%) 근처로 떨어진다.
-- 실행: python -m scripts.query scripts/m36/a_top_account.sql
with a as (
  select event_date, actor_key, count(*) as n,
         not bool_and(is_real_lenient) as out_lenient, bool_or(actor_name like '%[bot]') as bot_suffix
  from real_user_events where source = 'gh' group by event_date, actor_key
),
r as (select *, row_number() over (partition by event_date order by n desc) as rk from a),
s as (
  select event_date, count(*) as accounts, sum(n) as events,
         max(n) filter (where rk = 1) as top1_events,
         bool_or(rk = 1 and out_lenient) as top1_out_lenient,
         bool_or(rk = 1 and bot_suffix) as top1_bot_suffix,
         avg(n) as mean_all, var_samp(n) as var_all,
         count(*) filter (where rk > 1) as accounts_wo, avg(n) filter (where rk > 1) as mean_wo, var_samp(n) filter (where rk > 1) as var_wo
  from r group by event_date
)
select event_date, accounts, top1_events, top1_events * 1.0 / events as top1_share_of_events,
       top1_out_lenient, top1_bot_suffix,
       2.8 * sqrt(2 * var_all / (accounts / 2.0)) / mean_all as rel_mde_none,
       2.8 * sqrt(2 * var_wo / (accounts_wo / 2.0)) / mean_wo as rel_mde_none_without_top1
from s order by event_date
