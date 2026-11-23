-- G4 (M-46): GH 7일, 강도별 지표 범위. 공개 묶음 기준 (policy_compare_released)
select metric, treatment, count(*) as n_days,
  round(100*min(change_vs_none),3) as chg_min_pct, round(100*max(change_vs_none),3) as chg_max_pct,
  round(100*min(rel_mde),2) as mde_min_pct, round(100*max(rel_mde),2) as mde_max_pct,
  min(value) as v_min, max(value) as v_max, any_value(ruleset_id) as rs
from policy_compare_released
where source='gh' and metric in ('active_accounts','top100_kept','events_per_account','events')
group by 1,2 order by 1, case treatment when 'none' then 0 when 'lenient' then 1 when 'balanced' then 2 when 'strict' then 3 else 4 end
