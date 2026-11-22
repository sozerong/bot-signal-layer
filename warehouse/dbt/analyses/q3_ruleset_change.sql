-- ③ 규칙 묶음이 바뀌면 지표가 얼마나 바뀌나: 기본 묶음 대 다른 묶음, 07-15. 바뀐 것만
select metric, treatment, base_ruleset, other_ruleset, base_value, other_value, round(100 * rel_change, 3) as rel_change_pct
from {{ ref('ruleset_change_log') }}
where event_date = date '2024-07-15' and diff <> 0
order by metric, treatment
