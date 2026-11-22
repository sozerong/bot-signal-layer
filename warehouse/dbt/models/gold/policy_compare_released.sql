-- 분석가용 기본 뷰: 가장 최근 공개한 규칙 묶음(seeds/ruleset_release.csv의 seq 최대)의 policy_compare만 (결정 24).
-- 규칙을 바꿔 다시 만들어도, 변경 기록을 남기고 시드에 한 줄을 더하기 전까지 이 뷰의 숫자는 그대로다
{{ config(materialized='view') }}
select * from {{ ref('policy_compare') }}
where ruleset_id = (select ruleset_id from {{ ref('ruleset_release') }} order by seq desc limit 1)
