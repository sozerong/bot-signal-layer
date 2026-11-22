{#- 실유저 대상 목록 (지시서 1-5 (3)). source는 필수: 원천마다 대상의 뜻이 다르다 (GH 계정과 웹 클라이언트가 같은 사람이어도 따로 센다).
    include_ai_agent=false면 사용자 요청으로 가져오는 AI 에이전트도 뺀다. AI 에이전트를 사람 쪽에 셀지는 쓰는 쪽이 고른다 (criteria.md 4절)
    예: select count(*) from {{ ref('real_user_events') }} e join {{ real_users(default_policy('events'), 'gh') }} u
          on u.entity_key = e.actor_key and u.event_date = e.event_date -#}
{%- macro real_users(policy, source, include_ai_agent=false, entity_type='gh_account') -%}
  {%- if policy not in ('lenient', 'balanced', 'strict') -%}
    {{ exceptions.raise_compiler_error("real_users: 정책은 lenient, balanced, strict 중 하나다: " ~ policy) }}
  {%- endif -%}
  {%- if not source -%}
    {{ exceptions.raise_compiler_error("real_users: source가 없다") }}
  {%- endif -%}
  (select source, entity_type, entity_key, event_date, ruleset_id
   from {{ ref('actor_policy_status') }}
   where policy = '{{ policy }}' and source = '{{ source }}' and entity_type = '{{ entity_type }}' and is_real
   {%- if not include_ai_agent %} and not coalesce(is_ai_agent, false){% endif %})
{%- endmacro -%}

{#- 지표의 기본 정책 (seeds/metric_policy.csv). 컴파일 때 시드를 읽는다. 시드에 없는 지표면 컴파일이 실패한다.
    쓰는 파일 맨 위에 `-- depends_on: {{ ref('metric_policy') }}`가 있어야 한다 (dbt가 조건문 안의 ref를 미리 못 본다) -#}
{%- macro default_policy(metric) -%}
  {%- if execute -%}
    {%- set r = run_query("select default_policy from " ~ ref('metric_policy') ~ " where metric = '" ~ metric ~ "'") -%}
    {%- if r.rows | length != 1 -%}
      {{ exceptions.raise_compiler_error("default_policy: metric_policy에 지표가 없다: " ~ metric) }}
    {%- endif -%}
    {{- return(r.rows[0][0]) -}}
  {%- else -%}
    {{- return('lenient') -}}
  {%- endif -%}
{%- endmacro -%}
