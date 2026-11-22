# 데이터 모델

이 문서는 표마다 한 행이 무엇인지, 키와 파티션, 누가 만들고 누가 쓰는지를 적은 지도다.
분석가가 어느 표를 쓸지 고를 때, 엔지니어가 표를 바꾸기 전에 읽는다.

Silver 표와 Gold 모델마다 한 행의 뜻, 키, 파티션, 쓰기 방식, 만드는 작업, 쓰는 쪽, 계약 여부를 적는다. 〔근거: 문서: handoff_v6.md Q9〕
표 목록은 코드와 같아야 한다: 2절 = `spark_jobs/create_tables.py`의 `TABLES`, 3절 = `warehouse/dbt/models/gold/`의 모델(`gold.yml`에 적힌 것과 SQL 파일 전부). `tests/test_data_model.py`가 대조한다. 〔근거: 코드: tests/test_data_model.py〕
표를 더하거나 빼면 이 문서의 2절이나 3절 표에 한 줄을 같이 고친다. 안 고치면 시험이 실패한다. 〔근거: 판단: 문서가 코드와 어긋나는 것을 시험이 잡게 한다(handoff_v6.md Q9). 반증 조건: 시험이 줄 하나를 지워도 초록이면 시험이 틀렸다〕

## 1. 층과 위치

| 층 | 저장 | 로컬 위치 | 누가 쓰나 | 근거 |
|---|---|---|---|---|
| Bronze | 원천 파일 그대로 + `_manifest.json` | `raw/dt=…/hour=…/`, `raw/ref/`, `raw/truth_raw/` | `ingest.py`, `spark_jobs/load_ref.py fetch`, `truth/repo_alive.py` | 〔근거: 코드: sources/registry.yml bronze.path, spark_jobs/load_ref.py 1 ~ 8행, truth/repo_alive.py 1행〕 |
| Silver | Iceberg 1.11.0, Hadoop 카탈로그 `bot` | `warehouse_local/silver/` (`BOT_WAREHOUSE`로 바꿈) | Spark 4.1.2 작업(`spark_jobs/`, `truth/build_truth.py`) | 〔근거: 코드: spark_jobs/session.py 27 ~ 32행, Dockerfile 2행〕 |
| Gold | DuckDB 파일, 스키마 `main` | `warehouse_local/gold.duckdb` | dbt-duckdb (`warehouse/dbt_build.py`) | 〔근거: 코드: warehouse/dbt/profiles.yml path, scripts/export_site.py "main 스키마"〕 |
| Gold 밖 메타 | 같은 DuckDB의 `lineage`, `ops` 스키마 | 같은 파일 | `lineage/extract.py`, `warehouse/record_build.py` | 〔근거: 코드: lineage/extract.py 133 ~ 135행, warehouse/record_build.py 32행〕 |

- Gold는 Silver를 복사하지 않고 `iceberg_scan`으로 최신 스냅샷을 읽는다. 〔근거: 코드: warehouse/dbt/models/sources.yml external_location〕
- Silver 쓰기는 SQL 파일이 SELECT만 내고 `run_sql.py`가 그 파티션을 덮어쓴다(`overwritePartitions`). 표의 모양은 `create_tables.py` 한 곳에서만 정한다. 〔근거: 코드: spark_jobs/run_sql.py 1 ~ 6행, 78행. spark_jobs/create_tables.py 3 ~ 7행〕
- 날짜는 UTC로 자른다. 〔근거: 코드: spark_jobs/session.py `spark.sql.session.timeZone` UTC〕

## 2. Silver 표

| 표 | 한 행의 뜻 | 키 (한 행을 정하는 컬럼) | 파티션 | 쓰기 방식 | 만드는 작업 | 쓰는 쪽 | 계약 | 근거 |
|---|---|---|---|---|---|---|---|---|
| `bot.silver.events` | 원천 이벤트(웹은 요청) 하나 | (source, event_id). 다만 GH Archive에 같은 id가 하루 1 ~ 12개 겹친다 | source, event_date | 날짜 파티션 덮어쓰기. GH는 파일 날짜와 다른 event_date가 있으면 쓰기 전에 실패. 웹(auth_lib)은 한 달치 파일 하나라 파일 안 모든 날짜를 한 번에 | GH `spark_jobs/load_events.py` + `sql/silver/events.sql` (bot_build `load_events`). 웹 `spark_jobs/load_web.py` + `sql/silver/events_auth_lib.sql` (손으로, 결정 26) | features_account, 웹 근거 SQL, Gold `stg_events` | 표 계약(create_tables.py) | 〔근거: 코드: create_tables.py TABLES, PARTITIONS. load_events.py reject_late. 측정: M-36 (e) event_id 중복〕 |
| `bot.silver.entity_features` | 대상(계정 또는 저장소) × 날짜 × 피처 버전의 피처 | (entity_type, entity_key, event_date, feature_version) | entity_type, event_date | 파티션 덮어쓰기. 저장소 피처는 한 파티션에 지금 버전 하나만 | `sql/silver/features_account.sql`, `features_repo.sql`(f1) 또는 `features_repo_f2.sql` (run_sql.py) | 근거 SQL, `sql/truth/pick_targets.sql` | 표 계약 | 〔근거: 코드: run_sql.py TARGETS, sql/evidence/burst_5m/v1.sql 3행 "파티션에 한 버전만", rules/registry.yml features_repo〕 |
| `bot.silver.evidence` | 대상 × 근거 × 규칙 버전 × 판정 구간의 판정 (hit, 점수, metrics) | (entity_type, entity_key, evidence_id, rule_version, window_end) | evidence_id, rule_version, window_end | 파티션 덮어쓰기. 새 규칙 버전은 새 파티션이라 옛 버전 행이 남는다 | `sql/evidence/<근거>/<버전>.sql` (run_sql.py가 judged_at, sql_sha256, run_id를 붙임) | Gold `stg_evidence`, pick_targets | 표 계약 | 〔근거: 코드: run_sql.py 4 ~ 6행, create_tables.py PARTITIONS. 결정 9〕 |
| `bot.silver.truth` | 대상 × 정답 출처 × 확인 한 번 | (truth_source, entity_key, target_window, known_at 날짜) | truth_source | 추가만 (MERGE, 이미 있으면 넣지 않음) | `truth/build_truth.py` + `sql/truth/truth.sql` (late_truth `build_truth`). 웹 고정 정답은 `build_truth auth_library` + `sql/truth/auth_library.sql` | Gold `stg_truth` | 표 계약 | 〔근거: 코드: create_tables.py 33 ~ 34행, truth/build_truth.py 3 ~ 5행. 결정 14〕 |
| `bot.silver.ref_ua_patterns` | 참조 목록의 UA 패턴 하나 (목록, 적재일마다) | (list, load_date, pattern) | list, load_date | 적재일 파티션 덮어쓰기. 다른 적재일은 남는다 | `spark_jobs/load_ref.py load` | 웹 근거 `declared_ua`, `assetless_browser`, `ai_agent_ua`(가장 최근 적재일), 정답 `sql/truth/auth_library.sql` | 표 계약 | 〔근거: 코드: load_ref.py 62 ~ 71행. 판단: 키는 목록 안 패턴이 겹치지 않는다는 가정. 확인 필요: `select list, load_date, pattern, count(*) … having count(*) > 1`〕 |
| `bot.silver.load_audit` | 적재 시도 하나 (원천 × 날짜 × run_id): 읽은 줄, 쓴 행, 버린 줄, 필수 필드 NULL 수, 늦은 행, 모르는 type, 상태(ok 또는 rejected) | 없음 (시도마다 추가) | source | 추가만. 문턱을 넘어 막힌 시도도 rejected로 남는다 | `spark_jobs/load_events.py` `record` (bot_build `load_events`, 웹은 `load_web.py`가 같은 함수로 event_date 없이 한 행) | 사람 (런북의 확인 질의) | 표 계약 | 〔근거: 코드: create_tables.py load_audit, load_events.py `audit`, `gate`, `record`. 결정 27〕 |

## 3. Gold 모델

쓰기 방식: dbt `table`은 빌드마다 통째로 다시 만든다. `view`는 저장하지 않고 읽을 때 계산한다. `incremental`은 아래에 적은 단위만 바꾼다. 기본값은 table이다. 〔근거: 코드: warehouse/dbt/dbt_project.yml `+materialized: table`〕
계약: dbt 모델 계약(`contract: enforced`)이 걸린 모델은 컬럼 이름이나 형이 바뀌면 빌드가 실패한다. 〔근거: 코드: warehouse/dbt/models/gold/gold.yml 1 ~ 3행〕

| 모델 | 한 행의 뜻 | 키 | 파티션 | 쓰기 방식 | 만드는 작업 | 쓰는 쪽 | 계약 | 근거 |
|---|---|---|---|---|---|---|---|---|
| `evidence_flags` | 대상 × 날짜, 근거별 hit를 한 행에 | (entity_type, entity_key, event_date) | 없음 | table | dbt_build | fake_star_repo, entity_verdict, disagreement, eval_pairs, evaluation | 없음. gold.yml에 항목이 없다 | 〔근거: 코드: evidence_flags.sql 1 ~ 3행. gold.yml models 목록에 없음(2026-11-22)〕 |
| `fake_star_repo` | 가짜 star 저장소 × 날짜 (low_activity AND (lockstep OR burst)) | (repo_key, event_date) | 없음 | table | dbt_build | entity_verdict | 없음 | 〔근거: 코드: fake_star_repo.sql 1 ~ 4행〕 |
| `entity_verdict` | 대상 × 날짜 × 정책의 범주(human, declared_bot, suspected_bot)와 걸린 근거 | (entity_type, entity_key, event_date, policy) | 없음 | table | dbt_build | real_user_events, actor_policy_status, why_excluded | 범주 값 시험(accepted_values) | 〔근거: 코드: entity_verdict.sql 1 ~ 4행, gold.yml entity_verdict〕 |
| `real_user_events` | 이벤트 하나 + 정책별 실유저 여부 3개. 판정이 없으면 실유저 아님 | stg_events 행과 같다 | 없음 | table | dbt_build | real_user_*, impact, policy_compare, 분석가 | 모델 계약 | 〔근거: 코드: real_user_events.sql 1 ~ 3행, 14 ~ 16행. gold.yml contract〕 |
| `real_user_lenient` | lenient(자기 신고 봇만 뺌)에서 실유저인 이벤트 | real_user_events와 같다 | 없음 | view | dbt_build | 분석가 | 모델 계약 | 〔근거: 코드: real_user_lenient.sql. seeds/policy_family.csv〕 |
| `real_user_balanced` | balanced(+ 가짜 star 저장소와 거기 star한 저활동 계정)에서 실유저인 이벤트 | real_user_events와 같다 | 없음 | view | dbt_build | 분석가 | 모델 계약 | 〔근거: 코드: real_user_balanced.sql, entity_verdict.sql 30 ~ 31행〕 |
| `real_user_strict` | strict(+ 행동 근거로 걸린 계정까지)에서 실유저인 이벤트 | real_user_events와 같다 | 없음 | view | dbt_build | 분석가 | 모델 계약 | 〔근거: 코드: entity_verdict.sql 29행. docs/review_logic.md R-13 "근거 하나라도"가 아님〕 |
| `impact` | 저장소 × 날짜의 star 수(전체, 정책별)와 순위 | (repo_key, event_date) | 없음 | table | dbt_build | policy_compare(top100), 화면, 분석가 | 모델 계약 | 〔근거: 코드: impact.sql 1행, gold.yml impact. scripts/export_site.py `main.impact`〕 |
| `disagreement` | burst와 lockstep 중 하나만 걸린 저활동 star 저장소 × 날짜 | (repo_key, event_date) | 없음 | table | dbt_build | 사람 확인 표본 후보 | 없음 | 〔근거: 코드: disagreement.sql 1 ~ 6행〕 |
| `eval_pairs` | 판정 한 행 × 정답 한 행 (허용 쌍, 판정 뒤 정답, 같은 구간) | (evidence_id, rule_version, entity_type, entity_key, window_end, truth_source) | 없음 | table | dbt_build (late_truth는 `stg_truth+`만) | evaluation, 시험 judged_before_known, independence_forbidden | 없음 | 〔근거: 코드: eval_pairs.sql 1 ~ 5행, dags/late_truth.py〕 |
| `evaluation` | 근거 × 버전 × 정답 출처 × 판정 구간의 정밀도와 대조군 양성률. window_end NULL은 구간 전체 | (evidence_id, rule_version, truth_source, window_end) | 없음 | table | dbt_build | 화면, 규칙 담당 | 없음 | 〔근거: 코드: evaluation.sql 1 ~ 4행. 결정 14〕 |
| `policy_compare` | 원천 × 날짜 × 지표 × 처리 방식 × 규칙 묶음의 지표 값과 변화율, MDE | (ruleset_id, source, event_date, metric, treatment) | 없음 | incremental (delete+insert, unique_key ruleset_id): 같은 묶음만 다시 쓰고 다른 묶음 행은 남는다 | dbt_build | policy_compare_released, ruleset_change_log, metric_policy_proposal, 화면 | 없음 | 〔근거: 코드: policy_compare.sql 1 ~ 6행. 결정 24〕 |
| `policy_compare_released` | 가장 최근 공개 묶음(ruleset_release seq 최대)의 policy_compare | (source, event_date, metric, treatment) | 없음 | view | dbt_build | 분석가 기본 뷰, 화면 | 없음 | 〔근거: 코드: policy_compare_released.sql 1 ~ 5행〕 |
| `ruleset_change_log` | 기본 묶음 대 다른 묶음, 지표 × 날짜 × 처리 방식마다 바뀐 폭 | (source, event_date, metric, treatment, other_ruleset) | 없음 | view | dbt_build | 규칙 담당(공개 전 확인), 분석가 세 쿼리 | 없음 | 〔근거: 코드: ruleset_change_log.sql 1 ~ 3행, models/exposures.yml〕 |
| `metric_policy_proposal` | 원천 × 지표의 기본 정책 제안(자연 변동 대비 규칙) | (source, metric) | 없음 | table | dbt_build | 사람이 보고 seeds/metric_policy.csv에 옮김 | 없음 | 〔근거: 코드: metric_policy_proposal.sql 1 ~ 6행. 결정 24〕 |
| `actor_policy_status` | 원천 × 대상 × 날짜 × 정책의 실유저 여부, 빠지게 한 근거, AI 에이전트 여부, 규칙 묶음 id | (source, entity_type, entity_key, event_date, policy) | 없음 | view | dbt_build | 매크로 real_users(), 화면, 분석가 세 쿼리 | 모델 계약 | 〔근거: 코드: actor_policy_status.sql 1 ~ 4행, macros/real_users.sql, gold.yml contract〕 |
| `why_excluded` | 정책에서 실유저가 아닌 대상의 hit 근거 하나 (버전, 점수, metrics, 판정 시각, SQL 지문) | (entity_type, entity_key, event_date, policy, evidence_id) | 없음 | view | dbt_build | 분석가 세 쿼리, 화면(로컬) | 모델 계약 | 〔근거: 코드: why_excluded.sql 1 ~ 3행, gold.yml contract. 판단: 범주를 바꾸지 않은 근거도 보인다(review_logic.md R-13)〕 |

## 4. 표 목록에 넣지 않은 것

| 이름 | 무엇 | 왜 2, 3절 밖인가 | 근거 |
|---|---|---|---|
| `stg_events`, `stg_evidence`, `stg_truth` | Silver를 Gold로 들이는 view. stg_evidence는 지금 규칙 버전만 남기고 계열을 붙인다 | 분석가가 쓰지 않는 들이는 층 | 〔근거: 코드: warehouse/dbt/models/staging/*.sql, dbt_project.yml staging view〕 |
| 시드 5개 (`evidence_current`, `policy_family`, `metric_policy`, `ruleset_release`, `truth_independence`) | 규칙 버전, 정책 계열, 지표별 기본 정책, 공개 묶음, 정답 독립성 | 데이터가 아니라 규칙이다. evidence_current는 빌드마다 rules/registry.yml에서 만든다 | 〔근거: 코드: warehouse/dbt/seeds/, warehouse/dbt_build.py 1 ~ 4행〕 |
| `lineage.run`, `lineage.edge` | 컬럼 리니지 회차와 간선 | dbt 밖, 리니지 추출기가 쓴다 | 〔근거: 코드: lineage/extract.py 133 ~ 141행〕 |
| `ops.build_record` | 빌드마다 한 줄 (git 커밋, 규칙 버전, SQL 해시, dbt 결과) | dbt 밖, 빌드 뒤 기록 | 〔근거: 코드: warehouse/record_build.py 1 ~ 6행〕 |

## 5. 로컬 한계

- DuckDB는 한 프로세스가 읽고 쓰거나 여러 프로세스가 읽기만 할 수 있어, 쓰기로 연 파일을 다른 프로세스가 열 수 없다. 그래서 Gold 빌드 중에는 화면 내보내기, 쿼리, 다른 빌드가 같은 `gold.duckdb`를 못 연다. 같은 Gold를 두 작업이 쓰지 않게 `BOT_WAREHOUSE`로 창고를 나눈다. 〔근거: 문서: [DuckDB 동시성](https://duckdb.org/docs/2.0/connect/concurrency). docs/runbook.md "DuckDB 잠금"〕
- Silver는 Hadoop 카탈로그라 두 작업이 같은 표에 동시에 커밋하면 한쪽이 조용히 사라질 수 있다. Airflow는 Silver 쓰기를 풀 한 자리(`silver_write`)로 줄 세운다. 〔근거: 코드: dags/jobs.py SILVER_WRITE. failure_analysis.md F-7〕
- dbt 컴파일 결과(`warehouse/dbt/target`)는 하나뿐이라 두 dbt를 함께 돌리면 서로 덮는다. 따로 돌릴 때는 `DBT_TARGET_PATH`, `DBT_LOG_PATH`를 나눈다. 〔근거: 판단: dbt는 target 폴더에 manifest를 쓰고 리니지 추출기가 그것을 읽는다(lineage/extract.py 5행). 반증 조건: 두 빌드를 동시에 돌려도 manifest가 섞이지 않으면 이 줄은 지운다〕
