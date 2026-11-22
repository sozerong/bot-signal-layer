# 결정 기록

이 문서는 설계에서 고른 것과 그 이유, 버린 대안, 잃는 것을 번호(결정 N)로 남긴 기록이다.
"왜 이렇게 만들었나"가 궁금할 때 읽는다. 지난 결정은 고쳐 쓰지 않고, 바뀌면 끝에 갱신 줄을 단다.

형식: 결정, 이유, 대안과 버린 이유, 다시 볼 조건.

## 결정 1. 실행 환경은 처음부터 GCP (2026-11-16)

- **결정:** 로컬 PC가 아니라 GCP에서 시작한다. BigQuery도 함께 쓴다.
- **이유:**
  - 단일 PC의 Spark는 진짜 분산이 아니라서, 셔플과 쏠림 문제를 분산 환경에서 겪었다고 말하기 어렵다.
  - GH Archive가 BigQuery 공개 데이터셋으로 있다.
- **대안:**
  - 로컬 Docker 후 이전: 이전 작업이 한 번 더 생긴다.
  - AWS: 서울 동네 찾기 이전 계획과 맞출 수 있지만, 크레딧이 GCP(300달러, 90일)보다 작다.
- **다시 볼 조건:** 무료 체험의 8 CPU 제한 때문에 진행이 막힐 때 (유료 전환 또는 클러스터 구성 조정).

## 결정 2. 데이터는 일주일치로 시작해 단계적으로 늘린다 (2026-11-16)

- **결정:** GH Archive 2024-07-15 ~ 2024-07-21 일주일치로 시작한다. 단계 계획 1~3과 "규칙 변경 재처리 1회"를 이 범위에서 먼저 끝낸다.
- **이유:**
  - 한 경로를 끝까지 돌린 결과가 있어야 지원 서류에 쓸 수 있다.
  - 사전 실험 날짜(2024-07-15)가 들어 있어서, v3 의심 저장소 276개 재현으로 파이프라인이 맞는지 바로 확인할 수 있다.
  - 이 주는 StarScout가 보고한 가짜 star 정점(2024년 7월) 안에 있다.
- **늘리는 순서 (안):** 1주 → 2주 → 1달 → 3달. 단계마다 같은 측정을 남긴다 (처리 시간, 비용, 셔플 크기, 태스크별 처리 시간 분포).
  - 병목은 미리 정하지 않는다. 데이터를 늘렸을 때 실제로 처음 터지는 지점을 `failure_analysis.md`에 원인까지 기록한다.
  - 하루치 사전 실험에서 걸어 둔 계정당 50개 상한이 첫 후보다.
- **다시 볼 조건:**
  - 일주일치에서 이미 비용이나 할당량 문제가 생길 때
  - 늘려도 병목이 안 보여 측정 의미가 없을 때

## 결정 3. L1에서 L2, L3 일부를 앞당긴다 [확정] (2026-11-17)

- **결정:** 지시서 L1이 L2, L3의 결과물에 기대는 두 곳을 앞당긴다.
  - 시험 표본: 2024-07-15 12시 파일 하나만 L1에서 먼저 받는다 (`ingest.py fetch 2024-07-15T12 2024-07-15T12`). L2에서 하루치를 받을 때 이 시간은 완료된 파티션이라 건너뛴다.
  - SQL: `load_events.py`, `sql/silver/*.sql`, `sql/evidence/*/*.sql`(L3-1)을 L1에서 쓴다. L3에는 하루치 실행과 276개 대조가 남는다.
- **이유:** L1의 목표(클라우드 없이 SQL을 시험)와 체크포인트(events → features → 근거 시험 통과)를 지키려면 표본과 SQL이 L1에 있어야 한다. 진짜 데이터 표본이라 L3 결과와 이어진다.
- **대안:** 손으로 지어낸 표본(실제 데이터의 이상한 값을 못 봄, L2 뒤 다시 만들어야 함). L1은 뼈대만 시험(체크포인트 문장과 다름).
- **다시 볼 조건:** 없음 (순서 조정이라 결과물은 지시서와 같다).

## 결정 4. Iceberg SQL 확장을 켜지 않는다 [결정] (2026-11-17)

- **결정:** `spark_jobs/session.py`에서 `spark.sql.extensions=IcebergSparkSessionExtensions`를 빼고, 파라미터는 Spark의 이름 붙은 파라미터(`:window_end`)로 넘긴다.
- **이유:** 확장을 켜면 Spark 4.1.2 + Iceberg 1.11.0에서 이름 붙은 파라미터가 묶이지 않는다 (failure_analysis.md F-1). 확장 없이 우리가 쓸 기능이 모두 도는 것을 bot-job 컨테이너에서 확인했다: 표 만들기, `overwritePartitions`, `.snapshots` 메타데이터 표, `MERGE INTO`(C3 truth), `CALL bot.system.expire_snapshots`.
- **대안:** 확장을 켜고 실행기가 파라미터를 문자열로 바꿔 넣기. SQL 주입과 따옴표 실수를 실행기가 떠안는다. 리니지 추출기가 보는 SQL과 실제로 돈 SQL이 달라진다.
- **잃는 것:** 확장에만 있는 Iceberg 전용 DDL (`ALTER TABLE ... WRITE ORDERED BY`, 브랜치·태그 DDL 등). 지금 계획에 없다.
- **다시 볼 조건:** 그런 DDL이 필요해질 때, 또는 Iceberg 새 버전에서 확장과 파라미터가 같이 돌 때. C2 클러스터 3.0 이미지 기본값에 확장이 켜져 있는지 `[확인 필요]`.

## 결정 5. 데이터 엔지니어 범위를 로컬에서 끝까지 완성한 뒤 GCP로 옮긴다 [확정] (2026-11-17)

- **결정:** 결정 1(처음부터 GCP)의 순서를 바꾼다. GCP와 BigQuery를 쓰는 것은 그대로이고, 로컬에서 L1~L9(일주일치, 정답 경로, Gold, Airflow, 리니지와 재처리)를 모두 끝낸 뒤 C1~C4로 옮긴다.
- **이유:**
  - 로컬에서 같은 SQL과 표 계약으로 끝까지 돌아가야 옮긴 뒤 "결과가 같다"를 증명할 수 있다.
  - 옮기면서 바뀐 파일과 줄 수가 "변하는 부분과 유지할 부분을 나눈 설계"의 증거가 된다.
  - 반복하는 동안 클라우드 비용이 들지 않는다.
- **잃는 것:** 결정 1의 첫 이유(노드 사이 네트워크 셔플을 겪는 것)는 이식 뒤(C2)로 밀린다. 로컬에서도 셔플과 쏠림은 태스크 시간으로 보인다.
- **범위 밖:** 분석가 쪽 확장과 웹 확장안은 데이터 엔지니어 쪽을 끝낸 뒤 다시 본다.
- **다시 볼 조건:** 일주일치가 로컬 메모리(24GB 상한)로 돌지 않을 때.

## 결정 6. 로컬 Gold는 dbt-duckdb로 만든다 [확정] (2026-11-17)

- **결정:** dbt-duckdb. 사용자가 추천안대로 확정했다 (2026-11-17, L3 시작 때). Silver Iceberg 표를 `iceberg_scan`으로 읽고 Gold는 `warehouse_local/gold.duckdb`에 쓴다. 이식 때 dbt-bigquery로 바꾼다.
- **이유:** 사전 실험과 서울 동네 찾기에서 DuckDB를 이미 썼다. 가볍고, 분석가가 로컬에서 바로 열어 볼 수 있다.
- **대안:**
  - dbt-spark(세션 방식): Silver와 같은 엔진과 카탈로그. 다만 세션 방식은 개발용이고 느리다. BigQuery로 옮길 때 방언을 바꾸는 비용은 DuckDB와 비슷하다.
  - PostgreSQL + dbt-postgres(서울 동네 찾기와 같음): Silver를 한 번 더 옮겨야 한다.
- **방언 위험 대비:** 모델마다 sqlglot으로 duckdb → bigquery 변환이 되는지 시험한다 (지시서 L5).
- **다시 볼 조건:** DuckDB가 Spark가 쓴 Iceberg 표를 읽지 못할 때 (지시서 11절).

## 결정 7. truth 표와 생존 확인 작업의 모양 [결정] (2026-11-17)

사용자가 "지시서 작업을 끝까지 하라"고 해서 Claude가 근거를 붙여 정했다. 바꾸려면 아래 "다시 볼 조건"을 본다.

- **결정:**
  1. `bot.silver.truth`는 `create_tables.py`가 만들고 `truth_source`로 파티션한다. 정답은 작고(하루 수백 행) 평가 때 출처별로 읽는다.
  2. `pick_targets`를 따로 된 작업으로 두지 않고 `sql/truth/pick_targets.sql`로 두고 `truth/repo_alive.py`가 읽는다. DAG에서는 `repo_alive` 태스크 하나가 대상 고르기와 확인을 한다.
  3. 대조군은 지시서대로 의심 저장소와 **같은 수**(사전 실험은 300개 고정), star 20개 이상 ~ 의심 저장소 최대 star 수(사전 실험 그대로), Spark `xxhash64` 순서다.
  4. 301(이름 바뀜)은 리다이렉트를 따라가지 않고 첫 응답 코드를 남긴다. 사전 실험은 따라가서 200으로 남겼다.
  5. 같은 날 다시 확인하면 `checked_date=` 폴더에 새 part 파일을 더한다(덮어쓰지 않음). truth에는 그날 처음 확인한 것 하나만 MERGE로 들어간다.
- **이유:**
  - 대상 목록을 파일로 넘기려면 Bronze도 Silver도 아닌 중간 파일이 생긴다. SQL 파일로 두면 리니지 추출기가 대상 고르기도 본다.
  - 301을 그대로 남기면 "이름이 바뀐 저장소"를 따로 셀 수 있다 (사후 확정 정답의 한 종류).
- **대안:** pick_targets를 Spark 작업으로 두고 결과를 Iceberg 표로(표가 하나 더 생김). 대조군 300개 고정(사전 실험과 같은 표본 크기, 다만 해시 함수가 달라 어차피 다른 저장소가 뽑힌다).
- **다시 볼 조건:** 생존 확인 대상이 수천 개로 늘어 Spark 세션을 띄운 채 HTTP를 기다리는 것이 아까울 때.

## 결정 8. Gold 모델의 세부 [결정] (2026-11-17)

- **결정:**
  1. 정책 조합 규칙은 `entity_verdict.sql` 하나에 둔다. 근거별 hit를 대상×날짜 한 행으로 편 `evidence_flags`를 두고, `fake_star_repo`, `entity_verdict`, `eval_pairs`, `disagreement`가 그것만 읽는다.
  2. evaluation에 조합 판정 `fake_star_repo`를 `rule_version = 'combo'`인 근거처럼 한 줄로 넣는다 (L5 체크포인트가 이 줄의 삭제율을 본다).
  3. 독립성은 두 목록으로 지킨다. 모델(`eval_pairs`)은 허용 목록(`allowed = true`)만 보고 고르고, 시험(`independence_forbidden`)은 금지 목록(`allowed = false`)과 대조한다. 한 목록만 쓰면 모델과 시험이 같은 것을 보게 되어 시험이 늘 통과한다.
  4. 같은 대상의 정답이 여러 번이면 가장 최근 것으로 채점한다. 판정 뒤(known_at >= judged_at)에 알게 된 것만 쓴다.
  5. `seeds/evidence_current.csv`는 커밋하지 않고 빌드 때마다 `rules/registry.yml`에서 만든다 (`warehouse/dbt_build.py`).
- **잃는 것:** 근거가 늘면 `evidence_flags`에 컬럼을 하나씩 더해야 한다 (세로로 긴 evidence 표의 장점이 Gold에서는 일부 줄어든다).
- **다시 볼 조건:** 근거가 10개를 넘을 때.

## 결정 9. 피처 버전과 규칙 재처리 범위 [결정] (2026-11-17)

L8을 하며 Claude가 근거를 붙여 정했다. 지시서에 없는 빈칸이었다.

- **빈칸:** entity_features는 `entity_type, event_date`로 파티션한다(4.2절). 피처 버전이 파티션에 없어서, f2를 쓰면 그날 저장소 파티션의 f1 행이 덮인다. 그런데 근거 SQL은 `feature_version = 'f1'`을 걸러 읽는다. f2를 쓴 뒤 근거를 다시 돌리면 0행이 되고, 실행기는 0행이면 아무 파티션도 바꾸지 않으므로 **옛 판정이 조용히 남는다.**
- **결정:**
  1. 피처 버전은 파일로 나눈다: `sql/silver/features_repo.sql`(f1)은 그대로 두고 `features_repo_f2.sql`(상한 200)을 더한다. 근거의 옛 버전 파일을 남기는 것과 같은 방식이다.
  2. 저장소 피처를 읽는 근거 SQL은 피처 버전을 거르지 않는다. 파티션에는 한 버전만 있다. 대신 읽은 버전을 `metrics`에 `feature_version`으로 남긴다. 지금 버전 파일(burst_5m v1, lockstep_ratio v2, low_activity v1의 저장소 부분)을 이렇게 고쳤다. 판정 식은 그대로라 버전을 올리지 않았다. SQL 해시는 바뀐다.
  3. 재처리 범위는 `lineage/impact.py`가 정한다: 컬럼 리니지로 넓게 잡고, Silver는 (가) 지금 버전 근거만, (나) 바뀐 파일이 쓰는 entity_type 파티션을 읽는 파일만, (다) 정답 경로 제외로 좁힌다. Gold는 dbt 모델 단위.
  4. 재처리 실험 뒤에는 지금 버전을 lockstep_ratio v2, 피처 f1로 되돌린다. 사전 실험과 같은 기준(276개)을 일주일 결과의 기준으로 남기기 위해서다. 근거 v3 행은 evidence에 남는다 (파티션에 rule_version이 있다).
- **잃는 것:**
  - 피처 f1과 f2를 한 날짜에 같이 둘 수 없다. 되돌리려면 f1을 다시 돌린다(또는 Iceberg 스냅샷).
  - `bot_build` DAG의 근거 목록(`EVIDENCE`)과 저장소 피처 파일은 `rules/registry.yml`과 따로 적혀 있다. registry만 바꾸고 DAG를 안 바꾸면 새 날짜는 옛 버전으로 판정되고 Gold는 새 버전을 찾아 빈다. 지금은 재처리 실험 뒤 되돌리므로 그대로 둔다. **(2026-11-17 지시서 H4-2에서 해소: DAG가 registry를 읽고, registry에 `features_repo: {current: f1}`를 더했다. measurements.md M-15)**
- **대안:** entity_features 파티션에 feature_version을 더한다 (표를 지우고 다시 만들어야 해서 사용자에게 물어야 한다. Iceberg 확장을 켜면 파티션 변경이 되지만 결정 4와 어긋난다). 근거 SQL이 피처 버전을 파라미터로 받는다 (버전 값을 둘 곳이 하나 더 생긴다).
- **다시 볼 조건:** 피처 버전 둘을 나란히 평가해야 할 때. 그때는 파티션에 feature_version을 넣는다.

## 결정 10. 병렬 근거 커밋 손실(F-7)의 처방: 쓴 뒤 확인 + Silver 쓰기 풀 [확정] (2026-11-17, 사용자 확정 2026-11-18)

지시서 H2-1의 사용자 결정 자리다. Claude가 추천안대로 먼저 진행했고, 2026-11-18 사용자가 "F-7은 A+B와 원인 확인 실험까지"로 확정했다.

- **근거 (원인 실험, `measure/commit_race.py`):** 프로세스 5개가 같은 표에 20번씩 append(100 커밋)를 3번 돌렸다.
  - Windows 폴더 붙임(지금 `warehouse_local/`): 조용한 손실 6, 10, 9개.
  - Docker 볼륨(리눅스 파일 시스템): 0, 0, 1개.
  - 폴더 붙임이 주원인이지만 볼륨에서도 0은 아니다. Hadoop 카탈로그가 "파일이 있나 보고 이름 바꾸기"를 프로세스 사이에서 원자적으로 하지 못한다는 지시서의 우려가 맞았다. 그래서 D(볼륨으로 옮기기)만으로는 안 된다.
  - 커밋 직후 표를 다시 읽어 자기 스냅샷을 찾는 확인은 조용한 손실 26개를 모두 잡았다.
- **결정:**
  - A. `run_sql.run`이 쓸 때 스냅샷 속성 `bot.write-id`를 남기고, 쓴 뒤 `REFRESH TABLE`을 하고 그 id의 스냅샷을 찾는다. 없으면 실패로 끝난다. 근거 태스크는 `retries=1`이다.
  - B. Silver에 쓰는 태스크(create_tables, load_events, 피처, 근거, build_truth, rerun_silver)를 Airflow 풀 `silver_write`(자리 1개)로 줄 세운다. 풀은 `airflow-init`이 만든다.
- **잃는 것:** 근거 다섯 개가 하나씩 돈다. 하루치 근거 구간이 길어진다 (M-13). 풀이 표마다가 아니라 Silver 전체라서 truth(다른 표) 쓰기도 함께 기다린다.
- **대안:** C(JDBC 카탈로그)는 dbt 소스를 바꿔야 한다. D(볼륨)는 손실이 0이 아니다.
- **다시 볼 조건:** C2에서 REST 카탈로그로 옮기면 서버가 충돌을 거절하므로 B를 푼다. A는 남긴다.

## 결정 11. 같은 규칙으로 다시 낸 판정은 옛 판정 시각을 이어받는다 (F-8) [확정] (2026-11-17, 사용자 확정 2026-11-18)

지시서 H4-1의 사용자 결정 자리다. Claude가 추천안(B)대로 먼저 진행했고, 2026-11-18 사용자가 "F-8은 B안"으로 확정했다.

- **문제:** 판정 시각(judged_at)이 실행 시각이라, 재시도나 백필로 같은 규칙을 다시 돌리기만 해도 그날 채점이 사라진다 (판정 뒤에 알게 된 정답만 쓰므로).
- **결정:** `run_sql.run`이 근거를 쓸 때 같은 파티션(evidence_id, rule_version, window_end)의 옛 행과 (entity_type, entity_key)로 맞춰, `sql_sha256`과 `hit`가 같으면 옛 `judged_at`을 쓴다. 새 대상, 바뀐 hit, 바뀐 SQL(해시)은 지금 시각이다. 새 규칙 버전은 파티션이 달라 늘 새 판정이다.
- **잃는 것:** 실행기가 쓰기 전에 옛 파티션을 한 번 읽는다 (근거 파티션은 하루 최대 66만 행). SQL의 주석만 바꿔도 해시가 달라져 새 판정이 된다. score나 metrics만 달라지고 hit가 같으면 옛 시각을 이어받는다(채점은 hit만 쓴다). 결정 9로 지금 버전 근거 세 개의 해시가 이미 바뀌어, 이 결정 전에 만든 판정 시각은 되살아나지 않는다.
- **대안:** A(그대로, 가장 엄격), C(판정 시각 = 구간 끝, 독립성의 시간 조건이 약해진다).

## 결정 12. 새 근거 multi_repo_burst v1을 더해 strict에 넣는다 [확정] (2026-11-17, 사용자 확정 2026-11-18)

지시서 H5(선택)의 사용자 결정 자리다. 목적은 "근거 하나를 더하는 비용 재기"다. Claude가 먼저 진행했고, 2026-11-18 사용자가 "H5도 해"로 확정했다.

- **결정:** `multi_repo_burst` v1(행동 계열, 계정)을 registry에 두고, Gold 계정 판정에서 행동 계열을 쓰는 정책(strict)이 always_on과 함께 쓴다. 문턱은 한 시간 100개 저장소다 (M-6의 세 계정이 15분에 576 ~ 579개, 사람 손으로 한 시간 100개는 드물다고 보았다. 근거는 그것뿐이다 [확인 필요]).
- **잃는 것:** 정답으로 채점하지 않은 근거가 strict 숫자를 바꾼다 (의심 계정 +219, 실유저 이벤트 -104,485, M-15). always_on도 같은 처지다.
- **되돌리는 법:** registry에서 한 줄을 지우고 Gold만 다시 만든다 (Silver 행은 자기 파티션에 남는다). evidence_flags와 entity_verdict의 컬럼은 그대로 두어도 false라 해가 없다.
- **다시 볼 조건:** account_deleted 정답이 생기면 채점하고 문턱을 다시 정한다.

## 결정 13. 힙은 컨테이너 상한에서 계산하고, 무거운 집계는 피처에서 한 번 (F-12) [확정] (2026-11-18, 사용자 선택)

지시서 J1의 사용자 결정 자리. 사용자가 "J1 처방은 A+B"로 골랐다.

- **결정 A:** 계산 태스크의 JVM 힙은 `dags/jobs.py heap(mem)`이 컨테이너 상한에서 정한다: 상한 − max(상한의 25%, 1GB). 힙을 따로 적지 않는다. 손 실행용 `docker-compose.yml` bot-job도 같은 규칙으로 24g / 18g. 상한은 2g 이상이어야 한다 (assert).
- **결정 B:** `multi_repo_burst`는 v2부터 events가 아니라 계정 피처 `max_repos_per_hour`를 읽는다. 기존 컬럼 값이 그대로라(M-17 지문) 피처 버전은 f1 그대로 두고 컬럼만 더한다 (결정 9와 같은 판단). 표 계약에 컬럼이 늘면 `create_tables`가 `ALTER TABLE ... ADD COLUMNS`로 끝에 붙인다. Iceberg SQL 확장 없이 된다 (결정 4의 [확인 필요] 풀림, 2026-11-18).
- **잃는 것:** 바깥 몫 25%만큼 힙이 줄었다 (예: 4g → 3g). 계정 피처 작업이 조금 무거워졌다 (2분 42초, 6.09GiB / 12g).
- **되돌리는 법:** registry에서 multi_repo_burst를 v1로 (v1 파티션은 남아 있다, Gold만 다시). 힙은 `heap()`의 비율 한 곳.
- **다시 볼 조건:** 클라우드(C4)에서 `job()`이 클러스터 연산자로 바뀔 때 힙은 클러스터 설정이 정한다. 로컬 25%는 상한 대비 최대 55%(M-17)라 줄일 여지가 있지만 근거가 생기면 줄인다.
- 갱신(2026-11-22, handoff_v7.md 단계 6): 6.09GiB는 계정 피처에 필요한 메모리가 아니었다. 컨테이너 상한을 12g에서 2g까지 낮춰도 모두 성공했고 spill은 2g에서만 생겼다. 계정 피처 상한은 4g로 내렸다. 〔근거: 측정: M-37. 결정 31〕

## 결정 14. 채점은 정답을 고른 판정 구간의 판정과만 짝짓는다 [확정] (2026-11-18, 사용자 선택)

지시서 J2의 사용자 결정 자리. 사용자가 "J2는 A"로 골랐다.

- **결정:** truth에 `target_window`(이 대상을 어느 판정 구간을 위해 골랐는지)를 두고, `eval_pairs`는 (대상, 구간)으로 짝짓는다. `evaluation`은 구간마다 한 행과 구간을 합친 전체 행(`window_end` NULL)을 낸다.
- **지시서와 다른 점 하나:** 지시서는 "repo_alive가 target_window를 적고, 예전 truth_raw에는 값이 없으니 pick_targets 구간으로 채운다"고 했다. 실제로는 repo_alive가 L4부터 확인 줄마다 `window_end`를 적고 있었다 (옛 파일 4개 2,216줄 모두 2024-07-15). 그래서 repo_alive와 raw는 고치지 않고, `truth.sql`이 그 값을 `target_window`로 옮긴다. 컬럼을 더하기 전에 넣은 truth 1,108행은 `build_truth`의 MERGE가 같은 확인 줄에서 한 번 채운다 (`WHEN MATCHED AND target_window IS NULL`).
- **MERGE 키:** (truth_source, entity_key, target_window, known_at 날짜). 같은 날 같은 저장소를 두 구간을 위해 확인하면 두 행이 남는다.
- **잃는 것:** 07-15를 위해 확인한 저장소가 다른 날 판정되어도 그 판정은 채점되지 않는다 (전에는 됐다). 구간마다 확인을 따로 해야 숫자가 생긴다.
- **되돌리는 법:** eval_pairs의 구간 조건 한 줄과 evaluation의 grouping sets. truth 컬럼은 남겨도 해가 없다.
- **같이 드러난 것:** F-13 (대상 고르기가 규칙 버전을 거르지 않음). 이 결정과 별개로 사용자 결정이 필요하다.

## 결정 15. registry를 리니지 원천으로, Gold는 meta 근거 계열로 좁힌다 [확정] (2026-11-18, 사용자 선택)

지시서 J3. (1)은 필수, (2)는 사용자가 "J3 (2)는 한다"로 골랐다.

- **결정 (1):** `lineage/extract.py`가 `rules.registry` → 시드 `evidence_current` 간선 세 개(evidence_id, rule_version, family)를 더한다. `lineage/impact.py`는 근거 파일을 바꿨거나 `--change rules/registry.yml`이면 registry에서도 출발한다. registry만 바꿨으면 바뀐 항목은 마지막으로 만든 시드 csv와 비교해 찾는다. `dbt_build.py`의 `evidence_current+` 늘-더하기는 지시서대로 안전장치로 남겼다.
- **결정 (2):** Gold 모델의 dbt `config.meta.evidence_families`에 그 모델 결과를 바꿀 수 있는 근거 계열을 적는다. impact.py가 바뀐 근거의 계열과 겹치지 않는 모델을 뺀다. `tests/test_gold_meta.py`가 meta를 SQL과 정책 시드에 대조한다.
- **지시서와 다른 점 하나:** 지시서는 "모델 13개에 meta 한 줄씩"이라 했다. meta는 좁힐 수 있는 5개(fake_star_repo, disagreement, real_user_lenient, real_user_balanced, real_user_strict)에만 적고, 적지 않은 모델은 "모든 계열"로 본다. 잊으면 빠지는 쪽이 아니라 다시 도는 쪽이 되게.
- **잃는 것:** 사람이 적는 값이 하나 늘었다. 정책 시드를 바꾸면 meta도 바꿔야 한다 (시험이 잡는다).
- **확인한 한계 (M-19):** 보고서는 좁혀지지만, `dbt_build.py`가 `evidence_current+`를 늘 더해서 rule_backfill의 실제 Gold 빌드는 좁혀지지 않는다. 좁혀도 줄어드는 시간은 약 0.7초(3%)다. 늘-더하기를 "시드만 더하기"로 바꿀지는 새 사용자 결정이다 (아래 그림 문장도 이것에 따라 달라진다).
- **그림 `docs/img/lineage.png` B 패널 (J3-3), 확정 (2026-11-18, 지시서 1절 D3 = A):** 늘-더하기는 그대로 둔다 (아껴지는 0.7초보다 빠뜨려서 틀린 마트가 나가는 쪽이 비싸다). 그림은 영향 보고서와 맞고, 그림 아래에 다음 문장을 넣는다: "보고서는 정책(meta 근거 계열)으로 Gold 모델을 좁힌다. 빌드는 안전장치(`evidence_current+`) 때문에 시드 아래를 모두 다시 만든다." 그림 스크립트는 사용자가 가지고 있어 사용자가 넣는다.

## 결정 16. 생존 확인은 막히면 같이 쉬고, 빈 것만 다시 묻고, 실패가 많으면 빨갛게 끝난다 (F-15, F-13) [확정] (2026-11-18, 사용자 선택)

지시서 `handoff_l9_close.md` 1절 D1 = A + B + D + E, D2 = A, D4 = 예.

- **결정 (F-15):** `truth/repo_alive.py`
  - 429면 모든 동시 요청이 같이 쉰다. Retry-After를 따르고, 없으면 60, 120, 300초 (B). 날 사이에 쉬는 (C)는 하지 않는다. 막힐 때 쉬는 B가 대신한다.
  - `--only-missing`: 같은 구간에 성공한 확인이 있으면 묻지 않는다 (A). `late_truth`가 넘긴다.
  - 확인 실패 비율이 `--max-fail-rate`(기본 5%)를 넘으면 exit 1 (E).
  - `pick_targets.sql`이 의심과 대조군을 번갈아 낸다 (D).
- **결정 (F-13):** repo_alive가 registry의 지금 버전을 읽어 pick_targets에 파라미터로 넘긴다 (A). Silver가 Gold를 읽는 (B), truth에 컬럼을 더하는 (C)는 하지 않는다.
- **D4:** 07-16 ~ 07-21 생존 확인을 다시 보냈다 (2026-11-18 09:44Z, `k1_truth_retry`, 요청 2,445번). 07-15는 다시 보내지 않았다 (M-8 기준).
- **잃는 것:** 막히면 늦게 끝난다 (쉼이 쌓이면 수 분). 문턱을 넘으면 정답 일부가 들어간 채로 태스크가 빨갛다 (raw는 남고, 다음 실행의 `--only-missing`이 빈 것만 묻는다). F-13 옛 몫은 truth에 남는다.
- **기록:** M-21, F-13, F-15, guarantees 1절.

## 결정 17. 클라우드 리전은 서울 (asia-northeast3) [확정] (2026-11-22, 사용자 선택)

engine_selection.md E2. 추천은 A(미국)였고 사용자가 B(서울)를 골랐다.

- **결정:** GCS 버킷, BigQuery 데이터셋, Spark 클러스터, Airflow VM을 모두 asia-northeast3에 둔다.
- **이유 (사용자 선택):** 한국 서비스라면 실제로 이렇게 둔다 (E2 B의 장점).
- **잃는 것:** GH Archive 공개 데이터셋이 US라면 한 쿼리로 조인할 수 없다. Bronze는 시간별 파일을 직접 받아 GCS에 두므로(E3 A) 처리 경로에는 영향이 없고, 공개 데이터셋은 검증용으로만 쓴다. 검증은 US 위치에서 날짜별 건수를 뽑아(결과는 몇 줄) 우리 표와 비교한다. 단가가 조금 오른다 (e2-standard-2 시간당 약 0.087 대 0.067달러, GCS GiB당 월 0.023 대 0.020달러, c1_forecast.md 3절).
- **확인할 것:** 첫날 `bq show githubarchive:day`로 위치를 적는다. 서울 리전에서 Lakehouse runtime catalog와 Managed Service for Apache Spark 3.0 이미지를 쓸 수 있는지 [확인 필요: 위치 목록].

## 결정 18. Bronze는 GCS 원본 파일, Silver는 Iceberg + Lakehouse REST 카탈로그 [확정] (2026-11-22, 사용자 선택)

engine_selection.md E3 A, E4 A, E5 A 묶음.

- **이유:** 원본 보관이 첫 원칙이고 재처리 단위가 파일이어야 한다(E3). 스냅샷으로 그때의 판정을 다시 본다(E4). Hadoop 카탈로그의 동시 쓰기 손실을 로컬에서 이미 겪었다(결정 10, F-7). REST 카탈로그는 서버가 충돌을 거절한다(결정 10 다시 볼 조건).
- **잃는 것:** GCP 카탈로그에 묶인다. 표준 REST API라 옮길 때는 접속 설정만 바뀐다(E5).

## 결정 19. Spark는 클러스터 방식 + 3.0 이미지 [확정] (2026-11-22, 사용자 선택)

engine_selection.md E7 A.

- **이유:** Spark UI, YARN, 노드 설정을 직접 본다. 6 vCPU(마스터 1 + 워커 2, e2-standard-2)로 무료 체험 8 CPU 안에 들어간다. 로컬과 같은 Spark 4.1 계열이라 지문 비교가 된다(local_baseline.md 1절).
- **잃는 것:** 클러스터를 만드는 몇 분, 지우는 걸 잊으면 새는 돈(유휴 자동 삭제로 막음).
- **예측:** 일주일 실행 37.5 ~ 73.7분 (c1_forecast.md).

## 결정 20. Airflow는 VM 위 Docker Compose, Gold는 BigQuery + dbt-bigquery [확정] (2026-11-22, 사용자 선택)

engine_selection.md E8 A, E9 A.

- **이유:** 관리형 Airflow는 외부 추정 최소 월 300달러 이상(gcp_research.md 6절)이라 크레딧 전부와 비슷하다. VM 방식은 한 달 몇 달러다(c1_forecast.md 3절). Gold는 분석팀이 실제로 쓰는 웨어하우스이고 Silver Iceberg를 옮기지 않고 읽는다.
- **잃는 것:** VM 업그레이드, 백업, 보안 패치를 직접 한다. 클러스터 6 + VM 2 = 8 vCPU로 체험 제한에 여유가 없다.



## 결정 22. 원천 등록부와 늦은 데이터 거부 가드 [결정] (2026-11-22, 지시서 handoff_v5_wp1.md 1-2) 〔근거: 지시서 1-2 "기록: 결정 22"〕

- **결정:** `sources/registry.yml`에 원천마다 Bronze 위치와 형식, 주체 종류, 쓰기 방식, 늦은 데이터 처리를 둔다. `spark_jobs/load_events.py`가 이것을 읽는다. 〔근거: 코드: sources/registry.yml, spark_jobs/load_events.py `source()`〕
- **쓰기 방식은 `overwrite_partition` 하나:** 등록부에 다른 값을 적으면 적재가 시작 전에 멈춘다. 〔근거: 코드: load_events.py `source()`. 판단: 원천이 모두 정적 파일이라 다른 방식이 필요 없다(지시서 1-2 2번). 스트림 원천이 오면 MERGE를 더한다〕
- **늦은 데이터 `reject`:** 쓰기 직전에 파일 날짜(window_end)와 다른 `event_date`(NULL 포함) 행을 세고, 1 이상이면 쓰지 않고 실패한다. 〔근거: 코드: load_events.py `reject_late`, run_sql.py `run(..., check=)`. 계산: 쓰기 뒤에는 이미 덮어써졌으므로 막을 수 있는 시점은 쓰기 전뿐(지시서 1-2 3번)〕
- **잃는 것:** 그날 Bronze를 한 번 더 읽는다. 07-15 12.7 ~ 13.9초, 07-20 6.2 ~ 6.6초로, load_events 앱 하나(26 ~ 38초)의 약 35%가 늘어난다. 〔근거: 측정: M-27, M-23 "load_events 26.0 ~ 38.0초"〕
- **다시 볼 조건:** 이 비용이 문제가 되면 쓰기 전 `persist`로 한 번만 읽게 바꾼다. 날짜를 넘는 이벤트가 정상인 원천(시간대가 다른 웹 로그)이 오면 `reject` 대신 "다른 날짜 행은 그 날짜 파티션에 MERGE"를 더한다. 〔근거: 판단: 지금 GH는 7일 모두 다른 날짜 행 0개라 거부가 맞다(M-27). 웹 원천의 날짜 경계는 1-1에서 본다〕


## 결정 23. 참조 목록은 적재일마다 쌓고, AI 에이전트는 ai.robots.txt의 목적 칸으로 고른다 [결정] (2026-11-22, 지시서 1-3) 〔근거: 지시서 handoff_v5_wp1.md 1-3 참조 목록, ai_agent_ua〕

- **Bronze:** `raw/ref/<목록>/dt=<적재일>/`에 받은 그대로와 `_manifest.json`(주소, 크기, sha256, 라이선스). 이미 있으면 받지 않는다. 〔근거: 코드: spark_jobs/load_ref.py `fetch`, sources/registry.yml crawler_ua·ai_robots〕
- **Silver:** `bot.silver.ref_ua_patterns`(pattern, list, name, operator, purpose, load_date), 파티션 (list, load_date). 다른 적재일은 그대로 남아 과거 판정을 다시 만들 수 있다. 〔근거: 코드: create_tables.py, load_ref.py `load`. 결정 9 되돌릴 수 있는 버전〕
- **ai.robots.txt 쪽 원천:** 지시서 링크(fbartho 포크) 대신 원 저장소 `ai-robots-txt/ai.robots.txt`의 `robots.json`을 받았다. 〔근거: 판단: 포크보다 원 저장소가 갱신된다. 확인 필요: 원 저장소 LICENSE가 MIT인지 본다〕
- **AI 이름은 낱말 경계 정규식으로 바꾼다:** robots.json의 키는 정규식이 아니라 이름이고 `Code`, `Dot`, `Muse`처럼 짧은 것이 있어, 앞뒤가 글자·숫자가 아닐 때만 걸리게 했다. 〔근거: 코드: load_ref.py `token`. 측정: M-25〕
- **ai_agent_ua 범주(웹 근거, 아직 SQL 없음):** robots.json `function`이 `AI Assistants`, `AI Agents`, `AI Coding Agents`, `Undocumented AI Agents` 중 하나인 이름. 사용자 요청으로 가져오는 것(ChatGPT-User, Claude-User, Perplexity-User가 `AI Assistants`)이 여기 든다. 학습용 수집(GPTBot)과 검색 색인(OAI-SearchBot)은 빠진다. 〔근거: 측정: M-25 목적 칸 분포. 문서: [OpenAI 크롤러 문서](https://developers.openai.com/api/docs/bots?rc=novicell) ChatGPT-User는 사용자 요청〕
- **잃는 것:** 목적 칸이 표준 값이 아닌 자유 글(예: "LLM training.")인 이름은 에이전트로 세지 않는다. 〔근거: 측정: M-25 자유 글 81개. 판단: 에이전트를 사람 쪽에 셀지 고르는 범주라 넓게 잡으면 봇이 사람으로 들어간다〕

## 결정 24. 규칙 묶음 id를 Gold에 남기고, 기본 묶음은 공개할 때만 옮긴다. 지표별 기본 정책은 시드 [결정] (2026-11-22, 지시서 1-5) 〔근거: 지시서 handoff_v5_wp1.md 1-5 (1), (2). direction_v5.md 1.2, 3.1〕

- **ruleset_id:** 시드 `evidence_current.csv`와 `policy_family.csv`의 행을 정렬해 sha256한 앞 12자리. `warehouse/dbt_build.py`가 계산해 dbt 변수로 넘긴다. 지금 묶음은 `ad4bfe93bcd9`. 〔근거: 코드: warehouse/dbt_build.py `ruleset_id`. 판단: 행 정렬과 줄 단위 해시라 CRLF로 받은 레포와 클라우드가 같은 id가 된다〕
- **SQL이 아니라 Python에서 계산하는 이유:** 정렬된 `string_agg`는 BigQuery 변환이 안 된다(F-5와 같은 문제). 〔근거: 코드: failure_analysis.md F-5, tests/test_gold_dialect.py〕
- **policy_compare는 묶음마다 행을 쌓는다:** dbt incremental `delete+insert`, 키는 ruleset_id. 같은 묶음을 다시 만들면 그 행만 바뀐다. 〔근거: 코드: models/gold/policy_compare.sql 설정 줄. 측정: M-28 시험 뒤 두 묶음 161행씩〕
- **기본 뷰 `policy_compare_released`:** `seeds/ruleset_release.csv`의 seq가 가장 큰 묶음만. 규칙을 바꿔 다시 빌드해도 공개 줄을 더하기 전까지 분석가 숫자는 그대로다. 〔근거: 코드: models/gold/policy_compare_released.sql. 문서: [당근 데이터 신뢰성 발표](https://speakerdeck.com/bskim/danggeunmakesgwa-hamgge-seongjanghamyeo-deiteo-sinroeseong-jikyeonaegi-sugunsugun-deiteo-enjinieo-miseob-1hoe) 지표 불안정성〕
- **변경 기록 `ruleset_change_log`:** 기본 묶음 대 policy_compare의 다른 묶음, 지표 x 날짜 x 처리 방식마다 차이. 〔근거: 코드: models/gold/ruleset_change_log.sql. 측정: M-28〕
- **metric_policy:** `metric_policy_proposal`이 규칙(direction_v5.md 3.1)으로 제안하고 사람이 `seeds/metric_policy.csv`에 옮긴다. 지금 시드는 제안 그대로이고 이유 칸에 "사람 확인 전"이라 적었다. 〔근거: 코드: models/gold/metric_policy_proposal.sql, seeds/metric_policy.csv. 판단: 기본값은 사람이 책임지는 결정(CLAUDE.md 3절 4번)〕
- **잃는 것:** 시험으로 만든 묶음의 행이 policy_compare에 남는다(v3 시험 `fdcdcd213e30`). 지우려면 `--full-refresh`로 다시 만든다. 〔근거: 측정: M-28. 판단: 남은 행이 시험 기록 구실을 한다〕
- **이식 때 확인:** dbt-bigquery에서 incremental `delete+insert`를 쓸 수 있는지. 안 되면 ruleset_id로 파티션을 나눠 `insert_overwrite`. 〔근거: 확인 필요: dbt-bigquery 문서의 incremental_strategy 목록〕
- **덧붙임 (2026-11-22, D10 = (나)+(다)) [확정]:** 사용자가 정했다. 〔근거: 사용자 2026-11-22 지시 "D10은 (나)+(다)로 정했어". direction_v6.md D10. handoff_v7.md 1-6〕
  - (나) 기본 정책 규칙의 잣대를 채점된 balanced로 바꿨다: lenient와 balanced 차이가 자연 변동보다 크면 balanced, 아니면 lenient. strict는 제안하지 않고 `step_strict` 칸만 보고용 상한으로 남긴다. 〔근거: 코드: models/gold/metric_policy_proposal.sql. review_logic.md R-2〕
  - 결과(예측과 같음): 활동 계정 lenient, 이벤트 lenient, 계정당 이벤트 balanced(옛 strict), star 상위 100 balanced. 시드를 그대로 옮기고 이유 칸에 "사람 확인 전"을 남겼다. q1(이벤트 기본 정책)은 lenient 그대로다. 〔근거: 실행 출력 2026-11-22 04:25 metric_policy_proposal. 코드: seeds/metric_policy.csv〕
  - 남은 약점: 잣대인 자연 변동(날짜 사이 변화)은 정책 사이 수준 차이와 다른 양이고, star 상위 100에서는 남은 개수의 변화(0.99%)라 목록 교체(날마다 73 ~ 89%)를 보지 못한다. 규칙은 그대로 두고 시드 이유 칸에 적었다. 〔근거: review_logic.md R-3. 측정: M-36 (c)〕
  - (다) 설계안 (구현 전, 사용자 확인 대기): 지시서는 "같은 날 이벤트 수를 쓰는 근거(always_on, multi_repo_burst)를 뺀 정책을 시드에 하나 더하는 안"을 말하지만, 지금 시드에서 그 정책은 balanced와 계열이 똑같다(자기 신고 + 인프라 + 조직). 새 줄을 더하면 이름만 다른 같은 숫자가 생긴다. 〔근거: 코드: seeds/policy_family.csv. 판단: 같은 정책 두 이름은 분석가를 헷갈리게 한다〕
  - 그래서 (다)의 실제 내용은 "행동 근거를 배정 전 기간으로 고정"이다: 매크로 `experiment_population(start_date, lookback_days=7)`가 balanced 판정에 더해 [start − 7일, start − 1일] 안에 always_on 또는 multi_repo_burst가 한 번이라도 걸린 계정을 빼고, 그 목록을 실험 기간 내내 고정한다. 실험 중의 행동은 모집단을 바꾸지 않으므로 처리 뒤 선택이 생기지 않는다. 〔근거: review_logic.md R-4 고칠 방향 "배정 전 기간 행동으로만". 측정: M-36 (b) strict에서만 빠진 계정 100%가 그날 이벤트 100건 이상〕
  - 바뀌는 곳(구현하면): 매크로 하나, README 가져가는 법 한 줄, metric_policy에 "실험용은 experiment_population" 메모. policy_compare와 화면은 그대로다(일별 정책이 아니므로). 〔근거: 판단: 일별 판정 표(entity_verdict)는 건드리지 않고 조회 시점에 고정 목록을 만든다〕
  - 잃는 것: 실험 시작 전 7일 안에 처음 나타난 봇(실험 중 새로 생긴 봇)은 빠지지 않는다. 7일 데이터로는 07-22 이후 시작 실험만 lookback 전부를 채운다. 〔근거: 계산: 데이터 07-15 ~ 07-21. 판단: 편향 없는 추정과 봇 제거 사이의 교환〕
- 갱신(2026-11-22, handoff_v7.md 단계 6): 지금 공개 묶음은 `9eff1a5e22a6`(seq 3)이다. 위의 `ad4bfe93bcd9`는 옛 id 정의(시드 두 개만 해시)의 값이고 숫자는 같다. 지표별 기본 정책의 잣대는 D10 (나)로 balanced가 되었다(아래 덧붙임). 〔근거: 코드: warehouse/dbt/seeds/ruleset_release.csv. 결정 30, 25〕

## 결정 30. 규칙 묶음 id는 판정을 정하는 입력 전부를 해시한다 [결정] (2026-11-22, 지시서 handoff_v7.md 1-2) 〔근거: 지시서 handoff_v7.md 1-2 "기록: 결정 30". docs/review_logic.md R-1〕

- **해시 입력:** 시드 evidence_current.csv와 policy_family.csv의 행(정렬), 지금 쓰는 근거마다 `sql/evidence/<id>/<ver>.sql`, 근거가 읽는 피처 SQL(features_account.sql과 registry `features_repo` 버전 파일), 판정 모델 4개(stg_evidence, evidence_flags, fake_star_repo, entity_verdict). 〔근거: 코드: warehouse/dbt_build.py `ruleset_parts`, `VERDICT_MODELS`〕
- **SQL은 뜻만 해시한다:** `--` 주석, Jinja 주석, 들여쓰기, 줄 끝 공백, 빈 줄, 줄바꿈 종류를 지운 뒤. 주석만 고쳐서 id가 바뀌면 공개 줄이 쓸데없이 늘어난다. 〔근거: 코드: dbt_build.py `_sql_text`. 시험: tests/test_ruleset_id.py `test_same_meaning_same_id`〕
- **옛 정의가 놓친 것:** 시드 두 개만 해시해서, always_on 문턱을 버전을 올리지 않고 바꿔도 id가 `ad4bfe93bcd9` 그대로였다. 새 정의는 바뀐다. 〔근거: 시험: test_ruleset_id.py `test_verdict_change_new_id` 4경우(근거 문턱, 판정 모델, 피처 SQL, 정책 시드), `test_features_repo_version_switch`. 6개 통과, 2026-11-22 bot-job〕
- **바뀐 id와 공개:** 지금 규칙은 `ad4bfe93bcd9` → `56aead61a1fd`. 숫자가 같아 공개 줄(seq 2)을 더했다. 지시서가 정한 예외 조건(숫자 같음이면 사람 판단 없이 공개)이다. 〔근거: 측정: M-28 덧붙임. 코드: seeds/ruleset_release.csv 3행. 지시서 1-2 공개 줄 조건〕
- **"숫자 같음"의 뜻:** ruleset_change_log(값 컬럼) 161행 다름 0. 계정당 이벤트의 분산과 MDE는 상대 1e-14 크기로 다르지만, 같은 id를 다시 빌드해도 35행 중 33행이 그만큼 흔들리므로 규칙 차이가 아니라 합산 순서 차이다. 〔근거: 측정: M-28 덧붙임 표〕
- **잃는 것:** SQL 모양을 바꾸는 정리(예: F-5 방언 꼴)도 숫자가 같으면 새 id와 공개 줄이 생긴다. 〔근거: 판단: 뜻이 같은지를 SQL 글자로 판정할 수 없다. 숫자 같음 확인이 그 비용을 막는다〕
- **알아 둘 것:** `ruleset_id()`는 만들어 둔 evidence_current.csv를 읽는다. registry를 바꾼 뒤 `dbt_build` 밖에서 바로 부르면 csv가 옛 버전이라 다른 id가 나온다(M-28 덧붙임에서 538f61e12f11로 봄). `dbt_build`는 csv를 먼저 쓰므로 빌드 id는 맞다. 〔근거: 코드: dbt_build.py `__main__` 81 ~ 82행 순서. 실행 출력 2026-11-22 04:08〕
- **다시 볼 조건:** 판정 경로에 모델이 늘면 `VERDICT_MODELS`에 더한다. 빠뜨리면 그 모델만 고친 변경이 같은 id로 덮인다. 〔근거: 코드: dbt_build.py 44행 주석. 판단: 목록은 손으로 관리한다〕

## 결정 25. 정렬된 근거 목록은 NULL 위치를 적어 BigQuery로 바뀌게 한다 [결정] (2026-11-22, 지시서 handoff_v7.md 1-3, handoff_v6.md Q2) 〔근거: 지시서 handoff_v6.md Q2 "하나를 골라 결정 25로"〕

- **고른 것:** `entity_verdict.sql`의 `string_agg(distinct e.evidence_id, ',' order by e.evidence_id nulls first)`. 글자 하나 더하기라 모델 구조가 그대로다. 〔근거: 코드: warehouse/dbt/models/gold/entity_verdict.sql 21행〕
- **결과가 같은 이유:** 근거 id는 `where e.hit`와 계열 조인을 지난 값이라 NULL이 없다. NULL이 없으면 NULL 위치는 결과를 바꾸지 않는다. 실제로 entity_verdict 해시가 같다. 〔근거: 코드: entity_verdict.sql hits CTE. 측정: F-5 해결 줄〕
- **버린 것:** 하위 쿼리에서 DISTINCT 뒤 정렬(같은 오류), 정렬 없이 모으기(목록 순서가 실행마다 달라져 why_excluded와 화면 비교가 흔들림), 모델 전용 매크로(방언 분기 코드가 생김), `list_sort(list(distinct ...))`(다른 변환 오류). 〔근거: 실행 출력 f5.py. 판단: 지시서 후보 셋은 예시였고, 넷째 꼴이 가장 작은 변경으로 통과했다〕
- **잃는 것:** 판정 모델 글자가 바뀌어 규칙 묶음 id가 `56aead61a1fd` → `9eff1a5e22a6`이 되었다. 숫자가 같아 공개 줄 seq 3을 더했다. 〔근거: 코드: seeds/ruleset_release.csv 4행. 결정 30 잃는 것〕

## 결정 29. 데이터 실험실 화면은 손으로 쓴 HTML 한 장 + DuckDB-Wasm, 공개는 집계만 [결정] (2026-11-22, 지시서 handoff_v6.md Q13) 〔근거: 지시서 Q13 "결정 29(화면 도구와 공개 범위, site_design.md 4절과 6절 요약)"〕

- **도구:** `site/index.html` 하나가 jsDelivr의 DuckDB-Wasm 1.32.0으로 Parquet을 읽는다. Evidence, Observable Framework, marimo보다 빌드 단계와 패키지가 없고 자유 SQL 편집기를 마음대로 만든다. 〔근거: 문서: docs/site_design.md 4절 후보 표. 측정: M-35 카드 3 ~ 7ms〕
- **다시 볼 조건:** 탭이 열 개를 넘거나 차트 요구가 늘면 Evidence로 옮긴다. 같은 Parquet을 그대로 읽는다. 〔근거: site_design.md 4절 반증 조건〕
- **공개 범위:** 공개 묶음(site/data)은 집계 8개(45KB)만. 계정·저장소 이름이 붙은 대상 단위 표는 `--with-entities`로 git 밖 site/data_local에만. 집계 표에 이름·키 컬럼이 섞이면 내보내기가 멈춘다. 〔근거: 코드: export_site.py ENTITY_TABLES, ENTITY_COLUMNS. direction_v5.md 3-1. 측정: M-35〕
- **누출 막기:** 계정당 이벤트 분포는 꼬리 칸으로 계정을 찾을 수 있어 백분위마다 평균·분산(mde_cap)만, daily_action은 계정 10개 미만 칸을 뺀다. 〔근거: review_logic.md R-5. 측정: M-35 빠진 칸 0개〕
- **숫자 섞임 막기:** 빌드 묶음이 공개 묶음과 다르면 내보내지 않는다. 〔근거: review_logic.md R-14. 코드: export_site.py〕
- **잃는 것:** 화면은 내보낸 시각의 숫자다(정적). 누가 어떤 카드를 쓰는지 기록할 서버가 없다. 〔근거: site_design.md 5절 "요청 흐름 재기 미룸"〕


## 결정 27. 적재 기록표와 문턱: 시도마다 한 행 추가, 0.1% 넘으면 쓰지 않고 실패 [결정] (2026-11-22, 지시서 handoff_v6.md Q5) 〔근거: 지시서 Q5 "기록: 결정 27"〕

- **결정:** `bot.silver.load_audit`(계약은 `spark_jobs/create_tables.py`)에 적재 시도마다 한 행을 추가한다. 칸: source, event_date, 파일 수, manifest 줄, 읽은 줄, 쓴 행, 버린 줄, 필수 필드 5개(id, type, created_at, actor.login, repo.name)별 NULL 수, 늦은 행, 처음 보는 type 수와 이름, status(ok/rejected), reason, run_id, recorded_at. 〔근거: 코드: create_tables.py `bot.silver.load_audit`, load_events.py `record`〕
- **문턱:** 버린 줄 ÷ manifest 줄(manifest가 없으면 읽은 줄), 또는 필수 필드 하나의 NULL ÷ 깨지지 않은 행이 0.1%를 넘으면 events를 쓰지 않고 실패한다. 문턱은 `sources/registry.yml` gh.audit.max_bad_ratio. 〔근거: 코드: load_events.py `gate`, sources/registry.yml. 문서: direction_v6.md Q5 판단과 반증 조건〕
- **버린 줄을 세는 법:** 읽기를 DROPMALFORMED에서 PERMISSIVE + `_corrupt_record`로 바꿔, 깨진 줄을 세고 쓰기 전에 뺀다. 쓰는 행은 전과 같다. 〔근거: 코드: load_events.py `load`의 `where _corrupt_record IS NULL`. 시험: tests/test_sql.py 5개와 tests/test_late_guard.py가 바꾼 뒤에도 통과(표본 3,407행 그대로)〕
- **늦은 행 가드를 한 집계로:** 결정 22의 늦은 행 count를 기록표 집계 안에서 센다. 날짜식은 events.sql과 같은 `to_date(to_timestamp(created_at))`이고, 메시지("다른 event_date")와 동작은 그대로다. 〔근거: 코드: load_events.py `audit` 주석. 시험: test_late_guard.py 통과〕
- **막힌 시도도 남긴다:** status='rejected'와 이유를 적고 쓴 행 0. 기록표 쓰기는 run_sql과 같은 write-id 확인을 한다. 〔근거: 코드: load_events.py `record`. 판단: 실패한 날이 기록에 없으면 "왜 그날이 비었나"를 로그 없이 답할 수 없다. 반증 조건: 기록표가 커져 문제가 되면 날짜 파티션 덮어쓰기로 바꾼다〕
- **대안과 버린 이유:**
  - (가) 날짜 파티션 덮어쓰기(마지막 시도만): 다시 돌리면 막힌 시도가 지워진다. 〔근거: 판단〕
  - (나) Spark `observe`로 쓰기와 같은 읽기에서 세기: 수치가 쓰기 뒤에야 나와 "쓰지 않고 실패"를 할 수 없다. 〔근거: 문서: [Observation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Observation.html) "metrics ... available after the action". 확인 필요: 설치된 4.1.2 문서 문구〕
  - (다) 읽은 데이터를 persist해 여러 번 세기: 하루 491만 행을 메모리에 들고 있어야 하고, 실제 크기에서 시험하지 못했다. 〔근거: 측정: M-3 하루 4,914,751행. 판단: 결정 22가 같은 이유로 미뤘다〕
  - (라) manifest 줄 − 쓴 행을 버린 줄로 보기(Spark 집계 없이): manifest가 없는 입력(표본, --input 파일)에서 못 쓰고, 빈 줄과 깨진 줄을 가르지 못한다. 〔근거: 판단〕
- **잃는 것:** 표본 340,700줄에서 적재가 1.24초(33%) 늘었다. 기록표 커밋이 시도마다 약 0.6초. 〔근거: 측정: M-34〕
- **0.1%의 뜻:** 표본 3,407줄에서 깨진 줄 1개는 0.029%라 기록만, 10개는 0.29%라 실패다. 하루 491만 줄이면 약 4,900줄까지 기록만이다. 〔근거: 계산: 1 ÷ 3,408, 10 ÷ 3,417, 4,914,751 × 0.001 ≈ 4,915. 시험: tests/test_load_audit.py 앞 두 시험〕
- **manifest 줄과 읽은 줄이 다르면:** 경고만 찍고 기록한다. 날짜 단위 대조는 dbt 시험 `events_match_manifest`가 이미 한다. 〔근거: 코드: load_events.py `load`의 경고 줄, warehouse/dbt/tests/events_match_manifest.sql〕
- **다시 볼 조건:** 실제 7일에서 버린 줄이나 NULL이 0이 아니면 문턱이 맞는지 다시 본다. 웹 원천(WP1-b)은 자체 audit 항목을 등록부에 둔다. 〔근거: 확인 필요: 사용자가 7일을 다시 돌린 뒤 `SELECT * FROM bot.silver.load_audit`. 판단: 문턱은 원천마다 다를 수 있다〕

## 결정 28. 처음 보는 이벤트 type은 경고와 기록만, 알려진 목록은 원천 등록부에 [결정] (2026-11-22, 지시서 handoff_v6.md Q6) 〔근거: 지시서 Q6 "기록: 결정 28"〕

- **결정:** `sources/registry.yml`의 gh.audit.known_types에 GitHub 문서의 이벤트 type 16개를 둔다. 적재 때 목록 밖 type이 나오면 stderr에 경고를 찍고 기록표에 수(n_unknown_type)와 이름(unknown_types)을 남기며, 쓰기는 계속한다. 〔근거: 코드: sources/registry.yml, load_events.py `audit`, `load`의 경고 줄. 문서: [GitHub event types](https://docs.github.com/en/rest/using-the-rest-api/github-event-types) 2026-11-22 확인 16개〕
- **필드가 사라지는 경우:** Q5의 NULL 문턱이 막는다(F-17). 〔근거: 지시서 Q6 첫 줄. 시험: test_load_audit.py actor 시험〕
- **목록을 둔 곳의 대안:**
  - (가) `rules/registry.yml`: 근거와 피처 버전의 등록부이고, `ruleset_parts`가 이 파일을 읽는다. 원천 형식과 판정 규칙이 한 파일에 섞이고, 항목이 늘면 규칙 묶음 쪽 코드가 목록 항목을 근거로 오해할 여지가 있다. 〔근거: 코드: warehouse/dbt_build.py `ruleset_parts`의 `for k, v in sorted(reg.items())`〕
  - (나) 코드 상수: 원천을 붙일 때마다 코드를 고친다. 결정 22는 원천의 성질을 등록부에 둔다. 〔근거: 결정 22〕
  - 고른 것 `sources/registry.yml`: 원천의 형식에 대한 사실이라 원천 등록부 자리다. 규칙 묶음 id가 바뀌지 않는다. 〔근거: 코드: dbt_build.py `ruleset_parts`는 sources/registry.yml을 읽지 않는다. 시험: tests/test_ruleset_id.py 통과〕
- **목록 내용:** 표본의 15개 type은 모두 문서 16개 안에 있다. DiscussionEvent는 문서에만 있다. 옛 type(예: SponsorshipEvent)은 문서에 없어 넣지 않았다. 〔근거: 코드: tests/test_load_audit.py `test_known_types_cover_fixture` 통과. 계산: 표본 type Counter 15종. 확인 필요: 7일 실제 데이터의 type 목록을 기록표 unknown_types로 본다〕
- **잃는 것:** 새 type이 근거 결과를 바꿔도 경고에서 멈춘다. 지금 근거는 WatchEvent, ReleaseEvent 등 이름을 지정해 고르므로 새 type은 대부분 n_types, n_events 같은 계정 피처의 수만 바꾼다. 〔근거: 코드: sql/silver/features_account.sql n_types. 판단: 반증 조건(지시서 Q6)은 새 type이 근거 SQL의 결과를 바꾸는 경우. 그때 실패로 올린다〕
- **행 단위 비율 시험과의 관계:** dbt 시험 `type_share_shift`(경고)는 알려진 type의 몫이 전날보다 절반 넘게 바뀌는 것을 본다. 새 type 경고는 적재 때, 몫 변화는 Gold 빌드 때 나온다. 〔근거: 코드: warehouse/dbt/tests/type_share_shift.sql〕
- **표본 시험:** 07-12 사본 첫 줄의 type을 FakeEvent로 바꿔 적재했다. stderr `처음 보는 이벤트 type FakeEvent (1행)`, 기록표 status ok, n_unknown_type 1, events 3,407행. 〔근거: 코드: test_load_audit.py `test_unknown_type_warns_and_loads` 통과〕

## 결정 33. 최소 관측 경고의 경계와 비교값 [결정] (2026-11-22, 지시서 handoff_v6.md Q12) 〔근거: 지시서 handoff_v6.md Q12, handoff_v7.md 단계 5〕

- **만든 것:** dbt 경고 시험 두 개. `warehouse/dbt/tests/daily_rows_vs_median.sql`(원천·날짜별 이벤트 행 수 ÷ 직전 7일 중앙값), `warehouse/dbt/tests/bot_share_vs_median.sql`(원천·날짜·정책별 봇 계정 비율 ÷ 직전 7일 중앙값). 〔근거: 코드: 두 파일〕
- **봇 비율의 정의:** 그날 이벤트를 낸 계정 중 그 정책에서 실유저가 아닌 계정의 몫이고 `real_user_events`에서 센다. 판정 없는 계정이 실유저 아님으로 들어가므로(handoff_v7.md 1-1의 기본값 false) 판정이 빠진 날이 여기 걸린다. 〔근거: 코드: bot_share_vs_median.sql, real_user_events.sql `coalesce(..., false)`〕
- **[결정] 경계를 포함한다(0.5 이하, 2 이상):** 지시서는 "0.5 미만 또는 2 초과"인데 같은 절의 시험은 "하루 행을 절반으로 → 경고"라, 정확히 절반이면 미만 경계로는 경고가 나지 않는다. 대안 (가) 지시서 그대로 미만: 짝수 행 수의 절반 날을 놓친다. (나) 시험을 "절반보다 조금 적게"로: 시험이 경계를 피해 간다. 잃는 것: 비가 정확히 0.5나 2인 날 경고가 하나 더 나올 수 있다. 〔근거: 지시서 Q12 두 줄. 시험: test_daily_watch.py에서 3,406행 → 1,703행, 비 정확히 0.5로 경고〕
- **[결정] 비교값:** 직전 7일 창(1 ~ 7일 전)에 적재된 날이 하루라도 있으면 그날들의 중앙값으로 비교한다(n_prev 칸에 며칠인지 남김). 창에 날이 없거나 중앙값이 0이면 "비교값 없음"으로 행을 내지 않는다. 대안: 7일이 다 차야 비교(실제 데이터가 7일뿐이라 한 번도 돌지 않는다). 잃는 것: 앞쪽 날은 하루 이틀 값으로 비교해 요일 차이를 더 탄다. 〔근거: 코드: 두 시험의 `n_prev > 0 and prev_median > 0`. 계산: 07-15 ~ 07-21 7일이면 7일 창이 다 찬 날이 0개〕
- **표본 시험 (`tests/test_daily_watch.py`):** 앞 네 날(07-11 ~ 07-14) 3,406행, 07-15 1,703행, 07-16 3,406행에 근거를 돌리지 않음. 〔근거: 코드: tests/test_daily_watch.py docstring〕

  | 시험 | 예측 | 실제 | 근거 |
  |---|---|---|---|
  | daily_rows_vs_median | 07-15 한 행, 비 0.5 | 07-15 한 행, n 1,703, n_prev 4, 중앙값 3,406, 비 0.5 (warn) | 〔근거: 실행 출력 스크래치 watch_rows.py, test_half_rows_warns 통과〕 |
  | bot_share_vs_median | 07-16, 중앙값이 0보다 큰 정책 모두 | 07-16 세 정책: lenient 중앙값 0.0245 → 비 40.9, balanced 0.2578 → 3.88, strict 0.2578 → 3.88 (warn). 다른 날 0행 | 〔근거: 같은 실행 출력, test_missing_verdicts_raise_bot_share 통과〕 |

- 07-15(행 절반)는 봇 비율 경고에 걸리지 않았다. 행이 줄어도 계정의 판정 몫은 거의 그대로였다. 〔근거: 실행 출력에 07-15 bot_share 행 없음〕
- 표본에서 balanced와 strict 봇 비율이 같다(25.8%). 표본이 star 집중 저장소 위주로 뽑혀 저활동 계정이 많고, 한 시간치라 always_on이 0이기 때문이다. 실제 비율이 아니다. 〔근거: 코드: tests/make_fixture.py docstring, guarantees.md "always_on은 표본이 한 시간치라 hit가 0개". 판단: 반증 조건은 multi_repo_burst가 표본에서 hit를 내는데도 둘이 같은 경우〕
- **실제 7일 예측(돌리지 않음):** 두 시험 모두 0행. 07-20(토)은 491만 → 362만이라 07-15 ~ 07-19 중앙값 대비 비 약 0.74 ~ 0.8로 경계 밖이 아니다. 〔근거: 측정: M-27 07-15 4,914,751행, 07-20 3,616,185행. 계산: 3,616,185 ÷ 4,914,751 ≈ 0.74(중앙값이 07-15보다 작으면 비는 더 크다). 확인 필요: 사용자가 Gold 빌드 뒤 `dbt test --select daily_rows_vs_median bot_share_vs_median`〕
- 봇 비율의 실제 예측: 정책별 날짜 사이 변화가 M-26 자연 변동 범위라 0행. 〔근거: 측정: M-26 자연 변동. 확인 필요: 위와 같은 명령〕

---

## 결정 31. features_account는 SQL·Spark 설정을 그대로 두고 컨테이너 상한만 12g → 4g [결정] (2026-11-22, 지시서 handoff_v7.md 3-1) 〔근거: 지시서 3-1 "결정 31"〕

- 고른 것: `dags/bot_build.py`의 features_account `mem="12g"`를 `"4g"`로 바꾼다(힙은 `heap()`이 3072m로 계산). features_repo는 재지 않았으니 12g 그대로 둔다. 〔근거: 코드: dags/bot_build.py 52번째 줄, dags/jobs.py heap(). 측정: M-37 4g 줄〕
- 이유 1: 가장 큰 날(07-15)에 4g에서도 spill 0, 앱 시간 26.4초로 12g(25.8초)와 같다. 3g까지 spill이 없어 4g는 한 칸 여유가 있다. 〔근거: 측정: M-37 상한별 표〕
- 이유 2: 2g에서도 실패하지 않는다(spill 1.14GiB, failcnt 0). 상한을 잘못 잡아도 느려질 뿐 죽지 않는다는 것이 재서 확인되었다. 〔근거: 측정: M-37 2g 줄 네 번〕
- 버린 처방: B(advisoryPartitionSizeInBytes)는 효과 없음. C(셔플 파티션 합치기 끄기)는 spill을 73% 줄이지만 GC가 2 ~ 3배, 태스크 1,002개다. SQL 고치기(투영 줄이기, 파티션 키)는 실패가 없어 고칠 대상이 없다. 〔근거: 측정: M-37 후보 표. 계산: 1 − 0.310 ÷ 1.138 = 73%〕
- 남겨 둔 처방: 2g 아래 머신에서 돌려야 하면 A(동시 태스크 수 줄이기, local[4] 또는 executor 코어 수)를 먼저 쓴다. GC 13.9 → 0.7초, 태스크 실행 합 1/3. 〔근거: 측정: M-37 후보 A〕
- 되돌리기: 한 줄을 12g로 되돌리면 된다. 다음 DAG 7일 실행에서 features_account의 `[mem.peak]` 줄로 4g 안에 드는지 본다. 〔근거: 코드: dags/jobs.py `[mem.peak]` trap. 확인 필요(사용자가 DAG를 돌릴 때)〕

## 결정 32. Spark 앱은 날짜마다가 아니라 태스크마다 하나, 날짜는 앱 안에서 차례로 [결정] (2026-11-22, 지시서 handoff_v7.md 3-2) 〔근거: 지시서 3-2 "결정 32"〕

- 고른 것: `spark_jobs/run_days.py`(태스크 하나 = 앱 하나, 날짜마다 `load()`나 `run_sql.run()`)를 새로 두고, `dags/bot_build.py`의 `each_day`가 날짜 반복 셸 대신 이것을 부른다. 쓰기 단위(날짜 파티션 덮어쓰기와 가드)는 그대로다. 〔근거: 코드: spark_jobs/run_days.py, dags/bot_build.py each_day〕
- 이유 1: 결과가 같다. 묶어 7일을 다시 쓴 뒤 기준값 329개 중 다름 0개, 적재 기록표도 날짜별 쪽과 같다. 〔근거: 측정: M-38〕
- 이유 2: 컨테이너 시간 1,072 → 586초(−45%), 태스크 실행 합 −22%, GC −48%. 〔근거: 측정: M-38 표〕
- 이유 3: 결정 31의 4g 상한에서도 features_account 7일을 한 앱으로 돌려 spill 0, 93초, 실패 없음. 〔근거: 측정: M-38 덧붙임(run_days 확인)〕
- 잃는 것: 중간 날짜에서 실패하면 다시 돌릴 때 앞 날짜도 다시 쓴다. 덮어쓰기라 결과는 같고 시간만 든다. 끝난 날짜 건너뛰기(run_id)는 c1_forecast.md 2절에 적었지만 아직 만들지 않았다. 〔근거: 코드: run_days.py ponytail 주석. c1_forecast.md 2절 "묶을 때 잃는 것"〕
- 확인 안 한 것: Airflow로 bot_build를 돌려 보지 않았다(명령 모양과 파이썬 문법만 확인). 다음 DAG 실행이 첫 확인이다. 〔근거: 확인 필요(사용자가 bot_build를 7일로 돌릴 때 태스크 9개 모두 성공하는지)〕
- 되돌리기: `each_day`를 날짜 반복 셸로 되돌리면 된다(커밋 79ef270의 줄). 〔근거: git 기록〕

## 결정 26. 웹 원천은 AUTH(도서관 로그)를 먼저 붙이고, EClog는 사용자가 파일을 주면 셋째 원천으로 [결정] (2026-11-22, 지시서 handoff_v6.md WP1-b) 〔근거: 지시서 handoff_v6.md WP1-b "결정 26"〕

- 고른 것: Zenodo 3477932(그리스 테살로니키 대학 도서관 검색엔진 nginx 로그 2018-03, 요청 4,091,155건, CC BY 4.0)를 원천 `auth_lib`로 붙인다. 대상 종류는 `web_client`(가린 IP + UA의 해시), 세션은 같은 대상의 30분 무활동으로 끊는다. 〔근거: 코드: sources/registry.yml auth_lib, sql/silver/events_auth_lib.sql. 문서: [Zenodo 3477932](https://zenodo.org/records/3477932)〕
- 이유 1: 로그인 없이 받는다. EClog는 방명록을 거쳐야 해서 사용자 손이 필요하다. 〔근거: direction_v6.md WP1-b, D7. 측정: M-25 머리〕
- 이유 2: 맞춰 볼 기준값이 있다. 사전 실험이 같은 파일로 세션 67,417개, 자기 신고 세션 19.1%, 상세 조회 −57%를 냈다. 〔근거: 코드: experiments/p0_bot_detection/REPORT.md 3절〕
- 이유 3: "원천을 더 붙이기 쉬운 플랫폼"을 보이려면 원천 하나 붙이는 비용을 실제로 재야 한다. 막힌 원천을 기다리지 않고 지금 잴 수 있는 쪽을 먼저 한다. 〔근거: handoff_v7.md 단계 4. 측정: M-30 비용 표〕
- 원본은 받은 그대로 둔다: JSON 객체 하나(한 줄에 요청 하나)를 NDJSON으로 바꿔 두지 않고, Spark가 텍스트 줄로 읽어 줄마다 파싱한다. 〔근거: 코드: spark_jobs/load_web.py. CLAUDE.md 3절 6번〕
- GitHub 매일 빌드(dags/bot_build.py)에는 넣지 않는다: 한 달치 고정 파일이라 매일 돌 일이 없다. 웹 근거를 GH 날짜에 돌리면 0행이라 쓰기 확인이 실패하므로(F-22) GH 날짜를 도는 곳은 `dbt_build.is_gh`로 거른다. 〔근거: 코드: dags/bot_build.py EVIDENCE, warehouse/dbt_build.py is_gh, scripts/demo.py. 실행: 다른 세션이 demo 실패를 알림(2026-11-22)〕
- 잃는 것: 웹 정답 `auth_library`는 공개 파일에 사람 검토 라벨이 없어 UA 목록 + robots.txt로 만든다. 그래서 `declared_ua`와 같은 신호이고(시드가 그 채점을 막는다), 행동 근거를 재는 힘도 약하다. 〔근거: 코드: sql/truth/auth_library.sql 머리 주석, truth_independence.csv `auth_library,declared_ua,false`〕
- EClog: 사용자가 하루치를 주면 같은 방식(등록부 한 항목, 적재기, Silver SQL)으로 셋째 원천을 붙이고 비용을 한 번 더 잰다. 〔근거: direction_v6.md D7〕

## 결정 34. AUTH의 요청 없이 끊긴 줄(408, 400)은 버리지 않고 action = 'no_request'로 남기며, 적재 문턱에서는 resource 빠짐으로 세지 않는다 [결정] (2026-11-22, F-23) 〔근거: 측정: F-23 원인 줄〕

- 고른 것: 응답이 408 또는 400인 줄의 resource NULL은 문턱 계산에서 빼고 따로 센다. 그 밖의 줄에서 resource가 빠지면 지금처럼 쓰지 않고 실패한다. 〔근거: 코드: spark_jobs/load_web.py no_req〕
- 이유 1: 원본의 정상 줄이다. 서버가 요청을 받지 못하고 끊은 기록이라 resource가 원래 없다. 〔근거: F-23 원인 줄. 문서: nginx client_header_timeout〕
- 이유 2: 버리면 원본과 events 행 수가 달라져 manifest 대조(읽은 줄 = manifest 줄)가 뜻을 잃는다. 남기면 4,091,155 = 4,091,155가 그대로다. 〔근거: 코드: load_web.py manifest 대조. raw/auth_lib/_manifest.json lines〕
- 이유 3: 문턱의 목적(원천에서 필드가 조용히 사라지는 것 잡기, 결정 27)은 남는다. 408/400이 아닌 줄의 resource 빠짐은 1줄(0.00002%)이다. 〔근거: 계산: 1 ÷ 4,091,155. 결정 27〕
- 잃는 것: 응답 코드가 408/400이면서 진짜로 resource가 사라진 줄은 못 잡는다. 지금 그런 줄의 응답 분포를 아는 방법이 없다. 〔근거: 판단〕
- 판정 쪽: 이 줄들의 대상은 UA가 "-"다. assetless_browser는 UA가 없거나 '', '-'인 대상을 걸지 않게 했다(사전 실험 L2도 그런 요청을 세션에서 뺐다). 그래서 이 줄들은 어느 근거에도 걸리지 않고 실유저로 남는다. 〔근거: 코드: sql/evidence/assetless_browser/v1.sql hit 식. 02_detect.py〕
- 되돌리기: load_web.py의 no_req 한 줄과 events_auth_lib.sql의 'no_request' 한 줄을 지우면 처음 계약(전부 필수)으로 돌아간다. 〔근거: 코드〕


## 결정 35. 웹 원천을 더한 규칙 묶음 841ebd6f310f를 공개 순번 4로 낸다 [결정] (2026-11-22, 지시서 handoff_v6.md WP1-b ~ d) 〔근거: 코드: warehouse/dbt/seeds/ruleset_release.csv 4행〕

- 고른 것: 웹 근거 셋(declared_ua, assetless_browser, ai_agent_ua)과 웹 정답, 웹 지표가 들어간 규칙 묶음을 공개 순번 4로 올린다. 〔근거: 코드: ruleset_release.csv〕
- 이유 1: GH 숫자가 그대로다. 앞 묶음 9eff1a5e22a6과 GH policy_compare 161행 값 다름 0, MDE 차이 최대 1.6×10⁻¹⁴(부동소수 합 순서), 기준값 329개 중 바뀐 값 0. 〔근거: M-32 마지막 줄〕
- 이유 2: 웹 숫자가 사전 실험과 맞는다. 자기 신고 세션 19.06%(사전 19.1%), 상세 조회 −56.8%(사전 −57%), 전환율 64.9 → 64.8%. 〔근거: M-31, M-32〕
- 이유 3: 묶음 id가 바뀐 것은 판정 입력(근거 등록부, 시드)이 바뀌었기 때문이라 맞다. 같은 id로 다른 판정을 내면 안 된다. 〔근거: 결정 30〕
- 잃는 것: ai_agent_ua에 오탐 3건(주소 속 "code")이 들어 있는 채로 낸다. 판정은 바뀌지 않는다(이미 신고된 봇). 〔근거: M-31〕
- 되돌리기: ruleset_release.csv의 4행을 지우고 dbt를 다시 빌드하면 순번 3이 공개 묶음으로 돌아온다. policy_compare의 841ebd6f310f 행은 남는다(이력). 〔근거: 코드: ruleset_release.csv, policy_compare.sql incremental〕

## 결정 36. 공개 전에 봇 의심과 함께 적힌 계정 이름 다섯 개를 기록 전체에서 "계정 A ~ E"로 바꾼다 [결정] (2026-11-22, 단계 7 공개) 〔근거: 사용자 2026-11-22 "커밋, U1 정리, push, 레포 공개 … 맡길게"〕

- 고른 것: measurements.md M-6, M-15와 옛 q2_why_excluded.sql에 있던 GitHub 계정 이름 다섯 개를 작업 문서 빼기(U1)와 같은 filter-branch에서 모든 커밋에 걸쳐 "계정 A ~ E"로 바꿨다. github-actions[bot]은 GitHub가 운영하는 공식 봇이라 그대로 둔다. 〔근거: 코드: 기록 다시 쓰기 스크립트(스크래치 u1_tree.sh). 실행: git log -p main에서 다섯 이름 0줄〕
- 이유 1: 레포를 공개하면 실제 사람일 수 있는 계정 이름이 "한 주체가 조종한 계정 무리로 보인다" 같은 의심과 함께 인터넷에 남는다. 판정은 확인된 사실이 아니다. 〔근거: measurements.md M-6 해석 줄 "[확인 필요]". 판단〕
- 이유 2: 프로젝트 규칙과 같다. 화면과 내보내기는 이미 계정 이름을 싣지 않고, q2 쿼리도 이름을 뺐다(M-36 ②). 〔근거: scripts/export_site.py ENTITY_COLUMNS. M-36 표 ②줄〕
- 잃는 것: 기록만 보고 그 계정을 다시 찾을 수 없다. 숫자(579건, 03:25:51 등)는 그대로라 같은 Silver 표에서 다시 찾을 수 있다. 〔근거: 판단〕
- 되돌리기: 로컬 가지 backup-before-clean에 바꾸기 전 기록이 있다(원격에 올리지 않는다). 〔근거: 실행: git branch backup-before-clean〕

## 결정 37. 데이터 상품 카드 규격과 변경 정책 세 문장을 지시서 문구대로 쓰되, 주인 칸에는 실명 대신 GitHub 계정을 쓴다 [확정] (2026-11-22, handoff_delivery.md 3절 1번) 〔근거: 사용자 2026-11-22 선택 "주인 이름만 빼기"〕

- 고른 것: 화면 맨 위 카드에 이름과 한 줄 소개, 한 행의 뜻, 주인과 문의, 갱신 약속, 마지막 갱신, 버전과 변경 정책, 컬럼 사전 일곱 항목을 둔다. 값은 meta.json에서 오고 비면 내보내기가 실패한다. 〔근거: handoff_delivery.md 1절 표〕
- 변경 정책: (1) 숫자가 같은 내부 변경은 같음 증명을 붙여 공개한다. (2) 숫자가 바뀌는 공개는 바뀐 지표와 폭을 먼저 보여 준 뒤 공개한다. (3) 컬럼을 지우거나 이름을 바꾸지 않고 새 컬럼을 더한다. 꼭 지워야 하면 한 버전 동안 옛 컬럼을 남기고 미리 알린다. 〔근거: handoff_delivery.md 1절 변경 정책 문장〕
- 주인과 문의: 주인은 GitHub 계정 sozerong, 문의는 레포 이슈 양식이다. 지시서의 실명은 공개 사이트에 싣지 않는다. 〔근거: 사용자 선택〕
- 갱신 약속: "데이터 범위는 2024-07-15 ~ 21로 고정되어 있고 정기 갱신은 없다. 숫자는 규칙 버전을 새로 공개할 때만 바뀐다." 〔근거: handoff_delivery.md D5. meta.json event_date_min/max〕
- 잃는 것: 서울 레포 카드와 주인 칸 표기가 다를 수 있다(서울은 아직 확인 안 함). 〔근거: 확인 필요(서울 레포 handoff_delivery.md D 항목의 주인 값 확인)〕

## 결정 38. 컬럼 설명은 gold.yml columns에 쓰고, 내보내기가 컬럼 이름으로 이어 붙인다 (D1 선택지 가) [확정] (2026-11-22) 〔근거: 사용자 2026-11-22 선택 "(가) gold.yml"〕

- 고른 것: Gold 모델의 컬럼 설명을 warehouse/dbt/models/gold/gold.yml에 둔다. 화면에서만 계산하는 컬럼(days_changed 등)만 내보내기 안에 둔다. 〔근거: handoff_delivery.md D1 표 (가)줄〕
- 이유: dbt docs, 리니지, 화면이 한 곳의 설명을 쓴다. 분석가가 화면 밖에서 SQL로 볼 때도 같은 설명이다. 〔근거: handoff_delivery.md D1 추천 줄〕
- 잃는 것: 서울의 public_columns.json과 형식이 다르다. 화면 전용 컬럼은 두 번째 장소에 산다. 〔근거: handoff_delivery.md D1 표 (가) 단점, (나) 장점〕
