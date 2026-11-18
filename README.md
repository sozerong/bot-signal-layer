# 봇 프로젝트: 사용자 분석에서 봇을 걸러내는 데이터 플랫폼

회사가 사용자 행동을 분석할 때 봇은 노이즈다. 이 프로젝트는 원본을 그대로 둔 채 데이터 플랫폼 안에서 여러 근거로 봇을 판정하고, 정답 데이터로 근거를 채점하고, 분석팀에 정책별 실유저 데이터를 준다. 원천은 GH Archive다.

- 전체 설계: [docs/design_overview.md](docs/design_overview.md)
- 엔진 선택: [docs/engine_selection.md](docs/engine_selection.md)
- 구현 순서: [docs/implementation_design.md](docs/implementation_design.md)
- 사전 실험: [experiments/p0_bot_detection/REPORT.md](experiments/p0_bot_detection/REPORT.md)
- 이전 설계 (가짜 star 한정, v0.3): [GH_PROJECT_DESIGN.md](GH_PROJECT_DESIGN.md)


## 로컬에서 돌리기

Windows 데스크톱과 Docker Desktop(메모리 32GB 이상 권장, Spark 컨테이너 상한 24GB)을 기준으로 적었다. 명령은 레포 폴더에서 Git Bash로 친다.

### 1. 준비 (처음 한 번)

`.env`를 만든다 (git에 들어가지 않는다). 값은 이 PC에서만 쓰는 개발용으로 새로 만든다.

```bash
cat > .env <<EOF2
POSTGRES_PASSWORD=$(python -c "import secrets; print(secrets.token_urlsafe(16))")
AIRFLOW_FERNET_KEY=$(python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
AIRFLOW_JWT_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(32))")
HOST_PROJECT_DIR=$(pwd -W)
EOF2
```

이미지 두 개(계산용 bot-job, Airflow)를 만들고 Airflow를 띄운다.

```bash
docker compose --profile jobs build
```

```bash
docker compose up -d
```

화면: http://127.0.0.1:8081 (로그인 없이 관리자). DAG 다섯 개가 보인다. `airflow-init`이 DB 준비와 함께 Silver 쓰기 풀 `silver_write`(자리 1개)를 만든다. Silver Iceberg 표에 쓰는 태스크는 이 풀로 하나씩 돈다 (docs/decisions.md 결정 10).

### 2. 일주일치를 처음부터 끝까지

`ingest_gh`를 켜면 2024-07-15 ~ 07-21을 하루씩 받고(catchup), 하루를 다 받을 때마다 `bot_build`가 그날 Silver, 근거, Gold를 만든다.

```bash
docker compose exec airflow-scheduler airflow dags unpause bot_build
```

```bash
docker compose exec airflow-scheduler airflow dags unpause ingest_gh
```

결과: Silver Iceberg 표는 `warehouse_local/silver/`, Gold는 `warehouse_local/gold.duckdb`. 분석팀 표는 `main.real_user_lenient`, `main.real_user_balanced`, `main.real_user_strict`, `main.impact`, 채점은 `main.evaluation`.

### 3. 정답과 채점

`late_truth`는 주 1회 저장소 생존을 확인해 정답을 더하고 채점을 다시 만든다. 손으로 돌릴 때:

```bash
docker compose exec airflow-scheduler airflow dags trigger late_truth -c '{"days": "2024-07-15"}'
```

채점은 정답을 고른 판정 구간(`days`의 날짜)의 판정과만 짝짓는다. `main.evaluation`은 구간마다 한 행과 전체 행(`window_end`가 비어 있음)을 낸다 (docs/decisions.md 결정 14). 판정보다 뒤에 확인한 정답만 채점에 쓴다. 같은 규칙으로 다시 돌린 근거는 옛 판정 시각을 이어받아 채점이 그대로다. 규칙을 바꾼 날에는 다음 정답 확인 전까지 새 버전의 채점이 빈다 (docs/decisions.md 결정 11).

### 4. 규칙을 바꿨을 때

1. 새 버전 SQL을 더한다 (예: `sql/evidence/lockstep_ratio/v3.sql`). `rules/registry.yml`의 `current`를 바꾼다 (근거 규칙, 또는 저장소 피처 `features_repo`). `bot_build`는 registry를 읽으므로 다음 날짜부터 새 버전을 돈다. 새 근거를 더할 때는 registry 한 줄에 Gold `evidence_flags`와 판정식을 더한다 (docs/measurements.md M-15).
2. 리니지 지도를 다시 만든다. 영향 범위 보고서는 registry에서도 출발해 시드와 그 아래를 넣고, Gold 모델은 `gold.yml`의 meta 근거 계열로 좁힌다 (결정 15). registry만 바꿨으면 `lineage.impact --change rules/registry.yml`.

```bash
docker compose exec airflow-scheduler airflow dags trigger column_lineage
```

3. 바뀐 범위만 다시 돌린다.

```bash
docker compose exec airflow-scheduler airflow dags trigger rule_backfill -c '{"change": "sql/evidence/lockstep_ratio/v3.sql", "days": "2024-07-15 2024-07-16"}'
```

### 5. 손으로 한 단계씩 (Airflow 없이)

```bash
docker compose run --rm bot-job python -m spark_jobs.create_tables
```

```bash
docker compose run --rm bot-job python -m spark_jobs.load_events --input raw/dt=2024-07-15 --param window_start=2024-07-15 --param window_end=2024-07-15 --param run_id=manual-1
```

```bash
docker compose run --rm bot-job python -m warehouse.dbt_build
```

측정 (bot-job 안):

```bash
docker compose run --rm bot-job python -m measure.table_digest --day 2024-07-15
```

```bash
python -m measure.spark_eventlog --app features_repo
```

첫째는 그날 Silver 표 내용 지문(`--table`, `--snapshots A B`로 두 스냅샷 비교), 둘째는 Spark 이벤트 로그(`spark-events/`)에서 스테이지별 태스크 시간과 셔플(호스트 Python으로도 돈다).

시험. 방언 시험(`tests/test_gold_dialect.py`)은 dbt 컴파일 결과를 읽는다. 새로 받은 레포에는 `warehouse_local/`이 없어서(git 제외) 컴파일이 `gold.duckdb`를 만들지 못하고 실패하니 폴더부터 만든다. 컴파일을 건너뛰면 방언 시험이 조용히 건너뛰어진다(skip).

```bash
mkdir -p warehouse_local
```

```bash
docker compose run --rm bot-job python -m warehouse.dbt_build compile
```

```bash
docker compose run --rm bot-job python -m pytest tests -q
```

기대: 21개 중 20개 통과, entity_verdict 방언 시험 하나 실패 (아래 알려진 문제).

### 알려진 문제

미해결 오류는 [docs/failure_analysis.md](docs/failure_analysis.md)에 있다. 로컬에서 돌릴 때 부딪히는 것:
- 시험 하나(`tests/test_gold_dialect.py`의 entity_verdict)가 실패한다. BigQuery 방언 변환 문제로 이식 때(C3) 고친다 (F-5).
- 생존 확인 대상 고르기(`sql/truth/pick_targets.sql`)가 근거 규칙 버전을 거르지 않는다. 옛 버전 파티션이 남아 있으면 의심 대상에 섞이고 채점에서 대조군으로 세어진다 (F-13, 미해결).
- Silver 표는 Iceberg Hadoop 카탈로그라 동시 커밋이 서로를 덮을 수 있다. 그래서 Airflow 풀로 줄 세우고 실행기가 쓴 뒤 확인한다 (F-7). Airflow 밖에서 손으로 여러 작업을 동시에 돌리지 않는다.
