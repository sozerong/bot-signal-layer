# 구현 설계: 로컬에서 끝까지 완성하고 클라우드로 옮긴다

Claude Code가 이 문서를 위에서부터 한 절씩 구현한다. 함께 읽을 문서는 `design_overview.md`(전체 구조), `criteria.md`(근거와 정답, 독립성 표), `decisions.md`(확정한 결정), `engine_selection.md`(클라우드 엔진, 9절 이식 때 씀)다.

**2026-11-17 방향 변경 (decisions.md 결정 5):** 데이터 엔지니어 범위를 **로컬에서 전부 완성**한 뒤 GCP로 옮긴다. 이유는 셋이다.

1. 로컬에서 같은 SQL과 같은 표 계약으로 끝까지 돌아가야, 옮긴 뒤 "결과가 같다"를 증명할 수 있다.
2. 옮기면서 바뀐 곳과 줄 수가 "변하는 부분과 유지할 부분을 나눈 설계"의 증거가 된다.
3. 반복하는 동안 클라우드 비용이 들지 않는다.

분석가 쪽으로 넓히는 일(지표 정의 층, 정책 비용 곡선)과 웹 확장안(가상 서비스, 실시간 수집)은 이 문서 범위 밖이다. 12절에 적어 두고 데이터 엔지니어 쪽을 끝낸 뒤 다시 본다.

## 0. 작업 규칙

- `CLAUDE.md`를 따른다. 체크포인트에서 멈추고, 설계와 다르게 해야 하면 표로 묻고, 고른 것은 `decisions.md`에 적는다.
- 수치는 `measurements.md`, 오류는 `failure_analysis.md`, 성질 시험은 `guarantees.md`에 남긴다.
- `raw/`와 사전 실험 폴더는 읽기만 한다. 파일이나 표를 지우기 전에는 묻는다.
- **로컬 코드에 클라우드 분기를 미리 넣지 않는다.** 이미 있는 `session.py`의 `rest` 모드만 둔다. 클라우드용 코드는 9절 이식 단계에서 쓴다. 미리 넣으면 시험하지 못한 코드가 쌓이고, 이식 때 바뀐 양을 잴 수 없다.
- 커밋은 사용자가 한다. 단계가 끝나면 커밋 명령만 준다.

## 1. 지금까지 된 것 (2026-11-17)

| 단계 | 상태 | 결과물 |
|---|---|---|
| L1 뼈대와 시험 환경 | 끝, 커밋 `199ec0d` (Claude 표시를 지우며 다시 쓴 해시) | 아래 코드 목록, 시험 5개 통과, 결정 3·4, F-1 |
| L2 하루치 받기 | 끝, 커밋 전 | `raw/dt=2024-07-15/` 24개 모두 `ok`, 4,914,751줄. measurements.md M-1~M-3, guarantees.md 수집 멱등 |
| L3 하루치 Silver와 근거 | 끝, 커밋 전 | `create_tables`, 276개 = 사전 실험, M-4~M-6. 내용 비교는 F-3, 태스크 분포는 F-2로 막힘 |
| L4 정답 경로 | 끝, 커밋 전 | `truth/repo_alive.py`, truth 550행, MERGE 중복 0, 결정 7, M-7 |
| L5 Gold | 끝, 커밋 전 | `warehouse/dbt/`, 94.9% 대 4.0%, 독립성 음성 시험, 결정 6·8, M-8. F-4, F-5 |
| L6 로컬 Airflow | 끝, 커밋 전 | `dags/` 5개, DAG = 손 실행, 실패 태스크만 다시 쓰기, M-9. 병렬 근거 커밋 유실 F-7, 채점 공백 F-8 |
| L7 일주일 | 끝, 커밋 전 | 7일 catchup 16분 46초, M-10. 셔플·쏠림은 F-2로 못 잼 |
| L8 리니지와 재처리 | 끝, 커밋 전 | 재처리 두 번과 되돌리기, 결정 9, M-11. 지시서 H3에서 리니지 범위로 다시 (M-14) |
| 지시서 H0~H5 | 끝, 커밋 전 | 측정 경로(F-2, F-3), 정확성(F-7, F-10, F-4), 리니지(F-9), 운영 규칙(결정 10, 11, registry를 읽는 DAG), 새 근거(결정 12). M-12 ~ M-15 |
| L9 로컬 완성 점검 | 다시 점검함, 사용자 결정 대기 | 아래 표. F-12 하나가 "일주일 DAG" 항목을 막는다 |

**L9 점검 (2026-11-17, 지시서 `handoff_local_finish.md` H0 ~ H6 뒤)**

| 항목 | 결과 | 근거 |
|---|---|---|
| F-2, F-3, F-4, F-7, F-9, F-10이 [해결] | 그렇다 | failure_analysis.md 각 항목의 "해결". 원인 실험(F-7), 거꾸로 확인(F-4 manifest 사본, F-7 확인 틀리게) 포함 |
| 일주일치가 DAG만으로 처음부터 끝까지 돌고 `dbt build`가 0으로 끝난다 | **아니다** | 일주일 한 번 실행(`h6_week`)에서 근거 태스크 두 개가 컨테이너 메모리 4g를 넘어 죽었다 (**F-12, 새로 생김, 미해결**). Silver는 끝까지 돌았다. 하루치 `bot_build` 다섯 번(M-13, M-15)과 `rule_backfill` 네 번(M-14, M-15)은 `dbt build` 0으로 끝났다 |
| 같은 날 다시 돌린 결과의 **내용**이 같다 (지문) | 그렇다 | 07-15 세 표(M-12), 일주일 실행 앞뒤 07-18 세 표 (F-12 "막히지 않은 것") |
| 리니지가 정한 범위로 규칙 변경 재처리 두 건 (A, B), 스테이지 쏠림 지표 포함 | 그렇다 | M-14. Silver 범위 = 손 범위, Gold는 리니지가 `stg_truth` 하나 더. f2는 태스크 쏠림이 아니라 일 전체가 늘었다 |
| `ops.build_record`의 규칙 버전 = Gold의 실제 규칙 버전 | 그렇다 | F-10 해결 확인, `h3_A_v3`, `h4_registry_v3` |
| 2절 "안 바뀌는 것" 파일에 로컬 전용 코드가 없다 | **일부** | 리니지 추출 논리는 manifest의 소스 이름과 adapter_type을 읽어 중립이 됐다. 리니지 **저장 위치**(`lineage/extract.py`의 쓰기, `lineage/impact.py`의 읽기)는 아직 로컬 `gold.duckdb`다. `sql/`, `rules/`, 시험에는 없다 |
| README에 처음 받는 사람이 따라 할 로컬 실행 방법 | 그렇다 | "로컬에서 돌리기"에 풀, registry, 측정 도구, 알려진 문제를 더했다 |

남은 것: F-12(미해결, 위), F-5(BigQuery 방언, C3), F-11(클러스터 비교, C2). F-8의 숫자 확인은 2026-11-18에 끝났다 (M-16).

**코드 (547줄)**

| 파일 | 하는 일 |
|---|---|
| `spark_jobs/session.py` | SparkSession. 카탈로그 이름은 항상 `bot`. `local`(Hadoop 카탈로그, `warehouse_local/`)과 `rest`(Lakehouse) 두 모드. 시간대 UTC 고정 |
| `spark_jobs/run_sql.py` | SELECT만 있는 SQL 파일을 돌려 그 파티션만 덮어쓴다. 근거 SQL은 경로에서 evidence_id와 rule_version을 읽고 judged_at, sql_sha256, run_id를 붙인다. 같은 규칙으로 같은 hit면 옛 judged_at을 이어받는다(결정 11). 쓴 뒤 자기 스냅샷(`bot.write-id`)이 표에 있는지 확인한다(결정 10). 파라미터는 Spark의 이름 붙은 파라미터로만 넘긴다 |
| `spark_jobs/load_events.py` | json.gz를 필요한 필드만 스키마로 읽고(payload 제외), 깨진 줄은 버리고, 원본 파일 경로를 붙여 events.sql로 넘긴다 |
| `sql/silver/` 3개 | events, 계정 피처, 저장소 피처. 저장소 피처에 가장 무거운 lockstep 자기 조인이 있다 (계정당 star 2~50개, 공통 10명 이상, feature_version f1) |
| `sql/evidence/` | 지금 버전 6개: declared_app v1, always_on v1, low_activity v1, lockstep_ratio v2, burst_5m v1, multi_repo_burst v1 (H5). 옛·실험 버전 lockstep_ratio v1, v3 |
| `rules/registry.yml` | 근거마다 계열, 지금 버전, 대상. 저장소 피처 버전(`features_repo`). `bot_build`, `dbt_build.py`, `lineage/impact.py`, 시험이 여기서 읽는다 |
| `tests/` | 12시 파일에서 뽑은 표본 3,407줄. 같은 규칙을 순수 Python으로 따로 세어 Spark 결과와 비교, 재실행 멱등 |
| `Dockerfile`, `docker-compose.yml` | bot-job 이미지: Python 3.12, Java 21, PySpark 4.1.2, Iceberg 1.11.0 런타임 내장. 메모리 상한 24GB |

**남겨 둔 지름길 (코드의 `ponytail:` 주석)**

- Silver 쓰기 풀이 표마다가 아니라 Silver 전체 한 줄이다. 클라우드 REST 카탈로그(C2)에서 푼다.
- 리니지 저장 위치가 로컬 gold.duckdb다. C3에서 BigQuery 표로 바꿀 때 `lineage/extract.py`의 저장 부분과 `lineage/impact.py`의 읽기만 바꾼다.
- 결과가 0행이면 옛 파티션이 남는다. 근거는 대상 전부에 행을 내서 0행이 나오지 않는다.
- `rest` 모드에 필요한 GCS 모듈이 이미지에 없다. 이식 때(C2) 넣는다.

**L2에서 새로 안 것**

- 받기 속도가 초당 약 23MB로 이전 기록(0.3~0.7MB)의 30배였다. 원인은 모른다. 일주일치 받는 시간은 다시 재야 한다.
- 03시 Release(다른 시간의 4~8배), 10시와 18시 Watch가 튄다. L3에서 한 계정이 몰아서 낸 것인지 본다.

## 2. 로컬과 클라우드에서 바뀌는 것과 안 바뀌는 것

| 자리 | 로컬 | 클라우드 (9절) | 바뀌는 곳 |
|---|---|---|---|
| Bronze | `raw/` 폴더 | GCS | 환경 변수 `BOT_BRONZE` |
| Silver 카탈로그 | Iceberg Hadoop 카탈로그 `warehouse_local/` | Lakehouse REST 카탈로그 | `session.py` 모드 (이미 있음) |
| Spark | bot-job 컨테이너 안 `local[*]` | Managed Spark 클러스터 | 실행 위치, 작업 제출 방식 |
| Gold | dbt-duckdb, `warehouse_local/gold.duckdb` | dbt-bigquery, BigQuery | dbt 프로필, 방언이 다른 SQL 몇 줄 |
| 지휘 | 로컬 Docker Compose의 Airflow, DockerOperator로 bot-job 실행 | VM 위 Airflow, Spark 클러스터 연산자 | DAG의 연산자를 만드는 함수 하나 |
| 정답 확인 | bot-job 컨테이너에서 GitHub에 요청 | 같음 (VM에서) | 없음 |

**안 바뀌는 것:** `sql/` 아래 Silver와 근거 SQL, 4절 표 계약, `rules/registry.yml`, 시험, 리니지 추출기, DAG의 작업 순서. 이식 때 이 목록 밖에서만 고친다면 설계가 의도대로 나뉜 것이다. 이식 단계마다 바뀐 파일과 줄 수를 `measurements.md`에 남긴다.

## 3. 로컬 구성

```
GH/
├── docker-compose.yml        # bot-job + Airflow (L6에서 Airflow 추가)
├── Dockerfile                # bot-job: Spark + Iceberg + dbt-duckdb + 정답 확인 + 리니지
├── raw/                      # Bronze (git 제외)
├── warehouse_local/          # Silver Iceberg 표, gold.duckdb (git 제외)
├── spark-events/             # Spark 이벤트 로그 (git 제외)
├── ingest.py, measure_health.py
├── spark_jobs/  sql/  rules/  tests/          # 이미 있음
├── truth/repo_alive.py       # L4
├── sql/truth/truth.sql       # L4
├── warehouse/dbt/            # L5 (dbt-duckdb)
├── dags/                     # L6
├── lineage/                  # L8 (서울 동네 찾기에서 복사)
└── measure/spark_eventlog.py # L7
```

- **이미지는 bot-job 하나로 간다.** dbt-duckdb, 정답 확인, 리니지를 같은 이미지에 넣는다. 서울 동네 찾기처럼 이미지를 나누는 것은 의존성이 충돌할 때 한다.
- **Airflow는 서울 동네 찾기 구성을 그대로 가져온다** (결정 109: 구성 요소를 컨테이너로 나누고, 계산은 DockerOperator로 작업마다 메모리 상한을 건 bot-job 컨테이너에서).
- 메모리: 데스크톱 64GB 기준. bot-job 24GB, Airflow 구성 요소 합쳐 약 4GB로 시작하고 L7에서 실제 사용량을 보고 고친다.

## 4. 표 계약

기대는 결정: 설계 14절의 결정 2~5(판정 단위, 이벤트 모델, 근거 표 형태, 정책). 아래는 그 초안이고 L1~L2에서 이대로 구현했다. 확정이 바뀌면 이 절과 SQL만 바뀐다. 로컬과 클라우드에서 같은 계약을 쓴다.

### 4.1 Bronze

```
raw/dt=2024-07-15/hour=00/2024-07-15-0.json.gz
raw/dt=2024-07-15/hour=00/_manifest.json
raw/truth_raw/repo_alive/checked_date=2026-11-17/part-000.jsonl
```

- **경로 규칙은 기존 `ingest.py`를 따른다** (`GH_PROJECT_DESIGN.md` D1, D3). 파티션 폴더는 두 자리 시간, 파일 이름은 GH Archive 원래 이름(시간 앞에 0 없음)이다.
- 경로 앞부분(`raw/`)은 환경 변수 `BOT_BRONZE` 하나로 둔다. 이식 때 GCS 경로로 바꾸면 모양이 같아서 Spark 작업은 그대로 읽는다.
- 덮어쓰기 금지는 로컬에서는 `ingest.py`의 기존 동작(완료된 파티션은 건너뜀)으로 지킨다. 이식 때는 GCS 업로드 조건으로 건다 (9절 C2).
- manifest 한 개 = 원본 파일 한 개다. 담는 값은 `ingest.py`가 이미 쓰는 것(`hour`, `source_url`, `status`, `file`, `bytes`, 줄 수 등)을 그대로 쓴다.

### 4.2 Silver (Iceberg, 네임스페이스 `silver`)

**events** (한 행 = 이벤트 하나)

| 컬럼 | 형 | GH Archive에서 |
|---|---|---|
| source | STRING | `'gh'` |
| event_id | STRING | `id` |
| event_ts | TIMESTAMP | `created_at` |
| event_date | DATE | `created_at`의 날짜 (UTC) |
| actor_key | STRING | `actor.id` |
| actor_name | STRING | `actor.login` |
| session_key | STRING | NULL (웹 소스용) |
| action | STRING | `type` |
| target | STRING | `repo.id` |
| target_name | STRING | `repo.name` |
| client_ua, client_ip, referrer, status | STRING | NULL (웹 소스용) |
| bronze_file | STRING | 읽은 원본 경로 |
| run_id | STRING | Airflow 실행 ID |

- 파티션: `source`, `event_date`
- 쓰기: 그 (source, event_date) 파티션 덮어쓰기. 같은 날을 다시 돌려도 같은 결과다.
- `payload`는 넣지 않는다. 쓰는 근거가 없다. 댓글 반복성(`comment_repetition`)을 할 때 추가한다.

**entity_features** (한 행 = 대상 × 날짜 × 피처 버전)

| 컬럼 | 계정 (`gh_account`) | 저장소 (`gh_repo`) |
|---|---|---|
| entity_type, entity_key, event_date, feature_version | 공통 | 공통 |
| entity_name | login | repo 이름 |
| n_events | 이벤트 수 | |
| active_hours | 활동한 시간대 수 (0~24) | |
| n_types | 이벤트 종류 수 | |
| is_low_activity | 그날 WatchEvent만 함 (`n_types = 1`) | |
| stars | | 선언 봇을 뺀 WatchEvent 수 |
| low_act_stars | | 그중 저활동 계정이 누른 수 |
| max_5m_stars | | 가장 몰린 5분 구간의 star 수 |
| max_co_star | | 다른 저장소와 1시간 안에 같이 star한 저활동 계정 수의 최댓값 |

- 파티션: `entity_type`, `event_date`
- `max_co_star`가 이 프로젝트에서 가장 무거운 계산이다. 저활동 계정끼리 자기 조인을 한다. 사전 실험처럼 **계정당 star 2~50개만** 넣는다. 이 상한이 `feature_version = 'f1'`의 일부다.
- 상한을 바꾸는 것은 피처 버전 변경이다(f1 → f2). 피처부터 다시 돌아서 근거 규칙 변경보다 범위가 넓다. 8절에서 둘을 비교한다.

**evidence** (한 행 = 대상 × 근거 × 규칙 버전 × 판정 구간)

| 컬럼 | 형 | 뜻 |
|---|---|---|
| entity_type, entity_key | STRING | |
| evidence_id | STRING | `criteria.md` 2절의 이름 |
| rule_version | STRING | `v1`, `v2` … SQL 파일 이름에서 온다 |
| window_start, window_end | DATE | 판정에 쓴 데이터 구간. 지금은 하루 |
| hit | BOOLEAN | 이 근거로 봇 쪽이냐 |
| score | DOUBLE | 비율 같은 연속값. 없으면 NULL |
| metrics | STRING | 판정에 쓴 값들 (JSON). 예: `{"stars":65,"burst":0.97}` |
| judged_at | TIMESTAMP | 판정한 시각 |
| sql_sha256 | STRING | 돌린 SQL의 해시 |
| run_id | STRING | |

- 파티션: `evidence_id`, `rule_version`, `window_end`
- 쓰기: 그 셋으로 정해지는 파티션만 덮어쓴다. 근거 작업끼리 서로의 파티션을 건드리지 않아 동시에 돌아도 된다.
- 규칙을 바꾸면 새 버전 행을 **추가**한다. 옛 버전은 남긴다(평가에서 버전끼리 비교).

**truth** (한 행 = 대상 × 정답 출처 × 확인)

| 컬럼 | 뜻 |
|---|---|
| entity_type, entity_key, entity_name | |
| truth_source | `repo_deleted`, `account_deleted`, `golzadeh` … |
| label | `deleted` / `alive`, 또는 `bot` / `human` |
| label_method | `http_head`, `manual_label` … |
| observed_at | 원천이 말하는 시각 (모르면 NULL) |
| known_at | **우리가 이 사실을 알게 된 시각** |
| raw_ref | truth_raw 파일 경로 |

- 추가만 한다. 같은 (truth_source, entity_key, known_at 날짜)는 MERGE로 하나만 남긴다.

**규칙 목록** `rules/registry.yml`

```yaml
declared_app:   {family: self_report,  current: v1, entity: gh_account}
always_on:      {family: behavior,     current: v1, entity: gh_account}
low_activity:   {family: coordination, current: v1, entity: [gh_account, gh_repo]}
lockstep_ratio: {family: coordination, current: v2, entity: gh_repo}
burst_5m:       {family: coordination, current: v1, entity: gh_repo}
```

- 근거 SQL 규칙의 첫 값 (사전 실험 그대로):

| evidence_id | 대상 | hit 조건 |
|---|---|---|
| declared_app v1 | 계정 | login이 `[bot]`으로 끝남 |
| always_on v1 | 계정 | 선언 봇 아님, `active_hours >= 20`, `n_events >= 100` |
| low_activity v1 | 계정 | `is_low_activity` |
| low_activity v1 | 저장소 | `stars >= 20`, `low_act_stars / stars >= 0.5` |
| lockstep_ratio v2 | 저장소 | `max_co_star / stars >= 0.5` |
| burst_5m v1 | 저장소 | `max_5m_stars / stars >= 0.5` |

- lockstep_ratio를 v2로 시작하는 이유: 사전 실험의 v1(절대 수)은 트렌딩 저장소 오탐으로 버렸다. 그 이력을 버전 번호에 남긴다.

### 4.3 Gold (로컬: `warehouse_local/gold.duckdb`와 dbt-duckdb, 이식 때: BigQuery와 dbt-bigquery)

**시드 (CSV, 분석팀이 SQL 없이 바꾸는 부분)**

- `policy_family.csv`: (policy, family). lenient = 자기 신고. balanced = 자기 신고 + 인프라 + 조직. strict = 전부.
- `truth_independence.csv`: (truth_source, family 또는 evidence_id, allowed). `criteria.md` 3절 표 그대로.
- `evidence_current.csv`: `registry.yml`에서 생성. 지금 쓰는 규칙 버전.

**모델**

| 모델 | 한 행 | 하는 일 |
|---|---|---|
| `stg_evidence` | evidence 한 행 | Silver evidence를 읽고 `evidence_current`의 버전만 남김 |
| `fake_star_repo` | 저장소 × 날짜 | low_activity(저장소) **그리고** (lockstep_ratio **또는** burst_5m). 사전 실험 v3와 같은 식 |
| `entity_verdict` | 대상 × 날짜 × 정책 | 범주(human, declared_bot, suspected_bot)와 걸린 근거 목록 |
| `real_user_events` | 이벤트 | 이벤트 + `is_real_lenient`, `is_real_balanced`, `is_real_strict` |
| `real_user_lenient` 등 3개 | | 위 표의 뷰. 분석팀이 정책 이름으로 고른다 |
| `evaluation` | 근거 × 버전 × 정답 출처 | 판정 수, 정답이 있는 수, 정밀도, 대조군 삭제율 |
| `impact` | 저장소 × 날짜 | 원래 star 수와 정책별 실유저 star 수, 인기 순위 변화 |
| `disagreement` | 대상 × 날짜 | 근거끼리 엇갈린 경우 (예: burst는 걸렸는데 lockstep은 아님) |

- **계정이 가짜 star 계정으로 판정되는 조건:**
  - 정책에 조직 계열이 들어 있고,
  - 그 계정이 그날 저활동이고,
  - 그날 `fake_star_repo`에 star를 눌렀다.
  - 이 조합 규칙은 정책 SQL에 있다. 근거 계산과 따로 바뀐다.
- **evaluation이 꼭 지켜야 하는 두 가지 (dbt 테스트로 강제):**
  1. `truth_independence`에서 금지된 (정답 출처, 근거) 쌍이 결과에 0건이다.
  2. 모든 행에서 `judged_at <= known_at`이다. 판정 뒤에 알게 된 정답만 쓴다.
- **재현율은 아직 계산하지 않는다.** 생존 확인은 의심 대상과 표본 대조군만 하기 때문이다. 정밀도와 대조군 삭제율을 나란히 보인다.
- 분석팀에 주는 표(`real_user_*`, `impact`)는 dbt 모델 계약(`contract: enforced`)을 건다. 컬럼 이름이나 형이 바뀌면 빌드가 실패한다. 서울 동네 찾기의 상품 계약(결정 83)을 dbt 기본 기능으로 대신한다.
- 빌드마다 `ops.build_record`에 한 줄: build_id, 시각, git 커밋, 근거별 규칙 버전, SQL 해시. 로컬에서는 같은 DuckDB 파일 안의 `ops` 스키마다.
- **Silver를 읽는 방법:** dbt-duckdb 소스의 `external_location`에 `iceberg_scan('warehouse_local/silver/<표>')`를 준다. Hadoop 카탈로그가 남기는 `version-hint.text`를 DuckDB가 읽어 최신 스냅샷을 연다.
- **이식 대비:** Gold SQL은 BigQuery로 옮겨야 한다. 방언 차이를 미리 줄이려고, 모델마다 sqlglot으로 duckdb → bigquery 변환이 오류 없이 되는지 시험 하나를 둔다 (L5).

---


## 5. 작업 명세 (로컬)

| 작업 | 실행 | 입력 | 출력 | 다시 돌리면 |
|---|---|---|---|---|
| `fetch_hour` | `python ingest.py fetch` | 시간 하나 | raw 파티션 + manifest | 완료된 파티션은 건너뜀 |
| `create_tables` | bot-job | 4.2절 계약 | 빈 Silver 표 | 있으면 그대로 |
| `load_events` | bot-job | 그날 raw 24개 | events 파티션 | 파티션 덮어쓰기 |
| `features_account`, `features_repo` | bot-job | events 그날 | entity_features 파티션 | 파티션 덮어쓰기 |
| `evidence_<id>` | bot-job | entity_features (+ events) | evidence 파티션 | 파티션 덮어쓰기 |
| `repo_alive` | bot-job | 판정 대상 저장소 목록 | `raw/truth_raw/` 파일 | 날짜별 새 파일 |
| `build_truth` | bot-job | truth_raw | truth (MERGE) | 같은 키는 하나만 |
| `dbt_build` | bot-job | Silver 표 | gold.duckdb | 전체 재생성 |
| `extract_lineage` | bot-job | SQL 파일, dbt 컴파일 SQL | lineage 표 | 회차마다 새 스냅샷 |

## 6. DAG (로컬, L6)

| DAG | 언제 | 태스크 |
|---|---|---|
| `ingest_gh` | `@daily`, 2024-07-15 ~ 07-21, catchup | `fetch_hour` × 24 (동시 4개) → Asset `raw/gharchive` 갱신 (extra에 날짜) |
| `bot_build` | Asset 갱신 시 | `create_tables` → `load_events` → `features_account` → `features_repo` → 근거 5개 (병렬) → `dbt_build` → `record_build` |
| `late_truth` | 주 1회 | `pick_targets` → `repo_alive` → `build_truth` → `dbt_build --select evaluation+` |
| `rule_backfill` | 수동 (evidence_id 또는 feature_version, 새 버전, 날짜 범위) | `lineage_impact` → 범위 안 작업만 → 범위 안 Gold만 `dbt_build` → `record_build` |
| `column_lineage` | `sql/`나 dbt 모델이 바뀐 뒤 수동 | `dbt compile` → `extract_lineage` |

- 모든 계산 태스크는 DockerOperator로 bot-job을 띄운다. 태스크를 만드는 함수 하나(`job(name, command)`)로 모은다. 이식 때 이 함수만 클러스터 연산자로 바꾼다.
- `pick_targets`: 판정 구간의 의심 저장소 전부 + 같은 날 star 20개 이상인 비의심 저장소에서 해시 순으로 같은 수만큼. 사전 실험과 같은 방식이다.

## 7. 측정

| 값 | 어디서 |
|---|---|
| 작업별 시작, 끝, 입력 행, 출력 행 | 실행기 출력, Airflow |
| 스테이지별 셔플 읽기·쓰기, 태스크 시간 (중앙값, 최댓값) | `measure/spark_eventlog.py`가 `spark-events/`에서 |
| 컨테이너 최대 메모리 | `docker stats` 기록 |
| 이식 때 바뀐 파일과 줄 수 | `git diff --stat` (9절) |

쏠림은 "태스크 시간 최댓값 / 중앙값"으로 본다. 로컬 `local[*]`에서도 셔플은 디스크를 거쳐 일어나고 쏠림은 태스크 시간에 보인다. 노드 사이 네트워크로 오가는 셔플은 이식 뒤(C2)에 처음 본다. 같은 스테이지를 로컬과 클러스터에서 나란히 적는 것이 분산의 이득과 비용을 보여 주는 재료다.

## 8. 단계와 체크포인트 (로컬)

### L3. 하루치 Silver와 근거

1. `create_tables`(4.2절 계약대로 빈 표 만들기)를 먼저 둔다.
2. 2024-07-15 하루치를 local 모드로 돈다: events → 계정 피처 → 저장소 피처 → 근거 5개.
3. 사전 실험과 대조한다.
   - events 행 수 = 4,914,751 (M-3)
   - `fake_star_repo` 식을 Spark SQL로 한 번 돌려 **276개**
   - `experiments/p0_bot_detection/results/gh_repo_flags.parquet`의 `suspect_v3`와 저장소 단위로 맞춰 보고, 다르면 차이 목록을 뽑는다.
4. Spark UI와 이벤트 로그로 `features_repo`(자기 조인) 스테이지의 태스크 시간 분포를 본다.
5. M-3에서 튄 03시 Release가 한 계정에서 나왔는지 events로 확인한다.

**[체크포인트]**
- 276개가 같은지. 다르면 시간 창 경계(1시간, 5분 RANGE)를 DuckDB와 Spark가 다르게 처리하는지부터 본다.
- 같은 날을 다시 돌려 표 **내용**이 같은지(행 수만이 아니라). `guarantees.md`에 적는다.
- 하루치 실행 시간과 최대 메모리. compose의 24GB가 맞는지.

### L4. 정답 경로

1. `truth/repo_alive.py`: 사전 실험 `03_validate_exists.py`를 옮긴다. 동시 4개, 실패만 재시도, 301(이름 바뀜)은 살아 있음.
2. `sql/truth/truth.sql`: truth_raw → truth 표 MERGE. `known_at`은 확인한 시각.
3. 대상은 `pick_targets` 기준. 2024-07-15 의심 저장소와 대조군.

**[체크포인트]**
- 같은 대상을 다시 확인하면 truth에 중복이 생기지 않는지 (MERGE 키).
- 사전 실험 결과 파일(`gh_validation_result.csv`, 2026-11-16 확인)과 오늘 확인 결과가 얼마나 다른지. 하루 사이에 바뀐 저장소가 있으면 사후 확정 정답이 실제로 움직인다는 첫 기록이다.

### L5. Gold (dbt-duckdb)

1. `warehouse/dbt/`: 프로필은 duckdb, 소스는 Silver 표를 `iceberg_scan`으로 읽는다 (4.3절).
2. 시드 3개, 4.3절 모델, 분석팀용 표에 모델 계약.
3. 시험:
   - 독립성 (금지 쌍 0건), 판정 시각 ≤ 확인 시각, 정책별 계정 수 합
   - **데이터 품질:** events의 이벤트 종류별 건수가 manifest 줄 수 합과 맞는지, 종류별 비율이 전날보다 크게 변했는지 (GH 설계 D10). 오류 없이 무너지는 원천을 잡는 시험이다.
   - **이식 대비:** 모델마다 sqlglot으로 duckdb → bigquery 변환이 오류 없이 되는지
4. `ops.build_record`에 빌드 한 줄.

**[체크포인트]**
- `evaluation`에서 `fake_star_repo` 삭제율과 대조군 삭제율. 사전 실험은 95.3% 대 3.6%였다. 다르면 차이를 기록한다.
- 독립성 시드에 금지 쌍을 일부러 하나 넣어 시험이 실패하는지.

### L6. 로컬 Airflow

1. 서울 동네 찾기 `docker-compose.yml`의 Airflow 부분을 가져와 bot-job 서비스와 합친다.
2. 6절 DAG 5개. 계산 태스크는 `job(name, command)` 함수 하나로 만든다.
3. 2024-07-15 하루를 DAG로 다시 돈다.

**[체크포인트]**
- DAG로 돈 결과가 L3~L5의 손 실행 결과와 같은지 (행 수, 276개, 채점 숫자).
- 근거 작업 5개를 병렬로 돌려도 표가 깨지지 않는지 (`create_tables`가 앞에 있어서).
- 태스크 하나를 일부러 실패시키고 다시 돌렸을 때 그 파티션만 다시 쓰이는지.

### L7. 일주일

1. `ingest_gh` catchup으로 07-15 ~ 07-21. 받는 시간을 다시 잰다 (M-1과 비교).
2. `measure/spark_eventlog.py`와 7절 측정.

**[체크포인트]**
- 날짜별 처리 시간, 셔플, 쏠림 지표, 최대 메모리 표.
- 생긴 오류를 `failure_analysis.md`에 원인까지 남겼는지.

### L8. 리니지와 규칙 변경 재처리

1. `lineage/` 복사. 입력은 `sql/`(spark 방언)과 dbt 컴파일 SQL(duckdb 방언).
2. `rule_backfill` DAG.
3. **재처리 두 번을 비교한다.**
   - 근거 규칙 변경: lockstep_ratio v2 → v3 (문턱값 0.5 → 0.4). 그 근거와 하위 Gold만 돈다.
   - 피처 변경: `features_repo`의 계정당 상한 50 → 200 (f1 → f2). 피처부터 다시 돈다. 쏠림이 커질 것으로 예상한다.

**[체크포인트]**
- 두 경우 각각 리니지가 찍은 범위, 원천 파티션으로 좁힌 범위, 실제로 다시 돈 작업과 시간 (`img/lineage.png` B 패널과 대조).
- 상한을 풀었을 때 쏠림이 어떻게 변했고 무엇으로 처방했는지 `failure_analysis.md`에.

### L9. 로컬 완성 점검

- [ ] L3~L8 체크포인트가 모두 통과했다
- [ ] 일주일치가 DAG 하나로 처음부터 끝까지 돈다 (손 실행 없음)
- [ ] 규칙 변경 재처리 기록이 두 건 있다
- [ ] 2절 "안 바뀌는 것" 목록의 파일에 로컬 전용 코드가 들어가 있지 않다
- [ ] README에 로컬 실행 방법이 있다 (처음 받는 사람이 그대로 따라 할 수 있게)

**[체크포인트]** 여기서 사용자와 "로컬 완성"을 선언하고 이식을 시작할지 정한다.

## 9. 클라우드 이식 (C1~C4)

엔진과 이유는 `engine_selection.md`를 따른다. 단계마다 **로컬과 같은 결과**가 나오는지 확인하고, 바뀐 파일과 줄 수를 남긴다.

| 단계 | 하는 일 | 같은지 볼 것 |
|---|---|---|
| C1 GCP 준비 | 버킷, Lakehouse 카탈로그, BigQuery 데이터셋, 서비스 계정 2개, 예산 알림과 결제 끊기, BigQuery 하루 한도. `bq show githubarchive:day`로 위치 확인 | 없음 (준비) |
| C2 저장과 Spark | `raw/` → GCS (업로드 조건으로 덮어쓰기 금지), 카탈로그 `rest` 모드, 3.0 이미지 클러스터(작업 때만, 유휴 삭제), bot-job 이미지에 GCS 모듈 | events 행 수, 276개, 같은 스테이지의 로컬 대 클러스터 시간 |
| C3 Gold | dbt 프로필 bigquery, 방언 차이 수정 (L5 sqlglot 시험이 미리 알려 준 곳) | 채점 숫자, 정책별 계정 수 |
| C4 지휘 | VM 위 Airflow, `job()` 함수를 클러스터 연산자로 | DAG 결과, 일주일 재실행 시간과 비용 |

**GCP 리소스 (C1)**

| 리소스 | 이름 | 위치 |
|---|---|---|
| 버킷 | `<PROJECT_ID>-bot-lake` (`bronze/`, `silver/`, `code/`, `spark-events/`) | us-central1 |
| Lakehouse 카탈로그 | `bot_lh`, 네임스페이스 `silver` | us-central1 |
| BigQuery 데이터셋 | `gold`, `ops`, `lineage` | US |
| Spark 클러스터 | 3.0 이미지, 마스터 e2-standard-2 1대 + 워커 2대, 유휴 15분 삭제, 최대 3시간 | us-central1 |
| Airflow VM | e2-standard-2, 밤에 멈춤 | us-central1-a |
| 서비스 계정 | `sa-airflow`, `sa-spark`, 키 파일 없음 | |

리전은 GH Archive 위치 확인 뒤 확정한다 (engine_selection.md E2).

**이식의 체크포인트**
- C2~C4마다 로컬 결과와 표로 대조하고, 다르면 SQL이 아니라 환경 차이부터 의심한다 (L3에서 SQL을 확정했으므로).
- 이식 전체에서 바뀐 파일이 2절 "바뀌는 곳" 안에만 있는지. 밖에서 바뀌었으면 그 이유를 `decisions.md`에 적는다.

## 10. 서울 동네 찾기에서 가져오는 것

| 가져올 것 | 서울 동네 찾기 위치 | 언제 | 바꿀 것 |
|---|---|---|---|
| Airflow 구성 | `docker-compose.yml`, `Dockerfile` | L6 | 계산 컨테이너를 bot-job 하나로 |
| 리니지 추출기 | `lineage/` | L8 | 방언 입력 (spark, duckdb, 이식 뒤 bigquery) |
| dbt 프로젝트 구조 | `warehouse/dbt/` | L5 | 어댑터 postgres → duckdb |
| 기록 문서 형식 | `decisions.md` 등 4개 | 이미 씀 | 그대로 |

## 11. 확인 필요

| 항목 | 언제 | 안 되면 |
|---|---|---|
| DuckDB가 Spark가 쓴 Hadoop 카탈로그 표를 `iceberg_scan`으로 읽는지 (version-hint.text) | L5 시작 | `version`을 직접 주거나, Silver를 Parquet으로 한 번 내보내 읽기 |
| dbt-duckdb 소스의 `external_location`으로 `iceberg_scan`을 쓰는지 | L5 | 소스 대신 스테이징 모델 안에서 `iceberg_scan` 직접 호출 |
| Windows Docker Desktop에서 Airflow의 DockerOperator가 도는지 | L6 | 서울 동네 찾기에서 이미 돈 방법(도커 소켓 연결)을 그대로 |
| 일주일치를 로컬 Spark 24GB로 돌릴 수 있는지 | L7 | 날짜별로 나눠 돌리기 (이미 날짜 파티션 단위) |
| 분석 기간: 2024-07 주 대 2025-04 (GH 설계 D13) | L7 전 | 2024-07로 로컬 완성, 기간 변경은 이식 뒤 |

## 12. 나중에 (이 문서 범위 밖)

- **분석가 쪽으로 넓히기:** 지표 정의 층(같은 지표를 한 곳에서 계산), 정책별 손익(사람을 잘못 거름 대 봇을 놓침), 사람 검토 흐름. 데이터 엔지니어 쪽을 끝낸 뒤 범위를 다시 정한다.
- **웹 확장안:** 가상 서비스, 실시간 수집 API, 심은 봇으로 만든 정답. 공유 문서 "확장안" 절과 직무 경계 그림 참고.
- **k8s:** 본체를 끝낸 뒤 (GH 설계 2026-11-12 결정).
