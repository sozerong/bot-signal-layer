# 오류 분석 기록

구현과 측정 중 실제로 만난 오류를 원인까지 남긴다. 형식은 서울 동네 찾기 `docs/failure_analysis.md`와 같다. 원인이 자명하고 배울 것이 없는 일, 실행하는 PC 환경 탓인 일은 넣지 않는다.

분류: `메모리` / `분포` / `저장` / `연결` / `타입·스키마` / `정확성` / `운영` / `계산량·인덱스` / `설계 가정`

## F-1. Iceberg SQL 확장을 켜면 이름 붙은 파라미터가 묶이지 않는다

- 태그: `타입·스키마`, `설계 가정`
- 언제·어디: 2026-11-17, implementation_design.md 8절 L1-4, `docker compose run --rm bot-job pytest`
- 증상: `spark.sql("... :run_id AS run_id ...", args={"run_id": "test"})`가 `[UNBOUND_SQL_PARAMETER] Found the unbound parameter: run_id`로 실패. 실행 계획에 `namedparameter(run_id)`가 치환되지 않은 채 남는다.
- 재현: bot-job 이미지(PySpark 4.1.2, iceberg-spark-runtime-4.1_2.13 1.11.0). 확장을 켠 세션에서 `SELECT :x AS a`만으로 매번 실패. 확장 없는 세션에서는 같은 문장이 매번 성공.
- 가설:
  - H1 실행기가 파라미터를 안 넘겼다 → 실행기는 SQL에서 `:이름`을 찾아 빠진 것이 있으면 먼저 멈춘다. 멈추지 않았으니 넘겼다 → 지움
  - H2 SQL 파일의 한글 주석이나 `]` 같은 문자가 파서를 흔든다 → 같은 파일을 확장 없는 세션에서 돌리면 성공 → 지움
  - H3 Iceberg 확장의 파서가 파라미터를 놓친다 → 확장을 켠 세션에서 가장 단순한 `SELECT :x`도 실패 → 남김
- 원인: 확장을 켜면 Spark의 SQL 파서 앞에 Iceberg의 파서가 끼어든다. 이 조합에서 파라미터가 파서 단계에서 묶이지 않고 분석 단계까지 남는다. 파서 내부의 어느 메서드가 빠졌는지는 코드로 확인하지 않았다 `[확인 필요]`.
- 처방 선택지:
  | 선택지 | 장점 | 단점 |
  |---|---|---|
  | 확장을 끈다 | SQL과 실행이 같은 문장. Spark 4.1이 MERGE, 메타데이터 표, CALL을 자체로 지원해서 잃는 것이 적다 | Iceberg 전용 DDL을 못 쓴다 |
  | 확장을 켜고 문자열 치환 | Iceberg 전용 DDL을 쓸 수 있다 | 따옴표·주입 처리를 실행기가 떠안는다. 리니지가 보는 SQL과 실제 SQL이 다르다 |
- 고른 것: 확장을 끈다 (decisions.md 결정 4). 확장 없이 쓸 기능이 모두 도는 것을 컨테이너에서 확인한 뒤 시험 5개 통과.

---

> **2026-11-17 사용자 지시:** 지시서 작업을 끝까지 돌리고, 오류가 나면 고치지 말고 여기에 기록만 한다. 아래 F-2부터는 **미해결**이다. 막힌 작업은 각 항목의 "막힌 것"에 적었고, 막히지 않은 작업은 계속했다.

## F-2. Spark 4.1 이벤트 로그를 측정 스크립트가 읽지 못한다 [해결]

- 태그: `운영`, `설계 가정`
- 언제·어디: 2026-11-17, 8절 L3-4, `python -m measure.spark_eventlog --app features_repo` (호스트 Python 3.12)
- 증상: "맞는 이벤트 로그가 없다"로 끝난다. `spark-events/` 아래에 이벤트 로그가 8개(작업마다 하나) 있는데도.
- 관찰:
  - Spark 4.1.2가 남긴 것은 파일이 아니라 폴더다: `spark-events/eventlog_v2_local-<번호>/` 안에 `events_1_local-<번호>.zstd`와 `appstatus_local-<번호>.inprogress`.
  - `events_1_*.zstd`는 zstd로 압축돼 있다 (첫 줄만 평문 `{"Event":"SparkListenerLogStart","Spark Version":"4.1.2"}`, 나머지는 압축).
  - 세션 설정에서 `spark.eventLog.rolling.enabled`를 주지 않았다 (기본값).
  - 작업이 끝났는데도 상태 파일 이름이 `.inprogress`로 남아 있다.
- 가설 (확인 안 함):
  - H1 스크립트는 "압축 안 된 JSON 파일 하나"(Spark 3.x 기본)를 가정했다. Spark 4.x는 기본으로 이벤트 로그를 굴리는 폴더(v2)에 쓰고 zstd로 압축한다 [확인 필요: 4.0 이전 기본값과 바뀐 시점].
  - H2 `.inprogress`가 남는 것은 `run_sql.py`, `load_events.py`가 끝에서 `spark.stop()`을 부르지 않아서일 수 있다. 그러면 이벤트 로그 마지막(SparkListenerApplicationEnd)이 빠졌을 수 있다.
  - H3 Python 3.12 표준 라이브러리에는 zstd가 없다 (3.14의 `compression.zstd`부터). 표준 라이브러리만 쓰겠다는 스크립트 전제와 부딪힌다.
- 막힌 것: L3-4(features_repo 자기 조인 스테이지의 태스크 시간 분포), L7-2(측정 표의 셔플, 쏠림 열).
- 처방 후보 (고르지 않음): 압축을 끄는 설정, zstd 라이브러리 추가, Spark 히스토리 서버로 읽기.
- **해결 (지시서 H1-1, 2026-11-17):** H1과 H2가 원인이었다. Spark 4.0 이전 안내대로 `spark.eventLog.rolling.enabled=false`, `spark.eventLog.compress=false`를 `run_sql.event_log_conf()`에 더했고, `run_sql.py`, `load_events.py` 끝에서 `spark.stop()`을 부른다. 다시 돌리자 `spark-events/local-<번호>` 파일 하나(평문 JSON, 6.1MB)가 `.inprogress` 없이 남고, 측정 스크립트가 스테이지 분포를 냈다 (measurements.md M-12). H3(zstd)은 압축을 꺼서 피했다. 클러스터(C2)에도 같은 두 속성을 주면 같은 스크립트로 읽는다.

## F-3. 표 내용 지문이 long 넘침으로 실패한다 [해결]

- 태그: `타입·스키마`, `정확성`
- 언제·어디: 2026-11-17, 8절 L3 체크포인트(같은 날을 다시 돌려 표 내용이 같은지), `python -m measure.table_digest --day 2024-07-15` (bot-job)
- 증상: `pyspark.errors.exceptions.captured.ArithmeticException: [ARITHMETIC_OVERFLOW] long overflow. Use 'try_add' to tolerate overflow and return NULL instead.`
- 관찰: 지문 식은 `sum(xxhash64(모든 컬럼))`이다. xxhash64는 64비트 정수 전체 범위의 값을 내므로, 491만 행을 더하면 long 범위를 넘는다.
- 가설 (확인 안 함): Spark 4.x는 ANSI 모드가 기본으로 켜져 있어(`spark.sql.ansi.enabled`) 정수 넘침을 조용히 감싸지 않고 오류로 낸다. Spark 3.x 기본(ANSI 꺼짐)이라면 넘침이 감싸져 지나갔을 것이다 [확인 필요].
- 막힌 것: L3 체크포인트의 "표 **내용**이 같은지" 시험. 행 수 비교만 했다 (guarantees.md). L6 체크포인트의 DAG 결과 대 손 실행 내용 비교도 같은 스크립트를 쓰려던 것이라 함께 막힌다.
- 처방 후보 (고르지 않음): 넘침을 감싸는 합, 해시를 작은 범위로 줄이기, 정렬한 행끼리 차집합(`EXCEPT ALL`) 비교.
- **해결 (지시서 H1-2, 2026-11-17):** 해시를 `DECIMAL(38,0)`로 바꿔 더한다 (491만 x 9.2e18 = 4.5e25 < 1e38). `bit_xor`는 같은 행 두 개가 서로 지워 중복 버그를 못 잡아 쓰지 않았다. `--snapshots A B`로 Iceberg 스냅샷 두 개의 그날 지문을 비교하고, 다르면 `EXCEPT ALL` 양쪽 차집합을 센다. 같은 날을 다시 돌린 앞뒤 스냅샷의 지문이 세 표 모두 같았다 (M-12). ANSI 기본값 가설은 확인하지 않았다.

## F-4. 데이터 품질 시험 events_match_manifest가 날짜 변환에서 실패한다 [해결]

- 태그: `타입·스키마`, `설계 가정`
- 언제·어디: 2026-11-17, 8절 L5-3, `python -m warehouse.dbt_build` (bot-job, dbt-duckdb 1.11.1, DuckDB 2.0.0)
- 증상: `Runtime Error in test events_match_manifest ... Conversion Error: invalid date field format: "10", expected format is (YYYY-MM-DD)`. 나머지 22개(모델 17, 시험 5)는 통과.
- 관찰 (bot-job에서 DuckDB로 직접 읽어 봄):
  - `read_json_auto('/work/raw/dt=*/hour=*/_manifest.json')`의 컬럼: `hour, source_url, status, file, bytes, sha256, lines, attempts, fetched_at, dt`. 파일에 없는 `dt`가 생겼다.
  - `hour` 값이 manifest 안의 `"2024-07-15T03"`이 아니라 `'00'`, `'01'` …이다. 폴더 이름 `hour=00`의 값이다.
- 원인 (관찰에서 거의 확실): DuckDB가 경로의 `키=값` 폴더를 하이브 파티션으로 읽어 컬럼으로 붙이고(`hive_partitioning` 자동 감지), 같은 이름의 JSON 필드(`hour`)를 폴더 값이 덮는다. 시험은 `left(hour, 10)`을 날짜로 바꾸므로 `'10'` 같은 값에서 실패한다.
- 막힌 것: L5-3 데이터 품질 시험 중 "events 행 수 = manifest 줄 수 합". 이 시험이 실패해서 `dbt build`가 0이 아닌 코드로 끝난다. **그래서 Airflow `bot_build` DAG의 `dbt_build` 태스크도 실패할 것이다 (L6).**
- 처방 후보 (고르지 않음): 하이브 파티션 읽기를 끄기, `dt` 컬럼을 날짜로 쓰기, manifest의 필드 이름을 폴더 키와 다르게.
- **해결 (지시서 H2-3, 2026-11-17):** dbt 소스를 `read_json_auto(..., hive_partitioning = false)`로 바꿨다 (`warehouse/dbt/models/sources.yml`). 원본 manifest는 그대로다. `dbt build` 전체가 0으로 끝났고(PASS 22, WARN 1은 설계상 경고인 type_share_shift), 7일 모두 events 행 수 = manifest 줄 수 합으로 통과했다. 거꾸로 확인: raw는 읽기만 하므로 manifest를 스크래치에 복사해 07-16 03시 줄 수만 하나 늘리고 같은 시험 SQL을 돌리자 `(2024-07-16, 4870089, 4870090)` 한 행이 나왔다 (실패). 리니지 추출기가 이 소스 이름을 정규식으로 바꾸던 곳도 함께 고쳤다 (F-9 해결 참고).

## F-5. Gold 모델 하나가 BigQuery 방언으로 바뀌지 않는다 [미해결]

- 태그: `타입·스키마`, `설계 가정`
- 언제·어디: 2026-11-17, 8절 L5-3 이식 대비 시험, `pytest tests/test_gold_dialect.py` (sqlglot 30.32.0)
- 증상: 15개 중 14개 통과, `entity_verdict` 실패. `sqlglot.errors.UnsupportedError: 'NULLS LAST' translation not supported for aggregate function GROUP_CONCAT with sort order`
- 관찰: `entity_verdict.sql`의 `string_agg(distinct e.evidence_id, ',' order by e.evidence_id)`. DuckDB는 정렬의 NULL 위치 기본값이 NULLS LAST다.
- 가설 (확인 안 함): sqlglot이 DuckDB의 기본 NULL 순서를 명시적으로 옮기려 하는데, BigQuery `STRING_AGG ... ORDER BY` 안에서는 NULL 순서를 줄 수 없다고 보고 거부한다. 걸린 근거 목록은 NULL이 없으므로 결과에는 영향이 없을 수 있다 [확인 필요].
- 막힌 것: 없음 (로컬 Gold는 정상). C3 이식 때 고칠 곳으로 미리 드러났다. 이 시험의 목적대로다.
- 처방 후보 (고르지 않음): 정렬 없이 모으기, DISTINCT와 정렬을 하위 쿼리로 빼기, 이 모델만 BigQuery용 매크로.

## F-6. DuckDB 2.0에서 관계 객체의 fetchone()이 없어졌다 (참고)

- 태그: `운영`
- 언제·어디: 2026-11-17, L5 결과를 손으로 조회하던 한 줄짜리 명령 (bot-job, DuckDB 2.0.0)
- 증상: `AttributeError: Relations no longer have fetchone(): a relation keeps no result between calls, so every call runs it again. Use rel.limit(1).fetchall() instead.`
- 원인: 오류 메시지가 말하는 대로 DuckDB 2.0의 API 변경. `con.sql(...)`이 돌려주는 관계 객체는 결과를 들고 있지 않는다.
- 처리: 이 조회 명령은 지시서 작업이 아니라 손 조회였다. 같은 호출을 쓰던 `warehouse/record_build.py`는 **아직 한 번도 돌기 전**이어서 그 자리에서 `fetchall()[0]`으로 바꿔 썼다 (오류를 만나고 고친 것이 아니라 처음 쓸 때 맞춘 것). 서울 동네 찾기는 DuckDB 2.0.0 이미지를 쓰므로 같은 호출이 있는지 볼 만하다 [확인 필요].

## F-7. 근거 다섯 개를 병렬로 쓰면 한 커밋이 조용히 사라진다 [해결]

- 태그: `동시성`, `정확성`, `설계 가정`
- 언제·어디: 2026-11-17 08:30Z, 8절 L6 체크포인트 "근거 작업 5개를 병렬로 돌려도 표가 깨지지 않는지", `bot_build` 실행 `manual_l6_2`. 다섯 태스크가 08:30:03에 함께 시작했다 (burst_5m은 일부러 죽임).
- 증상: `evidence_declared_app` 태스크는 success인데 evidence 표의 declared_app 파티션은 앞 실행 `manual_l6_1`의 행 그대로다. 그 태스크 로그:
  - `WARN SnapshotProducer: Failed to load committed snapshot, skipping manifest clean-up`
  - `WARN MergingSnapshotProducer: Failed to load committed snapshot: omitting sequence number from notifications`
  - 실행기가 찍은 행 수가 `762394행`이다. declared_app의 행 수(660,123)가 아니라 같은 시각에 커밋한 low_activity의 행 수다.
- 관찰:
  - 직전 실행 `manual_l6_1`에서는 다섯 개 모두 반영됐다 (run_id가 모두 새 것). 매번 나는 것이 아니다.
  - `warehouse_local/silver/evidence/metadata/`: `manual_l6_2`의 성공한 근거는 네 개(always_on, lockstep_ratio, low_activity, declared_app)인데 새 메타데이터 버전은 v17(lockstep 102,271행), v18(660,123행), v19(low_activity 762,394행) 세 개뿐이다. 커밋 하나가 다른 커밋에 덮였다.
- 가설 (확인 안 함): 결정 4의 Hadoop 카탈로그는 "새 버전 메타데이터 파일 이름 바꾸기가 대상이 이미 있으면 실패한다"는 파일 시스템 성질에 기대어 동시 커밋을 막는다. 이 표는 Windows 폴더를 Docker Desktop이 컨테이너에 붙인 곳(`/work`)에 있다. 그 붙인 파일 시스템에서 이름 바꾸기가 대상을 덮어써 버리면, 두 작업이 같은 v18을 만들고 둘 다 성공으로 안다 [확인 필요]. 실행기가 행 수를 "가장 최근 스냅샷"에서 읽는 것도 동시 커밋에서는 남의 스냅샷을 읽는다(부차 문제).
- 막힌 것: L6 체크포인트 "병렬 근거로 표가 깨지지 않는다"는 **통과하지 못했다.** L7 일주일 실행도 같은 DAG이므로 근거 파티션 일부가 조용히 빠질 수 있다. L7에서는 날마다 근거별 run_id를 확인해 빠진 것을 적는다.
- 처방 후보 (고르지 않음): Airflow 풀로 근거 태스크를 하나씩, 동시 커밋을 막는 카탈로그(JDBC/REST)로 바꾸기, 표를 컨테이너 안 파일 시스템에 두기, 실행기가 쓴 뒤 자기 run_id가 그 파티션에 있는지 확인하고 없으면 실패하기. 클라우드(C2)의 Lakehouse 카탈로그(`rest`)는 서버가 커밋을 하나씩 받으므로 이 문제가 없을 것이다 [확인 필요].
- **원인 실험 (지시서 H2-1, 2026-11-17, `measure/commit_race.py`):** 프로세스(JVM) 5개가 작은 표에 20번씩 append해 100 커밋, 3번씩.

  | 표 위치 | 조용한 손실 (100 커밋당) | 시끄러운 실패 (재시도 4번 소진) | 충돌 감지 ("Version N already exists") |
  |---|---|---|---|
  | Windows 폴더 붙임 (`/work`와 같은 방식) | 6, 10, 9 | 3, 1, 1 | 154, 172, 154 |
  | Docker 볼륨 (리눅스 파일 시스템) | 0, 0, 1 | 0, 3, 1 | 120, 148, 130 |

  - 가설은 반만 맞았다. 폴더 붙임에서 손실이 10배쯤 많고, `version-hint.text` 읽기 오류(Input/output error, 파일 없음)도 폴더 붙임에서만 났다. 그러나 볼륨에서도 300 커밋 중 1개가 사라졌다. Hadoop 카탈로그의 "있나 보고 이름 바꾸기"가 프로세스 사이에서 원자적이지 않다고 본다 [확인 필요: Iceberg 코드에서 확인 안 함].
  - 커밋 직후 `REFRESH TABLE` 뒤 자기 스냅샷(스냅샷 속성 `bot.write-id`)을 찾는 확인은 조용한 손실 26개를 모두 잡았다 (손실 수 = 확인 실패 수, 6번 모두).
- **해결 (결정 10):** A + B. `run_sql.run`이 쓴 뒤 자기 스냅샷을 확인하고 없으면 실패(근거 태스크 `retries=1`), Silver에 쓰는 태스크를 Airflow 풀 `silver_write`(자리 1개)로 줄 세움.
  - 확인 1: `bot_build`를 2024-07-15로 세 번(`h2_f7_1~3`). 세 번 모두 성공(dbt_build 포함). 근거 커밋 15개 모두 자기 write-id 스냅샷이 있고, 실행마다 끝난 시점에 근거 다섯 파티션의 run_id가 모두 그 실행 것이다.
  - 확인 2: 확인 질의의 id만 틀리게 바꿔 돌리자 `커밋이 표에 없다 ... (F-7)`로 exit 1. 이때 쓰기 자체는 들어갔다 (그 뒤 스냅샷의 burst_5m run_id가 `neg-verify`). 실제 손실이면 들어가지 않는다.
  - 부차 문제(행 수를 "가장 최근 스냅샷"에서 읽음)도 함께 없어졌다. 이제 자기 write-id 스냅샷에서 읽는다.

## F-8. 근거를 다시 돌린 날에는 채점이 빈다 [해결, 결정 11]

- 태그: `설계 가정`
- 언제·어디: 2026-11-17, L6 `manual_l6_1` 뒤 Gold `evaluation` 0행, `eval_pairs` 0행
- 원인: 판정 뒤에 알게 된 정답만 쓴다(`known_at >= judged_at`). DAG가 근거를 08:26에 다시 판정했고, truth는 그보다 앞선 08:10Z대에 확인한 것뿐이다. late_truth로 다시 확인해도 truth는 "하루에 저장소당 첫 확인 하나"(결정 7, `sql/truth/truth.sql`)라 같은 날 확인은 새 행이 되지 않는다 (+0). 다음 날 확인부터 다시 채점된다.
- 의미: 오류는 아니다. 다만 "규칙을 바꾸고 다시 돌리면 그날은 채점 숫자를 볼 수 없다"는 운영 제약이다. L8 재처리(새 버전 판정)도 같은 날에는 채점되지 않는다. 같은 날 채점이 필요하면 "판정 시각"을 실행 시각이 아니라 판정 구간 끝으로 두는 방법이 있지만, 그러면 독립성의 시간 조건이 약해진다 [다음 결정 후보].
- 막힌 것: L6 체크포인트의 "채점 숫자 같은지"는 오늘은 볼 수 없다. 내일 이후 late_truth를 다시 돌리면 볼 수 있다.
- **처방 (지시서 H4-1, 결정 11, 2026-11-17):** 같은 규칙(SQL 해시)으로 같은 hit를 낸 행은 옛 판정 시각을 이어받는다.
  - 확인: burst_5m v1을 07-15로 같은 SQL로 다시 돌리자 102,271행 모두 run_id는 새것, judged_at은 그대로(12:32:42). 주석 한 줄만 더한 사본(해시가 다름)으로 돌리자 102,271행 모두 새 judged_at.
  - **아직 못 본 것:** 지시서 체크포인트의 "같은 규칙으로 다시 돌린 날에도 evaluation이 94.9% 대 4.0%". 오늘의 판정 시각이 모두 정답 확인(08:10 ~ 08:12Z)보다 늦다. 이 처방 전에 L6 ~ L8과 H1 ~ H3에서 근거를 이미 다시 돌렸고, 결정 9로 해시도 바뀌었기 때문이다. truth는 하루에 대상마다 첫 확인 하나라 오늘 다시 확인해도 +0이다. **2026-11-18 이후 late_truth를 한 번 돌린 뒤, 근거를 같은 규칙으로 다시 돌려도 evaluation이 그대로인지 본다.**
  - **확인 (2026-11-18):** late_truth(07-15) 뒤 07-15 근거 여섯 개를 같은 규칙으로 다시 돌리고 Gold를 다시 만들었다. evaluation, truth 집계, eval_pairs가 앞뒤로 같다 (measurements.md M-16). 숫자가 94.9% 대 4.0%가 아닌 것은 판정이 7일치로 늘어서다(M-16에 이유).

## F-9. 리니지 추출기가 dbt 시드에서 멈춘다 [해결]

- 태그: `설계 가정`
- 언제·어디: 2026-11-17, 8절 L8-1, `lineage/extract.py`의 `build_schema` (bot-job, dbt-core 1.12.6). 처음 돌린 것이다 (DB에 쓰지 않는 시험 실행, 스크립트는 `build_schema`와 `extract_edges`만 부름).
- 증상:
  ```
  File "/work/lineage/extract.py", line 77, in build_schema
    order = TopologicalSorter({k: [d for d in n["depends_on"]["nodes"] if d in nodes] for k, n in nodes.items()})
  KeyError: 'nodes'
  ```
- 관찰: `warehouse/dbt/target/manifest.json`에서 모델 노드의 `depends_on`은 `macros`, `nodes` 두 키를 갖지만, 시드 노드 세 개는 `{'macros': []}`뿐이다. 추출기는 모델과 시드를 같은 사전에 넣고 둘 다 `depends_on["nodes"]`를 읽는다.
- 원인 (관찰에서 거의 확실): 서울 동네 찾기 `lineage/schema.py` 43행에서 같은 줄을 가져오면서 시드도 의존 순서에 넣었다. 그쪽은 이 줄에 시드가 들어가지 않는 것으로 보인다 [확인 필요].
- 막힌 것: L8-1 리니지 지도(`lineage.run`, `lineage.edge`가 없다). 그래서 `lineage/impact.py`(재처리 범위), `rule_backfill` DAG의 `lineage_impact` 태스크, `column_lineage` DAG가 모두 막힌다. L8 체크포인트의 "리니지가 찍은 범위"를 잴 수 없다.
- 막히지 않은 것: 재처리 자체. 범위를 손으로 정해(근거 규칙은 그 근거 SQL과 Gold 전체, 피처는 피처 → 저장소 근거 세 개 → Gold 전체) 같은 작업을 돌리고 시간과 쏠림을 잰다 (measurements.md M-11).
- 처방 후보 (고르지 않음): `n["depends_on"].get("nodes", [])`, 시드를 의존 순서 밖에서 먼저 넣기.
- **해결 (지시서 H3-1, 2026-11-17):** `n["depends_on"].get("nodes", [])`. `column_lineage` DAG가 성공했다 (SQL 파일 27개, 간선 1,250개, ok 아님 0, measurements.md M-14). `rule_backfill`이 리니지 범위로 세 번 돌았다. 서울 동네 찾기 `lineage/schema.py` 41~43행은 `resource_type == "model"`만 넣어서 이 키를 밟지 않는다 (가설 확인, 그쪽은 고칠 것 없음).
  - 같이 고친 것: F-4 처방으로 소스가 `read_json_auto(..., hive_partitioning = false)`가 되자 소스를 물리 이름으로 바꾸던 정규식이 맞지 않게 됐다. 정규식을 버리고 manifest의 소스 `relation_name`을 그대로 찾아 바꾸게 했고, Gold 방언도 manifest의 `adapter_type`에서 읽는다 (클라우드에서 이 파일을 고치지 않으려고, implementation_design.md 2절).

## F-10. 범위를 좁힌 Gold 빌드가 규칙 버전 시드를 다시 읽지 않는다 [해결]

- 태그: `정확성`, `설계 가정`
- 언제·어디: 2026-11-17 08:58Z, 8절 L8-3 근거 규칙 재처리(lockstep_ratio v2 → v3). `rules/registry.yml` current를 v3으로 바꾸고 v3 SQL을 7일치 돈 뒤 `python -m warehouse.dbt_build --select stg_evidence+`.
- 증상: 오류 없이 끝났다 (PASS 16). 그런데 Gold 결과가 v2 때와 한 행도 다르지 않았다 (가짜 star 저장소 2,793개, 실유저 이벤트 수까지 같음). Silver에서 직접 세면 v3에서 새로 걸린 저장소 135개 중 19개는 저활동이면서 burst에 안 걸린 것이라 가짜 star 저장소가 19개 늘어야 한다.
- 관찰:
  - Gold의 `main.evidence_current` 표는 lockstep_ratio `v2` 그대로였다. 시드 파일(`seeds/evidence_current.csv`)은 `dbt_build.py`가 v3으로 새로 썼다.
  - `ops.build_record`의 그 빌드 행(`7cad44da…`)은 `rule_versions`에 lockstep_ratio `v3`을 적었다. **기록은 v3, 실제 Gold는 v2**다. `record_build.py`가 DB 표가 아니라 시드 파일을 읽기 때문이다.
  - 같은 명령에 선택만 `--select evidence_current+`로 바꾸자 시드가 다시 실렸고(PASS 17) 2,793 → 2,812(+19)로 바뀌었다 (measurements.md M-11).
- 원인 (관찰에서 거의 확실): dbt의 `+` 선택은 아래쪽만 고른다. 시드 `evidence_current`는 `stg_evidence`의 위쪽이라 선택에서 빠지고, 빌드는 DB에 이미 있는 옛 시드 표를 읽는다. 규칙 버전 변경은 SQL 파일이 아니라 registry(→ 시드)를 바꾸는 일인데, 범위를 정하는 쪽(손으로 준 선택, 그리고 `lineage/impact.py`의 Gold 모델 목록)이 시드를 모른다.
- 막힌 것: 없음. M-11의 v3 숫자는 `--select evidence_current+`로 다시 잰 것이다. 코드는 고치지 않았다. `rule_backfill` DAG의 `dbt_build`도 같은 이유로 시드를 빠뜨릴 것이다 (F-9로 아직 돌지 못함).
- 처방 후보 (고르지 않음): 재처리 빌드는 항상 시드를 함께 고른다(`evidence_current+`), `record_build`가 시드 파일 대신 Gold의 `evidence_current` 표를 읽는다, `dbt_build.py`가 시드를 먼저 `dbt seed`로 싣는다.
- **해결 (지시서 H2-2, 2026-11-17):** 둘 다 했다.
  1. `warehouse/dbt_build.py`가 `--select`가 있으면 `evidence_current+`를 늘 더한다. `dbt seed`만 먼저 하는 길은 고르지 않았다: 시드는 새로 실려도 선택 밖의 아래 모델이 옛 버전으로 남을 수 있다.
  2. `warehouse/record_build.py`가 시드 파일이 아니라 Gold의 `main.evidence_current` 표를 읽어 적는다.
  - 확인: M-11 A를 `--select stg_evidence+`로 다시 했다. registry v3에서 가짜 star 저장소 2,793 → 2,812, Gold 시드 v3, `ops.build_record` v3. v2로 되돌리자 2,793, 시드 v2, 기록 v2 (26초).

## F-11. 피처 상한을 풀면 쏠림이 커지는데 시간은 그대로다 (참고, 지시서 L8 체크포인트 요구)

- 태그: `성능`
- 언제·어디: 2026-11-17, 8절 L8-3 피처 변경 f1 → f2 (`sql/silver/features_repo_f2.sql`, 계정당 star 상한 50 → 200), 7일치
- 관찰 (measurements.md M-11 B): 자기 조인에 들어가는 쌍 후보가 2 ~ 8배, 한 계정이 만드는 쌍 후보 최댓값이 8 ~ 16배(1,225 → 최대 19,701)가 됐다. 데이터 쏠림은 예상대로 커졌다. 그런데 features_repo 시간은 f1과 f2가 같다 (15.5 ~ 18.8초).
- 해석: 하루 쌍 후보가 많아야 330만이라 로컬 한 대에서는 Spark 시작과 컨테이너 띄우기가 시간을 정한다. 쏠림이 시간으로 드러나려면 데이터가 더 크거나, 셔플이 노드 사이를 오가야 한다(C2 클러스터).
- 처방: 하지 않았다. 시간이 늘지 않아 처방할 증상이 없다. 판정 결과도 바뀌지 않았다(burst_5m이 이미 잡음). 그래서 f1으로 되돌렸다 (결정 9의 4).
- 보지 못한 것: 스테이지 안 태스크 시간의 최대/중앙값(이 문서 7절의 쏠림 지표). 이벤트 로그 파서가 F-2로 막혀 있다. C2에서 같은 f2를 클러스터로 돌려 비교할 거리로 남긴다.
- **보충 (지시서 H3, F-2 해결 뒤):** 태스크 분포를 쟀다 (measurements.md M-14). f2에서 가장 긴 스테이지의 태스크가 중앙값까지 함께 1.1 ~ 2.5배 길어졌고, 태스크 사이 최대/중앙은 1.2 ~ 2.5로 f1(1.2 ~ 1.7)과 크게 다르지 않다. 데이터의 쏠림(한 계정의 쌍 후보 8 ~ 16배)이 태스크 쏠림으로 바뀌지 않았다. 그 스테이지도 3초 아래라 작업 시간에 안 보인다. C2 클러스터에서 같은 비교를 할 거리로 남긴다.

## F-12. 근거 태스크가 컨테이너 메모리 4g를 넘어 죽는다 [미해결]

- 태그: `운영`, `설계 가정`
- 언제·어디: 2026-11-17 13:11 ~ 13:13Z, 지시서 H6 점검으로 `bot_build`를 7일(`h6_week`, conf days 7개) 한 번에 돌림. 이 세션에서 근거 태스크에 두 가지를 더한 뒤 처음 돈 일주일이다: 결정 11의 옛 파티션 맞추기(조인), 결정 12의 새 근거 `multi_repo_burst`(events를 읽음).
- 증상:
  - `evidence_multi_repo_burst`: 첫 날(07-15)에서 죽음, 재시도도 같은 곳에서 죽음 → failed.
  - `evidence_low_activity`: 1회차가 4번째 날(07-18)에서 죽음, 재시도(7일 모두)는 성공.
  - 그래서 `dbt_build`, `record_build`가 upstream_failed, 실행이 failed. 나머지 근거 네 개와 Silver 전부는 성공.
  - 로그: `overwritePartitions` 중 `py4j.protocol.Py4JNetworkError: Answer from Java side is empty` 다음 `ConnectionRefusedError`. JVM이 사라졌다.
- 관찰:
  - `dags/jobs.py`는 모든 태스크에 `SPARK_DRIVER_MEMORY=16g`을 주는데, 근거 태스크의 컨테이너 상한은 `mem="4g"`다.
  - 같은 조건을 손으로 재현(`docker run -m 4g --memory-swap 4g`, `SPARK_DRIVER_MEMORY=16g`, multi_repo_burst 07-15): `OOMKilled=true`, 같은 py4j 오류.
  - 같은 SQL이 `rule_backfill`의 `rerun_silver`(상한 24g)에서는 7일 모두 돌았다 (M-15). 결정 11 뒤 하루치 `bot_build`(`h4_registry_v3`, 근거 다섯 개, 4g)는 성공했다.
  - Airflow가 띄운 컨테이너는 실패하면 지워져 OOMKilled 표시를 직접 보지 못했다. `docker events`에도 그 시간의 oom이 없다 (재현한 것 하나만 있다) [확인 필요].
- 원인 (재현으로 거의 확실): JVM 힙 상한(16g)이 컨테이너 상한(4g)보다 커서, 일이 커지면 JVM이 4g를 넘고 컨테이너 OOM으로 죽는다. 전에는 근거 SQL이 작은 피처 표만 읽어 4g 안에 머물렀다. 이번에 events 491만 행을 읽는 근거와, 쓰기 전에 옛 파티션을 읽어 맞추는 조인을 더해 넘게 됐다. **이 세션의 변경(결정 11, 12)이 드러낸 것이다.**
- 막힌 것: L9 점검 "일주일치가 DAG만으로 처음부터 끝까지 돌고 `dbt build`가 0으로 끝난다". 하루치 DAG(M-13, M-15)와 rule_backfill에서는 `dbt build`가 0으로 끝났지만, 일주일 한 번 실행은 이 오류로 Gold까지 가지 못했다.
- 막히지 않은 것: Silver는 끝까지 돌았다. 07-18의 events, entity_features, evidence 지문이 일주일 실행 앞뒤로 같다 (행 4,724,216 / 774,272 / 3,097,088). Gold는 직전 rule_backfill(`h5_multi_repo_burst`)의 결과로 남아 있다.
- 처방 후보 (고르지 않음): 근거 태스크의 컨테이너 상한을 올림, 태스크마다 `SPARK_DRIVER_MEMORY`를 상한보다 작게(상한에서 JVM 바깥 몫을 뺀 값) 주기, multi_repo_burst가 events 대신 피처를 읽게(피처에 "한 시간 최대 저장소 수"를 더함).
