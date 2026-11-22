# 오류 분석 기록

이 문서는 만들고 돌리면서 실제로 만난 오류를 증상, 원인, 처방으로 남긴 기록이다(F-N).
같은 증상을 만났을 때, 또는 알려진 한계가 무엇인지 볼 때 읽는다. 장애 대응 순서는 [런북](runbook.md)에 있다.

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

## F-5. Gold 모델 하나가 BigQuery 방언으로 바뀌지 않는다 [해결, 결정 25]

- 태그: `타입·스키마`, `설계 가정`
- 언제·어디: 2026-11-17, 8절 L5-3 이식 대비 시험, `pytest tests/test_gold_dialect.py` (sqlglot 30.32.0)
- 증상: 15개 중 14개 통과, `entity_verdict` 실패. `sqlglot.errors.UnsupportedError: 'NULLS LAST' translation not supported for aggregate function GROUP_CONCAT with sort order`
- 관찰: `entity_verdict.sql`의 `string_agg(distinct e.evidence_id, ',' order by e.evidence_id)`. DuckDB는 정렬의 NULL 위치 기본값이 NULLS LAST다.
- 가설 (확인 안 함): sqlglot이 DuckDB의 기본 NULL 순서를 명시적으로 옮기려 하는데, BigQuery `STRING_AGG ... ORDER BY` 안에서는 NULL 순서를 줄 수 없다고 보고 거부한다. 걸린 근거 목록은 NULL이 없으므로 결과에는 영향이 없을 수 있다 [확인 필요].
- 막힌 것: 없음 (로컬 Gold는 정상). C3 이식 때 고칠 곳으로 미리 드러났다. 이 시험의 목적대로다.
- 처방 후보 (고르지 않음): 정렬 없이 모으기, DISTINCT와 정렬을 하위 쿼리로 빼기, 이 모델만 BigQuery용 매크로.
- 해결 (2026-11-22, 지시서 handoff_v7.md 1-3): 후보 꼴 다섯 개를 sqlglot으로 변환해 보니 `order by e.evidence_id nulls first`만 정렬을 지키며 통과했다. DISTINCT를 하위 쿼리로 빼도 같은 오류가 났다. 결정 25. 〔근거: 실행 출력 2026-11-22 f5.py: orig, sub_distinct, list_sort 실패, nulls_first_explicit, nosort 통과〕
- 가설 확인: 오류는 DuckDB 기본값(NULLS LAST)을 BigQuery로 옮기지 못해서 났다. NULL 위치를 적으면 BigQuery 기본(오름차순 NULLS FIRST)과 같아 그대로 옮긴다. 〔근거: 실행 출력 `STRING_AGG(DISTINCT e, ',' ORDER BY e)`〕
- 끝난 기준: `tests/test_gold_dialect.py` 21개 통과, entity_verdict 1,474만 행 해시 같음, Gold 기준값 같음. 〔근거: 실행 출력 2026-11-22 04:15〕

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

## F-12. 근거 태스크가 컨테이너 메모리 4g를 넘어 죽는다 [해결, 결정 13]

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
- **해결 (지시서 J1, 사용자 선택 A + B, 2026-11-18):**
  - A: `dags/jobs.py`의 `heap(mem)`이 컨테이너 상한에서 힙을 계산한다 (상한 − max(25%, 1GB)). `job()`은 상한 하나만 받는다. 태스크별 상한을 M-10에 맞춰 다시 정했다 (load_events 8g, 피처 12g, 근거 4g, dbt 8g).
  - B: 계정 피처에 `max_repos_per_hour`를 더하고 `multi_repo_burst` v2가 그것을 읽는다. v1 파일은 남겼다.
  - 확인: 일주일 한 번 실행(`j1_week`)이 끝까지 성공, `dbt build` ERROR 0 (M-17). 같은 4g 컨테이너에서 힙을 512m로 일부러 줄이면 이제 `java.lang.OutOfMemoryError: Java heap space`로 끝나고 컨테이너는 죽지 않는다 (OOMKilled=false). 원인이 보이는 오류다.
  - 남은 [확인 필요] "Airflow 컨테이너의 OOM이 `docker events`에 안 보인 이유"는 풀지 않았다. 이제 태스크가 끝날 때 로그에 `[mem.peak]`를 남겨 다음에는 상한에 닿았는지 로그만으로 본다.

## F-13. 생존 확인 대상 고르기가 규칙 버전을 거르지 않는다 [해결, 결정 16]

- 태그: `정답`, `설계 가정`
- 언제·어디: 2026-11-18, 지시서 J2. truth에 target_window를 더한 뒤 07-15 구간 채점을 M-8과 맞춰 보다가 발견.
- 증상: 07-15 구간 fake_star_repo 정밀도 262/276 = 94.9%는 M-8과 같은데, 대조군이 274개에서 282개로 늘었다 (양성 11개는 같음, 3.9% 대 4.0%). 11-18에 다시 확인한 파일(`checked_date=2026-11-18/part-000.jsonl`)이 의심 280 + 대조군 280이다 (11-17 파일 세 개는 276 + 276).
- 관찰:
  - `sql/truth/pick_targets.sql`이 `evidence_id IN ('low_activity', 'lockstep_ratio', 'burst_5m')`로만 거르고 `rule_version`을 보지 않는다. Silver에는 lockstep_ratio v2와 v3이 7일 모두 있다 (L8, M-11에서 v3을 남김). 그래서 의심 = v2 ∪ v3 조합이 되었다.
  - 11-18 의심 280개 중 Gold(v2) fake_star_repo가 아닌 4개: Anmol-Baranwal/Cool-GIFs-For-GitHub, ruucm/shadergradient, veltman/flubber, krahets/LeetCode-Book. 모두 200(살아 있음). 채점에서 이들은 "fake_star_repo에 안 걸렸는데 정답이 있는 대상", 즉 대조군으로 세어진다. 여기에 대조군도 280개로 4개 더 뽑혔다 → 274 + 8 = 282.
  - 주석은 "suspect: 그 판정 구간의 fake_star_repo (... Gold와 같은 식)"이라 적혀 있다. Gold는 registry의 지금 버전만 본다. 주석과 SQL이 어긋난다.
- 원인: pick_targets는 registry를 모른다 (Silver SQL은 registry를 읽지 않는다). L4에서 만들 때는 근거 버전이 하나씩이라 드러나지 않았다. L8에서 옛 버전 파티션을 남기기로 하면서(결정 9) 생긴 틈이다.
- 영향: 07-16 ~ 07-21 생존 확인(J2, `j2_truth_0716_0721`)도 같은 SQL로 골랐다. v3에만 걸린 저장소가 의심 묶음에 들어가 확인되고, 채점에서는 대조군으로 세어진다. 날짜별 몫은 measurements.md M-18에 센다.
- 처방 후보 (고르지 않음, 사용자 결정): (A) repo_alive가 registry의 지금 버전을 읽어 pick_targets에 파라미터로 넘긴다 (B) pick_targets가 Gold `fake_star_repo`를 읽는다 (Silver가 Gold에 기대게 되어 방향이 거꾸로) (C) 채점(evaluation)에서 대조군을 "생존 확인에서 control로 고른 것"(raw의 grp)으로 한정한다 (truth에 grp를 옮겨야 한다).
- **해결 (2026-11-18, 지시서 K1, 사용자 선택 A):** `truth/repo_alive.py`가 registry의 근거별 지금 버전을 읽어 `pick_targets.sql`에 `:v_low_activity`, `:v_lockstep_ratio`, `:v_burst_5m`로 넘기고, SQL이 그 버전만 본다. 7개 구간에서 옛 SQL과 비교해 의심이 Gold fake_star_repo와 같아졌다 (빠진 것 4, 1, 1, 5, 4, 2, 2개, M-21).
- **남은 것:** 원천만 고쳤다. 옛 확인에서 v3 때문에 의심으로 확인된 저장소는 truth에 그대로 있고, 채점에서 대조군으로 세어진다 (07-15 4, 07-16 1, 07-17 1, 07-18 4, 07-19 4, 07-20 2, 07-21 2개). 몫이 작아 결론을 바꾸지 않는다. 대조군을 정확히 세야 할 때는 raw의 grp로 센다 (M-22가 그렇게 셌다).

## F-14. 새로 받은 레포에서 dbt 컴파일이 실패하고 방언 시험이 조용히 건너뛰어진다 [해결, README]

- 태그: `운영`, `문서`
- 언제·어디: 2026-11-18, 지시서 J6 README 따라 하기. 레포를 스크래치 폴더에 새로 받고(HEAD 56fc962 + 커밋 전 변경) README대로 `.env` → `docker compose --profile jobs build` → `pytest`.
- 증상: README의 시험 명령이 "6 passed, 1 skipped"로 끝났다 (지금 레포는 21개). 건너뛴 것은 `tests/test_gold_dialect.py` 전체: `dbt compile 결과가 없다`. 컴파일을 먼저 돌리자 `IO Error: Cannot open file "/work/warehouse_local/gold.duckdb": No such file or directory`.
- 원인: `warehouse_local/`은 git에서 빠져 있다. 평소에는 Spark(create_tables)가 Silver를 만들며 이 폴더를 만들어 두지만, 새 레포에서 시험만 돌리면 폴더가 없다. DuckDB는 없는 폴더에 파일을 만들지 않는다. 방언 시험은 컴파일 결과가 없으면 실패가 아니라 skip이라 빠진 것이 눈에 띄지 않았다.
- 해결 (지시서 J6 "막힌 곳을 README에 고친다"): README 시험 절에 `mkdir -p warehouse_local` → `python -m warehouse.dbt_build compile` → `pytest`와 기대 결과(21개 중 20개 통과, F-5 하나 실패)를 적었다. 고친 순서로 새 레포에서 20 passed, 1 failed.
- 한계: 이미지 빌드는 이 PC의 Docker 캐시를 썼다(5초). 캐시 없는 첫 빌드(내려받기 포함)는 이번에 보지 않았다.

## F-15. 엿새치 생존 확인을 한 번에 하자 GitHub가 429로 막았다 [해결, 결정 16]

- 태그: `정답`, `바깥 의존`
- 언제·어디: 2026-11-18 04:25 ~ 04:49Z, 지시서 J2, `late_truth` 한 번에 07-16 ~ 07-21 (`j2_truth_0716_0721`).
- 증상: repo_alive가 성공으로 끝났지만 확인 실패가 많다. 429(요청 너무 많음)가 07-16 340, 07-17 572, 07-18 982(전부), 07-19 434, 07-20 137, 07-21 0. 확인 실패는 truth에 넣지 않으므로 07-18은 정답이 없고, 07-16, 07-17, 07-19의 대조군이 0 ~ 81개로 줄었다 (M-18).
- 관찰:
  - 요청 수가 전보다 약 9배다 (약 5,000번 / 25분, 전에는 한 번에 약 560번). repo_alive는 동시 4개, 실패만 30초와 60초 쉬고 다시 묻는다. 그날 안의 재시도 간격이 GitHub의 제한이 풀리는 시간보다 짧았다 [확인 필요: GitHub 웹(github.com HEAD)의 제한 규칙은 공개되지 않았다].
  - 07-18은 6초 만에 982개가 모두 429였다. 앞 날의 요청으로 이미 막힌 상태에서 시작했다.
  - 대상 순서가 의심 먼저라(pick_targets의 UNION ALL) 막히면 대조군이 먼저 빈다. 대조군 삭제율이 정밀도보다 더 흔들린 이유 중 하나다.
  - 태스크는 성공(초록)이라 Airflow 화면에서는 보이지 않는다. 확인 실패 수는 로그 줄(`확인 실패=`)에만 있다.
- 막힌 것: J2 체크포인트의 "7일 구간별 정밀도와 대조군 삭제율 표" 중 07-18 전체와 07-16, 07-17, 07-19의 대조군.
- 막히지 않은 것: 07-15 구간 비교(94.9% 대 3.9%), 판정 시각과 독립성 시험, truth 멱등.
- 처방 후보 (고르지 않음, 사용자 결정. 바깥에 다시 요청을 보내는 일이라 이 세션에서 다시 돌리지 않았다):
  (A) 실패한 대상만 나중에 다시 확인한다. 같은 날 다시 하면 MERGE가 이미 확인한 대상을 건너뛰고 실패한 것만 더한다.
  (B) repo_alive가 429를 받으면 길게 쉰다 (응답의 Retry-After를 따르거나 분 단위로).
  (C) late_truth가 하루치씩 나눠 돈다 (날 사이에 쉼).
  (D) 대상 순서를 의심과 대조군이 번갈아 오게 해서, 막혀도 두 묶음이 같이 줄게 한다.
  (E) 확인 실패 비율이 문턱을 넘으면 태스크를 실패로 끝내 화면에 드러나게 한다.
- **해결 (2026-11-18, 지시서 K1, 사용자 선택 A + B + D + E):**
  - (A) `--only-missing`: 같은 (저장소, 구간)에 성공한 확인(200, 301, 404, 451)이 raw에 있으면 묻지 않는다. `late_truth`가 넘긴다.
  - (B) 429를 받으면 모든 동시 요청이 같이 쉰다 (`Gate`: Retry-After를 따르고, 없으면 60, 120, 300초). 한 대상에 429가 6번이면 확인 실패로 둔다.
  - (D) 대상 순서를 의심과 대조군이 번갈아 (`pick_targets.sql`의 `ORDER BY rn, grp DESC`).
  - (E) 확인 실패 비율이 `--max-fail-rate`(기본 5%)를 넘으면 exit 1. 끝에 요청 수, 429 수, 쉰 시간, 확인 실패 비율을 한 줄로 찍는다.
  - 결과 (M-21): 07-16 ~ 07-20 다시 보내기에 요청 2,445번, 429 한 번, 쉼 60초, 확인 실패 0. 7개 구간 모두 정답과 대조군이 찼다. `--only-missing`으로 다시 돌리면 요청 0번. 문턱 0%로 일부러 실패시키면 exit 1.


## F-16. 다른 날짜 이벤트 한 줄이 그 날짜 파티션 전체를 덮어쓴다 (재현, 가드로 막음) [해결, 결정 22] 〔근거: 지시서 handoff_v5_wp1.md 1-2 "F-16(가드 없을 때의 손실 재현)"〕

- 태그: `쓰기`, `설계 가정` 〔근거: 판단: 실제로 잃은 적은 없고 쓰기 방식의 성질이다〕
- 언제·어디: 2026-11-22, 지시서 1-2. `tests/test_late_guard.py`의 임시 창고. 〔근거: 코드: tests/test_late_guard.py〕
- 재현: 07-14로 옮긴 표본 3,407행을 적재한 뒤, 07-15 표본 3,407행에 07-14 23:59:59 이벤트 한 줄을 섞어 가드 없이 적재했다. 07-14 파티션이 3,407행에서 1행이 되었다. 돌리기 전 예측도 1행이었다. 〔근거: 코드: test_late_guard.py `assert count(s, PREV) == 1` 통과. 문서: [Spark overwritePartitions](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriterV2.overwritePartitions.html)〕
- 원인: `run_sql.run`이 `overwritePartitions()`로 쓰고, 이 쓰기는 결과에 한 행이라도 있는 파티션을 통째로 바꾼다. `events.sql`은 날짜로 거르지 않는다. 〔근거: 코드: spark_jobs/run_sql.py 쓰기 줄, sql/silver/events.sql 2~3번째 줄〕
- 지금까지 잃지 않은 이유: GH 7일 모두 그날 파일에 다른 날짜 행이 0개다. 관찰이지 보장이 아니었다. 〔근거: 측정: M-27(07-15, 07-20 다른 날짜 행 0), local_baseline.md 1절(manifest 줄 수 = events 행 수)〕
- 처방: 쓰기 전에 다른 날짜 행을 세어 실패 (결정 22). 같은 표본을 가드를 켜고 적재하면 SystemExit로 끝나고 07-14 지문이 그대로, 07-15는 0행이다. 〔근거: 코드: test_late_guard.py 앞 세 assert 통과〕

<a id="f-18"></a>
## F-18. 판정이 빠진 계정이 실유저로 통과하고, 막는다던 시험은 늘 초록이었다 (재현, 시험 두 개로 막음) [해결] 〔근거: 지시서 handoff_v7.md 1-1 "기록: F-18". docs/review_logic.md R-6〕

- 태그: `시험`, `설계 가정` 〔근거: 판단: 실제 7일 데이터에서 빠진 계정은 0개였다. 시험이 지키지 못하는 성질이었다〕
- 언제·어디: 2026-11-22, 표본(`tests/fixtures/gh_sample.json.gz`, 07-15 3,407행)으로 만든 임시 창고. 〔근거: 코드: tests/test_judged_red.py〕
- 옛 상태: `real_user_events`가 판정이 없으면 `coalesce(..., true)`로 실유저로 두었고, 이를 막는다던 `policy_account_sum`은 entity_verdict와 evidence_flags를 비교해 식으로 늘 같았다. 〔근거: 코드: 커밋 d2c8bd3의 real_user_events.sql 13 ~ 15행, tests/policy_account_sum.sql. review_logic.md R-6 재확인〕
- 1차 처방(2026-11-22, Claude): 시험 `every_account_judged`(날짜별 stg_events 고유 계정마다 정책별 판정이 있나)와 기본값 false(real_users()와 같은 동작). 〔근거: 코드: 커밋 35f971c, warehouse/dbt/tests/every_account_judged.sql, real_user_events.sql〕
- 재현 (Gold를 세 번 만든다):

  | 표본 상태 | every_account_judged | every_evidence_ran | 예측 |
  |---|---|---|---|
  | 근거 0개 | 실패 | 실패 | 같음 〔근거: 코드: test_judged_red.py 첫 assert 통과〕 |
  | always_on 하나만 안 돎 | **통과** | 실패 | 지시서는 실패를 예측, Claude Code는 실행 전 통과를 예측 〔근거: 코드: test_judged_red.py 둘째 assert 통과. handoff_v7.md 1-1 빨간 확인〕 |
  | 근거 전부 | 통과 | 통과 | 같음 〔근거: 코드: test_judged_red.py 셋째 assert 통과〕 |

- 원인 (둘째 줄): `evidence_flags`가 근거들을 (대상, 날짜) 한 행으로 모으므로, 근거 하나라도 돈 계정은 판정이 생긴다. always_on 행이 없는 계정은 `bool_or`가 false가 되어 strict가 그 계정을 사람으로 둔다. 숫자가 조용히 늘어나는 실패다. 〔근거: 코드: evidence_flags.sql `group by entity_type, entity_key, window_end`, entity_verdict.sql strict 줄〕
- 2차 처방: 시험 `every_evidence_ran`을 더했다. 계정 근거마다 그날 이벤트를 낸 계정 전부에 행이 있는지, 지금 쓰는 근거(evidence_current)가 날마다 행이 있는지 본다. 기존 두 시험은 지우지 않았다. 〔근거: 코드: warehouse/dbt/tests/every_evidence_ran.sql. 판단: every_account_judged는 "근거가 하나도 안 돈 날"을, 새 시험은 "근거 하나가 빠진 날"을 잡는다. 반증 조건: 근거 SQL이 대상 일부에만 행을 내게 바뀌면 이 시험이 실패하고, 그때 근거 계약을 다시 정한다〕
- 실제 7일 Gold: 두 시험 모두 행 0개(PASS), every_evidence_ran 2.7초. 〔근거: 실행 출력 2026-11-22 04:11 `dbt_build --select every_evidence_ran every_account_judged`〕
- 숫자 변화: 기본값을 false로 바꾼 뒤 `scripts.baseline` gold 부분이 기준값과 같다. 판정 없는 계정이 0이라 기본값이 쓰이지 않는다. 〔근거: 실행 출력 b_g1.json 비교 "gold 같음". 계산: 지시서 1-1 예측〕
- 덧붙여 바꾼 것: 표본으로 Gold를 따로 만들려고 dbt 프로필 경로와 Silver 위치를 `BOT_WAREHOUSE`(기본 `/work/warehouse_local`)로 받게 했다. Spark 세션과 같은 변수다. 〔근거: 코드: warehouse/dbt/profiles.yml, models/sources.yml, spark_jobs/session.py 27행〕
- 남은 한계: 저장소 근거(lockstep_ratio, burst_5m)는 "날마다 행이 있나"만 본다. 저장소 대상 전부에 행이 있는지는 보지 않는다. 〔근거: 코드: every_evidence_ran.sql 둘째 부분. 판단: 저장소 대상 집합(entity_features)을 Gold가 읽지 않아 기준이 없다. 웹 원천(WP1-b) 때 대상 종류별로 넓힌다〕

<a id="f-19"></a>
## F-19. GH Archive 시간 파일 안에 같은 이벤트가 두 번 들어 있다 [기록, 처방은 사용자 결정 대기] 〔근거: 측정: M-36 (e)〕

- 태그: `원천`, `설계 가정` 〔근거: 판단: 우리 적재가 아니라 원천 파일의 성질이다〕
- 언제·어디: 2026-11-22, 지시서 handoff_v7.md 1-4 (e). 7일 stg_events. 〔근거: 코드: scripts/m36/e_event_id_dup.sql〕
- 증상: 날짜 안 event_id 중복 하루 1 ~ 12개, 7일 44개. 예측은 0이었다. 〔근거: 측정: M-36 (e)〕
- 관찰: 중복 행은 내용이 모두 같고 같은 시간에 있다. 07-17 id 하나는 원천 `2024-07-17-2.json.gz`에 두 줄로 들어 있다(앞뒤 시간 파일에는 0줄). 날짜를 넘는 중복은 없다. 〔근거: 실행 출력 2026-11-22. zcat 줄 세기〕
- 영향: 이벤트 수가 하루 최대 12건(3.3×10⁻⁶) 부풀었다. 중복이 WatchEvent면 그 저장소의 star가 1 늘어 lockstep, burst 근거에 들어간다. 지금까지의 판정·지표가 눈에 띄게 바뀔 크기는 아니다 [확인 필요: 중복을 뺀 events로 근거를 다시 돌려 fake_star_repo 수를 비교]. 〔근거: 계산: 12 ÷ 3,615,363. 판단: 판정 문턱은 star 수십 개 단위(결정 12)〕
- 지금 보증의 틈: `local_baseline.md`의 "manifest 줄 수 = events 행 수"는 중복 줄도 한 줄로 세므로 이 중복을 잡지 못한다. 〔근거: 코드: scripts/baseline.py manifest_lines. 측정: local_baseline.md 1절〕
- 처방 후보 (사용자 결정): (가) 그대로 두고 기록만, (나) `events.sql`에서 event_id로 하나만 남기고 load_audit(Q5)에 버린 중복 수를 적는다(Silver 지문과 기준값이 바뀐다), (다) 경고 시험만 더한다(Gold 시험: 날짜 안 event_id 중복 > 0이면 경고). 〔근거: 판단: (나)는 Silver 숫자와 기준값을 바꾸므로 사람이 정한다. CLAUDE.md 3절 4번〕


## F-17. 원천에서 필수 필드가 사라져도 적재가 오류 없이 NULL 행을 쓴다 (재현, 기록표 문턱으로 막음) [해결, 결정 27, 28] 〔근거: 지시서 handoff_v6.md Q6 "기록: F-17(재현)"〕

- 태그: `원천 변화`, `설계 가정` 〔근거: 판단: 실제로 잃은 적은 없고, 읽기 방식의 성질이다〕
- 언제·어디: 2026-11-22, `tests/test_load_audit.py`의 임시 창고. 〔근거: 코드: tests/test_load_audit.py `test_actor_removed_fails_and_unguarded_loads_silently`〕
- 재현: 07-13으로 옮긴 표본 3,407줄에서 모든 줄의 `actor`를 지우고 가드를 끈 채(`gate_on=False`) 적재했다. 오류 없이 3,407행이 들어가고 actor_name, actor_key가 모두 NULL이었다(`count(actor_name) = 0`). 〔근거: 코드: 같은 시험의 마지막 assert `(3407, 0, 0)` 통과〕
- 원인: 스키마를 지정한 JSON 읽기에서 없는 필드는 깨진 줄이 아니라 NULL이다. 옛 적재(DROPMALFORMED)는 깨진 줄만 버리므로 필드가 통째로 사라지면 아무것도 막지 않았다. 〔근거: 코드: 커밋 79ef270 load_events.py `option("mode", "DROPMALFORMED")`. 문서: [Spark JSON mode](https://spark.apache.org/docs/latest/sql-data-sources-json.html) "PERMISSIVE ... sets malformed fields to null"〕
- 실제 사례: GitHub는 2025-10에 Events API의 PushEvent에서 `payload.commits` 등을 뺐고, 그 필드를 읽던 쪽은 오류 없이 빈 값을 받았다. 이 프로젝트는 payload를 읽지 않아 그 변화에는 영향이 없다. 〔근거: 문서: [GitHub changelog 2025-08-08](https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/), [DEV 글](https://dev.to/flarecanary/github-silently-removed-payloadcommits-from-pushevent-heres-what-broke-and-how-to-catch-the-2i33). 코드: load_events.py `SCHEMA`에 payload 없음〕
- 처방: 필수 필드 5개의 NULL 비율이 0.1%를 넘으면 쓰지 않고 실패한다(결정 27). 같은 파일을 가드를 켜고 적재하면 `actor.login NULL 3407 / 3407행`으로 SystemExit, 07-13 events 0행, 기록표 null_actor_login 3407. 〔근거: 코드: 같은 시험의 앞 세 assert 통과〕
- 남는 틈: 필수 필드가 아닌 칸이 사라지는 것은 막지 않는다. 지금 events가 읽는 칸은 actor.id와 repo.id가 더 있는데 문턱에 넣지 않았다(login과 name이 같은 객체에서 오므로 객체째 사라지면 함께 걸린다). 〔근거: 코드: load_events.py `SCHEMA`, `REQUIRED`. 판단: 반증 조건은 actor.id만 사라지고 login은 남는 원천 변화가 생기는 경우. 그때 REQUIRED에 더한다〕

## F-21. 새로 받은 레포에서 `docker compose build`/`run bot-job`이 멈춘다 [열림, 처방은 사용자 결정 대기] 〔근거: 측정: M-33 사본 copy1 `docker compose build bot-job` 종료 코드 1〕
- 증상: `error while interpolating services.airflow-db.environment.POSTGRES_PASSWORD: required variable POSTGRES_PASSWORD is missing a value: set POSTGRES_PASSWORD in .env` 〔근거: 측정: copy1에서 docker compose build bot-job, 종료 코드 1, 0초〕
- 조건: .env가 없는 사본(.env는 .gitignore). bot-job만 골라도 compose는 파일 전체의 `${VAR:?}`를 먼저 풀어 Airflow 서비스 변수 때문에 멈춘다. 〔근거: 코드: docker-compose.yml x-airflow-common, airflow-db의 `:?`. .gitignore `.env`〕
- 처리: 데모와 CI는 compose 대신 `docker build --target bot-job` + `docker run -v <레포>:/work`로 돌렸다. compose 파일은 고치지 않았다. 〔근거: 코드: .github/workflows/ci.yml 머리말〕
- 처방 후보(결정 필요): (가) .env.example을 두고 README에 복사 단계, (나) bot-job을 별도 compose 파일로 분리, (다) README 한 줄 실행을 docker run으로. 〔근거: 판단: (다)는 지금 바로 되고 (가)(나)는 compose 관례를 지킨다. 사용자가 고른다(CLAUDE.md 3절 4번)〕

## F-22. 결과가 0행인 쓰기를 "다른 커밋에 덮였다(F-7)"로 보고한다 [열림, 고치지 않음] 〔근거: 코드: spark_jobs/run_sql.py 77 ~ 85행. 번호는 두 세션 합의(이 세션 F-21, F-22)〕

- 태그: `진단`, `기존 코드` 〔근거: 판단: 시험 코드를 쓰다 만났고, 기존 run_sql.py라 고치지 않았다(CLAUDE.md 3절 8번, 작업 규칙)〕
- 증상: `SystemExit: bot.silver.entity_features: 커밋이 표에 없다 (write-id 192f2c20-...). 다른 커밋에 덮였다 (F-7)` 〔근거: 실행 출력 2026-11-22 04:30 tests/test_pick_targets.py 첫 실행〕
- 조건: 임시 창고에 features_account를 돌리지 않고 features_repo만 돌렸다. features_repo는 계정 피처를 읽으므로 결과가 0행이다. 동시에 쓰는 다른 프로세스는 없었다. 〔근거: 코드: sql/silver/features_repo.sql 1, 6행 `FROM bot.silver.entity_features`. 판단: 컨테이너 하나, 세션 하나〕
- 원인 추정: 0행 `overwritePartitions()`는 Iceberg가 커밋을 건너뛰어 스냅샷이 생기지 않고, run_sql의 쓴 뒤 확인은 이를 동시 커밋 손실과 구분하지 못한다. 〔근거: 코드: run_sql.py 77 ~ 85행. 확인 필요: Iceberg 1.11.0 `SparkWrite` DynamicOverwrite의 빈 커밋 처리(소스에서 "skipping commit" 분기 확인)〕
- 영향: 실제로는 결과가 비면 이 메시지가 원인을 F-7로 잘못 가리킨다. run_sql.py 주석은 "근거는 대상 전부에 행을 내서 0행이 안 나온다"고 가정한다. 피처를 빠뜨린 순서 실수가 동시성 문제로 보인다. 〔근거: 코드: run_sql.py 76행 ponytail 주석〕
- 처방 후보(고르지 않음): (가) 쓰기 전에 결과 행 수를 세어 0이면 "결과 0행"으로 따로 실패, (나) 확인 실패 메시지에 "결과가 0행이었을 수도 있다"를 더함. 〔근거: 판단: (가)는 한 번 더 읽고, (나)는 비용이 없다〕
- 이번 처리: 시험 쪽에서 features_account를 먼저 돌리게 고쳤다(내가 새로 쓴 시험이라 고침). 〔근거: 코드: tests/test_pick_targets.py fixture〕
- 갱신(2026-11-22): 웹 근거 셋(대상 web_client)이 registry에 들어온 뒤 같은 기제가 GH 날짜를 도는 곳에서 나타났다. scripts/demo.py가 웹 근거를 GH 표본에 돌려 0행 쓰기를 F-7로 알렸다(다른 세션이 실행해 찾음). run_sql.py는 그대로 두고, GH 날짜를 도는 곳(demo, 시험 셋, dags/bot_build.py)이 `dbt_build.is_gh`로 GH 근거만 고르게 비켜 갔다. 열림 상태는 그대로다. 〔근거: 코드: warehouse/dbt_build.py is_gh, scripts/demo.py 74행. 실행: demo rc=0, dbt PASS 35 (2026-11-22 06:52Z). 결정 26〕

<a id="f-23"></a>
## F-23. AUTH 원본에 요청 없이 끊긴 줄 30,763개가 있어 적재 가드가 막았다 (가드가 제대로 막음, 결정 34로 계약을 맞춤) [해결] 〔근거: 실행: spark_jobs/load_web.py 첫 실행 2026-11-22 07:0xZ〕

- 태그: `새 원천`, `계약` 〔근거: 판단: 오늘 쓴 적재기의 필수 필드 가정이 원본과 달랐다〕
- 증상: `auth_lib: 쓰지 않고 멈춘다. resource NULL 30763 / 4091155행 > 문턱 0.1%`. 적재 기록표에 status='rejected' 한 행이 남고 events는 쓰지 않았다. 〔근거: 실행 출력 스크래치 auth/log_load.txt. 코드: load_web.py gate〕
- 원인: resource가 없는 줄은 깨진 줄이 아니라 nginx가 남긴 정상 줄이다. 30,752줄이 응답 408(연결만 열고 요청을 보내지 않아 시간 초과), 10줄이 400, 1줄이 200이다. 모두 요청 줄이 "-"이고 UA도 "-"이며, JSON에 resource와 method 키가 아예 없다. 〔근거: 계산: 원본 4,091,155줄을 파이썬으로 훑어 키 묶음과 (method, response)를 셈. 문서: [nginx 408](https://nginx.org/en/docs/http/ngx_http_core_module.html#client_header_timeout) client_header_timeout〕
- 영향: 막지 않았다면 없음(이 줄들은 UA가 "-"라 사전 실험도 세션에서 뺐다). 가드가 원천의 실제 모양을 처음 적재에서 드러냈다. 〔근거: 코드: experiments/p0_bot_detection/web/02_detect.py `ua NOT IN ('', '-')`〕
- 처방: 응답 408, 400 줄은 resource NULL을 문턱에서 빼고 따로 센다(`no_request`). 그 밖의 줄에서 resource가 빠지면 여전히 막는다. events에는 action = 'no_request'로 남긴다(원본 그대로). 〔근거: 코드: load_web.py no_req, sql/silver/events_auth_lib.sql. 결정 34〕
- 재발 방지: 새 원천은 필수 필드를 정하기 전에 원본 전체의 키 묶음을 한 번 센다. 원천 붙이는 비용(M-30)에 이 한 번을 넣었다. 〔근거: 판단: 표본 몇 줄로는 0.75%짜리 줄을 못 봤다〕


<a id="f-24"></a>
## F-24. CI 이미지 빌드가 Maven Central의 429(요청 너무 많음)로 실패했다 (일시적, 다시 돌려 통과) [기록만] 〔근거: 실행: GitHub Actions run 45001102955 1회차 "invalid response status 429"〕

- 태그: `CI`, `바깥 의존` 〔근거: 판단: 우리 코드가 아니라 바깥 저장소 응답〕
- 증상: 2026-11-22 08:04Z push(커밋 3d022cf)의 ci가 "bot-job 이미지" 단계 7초 만에 실패. `failed to load cache key: invalid response status 429`. 뒤 단계는 모두 건너뜀. 〔근거: gh run view --log-failed〕
- 원인: Dockerfile 9행 `ADD https://repo1.maven.org/…/iceberg-spark-runtime-4.1_2.13-1.11.0.jar`는 이미지 캐시가 있어도 캐시 열쇠를 정하려고 빌드마다 Maven Central에 요청한다. 그 요청이 GitHub 공용 러너 주소에서 요청 수 제한에 걸렸다. 〔근거: 코드: Dockerfile 9행. 로그 "#6 … ADD … ERROR: invalid response status 429". 확인 필요: Maven Central의 정확한 제한 기준〕
- 영향: 그 push의 시험 결과가 빠졌다. 실패한 작업만 다시 돌리자(2회차) 이미지 44초, 전체 3분 56초로 모두 통과했다. 코드 결함은 아니다. 〔근거: gh run view attempt 2 success〕
- 처방 제안(만들지 않음): jar를 `ADD` URL 대신 `RUN curl … && sha256sum -c`로 받거나 `ADD --checksum=sha256:…`을 쓰면, 내용 지문이 고정돼 캐시가 맞을 때 Maven에 다시 묻지 않는다. 〔근거: 문서: Dockerfile reference "ADD --checksum". 판단: 고치기 전 사용자 확인(기존 이미지 정의)〕
- 재발 방지: 지금은 빨간 CI가 이 오류면 "실패한 작업 다시 실행"을 먼저 한다. 〔근거: 이번 2회차 통과〕


<a id="f-25"></a>
## F-25. 규칙 버전 탭의 "바뀐 날 수"가 7 대신 700%로 보였다 (컬럼 이름으로 단위를 추측) [해결] 〔근거: handoff_delivery.md D3. 화면 확인 2026-11-22〕

- 태그: `화면`, `표시` 〔근거: 판단: 데이터는 맞고 보이는 꼴만 틀림〕
- 증상: 규칙 버전 탭 요약 표에서 바뀐 16조합의 "바뀐 날 수"가 모두 700%로 보였다. 실제 값은 7(날)이다. 〔근거: site/data/ruleset_change_log.parquet 집계: 115조합 중 diff <> 0인 날이 있는 조합 16개, 모두 7일〕
- 원인: 표 그리기 함수가 컬럼 이름에 change, rate, ratio 같은 말이 들어 있고 값이 50 이하면 백분율로 바꿨다. 화면이 계산한 `days_changed`가 "change"에 걸렸다. 〔근거: 코드: site/index.html 옛 452행 `/(change|extra|rate|share|mde|precision|ratio)/i`, 456행 `Math.abs(v) <= 50`〕
- 원인이 자명하지 않았던 이유: 정규식은 데이터 파일의 컬럼만 보고 만들었는데, 화면 SQL이 새 별칭을 만들면 그 별칭도 같은 규칙을 탄다. 별칭을 더한 쪽에서는 표시 규칙이 보이지 않는다. 〔근거: 코드: site/index.html 813행 별칭 days_changed〕
- 처방: 백분율 컬럼을 이름 목록 13개로 적었다. 목록은 옛 정규식에 걸리던 이름 전부(데이터 파일 컬럼과 화면 SQL 별칭)에서 days_changed만 뺀 것이라, 다른 탭의 백분율 표시는 그대로다. `≤ 50` 조건도 뺐다(목록이면 필요 없다). 〔근거: 코드: site/index.html `PCT` Set. 실행: index.html과 site/data/*.parquet에서 옛 정규식에 걸리는 이름 grep〕
- 남은 것: 같은 이유로 반대쪽 빈틈이 있다. `bot_lenient`, `bot_strict`(데이터 사전 탭 건강 표, 0~1 비율)와 `natural_var`, `step_balanced`(0~1 차이)는 이름이 안 걸려 소수로 보인다. D1에서 컬럼 단위를 meta.json에 실으면 목록 대신 단위로 판정한다. 〔근거: 코드: site/index.html 건강 표 SQL. handoff_delivery.md D3 할 일 둘째 문장〕
- 재발 방지: 단위를 이름에서 추측하지 않는다. 화면이 새 별칭을 만들면 PCT 목록이나(D1 뒤에는) 단위 사전에 같이 적는다. 〔근거: 판단〕


<a id="f-26"></a>
## F-26. 다시 내보낸 뒤 화면이 새 meta.json과 옛 표 파일을 섞어 읽었다 (브라우저 캐시) [해결] 〔근거: 실행: 로컬 화면 2026-11-22 09:3xZ〕

- 태그: `화면`, `캐시` 〔근거: 판단〕
- 증상: 공개 시드에 label, impact를 더해 다시 내보낸 뒤 화면을 새로 열었더니 카드(새 meta.json)는 보이는데 규칙 버전 표의 버전 이름과 숫자 영향 칸이 비었다. 〔근거: 화면 확인〕
- 원인: 브라우저가 ruleset_release.parquet을 캐시에서 꺼냈다. 같은 주소를 기본 fetch로 받으면 1,397바이트(옛 파일), `cache: 'no-store'`로 받으면 1,945바이트(새 파일)였다. 표 파일 주소가 내용과 상관없이 늘 같아서 생긴다. 〔근거: 실행: 화면 안 fetch 두 번 비교〕
- 원인이 자명하지 않았던 이유: meta.json은 새것으로 와서 화면 머리는 새 값이었다. 데이터가 바뀐 것처럼 보이지 않고 "컬럼이 빈 것"처럼 보였다. 〔근거: 위 증상〕
- 공개 사이트 영향: GitHub Pages는 파일마다 짧은 캐시 시간을 준다. 배포 직후 다시 온 방문자는 새 meta와 옛 표를 섞어 볼 수 있다. 〔근거: 확인 필요(공개 주소 응답의 cache-control 값을 배포 뒤 확인)〕
- 처방: meta.json은 `cache: "no-cache"`로 매번 서버에 묻고, 표 파일 주소에는 `?v=내보낸 시각`을 붙인다. 내보낼 때마다 주소가 바뀌어 옛 표를 쓰지 않는다. 엑셀, 구글 시트에 주는 고정 주소(가져가기 탭)는 그대로다. 〔근거: 코드: site/index.html VER〕
- 남은 것: index.html 자체도 캐시된다. 옛 화면 코드가 새 데이터를 읽어도 컬럼을 더하기만 해서(변경 정책 3번) 깨지지는 않는다. 〔근거: 결정 37, 39〕


<a id="f-27"></a>
## F-27. SQL 콘솔 스키마 나무에서 policy_compare_released 컬럼에만 설명(마우스 올림)이 비었다 [해결] 〔근거: 실행: 공개 주소 점검 2026-11-22 09:4xZ, 표 9개 중 이 뷰만 18개 중 0개〕

- 태그: `화면` 〔근거: 판단〕
- 원인: 설명을 `meta.models[뷰 이름].columns || meta.models[원래 표].columns`로 찾았다. gold.yml에 뷰 항목(설명만 있고 컬럼 없음)이 있어 앞쪽이 빈 객체 `{}`로 참이 되어 뒤쪽으로 넘어가지 않았다. 컬럼 사전 전에는 모든 표가 비어 있어서 드러나지 않았다. 〔근거: 코드: site/index.html schema()〕
- 처방: `_released`를 떼고 원래 표 이름으로 바로 찾는다(두 이름의 컬럼이 같다). 〔근거: 코드: policy_compare_released.sql `select *`〕
