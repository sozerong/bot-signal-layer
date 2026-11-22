# 엔진 선택 기록

이 문서는 클라우드에서 쓸 엔진(저장, 계산, 조율)을 무엇으로 고를지 선택지와 이유를 적은 기록이다.
클라우드 이식 단계에서 읽는다. 지금 로컬은 Spark, Iceberg, dbt, DuckDB, Airflow다.

서울 동네 찾기 `decisions.md`와 같은 형식이다: 상황, 선택지(장점, 단점), 추천과 이유, 이미 있는 것, 검증, 남은 것.

- **상태 표기:** `확정`은 이미 정한 것, `추천 (결정 대기)`는 사용자가 고를 것.
- 확정되면 `decisions.md`에 결정 번호를 붙여 옮긴다.
- 가격과 제한은 `gcp_research.md`(2026-11-16 조사)를 따른다.
- **2026-11-17:** 로컬에서 먼저 완성하고 옮기기로 했다 (`decisions.md` 결정 5). 이 문서의 엔진은 이식 단계(지시서 9절)에서 쓴다. 로컬 엔진은 지시서 2절에 있다.

## 0. 판단 기준

모든 선택지를 같은 다섯 가지로 본다.

1. **엔진 안이 보이는가.** 이 프로젝트의 차별점은 엔진 깊이다(설계 10절). Spark UI, 셔플 크기, 태스크별 시간을 직접 볼 수 있어야 병목을 원인까지 쫓을 수 있다.
2. **이미 있는 것.** 서울 동네 찾기에 돌아가는 코드와 경험이 있으면 그 값만큼 싸다.
3. **비용.** 무료 체험은 90일, 300달러, CPU 8개 제한이다. 켜 두기만 해도 돈이 드는 것은 피한다.
4. **바꾸기 쉬운가.** 변하는 부분과 유지할 부분을 나눈다. 엔진을 바꿔도 SQL과 표 계약은 남아야 한다.
5. **당근은 어떻게 했나.** 같은 선택을 할 필요는 없다. 다만 다르면 이유를 적는다.

## 1. 결정 순서 (큰 것부터)

| 순서 | 결정 | 상태 | 이게 정해져야 정할 수 있는 것 |
|---|---|---|---|
| E1 | 클라우드와 웨어하우스 | 확정 (GCP, BigQuery) | 전부 |
| E2 | 리전 | 확정 (서울, 결정 17) | 버킷, 데이터셋, 클러스터 위치 |
| E3 | Bronze 저장 | 확정 (A, 결정 18) | 수집 방식 |
| E4 | Silver 테이블 형식 | 확정 (Iceberg, 결정 18) | 카탈로그, 되돌리기 방식 |
| E5 | Silver 카탈로그 | 확정 (Lakehouse REST, 결정 18) | Spark와 BigQuery가 같은 표를 보는 방법 |
| E6 | Silver 계산 엔진 | 확정 (Spark, 사용자 요구) | 실행 방식 |
| E7 | Spark 실행 방식과 버전 | 확정 (클러스터 + 3.0, 결정 19) | 클러스터 크기, DAG 모양 |
| E8 | 지휘 (Airflow 위치) | 확정 (VM 위 Docker Compose, 결정 20) | DAG 코드, 비밀 관리 |
| E9 | Gold 변환과 저장 | 확정 (BigQuery + dbt-bigquery, 결정 20) | 분석팀이 받는 곳, 리니지 입력 |
| E10 | 컬럼 리니지 | 추천 (결정 대기) | 규칙 변경 재처리 범위 |
| E11 | 조회 화면 | 나중에 | 없음 |

## E1. 클라우드와 웨어하우스 (확정)

- `decisions.md` 결정 1: GCP에서 처음부터 하고 BigQuery를 쓴다.
- 이 문서는 그 위에서 "어떤 엔진을 어떻게 쓰나"만 정한다.

## E2. 리전

- **상황:**
  - BigQuery 쿼리는 읽는 데이터셋이 있는 위치에서 돈다.
  - 위치가 다른 데이터셋끼리는 한 쿼리에서 조인할 수 없다. 지역 간 조인 기능은 2026년 2월 기준 Preview였다.
  - 공개 데이터셋은 US나 EU처럼 정해진 위치에 있다.
  - GH Archive 데이터셋(`githubarchive`)의 위치는 아직 직접 확인하지 못했다. 계정이 연결되면 `bq show githubarchive:day`로 본다.
- **선택지:**
  - **A. 미국 (BigQuery 데이터셋은 US 멀티리전, 버킷, Spark, VM은 us-central1).**
    - 장점: GH Archive가 US라면 공개 데이터셋과 우리 표를 바로 조인할 수 있다. 검증할 때 원본과 대조하기 쉽다.
    - 장점: GCS 저장비가 서울보다 조금 싸다 (GB당 월 0.020 vs 0.023달러).
    - 단점: 내 PC에서 멀어 콘솔과 SSH가 조금 느리다.
  - **B. 서울 (asia-northeast3).**
    - 장점: 가깝다. 한국 서비스라면 실제로 이렇게 둔다.
    - 단점: GH Archive와 위치가 다르면 공개 데이터셋 대조를 할 때마다 데이터를 옮겨야 한다.
- **추천:** A. 단, GH Archive가 US라는 확인이 먼저다. US가 아니면 그 위치에 맞춘다.
- **사용자 선택 (2026-11-22): B 서울.** 공개 데이터셋은 검증용으로만 쓰고 US 위치에서 건수만 뽑아 비교한다 (decisions.md 결정 17).
  - 이 프로젝트에서 지연 시간은 중요하지 않다. 공개 데이터셋 대조가 더 자주 일어난다.
- **이미 있는 것:** 없음. 서울 동네 찾기는 로컬이다.
- **검증:** 첫날 `bq show`로 위치를 확인해 여기에 적는다.
- **남은 것:**
  - US 멀티리전 데이터셋이 us-central1 버킷의 Iceberg 표를 읽을 수 있는지 `[확인 필요: 위치 호환 규칙]`.
  - 안 되면 버킷도 US 멀티리전으로 둔다.

## E3. Bronze 저장

- **상황:**
  - 원본은 시간별 json.gz 파일이다. 사전 실험 기준 하루치 압축이 2.6GB라서 일주일치는 약 18GB로 예상한다.
  - 원칙은 "받은 그대로 보관, 수정하지 않음"이다(설계 3절).
  - 같은 데이터가 BigQuery 공개 데이터셋에도 있다.
- **선택지:**
  - **A. 시간별 파일을 받아 GCS에 그대로 둔다.**
    - 장점: 원본이 우리 손에 있다. 공개 데이터셋이 바뀌거나 사라져도 재처리할 수 있다.
    - 장점: 파일 단위 재처리, 체크섬 비교, manifest가 서울 동네 찾기 방식 그대로 된다.
    - 단점: 내려받기 코드와 GCS 저장비(일주일치 월 1달러 미만 추정)가 든다.
  - **B. BigQuery 공개 데이터셋에서 필요한 컬럼만 뽑아 쓴다.**
    - 장점: 받는 코드가 없다. SQL 한 줄이다.
    - 단점: 원본이 남의 것이다. "그때 받은 원본"을 증명할 수 없다.
    - 단점: 스캔한 만큼 돈이 든다. `payload`처럼 큰 컬럼을 고르면 바로 커진다.
  - **C. Storage Transfer Service로 URL 목록을 GCS에 바로 옮긴다.**
    - 장점: GCP 기본 기능이라 코드가 거의 없다.
    - 단점: URL 목록 형식(크기, MD5 칸)과 실패 처리를 따로 확인해야 한다. Airflow에서 파일 단위 상태를 보기 어렵다.
- **추천:** A. 원본 보관이 이 설계의 첫 원칙이고, 재처리 단위가 파일이어야 규칙 변경 재처리 범위를 좁힐 수 있다.
  - B는 버리지 않는다. Silver 결과를 공개 데이터셋과 건수로 맞춰 보는 **검증 용도**로 쓴다.
  - C는 받는 양이 3달치 이상으로 커져 내려받기가 병목이 되면 다시 본다.
- **이미 있는 것:** GH 레포의 `ingest.py`. 시간 파일 하나 = 파티션 하나 = manifest 하나, 멈춘 연결 끊기, 멱등까지 이미 돌아간다. GCS로 올리는 부분만 새로 쓴다.
- **검증:** 2024-07-15 하루치를 받아 사전 실험 파일 크기와 비교한다.
- **남은 것:** 없음.

## E4. Silver 테이블 형식

- **상황:**
  - Silver는 세 가지를 해야 한다.
    - 같은 표를 Spark가 쓰고 BigQuery가 읽는다.
    - "그 시점의 판정"을 다시 볼 수 있다. 사후 확정 정답으로 과거 판정을 채점하려면 필요하다.
    - 규칙 버전별로 일부 파티션만 다시 쓴다.
- **선택지:**
  - **A. Apache Iceberg.**
    - 장점: 스냅샷과 시간 여행으로 그때의 표를 다시 볼 수 있다. 파티션 단위 덮어쓰기가 원자적이다.
    - 장점: GCP에서 Spark와 BigQuery가 같은 Iceberg 표를 복사 없이 함께 쓴다.
    - 단점: 카탈로그를 하나 더 관리해야 한다 (E5).
  - **B. BigQuery 기본 표.**
    - 장점: 가장 단순하다. 카탈로그가 필요 없다.
    - 단점: Spark가 쓰려면 커넥터로 통째로 옮겨야 한다. 시간 여행은 최대 7일이라 몇 달 뒤 채점에 못 쓴다.
  - **C. 그냥 Parquet 파일.**
    - 장점: 아무것도 필요 없다.
    - 단점: 원자적 덮어쓰기와 스냅샷이 없다. 재처리 중 실패하면 반쯤 쓰인 상태가 남는다.
  - **D. Delta Lake.**
    - 장점: 기능은 Iceberg와 비슷하다.
    - 단점: GCP에서 BigQuery와 같이 쓰는 경로가 Iceberg보다 좁다. 서울 동네 찾기 경험도 없다.
- **추천:** A.
- **이미 있는 것:** 서울 동네 찾기 생활이동 원본이 Iceberg다(결정 87). 읽고 쓰는 코드(`transform/iceberg.py`)와 SQL 실행기(`transform/spark_sql_runner.py`)가 있다.
- **검증:** events 표의 스냅샷 하나로 되돌린 뒤 같은 판정이 나오는지 본다.
- **남은 것:** 스냅샷 보존 기간. 사후 확정 정답이 몇 달 뒤에 오니 최소 3달로 둔다 `[확인 필요: 저장비 실측]`.

## E5. Silver 카탈로그

- **상황:** Iceberg 표 목록을 누가 관리하느냐다. Spark가 쓰고 BigQuery와 dbt가 읽어야 한다.
- **선택지:**
  - **A. Lakehouse runtime catalog의 Iceberg REST 카탈로그 (옛 BigLake metastore).**
    - 장점: GCP가 권장하는 방식이다. Spark는 표준 Iceberg REST API로 붙는다. BigQuery는 같은 표를 `프로젝트.카탈로그.네임스페이스.표` 이름으로 바로 읽는다.
    - 장점: 관리할 서버가 없다.
    - 단점: GCP에 묶인다. 다만 표준 REST API라서 다른 카탈로그로 옮길 때는 접속 설정만 바뀐다.
  - **B. JDBC 카탈로그 (Cloud SQL PostgreSQL).**
    - 장점: 서울 동네 찾기와 똑같다.
    - 단점: BigQuery가 이 카탈로그를 못 읽는다. Gold를 BigQuery에 두면 Silver를 한 번 더 옮겨야 한다. Cloud SQL은 켜 두기만 해도 돈이 든다.
  - **C. Hadoop 카탈로그 (GCS 경로 자체가 카탈로그).**
    - 장점: 아무것도 안 띄워도 된다.
    - 단점: 동시 쓰기가 안전하지 않다. 근거 작업 여러 개가 같은 evidence 표에 동시에 쓰는 이 구조와 안 맞는다.
- **추천:** A.
- **이미 있는 것:** Iceberg 사용 경험. 카탈로그 접속 설정만 JDBC에서 REST로 바뀐다(`transform/iceberg.py`의 설정 다섯 줄).
- **검증:** Spark로 표를 하나 만들고 BigQuery 콘솔에서 같은 행 수가 나오는지 본다.
- **남은 것:** dbt 소스가 네 부분 이름을 그대로 쓸 수 있는지 `[확인 필요]`. 안 되면 BigQuery 뷰 하나로 감싸고 dbt는 그 뷰를 읽는다.

## E6. Silver 계산 엔진 (Spark 확정)

Spark는 사용자 요구로 정했다. 여기서는 "왜 이 작업에 Spark가 맞나"를 적는다. 면접에서 가장 먼저 받을 질문이라서다.

- **상황:** 일주일치는 약 3,400만 이벤트로 예상한다(하루 491만 × 7). 이 정도는 BigQuery SQL만으로도 몇 초 만에 끝난다.
- **선택지:**
  - **A. Spark.**
    - 장점: 무거운 부분은 lockstep 계산이다. 같은 시간대에 함께 star를 누른 계정 쌍을 찾는 **자기 조인**이라서 인기 저장소에 쌍이 몰린다. 셔플과 쏠림이 실제로 생기는 작업이고, Spark에서는 그 과정이 스테이지와 태스크 단위로 보인다.
    - 장점: 사전 실험에서 걸어 둔 "계정당 50개 상한"은 이 쏠림을 피하려던 임시 처방이다. 데이터를 늘리며 이 상한을 풀었을 때 무슨 일이 생기는지가 이 프로젝트의 엔진 이야기다.
    - 단점: 지금 규모만 보면 과하다. 이 점은 숨기지 않고 적는다.
  - **B. BigQuery SQL만.**
    - 장점: 가장 싸고 단순하다. 당근도 BigQuery 안에서 SQL로 처리하는 구조다.
    - 단점: 셔플과 쏠림이 슬롯 안에 숨는다. 실행 계획은 보이지만 메모리와 태스크 단위 원인을 쫓을 수 없다.
  - **C. DuckDB.**
    - 장점: 사전 실험이 DuckDB였다. 한 대에서 빠르다.
    - 단점: 분산이 아니다.
- **추천 (역할 나누기):**
  - 무거운 집계(events, entity_features, 근거 계산)는 Spark.
  - 같은 SQL을 BigQuery와 DuckDB로도 한 번씩 돌려 **비교 기준선**으로 둔다.
  - 서울 동네 찾기의 결정 94(규모가 아니라 병목의 원인과 처방을 잰다)를 그대로 따른다.
- **이미 있는 것:** 서울 동네 찾기 Spark 작업 이미지(Spark 3.5.9), SQL 파일 + 실행기 구조(결정 88), 오류 분석 형식(`failure_analysis.md`).
- **검증:** 2024-07-15 하루치로 의심 저장소 276개가 다시 나오는지 본다.
- **남은 것:** 비교 기준선을 언제 돌릴지. 일주일치를 끝낸 뒤 한 번 돌린다.

## E7. Spark 실행 방식과 버전

- **상황:**
  - Managed Service for Apache Spark(옛 Dataproc)에는 클러스터 방식과 서버리스 방식이 있다.
  - 무료 체험은 CPU 8개 제한이다. 서버리스 기본 구성은 12 vCPU로 계산돼 막힐 가능성이 높다(`gcp_research.md` 1절, 첫 실행에서 확인).
- **선택지 (방식):**
  - **A. 클러스터 방식, 작업 때만 만들고 끝나면 지운다.**
    - 장점: YARN, Spark UI, 노드 설정을 직접 본다. 엔진 깊이를 보여 주기 가장 좋다.
    - 장점: 8 CPU 안에 들어가게 짤 수 있다. 예: 마스터 e2-standard-2 1대 + 워커 e2-standard-2 2대 = 6 vCPU.
    - 단점: 클러스터를 만드는 데 몇 분 걸린다. 지우는 걸 잊으면 돈이 샌다. 그래서 유휴 자동 삭제를 꼭 건다.
  - **B. 서버리스.**
    - 장점: 관리할 것이 없다. 쓴 만큼만 낸다.
    - 단점: 무료 체험 제한에 걸릴 가능성이 높다. 노드 안쪽이 덜 보인다.
  - **C. VM에 Spark를 직접 설치하거나 GKE 위 Spark.**
    - 장점: 가장 많이 보인다. 나중에 k8s 단계와 이어진다.
    - 단점: 지금 할 일이 아니다. k8s는 본체를 끝낸 뒤로 이미 정했다.
- **선택지 (이미지 버전):**
  - **3.0 이미지:** Spark 4.1.2, Iceberg 1.11.0, Java 21, Scala 2.13. Iceberg REST 카탈로그를 기본 지원한다. 공식 문서도 보안 요건에는 3.0 이상을 권한다.
    - 주의 1: 머신 타입을 지정하지 않으면 N4, N2, E2 중 가용한 것을 자동으로 고른다. CPU 제한 때문에 꼭 직접 지정한다.
    - 주의 2: Spark 4는 ANSI 모드가 기본이라 잘못된 형 변환이 조용히 NULL이 되지 않고 오류가 난다 `[확인 필요: 실행 시 확인]`.
  - **2.3 이미지:** Spark 3.5.3, Java 11, Iceberg 기본 1.6.1 (속성으로 1.10 선택 가능). 서울 동네 찾기(Spark 3.5.9)와 같은 3.5 계열이다.
- **추천:** A + 3.0 이미지.
  - 방식: 이 프로젝트의 목적이 엔진 안을 보는 것이라 A가 맞다.
  - 버전: 새로 시작하는 프로젝트라 지원 기간이 긴 최신 계열로 간다. 서울 동네 찾기 코드는 PySpark와 Spark SQL이라 옮기는 비용이 작다.
  - 서울 동네 찾기 측정과 같은 버전으로 비교하고 싶으면 2.3으로 바꾼다. 바뀌는 것은 이미지 이름과 Iceberg 버전 한 줄이다.
- **이미 있는 것:** Spark 작업 코드, 메모리 상한과 OOM 분석 경험(F-19, 결정 110).
- **검증:** 첫 클러스터를 만들 때 CPU 할당량 오류 여부를 적는다. 막히면 `decisions.md` 결정 1의 "다시 볼 조건"대로 유료 전환을 검토한다(크레딧은 유지된다).
- **남은 것:** 워커 수와 머신 크기. 일주일치를 돌려 보고 측정으로 정한다.

## E8. 지휘 (Airflow 위치)

- **상황:** Airflow는 정했다. 남은 건 어디에 띄우느냐다.
- **선택지:**
  - **A. Compute Engine VM 하나에 Docker Compose로 띄운다.**
    - 장점: 서울 동네 찾기의 Airflow 구성(지휘와 계산 분리, 결정 109)을 거의 그대로 올린다.
    - 장점: 무거운 계산은 Spark 클러스터와 BigQuery로 나가니 VM은 작아도 된다. e2-standard-2(2 vCPU, 8GB)는 서울 리전 기준 시간당 약 0.087달러(제3자 집계)이고, 작업 안 할 때 멈추면 더 줄어든다.
    - 단점: 업그레이드, 백업, 보안 패치를 직접 한다.
  - **B. Managed Service for Apache Airflow (Cloud Composer 3).**
    - 장점: 관리가 없다. Airflow 3가 2026-04-15부터 GA다.
    - 단점: 작업이 없어도 기본 구성요소가 계속 돈을 쓴다. 외부 추정으로 최소 월 300달러 이상이라 무료 크레딧 전부와 비슷하다.
  - **C. Cloud Scheduler + Cloud Run Jobs.**
    - 장점: 가장 싸다.
    - 단점: Airflow를 쓰는 게 요구사항이다. 재시도, 백필, 의존 관계를 다 새로 만들어야 한다.
- **추천:** A. 비용 차이가 너무 크다. 운영 부담은 서울 동네 찾기에서 이미 겪은 것이다.
  - 회사에서는 B를 쓰는 경우가 많다는 점은 글에 같이 적는다. "왜 관리형을 안 썼나"에 대한 답이 비용과 학습 목적이다.
- **이미 있는 것:** `docker-compose.yml`, `Dockerfile`의 Airflow 3.3.2 이미지, DockerOperator 분리, 메모리 상한, 고아 컨테이너 정리.
- **검증:** VM에서 Airflow를 띄우고 빈 DAG가 Spark 클러스터를 만들었다 지우는지 본다.
- **남은 것:**
  - VM과 Spark 클러스터를 합쳐 8 vCPU 안에 들어가는지 (2 + 6 = 8, 여유 없음).
  - Airflow용 Google 연동 패키지는 새 이름(`ManagedSparkCreateClusterOperator` 등)을 쓴다. 예전 이름(`DataprocCreateClusterOperator`)도 호환용으로 남아 있다.

## E9. Gold 변환과 저장

- **상황:**
  - Gold는 분석팀이 받는 곳이다. 정책별 실유저 마트, 평가, 영향, 불일치가 있다.
  - 지금 설계 문서에는 "PostgreSQL + dbt"로 적혀 있다. 로컬 기준이라 GCP로 오면서 다시 정한다.
- **선택지:**
  - **A. BigQuery 기본 표 + dbt-bigquery.**
    - 장점: 분석팀이 실제로 쓰는 곳이 웨어하우스다. BigQuery를 쓰기로 한 목적과도 맞다.
    - 장점: Silver Iceberg 표를 BigQuery가 바로 읽으니 옮길 필요가 없다.
    - 장점: dbt의 컴파일 SQL이 그대로 리니지 입력이 된다(E10).
    - 단점: 쿼리마다 스캔 비용이 든다. 파티션과 클러스터링을 처음부터 걸어야 한다.
  - **B. PostgreSQL(Cloud SQL) + dbt-postgres.**
    - 장점: 서울 동네 찾기와 같다.
    - 단점: Silver를 한 번 더 옮겨야 한다. 켜 두는 비용이 든다. BigQuery를 쓰는 의미가 없어진다.
  - **C. Gold도 Spark가 Iceberg로 만든다.**
    - 장점: 엔진이 하나로 준다.
    - 단점: 분석팀이 SQL로 고치기 어렵다. 정책 조합은 자주 바뀌는 부분이라 SQL로 두는 게 맞다.
- **추천:** A.
  - **당근 참고:** 당근은 Airflow와 dbt로, 데이터 엔지니어가 아닌 구성원도 BigQuery 위에서 직접 변환 작업을 할 수 있게 했다. dbt 모델마다 담당자, 태그, 컬럼 테스트를 붙이는 방식이다.
  - 이 프로젝트의 "정책은 SQL로, 근거 계산은 Spark로" 나눔과 같은 생각이다. 분석팀이 정책을 바꿔도 근거 계산은 안 바뀐다.
- **이미 있는 것:** 서울 동네 찾기 dbt 프로젝트 구조(dbt-core 1.12.6), 데이터 계약과 manifest, 관문 검사(결정 75~77). 어댑터만 postgres에서 bigquery로 바뀐다(dbt-bigquery 최신 1.12.1).
- **검증:** 정책 3종 마트의 계정 수 합이 entity 수와 맞는지 dbt 테스트로 본다.
- **남은 것:** 분석팀이 받을 형태. 이벤트 단위 마트와 계정 판정 표 중 무엇을 기본으로 줄지는 설계 결정 2(판정 단위)와 같이 정한다.

## E10. 컬럼 리니지

- **상황:** 규칙 하나를 바꿨을 때 다시 돌릴 범위를 정하는 데 쓴다. 입력은 Spark SQL 파일과 dbt 컴파일 SQL 두 가지다.
- **선택지:**
  - **A. 서울 동네 찾기 추출기(sqlglot 파싱)를 가져와 방언만 추가한다.**
    - 장점: 이미 돌아간다. 행 관계(필터, 조인) 간선까지 있다(결정 46, 68, 80).
    - 단점: BigQuery 방언에서 sqlglot이 못 푸는 구문(절차형 SQL, STRUCT 펼치기)이 있다. 우리 SQL에서 그런 구문을 안 쓰면 된다.
  - **B. GCP 기본 데이터 리니지 기능.** Spark 클러스터는 `dataproc:dataproc.lineage.enabled=true` 속성으로 켠다.
    - 장점: 켜기만 하면 작업 실행 기록에서 리니지가 쌓인다.
    - 단점: 실행된 뒤에만 생긴다. 바꾸기 **전에** 영향을 미리 보는 목적과 안 맞는다.
  - **C. BigQuery 쿼리 로그(`INFORMATION_SCHEMA.JOBS`)를 파싱한다.**
    - 장점: 실제로 실행된 SQL을 다 본다.
    - 단점: Spark 쪽 SQL은 이 로그에 없다.
- **추천:** A.
  - **당근 참고:** 당근의 컬럼 레벨 리니지 구축기는 BigQuery 쿼리 로그를 sqlglot으로 파싱하고, Spark로 병렬 처리하고, Airflow로 돌리고, 원본 표와 목적별 뷰로 나눠 MCP로 제공한다.
  - 당근이 로그를 택한 이유는 쿼리가 여러 조직에서 흩어져 들어오기 때문이다. 여기는 변환 SQL이 레포 한 곳에 모여 있으니 서울 동네 찾기 결정 41처럼 코드를 읽는다. C는 "로그와 코드가 같은지" 대조하는 검증용으로만 둔다.
- **이미 있는 것:** `lineage/` 패키지 전체, MCP 서버(결정 69), 변이 시험(결정 84).
- **검증:** lockstep_ratio를 v2에서 v3로 바꿨을 때 리니지가 찍는 범위가 `img/lineage.png` B 패널과 같은지 본다.
- **남은 것:** 없음.

## E11. 조회 화면 (나중에)

- Looker Studio(무료, BigQuery 바로 연결)와 VM 위 Metabase(서울 동네 찾기와 같음) 중에서 고른다.
- Gold가 생긴 뒤에 정한다. 지금 정할 이유가 없다.

## 2. 한눈에 보기

| 층 | 추천 엔진 | 서울 동네 찾기 | 바뀌는 것 |
|---|---|---|---|
| Bronze | GCS 버킷 (시간별 원본 그대로) | 로컬 `lake/` | 경로 접두어 |
| Silver 형식 | Iceberg | Iceberg | 없음 |
| Silver 카탈로그 | Lakehouse runtime catalog (REST) | JDBC (PostgreSQL) | 접속 설정 |
| Silver 계산 | Spark (클러스터 방식, 3.0 이미지) | Spark 3.5.9 컨테이너 | 실행 위치, 버전 |
| 비교 기준선 | BigQuery SQL, DuckDB | DuckDB | BigQuery 추가 |
| 지휘 | Airflow 3 (VM 위 Docker Compose) | Airflow 3.3.2 (로컬 Docker Compose) | 위치, Spark 연산자 |
| Gold | BigQuery + dbt-bigquery | PostgreSQL + dbt-postgres | 어댑터 |
| 리니지 | sqlglot 추출기 | 같음 | 방언 추가 |
| 조회 | 나중에 | Metabase | 미정 |

## 출처

- `gcp_research.md` 1~9절 (2026-11-16 조사, 출처 39개)
- Managed Service for Apache Spark 3.0 이미지 구성: https://docs.cloud.google.com/managed-spark/docs/concepts/versioning/image-release-3.0
- Managed Service for Apache Spark 2.3 이미지 구성: https://docs.cloud.google.com/managed-spark/docs/concepts/versioning/image-release-2.3
- Spark에서 Lakehouse runtime catalog REST 접속 설정: https://docs.cloud.google.cn/lakehouse/docs/spark-support?hl=en
- Spark와 Lakehouse runtime catalog로 레이크하우스 만들기: https://docs.cloud.google.com/managed-spark/docs/guides/create-lakehouse
- REST 카탈로그와 기존 커스텀 카탈로그 비교 (REST 권장): https://docs.cloud.google.com/lakehouse/docs/about-custom-iceberg-catalog-endpoint
- BigQuery 공개 데이터셋 위치: https://docs.cloud.google.com/bigquery/docs/dataset-gsod
- Airflow Google 연동의 Managed Spark 연산자 (새 이름과 호환 이름): https://airflow.apache.org/docs/apache-airflow-providers-google/22.3.0/operators/cloud/dataproc.html
- dbt 호환 릴리스 (dbt-bigquery 1.12.1): https://docs.getdbt.com/docs/dbt-versions/compatible-track-changelog
- 당근, dbt와 Airflow로 데이터 변환 작업 넓히기: https://medium.com/daangn/61250a9904ab (자동 요청이 막혀 검색 요약으로 확인)
- 당근, 컬럼 레벨 리니지 구축기 (2025-12-04): 서울 동네 찾기 `docs/judgment_log.md`에 읽은 내용 기록
- sqlglot BigQuery 절차형 SQL 한계: https://www.dpriver.com/blog/2026/04/why-your-datahub-bigquery-lineage-silently-breaks-on-procedural-sql-and-how-to-f/
