# GCP 조사: 봇 프로젝트를 GCP에서 돌릴 때 알아야 할 것

조사일: 2026-11-16. 모든 값은 이날 웹에서 확인한 것이고, 출처는 각 항목 끝 번호(맨 아래 목록)로 단다. 가격은 별도 표시가 없으면 미국 리전(us-central1 또는 US) 기준이다. 서울 리전 가격은 다를 수 있으니 실제 사용 전에 공식 가격표에서 다시 확인한다.

## 0. 먼저 알아야 할 것: 2026년에 이름이 많이 바뀌었다

| 예전 이름 | 지금 이름 | 비고 |
|---|---|---|
| Dataproc (클러스터 + Serverless) | **Managed Service for Apache Spark** | 2026년 4월부터 두 방식을 묶은 이름 [5][6] |
| Cloud Composer | **Managed Service for Apache Airflow** (세대 이름은 Cloud Composer 3 그대로) | [8][9] |
| BigLake | **Lakehouse** | 2026-04-20부터. API, CLI, IAM 이름은 그대로 BigLake [11] |
| BigLake metastore | **Lakehouse runtime catalog** | 같은 날 변경 [11] |
| BigQuery tables for Apache Iceberg | **Apache Iceberg managed tables** | [12][13] |

예전 이름으로 된 블로그 글과 명령어가 섞여 있으니, 검색할 때 두 이름을 모두 쓴다.

## 1. 무료 체험과 꼭 알아야 할 제한

- 신규 고객은 90일, 300달러 크레딧을 받는다. 체험 계정 상태에서는 GPU 추가와 **할당량 증액 요청을 할 수 없다** [1][2].
- **Spark에 직접 걸리는 제한:** Compute Engine 무료 체험 프로젝트는 CPU 8개로 제한되고, 늘리려면 결제를 활성화해야 한다 [7].
- Serverless Spark는 드라이버 코어를 4, 8, 16 중에서만 고를 수 있고 [14], 기본 executor 수는 2다 [15]. 공식 문서의 예시 구성(드라이버 4코어, 4코어 executor 2개)으로 계산하면 4 + 4 × 2 = 12 vCPU다 [4][7]. **따라서 무료 체험 상태 그대로는 Serverless Spark 기본 구성이 8 CPU 제한에 걸린다.** 이건 문서를 조합해 추론한 것이라 `[확인 필요: 첫 실행에서 실제로 확인]`.
- 대안 두 가지:
  - 유료 계정으로 업그레이드한다. 업그레이드해도 남은 크레딧은 만료일까지 유지되고 제한이 바로 풀린다 [3].
  - 체험 상태에서는 작은 VM으로 만든 **클러스터 방식** Spark를 8 vCPU 안에서 구성한다.

## 2. 저장: Cloud Storage

- Standard 등급 기준 미국 리전은 GB당 월 0.020달러부터 [17], 서울(asia-northeast3)은 GiB당 월 0.023달러 [18].
- GH Archive 하루치가 압축 2.6GB였으니 한 달치 원본 보관비는 월 몇 달러 수준으로 추정된다. 받아 보고 실측한다.

## 3. BigQuery

### 요금
- 온디맨드 쿼리는 스캔한 TiB당 6.25달러이고, 매달 처음 1TiB는 무료다. 저장은 매달 10GiB 무료 [19][20].
- 무료 1TiB는 프로젝트가 아니라 결제 계정 단위로 적용된다 [21].
- 활성 논리 저장은 GiB당 월 약 0.02달러, 90일 동안 안 바뀐 데이터는 약 0.01달러 [22].
- 정해진 비용이 필요하면 슬롯 방식(Standard 에디션 슬롯-시간당 0.04달러부터)도 있다 [19].

### 샌드박스 (결제 계정 없이)
- 신용카드 없이 쓸 수 있지만, 모든 테이블, 뷰, 파티션이 **60일 뒤 자동으로 사라진다** [23]. 데이터 스트리밍, DML, Data Transfer Service도 쓸 수 없다 [24].
- 이 프로젝트는 DML(규칙 버전 갱신)이 필요하고 표가 남아 있어야 하므로 **샌드박스로는 안 된다.** 무료 체험 결제 계정을 연결해 무료 사용량 안에서 쓴다.

### GH Archive가 BigQuery에 있다
- GH Archive 전체가 BigQuery 공개 데이터셋으로 있고 매시간 자동 갱신된다. year, month, day 데이터셋으로 나뉜다 [25].
- 표 이름 예: `githubarchive.day.20240201`, `githubarchive.month.202402` [26].
- **위치(리전) 주의:**
  - 쿼리는 읽는 데이터셋이 있는 위치에서 돈다. 서로 다른 위치의 데이터셋을 한 쿼리에서 읽으려면 global queries가 켜져 있어야 한다 [27].
  - 위치를 지정하지 않으면 BigQuery는 작업을 US 멀티리전에서 돌린다. 서울 리전 데이터셋을 와일드카드 표로 조회하면 오류 없이 0행이 나온 사례가 있다 [28].
  - githubarchive 데이터셋의 위치는 `[확인 필요: bq show githubarchive:day로 확인]`. US라면 우리 BigQuery 데이터셋과 GCS 버킷도 US 쪽에 둬야 조인이 편하다.
  - 지역 간 조인을 해 주는 BigQuery Global Queries는 2026년 2월 기준 Preview였다 [29].

### BigQuery를 쓰면 바뀌는 설계 지점
- Bronze 수집을 두 가지로 할 수 있다.
  - (a) 시간별 파일을 받아 GCS에 올린다 (지금 방식).
  - (b) BigQuery 공개 데이터셋에서 필요한 컬럼만 뽑는다. 이 경우 스캔 비용은 고른 컬럼 크기에 비례하므로, 실행 전에 dry run으로 바이트를 확인한다.
- (b)는 편하지만 "원본을 받은 그대로 보관한다"는 원칙과 충돌한다. 공개 데이터셋은 우리 것이 아니라서, 언제든 바뀌거나 사라질 수 있다. 원본 보존은 (a)로 하고, (b)는 탐색과 검증용으로 쓰는 게 원칙에 맞다.

## 4. Iceberg: Lakehouse (옛 BigLake)

- Iceberg 테이블은 GCS에 저장되고, Lakehouse runtime catalog가 테이블 목록을 관리한다. BigQuery와 Spark가 같은 테이블을 복사 없이 함께 쓴다 [12][30].
- 방식은 두 가지다.
  - **Apache Iceberg managed tables:** BigQuery가 쓰고 관리한다. Spark 같은 오픈소스 엔진은 내보낸 Iceberg 메타데이터로 읽는다 [13].
  - **Iceberg REST catalog 테이블:** Spark가 REST 카탈로그로 쓰고, BigQuery는 `project.catalog.namespace.table` 네 부분 이름으로 읽는다 [31]. 공식 빠른 시작 문서가 바로 이 흐름이다. Managed Service for Apache Spark로 Iceberg 테이블을 만들고 BigQuery에서 조회한다 [32].
- 2026-06-25부터 runtime catalog의 Iceberg 테이블에 대한 BigQuery 쓰기(DML, DDL)와 자동 테이블 관리가 Preview로 나왔다. 2026-06-11부터는 Iceberg V3 테이블(Binary Deletion Vectors)도 Preview다 [33].
- dbt는 BigQuery에서 `table_format = 'iceberg'` 설정 하나로 모델을 Lakehouse 카탈로그의 Iceberg 테이블로 만들 수 있다 [34].

**이 프로젝트에 맞는 쪽:** Silver는 Spark가 쓰므로 **REST catalog 테이블**, Gold는 dbt가 BigQuery에서 만든다. 이러면 Silver의 Iceberg 스냅샷(되돌리기, 그때 판정 재현)과 BigQuery 조회를 둘 다 얻는다.

## 5. Spark: Managed Service for Apache Spark (옛 Dataproc)

| | Serverless | 클러스터 |
|---|---|---|
| 과금 | DCU-시간 (Standard 0.06달러, Premium 0.089달러), 셔플 저장 별도, 1분 최소 [5] | Compute Engine VM 비용 + 관리비 vCPU-시간당 0.01달러 [5] |
| 장점 | 작업할 때만 과금, 클러스터 관리 없음 | YARN, Spark UI, 노드 설정을 직접 볼 수 있음. 엔진 깊이를 파기 좋음 |
| 단점 | 무료 체험의 8 CPU 제한에 걸림 (1절). 내부가 덜 보임 | 켜 둔 시간만큼 과금. 작업 후 꼭 지워야 함 |

- Premium 등급은 Lightning Engine으로 돌아가고, 오픈소스 Spark보다 최대 4.9배 빠르다고 주장한다 [5]. 클러스터용 Lightning Engine 추가 요금은 2026-06-01부터 vCPU-시간당 0.0025달러다 [5].
- **엔진 깊이를 증명하는 게 목표라면 비교 기준은 오픈소스 Spark여야 한다.** Lightning Engine은 별도 비교 항목으로만 둔다.

## 6. Airflow: Managed Service for Apache Airflow (Cloud Composer 3)

- Airflow 3는 2026-04-15부터 Composer 3에서 GA다 [10]. 2026-10-20 기준 composer-3-airflow-3.3.1 빌드가 있다 [9].
- 과금 단위는 DCU-시간당 0.06달러다 [8]. 다만 작업이 없어도 기본 구성요소가 DCU를 쓰고 [35], 외부 추정으로는 최소 구성도 월 약 300달러 이상이다 [36]. 이 숫자는 공식 값이 아니라서 `[확인 필요: 가격 계산기]`.
- **대안:** 서울 동네 찾기의 Docker Compose(Airflow 지휘와 계산 분리)를 Compute Engine VM 하나에 올리고, 계산은 Spark 작업으로 보낸다. 작업 안 할 때 VM을 멈추면 비용이 크게 줄어든다. 서울 리전 e2-standard-2(2 vCPU, 8GB)는 시간당 약 0.087달러로 나온다 [37]. 이것도 제3자 집계라 공식 가격표로 다시 확인한다.

## 7. 비용 안전장치

- 예산 알림만으로는 사용량이나 지출이 **자동으로 막히지 않는다** [38].
- 실제로 멈추려면 공식 문서의 방식을 쓴다. 예산 알림을 Pub/Sub로 보내고, Cloud Run 함수가 Cloud Billing API로 프로젝트 결제를 끊는다. 다만 결제를 끊으면 무료 사용량 서비스까지 포함해 프로젝트의 모든 서비스가 멈춘다 [39].
- 첫날 할 일: 예산 알림(50%, 90%, 100%), 결제 끊기 함수, 클러스터 자동 삭제(유휴 시간 설정).

## 8. 나중에 쿠버네티스로 갈 때

- GKE는 클러스터당 시간당 0.10달러지만, 결제 계정마다 월 74.40달러 크레딧이 있어 Autopilot 또는 zonal Standard 클러스터 하나는 상쇄된다 [16].

## 9. 지금 구조를 GCP에 대응시키면 (결정 대기)

| 층 | 범용 후보 | GCP 대응 | 결정할 것 |
|---|---|---|---|
| Bronze | lake/ 파일 | GCS 버킷 | 리전 (GH Archive BigQuery 위치에 맞출지) |
| Silver | Iceberg | Lakehouse runtime catalog의 Iceberg REST 테이블 | 카탈로그 방식 |
| 계산 | Spark | Managed Service for Apache Spark | Serverless / 클러스터 |
| Gold | PostgreSQL + dbt | BigQuery + dbt | 거의 확정 (BigQuery 사용이 목적) |
| 지휘 | Airflow | Composer 3 / VM 위 Docker Compose | 비용 대비 |
| 조회 | Metabase | Looker Studio / Metabase(VM) | 나중에 |

## 출처

1. Google Cloud, Free Google Cloud features and trial offer: https://docs.cloud.google.com/free/docs/free-cloud-features
2. Is Google Cloud Free? (2026): https://www.lilachbullock.com/is-google-cloud-free/
3. How to Get $300 in Google Cloud Free Credits (2026): https://aicreditmart.com/ai-credits-providers/how-to-get-300-in-google-cloud-free-credits-2026-guide/
4. Managed Service for Apache Spark serverless quotas: https://docs.cloud.google.com/managed-spark/quotas-serverless
5. Managed Service for Apache Spark pricing: https://cloud.google.com/products/managed-service-for-apache-spark/pricing
6. innFactory, Managed Service for Apache Spark: https://innfactory.de/en/cloud/gcp/products/dataproc/
7. Managed Service for Apache Spark quotas: https://docs.cloud.google.com/dataproc-serverless/quotas
8. Managed Service for Apache Airflow pricing: https://cloud.google.com/products/managed-service-for-apache-airflow/pricing
9. Managed Service for Apache Airflow version list: https://docs.cloud.google.com/composer/docs/composer-versions
10. Managed Service for Apache Airflow release notes: https://docs.cloud.google.com/composer/docs/release-notes
11. About the Lakehouse runtime catalog: https://docs.cloud.google.com/lakehouse/docs/about-lakehouse-catalogs
12. Atlan, Apache Iceberg in Google BigQuery: https://atlan.com/know/iceberg/apache-iceberg-bigquery/
13. Apache Iceberg managed tables: https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery
14. Spark properties (serverless): https://docs.cloud.google.com/dataproc-serverless/docs/concepts/properties
15. Tune Spark properties to optimize Dataproc Serverless jobs: https://cloud.google.com/blog/products/data-analytics/tune-spark-properties-to-optimize-dataproc-serverless-jobs/
16. Managed Kubernetes pricing comparison: https://oneuptime.com/blog/post/2026-02-09-managed-kubernetes-pricing-comparison/markdown
17. CloudZero, GCP storage pricing 2026: https://www.cloudzero.com/blog/gcp-storage-pricing/
18. Object Storage 단가 (cloud.google.com 값 인용): https://cloud.wibaek.com/category/object-storage/
19. Panoply, BigQuery review 2026: https://panoply.io/tools/bigquery/
20. OWOX, What is Google BigQuery: https://www.owox.com/blog/articles/bigquery-everything-you-need-to-know
21. How Much Does a BigQuery Query Actually Cost: https://followrabbit.ai/blog/how-much-does-a-bigquery-query-actually-cost
22. Activepieces, BigQuery pricing 2026: https://www.activepieces.com/blog/google-bigquery-pricing-explained-2026-cost-guide
23. Enable the BigQuery sandbox: https://cloud.google.com/bigquery/docs/sandbox
24. Estuary, BigQuery sandbox: https://www.estuary.dev/blog/bigquery-sandbox/
25. GH Archive: https://www.gharchive.org/
26. GH Archive BigQuery examples: https://davelester.github.io/gharchive-bigquery-examples/
27. Querylab, BigQuery locations: https://docs.querylab.io/bigquery/locations/
28. growthbook PR #7005: https://github.com/growthbook/growthbook/pull/7005
29. BigQuery Global Queries: https://dev.to/toyama0919/bigquery-global-queries-join-data-across-regions-without-etl-1ho1
30. Use Apache Iceberg tables on GCP with BigLake Metastore: https://oneuptime.com/blog/post/2026-02-17-how-to-use-apache-iceberg-tables-on-gcp-with-biglake-metastore/view
31. Snowflake guide, Snowflake and BigQuery via Iceberg: https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-and-bigquery-via-iceberg/
32. Quickstart: Use the Iceberg REST catalog with Cloud Storage: https://docs.cloud.google.com/lakehouse/docs/use-lakehouse-metastore-iceberg-rest-catalog
33. Lakehouse release notes: https://docs.cloud.google.com/lakehouse/docs/release-notes
34. dbt, BigQuery and Apache Iceberg: https://docs.getdbt.com/docs/build/iceberg/adapters/bigquery-iceberg-support
35. Orchestra, Managed Airflow pricing: https://www.getorchestra.io/guides/mwaa-pricing
36. Tasrie IT, Managed Airflow services compared 2026: https://tasrieit.com/blog/managed-airflow-services-compared-2026
37. Instance pricing, GCP Seoul: https://www.instance-pricing.com/provider=gcp/region=asia-northeast3
38. Create budgets and budget alerts: https://docs.cloud.google.com/billing/docs/how-to/budgets
39. Disable billing usage with notifications: https://docs.cloud.google.com/billing/docs/how-to/disable-billing-with-notifications
