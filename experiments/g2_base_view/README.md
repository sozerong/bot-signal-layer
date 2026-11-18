# G2: base를 view로 두면 하류가 내는 비용

설계 문서 6.1절 G2. 관련 결정: D4(BigQuery 적재 구역), D7(base 물리화).

## 1. 원한 장점 (A를 고른 이유)
base를 view로 둔다. 저장이 0이고, 만들고 갱신하는 작업도 없다. 매핑을 고치면 view 정의만 바꾸면 끝난다.
dbt 가이드의 staging 기본값도 view다.

## 2. 예상 문제 (실행 전에 쓴다)
- 하류 쿼리 하나가 view에서 스캔하는 양은 table의 몇 배일까:
- q3(하루 × 저장소 1개)처럼 아주 좁은 쿼리에서도 차이가 날까:
- base_table을 만드는 비용(CTAS 1회 스캔)은 하류 쿼리 몇 번이면 본전일까:
- 이유:

## 3. 준비 (한 번만, 직접 해야 하는 것)
1. **GCP 프로젝트를 만들고 결제 계정을 연결한다.** 무료 사용량(쿼리 월 1TiB, 저장 10GiB)은 그대로 적용된다. 샌드박스(결제 계정 없음)로는 이 실험과 D7이 안 된다.
   - 샌드박스에서는 모든 표와 파티션이 60일 뒤 만료된다. 파티션 만료는 **파티션 날짜** 기준으로 계산하고, 만료 기간보다 오래된 파티션에 쓰면 즉시 만료된다. 그래서 2025년 `dt` 파티션은 올리자마자 사라진다(두 문서를 합쳐 추론한 것이다. 샌드박스 문서가 직접 말하지는 않는다).
   - 샌드박스는 DML(INSERT, MERGE 등)을 지원하지 않는다. dbt `insert_overwrite`(D7)는 MERGE를 쓴다.
   - 출처: [BigQuery 샌드박스](https://docs.cloud.google.com/bigquery/docs/sandbox), [파티션 표 관리(만료 계산 규칙)](https://docs.cloud.google.com/bigquery/docs/managing-partitioned-tables)
   - 안전장치: 프로젝트에 **예산 알림**을 걸어 둔다(예: 1달러). 키트도 `--run` 한 번에 5GB 넘게 청구될 쿼리는 BigQuery가 거부하도록 걸어 두었다(`maximum_bytes_billed`).
2. [gcloud CLI](https://cloud.google.com/sdk/docs/install)를 설치하고 로그인한다.
   ```
   gcloud auth application-default login
   ```
3. 프로젝트 ID를 환경 변수로 둔다. PowerShell에서는 `$env:GOOGLE_CLOUD_PROJECT = "내-프로젝트-id"`. Python 클라이언트(`google-cloud-bigquery`)는 이미 설치해 두었다.

## 4. 실행 (프로젝트 루트에서)
```
python experiments/g2_base_view/g2.py prep          # raw에서 Watch/Fork/Create/Release만 골라 적재 파일 생성 (로컬)
python experiments/g2_base_view/g2.py load          # landing 표에 올림 (무료)
python experiments/g2_base_view/g2.py build         # base_view와 base_table 생성
python experiments/g2_base_view/g2.py measure       # dry-run 스캔 바이트 (무료)
python experiments/g2_base_view/g2.py measure --run # 실제 실행: 청구 바이트, 슬롯 시간, 걸린 시간
```
- 손잡이: `prep --files N`으로 데이터 크기를 바꾼다. 기본값은 raw에 있는 파일 전부다(D13 표본 22개).
- `load`는 올린 행 수와 로컬에서 고른 줄 수를 비교한다. 어긋나면 어디선가 버려진 것이다(샌드박스 파티션 만료 등).
- prep 동작 확인(2026-11-12, 파일 1개): 27만 줄 중 4만 354줄을 골랐다. 종류별 건수는 전체 파싱으로 센 값과 같다. 적재 파일은 5.1MB(gz)다.

### 하류 쿼리 3개
| 이름 | 모양 | 무엇을 보려는가 |
|---|---|---|
| q1 | 저장소별 star 수 | fact 집계처럼 넓게 읽는 쿼리 |
| q2 | star만 찍은 계정 수 | 신호 S-A 모양, 계정별 그룹 |
| q3 | 하루 × 저장소 1개 | 점검 창구처럼 아주 좁은 쿼리. 파티션과 클러스터링이 듣는지 |

### 내부를 보는 계기
- **dry-run 스캔 바이트**: 실행 전에 BigQuery가 계산한 "읽을 양"이다. 무료다. 이게 곧 요금이다(온디맨드는 읽은 바이트로 청구).
- **청구 바이트(`--run`)**: dry-run과 다를 수 있다. 왜 다른지 본다.
- **슬롯 시간(`--run`)**: 실제로 쓴 계산량이다. 바이트가 같아도 다를 수 있다.
- **BigQuery 콘솔 → 쿼리 기록 → 실행 세부정보**: 단계별(Stage) 입력 행 수, 읽은 바이트, 각 단계에서 가장 오래 걸린 작업자(max)와 평균(avg). view 쿼리에서 JSON 파싱이 어느 단계에서 일어나는지 본다.
- **표 세부정보 → 저장 정보**: landing과 base_table의 크기. table로 만들면 저장이 몇 벌이 되는지 본다.

| 쿼리 | base | dry-run 스캔 | 청구 | 슬롯 시간 | 걸린 시간 |
|---|---|---|---|---|---|
| q1 | view | | | | |
| q1 | table | | | | |
| q2 | view | | | | |
| q2 | table | | | | |
| q3 | view | | | | |
| q3 | table | | | | |

| | 행 | 크기 | 만드는 데 스캔 |
|---|---|---|---|
| landing | | | (load, 무료) |
| base_table | | | |

## 5. 실제로 본 것 (숫자)


## 6. 해결 (B)와 다시 잰 숫자
> 막히면 Claude에게 힌트를 요청한다.


## 7. 지킨 장점 / 낸 대가


## 서사 한 줄
[장점]을 원해서 [A]를 썼다 → 돌려 보니 [숫자]만큼 [문제]가 생겼다 → [B]로 바꿨다 → [숫자]로 줄었고 [장점]은 그대로다 → 대가로 [C]를 냈다.
