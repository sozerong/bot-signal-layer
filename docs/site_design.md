# 데이터 실험실 화면 (GitHub Pages) 설계

이 문서는 데이터 실험실 화면(브라우저에서 집계를 보고 SQL을 돌리는 페이지)을 왜 이렇게 만들었는지 적은 설계다.
화면을 고치거나 공개 범위를 바꿀 때 읽는다.

작성 2026-11-22. 모든 줄에 근거를 단다. 〔근거: CLAUDE.md 근거 규칙 (2026-11-21)〕

## 0. 한 문장

분석가가 레포, Docker, 14GB 원본 없이 링크 하나로 봇을 거른 숫자를 보고 자기 요구사항을 SQL로 바로 시험해 보는 정적 화면이다. 〔근거: 사용자 2026-11-22 "우리가 만든 데이터를 실제로 여러 요구사항을 넣고 테스트 해볼 수 있는 화면". review_mentor.md 12-4 "Docker 메모리 32GB, 14GB 내려받기가 필요하다"〕

---

## 1. DuckDB를 현업에서 공유 웨어하우스로 잘 쓰지 않는 이유 (현상)

| 현상 | 근거 |
|---|---|
| 프로덕션 배포는 아직 드물고, 로컬 프로토타입과 탐색에 많이 쓴다 | 〔근거: 문서: [dlthub, DuckDB 1M 다운로드 사용자 조사](https://dlthub.com/blog/duckdb-1M-downloads-users) "deployment of DuckDB to production is still quite rare"〕 |
| 다중 사용자 서버, 인증, 자원 격리, 수평 확장을 주지 않는다 | 〔근거: 문서: [Tinybird, DuckDB 대안](https://www.tinybird.co/blog/duckdb-alternatives). 경쟁사 글이라 동기가 있지만, 아래 공식 문서의 동시성 모델과 맞는다〕 |
| 한 파일은 한 프로세스만 쓰고, 여러 프로세스는 읽기 전용으로만 연다 | 〔근거: 문서: [DuckDB 2.0 동시성](https://duckdb.org/docs/2.0/connect/concurrency) "Read-write mode: one process", "multiple processes can read ... but no processes can write"〕 |
| 큰 회사 사례: PostHog가 DuckDB + DuckLake로 웨어하우스를 다시 만든 지 5개월 만에 Trino + 자체 HogLake로 옮겼다(약 2PB, 1조 행, 스냅샷 1,530만 개) | 〔근거: 문서: [PostHog, 2026-11-02](https://posthog.com/blog/moving-off-duckdb). 한 회사의 사례이고, PB 규모 다중 고객 클라우드 서비스라는 조건이 붙는다〕 |
| PostHog가 든 이유: 단일 노드라 쿼리마다 자원을 미리 정하기 어렵다, S3를 비동기로 많이 읽는 기능이 최근에야 생겼다, 카탈로그 통계 때문에 쓰기가 3분을 넘었다, 포크를 유지해야 했다, Postgres 호환 연결이 어려웠다 | 〔근거: 같은 글. 3분 수치는 [글 요약](https://zeli.app/ko/story/50338747) 기준이라 원문 대조 필요〕 |
| 2026년에 바뀐 것: 클라이언트-서버 프로토콜 Quack이 5월 공개, 10월 21일 DuckDB 2.0.0에서 첫 정식판. 여러 프로세스 쓰기를 공식 지원한다 | 〔근거: 문서: [Quack 발표](https://duckdb.org/2026/05/12/quack-remote-protocol), [The New Stack](https://thenewstack.io/?p=22843575), [DuckDB FAQ](https://duckdb.org/faq)〕 |
| 그래도 정식판이 나온 지 한 달이라 운영 사례가 없다. 그래서 "현업에서 안 쓴다"는 2026-11 기준으로는 "공유 서버로는 아직 검증이 짧다"로 고쳐 말하는 것이 정확하다 | 〔근거: 계산: 2026-10-21 → 2026-11-22, 32일. 판단〕 |

## 2. 근본 원인: 알고리즘, 자료구조, 컴퓨터 구조

결함이 아니라 "한 기계에서 분석 쿼리를 가장 빨리"를 고른 설계의 대가다. 각 줄은 설계 → 얻는 것 → 치르는 것 순서다. 〔근거: 판단: 아래 표의 공식 문서와 논문〕

| 층 | 설계 | 얻는 것 | 치르는 것 | 근거 |
|---|---|---|---|---|
| 구조 | 프로세스 안(in-process) 엔진. 서버, 프로토콜, 네트워크가 없다 | 데이터를 프로세스 메모리에 캐시해 왕복 없이 읽는다 | 다른 프로세스와 상태를 나누지 못해 쓰는 프로세스가 하나뿐이다 | 〔근거: 문서: [동시성](https://duckdb.org/docs/2.0/connect/concurrency) "to allow for the caching of data in RAM". [Quack 발표](https://duckdb.org/2026/05/12/quack-remote-protocol) "no client/server, no protocol, just low-level API calls"〕 |
| 자료구조 (MVCC) | HyPer 방식: 행을 제자리에서 고치고 이전 값은 undo buffer에 둔다. 잠금 없는 낙관적 동시성 제어 | 스캔은 최신 값을 그대로 빠르게 읽고, 큰 일괄 수정이 싸다 | 버전 정보(undo buffer)가 그 프로세스 메모리에 있다. 다른 프로세스가 이 버전을 볼 길이 없어 쓰기를 한 프로세스로 묶는다 | 〔근거: 문서: [DuckDB 동시 트랜잭션](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions) "update the table data in-place ... previous version ... in the undo buffers". 논문: [Neumann 외, SIGMOD 2015](https://db.in.tum.de/~muehlbau/papers/mvcc.pdf). "그래서 한 프로세스"는 판단: 공식 문서는 이유를 RAM 캐시로 든다〕 |
| 알고리즘 (실행) | 벡터화 실행. 연산자가 고정 크기 2,048행 묶음(vector)을 한 번에 처리한다 | 묶음이 CPU 캐시에 들어가 루프가 파이프라인과 SIMD를 탄다. 행마다 해석하는 비용이 2,048분의 1로 줄어든다 | 한 행씩 넣고 고치는 일에는 묶음의 고정 비용이 그대로 남는다. 공식 문서도 작은 트랜잭션 다량은 설계 목표가 아니라고 한다 | 〔근거: 문서: [Vector](https://duckdb.org/docs/2.0/internals/vector) "STANDARD_VECTOR_SIZE ... 2048". 논문: [MonetDB/X100, CIDR 2005](https://cs.brown.edu/courses/cs227/archives/2008/Papers/ColumnStores/MonetDB.pdf). 문서: [동시성](https://duckdb.org/docs/lts/connect/concurrency.html) "optimized for bulk operations"〕 |
| 자료구조 (저장) | 행 그룹 122,880행(= 60 × 2,048) 단위, 그 안에서 컬럼별 고정 크기 블록. 행 그룹·컬럼마다 최소·최대(zone map). 가벼운 압축 | 필요한 컬럼만 읽고, 조건에 안 맞는 행 그룹을 통째로 건너뛴다. 고정 블록이라 제자리 ACID 수정이 된다 | 키 하나를 찾는 점 조회용 구조가 아니다. 데이터가 정렬돼 있지 않으면 zone map이 거의 못 건너뛴다 | 〔근거: 문서: [정렬과 선택적 쿼리](https://duckdb.org/2025/05/14/sorting-for-fast-selective-queries.html) "122 880 rows", [경량 압축](https://duckdb.org/2022/10/28/lightweight-compression.html) "columns are split into blocks of a fixed-size ... in-place ACID", [인덱싱](https://www.duckdb.org/docs/current/guides/performance/indexing.html) "The more ordered ... the more valuable"〕 |
| 컴퓨터 구조 (병렬) | morsel 단위 작업 분배: 작은 조각을 스레드가 가져가 파이프라인 끝까지 돌린다 | 한 기계의 코어를 고르게 다 쓴다(논문 32코어에서 평균 30배 넘게) | 한 기계 안의 확장(scale-up)뿐이다. 노드를 늘리는 확장은 없다 | 〔근거: 논문: [Leis 외, SIGMOD 2014](https://db.in.tum.de/~leis/papers/morsels.pdf) "average speedup of over 30 with 32 cores". 판단: 분산 실행 계층이 없다〕 |
| 컴퓨터 구조 (메모리) | 통합 메모리 관리: 영구 데이터와 중간 결과를 같은 버퍼 관리자가 맡고, 한도를 넘으면 임시 폴더로 흘린다(spilling) | 메모리보다 큰 집계와 조인도 한 기계에서 끝난다 | 쿼리 하나가 그 기계의 메모리와 코어를 다 쓰려 한다. 사용자별 자원 격리는 엔진 밖 일이고, 스레드당 최소 125MB가 든다 | 〔근거: 문서: [메모리 관리](https://duckdb.org/2024/07/09/memory-management), [외부 집계](https://duckdb.org/2024/03/29/external-aggregation), [환경](https://duckdb.org/docs/lts/guides/performance/environment.html) "minimum of 125 MB of memory per thread". 판단: 이것이 PostHog가 말한 "자원을 미리 정하기 어렵다"와 Tinybird의 "자원 격리 없음"의 아래층이다〕 |

정리: 여섯 층이 모두 "한 프로세스, 한 기계, 대량 처리"를 향한다. 그래서 한 사람의 분석, 빌드 단계의 변환 엔진, 브라우저 안 엔진에는 맞고, 여러 사람이 동시에 쓰는 공유 서버에는 엔진 밖 장치(Quack, DuckLake의 Postgres 카탈로그, MotherDuck 같은 서비스)가 필요하다. 〔근거: 판단: 위 표. 문서: [DuckDB FAQ](https://duckdb.org/faq) 다중 사용자는 DuckLake 또는 Quack〕

## 3. 이 프로젝트에서 DuckDB의 자리

| 쓰는 곳 | 왜 맞나 | 근거 |
|---|---|---|
| Gold 빌드(dbt-duckdb) | 쓰는 프로세스가 dbt 하나뿐이다 | 〔근거: decisions.md 결정 6. 위 2절 구조 줄〕 |
| 내보내기(`scripts/export_site.py`) | 빌드가 끝난 뒤 `read_only=True`로 연다. 빌드 중이면 잠금으로 실패하는 것이 맞는 동작이다 | 〔근거: 코드: export_site.py `duckdb.connect(..., read_only=True)`. 문서: [동시성](https://duckdb.org/docs/2.0/connect/concurrency)〕 |
| 화면(DuckDB-Wasm) | 여러 사람이 동시에 보지만 각자 자기 브라우저에 엔진이 따로 뜨고, 공유하는 것은 읽기 전용 Parquet 파일뿐이다. 그래서 2절의 쓰기 제약에 걸리지 않는다 | 〔근거: 판단: 정적 파일 + 사용자별 엔진. 문서: [DuckDB-Wasm](https://duckdb.org/docs/1.4/clients/wasm/overview)〕 |
| 클라우드의 공유 계층 | BigQuery로 정했다(결정 20). 화면은 클라우드가 미뤄진 동안 공유를 대신한다 | 〔근거: decisions.md 결정 20. 사용자 2026-11-22 "클라우드에 배포하는건 일단 미뤄두고"〕 |
| Quack | 쓰지 않는다. 정식판이 한 달이고, 지금 공유 요구는 읽기뿐이라 쓰기 서버가 필요 없다 | 〔근거: 1절 마지막 줄. 판단. 반증 조건: 분석가가 화면에서 결과를 저장하고 남과 나누고 싶어 하면 다시 본다〕 |

## 4. 화면 도구 후보

| 후보 | 방식 | 장점 | 약점 | 근거 |
|---|---|---|---|---|
| A. 손으로 쓴 HTML 한 장 + DuckDB-Wasm (채택) | `site/index.html` 하나가 Parquet을 읽는다 | 빌드 단계와 패키지가 없다. SQL 편집기와 요구사항 카드를 마음대로 만든다 | 화면이 늘면 손이 많이 간다. 차트는 직접 그린다 | 〔근거: 코드: site/index.html. 문서: [DuckDB-Wasm](https://duckdb.org/docs/1.4/clients/wasm/overview)〕 |
| B. Evidence | SQL + Markdown으로 페이지를 쓰고, 정적 사이트로 빌드해 브라우저의 DuckDB-Wasm이 Parquet을 읽는다 | 차트, 표, 필터 부품이 있다. GitHub Pages 배포 안내가 있다 | Node 빌드가 붙는다. 자유 SQL 편집기는 기본 부품이 아니다 | 〔근거: 문서: [Evidence 저장소](https://github.com/evidence-dev/evidence), [ClickHouse 블로그](https://clickhouse.com/fr/blog/evidence-bluesky-dashboard) "queries the pre-generated Parquet files using DuckDB WASM"〕 |
| C. Observable Framework | 빌드 때 data loader가 스냅샷을 만들고, 페이지에서 DuckDB SQL을 쓴다 | data loader가 export_site.py 자리를 그대로 대신한다. Mosaic 예시가 있다 | 빌드 때 Gold가 있어야 해서 CI에서 못 돈다(Gold는 로컬 2.2GB) | 〔근거: 문서: [data loaders](https://observablehq.com/framework/data-loaders), [SQL](https://observablehq.com/framework/sql), [Mosaic 예시](https://idl.uw.edu/mosaic-framework-example). 측정: gold.duckdb 2.2GB〕 |
| D. marimo html-wasm | Python 노트북을 WebAssembly HTML로 내보낸다 | 분석가가 Python으로 이어 쓸 수 있다 | 브라우저에서 Python 런타임을 내려받아 첫 화면이 무겁다 | 〔근거: 문서: [marimo GitHub Pages](https://docs.marimo.io/guides/publishing/github_pages). 판단: 런타임 크기는 확인 필요〕 |

- A를 고른 이유: 요구사항 실험실(자유 SQL)이 이 화면의 중심이라 부품보다 편집기 자유도가 중요하고, 의존성 0이 유지보수 위험이 가장 적다. 〔근거: 판단: 사용자 요구 "여러 요구사항을 넣고 테스트". ponytail 원칙: 패키지보다 플랫폼 기능〕
- 반증 조건: 탭이 열 개를 넘거나 차트 요구가 늘면 B(Evidence)로 옮긴다. Parquet과 SQL은 그대로 쓴다. 〔근거: 판단: B도 같은 Parquet을 읽는다〕

## 5. 화면에 넣을 것: 후보와 채택

| 후보 | 출처 (비슷한 것을 보여 주는 곳) | 채택 | 근거 |
|---|---|---|---|
| 요약 카드: 정책을 적용하면 지표가 얼마나 바뀌나 | Cloudflare Radar "Bot vs. Human"은 봇 비율 하나를 맨 위에 둔다(점수 1 ~ 29를 봇으로) | 채택 (요약 탭) | 〔근거: 문서: [Radar 용어집](https://radar.cloudflare.com/glossary), [Bot classes](https://developers.cloudflare.com/radar/concepts/bot-classes)〕 |
| 보는 사람이 거르는 강도를 고른다 | Wikimedia 페이지뷰는 agent를 user, spider(자기 신고), automated(휴리스틱, 2020년 추가)로 나눠 사용자가 고른다. 우리의 lenient(자기 신고)와 strict(행동)와 같은 구조다 | 채택 (모든 탭의 정책 선택) | 〔근거: 문서: [Wikimedia 페이지뷰](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/concepts/page-views.html), [변경 기록](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/changelog.html) "2020-04-29: Automated agent type"〕 |
| 정책 비교(예전 이름 지표 × 정책): 표와 lenient 대비 추가분 | M-26 결과를 그대로 보인다 | 채택 | 〔근거: measurements.md M-26. 모델 policy_compare〕 |
| SQL 콘솔: 왼쪽에 표와 컬럼(타입, 설명은 마우스를 올리면), 표 이름을 누르면 처음 100행, 컬럼을 누르면 커서 자리에 이름, 표마다 컬럼별 요약(DuckDB `summarize`), 표·컬럼 찾기, 고른 부분만 실행, 최근 실행 20개, 결과는 같은 다섯 형식으로 받기 | 분석가가 쓰는 쿼리 도구(BigQuery 콘솔, DBeaver 등)의 기본 배치: 스키마 나무, 편집기, 결과 격자 | 채택 (주 작업면) | 〔근거: 사용자 2026-11-22 12:16 "직접 쿼리던질수있게 내부 스키마랑 이런거 화면에 보여주고". 10절 SQL 근거. 코드: index.html schema(), runSql(). 판단: 스키마는 meta.json이 아니라 DuckDB information_schema에서 읽어 화면의 뷰(policy_compare_released)까지 보인다〕 |
| 요구사항: 요구사항 카드(값을 고르는 칸 + SQL) + 자유 SQL + 엑셀·CSV·JSON·Parquet 받기, 표 복사 | 분석가가 "가져가는 법" README를 읽지 않고 바로 시험한다 | 채택 (중심) | 〔근거: 사용자 2026-11-22 요구. review_mentor.md 12-4〕 |
| 실험 감도 계산기: 상위 몇 %를 자를지와 정책을 바꾸며 MDE를 다시 센다 | M-26의 "봇 거르기 vs 자르기" 비교를 사람이 직접 돌린다 | 채택 | 〔근거: measurements.md M-26 상대 MDE none 101 ~ 138%, lenient 7 ~ 11%. 계산은 mde_cap(백분위마다 미리 센 평균·분산)에서〕 |
| 규칙 묶음 변경 기록 | 공개 묶음에 고정, 새 묶음은 차이를 먼저 본다 | 채택 | 〔근거: decisions.md 결정 24, measurements.md M-28〕 |
| 데이터 사전: 표와 컬럼, 날짜별 상태. 가져가기: 표마다 엑셀, 구글 시트, pandas, DuckDB에서 부르는 주소와 코드. 상태: 표마다 한 행의 뜻, 컬럼, 내보낸 시각, 날짜별 양과 봇 비율을 직전 중앙값과 비교 | 데이터 제품 카드(주인, 갱신 주기, 설명)를 화면으로 | 채택 | 〔근거: direction_v6.md Q9(grain), Q12(같은 문턱 0.5와 2). daangn_review.md "dbt meta owner 없음"〕 |
| 이 계정·저장소는 왜 빠졌나 | 판정을 설명하는 화면 | 로컬에서만 (`?data=data_local`) | 〔근거: direction_v5.md 3-1 "대상별 라벨은 공개하지 않음", 원본에 제3자 권리 대상이 섞일 수 있음([gharchive.org](https://github.com/stjordanis/gharchive.org)). 아래 6절〕 |
| star 상위 100에서 밀려난 저장소 이름 | 순위 변화 | 로컬에서만. 공개 화면은 "100위 안에 남은 개수"만 | 〔근거: 같은 원칙. 이름이 붙으면 저장소 단위 판정이다〕 |
| 실시간 갱신 | 대시보드 | 넣지 않음 | 〔근거: 판단: 원본이 7일 고정 표본이고 정적 사이트다〕 |
| 차트 라이브러리 | 대부분의 대시보드 | 넣지 않음. 표 안 막대로 대신 | 〔근거: 판단: 의존성 0. 반증 조건: 4절과 같음〕 |
| 요청 흐름 재기 (누가 어떤 카드를 쓰나) | 분석팀 요구를 받는 구조 | 미룸. 정적 사이트라 기록할 서버가 없다 | 〔근거: 판단. 대안: README에 "원하는 요구사항은 이슈로" 한 줄, 이슈 수를 M-번호로〕 |

## 6. 내보내기 설계 (`scripts/export_site.py`)

- 기본은 집계만 내보낸다: policy_compare, ruleset_change_log, ruleset_release, metric_policy, metric_policy_proposal, daily_action(날짜 × 이벤트 종류 × 정책별 이벤트 수와 계정 수), mde_cap(자르기 백분위마다 계정 수, 평균, 분산), evaluation. 〔근거: 코드: export_site.py EXPORTS〕
- 이름이 붙은 대상 단위 표(suspects, why_excluded, impact_top)는 `--with-entities`일 때만, 그리고 git에 안 올리는 `site/data_local/`로 간다. 〔근거: direction_v5.md 3-1 "대상별 라벨은 공개하지 않음", 원본에 제3자 권리 대상이 섞일 수 있음([gharchive.org](https://github.com/stjordanis/gharchive.org)). 코드: ENTITY_TABLES〕
- 집계 표에 이름·키 컬럼(name, actor_name, repo_name, actor_key, repo_key, entity_key 등)이 섞이면 내보내기가 멈춘다. 〔근거: 코드: ENTITY_COLUMNS assert. 판단: 나중에 누가 SQL을 고쳐도 공개 묶음에 새지 않게〕
- mde_cap은 자르기 백분위(90 ~ 100, 0.5 간격)마다 계정 수, 자른 값, 평균, 분산만 담는다. 처음에는 계정당 이벤트 분포(acct_dist)를 그대로 냈는데, 꼬리의 계정 하나짜리 칸이 none에는 있고 strict에는 없으면 공개 데이터로 그 계정을 찾을 수 있어 바꿨다. q=100 값은 policy_compare의 events_per_account와 같다. daily_action은 그날 그 종류를 낸 계정이 10개 미만이면 싣지 않는다. 〔근거: review_logic.md R-5. 측정: 목업 07-21 none q=100 평균 18.65, 분산 3162.1 = policy_compare. 코드: export_site.py mde_cap, daily_action having〕
- 내보낼 때 빌드 묶음(actor_policy_status의 ruleset_id)이 공개 묶음과 다르면 멈춘다. 화면 한 장에 공개 묶음 숫자와 빌드 묶음 숫자가 섞이지 않게 하려는 것이다. 로컬 확인은 `--allow-unreleased`. 〔근거: review_logic.md R-14. 측정: 공개 줄을 하나 더한 목업에서 내보내기가 멈춤〕
- 분석가 기본 숫자는 공개 묶음(released_ruleset)에 고정한다. 〔근거: decisions.md 결정 24 기본 뷰. 코드: index.html policy_compare_released 뷰〕
- 파일 하나 100MB 미만을 assert로 막는다. 사이트 전체 1GB, 월 대역폭 100GB(약한 한도), 배포 10분 제한. 〔근거: 문서: [GitHub Pages 한도](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits). 100MB는 git 파일 한도〕
- 실제 Gold의 크기는 아직 모른다. Claude Code가 bot-job에서 돌려 M-35에 표별 행 수와 바이트를 남긴다. 〔근거: 확인 필요: 이 VM의 Python 3.10은 duckdb 2.0을 못 깔아 gold.duckdb(2.0 형식)를 열지 못했다〕
- `.gitignore`의 `*.parquet` 때문에 `site/data/*.parquet`은 예외를 둬야 올라간다. 〔근거: 코드: .gitignore 8행〕

## 7. 화면 구현에서 정한 것

- DuckDB-Wasm 1.32.0을 jsDelivr에서 받는다. npm `latest` 태그는 개발판(1.33.1-dev)이라 버전을 고정한다. 〔근거: 측정: npm 2026-11-22. 판단〕
- DECIMAL·HUGEINT 결과가 Arrow 객체로 와서 화면에 NaN이 찍혔다. `castDecimalToDouble`, `castBigIntToDouble`로 연다. 〔근거: 측정: 목업 시험에서 MDE 카드 NaN → 수정 뒤 77.83%. 코드: duckdb-wasm config.d.ts〕
- `precision`은 DuckDB 2.0 예약어라 SQL에서 `"precision"`으로 감싼다. 〔근거: 측정: 목업 생성 중 "syntax error at or near precision"〕
- 표 이름은 meta.json에서 읽되 `^\w+$`만 받는다. 데이터 폴더 이름(`?data=`)도 영문·숫자·밑줄·대시만 남긴다. 〔근거: 판단: SQL과 경로에 들어가는 값이라 입력 경계에서 거른다〕
- 브라우저 메모리는 4GB가 상한이고 기본은 단일 스레드다. 다중 스레드에는 COOP/COEP 헤더가 필요한데 GitHub Pages가 이 헤더를 줄 수 있는지는 확인 필요다. 지금 집계 크기에서는 단일 스레드로 충분하다(목업 질의 6 ~ 26ms). 〔근거: 문서: [DuckDB-Wasm 한계](https://duckdb.org/docs/1.4/clients/wasm/overview.html), [문제 해결](https://duckdb.org/docs/current/clients/wasm/troubleshoot). 측정: 아래 8절〕
- 원격 파일은 HTTP 범위 요청으로 읽는데, 범위 요청을 지원하지 않는 시험 서버(Python http.server)에서도 정상 동작했다. 〔근거: 측정: 8절 시험. 판단: 작은 파일은 통째로 읽는 쪽으로 넘어간다〕

## 8. 검증

- 목업 Gold(같은 표·컬럼 이름, 계정 400개 × 7일)로 내보내고 헤드리스 Chrome(Playwright)에서 화면을 돌렸다. 〔근거: 측정: 2026-11-22, 클라우드 작업 공간. 목업 생성기와 시험 스크립트는 레포에 넣지 않았다(경로가 작업 공간 전용)〕
- 공개 모드: 요구사항 카드 8개 모두 오류 없음(6 ~ 18ms), 틀린 SQL은 오류 문구로 보임, "왜 빠졌나" 탭 숨김, 페이지 안에 계정 이름 없음, 콘솔 오류 0. 〔근거: 측정: 같은 시험. 처음엔 입력칸 예시 문구에 실제 계정 이름이 있어 실패했고 고쳤다〕
- 전체 모드(`?data=data_local`): 카드 11개 모두 오류 없음, 왜 빠졌나 조회, 실험 감도에서 99% 자르기가 MDE를 80.69% → 77.83%로 낮춤. 〔근거: 측정: 같은 시험. 수치는 목업이라 의미 없음, 방향만 본다〕
- 화면 폭 390px(휴대폰)에서 탭이 가로로 밀리고 카드가 한 줄씩 쌓인다. 〔근거: 측정: 스크린샷〕
- 다시 잡은 화면(11절)과 내보내기(10절): 공개·전체 모드 모두 통과. 내려받은 .xlsx를 openpyxl과 LibreOffice로 열어 날짜 칸, 숫자 칸, 첫 줄 고정을 확인, CSV 첫 3바이트가 BOM, JSON 행 수 = CSV 행 수, Parquet의 event_date가 DATE, 표 복사는 탭 구분, 모든 표 엑셀은 "설명" + 표 8개 시트. 〔근거: 측정: 2026-11-22 Playwright 시험〕
- SQL 콘솔: 공개 모드 표·뷰 9개, 전체 모드 12개가 스키마에 보임. 미리보기, 컬럼 넣기, 직접 쿼리, 고른 부분만 실행, summarize, 찾기, 최근 실행, 콘솔 결과 CSV 받기 통과. 결과는 시트(패널)마다 따로 두어 콘솔 결과가 요구사항 시트의 버튼으로 새지 않는다. 〔근거: 측정: 2026-11-22 Playwright 시험. 코드: index.html panels〕
- 콘솔에서 사용자가 뷰를 지우거나 바꾸면 그 브라우저 탭에서만 바뀌고 새로 고침하면 돌아온다. 원본 파일은 읽기만 한다. 〔근거: 판단: 브라우저 안 DuckDB는 메모리 DB이고 Parquet은 HTTP로 읽기만 한다〕
- 남은 것: 실제 Gold로 내보내기, 크기, 첫 화면 시간, Firefox, 실제 엑셀에서 열기. M-35로 남긴다. 〔근거: 확인 필요〕

## 9. 배포

- 워크플로는 `.github/workflows/pages.yml`로 옮겼다(2026-11-22, 처음에는 원격 도구가 .github/workflows에 쓸 수 없어 docs/pages_workflow.yml에 두었다). 손으로 누를 때만 돈다(workflow_dispatch). 공개는 사용자 결정이라 push에 자동으로 붙이지 않는다. 〔근거: 판단: 바깥에 내보내는 일은 되돌리기 어렵다(공개 뒤 남이 받아 갈 수 있다). direction_v6.md D9〕
- 올리기 전에 workflow가 site/ 안에 대상 단위 파일이 없는지, meta.json의 with_entities가 false인지 다시 본다. 〔근거: 코드: .github/workflows/pages.yml 검사 단계. 판단: 내보내기 검사와 이중으로〕
- 순서: U1 정리 → U2 올리기 → GitHub 설정 Pages에서 원천을 "GitHub Actions"로 → Actions 탭에서 "pages" 실행. 〔근거: 문서: [GitHub Pages 한도와 안내](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits). handoff_v6.md U1, U2〕
- 저장소가 비공개여도 Pages 사이트는 인터넷에 공개된다. 〔근거: 문서: [What is GitHub Pages](https://help.github.com/articles/what-is-github-pages) "publicly available on the internet, even if the repository for the site is private"〕

## 10. 내보내기 형식 (2026-11-22 추가)

분석가가 실제로 쓰는 도구에 맞춰, 화면에서 뽑은 결과와 표를 그 도구가 바로 여는 형식으로 준다. 〔근거: 사용자 2026-11-22 "엑셀이나 json 이런 형식으로 내보내거나"〕

| 분석가가 쓰는 곳 | 근거 | 이 화면이 주는 것 | 구현 근거 |
|---|---|---|---|
| 엑셀 | 실무 분석가에게 무엇을 가장 많이 쓰는지 물으면 SQL과 엑셀이 거의 모든 답에 나온다. 분석가 76%가 아직 스프레드시트로 데이터를 정리한다 | 결과를 .xlsx로(숫자는 숫자 칸, 날짜는 날짜 칸, 첫 줄 고정, SQL 시트 동봉). 모든 공개 표를 시트별로 담은 엑셀 하나("설명" 시트 포함) | 〔근거: 문서: [Dataquest 2026](https://www.dataquest.io/blog/10-data-analysis-tools-for-entry-level-analysts), [ITPro, Alteryx 조사](https://www.itpro.com/uk/business/data-and-insights/data-analyst-manual-tasks-ai-automation). 측정: 8절 openpyxl과 LibreOffice로 열림〕 |
| 엑셀에서 CSV | 엑셀은 BOM이 없는 UTF-8 CSV의 한글을 깨뜨린다 | 내려받는 CSV에는 BOM을 붙인다 | 〔근거: 문서: [Databricks KB, 한글 CSV 깨짐](https://kb.databricks.com/dbsql/korean-characters-breaking-when-downloading-genie-results-as-csv). 측정: 8절 첫 3바이트 EF BB BF〕 |
| 구글 시트 | GCP 쓰는 회사는 BigQuery를 시트로 연결해 쓰고(연결된 시트), 외부 파일은 IMPORTDATA로 부른다. IMPORTDATA는 CSV와 TSV만 읽는다 | 표마다 고정 주소의 CSV(`data/csv/표.csv`)와 IMPORTDATA 수식, 결과 "표 복사"(탭 구분이라 시트에 그대로 붙는다) | 〔근거: 문서: [BigQuery 연결된 시트](https://docs.cloud.google.com/bigquery/docs/connected-sheets?hl=ko), [IMPORTDATA](https://support.google.com/docs/answer/3093335?hl=en). 코드: export_site.py csv 폴더〕 |
| 파이썬(pandas) | 스프레드시트와 쿼리로 모자랄 때 다음 도구 | Parquet 주소와 `pd.read_parquet` 한 줄 | 〔근거: 문서: [Dataquest 2026](https://www.dataquest.io/blog/10-data-analysis-tools-for-entry-level-analysts), [pandas read_parquet](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html) URL 지원〕 |
| SQL(DuckDB 등) | SQL은 분석가 공고의 약 53%, 응답자 58.6%가 쓴다 | 화면 안 SQL 그대로, 바깥 DuckDB에서는 주소를 표처럼 읽는 한 줄 | 〔근거: 문서: [Dataquest 2026](https://www.dataquest.io/blog/10-data-analysis-tools-for-entry-level-analysts), [DuckDB HTTP Parquet](https://duckdb.org/docs/preview/guides/network_cloud_storage/http_import)〕 |
| 앱, 다른 서비스 | 개발자가 받는 형식 | JSON(행마다 객체 하나인 배열) | 〔근거: 판단: 별도 근거 없음. 반증 조건: 받는 쪽이 줄 단위(NDJSON)를 원하면 바꾼다〕 |
| 큰 결과를 다른 엔진으로 | 컬럼 형식이라 작고 타입이 남는다 | 결과를 Parquet으로(브라우저 안 DuckDB의 COPY) | 〔근거: 측정: 8절 내려받은 Parquet의 event_date가 DATE로 남음〕 |

- 엑셀 파일은 DuckDB의 excel 확장 대신 화면 안 70줄로 만든다. Wasm 확장은 실행 때 extensions.duckdb.org에서 받아야 해서 바깥 의존이 하나 늘고, 이 작업 공간에서는 시험할 수 없었다. 〔근거: 문서: [DuckDB 엑셀 내보내기](https://duckdb.org/docs/2.0/guides/file_formats/excel_export), [DuckDB-Wasm 확장](https://www.duckdb.org/docs/current/clients/wasm/extensions.html) "fetched only on load". 반증 조건: 서식 요구(색, 열 너비)가 늘면 확장으로 바꾼다〕
- 엑셀 한 시트는 1,048,576행까지라 넘으면 막고 CSV나 Parquet을 권한다. 〔근거: 코드: index.html sheetXml. 확인 필요: Microsoft "Excel 사양 및 제한" 문서〕
- 엑셀 "데이터 → 웹에서"로 CSV 주소를 바로 여는 안내는 실제 엑셀에서 확인하지 않았다. 〔근거: 확인 필요: Windows 엑셀에서 한 번 해 본다(Q13)〕
- 정적 CSV는 BOM 없이 둔다. 기계(시트, 엑셀 웹에서, 파이썬)가 읽는 주소라 BOM이 첫 컬럼 이름에 섞일 수 있어서다. 사람이 내려받는 CSV에만 붙인다. 〔근거: 판단. 확인 필요: IMPORTDATA가 BOM을 지우는지〕
- 결과를 받을 때는 요구사항 이름이 파일 이름이 되고, 엑셀에는 SQL과 데이터 기간, 규칙 묶음이 든 시트를 함께 넣는다. 받은 숫자가 어디서 왔는지 파일만 봐도 알게 하려는 것이다. 〔근거: decisions.md 결정 24(숫자는 묶음에 고정). 코드: index.html exportResult〕

## 11. 디자인 (2026-11-22 두 번째 판)

- 첫 판(카드, 알약 배지)과 두 번째 판(시트 탭, 형광펜, 체 아이콘)은 모두 생성형 화면처럼 보인다는 평을 받았다. 디자인 에이전트가 가독성 위주로 다시 잡았다. 〔근거: 사용자 2026-11-22 "웹 디자인이 너무 claude가 만든거같아", 12:16 "가독성 좋게 UX 디자인 다시하고 글씨체도 바꾸고"〕
- 방향: 소개 페이지가 아니라 매일 여는 조회 도구로 본다. 흰 작업면 하나에 탭, 편집기, 결과 표만 두고, 테두리는 편집기와 결과 표 같은 실제 부품에만 쓴다. 〔근거: 판단: 모든 요소에 같은 테두리를 준 것이 생성형 인상의 큰 원인이었다〕
- 하나만 크게: 거름 강도의 단계마다 그날 남는 이벤트 수와 비율을 막대로 미리 보인다. 색은 "남은 실유저" 하나만 뜻한다(짙은 청록 #0B6B5D, 걸러진 몫은 회색 빗금). 같은 표현을 요약 차트와 정책 비교 표에도 쓴다. 거름 강도는 영향을 받는 요약, 정책 비교 탭에서만 보인다. 〔근거: 코드: index.html LEVELS. 판단: 고르면 무엇이 바뀌는지를 고르기 전에 보여 준다〕
- 탭: 매일 쓰는 넷(요약, SQL 콘솔, 요구사항, 가져가기)과 참고용(정책 비교, 실험 감도, 규칙 묶음, 데이터 사전)을 세로선으로 나눴다. "지표 × 정책"은 "정책 비교"로 바꿨다. 〔근거: 판단: 무엇을 하는 탭인지 이름이 말해야 한다〕
- SQL 콘솔: 표 목록은 화면에 고정, 실행은 편집기 바로 밑 왼쪽, 내려받기는 결과 바로 위 오른쪽. 결과 표는 머리줄과 행 번호 고정, 숫자는 머리줄까지 오른쪽 정렬, 오류는 결과 자리에 DuckDB 메시지 전문. 〔근거: 사용자 12:16 "직접 쿼리던질수있게". 판단: 실행과 저장이 손 닿는 자리에서 갈린다〕
- 글꼴: 본문은 원티드 산스(Wanted Sans Variable, npm wanted-sans 1.0.3). 한글 11,172자, 고정폭 숫자(tnum)와 빗금 0이 있어 숫자 열과 규칙 묶음 id를 읽기 좋다. SQL과 코드는 D2Coding(npm d2coding 1.3.2)으로 한글이 영문 두 칸에 맞는다. 대체 글꼴은 Apple SD Gothic Neo, Malgun Gothic. 〔근거: 확인: 두 패키지의 CSS와 woff2가 npm 패키지에 있다(2026-11-22 npm 설치). 판단: Pretendard는 라틴이 Inter라 생성형 화면의 기본값과 같은 인상〕
- 읽기: 본문 15px / 줄 간격 1.6, 표 14px, 요약 문장 20px(모바일 17px), `word-break: keep-all`로 어절 중간 줄바꿈 없음, 한글 문단은 한 줄 46자 이내. 〔근거: 디자인 에이전트 노트. 측정: 스크린샷에서 "바뀝 / 니다" 같은 끊김이 사라짐〕
- 문구: strict 설명을 "근거가 하나라도 걸리면"에서 "행동 근거로 걸린 계정까지 뺌"으로 고쳤다. 코드에서 strict는 low_activity 하나로는 빼지 않는다. 〔근거: review_logic.md R-13. 코드: entity_verdict.sql〕
- 문구 갱신(2026-11-22, handoff_v7.md 6-5): 거름 강도 설명을 코드에 맞췄다. 지표별 기본 = "잣대는 정답으로 채점된 balanced", balanced = "무리 지어 star를 누른 계정까지, 채점됨"(인프라 계열 근거가 없어 "사칭 계정"을 뺐다), strict = "하루 20시간 넘게 돌거나 한 시간에 저장소 100개 넘게 움직인 계정까지, 채점 안 됨, 보고용 상한". 요약에 "상위 100 목록은 거르지 않아도 날마다 73 ~ 89% 바뀐다", "거르지 않은 MDE가 큰 것은 대부분 자기 신고 계정 하나" 단서를 붙이고, 지표별 기본 정책 줄의 지표 이름을 한국어로 보인다. id, data- 속성, 함수 이름은 그대로다. 〔근거: 코드: site/index.html LEVELS, summary(), METRIC_KO. 측정: M-36 (a), (c). review_logic.md R-12〕
- 확인: 공개·전체 모드 시험 통과, 390px 폭, 어두운 화면, 키보드 초점, 움직임 줄이기 설정. 〔근거: 측정: 2026-11-22 Playwright 시험과 스크린샷〕

## 12. 화면 문구 (2026-11-22 세 번째 판)

- 계기: 사용자가 화면을 돌며 "거르지 않음", "balanced" 같은 말과 어투가 어색하다고 했다. 실제 서비스가 같은 개념을 어떻게 쓰는지 찾아보고 고쳤다. 〔근거: 사용자 2026-11-22 16:19 "이상한 언어, 단어, 어투 찾아서 수정해줘 실제로 서비스 찾아보면서"〕
- 어투: 화면 문구를 모두 해요체로 맞췄다("남습니다" → "남아요", "확인하세요" → "확인해 주세요"). 전에는 합니다체와 반말 메모체("싣지 않는다", "센다")가 섞여 있었다. 〔근거: 토스는 모든 문구를 해요체로 통일했다(ditoday.com/?p=151644). 토스 보이스톤 메이커는 "5000원을 보냈어요"처럼 문체를 통일하고 어려운 말 대신 쉬운 문장을 쓰게 한다(byline.network/2022/06/08-88)〕
- 정책 이름: 화면에서는 "봇 필터 강도"로 부르고 원본(none) / 약하게(lenient) / 보통(balanced) / 강하게(strict), 지표별 기본은 "지표별 추천"으로 보인다. 영문 값은 지우지 않고 옆에 작게 둔다. SQL의 treatment 값이 영문이라 둘을 이어 줘야 콘솔에서 쓸 수 있다. 〔근거: 메일 서비스는 같은 기능을 "스팸 필터링", "정크 메일 필터"로 부른다(support.microsoft.com/ko-kr/outlook/block-or-allow-junk-email-settings). 전문용어를 쓰지 않을 때 간결성과 심리적 안정감이 높았다(koreascience.kr JAKO202322957621664). 코드: index.html LABEL, plab()〕
- "거름 강도"를 버린 이유: "거름"은 비료 뜻이 먼저 읽히고, "거르지 않음"은 설정을 끈 것처럼 읽힌다. 원본은 "봇을 빼기 전 그대로"라는 뜻이라 "원본"이 맞다. 〔근거: 판단〕
- 쉬운 말이 위험을 가리지 않게: strict 설명에 "채점하지 않았으니 최대치로만 보세요"를, balanced에 "정답 데이터로 채점한 기준"을 남겼다. 〔근거: 토스 "외상구매" 표기는 고위험 상품을 친근한 말로 바꿔 위험을 단순화했다는 비판을 받았다(ditoday.com 토스 10년 기사). review_logic.md R-12〕
- MDE: "반반 실험", "잡을 수 있는 최소 효과" → "50:50 A/B 테스트", "최소 탐지 효과(MDE)". 공식 설명은 "양측 검정, 유의수준 5%, 검정력 80%"로 쓴다. 〔근거: 국내 표본 크기 계산기는 "최소 탐지 가능 효과(MDE)"와 검정력 80%를 쓴다(miniwebtool.com/ko/a-b-테스트-표본-크기-계산기). 핵클 계산기는 양측, 유의수준 0.05, 검정력 80%를 업계 표준으로 둔다(hackle.io/tools/sample-size-calculator)〕
- 자르기 슬라이더: "상위 자르기 99 백분위"는 오른쪽으로 갈수록 덜 자르는 꼴이었다. 슬라이더 값을 자르는 비율(0~10%)로 바꿔 "상위 1% 자르기"로 읽히게 했다. 데이터의 q는 100에서 빼서 쓴다. 〔근거: 코드: index.html mde(). 측정: 실데이터 시험에서 07-21 원본 MDE 137.4% → 상위 1% 자르기 1.23%, 바꾸기 전과 같다〕
- 탭 이름: 요구사항 → 자주 쓰는 쿼리, 정책 비교 → 강도별 비교, 실험 감도 → MDE 계산기, 규칙 묶음 → 규칙 버전, 왜 빠졌나 → 제외 이유. 무엇을 하는 탭인지 이름이 말하게 했다. 〔근거: 판단. 11절 탭 원칙〕
- 표 머리줄: 고정 화면(강도별 비교, 규칙 버전, 데이터 사전)은 머리줄과 강도·지표 값을 한글로 보이고, 원래 컬럼 이름은 마우스를 올리면 보인다. SQL 콘솔, 자주 쓰는 쿼리 결과, 모든 내려받기 파일은 원래 컬럼 이름 그대로다. SQL과 파일은 다른 도구에서 다시 쓰이기 때문이다. 〔근거: 코드: index.html HEAD, CELL, grid(heads, cell)〕
- 데이터 사전 "한 행의 뜻"은 meta.json에서 온다. 같은 말로 scripts/export_site.py와 지금 site/data/meta.json을 함께 고쳤다(데이터 값은 그대로). 〔근거: 코드: export_site.py EXPORTS〕
- 그대로 둔 것: id, data- 속성, 내부 값(none/lenient/balanced/strict), SQL 문, README와 기록 문서의 정책 이름. 기록은 그때 쓴 말 그대로 둔다. 〔근거: 판단. glossary.md에 화면 이름 대응을 더했다〕
- 확인: 공개 모드, 전체 모드(로컬 데이터), 실데이터 시험 모두 통과, 콘솔 오류 없음. 〔근거: 측정: 2026-11-22 Playwright test_site.py, test_site.py --full, test_real.py〕
