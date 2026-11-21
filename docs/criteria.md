# 봇 판정 기준

상태: 초안 (2026-11-16), 2.1절 기준 표 추가 (2026-11-22). 이 문서는 세 부분이다.
1. 공개된 데이터와 연구가 **실제로 어떤 기준으로 봇이라고 판정했는지** (조사)
2. 이 프로젝트가 쓸 **근거 목록** (각 근거가 무엇을 보고, 얼마나 믿을 만한지)
3. **어떤 정답으로 어떤 근거를 평가해도 되는지** (독립성 표)

정책(근거를 어떻게 조합해 실유저를 정할지)은 4절에 초안만 두고, 확정은 `design_overview.md` 14절 5번에서 한다.

## 1. 데이터와 연구별 판정 기준 (조사)

### 1.1 GitHub

| 출처 | 대상 | 판정 기준 | 라벨을 만든 방식 | 주의 |
|---|---|---|---|---|
| **GitHub App** | 계정 | login이 `[bot]`으로 끝남 | 플랫폼이 붙이는 표시 (자기 신고) | 사람 계정으로 돌리는 스크립트는 안 잡힘 |
| **StarScout** [1] | star, 계정, 저장소 | **저활동:** 계정 전체 이력에서 WatchEvent가 하나뿐이고, 같은 날 같은 저장소에 다른 이벤트가 최대 하나. 의심 star가 50개 이상인 저장소만 남김<br>**lockstep (CopyCatch):** 계정 50개, 저장소 10개, Δt = 15일, ρ = 0.5. 각 저장소에 대해 계정의 절반 이상이 2Δt 안에 star<br>**캠페인:** 어떤 달에 가짜 star가 50개를 넘고 그달 star의 50%를 넘으며, 전체 기간 가짜 star 비율이 10%를 넘음 | 휴리스틱. 사람이 붙인 정답 없음 | 검증은 삭제율로 함: 의심 저장소 90.75% 삭제 (기준선 5.84%), 의심 계정 61.95% 삭제 (기준선 4.43%). 재현율 근거는 없다고 논문이 밝힘 |
| **Golzadeh (BoDeGHa)** [2][3] | 계정 | PR·이슈 댓글의 반복성: 댓글 수, 빈 댓글 수, 댓글 패턴 수, 패턴 안 댓글 수의 불균등 | 5,000개 계정을 사람이 댓글 내용을 보고 판정 (평가자 3명, 높은 일치도). 봇 527개 | 라벨이 댓글 내용 기반 → 댓글 반복성 근거를 이걸로 평가하면 순환 |
| **RABBIT** [4] | 계정 | 최근 이벤트 최대 300개에서 피처 6개: 활동 종류 수, 종류당 평균 활동 수, 다른 종류 활동 사이 시간 간격 중앙값, 저장소당 평균 활동 수, 저장소 소유자 수, 시간 간격의 지니 불균등 | 기존 데이터 + 629개 계정을 연구자 2명이 독립 라벨링 (κ = 0.79), 불일치는 3번째 연구자가 판정. 총 1,335개 (봇 644). GitHub App은 제외 | 테스트셋 정밀도·재현율·F1 각 0.919 |
| **BotHawk** [5] | 계정 | 피처 17개: 프로필(이름·bio·이메일의 문자열 표시, 계정 태그, 팔로워·팔로잉), 활동량, 응답 시간 중앙값, 연결 계정 수, 최근 댓글 100개의 TF-IDF 유사도, 활동 주기성 | 질의·시각화 도구와 정해진 작업 절차로 라벨링. 봇의 특징으로 빈 프로필, 이름에 bot/auto/ci/cla/io, 적은 팔로워, 기본 아바타를 봄 | **평가자 간 일치도와 라벨 정확도 검사가 없음.** 계정의 Bot 태그가 피처이면서 라벨 근거였는지도 불분명 → `[bot]` 기반 근거를 이걸로 평가하면 순환 위험 |

**GH Archive에서 바로 계산할 수 있는가:** StarScout와 RABBIT 계열(이벤트 종류, 시간 간격) 근거는 바로 계산된다. 댓글 반복성(BoDeGHa, BotHawk의 TF-IDF)은 IssueCommentEvent의 payload에 본문이 있어 계산할 수 있다. 지금 적재 스크립트는 payload를 버리므로 Bronze → Silver에서 필요한 필드만 추가로 뽑는다 `[확인 필요: payload 필드 구조]`. 팔로워 수 같은 프로필 정보는 GH Archive에 없다.

### 1.2 웹

| 출처 | 대상 | 판정 기준 | 라벨을 만든 방식 | 주의 |
|---|---|---|---|---|
| **AUTH 도서관 로그** (Zenodo 3477932) [6] | 세션 | UA가 알려진 봇 목록에 있거나 robots.txt를 요청 | UA 목록, robots.txt, UA 수작업 검토 | 사실상 자기 신고 → 행동 근거 평가에만 사용 |
| **Suchacka 이커머스 세션** [7] | 세션 | IP나 UA가 알려진 봇 표에 있거나 봇 휴리스틱에 걸리면 봇, 알려진 브라우저 UA면 사람, 나머지는 미분류. 휴리스틱 예: 4xx 응답, robots.txt 요청 | 규칙 기반. 세션 13,397개 (봇 6,195) | 휴리스틱 라벨 |
| **Wikimedia** [8][9] | 요청, 세션 | UA에 bot 등으로 자기 신고 → spider. 나머지는 user, automated, unclassified로 라벨링. automated 예: 쿠키 없이 요청이 너무 많고 문서 다양성이 낮은 세션, UA 길이가 너무 길거나 짧은 세션 | 운영 중인 플랫폼의 휴리스틱 | 집계만 공개. 탐지 정답으로는 못 씀 |
| **Iliou (CERTH)** [10][11] | 세션 | 실험 설계 자체가 라벨. 사람은 실제 참가자가 2세션씩, 같은 수의 중간 봇과 고급 봇 세션을 연구진이 생성. 중간 봇은 브라우저 지문 + 마우스 움직임과 링크 클릭을 하지만 사람 같은 지능적 행동은 없음. 고급 봇은 거기에 사람 같은 행동까지 | 설계상 정답 (생성한 봇) | 작음 (단계마다 사람 세션 50~56개). 위키 페이지라 이커머스가 아님 |
| **BOTracle** [12] | 요청, 세션 | 사람: 조직 내부 실제 사용자 계정. 봇: 클라우드 데이터센터 IP. 여기에 휴리스틱(수정 안 한 봇 UA 등)과 전문가 30명 이상의 수작업 라벨 | 가정 + 휴리스틱 + 수작업 | 비공개 데이터 |
| **Botcha** [13] | 세션 | 사람: 구매자 세션. 봇: AWS·Azure에서 온 세션. 정상 봇은 UA로 먼저 제외 | 부분 라벨 (PU 학습) | 비공개 데이터. 구매 = 사람 가정이 흔들린다는 반론이 있음 [12] |
| **크롤러 IP 검증** [14][15] | 요청 | UA가 주장한 크롤러의 공개 IP 대역(크롤러 전용 파일)에 들어가는지 + 역방향 DNS를 정방향으로 다시 확인(FCrDNS) | 외부에서 확인되는 사실 | 회사 전체 인프라 파일과 대조하면 클라우드만 빌려도 통과함. 2026년 Google 파일 경로와 이름이 바뀜. **로그 시점(2019)과 대역 시점(2026)이 다름** |

## 2. 이 프로젝트의 근거 목록

근거 하나 = evidence 표의 `evidence_id` 하나. 규칙이 바뀌면 같은 `evidence_id`에 `rule_version`만 올린다.

| evidence_id | 계열 | 보는 것 | 대상 | 소스 | 강도 |
|---|---|---|---|---|---|
| `declared_app` | 자기 신고 | login이 `[bot]`으로 끝남 | 계정 | GH | 정밀도 높음 |
| `declared_ua` | 자기 신고 | UA가 크롤러·AI 에이전트 목록에 걸림, robots.txt 요청 | 세션 | 웹 | 정밀도 높음, 위장은 못 잡음 |
| `ai_agent_ua` | 자기 신고 | AI 어시스턴트·에이전트 UA (사람 대신 행동) | 세션 | 웹 | 범주 표시용, 봇 판정과 따로 |
| `ip_verified_crawler` | 인프라 | 주장한 크롤러의 공개 대역 + FCrDNS 일치 여부 | 요청 → 세션 | Zanbil | 강함, 시점 차이 주의 |
| `cloud_ip` | 인프라 | 클라우드 업체 IP 대역에서 옴 | 세션 | Zanbil | 중간 (사람도 VPN·클라우드를 씀) |
| `always_on` | 행동 | 하루 20시간 이상 활동, 100건 이상 | 계정 | GH | 크론형만 잡음 (선언 봇 재현율 55.8%) |
| `event_regularity` | 행동 | 이벤트 간격의 규칙성 (RABBIT의 지니 불균등) | 계정, 세션 | 전부 | 미측정 |
| `comment_repetition` | 행동 | 댓글 반복성 (BoDeGHa) | 계정 | GH | 미측정 |
| `assetless_browser` | 행동 | 브라우저 UA인데 CSS/JS·AJAX·이미지를 안 받음 | 세션 | 웹 | 도서관 로그에서는 무해한 자동화만 잡힘 |
| `low_activity` | 조직(전제) | 기간 안에 star 하나만 누른 계정 (StarScout 정의는 전체 이력) | 계정 | GH | 계정 단위로는 약함, 저장소 단위로 모으면 강함 |
| `lockstep_ratio` | 조직 | 내 stargazer 중 다른 저장소와 같이 움직인 비율 | 저장소 | GH (웹 상품으로 확장 실험) | 강함 |
| `burst_5m` | 조직 | 가장 몰린 5분 구간의 비중 | 저장소 | GH (웹 상품으로 확장 실험) | 강함 |
| `multi_repo_burst` | 행동 | 한 시간 안에 같은 행동을 서로 다른 저장소 100개 넘게에 (measurements.md M-6의 빈 곳) | 계정 | GH | 미측정 (계정 정답이 아직 없다). 지시서 H5에서 더해 본 것 |
| `purchase` | 확정 사람 | 구매 세션 | 세션 | EClog, Zanbil | 사람 쪽만 확정 |
| `mouse_humanlike` | 클라이언트 | 마우스 궤적 | 세션 | Iliou | 위장 봇에 강함, 데이터 작음 |

## 2.1 판정 기준 표: 쓴 것과 버린 것 (2026-11-22)

"왜 뽑았고 왜 버리는지"를 한 표로 둔다. 근거마다 쓰는 이유, 잰 강도, 버렸다면 다시 볼 조건을 적는다. 〔근거: mentoring_2026-11-19.md 28행 "왜 뽑았고 왜 버리는지의 판단 기준이 드러나면 좋다"〕

### 쓰는 근거 (GitHub, `rules/registry.yml` 현재 버전)

| 근거 | 버전 | 계열 → 정책 | 쓰는 이유 | 잰 강도 | 근거 |
|---|---|---|---|---|---|
| `declared_app` | v1 | 자기 신고 → lenient, balanced, strict | 플랫폼이 붙인 `[bot]` 표시라 사람이 걸릴 일이 거의 없다 | 07-15 hit 1,530 계정 | 〔근거: rules/registry.yml, policy_family.csv, local_baseline.md 1절〕 |
| `always_on` | v1 | 행동 → strict | 하루 20시간 넘게 도는 크론형 자동화를 잡는다 | 선언 봇 재현율 55.8%, 07-15 hit 428 | 〔근거: 이 문서 2절 강도 칸, local_baseline.md 1절〕 |
| `low_activity` | v1 | 조직 → balanced, strict | 가짜 star의 전제(StarScout 저활동 정의). 저장소로 모으면 강해진다 | 저장소 단독 정밀도 49.1 ~ 96.2% | 〔근거: 측정 M-18 근거 하나씩〕 |
| `lockstep_ratio` | v2 | 조직 → balanced, strict | 같은 계정 무리가 여러 저장소를 같이 찍는 모양(CopyCatch) | 단독 정밀도 87.2 ~ 94.7% | 〔근거: M-18〕 |
| `burst_5m` | v1 | 조직 → balanced, strict | 하루 100개 넘게 star하는 무리는 lockstep에서 빠지므로 따로 잡는다 | 단독 정밀도 92.6 ~ 98.6% | 〔근거: M-18, M-11 B "burst_5m SQL 머리말이 적은 설계 의도가 데이터로 확인됐다"〕 |
| `multi_repo_burst` | v2 | 행동 → strict | 03시 ReleaseEvent 1,606건을 낸 세 계정이 기존 근거 다섯 개에 모두 0 hit였다 | 정밀도 미측정(계정 정답 없음), 07-15 hit 52 | 〔근거: M-6, 결정 12, 결정 13〕 |

- 조합 판정 `fake_star_repo` = `low_activity AND (lockstep_ratio OR burst_5m)`. 일주일 정밀도 92.1%, 07-15는 94.9%. 〔근거: M-22 결론 줄, local_baseline.md 1절 채점 표〕

### 붙일 근거 (웹, direction_v5.md WP1 1-3)

| 근거 | 계열 → 정책 | 쓰는 이유 | 아는 강도 | 근거 |
|---|---|---|---|---|
| `declared_ua` | 자기 신고 → 전 정책 | 크롤러 목록 UA나 robots.txt 요청은 스스로 봇이라 밝힌 것 | AUTH 세션 19.1%, 요청 10.1% | 〔근거: experiments/p0_bot_detection/REPORT.md 3절〕 |
| `assetless_browser` | 행동 → strict | 브라우저 UA인데 CSS/JS/이미지를 안 받으면 사람 브라우저가 아닐 가능성이 크다 | 도서관 로그에서는 무해한 자동화만 잡혔다. 그래서 strict에만 넣는다 | 〔근거: 이 문서 2절 강도 칸, 02_detect.py 57번째 줄 L2 규칙〕 |
| `ai_agent_ua` | 범주(정책에 안 넣음) | 사람 대신 행동하는 에이전트라 봇도 사람도 아니다. 넣을지는 쓰는 쪽이 고른다 | 해당 없음 | 〔근거: 이 문서 4절 마지막 줄〕 |

### 버린 것과 보류한 것

| 대상 | 결정 | 이유 | 다시 볼 조건 | 근거 |
|---|---|---|---|---|
| `lockstep_ratio` v1 (절대 수) | 버림 | 트렌딩 저장소를 봇으로 찍었다 | 없음 | 〔근거: rules/registry.yml 3번째 주석 줄〕 |
| `lockstep_ratio` v3 (문턱 0.5 → 0.4) | 시험 후 v2 유지 | 판정이 바뀐 저장소 19개(2,793 → 2,812). 새로 걸린 135개 중 116개는 이미 burst_5m에 걸려 있었다 | v3가 정밀도를 지키며 놓친 봇을 줄인다는 채점 숫자가 나오면 | 〔근거: M-11 A〕 |
| 저장소 피처 f2 (계정당 상한 50 → 200) | 시험 후 f1 유지 | 쌍 후보가 2 ~ 8배가 됐는데 가짜 star 저장소, 의심 계정, 실유저 이벤트가 하나도 안 바뀌었다 | 데이터를 키워 상한이 판정을 바꾸는지 볼 때 (direction_v5.md 2-1 S1) | 〔근거: M-11 B〕 |
| `low_activity` 문턱 내리기 | 하지 않음 | 놓친 봇이 문턱 바로 아래가 아니라 0.0 ~ 0.4에 있다. 조금 내려서는 안 잡힌다 | 없음 | 〔근거: M-22 4절, 제안 (다)〕 |
| `burst_5m` 단독 경로 | 보류 (사용자 결정) | 대조군 burst hit 113개 중 110개가 삭제됐지만, 대조군 밖에서는 재지 않았다 | 비의심 전체로 넓혀 정밀도를 재면 | 〔근거: M-22 제안 (가)〕 |
| 계정 품질 근거 (나이, 팔로워, 공개 저장소 수) | 보류 | GH Archive 이벤트에 없는 값이라 새 원천이 필요하다 | 프로필 원천을 붙일 때 | 〔근거: M-22 제안 (나), 이 문서 1.1 "팔로워 수 같은 프로필 정보는 GH Archive에 없다"〕 |
| `ip_verified_crawler` | 이번 범위에서 버림 | 실제 IP가 있는 원천이 없다. 로그 시점(2019)과 대역 시점(2026)도 다르다 | 실제 IP가 있는 원천이 생기면 | 〔근거: 이 문서 1.2 크롤러 IP 검증 주의 칸, direction_v5.md 4절〕 |
| `cloud_ip` | 이번 범위에서 버림 | 같은 이유. 사람도 VPN과 클라우드를 써서 강도가 중간이다 | 위와 같음 | 〔근거: 이 문서 2절 강도 칸〕 |
| `event_regularity` | 보류 | RABBIT 정답으로 채점하면 순환이다. 다른 계정 정답과 겹치는지 아직 안 봤다 | direction_v5.md 2-3에서 golzadeh, bothawk와 겹침이 충분하면 | 〔근거: 이 문서 3절 rabbit 행〕 |
| `comment_repetition` | 보류 | 적재 때 payload를 버려 지금은 계산할 수 없다. golzadeh로 채점하면 순환이다 | 댓글 본문 필드를 Silver로 뽑을 때 | 〔근거: 이 문서 1.1 "지금 적재 스크립트는 payload를 버리므로", 3절 golzadeh 행〕 |
| `mouse_humanlike` | 버림 | 우리 원천에 마우스 데이터가 없다. 있는 곳(Iliou)은 단계마다 사람 세션 50 ~ 56개로 작다 | 없음 | 〔근거: 이 문서 1.2 Iliou 행〕 |
| `purchase`를 근거로 쓰기 | 근거가 아니라 정답으로만 | 사람 쪽만 확정이라 봇 판정 신호가 될 수 없다 | 없음 | 〔근거: 이 문서 3절 purchase 행〕 |

## 3. 정답 목록과 독립성

| truth_source | 무엇 | 대상 | 도착 | 평가해도 되는 근거 | 쓰면 안 되는 근거 (순환) |
|---|---|---|---|---|---|
| `repo_deleted` | 저장소 404 | 저장소 | 늦게 (사후) | 조직, 행동 | 없음 |
| `account_deleted` | 계정 404 | 계정 | 늦게 (사후) | 조직, 행동 | 없음 |
| `golzadeh` | 사람 라벨 | 계정 | 고정 | 자기 신고, 조직, 이벤트 행동 | `comment_repetition` |
| `rabbit` | 사람 라벨 | 계정 | 고정 | 자기 신고, 조직, 댓글 | `event_regularity` (같은 피처 계열) |
| `bothawk` | 도구 기반 라벨 | 계정 | 고정 | 조직, `always_on` | `declared_app`, `comment_repetition` (라벨 근거로 쓰였을 가능성) |
| `starscout` | 휴리스틱 의심 | star, 계정, 저장소 | 고정 | **정답 아님.** 일치도 비교만 | 해당 없음 |
| `iliou` | 실험 설계 라벨 | 세션 | 고정 | 전부 | 없음 |
| `ip_verification` | 외부 사실 | 요청 | 지금 확인 | 자기 신고의 정직성, 행동 | `ip_verified_crawler`, `cloud_ip` |
| `purchase` | 부분 정답 (사람) | 세션 | 늦게 | 모든 봇 근거의 오탐 (구매자를 봇으로 찍었나) | `purchase` |
| `auth_library` | 자기 신고 라벨 | 세션 | 고정 | 행동 | `declared_ua` |

이 표는 `truth_independence.csv`로 기계가 읽게 두고, 평가 마트를 만들 때 순환 쌍을 자동으로 거른다 (설계 11절 보증 4).

## 4. 정책 초안 (확정 전)

| 정책 | 봇으로 빼는 것 | 쓰는 곳 |
|---|---|---|
| lenient | 자기 신고만 | 순수 트래픽 추이 |
| balanced | 자기 신고 + 인프라 검증 실패(크롤러 사칭) + 조직 근거 | 분석팀 기본 |
| strict | 근거 하나라도 걸리면 | 메시지 발송, 실험 대상자 선정 |

`ai_agent`는 세 정책 모두에서 봇과 따로 표시하고, 포함 여부는 쓰는 쪽이 고른다.

## 출처

1. He et al., 4.5 Million (Suspected) Fake Stars in GitHub: https://arxiv.org/html/2412.13459v1
2. Golzadeh et al., A ground-truth dataset and classification model for detecting bots in GitHub issue and PR comments: https://arxiv.org/pdf/2010.03303
3. 같은 논문 초록: https://ersearch2.cvtisr.sk/vufind/SummonRecord/FETCH-LOGICAL-c363t-e659b9b059991b8e5516ab82022eb511dc4d2ab05fc9f8b6c2c6316c8fdf21e63/Details?lng=cs
4. RABBIT (MSR 2024): https://decan.lexpage.net/files/msr-2024c.pdf
5. BotHawk: https://arxiv.org/pdf/2307.13386
6. Lagopoulos & Tsoumakas, Content-aware web robot detection: https://intelligence.csd.auth.gr/wp-content/uploads/sites/319/2024/10/lagopoulos-apin-2020.pdf
7. Rovetta, Suchacka, Masulli, Bot recognition in a Web store: https://www.researchgate.net/publication/339258807_Bot_recognition_in_a_Web_store_An_approach_based_on_unsupervised_learning
8. Wikimedia Analytics API, page views: https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/concepts/page-views.html
9. Wikimedia Bot detection (Wikitech): https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/BotDetection
10. Web Bot Detection Dataset (BORDaR): https://bordar.bournemouth.ac.uk/272/
11. Iliou et al., Detection of Advanced Web Bots (ACM DTRAP): https://dl.acm.org/doi/pdf/10.1145/3447815
12. BOTracle: https://arxiv.org/pdf/2412.02266
13. Botcha: https://arxiv.org/pdf/2103.01428
14. Verify Googlebot and AI Crawlers (trustmyip): https://trustmyip.com/blog/verify-googlebot-ai-crawlers
15. Bot Verifier, Google 파일 이름 변경 (trustmyip): https://trustmyip.com/bot-verifier
