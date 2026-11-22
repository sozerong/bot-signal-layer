# 용어집

이 문서는 README와 화면, 기록 문서에 설명 없이 나오는 말을 한곳에 모은 것이다.
처음 보는 사람(분석가, 멘토, 면접관)이 막힐 때 찾아본다. 정의는 코드와 맞춘다. 〔근거: handoff_v7.md 6-2〕

## 문제와 판정

- **봇 신호 레이어**: 원본 데이터는 그대로 두고, 그 옆에 "이 계정은 봇인가"를 판정한 결과를 근거, 규칙 버전과 함께 따로 쌓는 층이다. 판정을 원본에 굽지 않아서 규칙이 바뀌어도 다시 계산할 수 있다. 〔근거: docs/design_overview.md, 결정 24〕
- **실유저**: 고른 정책에서 봇으로 판정되지 않은 계정(또는 그 계정의 이벤트)이다. 판정이 아예 없는 계정은 실유저로 세지 않는다. 〔근거: 코드: warehouse/dbt/models/gold/real_user_events.sql. F-18〕
- **근거 (evidence)**: 봇이라고 볼 이유 하나를 계산하는 SQL 규칙이다. 예: `always_on`(하루 20시간 넘게 활동), `lockstep_ratio`(같은 계정 무리가 여러 저장소를 같이 star). 근거마다 버전이 있다. 〔근거: 코드: rules/registry.yml, sql/evidence/〕
- **계열**: 근거를 성격으로 묶은 것이다. 자기 신고(스스로 봇이라고 밝힘, 예: 이름이 `[bot]`으로 끝남, 웹에서는 크롤러 UA), 인프라(크롤러 사칭 등, 지금 근거가 없다), 조직(무리 지어 움직임, GitHub만), 행동(사람이 하기 힘든 활동량, 웹에서는 자산 없이 많이 받는 브라우저)의 넷이다. 〔근거: 코드: warehouse/dbt/seeds/policy_family.csv, rules/registry.yml. review_logic.md R-12〕
- **저활동 계정**: 그날 한 일이 star(WatchEvent) 한 종류뿐인 계정이다. 가짜 star 계정의 전제다. 〔근거: 코드: sql/silver/features_account.sql `is_low_activity`〕
- **가짜 star 저장소 (`fake_star_repo`)**: 저활동 계정의 star를 받았고, 무리 지어 같이 누른 흔적(lockstep) 또는 5분 안에 몰린 흔적(burst)이 있는 저장소다. 〔근거: 코드: warehouse/dbt/models/gold/fake_star_repo.sql. criteria.md 2.1절〕

- **원천**: 데이터를 가져오는 곳 하나다. 지금은 GitHub 공개 이벤트(`gh`)와 도서관 검색엔진 웹 로그(`auth_lib`, 2018-03 한 달) 둘이다. 원천마다 등록부에 한 항목이 있다. 〔근거: 코드: sources/registry.yml. 결정 26〕
- **대상 (entity)**: 판정을 받는 단위다. GitHub는 계정(`gh_account`)과 저장소(`gh_repo`), 웹은 `web_client`(가린 IP와 User-Agent의 조합)다. 판정은 대상 x 날짜마다 하나다. 〔근거: 코드: rules/registry.yml entity, sql/silver/events_auth_lib.sql actor_key〕
- **세션**: 웹에서 같은 대상의 요청을, 30분 넘게 쉬면 끊어 묶은 것이다. 웹 지표(세션 수, 검색 후 상세 전환율)의 단위다. 〔근거: 코드: sql/silver/events_auth_lib.sql. experiments/p0_bot_detection/REPORT.md 3절〕
- **AI 에이전트 (`is_ai_agent`)**: 사람 요청으로 대신 움직이는 AI 도구(UA가 AI 봇 목록의 "AI Assistants", "AI Agents" 같은 항목)다. 봇도 사람도 아니라서 어느 정책도 빼지 않고 따로 표시만 한다. 〔근거: 코드: sql/evidence/ai_agent_ua/v1.sql. criteria.md 4절〕

## 정책

- **정책**: 어느 계열까지 봇으로 뺄지 정한 묶음이다. 같은 데이터라도 쓰는 목적마다 알맞은 거름 강도가 달라서 셋을 함께 둔다. 〔근거: criteria.md 4절〕
- **lenient**: 스스로 봇이라고 밝힌 계정만 뺀다. 상용 분석 도구가 기본으로 거르는 수준이다. 〔근거: 코드: policy_family.csv lenient 줄〕
- **balanced**: lenient에 더해, 저활동이면서 그날 가짜 star 저장소에 star를 누른 계정과 가짜 star 저장소를 뺀다. 정답(나중에 확인된 저장소 삭제)으로 채점된 정책이다. 〔근거: 코드: entity_verdict.sql 조직 계열 줄. measurements.md M-21〕
- **strict**: balanced에 더해, 행동 근거(`always_on`: 하루 20시간 넘게 활동, `multi_repo_burst`: 한 시간에 저장소 100개 넘게 같은 행동)로 걸린 계정까지 뺀다. 웹 원천에서는 자산 없는 브라우저(`assetless_browser`)로 걸린 대상까지 뺀다. 저활동 하나만으로는 빼지 않는다. 채점되지 않았고, 그날 이벤트가 많은 계정만 빼므로 실험 대상 선정에 쓰면 치우친다. 보고용 상한으로만 쓴다. 〔근거: 코드: entity_verdict.sql 행동 계열 줄. measurements.md M-36 (b)〕
- **지표별 기본 정책**: 분석가가 정책을 고르지 않아도 되게 지표마다 정해 둔 정책이다. 잣대는 채점된 balanced다. lenient와 balanced의 차이가 그 지표의 자연 변동보다 크면 balanced, 아니면 lenient를 고른다. 〔근거: 코드: warehouse/dbt/seeds/metric_policy.csv, models/gold/metric_policy_proposal.sql. 결정 24 덧붙임〕

## 규칙의 버전

- **규칙 묶음 (ruleset)**: 판정을 정하는 입력 전부(정책 시드, 근거 SQL, 피처 SQL, 판정 모델)를 해시한 12자리 id다. 규칙이 하나라도 바뀌면 id가 바뀐다. 〔근거: 코드: warehouse/dbt_build.py `ruleset_id`. 결정 30〕
- **공개 묶음**: 분석가 숫자와 화면이 고정해 쓰는 규칙 묶음이다. 새 규칙으로 빌드해도 공개 줄을 더하기 전까지 분석가 숫자는 바뀌지 않는다. 지금은 `9eff1a5e22a6`이다. 〔근거: 코드: warehouse/dbt/seeds/ruleset_release.csv. 결정 24〕

## 채점

- **정답 (truth)**: 판정과 따로, 나중에 확인한 사실이다. GitHub에서는 판정 뒤 저장소가 지워졌는지(404)를 본다. 판정 뒤에 알게 된 정답만 채점에 쓴다. 〔근거: 코드: truth/repo_alive.py. criteria.md 3절〕
- **나중에 확인된 정답**: 판정할 때는 몰랐고 며칠 뒤 생존 확인으로 알게 된 정답이다. 이벤트보다 늦게 도착한다. 〔근거: 코드: dags/late_truth.py〕
- **정밀도**: 봇이라고 판정한 것 중 정답도 봇(삭제됨)인 비율이다. 〔근거: 코드: warehouse/dbt/models/gold/evaluation.sql〕
- **조합 안 정밀도**: 근거 하나의 정밀도를, 조합 판정이 고른 표본과 대조군 안에서만 잰 값이다. 근거 하나가 걸린 대상 전체의 정밀도가 아니다. 〔근거: 코드: sql/truth/pick_targets.sql. review_logic.md R-7〕

## 지표와 실험

- **MDE (최소 검출 효과)**: 반반으로 나눈 실험에서 잡을 수 있는 가장 작은 차이다. 값이 클수록 실험이 둔하다. 봇 몇 개가 분산을 키우면 커진다. 〔근거: 코드: warehouse/dbt/models/gold/policy_compare.sql rel_mde〕
- **자연 변동**: 거르기와 상관없이 지표가 날짜마다 바뀌는 정도다. 정책 차이가 이보다 작으면 정책을 바꿀 이유가 약하다고 본다. 〔근거: 코드: metric_policy_proposal.sql natural_var〕
- **star 상위 100**: 그날 star를 가장 많이 받은 저장소 100개다. 봇을 거르면 이 중 몇 개가 남는지 본다. 목록 자체는 거르지 않아도 날마다 73 ~ 89% 바뀐다. 〔근거: measurements.md M-36 (c)〕

## 데이터 플랫폼

- **Bronze, Silver, Gold**: 데이터를 다듬은 정도로 나눈 세 층이다. Bronze는 받은 원본 파일 그대로, Silver는 정리한 표(이벤트, 피처, 근거), Gold는 분석가가 바로 쓰는 표(정책별 실유저, 채점, 정책 비교)다. 〔근거: docs/data_model.md〕
- **Iceberg**: 파일 위에 표를 얹는 형식이다. 날짜 파티션을 통째로 갈아 끼우고, 예전 스냅샷으로 돌아갈 수 있다. Silver가 쓴다. 〔근거: 코드: spark_jobs/create_tables.py. engine_selection.md〕
- **Spark**: 큰 데이터를 여러 코어에 나눠 계산하는 엔진이다. Bronze에서 Silver를 만든다. 〔근거: 코드: spark_jobs/〕
- **spill**: Spark가 메모리가 모자랄 때 중간 결과를 디스크로 흘려 두고 계속 도는 것이다. 느려지지만 죽지는 않는다. 〔근거: measurements.md M-37〕
- **Airflow**: 작업 순서를 잡고 정해진 때에 돌리는 조율 도구다. 원천 받기, Silver → 근거 → Gold 빌드, 정답 확인을 DAG(작업 순서도)로 돈다. 〔근거: 코드: dags/〕
- **dbt**: SQL 모델을 순서대로 빌드하고 시험하는 도구다. Gold를 만든다. 〔근거: 코드: warehouse/dbt/〕
- **DuckDB, DuckDB-Wasm**: 한 대에서 도는 분석용 데이터베이스다. Gold가 DuckDB 파일이고, 화면은 브라우저 안에서 도는 DuckDB-Wasm으로 집계 파일을 읽는다. 〔근거: 결정 29. 코드: site/index.html〕
- **늦은 데이터**: 그날 파일에 다른 날짜의 이벤트가 섞여 오는 것이다. 섞이면 쓰기 전에 멈춘다. 〔근거: 결정 22. 코드: spark_jobs/load_events.py〕
- **리니지**: 어떤 컬럼이 어떤 컬럼에서 왔는지 이은 지도다. 규칙을 바꿀 때 다시 계산할 범위를 정한다. 〔근거: 코드: lineage/〕

## 기록 번호

- **M-번호**: 측정 기록([measurements.md](measurements.md)). 실행해서 얻은 값과 조건이다. 〔근거: CLAUDE.md 3절〕
- **F-번호**: 오류 기록([failure_analysis.md](failure_analysis.md)). 증상, 원인, 처방이다. 〔근거: CLAUDE.md 3절〕
- **결정 번호**: 확정한 설계 결정([decisions.md](decisions.md)). 고른 것, 이유, 잃는 것이다. 〔근거: CLAUDE.md 3절〕
- **R-번호**: 논리 점검에서 나온 의심과 처리 결과다(작업 문서라 공개하지 않는다). 〔근거: handoff_v6.md U1〕
