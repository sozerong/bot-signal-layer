# 런북: 장애가 났을 때

이 문서는 장애가 났을 때 무엇을 보고 어떻게 고치는지 장애마다 적은 대응 순서다.
돌리다가 멈췄을 때 증상(로그 한 줄)으로 찾아 읽는다.

장애마다 증상(로그 한 줄이나 경고 이름), 확인 명령, 처방, 재발 방지, 관련 기록을 적는다. 〔근거: 문서: handoff_v6.md Q7〕
명령은 레포 폴더에서 돌린다. 계산 명령은 bot-job 컨테이너 안에서 돈다(`docker compose run --rm bot-job <명령>`). 〔근거: 코드: docker-compose.yml bot-job `profiles: [jobs]`, CLAUDE.md 5절〕
Git Bash에서 `/tmp/...` 같은 컨테이너 경로를 인자로 넘길 때는 앞에 `MSYS_NO_PATHCONV=1`을 붙인다. 안 붙이면 Git Bash가 Windows 경로로 바꿔 넘긴다. 〔근거: 판단: MSYS는 `/`로 시작하는 인자를 Windows 경로로 바꾼다. 반증 조건: PowerShell에서 돌리면 필요 없다〕
실제 창고(`warehouse_local/`)를 건드리지 않고 연습하려면 `-e BOT_WAREHOUSE=/tmp/<이름>`으로 임시 창고를 준다. 컨테이너가 끝나면 사라진다. 〔근거: 코드: spark_jobs/session.py 27행, warehouse/dbt/profiles.yml path〕

| 장애 | 첫 신호 | 절 | 근거 |
|---|---|---|---|
| 메모리 부족 | `Answer from Java side is empty`, `OutOfMemoryError` | 1 | 〔근거: 문서: failure_analysis.md F-12〕 |
| GitHub 429 | `429: 모두 N초 쉼`, `확인 실패 비율 … 문턱 … 넘는다` | 2 | 〔근거: 코드: truth/repo_alive.py 59, 196행〕 |
| 늦은 데이터 | `다른 event_date(NULL 포함)가 N행이다` | 3 | 〔근거: 코드: spark_jobs/load_events.py gate〕 |
| 병렬 커밋 손실 | `커밋이 표에 없다 (write-id …)` | 4 | 〔근거: 코드: spark_jobs/run_sql.py 85행〕 |
| 스키마 변화 | `처음 보는 이벤트 type`, `NULL … > 문턱` | 5 | 〔근거: 코드: spark_jobs/load_events.py load, gate〕 |
| DuckDB 잠금 | `Could not set lock on file … Conflicting lock is held` | 6 | 〔근거: 측정: 2026-11-22 bot-job에서 재현한 오류 문장(DuckDB 2.0.0)〕 |

---

## 1. 메모리 부족 (F-12)

**증상** 〔근거: 문서: failure_analysis.md F-12 증상, 해결〕
- 컨테이너가 죽을 때: `py4j.protocol.Py4JNetworkError: Answer from Java side is empty` 다음 `ConnectionRefusedError`. JVM이 통째로 사라진 것이다. 〔근거: 문서: failure_analysis.md F-12 로그 줄〕
- 힙이 모자랄 때(지금 설정에서 기대하는 꼴): `java.lang.OutOfMemoryError: Java heap space`. 컨테이너는 살아 있다. 〔근거: 문서: failure_analysis.md F-12 해결 "힙을 512m로 줄이면 … OOMKilled=false"〕

**확인 명령** 〔근거: 코드: dags/jobs.py 50 ~ 51행 `[mem.peak]`〕
- Airflow 태스크 로그 끝의 `[mem.peak] <바이트>`를 컨테이너 상한(`job(..., mem=)`)과 비교한다. 상한에 닿았으면 컨테이너 OOM이다. 〔근거: 코드: dags/jobs.py job(), dags/bot_build.py 태스크별 mem〕
- 손으로 재현할 때: 〔근거: 문서: failure_analysis.md F-12 관찰 "docker run -m 4g --memory-swap 4g"〕
```bash
docker ps -a --filter ancestor=bot-job:4.1.2 --format '{{.ID}} {{.Status}}'
docker inspect <컨테이너 id> --format '{{.State.OOMKilled}} {{.State.ExitCode}}'   # true 137 이면 컨테이너 OOM
```
- Airflow가 띄운 컨테이너는 끝나면 지워져 `OOMKilled`를 직접 못 본다. 그래서 `[mem.peak]` 줄을 본다. 〔근거: 문서: failure_analysis.md F-12 "남은 [확인 필요]"〕

**처방** 〔근거: 결정 13. 코드: dags/jobs.py heap()〕
- 컨테이너 OOM(137, `Answer from Java side is empty`)이면 힙이 상한보다 큰지 먼저 본다. 힙은 `heap(mem)`이 상한에서 계산하므로 `SPARK_DRIVER_MEMORY`를 따로 주지 않는다. 〔근거: 코드: dags/jobs.py 33 ~ 44행〕
- 힙 OOM이면 그 태스크의 `mem`을 올리거나, 무거운 집계를 피처 단계로 옮긴다(multi_repo_burst v2가 그렇게 했다). 〔근거: 결정 13. 코드: sql/evidence/multi_repo_burst/v2.sql 1 ~ 2행〕
- 실패한 날만 다시 돌린다: `bot_build`를 params.days에 그 날짜로. 날짜 파티션 덮어쓰기라 다시 돌려도 같다. 〔근거: 코드: dags/bot_build.py params days. 측정: M-9〕

**재발 방지** 〔근거: 측정: M-17〕
- 상한은 M-10, M-17의 최대 메모리에 여유를 두고 정한다(load_events 8g, 피처 12g, 근거 4g, dbt 8g). 근거 SQL이 events를 직접 읽게 바꾸면 4g 안인지 다시 잰다. 〔근거: 코드: dags/bot_build.py 50 ~ 57행. 문서: failure_analysis.md F-12 원인〕

**관련 기록** F-12, 결정 13, M-10, M-17. 〔근거: 문서: failure_analysis.md, decisions.md, measurements.md〕

---

## 2. GitHub 429 (F-15)

**증상** 〔근거: 코드: truth/repo_alive.py 59, 190 ~ 196행〕
- 로그에 `429: 모두 60초 쉼 (단계 1)`이 이어진다. 끝 줄 `요청 N번, 429 M번, 쉼 S초 …`. 〔근거: 코드: truth/repo_alive.py 59, 193행〕
- 확인 실패가 5%를 넘으면 `확인 실패 비율 x%가 문턱 5.0%를 넘는다 (F-15)`로 exit 1. late_truth의 build_truth 아래가 돌지 않는다. 〔근거: 코드: truth/repo_alive.py 196행. dags/late_truth.py 순서 alive >> truth〕

**확인 명령** 〔근거: 코드: truth/repo_alive.py 190행 그룹별 줄〕
- 태스크 로그의 그룹별 줄 `suspect n=… 404=… 확인 실패=…`, `control …`에서 어느 묶음이 비었는지 본다. 〔근거: 코드: truth/repo_alive.py 190행〕
- 결과 파일은 `raw/truth_raw/repo_alive/checked_date=<날짜>/`에 쌓인다. 덮어쓰지 않는다. 〔근거: 코드: truth/repo_alive.py 1 ~ 13행 "결과는 원본처럼 쌓는다"〕

**처방** 〔근거: 결정 16〕
- 시간을 두고 같은 구간으로 다시 돌린다. `--only-missing`이라 이미 확인한 저장소는 묻지 않고 실패한 것만 묻는다. 〔근거: 코드: dags/late_truth.py repo_alive 명령. 측정: M-21 "다시 돌리면 요청 0번"〕
```bash
docker compose run --rm bot-job python -m truth.repo_alive --window-end 2024-07-18 --only-missing
docker compose run --rm bot-job python -m truth.build_truth
docker compose run --rm bot-job python -m warehouse.dbt_build --select stg_truth+
```
- 여러 구간을 한 번에 묻지 않는다. 한 구간씩 나눠 돈다. 〔근거: 문서: failure_analysis.md F-15 "엿새치를 한 번에", 처방 후보 (C)〕

**재발 방지** 〔근거: 결정 16〕
- 429 하나에 모든 동시 요청이 같이 쉰다(Retry-After, 없으면 60, 120, 300초). 의심과 대조군을 번갈아 물어 막혀도 두 묶음이 같이 준다. 실패 비율 문턱으로 초록 뒤에 숨지 않게 한다. 〔근거: 코드: truth/repo_alive.py 5 ~ 9행, sql/truth/pick_targets.sql 순서 주석〕
- 한 번 확인한 저장소는 다시 묻지 않는 것이 한계다(정답이 바뀌어도 모름). 〔근거: 문서: review_logic.md R-9〕

**관련 기록** F-15, 결정 16, M-18, M-21. 〔근거: 문서: failure_analysis.md, decisions.md, measurements.md〕

---

## 3. 늦은 데이터 (F-16)

파일 날짜(window_end)와 다른 event_date가 입력에 한 줄이라도 있으면, 쓰면 그 다른 날짜 파티션 전체가 그 줄들로 바뀐다. 그래서 쓰기 전에 세어 멈춘다. 〔근거: 문서: failure_analysis.md F-16 원인. 코드: spark_jobs/run_sql.py overwritePartitions, spark_jobs/load_events.py gate〕

**증상** 〔근거: 코드: spark_jobs/load_events.py gate, load〕
- `load_events`가 exit 1: `events 2024-07-15: 쓰지 않고 멈춘다. 파일 날짜 2024-07-15와 다른 event_date(NULL 포함)가 N행이다. 쓰면 그 날짜 파티션을 덮어쓴다 (F-16, 결정 22)`. 〔근거: 코드: spark_jobs/load_events.py gate의 문장〕
- Airflow에서는 bot_build의 `load_events`가 빨갛고 아래 태스크가 돌지 않는다. 〔근거: 코드: dags/bot_build.py `d >> create >> load >> acct …`〕
- 적재 기록표 `bot.silver.load_audit`에 `status='rejected'`, `late_rows=N` 행이 남는다. 〔근거: 코드: spark_jobs/load_events.py record(), spark_jobs/create_tables.py load_audit〕

**확인 명령** (한 컨테이너 셸 안에서. 실제 날짜면 `INPUT='raw/dt=2024-07-15/hour=*/*.json.gz'`) 〔근거: 코드: sources/registry.yml gh.bronze.path〕

(가) 입력 파일마다 어느 날짜 줄이 몇 개인지. 하루치 실제 파일(약 490만 줄)은 몇 분 걸린다. 〔근거: 측정: M-36 (e) 하루 3,615,363 ~ 4,914,751건. 판단: 파이썬 json 한 줄씩이라 느리다〕
```bash
python - "$INPUT" <<'PY'
import collections, glob, gzip, json, sys
c = collections.Counter()
for p in sorted(glob.glob(sys.argv[1])):
    for line in gzip.open(p, "rt", encoding="utf-8"):
        try:
            c[(p, json.loads(line)["created_at"][:10])] += 1
        except Exception:
            c[(p, "깨진 줄")] += 1
for (p, d), n in sorted(c.items()):
    print(p, d, n)
PY
```
(나) 파티션이 그대로인지와 기록표. 〔근거: 코드: spark_jobs/create_tables.py events, load_audit 컬럼〕
```bash
python - <<'PY'
from spark_jobs.session import spark_session
s = spark_session("runbook_check", {"spark.ui.enabled": "false"})
s.sql("SELECT event_date, count(*) n, min(run_id) run_id FROM bot.silver.events GROUP BY 1 ORDER BY 1").show()
s.sql("SELECT event_date, status, rows_read, late_rows, run_id, left(reason, 60) reason "
      "FROM bot.silver.load_audit ORDER BY recorded_at").show(truncate=False)
s.stop()
PY
```

**처방** 〔근거: 결정 22. 코드: sources/registry.yml late_data〕
- 먼저 다른 날짜 파티션이 그대로인지 (나)로 본다. 가드는 쓰기 전에 멈추므로 그대로여야 한다. 아니면 이 절이 아니라 새 오류다. 〔근거: 코드: load_events.py load() "why가 있으면 events는 쓰지 않는다". 문서: failure_analysis.md F-16 처방 "07-14 지문이 그대로"〕
- (가)로 늦은 줄이 어디서 왔는지 가른다. 〔근거: 판단: 원인에 따라 고칠 곳이 다르다. 반증 조건: 늦은 줄이 입력 파일 어디에도 없으면 날짜식(events.sql과 audit의 to_date)이 어긋난 것이다〕
  - 입력 글롭이나 `--input`이 다른 날 파일까지 잡았다: 그날 파일만 주고 다시 돌린다. 날짜 파티션 덮어쓰기라 다시 돌려도 같다. 〔근거: 코드: load_events.py main `--input`. 측정: M-9〕
  - 원천 파일 안에 진짜 다른 날짜 줄이 있다: 그날은 적재하지 않은 채로 둔다(있던 파티션은 안전). Bronze는 고치지 않는다. 지금 등록부에는 `late_data: reject` 한 방식만 있어, 늦은 줄을 다른 날짜에 더하거나 버리는 방식은 사용자 결정으로 새로 만든다. failure_analysis.md에 원천 파일 이름과 줄 수를 적는다. 〔근거: 코드: load_events.py source() "아직 없는 방식이다". CLAUDE.md 3절 6번. sources/registry.yml 6행 ponytail〕

**재발 방지** 〔근거: 결정 22〕
- 가드는 등록부(`sources/registry.yml`)의 `late_data: reject`로 켜진다. 새 원천을 더할 때도 같은 항목을 둔다. 〔근거: 코드: sources/registry.yml 7 ~ 8행, load_events.py source()〕
- `tests/test_late_guard.py`가 가드를 켠 경우(멈춤, 전날 지문 그대로)와 끈 경우(전날 3,407행 → 1행)를 함께 본다. 〔근거: 코드: tests/test_late_guard.py〕

**연습: 표본으로 재현하고 처방까지** (실제 창고는 건드리지 않는다. 이벤트 로그는 익명 볼륨에 남아 컨테이너와 함께 지워진다) 〔근거: 코드: spark_jobs/run_sql.py event_log_conf() 레포 spark-events/. 판단: 연습 기록이 측정용 이벤트 로그에 섞이지 않게〕
```bash
MSYS_NO_PATHCONV=1 docker compose run --rm -T -e BOT_WAREHOUSE=/tmp/wh_runbook -v /work/spark-events bot-job bash -s <<'EOF'
mkdir -p /tmp/runbook
# 0. 전날 파일(표본을 07-14로 옮김)과 그날 파일(표본 07-15). 전날을 먼저 적재해 둔다
python - <<'PY'
import gzip, json
rows = [json.loads(l) for l in gzip.open("tests/fixtures/gh_sample.json.gz", "rt", encoding="utf-8")]
def w(p, rs):
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.writelines(json.dumps(r) + "\n" for r in rs)
w("/tmp/runbook/2024-07-14.json.gz", [{**r, "created_at": r["created_at"].replace("2024-07-15", "2024-07-14")} for r in rows])
w("/tmp/runbook/2024-07-15.json.gz", rows)
print("표본", len(rows), "행씩")
PY
python -m spark_jobs.create_tables
python -m spark_jobs.load_events --input /tmp/runbook/2024-07-14.json.gz --param window_end=2024-07-14 --param run_id=rb-prev
# 1. 장애: 글롭이 전날 파일까지 잡았다
python -m spark_jobs.load_events --input '/tmp/runbook/*.json.gz' --param window_end=2024-07-15 --param run_id=rb-bad; echo "exit=$?"
# 2. 확인: 위 (가) INPUT='/tmp/runbook/*.json.gz', (나)
# 3. 처방: 그날 파일만 주고 다시
python -m spark_jobs.load_events --input /tmp/runbook/2024-07-15.json.gz --param window_end=2024-07-15 --param run_id=rb-fix; echo "exit=$?"
# 4. 다시 (나): 07-14는 rb-prev 그대로, 07-15는 rb-fix, 기록표는 rejected 한 줄과 ok 두 줄
EOF
```
- 기대: 1에서 exit 1과 `다른 event_date(NULL 포함)가 3407행`, (나)에서 07-14 3,407행(rb-prev), 07-15 없음, 3 뒤 07-15 3,407행(rb-fix). 〔근거: 계산: 표본 3,407행이 모두 2024-07-15T12시(tests/fixtures/gh_sample.json.gz), 전날 파일 전부가 늦은 줄〕
- 2026-11-22에 이 블록 그대로 돌려 기대와 같았다(약 1분). 〔근거: 측정: 2026-11-22 13:36 ~ 13:37 bot-job 실행 출력〕
- 글롭 입력이면 `java.io.FileNotFoundException: File …/*.json.gz does not exist`가 한 번 찍히지만 장애가 아니다. Spark가 경로가 스트리밍 출력 폴더인지 보려고 글롭 문자열 그대로 파일을 찾다가 나는 것이고, 읽기는 계속된다. 실제 날짜 적재(`hour=*/*.json.gz`)에서도 같다. 〔근거: 측정: 같은 날 스택 첫 줄 `FileStreamSink$.hasMetadata(FileStreamSink.scala:58)`. 판단: 그 뒤 적재가 3,407행을 썼다〕

**관련 기록** F-16, 결정 22, M-27(가드 비용), 결정 27(적재 기록표). 〔근거: 문서: failure_analysis.md F-16, decisions.md, measurements.md M-27〕

---

## 4. 병렬 커밋 손실 (F-7)

**증상** 〔근거: 코드: spark_jobs/run_sql.py 85행〕
- `bot.silver.evidence: 커밋이 표에 없다 (write-id …). 다른 커밋에 덮였다 (F-7)`로 exit 1. 근거 태스크는 한 번 다시 돈다(`retries=1`). 〔근거: 코드: spark_jobs/run_sql.py 85행, dags/bot_build.py 근거 job retries=1〕
- 확인이 없던 때의 꼴: 태스크는 초록인데 그 파티션의 run_id가 앞 실행 것이고 로그에 `Failed to load committed snapshot`. 〔근거: 문서: failure_analysis.md F-7 증상〕

**확인 명령** 〔근거: 문서: failure_analysis.md F-7 확인 1〕
```bash
docker compose run --rm bot-job python - <<'PY'
from spark_jobs.session import spark_session
s = spark_session("runbook_f7", {"spark.ui.enabled": "false"})
s.sql("SELECT evidence_id, rule_version, window_end, min(run_id), max(run_id), count(*) FROM bot.silver.evidence "
      "WHERE window_end = DATE'2024-07-15' GROUP BY 1, 2, 3 ORDER BY 1").show(truncate=False)
s.stop()
PY
```
- 근거마다 run_id가 그 실행의 것인지 본다. 다른 실행 것이 남아 있으면 그 근거만 다시 돌린다. 〔근거: 문서: failure_analysis.md F-7 확인 1 "근거 다섯 파티션의 run_id가 모두 그 실행 것"〕

**처방** 〔근거: 결정 10〕
- 같은 날짜로 그 근거 태스크만 다시 돌린다(파티션 덮어쓰기라 같은 결과). 손으로 Silver를 쓸 때는 다른 Silver 쓰기와 겹치지 않게 한다. 〔근거: 코드: dags/jobs.py SILVER_WRITE 주석. 측정: M-9〕

**재발 방지** 〔근거: 결정 10〕
- Airflow 풀 `silver_write`(자리 1개)로 Silver 쓰기를 줄 세우고, `run_sql.run`이 쓴 뒤 자기 스냅샷(`bot.write-id`)을 확인한다. 클라우드 REST 카탈로그에서는 풀을 푼다. 〔근거: 코드: dags/jobs.py 17 ~ 20행 SILVER_WRITE, spark_jobs/run_sql.py 78 ~ 85행. 문서: failure_analysis.md F-7 원인 실험〕

**관련 기록** F-7, 결정 4, 결정 10, M-13. 〔근거: 문서: failure_analysis.md, decisions.md, measurements.md〕

---

## 5. 스키마 변화 (Q6)

원천이 필드를 조용히 빼거나 새 이벤트 type을 낸다. GitHub는 2025년에 Events API payload의 필드를 줄이겠다고 알렸고, PushEvent의 `payload.commits`가 빠져 이를 읽던 도구가 깨진 사례가 있다. 〔근거: 문서: [GitHub changelog 2025-08-08](https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/), [DEV 글](https://dev.to/flarecanary/github-silently-removed-payloadcommits-from-pushevent-heres-what-broke-and-how-to-catch-the-2i33). handoff_v6.md Q6〕
이 레포는 payload를 읽지 않으므로 그 변경 자체는 영향이 없다. 읽는 필드는 id, type, created_at, actor, repo다. 〔근거: 코드: spark_jobs/load_events.py SCHEMA "payload는 … 읽지 않는다"〕

**증상** 〔근거: 코드: spark_jobs/load_events.py gate, load〕
- 필수 필드가 빠짐: `events <날짜>: 쓰지 않고 멈춘다. actor.login NULL N / M행 = x% > 문턱 0.1%`. 기록표에 `status='rejected'`, `null_*` 칸. 〔근거: 코드: load_events.py REQUIRED, gate. sources/registry.yml gh.audit.max_bad_ratio 0.001〕
- 깨진 줄이 많음: `버린 줄 N / manifest 줄 M = x% > 문턱 0.1%`. 〔근거: 코드: load_events.py gate 첫 조건〕
- 처음 보는 type: 경고만 `처음 보는 이벤트 type X (N행). … known_types에 없다. 쓰기는 계속한다 (결정 28)`. 〔근거: 코드: load_events.py load 경고 줄〕
- 문턱 아래의 비율 변화: dbt 경고 시험 `type_share_shift`(전날 대비 type 비율 급변). 〔근거: 코드: warehouse/dbt/tests/type_share_shift.sql〕

**확인 명령** 〔근거: 코드: spark_jobs/create_tables.py load_audit 컬럼〕
```bash
docker compose run --rm bot-job python - <<'PY'
from spark_jobs.session import spark_session
s = spark_session("runbook_schema", {"spark.ui.enabled": "false"})
s.sql("SELECT event_date, status, rows_read, rows_dropped, null_id, null_type, null_created_at, null_actor_login, "
      "null_repo_name, n_unknown_type, unknown_types FROM bot.silver.load_audit ORDER BY recorded_at DESC LIMIT 10").show(truncate=False)
s.stop()
PY
```
- 원천 파일 한 줄을 직접 열어 필드가 있는지 본다: `python -c "import gzip,json; print(json.loads(next(gzip.open('raw/dt=2024-07-15/hour=00/<파일>.json.gz','rt'))).keys())"`. 〔근거: 판단: 원천이 바뀐 것인지 읽기가 틀린 것인지 가른다〕

**처방** 〔근거: 결정 27, 결정 28〕
- 필수 필드가 원천에서 사라졌으면 그 날짜는 적재하지 않은 채로 두고, 읽는 필드(SCHEMA)와 events.sql을 고칠지 사용자가 정한다. 문턱을 올려 통과시키지 않는다. 〔근거: 판단: 문턱을 올리면 키나 근거 입력이 NULL인 행이 Silver에 들어간다. 반증 조건: 그 필드를 쓰는 근거가 없으면 필수 목록에서 빼는 결정을 할 수 있다〕
- 새 type이면 근거 SQL이 그 type을 세는지 본다. 결과를 바꾸지 않으면 `known_types`에 더한다. 바꾸면 실패로 올리는 결정을 한다. 〔근거: 문서: handoff_v6.md Q6 판단과 반증 조건〕

**재발 방지** 〔근거: 결정 27, 28〕
- 적재마다 기록표 한 행, 문턱은 등록부 한 곳. 〔근거: 코드: sources/registry.yml gh.audit〕

**관련 기록** 결정 27, 결정 28, Q6 기록(F-17). 〔근거: 문서: handoff_v6.md Q5, Q6 기록 번호〕

---

## 6. DuckDB 잠금 (Q9 한계)

**증상** 〔근거: 측정: 2026-11-22 bot-job에서 두 프로세스로 재현(DuckDB 2.0.0)〕
- `IO Error: Could not set lock on file "…/gold.duckdb": Conflicting lock is held in /usr/local/bin/python3.12 (PID …)`. 〔근거: 측정: 같은 재현의 오류 문장〕
- 언제: dbt 빌드, 리니지 추출(`lineage.extract`), 빌드 기록, 화면 내보내기, 쿼리가 같은 `gold.duckdb`를 동시에 연다. 한 프로세스가 쓰기로 열면 다른 프로세스는 읽기도 못 한다. 〔근거: 문서: [DuckDB 동시성](https://duckdb.org/docs/2.0/connect/concurrency). docs/data_model.md 5절〕

**확인 명령** 〔근거: 판단: 잠근 프로세스를 찾는다〕
```bash
docker ps --filter ancestor=bot-job:4.1.2 --format '{{.ID}} {{.RunningFor}} {{.Command}}'
```
- 오류 문장의 PID는 컨테이너 안 번호다. 위 목록에서 같은 창고를 쓰는 컨테이너를 찾는다. 〔근거: 측정: 재현에서 PID 1(컨테이너의 첫 프로세스)〕

**처방** 〔근거: 판단: 잠금은 기다리면 풀리고, 억지로 풀면 파일이 깨질 수 있다〕
- 앞 작업이 끝나길 기다렸다가 다시 돌린다. 앞 작업 컨테이너를 죽이지 않는다. 〔근거: 판단: 같은 이유〕
- 두 작업이 함께 돌아야 하면 창고를 나눈다: 한쪽에 `-e BOT_WAREHOUSE=/tmp/<이름>`. dbt를 직접 돌리면 `-e DBT_TARGET_PATH=/tmp/dbt_target -e DBT_LOG_PATH=/tmp/dbt_logs`도 준다(`warehouse/dbt/target`은 하나뿐). 〔근거: 코드: warehouse/dbt/profiles.yml path. 문서: docs/data_model.md 5절〕

**재발 방지** 〔근거: 코드: dags/bot_build.py `gold >> record` 순서〕
- DAG 안에서는 dbt_build → record_build가 차례로 돈다. 손으로 돌리는 내보내기와 쿼리는 빌드가 끝난 뒤에 한다. 이식 뒤 BigQuery에서는 없어지는 문제다. 〔근거: 문서: direction_v6.md 14회차 "로컬 한계이고 이식 뒤 BigQuery에서는 없어지는 문제"〕

**관련 기록** 결정 6(dbt-duckdb), docs/data_model.md 5절. 〔근거: 문서: decisions.md 결정 6〕

## 7. 웹 원천 AUTH 다시 적재 (결정 26)

**언제** 〔근거: 결정 26 "매일 빌드에 넣지 않는다"〕
- AUTH는 한 달치 고정 파일이라 매일 DAG(bot_build)가 돌리지 않는다. 적재 SQL이나 웹 근거를 바꿨을 때, 참조 목록을 새로 적재했을 때 손으로 돌린다. 〔근거: 코드: dags/bot_build.py EVIDENCE(gh_ 대상만)〕

**명령** (차례로, 각각 bot-job 컨테이너에서) 〔근거: 코드: spark_jobs/load_web.py, spark_jobs/run_days.py, truth/build_truth.py〕
```bash
python -m spark_jobs.load_web --param run_id=auth-2
python -m spark_jobs.run_days --task sql/evidence/declared_ua/v1.sql --run-id auth-2 2018-02-28 2018-03-01 ... 2018-03-31
python -m spark_jobs.run_days --task sql/evidence/assetless_browser/v1.sql --run-id auth-2 <같은 32일>
python -m spark_jobs.run_days --task sql/evidence/ai_agent_ua/v1.sql --run-id auth-2 <같은 32일>
python -m truth.build_truth auth_library
python -m warehouse.dbt_build
```
- 날짜는 UTC로 2018-02-28(2시간)부터 03-31까지 32일이다. 〔근거: 코드: sources/registry.yml auth_lib 주석〕
- 정답은 근거 뒤에 돌린다. 정답의 known_at이 판정 시각보다 늦어야 채점에 들어간다. 〔근거: 코드: warehouse/dbt/models/gold/eval_pairs.sql known_at >= judged_at〕

**막혔을 때** 〔근거: 측정: F-23〕
- `auth_lib: 쓰지 않고 멈춘다. 읽은 줄 … != manifest 줄`: 원본이 잘려 있다. `raw/auth_lib/_manifest.json`의 md5와 파일 md5를 비교한다. 〔근거: 코드: load_web.py manifest 대조〕
- `resource NULL … > 문턱`: 요청 없이 끊긴 줄(408, 400) 말고도 resource가 빠졌다. 원본 키 묶음을 센다(F-23의 방법). 〔근거: 코드: load_web.py no_req. F-23〕
- 기록표 확인: `SELECT status, reason, rows_read, rows_dropped, null_repo_name FROM bot.silver.load_audit WHERE source = 'auth_lib' ORDER BY recorded_at`. 〔근거: 코드: create_tables.py load_audit〕
