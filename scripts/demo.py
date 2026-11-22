"""표본 한 줄 실행 (handoff_v6.md Q3). Airflow 없이 bot-job 컨테이너 하나에서 끝까지 돈다.

tests/fixtures/gh_sample.json.gz(2024-07-15 12시, 3,407행)로
  create_tables -> load_events -> 피처(계정, 저장소) -> 지금 쓰는 근거 전부(rules/registry.yml current) -> dbt build -> 분석 쿼리 셋 출력.

창고는 레포 안 warehouse_demo/(git 제외). warehouse_local/(실제 Gold, docs/local_baseline.md 기준값)은 읽지도 쓰지도 않는다.
dbt 산출물(target, logs)도 warehouse_demo/ 아래에 둔다. warehouse/dbt/target은 다른 빌드가 쓴다.
다시 돌려도 같다: Silver는 파티션 덮어쓰기(run_sql.py), Gold는 dbt가 표를 다시 만든다.

dbt build에서 빼는 시험 하나: events_match_manifest. 표본은 Bronze 적재(manifest)를 거치지 않고, 그 시험은 /work/raw의 manifest를 읽는다
(사본과 CI에는 raw/가 없고, 이 PC에서는 하루 전체 줄 수와 비교해 늘 실패한다).

사용:
  docker compose run --rm bot-job python -m scripts.demo         # 전체
  docker compose run --rm bot-job python -m scripts.demo test    # 만든 데모 Gold에 dbt 시험만 (CI)
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WH = ROOT / "warehouse_demo"
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
DAY = "2024-07-15"
P = {"window_start": DAY, "window_end": DAY, "run_id": "demo"}
ANALYSES = ("q1_events_default_policy", "q2_why_excluded", "q3_ruleset_change")
NO_MANIFEST = ["--exclude", "events_match_manifest"]

# 아래 import(spark_jobs.session, dbt)가 이 값을 읽는다. 표본은 작아 드라이버 2g면 된다 (tests/test_judged_red.py와 같다)
os.environ.update({"BOT_CATALOG": "local", "BOT_WAREHOUSE": str(WH), "SPARK_DRIVER_MEMORY": "2g",
                   "DBT_TARGET_PATH": str(WH / "dbt_target"), "DBT_LOG_PATH": str(WH / "dbt_logs")})

from warehouse import dbt_build  # noqa: E402
from scripts.query import cell  # noqa: E402


def step(name, t0=[time.time()]):
    now = time.time()
    print(f"\n== {name}  (지난 단계 {now - t0[0]:.1f}초)", flush=True)
    t0[0] = now


def dbt(*args, quiet=False):
    """quiet: 출력을 실패할 때만 보인다 (compile은 컴파일한 SQL을 화면에 쏟는다)"""
    r = subprocess.run(["dbt", *args, "--project-dir", str(dbt_build.DBT), "--profiles-dir", str(dbt_build.DBT),
                        "--vars", json.dumps({"ruleset_id": dbt_build.ruleset_id()})],
                       cwd=dbt_build.DBT, capture_output=quiet, text=True)
    if r.returncode:
        sys.exit(f"{r.stdout or ''}dbt {args[0]} 실패 (종료 코드 {r.returncode})")


def silver():
    from spark_jobs import create_tables, run_sql
    from spark_jobs.load_events import load
    from spark_jobs.session import spark_session

    reg = dbt_build.yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    fr = reg["features_repo"]["current"]
    feats = ["features_account.sql", "features_repo.sql" if fr == "f1" else f"features_repo_{fr}.sql"]
    s = spark_session("demo", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    try:
        step("create_tables")
        create_tables.create(s)
        step(f"load_events {FIX.relative_to(ROOT).as_posix()}")
        load(s, str(FIX), P)
        step("피처")
        for f in feats:
            run_sql.run(s, ROOT / "sql" / "silver" / f, P)
        step("근거 (rules/registry.yml current)")
        for k, v in sorted(reg.items()):
            if dbt_build.is_gh(v):  # 웹 근거는 GH 표본에 0행이라 쓰기 확인이 실패한다 (F-22, 결정 26)
                run_sql.run(s, ROOT / "sql" / "evidence" / k / f"{v['current']}.sql", P)
    finally:
        s.stop()


def show(name):
    import duckdb

    sql = (WH / "dbt_target" / "compiled" / "bot_gold" / "analyses" / f"{name}.sql").read_text(encoding="utf-8")
    con = duckdb.connect(str(WH / "gold.duckdb"), read_only=True)
    r = con.sql(sql)
    print(f"\n-- {name}\n| " + " | ".join(r.columns) + " |\n|" + "---|" * len(r.columns))
    for row in r.fetchall():
        print("| " + " | ".join(cell(v) for v in row) + " |")
    con.close()


def main(args):
    WH.mkdir(exist_ok=True)
    dbt_build.write_evidence_current()
    if args == ["test"]:
        dbt("test", *NO_MANIFEST)
        return
    silver()
    step(f"dbt build (ruleset_id {dbt_build.ruleset_id()})")
    dbt("build", *NO_MANIFEST)
    step("분석 쿼리 셋 (warehouse/dbt/analyses)")
    dbt("compile", "--select", *ANALYSES, quiet=True)
    for a in ANALYSES:
        show(a)
    step("끝")


if __name__ == "__main__":
    main(sys.argv[1:])
