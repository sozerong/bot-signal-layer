"""최소 관측 경고 시험 두 개가 실제로 경고를 내는지 (handoff_v6.md Q12, handoff_v7.md 단계 5).

표본을 날짜를 옮겨 여섯 날로 만든다. 앞 네 날은 3,406행(짝수로 맞춤), 07-15는 그 절반 1,703행, 07-16은 3,406행이지만 근거를 돌리지 않는다.
  daily_rows_vs_median : 07-15 한 행 (비 정확히 0.5, 경계 포함)
  bot_share_vs_median  : 07-16 (판정이 없어 모든 계정이 실유저 아님, 봇 비율 1.0) 정책마다
그 밖의 날은 경고가 없어야 한다. 첫날(07-11)은 비교값이 없다.
실행: docker compose run --rm -e DBT_TARGET_PATH=/tmp/dbt_target -e DBT_LOG_PATH=/tmp/dbt_logs bot-job python -m pytest tests/test_daily_watch.py -q
     (Spark와 dbt, 약 3분)
"""
import gzip
import json
import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from warehouse import dbt_build

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
DAY = "2024-07-15"
FULL = ["2024-07-11", "2024-07-12", "2024-07-13", "2024-07-14"]
HALF, NOJUDGE = "2024-07-15", "2024-07-16"
TESTS = ("daily_rows_vs_median", "bot_share_vs_median")


def write(path, rows, day):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.writelines(json.dumps({**r, "created_at": r["created_at"].replace(DAY, day)}) + "\n" for r in rows)
    return str(path)


@pytest.fixture(scope="module")
def gold(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("watch")
    env = {**os.environ, "BOT_CATALOG": "local", "BOT_WAREHOUSE": str(tmp / "wh"), "SPARK_DRIVER_MEMORY": "2g",
           "DBT_TARGET_PATH": str(tmp / "dbt_target"), "DBT_LOG_PATH": str(tmp / "dbt_logs")}
    os.environ.update({k: env[k] for k in ("BOT_CATALOG", "BOT_WAREHOUSE", "SPARK_DRIVER_MEMORY")})
    from spark_jobs import create_tables, run_sql
    from spark_jobs.load_events import load
    from spark_jobs.session import spark_session

    rows = [json.loads(line) for line in gzip.open(FIX, "rt", encoding="utf-8")][:3406]
    dbt_build.write_evidence_current()
    reg = dbt_build.yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    fr = reg["features_repo"]["current"]
    feats = ["features_account.sql", "features_repo.sql" if fr == "f1" else f"features_repo_{fr}.sql"]
    ev = [ROOT / "sql" / "evidence" / k / f"{v['current']}.sql" for k, v in sorted(reg.items()) if dbt_build.is_gh(v)]
    s = spark_session("test_daily_watch", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    try:
        create_tables.create(s)
        for day in FULL + [HALF, NOJUDGE]:
            p = {"window_start": day, "window_end": day, "run_id": "test"}
            load(s, write(tmp / f"{day}.json.gz", rows[:1703] if day == HALF else rows, day), p)
            if day == NOJUDGE:
                continue
            for f in feats:
                run_sql.run(s, ROOT / "sql" / "silver" / f, p)
            for f in ev:
                run_sql.run(s, f, p)
    finally:
        s.stop()

    def dbt(*args):
        return subprocess.run(["dbt", *args, "--project-dir", str(dbt_build.DBT), "--profiles-dir", str(dbt_build.DBT),
                               "--vars", json.dumps({"ruleset_id": dbt_build.ruleset_id()})],
                              cwd=dbt_build.DBT, env=env, capture_output=True, text=True)

    r = dbt("build", "--select", "+real_user_events", "--exclude", "resource_type:test")
    assert r.returncode == 0, r.stdout[-3000:]
    r = dbt("test", "--select", *TESTS)
    res = json.loads((tmp / "dbt_target" / "run_results.json").read_text(encoding="utf-8"))["results"]
    status = {x["unique_id"].split(".")[2]: x["status"] for x in res}
    assert status.keys() == set(TESTS), r.stdout[-3000:]
    # 경고 행을 직접 본다: dbt가 돌린 시험 SQL(컴파일 결과)을 Gold에 다시 돌린다
    con = duckdb.connect(str(tmp / "wh" / "gold.duckdb"), read_only=True)
    con.sql("INSTALL iceberg; LOAD iceberg")
    compiled = tmp / "dbt_target" / "compiled" / "bot_gold" / "tests"
    out = {t: con.sql((compiled / f"{t}.sql").read_text(encoding="utf-8")).fetchall() for t in TESTS}
    cols = {t: con.sql((compiled / f"{t}.sql").read_text(encoding="utf-8")).columns for t in TESTS}
    yield status, {t: [dict(zip(cols[t], row)) for row in out[t]] for t in TESTS}
    con.close()


def test_half_rows_warns(gold):
    status, rows = gold
    assert status["daily_rows_vs_median"] == "warn"
    got = [(str(r["event_date"]), r["n"], r["n_prev"], float(r["ratio"])) for r in rows["daily_rows_vs_median"]]
    assert got == [(HALF, 1703, 4, 0.5)]


def test_missing_verdicts_raise_bot_share(gold):
    """판정 없음 = 실유저 아님(handoff_v7.md 1-1)이 만든 숫자 변화를 관측이 잡는다"""
    status, rows = gold
    assert status["bot_share_vs_median"] == "warn"
    got = rows["bot_share_vs_median"]
    assert {str(r["event_date"]) for r in got} == {NOJUDGE}, got
    assert all(float(r["bot_share"]) == 1.0 for r in got)
    assert {r["policy"] for r in got} >= {"balanced", "strict"}, got
