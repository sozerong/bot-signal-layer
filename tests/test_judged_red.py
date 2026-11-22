"""판정 빠짐 시험이 실제로 빨개지는지 (docs/review_logic.md R-6, failure_analysis.md F-18, handoff_v7.md 1-1).

늘 초록인 시험(policy_account_sum)을 다시 만들지 않으려고, 표본 하루로 Gold를 세 번 만들어 두 dbt 시험의 결과를 본다.
  근거 0개      : every_account_judged 실패, every_evidence_ran 실패
  always_on 빠짐: every_account_judged 통과(다른 근거로 판정이 생긴다), every_evidence_ran 실패
  근거 전부      : 둘 다 통과
실행: docker compose run --rm bot-job python -m pytest tests/test_judged_red.py -q   (Spark와 dbt, 약 2분)
"""
import json
import os
import subprocess
from pathlib import Path

import pytest

from warehouse import dbt_build

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
DAY = "2024-07-15"
P = {"window_start": DAY, "window_end": DAY, "run_id": "test"}
TESTS = ("every_account_judged", "every_evidence_ran")


def dbt(env, *args):
    return subprocess.run(["dbt", *args, "--project-dir", str(dbt_build.DBT), "--profiles-dir", str(dbt_build.DBT),
                           "--vars", json.dumps({"ruleset_id": dbt_build.ruleset_id()})],
                          cwd=dbt_build.DBT, env=env, capture_output=True, text=True)


def judged(env):
    """Gold를 다시 만들고 두 시험의 결과 {이름: pass|fail}"""
    r = dbt(env, "build", "--exclude", "resource_type:test")
    assert r.returncode == 0, r.stdout[-2000:]
    dbt(env, "test", "--select", *TESTS)
    target = Path(env.get("DBT_TARGET_PATH") or dbt_build.DBT / "target")  # 다른 빌드와 target을 나눠 쓸 때 (scripts/demo.py와 같은 변수)
    res = json.loads((target / "run_results.json").read_text(encoding="utf-8"))["results"]
    return {x["unique_id"].split(".")[2]: x["status"] for x in res}


def test_missing_evidence_turns_red(tmp_path):
    wh = tmp_path / "wh"
    env = {**os.environ, "BOT_CATALOG": "local", "BOT_WAREHOUSE": str(wh), "SPARK_DRIVER_MEMORY": "2g"}
    os.environ.update({k: env[k] for k in ("BOT_CATALOG", "BOT_WAREHOUSE", "SPARK_DRIVER_MEMORY")})
    from spark_jobs import create_tables, run_sql
    from spark_jobs.load_events import load
    from spark_jobs.session import spark_session

    dbt_build.write_evidence_current()
    reg = dbt_build.yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    ev = {k: ROOT / "sql" / "evidence" / k / f"{v['current']}.sql" for k, v in reg.items() if dbt_build.is_gh(v)}
    s = spark_session("test_judged_red", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    try:
        create_tables.create(s)
        load(s, str(FIX), P)
        for f in ("features_account.sql", "features_repo.sql"):
            run_sql.run(s, ROOT / "sql" / "silver" / f, P)
        assert judged(env) == {"every_account_judged": "fail", "every_evidence_ran": "fail"}

        for k, f in ev.items():
            if k != "always_on":
                run_sql.run(s, f, P)
        assert judged(env) == {"every_account_judged": "pass", "every_evidence_ran": "fail"}

        run_sql.run(s, ev["always_on"], P)
        assert judged(env) == {"every_account_judged": "pass", "every_evidence_ran": "pass"}
    finally:
        s.stop()
