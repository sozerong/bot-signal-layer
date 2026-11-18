"""빌드마다 ops.build_record에 한 줄 (implementation_design.md 4.3절, 6절 record_build).

build_id(dbt invocation id), 시각, git 커밋(+작업 트리가 더러웠나), 근거별 규칙 버전, 근거별 SQL 해시, dbt 결과 요약.
로컬에서는 gold.duckdb 안의 ops 스키마다.

사용 (bot-job 컨테이너 안, dbt_build 뒤): python -m warehouse.record_build
"""
import json
import subprocess
from collections import Counter

import duckdb

from warehouse.dbt_build import DBT, ROOT

GOLD = ROOT / "warehouse_local" / "gold.duckdb"


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def main():
    rr = json.loads((DBT / "target" / "run_results.json").read_text(encoding="utf-8"))
    status = dict(Counter(r["status"] for r in rr["results"]))
    con = duckdb.connect(str(GOLD))
    con.load_extension("iceberg")
    # 시드 파일이 아니라 Gold에 실제로 실린 표를 적는다. 기록은 실제로 만든 것을 적어야 한다 (failure_analysis.md F-10)
    rules = dict(con.sql("SELECT evidence_id, rule_version FROM main.evidence_current ORDER BY 1").fetchall())
    hashes = dict(con.sql("SELECT evidence_id, string_agg(DISTINCT sql_sha256, ',') FROM main.stg_evidence GROUP BY 1").fetchall())
    con.sql("CREATE SCHEMA IF NOT EXISTS ops")
    con.sql("""CREATE TABLE IF NOT EXISTS ops.build_record (
        build_id VARCHAR, built_at TIMESTAMPTZ, git_commit VARCHAR, git_dirty BOOLEAN,
        rule_versions JSON, sql_sha256 JSON, dbt_status JSON)""")
    con.execute("INSERT INTO ops.build_record VALUES (?, ?, ?, ?, ?, ?, ?)", [
        rr["metadata"]["invocation_id"], rr["metadata"]["generated_at"], git("rev-parse", "HEAD"), bool(git("status", "--porcelain")),
        json.dumps(rules), json.dumps(hashes), json.dumps(status)])
    print(con.sql("SELECT build_id, built_at, git_commit, git_dirty, dbt_status FROM ops.build_record ORDER BY built_at DESC LIMIT 1").fetchall()[0])


if __name__ == "__main__":
    main()
