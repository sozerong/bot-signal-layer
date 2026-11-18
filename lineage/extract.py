"""컬럼 리니지 추출 (implementation_design.md 8절 L8-1, 6절 column_lineage). 서울 동네 찾기 lineage/extract.py를 이 레포에 맞췄다.

입력 두 가지 (방언이 다르다):
  - Silver SQL (spark): sql/silver, sql/evidence, sql/truth. 파일 하나 = 대상 표 하나 (spark_jobs/run_sql.py TARGETS와 같은 규칙)
  - Gold dbt 컴파일 SQL (duckdb): warehouse/dbt/target. `dbt compile`(또는 build)이 먼저 돌아 있어야 한다
스키마는 DB에 묻지 않고 레포 안의 것으로 만든다: Silver는 create_tables.py의 계약, seed는 csv 첫 줄, 모델은 의존 순서대로 출력 컬럼.
임시 뷰(bronze, truth_raw)와 dbt 소스는 물리 이름(raw.*, bot.silver.*)으로 바꿔 넣는다. 그래야 Gold가 Silver에 이어진다.
dbt 소스는 manifest의 relation_name(로컬은 iceberg_scan(...), 클라우드는 `프로젝트.데이터셋.표`)을 그대로 찾아 바꾸고,
Gold 방언은 manifest의 adapter_type에서 읽는다. 로컬과 클라우드에서 이 파일의 추출 논리는 그대로다 (implementation_design.md 2절).
저장 위치(GOLD)만 로컬 DuckDB다. ponytail: C3에서 BigQuery 표로 바꿀 때 main()의 저장 부분만 바꾼다
저장: gold.duckdb의 lineage.run 한 행 + lineage.edge (회차마다 새 스냅샷).

사용 (bot-job 컨테이너 안): python -m lineage.extract
"""
import csv
import json
import re
import subprocess
import time
from graphlib import TopologicalSorter
from pathlib import Path

import duckdb
import sqlglot
from sqlglot.optimizer.qualify import qualify

from lineage.edges import extract_edges
from spark_jobs.create_tables import TABLES
from spark_jobs.run_sql import TARGETS

ROOT = Path(__file__).resolve().parents[1]
DBT = ROOT / "warehouse" / "dbt"
GOLD = ROOT / "warehouse_local" / "gold.duckdb"

# 임시 뷰와 소스 함수 -> 물리 이름. 컬럼은 load_events.py / build_truth.py가 읽는 모양
RAW = {
    ("raw", "gharchive", "events"): ["id", "type", "created_at", "actor", "repo", "bronze_file"],
    ("raw", "gharchive", "manifest"): ["hour", "source_url", "status", "file", "bytes", "sha256", "lines", "attempts", "fetched_at"],
    ("raw", "truth_raw", "repo_alive"): ["entity_key", "entity_name", "stars", "grp", "http", "attempts", "checked_at", "window_end", "raw_file"],
}
VIEWS = {"bronze": "raw.gharchive.events", "truth_raw": "raw.truth_raw.repo_alive"}
SILVER_TARGETS = {**{f"sql/{k}": v for k, v in TARGETS.items()},
                  "sql/truth/truth.sql": "bot.silver.truth",
                  "sql/truth/pick_targets.sql": "raw.truth_raw.repo_alive"}  # 확인 대상 목록 -> 확인 결과 파일


def silver_files():
    """[(대상 표, 파일 경로)]. 근거 SQL은 지금 버전만이 아니라 남아 있는 모든 버전."""
    out = [(t, ROOT / f) for f, t in SILVER_TARGETS.items()]
    out += [("bot.silver.evidence", p) for p in sorted((ROOT / "sql" / "evidence").glob("*/*.sql"))]
    return out


def with_views(sql):
    """FROM bronze 같은 임시 뷰를 물리 이름으로."""
    for view, phys in VIEWS.items():
        sql = re.sub(rf"\bFROM\s+{view}\b", f"FROM {phys}", sql, flags=re.I)
    return sql


SOURCES = {"silver": "bot.silver", "bronze": "raw.gharchive"}  # dbt 소스 이름 -> 물리 이름 앞부분


def gold_sql(sql, manifest):
    """dbt 컴파일 SQL의 소스 자리(relation_name)를 물리 이름으로. 예: iceberg_scan('.../silver/events') -> bot.silver.events"""
    for src in manifest["sources"].values():
        sql = sql.replace(src["relation_name"], f"{SOURCES[src['source_name']]}.{src['name']}")
    return sql


def build_schema(manifest):
    schema = {}

    def put(cat, db, name, cols):
        schema.setdefault(cat, {}).setdefault(db, {})[name] = {c: "text" for c in cols}

    for (cat, db, name), cols in RAW.items():
        put(cat, db, name, cols)
    for table, ddl in TABLES.items():
        cat, db, name = table.split(".")
        put(cat, db, name, [c.split()[0] for c in ddl.replace("\n", " ").split(",") if c.strip()])
    models = []
    nodes = {k: n for k, n in manifest["nodes"].items() if n["resource_type"] in ("model", "seed")}
    # 시드는 depends_on에 "nodes" 키가 없다 (failure_analysis.md F-9). 서울 동네 찾기는 모델만 넣어 이 키를 안 밟는다
    order = TopologicalSorter({k: [d for d in n["depends_on"].get("nodes", []) if d in nodes] for k, n in nodes.items()})
    for key in order.static_order():
        node = nodes[key]
        name = f"{node['database']}.{node['schema']}.{node['alias']}"
        if node["resource_type"] == "seed":
            with open(DBT / node["original_file_path"], encoding="utf-8", newline="") as f:
                put(node["database"], node["schema"], node["alias"], next(csv.reader(f)))
            continue
        sql = gold_sql((DBT / "target" / "compiled" / node["package_name"] / node["original_file_path"]).read_text(encoding="utf-8"), manifest)
        dialect = manifest["metadata"]["adapter_type"]
        tree = qualify(sqlglot.parse_one(sql, dialect=dialect), schema=schema, dialect=dialect, validate_qualify_columns=False)
        put(node["database"], node["schema"], node["alias"], tree.named_selects)
        models.append((name, node["original_file_path"], sql))
    return schema, models


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def main():
    started = time.perf_counter()
    manifest = json.loads((DBT / "target" / "manifest.json").read_text(encoding="utf-8"))
    schema, models = build_schema(manifest)
    edges = []
    for target, path in silver_files():
        rel = path.relative_to(ROOT).as_posix()
        edges += [{**e, "sql_file": rel, "file_kind": "spark_sql"}
                  for e in extract_edges(with_views(path.read_text(encoding="utf-8")), target, schema, dialect="spark")]
    for name, rel, sql in models:
        edges += [{**e, "sql_file": f"warehouse/dbt/{rel}", "file_kind": "dbt_model"}
                  for e in extract_edges(sql, name, schema, dialect=manifest["metadata"]["adapter_type"])]
    con = duckdb.connect(str(GOLD))
    con.sql("CREATE SCHEMA IF NOT EXISTS lineage")
    con.sql("""CREATE TABLE IF NOT EXISTS lineage.run (run_id INTEGER, created_at TIMESTAMPTZ, git_commit VARCHAR, git_dirty BOOLEAN,
               sqlglot_version VARCHAR, sql_files INTEGER, edges INTEGER, not_ok INTEGER)""")
    con.sql("""CREATE TABLE IF NOT EXISTS lineage.edge (run_id INTEGER, sql_file VARCHAR, file_kind VARCHAR, target_table VARCHAR,
               target_column VARCHAR, source_table VARCHAR, source_column VARCHAR, kind VARCHAR, subkind VARCHAR, status VARCHAR, detail VARCHAR)""")
    run_id = con.sql("SELECT coalesce(max(run_id), 0) + 1 FROM lineage.run").fetchall()[0][0]
    cols = ["sql_file", "file_kind", "target_table", "target_column", "source_table", "source_column", "kind", "subkind", "status", "detail"]
    con.executemany(f"INSERT INTO lineage.edge VALUES (?, {', '.join('?' * len(cols))})", [[run_id] + [e[c] for c in cols] for e in edges])
    bad = sum(e["status"] != "ok" for e in edges)
    con.execute("INSERT INTO lineage.run VALUES (?, now(), ?, ?, ?, ?, ?, ?)",
                [run_id, git("rev-parse", "HEAD"), bool(git("status", "--porcelain")), sqlglot.__version__,
                 len(silver_files()) + len(models), len(edges), bad])
    print(f"lineage 회차 {run_id}: SQL 파일 {len(silver_files()) + len(models)}개, 간선 {len(edges)}개 (ok 아님 {bad}), "
          f"{time.perf_counter() - started:.2f}초")
    for e in edges:
        if e["status"] != "ok":
            print("  ", e["status"], e["sql_file"], e["target_column"], e["source_column"], (e["detail"] or "")[:120])


if __name__ == "__main__":
    main()
