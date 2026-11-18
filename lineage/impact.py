"""규칙이나 피처 하나를 바꿨을 때 다시 돌 범위 (implementation_design.md 6절 rule_backfill의 lineage_impact, docs/img/lineage.png B).

1. 넓게: 바뀐 SQL 파일이 쓰는 컬럼에서 lineage.edge(가장 최근 회차)를 아래로 따라가 닿는 SQL 파일 전부.
   표의 행 집합('*')에 기대는 간선도 따라간다 (새 버전 행이 더해지면 행 집합이 바뀐다).
2. 좁히기: 컬럼 리니지는 행 조건을 모른다. 그래서 Silver에서는
   - 근거 SQL과 저장소 피처 SQL은 지금 버전(rules/registry.yml current)만. 옛 버전 파일은 다시 돌리지 않는다.
   - 정답 경로(sql/truth)는 뺀다.
   - 바뀐 파일이 쓰는 entity_type 파티션을 읽지 않는 파일은 뺀다 (예: 저장소 피처를 바꾸면 계정만 읽는 always_on은 범위 밖).
   Gold는 dbt 표가 통째로 다시 만들어지므로 모델 단위로 고른다. 정책이 근거 계열을 고르는 것은 시드와의 조인(행 조건)이라
   컬럼 리니지가 모른다. 그래서 모델의 dbt meta `evidence_families`(사람이 적음)와 바뀐 근거의 계열이 겹치지 않으면 뺀다.
   meta가 없는 모델은 모든 계열을 쓴다고 본다 (적는 것을 잊으면 빠지지 않고 다시 돈다). meta와 SQL이 맞는지는
   tests/test_gold_meta.py가 본다 (결정 15).
3. registry: 규칙 버전은 시드 evidence_current로 Gold에 들어간다. 근거 파일을 바꿨거나 --change rules/registry.yml이면
   registry -> 시드 간선(lineage/extract.py)에서도 출발해 시드와 그 아래가 범위에 든다. registry만 바꿨으면 바뀐 항목은
   마지막으로 만든 시드 csv와 비교해 찾는다.
마지막 줄: "<Silver SQL 파일들>|<dbt 모델들>" (Airflow가 이 줄을 XCom으로 받는다).

사용 (bot-job 컨테이너 안, column_lineage 뒤): python -m lineage.impact --change sql/evidence/lockstep_ratio/v3.sql
  registry만 바꿨을 때: python -m lineage.impact --change rules/registry.yml
"""
import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "warehouse_local" / "gold.duckdb"
DBT = ROOT / "warehouse" / "dbt"
REGISTRY_FILE = "rules/registry.yml"
SEED = "evidence_current"  # registry 간선이 닿는 시드 (lineage/extract.py registry_edges)


def changed_entries(reg):
    """registry에서 마지막으로 만든 시드 csv(warehouse/dbt_build.py가 씀)와 current가 다른 근거들"""
    seed = DBT / "seeds" / f"{SEED}.csv"
    built = {r["evidence_id"]: r["rule_version"] for r in csv.DictReader(seed.open(encoding="utf-8"))} if seed.exists() else {}
    return [k for k, v in reg.items() if "family" in v and built.get(k) != v["current"]]


def model_families():
    """dbt 모델 이름 -> meta evidence_families (적은 모델만). manifest는 dbt compile/build가 남긴 것"""
    nodes = json.loads((DBT / "target" / "manifest.json").read_text(encoding="utf-8"))["nodes"].values()
    return {n["name"]: set(n["config"]["meta"]["evidence_families"]) for n in nodes
            if n["resource_type"] == "model" and "evidence_families" in n["config"].get("meta", {})}


def feature_file(reg):
    """registry의 저장소 피처 버전 -> 파일 (f1은 이름에 버전이 없다, 결정 9)"""
    v = reg["features_repo"]["current"]
    return f"sql/silver/features_repo{'' if v == 'f1' else '_' + v}.sql"


def entity_types(sql, writes):
    pat = r"'(\w+)'\s+AS\s+entity_type" if writes else r"entity_type\s*=\s*'(\w+)'"
    return set(re.findall(pat, sql, flags=re.I))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--change", required=True, help="바뀐(새 버전) SQL 파일, 레포 기준 경로")
    change = ap.parse_args().change
    reg = yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    p = Path(change)
    starts = [change]
    if change == REGISTRY_FILE:
        entries = changed_entries(reg)
        if not entries:
            raise SystemExit("registry가 마지막으로 만든 시드와 같다. 바뀐 근거가 없다")
        starts = [change] + [f"sql/evidence/{k}/{reg[k]['current']}.sql" for k in entries]
    elif p.parent.parent.name == "evidence":
        starts = [change, REGISTRY_FILE]  # 새 버전은 registry를 바꿔야 Gold에 들어간다 (아래 확인)
    if p.parent.parent.name == "evidence" and reg[p.parent.name]["current"] != p.stem:
        raise SystemExit(f"rules/registry.yml의 {p.parent.name} current가 {p.stem}가 아니다. Gold가 새 버전을 읽지 않는다. 먼저 registry를 바꾼다")
    if p.stem.startswith("features_repo") and change != feature_file(reg):
        raise SystemExit(f"rules/registry.yml의 features_repo current가 {change}를 가리키지 않는다. 먼저 registry를 바꾼다")

    con = duckdb.connect(str(GOLD), read_only=True)
    run_id, = con.sql("SELECT max(run_id) FROM lineage.run").fetchall()[0]
    rows = con.execute("SELECT sql_file, target_table, target_column, source_table, source_column FROM lineage.edge WHERE run_id = ?",
                       [run_id]).fetchall()
    down = defaultdict(set)
    for f, tt, tc, st, sc in rows:
        down[(st, sc)].add((f, tt, tc))
    start = {(tt, tc) for f, tt, tc, _, _ in rows if f in starts}
    missing = [f for f in starts if not any(r[0] == f for r in rows)]
    if missing:
        raise SystemExit(f"리니지 회차 {run_id}에 {missing}가 없다. column_lineage를 먼저 돌린다")

    # 1. 넓게: 컬럼을 따라 내려간다. 파일은 처음 닿은 순서(얕은 것부터)로 둔다
    order, seen, frontier = list(starts), set(start), set(start)
    while frontier:
        nxt = set()
        for t, c in frontier:
            for f, tt, tc in down[(t, c)] | down[(t, "*")]:
                if f not in order:
                    order.append(f)
                for col in ((tt, tc), (tt, "*")):
                    if col not in seen:
                        seen.add(col)
                        nxt.add(col)
        frontier = nxt
    silver = [f for f in order if f.startswith("sql/")]
    gold = [f for f in order if f.startswith("warehouse/") or f == REGISTRY_FILE]
    print(f"리니지 회차 {run_id}, 바뀐 파일 {change}")
    print(f"1. 넓게: Silver SQL {len(silver)}개, Gold 모델 {len(gold)}개, 컬럼 {len(seen)}개")

    # 2. 좁히기 (Silver만)
    wrote = set().union(*(entity_types((ROOT / f).read_text(encoding="utf-8"), writes=True) for f in starts if f.startswith("sql/")))
    keep, why = [], []
    for f in silver:
        q = Path(f)
        if f in starts:
            keep.append(f)
        elif q.parts[1] == "truth":
            why.append(f"{f}: 정답 경로는 late_truth가 맡는다 (바깥 확인이라 다시 돌려 만들 수 없다)")
        elif q.parent.parent.name == "evidence" and reg[q.parent.name]["current"] != q.stem:
            why.append(f"{f}: 지금 버전 아님")
        elif q.stem.startswith("features_repo") and f != feature_file(reg):
            why.append(f"{f}: 지금 피처 버전 아님")
        elif wrote and not (wrote & entity_types((ROOT / f).read_text(encoding="utf-8"), writes=False)):
            why.append(f"{f}: {sorted(wrote)} 파티션을 읽지 않음")
        else:
            keep.append(f)
    # Gold: 바뀐 근거 계열 = 범위에 남은 근거 SQL의 계열. 근거가 없으면(예: events만 바뀜) 좁히지 않는다
    fams = {reg[Path(f).parent.name]["family"] for f in keep if Path(f).parent.parent.name == "evidence"}
    meta = model_families() if fams else {}
    models = []
    for f in gold:
        m = SEED if f == REGISTRY_FILE else Path(f).stem
        if m in meta and not (meta[m] & fams):
            why.append(f"{m}: meta 계열 {sorted(meta[m])}이 바뀐 계열 {sorted(fams)}과 겹치지 않음")
        else:
            models.append(m)
    print(f"2. 좁힘: Silver SQL {len(keep)}개, Gold 모델 {len(models)}개 (바뀐 근거 계열 {sorted(fams) or '없음'})")
    for w in why:
        print("   뺌", w)
    print(" ".join(keep) + "|" + (" ".join(models) or "none"))


if __name__ == "__main__":
    main()
