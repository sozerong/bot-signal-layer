"""규칙이나 피처 하나를 바꿨을 때 다시 돌 범위 (implementation_design.md 6절 rule_backfill의 lineage_impact, docs/img/lineage.png B).

1. 넓게: 바뀐 SQL 파일이 쓰는 컬럼에서 lineage.edge(가장 최근 회차)를 아래로 따라가 닿는 SQL 파일 전부.
   표의 행 집합('*')에 기대는 간선도 따라간다 (새 버전 행이 더해지면 행 집합이 바뀐다).
2. 좁히기: 컬럼 리니지는 행 조건을 모른다. 그래서 Silver에서는
   - 근거 SQL과 저장소 피처 SQL은 지금 버전(rules/registry.yml current)만. 옛 버전 파일은 다시 돌리지 않는다.
   - 정답 경로(sql/truth)는 뺀다.
   - 바뀐 파일이 쓰는 entity_type 파티션을 읽지 않는 파일은 뺀다 (예: 저장소 피처를 바꾸면 계정만 읽는 always_on은 범위 밖).
   Gold는 dbt 표가 통째로 다시 만들어지므로 모델 단위로만 고른다.
마지막 줄: "<Silver SQL 파일들>|<dbt 모델들>" (Airflow가 이 줄을 XCom으로 받는다).

사용 (bot-job 컨테이너 안, column_lineage 뒤): python -m lineage.impact --change sql/evidence/lockstep_ratio/v3.sql
"""
import argparse
import re
from collections import defaultdict
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "warehouse_local" / "gold.duckdb"


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
    start = {(tt, tc) for f, tt, tc, _, _ in rows if f == change}
    if not start:
        raise SystemExit(f"리니지 회차 {run_id}에 {change}가 없다. column_lineage를 먼저 돌린다")

    # 1. 넓게: 컬럼을 따라 내려간다. 파일은 처음 닿은 순서(얕은 것부터)로 둔다
    order, seen, frontier = [change], set(start), set(start)
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
    gold = [f for f in order if f.startswith("warehouse/")]
    print(f"리니지 회차 {run_id}, 바뀐 파일 {change}")
    print(f"1. 넓게: Silver SQL {len(silver)}개, Gold 모델 {len(gold)}개, 컬럼 {len(seen)}개")

    # 2. 좁히기 (Silver만)
    wrote = entity_types((ROOT / change).read_text(encoding="utf-8"), writes=True)
    keep, why = [], []
    for f in silver:
        q = Path(f)
        if f == change:
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
    models = [Path(f).stem for f in gold]
    print(f"2. 좁힘: Silver SQL {len(keep)}개, Gold 모델 {len(models)}개")
    for w in why:
        print("   뺌", w)
    print(" ".join(keep) + "|" + (" ".join(models) or "none"))


if __name__ == "__main__":
    main()
