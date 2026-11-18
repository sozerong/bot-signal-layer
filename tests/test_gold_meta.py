"""Gold 모델의 meta evidence_families가 SQL과 시드에 맞는지 (결정 15, 지시서 J3-2).

lineage/impact.py는 이 meta로 Gold 모델을 뺀다. meta가 실제보다 좁으면 다시 돌아야 할 모델이 빠진다. 그래서:
- 정책 뷰(is_real_<정책>으로 거르는 모델): meta = seeds/policy_family.csv의 그 정책 계열
- 근거 컬럼을 직접 읽는 모델: SQL에 나오는 근거 이름(registry)의 계열이 모두 meta 안에 있다
- meta를 적은 모델은 meta가 없는(모든 계열) 모델의 다른 컬럼을 읽지 않는다: 읽는 ref가 evidence_flags(근거 이름으로 위에서 검사)나
  real_user_events(정책 컬럼으로 위에서 검사)뿐이다
Spark 없이 돈다.
"""
import csv
import re
from collections import defaultdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "warehouse" / "dbt" / "models" / "gold"
CHECKED_REFS = {"evidence_flags", "real_user_events"}


def test_meta_matches_sql():
    reg = yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    family = {k: v["family"] for k, v in reg.items() if "family" in v}
    policy = defaultdict(set)
    for r in csv.DictReader((ROOT / "warehouse" / "dbt" / "seeds" / "policy_family.csv").open(encoding="utf-8")):
        policy[r["policy"]].add(r["family"])
    models = yaml.safe_load((GOLD / "gold.yml").read_text(encoding="utf-8"))["models"]
    meta = {m["name"]: set(m["config"]["meta"]["evidence_families"]) for m in models
            if "evidence_families" in m.get("config", {}).get("meta", {})}
    assert meta, "meta를 적은 모델이 없다"
    for name, fams in meta.items():
        sql = re.sub(r"--[^\n]*", "", (GOLD / f"{name}.sql").read_text(encoding="utf-8"))
        refs = set(re.findall(r"ref\('(\w+)'\)", sql))
        assert refs <= CHECKED_REFS, (name, refs - CHECKED_REFS)
        for p in re.findall(r"is_real_(\w+)", sql):
            assert fams == policy[p], (name, p, fams, policy[p])
        used = {family[e] for e in family if re.search(rf"\b{e}\b", sql)}
        assert used <= fams, (name, used - fams)
        assert used or "is_real_" in sql, (name, "근거도 정책도 안 읽는데 meta가 있다")
