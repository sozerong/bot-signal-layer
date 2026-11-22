"""규칙 묶음 id가 판정을 바꾸는 변경만 잡는지 (docs/review_logic.md R-1, 결정 30).

뜻이 같은 변경(주석, 들여쓰기, CRLF, 안 쓰는 옛 버전 파일)은 같은 id, 판정이 바뀌는 변경(근거 SQL 문턱, 판정 모델,
피처 SQL, 정책 시드)은 다른 id. 처음 정의(시드 두 개만)는 근거 SQL을 버전을 올리지 않고 고치면 같은 id였다.
실행: python -m pytest tests/test_ruleset_id.py -q   (Spark, dbt 없이 돈다)
"""
import shutil
from pathlib import Path

import pytest

from warehouse import dbt_build

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def repo(tmp_path):
    for d in ("rules", "sql", "warehouse/dbt/models", "warehouse/dbt/seeds"):
        shutil.copytree(ROOT / d, tmp_path / d)
    # evidence_current.csv는 빌드 때 registry에서 만든다(.gitignore)
    reg = dbt_build.yaml.safe_load((tmp_path / "rules/registry.yml").read_text(encoding="utf-8"))
    rows = [f"{k},{v['current']},{v['family']}" for k, v in sorted(reg.items()) if "family" in v]
    (tmp_path / "warehouse/dbt/seeds/evidence_current.csv").write_text("\n".join(["evidence_id,rule_version,family", *rows]) + "\n", encoding="utf-8")
    return tmp_path


def edit(path, old, new):
    text = path.read_text(encoding="utf-8")
    assert old in text, f"{path.name}에 {old!r}가 없다"
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def test_same_meaning_same_id(repo):
    base = dbt_build.ruleset_id(repo)
    ev = repo / "sql/evidence/always_on/v1.sql"
    ev.write_text("-- 주석 한 줄 더\n" + ev.read_text(encoding="utf-8").replace("\n", "   \r\n") + "\n\n", encoding="utf-8")
    edit(repo / "warehouse/dbt/models/gold/entity_verdict.sql", "\n", "\n    {# 설명 #}\n")
    edit(repo / "sql/evidence/lockstep_ratio/v3.sql", "SELECT", "SELECT /* 안 쓰는 옛 버전 */")  # current는 v2
    assert dbt_build.ruleset_id(repo) == base


@pytest.mark.parametrize("path, old, new", [
    ("sql/evidence/always_on/v1.sql", "n_events >= 100", "n_events >= 50"),        # 버전을 안 올린 문턱 변경 (R-1)
    ("warehouse/dbt/models/gold/fake_star_repo.sql", "select", "select distinct"),  # 판정 조합 모델
    ("sql/silver/features_account.sql", "SELECT", "SELECT 1 AS x,"),               # 근거가 읽는 피처
    ("warehouse/dbt/seeds/policy_family.csv", "strict,behavior", "strict,infrastructure"),  # 정책
])
def test_verdict_change_new_id(repo, path, old, new):
    base = dbt_build.ruleset_id(repo)
    edit(repo / path, old, new)
    assert dbt_build.ruleset_id(repo) != base


def test_features_repo_version_switch(repo):
    base = dbt_build.ruleset_id(repo)
    edit(repo / "rules/registry.yml", "features_repo:  {current: f1}", "features_repo:  {current: f2}")
    assert dbt_build.ruleset_id(repo) != base  # f2는 다른 파일(features_repo_f2.sql)을 해시한다
