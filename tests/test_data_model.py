"""docs/data_model.md의 표 목록이 코드와 같은지 (지시서 handoff_v6.md Q9).

- 2절(Silver 표) = spark_jobs/create_tables.py의 TABLES 키
- 3절(Gold 모델) = warehouse/dbt/models/gold/gold.yml의 models 이름 + 같은 폴더의 SQL 파일 이름
  (gold.yml에 항목이 없는 모델도 Gold에 만들어지므로 SQL 파일로도 센다)
표 이름은 각 절 표에서 첫 칸의 `백틱` 안 글자다. Spark 없이 돈다 (create_tables.py는 문자열로 읽는다).
"""
import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "data_model.md"
GOLD = ROOT / "warehouse" / "dbt" / "models" / "gold"


def doc_tables(section):
    """'## <section>'으로 시작하는 절의 표에서 첫 칸 `이름`을 모은다. 다음 '## '에서 끝난다."""
    names, inside = [], False
    for line in DOC.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            inside = line.startswith(f"## {section}")
            continue
        m = re.match(r"^\|\s*`([^`]+)`\s*\|", line)
        if inside and m:
            names.append(m.group(1))
    assert names, f"data_model.md에 '## {section}' 절의 표가 없다"
    return names


def silver_tables():
    tree = ast.parse((ROOT / "spark_jobs" / "create_tables.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TABLES" for t in node.targets):
            return {k.value for k in node.value.keys}
    raise AssertionError("create_tables.py에 TABLES가 없다")


def gold_models():
    yml = {m["name"] for m in yaml.safe_load((GOLD / "gold.yml").read_text(encoding="utf-8"))["models"]}
    sql = {p.stem for p in GOLD.glob("*.sql")}
    assert yml <= sql, f"gold.yml에만 있고 SQL 파일이 없는 모델: {sorted(yml - sql)}"
    return yml | sql


def test_silver_tables_match_create_tables():
    doc = doc_tables("2.")
    assert len(doc) == len(set(doc)), f"2절에 같은 표가 두 번: {doc}"
    code = silver_tables()
    assert set(doc) == code, f"문서에만: {sorted(set(doc) - code)}, 코드에만: {sorted(code - set(doc))}"


def test_gold_models_match_dbt():
    doc = doc_tables("3.")
    assert len(doc) == len(set(doc)), f"3절에 같은 모델이 두 번: {doc}"
    code = gold_models()
    assert set(doc) == code, f"문서에만: {sorted(set(doc) - code)}, 코드에만: {sorted(code - set(doc))}"
