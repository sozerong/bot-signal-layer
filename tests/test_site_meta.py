"""화면 데이터의 컬럼 사전과 변경 정책 (handoff_delivery.md D1, D5, D6. docs/guarantees.md).

- 공개된 site/data/meta.json의 컬럼마다 설명, 한글 이름, 단위가 있고 카드 값이 비지 않았다.
- 내보내기 검사 함수가 빈 설명과 말없이 빠진 컬럼을 실제로 잡는다.
"""
import json
from pathlib import Path

from scripts.export_site import missing_descriptions, removed_columns

ROOT = Path(__file__).resolve().parents[1]
META = json.loads((ROOT / "site/data/meta.json").read_text(encoding="utf-8"))


def test_published_columns_have_dictionary():
    bad = [f"{t}.{c['name']}" for t, v in META["tables"].items() for c in v["columns"]
           if not all(c.get(k) for k in ("description", "ko", "unit"))]
    assert not bad, bad


def test_product_card_filled():
    p = META["product"]
    for k in ("name", "intro", "owner", "contact", "refresh", "change_policy", "released_label", "released_on", "released_impact"):
        assert p.get(k), k
    assert len(p["change_policy"]) == 3


def test_checks_catch_blank_and_removed():
    tables = {"t": {"columns": [{"name": "a"}, {"name": "b"}]}}
    desc = {"t": {"columns": {"a": {"description": "x", "ko": "가", "unit": "개"}, "b": {"description": "", "ko": "나", "unit": "개"}}}}
    assert missing_descriptions(tables, desc) == ["t.b"]

    now = {"t": {"columns": [{"name": "a"}]}}
    assert removed_columns(tables, now, desc) == ["t.b"]  # 사유 없이 빠지면 실패
    desc["t"]["columns"]["b"]["removed"] = "v5에서 c로 대체"
    assert removed_columns(tables, now, desc) == []  # 사전에 사유를 적으면 통과
    assert removed_columns(now, tables, desc) == []  # 더하는 것은 언제나 통과
