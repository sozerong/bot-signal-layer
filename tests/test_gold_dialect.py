"""이식 대비 (4.3절, L5-3): dbt가 컴파일한 Gold 모델마다 sqlglot으로 duckdb -> bigquery 변환이 오류 없이 되는지.

dbt compile(또는 build) 뒤에 돈다. 컴파일 결과가 없으면 건너뛴다.
실행: docker compose run --rm bot-job pytest tests/test_gold_dialect.py
"""
from pathlib import Path

import pytest
import sqlglot
from sqlglot.errors import ErrorLevel

COMPILED = Path(__file__).resolve().parents[1] / "warehouse" / "dbt" / "target" / "compiled" / "bot_gold" / "models"
MODELS = sorted(COMPILED.rglob("*.sql")) if COMPILED.exists() else []


@pytest.mark.skipif(not MODELS, reason="dbt compile 결과가 없다")
@pytest.mark.parametrize("path", MODELS, ids=lambda p: p.stem)
def test_transpiles_to_bigquery(path):
    out = sqlglot.transpile(path.read_text(encoding="utf-8"), read="duckdb", write="bigquery", unsupported_level=ErrorLevel.RAISE)
    assert out and out[0].strip()
