"""Gold(DuckDB)에 SQL을 읽기 전용으로 돌려 마크다운 표로 찍는다. dbt를 몰라도 계약 뷰를 읽는 예 (지시서 1-5 (4)).

사용 (bot-job 컨테이너 안):
  python -m scripts.query "select policy, count(*) from actor_policy_status where event_date = date '2024-07-15' group by 1"
  python -m scripts.query warehouse/dbt/target/compiled/bot_gold/analyses/q1_events_default_policy.sql   # dbt가 컴파일한 분석 쿼리
"""
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def cell(v):
    if isinstance(v, float):
        return f"{int(v):,}" if v.is_integer() else f"{v:.4g}"
    return f"{v:,}" if isinstance(v, int) else ("" if v is None else str(v))


def main(arg):
    sql = Path(arg).read_text(encoding="utf-8") if arg.endswith(".sql") else arg
    r = duckdb.connect(str(ROOT / "warehouse_local" / "gold.duckdb"), read_only=True).sql(sql)
    print("| " + " | ".join(r.columns) + " |\n|" + "---|" * len(r.columns))
    for row in r.fetchall():
        print("| " + " | ".join(cell(v) for v in row) + " |")


if __name__ == "__main__":
    main(sys.argv[1])
