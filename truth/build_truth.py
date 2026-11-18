"""raw/truth_raw/repo_alive/ -> bot.silver.truth (implementation_design.md 8절 L4-2, 5절 build_truth).

truth는 추가만 한다. (truth_source, entity_key, known_at 날짜)가 이미 있으면 넣지 않는다 (MERGE).
그래서 같은 파일을 몇 번 다시 읽어도 truth가 늘지 않는다.

사용 (bot-job 컨테이너 안):
  python -m truth.build_truth
"""
import os
from pathlib import Path

from spark_jobs.session import ROOT, spark_session

SCHEMA = "entity_key STRING, entity_name STRING, stars BIGINT, grp STRING, http INT, attempts INT, checked_at STRING, window_end STRING"


def build(spark, files):
    (spark.read.schema(SCHEMA).json(files).selectExpr("*", "_metadata.file_path AS raw_file")
     .createOrReplaceTempView("truth_raw"))
    spark.sql((ROOT / "sql" / "truth" / "truth.sql").read_text(encoding="utf-8")).createOrReplaceTempView("new_truth")
    before = spark.table("bot.silver.truth").count()
    spark.sql("""
        MERGE INTO bot.silver.truth t USING new_truth s
        ON t.truth_source = s.truth_source AND t.entity_key = s.entity_key AND to_date(t.known_at) = to_date(s.known_at)
        WHEN NOT MATCHED THEN INSERT *""")
    after = spark.table("bot.silver.truth").count()
    print(f"truth: {before} -> {after}행 (+{after - before})")


def main():
    raw = Path(os.environ.get("BOT_BRONZE", ROOT / "raw")) / "truth_raw" / "repo_alive"
    build(spark_session("build_truth", {"spark.ui.enabled": "false"}), f"{raw}/checked_date=*/part-*.jsonl")


if __name__ == "__main__":
    main()
