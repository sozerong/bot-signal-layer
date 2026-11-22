"""raw/truth_raw/repo_alive/ -> bot.silver.truth (implementation_design.md 8절 L4-2, 5절 build_truth).

truth는 추가만 한다. (truth_source, entity_key, target_window, known_at 날짜)가 이미 있으면 넣지 않는다 (MERGE).
target_window 컬럼을 더하기 전에 넣은 행(NULL)은 같은 확인 줄의 구간으로 한 번 채운다. 다 채운 뒤에는 이 갈래가 아무 일도 안 한다 (결정 14).
그래서 같은 파일을 몇 번 다시 읽어도 truth가 늘지 않는다.

웹 원천 AUTH의 정답(auth_library, 고정 라벨)은 Silver events에서 sql/truth/auth_library.sql로 만든다. 근거를 돌린 뒤에 돌린다 (known_at >= judged_at).

사용 (bot-job 컨테이너 안):
  python -m truth.build_truth
  python -m truth.build_truth auth_library
"""
import os
import sys
from pathlib import Path

from spark_jobs.session import ROOT, spark_session

SCHEMA = "entity_key STRING, entity_name STRING, stars BIGINT, grp STRING, http INT, attempts INT, checked_at STRING, window_end STRING"


def build(spark, files):
    (spark.read.schema(SCHEMA).json(files).selectExpr("*", "_metadata.file_path AS raw_file")
     .createOrReplaceTempView("truth_raw"))
    spark.sql((ROOT / "sql" / "truth" / "truth.sql").read_text(encoding="utf-8")).createOrReplaceTempView("new_truth")
    merge(spark)


def merge(spark):
    """임시 뷰 new_truth -> bot.silver.truth (추가만, 같은 키는 넣지 않는다)"""
    before = spark.table("bot.silver.truth").count()
    spark.sql("""
        MERGE INTO bot.silver.truth t USING new_truth s
        ON t.truth_source = s.truth_source AND t.entity_key = s.entity_key AND to_date(t.known_at) = to_date(s.known_at)
           AND coalesce(t.target_window, s.target_window) = s.target_window
        WHEN MATCHED AND t.target_window IS NULL THEN UPDATE SET target_window = s.target_window
        WHEN NOT MATCHED THEN INSERT *""")
    after = spark.table("bot.silver.truth").count()
    print(f"truth: {before} -> {after}행 (+{after - before})")


def main():
    if sys.argv[1:] == ["auth_library"]:
        spark = spark_session("build_truth auth_library", {"spark.ui.enabled": "false"})
        spark.sql((ROOT / "sql" / "truth" / "auth_library.sql").read_text(encoding="utf-8")).createOrReplaceTempView("new_truth")
        return merge(spark)
    raw = Path(os.environ.get("BOT_BRONZE", ROOT / "raw")) / "truth_raw" / "repo_alive"
    build(spark_session("build_truth", {"spark.ui.enabled": "false"}), f"{raw}/checked_date=*/part-*.jsonl")


if __name__ == "__main__":
    main()
