"""Silver 빈 표를 4.2절 표 계약대로 만든다 (implementation_design.md 5절 create_tables, 8절 L3-1).

- 표의 모양은 여기 한 곳에서만 정한다. 작업(run_sql.py)은 표를 만들지 않고 이미 있는 표에 쓰기만 한다.
  SQL 결과가 계약과 맞지 않는 형(문자열을 숫자 칸에)이면 쓰기가 실패해 드러난다.
  다만 숫자끼리(BIGINT 결과를 INT 칸에)는 Spark의 ANSI 쓰기 규칙이 바꿔 쓰고, 값이 넘칠 때만 실행 중 실패한다 (2026-11-17 bot-job에서 확인).
- 있으면 그대로 둔다 (IF NOT EXISTS). 몇 번 돌려도 같다.

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.create_tables
"""
from spark_jobs.session import spark_session

TABLES = {
    "bot.silver.events": """
        source STRING, event_id STRING, event_ts TIMESTAMP, event_date DATE,
        actor_key STRING, actor_name STRING, session_key STRING, action STRING,
        target STRING, target_name STRING,
        client_ua STRING, client_ip STRING, referrer STRING, status STRING,
        bronze_file STRING, run_id STRING
    """,
    "bot.silver.entity_features": """
        entity_type STRING, entity_key STRING, event_date DATE, feature_version STRING, entity_name STRING,
        n_events BIGINT, active_hours BIGINT, n_types BIGINT, is_low_activity BOOLEAN,
        stars BIGINT, low_act_stars BIGINT, max_5m_stars BIGINT, max_co_star BIGINT
    """,
    "bot.silver.evidence": """
        entity_type STRING, entity_key STRING, evidence_id STRING, rule_version STRING,
        window_start DATE, window_end DATE, hit BOOLEAN, score DOUBLE, metrics STRING,
        judged_at TIMESTAMP, sql_sha256 STRING, run_id STRING
    """,
    # 추가만 한다. (truth_source, entity_key, known_at 날짜)마다 하나 (build_truth.py의 MERGE)
    "bot.silver.truth": """
        entity_type STRING, entity_key STRING, entity_name STRING,
        truth_source STRING, label STRING, label_method STRING,
        observed_at TIMESTAMP, known_at TIMESTAMP, raw_ref STRING
    """,
}
PARTITIONS = {
    "bot.silver.events": "source, event_date",
    "bot.silver.entity_features": "entity_type, event_date",
    "bot.silver.evidence": "evidence_id, rule_version, window_end",
    "bot.silver.truth": "truth_source",  # 결정 7: 정답은 작고 출처별로 읽는다
}


def create(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bot.silver")
    for table, cols in TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING iceberg PARTITIONED BY ({PARTITIONS[table]})")
        print(f"{table}: 있음")


if __name__ == "__main__":
    create(spark_session("create_tables", {"spark.ui.enabled": "false"}))
