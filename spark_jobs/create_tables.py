"""Silver 빈 표를 4.2절 표 계약대로 만든다 (implementation_design.md 5절 create_tables, 8절 L3-1).

- 표의 모양은 여기 한 곳에서만 정한다. 작업(run_sql.py)은 표를 만들지 않고 이미 있는 표에 쓰기만 한다.
  SQL 결과가 계약과 맞지 않는 형(문자열을 숫자 칸에)이면 쓰기가 실패해 드러난다.
  다만 숫자끼리(BIGINT 결과를 INT 칸에)는 Spark의 ANSI 쓰기 규칙이 바꿔 쓰고, 값이 넘칠 때만 실행 중 실패한다 (2026-11-17 bot-job에서 확인).
- 있으면 그대로 둔다 (IF NOT EXISTS). 몇 번 돌려도 같다.
- 계약에 컬럼이 늘었으면 있는 표 끝에 더한다 (ADD COLUMNS, 옛 행은 NULL). 컬럼을 빼거나 형을 바꾸는 일은 하지 않는다 (결정 13).

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
        stars BIGINT, low_act_stars BIGINT, max_5m_stars BIGINT, max_co_star BIGINT,
        max_repos_per_hour BIGINT
    """,
    "bot.silver.evidence": """
        entity_type STRING, entity_key STRING, evidence_id STRING, rule_version STRING,
        window_start DATE, window_end DATE, hit BOOLEAN, score DOUBLE, metrics STRING,
        judged_at TIMESTAMP, sql_sha256 STRING, run_id STRING
    """,
    # 추가만 한다. (truth_source, entity_key, target_window, known_at 날짜)마다 하나 (build_truth.py의 MERGE)
    # target_window: 이 대상을 어느 판정 구간을 위해 골랐는지 (pick_targets의 window_end). 채점은 같은 구간 판정과만 짝짓는다 (결정 14)
    "bot.silver.truth": """
        entity_type STRING, entity_key STRING, entity_name STRING,
        truth_source STRING, label STRING, label_method STRING,
        observed_at TIMESTAMP, known_at TIMESTAMP, raw_ref STRING,
        target_window DATE
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
        have = set(spark.table(table).columns)
        new = [c.strip() for c in cols.replace("\n", " ").split(",") if c.strip() and c.split()[0] not in have]
        if new:  # ponytail: 새 컬럼은 계약 끝에만 붙인다. 중간에 넣으면 새로 만든 표와 순서가 달라진다
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(new)})")
        print(f"{table}: 있음" + (f", 컬럼 추가 {new}" if new else ""))


if __name__ == "__main__":
    create(spark_session("create_tables", {"spark.ui.enabled": "false"}))
