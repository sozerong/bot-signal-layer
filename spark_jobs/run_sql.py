"""SQL 파일 하나를 돌려 Iceberg 표의 그 파티션만 덮어쓴다 (implementation_design.md 5절).

- SQL 파일에는 SELECT만 있다. 쓰기는 여기서 한다. 리니지 추출기가 SELECT만 파싱하면 된다.
- 파라미터는 Spark의 이름 붙은 파라미터(:window_end)로 넘긴다. 문자열 치환을 하지 않는다.
- 근거 SQL(sql/evidence/<evidence_id>/<rule_version>.sql)은 경로에서 evidence_id와 rule_version을 읽고,
  judged_at, sql_sha256(파일 내용의 SHA-256), run_id를 붙인다.

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.run_sql --sql sql/evidence/burst_5m/v1.sql \
      --param window_start=2024-07-15 --param window_end=2024-07-15 --param run_id=local-1
"""
import argparse
import hashlib
import re
from pathlib import Path

from spark_jobs.session import ROOT, spark_session

# SQL 파일 -> (표, 파티션 컬럼). 4.2절 표 계약
TARGETS = {
    "silver/events.sql": ("bot.silver.events", ["source", "event_date"]),
    "silver/features_account.sql": ("bot.silver.entity_features", ["entity_type", "event_date"]),
    "silver/features_repo.sql": ("bot.silver.entity_features", ["entity_type", "event_date"]),
}
EVIDENCE = ("bot.silver.evidence", ["evidence_id", "rule_version", "window_end"])


def target(sql_path):
    """(표, 파티션 컬럼, (evidence_id, rule_version) 또는 None)"""
    p = Path(sql_path)
    if p.parent.parent.name == "evidence":
        return (*EVIDENCE, (p.parent.name, p.stem))
    return (*TARGETS[f"{p.parent.name}/{p.name}"], None)


def run(spark, sql_path, params):
    from pyspark.sql import functions as F

    text = Path(sql_path).read_text(encoding="utf-8")
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    table, parts, ev = target(sql_path)
    used = set(re.findall(r":(\w+)", text))  # SQL이 쓰는 파라미터만 넘긴다
    missing = used - set(params)
    if missing:
        raise SystemExit(f"{sql_path}: 파라미터가 없다 {sorted(missing)}")
    df = spark.sql(text, args={k: v for k, v in params.items() if k in used})
    if ev:
        df = df.select("entity_type", "entity_key", F.lit(ev[0]).alias("evidence_id"), F.lit(ev[1]).alias("rule_version"),
                       "window_start", "window_end", "hit", "score", "metrics",
                       F.current_timestamp().alias("judged_at"), F.lit(sha).alias("sql_sha256"),
                       F.lit(params["run_id"]).alias("run_id"))
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sparkContext.setJobDescription(f"write {table} <- {Path(sql_path).as_posix()}")
    if spark.catalog.tableExists(table):
        # 결과에 들어 있는 파티션만 바꾼다. 같은 날을 다시 돌려도 같은 결과(멱등).
        # ponytail: 결과가 0행이면 아무 파티션도 안 바뀌어 옛 행이 남는다. 근거는 대상 전부에 행을 내서 0행이 안 나온다
        df.writeTo(table).overwritePartitions()
    else:
        # ponytail: 처음 쓰는 작업이 표를 만든다. 근거 작업을 병렬로 처음 돌리면 둘이 동시에 만들 수 있다. C2에서 표를 미리 만든다
        df.writeTo(table).partitionedBy(*[F.col(c) for c in parts]).create()
    added = spark.sql(f"SELECT summary['added-records'] FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
    print(f"{Path(sql_path).as_posix()} -> {table}: {added}행")


def parse_params(pairs):
    return dict(p.split("=", 1) for p in pairs)


def cli(description=__doc__):
    ap = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    return ap


def event_log_conf():
    # 이벤트 로그는 레포 spark-events/(git 제외)에 남는다. 컨테이너가 지워져도 Spark UI 기록을 다시 볼 수 있다 (L3-4)
    (ROOT / "spark-events").mkdir(exist_ok=True)
    return {"spark.eventLog.enabled": "true", "spark.eventLog.dir": f"file:{ROOT / 'spark-events'}"}


def main():
    ap = cli()
    ap.add_argument("--sql", required=True)
    a = ap.parse_args()
    spark = spark_session(f"run_sql {a.sql}", event_log_conf())
    run(spark, a.sql, parse_params(a.param))


if __name__ == "__main__":
    main()
