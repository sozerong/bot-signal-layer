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
import uuid
from pathlib import Path

from spark_jobs.session import ROOT, spark_session

# SQL 파일 -> 쓰는 표. 표의 모양과 파티션은 create_tables.py 한 곳에 있다 (4.2절 표 계약)
TARGETS = {
    "silver/events.sql": "bot.silver.events",
    "silver/features_account.sql": "bot.silver.entity_features",
    "silver/features_repo.sql": "bot.silver.entity_features",
    "silver/features_repo_f2.sql": "bot.silver.entity_features",  # 피처 버전 f2 (결정 9). 같은 파티션을 f1 대신 채운다
}
EVIDENCE = "bot.silver.evidence"


def target(sql_path):
    """(표, (evidence_id, rule_version) 또는 None)"""
    p = Path(sql_path)
    if p.parent.parent.name == "evidence":
        return EVIDENCE, (p.parent.name, p.stem)
    return TARGETS[f"{p.parent.name}/{p.name}"], None


def run(spark, sql_path, params, check=None):
    """check: 쓰기 전에 결과 데이터프레임을 받아 보는 함수. 막을 일이면 SystemExit (예: load_events의 늦은 데이터 거부, 결정 22)"""
    from pyspark.sql import functions as F

    text = Path(sql_path).read_text(encoding="utf-8")
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    table, ev = target(sql_path)
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
    # 표는 create_tables.py가 계약대로 미리 만든다. 여기서 만들지 않는다 (계약과 맞지 않는 형이면 쓰기가 실패해 드러나게)
    if not spark.catalog.tableExists(table):
        raise SystemExit(f"{table}가 없다. 먼저: python -m spark_jobs.create_tables")
    if ev:
        # 같은 규칙(sql_sha256)으로 같은 hit를 다시 낸 행은 새 판정이 아니다. 옛 판정 시각을 이어받는다.
        # 재시도나 백필로 다시 돌린 날의 채점이 사라지지 않게 (failure_analysis.md F-8, decisions.md 결정 11).
        # 규칙이나 결과가 다르면 지금 시각이다 (판정 뒤에 알게 된 정답만 쓴다는 조건은 그대로)
        old = (spark.table(table)
               .where((F.col("evidence_id") == ev[0]) & (F.col("rule_version") == ev[1])
                      & (F.col("window_end") == F.to_date(F.lit(params["window_end"]))))
               .select("entity_type", "entity_key", F.col("hit").alias("_hit"), F.col("sql_sha256").alias("_sha"),
                       F.col("judged_at").alias("_judged")))
        same = F.col("_sha").eqNullSafe(F.col("sql_sha256")) & F.col("_hit").eqNullSafe(F.col("hit"))
        df = (df.join(old, ["entity_type", "entity_key"], "left")
              .withColumn("judged_at", F.when(same, F.col("_judged")).otherwise(F.col("judged_at")))
              .drop("_hit", "_sha", "_judged"))
    if check:
        check(df)
    save_plan(spark, df, sql_path, params)
    spark.sparkContext.setJobDescription(f"write {table} <- {Path(sql_path).as_posix()}")
    # 결과에 들어 있는 파티션만 바꾼다. 같은 날을 다시 돌려도 같은 결과(멱등).
    # ponytail: 결과가 0행이면 아무 파티션도 안 바뀌어 옛 행이 남는다. 근거는 대상 전부에 행을 내서 0행이 안 나온다
    write_id = str(uuid.uuid4())
    df.writeTo(table).option("snapshot-property.bot.write-id", write_id).overwritePartitions()
    # 쓴 뒤 확인: 표를 다시 읽어 내 스냅샷이 있는지 본다. Hadoop 카탈로그는 동시 커밋이 서로를 조용히 덮을 수 있다
    # (failure_analysis.md F-7, 실험에서 조용한 손실 26개를 이 확인이 모두 잡았다). 없으면 실패로 끝내 Airflow가 다시 돌린다
    spark.sql(f"REFRESH TABLE {table}")
    got = spark.sql(f"SELECT summary['added-records'] FROM {table}.snapshots WHERE summary['bot.write-id'] = :w",
                    args={"w": write_id}).collect()
    if not got:
        raise SystemExit(f"{table}: 커밋이 표에 없다 (write-id {write_id}). 다른 커밋에 덮였다 (F-7)")
    print(f"{Path(sql_path).as_posix()} -> {table}: {got[0][0]}행")


def save_plan(spark, df, sql_path, params):
    """실행 계획(EXPLAIN FORMATTED)을 spark-events/plans/<앱 id>__<SQL 파일>.txt로 남긴다 (지시서 J4).
    이벤트 로그와 같은 앱 id라 히스토리 서버의 SQL 탭(최종 계획, 스테이지 번호)과 손으로 맞춰 볼 수 있다.
    ponytail: 실행 전 계획이다(AQE가 바꾸기 전). AQE가 합친 파티션 수는 SQL 탭의 최종 계획에서 본다. 자동 짝짓기는 만들지 않았다"""
    import contextlib
    import io

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain("formatted")
    plans = ROOT / "spark-events" / "plans"
    plans.mkdir(parents=True, exist_ok=True)
    name = Path(sql_path).as_posix().split("sql/", 1)[-1].replace("/", "_")
    (plans / f"{spark.sparkContext.applicationId}__{name}.txt").write_text(
        f"-- {Path(sql_path).as_posix()} {params}\n{out.getvalue()}", encoding="utf-8")


def parse_params(pairs):
    return dict(p.split("=", 1) for p in pairs)


def cli(description=__doc__):
    ap = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    return ap


def event_log_conf():
    # 이벤트 로그는 레포 spark-events/(git 제외)에 남는다. 컨테이너가 지워져도 Spark UI 기록을 다시 볼 수 있다 (L3-4)
    (ROOT / "spark-events").mkdir(exist_ok=True)
    # Spark 4.0부터 기본이 굴리는 폴더(v2)와 zstd 압축이다. 예전 형식(파일 하나, 평문 JSON)으로 돌려 표준 라이브러리로 읽는다 (failure_analysis.md F-2)
    return {"spark.eventLog.enabled": "true", "spark.eventLog.dir": f"file:{ROOT / 'spark-events'}",
            "spark.eventLog.rolling.enabled": "false", "spark.eventLog.compress": "false"}


def main():
    ap = cli()
    ap.add_argument("--sql", required=True)
    a = ap.parse_args()
    spark = spark_session(f"run_sql {a.sql}", event_log_conf())
    run(spark, a.sql, parse_params(a.param))
    spark.stop()  # 이벤트 로그를 닫는다 (.inprogress가 남지 않게, F-2)


if __name__ == "__main__":
    main()
