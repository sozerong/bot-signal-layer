"""Silver 표의 내용 지문: 행 수와 행마다 xxhash64를 더한 값 (guarantees.md 재실행 멱등, L6 DAG 대 손 실행 대조).

행 순서와 파일 나뉨에 상관없이 내용이 같으면 같은 값이 나온다. 같은 행이 두 번 있으면 두 번 센다.
실행마다 바뀌는 컬럼(judged_at, run_id)은 뺀다.
xxhash64는 long 전체 범위라 그대로 더하면 넘친다(Spark 4는 ANSI가 기본이라 오류). DECIMAL(38,0)로 더한다:
491만 행 x 9.2e18 = 4.5e25 < 1e38 (failure_analysis.md F-3).

사용 (bot-job 컨테이너 안):
  python -m measure.table_digest --day 2024-07-15                         # 지금 표
  python -m measure.table_digest --day 2024-07-15 --table bot.silver.events --snapshots <앞 id> <뒤 id>
      # 두 스냅샷의 그날 지문을 비교하고, 다르면 EXCEPT ALL로 양쪽에만 있는 행을 센다
"""
import argparse

from spark_jobs.session import spark_session

SKIP = {"judged_at", "run_id"}
WHERE = {
    "bot.silver.events": "event_date = DATE(:day)",
    "bot.silver.entity_features": "event_date = DATE(:day)",
    "bot.silver.evidence": "window_end = DATE(:day)",
}


def rows(spark, t, snap=None):
    """그날 행에서 SKIP 컬럼을 뺀 SELECT 문"""
    cols = ", ".join(c for c in spark.table(t).columns if c not in SKIP)
    at = f" VERSION AS OF {int(snap)}" if snap else ""
    return f"SELECT {cols} FROM {t}{at} WHERE {WHERE[t]}"


def one(spark, t, day, snap=None):
    r = spark.sql(f"SELECT count(*) AS n, sum(CAST(xxhash64(*) AS DECIMAL(38,0))) AS h FROM ({rows(spark, t, snap)})",
                  args={"day": day}).first()
    return r.n, r.h


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--table", choices=list(WHERE))
    ap.add_argument("--snapshots", nargs=2, metavar=("A", "B"))
    a = ap.parse_args()
    spark = spark_session("table_digest", {"spark.ui.enabled": "false"})
    if not a.snapshots:
        for t in [a.table] if a.table else WHERE:
            n, h = one(spark, t, a.day)
            print(f"DIGEST {t} rows={n} hash={h}")
        return
    sa, sb = a.snapshots
    da, db = one(spark, a.table, a.day, sa), one(spark, a.table, a.day, sb)
    print(f"DIGEST {a.table}@{sa} rows={da[0]} hash={da[1]}")
    print(f"DIGEST {a.table}@{sb} rows={db[0]} hash={db[1]}")
    if da == db:
        print("SAME")
        return
    for x, y in ((sa, sb), (sb, sa)):
        diff = spark.sql(f"{rows(spark, a.table, x)} EXCEPT ALL {rows(spark, a.table, y)}", args={"day": a.day})
        print(f"ONLY@{x} rows={diff.count()}")
        diff.show(5, truncate=60)


if __name__ == "__main__":
    main()
