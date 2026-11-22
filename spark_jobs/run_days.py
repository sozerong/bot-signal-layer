"""태스크 하나 = Spark 앱 하나, 그 안에서 날짜를 차례로 돈다 (결정 32, measurements.md M-38).
쓰기 단위는 그대로 날짜 파티션이다: 날짜마다 load()나 run_sql.run()이 덮어쓰기와 가드를 돈다.
사용: python -m spark_jobs.run_days --task load_events|<sql 경로> --run-id X 2024-07-15 2024-07-16 ...
ponytail: 중간 날짜에서 실패하면 다시 돌릴 때 앞 날짜도 다시 쓴다(덮어쓰기라 결과는 같다). 끝난 날짜 건너뛰기는 그게 느려질 때 run_id로 더한다
"""
import argparse

from spark_jobs import run_sql
from spark_jobs.load_events import load, source
from spark_jobs.session import spark_session


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--task", required=True, help="load_events 또는 sql 파일 경로")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("days", nargs="+")
    a = ap.parse_args()
    spark = spark_session(f"run_days {a.task}", run_sql.event_log_conf())
    try:
        for d in a.days:
            p = {"window_start": d, "window_end": d, "run_id": a.run_id}
            if a.task == "load_events":
                src = source("gh")
                load(spark, src["bronze"]["path"].format(day=d), p, src["late_data"])
            else:
                run_sql.run(spark, a.task, p)
    finally:
        spark.stop()  # 이벤트 로그를 닫는다 (F-2)


if __name__ == "__main__":
    main()
