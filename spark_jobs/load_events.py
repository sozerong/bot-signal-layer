"""Bronze json.gz -> bot.silver.events (implementation_design.md 5절).

JSON 파싱만 여기서 한다. 컬럼 이름, 형 변환, 날짜는 sql/silver/events.sql이 한다.

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.load_events --input raw/dt=2024-07-15 --param window_end=2024-07-15 --param run_id=local-1
"""
from spark_jobs import run_sql
from spark_jobs.session import ROOT, spark_session

# 쓰는 필드만 읽는다. payload는 쓰는 근거가 없어 읽지 않는다 (4.2절)
SCHEMA = "id STRING, type STRING, created_at STRING, actor STRUCT<id: BIGINT, login: STRING>, repo STRUCT<id: BIGINT, name: STRING>"


def load(spark, files, params):
    # DROPMALFORMED: 깨진 줄은 버린다 (사전 실험 DuckDB의 ignore_errors=true와 같게). 버린 수는 manifest 줄 수와 행 수 차이로 본다
    (spark.read.schema(SCHEMA).option("mode", "DROPMALFORMED").json(files)
     .selectExpr("*", "_metadata.file_path AS bronze_file")
     .createOrReplaceTempView("bronze"))
    run_sql.run(spark, ROOT / "sql" / "silver" / "events.sql", params)


def main():
    ap = run_sql.cli(__doc__)
    ap.add_argument("--input", required=True, help="raw/dt=YYYY-MM-DD 같은 날짜 폴더, 또는 json.gz 경로(글롭)")
    a = ap.parse_args()
    files = a.input if a.input.endswith(".gz") else f"{a.input}/hour=*/*.json.gz"
    spark = spark_session(f"load_events {a.input}", run_sql.event_log_conf())
    load(spark, files, run_sql.parse_params(a.param))
    spark.stop()  # 이벤트 로그를 닫는다 (F-2)


if __name__ == "__main__":
    main()
