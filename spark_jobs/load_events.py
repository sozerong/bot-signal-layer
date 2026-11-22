"""Bronze json.gz -> bot.silver.events (implementation_design.md 5절).

JSON 파싱만 여기서 한다. 컬럼 이름, 형 변환, 날짜는 sql/silver/events.sql이 한다.
원천의 Bronze 위치, 쓰기 방식, 늦은 데이터 처리는 sources/registry.yml에서 읽는다 (결정 22).

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.load_events --input raw/dt=2024-07-15 --param window_end=2024-07-15 --param run_id=local-1
  python -m spark_jobs.load_events --param window_end=2024-07-15 --param run_id=local-1   # --input 없으면 등록부의 bronze.path
"""
import yaml

from spark_jobs import run_sql
from spark_jobs.session import ROOT, spark_session

# 쓰는 필드만 읽는다. payload는 쓰는 근거가 없어 읽지 않는다 (4.2절)
SCHEMA = "id STRING, type STRING, created_at STRING, actor STRUCT<id: BIGINT, login: STRING>, repo STRUCT<id: BIGINT, name: STRING>"


def source(name="gh"):
    s = yaml.safe_load((ROOT / "sources" / "registry.yml").read_text(encoding="utf-8"))[name]
    if s["write"] != "overwrite_partition" or s["late_data"] != "reject":
        raise SystemExit(f"{name}: 등록부의 write={s['write']}, late_data={s['late_data']}는 아직 없는 방식이다 (결정 22)")
    return s


def reject_late(day):
    """쓰기 전에 파일 날짜(window_end)와 다른 event_date 행을 센다. 하나라도 있으면 실패.
    overwritePartitions는 결과에 한 행이라도 있는 파티션 전체를 바꾸므로, 다른 날짜 한 줄이 그날 파티션을 그 한 줄로 만든다 (F-16).
    ponytail: 데이터프레임을 한 번 더 센다(Bronze를 한 번 더 읽음). 비용은 M-27. 크면 쓰기 전 persist로 바꾼다"""
    from pyspark.sql import functions as F

    def check(df):
        n = df.where(~F.col("event_date").eqNullSafe(F.to_date(F.lit(day)))).count()
        if n:
            raise SystemExit(f"events: 파일 날짜 {day}와 다른 event_date(NULL 포함)가 {n}행이다. "
                             f"쓰면 그 날짜 파티션을 덮어쓴다. 쓰지 않고 멈춘다 (F-16, 결정 22)")
    return check


def load(spark, files, params, late_data="reject"):
    """late_data=None은 시험에서 가드 없는 손실을 재현할 때만 쓴다 (tests/test_late_guard.py)"""
    # DROPMALFORMED: 깨진 줄은 버린다 (사전 실험 DuckDB의 ignore_errors=true와 같게). 버린 수는 manifest 줄 수와 행 수 차이로 본다
    (spark.read.schema(SCHEMA).option("mode", "DROPMALFORMED").json(files)
     .selectExpr("*", "_metadata.file_path AS bronze_file")
     .createOrReplaceTempView("bronze"))
    check = reject_late(params["window_end"]) if late_data == "reject" else None
    run_sql.run(spark, ROOT / "sql" / "silver" / "events.sql", params, check=check)


def main():
    ap = run_sql.cli(__doc__)
    ap.add_argument("--input", help="raw/dt=YYYY-MM-DD 같은 날짜 폴더, 또는 json.gz 경로(글롭). 없으면 등록부의 bronze.path")
    a = ap.parse_args()
    params = run_sql.parse_params(a.param)
    src = source("gh")
    if a.input:
        files = a.input if a.input.endswith(".gz") else f"{a.input}/hour=*/*.json.gz"
    else:
        files = src["bronze"]["path"].format(day=params["window_end"])
    spark = spark_session(f"load_events {files}", run_sql.event_log_conf())
    load(spark, files, params, src["late_data"])
    spark.stop()  # 이벤트 로그를 닫는다 (F-2)


if __name__ == "__main__":
    main()
