"""Bronze json.gz -> bot.silver.events (implementation_design.md 5절).

JSON 파싱만 여기서 한다. 컬럼 이름, 형 변환, 날짜는 sql/silver/events.sql이 한다.
원천의 Bronze 위치, 쓰기 방식, 늦은 데이터 처리, 적재 문턱은 sources/registry.yml에서 읽는다 (결정 22, 27, 28).

쓰기 전에 Bronze를 한 번 세어 적재 기록표(bot.silver.load_audit)에 한 행을 남긴다 (Q5, 결정 27):
  - 버린 줄(JSON으로 못 읽은 줄) ÷ manifest 줄, 또는 필수 필드 하나의 NULL ÷ 읽은 행이 문턱(등록부 audit.max_bad_ratio)을 넘으면 실패
  - 파일 날짜와 다른 event_date가 한 행이라도 있으면 실패 (F-16, 결정 22)
  - 등록부 known_types 밖의 type은 경고와 기록만 (Q6, 결정 28)
  막힌 시도도 status='rejected'로 기록표에 남고, events는 쓰지 않는다.

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.load_events --input raw/dt=2024-07-15 --param window_end=2024-07-15 --param run_id=local-1
  python -m spark_jobs.load_events --param window_end=2024-07-15 --param run_id=local-1   # --input 없으면 등록부의 bronze.path
"""
import glob
import json
import sys
import uuid
from pathlib import Path

import yaml

from spark_jobs import run_sql
from spark_jobs.session import ROOT, spark_session

# 쓰는 필드만 읽는다. payload는 쓰는 근거가 없어 읽지 않는다 (4.2절).
# _corrupt_record: JSON으로 못 읽은 줄의 원문 (PERMISSIVE). 전에는 DROPMALFORMED로 말없이 버려 버린 수를 몰랐다 (결정 27)
SCHEMA = "id STRING, type STRING, created_at STRING, actor STRUCT<id: BIGINT, login: STRING>, repo STRUCT<id: BIGINT, name: STRING>"
CORRUPT = "_corrupt_record"
# 필수 필드 (기록표 칸 이름: 식). 빠지면 events의 키나 근거 입력이 NULL이 된다 (direction_v6.md Q5)
REQUIRED = {"null_id": "id", "null_type": "type", "null_created_at": "created_at",
            "null_actor_login": "actor.login", "null_repo_name": "repo.name"}
AUDIT = "bot.silver.load_audit"
AUDIT_TYPES = {"source": "STRING", "n_files": "BIGINT", "manifest_lines": "BIGINT", "rows_read": "BIGINT",
               "rows_written": "BIGINT", "rows_dropped": "BIGINT", **{k: "BIGINT" for k in REQUIRED},
               "late_rows": "BIGINT", "n_unknown_type": "BIGINT", "unknown_types": "STRING",
               "status": "STRING", "reason": "STRING", "run_id": "STRING"}


def source(name="gh"):
    s = yaml.safe_load((ROOT / "sources" / "registry.yml").read_text(encoding="utf-8"))[name]
    if s["write"] != "overwrite_partition" or s["late_data"] != "reject":
        raise SystemExit(f"{name}: 등록부의 write={s['write']}, late_data={s['late_data']}는 아직 없는 방식이다 (결정 22)")
    return s


def manifest(files):
    """(파일 수, manifest 줄 합). 파일이 든 폴더마다 _manifest.json(status ok)이 있어야 줄 합을 낸다. 하나라도 없으면 None.
    표본처럼 json.gz 하나를 바로 주면 manifest가 없어 None이고, 그때 문턱의 분모는 읽은 줄이다"""
    paths = sorted(glob.glob(files))
    total = 0
    for d in sorted({str(Path(p).parent) for p in paths}):
        m = Path(d) / "_manifest.json"
        if not m.exists():
            return len(paths), None
        j = json.loads(m.read_text(encoding="utf-8"))
        if j.get("status") != "ok" or j.get("lines") is None:
            return len(paths), None
        total += j["lines"]
    return len(paths), (total if paths else None)


def audit(raw, day, known):
    """Bronze를 한 번 읽어 기록표 칸을 센다 (집계 한 번).
    늦은 행의 날짜식은 events.sql과 같다(to_date(to_timestamp(created_at))). events.sql의 날짜식을 바꾸면 여기도 바꾼다"""
    from pyspark.sql import functions as F

    ok = F.col(CORRUPT).isNull()

    def n(cond):
        return F.sum(F.when(cond, 1).otherwise(0)).cast("bigint")

    known_col = F.col("type").isin(list(known))
    late = ~F.to_date(F.to_timestamp("created_at")).eqNullSafe(F.to_date(F.lit(day)))
    cols = [F.count(F.lit(1)).alias("rows_read"), n(~ok).alias("rows_dropped")]
    cols += [n(ok & F.col(c).isNull()).alias(k) for k, c in REQUIRED.items()]
    cols += [n(ok & late).alias("late_rows"),
             n(ok & F.col("type").isNotNull() & ~known_col).alias("n_unknown_type"),
             F.array_sort(F.collect_set(F.when(ok & ~known_col, F.col("type")))).alias("unknown")]
    a = raw.agg(*cols).first().asDict()
    a["unknown_types"] = ",".join(a.pop("unknown")) or None
    return a


def gate(a, max_bad, late_data):
    """막을 이유 목록. 비면 쓴다"""
    why = []
    denom = a["manifest_lines"] or a["rows_read"]
    if denom and a["rows_dropped"] / denom > max_bad:
        why.append(f"버린 줄 {a['rows_dropped']} / {'manifest' if a['manifest_lines'] else '읽은'} 줄 {denom}"
                   f" = {a['rows_dropped'] / denom:.3%} > 문턱 {max_bad:.1%}")
    good = a["rows_read"] - a["rows_dropped"]
    for k, c in REQUIRED.items():
        if good and a[k] / good > max_bad:
            why.append(f"{c} NULL {a[k]} / {good}행 = {a[k] / good:.3%} > 문턱 {max_bad:.1%}")
    if late_data == "reject" and a["late_rows"]:
        why.append(f"파일 날짜 {a['day']}와 다른 event_date(NULL 포함)가 {a['late_rows']}행이다. "
                   f"쓰면 그 날짜 파티션을 덮어쓴다 (F-16, 결정 22)")
    return why


def record(spark, a, status, reason, run_id):
    """기록표에 한 행 추가. 쓴 뒤 내 스냅샷이 표에 있는지 본다 (Hadoop 카탈로그 동시 커밋 손실, F-7)"""
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(AUDIT):
        raise SystemExit(f"{AUDIT}가 없다. 먼저: python -m spark_jobs.create_tables")
    row = {k: a[k] for k in AUDIT_TYPES if k in a}
    row.update(rows_written=0 if status == "rejected" else a["rows_read"] - a["rows_dropped"],
               status=status, reason=reason, run_id=run_id)
    df = (spark.createDataFrame([tuple(row[k] for k in AUDIT_TYPES)], ", ".join(f"{c} {t}" for c, t in AUDIT_TYPES.items()))
          .withColumn("event_date", F.to_date(F.lit(a["day"])))
          .withColumn("recorded_at", F.current_timestamp())
          .select(*spark.table(AUDIT).columns))
    wid = str(uuid.uuid4())
    df.writeTo(AUDIT).option("snapshot-property.bot.write-id", wid).append()
    spark.sql(f"REFRESH TABLE {AUDIT}")
    if not spark.sql(f"SELECT 1 FROM {AUDIT}.snapshots WHERE summary['bot.write-id'] = :w", args={"w": wid}).collect():
        raise SystemExit(f"{AUDIT}: 커밋이 표에 없다 (write-id {wid}). 다른 커밋에 덮였다 (F-7)")


def load(spark, files, params, late_data="reject", gate_on=True, src="gh"):
    """late_data=None, gate_on=False는 시험에서 가드 없는 손실을 재현할 때만 쓴다 (tests/test_late_guard.py, tests/test_load_audit.py).
    둘 다 끈 때도 기록표는 남긴다. 돌려주는 값은 기록표에 쓴 칸 (시험용)"""
    cfg = yaml.safe_load((ROOT / "sources" / "registry.yml").read_text(encoding="utf-8"))[src]["audit"]
    day = params["window_end"]
    raw = (spark.read.schema(f"{SCHEMA}, {CORRUPT} STRING")
           .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", CORRUPT)
           .json(files)
           .selectExpr("*", "_metadata.file_path AS bronze_file"))
    n_files, lines = manifest(files)
    a = {**audit(raw, day, cfg["known_types"]), "source": src, "day": day, "n_files": n_files, "manifest_lines": lines}
    why = gate(a, cfg["max_bad_ratio"], late_data) if gate_on else []
    if lines is not None and lines != a["rows_read"]:  # 기록만. 날짜 단위 대조는 dbt 시험 events_match_manifest가 한다
        print(f"경고: manifest 줄 {lines}와 읽은 줄 {a['rows_read']}가 다르다", file=sys.stderr)
    if a["unknown_types"]:
        print(f"경고: 처음 보는 이벤트 type {a['unknown_types']} ({a['n_unknown_type']}행). "
              f"sources/registry.yml {src}.audit.known_types에 없다. 쓰기는 계속한다 (결정 28)", file=sys.stderr)
    if why:
        record(spark, a, "rejected", "; ".join(why), params["run_id"])
        raise SystemExit(f"events {day}: 쓰지 않고 멈춘다. " + "; ".join(why))
    # 깨진 줄만 뺀다 (전의 DROPMALFORMED와 같은 행). _corrupt_record만 참조하는 질의는 Spark가 막으므로 지운 뒤 넘긴다
    raw.where(f"{CORRUPT} IS NULL").drop(CORRUPT).createOrReplaceTempView("bronze")
    run_sql.run(spark, ROOT / "sql" / "silver" / "events.sql", params)
    record(spark, a, "ok", None, params["run_id"])
    print(f"load_audit {day}: 읽은 줄 {a['rows_read']}, 버린 줄 {a['rows_dropped']}, 늦은 행 {a['late_rows']}, "
          f"처음 보는 type {a['n_unknown_type']}")
    return a


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
