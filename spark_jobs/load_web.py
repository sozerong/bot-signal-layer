"""웹 원천 AUTH Bronze -> bot.silver.events (handoff_v5_wp1.md 1-3, 결정 26).

Bronze raw/auth_lib/public_v2.json은 JSON 객체 하나다. 첫 줄 "{", 끝 줄 "}" 사이에 한 줄에 요청 하나가 "id":{...}, 꼴로 있다.
그래서 multiLine JSON(파일 하나를 통째로 한 태스크가 읽는다) 대신 텍스트 줄로 읽어 줄마다 id와 객체를 떼어 파싱한다.
줄 단위라 Spark가 파일을 나눠 읽는다. 원본을 NDJSON으로 바꿔 두지 않는다 (Bronze는 받은 그대로, CLAUDE.md 3절 6번).

적재 기록표(bot.silver.load_audit)에 한 행을 남긴다. 칸은 GH 적재와 같고 뜻만 웹에 맞춘다 (event_date는 비운다: 한 달치 파일 하나):
  null_id = 줄 키(요청 id), null_created_at = timestamp, null_actor_login = ip, null_repo_name = resource, null_type = useragent
  useragent NULL은 세지만 막지 않는다 (UA 없는 요청도 원본이다). 나머지 넷과 버린 줄은 등록부 문턱을 넘으면 쓰지 않고 실패
  resource는 요청 없이 끊긴 줄(응답 408, 400, 요청 줄이 "-")을 빼고 센다. 그런 줄은 원래 resource가 없다 (F-23, 결정 34).
  events에는 action = 'no_request'로 남는다
manifest 줄(_manifest.json의 lines)과 읽은 줄이 다르면 막는다: 파일이 잘려 받혔다는 뜻이다 (GH는 dbt 시험 events_match_manifest가 본다).

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.load_web --param run_id=auth-1
"""
import json
import sys

import yaml

from spark_jobs import run_sql
from spark_jobs.load_events import AUDIT_TYPES, record
from spark_jobs.session import ROOT, spark_session

SRC = "auth_lib"
SCHEMA = ("ip STRING, useragent STRING, timestamp STRING, method STRING, resource STRING, "
          "bytes STRING, response STRING, referrer STRING")
REQUIRED = {"null_id": "id", "null_created_at": "timestamp", "null_actor_login": "ip", "null_repo_name": "resource"}


def load(spark, params, path=None):
    """path: 시험이 작은 파일을 줄 때만. 없으면 등록부의 bronze.path"""
    from pyspark.sql import functions as F

    reg = yaml.safe_load((ROOT / "sources" / "registry.yml").read_text(encoding="utf-8"))[SRC]
    path = path or ROOT / reg["bronze"]["path"]
    man = json.loads((path.parent / "_manifest.json").read_text(encoding="utf-8"))
    lines = (spark.read.text(str(path)).where(F.col("value").startswith('"'))  # "{"와 "}" 줄을 뺀다
             .selectExpr("*", "_metadata.file_path AS bronze_file"))
    parsed = lines.select(
        F.regexp_extract("value", r'^"([^"]+)":', 1).alias("id"),
        F.from_json(F.regexp_extract("value", r'^"[^"]+":(\{.*\}),?\s*$', 1), SCHEMA).alias("j"),
        "bronze_file")
    ok = (F.col("id") != "") & F.col("j").isNotNull()

    def n(cond):
        return F.sum(F.when(cond, 1).otherwise(0)).cast("bigint")

    cols = [F.count(F.lit(1)).alias("rows_read"), n(~ok).alias("rows_dropped"), n(ok & F.col("j.useragent").isNull()).alias("null_type")]
    no_req = F.col("j.response").isin("408", "400")  # 요청 없이 끊긴 줄. resource가 원래 없다 (F-23, 결정 34)
    cols += [n(ok & F.col(c if c == "id" else f"j.{c}").isNull() & (~no_req if c == "resource" else F.lit(True))).alias(k)
             for k, c in REQUIRED.items()]
    cols += [n(ok & no_req & F.col("j.resource").isNull()).alias("no_request")]
    a = parsed.agg(*cols).first().asDict()
    a.update(source=SRC, day=None, n_files=1, manifest_lines=man["lines"], late_rows=None, n_unknown_type=None, unknown_types=None)
    assert set(AUDIT_TYPES) - {"rows_written", "status", "reason", "run_id"} <= set(a)  # 넷은 record()가 채운다
    why, good, max_bad = [], a["rows_read"] - a["rows_dropped"], reg["audit"]["max_bad_ratio"]
    if a["rows_read"] != man["lines"]:
        why.append(f"읽은 줄 {a['rows_read']} != manifest 줄 {man['lines']} (잘려 받힌 파일)")
    if a["rows_read"] and a["rows_dropped"] / a["rows_read"] > max_bad:
        why.append(f"버린 줄 {a['rows_dropped']} / {a['rows_read']} > 문턱 {max_bad:.1%}")
    for k, c in REQUIRED.items():
        if good and a[k] / good > max_bad:
            why.append(f"{c} NULL {a[k]} / {good}행 > 문턱 {max_bad:.1%}")
    if why:
        record(spark, a, "rejected", "; ".join(why), params["run_id"])
        raise SystemExit(f"{SRC}: 쓰지 않고 멈춘다. " + "; ".join(why))
    parsed.where(ok).select("id", "j.*", "bronze_file").createOrReplaceTempView("bronze_auth")
    run_sql.run(spark, ROOT / "sql" / "silver" / "events_auth_lib.sql", params)
    record(spark, a, "ok", None, params["run_id"])
    print(f"load_audit {SRC}: 읽은 줄 {a['rows_read']}, 버린 줄 {a['rows_dropped']}, UA 없음 {a['null_type']}, "
          f"요청 없이 끊긴 줄 {a['no_request']}, 그 밖의 resource 없음 {a['null_repo_name']}")
    return a


def main():
    ap = run_sql.cli(__doc__)
    params = run_sql.parse_params(ap.parse_args().param)
    spark = spark_session(f"load_web {SRC}", run_sql.event_log_conf())
    try:
        load(spark, params)
    finally:
        spark.stop()  # 이벤트 로그를 닫는다 (F-2)


if __name__ == "__main__":
    sys.exit(main())
