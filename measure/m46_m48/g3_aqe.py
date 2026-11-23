"""G3 선택 실험 (M-48): features_repo(자기 조인 포함) 07-15를 AQE 켬·끔으로. 표에 쓰지 않는다(noop 싱크).
JIT 데움이 섞이지 않게 첫 실행은 데움으로 버리고, 켬·끔을 번갈아 두 번씩. 잡 설명 "m48 <on|off> r<k>"로 이벤트 로그에서 나눈다.
사용 (bot-job 안): python /n/g3_aqe.py 2024-07-15
"""
import json
import sys
import time

sys.path.insert(0, "/work")
from spark_jobs import run_sql  # noqa: E402
from spark_jobs.session import spark_session  # noqa: E402

d = sys.argv[1]
spark = spark_session("m48 aqe features_repo", run_sql.event_log_conf())
text = open("/work/sql/silver/features_repo.sql", encoding="utf-8").read()
for i, mode in enumerate(["warm", "on", "off", "on", "off"]):
    spark.conf.set("spark.sql.adaptive.enabled", "false" if mode == "off" else "true")
    spark.sparkContext.setJobDescription(f"m48 {mode} r{i}")
    t0 = time.time()
    spark.sql(text, args={"window_end": d}).write.format("noop").mode("overwrite").save()
    print("ROW", json.dumps({"run": i, "mode": mode, "wall_s": round(time.time() - t0, 3)}), flush=True)
print("APP", json.dumps({"app_id": spark.sparkContext.applicationId}), flush=True)
spark.stop()
