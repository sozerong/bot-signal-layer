"""G2 실험 (M-47): 같은 앱 안에서 같은 날짜를 연달아 세 번 계산해 1회째와 2·3회째를 비교한다. 표에 쓰지 않는다(noop 싱크).

실행마다 같은 JVM(local 모드라 드라이버 = 실행기)에서 바로 읽는다:
  jit_ms   : HotSpot JIT 컴파일러가 쓴 누적 시간 차이 (CompilationMXBean, jstat -compiler의 Time과 같은 값)
  classes  : 새로 적재한 클래스 수 (ClassLoadingMXBean)
  codegen  : Spark 코드 생성(Janino) 컴파일 횟수 차이 = 생성 코드 캐시 미스 (CodegenMetrics.METRIC_COMPILATION_TIME 개수)
태스크 시간은 이벤트 로그에서 잡 설명 "m47 <tag> r<k> <날짜>"로 나눠 본다.
사용 (bot-job 안): python /n/g2_jit.py <sql 경로> <tag> d1 d2 d3
"""
import json
import re
import sys
import time

sys.path.insert(0, "/work")
from spark_jobs import run_sql  # noqa: E402
from spark_jobs.session import spark_session  # noqa: E402

sql_path, tag, days = sys.argv[1], sys.argv[2], sys.argv[3:]
spark = spark_session(f"m47 {tag} {sql_path}", run_sql.event_log_conf())
jvm = spark._jvm
mf = jvm.java.lang.management.ManagementFactory
comp, cl = mf.getCompilationMXBean(), mf.getClassLoadingMXBean()
cg = jvm.org.apache.spark.metrics.source.CodegenMetrics


def snap():
    return comp.getTotalCompilationTime(), cl.getTotalLoadedClassCount(), cg.METRIC_COMPILATION_TIME().getCount()


text = open(f"/work/{sql_path}", encoding="utf-8").read()
used = set(re.findall(r":(\w+)", text)) & {"window_start", "window_end"}
t_app = time.time()
out = []
for d in days:
    for k in (1, 2, 3):
        spark.sparkContext.setJobDescription(f"m47 {tag} r{k} {d}")
        a, t0 = snap(), time.time()
        df = spark.sql(text, args={p: d for p in used})
        df.write.format("noop").mode("overwrite").save()
        t1, b = time.time(), snap()
        row = {"tag": tag, "date": d, "rep": k, "wall_s": round(t1 - t0, 3), "jit_ms": b[0] - a[0], "classes": b[1] - a[1], "codegen": b[2] - a[2]}
        out.append(row)
        print("ROW", json.dumps(row), flush=True)
print("APP", json.dumps({"tag": tag, "app_id": spark.sparkContext.applicationId, "total_s": round(time.time() - t_app, 2),
                         "jit_ms_total": comp.getTotalCompilationTime(), "classes_total": cl.getTotalLoadedClassCount()}), flush=True)
spark.stop()
