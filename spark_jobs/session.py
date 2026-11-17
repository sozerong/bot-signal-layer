"""SparkSession. 카탈로그 이름은 어느 모드든 `bot`이다. SQL은 `bot.silver.events`처럼 쓰고 모드를 모른다.

모드는 환경 변수 BOT_CATALOG로 고른다 (implementation_design.md 3절, 8절 L1).
  local : 레포 안 warehouse_local/(git 제외)에 Iceberg Hadoop 카탈로그. 시험은 BOT_WAREHOUSE로 임시 폴더를 준다.
  rest  : Lakehouse runtime catalog (3절 설정). PROJECT_ID 환경 변수가 있어야 한다.
"""
import glob
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JARS = "/opt/spark-jars"  # bot-job 이미지가 Iceberg 런타임을 여기에 넣는다 (Dockerfile)


def spark_session(app, conf=None):
    from pyspark.sql import SparkSession

    mode = os.environ.get("BOT_CATALOG", "local")
    cat = "spark.sql.catalog.bot"
    # Iceberg SQL 확장(IcebergSparkSessionExtensions)은 켜지 않는다: 켜면 Spark 4.1의 이름 붙은 파라미터(:x)가 묶이지 않는다.
    # 확장 없이도 파티션 덮어쓰기, MERGE, 메타데이터 표, CALL 프로시저가 돈다 (failure_analysis.md F-1, decisions.md 결정 3)
    b = (SparkSession.builder.appName(app)
         .config(cat, "org.apache.iceberg.spark.SparkCatalog")
         # GH Archive의 created_at은 UTC다. 날짜(event_date)와 시간대(active_hours)를 UTC로 자른다
         .config("spark.sql.session.timeZone", "UTC"))
    if mode == "local":
        warehouse = os.environ.get("BOT_WAREHOUSE", str(ROOT / "warehouse_local"))
        b = (b.master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars", ",".join(sorted(glob.glob(f"{JARS}/*.jar"))))
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
             .config(f"{cat}.type", "hadoop")
             .config(f"{cat}.warehouse", f"file:{warehouse}"))
    elif mode == "rest":
        # ponytail: 클러스터에서는 같은 값이 클러스터 속성으로 이미 들어 있다(3절). 여기는 VM의 bot-job(local[2])이 같은 카탈로그에 쓸 때용.
        # GCS/인증 모듈(iceberg-gcp-bundle)은 이미지에 아직 없다. C1에서 VM용으로 넣는다 [확인 필요]
        project = os.environ["PROJECT_ID"]
        b = (b.config(f"{cat}.type", "rest")
             .config(f"{cat}.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
             .config(f"{cat}.warehouse", f"bl://projects/{project}/catalogs/bot_lh")
             .config(f"{cat}.header.x-goog-user-project", project)
             .config(f"{cat}.rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
             .config(f"{cat}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"))
    else:
        raise SystemExit(f"BOT_CATALOG는 local 또는 rest: {mode!r}")
    for k, v in (conf or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
