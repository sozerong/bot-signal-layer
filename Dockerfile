# bot-job: 로컬 Spark 실행 환경 (implementation_design.md 8절 L1-2).
# 버전은 C2 클러스터 3.0 이미지(Spark 4.1.2, Iceberg 1.11.0)와 맞춘다. 같은 이미지를 나중에 Airflow VM에서도 쓴다.
# trixie: Java 21이 배포판 저장소에 있다 (bookworm은 17까지).
FROM python:3.12-slim-trixie AS bot-job
# git: Gold 빌드 기록(ops.build_record)과 리니지 회차가 커밋과 작업 트리 상태를 남긴다
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless git && rm -rf /var/lib/apt/lists/* \
 && git config --system --add safe.directory /work
# Iceberg 런타임을 이미지에 넣는다. 실행 중 내려받기가 없어야 시험이 인터넷 없이 돈다.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar /opt/spark-jars/
RUN chmod 644 /opt/spark-jars/*.jar
RUN pip install --no-cache-dir "pyspark==4.1.2" "pytest==8.4.2" "pyyaml==6.0.3"
# L5 Gold (결정 6): dbt-duckdb. sqlglot은 모델마다 duckdb -> bigquery 변환 시험(4.3절 이식 대비)
RUN pip install --no-cache-dir "dbt-core==1.12.6" "dbt-duckdb==1.11.1" "duckdb==2.0.0" "sqlglot==30.32.0"
# DuckDB가 Silver Iceberg 표를 읽는 확장을 빌드 때 받아 둔다. 실행 중 내려받기가 없다 (~/.duckdb/extensions)
RUN python -c "import duckdb; duckdb.connect().install_extension('iceberg')"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=UTC
WORKDIR /work

# ---------- 지휘: 로컬 Airflow (L6). 서울 동네 찾기 구성(결정 109) 그대로, 계산은 DockerOperator로 bot-job 컨테이너에서 ----------
# DockerOperator provider와 docker SDK는 Airflow 3.3.2 constraints(python 3.12)의 버전 (서울 동네 찾기와 같다)
FROM apache/airflow:3.3.2-python3.12 AS airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-docker==4.5.9" "docker==7.2.0"
