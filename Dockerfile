# bot-job: 로컬 Spark 실행 환경 (implementation_design.md 8절 L1-2).
# 버전은 C2 클러스터 3.0 이미지(Spark 4.1.2, Iceberg 1.11.0)와 맞춘다. 같은 이미지를 나중에 Airflow VM에서도 쓴다.
# trixie: Java 21이 배포판 저장소에 있다 (bookworm은 17까지).
FROM python:3.12-slim-trixie
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless && rm -rf /var/lib/apt/lists/*
# Iceberg 런타임을 이미지에 넣는다. 실행 중 내려받기가 없어야 시험이 인터넷 없이 돈다.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar /opt/spark-jars/
RUN chmod 644 /opt/spark-jars/*.jar
RUN pip install --no-cache-dir "pyspark==4.1.2" "pytest==8.4.2" "pyyaml==6.0.3"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=UTC
WORKDIR /work
