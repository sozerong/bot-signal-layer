"""컬럼 리니지 지도 다시 만들기 (implementation_design.md 6절 column_lineage). sql/나 dbt 모델이 바뀐 뒤 손으로.

dbt compile로 Gold 컴파일 SQL을 만든 뒤 lineage.extract가 Silver SQL(spark)과 Gold(duckdb)를 함께 읽어 gold.duckdb lineage 스키마에 새 회차를 쓴다.
"""
import pendulum
from airflow.sdk import DAG

from jobs import job

with DAG(
    dag_id="column_lineage",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 17, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["lineage"],
):
    job("dbt_compile", "python -m warehouse.dbt_build compile", mem="4g") >> job("extract_lineage", "python -m lineage.extract", mem="4g")
