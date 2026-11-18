"""규칙이나 피처를 바꾼 뒤 그 범위만 다시 돌리기 (implementation_design.md 6절 rule_backfill, 8절 L8).

손으로: params.change = 바뀐(새 버전) SQL 파일, params.days = 다시 돌 날짜들.
근거 규칙이면 rules/registry.yml의 current를 먼저 새 버전으로 바꾸고, column_lineage를 먼저 돌려 지도에 새 파일이 있어야 한다.
lineage_impact가 범위를 정하고 마지막 줄 "<Silver SQL들>|<dbt 모델들>"을 XCom으로 넘긴다 (lineage/impact.py).
Silver SQL은 한 태스크 안에서 차례로 돈다. 피처 -> 근거 순서가 impact 출력 순서다.
"""
import pendulum
from airflow.sdk import DAG, Param

from jobs import SILVER_WRITE, job

SCOPE = "ti.xcom_pull(task_ids='lineage_impact').split('|')"
P = "--param window_start=$d --param window_end=$d --param run_id={{ run_id }}"

with DAG(
    dag_id="rule_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 17, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"change": Param("sql/evidence/lockstep_ratio/v3.sql", type="string", description="바뀐(새 버전) SQL 파일"),
            "days": Param("2024-07-15 2024-07-16 2024-07-17 2024-07-18 2024-07-19 2024-07-20 2024-07-21", type="string")},
    default_args={"retries": 0},
    tags=["build"],
):
    impact = job("lineage_impact", "python -m lineage.impact --change {{ params.change }}", mem="2g")
    silver = job("rerun_silver", "for d in {{ params.days }}; do for f in {{ " + SCOPE + "[0] }}; do "
                 f"python -m spark_jobs.run_sql --sql $f {P}; done; done", pool=SILVER_WRITE)
    gold = job("dbt_build", "python -m warehouse.dbt_build --select {{ " + SCOPE + "[1] }}")
    record = job("record_build", "python -m warehouse.record_build", mem="2g")
    impact >> silver >> gold >> record
