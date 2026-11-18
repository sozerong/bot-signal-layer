"""사후 확정 정답 갱신 (implementation_design.md 6절 late_truth). 주 1회.

repo_alive가 대상 고르기(sql/truth/pick_targets.sql)와 생존 확인을 함께 한다 (decisions.md 결정 7).
dbt는 truth가 바뀐 곳부터만 다시 만든다: stg_truth+ (eval_pairs, evaluation).
지시서는 `evaluation+`라고 적었지만 그러면 truth를 읽는 eval_pairs가 다시 만들어지지 않는다.
"""
import pendulum
from airflow.sdk import DAG, Param

from jobs import SILVER_WRITE, job

with DAG(
    dag_id="late_truth",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"days": Param("2024-07-15 2024-07-16 2024-07-17 2024-07-18 2024-07-19 2024-07-20 2024-07-21", type="string",
                          description="생존 확인할 판정 구간(window_end)들")},
    default_args={"retries": 0},
    tags=["truth"],
):
    alive = job("repo_alive", "for d in {{ params.days }}; do python -m truth.repo_alive --window-end $d; done", mem="4g")
    truth = job("build_truth", "python -m truth.build_truth", mem="4g", pool=SILVER_WRITE)
    gold = job("dbt_build", "python -m warehouse.dbt_build --select stg_truth+")
    record = job("record_build", "python -m warehouse.record_build", mem="2g")
    alive >> truth >> gold >> record
