"""GH Archive 하루치 받기 (implementation_design.md 6절 ingest_gh).

@daily, 2024-07-15 ~ 07-21 catchup. 시간 파일 24개를 받고(동시 4개) 다 받으면 Asset raw.gharchive에 그날을 싣는다.
받기는 기존 ingest.py 그대로다(완료된 파티션은 건너뜀). 그래서 이미 받은 날을 다시 돌려도 아무것도 다시 받지 않는다.
"""
import pendulum
from airflow.sdk import DAG, task

from assets import RAW_GH
from jobs import job

with DAG(
    dag_id="ingest_gh",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 7, 15, tz="UTC"),
    end_date=pendulum.datetime(2024, 7, 21, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,  # 동시 4개 (M-1과 같은 연결 수)
    default_args={"retries": 1},
    tags=["ingest"],
):
    hours = [job(f"fetch_hour_{h:02d}", f"python ingest.py fetch {{{{ ds }}}}T{h:02d} {{{{ ds }}}}T{h:02d} --root raw", mem="2g")
             for h in range(24)]

    @task(outlets=[RAW_GH])
    def publish(*, outlet_events, ds):
        outlet_events[RAW_GH].extra = {"day": ds}

    hours >> publish()
