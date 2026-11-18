"""하루치 Silver -> 근거 -> Gold (implementation_design.md 6절 bot_build).

Asset raw.gharchive가 갱신되면 돈다. 여러 날이 한꺼번에 쌓였으면 한 실행에서 날짜를 차례로 돈다
(Asset 이벤트 여러 개가 실행 하나로 묶일 수 있어서). 손으로 돌릴 때는 params.days에 날짜를 띄어 쓴다.
근거 다섯 개는 서로 기대지 않지만 Silver 쓰기 풀(자리 1개) 때문에 하나씩 돈다 (F-7).
"""
from pathlib import Path

import pendulum
import yaml
from airflow.sdk import DAG, Param, task

from assets import RAW_GH
from jobs import SILVER_WRITE, job

# 근거 목록, 지금 규칙 버전, 저장소 피처 버전은 rules/registry.yml 한 곳에서 읽는다 (결정 9의 "잃는 것", 지시서 H4-2).
# Airflow 컨테이너에 rules/가 /opt/airflow/rules로 붙는다 (docker-compose.yml). registry를 바꾸면 다음 파싱부터 반영된다
REG = yaml.safe_load((Path(__file__).resolve().parents[1] / "rules" / "registry.yml").read_text(encoding="utf-8"))
EVIDENCE = [f"{k}/{v['current']}" for k, v in REG.items() if "family" in v]
FEAT = REG["features_repo"]["current"]
FEATURES_REPO = f"sql/silver/features_repo{'' if FEAT == 'f1' else '_' + FEAT}.sql"  # f1만 이름에 버전이 없다 (lineage/impact.py와 같은 규칙)
DAYS = "{{ ti.xcom_pull(task_ids='days') }}"
P = "--param window_start=$d --param window_end=$d --param run_id={{ run_id }}"


def each_day(cmd):
    return f"for d in {DAYS}; do {cmd}; done"


with DAG(
    dag_id="bot_build",
    schedule=[RAW_GH],
    start_date=pendulum.datetime(2024, 7, 15, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"days": Param("", type="string", description="손으로 돌릴 때 날짜들 (예: 2024-07-15 2024-07-16)")},
    default_args={"retries": 0},
    tags=["build"],
):
    @task
    def days(*, params, triggering_asset_events=None):
        got = sorted({e.extra["day"] for evs in (triggering_asset_events or {}).values() for e in evs if "day" in e.extra})
        out = got or params["days"].split()
        if not out:
            raise ValueError("돌릴 날짜가 없다 (Asset 이벤트도 params.days도 비었다)")
        return " ".join(out)

    d = days()
    # 상한은 M-10 최대 메모리에 여유를 두고 정한다. 힙은 job()이 상한에서 계산한다 (F-12, 결정 13, M-17)
    create = job("create_tables", "python -m spark_jobs.create_tables", mem="4g", pool=SILVER_WRITE)
    load = job("load_events", each_day(f"python -m spark_jobs.load_events --input raw/dt=$d {P}"), mem="8g", pool=SILVER_WRITE)
    acct = job("features_account", each_day(f"python -m spark_jobs.run_sql --sql sql/silver/features_account.sql {P}"), mem="12g", pool=SILVER_WRITE)
    repo = job("features_repo", each_day(f"python -m spark_jobs.run_sql --sql {FEATURES_REPO} {P}"), mem="12g", pool=SILVER_WRITE)
    # 근거 다섯 개는 풀 때문에 하나씩 돈다. 쓴 뒤 확인(run_sql.run)이 실패를 내면 한 번 다시 돈다 (F-7)
    ev = [job(f"evidence_{e.split('/')[0]}", each_day(f"python -m spark_jobs.run_sql --sql sql/evidence/{e}.sql {P}"), mem="4g",
              pool=SILVER_WRITE, retries=1) for e in EVIDENCE]
    gold = job("dbt_build", "python -m warehouse.dbt_build", mem="8g")  # DuckDB는 상한의 80%를 스스로 잡는다
    record = job("record_build", "python -m warehouse.record_build", mem="2g")
    d >> create >> load >> acct >> repo >> ev >> gold >> record
