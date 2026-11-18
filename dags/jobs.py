"""계산 태스크를 만드는 함수 하나: job(name, command) (implementation_design.md 6절).

모든 계산은 Airflow 컨테이너가 아니라 bot-job 컨테이너에서 돈다. 이식 때(C4) 이 함수만 클러스터 연산자로 바꾼다.
서울 동네 찾기 dags/jobs.py(결정 109 H4)에서 가져온 것:
- 바인드 마운트 경로는 호스트 기준(HOST_PROJECT_DIR). 컨테이너 안 경로를 넘기면 호스트에 없는 경로가 된다.
- mount_tmp_dir=False: 기본값은 Airflow 컨테이너의 임시 폴더를 호스트 경로처럼 붙이려다 실패한다.
- 스왑 금지(memswap_limit = mem_limit). provider 4.5.9에 인자가 없어 create_host_config를 감싼다.
- 1분마다 한 줄 찍기: socket-proxy가 유휴 연결을 10분에 끊는다 (서울 동네 찾기 F-29).
가져오지 않은 것: heavy 잠금(PostgreSQL advisory lock), 주인 없는 컨테이너 정리, 실패 시 컨테이너 남기기. 필요해지면 그때 옮긴다.
"""
import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST = os.environ["HOST_PROJECT_DIR"]
MEM = "24g"  # docker-compose.yml bot-job과 같은 값. 바꿀 때 둘을 함께
# Silver Iceberg 표에 쓰는 태스크는 이 풀(자리 1개)로 줄 세운다. Hadoop 카탈로그의 동시 커밋이 조용히 사라진다 (failure_analysis.md F-7).
# airflow-init이 만든다 (docker-compose.yml). ponytail: 표마다가 아니라 Silver 전체 한 줄. 클라우드 REST 카탈로그(C2)에서는 푼다
SILVER_WRITE = "silver_write"


class JobOperator(DockerOperator):
    def _run_image_with_mounts(self, target_mounts, add_tmp_variable):
        create_host_config = self.cli.create_host_config
        self.cli.create_host_config = lambda **kw: create_host_config(memswap_limit=kw.get("mem_limit"), **kw)
        try:
            return super()._run_image_with_mounts(target_mounts, add_tmp_variable)
        finally:
            self.cli.create_host_config = create_host_config


def job(name, command, mem=MEM, **kw):
    """command: bot-job 컨테이너 안에서 bash로 돌릴 한 줄 (Jinja 템플릿 가능). 작업 폴더는 /work(레포)."""
    return JobOperator(
        task_id=name,
        image="bot-job:4.1.2",
        command=["bash", "-c", "set -euo pipefail; (while sleep 60; do echo \"[alive] $(date +%T)\"; done) & HB=$!; "
                               "trap 'kill $HB' EXIT; " + command],
        docker_url="tcp://socket-proxy:2375",
        mounts=[Mount("/work", HOST, type="bind")],
        mount_tmp_dir=False,
        auto_remove="success",
        working_dir="/work",
        environment={"BOT_CATALOG": "local", "SPARK_DRIVER_MEMORY": "16g", "PYTHONDONTWRITEBYTECODE": "1"},
        mem_limit=mem,
        **kw,
    )
