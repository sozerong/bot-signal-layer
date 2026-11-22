"""늦은 데이터 거부 가드 (지시서 handoff_v5_wp1.md 1-2, 결정 22, F-16).

표본 두 개: 전날(07-14)로 옮긴 표본을 먼저 적재하고, 07-15 표본에 07-14 이벤트 한 줄을 섞어 다시 적재한다.
  가드 켬 : 적재가 실패하고 07-14 파티션 지문이 그대로, 07-15는 비어 있다.
  가드 끔 : 07-14 파티션이 섞인 한 줄로 바뀐다 (덮어쓰기 손실 재현. 이것이 1이 아니면 F-16의 전제가 틀린 것)
실행: docker compose run --rm bot-job python -m pytest tests/test_late_guard.py -q
"""
import gzip
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
PREV, DAY = "2024-07-14", "2024-07-15"


def write(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
    return str(path)


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("late")
    os.environ.update(BOT_CATALOG="local", BOT_WAREHOUSE=str(tmp / "wh"), SPARK_DRIVER_MEMORY="2g")
    from spark_jobs import create_tables
    from spark_jobs.session import spark_session

    rows = [json.loads(line) for line in gzip.open(FIX, "rt", encoding="utf-8")]
    assert all(r["created_at"].startswith(DAY) for r in rows)
    prev = [{**r, "created_at": r["created_at"].replace(DAY, PREV)} for r in rows]
    late = {**rows[0], "id": "late-1", "created_at": f"{PREV}T23:59:59Z"}
    s = spark_session("test_late_guard", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    create_tables.create(s)
    yield s, write(tmp / "prev.json.gz", prev), write(tmp / "mixed.json.gz", rows + [late]), len(rows)
    s.stop()


def count(s, day):
    return s.sql(f"SELECT count(*) FROM bot.silver.events WHERE event_date = DATE'{day}'").first()[0]


def test_guard_rejects_then_unguarded_overwrites(env):
    from measure.table_digest import one
    from spark_jobs.load_events import load

    s, prev, mixed, n = env
    load(s, prev, {"window_end": PREV, "run_id": "prev"})
    before = one(s, "bot.silver.events", PREV)
    assert before[0] == n

    with pytest.raises(SystemExit, match="다른 event_date"):
        load(s, mixed, {"window_end": DAY, "run_id": "guarded"})
    assert one(s, "bot.silver.events", PREV) == before  # 전날 지문 그대로
    assert count(s, DAY) == 0  # 아무것도 쓰지 않았다

    load(s, mixed, {"window_end": DAY, "run_id": "unguarded"}, late_data=None)
    assert count(s, PREV) == 1  # 전날 3,407행이 섞인 한 줄로 바뀌었다 (F-16)
    assert count(s, DAY) == n
