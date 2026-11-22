"""적재 기록표와 스키마 변화 가드 (handoff_v6.md Q5, Q6, 결정 27, 28, failure_analysis.md F-17).

표본(07-15 3,407줄)을 날짜를 옮겨 여러 사본으로 만들어 각기 다른 날로 적재한다 (막힌 적재가 다른 날을 건드리지 않게):
  07-15  깨진 줄 1개   -> 기록만 (0.029% < 0.1%), events 3,407행
  07-14  깨진 줄 10개  -> 실패 (0.29% > 0.1%), events 0행, 기록표 status=rejected
  07-13  actor를 지움  -> 실패 (actor.login NULL 100%). 가드를 끄면 actor 없는 3,407행이 말없이 들어간다 (F-17 재현)
  07-12  가짜 type 1행 -> 경고와 기록만, events 3,407행
실행: docker compose run --rm bot-job python -m pytest tests/test_load_audit.py -q   (Spark, 약 1분)
"""
import gzip
import json
import os
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
DAY = "2024-07-15"


def rows_on(day):
    rows = [json.loads(line) for line in gzip.open(FIX, "rt", encoding="utf-8")]
    return [{**r, "created_at": r["created_at"].replace(DAY, day)} for r in rows]


def write(path, rows, broken=0):
    """broken: 끝에 붙일 깨진 줄(잘린 JSON) 수"""
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
        f.writelines(f'{{"id": "broken-{i}", "type": "PushEvent", "created_at": \n' for i in range(broken))
    return str(path)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("audit")
    os.environ.update(BOT_CATALOG="local", BOT_WAREHOUSE=str(tmp / "wh"), SPARK_DRIVER_MEMORY="2g")
    from spark_jobs import create_tables
    from spark_jobs.session import spark_session

    s = spark_session("test_load_audit", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    create_tables.create(s)
    yield s, tmp
    s.stop()


def events(s, day):
    return s.sql(f"SELECT count(*) FROM bot.silver.events WHERE event_date = DATE'{day}'").first()[0]


def last_audit(s, day):
    s.sql("REFRESH TABLE bot.silver.load_audit")
    return s.sql(f"SELECT * FROM bot.silver.load_audit WHERE event_date = DATE'{day}' ORDER BY recorded_at DESC").first().asDict()


def P(day, run="test"):
    return {"window_end": day, "run_id": run}


def test_one_broken_line_is_recorded_only(spark):
    from spark_jobs.load_events import load

    s, tmp = spark
    load(s, write(tmp / "d15.json.gz", rows_on(DAY), broken=1), P(DAY))
    a = last_audit(s, DAY)
    assert (a["status"], a["rows_read"], a["rows_dropped"], a["rows_written"]) == ("ok", 3408, 1, 3407)
    assert a["manifest_lines"] is None and a["n_files"] == 1  # 표본은 manifest가 없다 -> 분모는 읽은 줄
    assert events(s, DAY) == 3407


def test_ten_broken_lines_fail_without_writing(spark):
    from spark_jobs.load_events import load

    s, tmp = spark
    day = "2024-07-14"
    with pytest.raises(SystemExit, match="버린 줄 10 / 읽은 줄 3417"):
        load(s, write(tmp / "d14.json.gz", rows_on(day), broken=10), P(day))
    assert events(s, day) == 0
    a = last_audit(s, day)
    assert (a["status"], a["rows_dropped"], a["rows_written"]) == ("rejected", 10, 0)
    assert "버린 줄" in a["reason"]


def test_actor_removed_fails_and_unguarded_loads_silently(spark):
    """F-17 재현: 필수 필드가 원천에서 사라지면 옛 적재(DROPMALFORMED, 가드 없음)는 오류 없이 NULL 행을 쓴다"""
    from spark_jobs.load_events import load

    s, tmp = spark
    day = "2024-07-13"
    path = write(tmp / "d13.json.gz", [{k: v for k, v in r.items() if k != "actor"} for r in rows_on(day)])
    with pytest.raises(SystemExit, match="actor.login NULL 3407 / 3407행"):
        load(s, path, P(day))
    assert events(s, day) == 0
    assert last_audit(s, day)["null_actor_login"] == 3407

    load(s, path, P(day, "unguarded"), gate_on=False)
    got = s.sql(f"SELECT count(*), count(actor_name), count(actor_key) FROM bot.silver.events WHERE event_date = DATE'{day}'").first()
    assert tuple(got) == (3407, 0, 0)  # 행은 다 들어가고 계정은 하나도 없다


def test_unknown_type_warns_and_loads(spark, capsys):
    from spark_jobs.load_events import load

    s, tmp = spark
    day = "2024-07-12"
    rows = rows_on(day)
    rows[0] = {**rows[0], "type": "FakeEvent"}
    load(s, write(tmp / "d12.json.gz", rows), P(day))
    assert "처음 보는 이벤트 type FakeEvent (1행)" in capsys.readouterr().err
    a = last_audit(s, day)
    assert (a["status"], a["n_unknown_type"], a["unknown_types"]) == ("ok", 1, "FakeEvent")
    assert events(s, day) == 3407


def test_known_types_cover_fixture():
    """등록부의 알려진 type이 표본의 type을 모두 덮는다 (표본에서 경고가 나지 않게)"""
    known = set(yaml.safe_load((ROOT / "sources" / "registry.yml").read_text(encoding="utf-8"))["gh"]["audit"]["known_types"])
    seen = {json.loads(line)["type"] for line in gzip.open(FIX, "rt", encoding="utf-8")}
    assert seen <= known, seen - known


def test_manifest_lines_sum(tmp_path):
    """날짜 폴더를 주면 hour 폴더마다 _manifest.json의 lines를 더한다. 하나라도 없거나 ok가 아니면 None"""
    from spark_jobs.load_events import manifest

    for h, lines in (("00", 5), ("01", 7)):
        d = tmp_path / f"hour={h}"
        d.mkdir()
        (d / f"{h}.json.gz").write_bytes(b"")
        (d / "_manifest.json").write_text(json.dumps({"status": "ok", "lines": lines}), encoding="utf-8")
    assert manifest(f"{tmp_path}/hour=*/*.json.gz") == (2, 12)
    (tmp_path / "hour=01" / "_manifest.json").write_text(json.dumps({"status": "failed"}), encoding="utf-8")
    assert manifest(f"{tmp_path}/hour=*/*.json.gz") == (2, None)
