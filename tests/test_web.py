"""웹 원천 AUTH: 적재(load_web.py)부터 근거 셋까지 작은 파일로 (결정 26, handoff_v5_wp1.md 1-3).

만든 요청 (2018-03-01 UTC):
  A 브라우저 UA, 25개 연속 /Record, 자산 0         -> 세션 1, assetless_browser hit
  B 브라우저 UA, 3개, 31분 띄고 2개, /themes 받음    -> 세션 2 (30분 넘게 끊김), 아무 근거도 안 걸림
  G Googlebot UA                                    -> declared_ua hit (크롤러 목록)
  R 브라우저 UA, /robots.txt 요청                    -> declared_ua hit (robots.txt)
  C ChatGPT-User UA                                 -> declared_ua hit (AI 목록), ai_agent_ua hit
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAY = "2018-03-01"
P = {"window_start": DAY, "window_end": DAY, "run_id": "test"}
FIREFOX = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0"


def req(ip, ua, t, resource):
    return {"ip": ip, "useragent": ua, "timestamp": t.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "method": "GET",
            "resource": resource, "bytes": "1", "response": "200", "referrer": "-"}


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("web")
    os.environ.update(BOT_CATALOG="local", BOT_WAREHOUSE=str(tmp / "wh"), SPARK_DRIVER_MEMORY="2g")
    from spark_jobs import create_tables, load_web, run_sql
    from spark_jobs.session import spark_session

    t0 = datetime(2018, 3, 1, 8, 0, tzinfo=timezone.utc)
    rows = [req("1.1.1", FIREFOX, t0 + timedelta(seconds=10 * i), f"/Record/{i}") for i in range(25)]
    rows += [req("2.2.2", FIREFOX, t0 + timedelta(minutes=m), r) for m, r in
             [(0, "/Search"), (1, "/themes/a.css"), (2, "/Record/1"), (33, "/Search"), (34, "/Record/2")]]
    rows += [req("3.3.3", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", t0, "/Record/9")]
    rows += [req("4.4.4", FIREFOX, t0, "/robots.txt")]
    rows += [req("5.5.5", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0", t0, "/Record/7")]
    lines = ["{"] + [f'"id{i}":{json.dumps(r)},' for i, r in enumerate(rows)] + ["}"]
    f = tmp / "public_v2.json"
    f.write_text("\n".join(lines), encoding="utf-8")
    (tmp / "_manifest.json").write_text(json.dumps({"lines": len(rows)}), encoding="utf-8")

    s = spark_session("test_web", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    create_tables.create(s)
    s.sql("""INSERT INTO bot.silver.ref_ua_patterns VALUES
             ('Googlebot', 'crawler_ua', 'Googlebot', NULL, NULL, DATE'2026-11-22'),
             ('(?i)(?<![A-Za-z0-9])ChatGPT\\-User(?![A-Za-z0-9])', 'ai_robots', 'ChatGPT-User', 'OpenAI', 'AI Assistants', DATE'2026-11-22')""")
    a = load_web.load(s, P, path=f)
    for e in ("declared_ua", "assetless_browser", "ai_agent_ua"):
        run_sql.run(s, ROOT / "sql" / "evidence" / e / "v1.sql", P)
    yield s, a
    s.stop()


def hits(s, evidence):
    return {r.client_ip for r in s.sql(f"""
        SELECT DISTINCT e.client_ip FROM bot.silver.evidence v JOIN bot.silver.events e ON e.actor_key = v.entity_key
        WHERE v.evidence_id = '{evidence}' AND v.hit""").collect()}


def test_load_and_sessions(spark):
    s, a = spark
    assert (a["rows_read"], a["rows_dropped"]) == (33, 0)
    sess = {r.client_ip: r.n for r in s.sql(
        "SELECT client_ip, count(DISTINCT session_key) n FROM bot.silver.events WHERE source = 'auth_lib' GROUP BY 1").collect()}
    assert sess == {"1.1.1": 1, "2.2.2": 2, "3.3.3": 1, "4.4.4": 1, "5.5.5": 1}
    assert s.sql("SELECT status FROM bot.silver.load_audit WHERE source = 'auth_lib'").first().status == "ok"


def test_evidence(spark):
    s, _ = spark
    assert hits(s, "declared_ua") == {"3.3.3", "4.4.4", "5.5.5"}
    assert hits(s, "assetless_browser") == {"1.1.1"}
    assert hits(s, "ai_agent_ua") == {"5.5.5"}
    # 근거마다 그날 대상 전부에 행이 있다 (every_evidence_ran과 같은 성질)
    n = s.sql("SELECT evidence_id, count(*) n FROM bot.silver.evidence GROUP BY 1").collect()
    assert {r.evidence_id: r.n for r in n} == {"declared_ua": 5, "assetless_browser": 5, "ai_agent_ua": 5}
