"""표본으로 events -> features -> 근거까지 같은 SQL을 돌리고, 순수 Python으로 센 기대값과 맞춘다 (8절 L1-4).

인터넷과 클라우드 없이 돈다: session.py의 local 모드, 임시 폴더의 Hadoop 카탈로그.
실행: docker compose run --rm bot-job pytest
"""
import bisect
import datetime as dt
import gzip
import json
import os
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
DAY = "2024-07-15"
P = {"window_start": DAY, "window_end": DAY, "run_id": "test"}


def evidence_sqls():
    reg = yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    return [ROOT / "sql" / "evidence" / k / f"{v['current']}.sql" for k, v in reg.items()]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.update(BOT_CATALOG="local", BOT_WAREHOUSE=str(tmp_path_factory.mktemp("wh")), SPARK_DRIVER_MEMORY="2g")
    from spark_jobs import run_sql
    from spark_jobs.load_events import load
    from spark_jobs.session import spark_session

    s = spark_session("test_sql", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    load(s, str(FIX), P)
    run_sql.run(s, ROOT / "sql" / "silver" / "features_account.sql", P)
    run_sql.run(s, ROOT / "sql" / "silver" / "features_repo.sql", P)
    for f in evidence_sqls():
        run_sql.run(s, f, P)
    yield s
    s.stop()


@pytest.fixture(scope="module")
def want():
    """규칙을 Spark와 따로, 순수 Python으로 센다 (4.2절 규칙 표, 사전 실험 02_detect.py)."""
    ev = [json.loads(line) for line in gzip.open(FIX, "rt", encoding="utf-8")]
    name, n, hours, types = {}, defaultdict(int), defaultdict(set), defaultdict(set)
    for e in ev:
        a, ts = str(e["actor"]["id"]), dt.datetime.fromisoformat(e["created_at"])
        name[a] = max(name.get(a, ""), e["actor"]["login"])
        n[a] += 1
        hours[a].add(ts.hour)
        types[a].add(e["type"])
    declared = {a for a in name if name[a].endswith("[bot]")}
    low = {a for a in name if types[a] == {"WatchEvent"}}
    stars = [(str(e["actor"]["id"]), str(e["repo"]["id"]), dt.datetime.fromisoformat(e["created_at"]).timestamp())
             for e in ev if e["type"] == "WatchEvent" and str(e["actor"]["id"]) not in declared]
    by_repo, by_actor = defaultdict(list), defaultdict(list)
    for a, r, t in stars:
        by_repo[r].append(t)
        if a in low:
            by_actor[a].append((r, t))
    repo = {}
    for r, ts in by_repo.items():
        ts.sort()
        burst = max(bisect.bisect_right(ts, t + 300) - bisect.bisect_left(ts, t) for t in ts)
        repo[r] = {"stars": len(ts), "low_act_stars": sum(1 for a, rr, _ in stars if rr == r and a in low),
                   "max_5m_stars": burst, "max_co_star": 0}
    pairs = defaultdict(set)
    for a, st in by_actor.items():
        if 2 <= len(st) <= 50:
            for r1, t1 in st:
                for r2, t2 in st:
                    if r1 < r2 and abs(t1 - t2) <= 3600:
                        pairs[r1, r2].add(a)
    for (r1, r2), common in pairs.items():
        if len(common) >= 10:
            for r in (r1, r2):
                repo[r]["max_co_star"] = max(repo[r]["max_co_star"], len(common))
    hits = {
        ("declared_app", "gh_account"): declared,
        ("always_on", "gh_account"): {a for a in name if a not in declared and len(hours[a]) >= 20 and n[a] >= 100},
        ("low_activity", "gh_account"): low,
        ("low_activity", "gh_repo"): {r for r, v in repo.items() if v["stars"] >= 20 and v["low_act_stars"] / v["stars"] >= 0.5},
        ("lockstep_ratio", "gh_repo"): {r for r, v in repo.items() if v["max_co_star"] / v["stars"] >= 0.5},
        ("burst_5m", "gh_repo"): {r for r, v in repo.items() if v["max_5m_stars"] / v["stars"] >= 0.5},
    }
    return {"events": len(ev), "accounts": len(name), "repo": repo, "hits": hits}


def test_events(spark, want):
    assert spark.table("bot.silver.events").count() == want["events"]


def test_repo_features(spark, want):
    got = {r.entity_key: r.asDict() for r in spark.sql(
        "SELECT entity_key, stars, low_act_stars, max_5m_stars, max_co_star FROM bot.silver.entity_features "
        "WHERE entity_type = 'gh_repo'").collect()}
    for v in got.values():
        v.pop("entity_key")
    assert got == want["repo"]


def test_evidence_hits(spark, want):
    got = defaultdict(set)
    for r in spark.sql("SELECT evidence_id, entity_type, entity_key FROM bot.silver.evidence WHERE hit").collect():
        got[r.evidence_id, r.entity_type].add(r.entity_key)
    for key, keys in want["hits"].items():
        assert got[key] == keys, key
    # 근거는 대상 전부에 행을 낸다 (hit가 아니어도). 0행이면 파티션 덮어쓰기가 옛 행을 못 지운다 (run_sql.py)
    rows = dict(spark.sql("SELECT evidence_id, count(*) FROM bot.silver.evidence WHERE entity_type = 'gh_account' "
                          "GROUP BY 1").collect())
    assert rows == {"declared_app": want["accounts"], "always_on": want["accounts"], "low_activity": want["accounts"]}


def test_fake_star_repo(spark, want):
    """Gold fake_star_repo 식: low_activity(저장소) AND (lockstep_ratio OR burst_5m). 표본의 집중 저장소 3개가 걸려야 한다."""
    h = want["hits"]
    expect = h["low_activity", "gh_repo"] & (h["lockstep_ratio", "gh_repo"] | h["burst_5m", "gh_repo"])
    got = {r[0] for r in spark.sql("""
        SELECT entity_key FROM bot.silver.evidence WHERE entity_type = 'gh_repo'
        GROUP BY entity_key
        HAVING bool_or(evidence_id = 'low_activity' AND hit)
           AND bool_or(evidence_id IN ('lockstep_ratio', 'burst_5m') AND hit)""").collect()}
    assert got == expect
    assert {"828935508", "828931890", "828923123"} <= got  # make_fixture.py가 고른 집중 저장소


def test_rerun_same_partition(spark, want):
    """같은 날을 다시 돌려도 행 수가 같다 (파티션 덮어쓰기). 추가로 쌓이면 실패한다."""
    from spark_jobs import run_sql
    from spark_jobs.load_events import load

    load(spark, str(FIX), P)
    run_sql.run(spark, ROOT / "sql" / "evidence" / "burst_5m" / "v1.sql", P)
    assert spark.table("bot.silver.events").count() == want["events"]
    assert spark.sql("SELECT count(*) FROM bot.silver.evidence WHERE evidence_id = 'burst_5m'").first()[0] == len(want["repo"])
