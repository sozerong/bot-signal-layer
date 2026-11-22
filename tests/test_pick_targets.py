"""생존 확인 대상 고르기가 피처 버전을 registry에서 받고, 대상이 0개로 조용히 끝나지 않는지 (docs/review_logic.md R-16).

표본 하루(07-15)로 피처(계정, 저장소 f1)와 저장소 근거 3개를 만든 뒤:
  registry 그대로(f1)         -> 대상이 있다 (의심 + 대조군)
  피처 버전만 f2로 어긋나게 줌 -> 근거 의심은 있는데 피처가 0행이라 대상 0개 -> SystemExit (전에는 'f1' 고정이라 f2로 바꾼 날 초록 종료)
  의심이 없는 날              -> 대상 0개여도 실패하지 않는다
네트워크는 쓰지 않는다 (pick만 부른다).
실행: docker compose run --rm bot-job python -m pytest tests/test_pick_targets.py -q   (Spark, 약 1분)
"""
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "gh_sample.json.gz"
DAY = "2024-07-15"
P = {"window_start": DAY, "window_end": DAY, "run_id": "test"}


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.update(BOT_CATALOG="local", BOT_WAREHOUSE=str(tmp_path_factory.mktemp("pick") / "wh"), SPARK_DRIVER_MEMORY="2g")
    from spark_jobs import create_tables, run_sql
    from spark_jobs.load_events import load
    from spark_jobs.session import spark_session
    from truth.repo_alive import versions

    s = spark_session("test_pick_targets", {"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4"})
    create_tables.create(s)
    load(s, str(FIX), P)
    assert versions()["feature_version"] == "f1", "이 시험은 registry features_repo가 f1일 때를 전제로 표본 피처를 만든다"
    for f in ("features_account.sql", "features_repo.sql"):  # 저장소 피처가 계정 피처를 읽는다
        run_sql.run(s, ROOT / "sql" / "silver" / f, P)
    v = versions()
    for k in ("low_activity", "lockstep_ratio", "burst_5m"):
        run_sql.run(s, ROOT / "sql" / "evidence" / k / f"{v[f'v_{k}']}.sql", P)
    yield s
    s.stop()


def test_versions_reads_feature_version_from_registry():
    import yaml

    from truth.repo_alive import versions

    reg = yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    assert versions()["feature_version"] == reg["features_repo"]["current"]


def test_registry_version_picks_targets(spark):
    from truth.repo_alive import pick

    rows = pick(spark, DAY)
    groups = {g: sum(r["grp"] == g for r in rows) for g in ("suspect", "control")}
    assert groups["suspect"] >= 3, groups  # make_fixture.py가 고른 집중 저장소 3개
    assert groups["control"] <= groups["suspect"]


def test_mismatched_feature_version_fails(spark):
    from truth.repo_alive import pick, versions

    with pytest.raises(SystemExit, match="확인 대상이 0개다"):
        pick(spark, DAY, {**versions(), "feature_version": "f2"})


def test_no_suspect_day_is_not_failure(spark):
    from truth.repo_alive import pick

    assert pick(spark, "2024-07-01") == []
