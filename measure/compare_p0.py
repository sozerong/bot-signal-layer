"""L3-3: Silver 근거로 만든 fake_star_repo를 사전 실험(DuckDB) suspect_v3와 저장소 단위로 맞춘다 (implementation_design.md 8절).

fake_star_repo 식은 Gold(L5)와 같다: low_activity(저장소) AND (lockstep_ratio OR burst_5m).
다르면 양쪽 피처(stars, 저활동 star, 5분 최대, 공통 계정)를 나란히 찍는다.

사용 (bot-job 컨테이너 안):
  python -m measure.compare_p0 --day 2024-07-15
"""
import argparse

from spark_jobs.session import ROOT, spark_session

P0 = ROOT / "experiments" / "p0_bot_detection" / "results" / "gh_repo_flags.parquet"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    day = ap.parse_args().day
    s = spark_session("compare_p0", {"spark.ui.enabled": "false"})
    s.read.parquet(str(P0)).createOrReplaceTempView("p0")
    s.sql("""
        SELECT entity_key FROM bot.silver.evidence
        WHERE entity_type = 'gh_repo' AND window_end = DATE(:day)
          AND evidence_id IN ('low_activity', 'lockstep_ratio', 'burst_5m')
        GROUP BY entity_key
        HAVING bool_or(evidence_id = 'low_activity' AND hit)
           AND bool_or(evidence_id IN ('lockstep_ratio', 'burst_5m') AND hit)""", args={"day": day}
          ).createOrReplaceTempView("ours")
    r = s.sql("""
        SELECT (SELECT count(*) FROM ours) AS ours,
               (SELECT count(*) FROM p0 WHERE suspect_v3) AS p0,
               (SELECT count(*) FROM ours o JOIN p0 ON o.entity_key = CAST(p0.repo_id AS STRING) AND p0.suspect_v3) AS both""").first()
    print(f"fake_star_repo {r.ours}개, 사전 실험 suspect_v3 {r.p0}개, 둘 다 {r.both}개")
    diff = s.sql("""
        WITH d AS (
          SELECT entity_key AS k, 'ours_only' AS side FROM ours
          WHERE entity_key NOT IN (SELECT CAST(repo_id AS STRING) FROM p0 WHERE suspect_v3)
          UNION ALL
          SELECT CAST(repo_id AS STRING), 'p0_only' FROM p0
          WHERE suspect_v3 AND CAST(repo_id AS STRING) NOT IN (SELECT entity_key FROM ours))
        SELECT d.side, d.k, f.entity_name, f.stars, f.low_act_stars, f.max_5m_stars, f.max_co_star, p0.*
        FROM d
        LEFT JOIN bot.silver.entity_features f
          ON f.entity_type = 'gh_repo' AND f.entity_key = d.k AND f.event_date = DATE(:day) AND f.feature_version = 'f1'
        LEFT JOIN p0 ON CAST(p0.repo_id AS STRING) = d.k
        ORDER BY d.side, d.k""", args={"day": day})
    rows = diff.collect()
    print(f"차이 {len(rows)}개")
    for row in rows:
        print(row.asDict())


if __name__ == "__main__":
    main()
