"""로컬 기준값 뽑기 (지시서 K3). 이식 뒤 같은 스크립트를 클라우드 쪽에 돌려 docs/local_baseline.json과 비교한다.

두 부분:
  silver : Spark로. Bronze manifest 줄 수, Silver 날짜별 행 수와 내용 지문(measure.table_digest와 같은 식), 근거 hit 수.
           카탈로그 모드(BOT_CATALOG)만 바꾸면 클러스터에서도 같은 값이 나와야 한다 (지문 함수가 같은 Spark라서).
  gold   : Gold 표에서 정책별 실유저 수, fake_star_repo 수와 07-15 목록 지문, 구간별 채점.
           SQL은 DuckDB와 BigQuery에 같이 도는 꼴로만 쓴다 (count_if 대신 sum(case ...)). 목록 지문은 Python에서
           정렬한 키의 sha256이라 엔진과 상관없다. ponytail: 연결은 지금 DuckDB 하나다. C3에서 BigQuery 연결과 --schema만 더한다

같아야 함: 행 수, hit 수, 지문, 정책별 수, 채점 숫자. 달라도 됨: 시간, 메모리 (docs/local_baseline.md에 따로).

사용 (bot-job 컨테이너 안):
  python -m scripts.baseline all > /tmp/b.json                            # silver와 gold 둘 다 (docs/local_baseline.json 꼴)
  python -m scripts.baseline silver > /tmp/s.json                         # 한쪽만
  python -m scripts.baseline compare docs/local_baseline.json /tmp/b.json  # 다른 값만 찍는다
"""
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAYS = [f"2024-07-{d}" for d in range(15, 22)]


def silver():
    from measure.table_digest import one
    from spark_jobs.session import spark_session

    spark = spark_session("baseline", {"spark.ui.enabled": "false"})
    # manifest는 Spark가 읽지 않는다: 이름이 _로 시작해 숨은 파일로 건너뛴다. 작은 파일이라 Python으로.
    # ponytail: 로컬 경로만. C2에서 Bronze가 GCS면 이 부분만 gcsfs나 gcloud storage로 바꾼다
    man = {}
    for f in Path(os.environ.get("BOT_BRONZE", ROOT / "raw")).glob("dt=*/hour=*/_manifest.json"):
        m = json.loads(f.read_text(encoding="utf-8"))
        if m["status"] == "ok":
            man[m["hour"][:10]] = man.get(m["hour"][:10], 0) + m["lines"]
    out = {"manifest_lines": dict(sorted(man.items())), "digest": {}, "evidence_hits": {}}
    for d in DAYS:
        for t in ("bot.silver.events", "bot.silver.entity_features", "bot.silver.evidence"):
            n, h = one(spark, t, d)
            out["digest"][f"{t}|{d}"] = {"rows": n, "hash": str(h)}
    for r in spark.sql("""SELECT evidence_id, rule_version, entity_type, CAST(window_end AS STRING) d, count_if(hit) n
                          FROM bot.silver.evidence GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4""").collect():
        out["evidence_hits"][f"{r.evidence_id}|{r.rule_version}|{r.entity_type}|{r.d}"] = r.n
    spark.stop()
    return out


def gold():
    import duckdb

    con = duckdb.connect(str(ROOT / "warehouse_local" / "gold.duckdb"), read_only=True)
    q = lambda sql: con.sql(sql.replace("{s}", "main")).fetchall()  # noqa: E731  {s} = 스키마(BigQuery에서는 데이터셋)
    out = {"real_user": {}, "fake_star_repo": {}, "evaluation": {}}
    for d, *v in q("""select cast(event_date as varchar),
                             count(distinct case when is_real_lenient then actor_key end), sum(case when is_real_lenient then 1 else 0 end),
                             count(distinct case when is_real_balanced then actor_key end), sum(case when is_real_balanced then 1 else 0 end),
                             count(distinct case when is_real_strict then actor_key end), sum(case when is_real_strict then 1 else 0 end),
                             count(*)
                      from {s}.real_user_events group by 1 order by 1"""):
        out["real_user"][d] = dict(zip(["lenient_accounts", "lenient_events", "balanced_accounts", "balanced_events",
                                        "strict_accounts", "strict_events", "all_events"], map(int, v)))
    for d, n in q("select cast(event_date as varchar), count(*) from {s}.fake_star_repo group by 1 order by 1"):
        out["fake_star_repo"][d] = n
    keys = [k for (k,) in q("select repo_key from {s}.fake_star_repo where event_date = date '2024-07-15' order by repo_key")]
    out["fake_star_repo_0715_sha256"] = hashlib.sha256("\n".join(keys).encode()).hexdigest()
    for ev, ver, w, *v in q("""select evidence_id, rule_version, coalesce(cast(window_end as varchar), 'all'),
                                      n_hit, n_hit_with_truth, n_hit_positive, n_control_with_truth, n_control_positive
                               from {s}.evaluation order by 1, 2, 3"""):
        out["evaluation"][f"{ev}|{ver}|{w}"] = dict(zip(["n_hit", "hit_truth", "hit_pos", "control_truth", "control_pos"], v))
    return out


def flat(d, pre=""):
    for k, v in d.items():
        yield from flat(v, f"{pre}{k}.") if isinstance(v, dict) else [(f"{pre}{k}", v)]


def compare(a, b):
    fa, fb = dict(flat(json.loads(Path(a).read_text(encoding="utf-8")))), dict(flat(json.loads(Path(b).read_text(encoding="utf-8"))))
    diff = [(k, fa.get(k), fb.get(k)) for k in sorted(fa.keys() | fb.keys()) if fa.get(k) != fb.get(k)]
    for k, x, y in diff:
        print(f"{k}: {x} -> {y}")
    print(f"값 {len(fa | fb)}개 중 다름 {len(diff)}개")
    return 1 if diff else 0


if __name__ == "__main__":
    part = sys.argv[1]
    if part == "compare":
        sys.exit(compare(*sys.argv[2:4]))
    parts = {"silver": silver, "gold": gold}
    out = {k: f() for k, f in parts.items()} if part == "all" else parts[part]()
    print(json.dumps(out, ensure_ascii=False, indent=1, sort_keys=True))
