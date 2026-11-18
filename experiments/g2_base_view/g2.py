"""G2 실험 키트: base를 view로 둘 때와 table로 물리화할 때, 하류 쿼리가 내는 비용을 잰다.

A = view: base_view는 적재 구역(landing)의 JSON 원문 위에 얹힌 view다. 저장은 0이다.
비교 대상 = table: 같은 SELECT를 일 파티션 + repo_id 클러스터링 표로 물리화한 것이다(D7).
답은 여기 없다. 같은 하류 쿼리 3개를 두 base에 던지고 계기를 보는 것까지만 한다.

명령 (프로젝트 루트에서, 순서대로)
  prep     raw에서 쓰는 이벤트만 골라 적재 파일을 만든다 (로컬, 무료)
  load     적재 파일을 BigQuery landing 표에 올린다 (load job은 무료)
  build    base_view(view)와 base_table(CTAS)을 만든다. CTAS는 landing을 한 번 스캔한다
  measure  하류 쿼리 × {view, table}의 dry-run 스캔 바이트를 잰다 (무료)
           --run을 붙이면 실제로 돌려 청구 바이트, 슬롯 시간, 걸린 시간까지 잰다 (무료 한도에서 차감)

손잡이
  --files N        prep에서 raw 파일 몇 개를 쓸지 (데이터 크기)
  --run            measure에서 실제 실행
  --project, --dataset, --location
"""
import argparse
import collections
import gzip
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
WORK = HERE / "work"
LANDING_FILE = WORK / "landing.ndjson.gz"
PREP_INFO = WORK / "prep.json"
TYPES = ("WatchEvent", "ForkEvent", "CreateEvent", "ReleaseEvent")
GUARD = 5 * 10**9  # --run 한 번에 청구될 수 있는 최대 바이트. 넘으면 BigQuery가 실행을 거부한다

BASE_SELECT = """
SELECT
  dt,
  JSON_VALUE(raw, '$.id') AS event_id,
  JSON_VALUE(raw, '$.type') AS event_type,
  TIMESTAMP(JSON_VALUE(raw, '$.created_at')) AS created_at,
  SAFE_CAST(JSON_VALUE(raw, '$.actor.id') AS INT64) AS actor_id,
  JSON_VALUE(raw, '$.actor.login') AS actor_login,
  SAFE_CAST(JSON_VALUE(raw, '$.repo.id') AS INT64) AS repo_id,
  JSON_VALUE(raw, '$.repo.name') AS repo_name,
  JSON_VALUE(raw, '$.payload.action') AS payload_action
FROM `{ds}.landing`
"""

# 하류가 base를 읽는 대표적인 모양 세 가지
QUERIES = {
    "q1 저장소별 star 수 (fact 집계)":
        "SELECT repo_id, COUNT(*) AS n FROM `{src}` WHERE event_type = 'WatchEvent' "
        "GROUP BY repo_id ORDER BY n DESC LIMIT 20",
    "q2 star만 찍은 계정 수 (S-A 모양)":
        "SELECT COUNT(*) FROM (SELECT actor_id, COUNTIF(event_type = 'WatchEvent') AS stars, "
        "COUNTIF(event_type != 'WatchEvent') AS other FROM `{src}` GROUP BY actor_id) "
        "WHERE stars > 0 AND other = 0",
    "q3 하루 × 저장소 1개 (점검 창구 모양)":
        "SELECT COUNT(*) FROM `{src}` WHERE dt = '{dt}' AND repo_id = {repo_id}",
}


def prep(a):
    files = sorted(Path(a.root).glob("dt=*/hour=*/*.json.gz"))[: a.files]
    if not files:
        raise SystemExit("raw에 파일이 없다")
    WORK.mkdir(exist_ok=True)
    kept, total, stars = collections.Counter(), 0, collections.Counter()
    with gzip.open(LANDING_FILE, "wt", encoding="utf-8") as out:
        for f in files:
            dt_, hour = f.parent.parent.name[3:], int(f.parent.name[5:])
            with gzip.open(f, "rt", encoding="utf-8") as src:
                for line in src:
                    total += 1
                    # ponytail: 봉투의 type은 줄 앞쪽(id 다음)에 있다. 전체 json.loads 대신 앞 120자만 본다.
                    # 틀리면 base의 event_type 분포에서 드러난다
                    head = line[:120]
                    t = next((t for t in TYPES if f'"type":"{t}"' in head), None)
                    if t is None:
                        continue
                    kept[t] += 1
                    if f is files[0] and t == "WatchEvent":
                        stars[json.loads(line)["repo"]["id"]] += 1
                    out.write(json.dumps({"dt": dt_, "hour": hour, "source_file": f.name,
                                          "raw": line.rstrip("\n")}, ensure_ascii=False) + "\n")
    repo_id, n = stars.most_common(1)[0]
    info = {"files": [f.name for f in files], "lines_total": total, "lines_kept": sum(kept.values()),
            "kept_by_type": dict(kept), "q3_dt": files[0].parent.parent.name[3:], "q3_repo_id": repo_id,
            "q3_repo_stars": n, "landing_file_bytes": LANDING_FILE.stat().st_size}
    PREP_INFO.write_text(json.dumps(info, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(info, ensure_ascii=False, indent=1))


def client(a):
    from google.cloud import bigquery
    if not a.project:
        raise SystemExit("--project 또는 GOOGLE_CLOUD_PROJECT가 필요하다")
    return bigquery, bigquery.Client(project=a.project, location=a.location)


def load(a):
    bq, c = client(a)
    c.create_dataset(bq.Dataset(f"{a.project}.{a.dataset}"), exists_ok=True)
    cfg = bq.LoadJobConfig(
        schema=[bq.SchemaField("dt", "DATE"), bq.SchemaField("hour", "INT64"),
                bq.SchemaField("source_file", "STRING"), bq.SchemaField("raw", "STRING")],
        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bq.TimePartitioning(field="dt"),
        write_disposition="WRITE_TRUNCATE")
    t = time.perf_counter()
    with open(LANDING_FILE, "rb") as f:
        c.load_table_from_file(f, f"{a.dataset}.landing", job_config=cfg).result()
    tbl = c.get_table(f"{a.dataset}.landing")
    want = json.loads(PREP_INFO.read_text(encoding="utf-8"))["lines_kept"]
    print(f"landing: {tbl.num_rows}행, {tbl.num_bytes / 1e6:.0f}MB, 올리는 데 {time.perf_counter() - t:.0f}s")
    if tbl.num_rows != want:  # 적재 수 = 로컬에서 고른 수. 어긋나면 어디선가 버려졌다
        print(f"!! 로컬에서 고른 줄은 {want}개인데 landing은 {tbl.num_rows}행이다")


def build(a):
    _, c = client(a)
    sel = BASE_SELECT.format(ds=a.dataset)
    c.query(f"CREATE OR REPLACE VIEW `{a.dataset}.base_view` AS {sel}").result()
    j = c.query(f"CREATE OR REPLACE TABLE `{a.dataset}.base_table` "
                f"PARTITION BY dt CLUSTER BY repo_id AS {sel}")
    j.result()
    tbl = c.get_table(f"{a.dataset}.base_table")
    print(f"base_view: 저장 0")
    print(f"base_table: {tbl.num_rows}행, {tbl.num_bytes / 1e6:.0f}MB. "
          f"만드는 데 스캔 {j.total_bytes_processed / 1e6:.0f}MB, 슬롯 {j.slot_millis / 1000:.1f}s")


def measure(a):
    bq, c = client(a)
    info = json.loads(PREP_INFO.read_text(encoding="utf-8"))
    print("| 쿼리 | base | dry-run 스캔 | " + ("청구 | 슬롯 시간 | 걸린 시간 |" if a.run else ""))
    print("|---|---|---|" + ("---|---|---|" if a.run else ""))
    for name, q in QUERIES.items():
        for src in ("base_view", "base_table"):
            sql = q.format(src=f"{a.dataset}.{src}", dt=info["q3_dt"], repo_id=info["q3_repo_id"])
            dry = c.query(sql, job_config=bq.QueryJobConfig(dry_run=True, use_query_cache=False))
            row = f"| {name} | {src} | {dry.total_bytes_processed / 1e6:.1f}MB |"
            if a.run:
                j = c.query(sql, job_config=bq.QueryJobConfig(use_query_cache=False, maximum_bytes_billed=GUARD))
                j.result()
                row += (f" {j.total_bytes_billed / 1e6:.1f}MB | {j.slot_millis / 1000:.1f}s | "
                        f"{(j.ended - j.started).total_seconds():.1f}s |")
            print(row)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cmd", choices=["prep", "load", "build", "measure"])
    p.add_argument("--root", default="raw")
    p.add_argument("--files", type=int, default=1000)
    p.add_argument("--run", action="store_true")
    p.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    p.add_argument("--dataset", default="gh_g2")
    p.add_argument("--location", default="US")
    a = p.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    {"prep": prep, "load": load, "build": build, "measure": measure}[a.cmd](a)


if __name__ == "__main__":
    main()
