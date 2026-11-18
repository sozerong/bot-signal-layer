"""GH Archive 시간별 json.gz -> 필요한 컬럼만 parquet로.
usage: python 01_ingest.py <gharchive_dir> <out.parquet>
"""
import sys, duckdb

src, out = sys.argv[1], sys.argv[2]
duckdb.sql(f"""
COPY (
  SELECT
    type,
    actor.id::BIGINT     AS actor_id,
    actor.login          AS actor_login,
    repo.id::BIGINT      AS repo_id,
    repo.name            AS repo_name,
    created_at::TIMESTAMP AS ts
  FROM read_json('{src}/*.json.gz',
    columns={{type:'VARCHAR', actor:'STRUCT(id BIGINT, login VARCHAR)',
             repo:'STRUCT(id BIGINT, name VARCHAR)', created_at:'VARCHAR'}},
    format='newline_delimited', ignore_errors=true)
) TO '{out}' (FORMAT parquet)
""")
print(duckdb.sql(f"SELECT count(*) events, count(DISTINCT actor_id) actors, count(DISTINCT repo_id) repos FROM '{out}'"))
