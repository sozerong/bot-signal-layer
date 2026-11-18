"""Zenodo 3477932 (AUTH 도서관 검색엔진 로그, 2018.03, 409만 요청) -> parquet.
원본은 3.2GB짜리 JSON 객체 하나지만 한 줄에 요청 하나라서, 줄 단위로 NDJSON으로 바꾼 뒤 DuckDB로 읽는다.
usage: python 01_ingest.py public_v2.json web_requests.parquet
"""
import sys, os, duckdb

src, out = sys.argv[1], sys.argv[2]
nd = out + ".ndjson"
with open(src, encoding="utf-8") as f, open(nd, "w", encoding="utf-8") as g:
    for line in f:
        line = line.strip().rstrip(",")
        if line.startswith('"'):                 # "key":{...}  ->  {"id":"key",...}
            k, v = line.split(":", 1)
            g.write('{"id":' + k + "," + v[1:] + "\n")

duckdb.sql(f"""COPY (
  SELECT id, ip, useragent ua, timestamp::TIMESTAMP ts, method, resource,
         TRY_CAST(response AS INT) status, TRY_CAST(bytes AS BIGINT) bytes, referrer
  FROM read_json('{nd}', format='newline_delimited',
       columns={{id:'VARCHAR', ip:'VARCHAR', useragent:'VARCHAR', timestamp:'VARCHAR', method:'VARCHAR',
                resource:'VARCHAR', response:'VARCHAR', bytes:'VARCHAR', referrer:'VARCHAR'}})
) TO '{out}' (FORMAT parquet)""")
os.remove(nd)
print(duckdb.sql(f"SELECT count(*) n, count(DISTINCT ip) ips, count(DISTINCT ua) uas, min(ts), max(ts) FROM '{out}'"))
