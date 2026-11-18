"""웹 봇 탐지 (AUTH 도서관 검색엔진 로그).
세션 = 같은 IP + 같은 UA, 30분 무활동이면 끊음 (원 논문과 같은 정의).
L1 선언 봇   : UA가 크롤러 목록(monperrus/crawler-user-agents)에 걸리거나 robots.txt를 요청.
               원 논문의 '정답 라벨'도 사실상 이것(UA 목록 + robots.txt + UA 수작업 검토)이다.
L2 비선언 봇 : 브라우저 UA를 달았는데 브라우저처럼 행동하지 않음.
               - 규칙: 요청 20개 이상인데 CSS/JS(/themes)·AJAX·표지(/Cover) 요청이 하나도 없음 (JS 실행·리소스 로딩 없음)
               - 모델: L1 라벨로 '행동 피처만' 학습한 분류기를 비선언 세션에 적용
usage: python 02_detect.py <web_requests.parquet> <crawler-user-agents.json> <out_dir>
"""
import sys, os, json, duckdb
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score

req, ua_list, out = sys.argv[1:4]
os.makedirs(out, exist_ok=True)
con = duckdb.connect()
def show(t, q):
    print(f"\n== {t}"); r = con.sql(q); print(r); return r

pattern = "|".join(p["pattern"] for p in json.load(open(ua_list, encoding="utf-8")))
con.execute("SET VARIABLE crawler_re = ?", [pattern])

con.sql(f"""
CREATE TABLE sess AS
WITH g AS (
  SELECT *, coalesce(epoch(ts) - epoch(lag(ts) OVER w) > 1800, true)::INT newsess
  FROM '{req}' WHERE ua IS NOT NULL AND ua NOT IN ('', '-')
  WINDOW w AS (PARTITION BY ip, ua ORDER BY ts)),
h AS (SELECT *, sum(newsess) OVER (PARTITION BY ip, ua ORDER BY ts ROWS UNBOUNDED PRECEDING) sn FROM g)
SELECT ip, ua, sn, min(ts) start_ts, count(*) n,
  epoch(max(ts)) - epoch(min(ts)) dur,
  count(*) / greatest((epoch(max(ts)) - epoch(min(ts))) / 60, 1) req_per_min,
  avg((resource LIKE '/themes%')::INT)  static_sh,
  avg((resource LIKE '/AJAX%')::INT)    ajax_sh,
  avg((resource LIKE '/Cover%')::INT)   cover_sh,
  avg((resource LIKE '/Record%')::INT)  rec_sh,
  avg((resource LIKE '/Search%')::INT)  search_sh,
  avg((referrer = '-')::INT)            noref_sh,
  avg((method = 'HEAD')::INT)           head_sh,
  avg((status BETWEEN 400 AND 499)::INT) err4_sh,
  avg((hour(ts + INTERVAL 2 HOUR) < 6)::INT) night_sh,          -- 그리스 현지 새벽
  count(DISTINCT resource) / count(*)   uniq_sh,
  coalesce(max((resource = '/robots.txt')::INT), 0) robots,
  -- 퍼널: 검색 후 상세(Record)로 갔나
  max((resource LIKE '/Search%')::INT) did_search,
  max((resource LIKE '/Record%')::INT) did_record
FROM h GROUP BY ip, ua, sn
""")
con.sql("""ALTER TABLE sess ADD COLUMN l1 BOOLEAN;
UPDATE sess SET l1 = regexp_matches(ua, getvariable('crawler_re')) OR robots = 1""")

show("세션 수 / L1 선언 봇 비율 (원 논문: 67,352세션 중 약 20%)",
     "SELECT count(*) sessions, round(100*avg(l1::INT),1) pct_l1, sum(n) FILTER (WHERE l1)*100.0/sum(n) pct_requests_l1 FROM sess")

# ---------- 규칙 기반 L2 ----------
con.sql("""ALTER TABLE sess ADD COLUMN l2_rule BOOLEAN;
UPDATE sess SET l2_rule = NOT l1 AND n >= 20 AND static_sh = 0 AND ajax_sh = 0 AND cover_sh = 0""")

show("L1 vs 나머지 행동 차이", """
SELECT CASE WHEN l1 THEN 'L1' WHEN l2_rule THEN 'L2 rule' ELSE 'human?' END grp, count(*) sessions,
  round(median(n),0) med_n, round(avg(static_sh),3) static, round(avg(ajax_sh),3) ajax, round(avg(cover_sh),3) cover,
  round(avg(noref_sh),2) noref, round(avg(night_sh),2) night, round(median(req_per_min),1) med_rpm
FROM sess GROUP BY 1 ORDER BY 1""")

# ---------- 모델: L1으로 학습, UA는 피처에서 제외 ----------
F = ["n", "dur", "req_per_min", "static_sh", "ajax_sh", "cover_sh", "rec_sh", "search_sh",
     "noref_sh", "head_sh", "err4_sh", "night_sh", "uniq_sh"]   # robots는 라벨 정의에 들어가서 제외
df = con.sql(f"SELECT {', '.join(F)}, l1::INT y, start_ts FROM sess ORDER BY start_ts").df()
cut = df.start_ts.quantile(0.75)                       # 시간 순 분할: 앞 3주 학습, 마지막 1주 평가
tr, te = df[df.start_ts < cut], df[df.start_ts >= cut]
clf = RandomForestClassifier(n_estimators=200, min_samples_leaf=5, n_jobs=-1, random_state=0).fit(tr[F], tr.y)
p = clf.predict_proba(te[F])[:, 1]
print(f"\n== 모델(행동만으로 '선언 봇' 맞히기) 시간분할 평가: AUC={roc_auc_score(te.y, p):.3f} "
      f"precision={precision_score(te.y, p>=0.5):.3f} recall={recall_score(te.y, p>=0.5):.3f}")
print("   중요 피처:", sorted(zip(clf.feature_importances_.round(3), F), reverse=True)[:6])

allp = clf.predict_proba(con.sql(f"SELECT {', '.join(F)} FROM sess").df())[:, 1]
con.sql("ALTER TABLE sess ADD COLUMN rowid_ BIGINT; UPDATE sess SET rowid_ = rowid")
import pandas as pd
con.register("score_df", pd.DataFrame({"rowid_": con.sql("SELECT rowid_ FROM sess").df().rowid_, "bot_score": allp}))
con.sql("CREATE TABLE sess2 AS SELECT s.*, d.bot_score, (NOT s.l1 AND d.bot_score >= 0.8) l2_model FROM sess s JOIN score_df d USING(rowid_)")

show("L2 후보 규모 (L1 아닌 세션 중)", """
SELECT count(*) FILTER (WHERE NOT l1) non_l1, count(*) FILTER (WHERE l2_rule) l2_rule, count(*) FILTER (WHERE l2_model) l2_model,
       count(*) FILTER (WHERE l2_rule AND l2_model) both_, sum(n) FILTER (WHERE l2_rule OR l2_model) l2_requests
FROM sess2""")

# ---------- 약한 검증: 라벨이 없으니 '봇일 법한 다른 증거'를 본다 ----------
# (a) 2018년 3월에 Firefox<=45, Chrome<=49 같은 낡은 UA = 스크립트에 박아 둔 UA일 가능성
# (b) 같은 IP에서 선언 봇 세션도 나왔나
show("약한 검증 (a) 낡은 브라우저 UA 비율, (b) 선언 봇과 IP 공유 비율", """
WITH l1ip AS (SELECT DISTINCT ip FROM sess2 WHERE l1),
x AS (SELECT *, coalesce((TRY_CAST(regexp_extract(ua, 'Firefox/(\\d+)', 1) AS INT) <= 45)
             OR (TRY_CAST(regexp_extract(ua, 'Chrome/(\\d+)', 1) AS INT) <= 49), false) old_ua,
             ip IN (SELECT ip FROM l1ip) shares_ip FROM sess2)
SELECT CASE WHEN l1 THEN 'L1' WHEN l2_rule OR l2_model THEN 'L2 후보' ELSE 'human?' END grp, count(*) sessions,
       round(100*avg(old_ua::INT),1) pct_old_ua, round(100*avg(shares_ip::INT),1) pct_shares_ip_with_l1
FROM x GROUP BY 1 ORDER BY 1""")

show("L2 후보 UA 상위 15", """
SELECT ua, count(*) sessions, sum(n) requests, round(avg(bot_score),2) score FROM sess2
WHERE l2_rule OR l2_model GROUP BY ua ORDER BY requests DESC LIMIT 15""")

# ---------- 지표 왜곡: 검색 -> 상세 전환율 ----------
show("지표 왜곡: 검색 세션 중 상세(Record)까지 간 비율 / 전체 상세 조회수", """
SELECT v, count(*) FILTER (WHERE did_search=1) search_sessions,
       round(100*count(*) FILTER (WHERE did_search=1 AND did_record=1) / count(*) FILTER (WHERE did_search=1), 1) search_to_record_pct,
       sum(rec_sh*n)::BIGINT record_views
FROM (SELECT 'A 전체' v, * FROM sess2
      UNION ALL SELECT 'B L1 제외', * FROM sess2 WHERE NOT l1
      UNION ALL SELECT 'C L1+L2 제외', * FROM sess2 WHERE NOT l1 AND NOT l2_rule AND NOT l2_model)
GROUP BY v ORDER BY v""")

con.sql(f"COPY (SELECT * EXCLUDE (rowid_) FROM sess2) TO '{out}/web_session_flags.parquet' (FORMAT parquet)")
print("\nsaved ->", out)
