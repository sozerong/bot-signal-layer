"""GitHub 봇 탐지 3단계 (하루치 GH Archive).
L1 선언 봇   : login이 [bot]으로 끝남 (GitHub App). 이게 유일한 '정답 라벨'.
L2 비선언 자동화: 사람 계정으로 돌아가는 스크립트. 하루 24시간 중 거의 쉬지 않고 많이 움직임.
L3 가짜 star  : 오늘 star만 누른 계정(저활동)이 같은 저장소 묶음에 함께 star (lockstep).
usage: python 02_detect.py <events.parquet> <out_dir>
"""
import sys, os, duckdb

ev, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
con = duckdb.connect()
con.sql(f"CREATE VIEW ev AS SELECT * FROM '{ev}'")

def show(title, q):
    print(f"\n== {title}"); r = con.sql(q); print(r); return r

# ---------- 액터별 행동 피처 ----------
con.sql("""
CREATE TABLE actor AS
WITH g AS (
  SELECT actor_id, actor_login, type, ts,
         epoch(ts) - epoch(lag(ts) OVER (PARTITION BY actor_id ORDER BY ts)) AS gap
  FROM ev)
SELECT actor_id, any_value(actor_login) login,
       count(*) n, count(DISTINCT hour(ts)) active_hours,
       count(DISTINCT type) n_types,
       avg(CASE WHEN type='WatchEvent' THEN 1 ELSE 0 END) watch_share,
       stddev_pop(gap)/nullif(avg(gap),0) gap_cv,
       any_value(actor_login) LIKE '%[bot]' declared_bot
FROM g GROUP BY actor_id
""")

# L2 규칙: 사람은 잔다. 20시간 이상 활동 + 하루 100건 이상
L2 = "NOT declared_bot AND active_hours >= 20 AND n >= 100"
con.sql(f"ALTER TABLE actor ADD COLUMN l2_auto BOOLEAN; UPDATE actor SET l2_auto = {L2}")

show("L1/L2 규모", """
SELECT CASE WHEN declared_bot THEN 'L1 선언 봇' WHEN l2_auto THEN 'L2 비선언 자동화' ELSE '나머지' END grp,
       count(*) actors, sum(n) events, round(100*sum(n)/(SELECT count(*) FROM ev),1) pct_events
FROM actor GROUP BY 1 ORDER BY events DESC""")

# 규칙 보정: 선언 봇(정답)에 같은 행동 규칙을 적용하면 얼마나 잡히나 = 재현율 근사
show("L2 규칙의 선언 봇 재현율 (n>=100인 선언 봇 기준)", """
SELECT count(*) bots_n100, sum((active_hours>=20)::INT) caught,
       round(100*avg((active_hours>=20)::INT),1) recall_pct
FROM actor WHERE declared_bot AND n>=100""")

show("L2 상위 20 (이름 확인용)", """
SELECT login, n, active_hours, n_types, round(gap_cv,2) gap_cv FROM actor WHERE l2_auto ORDER BY n DESC LIMIT 20""")

show("L2 중 이름에 bot 들어간 비율 (약한 라벨)", """
SELECT round(100*avg((lower(login) LIKE '%bot%')::INT),1) pct_named_bot,
       (SELECT round(100*avg((lower(login) LIKE '%bot%')::INT),2) FROM actor WHERE NOT declared_bot AND NOT l2_auto) pct_in_rest
FROM actor WHERE l2_auto""")

# ---------- L3 가짜 star ----------
con.sql("""
CREATE TABLE star AS
SELECT e.actor_id, e.repo_id, e.repo_name, e.ts,
       (a.n_types = 1) AS low_act          -- 오늘 WatchEvent만 함
FROM ev e JOIN actor a USING(actor_id)
WHERE e.type='WatchEvent' AND NOT a.declared_bot
""")
show("star 이벤트 중 저활동 계정 비율", "SELECT count(*) stars, round(100*avg(low_act::INT),1) pct_low_act FROM star")

# lockstep: 저활동 계정들이 함께 star한 저장소 쌍. 한 계정당 star 50개 이하만 (쌍 폭발 방지)
con.sql("""
CREATE TABLE pair AS
WITH la AS (
  SELECT * FROM star WHERE low_act
  AND actor_id IN (SELECT actor_id FROM star WHERE low_act GROUP BY 1 HAVING count(*) BETWEEN 2 AND 50))
SELECT a.repo_id r1, b.repo_id r2, count(DISTINCT a.actor_id) common
FROM la a JOIN la b ON a.actor_id=b.actor_id AND a.repo_id<b.repo_id
     AND abs(epoch(a.ts)-epoch(b.ts)) <= 3600        -- 1시간 안에 같이
GROUP BY 1,2 HAVING count(DISTINCT a.actor_id) >= 10
""")

# burst: 저장소가 받은 star 중 가장 몰린 5분 구간의 비중. 하루 100개 넘게 star하는 계정 무리가
#        수십 초 안에 몰아 찍는 캠페인은 위 lockstep(계정당 50개 상한)에서 빠지기 때문에 따로 잡는다
con.sql("""
CREATE TABLE burst AS
SELECT repo_id, max(c) max5m FROM (
  SELECT repo_id, count(*) OVER (PARTITION BY repo_id ORDER BY epoch(ts) RANGE BETWEEN CURRENT ROW AND 300 FOLLOWING) c
  FROM star) GROUP BY 1
""")

con.sql("""
CREATE TABLE repo AS
SELECT s.repo_id, any_value(s.repo_name) repo_name, count(*) stars,
       round(avg(low_act::INT),3) low_act_share,
       coalesce(max(p.common),0) max_lockstep,
       coalesce(max(p.common),0)/count(*) lock_ratio,  -- 내 stargazer 중 '다른 저장소와 같이 움직인' 비율
       any_value(b.max5m)/count(*) burst_ratio
FROM star s JOIN burst b USING(repo_id) LEFT JOIN (
  SELECT repo_id, max(common) common FROM
    (SELECT r1 repo_id, common FROM pair UNION ALL SELECT r2, common FROM pair) GROUP BY 1
) p USING(repo_id)
GROUP BY s.repo_id
""")
# 규칙은 버전별 컬럼으로 남긴다. 원본(star)은 그대로 두고 판정만 다시 계산 -> 규칙이 바뀌어도 재수집 없음
#  v1: 절대 lockstep 수 -> 트렌딩 저장소 대량 오탐 (트렌딩 보고 몰아서 star하는 실제 사용자도 같이 움직임)
#  v2: lockstep '비율' -> 오탐은 줄었지만, 하루 100개 넘게 찍는 계정 캠페인을 놓침 (사후 검증에서 발견)
#  v3: v2 + 5분 burst 비율
con.sql("""ALTER TABLE repo ADD COLUMN suspect_v1 BOOLEAN; ALTER TABLE repo ADD COLUMN suspect_v2 BOOLEAN; ALTER TABLE repo ADD COLUMN suspect_v3 BOOLEAN;
UPDATE repo SET suspect_v1 = stars>=20 AND low_act_share>=0.5 AND max_lockstep>=10,
                suspect_v2 = stars>=20 AND low_act_share>=0.5 AND lock_ratio>=0.5,
                suspect_v3 = stars>=20 AND low_act_share>=0.5 AND (lock_ratio>=0.5 OR burst_ratio>=0.5)""")
con.sql("CREATE VIEW repo_cur AS SELECT *, suspect_v3 AS suspect FROM repo")
show("규칙 버전별 결과 (같은 원본, 판정만 재계산)", """
SELECT v, count(*) FILTER (WHERE flag) suspect_repos,
       round(100*sum(stars) FILTER (WHERE flag)/sum(stars),1) pct_of_all_stars
FROM (SELECT 'v1' v, suspect_v1 flag, stars FROM repo UNION ALL SELECT 'v2', suspect_v2, stars FROM repo
      UNION ALL SELECT 'v3', suspect_v3, stars FROM repo) GROUP BY v ORDER BY v""")

show("lockstep 쌍 / 의심 저장소", """
SELECT (SELECT count(*) FROM pair) lockstep_pairs,
       (SELECT count(*) FROM repo_cur WHERE stars>=20) repos_20plus,
       (SELECT count(*) FROM repo_cur WHERE suspect) suspect_repos,
       (SELECT sum(stars) FROM repo_cur WHERE suspect) suspect_stars""")
show("의심 저장소 상위 20", "SELECT repo_name, stars, low_act_share, max_lockstep, round(lock_ratio,2) lock_ratio, round(burst_ratio,2) burst FROM repo_cur WHERE suspect ORDER BY stars DESC LIMIT 20")

# 검증용 표본: 의심 저장소 vs 같은 star 구간의 비의심 저장소
con.sql(f"""COPY (
  SELECT repo_name, stars, low_act_share, lock_ratio, burst_ratio, 'suspect' grp FROM repo_cur WHERE suspect
  UNION ALL
  (SELECT repo_name, stars, low_act_share, lock_ratio, burst_ratio, 'control' FROM repo_cur
   WHERE NOT suspect AND stars BETWEEN 20 AND (SELECT max(stars) FROM repo_cur WHERE suspect)
   ORDER BY hash(repo_id) LIMIT 300)
) TO '{out}/gh_validation_sample.csv' (HEADER)""")
con.sql(f"COPY (SELECT * FROM actor) TO '{out}/gh_actor_flags.parquet' (FORMAT parquet)")
con.sql(f"COPY (SELECT * FROM repo) TO '{out}/gh_repo_flags.parquet' (FORMAT parquet)")
print("\nsaved ->", out)
