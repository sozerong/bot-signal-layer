"""EClog 하루치 표본 적합성 검사 (지시서 handoff_v5_wp1.md 1-1). 결과는 measurements.md M-24, 고른 원천은 결정 21.

검사 네 개 (지시서 1-1 "고르는 규칙": 1, 2, 3 통과면 EClog, 4까지면 purchase 정답도):
  1. 세션을 만들 수 있나: UserId 빈 값 비율, 고유 수, IpId와의 관계
  2. 정적 파일(CSS, JS, 이미지)을 경로·확장자로 가를 수 있나
  3. 자기 신고 봇 UA가 있나: 고유 UA만 뽑아 크롤러 목록(raw/ref/crawler_ua)으로 한 번 매칭
  4. 구매(주문 완료)를 경로로 가를 수 있나: URI 가림 정도를 경로 앞부분별 개수로

데이터 설명의 필드 10개(IpId, UserId, TimeStamp, HttpMethod, Uri, HttpVersion, ResponseCode, Bytes, Referrer, UserAgent)를 가정한다.
파일을 받기 전에 썼다: 첫 줄에서 실제 컬럼 이름을 찍고, 이름이 다르면 멈춘다 (그때 고친다).

사용 (bot-job 컨테이너 안): python -m scripts.eclog_check raw/eclog/eclog_1day.tab
"""
import json
import re
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WANT = ["IpId", "UserId", "TimeStamp", "HttpMethod", "Uri", "HttpVersion", "ResponseCode", "Bytes", "Referrer", "UserAgent"]
STATIC = r"\.(css|js|png|jpe?g|gif|svg|ico|webp|woff2?|ttf|eot|map)(\?|$)"


def main(path):
    c = duckdb.connect()
    c.sql(f"create view r as select * from read_csv('{path}', auto_detect = true, all_varchar = true)")
    cols = c.sql("select * from r limit 0").columns
    print("컬럼:", cols)
    missing = [w for w in WANT if w not in cols]
    if missing:
        raise SystemExit(f"설명과 다른 컬럼 이름: 없는 것 {missing}. 스크립트의 이름을 실제에 맞춘다")
    q = lambda s: c.sql(s).fetchall()  # noqa: E731
    n, = q("select count(*) from r")[0]
    print(f"\n행 {n:,}")
    print("시각 예:", q("select min(TimeStamp), max(TimeStamp) from r")[0])

    print("\n[1] 세션: UserId")
    print(q("""select count(*) filter (where UserId is null or UserId in ('', '-', '0')) as empty,
                      count(distinct UserId) as users, count(distinct IpId) as ips,
                      count(distinct IpId || '|' || UserAgent) as ip_ua from r"""))
    print("UserId 하나에 IpId가 몇 개 (상위):", q("""select n_ip, count(*) from (select UserId, count(distinct IpId) n_ip from r
                                                   group by 1) group by 1 order by 2 desc limit 5"""))

    print("\n[2] 정적 파일 비율과 확장자 상위")
    print(q(f"select count(*) filter (where regexp_matches(lower(Uri), '{STATIC}')) * 1.0 / count(*) from r"))
    print(q(f"""select regexp_extract(lower(Uri), '{STATIC}', 1) ext, count(*) from r
                where regexp_matches(lower(Uri), '{STATIC}') group by 1 order by 2 desc limit 10"""))

    print("\n[3] 자기 신고 봇 UA (고유 UA만 매칭)")
    ref = sorted((ROOT / "raw" / "ref" / "crawler_ua").glob("dt=*/crawler-user-agents.json"))[-1]
    pats = [re.compile(e["pattern"]) for e in json.loads(ref.read_text(encoding="utf-8"))]
    uas = q("select UserAgent, count(*) from r group by 1")
    hit = [(u, k) for u, k in uas if u and any(p.search(u) for p in pats)]
    print(f"고유 UA {len(uas):,}개 중 걸림 {len(hit):,}개, 요청 {sum(k for _, k in hit):,} / {n:,} (목록 {ref.parent.name})")
    print("robots.txt 요청:", q("select count(*) from r where lower(Uri) like '%robots.txt%'")[0][0])
    print("UA에 Code/ 가 든 요청 (M-25 한계):", q("select count(*) from r where UserAgent like '%Code/%'")[0][0])

    print("\n[4] 경로 앞부분별 개수 (가림 정도, 구매 경로 찾기)")
    print(q("""select regexp_extract(Uri, '^(/[^/?]*)', 1) head, count(*) from r group by 1 order by 2 desc limit 30"""))
    print("가린 흔적 예:", q("select Uri from r where Uri like '%*%' or Uri like '%xxx%' limit 5"))


if __name__ == "__main__":
    main(sys.argv[1])
