"""라벨이 없으니 '지금도 저장소가 살아 있나'로 사후 검증.
가짜 star로 띄운 악성 저장소는 신고·삭제돼 사라지는 경우가 많다(StarScout 논문 관찰).
의심 그룹과 대조 그룹(같은 날 star 20개 이상, 비의심)의 404 비율을 비교.
usage: python 03_validate_exists.py results/gh_validation_sample.csv results/gh_validation_result.csv
"""
import csv, sys, urllib.request, urllib.error

src, out = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(src, encoding="utf-8")))

def status(repo):
    req = urllib.request.Request(f"https://github.com/{repo}", method="HEAD",
                                 headers={"User-Agent": "bot-porj-validation"})
    try:
        return urllib.request.urlopen(req, timeout=15).status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return -1

from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(8) as ex:  # ponytail: 429가 많이 나오면 동시성을 줄일 것
    for r, code in zip(rows, ex.map(lambda r: status(r["repo_name"]), rows)):
        r["http"] = code

with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)

for g in ("suspect", "control"):
    rs = [r for r in rows if r["grp"] == g]
    gone = sum(r["http"] == 404 for r in rs)
    print(f"{g:8s} n={len(rs):4d}  gone(404)={gone:4d}  {100*gone/len(rs):.1f}%  errors={sum(r['http'] not in (200,404) for r in rs)}")
