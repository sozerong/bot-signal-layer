"""tests/fixtures/gh_sample.json.gz 만들기 (implementation_design.md 8절 L1-3). 한 번 돌려 결과를 커밋한다.

2024-07-15 12시 원본에서:
  - 그 시간 안에서 저활동(star만 함) 계정 비율이 높은, star 많은 저장소 3개 (근거가 걸리는 쪽)
  - 비율이 낮은, star 많은 저장소 2개 (트렌딩: 걸리면 안 되는 쪽)
  - 위 저장소에 star한 계정의 그 시간 이벤트 전부 (저활동 판정이 원본과 같아지게)
  - 무작위 이벤트 2000개 (seed 고정)
payload는 읽지 않는 필드라 뺀다.

사용: python tests/make_fixture.py raw/dt=2024-07-15/hour=12/2024-07-15-12.json.gz
"""
import gzip
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

OUT = Path(__file__).parent / "fixtures" / "gh_sample.json.gz"


def main(src):
    rows = []
    with gzip.open(src, "rt", encoding="utf-8") as f:
        for line in f:
            e = json.loads(line)
            e.pop("payload", None)
            rows.append(e)
    types = defaultdict(set)
    for e in rows:
        types[e["actor"]["id"]].add(e["type"])
    low = {a for a, t in types.items() if t == {"WatchEvent"}}
    stars, low_stars = Counter(), Counter()
    for e in rows:
        if e["type"] == "WatchEvent" and not e["actor"]["login"].endswith("[bot]"):
            stars[e["repo"]["id"]] += 1
            low_stars[e["repo"]["id"]] += e["actor"]["id"] in low
    big = [r for r, n in stars.most_common() if n >= 20]
    hot = [r for r in big if low_stars[r] / stars[r] >= 0.5][:3]
    # 한 시간 안에는 star 20개 이상이면서 비율이 낮은 저장소가 없었다(2024-07-15 12시). 그래서 20개 기준 없이 star 많은 순
    trend = [r for r, n in stars.most_common() if low_stars[r] / n < 0.5][:2]
    picked = set(hot + trend)
    actors = {e["actor"]["id"] for e in rows if e["type"] == "WatchEvent" and e["repo"]["id"] in picked}
    keep = {i for i, e in enumerate(rows) if e["actor"]["id"] in actors}
    keep |= set(random.Random(20240715).sample(range(len(rows)), 2000))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(OUT, "wt", encoding="utf-8") as f:
        for i in sorted(keep):
            f.write(json.dumps(rows[i], ensure_ascii=False) + "\n")
    for r in hot + trend:
        print(f"repo {r}: stars {stars[r]}, low {low_stars[r]}", "(hot)" if r in hot else "(trend)")
    print(f"{OUT}: {len(keep)}줄, 계정 {len(actors)}명")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main(sys.argv[1])
