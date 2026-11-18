"""저장소 생존 확인 -> raw/truth_raw/repo_alive/ (implementation_design.md 8절 L4-1, 6절 pick_targets + repo_alive).

사전 실험 03_validate_exists.py를 옮겼다. 바뀐 점:
- 대상은 sql/truth/pick_targets.sql (판정 구간의 의심 저장소 + 같은 수의 대조군).
- 동시 4개. 실패(200, 301, 404가 아닌 응답과 연결 오류)만 다시 묻는다. 사전 실험은 동시 8개에서 429가 21건 나왔다.
- 301(이름이 바뀜)은 살아 있음으로 본다. 리다이렉트를 따라가지 않고 첫 응답 코드를 남긴다.
- 결과는 원본처럼 쌓는다: checked_date=날짜 폴더에 새 part 파일. 옛 파일을 덮어쓰지 않는다.

사용 (bot-job 컨테이너 안):
  python -m truth.repo_alive --window-end 2024-07-15
"""
import argparse
import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from spark_jobs.session import ROOT, spark_session

OK = {200, 301, 404}
TRIES = 3


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None  # 301을 따라가지 않고 HTTPError로 받는다


OPENER = urllib.request.build_opener(NoRedirect)


def status(name):
    req = urllib.request.Request(f"https://github.com/{name}", method="HEAD", headers={"User-Agent": "bot-porj-validation"})
    try:
        return OPENER.open(req, timeout=15).status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return -1


def check(rows):
    """rows에 http, attempts, checked_at을 채운다. 실패만 다시 묻는다 (사이에 쉬는 시간을 늘린다)."""
    todo = rows
    for attempt in range(1, TRIES + 1):
        with ThreadPoolExecutor(4) as ex:
            for r, code in zip(todo, ex.map(lambda r: status(r["entity_name"]), todo)):
                r.update(http=code, attempts=attempt, checked_at=dt.datetime.now(dt.timezone.utc).isoformat())
        todo = [r for r in todo if r["http"] not in OK]
        if not todo:
            break
        time.sleep(30 * attempt)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-end", required=True)
    day = ap.parse_args().window_end
    spark = spark_session("repo_alive", {"spark.ui.enabled": "false"})
    sql = (ROOT / "sql" / "truth" / "pick_targets.sql").read_text(encoding="utf-8")
    rows = [r.asDict() for r in spark.sql(sql, args={"window_end": day}).collect()]
    spark.stop()
    rows = check(rows)
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    out = Path(os.environ.get("BOT_BRONZE", ROOT / "raw")) / "truth_raw" / "repo_alive" / f"checked_date={today}"
    out.mkdir(parents=True, exist_ok=True)
    n = len(list(out.glob("part-*.jsonl")))
    path = out / f"part-{n:03d}.jsonl"
    with open(path, "x", encoding="utf-8") as f:  # x: 있는 파일은 덮어쓰지 않는다
        for r in rows:
            f.write(json.dumps({**r, "window_end": day}, ensure_ascii=False) + "\n")
    for g in ("suspect", "control"):
        rs = [r for r in rows if r["grp"] == g]
        gone = sum(r["http"] == 404 for r in rs)
        bad = sum(r["http"] not in OK for r in rs)
        print(f"{g:8s} n={len(rs):4d}  404={gone:4d} ({100 * gone / max(len(rs), 1):.1f}%)  확인 실패={bad}")
    print(f"-> {path.relative_to(ROOT).as_posix()}")


if __name__ == "__main__":
    main()
