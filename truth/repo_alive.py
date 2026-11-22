"""저장소 생존 확인 -> raw/truth_raw/repo_alive/ (implementation_design.md 8절 L4-1, 6절 pick_targets + repo_alive).

사전 실험 03_validate_exists.py를 옮겼다. 바뀐 점:
- 대상은 sql/truth/pick_targets.sql (판정 구간의 의심 저장소 + 같은 수의 대조군).
- 동시 4개. 실패(200, 301, 404, 451이 아닌 응답과 연결 오류)만 다시 묻는다. 사전 실험은 동시 8개에서 429가 21건 나왔다.
- 429(요청 너무 많음)를 받으면 모든 동시 요청이 같이 쉰다: 응답의 Retry-After, 없으면 60초, 120초, 300초로 늘린다.
  성공하면 단계를 처음으로 되돌린다. 엿새치를 한 번에 묻다가 막힌 상태에서 각자 계속 두드렸다 (F-15, 결정 16).
- --only-missing: 같은 (저장소, 구간)에 성공한 확인이 확인 결과 파일에 이미 있으면 묻지 않는다. 다시 보내기는 실패한 것만.
- 끝날 때 확인 실패 비율이 --max-fail-rate(기본 5%)를 넘으면 exit 1. 확인 실패가 초록 태스크 뒤에 숨지 않게 (F-15).
- 근거별 지금 규칙 버전을 rules/registry.yml에서 읽어 pick_targets에 넘긴다 (F-13, 결정 16). 피처 버전(features_repo)도 (R-16).
- 근거로 의심이 된 저장소가 있는데 대상이 0개면 실패로 끝낸다. 피처 버전이 어긋난 날이 초록으로 지나가지 않게 (R-16).
- 301(이름이 바뀜)은 살아 있음으로 본다. 리다이렉트를 따라가지 않고 첫 응답 코드를 남긴다.
- 결과는 원본처럼 쌓는다: checked_date=날짜 폴더에 새 part 파일. 옛 파일을 덮어쓰지 않는다.

사용 (bot-job 컨테이너 안):
  python -m truth.repo_alive --window-end 2024-07-15 [--only-missing] [--max-fail-rate 0.05]
"""
import argparse
import datetime as dt
import json
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml

from spark_jobs.session import ROOT, spark_session

OK = {200, 301, 404, 451}  # 451(법적 이유로 막힘)은 확인은 된 것이다. truth에는 넣지 않는다 (truth.sql)
TRIES = 3
BACKOFF = [60, 120, 300]  # 429에 Retry-After가 없을 때. 마지막 값을 계속 쓴다
MAX_429 = 6  # 한 요청이 429를 이만큼 받으면 실패로 남긴다 (다음 실행의 --only-missing이 다시 묻는다)


class Gate:
    """동시 요청이 함께 쓰는 쉼 시각. 429 하나가 모두를 쉬게 한다"""
    def __init__(self):
        self.lock, self.until, self.level = threading.Lock(), 0.0, 0
        self.requests = self.n429 = 0
        self.paused = 0.0

    def wait(self):
        while (left := self.until - time.monotonic()) > 0:
            time.sleep(min(left, 5))

    def hit_429(self, retry_after):
        with self.lock:
            self.n429 += 1
            if time.monotonic() < self.until:  # 이미 쉬는 중이면 다른 요청이 단계를 올렸다
                return
            wait = int(retry_after) if (retry_after or "").isdigit() else BACKOFF[min(self.level, len(BACKOFF) - 1)]
            self.level += 1
            self.until = time.monotonic() + wait
            self.paused += wait
            print(f"429: 모두 {wait}초 쉼 (단계 {self.level})", flush=True)

    def ok(self):
        with self.lock:
            self.level = 0


GATE = Gate()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None  # 301을 따라가지 않고 HTTPError로 받는다


OPENER = urllib.request.build_opener(NoRedirect)


def status(name):
    req = urllib.request.Request(f"https://github.com/{name}", method="HEAD", headers={"User-Agent": "bot-porj-validation"})
    for _ in range(MAX_429):
        GATE.wait()
        with GATE.lock:
            GATE.requests += 1
        try:
            code = OPENER.open(req, timeout=15).status
        except urllib.error.HTTPError as e:
            if e.code == 429:
                GATE.hit_429(e.headers.get("Retry-After"))
                continue
            code = e.code
        except Exception:
            return -1
        GATE.ok()
        return code
    return 429


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


def checked(raw, day):
    """확인 결과 파일에서 이 구간에 이미 성공한 저장소들"""
    done = set()
    for f in raw.glob("checked_date=*/part-*.jsonl"):
        for line in f.open(encoding="utf-8"):
            r = json.loads(line)
            if r.get("window_end") == day and r["http"] in OK:
                done.add(str(r["entity_key"]))
    return done


def versions():
    """pick_targets 파라미터: 근거별 지금 규칙 버전과 피처 버전 (registry). 피처 버전은 전에 SQL에 'f1'로 박혀 있었다 (R-16)"""
    reg = yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    return {**{f"v_{k}": reg[k]["current"] for k in ("low_activity", "lockstep_ratio", "burst_5m")},
            "feature_version": reg["features_repo"]["current"]}


# 그 구간에 지금 버전 근거로 의심(low_activity AND (lockstep_ratio OR burst_5m))이 된 저장소 수. pick_targets.sql의 ev와 같은 식
SUSPECTS = """
SELECT count(*) FROM (
  SELECT entity_key FROM bot.silver.evidence
  WHERE entity_type = 'gh_repo' AND window_end = DATE(:window_end)
    AND ((evidence_id = 'low_activity' AND rule_version = :v_low_activity)
      OR (evidence_id = 'lockstep_ratio' AND rule_version = :v_lockstep_ratio)
      OR (evidence_id = 'burst_5m' AND rule_version = :v_burst_5m))
  GROUP BY entity_key
  HAVING bool_or(evidence_id = 'low_activity' AND hit) AND bool_or(evidence_id IN ('lockstep_ratio', 'burst_5m') AND hit)
)"""


def pick(spark, day, params=None):
    """pick_targets 결과 행. 근거로 의심이 된 저장소가 있는데 대상이 0개면 SystemExit (R-16).
    피처 버전이 어긋나면(registry와 다른 버전의 피처만 있으면) 피처 조인이 비어 대상이 0개가 되고, 전에는 초록으로 끝났다"""
    p = {"window_end": day, **(params or versions())}
    sql = (ROOT / "sql" / "truth" / "pick_targets.sql").read_text(encoding="utf-8")
    rows = [r.asDict() for r in spark.sql(sql, args=p).collect()]
    if not rows:
        n = spark.sql(SUSPECTS, args={k: v for k, v in p.items() if k != "feature_version"}).first()[0]
        if n:
            raise SystemExit(f"{day}: 근거로 의심이 된 저장소 {n}개인데 확인 대상이 0개다. "
                             f"피처 버전 {p['feature_version']}의 entity_features가 그날 있는지 본다 (docs/review_logic.md R-16)")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-end", required=True)
    ap.add_argument("--only-missing", action="store_true", help="이 구간에 이미 성공한 확인이 있는 저장소는 묻지 않는다")
    ap.add_argument("--max-fail-rate", type=float, default=0.05, help="확인 실패 비율이 이보다 크면 exit 1")
    a = ap.parse_args()
    day = a.window_end
    raw = Path(os.environ.get("BOT_BRONZE", ROOT / "raw")) / "truth_raw" / "repo_alive"
    spark = spark_session("repo_alive", {"spark.ui.enabled": "false"})
    try:
        rows = pick(spark, day)
    finally:
        spark.stop()
    picked = len(rows)
    if a.only_missing:
        done = checked(raw, day)
        rows = [r for r in rows if str(r["entity_key"]) not in done]
    print(f"{day}: 대상 {picked}개, 물을 것 {len(rows)}개", flush=True)
    if not rows:
        return
    started = time.monotonic()
    rows = check(rows)
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    out = raw / f"checked_date={today}"
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
    print(f"-> {path.relative_to(ROOT).as_posix()}" if path.is_relative_to(ROOT) else f"-> {path}")
    bad = sum(r["http"] not in OK for r in rows)
    print(f"요청 {GATE.requests}번, 429 {GATE.n429}번, 쉼 {GATE.paused:.0f}초, 걸린 시간 {time.monotonic() - started:.0f}초, "
          f"확인 실패 {bad}/{len(rows)} ({100 * bad / len(rows):.1f}%)", flush=True)
    if bad / len(rows) > a.max_fail_rate:
        raise SystemExit(f"확인 실패 비율 {bad / len(rows):.1%}가 문턱 {a.max_fail_rate:.1%}를 넘는다 (F-15). "
                         f"--only-missing으로 다시 돌리면 실패한 것만 묻는다")


if __name__ == "__main__":
    main()
