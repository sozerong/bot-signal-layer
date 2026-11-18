"""Spark 이벤트 로그(spark-events/) -> 스테이지별 태스크 시간 분포와 셔플 (implementation_design.md 7절).

쏠림 = 태스크 시간 최댓값 / 중앙값. 표준 라이브러리만 쓴다 (호스트에서도, bot-job에서도 돈다).

사용:
  python -m measure.spark_eventlog                 # 가장 최근 로그 하나
  python -m measure.spark_eventlog --app features_repo --all   # 이름에 features_repo가 든 로그 전부
"""
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def summarize(path):
    app, stages, tasks = None, {}, defaultdict(list)
    for line in open(path, encoding="utf-8"):
        e = json.loads(line)
        ev = e["Event"]
        if ev == "SparkListenerApplicationStart":
            app = e["App Name"]
        elif ev == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            stages[si["Stage ID"]] = si["Stage Name"]
        elif ev == "SparkListenerTaskEnd" and e.get("Task Metrics"):
            ti, tm = e["Task Info"], e["Task Metrics"]
            sr, sw = tm.get("Shuffle Read Metrics", {}), tm.get("Shuffle Write Metrics", {})
            tasks[e["Stage ID"]].append((
                ti["Finish Time"] - ti["Launch Time"],
                sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0),
                sw.get("Shuffle Bytes Written", 0),
                tm.get("Memory Bytes Spilled", 0) + tm.get("Disk Bytes Spilled", 0),
            ))
    rows = []
    for sid, ts in sorted(tasks.items()):
        d = sorted(t[0] for t in ts)
        med = statistics.median(d)
        rows.append({"stage": sid, "name": stages.get(sid, "")[:40], "tasks": len(d), "med_ms": int(med), "max_ms": d[-1],
                     "skew": round(d[-1] / med, 1) if med else None, "shuffle_read_mb": round(sum(t[1] for t in ts) / 2**20, 1),
                     "shuffle_write_mb": round(sum(t[2] for t in ts) / 2**20, 1), "spill_mb": round(sum(t[3] for t in ts) / 2**20, 1)})
    return app, rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(ROOT / "spark-events"))
    ap.add_argument("--app", default="", help="앱 이름에 든 글자로 거른다")
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    logs = sorted((p for p in Path(a.dir).iterdir() if p.is_file() and not p.name.endswith(".inprogress")),
                  key=lambda p: p.stat().st_mtime)
    shown = 0
    for p in reversed(logs):
        app, rows = summarize(p)
        if a.app not in (app or ""):
            continue
        print(f"## {app}  ({p.name})")
        print("| stage | 이름 | 태스크 | 중앙 ms | 최대 ms | 최대/중앙 | 셔플 읽기 MB | 셔플 쓰기 MB | spill MB |")
        print("|---|---|---|---|---|---|---|---|---|")
        for r in rows:
            print("| " + " | ".join(str(v) for v in r.values()) + " |")
        shown += 1
        if not a.all:
            break
    if not shown:
        raise SystemExit("맞는 이벤트 로그가 없다")


if __name__ == "__main__":
    main()
