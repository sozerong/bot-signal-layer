"""G2 (M-47): M-38 이벤트 로그를 날짜 구간으로 잘라 날짜별 앱(63개)과 묶음 앱(9개)을 나란히 본다. 읽기만 한다.

날짜 구간: 앱 안의 SQL 실행을 시각 순으로 7등분(run_sql 3개, load_events 8개가 하루). 묶음은 날짜 k의 첫 SQL 시작 ~ 날짜 k+1의 첫 SQL 시작,
날짜별 앱은 첫 SQL 시작 ~ 마지막 SQL 끝. 앱 시작 ~ 첫 SQL 시작 = 앞(세션·카탈로그 준비), 마지막 SQL 끝 ~ 앱 끝 = 뒤(멈추기).
스케줄러 지연은 Spark UI와 같은 식: 태스크 기간 - 실행 - 역직렬화 - 결과 직렬화 - 결과 받기 (0 아래는 0).
사용: python g2_ana.py  (레포 루트에서)
"""
import json
from collections import defaultdict
from pathlib import Path

N = Path(__file__).parent  # 앱 id 목록(apps_*.txt)이 스크립트 옆에 있다
DAYS = 7


def read(app):
    p = Path("spark-events") / app
    if p.is_dir():
        p = sorted(p.iterdir())[0]
    a = {"execs": {}, "job_exec": {}, "stage_job": {}, "tasks": []}
    for line in open(p, encoding="utf-8"):
        e = json.loads(line)
        ev = e["Event"]
        if ev == "SparkListenerApplicationStart":
            a["start"], a["name"] = e["Timestamp"], e["App Name"]
        elif ev == "SparkListenerApplicationEnd":
            a["end"] = e["Timestamp"]
        elif ev.endswith("SQLExecutionStart"):
            a["execs"][e["executionId"]] = [e["time"], None]
        elif ev.endswith("SQLExecutionEnd"):
            a["execs"][e["executionId"]][1] = e["time"]
        elif ev == "SparkListenerJobStart":
            x = e.get("Properties", {}).get("spark.sql.execution.id")
            a["job_exec"][e["Job ID"]] = int(x) if x is not None else None
            for s in e["Stage IDs"]:
                a["stage_job"][s] = e["Job ID"]
        elif ev == "SparkListenerTaskEnd" and e.get("Task Metrics"):
            ti, tm = e["Task Info"], e["Task Metrics"]
            dur = ti["Finish Time"] - ti["Launch Time"]
            run, des, ser = tm["Executor Run Time"], tm["Executor Deserialize Time"], tm["Result Serialization Time"]
            get = ti.get("Getting Result Time", 0)
            a["tasks"].append({"stage": e["Stage ID"], "launch": ti["Launch Time"], "run": run, "cpu": tm["Executor CPU Time"] / 1e6,
                               "gc": tm["JVM GC Time"], "des": des, "sched": max(0, dur - run - des - ser - get)})
    return a


def task_of(name):
    p = name.split(" ")
    return "/".join(p[1].split("/")[-2:]) if p[0] in ("run_sql", "bundle") and len(p) > 1 else p[0]


def seg(a, bundle):
    """앱 하나 -> 날짜 구간 목록 [{wall, run, cpu, gc, des, sched, tasks, first_task_delay}] 와 앞/뒤 시간."""
    ids = sorted(a["execs"])
    per = len(ids) // (DAYS if bundle else 1)
    groups = [ids[i * per:(i + 1) * per] for i in range(DAYS if bundle else 1)]
    starts = [a["execs"][g[0]][0] for g in groups]
    last_end = max(v[1] for v in a["execs"].values())
    bounds = starts[1:] + [last_end]
    exec_day = {x: k for k, g in enumerate(groups) for x in g}
    out = []
    for k, g in enumerate(groups):
        out.append({"wall": (bounds[k] - starts[k]) / 1000, "run": 0, "cpu": 0, "gc": 0, "des": 0, "sched": 0, "tasks": 0, "first_launch": None})
    for t in a["tasks"]:
        x = a["job_exec"].get(a["stage_job"].get(t["stage"]))
        k = exec_day.get(x)
        if k is None:  # SQL 실행 밖 잡: 시각으로 붙인다
            k = max([i for i, s in enumerate(starts) if s <= t["launch"]] or [0])
        o = out[k]
        for f in ("run", "cpu", "gc", "des", "sched"):
            o[f] += t[f] / 1000
        o["tasks"] += 1
        o["first_launch"] = t["launch"] if o["first_launch"] is None else min(o["first_launch"], t["launch"])
    for k, o in enumerate(out):
        o["to_first_task"] = (o["first_launch"] - starts[k]) / 1000 if o["first_launch"] else None
    pre = (starts[0] - a["start"]) / 1000
    post = (a["end"] - last_end) / 1000
    first_task = (min(t["launch"] for t in a["tasks"]) - a["start"]) / 1000
    return out, pre, post, first_task


def main():
    res = {}
    for arm in ("perday", "bundle"):
        apps = (N / f"apps_{arm}.txt").read_text().split()
        rows = defaultdict(list)  # task -> [(app, segs, pre, post, first_task, app_wall)]
        for app in apps:
            a = read(app)
            segs, pre, post, ft = seg(a, arm == "bundle")
            rows[task_of(a["name"])].append((app, segs, pre, post, ft, (a["end"] - a["start"]) / 1000))
        res[arm] = rows
    # 날짜별 쪽은 앱이 날짜 순서대로(run.sh DAYS 순) 7개
    tasks = list(res["perday"])
    print("| 태스크 | 날짜 | 날짜별: 구간 벽시계 | 태스크 실행 합 | GC | 역직렬화 | 스케줄러 지연 | 묶음: 구간 벽시계 | 태스크 실행 합 | GC | 역직렬화 | 스케줄러 지연 |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    tot = defaultdict(float)
    by_pos = defaultdict(lambda: defaultdict(float))
    for t in tasks:
        P = [r[1][0] for r in sorted(res["perday"][t])]
        B = res["bundle"][t][0][1]
        for k in range(DAYS):
            p, b = P[k], B[k]
            print(f"| {t} | {k + 1} | {p['wall']:.2f} | {p['run']:.2f} | {p['gc']:.2f} | {p['des']:.2f} | {p['sched']:.2f} | {b['wall']:.2f} | {b['run']:.2f} | {b['gc']:.2f} | {b['des']:.2f} | {b['sched']:.2f} |")
            for f in ("wall", "run", "gc", "des", "sched", "cpu"):
                tot[f"p_{f}"] += p[f]; tot[f"b_{f}"] += b[f]
                by_pos[k][f"p_{f}"] += p[f]; by_pos[k][f"b_{f}"] += b[f]
    print()
    print("| 날짜 순서 | 날짜별 구간 벽시계 합 | 묶음 구간 벽시계 합 | 묶음 ÷ 날짜별 | 날짜별 태스크 실행 합 | 묶음 태스크 실행 합 | 묶음 ÷ 날짜별 | 날짜별 GC | 묶음 GC |")
    print("|---|---|---|---|---|---|---|---|---|")
    for k in range(DAYS):
        v = by_pos[k]
        print(f"| {k + 1} | {v['p_wall']:.1f} | {v['b_wall']:.1f} | {v['b_wall'] / v['p_wall']:.2f} | {v['p_run']:.1f} | {v['b_run']:.1f} | {v['b_run'] / v['p_run']:.2f} | {v['p_gc']:.1f} | {v['b_gc']:.1f} |")
    print()
    for arm, key in (("perday", "p"), ("bundle", "b")):
        pre = sum(r[2] for rs in res[arm].values() for r in rs)
        post = sum(r[3] for rs in res[arm].values() for r in rs)
        ft = [r[4] for rs in res[arm].values() for r in rs]
        aw = sum(r[5] for rs in res[arm].values() for r in rs)
        n = sum(len(rs) for rs in res[arm].values())
        print(f"{arm}: apps={n} app_wall={aw:.1f} seg_wall={tot[key + '_wall']:.1f} pre={pre:.1f} post={post:.1f} "
              f"gap_check={aw - tot[key + '_wall'] - pre - post:.1f} run={tot[key + '_run']:.1f} cpu={tot[key + '_cpu']:.1f} gc={tot[key + '_gc']:.1f} "
              f"des={tot[key + '_des']:.1f} sched={tot[key + '_sched']:.1f} app_start_to_first_task_mean={sum(ft) / len(ft):.2f}")


if __name__ == "__main__":
    main()
