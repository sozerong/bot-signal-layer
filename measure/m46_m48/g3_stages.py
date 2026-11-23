"""G3 (M-48): 앱 하나의 스테이지별 셔플·태스크 분포·spill과 AQE 흔적(파티션 합치기, 쏠림 조인 나누기). 읽기만 한다.

AQE 흔적: SQLAdaptiveExecutionUpdate의 최종 계획에서 AQEShuffleRead / SortMergeJoin(isSkew) 노드를 찾고,
DriverAccumUpdates로 그 노드 지표(합친 파티션 수, 쏠린 파티션 수)를 읽는다.
사용: python g3_stages.py <앱 id>   (레포 루트에서)
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def walk(node, out):
    out.append(node)
    for c in node.get("children", []):
        walk(c, out)


def main(app):
    p = Path("spark-events") / app
    name, stages, tasks, accum, plans = "", {}, defaultdict(list), {}, {}
    for line in open(p, encoding="utf-8"):
        e = json.loads(line)
        ev = e["Event"]
        if ev == "SparkListenerApplicationStart":
            name = e["App Name"]
        elif ev == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            stages[si["Stage ID"]] = si["Number of Tasks"]
        elif ev == "SparkListenerTaskEnd" and e.get("Task Metrics"):
            ti, tm = e["Task Info"], e["Task Metrics"]
            sr, sw, im = tm.get("Shuffle Read Metrics", {}), tm.get("Shuffle Write Metrics", {}), tm.get("Input Metrics", {})
            tasks[e["Stage ID"]].append((ti["Finish Time"] - ti["Launch Time"], sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0),
                                         sr.get("Total Records Read", 0), sw.get("Shuffle Bytes Written", 0), sw.get("Shuffle Records Written", 0),
                                         tm.get("Memory Bytes Spilled", 0), tm.get("Disk Bytes Spilled", 0), im.get("Records Read", 0),
                                         tm.get("Peak Execution Memory", 0)))
        elif ev.endswith("SQLAdaptiveExecutionUpdate"):
            plans[e["executionId"]] = e["sparkPlanInfo"]
        elif ev.endswith("SparkListenerDriverAccumUpdates"):
            for k, v in e["accumUpdates"]:
                accum[k] = v
    print(f"## {name} ({app})\n")
    print("| 스테이지 | 태스크 | 중앙 ms | 최대 ms | 최대/중앙 | 입력 행 | 셔플 읽기 MiB / 행 | 셔플 쓰기 MiB / 행 | spill 메모리 / 디스크 MiB | 태스크 최대 실행 메모리 MiB |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    M = 2 ** 20
    for sid, ts in sorted(tasks.items()):
        d = sorted(t[0] for t in ts)
        med = statistics.median(d)
        s = [sum(t[i] for t in ts) for i in range(1, 8)]
        print(f"| {sid} | {len(d)} | {med:.0f} | {d[-1]} | {d[-1] / med if med else 0:.2f} | {s[6]:,} | {s[0] / M:.1f} / {s[1]:,} | {s[2] / M:.1f} / {s[3]:,} | "
              f"{s[4] / M:.1f} / {s[5] / M:.1f} | {max(t[8] for t in ts) / M:.0f} |")
    print("\nAQE 흔적 (최종 계획, SQL 실행별)\n")
    for x, plan in sorted(plans.items()):
        nodes = []
        walk(plan, nodes)
        for n in nodes:
            nm, ss = n["nodeName"], n.get("simpleString", "")
            if nm.startswith("AQEShuffleRead") or ("SortMergeJoin" in nm and "isSkew" in ss) or nm == "Exchange" or "SortMergeJoin" in nm or "BroadcastHashJoin" in nm:
                ms = {m["name"]: accum.get(m["accumulatorId"]) for m in n.get("metrics", [])
                      if m["name"] in ("number of coalesced partitions", "number of skewed partitions", "number of skewed partition splits",
                                       "number of partitions", "partition data size", "number of output rows")}
                print(f"- exec {x}: {ss[:110]} {ms}")


if __name__ == "__main__":
    main(sys.argv[1])
