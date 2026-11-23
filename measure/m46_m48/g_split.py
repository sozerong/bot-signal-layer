"""M-47·M-48 실험 앱의 이벤트 로그를 잡 설명(spark.job.description)으로 나눠 요약한다. 읽기만.
묶음마다: 잡 제출 ~ 잡 끝 벽시계, 태스크 실행·CPU·GC·역직렬화 합, 스테이지·태스크 수, 가장 긴 스테이지의 태스크 수와 최대/중앙.
사용: python g_split.py <앱 id>   (레포 루트에서)
"""
import json
import statistics
import sys
from collections import defaultdict

app = sys.argv[1]
desc, jt, stage_job, tasks = {}, {}, {}, defaultdict(list)
for line in open(f"spark-events/{app}", encoding="utf-8"):
    e = json.loads(line)
    ev = e["Event"]
    if ev == "SparkListenerJobStart":
        desc[e["Job ID"]] = e.get("Properties", {}).get("spark.job.description", "")
        jt[e["Job ID"]] = [e["Submission Time"], None]
        for s in e["Stage IDs"]:
            stage_job[s] = e["Job ID"]
    elif ev == "SparkListenerJobEnd":
        jt[e["Job ID"]][1] = e["Completion Time"]
    elif ev == "SparkListenerTaskEnd" and e.get("Task Metrics"):
        ti, tm = e["Task Info"], e["Task Metrics"]
        tasks[e["Stage ID"]].append((ti["Finish Time"] - ti["Launch Time"], tm["Executor Run Time"], tm["Executor CPU Time"] / 1e6,
                                     tm["JVM GC Time"], tm["Executor Deserialize Time"]))
groups = defaultdict(lambda: {"jobs": [], "stages": []})
for j, d in desc.items():
    groups[d]["jobs"].append(j)
for s, j in stage_job.items():
    if s in tasks:
        groups[desc[j]]["stages"].append(s)
print("| 묶음 | 벽시계 s | 태스크 실행 s | CPU s | GC s | 역직렬화 s | 스테이지 | 태스크 | 가장 긴 스테이지 태스크 수 | 그 최대/중앙 |")
print("|---|---|---|---|---|---|---|---|---|---|")
for d, g in sorted(groups.items(), key=lambda kv: min(jt[j][0] for j in kv[1]["jobs"])):
    w = (max(jt[j][1] for j in g["jobs"]) - min(jt[j][0] for j in g["jobs"])) / 1000
    ts = [t for s in g["stages"] for t in tasks[s]]
    if not ts:
        continue
    big = max(g["stages"], key=lambda s: sum(t[0] for t in tasks[s]))
    bd = sorted(t[0] for t in tasks[big])
    print(f"| {d} | {w:.2f} | {sum(t[1] for t in ts) / 1000:.2f} | {sum(t[2] for t in ts) / 1000:.2f} | {sum(t[3] for t in ts) / 1000:.2f} | "
          f"{sum(t[4] for t in ts) / 1000:.2f} | {len(g['stages'])} | {len(ts)} | {len(bd)} | {bd[-1] / statistics.median(bd):.2f} |")
