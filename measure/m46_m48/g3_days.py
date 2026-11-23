"""G3: M-38 날짜별 앱(24g)에서 features_account, features_repo의 날짜별 핵심 스테이지. 읽기만."""
import json, statistics, sys
from collections import defaultdict
apps = open(__import__("pathlib").Path(__file__).parent / "apps_perday.txt").read().split()
print("| 작업 | 날짜 순서 | 가장 긴 스테이지(태스크 시간 합) | 태스크 | 중앙 ms | 최대 ms | 최대/중앙 | 셔플 읽기 MiB | 태스크 1개 스테이지 시간 합 ms | 여러 태스크 스테이지 최대/중앙 중 최대 | spill MiB |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
k = defaultdict(int)
for a in apps:
    name, t = "", defaultdict(list)
    for l in open(f"spark-events/{a}", encoding="utf-8"):
        e = json.loads(l)
        if e["Event"] == "SparkListenerApplicationStart": name = e["App Name"]
        elif e["Event"] == "SparkListenerTaskEnd" and e.get("Task Metrics"):
            ti, tm = e["Task Info"], e["Task Metrics"]; sr = tm.get("Shuffle Read Metrics", {})
            t[e["Stage ID"]].append((ti["Finish Time"] - ti["Launch Time"], sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0),
                                     tm.get("Memory Bytes Spilled", 0) + tm.get("Disk Bytes Spilled", 0)))
    if "features_" not in name: continue
    job = name.split("/")[-1]; k[job] += 1
    sid = max(t, key=lambda s: sum(x[0] for x in t[s]))
    d = sorted(x[0] for x in t[sid]); med = statistics.median(d)
    single = sum(x[0] for s, v in t.items() if len(v) == 1 for x in v)
    multi = max((max(x[0] for x in v) / statistics.median([x[0] for x in v]) for v in t.values() if len(v) > 1), default=0)
    sp = sum(x[2] for v in t.values() for x in v) / 2**20
    print(f"| {job} | {k[job]} | {sid} | {len(d)} | {med:.0f} | {d[-1]} | {d[-1]/med:.2f} | {sum(x[1] for x in t[sid])/2**20:.1f} | {single} | {multi:.2f} | {sp:.1f} |")
