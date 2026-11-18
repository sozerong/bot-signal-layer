"""G1 실험 키트: raw json.gz를 다시 처리하는 비용을 잰다.

A 구현 = ingest.py, measure_health.py와 같은 방식이다(스레드 풀, 줄마다 json.loads).
답(해결책)은 여기 없다. 손잡이를 돌리고 계기를 보는 것까지만 한다.

손잡이
  --workers N        스레드 수 (ingest.py의 ThreadPoolExecutor와 같은 방식)
  --stage read|parse read = gzip 해제 + 줄 나누기만, parse = 거기에 json.loads까지
  --files N          raw에서 앞에서부터 N개 파일
  --pool thread|process  B(5절): process면 파일마다 다른 프로세스에서 처리한다

계기 (끝나면 한 줄로 출력). process면 자식 프로세스까지 합친 값이다
  wall    : 걸린 시간
  cpu     : CPU 시간 (user + system)
  코어평균 : cpu / wall. 평균적으로 코어 몇 개분을 썼나
  코어최대 : 0.2초마다 잰 CPU%의 최댓값 (100% = 코어 1개)
  RSS최대 : 0.2초마다 잰 메모리 합의 최댓값
  MB/s    : 압축 파일 기준 처리량

실행 (프로젝트 루트에서)
  python experiments/g1_reparse/g1_reparse.py --workers 1 --stage parse
"""
import argparse
import gzip
import json
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

import psutil


def work(path, stage):
    t = time.perf_counter()
    n = 0
    with gzip.open(path, "rb") as f:
        if stage == "read":
            for _ in f:
                n += 1
        else:
            for line in f:
                json.loads(line)
                n += 1
    return path.name, n, time.perf_counter() - t


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--stage", choices=["read", "parse"], default="parse")
    p.add_argument("--files", type=int, default=4)
    p.add_argument("--root", default="raw")
    p.add_argument("--pool", choices=["thread", "process"], default="thread")
    p.add_argument("--quiet", action="store_true", help="파일별 시간 생략")
    a = p.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")

    # .part는 *.json.gz에 안 걸린다. 완료된 파일만 잡힌다
    files = sorted(Path(a.root).glob("dt=*/hour=*/*.json.gz"))[: a.files]
    if len(files) < a.files:
        raise SystemExit(f"raw에 완료된 파일이 {len(files)}개뿐이다")
    mb = sum(f.stat().st_size for f in files) / 1e6

    me = psutil.Process()
    procs = {me.pid: me}  # 자식 프로세스는 생기는 대로 붙인다. cpu_percent는 같은 객체로 불러야 차이를 잰다
    me.cpu_percent(None)  # 첫 호출은 기준점만 잡는다
    peak = {"cpu": 0.0, "rss": 0}
    done = threading.Event()

    def sample():
        while not done.wait(0.2):
            for c in me.children(recursive=True):
                if c.pid not in procs:
                    procs[c.pid] = c
                    c.cpu_percent(None)
            cpu = rss = 0
            for pr in list(procs.values()):
                try:
                    cpu += pr.cpu_percent(None)
                    rss += pr.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
            peak["cpu"], peak["rss"] = max(peak["cpu"], cpu), max(peak["rss"], rss)

    def cpu_total():
        t = 0.0
        for c in [me, *me.children(recursive=True)]:
            ct = c.cpu_times()
            t += ct.user + ct.system
        return t

    sampler = threading.Thread(target=sample, daemon=True)
    c0, t0 = cpu_total(), time.perf_counter()
    sampler.start()
    Pool = ProcessPoolExecutor if a.pool == "process" else ThreadPoolExecutor
    with Pool(a.workers) as ex:
        results = list(ex.map(work, files, [a.stage] * len(files)))
        c1 = cpu_total()  # 자식이 아직 살아 있을 때 잰다. 자식을 띄우는 비용까지 들어간다
    wall = time.perf_counter() - t0
    done.set()
    sampler.join()
    cpu = c1 - c0

    if not a.quiet:
        for name, n, s in results:
            print(f"  {name}  {n}줄  {s:.1f}s")
    lines = sum(n for _, n, _ in results)
    print(f"pool={a.pool} workers={a.workers} stage={a.stage} files={len(files)} {mb:.0f}MB {lines}줄 | "
          f"wall {wall:.1f}s | cpu {cpu:.1f}s | 코어평균 {cpu / wall:.2f} | "
          f"코어최대 {peak['cpu'] / 100:.2f} | RSS최대 {peak['rss'] / 1e6:.0f}MB | {mb / wall:.1f}MB/s")


if __name__ == "__main__":
    main()
