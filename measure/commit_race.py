"""병렬 커밋이 사라지는지 재는 실험 (failure_analysis.md F-7, 지시서 H2-1 권장 실험).

프로세스(JVM) 여러 개가 같은 Iceberg 표(Hadoop 카탈로그)에 동시에 append 커밋을 하고, 끝에 몇 개가 남았는지 센다.
커밋마다 스냅샷 속성 bot.wid에 표시를 남기고, 커밋 직후 표를 다시 읽어 자기 스냅샷이 보이는지도 본다(즉시 확인).

사용 (bot-job 컨테이너 안, BOT_WAREHOUSE로 시험할 폴더를 준다):
  python -m measure.commit_race --workers 5 --commits 20
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

T = "bot.race.t"


def session():
    from spark_jobs.session import spark_session
    return spark_session("commit_race", {"spark.ui.enabled": "false", "spark.master": "local[1]"})


def worker(wid, n, ready):
    s = session()
    Path(ready, f"{wid}").touch()
    while len(list(Path(ready).iterdir())) < int(Path(ready).name.split("-")[1]):  # 모두 뜰 때까지 기다린다
        time.sleep(0.2)
    ok, failed, seen_now = [], 0, 0
    for i in range(n):
        mark = f"{wid}-{i}"
        try:
            s.createDataFrame([(wid, i)], "w INT, i INT").writeTo(T).option("snapshot-property.bot.wid", mark).append()
        except Exception:  # 재시도(기본 4번)를 다 쓰고 실패를 알린 커밋. 시끄러운 실패라 조용한 손실과 따로 센다
            failed += 1
            continue
        ok.append(i)
        s.sql(f"REFRESH TABLE {T}")
        seen_now += s.sql(f"SELECT count(*) FROM {T}.snapshots WHERE summary['bot.wid'] = '{mark}'").first()[0]
    Path(ready).parent.joinpath(f"ok-{Path(ready).name}-{wid}").write_text(" ".join(map(str, ok)))
    print(f"WORKER {wid} ok={len(ok)} failed_loudly={failed} seen_right_after={seen_now}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--commits", type=int, default=20)
    ap.add_argument("--worker", type=int)
    ap.add_argument("--ready")
    a = ap.parse_args()
    if a.worker is not None:
        return worker(a.worker, a.commits, a.ready)
    s = session()
    s.sql("CREATE NAMESPACE IF NOT EXISTS bot.race")
    s.sql(f"DROP TABLE IF EXISTS {T}")
    s.sql(f"CREATE TABLE {T} (w INT, i INT) USING iceberg")
    ready = Path("/tmp", f"ready-{a.workers}-{time.time_ns()}")
    ready.mkdir()
    t0 = time.time()
    ps = [subprocess.Popen([sys.executable, "-m", "measure.commit_race", "--worker", str(w), "--commits", str(a.commits),
                            "--ready", str(ready)]) for w in range(a.workers)]
    codes = [p.wait() for p in ps]
    s.sql(f"REFRESH TABLE {T}")
    rows = s.sql(f"SELECT count(*) FROM {T}").first()[0]
    snaps = s.sql(f"SELECT count(*) FROM {T}.snapshots").first()[0]
    ok = {(w, int(i)) for w in range(a.workers)
          for i in Path("/tmp", f"ok-{ready.name}-{w}").read_text().split()}
    have = {(r.w, r.i) for r in s.sql(f"SELECT w, i FROM {T}").collect()}
    print(f"RESULT workers={a.workers} tried={a.workers * a.commits} ok={len(ok)} rows={rows} snapshots={snaps} "
          f"silently_lost={len(ok - have)} exit_codes={codes} seconds={time.time() - t0:.0f}")


if __name__ == "__main__":
    main()
