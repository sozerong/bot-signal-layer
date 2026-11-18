"""GH Archive 시간 파일 수집 (1단계).

- raw는 받은 그대로 둔다. 해석은 base에서 한다 (D2).
- 시간 파일 1개 = 파티션 1개 = 수집 기록(_manifest.json) 1개 (D1, D3).
- 이미 완료된 파티션은 다시 받지 않고 덮어쓰지도 않는다. 몇 번 돌려도 결과가 같다.

사용:
  python ingest.py fetch 2025-01-15T00 2025-01-15T23 [--root raw] [--workers 4]
  python ingest.py check 2025-01-15 [--root raw]
"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BASE_URL = "https://data.gharchive.org"
TIMEOUT = 60  # 소켓 하나의 recv가 이만큼 아무것도 못 받으면 끊는다. 조금씩 오는 연결은 이걸로 안 끊긴다
# 2026-11-12 사건: 연결 4개가 초당 약 4KB씩 50분 넘게 흘렀는데 TIMEOUT이 한 번도 안 걸렸다.
# 같은 시각 새 연결은 초당 131KB였다. 그래서 받은 양이 너무 적으면 끊고 새로 연결한다.
STALL_WINDOW, STALL_MIN_BYTES = 60, 1 << 20  # 60초 동안 1MB 미만(약 17KB/s)이면 멈춘 것으로 본다
USER_AGENT = "gh-ingest/0.1"  # 원천이 Python 기본 User-Agent를 403으로 막는다
MANIFEST = "_manifest.json"


def source_name(hour):
    # 원천은 시를 0으로 채우지 않는다: 2025-01-15-0.json.gz ... 2025-01-15-23.json.gz
    return f"{hour:%Y-%m-%d}-{hour.hour}.json.gz"


def partition(root, hour):
    return Path(root) / f"dt={hour:%Y-%m-%d}" / f"hour={hour:%H}"


def read_manifest(d):
    try:
        return json.loads((d / MANIFEST).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def write_manifest(d, m):
    tmp = d / (MANIFEST + ".part")
    tmp.write_text(json.dumps(m, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, d / MANIFEST)  # 기록은 원자적으로 바뀐다: 있거나, 없거나


def inspect(path):
    """(sha256, 줄 수). gzip이 깨졌으면 EOFError / BadGzipFile / zlib.error."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    with gzip.open(path, "rb") as f:
        lines = sum(1 for _ in f)  # 해석은 안 한다. 줄만 센다
    return sha.hexdigest(), lines


def copy_or_stall(src, dst, window=STALL_WINDOW, min_bytes=STALL_MIN_BYTES, clock=time.monotonic):
    """src를 dst로 복사한다. window초 동안 min_bytes 미만이면 TimeoutError(OSError라 재시도 대상)."""
    start, got = clock(), 0
    # read1: 지금 와 있는 만큼만 돌려준다. read(n)은 n바이트가 찰 때까지 기다려서 검사가 늦어진다
    for chunk in iter(lambda: src.read1(1 << 16), b""):
        dst.write(chunk)
        got += len(chunk)
        if clock() - start >= window:
            if got < min_bytes:
                raise TimeoutError(f"{window}초 동안 {got}바이트만 받았다")
            start, got = clock(), 0


def is_done(d, m):
    # ponytail: 크기만 비교한다. 체크섬 전체 대조는 check 명령이 한다
    return bool(m) and m["status"] == "ok" and (d / m["file"]).exists() \
        and (d / m["file"]).stat().st_size == m["bytes"]


def fetch_hour(root, hour, base_url=BASE_URL, retries=4):
    """'skip' | 'ok' | 'missing'. 재시도 후에도 실패하면 RuntimeError."""
    d = partition(root, hour)
    if is_done(d, read_manifest(d)):
        return "skip"
    d.mkdir(parents=True, exist_ok=True)
    name = source_name(hour)
    url = f"{base_url}/{name}"
    tmp = d / (name + ".part")
    base = {"hour": f"{hour:%Y-%m-%dT%H}", "source_url": url}
    err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r, open(tmp, "wb") as f:
                copy_or_stall(r, f)
            sha, lines = inspect(tmp)
            os.replace(tmp, d / name)  # 데이터 파일 먼저, 기록은 그 다음. 기록이 있으면 완료다
            write_manifest(d, {**base, "status": "ok", "file": name, "bytes": (d / name).stat().st_size,
                               "sha256": sha, "lines": lines, "attempts": attempt,
                               "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
            return "ok"
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return _missing(d, base)
            err = e
        except urllib.error.URLError as e:
            if isinstance(e.reason, FileNotFoundError):  # file:// 미러에서 파일이 없을 때
                return _missing(d, base)
            err = e
        except (OSError, EOFError, zlib.error) as e:  # 끊긴 다운로드, 타임아웃, 깨진 gzip
            err = e
        tmp.unlink(missing_ok=True)
        if attempt < retries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"{url}: {err!r}")


def _missing(d, base):
    # 원천에 파일이 없다. 다음 실행 때 다시 확인한다 (status가 ok가 아니므로)
    write_manifest(d, {**base, "status": "missing",
                       "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
    return "missing"


def hours(start, end):
    h, end = dt.datetime.strptime(start, "%Y-%m-%dT%H"), dt.datetime.strptime(end, "%Y-%m-%dT%H")
    if end < h:
        raise SystemExit("end가 start보다 앞입니다")
    while h <= end:
        yield h
        h += dt.timedelta(hours=1)


def check_day(root, day):
    """하루 24개 파티션 상태: ok | not_fetched | source_missing | corrupt."""
    out = {}
    for h in hours(f"{day}T00", f"{day}T23"):
        d = partition(root, h)
        m = read_manifest(d)
        if m is None:
            out[h.hour] = ("not_fetched", None)
        elif m["status"] == "missing":
            out[h.hour] = ("source_missing", None)
        else:
            try:
                sha, lines = inspect(d / m["file"])
                ok = sha == m["sha256"] and lines == m["lines"]
            except (OSError, EOFError, zlib.error):
                ok = False
            out[h.hour] = ("ok", m["lines"]) if ok else ("corrupt", None)
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("start")
    f.add_argument("end")
    f.add_argument("--workers", type=int, default=4)
    c = sub.add_parser("check")
    c.add_argument("day")
    for s in (f, c):
        s.add_argument("--root", default="raw")
    a = p.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")  # Windows 콘솔 기본 cp949에서 한글이 깨진다

    if a.cmd == "fetch":
        todo = list(hours(a.start, a.end))

        def run(h):
            try:
                return h, fetch_hour(a.root, h)
            except RuntimeError as e:
                return h, f"failed {e}"

        with ThreadPoolExecutor(a.workers) as ex:
            results = list(ex.map(run, todo))
        for h, r in results:
            print(f"{h:%Y-%m-%dT%H}  {r}")
        failed = sum(r.startswith("failed") for _, r in results)
        print(f"\n{len(todo)}개 중 실패 {failed}개")
        return 1 if failed else 0

    status = check_day(a.root, a.day)
    for hour, (s, lines) in status.items():
        print(f"{a.day}T{hour:02d}  {s}" + (f"  {lines}줄" if lines is not None else ""))
    bad = [h for h, (s, _) in status.items() if s != "ok"]
    total = sum(lines for s, lines in status.values() if s == "ok")
    print(f"\nok {24 - len(bad)}/24, 총 {total}줄" + (f", 문제 있는 시간: {bad}" if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
