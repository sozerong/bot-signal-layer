"""원천 건강도 실측 (D13). raw에 받아 둔 시간 파일마다 건수와 필드 유무를 잰다.

사용: python measure_health.py [--root raw] [--hour 12]
- 받는 건 ingest.py가 한다. 이 스크립트는 raw를 읽기만 한다.
- 필드 유무 = 키가 있고 값이 null/빈 문자열이 아닌 비율 (그 이벤트 종류 안에서).
"""
import argparse
import gzip
import json
import sys
from collections import Counter
from pathlib import Path

import ingest

TYPES = ["WatchEvent", "ForkEvent", "CreateEvent", "ReleaseEvent"]
WATCH_FIELDS = ["actor.id", "actor.login", "repo.id", "repo.name", "payload.action"]


def get(e, path):
    for k in path.split("."):
        if not isinstance(e, dict):
            return None
        e = e.get(k)
    return e


def present(v):
    return v is not None and v != ""


def measure(path):
    types, watch, create_desc, payload_keys = Counter(), Counter(), 0, {t: Counter() for t in TYPES}
    total = 0
    # ponytail: 한 줄씩 json.loads, 단일 스레드. 이게 G1의 출발점이다. 여기서 최적화하지 않는다
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            total += 1
            e = json.loads(line)
            t = e.get("type")
            types[t] += 1
            if t in payload_keys:
                payload_keys[t].update((e.get("payload") or {}).keys())
            if t == "WatchEvent":
                watch.update(k for k in WATCH_FIELDS if present(get(e, k)))
            elif t == "CreateEvent":
                create_desc += present(get(e, "payload.description"))
    return total, types, watch, create_desc, payload_keys


def pct(n, d):
    return "-" if d == 0 else f"{100 * n / d:.0f}%"


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--root", default="raw")
    p.add_argument("--hour", default="12")
    a = p.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")

    print("| 시간 | 파일 MB | 전체 | Watch | Fork | Create | Release | "
          + " | ".join(f"Watch {k}" for k in WATCH_FIELDS) + " | Create description |")
    print("|" + "---|" * (8 + len(WATCH_FIELDS)))
    keys_by_hour = {}
    for d in sorted(Path(a.root).glob(f"dt=*/hour={a.hour}")):
        m = ingest.read_manifest(d)
        if not m or m["status"] != "ok":
            print(f"| {d.parent.name[3:]} | {m and m['status']} |")
            continue
        total, types, watch, cdesc, pkeys = measure(d / m["file"])
        assert total == m["lines"], f"{d}: 줄 수가 수집 기록과 다르다 {total} != {m['lines']}"
        w = types["WatchEvent"]
        print(f"| {m['hour']} | {m['bytes'] / 1e6:.1f} | {total} | "
              + " | ".join(str(types[t]) for t in TYPES) + " | "
              + " | ".join(pct(watch[k], w) for k in WATCH_FIELDS) + f" | {pct(cdesc, types['CreateEvent'])} |")
        keys_by_hour[m["hour"]] = {t: sorted(c) for t, c in pkeys.items()}

    # payload 모양이 바뀐 시점만 보여 준다
    print("\npayload 키가 바뀐 시점")
    prev = {}
    for hour, keys in keys_by_hour.items():
        for t, ks in keys.items():
            if prev.get(t) != ks:
                print(f"- {hour} {t}: {ks}")
                prev[t] = ks


if __name__ == "__main__":
    sys.exit(main())
