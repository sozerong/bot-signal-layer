"""참조 목록(크롤러 UA, AI 봇) 받기와 Silver 적재 (지시서 handoff_v5_wp1.md 1-3).

  fetch : sources/registry.yml의 ref.url을 raw/ref/<이름>/dt=<오늘>/에 받은 그대로 두고 _manifest.json(주소, 크기, sha256)을 쓴다.
          이미 있으면 받지 않는다 (같은 날 다시 돌려도 같다). 바깥 요청이다.
  load  : 그 적재일 파일을 읽어 bot.silver.ref_ua_patterns의 (list, load_date) 파티션에 쓴다. 다른 적재일은 그대로 남는다.

사용 (bot-job 컨테이너 안):
  python -m spark_jobs.load_ref fetch
  python -m spark_jobs.load_ref load --day 2026-11-22
"""
import argparse
import datetime as dt
import hashlib
import json
import re
import sys
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FILES = {"crawler_ua": "crawler-user-agents.json", "ai_robots": "robots.json"}


def refs():
    reg = yaml.safe_load((ROOT / "sources" / "registry.yml").read_text(encoding="utf-8"))
    return {k: v["ref"] for k, v in reg.items() if "ref" in v}


def fetch(day):
    for name, ref in refs().items():
        d = ROOT / "raw" / "ref" / name / f"dt={day}"
        f = d / FILES[name]
        if not f.exists():
            d.mkdir(parents=True, exist_ok=True)
            req = urllib.request.Request(ref["url"], headers={"User-Agent": "bot-porj-validation"})
            f.write_bytes(urllib.request.urlopen(req, timeout=60).read())
        if not (d / "_manifest.json").exists():
            b = f.read_bytes()
            (d / "_manifest.json").write_text(json.dumps({
                "list": name, "url": ref["url"], "license": ref["license"], "file": f.name, "bytes": len(b),
                "sha256": hashlib.sha256(b).hexdigest(), "load_date": day, "status": "ok"}, indent=1), encoding="utf-8")
        print(f"{name}: {f.relative_to(ROOT)}")


def token(name):
    """AI 봇 이름(정규식이 아닌 글자)을 UA 안의 낱말 하나로만 걸리게. 'Code', 'Dot' 같은 짧은 이름이 다른 낱말 속에 걸리지 않게.
    ponytail: 앞뒤가 글자·숫자가 아니면 낱말로 본다. UA 표기가 'Name/1.0', '(Name;', 'Name ' 모두 걸린다"""
    return rf"(?i)(?<![A-Za-z0-9]){re.escape(name)}(?![A-Za-z0-9])"


def rows(day):
    base = ROOT / "raw" / "ref"
    c = json.loads((base / "crawler_ua" / f"dt={day}" / FILES["crawler_ua"]).read_text(encoding="utf-8"))
    out = [(e["pattern"], "crawler_ua", e["pattern"], None, ",".join(e.get("tags", [])) or None) for e in c]
    a = json.loads((base / "ai_robots" / f"dt={day}" / FILES["ai_robots"]).read_text(encoding="utf-8"))
    out += [(token(k), "ai_robots", k, v.get("operator"), v.get("function")) for k, v in a.items() if not k.startswith("__")]
    return out


def load(day):
    from pyspark.sql import functions as F

    from spark_jobs.session import spark_session

    spark = spark_session("load_ref", {"spark.ui.enabled": "false"})
    t = "bot.silver.ref_ua_patterns"
    df = (spark.createDataFrame(rows(day), "pattern STRING, list STRING, name STRING, operator STRING, purpose STRING")
          .withColumn("load_date", F.to_date(F.lit(day))))
    df.writeTo(t).overwritePartitions()
    for r in spark.sql(f"SELECT list, count(*) n FROM {t} WHERE load_date = DATE'{day}' GROUP BY 1 ORDER BY 1").collect():
        print(f"{t} {day} {r.list}: {r.n}행")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["fetch", "load"])
    ap.add_argument("--day", default=dt.date.today().isoformat())
    a = ap.parse_args()
    sys.exit(fetch(a.day) if a.cmd == "fetch" else load(a.day))
