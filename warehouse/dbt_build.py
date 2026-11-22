"""Gold 빌드 (implementation_design.md 5절 dbt_build). rules/registry.yml -> seeds/evidence_current.csv를 만든 뒤 dbt build.

시드를 매번 registry에서 만드는 이유: 지금 쓰는 규칙 버전을 한 곳(registry.yml)에서만 고치게.

사용 (bot-job 컨테이너 안): python -m warehouse.dbt_build [dbt build 인자...]   예: --select evaluation+
범위를 좁혀도(--select) 시드 evidence_current와 그 아래는 늘 함께 고른다. 규칙 버전 변경은 SQL이 아니라 시드를 바꾸는 일이라
`+`(아래쪽만) 선택이 시드를 빠뜨리면 Gold가 옛 버전을 읽는다 (failure_analysis.md F-10).
컴파일만 (리니지용): python -m warehouse.dbt_build compile

규칙 묶음 id(ruleset_id, 결정 24): 시드 evidence_current.csv와 policy_family.csv의 행을 정렬해 해시한 앞 12자리.
Gold가 판정에 쓰는 규칙 버전과 정책이 이 두 파일에서 정해지므로, 둘이 같으면 같은 id다. dbt 변수 ruleset_id로 넘긴다.
줄바꿈(CRLF/LF)과 행 순서에 상관없게 행 단위로 정렬해 해시한다 (Windows에서 받은 레포와 클라우드가 같은 id가 되게).
"""
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DBT = ROOT / "warehouse" / "dbt"


def write_evidence_current():
    reg = yaml.safe_load((ROOT / "rules" / "registry.yml").read_text(encoding="utf-8"))
    with open(DBT / "seeds" / "evidence_current.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["evidence_id", "rule_version", "family"])
        for k, v in sorted(reg.items()):
            if "family" not in v:  # 피처 버전 항목(features_repo)은 근거가 아니다
                continue
            w.writerow([k, v["current"], v["family"]])


def ruleset_id():
    h = hashlib.sha256()
    for name in ("evidence_current.csv", "policy_family.csv"):
        rows = sorted(r.strip() for r in (DBT / "seeds" / name).read_text(encoding="utf-8").splitlines()[1:] if r.strip())
        h.update(("\n".join([name, *rows]) + "\n").encode("utf-8"))
    return h.hexdigest()[:12]


if __name__ == "__main__":
    write_evidence_current()
    args = sys.argv[1:] + ["--vars", json.dumps({"ruleset_id": ruleset_id()})]
    cmd = args.pop(0) if args[:1] == ["compile"] else "build"
    for flag in ("--select", "-s"):
        if flag in args:
            args.insert(args.index(flag) + 2, "evidence_current+")
    sys.exit(subprocess.call(["dbt", cmd, "--project-dir", str(DBT), "--profiles-dir", str(DBT), *args], cwd=DBT))
