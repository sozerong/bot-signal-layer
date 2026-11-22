"""Gold 빌드 (implementation_design.md 5절 dbt_build). rules/registry.yml -> seeds/evidence_current.csv를 만든 뒤 dbt build.

시드를 매번 registry에서 만드는 이유: 지금 쓰는 규칙 버전을 한 곳(registry.yml)에서만 고치게.

사용 (bot-job 컨테이너 안): python -m warehouse.dbt_build [dbt build 인자...]   예: --select evaluation+
범위를 좁혀도(--select) 시드 evidence_current와 그 아래는 늘 함께 고른다. 규칙 버전 변경은 SQL이 아니라 시드를 바꾸는 일이라
`+`(아래쪽만) 선택이 시드를 빠뜨리면 Gold가 옛 버전을 읽는다 (failure_analysis.md F-10).
컴파일만 (리니지용): python -m warehouse.dbt_build compile

규칙 묶음 id(ruleset_id, 결정 24, 결정 30): 판정을 정하는 모든 것을 해시한 앞 12자리. dbt 변수 ruleset_id로 넘긴다.
  - 시드 evidence_current.csv와 policy_family.csv의 행 (규칙 버전과 정책)
  - 지금 쓰는 근거마다 sql/evidence/<이름>/<버전>.sql 내용
  - 근거가 읽는 피처 SQL: features_account.sql, registry의 features_repo 버전 파일
  - 근거를 판정으로 묶는 모델: stg_evidence, evidence_flags, fake_star_repo, entity_verdict
처음 정의(시드 두 개만)는 근거 SQL을 버전을 올리지 않고 고치면 같은 id로 공개 숫자를 덮었다 (docs/review_logic.md R-1).
SQL은 주석, 들여쓰기, 줄 끝 공백, 빈 줄, 줄바꿈 종류를 지운 뒤 해시한다. 주석만 고쳐서 id가 바뀌면 공개 줄이 쓸데없이 늘어난다.
시드는 행을 정렬해 해시한다 (Windows에서 받은 레포와 클라우드가 같은 id가 되게).
"""
import csv
import hashlib
import json
import re
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
        w.writerow(["evidence_id", "rule_version", "family", "entity"])
        for k, v in sorted(reg.items()):
            if "family" not in v:  # 피처 버전 항목(features_repo)은 근거가 아니다
                continue
            ent = v["entity"] if isinstance(v["entity"], str) else "|".join(v["entity"])  # 시험 every_evidence_ran이 원천을 가른다
            w.writerow([k, v["current"], v["family"], ent])


def is_gh(v):
    """registry 근거 항목이 GitHub 대상인가. 웹 근거(web_client)는 GH 날짜에 돌면 0행이라 Iceberg가 커밋을 건너뛰고
    쓰기 확인(run_sql.run)이 "다른 커밋에 덮였다"로 실패한다 (F-22). GH 날짜를 도는 곳(시험, dags/bot_build.py)은 이것으로 거른다 (결정 26)"""
    return "family" in v and all(e.startswith("gh_") for e in ([v["entity"]] if isinstance(v["entity"], str) else v["entity"]))


# 판정을 정하는 모델 (docs/review_logic.md R-1). 판정 경로에 모델이 늘면 여기에 더한다
VERDICT_MODELS = ("staging/stg_evidence.sql", "gold/evidence_flags.sql", "gold/fake_star_repo.sql", "gold/entity_verdict.sql")


def _sql_text(path):
    """뜻이 같은 SQL은 같은 글자가 되게: 주석, 들여쓰기, 줄 끝 공백, 빈 줄, 줄바꿈 종류를 지운다.
    ponytail: 문자열 안의 ' -- '도 주석으로 지운다. 지금 판정 SQL에는 없고, 생기면 그 부분만 같은 id가 된다"""
    text = re.sub(r"\{#.*?#\}", "", path.read_text(encoding="utf-8"), flags=re.S)  # dbt(Jinja) 주석
    lines = (re.sub(r"(^|\s)--(\s.*|)$", "", ln).strip() for ln in text.splitlines())
    return "\n".join(ln for ln in lines if ln)


def ruleset_parts(root=ROOT):
    """(이름, 내용) 목록. ruleset_id가 해시하는 입력을 그대로 보여 준다 (시험과 기록용)."""
    dbt, parts = root / "warehouse" / "dbt", []
    for name in ("evidence_current.csv", "policy_family.csv"):
        rows = sorted(r.strip() for r in (dbt / "seeds" / name).read_text(encoding="utf-8").splitlines()[1:] if r.strip())
        parts.append((name, "\n".join(rows)))
    reg = yaml.safe_load((root / "rules" / "registry.yml").read_text(encoding="utf-8"))
    for k, v in sorted(reg.items()):
        if "family" in v:
            parts.append((f"evidence/{k}/{v['current']}", _sql_text(root / "sql" / "evidence" / k / f"{v['current']}.sql")))
    fr = reg["features_repo"]["current"]
    feats = ["features_account.sql", "features_repo.sql" if fr == "f1" else f"features_repo_{fr}.sql"]
    parts += [(f"silver/{f}", _sql_text(root / "sql" / "silver" / f)) for f in feats]
    parts += [(f"models/{m}", _sql_text(dbt / "models" / m)) for m in VERDICT_MODELS]
    return parts


def ruleset_id(root=ROOT):
    h = hashlib.sha256()
    for name, body in ruleset_parts(root):
        h.update(f"{name}\n{body}\n".encode("utf-8"))
    return h.hexdigest()[:12]


if __name__ == "__main__":
    write_evidence_current()
    args = sys.argv[1:] + ["--vars", json.dumps({"ruleset_id": ruleset_id()})]
    cmd = args.pop(0) if args[:1] == ["compile"] else "build"
    for flag in ("--select", "-s"):
        if flag in args:
            args.insert(args.index(flag) + 2, "evidence_current+")
    sys.exit(subprocess.call(["dbt", cmd, "--project-dir", str(DBT), "--profiles-dir", str(DBT), *args], cwd=DBT))
