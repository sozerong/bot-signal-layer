"""응답 계약 자체 검사. 실행: python test_contract.py"""
import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator

HERE = Path(__file__).parent / "contract"
tool = json.loads((HERE / "check_popularity_signals.json").read_text(encoding="utf-8"))
examples = json.loads((HERE / "examples.json").read_text(encoding="utf-8"))
out = Draft202012Validator(tool["outputSchema"])
inp = Draft202012Validator(tool["inputSchema"])
Draft202012Validator.check_schema(tool["outputSchema"])


def bad(doc):
    return not out.is_valid(doc)


def main():
    # 모든 예시가 계약을 통과한다
    for name, doc in examples.items():
        if not name.startswith("_"):
            errs = [e.message for e in out.iter_errors(doc)]
            assert not errs, (name, errs)

    # 입력: owner/name 또는 GitHub 주소만
    for ok in ["owner/repo", "https://github.com/modelcontextprotocol/servers", "a-b/c.d_e/"]:
        assert inp.is_valid({"repo": ok}), ok
    for no in ["repo", "https://gitlab.com/a/b", "a/b/c", "-a/b", "", "a/b?x=1"]:
        assert not inp.is_valid({"repo": no}), no

    # 핵심 규칙: 데이터를 믿을 수 없으면 "문제 없음"이라고 말하지 않는다
    d = copy.deepcopy(examples["untrusted"])
    d["recommendation"] = "no_known_issue"
    assert bad(d)

    # suspicious는 실제로 걸린 신호가 하나는 있어야 한다
    d = copy.deepcopy(examples["suspicious"])
    for s in d["signals"]:
        s["fired"] = False
    assert bad(d)

    # no_signal인데 걸린 신호가 있으면 모순
    d = copy.deepcopy(examples["no_signal"])
    d["signals"][0]["fired"] = True
    assert bad(d)

    # 추적 대상 밖이면 신호도 판단도 없다
    d = copy.deepcopy(examples["not_tracked"])
    d["recommendation"] = "no_known_issue"
    assert bad(d)

    # 신호 이름은 열려 있다: 새 신호를 더해도 계약은 그대로 통과 (S2)
    d = copy.deepcopy(examples["suspicious"])
    d["signals"].append({**d["signals"][0], "name": "brand_new_signal"})
    assert not bad(d)

    # 모르는 필드는 거부한다 (필드 추가는 minor 버전을 올리는 계약 변경)
    d = copy.deepcopy(examples["suspicious"])
    d["malicious"] = True
    assert bad(d)

    # v1.1: 신호마다 정의, 판정마다 근거 표·열이 있어야 한다 (D16)
    d = copy.deepcopy(examples["suspicious"])
    del d["signals"][0]["definition"]
    assert bad(d)
    d = copy.deepcopy(examples["suspicious"])
    d["signals"][0]["definition"] = "x" * 201  # 정의는 한 문장
    assert bad(d)
    d = copy.deepcopy(examples["suspicious"])
    del d["provenance"]["sources"]
    assert bad(d)
    d = copy.deepcopy(examples["suspicious"])
    d["provenance"]["sources"][0]["columns"] = []  # 표만 있고 열이 없으면 근거가 아니다
    assert bad(d)
    print("ok")


if __name__ == "__main__":
    main()
