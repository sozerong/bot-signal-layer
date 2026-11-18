"""ingest.py 자체 검사. 네트워크 없이 file:// 가짜 원천으로 돈다. 실행: python test_ingest.py"""
import datetime as dt
import gzip
import io
import tempfile
from pathlib import Path

import ingest


def main():
    with tempfile.TemporaryDirectory() as t:
        src, raw = Path(t, "src"), Path(t, "raw")
        src.mkdir()
        (src / "2025-01-15-0.json.gz").write_bytes(gzip.compress(b'{"a":1}\n{"a":2}\n{"a":3}\n'))
        (src / "2025-01-15-1.json.gz").write_bytes(gzip.compress(b'{"a":1}\n{"a":2}\n'))
        (src / "2025-01-15-2.json.gz").write_bytes(gzip.compress(b'{"a":1}\n')[:-6])  # 끊긴 gzip
        base = src.as_uri()
        h = lambda n: dt.datetime(2025, 1, 15, n)

        # 정상: 시 0 채움 없는 원천 이름, 0 채운 파티션, 줄 수 기록
        assert ingest.fetch_hour(raw, h(0), base) == "ok"
        d0 = raw / "dt=2025-01-15" / "hour=00"
        m = ingest.read_manifest(d0)
        assert (d0 / "2025-01-15-0.json.gz").exists() and m["lines"] == 3 and m["status"] == "ok"

        # 멱등: 다시 돌려도 받지 않고 기록도 그대로
        before = (d0 / ingest.MANIFEST).read_text(encoding="utf-8")
        assert ingest.fetch_hour(raw, h(0), base) == "skip"
        assert (d0 / ingest.MANIFEST).read_text(encoding="utf-8") == before

        # 원천에 없는 시간: missing 기록, 다음에 다시 확인 대상
        assert ingest.fetch_hour(raw, h(3), base) == "missing"
        assert ingest.read_manifest(raw / "dt=2025-01-15" / "hour=03")["status"] == "missing"

        # 깨진 gzip: 실패하고 파일·기록·.part 아무것도 안 남는다
        try:
            ingest.fetch_hour(raw, h(2), base, retries=1)
            raise AssertionError("깨진 gzip이 통과했다")
        except RuntimeError:
            pass
        assert not any((raw / "dt=2025-01-15" / "hour=02").iterdir())

        # check: 상태 구분, 로컬 파일 변조 탐지
        assert ingest.fetch_hour(raw, h(1), base) == "ok"
        s = ingest.check_day(raw, "2025-01-15")
        assert s[0] == ("ok", 3) and s[1] == ("ok", 2)
        assert s[2][0] == "not_fetched" and s[3][0] == "source_missing" and s[23][0] == "not_fetched"
        (d0 / "2025-01-15-0.json.gz").write_bytes(gzip.compress(b'{"a":9}\n{"a":2}\n{"a":3}\n'))
        assert ingest.check_day(raw, "2025-01-15")[0][0] == "corrupt"

    # 조금씩 오는 연결: 청크 하나에 10초, 10바이트. 30초 창에 100바이트 미만이면 끊는다
    class Slow(io.BytesIO):
        def read1(self, n=-1):
            clock.t += 10
            return super().read1(10)
    clock = lambda: clock.t
    clock.t = 0
    try:
        ingest.copy_or_stall(Slow(b"x" * 1000), io.BytesIO(), window=30, min_bytes=100, clock=clock)
        raise AssertionError("멈춘 연결이 통과했다")
    except TimeoutError:
        pass
    out = io.BytesIO()  # 빠른 연결은 그대로 끝까지 복사된다
    ingest.copy_or_stall(Slow(b"x" * 1000), out, window=30, min_bytes=20, clock=clock)
    assert out.getvalue() == b"x" * 1000
    print("ok")


if __name__ == "__main__":
    main()
