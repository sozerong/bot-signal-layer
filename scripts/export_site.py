"""Gold(DuckDB)에서 화면용 Parquet 묶음을 뽑는다 (docs/site_design.md 3절).

GitHub Pages 화면(site/index.html)은 브라우저 안의 DuckDB-Wasm으로 이 파일들을 읽는다.
분석가가 레포나 Docker 없이 요구사항을 SQL로 시험해 볼 수 있게, 계정 단위 원본 대신 작게 줄인 표만 내보낸다
(GitHub Pages 사이트 1GB, 파일 100MB 한도 안. 실제 크기는 meta.json에 남긴다).

사용 (bot-job 컨테이너 안, dbt build 뒤):
  python -m scripts.export_site                      # warehouse_local/gold.duckdb -> site/data/
  python -m scripts.export_site --db other.duckdb --out /tmp/site_data
  python -m scripts.export_site --with-entities       # 대상 단위 표까지 -> site/data_local/ (git에 안 올림, 로컬 확인용)

기본값은 집계만 내보낸다. 계정·저장소 이름이 붙은 봇 판정(대상 단위 표)은 공개 사이트에 싣지 않는다
(GH Archive 원본의 제3자 권리, 대상 단위 봇 판정 비공개 원칙. docs/site_design.md 3절).
"""
import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

# 파일 이름 -> (한 행의 뜻, SQL). 표 이름은 dbt가 main 스키마에 만든 이름 그대로다.
EXPORTS = {
    "policy_compare": (
        "원천 x 날짜 x 지표 x 필터 강도 x 규칙 버전. 지표 값, 원본 대비 변화율, 약하게 대비 추가 변화율, MDE",
        "select * from main.policy_compare",
    ),
    "ruleset_change_log": (
        "원천 x 날짜 x 지표 x 필터 강도 x 비교한 버전. 공개 버전 대비 다른 버전의 값 차이",
        "select * from main.ruleset_change_log",
    ),
    "ruleset_release": ("공개한 규칙 버전 하나", "select * from main.ruleset_release"),
    "metric_policy": ("지표 하나와 그 추천 강도, 이유", "select * from main.metric_policy where metric in (select metric from main.policy_compare where source = 'gh')"),  # 원천 칸이 없어 GH 지표로 거른다
    "metric_policy_proposal": (
        "원천 x 지표. 규칙이 제안한 추천 강도와 자연 변동, 강도별 변화",
        "select * from main.metric_policy_proposal",
    ),
    "daily_action": (
        "원천 x 날짜 x 이벤트 종류. 강도별 이벤트 수와 계정 수. 그날 그 종류를 낸 계정이 10개 미만이면 싣지 않아요",
        # 계정 몇 개짜리 칸은 정책 사이 차이로 누가 빠졌는지 좁혀진다 (docs/review_logic.md R-5)
        """
        select source, event_date, action,
               count(*) as events_none,
               count(*) filter (where is_real_lenient)  as events_lenient,
               count(*) filter (where is_real_balanced) as events_balanced,
               count(*) filter (where is_real_strict)   as events_strict,
               count(distinct actor_key) as accounts_none,
               count(distinct case when is_real_lenient  then actor_key end) as accounts_lenient,
               count(distinct case when is_real_balanced then actor_key end) as accounts_balanced,
               count(distinct case when is_real_strict   then actor_key end) as accounts_strict
        from main.real_user_events
        group by all
        having count(distinct actor_key) >= 10
        """,
    ),
    "mde_cap": (
        "원천 x 날짜 x 필터 강도 x 자르기 백분위(90~100, 0.5 간격). 계정 수, 자른 값, 계정당 이벤트 평균과 분산. MDE 계산기에 써요",
        # 예전 acct_dist(계정당 이벤트 n별 계정 수)는 꼬리의 계정 하나짜리 칸이 none에는 있고 strict에는 없어서
        # 공개 데이터로 그 계정을 찾을 수 있었다 (docs/review_logic.md R-5). 백분위마다 모멘트만 내보낸다
        """
        with a as (
          select source, event_date, actor_key, count(*) as n,
                 bool_and(is_real_lenient) as l, bool_and(is_real_balanced) as b, bool_and(is_real_strict) as s
          from main.real_user_events group by source, event_date, actor_key
        ),
        d as (
          select source, event_date, 'none' as treatment, n, count(*) as accounts from a group by all
          union all select source, event_date, 'lenient',  n, count(*) from a where l group by all
          union all select source, event_date, 'balanced', n, count(*) from a where b group by all
          union all select source, event_date, 'strict',   n, count(*) from a where s group by all
        ),
        c as (
          select *, sum(accounts) over (partition by source, event_date, treatment order by n) as cum,
                    sum(accounts) over (partition by source, event_date, treatment) as tot
          from d
        ),
        qs as (select r / 2.0 as q from range(180, 201) t(r)),
        -- 가장 가까운 순위 방식: 누적 계정 수가 q% 이상이 되는 첫 n (policy_compare의 자르기와 같다)
        cap as (
          select source, event_date, treatment, q, min(n) as cap_value
          from c, qs where cum >= ceil(q / 100 * tot) group by all
        )
        select d.source, d.event_date, d.treatment, cap.q,
               sum(d.accounts) as n,
               case when cap.q < 100 then cap.cap_value end as cap_value,
               sum(least(d.n, cap.cap_value) * d.accounts)::double / sum(d.accounts) as mean,
               ((sum(pow(least(d.n, cap.cap_value), 2) * d.accounts) - pow(sum(least(d.n, cap.cap_value) * d.accounts), 2) / sum(d.accounts))
                 / (sum(d.accounts) - 1))::double as variance
        from d join cap using (source, event_date, treatment)
        group by all
        """,
    ),
    "suspects": (
        "원천 x 대상 x 날짜. 어느 한 강도에서라도 실유저가 아닌 대상과 강도별 제외 여부, 이름, 이벤트 수",
        """
        with st as (
          select source, entity_type, entity_key, event_date,
                 bool_or(policy = 'lenient'  and not is_real) as excluded_lenient,
                 bool_or(policy = 'balanced' and not is_real) as excluded_balanced,
                 bool_or(policy = 'strict'   and not is_real) as excluded_strict,
                 max(case when policy = 'strict' then excluded_by end) as excluded_by_strict
          from main.actor_policy_status
          group by all
          having bool_or(not is_real)
        ),
        acct as (
          select actor_key, event_date, max(name) as name, sum(cnt) as n_events, arg_max(action, cnt) as top_action
          from (select actor_key, event_date, action, max(actor_name) as name, count(*) as cnt
                from main.real_user_events group by actor_key, event_date, action)
          group by actor_key, event_date
        ),
        repo as (select repo_key, event_date, max(repo_name) as name, max(stars_all) as n_events from main.impact group by all)
        select st.*, coalesce(a.name, r.name) as name, coalesce(a.n_events, r.n_events) as n_events,
               case when st.entity_type = 'gh_repo' then 'WatchEvent' else a.top_action end as top_action
        from st
        left join acct a on st.entity_type = 'gh_account' and a.actor_key = st.entity_key and a.event_date = st.event_date
        left join repo r on st.entity_type = 'gh_repo' and r.repo_key = st.entity_key and r.event_date = st.event_date
        """,
    ),
    "why_excluded": (
        "원천 x 대상 x 날짜 x 필터 강도 x 근거. 실유저가 아닌 대상에 걸린 근거, 규칙 버전, 점수, 판정 시각, SQL 지문",
        "select * exclude (metrics), cast(metrics as varchar) as metrics from main.why_excluded",
    ),
    "impact_top": (
        "저장소 x 날짜 (그날 스타 상위 200). 강도별 스타 수와 순위",
        """
        select *,
               rank() over (partition by event_date order by stars_lenient desc) as rank_lenient,
               rank() over (partition by event_date order by stars_strict desc)  as rank_strict
        from main.impact qualify rank_all <= 200
        """,
    ),
    "evaluation": (
        "근거 x 버전 x 정답 출처 x 판정 구간(전체는 비어 있음). 정밀도와 대조군 양성률",
        "select * from main.evaluation where evidence_id not in (select evidence_id from main.evidence_current where entity = 'web_client')",  # 원천 칸이 없어 웹 근거를 뺀다 (조합 fake_star_repo는 시드에 없어 남긴다)
    ),
}


# 대상(계정·저장소) 이름과 봇 판정이 한 행에 붙는 표. --with-entities일 때만 내보낸다
ENTITY_TABLES = {"suspects", "why_excluded", "impact_top"}
# 집계 표에 이런 컬럼이 섞이면 공개 묶음에서 멈춘다
ENTITY_COLUMNS = {"name", "actor_name", "repo_name", "actor_key", "repo_key", "entity_key", "actor_id", "repo_id"}


def git_commit():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True,
                              check=True).stdout.strip()
    except Exception:  # noqa: BLE001  ponytail: git이 없는 곳(CI 캐시 등)에서도 내보내기는 돈다
        return None


# 데이터 상품 카드 (handoff_delivery.md 1절, D5, 결정 37). 화면 맨 위 카드가 meta.json의 이 값을 그대로 쓴다. 비면 내보내기가 실패한다
PRODUCT = {
    "name": "실유저 데이터 실험실",
    "intro": "GitHub 공개 이벤트 일주일 치에서 봇을 걸러 낸 숫자예요. SQL로 뽑아서 엑셀이나 구글 시트로 가져갈 수 있어요.",
    "owner": "sozerong (GitHub)",
    "contact": "https://github.com/sozerong/bot-signal-layer/issues/new?template=requirement.yml",
    "refresh": "데이터 범위는 {start} ~ {end}로 고정되어 있고 정기 갱신은 없어요. 숫자는 규칙 버전을 새로 공개할 때만 바뀌어요.",
    "change_policy": [
        "숫자가 같은 내부 변경은 같음 증명을 붙여 공개해요.",
        "숫자가 바뀌는 공개는 바뀐 지표와 폭을 먼저 보여 준 뒤 공개해요.",
        "컬럼을 지우거나 이름을 바꾸지 않고 새 컬럼을 더해요. 꼭 지워야 하면 한 버전 동안 옛 컬럼을 남기고 미리 알려요.",
    ],
}


def descriptions():
    """gold.yml 컬럼 사전 -> {표: {"description": 표 설명, "columns": {컬럼: {description, ko, unit, removed}}}} (결정 38).
    dbt 모델(models), 시드(seeds), 내보내기가 만드는 표(x-site-tables)를 한 파일에서 읽는다."""
    import yaml
    doc = yaml.safe_load((ROOT / "warehouse/dbt/models/gold/gold.yml").read_text(encoding="utf-8"))
    out = {}
    for m in doc.get("models", []) + doc.get("seeds", []) + doc.get("x-site-tables", []):
        out[m["name"]] = {"description": m.get("description"),
                          "columns": {c["name"]: {"description": c.get("description"), **(c.get("meta") or {})}
                                      for c in m.get("columns", []) or []}}
    return out


def missing_descriptions(tables, desc):
    """공개 표 컬럼 중 설명, 한글 이름, 단위가 하나라도 빈 것 (D1). 빈 목록이면 통과."""
    bad = []
    for t, v in tables.items():
        cols = desc.get(t, {}).get("columns", {})
        for c in v["columns"]:
            d = cols.get(c["name"], {})
            bad += [f"{t}.{c['name']}"] if not all(d.get(k) for k in ("description", "ko", "unit")) else []
    return bad


def removed_columns(prev_tables, tables, desc):
    """직전 meta.json에 있었는데 이번에 없는 컬럼 중, 컬럼 사전에 removed(사유)가 없는 것 (D6, 변경 정책 3번). 빈 목록이면 통과."""
    bad = []
    for t, v in prev_tables.items():
        now = {c["name"] for c in tables.get(t, {}).get("columns", [])}
        cols = desc.get(t, {}).get("columns", {})
        bad += [f"{t}.{c['name']}" for c in v["columns"] if c["name"] not in now and not cols.get(c["name"], {}).get("removed")]
    return bad


def example(con, path, col):
    """데이터에서 뽑은 예시값 하나 (모두 NULL이면 None)."""
    v = con.execute(f'select "{col}" from \'{path}\' where "{col}" is not null limit 1').fetchall()
    if not v:
        return None
    v = v[0][0]
    return round(v, 4) if isinstance(v, float) else str(v) if not isinstance(v, (int, bool)) else v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "warehouse_local/gold.duckdb"))
    ap.add_argument("--out", default=None, help="기본: site/data (집계만), --with-entities면 site/data_local")
    ap.add_argument("--with-entities", action="store_true", help="대상 단위 표까지. 공개 사이트에 올리지 말 것")
    ap.add_argument("--allow-unreleased", action="store_true", help="빌드 묶음이 공개 묶음과 달라도 내보낸다(로컬 확인용)")
    args = ap.parse_args()
    out = Path(args.out or ROOT / ("site/data_local" if args.with_entities else "site/data"))
    out.mkdir(parents=True, exist_ok=True)
    (out / "csv").mkdir(exist_ok=True)

    # 읽기 전용으로 연다: 빌드 중이면 잠금 때문에 실패한다 (DuckDB는 쓰는 프로세스 하나 또는 읽기만 하는 여러 프로세스)
    con = duckdb.connect(args.db, read_only=True)
    # 화면 한 장에 공개 묶음 숫자(policy_compare_released)와 빌드 묶음 숫자(나머지 표)가 섞이지 않게 (docs/review_logic.md R-14)
    released = con.execute("select ruleset_id from main.ruleset_release order by seq desc limit 1").fetchone()[0]
    built = [r[0] for r in con.execute("select distinct ruleset_id from main.actor_policy_status").fetchall()]
    if built != [released] and not args.allow_unreleased:
        raise SystemExit(f"빌드 묶음 {built}이 공개 묶음 {released}와 다르다. 공개 줄을 더하거나 공개 묶음으로 다시 빌드한 뒤 내보낸다 (--allow-unreleased는 로컬 확인용)")
    desc = descriptions()
    prev = out / "meta.json"
    prev_tables = json.loads(prev.read_text(encoding="utf-8")).get("tables", {}) if prev.exists() else {}
    tables = {}
    for name, (grain, sql) in EXPORTS.items():
        path = out / f"{name}.parquet"
        csv_path = out / "csv" / f"{name}.csv"
        if name in ENTITY_TABLES and not args.with_entities:
            path.unlink(missing_ok=True)  # 예전에 실수로 남은 파일도 지운다
            csv_path.unlink(missing_ok=True)
            continue
        # 화면은 GitHub 원천만 그린다 (날짜 고르기와 차트가 원천을 가르지 않는다). 웹 원천(auth_lib, 결정 26)은 Gold와 CSV 묶음 밖에서 본다
        # ponytail: 원천 칸이 있는 표만 거른다. 화면에 원천 고르기를 붙이면 이 줄을 지운다
        has_source = "source" in {c[0] for c in con.execute(f"describe select * from ({sql})").fetchall()}
        sql = f"select * from ({sql}) where source = 'gh'" if has_source else sql
        con.execute(f"copy ({sql}) to '{path.as_posix()}' (format parquet, compression zstd)")
        # 엑셀 "웹에서", 구글 시트 IMPORTDATA용 정적 CSV (UTF-8, BOM 없음: 기계가 읽는 주소. 사람이 받는 CSV는 화면이 BOM을 붙인다)
        con.execute(f"copy (select * from '{path.as_posix()}') to '{csv_path.as_posix()}' (header, delimiter ',')")
        cols = con.execute(f"describe select * from '{path.as_posix()}'").fetchall()
        rows = con.execute(f"select count(*) from '{path.as_posix()}'").fetchone()[0]
        leaked = ENTITY_COLUMNS & {c[0] for c in cols}
        assert args.with_entities or not leaked, f"{name}에 대상 단위 컬럼 {leaked}"
        cd = desc.get(name, {}).get("columns", {})
        tables[name] = {"grain": grain, "rows": rows, "bytes": path.stat().st_size, "csv_bytes": csv_path.stat().st_size,
                        "columns": [{"name": c[0], "type": c[1], "ko": cd.get(c[0], {}).get("ko"), "unit": cd.get(c[0], {}).get("unit"),
                                     "description": cd.get(c[0], {}).get("description"),
                                     "example": example(con, path.as_posix(), c[0])} for c in cols]}
        print(f"{name}: {rows:,}행, {path.stat().st_size:,}바이트")

    # 공개 표는 컬럼마다 설명이 있어야 하고(D1), 직전 공개에서 컬럼이 말없이 사라지면 안 된다(D6). 대상 단위 표는 공개하지 않아 뺀다
    public = {k: v for k, v in tables.items() if k not in ENTITY_TABLES}
    if bad := missing_descriptions(public, desc):
        raise SystemExit(f"컬럼 설명(description, meta.ko, meta.unit)이 빈 컬럼 {len(bad)}개: {bad}. warehouse/dbt/models/gold/gold.yml에 적는다")
    if bad := removed_columns({k: v for k, v in prev_tables.items() if k not in ENTITY_TABLES}, public, desc):
        raise SystemExit(f"직전 공개에 있던 컬럼이 빠졌다: {bad}. 변경 정책 3번: 지우지 말고 더한다. 꼭 지우면 gold.yml 그 컬럼 meta에 removed: 사유를 적는다")

    span = con.execute("select min(event_date), max(event_date) from main.policy_compare where source = 'gh'").fetchone()
    rel = con.execute("select label, released_on, impact from main.ruleset_release order by seq desc limit 1").fetchone()
    product = {**PRODUCT, "refresh": PRODUCT["refresh"].format(start=span[0], end=span[1]),
               "released_label": rel[0], "released_on": str(rel[1]), "released_impact": rel[2]}
    if empty := [k for k, v in product.items() if not v or v == "None"]:
        raise SystemExit(f"데이터 상품 카드 값이 비었다: {empty} (scripts/export_site.py PRODUCT, seeds/ruleset_release.csv label·impact)")
    meta = {
        "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "git_commit": git_commit(),
        "released_ruleset": released,
        "build_ruleset": built[0] if len(built) == 1 else built,
        "with_entities": args.with_entities,
        "event_date_min": str(span[0]), "event_date_max": str(span[1]),
        "product": product,
        "tables": tables,
        "models": {t: {"description": v["description"], "columns": {c: d.get("description") for c, d in v["columns"].items()}}
                   for t, v in desc.items()},
    }
    (out / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    total = sum(t["bytes"] for t in tables.values())
    print(f"합계 {total:,}바이트 -> {out}")
    # GitHub Pages: 파일 하나 100MB, 사이트 1GB (docs/site_design.md 1절)
    assert all(max(t["bytes"], t["csv_bytes"]) < 100 * 1024 * 1024 for t in tables.values()), "100MB 넘는 파일이 있다"


if __name__ == "__main__":
    main()
