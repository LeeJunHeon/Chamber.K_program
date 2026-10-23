"""블록 검사 골든(사용자 지시 5-10 ①) — v2/tests/golden/web_block_cases.json 이 지금 코드로 만든 것과 같은지.

웹 W-4b 가 TS 로 옮긴 블록 검사(blocks.check_blocks)·uses_of·펼치기(expand)를 이 파일로 확인한다.
다르면 python -m v2.tools.make_web_block_golden 으로 다시 만들고 웹에 알린다.
"""
import json
import re

from v2.chk import blocks as BK
from v2.chk import web_schema as W
from v2.tools import make_web_block_golden as G


def test_golden_file_matches_current_code():
    have = json.loads(G.OUT.read_text(encoding="utf-8"))
    want = json.loads(G.to_json(G.build()))
    assert have == want, "블록 검사 골든이 지금 코드와 다름 — python -m v2.tools.make_web_block_golden 으로 다시 만들 것"


def test_cfg_keys_exact_and_shape():
    read = set()
    d = G.build(read)
    assert read == set(G.CFG_KEYS)                                       # 검사·펼치기가 읽는 설정 키 = 파일의 cfg 키
    cases = d["cases"]
    assert len({c["name"] for c in cases}) == len(cases)
    for c in cases:
        assert c["igEnabled"] == c["cfg"]["IG_ENABLED"] and c["rfpulsePresent"] == c["cfg"]["RFPULSE_ENABLED"]
        if c["errors"]:
            assert c["clean"] is None and c["uses"] is None and c["expanded"] is None, c["name"]
        else:
            assert c["clean"] is not None and c["uses"] is not None and c["expanded"], c["name"]
            assert c["expanded"][-1]["type"] == "end_stop"                # 끝은 늘 정상 끝 정지(없으면 자동)


def test_covers_all_block_types_tags_and_value_rules():
    cases = G.build()["cases"]
    seen = set()

    def walk(bs):
        for b in bs:
            seen.add(b["type"])
            walk(b.get("blocks") or [])
    for c in cases:
        if c["clean"]:
            walk(c["clean"])
    assert seen == {d.type for d in BK.BLOCK_DEFS} and len(seen) == 18     # 통과 경우만으로 18종 모두
    tags = [t for c in cases for t in c["covers"]]
    assert set(tags) == set(G.TAGS)
    for r in BK.VALUE_RULES:                                             # 값 사이·꺼진 장비 규칙이 모두 한 번 이상 걸림
        pat = re.compile(re.escape(f"): {r.msg}").replace(r"\{", "{").replace(r"\}", "}")
                         .replace("{edge}", ".+").replace("{field}", r"\S+").replace("{sp}", r"\S+")
                         .replace("{ramp_c_per_min}", r"\S+") + "$")
        pool = [w for c in cases for w in c["warnings"]] if r.kind == "warn" else [e for c in cases for e in c["errors"]]
        assert any(pat.search(m) for m in pool), r.id
    assert any(c["warnings"] and not c["errors"] for c in cases) and any(c["warnings"] and c["errors"] for c in cases)


def test_expanded_summary_shape():
    by = {c["name"]: c for c in G.build()["cases"]}
    nested = by["repeat_nested"]["expanded"]
    assert [(x["no"], x["iter"]) for x in nested[:3]] == [("1.1#1", "1"), ("1.2.1#1.1", "1.1"), ("1.2.1#1.2", "1.2")]
    auto = by["pre_clean_defaults"]["expanded"][-1]
    assert auto["auto"] is True and auto["label"] == BK.AUTO_END_STOP_LABEL and auto["no"] == "2"
    assert by["end_stop_middle"]["expanded"][-1]["auto"] is True                # 중간 end_stop 뒤에 대기 → 자동 끝 정지
    pre = by["pre_clean_defaults"]["expanded"][0]
    assert pre["params"]["settle_sec"] == by["pre_clean_defaults"]["cfg"]["PRE_POWER_OFF_SETTLE_SEC"]   # "cfg:키" 는 값으로
    assert "watch" not in pre and "watch" in by["main_close"]["expanded"][0]
    assert by["main_close"]["expanded"][0]["watch"] == dict(BK.WATCH_MAIN, rf=False, pressure=False)
    assert all(set(s) >= {"action", "message"} for c in by.values() for x in (c["expanded"] or []) for s in x["steps"])


def test_numbers_follow_param_type():
    by = {c["name"]: c for c in G.build()["cases"]}
    sp = by["pressure_sp_int_as_float"]["clean"][0]["params"]["sp"]
    assert sp == 2 and type(sp) is int                                   # int 정의 — 2.0 도 2
    sec = by["wait_stages"]["clean"][1]["params"]["sec"]
    assert sec == 3.0 and type(sec) is float                             # float 정의 — 정수 3 도 3.0
    assert "블록 1(wait): sec: -1.0 < 0" in by["err_range_min"]["errors"]
    assert "블록 3(pressure): sp: 0 < 1" in by["err_range_min"]["errors"]
    # JSON 의 1 과 1.0 이 같은 결과(웹 JSON 은 둘을 가르지 못한다)
    a = BK.check_blocks([{"type": "main", "params": {"sec": 0}}], G.defaults(G.P.PARAMS))
    b = BK.check_blocks([{"type": "main", "params": {"sec": 0.0}}], G.defaults(G.P.PARAMS))
    assert a == b and a[1] == ["블록 1(main): sec: 0.0 ≤ 0"]


def test_bundle_has_what_block_check_needs():
    b = W.definitions()["blocks"]
    assert b["valueRules"] == [r.public() for r in BK.VALUE_RULES]
    assert b["blockKeys"] == list(BK.BLOCK_KEYS) and b["spDefaultKey"] == {"2": "SP2_MTORR", "3": "SP3_MTORR", "4": "SP4_MTORR"}
    assert b["heaterRampMin"] == BK.HEATER_RAMP_MIN and b["autoEndStopLabel"] == BK.AUTO_END_STOP_LABEL
    assert b["limits"] == {"repeatMax": BK.REPEAT_MAX, "maxDepth": BK.MAX_DEPTH, "maxExpandedSteps": BK.MAX_EXPANDED_STEPS,
                           "labelMax": BK.LABEL_MAX}
    assert b["pulseEdge"]["param"] in G.CFG_KEYS
    params = {p["key"] for p in W.definitions()["params"]}
    for d in b["defs"]:                                                  # 범위·기본값이 가리키는 설정 키는 묶음 params 에
        for p in d["params"]:
            for k in (p["paramMin"], p["paramMax"]):
                assert k is None or k in params
            if isinstance(p["default"], str) and p["default"].startswith("cfg:"):
                assert p["default"][4:] in params
    for r in b["valueRules"]:
        assert r["kind"] in ("need", "warn") or r["key"] in params
    assert set(G.CFG_KEYS) <= params
