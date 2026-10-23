"""기본 템플릿 골든(사용자 지시 5-8 ③) — v2/tests/golden/web_template_cases.json 이 지금 코드로 만든 것과 같은지.

웹 W-4 가 TS 로 옮긴 행 검사(recipe.check_process_row)·params(process_steps.row_to_params)·기본 템플릿(blocks.template_blocks)을
이 파일로 확인한다. 다르면 python -m v2.tools.make_web_template_golden 으로 다시 만든다(그리고 웹에 알린다).
"""
import json

from v2.tools import make_web_template_golden as G


def test_golden_file_matches_current_code():
    have = json.loads(G.OUT.read_text(encoding="utf-8"))
    want = json.loads(G.to_json(G.build()))
    assert have == want, "기본 템플릿 골든이 지금 코드와 다름 — python -m v2.tools.make_web_template_golden 으로 다시 만들 것"


def test_golden_cases_cover_and_cfg_keys_exact():
    read = set()
    d = G.build(read)
    assert read == set(G.CFG_KEYS)                                       # 검사·템플릿이 읽는 설정 키 = 파일의 cfg 키
    cases = d["cases"]
    assert 20 <= len(cases) <= 40 and len({c["name"] for c in cases}) == len(cases)
    ok = [c for c in cases if not c["checkErrors"]]
    bad = [c for c in cases if c["checkErrors"]]
    assert len(bad) >= 5 and all(c["params"] is None and c["blocks"] is None for c in bad)
    types = lambda c: [b["type"] for b in c["blocks"]]
    assert any("heater_ramp" in types(c) for c in ok) and any("heater_ramp" not in types(c) for c in ok)
    assert any("base_wait" in types(c) for c in ok)
    assert any(not c["igEnabled"] for c in ok) and any(c["rfpulsePresent"] for c in ok)
    pw = [b["params"] for c in ok for b in c["blocks"] if b["type"] == "power_on"]
    assert any(p["rfp_freq_khz"] and p["rfp_duty"] for p in pw) and any(p["dc_power"] and p["rf_power"] for p in pw)
    labels = [b["label"] for c in ok for b in c["blocks"]]
    assert any(l.startswith("DC Power 안정화") for l in labels) and any(l.startswith("Shutter Delay") for l in labels)
    assert any("main" not in types(c) for c in ok)                       # 공정 시간 0
    sp = [sum(1 for b in c["blocks"] if b["type"] == "pressure") for c in ok]
    assert {2, 3, 4} <= set(sp)                                          # SP4 + (SP3·SP2 중 0~2) + SP1


def test_template_blocks_pass_block_check_as_is():
    """5-11 ① — 장비가 만든 템플릿 블록(label·stage "")을 그대로 다시 검사·펼쳐도 오류 0, 펼친 단계 = v1 행으로 돌린 것."""
    from v2.common.paramdef import defaults
    from v2.chk import blocks as BK
    from v2.chk import process_steps as PSt
    n = 0
    for (name, _, over), c in zip(G.CASES, G.build()["cases"]):
        if c["checkErrors"]:
            continue
        cfg = dict(defaults(G.P.PARAMS), **over)
        ig, rfp = c["igEnabled"], c["rfpulsePresent"]
        clean, errs, _ = BK.check_blocks(c["blocks"], cfg)
        assert errs == [], (name, errs)
        bare = [{k: v for k, v in b.items() if k not in ("label", "stage") or v} for b in c["blocks"]]
        assert BK.check_blocks(bare, cfg)[0] == clean, name                # "" = 값 없음 — 정리된 모양이 같다
        got, _ = BK.expand(clean, cfg, ig_enabled=ig, rfpulse_present=rfp)
        assert [s for s in got if not s.internal] == PSt.build_process_steps(c["params"], cfg, ig_enabled=ig,
                                                                             rfpulse_present=rfp), name
        assert got == BK.expand(c["blocks"], cfg, ig_enabled=ig, rfpulse_present=rfp)[0], name
        n += 1
    assert n >= 25
