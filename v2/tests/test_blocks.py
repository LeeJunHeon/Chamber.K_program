"""블록 공정(사용자 결정 2026-10-20, 5-4 ②) — 블록 정의·값 검사·기본 템플릿·펼치기·실행·감시·정지 순서 설정.

- 기본 템플릿을 펼치면 5-3 의 스텝 빌더와 같은 스텝(동작·인자·값·시간·문구·상태 이름)이 나온다 — 아래 REF 가 5-3 빌더 사본
- 순서 규칙 없음: 어떤 순서든 받는다(값 범위·꺼진 장비만 거부)
- 블록 시작·끝 기록, 화면 상태(블록 번호/전체·종류·라벨·남은 시간), 정상 끝 정지 없이 끝나면 OUTPUTS_LEFT_ON 경고만
- 이탈 감시는 블록 watch 로(S31C·S38A …), 무출력은 램프 중에만, STOP 정지 순서는 STOP_SEQUENCE_ORDER
"""
import itertools
import math

import pytest

from v2.common.paramdef import check_config, defaults
from v2.chk import blocks as BK
from v2.chk import params as P
from v2.chk import recipe as RC
from v2.chk import snapshot_fields as SF
from v2.chk.process_steps import S, build_gas_intro_steps, build_gas_pre_steps, build_pressure_stage_steps, \
    build_process_steps, base_pressure_of, channels_of, heater_used, row_to_params
from v2.chk.sim.plc_sim import A
from v2.tests.test_process_engine import FAST, R, SHORT
from v2.tests.test_safety_engine import CFG, H

C0 = defaults(P.PARAMS)


# ───────────── 5-3 스텝 빌더 사본(기준 — 5-4 ④ 의 SP4_SET 만 다르다) ─────────────
def REF(p, cfg, *, ig_enabled, rfpulse_present=True):
    channels, buttons = channels_of(p)
    default_flow = float(p.get("mfc_flow", 0.0) or 0.0)
    flows = {}
    if 1 in channels:
        flows[1] = float(p.get("ar_flow", default_flow) or 0.0)
    if 2 in channels:
        flows[2] = float(p.get("o2_flow", default_flow) or 0.0)
    sp1 = float(p.get("sp1_set", 0.0) or 0.0)
    steps = [S("DC_POWER_STOP", "PRE: DC Power OFF", state="PRE_CLEAN"), S("RF_POWER_STOP", "PRE: RF Power OFF", state="PRE_CLEAN")]
    if rfpulse_present:
        steps.append(S("RF_PULSE_STOP", "PRE: RF Pulse OFF", state="PRE_CLEAN"))
    steps.append(S("DELAY", "PRE: Power OFF settle", duration_sec=int(round(cfg["PRE_POWER_OFF_SETTLE_SEC"])), state="PRE_CLEAN"))
    steps += build_gas_pre_steps(channels)
    if heater_used(p):
        temp = float(p["heater_temp"])
        ramp = float(p.get("heater_ramp") or 0.0) or float(cfg["HEATER_RAMP_RATE_C_PER_MIN"])
        steps.append(S("HEATER_RAMP", f"히터 램프 속도 {ramp:.0f}°C/min 설정", value=ramp, state="HEATER_RAMP"))
        steps.append(S("HEATER_SET", f"히터 목표 {temp:.1f}°C 설정 — 승온 시작", value=temp, state="HEATER_RAMP"))
        steps.append(S("HEATER_WAIT", f"히터 온도 도달 대기 (±{cfg['HEATER_SOAK_TOLERANCE']:.1f}°C, "
                       f"{int(cfg['HEATER_SOAK_TIME_SEC'])}s 유지)", value=temp, state="HEATER_RAMP"))
    bp = base_pressure_of(p)
    if bp > 0 and ig_enabled:
        steps += [S("IG_FILAMENT_ON", "이온 게이지 필라멘트 ON", state="BASE_WAIT"),
                  S("IG_BASE_WAIT", f"베이스 압력 대기 (목표 {bp:.1E} Torr)", value=bp, state="BASE_WAIT"),
                  S("IG_FILAMENT_OFF", "이온 게이지 필라멘트 OFF", state="BASE_WAIT")]
    steps += build_gas_intro_steps(channels, flows, buttons)
    stab = int(round(cfg["GAS_STABILIZE_SEC"]))
    steps.append(S("MFC_CMD", "SP4 ON", ("SP4_ON", {}), state="GAS_STABILIZE"))
    steps.append(S("DELAY", f"압력 안정화 대기({stab}초)", duration_sec=stab, state="GAS_STABILIZE"))
    if p.get("use_g1"):
        steps.append(S("PLC_CMD", "Gun Shutter 1 Open", ("S1_button", True), state="GUN_SHUTTER"))
    if p.get("use_g2"):
        steps.append(S("PLC_CMD", "Gun Shutter 2 Open", ("S2_button", True), state="GUN_SHUTTER"))
    steps.append(S("POWER_WAIT", "파워 목표치 도달 대기", state="POWER_WAIT"))
    sp3, sp2 = float(cfg["SP3_MTORR"]), float(cfg["SP2_MTORR"])
    stage_to = float(cfg["PRESSURE_STAGE_TIMEOUT_SEC"])
    if sp1 < sp3:
        steps += build_pressure_stage_steps(3, sp3, "Stage1(SP3)", cfg, stage_to, state="PRESSURE_SP3")
    if sp1 < sp2:
        steps += build_pressure_stage_steps(2, sp2, "Stage2(SP2)", cfg, stage_to, state="PRESSURE_SP2")
    steps += build_pressure_stage_steps(1, sp1, f"Stage3(SP1={sp1:.2f})", cfg, float(cfg["SP1_TIMEOUT_SEC"]),
                                        fail_on_timeout=True, state="PRESSURE_SP1")
    dc_power = float(p.get("dc_power", 0.0) or 0.0)
    dcd = int(cfg["DC_POWER_DELAY_SEC"])
    if p.get("use_dc_delay") and dc_power > 0.0 and dcd > 0:
        steps.append(S("DELAY", f"DC Power 안정화 대기 {dcd}s", duration_sec=dcd, state="DC_DELAY"))
    sd = max(0, int(math.ceil(float(p.get("shutter_delay", 0.0) or 0.0) * 60)))
    if sd > 0:
        steps.append(S("DELAY", f"Shutter Delay {sd}s", duration_sec=sd, timer_purpose="shutter", polling=True, state="SHUTTER_DELAY"))
    if float(p.get("process_time", 0.0) or 0.0) > 0:
        steps.append(S("PLC_CMD", "Main Shutter Open", ("MS_button", True), state="MAIN"))
    pt = max(0, int(math.ceil(float(p.get("process_time", 0.0) or 0.0) * 60)))
    if pt > 0:
        steps.append(S("DELAY", f"메인 공정 {pt}s 진행", duration_sec=pt, timer_purpose="process", polling=True, state="MAIN"))
    return steps


def _rows():
    for ar, o2, bp, wp, heat, dc, rf, rfp, g1, g2, dcd, sd, pt in itertools.product(
            (True, False), (False, True), (0, 1e-5), (5.0, 3.0, 20.0), (False, True), (True, False), (False, True),
            (False, True), (True, False), (False, True), (False, True), (0.0, 0.5), (0.1, 0.0)):
        if not (ar or o2) or not (dc or rf or rfp) or (sd == 0 and pt == 0):
            continue
        row = {"process_name": "X", "ar": ar, "o2": o2, "working_pressure": wp, "process_time": pt, "shutter_delay": sd,
               "use_dc": dc, "use_rf": rf, "use_rf_pulse": rfp, "gun1": g1, "gun2": g2, "use_dc_delay": dcd}
        if ar:
            row["ar_flow"] = 20.0
        if o2:
            row["o2_flow"] = 7.5
        if bp:
            row["base_pressure"] = bp
        if dc:
            row["dc_power"] = 100.0
        if rf:
            row["rf_power"] = 50.0
        if rfp:
            row.update(rf_pulse_power=80.0, rf_pulse_freq=2.5, rf_pulse_duty=40)
        if heat:
            row.update(use_heater=True, heater_temp=60.0, heater_ramp=30.0)
        yield row


@pytest.mark.parametrize("ig,rfpe", [(True, True), (False, True), (True, False)])
def test_template_expands_to_same_steps_as_5_3_builder(ig, rfpe):
    cfg = dict(C0, IG_ENABLED=ig, RFPULSE_ENABLED=rfpe, DC_POWER_DELAY_SEC=10)
    n = 0
    for row in _rows():
        ok, errs, _ = RC.check_process_row(row, cfg)
        if errs:
            continue
        p = row_to_params(ok, cfg)
        key = lambda ss: [(s.unit(), s.message, s.state, s.polling) for s in ss]
        new = build_process_steps(p, cfg, ig_enabled=ig, rfpulse_present=rfpe)
        i = [k for k, s in enumerate(new) if s.params and s.params[0] == "SP4_ON"][0]
        sp4 = new.pop(i - 1)                                               # 의도된 차이 SP4_FROM_CONFIG(5-4 ④)
        assert (sp4.params, sp4.state, sp4.message) == (("SP4_SET", {"value": cfg["SP4_MTORR"]}), "GAS_STABILIZE",
                                                        "SP4_SET=30.00 설정")
        assert key(new) == key(REF(p, cfg, ig_enabled=ig, rfpulse_present=rfpe)), row
        n += 1
    assert n > 1000


def test_template_last_block_is_end_stop_and_steps_are_tagged():
    p = row_to_params(RC.check_process_row(SHORT, C0)[0], C0)
    bl = BK.template_blocks(p, C0, ig_enabled=True)
    assert bl[0]["type"] == "pre_clean" and bl[-1]["type"] == "end_stop"
    steps, insts = BK.expand(bl, C0)
    assert steps[-1].action == "END_STOP" and steps[-1].internal
    assert all(0 <= s.block < len(insts) for s in steps)
    assert [i.type for i in insts] == [b["type"] for b in bl]
    sd = [i for i in insts if i.stage == "SHUTTER_DELAY"][0]
    assert sd.watch == dict(BK.WATCH_DEFAULT, dc=True)                     # v1: DC 이탈 감시는 셔터 딜레이부터
    assert [i for i in insts if i.type == "main"][0].watch == BK.WATCH_MAIN
    refs = {s.ref for s in steps if s.ref}
    assert refs == {"mfc.ar_flow", "mfc.wp", "process.main_remaining"}


# ───────────── 정의 표·검사 ─────────────
def test_block_defs_refs():
    keys = {p.key for p in P.PARAMS}
    cols = set(SF.columns())
    from v2.chk import process_states as PS
    states = set(PS.by_name())
    seen = set()
    for d in BK.BLOCK_DEFS:
        assert d.type not in seen
        seen.add(d.type)
        assert d.stage in states or d.container, d.type
        assert set(d.stages) <= states, d.type
        for p in d.params:
            for k in (p.param_min, p.param_max):
                assert k is None or k in keys, (d.type, p.name, k)
            if isinstance(p.default, str) and p.default.startswith("cfg:"):
                assert p.default[4:] in keys, (d.type, p.name)
    assert {f for f, _, _ in BK.COND_FIELDS} <= cols
    assert set(BK.STAGE_TO_TYPE.values()) <= seen


def _chk(blocks, **cfg):
    return BK.check_blocks(blocks, dict(C0, **cfg))


def test_any_order_is_accepted_only_values_checked():
    # 파워 → 가스 → 시작 전 정리 → 압력 … 순서가 이상해도 받는다(순서 규칙 없음)
    bl = [{"type": "power_on", "params": {"dc_power": 50.0, "wait": False}}, {"type": "gas_intro", "params": {"ar": True, "ar_flow": 10}},
          {"type": "pre_clean"}, {"type": "heater_wait"}, {"type": "wait", "params": {"sec": 1}}]
    ok, errs, _ = _chk(bl)
    assert errs == [] and [b["type"] for b in ok] == ["power_on", "gas_intro", "pre_clean", "heater_wait", "wait"]
    bad = [{"type": "power_on", "params": {"dc_power": 5000.0}},
           {"type": "pressure", "params": {"sp": 5, "value": 5}},
           {"type": "gas_intro", "params": {"ar": True}},
           {"type": "wait", "params": {"sec": "x"}},
           {"type": "shutter", "params": {"action": "open", "targets": ["g3"]}},
           {"type": "nope"},
           {"type": "wait", "params": {"sec": 1, "watch": {"dc": "yes"}}},
           {"type": "main", "params": {"sec": 0}},
           {"type": "power_on", "params": {"rfp_power": 10, "rfp_freq_khz": 100.0}},
           {"type": "end_stop", "params": {"skip": [99]}},
           {"type": "wait", "params": {"sec": 1}, "stage": "MAIN"}]
    for b in bad:
        ok, errs, _ = _chk([b])
        assert ok is None and errs, b
    _, errs, _ = _chk([{"type": "heater_ramp", "params": {"target_c": 50}}], HEATER_ENABLED=False)
    assert errs and "HEATER_ENABLED" in errs[0]
    _, errs, _ = _chk([{"type": "base_wait", "params": {"target_torr": 1e-5}}], IG_ENABLED=False)
    assert errs and "IG_ENABLED" in errs[0]


def test_repeat_unrolls_nested_with_numbers():
    bl = [{"type": "wait", "params": {"sec": 1}},
          {"type": "repeat", "params": {"count": 2}, "blocks": [
              {"type": "wait", "params": {"sec": 2}, "label": "a"},
              {"type": "repeat", "params": {"count": 3}, "blocks": [{"type": "wait", "params": {"sec": 3}}]}]}]
    ok, errs, _ = _chk(bl)
    assert not errs
    steps, insts = BK.expand(ok, C0)
    assert [i.no for i in insts] == ["1", "2.1#1", "2.2.1#1.1", "2.2.1#1.2", "2.2.1#1.3",
                                     "2.1#2", "2.2.1#2.1", "2.2.1#2.2", "2.2.1#2.3"]
    assert [s.duration_sec for s in steps] == [1, 2, 3, 3, 3, 2, 3, 3, 3]
    too_big = [{"type": "repeat", "params": {"count": 100}, "blocks": [
        {"type": "repeat", "params": {"count": 100}, "blocks": [{"type": "wait", "params": {"sec": 1}}]}]}]
    _, errs, _ = _chk(too_big)
    assert errs and "펼친 스텝" in errs[0]


def test_stop_sequence_order_param():
    vals = dict(C0, STOP_SEQUENCE_ORDER="2,1,3")
    assert check_config(P.PARAMS, P.CROSS_RULES, vals) == []
    for bad in ("1,1,2", "1,99", "a,b"):
        assert check_config(P.PARAMS, P.CROSS_RULES, dict(C0, STOP_SEQUENCE_ORDER=bad)), bad
    h = H(state="MAIN")
    h.s.cfg = dict(CFG, STOP_SEQUENCE_ORDER="2,12,7")
    assert [s.no for s in h.s.stop_steps()] == [2, 12, 7]
    assert [s.no for s in h.s.stop_steps(skip=(12,))] == [2, 7]
    h.s.cfg = dict(CFG)
    assert [s.no for s in h.s.stop_steps()] == list(range(1, 14))      # 기본값 = v1 순서


def test_stop_uses_configured_order(tmp_path):
    r = R(tmp_path, dict(FAST, STOP_SEQUENCE_ORDER="2,4,13,11"))
    r.on(3, r.cmd("PROCESS_START", {"row": dict(SHORT, process_time=2.0)}))
    r.when(lambda: r.rig.process.state == "MAIN", r.cmd("PROCESS_STOP"))
    r.run(120)
    assert [n for n, _, _ in r.rig.safety.engine.seq_results["stop"]] == [2, 4, 13, 11]
    assert r.rig.sims["dc"].out is False and r.plc.coils[A["s1"]] is False
    assert r.plc.coils[A["ar_valve"]] is True                            # 12(차단밸브 닫기)는 껐다


# ───────────── 감시·적용 구간 ─────────────
def test_watch_windows_follow_block_not_state_name():
    # 같은 상태(WAIT)라도 감시를 켠 블록에서만 DC 이탈(S31C)·유량 이탈(S38A)
    for item, rid, proc, v in (("dc", "S31C", dict(uses_dc=True, dc_set_w=100.0), {"dc.p": 50.0}),
                               ("flow", "S38A", dict(flow_set={"ar": 20.0, "o2": None}), {"mfc.ar_sccm": 10.0})):
        for on in (True, False):
            h = H(state="WAIT", watch=dict(BK.WATCH_DEFAULT, **{item: on}), **proc)
            got = set()
            for k in range(0, 120):
                got |= set(h.feed("tick", k, v))
            assert (rid in got) is on, (rid, on)
    # 무출력(S31A)은 상태 이름이 아니라 램프 중일 때
    h = H(state="WAIT", uses_dc=True, dc_ramp=True)
    assert any("S31A" in h.feed("tick", k, {"dc.i_set": 0.3, "dc.p": 0.5}) for k in range(0, 30))
    h = H(state="POWER_WAIT", uses_dc=True, dc_ramp=False)
    assert not any("S31A" in h.feed("tick", k, {"dc.i_set": 0.3, "dc.p": 0.5}) for k in range(0, 30))


def test_watch_for_state_matches_template():
    p = row_to_params(RC.check_process_row(dict(SHORT, use_dc_delay=True), C0)[0], C0)
    _, insts = BK.expand(BK.template_blocks(p, C0, ig_enabled=True), C0)
    for i in insts:
        if i.type != "end_stop":
            assert i.watch == BK.watch_for_state(i.stage), i


# ───────────── 실행(시뮬레이터) ─────────────
GAS = {"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}}
FREE = [{"type": "pre_clean"}, {"type": "gas_prep", "params": {"ar": True}}, GAS,
        {"type": "power_on", "params": {"dc_power": 100.0}, "label": "DC 먼저"},
        {"type": "pressure", "params": {"sp": 1, "value": 5.0}},
        {"type": "cond_wait", "params": {"field": "dc.p", "op": "ge", "value": 90.0, "stable_sec": 2, "timeout_sec": 60}},
        {"type": "shutter", "params": {"action": "open", "targets": ["g1"]}},
        {"type": "main", "params": {"sec": 5}, "label": "증착"},
        {"type": "power_off", "params": {"dc": True}},
        {"type": "gas_off", "params": {"ar": True}},
        {"type": "end_stop"}]


def test_free_order_block_recipe_runs_to_end(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "free", "blocks": FREE}))
    r.on(3, r.cmd("RECIPE_START"))
    snaps = []
    r.rig.loop.tick_hooks.append(lambda t: snaps.append(r.rig.process.snapshot_fields()))
    r.run(200)
    assert all(x.ok for x in r.results), r.results
    ends = r.ev("run_end")
    assert [(e.data["status"], e.data["reason"]) for e in ends] == [("done", None)]
    bev = [e.data for e in r.ev("block")]
    starts = [d for d in bev if d["phase"] == "start"]
    assert [d["type"] for d in starts] == [b["type"] for b in FREE]
    assert all(d["result"] == "ok" for d in bev if d["phase"] == "end") and len(bev) == 2 * len(FREE)
    assert starts[3]["label"] == "DC 먼저" and starts[3]["params"]["dc_power"] == 100.0 and starts[3]["no"] == "4"
    seen = {(s["proc.block"], s["proc.block_type"], s["proc.block_label"]) for s in snaps if s["proc.block"]}
    assert ("8/11", "main", "증착") in seen
    assert any(s["proc.block_type"] == "main" and s["proc.remain_s"] is not None for s in snaps)
    st = [s for s, _ in r.states()]
    assert "COND_WAIT" in st and "POWER_OFF" in st and "GAS_OFF" in st and st[-3:] == ["STOPPING", "DONE", "IDLE"]
    assert r.rig.sims["dc"].out is False and not any(r.rig.sims["mfc"].on)
    assert not [e for e in r.rig.safety.engine.events if e.kind in ("fired", "abort")]


def test_no_end_stop_leaves_outputs_on_with_warning(tmp_path):
    r = R(tmp_path, FAST)
    bl = [GAS, {"type": "power_on", "params": {"dc_power": 100.0}}, {"type": "wait", "params": {"sec": 3}}]
    r.on(3, r.cmd("PROCESS_START", {"blocks": bl}))
    r.run(120)
    ends = r.ev("run_end")
    assert [e.data["status"] for e in ends] == ["done"]
    w = [e for e in r.ev("warn") if e.code == "OUTPUTS_LEFT_ON"]
    assert w and set(w[0].data["outputs"]) == {"dc", "ar"}
    assert r.rig.sims["dc"].out is True and r.rig.sims["mfc"].on[0] is True       # 끄지 않음
    assert "row_end" not in r.rig.safety.engine.seq_results


def test_end_stop_in_middle_continues_to_next_block(tmp_path):
    r = R(tmp_path, FAST)
    bl = [GAS, {"type": "end_stop", "params": {"skip": [12]}}, {"type": "wait", "params": {"sec": 2}, "label": "뒤"}]
    r.on(3, r.cmd("PROCESS_START", {"blocks": bl}))
    r.run(120)
    assert [e.data["status"] for e in r.ev("run_end")] == ["done"]
    starts = [e.data["label"] for e in r.ev("block") if e.data["phase"] == "start"]
    assert starts[-1] == "뒤"
    res = r.rig.safety.engine.seq_results["row_end"]
    assert 12 not in [n for n, _, _ in res] and not any(r.rig.sims["mfc"].on)
    st = [s for s, _ in r.states()]
    assert st.index("STOPPING") < st.index("WAIT")


def test_cond_wait_timeout_abort_and_continue(tmp_path):
    for mode, status in (("abort", ("error", "COND_WAIT_TIMEOUT")), ("continue", ("done", None))):
        r = R(tmp_path / mode, FAST)
        bl = [{"type": "cond_wait", "params": {"field": "mfc.pressure_mtorr", "op": "ge", "value": 900.0,
                                               "timeout_sec": 5, "on_timeout": mode}},
              {"type": "wait", "params": {"sec": 1}}]
        r.on(3, r.cmd("PROCESS_START", {"blocks": bl}))
        r.run(40)
        assert [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")] == [status], mode
        ends = [e.data for e in r.ev("block") if e.data["phase"] == "end"]
        if mode == "abort":
            assert ends[0]["result"] == "failed" and ends[0]["reason"] == "COND_WAIT_TIMEOUT"
        else:
            assert [e.code for e in r.ev("warn")].count("COND_WAIT_TIMEOUT") == 1


def test_load_mixed_list_rows_delay_and_blocks(tmp_path):
    r = R(tmp_path, FAST)
    rows = [SHORT, {"process_name": "delay 3s"}, {"process_name": "B", "blocks": [{"type": "wait", "params": {"sec": 2}}]}]
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "mix", "rows": rows}))
    r.on(3, r.cmd("RECIPE_START"))
    r.run(200)
    assert [e.detail for e in r.ev("run_start")] == ["CSV 1/3 - S", "CSV 3/3 - B"]
    assert [e.data["status"] for e in r.ev("run_end")] == ["done", "done"]
    assert r.ev("list_done")


def test_block_recipe_rejections(tmp_path):
    r = R(tmp_path, dict(FAST, HEATER_ENABLED=False))
    seq = [("RECIPE_LOAD", {"name": "x", "blocks": [{"type": "wait", "params": {"sec": -1}}]}, "RECIPE_INVALID"),
           ("RECIPE_LOAD", {"name": "h", "blocks": [{"type": "heater_ramp", "params": {"target_c": 50}}]}, "DEVICE_DISABLED"),
           ("PROCESS_START", {"blocks": [{"type": "nope"}]}, "RECIPE_INVALID"),
           ("RECIPE_LOAD", {"name": "y", "rows": [{"process_name": "B", "blocks": [], "x": 1}]}, "RECIPE_INVALID")]
    for i, (k, a, _) in enumerate(seq):
        r.on(3 + i, r.cmd(k, a))
    r.run(8)
    assert [x.reason for x in r.results] == [e for _, _, e in seq]
    assert any(e.code == "RECIPE_INVALID" and e.data["errors"] for e in r.ev("reject"))
