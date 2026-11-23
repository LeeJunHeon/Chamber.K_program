"""수동 조작(5-20 ③) — 블록을 하나씩 실행하는 공정(MANUAL_BLOCK·MANUAL_END). 가상 시계·시뮬레이터만(실제 포트 없음).

- 같은 블록을 수동으로 하나씩 실행한 것과 블록 공정(PROCESS_START blocks)의 장비 명령·스텝이 같다(끝은 같은 정지 순서)
- 수동 대기(MANUAL_WAIT) 중 DC 저전류·RF 반사·DC 통신 두절·MV 인터록 해제가 공정과 같은 규칙을 내고 모두 끈다
- STOP·ALL STOP·끝내기, 거부(블록 실행 중·공정 중·수동 조작 중 공정 시작·쓰지 않는 블록), 웹·노트북 경로, 기록·실행 상태,
  S51·S10 이 수동 조작의 출력을 남은 출력으로 끄지 않음
"""
import pytest

from v2.common.commands import Source
from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk.sim.plc_sim import A
from v2.tests.test_process_engine import FAST, R

GAS = {"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}}
SP1 = {"type": "pressure", "params": {"sp": 1, "value": 5.0}}
DC_ON = {"type": "power_on", "params": {"dc_power": 100.0}}
DC_CHANGE = {"type": "power_on", "params": {"dc_power": 150.0}}
DC_OFF = {"type": "power_off", "params": {"dc": True}}
GAS_OFF = {"type": "gas_off", "params": {"ar": True}}
SCENARIO = [GAS, SP1, DC_ON, DC_CHANGE, DC_OFF, GAS_OFF]


class M(R):
    """수동 대기가 될 때마다 다음 명령을 보낸다. ("hold", n) = 수동 대기에서 n tick 더 기다림."""

    def __init__(self, tmp_path, cfg=None, source=Source.WEB, start=3):
        super().__init__(tmp_path, dict(FAST, **(cfg or {})))
        self.queue, self.sent, self.source, self.start = [], [], source, start
        self.hold = 0
        self.rig.loop.tick_hooks.append(self._feed)

    def _feed(self, t):
        p = self.rig.process
        if not self.queue or t.seq < self.start or (self.sent and p.state != "MANUAL_WAIT"):
            return
        if self.hold > 0:
            self.hold -= 1
            return
        item = self.queue.pop(0)
        if item[0] == "hold":
            self.hold = item[1]
            return
        kind, args = item
        self.sent.append((t.seq, kind, self.rig.core.command(kind, args, source=self.source)))

    def blocks(self, blocks, end=True, hold=0):
        for b in blocks:
            self.queue.append(("MANUAL_BLOCK", {"block": b}))
            if hold:
                self.queue.append(("hold", hold))
        if end:
            self.queue.append(("MANUAL_END", {}))

    def pev(self, kind):
        return [e for e in self.rig.events if type(e).__name__ == "ProcEvent" and e.kind == kind]


def _device_log(r, t0=0.0):
    """장비 쓰기(질의 빼고) — MFC 명령·PLC 코일 쓰기·DC 출력 명령. DC 전류 조절(CURR)·하트비트는 시간에 따라 달라 뺀다."""
    s = r.rig.sims
    mfc = [d.decode().strip() for t, d in s["mfc"].received if t >= t0 and not d.startswith(b"R")]
    plc = [bytes(f[:6]).hex() for t, f in s["plc"].frames if t >= t0 and len(f) > 1 and f[1] in (5, 15)]
    dc = [c for c in s["dc"].commands_seen() if c.startswith("OUTP") and "?" not in c]
    return mfc, plc, dc


def _steps(r):
    out = []
    for e in r.rig.events:
        if type(e).__name__ == "ProcEvent" and e.kind == "step":
            out.append(e.detail.split("] ", 1)[-1])
    return out


def _all_off(r):
    s, c = r.rig.sims, r.rig.sims["plc"].coils
    assert s["dc"].out is False and not any(s["mfc"].on), (s["dc"].out, s["mfc"].on)
    assert not any(c[A[k]] for k in ("ar_valve", "o2_valve", "ms", "s1", "s2")), [k for k in ("ar_valve", "o2_valve", "ms", "s1",
                                                                                             "s2") if c[A[k]]]
    assert (s["plc"].regs[A["rf_dac"]] or 0) == 0
    assert r.rig.process.left == {} and r.rig.process.manual is False


# ───────── 블록 공정과 같다 ─────────
@pytest.mark.slow
def test_manual_sequence_matches_block_process(tmp_path):
    m = M(tmp_path / "m")
    m.blocks(SCENARIO, hold=2)
    m.run(260)
    assert all(x.ok for _, _, x in m.sent), m.sent
    assert [s for s, _ in m.states()].count("MANUAL_WAIT") == len(SCENARIO)
    assert m.rig.process.state == "IDLE"
    p = R(tmp_path / "p", FAST)
    p.on(3, p.cmd("PROCESS_START", {"blocks": list(SCENARIO)}))
    p.run(260)
    assert all(x.ok for x in p.results)
    assert _steps(m) == _steps(p)                                         # 같은 스텝(문구 순서)
    mm, pm = _device_log(m), _device_log(p)
    assert mm == pm, (mm, pm)                                             # 같은 MFC 명령·PLC 버튼·DC 출력 명령(같은 정지 순서)
    (me,), (pe,) = m.ev("run_end"), p.ev("run_end")
    assert (me.data["status"], me.data["reason"], me.data["manual"]) == ("done", "MANUAL_END", True)
    assert (pe.data["status"], pe.data.get("manual")) == ("done", None)
    ex = me.data["summary"]["executed"]
    assert [x["type"] for x in ex] == [b["type"] for b in SCENARIO] and {x["result"] for x in ex} == {"done"}
    assert [x["no"] for x in ex] == [str(i) for i in range(1, len(SCENARIO) + 1)]
    _all_off(m)
    rec = m.pev("manual")
    assert [d.data["result"] for d in rec if d.code == "MANUAL_BLOCK"] == ["done"] * len(SCENARIO)
    assert all(d.data["source"] == "web" for d in rec) and rec[-1].code == "MANUAL_END"
    (rs,) = m.ev("run_start")
    assert rs.data["manual"] is True and rs.data["name"] == "수동 조작" and rs.data["source"] == "web"
    msgs = [x for x in m.rig.hub.recent if x["type"] in ("run_start", "run_end")]
    assert [x["data"].get("manual") for x in msgs] == [True, True]


def test_manual_state_export_and_adjust_in_wait(tmp_path):
    m = M(tmp_path)
    m.blocks([GAS, SP1, DC_ON], end=False)
    seen = {}

    def look(t):
        p = m.rig.process
        if p.state == "MANUAL_WAIT" and len(p.exec_log) == 3 and "run" not in seen:
            seen["run"] = p.run_state(True)
            seen["snap"] = p.snapshot_fields()
            seen["adj"] = m.rig.core.command("PROCESS_ADJUST", {"device": "dc", "item": "power", "value": 120.0})
            seen["busy"] = m.rig.core.command("MANUAL_BLOCK", {"block": DC_OFF})          # 받음 — 끝나면 다시 대기
            seen["edit"] = m.rig.core.command("PROCESS_EDIT", {"revision": 1, "blocks": []})
            seen["start"] = m.rig.core.command("PROCESS_START", {"blocks": [GAS]})
            seen["load"] = m.rig.core.command("RECIPE_LOAD", {"name": "x", "blocks": [GAS]})
            seen["clear"] = m.rig.core.command("RECIPE_CLEAR", {})
            seen["rstart"] = m.rig.core.command("RECIPE_START", {})
            seen["ig"] = m.rig.core.command("IG_FILAMENT", {"on": False})               # 공정이 없을 때처럼
            seen["now"] = m.rig.core.command("MANUAL_BLOCK", {"block": GAS_OFF})       # 블록 실행 중 → MANUAL_BUSY
            seen["dc_target"] = p.dcc.target
    m.rig.loop.tick_hooks.append(look)
    m.run(120)
    run = seen["run"]
    man = run["manual"]
    assert man["label"] == "수동 조작 중" and man["waiting"] is True and man["source"] == "web"
    assert run["current"] is None and [e["type"] for e in man["executed"]] == ["gas_intro", "pressure", "power_on"]
    keys = {o["key"] for o in man["outputs"]}
    assert {"ar_flow", "ar_valve", "pressure", "dc"} <= keys, keys
    assert next(o for o in man["outputs"] if o["key"] == "dc")["value"] == 100.0
    assert seen["snap"]["proc.manual"] is True and seen["snap"]["proc.state"] == "MANUAL_WAIT" and seen["snap"]["proc.block"] is None
    assert seen["adj"].ok and seen["dc_target"] == 120.0                                # 공정 중 수정은 공정과 같이
    assert seen["busy"].ok and seen["now"].reason == "MANUAL_BUSY"
    for k in ("edit", "start", "load", "clear", "rstart"):
        assert seen[k].reason == "MANUAL_ACTIVE" and "먼저 끝내" in seen[k].detail, k
    assert seen["ig"].ok
    assert m.rig.process.manual is True and m.rig.process.state == "MANUAL_WAIT"


# ───────── 수동 대기 중 안전 규칙 — 공정과 같은 규칙 ─────────
def _fault(kind, r):
    s = r.rig.sims
    if kind == "dc_low":
        r.rig.chamber.low_current = True
    elif kind == "rf_ref":
        s["plc"].ref_inject_w = 150.0
    elif kind == "dc_comm":
        now = r.c.monotonic()
        s["dc"].add_fault("disconnect", now, now + 1000)
    elif kind == "mv_itl":
        s["plc"].itl_fault = True


POWER = {"dc_low": DC_ON, "dc_comm": DC_ON, "mv_itl": DC_ON, "rf_ref": {"type": "power_on", "params": {"rf_power": 100.0}}}
EXPECT = {"dc_low": "DC_LOW_CURRENT", "rf_ref": "RF_REFLECTED_HIGH", "dc_comm": "DC_COMM_LOST", "mv_itl": "MV_INTERLOCK_LOST"}


def _run_fault(tmp_path, kind, manual):
    blocks = [GAS, SP1, POWER[kind]]
    if manual:
        r = M(tmp_path)
        r.blocks(blocks, end=False)
        when = lambda p: p.state == "MANUAL_WAIT" and len(p.exec_log) == 3
    else:
        r = R(tmp_path, FAST)
        r.on(3, r.cmd("PROCESS_START", {"blocks": blocks + [{"type": "wait", "params": {"sec": 600}}]}))
        when = lambda p: p.state == "WAIT"
    box = {}

    def hook(t):
        p = r.rig.process
        if "t" not in box and when(p):
            box["t"] = t.seq
        if box.get("t") is not None and t.seq == box["t"] + 3 and "f" not in box:
            box["f"] = True
            _fault(kind, r)
    r.rig.loop.tick_hooks.append(hook)
    r.run(260)
    return r


@pytest.mark.slow
@pytest.mark.parametrize("kind", ["dc_low", "rf_ref", "dc_comm", "mv_itl"])
def test_manual_wait_safety_rules_same_as_process(tmp_path, kind):
    m = _run_fault(tmp_path / "m", kind, True)
    p = _run_fault(tmp_path / "p", kind, False)
    (me,), (pe,) = m.ev("run_end"), p.ev("run_end")
    assert me.data["status"] == pe.data["status"] == "error"
    assert me.data["reason"] == pe.data["reason"] == EXPECT[kind], (me.data["reason"], pe.data["reason"])
    assert me.data["manual"] is True
    rules = lambda r: sorted({e.rule for e in r.rig.events if type(e).__name__ == "SafetyEvent" and e.kind == "abort"})
    assert rules(m) == rules(p)
    if kind != "dc_comm":                                                  # 두절은 링크가 없어 DC 출력을 확인할 수 없음
        _all_off(m)
    assert m.rig.process.state == "IDLE" and m.rig.process.manual is False


def test_manual_stop_and_all_stop_turn_everything_off(tmp_path):
    for kind, status, reason in (("PROCESS_STOP", "aborted", "USER_STOP"), ("ALL_STOP", "error", "ALL_STOP")):
        m = M(tmp_path / kind)
        m.blocks([GAS, SP1, DC_ON], end=False)
        m.queue.append(("hold", 3))
        m.queue.append((kind, {}))
        m.run(140)
        (e,) = m.ev("run_end")
        assert (e.data["status"], e.data["reason"], e.data["manual"]) == (status, reason, True), kind
        _all_off(m)
        assert m.rig.process.state == "IDLE"


def test_manual_end_while_block_running_stops_that_block(tmp_path):
    m = M(tmp_path)
    m.blocks([GAS, {"type": "pressure", "params": {"sp": 1, "value": 5.0, "timeout_sec": 600}}], end=False)
    box = {}

    def hook(t):
        p = m.rig.process
        if p.state == "PRESSURE_SP1" and "end" not in box:
            box["end"] = m.rig.core.command("MANUAL_END", {})
    m.rig.loop.tick_hooks.append(hook)
    m.run(80)
    assert box["end"].ok
    (e,) = m.ev("run_end")
    assert (e.data["status"], e.data["reason"]) == ("done", "MANUAL_END")
    assert [x["result"] for x in e.data["summary"]["executed"]] == ["done", "aborted"]
    _all_off(m)


# ───────── 거부 ─────────
def test_manual_rejections(tmp_path):
    r = R(tmp_path, FAST)
    out = {}

    def go():
        core = r.rig.core
        out["end"] = core.command("MANUAL_END", {})
        for t in BK.by_type():
            if t not in BK.MANUAL_BLOCK_TYPES:
                out[t] = core.command("MANUAL_BLOCK", {"block": {"type": t, "params": {}}})
        out["bad"] = core.command("MANUAL_BLOCK", {"block": {"type": "gas_intro", "params": {"ar": True, "ar_flow": 99999}}})
        out["none"] = core.command("MANUAL_BLOCK", {})
        out["proc"] = core.command("PROCESS_START", {"blocks": [GAS, {"type": "wait", "params": {"sec": 600}}]})
        return True

    def during():
        out["in_proc"] = r.rig.core.command("MANUAL_BLOCK", {"block": GAS})
        return True
    r.on(3, go)
    r.on(12, during)
    r.run(14)
    assert out["end"].reason == "MANUAL_NOT_ACTIVE"
    for t in BK.by_type():
        if t not in BK.MANUAL_BLOCK_TYPES:
            assert out[t].reason == "MANUAL_BLOCK_NOT_ALLOWED" and "쓸 수 있는 블록" in out[t].detail, t
    assert out["bad"].reason == "RECIPE_INVALID" and out["bad"].detail
    assert out["none"].reason == "BAD_ARGS"
    assert out["proc"].ok
    assert out["in_proc"].reason == "PROCESS_RUNNING" and "공정 중 값 바꾸기" in out["in_proc"].detail
    assert not r.rig.process.manual


def test_manual_start_needs_same_conditions_as_process_start(tmp_path):
    r = R(tmp_path, FAST)
    out = {}

    def go():
        r.plc.coils[A["mv"]] = False
        return True

    def later():
        out["mv"] = r.rig.core.command("MANUAL_BLOCK", {"block": GAS})
        out["start"] = r.rig.core.command("PROCESS_START", {"blocks": [GAS]})
        r.rig.core.command("RECIPE_LOAD", {"name": "x", "blocks": [GAS]})
        out["loaded"] = r.rig.core.command("MANUAL_BLOCK", {"block": GAS})
        return True
    r.on(2, go)
    r.on(6, later)
    r.run(8)
    assert out["mv"].reason == out["start"].reason == "MV_NOT_OPEN"
    assert out["loaded"].reason == "RECIPE_LOADED"


# ───────── 노트북 경로 ─────────
def test_manual_from_notebook_follows_hmi_control(tmp_path):
    m = M(tmp_path / "u", source=Source.HMI)
    m.blocks([GAS, GAS_OFF])
    m.run(80)
    assert all(x.ok for _, _, x in m.sent)
    rec = [e for e in m.pev("manual") if e.code == "MANUAL_BLOCK"]
    assert [e.data["source"] for e in rec] == ["local", "local"] and {e.data["by"] for e in rec} == {"local"}
    (rs,) = m.ev("run_start")
    assert rs.data["source"] == "local" and rs.data["by"] == "local"
    v = M(tmp_path / "v", cfg={"LOCAL_HMI_CONTROL": "view_stop"}, source=Source.HMI)
    v.blocks([GAS])
    v.run(6)
    assert [x.reason for _, _, x in v.sent] == ["HMI_CONTROL_OFF"] and not v.ev("run_start")
    assert C.by_kind()["MANUAL_BLOCK"].hmi == C.by_kind()["MANUAL_END"].hmi == "operate"


# ───────── S51·S10 ─────────
def test_s51_and_s10_do_not_turn_off_manual_outputs(tmp_path):
    from v2.chk.safety import View
    m = M(tmp_path)
    m.blocks([GAS, SP1, DC_ON], end=False)
    box = {}

    def hook(t):
        p, s = m.rig.process, m.rig.safety
        if p.state == "MANUAL_WAIT" and len(p.exec_log) == 3 and "done" not in box:
            box["done"] = True
            s.first_done = set()                                          # 장비별 첫 읽기를 다시(프로그램 시작 때처럼)
            m.rig.dc.out_at_connect = True                                # DC 가 켜져 있던 것처럼
            p.comm_aborted = True
            box["s10"] = s.event("plc_link_up", s.plc_view())
            box["seq"] = t.seq
    m.rig.loop.tick_hooks.append(hook)
    m.run(80)
    s = m.rig.safety
    assert "S10" not in box["s10"]
    assert not any(code == "DC_RESIDUAL_OUTPUT" and act for dev, code, act in s.residual)
    assert m.rig.sims["dc"].out is True and m.rig.process.state == "MANUAL_WAIT"           # 끄지 않음
    note = [n for n in s.notifications if n["code"] == "DC_RESIDUAL_OUTPUT"]
    assert note and "끄지 않음" in note[-1]["detail"]


def test_commands_table_and_bundle():
    by = C.by_kind()
    for k in ("MANUAL_BLOCK", "MANUAL_END"):
        c = by[k]
        assert c.role == "operator" and not c.bench_ok and Source.WEB in c.sources and c.hmi == "operate"
    assert {"MANUAL_ACTIVE", "MANUAL_BUSY", "MANUAL_BLOCK_NOT_ALLOWED", "MANUAL_NOT_ACTIVE", "MANUAL_BLOCK",
            "MANUAL_END"} <= C.reason_codes()
    from v2.chk import params as P
    from v2.chk import process_states as PS
    from v2.chk import web_schema as W
    from v2.common.paramdef import defaults
    assert "MANUAL_WAIT" in PS.ACTIVE_STATES and PS.by_name()["MANUAL_WAIT"].label == "수동 대기"
    b = W.bundle(defaults(P.PARAMS))
    assert b["manual"]["blockTypes"] == list(BK.MANUAL_BLOCK_TYPES) and b["manual"]["label"] == "수동 조작 중"
    assert ("process", "manual") in {(e["source"], e["kind"]) for e in b["eventKinds"]}
    assert "proc.manual" in b["snapshot"]["columns"]
    # 기존 블록 공정은 그대로 — 자동 끝 정지를 붙인다
    steps, insts = BK.expand([GAS], defaults(P.PARAMS))
    assert insts[-1].auto and steps[-1].action == "END_STOP"
    steps, insts = BK.expand([GAS], defaults(P.PARAMS), auto_end=False)
    assert not any(i.auto for i in insts) and steps[-1].action != "END_STOP"
