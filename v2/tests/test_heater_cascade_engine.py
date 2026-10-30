"""2단(캐스케이드) 제어의 HeaterEngine·공정 연결(5-15, v1 3f4cdef·3328872) — 시험대(ChkRig, 가상 시계·시뮬레이터)와 단위.

- 수동 ON/OFF·운전 중 목표 바꾸기: PLC 에 가는 쓰기 순서(v1 test_plc_comm T120·golden 28a 의 v2 형식 — golden/cascade_28a.json).
- 공정: 기본 템플릿(v1 행)에 셔터 전 정착이 v1 스텝 목록과 같은 자리·문구, 도달 대기는 r1 도달 + TI×2 여유,
  유지 모드 대기는 TC2 제어 확인(아니면 HEATER_HOLD_FAIL_ACTION), 셔터 전 정착 통과·상한 초과(proceed·abort).
- TC1 탈락 의심 조치(공정 소유 → 공정 중단 / 히터 레시피 → 레시피 중단 / 수동 → 경고만).
골든 갱신: CHK_UPDATE_GOLDEN=1 일 때만 파일을 다시 쓴다.
"""
import json
import os
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.chk import blocks as BK
from v2.chk import params as P
from v2.chk.heater_engine import HeaterEngine
from v2.chk.process_steps import row_to_params
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig

pytestmark = pytest.mark.cascade
GOLDEN = Path(__file__).resolve().parent / "golden" / "cascade_28a.json"
REG = {A["heater_sv"]: "D00012", A["heater_sv2"]: "D00035", A["heater_ramp_rate"]: "D00020",
       A["heater_mv_limit"]: "D00018"}
COIL = {A["heater_run"]: "RUN", A["heater_pv_sel"]: "M0004A"}


def _rig(tmp_path, **cfg):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, cfg=cfg or None)
    for k in ("mv", "turbo", "rotary", "fv"):
        rig.sims["plc"].coils[A[k]] = True
    return c, rig


def _writes(plc, n0=0):
    """PLC 시뮬레이터가 받은 히터 쓰기(FC5·FC6) — (이름, 값)."""
    out = []
    for _, f in list(plc.frames)[n0:]:
        if len(f) < 6:
            continue
        fc, addr, val = f[1], (f[2] << 8) | f[3], (f[4] << 8) | f[5]
        if fc == 6 and addr in REG:
            out.append((REG[addr], val / 10.0 if REG[addr] in ("D00012", "D00035") else val))
        elif fc == 5 and addr in COIL:
            out.append((COIL[addr], val == 0xFF00))
    return out


def _run(rig, n, hooks=None):
    if hooks:
        rig.loop.tick_hooks.append(lambda t: hooks.get(t.seq, lambda: None)())
    assert rig.start()
    try:
        rig.loop.run(n)
    finally:
        rig.stop()


def test_manual_on_engage_retarget_off_order_golden_28a(tmp_path):
    """28a(v2 형식): 수동 ON 300 → D00012 ← 지금 TC1(출발점), RUN → D00035 ← SV2 → (되읽기) M0004A → (M0004B) TC2 제어 →
    30 s 뒤 r1 이 6 °C/min 이면 3 °C 오른다(여기는 설정 12 °C/min). 운전 중 목표 250 은 끊지 않고 목표만. 수동 OFF 는
    M0004A OFF → D00035=0 → RUN OFF 순서."""
    c, rig = _rig(tmp_path)
    plc = rig.sims["plc"]
    rec, mark = [], {}

    def on():
        mark["n0"] = len(plc.frames)
        rec.append(("cmd", "HEATER_ON", rig.core.command("HEATER_ON", {"value": 300.0}).reason))

    def at_engaged():
        h = rig.heater
        rec.append(("state", h.casc.state, round(h.casc.r1, 1), h.casc.sv2))

    def retarget():
        rec.append(("cmd", "HEATER_SV", rig.core.command("HEATER_SV", {"value": 250.0}).reason))
        rec.append(("state", rig.heater.casc.state, rig.heater.casc.target()))

    def off():
        mark["n1"] = len(plc.frames)
        rec.append(("cmd", "HEATER_OFF", rig.core.command("HEATER_OFF", {}).reason))
    _run(rig, 50, {3: on, 10: at_engaged, 40: retarget, 45: off})
    first = _writes(plc, mark["n0"])
    seq = [k for k, _ in first[:5]]
    assert seq[:2] == ["D00012", "D00020"] or seq[:2] == ["D00020", "D00012"]
    assert first[[k for k, _ in first].index("D00012")][1] == 25.0          # 출발점 = 지금 TC1
    names = [k for k, _ in first]
    assert names.index("RUN") < names.index("D00035") < names.index("M0004A")
    assert rec[1][1] == "running" and rec[1][3] is not None
    assert rec[3][1] == "running" and rec[3][2] == 250.0                     # 끊지 않고 목표만
    last = _writes(plc, mark["n1"])
    assert [k for k, _ in last][:3] == ["M0004A", "D00035", "RUN"] and last[:3] == [("M0004A", False), ("D00035", 0.0),
                                                                                   ("RUN", False)]
    assert rig.heater.casc.state == "idle"
    got = {"timeline": rec, "on_writes": [list(x) for x in first[:6]], "off_writes": [list(x) for x in last[:3]]}
    got = json.loads(json.dumps(got))
    if os.environ.get("CHK_UPDATE_GOLDEN") == "1" or not GOLDEN.exists():
        GOLDEN.write_text(json.dumps(got, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
    assert got == json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_snapshot_row_has_cascade_fields(tmp_path):
    c, rig = _rig(tmp_path)
    rows = []
    real = rig.hub.on_row
    rig.hub.on_row = lambda row: (rows.append(dict(row)), real(row))[1]
    _run(rig, 20, {3: lambda: rig.core.command("HEATER_ON", {"value": 300.0})})
    r = rows[-1]
    assert r["heater.ctrl_mode"] == "cascade" and r["heater.casc_state"] == "running"
    assert r["heater.r1"] > 25.0 and r["heater.sv2"] is not None and r["heater.bias"] is not None
    assert r["heater.hold"] == "off"                                         # 2단 제어면 유지 모드는 쓰지 않는다


def test_template_inserts_heater_settle_like_v1_steps():
    """v1 _build_steps(3328872): 히터 사용·cascade·process_time > 0·HEATER_SHUTTER_SETTLE_MAX_SEC > 0 이면
    Shutter Delay 뒤·Main Shutter Open 앞에 HEATER_SETTLE(value=heater_temp, polling). 문구도 v1 그대로."""
    cfg = dict(defaults(P.PARAMS))
    row = {"process_name": "T", "use_heater": True, "heater_temp": 300.0, "ar": True, "ar_flow": 20.0,
           "working_pressure": 5.0, "use_dc": True, "dc_power": 100.0, "use_dc": True, "dc_power": 100.0, "shutter_delay": 1.0, "process_time": 2.0}
    p = row_to_params(row, cfg)
    steps, _ = BK.expand(BK.template_blocks(p, cfg, ig_enabled=False), cfg, ig_enabled=False)
    acts = [s.action for s in steps if not s.internal]
    msgs = [s.message for s in steps if not s.internal]
    i = acts.index("HEATER_SETTLE")
    assert msgs[i - 1].startswith("Shutter Delay") and msgs[i + 1] == "Main Shutter Open"
    s = [x for x in steps if x.action == "HEATER_SETTLE"][0]
    assert s.message == "셔터 전 히터 정착 확인 (TC1 ±2°C, 60s 연속, 상한 900s)"
    assert s.value == 300.0 and s.polling and s.timer_purpose is None and s.state == "HEATER_SETTLE"
    for extra in ({"HEATER_SHUTTER_SETTLE_MAX_SEC": 0.0}, {"HEATER_CTRL_MODE": "tc1"}):
        c2 = dict(cfg, **extra)
        st2, _ = BK.expand(BK.template_blocks(p, c2, ig_enabled=False), c2, ig_enabled=False)
        assert "HEATER_SETTLE" not in [x.action for x in st2]
    p0 = row_to_params(dict(row, process_time=0.0), cfg)
    st3, _ = BK.expand(BK.template_blocks(p0, cfg, ig_enabled=False), cfg, ig_enabled=False)
    assert "HEATER_SETTLE" not in [x.action for x in st3]


def test_process_row_cascade_reaches_settles_and_runs_main(tmp_path):
    """v1 행(히터 100 °C·셔터 딜레이·메인) 끝까지 — 도달 대기 r1 도달, 유지 모드 대기는 TC2 제어 확인, 셔터 전 정착 통과 뒤 메인."""
    c, rig = _rig(tmp_path)
    row = {"process_name": "C", "use_heater": True, "heater_temp": 100.0, "ar": True, "ar_flow": 20.0,
           "working_pressure": 5.0, "use_dc": True, "dc_power": 100.0, "shutter_delay": 0.5, "process_time": 0.5}
    _run(rig, 3600, {3: lambda: rig.core.command("RECIPE_LOAD", {"name": "c", "rows": [row]}),
                     4: lambda: rig.core.command("RECIPE_START", {})})
    ev = [e for e in rig.process.events]
    ends = [(e.data["status"], e.data["reason"]) for e in ev if e.kind == "run_end"]
    assert ends == [("done", None)], [e.detail for e in ev if e.kind in ("warn",)]
    msgs = [e.detail for e in ev if e.kind == "message"]
    assert any(m.startswith("셔터 전 히터 정착 확인 — TC1") for m in msgs)
    hev = [e.detail for e in rig.heater.events if e.kind == "message"]
    assert any("캐스케이드 TC2 제어 확인" in m for m in hev)
    steps = [e.detail for e in ev if e.kind == "step"]
    i = next(k for k, d in enumerate(steps) if "셔터 전 히터 정착 확인 (TC1 ±2°C" in d)
    assert "Shutter Delay" in steps[i - 1] and "Main Shutter Open" in steps[i + 1]
    assert rig.heater.casc.state == "idle"                     # 정지 순서가 히터를 끄며 해제


def _settle_blocks(**sp):
    return [{"type": "heater_ramp", "params": {"target_c": 200.0}},
            {"type": "heater_settle", "params": dict({"sec": 600.0, "max_sec": 20.0}, **sp)},
            {"type": "wait", "params": {"sec": 2}}]


@pytest.mark.parametrize("action,end", [("abort", ("error", "HEATER_SETTLE_FAILED")), ("proceed", ("done", None))])
def test_settle_block_over_max(tmp_path, action, end):
    c, rig = _rig(tmp_path)
    _run(rig, 200, {3: lambda: rig.core.command("PROCESS_START", {"blocks": _settle_blocks(fail_action=action)})})
    ev = rig.process.events
    assert [(e.data["status"], e.data["reason"]) for e in ev if e.kind == "run_end"] == [end]
    code = "HEATER_SETTLE_FAILED" if action == "abort" else "HEATER_SETTLE_TIMEOUT"
    w = [e for e in ev if e.kind == "warn" and e.code == code]
    assert len(w) == 1 and "셔터 전 정착 미달" in w[0].detail and w[0].data["action"] == action


@pytest.mark.parametrize("cascade", [True, False])
def test_settle_block_skipped_when_cascade_not_active(tmp_path, monkeypatch, cascade):
    """2단 제어가 돌고 있지 않으면(히터 목표 없음·tc1 모드) 경고 뒤 다음(v1: 캐스케이드 비활성 경고 통과)."""
    if not cascade:
        from v2.chk import heater_cascade as HC
        monkeypatch.setattr(HC, "ctrl_mode", lambda cfg: "tc1")
    c, rig = _rig(tmp_path)
    blocks = ([{"type": "heater_ramp", "params": {"target_c": 200.0}}] if not cascade else []) + \
        [{"type": "heater_settle", "params": {}}, {"type": "wait", "params": {"sec": 1}}]
    _run(rig, 40, {3: lambda: rig.core.command("PROCESS_START", {"blocks": blocks})})
    ev = rig.process.events
    assert [(e.data["status"], e.data["reason"]) for e in ev if e.kind == "run_end"] == [("done", None)]
    assert [e.code for e in ev if e.kind == "warn" and e.code == "HEATER_SETTLE_SKIPPED"] == ["HEATER_SETTLE_SKIPPED"]


def test_hold_wait_not_engaged_applies_fail_action(tmp_path):
    """래더가 TC2 제어를 지원하지 않음(D00036=0) → 캐스케이드는 TC1 제어로 도달 → 유지 모드 대기 한도 뒤 HEATER_HOLD_FAIL_ACTION."""
    c, rig = _rig(tmp_path, HEATER_HOLD_WAIT_SEC=5.0)
    rig.sims["plc"].forced_regs[A["heater_sv2_max"]] = 0
    blocks = [{"type": "heater_ramp", "params": {"target_c": 60.0}}, {"type": "heater_wait", "params": {}}]
    _run(rig, 2400, {3: lambda: rig.core.command("PROCESS_START", {"blocks": blocks})})
    ends = [(e.data["status"], e.data["reason"]) for e in rig.process.events if e.kind == "run_end"]
    assert ends == [("error", "HEATER_HOLD_FAILED")]
    al = [e for e in rig.heater.events if e.kind == "alert"]
    assert any(e.code == "HEATER_TC2_LOST" for e in al)
    assert any(e.code == "HEATER_HOLD_FAILED" and "TC2 제어가 아님(상태 tc1)" in e.detail for e in al)


# ───────────────────────── 단위(가짜 PLC) ─────────────────────────
class FakePlc:
    def __init__(self):
        self.calls = []

    def heater_set_sv(self, c, on_done=None):
        self.calls.append(("sv", c))

    def heater_set_reg(self, name, raw, on_done=None, urgent=False):
        self.calls.append((name, raw))

    def heater_run(self, on, on_done=None):
        self.calls.append(("run", on))

    def heater_set_pv_sel(self, on, on_done=None, urgent=False):
        self.calls.append(("sel", on))


def _engine(proc_active=True, **cfg):
    c = VirtualClock(start_wall=1_760_000_000.0)
    aborts = []
    h = HeaterEngine(c, dict(defaults(P.PARAMS), **cfg), FakePlc(), None, process_active=lambda: proc_active,
                     on_casc_abort=aborts.append)
    return h, aborts


@pytest.mark.parametrize("owner,recipe,proc,expect", [
    ("process", False, True, "공정 중단"), (None, True, False, "히터 레시피 중단(히터 OFF)"),
    ("manual", False, False, "경고만(수동 히터 — 계속 운전)")])
def test_tc1_detached_action_by_owner(owner, recipe, proc, expect):
    h, aborts = _engine(proc_active=proc)
    h.owner = owner
    if recipe:
        from v2.chk.heater_engine import RecipeStep
        h.st = {"itl": True, "fault": False, "pv": 300.0, "run": True}
        h.recipe.load("r", [RecipeStep(1, 300.0, 6.0, 10.0)])
        assert h.recipe.start()
    h._casc_alert("tc1_detached", {"text": "TC1 이 시료대에서 떨어졌거나 …", "action": "abort"})
    h._flush()
    al = [e for e in h.events if e.kind == "alert"][-1]
    assert al.code == "HEATER_TC1_DETACHED" and al.data["action"] == expect
    assert aborts == (["HEATER_TC1_DETACHED"] if owner == "process" else [])
    if recipe:
        assert not h.recipe.is_running() and h.recipe.finished[1] == "HEATER_TC1_DETACHED"
    h2, ab2 = _engine()
    h2.owner = "process"
    h2._casc_alert("tc1_detached", {"text": "x", "action": "warn"})
    h2._flush()
    assert ab2 == [] and [e for e in h2.events if e.kind == "alert"][-1].data["action"] is None


def test_process_wait_timeout_adds_two_ti_and_uses_r1():
    h, _ = _engine()
    h.st = {"pv": 25.0, "run": False, "fault": False, "sv_ramp": 25.0}
    h.process_ramp(12.0)
    h.process_set(300.0)
    assert h.casc.is_active() and h.casc.target() == 300.0 and not h.ramp.is_active()
    assert ("run", True) in h.plc.calls
    assert h.process_wait_tick(0.0) == "wait"
    tc1 = h.proc["timeout"]
    h2, _ = _engine()
    from v2.chk import heater_cascade as HC
    h2.cfg["HEATER_CTRL_MODE"] = "tc1"
    if HC.ctrl_mode(h2.cfg) == "tc1":
        h2.st = dict(h.st)
        h2.process_ramp(12.0)
        h2.process_set(300.0)
        h2.process_wait_tick(0.0)
        assert tc1 - h2.proc["timeout"] == pytest.approx(2 * 600.0)
    # TC1 이 목표 안이어도 r1 이 목표에 닿기 전에는 도달이 아니다(D00019 는 SV2 라 쓰지 않는다)
    h.st = {"pv": 300.0, "run": True, "fault": False, "sv_ramp": 300.0}
    h.process_wait_tick(1.0)
    assert h.proc["phase"] == "RAMP"


def test_manual_off_and_set_sv_routes():
    h, _ = _engine(proc_active=False)
    h.st = {"pv": 100.0, "run": False, "itl": True, "fault": False}
    assert h.manual_on(300.0) is None and h.casc.is_active() and not h.ramp.is_active()
    h.st["run"] = True
    h.set_sv(250.0)
    assert h.casc.target() == 250.0
    h.plc.calls.clear()
    h.manual_off()
    assert h.casc.state == "idle" and h.plc.calls[-1] == ("run", False)
    h.st["run"] = False
    h.manual_on(300.0)
    h.set_sv(200.0)                                   # 꺼져 있으면 목표만 기록(캐스케이드 해제)
    assert h.casc.state == "idle" and h.plc.calls[-1] == ("sv", 200.0)
