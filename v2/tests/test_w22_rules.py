"""W-22 A·B·C·D — 수동 조작 규칙·영점 보호·RF 표시 판정·반사파 대기·RF 운전 상한. 가상 시계·시뮬레이터만(실제 포트 없음).

A 1) 가스가 켜져 있으면 가스 주입 거부(GAS_ALREADY_ON) 2) 영점(L5~L8·Z1)은 유량 꺼짐·가스 밸브 닫힘·MFC 밸브 열림이
     ZERO_MIN_IDLE_SEC 이어졌을 때만 — 아니면 건너뛰고 경고(ZERO_SKIPPED) 3) 이미 그 상태인 셔터 SHUTTER_ALREADY
  5) 블록 진행 중 파워 끄기·셔터 닫기만 끼어든다(MANUAL_INTERRUPT), 나머지는 MANUAL_BUSY 6) 일시정지·재개·건너뛰기·끼어들기 뒤
     바로 run.blocks
B rf.cmd_on·rf.uncommanded, 명령 없는 출력 사건은 시작·끝 한 번씩
C 반사파 대기 — 사건 시작·끝 한 번씩, 여유 폭(RF_REFP_HYST_W) 아래로 내려가야 풀림
D RF_OPERATING_MAX_W — 레시피·블록·공정 중 수정·수동 조작 모두 같은 상한, RF_MAX_POWER 는 환산 기준만
"""
import pytest

from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import recipe as RC
from v2.chk.power_control import RfControl
from v2.chk.sim.plc_sim import A
from v2.common.paramdef import defaults
from v2.tests.test_manual import DC_OFF, DC_ON, GAS, GAS_OFF, M
from v2.tests.test_process_engine import FAST, R, SHORT

CFG = defaults(P.PARAMS)
RF_ON = {"type": "power_on", "params": {"rf_power": 100.0}}
RF_OFF = {"type": "power_off", "params": {"rf": True}}
G1_OPEN = {"type": "shutter", "params": {"action": "open", "targets": ["g1"]}}
G1_CLOSE = {"type": "shutter", "params": {"action": "close", "targets": ["g1"]}}
G12_CLOSE = {"type": "shutter", "params": {"action": "close", "targets": ["g1", "g2"]}}


def _mfc_writes(r):
    return [d.decode().strip() for t, d in r.rig.sims["mfc"].received if not d.startswith(b"R")]


# ───────── A1 가스 주입 거부 ─────────
def test_gas_intro_rejected_when_gas_on(tmp_path):
    m = M(tmp_path / "a")
    m.blocks([GAS, GAS], end=False)
    m.run(60)
    (t1, _, a), (t2, _, b) = m.sent
    assert a.ok and b.reason == "GAS_ALREADY_ON" and b.detail == "유량 켜짐"
    assert "유량은 값 바꾸기로" in C.REASON_HELP["GAS_ALREADY_ON"]
    # 유량은 꺼졌는데 가스 밸브가 열려 있으면 — 밸브 열림
    m = M(tmp_path / "b")
    m.queue.append(("MANUAL_BLOCK", {"block": {"type": "valve", "params": {"action": "open"}}}))
    m.run(5)
    m2 = M(tmp_path / "c")
    m2.rig.sims["plc"].coils[A["ar_valve"]] = True
    m2.blocks([GAS], end=False)
    m2.run(10)
    assert m2.sent[0][2].reason == "GAS_ALREADY_ON" and m2.sent[0][2].detail == "가스 밸브 열림"


# ───────── A2 영점 보호 ─────────
def test_zero_skipped_when_gas_just_stopped(tmp_path):
    """가스를 끈 바로 뒤 다시 가스 주입 — 유량 꺼짐이 ZERO_MIN_IDLE_SEC 이어지지 않아 영점을 건너뛰고 경고 한 번."""
    m = M(tmp_path, cfg={"IG_ENABLED": False}, start=15)                  # 첫 가스 주입 전에 조건이 10 s 넘게 이어짐
    m.blocks([GAS, GAS_OFF, GAS], end=False)
    m.run(120)
    assert all(x.ok for _, _, x in m.sent), m.sent
    w = _mfc_writes(m)
    assert w.count("Z1") == 1 and w.count("L5 1") == 1 and w.index("Z1") < w.index("L01000"), w   # 두 번째 가스 주입은 영점 없음
    warns = [e for e in m.ev("warn") if e.code == "ZERO_SKIPPED"]
    assert len(warns) == 1 and warns[0].detail.startswith("영점 건너뜀 — 가스가 흐르는 중(")
    assert C.REASON_HELP["ZERO_SKIPPED"].endswith("요.")


def test_zero_skipped_in_process_when_hold_not_reached(tmp_path):
    r = R(tmp_path, dict(FAST, IG_ENABLED=False, ZERO_MIN_IDLE_SEC=600.0))
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(60)
    w = _mfc_writes(r)
    assert not [x for x in w if x in ("L5 1", "L6 1", "Z1")], w
    warns = [e for e in r.ev("warn") if e.code == "ZERO_SKIPPED"]
    assert len(warns) == 1                                               # 블록마다 한 번(MFC·압력 센서 영점 둘이어도)


def test_zero_sent_after_flow_off_hold(tmp_path):
    """공정 시작은 유량 끄기 → MFC 밸브 열기(대기) → 영점 — 조건이 ZERO_MIN_IDLE_SEC 이어지면 보낸다."""
    r = R(tmp_path, dict(FAST, IG_ENABLED=False, ZERO_MIN_IDLE_SEC=3.0))
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(60)
    w = _mfc_writes(r)
    assert "Z1" in w and "L5 1" in w and w.index("L00000") < w.index("L5 1"), w
    assert not [e for e in r.ev("warn") if e.code == "ZERO_SKIPPED"]


def test_zero_blocked_by_gauge_a_when_ig_connected(tmp_path):
    r = R(tmp_path, dict(FAST, IG_ENABLED=True, ZERO_MIN_IDLE_SEC=1.0))
    r.on(1, lambda: setattr(r.rig.sims["ig"], "conv_a_torr", 5.0) if hasattr(r.rig.sims["ig"], "conv_a_torr") else None)
    p = r.rig.process
    view = {"plc.ar_valve": False, "plc.o2_valve": False, "mfc.valve_pct": 100.0, "ig.q": "ok", "ig.a_state": "ok"}
    r.run(3)
    p.mfc.onoff_bits = "0000"
    assert p._zero_cond(view) == "Gauge A 가 범위 아래가 아님"
    assert p._zero_cond(dict(view, **{"ig.a_state": "below"})) == ""
    assert p._zero_cond(dict(view, **{"ig.q": "stale"})) == ""                      # 게이지를 못 읽으면 Gauge 조건은 보지 않는다
    assert p._zero_cond(dict(view, **{"plc.o2_valve": True})) == "가스 밸브 열림"
    assert p._zero_cond(dict(view, **{"mfc.valve_pct": 50.0})) == "MFC 밸브가 열려 있지 않음"
    p.mfc.onoff_bits = "1000"
    assert p._zero_cond(view) == "유량 켜짐"
    p.mfc.onoff_bits = None
    assert p._zero_cond(view) == "유량 켜짐 상태를 모름"
    assert P.by_key()["ZERO_MIN_IDLE_SEC"].default == 10.0


# ───────── A3 셔터 같은 상태 ─────────
def test_shutter_same_state_rejected(tmp_path):
    m = M(tmp_path)
    m.queue.append(("MANUAL_BLOCK", {"block": G12_CLOSE}))
    m.run(5)
    assert m.sent[0][2].reason == "SHUTTER_ALREADY" and m.sent[0][2].detail == "이미 닫혀 있음"   # 처음엔 모두 닫힘
    m = M(tmp_path / "b")
    m.blocks([G1_OPEN, G1_OPEN, G1_CLOSE, G1_CLOSE, G12_CLOSE], end=False)
    m.run(60)
    rs = [x for _, _, x in m.sent]
    assert rs[0].ok and rs[1].reason == "SHUTTER_ALREADY" and rs[1].detail == "이미 열려 있음"
    assert rs[2].ok and rs[3].reason == "SHUTTER_ALREADY" and rs[4].reason == "SHUTTER_ALREADY"


# ───────── A5 끼어들기 ─────────
def test_interrupt_power_off_and_shutter_close_only(tmp_path):
    r = M(tmp_path, cfg={"RF_RAMP_STEP_W": 1.0})
    out = {}
    core = r.rig.core
    r.on(3, lambda: out.setdefault("rf", core.command("MANUAL_BLOCK", {"block": RF_ON})))        # 램프가 길다(1 W/s)
    r.on(12, lambda: out.setdefault("gas", core.command("MANUAL_BLOCK", {"block": GAS})))
    r.on(12, lambda: out.setdefault("open", core.command("MANUAL_BLOCK", {"block": G1_OPEN})))
    r.on(13, lambda: out.setdefault("off", core.command("MANUAL_BLOCK", {"block": RF_OFF})))
    r.run(40)
    assert out["rf"].ok and out["gas"].reason == "MANUAL_BUSY" and out["open"].reason == "MANUAL_BUSY"
    assert out["off"].ok
    rec = [e for e in r.pev("manual") if e.code == "MANUAL_INTERRUPT"]
    assert len(rec) == 1 and rec[0].data["type"] == "power_on" and rec[0].data["result"] == "aborted"
    ends = [e.data for e in r.ev("block") if e.data.get("phase") == "end"]
    assert any(d["type"] == "power_on" and d["result"] == "aborted" and d["reason"] == "MANUAL_INTERRUPT" for d in ends)
    assert r.rig.sims["plc"].regs[A["rf_dac"]] == 0 and r.rig.process.state == "MANUAL_WAIT"
    # 셔터 닫기도 끼어든다
    r = M(tmp_path / "s", cfg={"RF_RAMP_STEP_W": 1.0})
    core = r.rig.core
    r.on(3, lambda: out.setdefault("g1", core.command("MANUAL_BLOCK", {"block": G1_OPEN})))
    r.on(10, lambda: out.setdefault("rf2", core.command("MANUAL_BLOCK", {"block": RF_ON})))
    r.on(16, lambda: out.setdefault("close", core.command("MANUAL_BLOCK", {"block": G1_CLOSE})))
    r.run(30)
    assert out["g1"].ok and out["rf2"].ok and out["close"].ok, out
    assert [e for e in r.pev("manual") if e.code == "MANUAL_INTERRUPT"]
    assert r.rig.sims["plc"].coils[A["s1"]] is False


# ───────── A6 바로 run.blocks ─────────
def test_run_blocks_sent_right_after_pause_skip(tmp_path):
    r = R(tmp_path, dict(FAST))
    full = []
    r.rig.loop.tick_hooks.append(lambda t: full.append((t.seq, "blocks" in ((r.rig.hub.last_state or {}).get("data", {}).get("run") or {}))))
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))

    def pause():
        st = r.rig.process.run_state(False, False)
        return r.rig.core.command("PROCESS_PAUSE", {"no": st["current"], "revision": st["revision"]})
    r.on(24, pause)
    r.run(27)
    assert r.results[-1].ok, r.results
    seen = dict(full)
    assert seen[25] or seen[26], full[-6:]                              # 10초 주기를 기다리지 않는다


# ───────── B RF 명령·명령 없는 출력 ─────────
def test_rf_cmd_on_and_uncommanded_events_once(tmp_path):
    r = R(tmp_path, dict(FAST))
    plc = r.rig.sims["plc"]
    rows = []
    r.rig.loop.tick_hooks.append(lambda t: rows.append(dict(r.rig.hub.last_state["data"]["snapshot"])) if r.rig.hub.last_state else None)
    r.on(5, lambda: setattr(plc, "rf_local_w", 50.0))                  # 앞면(LOCAL)에서 켬 — 명령 없음
    r.on(15, lambda: setattr(plc, "rf_local_w", 0.0))
    r.run(25)
    unc = [x.get("rf.uncommanded") for x in rows]
    assert True in unc and unc[-1] is False
    assert all(x.get("rf.cmd_on") is False for x in rows if x.get("rf.cmd_on") is not None)
    evs = [m["data"] for m in r.rig.hub.recent if m["type"] == "event" and m["data"]["context"]["source"] == "plc"
           and m["data"]["context"]["kind"] in ("rf_uncommanded", "rf_uncommanded_end")]
    assert [e["code"] for e in evs] == ["RF_UNCOMMANDED", "RF_UNCOMMANDED_END"]
    assert "RF 전원 앞면(LOCAL)에서 켰는지 확인" in evs[0]["message"]
    # 명령으로 켜면 cmd_on, 명령 없는 출력 아님
    r = R(tmp_path / "b", dict(FAST))
    rows2 = []
    r.rig.loop.tick_hooks.append(lambda t: rows2.append(dict(r.rig.hub.last_state["data"]["snapshot"])) if r.rig.hub.last_state else None)
    r.on(3, r.cmd("PROCESS_START", {"blocks": [RF_ON]}))
    r.run(40)
    on = [x for x in rows2 if x.get("rf.cmd_on")]
    assert on and all(x.get("rf.uncommanded") is False for x in on)
    assert P.by_key()["RF_UNCOMMANDED_FWD_W"].default == 5.0


# ───────── C 반사파 대기 ─────────
class _Plc:
    def __init__(self):
        self.dac = []
        self._rf_set_w = 0.0

    def rf_write_dac(self, v):
        self.dac.append(v)


def test_refp_wait_events_once_and_hysteresis():
    calls = []
    c = dict(CFG, RF_REFP_ABORT_THRESHOLD=3.0, RF_REFP_HYST_W=1.0)
    rf = RfControl(c, _Plc(), lambda lv, t: None, on_refp=lambda ph, d: calls.append((ph, d)))
    rf.start(100.0, 0.0, 1.0)
    for ref in (1.0, 3.5, 2.5, 3.2, 2.1, 2.0):                          # 기준 3 W, 여유 1 W — 2.0 W 까지는 이어서 대기
        rf.tick(10.0, ref)
    assert [p for p, _ in calls] == ["start"] and rf.ref_waiting
    assert calls[0][1] == {"ref_w": 3.5, "threshold": 3.0, "max_s": CFG["RF_REFP_WAIT_SEC"]}
    rf.tick(10.0, 1.9)
    assert [p for p, _ in calls] == ["start", "end"] and calls[1][1]["why"] == "풀림" and not rf.ref_waiting
    rf.tick(10.0, 3.0)
    rf.stop()
    assert [p for p, _ in calls] == ["start", "end", "start", "end"] and calls[-1][1]["why"] == "RF 끔"
    assert P.by_key()["RF_REFP_HYST_W"].default == 1.0


def test_refp_wait_process_events_and_s32_continues(tmp_path):
    r = R(tmp_path, dict(FAST, RF_REFP_WAIT_SEC=60))
    plc = r.rig.sims["plc"]
    r.on(3, r.cmd("PROCESS_START", {"blocks": [RF_ON, {"type": "wait", "params": {"sec": 60}}]}))
    r.on(30, lambda: setattr(plc, "ref_inject_w", 4.0))                 # 기준 3 W 넘음 → 대기 시작
    r.on(36, lambda: setattr(plc, "ref_inject_w", 1.6))                 # 2~3 W 사이(여유 폭 안) — 이어서 대기
    r.on(42, lambda: setattr(plc, "ref_inject_w", 0.0))                 # 풀림
    r.run(50)
    assert all(x.ok for x in r.results if x is not None), r.results
    ev = r.ev("refp_wait")
    assert [e.data["phase"] for e in ev] == ["start", "end"], [e.detail for e in ev]
    assert ev[0].code == "RF_REFP_WAIT" and ev[0].detail.startswith("반사파 대기 시작 · ") and "최대 60 s" in ev[0].detail
    assert ev[1].code == "RF_REFP_WAIT_END" and ev[1].data["why"] == "풀림"
    assert not r.ev("run_end") or r.ev("run_end")[0].data["status"] != "aborted"


# ───────── D RF 운전 상한 ─────────
def test_operating_max_everywhere(tmp_path):
    p = P.by_key()
    assert p["RF_MAX_POWER"].label == "RF DAC 풀스케일(환산 기준)" and "상한으로 쓰지 않는다" in p["RF_MAX_POWER"].desc
    assert (p["RF_OPERATING_MAX_W"].default, p["RF_OPERATING_MAX_W"].min, p["RF_OPERATING_MAX_W"].max) == (600.0, 1.0, 600.0)
    assert p["RF_REF_FULL_SCALE_W"].default == 600.0 and "2026-11-25" in p["RF_REF_FULL_SCALE_W"].desc
    cfg = dict(CFG, RF_OPERATING_MAX_W=300.0)
    # 레시피 열
    _, errs, _ = RC.check_process_row(dict(SHORT, use_rf=True, rf_power=350.0, use_dc=False), cfg)
    assert errs and any("300" in e for e in errs)
    _, errs, _ = RC.check_process_row(dict(SHORT, use_rf=True, rf_power=300.0, use_dc=False), cfg)
    assert not errs
    # 블록
    _, errs, _ = BK.check_blocks([{"type": "power_on", "params": {"rf_power": 350.0}}], cfg)
    assert errs and any("300" in e for e in errs)
    assert not BK.check_blocks([{"type": "power_on", "params": {"rf_power": 300.0}}], cfg)[1]
    # 공정 중 수정 항목
    item = [a for a in BK.ADJUST_ITEMS if (a.device, a.item) == ("rf", "power")][0]
    assert item.public()["param"]["paramMax"] == "RF_OPERATING_MAX_W"
    # 수동 조작·공정 중 수정(조립)
    r = M(tmp_path, cfg={"RF_OPERATING_MAX_W": 300.0})
    out = {}
    core = r.rig.core
    r.on(3, lambda: out.setdefault("manual", core.command("MANUAL_BLOCK", {"block": {"type": "power_on", "params": {"rf_power": 350.0}}})))
    r.on(4, lambda: out.setdefault("ok", core.command("MANUAL_BLOCK", {"block": RF_ON})))
    r.on(8, lambda: out.setdefault("adj", core.command("PROCESS_ADJUST", {"device": "rf", "item": "power", "value": 350.0})))
    r.run(12)
    assert not out["manual"].ok and "300" in out["manual"].detail
    assert out["ok"].ok and not out["adj"].ok and "300" in (out["adj"].detail or "")
    # RfControl 도 상한으로 자른다(환산은 RF_MAX_POWER)
    rf = RfControl(dict(CFG, RF_OPERATING_MAX_W=250.0), _Plc(), lambda lv, t: None)
    rf.start(400.0, 0.0, 1.0)
    assert rf.target == 250.0 and rf.power_to_dac(300.0) == round(300.0 / CFG["RF_MAX_POWER"] * 4000)
