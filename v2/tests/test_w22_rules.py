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
def test_zero_waits_after_gas_just_stopped(tmp_path):
    """W-23 H1 — 가스를 끈 바로 뒤 다시 가스 주입: 조건 유지 시간이 모자라면 기다렸다가 영점을 보낸다(건너뛰지 않음)."""
    m = M(tmp_path, cfg={"IG_ENABLED": False}, start=15)
    texts = []
    m.rig.loop.tick_hooks.append(lambda t: texts.append((m.rig.process.run_state(False) or {}).get("waitText")))
    m.blocks([GAS, GAS_OFF, GAS], end=False)
    m.run(140)
    assert all(x.ok for _, _, x in m.sent), m.sent
    w = _mfc_writes(m)
    assert w.count("Z1") == 2 and w.count("L5 1") == 2, w                        # 두 번째 가스 주입도 영점
    assert not [e for e in m.ev("warn") if e.code == "ZERO_SKIPPED"]
    waits = [x for x in texts if x]
    assert waits and waits[0].startswith("영점 대기 ") and waits[0].endswith(" s")
    assert C.REASON_HELP["ZERO_SKIPPED"].endswith("요.")


def test_zero_skipped_after_wait_limit_with_reason(tmp_path):
    """조건이 끝내 맞지 않으면(가스 밸브 열림) ZERO_WAIT_MAX_SEC 뒤 그 블록의 영점만 건너뛰고 실제 이유로 경고 한 번."""
    r = R(tmp_path, dict(FAST, IG_ENABLED=False, ZERO_WAIT_MAX_SEC=12.0))
    p = r.rig.process
    orig = p._zero_cond
    p._zero_cond = lambda view: "가스 밸브 열림"
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(200)
    p._zero_cond = orig
    w = _mfc_writes(r)
    assert not [x for x in w if x in ("L5 1", "L6 1", "Z1")], w
    warns = [e for e in r.ev("warn") if e.code == "ZERO_SKIPPED"]
    assert len(warns) == 1 and warns[0].detail == "영점 건너뜀 — 가스 밸브 열림"      # 블록마다 한 번, '가스가 흐르는 중' 아님
    assert P.by_key()["ZERO_WAIT_MAX_SEC"].default == 30.0
    assert r.ev("run_end")[0].data["status"] == "done"                            # 공정은 이어 간다


def test_zero_two_row_recipe_second_row_sends(tmp_path):
    """2행 연속 레시피 — 둘째 행도 영점을 보낸다(행 사이 유량 끄기 뒤 기다림)."""
    r = R(tmp_path, dict(FAST, IG_ENABLED=False))
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "two", "rows": [dict(SHORT, process_name="a"), dict(SHORT, process_name="b")]}))
    r.on(4, r.cmd("RECIPE_START"))
    r.run(260)
    assert all(x.ok for x in r.results), r.results
    w = _mfc_writes(r)
    assert w.count("Z1") == 2, w
    assert not [e for e in r.ev("warn") if e.code == "ZERO_SKIPPED"]
    assert [e.data["status"] for e in r.ev("run_end")] == ["done", "done"]


def test_zero_waits_when_mfc_valve_was_closed(tmp_path):
    """대기 중 MFC 밸브가 닫혀 있었다 — 공정이 밸브를 연 뒤 유지 시간을 채우고 보낸다."""
    r = R(tmp_path, dict(FAST, IG_ENABLED=False))
    sim = r.rig.sims["mfc"]
    sim.valve_mode, sim.valve = "closed", 0.0
    r.on(20, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(90)
    w = _mfc_writes(r)
    assert "Z1" in w and w.index("O") < w.index("Z1"), w
    assert not [e for e in r.ev("warn") if e.code == "ZERO_SKIPPED"]


def test_zero_sent_after_flow_off_hold(tmp_path):
    """공정 시작은 유량 끄기 → MFC 밸브 열기(대기) → 영점 — 조건이 ZERO_MIN_IDLE_SEC 이어지면 보낸다."""
    r = R(tmp_path, dict(FAST, IG_ENABLED=False, ZERO_MIN_IDLE_SEC=3.0))
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(60)
    w = _mfc_writes(r)
    assert "Z1" in w and "L5 1" in w and w.index("L00000") < w.index("L5 1"), w
    assert not [e for e in r.ev("warn") if e.code == "ZERO_SKIPPED"]


def test_zero_sent_with_default_hold_when_flow_was_off(tmp_path):
    """기본 ZERO_MIN_IDLE_SEC(10 s) — 유량이 꺼진 채 대기했으면 공정 시작의 유량 끄기(L0 0000) 다시 보내기가 시간을 끊지 않는다."""
    r = R(tmp_path, dict(FAST, IG_ENABLED=False))
    r.on(20, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(70)
    w = _mfc_writes(r)
    assert "Z1" in w and "L5 1" in w, w
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
    assert p["RF_MAX_POWER"].label == "RF DAC 풀스케일(환산 기준)" and "상한으로 쓰지 않습니다" in p["RF_MAX_POWER"].desc
    assert (p["RF_OPERATING_MAX_W"].default, p["RF_OPERATING_MAX_W"].min, p["RF_OPERATING_MAX_W"].max) == (600.0, 1.0, 600.0)
    assert p["RF_REF_FULL_SCALE_W"].default == 600.0 and "2026-11-25" in p["RF_REF_FULL_SCALE_W"].note
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


# ───────── W-23 H2 MFC 통신 두절 ─────────
def _mfc_drop(tmp_path, down_s):
    r = R(tmp_path, dict(FAST))
    rows = []
    box = {}

    def hook(t):
        p = r.rig.process
        rows.append(p.state)
        if "t" not in box and p.state == "MAIN":
            box["t"] = t.seq
            now = r.c.monotonic()
            r.rig.sims["mfc"].add_fault("disconnect", now, now + down_s)
    r.rig.loop.tick_hooks.append(hook)
    r.on(3, r.cmd("PROCESS_START", {"row": dict(SHORT, process_time=3.0, use_rf=True, rf_power=50.0, use_dc=False)}))
    r.run(400)
    return r, box


def test_mfc_comm_loss_aborts_and_stop_sequence_finishes(tmp_path):
    r, box = _mfc_drop(tmp_path, 1000)
    (end,) = r.ev("run_end")
    assert end.data["status"] == "error" and end.data["reason"] == "MFC_COMM_LOST"
    fired = [e for e in r.rig.events if type(e).__name__ == "SafetyEvent" and e.rule == "S52" and e.kind in ("fired", "abort")]
    assert fired
    plc = r.rig.sims["plc"]
    assert r.rig.process.state == "IDLE"
    assert (plc.regs[A["rf_dac"]] or 0) == 0 and not plc.coils[A["rf_dac_enable"]]
    assert not any(plc.coils[A[k]] for k in ("ms", "s1", "s2", "ar_valve", "o2_valve"))
    assert P.by_key()["MFC_COMM_LOSS_ABORT_SEC"].default == 30.0 and "MFC_COMM_LOST" in C.reason_codes()


def test_mfc_comm_short_drop_no_abort(tmp_path):
    r, box = _mfc_drop(tmp_path, 10)
    ends = r.ev("run_end")
    assert ends and ends[0].data["status"] == "done", [e.data for e in ends]


# ───────── W-23 H4 RF 끈 뒤 늦은 DAC 쓰기 ─────────
def test_late_rf_dac_write_after_off_is_dropped():
    from v2.tests.test_plc_driver import Rig as PRig
    r = PRig()
    held = []
    orig = r.d.submit_write
    r.d.submit_write = lambda name, fn, **kw: held.append((name, fn, kw))
    r.d.rf_write_dac(800)                                   # RF 제어 루프 쓰기 — 대기열에 들어간 채
    r.d.rf_set_power(100.0)
    r.d.submit_write = orig
    box = []
    r.d.rf_off(on_done=lambda res: (box.append(res), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    done = []
    for name, fn, kw in held:                               # 끄기 뒤 늦게 실행
        orig(name, fn, on_done=lambda res: (done.append(res), r.c.notify()))
    r.c.wait_until(None, lambda: len(done) == 2)
    assert r.sim.reg("rf_dac") == 0 and not r.sim.coil("rf_dac_enable")
    assert [e.kind for e in r.events if e.kind == "rf_write_dropped"] == ["rf_write_dropped"] * 2
    # 끈 뒤 새로 만든 쓰기는 그대로 실행
    box.clear()
    r.d.rf_write_dac(400, on_done=lambda res: (box.append(res), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert r.sim.reg("rf_dac") == 400 and r.sim.coil("rf_dac_enable")
    r.done()


# ───────── W-23 H3 절전 막기 ─────────
def test_keep_awake_fake_api():
    from v2.chk import keep_awake as KA
    calls, evs, busy = [], [], [False]
    cfg = {"KEEP_AWAKE": "run"}
    ka = KA.KeepAwake(cfg, busy=lambda: busy[0], on_event=evs.append, api=calls.append)
    ka.tick()
    assert calls == [] and not ka.held                                  # 대기 중 — 막지 않음
    busy[0] = True
    ka.tick(); ka.tick()
    assert calls == [KA.ES_CONTINUOUS | KA.ES_SYSTEM_REQUIRED] and ka.held   # 한 번만 건다(화면 꺼짐은 막지 않음)
    busy[0] = False
    ka.tick()
    assert calls[-1] == KA.ES_CONTINUOUS and not ka.held
    cfg["KEEP_AWAKE"] = "always"
    ka.tick()
    assert ka.held
    cfg["KEEP_AWAKE"] = "off"
    busy[0] = True
    ka.tick()
    assert not ka.held and [e.kind for e in evs] == ["keep_awake_on", "keep_awake_off", "keep_awake_on", "keep_awake_off"]
    assert not any(c & 0x00000002 for c in calls)                       # ES_DISPLAY_REQUIRED 없음
    none = KA.KeepAwake(cfg, busy=lambda: True, api=None)               # 윈도우가 아님 — 아무것도 안 함
    none.tick(); none.start(); none.stop()
    assert not none.held and P.by_key()["KEEP_AWAKE"].default == "run"
    # 스레드: 같은 스레드에서 걸고 푼다
    calls.clear(); busy[0] = True; cfg["KEEP_AWAKE"] = "run"
    import threading
    tids = []
    ka2 = KA.KeepAwake(cfg, busy=lambda: busy[0], api=lambda f: (calls.append(f), tids.append(threading.get_ident())))
    ka2.start()
    import time
    for _ in range(50):
        if calls:
            break
        time.sleep(0.02)
    ka2.stop()
    assert calls == [KA.ES_CONTINUOUS | KA.ES_SYSTEM_REQUIRED, KA.ES_CONTINUOUS] and len(set(tids)) == 1
