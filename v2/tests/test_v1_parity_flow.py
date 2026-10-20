"""5-3 공정 흐름 — v1 과 줄 단위로 비교한 검토에서 나온 차이의 재현 시험(고치기 전 실패 → 고친 뒤 통과).

- FLOW_ON 유량 안정화(v1 device/MFC.py:1128-1139·1236-1270)
- CSV 리스트: 행 끝 정지 순서 중의 STOP·이상 중단·ALL STOP 이 리스트를 끝낸다(v1 core/process_service.py:725-727·868·938-943·1026)
- MFC 쓰기 재시도(v1 MFC.py:1058-1211, 질의 재시도 354·448-454, 재연결 대기 365-366), SP2·SP3 되읽기는 경고만
- HEATER_RAMP 스텝이 래더 램프 속도 D00020 를 쓴다(v1 process_controller.py:786)
- 베이스 대기: 재점등 필라멘트도 공정 소유(v1 pc:1092-1094·1254), IG_SETTLE 은 첫 방출 시각부터
- 정지 순서: DC OFF·RF Pulse OFF 는 보내 놓고 RF 램프다운(켰을 때만, v1 pc:1924-1981), DC OFF 경고는 켠 적 있을 때만(DCpower.py:356-359)
- 공정 중 PLC 버튼 허용 + PLC_MANUAL_IN_PROCESS 기록(사용자 결정 2026-10-20)
"""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.safety_engine import Act
from v2.chk import params as P
from v2.chk.heater_engine import HeaterEngine
from v2.chk.process_engine import ProcessEngine
from v2.chk.process_steps import ProcessStep, StepRunner, _Cur
from v2.chk.safety import View
from v2.tests.test_dc import Rig as DcRig
from v2.tests.test_mfc import Rig as MfcRig
from v2.tests.test_process_engine import FAST, R, SHORT
from v2.tests.test_safety_engine import H

CFG = defaults(P.PARAMS)
T0 = 1_760_000_000.0


# ───────── FLOW_ON 유량 안정화 ─────────
class _FlowMfc:
    ar_set, o2_set = 20.0, None

    def set_channel(self, ch, on, on_done=None):
        on_done("mfc:ch_on", None, [])


def _flow_runner():
    return StepRunner(None, [ProcessStep("MFC_CMD", "Ch1 Flow ON", ("FLOW_ON", {"channel": 1}))], _FlowMfc(), None,
                      cfg=CFG)


def _v(t, ar):
    return View({"mfc.ar_sccm": ar}, T0 + t, {"mfc": "ok"}, {"mfc": 0.0})


def test_flow_on_waits_until_flow_is_within_tolerance():
    r = _flow_runner()
    for t, ar in ((0, 0.0), (1, 5.0), (2, 17.5)):            # 20 × 10 % = 2 sccm 밖
        r.tick(_v(t, ar), float(t))
        assert not r.finished and not r.failed, (t, ar)
    r.tick(_v(3, 18.5), 3.0)                                 # |18.5 − 20| ≤ 2 → 통과(한 번이면 됨, v1)
    assert r.finished and not r.failed


def test_flow_on_fails_after_stabilize_timeout():
    r = _flow_runner()
    for t in range(0, 31):
        r.tick(_v(t, 0.0), float(t))
    assert r.failed == "FLOW_STABILIZE_TIMEOUT"              # v1 "유량 안정화 시간 초과" → 공정 중단


# ───────── 행 끝 정지 순서 중의 STOP·중단·ALL STOP ─────────
@pytest.mark.parametrize("kind,expect", [("PROCESS_STOP", ("aborted", "USER_STOP")), ("ALL_STOP", ("error", "ALL_STOP")),
                                         ("ABORT", ("error", "MV_CLOSED"))])
def test_stop_during_row_end_sequence_ends_list(tmp_path, kind, expect):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "two", "rows": [SHORT, dict(SHORT, process_name="T")]}))
    r.on(3, r.cmd("RECIPE_START"))
    seen = {}

    def act():
        seen["stopping"] = r.rig.safety.engine.stopping
        if kind == "ABORT":
            return r.rig.safety.engine.abort("MV_CLOSED", rule="test")
        return r.rig.core.command(kind)
    r.when(lambda: r.rig.process.state == "STOPPING" and r.rig.safety.engine.stopping == "row_end", act)
    r.run(260)
    assert seen["stopping"] == "row_end"
    assert len(r.ev("run_start")) == 1                       # 다음 행을 시작하지 않는다
    end = r.ev("run_end")
    assert [(e.data["status"], e.data["reason"]) for e in end] == [expect]
    assert r.ev("list_cancel") and not r.ev("list_done")
    assert r.rig.process.state == "IDLE" and not r.rig.process.rows


# ───────── MFC 쓰기 재시도 ─────────
def test_mfc_first_r69_query_is_retried_on_no_response():
    r = MfcRig()
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 0.5)               # R69 첫 질의를 놓침 — v1 은 질의를 다시(재시도 5)
    assert r.seq(r.d.set_channel, 1, True) is None and r.sim.on[0]
    r.done()


def test_mfc_write_waits_for_reconnect():
    r = MfcRig()
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 4.0)                # 재연결 중 — v1 은 큐에서 기다린다
    assert r.seq(r.d.set_flow, 1, 20.0) is None and r.sim.set_pct[0] == 20.0
    r.done()


def test_mfc_valve_resent_on_2nd_and_4th_check():
    r = MfcRig()
    r.sim.ignore_writes = {"C"}
    assert r.seq(r.d.valve, False) == "VERIFY_MISMATCH"
    assert r.sim.set_commands_seen().count("C") == 3         # 처음 + 2·4번째 확인 뒤(v1 MFC.py:1184-1189)
    r.done()


def test_mfc_l0_resent_once():
    r = MfcRig()
    r.sim.ignore_writes = {"L0"}
    assert r.seq(r.d.set_onoff, "1000") == "VERIFY_MISMATCH"
    assert r.sim.set_commands_seen().count("L01000") == 2    # v1 max_attempts=2(MFC.py:1131-1152)
    r.done()


def test_mfc_sp2_readback_mismatch_is_warning_only():
    r = MfcRig()
    r.sim.ignore_writes = {"S2"}
    assert r.seq(r.d.set_sp, 2, 5.0) is None                 # v1 은 SP2 를 되읽지 않고 확인(MFC.py:666-670)
    ev = [e for e in r.events if e.kind == "sp_readback"]
    assert ev and ev[0].reason == "VERIFY_MISMATCH"
    r.done()


# ───────── 히터 램프 스텝 ─────────
class _Plc:
    def __init__(self):
        self.calls = []

    def heater_set_reg(self, name, raw, on_done=None):
        self.calls.append((name, raw))

    def heater_set_sv(self, v, on_done=None):
        self.calls.append(("sv", v))

    def heater_run(self, on, on_done=None):
        self.calls.append(("run", on))


def test_heater_ramp_step_writes_ladder_ramp_rate():
    c = VirtualClock(start_wall=T0)
    c.register()
    plc = _Plc()
    h = HeaterEngine(c, CFG, plc, None)
    h.process_ramp(30.0)
    assert ("heater_ramp_rate", 5) in plc.calls              # max(1, round(30/6)) — v1 process_controller.py:786
    plc.calls.clear()
    h.process_set(200.0)                                     # PV 를 몰라 감속 접근을 못 쓰면 이 속도로 SV 직접
    assert plc.calls[:2] == [("heater_ramp_rate", 5), ("sv", 200.0)] and ("run", True) in plc.calls


# ───────── 베이스 대기 ─────────
class _Dc:
    def __init__(self):
        self.meas_listeners = []


class _Ig:
    emission_since = None
    last_result = None

    def filament_on(self, on_done=None):
        pass


def _engine():
    c = VirtualClock(start_wall=T0)
    c.register()
    return ProcessEngine(c, CFG, _Plc(), None, _Dc(), None, _Ig(), None, {})


def test_relit_filament_is_owned_by_process():
    pe = _engine()
    pe.filament_by_process = False
    pe._relight({"relights": 0}, 0.0)
    assert pe.filament_by_process                            # 정지 때 끈다(v1 pc:1092-1094·1254·1283-1285)


def test_ig_settle_counts_from_first_emission():
    pe = _engine()
    pe.ig.emission_since = T0                                # 드라이버가 방출을 처음 본 시각
    s, cur = ProcessStep("IG_BASE_WAIT", "", value=1e-5), _Cur(t0=0.0)
    vals = {"ig.filament": True, "ig.emission": True, "ig.ion_torr": 5e-6}
    res = None
    for k in range(20, 27):                                  # 방출 뒤 20 s(IG_SETTLE 15 s 지남)부터 5 s 연속
        res = pe._h_ig_wait(None, s, cur, View(vals, T0 + k, {"ig": "ok"}, {"ig": 0.0}), float(k))
        if res == "next":
            break
    assert res == "next" and k <= 26


# ───────── 정지 순서 ─────────
def test_stop_sequence_sends_dc_off_and_ramps_rf_without_waiting():
    h = H(state="MAIN", uses_rf=True, dc_on=True)
    t = {}

    def rec(k, finish=True):
        def f(a, done):
            t.setdefault(k, h.c.monotonic())
            if finish:
                done(None)
        return f
    h.e.handlers["DC_SAFE_OFF"] = rec("dc", finish=False)    # DC OFF 확인이 오래 걸려도
    h.e.handlers["RF_RAMP_DOWN"] = rec("ramp")
    h.e.handlers["GAS_FLOW_OFF"] = rec("flow")
    h.s.stop()
    h.settle()
    assert t["ramp"] - t["dc"] < 0.01 and t["flow"] - t["dc"] < 0.01     # 램프다운·가스 차단은 기다리지 않고
    res = dict((n, st) for n, _, st in h.e.seq_results["stop"])
    assert res[4] == "sent" and res[13] == "timeout"         # DC 결과는 끝에서 기록


def test_stop_sequence_skips_dc_and_rfp_not_turned_on():
    h = H(state="MAIN")
    h.s.stop()
    h.settle()
    kinds = [k for k, _ in h.calls]
    assert "DC_SAFE_OFF" not in kinds and "RFP_SAFE_OFF" not in kinds  # v1 is_dc_on·is_rfpulse_on 일 때만


def test_dc_off_unconfirmed_is_warning_only_after_output_was_on():
    r = DcRig()
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 30.0)
    assert r.seq_wait(r.d.output_off_confirmed) is None      # 켠 적 없음 — 정보 한 줄
    assert "off_unverified" in r.kinds() and "off_unconfirmed" not in r.kinds()
    r.done()


# ───────── 공정 중 PLC 버튼 ─────────
def test_plc_button_allowed_in_process_and_recorded(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PLC_BUTTON", {"target": "ion", "on": True}))          # 공정 밖 — 기록 없음
    r.on(4, r.cmd("PROCESS_START", {"row": dict(SHORT, process_time=1.0)}))
    r.when(lambda: r.rig.process.state == "MAIN", r.cmd("PLC_BUTTON", {"target": "vent", "on": False}))
    r.run(150)
    res = [x for x in r.results if hasattr(x, "ok")]
    assert all(x.ok for x in res), res
    ev = r.ev("plc_manual")
    assert len(ev) == 1 and ev[0].code == "PLC_MANUAL_IN_PROCESS"
    d = ev[0].data
    assert (d["target"], d["on"], d["source"], d["state"], d["level"]) == ("vent", False, "web", "MAIN", "info") and d["t"] > 0
