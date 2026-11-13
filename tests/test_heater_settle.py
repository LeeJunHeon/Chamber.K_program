# -*- coding: utf-8 -*-
"""셔터 전 히터 정착 확인(HEATER_SETTLE) — 메인 셔터 열기 직전 TC1 이 목표 ±TOL 안에 SEC 이상 연속인지.

연속 시간은 SettleTracker 가 가상 시계로 센다(공정은 time.monotonic). 대기 상한은 _exec_loop_with_timeout 의 실제 시간이라
상한 초과 시험만 상한을 짧게 둔다. PLC 폴링은 20 ms QTimer 가 흉내 내고, 한 번마다 가상 시계를 STEP_SEC 만큼 민다.
"""
from unittest.mock import MagicMock

import pytest
from PyQt6.QtCore import QObject, QTimer, pyqtSignal as Signal

import controller.process_controller as PC
from controller.process_controller import ActionType, ProcessStep, SettleTracker
from conftest import make_heater_st
from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)

pytestmark = pytest.mark.cascade
STEP_SEC = 5.0


class _FakePlc(QObject):
    update_heater_status = Signal(dict)

    def __init__(self):
        super().__init__()
        self.st = make_heater_st(run=True, pv=600.0, sv=600.0, pv2=842.0)

    def get_heater_status(self):
        return dict(self.st)


@pytest.fixture
def sc(qapp, monkeypatch):
    clk = [1000.0]
    plc = _FakePlc()
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), plc, None)
    c._running = True; c._stop_pending = False; c._idx = 0
    c._steps = [ProcessStep(ActionType.HEATER_SETTLE, "셔터 전 히터 정착 확인", value=600.0, polling=True),
                ProcessStep(ActionType.PLC_CMD, "Main Shutter Open", params=('MS_button', True))]
    c._settle_clock = lambda: clk[0]
    calls = {"next": 0, "abort": [], "msgs": [], "failed": [], "stage": []}
    monkeypatch.setattr(c, "_next_step", lambda: calls.__setitem__("next", calls["next"] + 1))
    monkeypatch.setattr(c, "_abort_with_error", lambda r: calls["abort"].append(r))
    c.status_message.connect(lambda l, m: calls["msgs"].append((l, m)))
    c.stage_monitor.connect(lambda m: calls["stage"].append(m))
    c.heater_settle_failed.connect(lambda d: calls["failed"].append(d))
    casc = {"active": True, "state": "running", "engaged": True, "final": 600.0, "r1": 600.0,
            "bias": 3.0, "bias_ff": 5.0, "bias_int": -2.0}
    c.set_hold_state_provider(lambda: {"casc": dict(casc)})
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_TOL_C", 2.0)
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_SEC", 60.0)
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_MAX_SEC", 900.0)
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_FAIL_ACTION", "proceed")
    script = {"pv": lambda t: 600.0, "fault": lambda t: False}

    def _tick():
        clk[0] += STEP_SEC
        t = clk[0] - 1000.0
        plc.st.update(pv=script["pv"](t), fault=bool(script["fault"](t)))
        plc.update_heater_status.emit(dict(plc.st))
    feeder = QTimer(); feeder.setInterval(20); feeder.timeout.connect(_tick)
    c._clk, c._plc, c._calls, c._casc, c._script, c._feeder = clk, plc, calls, casc, script, feeder
    yield c
    feeder.stop()
    c._settle_track_stop()


def _feed_direct(c, pv_of_t, sec):
    """HEATER_WAIT 통과 뒤 ~ HEATER_SETTLE 진입 전 구간(다른 스텝이 도는 동안)의 폴링 — 추적 슬롯만 받는다."""
    for _ in range(int(sec / STEP_SEC)):
        c._clk[0] += STEP_SEC
        c._plc.st.update(pv=pv_of_t(c._clk[0] - 1000.0))
        c._plc.update_heater_status.emit(dict(c._plc.st))


def _pass_line(c):
    return [m for l, m in c._calls["msgs"] if m.startswith("셔터 전 히터 정착 확인 — ")]


# (a) 이미 범위 안 60초 이상 → 즉시 통과
def test_a_already_settled_passes_immediately(sc):
    sc._settle_track_start(600.0)                         # HEATER_WAIT 통과 시점
    _feed_direct(sc, lambda t: 599.4, 120)                # 가스·압력·파워 동안 범위 안
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["failed"] == []
    line = _pass_line(sc)
    assert len(line) == 1 and "TC1 599.4 (목표 600.0, 편차 -0.6) · 대기 0:00" in line[0]
    assert sc._settle_track is None                       # 통과하면 구독 해제


# (b) 범위 밖에서 들어와 60초 → 통과
def test_b_enters_band_then_passes_after_60s(sc):
    sc._settle_track_start(600.0)
    _feed_direct(sc, lambda t: 595.0, 60)                 # 가스 도입 뒤 −5°C
    sc._script["pv"] = lambda t: 595.0 if t < 120 else 599.0
    sc._feeder.start()
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["failed"] == []
    waited = sc._clk[0] - 1000.0 - 60
    assert 115 <= waited <= 130                           # 60 s(범위 밖) + 60 s(범위 안) 뒤 통과
    assert "편차 -1.0" in _pass_line(sc)[0]
    waits = [m for l, m in c["msgs"] if m.startswith("셔터 전 히터 정착 대기 —")]
    assert waits and "TC1 595.0 / 목표 600.0 (±2, 60초 연속 필요" in waits[0]
    assert "bias +3.0 (ff +5.0 / int -2.0)" in waits[0] and "/ 상한 15:00" in waits[0]
    assert any("셔터 전 히터 정착 대기" in m for m in c["stage"])


# (c) 상한 초과 proceed → 경고·이벤트 신호·진행
def test_c_timeout_proceed_warns_and_continues(sc, monkeypatch):
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_MAX_SEC", 0.6)   # 실제 시간 상한
    sc._script["pv"] = lambda t: 594.8
    sc._settle_track_start(600.0)
    sc._feeder.start()
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 1 and c["abort"] == []
    assert len(c["failed"]) == 1 and c["failed"][0]["action"] == "proceed" and c["failed"][0]["pv"] == 594.8
    assert "셔터 전 정착 미달 — TC1 594.8 / 목표 600.0 ±2" in c["failed"][0]["reason"]
    assert any(l == "히터(경고)" and "경고 후 셔터를 열고 진행" in m for l, m in c["msgs"])
    assert sc._settle_track is None


# (d) 상한 초과 abort → 중단
def test_d_timeout_abort_stops_process(sc, monkeypatch):
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_MAX_SEC", 0.6)
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_FAIL_ACTION", "abort")
    sc._script["pv"] = lambda t: 594.8
    sc._settle_track_start(600.0)
    sc._feeder.start()
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 0 and len(c["abort"]) == 1 and "셔터 전 정착 미달" in c["abort"][0]
    assert c["failed"][0]["action"] == "abort"


# (e) fault → 중단
def test_e_fault_aborts(sc):
    sc._script["pv"] = lambda t: 595.0
    sc._script["fault"] = lambda t: t >= 30
    sc._settle_track_start(600.0)
    sc._feeder.start()
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 0 and c["failed"] == []
    assert len(c["abort"]) == 1 and "히터 이상 발생(셔터 전 정착 대기 중)" in c["abort"][0]


# (f) TC1 None 이면 연속 시간이 끊긴다
def test_f_tc1_none_resets_continuity():
    t = [0.0]
    tr = SettleTracker(600.0, 2.0, 60.0, clock=lambda: t[0])
    for _ in range(10):
        t[0] += 5.0; tr.feed(599.0)
    assert tr.held_sec() == pytest.approx(45.0) and not tr.ok()
    t[0] += 5.0; tr.feed(None)                            # 센서 이상(D00010=-1)
    assert tr.held_sec() == 0.0 and tr.since is None
    for _ in range(12):
        t[0] += 5.0; tr.feed(599.0)
    assert tr.held_sec() == pytest.approx(55.0) and not tr.ok()
    t[0] += 5.0; tr.feed(601.9)
    assert tr.ok()
    t[0] += 5.0; tr.feed(602.1)                           # 범위 밖도 끊긴다
    assert not tr.ok() and tr.since is None
    t[0] += 5.0; tr.feed(600.0, final_ok=False)           # 캐스케이드 최종 목표가 공정 목표와 다르면 끊긴다
    assert tr.since is None


def test_f2_tc1_none_in_loop_delays_pass(sc):
    sc._settle_track_start(600.0)
    sc._script["pv"] = lambda t: None if 40 <= t < 45 else 599.0
    sc._feeder.start()
    sc._heater_settle(600.0)
    waited = sc._clk[0] - 1000.0
    assert sc._calls["next"] == 1 and 100 <= waited <= 115      # 40 s 에 끊겨 45 s 부터 다시 60 s


def test_cascade_not_active_skips_with_warning(sc):
    sc._casc["active"] = False; sc._casc["state"] = "idle"
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 1 and c["abort"] == []
    assert any(l == "히터(경고)" and "셔터 전 히터 정착 확인 생략 — 캐스케이드가 활성이 아님" in m for l, m in c["msgs"])


def test_stop_during_settle_exits_without_next(sc):
    sc._script["pv"] = lambda t: 590.0
    sc._settle_track_start(600.0)
    sc._feeder.start()
    QTimer.singleShot(150, lambda: setattr(sc, "_stop_pending", True))
    sc._heater_settle(600.0)
    c = sc._calls
    assert c["next"] == 0 and c["abort"] == [] and c["failed"] == []
    assert sc._active_loops == []


def test_stop_impl_releases_tracking(sc):
    sc._settle_track_start(600.0)
    assert sc._settle_track is not None
    sc._running = False
    sc._stop_impl()
    assert sc._settle_track is None
    n = len(sc._calls["msgs"])
    sc._plc.update_heater_status.emit(dict(sc._plc.st))   # 해제 뒤 폴링은 추적에 닿지 않는다(예외 없음)
    assert len(sc._calls["msgs"]) == n


# (g) 스텝 목록: 캐스케이드·히터 사용·메인 공정·상한 > 0 일 때만, Shutter Delay 뒤 Main Shutter Open 바로 앞
def _steps(monkeypatch, mode="cascade", provider=True, **over):
    monkeypatch.setattr(PC, "HEATER_CTRL_MODE", mode)
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    c._is_connected = lambda obj: True
    c._invoke_self = lambda name: None
    c._next_step = lambda: None
    if provider:
        c.set_hold_state_provider(lambda: {"casc": {"active": False}})
    p = dict(process_note="t", use_dc=True, dc_power=100.0, sp1_set=5.0, shutter_delay=5.0, process_time=10.0,
             use_g1=True, use_heater=True, heater_temp=600.0, heater_ramp=6.0, ar_flow=20.0)
    p.update(over)
    c.start_process_flow(p)
    out = [(s.action, s.message, s.polling, s.timer_purpose, s.value) for s in c._steps]
    c.deleteLater()
    return out


def test_g_step_list(qapp, monkeypatch):
    on = _steps(monkeypatch)
    acts = [a for a, *_ in on]
    i = acts.index(ActionType.HEATER_SETTLE)
    assert on[i + 1][1] == "Main Shutter Open" and on[i - 1][1].startswith("Shutter Delay")
    assert on[i][2] is True and on[i][3] is None and on[i][4] == 600.0
    assert on[i][1] == "셔터 전 히터 정착 확인 (TC1 ±2°C, 60s 연속, 상한 900s)"
    assert acts.count(ActionType.HEATER_SETTLE) == 1
    without = [x for x in on if x[0] != ActionType.HEATER_SETTLE]
    assert _steps(monkeypatch, mode="tc1") == without                        # tc1 모드: 예전 목록 그대로
    assert ActionType.HEATER_SETTLE not in [a for a, *_ in _steps(monkeypatch, use_heater=False)]
    assert ActionType.HEATER_SETTLE not in [a for a, *_ in _steps(monkeypatch, process_time=0.0)]
    assert ActionType.HEATER_SETTLE not in [a for a, *_ in _steps(monkeypatch, provider=False)]   # _casc_mode False
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_MAX_SEC", 0.0)
    assert _steps(monkeypatch) == without
    monkeypatch.setattr(PC, "HEATER_SHUTTER_SETTLE_MAX_SEC", 900.0)
    sd0 = _steps(monkeypatch, shutter_delay=0.0)                              # Shutter Delay 없음 → 파워 단계 뒤
    j = [a for a, *_ in sd0].index(ActionType.HEATER_SETTLE)
    assert sd0[j + 1][1] == "Main Shutter Open"


def test_settle_starts_dc_power_monitor(sc):
    """Shutter Delay 가 없는 레시피에서도 이 단계부터 DC ±% 감시가 켜진다(Shutter Delay 와 같은 호출, 이미 armed 면 DC 가 무시)."""
    sc.params = {"dc_power": 100.0}
    calls = []
    import controller.process_controller as P
    orig = P.QMetaObject.invokeMethod
    P.QMetaObject.invokeMethod = lambda obj, name, *a: calls.append(name)
    try:
        sc._settle_track_start(600.0)
        _feed_direct(sc, lambda t: 600.0, 70)
        sc._heater_settle(600.0)
    finally:
        P.QMetaObject.invokeMethod = orig
    assert "arm_power_monitor" in calls and sc._calls["next"] == 1


def test_config_defaults_and_clamp(capsys):
    import importlib
    import lib.config as CFG
    assert (CFG.HEATER_SHUTTER_SETTLE_TOL_C, CFG.HEATER_SHUTTER_SETTLE_SEC, CFG.HEATER_SHUTTER_SETTLE_MAX_SEC,
            CFG.HEATER_SHUTTER_SETTLE_FAIL_ACTION) == (2.0, 60.0, 900.0, "proceed")
    saved = {k: getattr(CFG, k) for k in ("HEATER_SHUTTER_SETTLE_TOL_C", "HEATER_SHUTTER_SETTLE_SEC",
                                          "HEATER_SHUTTER_SETTLE_MAX_SEC", "HEATER_SHUTTER_SETTLE_FAIL_ACTION")}
    try:
        CFG.HEATER_SHUTTER_SETTLE_TOL_C = 0.1; CFG.HEATER_SHUTTER_SETTLE_SEC = 5000
        CFG.HEATER_SHUTTER_SETTLE_MAX_SEC = "x"; CFG.HEATER_SHUTTER_SETTLE_FAIL_ACTION = "wait"
        CFG._validate_heater_shutter_settle_config()
        assert (CFG.HEATER_SHUTTER_SETTLE_TOL_C, CFG.HEATER_SHUTTER_SETTLE_SEC, CFG.HEATER_SHUTTER_SETTLE_MAX_SEC,
                CFG.HEATER_SHUTTER_SETTLE_FAIL_ACTION) == (0.5, 600.0, 900.0, "proceed")
        out = capsys.readouterr().out
        assert "HEATER_SHUTTER_SETTLE_TOL_C 0.1 → 0.5" in out and "HEATER_SHUTTER_SETTLE_FAIL_ACTION 'wait'" in out
    finally:
        for k, v in saved.items():
            setattr(CFG, k, v)


def test_main_settle_failed_event_and_chat_card(fresh):
    """main: 정착 미달 → 경고 + 히터 런 이벤트 "셔터 전 정착 미달" + 챗 카드 1장(기존 히터 알림 카드 경로)."""
    from test_main_heater import feed
    w = fresh
    w._heater_run_events = []
    feed(w, make_heater_st(run=True, pv=594.8, sv=600.0, pv2=838.0))
    info = {"action": "proceed", "reason": "셔터 전 정착 미달 — TC1 594.8 / 목표 600.0 ±2", "pv": 594.8,
            "target": 600.0, "tol": 2.0, "need": 60.0, "elapsed": 900}
    w._on_heater_settle_failed(info)
    assert "셔터 전 정착 미달" in w._heater_run_events
    calls = w.chat_chk.notify_heater_alert.call_args_list
    assert len(calls) == 1
    title, _ctx, fields = calls[0].args[:3]
    assert title == "히터 셔터 전 정착 미달" and calls[0].kwargs.get("ok") is False
    assert fields["조치"] == "경고 후 셔터 열고 진행" and fields["대기"] == "900초" and fields["TC1"].startswith("594.8")
    assert "ff" in fields["bias"] and "int" in fields["bias"]
    w._on_heater_settle_failed(dict(info, action="abort"))
    assert w.chat_chk.notify_heater_alert.call_args_list[-1].args[2]["조치"] == "공정 중단"
    assert w._heater_run_events.count("셔터 전 정착 미달") == 1
