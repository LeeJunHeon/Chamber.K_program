# -*- coding: utf-8 -*-
"""T42~ — 공정 중 MV 닫힘 안전 중단 / 수동 모드 히터·공정 분리 / 히터 도달 카드 (MainDialog offscreen, 챗·ERP 스텁)."""
import time
from unittest.mock import MagicMock

import pytest
from PyQt6.QtCore import QTimer, QEventLoop

from conftest import make_heater_st
from test_main_heater import win, fresh, feed, spin   # noqa: F401  (픽스처 재사용)

import main as MAIN


@pytest.fixture
def safe(fresh, monkeypatch):
    """공정 안전 판정 테스트용: 로그 캡처 + 중단 카운터(기본은 실제 _abort_process_by_fault 를 감싼다)."""
    w = fresh
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w._chat_reset_run_state()
    w._plc_bits.clear()
    w._mv_itl_timer.stop()
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lvl, msg: logs.append((lvl, msg)))
    aborts = []
    orig = w._abort_process_by_fault
    monkeypatch.setattr(w, "_abort_process_by_fault",
                        lambda reason, detail="": (aborts.append((reason, detail)), orig(reason, detail)))
    w._logs = logs; w._aborts = aborts
    monkeypatch.setattr(w, "_chat_send_fault_detail", lambda *a, **k: None)
    monkeypatch.setattr(w, "_chat_notify_failed_now", lambda *a, **k: None)
    # 실제 ProcessController 로 stop 이 가지 않게(스레드에서 finished 가 돌아와 process_running 을 지운다) 기록만
    stops = []
    w.request_process_stop.disconnect()
    w.request_process_stop.connect(lambda: stops.append(1))
    w._stops = stops
    yield w
    w.request_process_stop.disconnect()
    w.request_process_stop.connect(w.process_controller.stop_process)
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w._mv_itl_timer.stop()


def test_T42_mv_button_off_aborts_only_during_process(safe):
    w = safe
    w._on_plc_bit_changed("MV_INTERLOCK", True, None)
    w._on_plc_bit_changed("Air", True, None)
    w._on_plc_bit_changed("MV_button", True, None)
    assert w._logs == []                                      # prev=None → 로그 없음
    # 공정 없음 → 로그만
    w._on_plc_bit_changed("MV_button", False, True)
    assert w._aborts == [] and ("PLC", "MV_button ON→OFF") in w._logs
    # 공정 중 → 중단 1회, detail 에 현재 비트값
    w.process_running = True; w._chat_reset_run_state()
    w._on_plc_bit_changed("MV_button", True, False)
    w._on_plc_bit_changed("MV_button", False, True)
    assert len(w._aborts) == 1 and w._stops == [1]
    assert w._aborts[0][0] == "메인밸브 닫힘 (M00003 OFF)"
    assert "MV=OFF MV_INTERLOCK=ON Air=ON Gauge1=? Gauge2=?" == w._aborts[0][1]
    # 중단 시퀀스 중 두 번째 전이는 무시
    w._on_plc_bit_changed("MV_button", True, False)
    w._on_plc_bit_changed("MV_button", False, True)
    assert len(w._aborts) == 1
    # 사용자 STOP 시퀀스 중에도 무시
    w._chat_reset_run_state(); w._chat_user_stopped = True
    w._on_plc_bit_changed("MV_button", False, True)
    assert len(w._aborts) == 1
    # 링크 복구 뒤 첫 발행(prev=None)이라도 False 면 판정
    w._chat_reset_run_state(); w.process_running = True
    w._on_plc_bit_changed("MV_button", False, None)
    assert len(w._aborts) == 2


def test_T43_mv_interlock_off_1s_aborts_but_short_glitch_does_not(safe):
    w = safe
    w.process_running = True; w._chat_reset_run_state()
    w._on_plc_bit_changed("MV_INTERLOCK", True, None)
    w._on_plc_bit_changed("MV_INTERLOCK", False, True)
    assert w._mv_itl_timer.isActive() and w._aborts == []
    spin(300)
    w._on_plc_bit_changed("MV_INTERLOCK", True, False)
    assert not w._mv_itl_timer.isActive() and w._aborts == []
    assert any("PLC MV 인터락 순간 해제 후 복귀 (" in m and "ms)" in m for _, m in w._logs)
    spin(1200)
    assert w._aborts == []
    # 1초 유지
    w._on_plc_bit_changed("MV_INTERLOCK", False, True)
    spin(1300)
    assert len(w._aborts) == 1 and w._aborts[0][0].startswith("메인밸브 인터락 해제 (M00032 OFF, 1초 지속)")


def test_T44_csv_delay_mv_off_cancels_list_and_no_next_step(safe, monkeypatch):
    w = safe
    starts = []
    monkeypatch.setattr(w, "_start_next_csv_step", lambda: starts.append(1))
    w.csv_mode = True; w.csv_rows = [{"Process_name": "A"}, {"Process_name": "B"}]; w.csv_index = 0
    w.csv_cancelled = False; w.process_running = False
    w._chat_reset_run_state()
    w._start_csv_delay_step(1, "delay 1s")
    assert w._csv_delay_active is True
    w._on_plc_bit_changed("MV_button", True, None)
    w._on_plc_bit_changed("MV_button", False, True)
    assert len(w._aborts) == 1
    assert w.csv_mode is False and w._csv_delay_active is False and w.csv_rows == []
    spin(1500)                                                # 딜레이가 끝났어도 다음 STEP 없음
    assert starts == []
    assert any("CSV Delay 중 설비 이상" in m for _, m in w._logs)


# ───────────────────────── 수동 모드: 히터 ↔ 공정 분리 ─────────────────────────
def _fill_manual_ui(w):
    ui = w.ui
    ui.Ar_gas_radio.setChecked(True); ui.O2_gas_radio.setChecked(False)
    ui.Ar_flow_edit.setPlainText("20"); ui.working_pressure_edit.setPlainText("5")
    ui.dc_power_checkbox.setChecked(True); ui.DC_power_edit.setPlainText("100")
    ui.rf_power_checkbox.setChecked(False); ui.rf_pulse_checkbox.setChecked(False)
    ui.Shutter_delay_edit.setPlainText("1"); ui.process_time_edit.setPlainText("1")
    ui.G1_checkbox.setChecked(True); ui.G1_edit.setPlainText("CeO2"); ui.G2_checkbox.setChecked(False)


@pytest.fixture
def manual(fresh, monkeypatch):
    w = fresh
    w.csv_file_path = None; w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    _fill_manual_ui(w)
    monkeypatch.setattr(w, "_check_main_valve_open", lambda: True)
    monkeypatch.setattr(MAIN, "set_process_log_file", lambda **k: None)
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lvl, msg: logs.append((lvl, msg)))
    started = []
    w.request_process_start.disconnect()
    w.request_process_start.connect(lambda p: started.append(dict(p)))
    w._started = started; w._logs = logs
    yield w
    w.request_process_start.disconnect()
    w.request_process_start.connect(w.process_controller.start_process_flow)
    w.process_running = False
    w.ui.Sputter_Start_Button.setEnabled(True)


def test_T45_manual_process_never_owns_heater(manual):
    w = manual
    w._set_heater_button_view(True, "OFF")               # 히터 패널: 600°C ON 상태
    w.ui.heater_sv_edit.setText("600")
    feed(w, make_heater_st(run=True, pv=600.0, sv=600.0))
    w._handle_start_process()
    assert len(w._started) == 1, MAIN.QMessageBox.warning.call_args
    p = w._started[0]
    assert p["use_heater"] is False and p["heater_temp"] == 0.0
    assert w._process_heater_claimed is False
    assert ("정보", "[히터] 미사용") in w._logs
    assert w.process_running is True


def test_T46_start_rejected_when_heater_atmosphere_active(manual):
    w = manual
    w.heater_atmosphere.is_active.return_value = True
    w._handle_start_process()
    assert w._started == [] and w.process_running is False
    msg = MAIN.QMessageBox.warning.call_args.args
    assert msg[1] == "공정 시작 불가" and "히터 가스·압력이 잡혀 있습니다. [가스 해제] 후 시작하세요." in msg[2]
    assert not hasattr(w, "_start_after_release")


def test_T47_gas_hold_tick_noop_during_process(fresh, monkeypatch):
    w = fresh
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lvl, msg: logs.append((lvl, msg)))
    w.heater_atmosphere.is_active.return_value = True
    w.process_running = True
    try:
        feed(w, make_heater_st(run=True, pv=500.0))
        feed(w, make_heater_st(run=False, pv=500.0))         # 하강 엣지, 뜨거움 → 원래는 가스 유지
        assert w._atm_hold is False and w.heater_atmosphere.release.call_count == 0
        assert any("공정 중이라 가스·압력 유지/해제는 하지 않습니다" in m for _, m in logs)
        feed(w, make_heater_st(run=False, pv=50.0))          # 식어도 해제 없음
        assert w.heater_atmosphere.release.call_count == 0
    finally:
        w.process_running = False


# ───────────────────────── 히터 도달 카드 ─────────────────────────
def test_T48_heater_reached_card_once_from_process_path(fresh):
    w = fresh
    w.process_running = True; w.current_process_name = "CeO2 #1"
    try:
        w.process_controller.heater_reached.emit({"pv": 599.6, "target": 600.0, "took_sec": 3725, "next": "압력 안정화 대기"})
        spin(50)
        assert w.chat_chk.notify_heater_reached.call_count == 1
        sub, fields = w.chat_chk.notify_heater_reached.call_args.args
        assert sub == '공정 "CeO2 #1"'
        assert fields == {"목표": "600.0°C", "도달 TC1": "599.6°C", "도달 TC2": "--.-",
                          "유지 모드": "미진입 (상태: idle)",        # 유지 모드 결과는 도달 카드에 합쳐진다(T84)
                          "승온 소요": "62분 5초", "다음 단계": "압력 안정화 대기"}
        assert w.chat_chk.notify_heater_run.call_count == 0
    finally:
        w.process_running = False
    # 발행 지점은 _heater_wait 의 "히터 온도 도달 완료" 통과 지점 하나뿐(수동 히터·히터 레시피는 이 경로가 없다)
    import inspect, controller.process_controller as PC
    src = inspect.getsource(PC.SputterProcessController)
    assert src.count("self.heater_reached.emit(") == 1
    body = src.split("def _heater_wait(")[1].split("\n    def ")[0]
    assert "히터 온도 도달 완료" in body and "self.heater_reached.emit(" in body
    assert body.index("self.heater_reached.emit(") < body.rindex("self._next_step()")


# ───────────────────────── 코일 전이 단일화(UI 경유 MV OFF / ALL STOP 순서) ─────────────────────────
def _fake_plc_poll(w, monkeypatch):
    """PLC 스레드 대신: update_port_state 는 즉시 display 만, 폴링은 테스트가 값을 정해 plc_bit_changed 를 낸다."""
    plc = w.plc_controller
    writes = []
    monkeypatch.setattr(plc, "update_port_state", lambda n, v: writes.append((n, v)))
    return writes


def test_T52_ui_mv_off_during_process_aborts_via_poll_transition(safe, monkeypatch):
    w = safe
    writes = _fake_plc_poll(w, monkeypatch)
    w.process_running = True; w._chat_reset_run_state()
    w._on_plc_bit_changed("MV_button", True, None)
    w.plc_controller.update_port_state("MV_button", False)          # UI 경유 쓰기(즉시 표시만, 캐시 불변)
    assert writes == [("MV_button", False)] and w._aborts == []
    w._on_plc_bit_changed("MV_button", False, True)                 # 다음 폴링이 본 전이
    assert len(w._aborts) == 1 and w._aborts[0][0] == "메인밸브 닫힘 (M00003 OFF)"
    assert ("PLC", "MV_button ON→OFF") in w._logs


def test_T53_all_stop_sets_flags_before_plc_emergency_then_poll_off_no_abort(safe, monkeypatch):
    w = safe
    order = []
    w.request_plc_emergency_stop.disconnect()
    w.request_plc_emergency_stop.connect(lambda: order.append(("emg", w._chat_emergency_stopped, w._chk_process_ok)))
    monkeypatch.setattr(w, "_chat_notify_failed_now", lambda *a, **k: order.append(("card",)))
    try:
        w.process_running = True; w._chat_reset_run_state(); w._chk_process_ok = True
        w._on_plc_bit_changed("MV_button", True, None)
        w._on_all_stop_clicked()
        assert order[0] == ("emg", True, False)                      # 플래그가 먼저
        assert w._chat_emergency_stopped is True
        w._on_plc_bit_changed("MV_button", False, True)             # 비상정지로 꺼진 코일을 다음 폴링이 본다
        assert w._aborts == []                                       # 이중 중단 없음
        assert ("PLC", "MV_button ON→OFF") in w._logs
    finally:
        w.request_plc_emergency_stop.disconnect()
        w.request_plc_emergency_stop.connect(w.plc_controller.on_emergency_stop)
        MAIN.QMessageBox.reset_mock()


# ═══════════════ 유지 모드 진입 대기 + 폴백 순서 (process_controller._heater_wait / _heater_hold_wait) ═══════════════
from PyQt6.QtCore import QObject, pyqtSignal as Signal
import controller.process_controller as PC


class _FakePlcStatus(QObject):
    update_heater_status = Signal(dict)

    def __init__(self):
        super().__init__()
        self.st = make_heater_st(run=True, pv=600.0, sv=600.0, sv_ramp=600.0, cur_sv=600.0, mv=1077)

    def get_heater_status(self):
        return dict(self.st)


@pytest.fixture
def pc(qapp, monkeypatch):
    """테스트 스레드에서 도는 컨트롤러 + 50ms 폴링 흉내. soak 1초, 유지 대기 상한 1초.
    폴백 요청(request_hold_force)은 main 을 흉내 내는 _force 가 받는다: pc._force_ok[mode]=(수락, holding 까지 ms|None, 사유)."""
    plc = _FakePlcStatus()
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), plc, None)
    c._running = True; c._stop_pending = False; c._steps = []; c._idx = 0
    calls = {"next": 0, "abort": [], "msgs": [], "failed": [], "force": []}
    monkeypatch.setattr(c, "_next_step", lambda: calls.__setitem__("next", calls["next"] + 1))
    monkeypatch.setattr(c, "_abort_with_error", lambda r: calls["abort"].append(r))
    c.status_message.connect(lambda l, m: calls["msgs"].append((l, m)))
    c.heater_hold_failed.connect(lambda d: calls["failed"].append(d))
    monkeypatch.setattr(PC, "HEATER_SOAK_TIME_SEC", 1)
    monkeypatch.setattr(PC, "HEATER_HOLD_WAIT_SEC", 1.0)
    monkeypatch.setattr(PC, "HEATER_HOLD_FAIL_ACTION", "abort")
    monkeypatch.setattr(PC, "ENGAGE_TIMEOUT_SEC", 0.2)
    hold = {"mode": "tc2", "state": "arming", "kind": None, "holding": False, "sv2": None, "value": None,
            "gave_up": None, "relaxed": False, "force_result": None}
    c.set_hold_state_provider(lambda: dict(hold))
    c._force_ok = {"tc2": (False, None, "TC2 값 없음(D00011=-1) — DAC 상한 고정으로 대체"),
                   "dac": (False, None, "측정 창 평균도 현재 MV 도 없음")}

    def _force(mode, mv):
        calls["force"].append((mode, mv))
        ok, ms, why = c._force_ok[mode]
        hold["force_result"] = {"mode": mode, "ok": ok, "reason": ("" if ok else why)}
        if ok:
            hold.update(state=("engaging_sv2" if mode == "tc2" else "holding"), kind=mode, relaxed=True)
            if ms is not None:
                def _hold():
                    hold.update(state="holding", holding=True, sv2=(893.4 if mode == "tc2" else None),
                                value=(None if mode == "tc2" else 1077))
                QTimer.singleShot(ms, _hold)
    c.request_hold_force.connect(_force)
    feeder = QTimer(); feeder.setInterval(50)
    feeder.timeout.connect(lambda: plc.update_heater_status.emit(dict(plc.st)))
    feeder.start()
    c._plc = plc; c._hold = hold; c._calls = calls
    yield c
    feeder.stop()


def test_T73_hold_wait_passes_when_holding_arrives(pc):
    QTimer.singleShot(1500, lambda: pc._hold.update(state="holding", kind="tc2", holding=True, sv2=854.9))
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["failed"] == [] and c["force"] == []
    assert any("유지 모드 진입 확인 (tc2, SV2 854.9°C)" in m for _, m in c["msgs"])


def test_T74_timeout_relaxed_tc2_succeeds_and_continues(pc):
    pc._force_ok["tc2"] = (True, 150, "")
    pc._heater_wait(600.0)
    c = pc._calls
    assert [m for m, _ in c["force"]] == ["tc2"] and c["next"] == 1 and c["abort"] == []
    assert len(c["failed"]) == 1 and c["failed"][0]["action"] == "tc2_relaxed" and c["failed"][0]["sv2"] == 893.4
    assert c["failed"][0]["why"] == "정착 창 미확보"
    assert any(l == "히터(경고)" and "완화 조건으로 TC2 추종 진입(SV2 893.4°C) 후 진행" in m for l, m in c["msgs"])


def test_T74b_tc2_rejected_then_dac_succeeds(pc):
    pc._force_ok["dac"] = (True, 100, "")
    pc._heater_wait(600.0)
    c = pc._calls
    assert [m for m, _ in c["force"]] == ["tc2", "dac"] and c["next"] == 1 and c["abort"] == []
    f = c["failed"][0]
    assert f["action"] == "dac" and "TC2 값 없음" in f["tc2_why"]
    assert any(l == "히터(경고)" and "TC2 추종 불가(TC2 값 없음" in m and "DAC 상한 강제 고정(DAC 상한 1077)" in m
               for l, m in c["msgs"])


def test_T74c_tc2_handshake_fails_then_dac(pc):
    pc._force_ok["tc2"] = (True, None, "")                     # 수락됐지만 holding 이 오지 않는다
    pc._force_ok["dac"] = (True, 100, "")
    pc._heater_wait(600.0)
    c = pc._calls
    assert [m for m, _ in c["force"]] == ["tc2", "dac"] and c["next"] == 1
    assert "핸드셰이크 실패" in c["failed"][0]["tc2_why"]


def test_T75_both_fail_abort(pc):
    pc._heater_wait(600.0)
    c = pc._calls
    assert [m for m, _ in c["force"]] == ["tc2", "dac"] and c["next"] == 0 and len(c["abort"]) == 1
    f = c["failed"][0]
    assert f["action"] == "abort" and "완화 tc2 불가(TC2 값 없음" in f["reason"] and "dac 불가(측정 창" in f["reason"]


def test_T75b_both_fail_proceed_warns(pc, monkeypatch):
    monkeypatch.setattr(PC, "HEATER_HOLD_FAIL_ACTION", "proceed")
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["failed"][0]["action"] == "proceed"
    assert any("경고만 내고 진행" in m for _, m in c["msgs"])


def test_T76_mode_off_passes_immediately(pc):
    pc._hold["mode"] = "off"
    t0 = time.monotonic()
    pc._heater_wait(600.0)
    assert pc._calls["next"] == 1 and time.monotonic() - t0 < 1.8
    assert not any("진입 대기" in m for _, m in pc._calls["msgs"]) and pc._calls["force"] == []


def test_T77_fault_and_stop_during_hold_wait(pc):
    QTimer.singleShot(1400, lambda: pc._plc.st.update(fault=True))
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 0 and len(c["abort"]) == 1 and "히터 이상 발생(유지 모드 대기 중)" in c["abort"][0]
    pc._calls.update(next=0, abort=[]); pc._plc.st.update(fault=False)
    QTimer.singleShot(1400, lambda: setattr(pc, "_stop_pending", True))
    pc._heater_wait(600.0)
    assert pc._calls["next"] == 0 and pc._calls["abort"] == [] and pc._calls["failed"] == []


def test_T78_give_up_ends_wait_early_then_relaxed_tc2(pc):
    pc._force_ok["tc2"] = (True, 150, "")
    QTimer.singleShot(1300, lambda: pc._hold.update(gave_up="도달 시점 출력이 상한에 붙어 있어 고정하지 않음"))
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["failed"] and "상한에 붙어" in c["failed"][0]["reason"] and c["failed"][0]["action"] == "tc2_relaxed"
    assert c["next"] == 1


def _st(**kw):
    d = {"run": True, "pv": 599.9, "sv": 600.0, "sv_ramp": 600.0, "cur_sv": 600.0, "mv": 1077,
         "pv2": 893.4, "sv2": 0.0, "sv2_max": 1100.0, "ot2_limit": 1150.0, "pv_sel_eff": False}
    d.update(kw); return d


def test_T79_0922_end_to_end_strict_fail_then_relaxed_tc2(pc):
    """09-22 재현: 실제 HeaterHold 에 6개 탈락 창을 그대로 먹여 엄격 캡처를 실패시킨 뒤(당시 절대 게이트로 되돌려 재현),
    컨트롤러 폴백 ① 이 force_tc2_hold 로 tc2 추종에 진입하는 것까지 한 번에."""
    import controller.heater_hold as HH
    from test_heater_hold import Harness, _window, _run_window
    H = Harness("tc2")
    saved = (HH.DRIFT_PV_REL, HH.DRIFT_MV_REL, HH.DRIFT_TC2_REL)
    HH.DRIFT_PV_REL, HH.DRIFT_MV_REL, HH.DRIFT_TC2_REL = 0.0, 0.0, 0.0        # 09-22 당시 게이트
    try:
        for pv, mv, tc2 in ((-1.01, 22.3, -3.60), (0.56, -13.8, -3.63), (1.21, -26.4, 1.15),
                            (-0.32, 12.2, 13.89), (-1.64, 35.5, -2.91), (0.56, -14.0, -2.53)):
            st = _run_window(H, _window(1077, 20, mv), _window(599.9, 1.0, pv), _window(893.4, 2.0, tc2))
            assert st == "arming"                                                # 엄격 캡처 전부 탈락
    finally:
        HH.DRIFT_PV_REL, HH.DRIFT_MV_REL, HH.DRIFT_TC2_REL = saved
    assert H.ev == [] and len(H.h._ring) >= 4

    def _force(mode, mv):                                                        # main 흉내: 실제 force_*_hold 호출
        ok = H.h.force_tc2_hold() if mode == "tc2" else H.h.force_dac_hold(mv)
        pc._hold["force_result"] = {"mode": mode, "ok": ok, "reason": "" if ok else H.h.last_force_reason}
        if ok and mode == "tc2":                                                 # 핸드셰이크: 되읽기 일치 → M0004B
            H.step(**_st(sv2=H.h.sv2)); H.step(**_st(sv2=H.h.sv2, pv_sel_eff=True))
        pc._hold.update(state=H.h.state, kind=H.h.kind, holding=H.h.is_holding(), sv2=H.h.sv2, relaxed=True)
    pc.request_hold_force.disconnect()
    pc.request_hold_force.connect(_force)
    pc._heater_wait(600.0)
    c = pc._calls
    assert H.h.is_holding() and H.h.kind == "tc2" and H.h.engaged_relaxed is True
    assert abs(H.h.sv2 - 893.4) < 3.0
    assert [e[0] for e in H.ev] == ["sv2", "sel"]                                # D00035 → M0004A 순서
    assert c["next"] == 1 and c["failed"][0]["action"] == "tc2_relaxed"
    assert any("완화 조건으로 TC2 추종 진입 — SV2" in m and "정착 창 미확보" in m for l, m in H.msgs)


# ═══════════════ ALL STOP 이 DC 를 직접 끈다 · DC OFF 미확인 알림 ═══════════════
@pytest.fixture
def allstop(safe, monkeypatch):
    """QMetaObject.invokeMethod 를 기록용으로 바꿔 ALL STOP 의 직결 OFF 호출을 본다."""
    w = safe
    calls = []
    monkeypatch.setattr(MAIN, "QMetaObject",
                        type("MO", (), {"invokeMethod": staticmethod(lambda obj, name, *a: calls.append((obj, name)))}))
    emg = []
    w.request_plc_emergency_stop.disconnect()
    w.request_plc_emergency_stop.connect(lambda: emg.append(len(calls)))
    w._emg = emg; w._invokes = calls
    monkeypatch.setattr(w, "_chat_notify_failed_now", lambda *a, **k: None)
    monkeypatch.setattr(w.heater_recipe, "is_running", lambda: False, raising=False)
    yield w
    w.request_plc_emergency_stop.disconnect()
    w.request_plc_emergency_stop.connect(w.plc_controller.on_emergency_stop)


def _names(w, obj):
    return [n for o, n in w._invokes if o is obj]


def test_T102_all_stop_without_process_still_kills_dc(allstop):
    w = allstop
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w._on_all_stop_clicked()
    assert _names(w, w.dcpower_controller) == ["emergency_off"]          # 공정 상태와 무관
    assert _names(w, w.rfpulse_controller) == ["stop_process"]
    assert w._emg == [0] and w._stops == []                              # PLC 비상정지가 DC 보다 먼저
    assert w._invokes[0][1] == "emergency_off"


def test_T103_all_stop_during_process(allstop):
    w = allstop
    w.process_running = True; w._chat_reset_run_state(); w._chk_process_ok = True
    w._on_plc_bit_changed("MV_button", True, None)
    w._on_all_stop_clicked()
    assert _names(w, w.dcpower_controller) == ["emergency_off"]
    assert _names(w, w.rfpulse_controller) == ["stop_process"] and w._stops == [1]
    # T53 규칙: 플래그가 PLC 비상정지보다 먼저, 이후 폴링의 MV OFF 에 이중 중단 없음
    assert w._chat_emergency_stopped is True and w._chk_process_ok is False
    w._on_plc_bit_changed("MV_button", False, True)
    assert w._aborts == []


def test_T104_dc_off_unconfirmed_and_confirmed_chat(safe):
    w = safe
    w.chat_chk.reset_mock()
    w._on_dc_off_unconfirmed("ALL STOP")
    txt = [c.args[0] for c in w.chat_chk.notify_text.call_args_list]
    assert len(txt) == 1 and txt[0].startswith("❌ CHK DC 출력 OFF 미확인 (ALL STOP)") and "장비 전면" in txt[0]
    assert w.chat_chk.flush.called
    w.chat_chk.reset_mock()
    w._on_dc_off_confirmed("OFF 재시도", 73.0)
    txt = [c.args[0] for c in w.chat_chk.notify_text.call_args_list]
    assert txt == ["✅ CHK DC 출력 OFF 확인 (OFF 재시도, 미확인 73초 뒤)"]
    w.chat_chk = MagicMock()
