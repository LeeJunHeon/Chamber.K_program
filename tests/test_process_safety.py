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


# ═══════════════ 유지 모드 진입 대기 (process_controller._heater_wait + _heater_hold_wait) ═══════════════
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
    """테스트 스레드에서 도는 컨트롤러 + 50ms 폴링 흉내. soak 1초, 유지 대기 상한 1초."""
    plc = _FakePlcStatus()
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), plc, None)
    c._running = True; c._stop_pending = False; c._steps = []; c._idx = 0
    calls = {"next": 0, "abort": [], "msgs": [], "failed": [], "force": []}
    monkeypatch.setattr(c, "_next_step", lambda: calls.__setitem__("next", calls["next"] + 1))
    monkeypatch.setattr(c, "_abort_with_error", lambda r: calls["abort"].append(r))
    c.status_message.connect(lambda l, m: calls["msgs"].append((l, m)))
    c.heater_hold_failed.connect(lambda d: calls["failed"].append(d))
    c.request_hold_force_dac.connect(lambda mv: calls["force"].append(mv))
    monkeypatch.setattr(PC, "HEATER_SOAK_TIME_SEC", 1)
    monkeypatch.setattr(PC, "HEATER_HOLD_WAIT_SEC", 1.0)
    monkeypatch.setattr(PC, "HEATER_HOLD_FAIL_ACTION", "dac")
    hold = {"mode": "tc2", "state": "arming", "kind": None, "holding": False, "sv2": None, "value": None, "gave_up": None}
    c.set_hold_state_provider(lambda: dict(hold))
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
    assert c["next"] == 1 and c["abort"] == [] and c["failed"] == []
    assert any("유지 모드 진입 확인 (tc2, SV2 854.9°C)" in m for _, m in c["msgs"])
    assert any("유지 모드(tc2) 진입 대기 시작" in m for _, m in c["msgs"])


def test_T74_hold_wait_timeout_dac_forces_and_continues(pc):
    def _force(mv):                                    # main 이 force_dac_hold 를 실행한 것처럼
        pc._hold.update(state="holding", kind="dac", holding=True, value=1077)
    pc.request_hold_force_dac.connect(_force)
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["force"] == [1077] and c["next"] == 1 and c["abort"] == []
    assert len(c["failed"]) == 1 and c["failed"][0]["action"] == "dac" and c["failed"][0]["forced"] is True
    assert any(l == "히터(경고)" and "DAC 상한 강제 고정 후 진행" in m for l, m in c["msgs"])
    assert any("유지 모드 진입 확인 (dac, DAC 상한 1077)" not in m for _, m in c["msgs"])


def test_T74b_dac_force_fails_falls_back_to_abort(pc):
    pc._heater_wait(600.0)                             # force 요청에 아무도 응답하지 않음 → abort
    c = pc._calls
    assert c["force"] == [1077] and c["next"] == 0 and len(c["abort"]) == 1
    assert "유지 모드 진입 실패" in c["abort"][0] and c["failed"][0]["action"] == "abort"


def test_T75_fail_action_abort(pc, monkeypatch):
    monkeypatch.setattr(PC, "HEATER_HOLD_FAIL_ACTION", "abort")
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["force"] == [] and c["next"] == 0 and len(c["abort"]) == 1
    assert c["failed"][0]["action"] == "abort" and "대기 1s 초과" in c["failed"][0]["reason"]


def test_T75b_fail_action_proceed_warns(pc, monkeypatch):
    monkeypatch.setattr(PC, "HEATER_HOLD_FAIL_ACTION", "proceed")
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["failed"][0]["action"] == "proceed"
    assert any("경고만 내고 진행" in m for _, m in c["msgs"])


def test_T76_mode_off_passes_immediately(pc):
    pc._hold["mode"] = "off"
    t0 = time.monotonic()
    pc._heater_wait(600.0)
    assert pc._calls["next"] == 1 and time.monotonic() - t0 < 1.8          # soak 1초 + 즉시
    assert not any("진입 대기" in m for _, m in pc._calls["msgs"])


def test_T77_fault_and_stop_during_hold_wait(pc):
    QTimer.singleShot(1400, lambda: pc._plc.st.update(fault=True))
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 0 and len(c["abort"]) == 1 and "히터 이상 발생(유지 모드 대기 중)" in c["abort"][0]
    # 중단 요청
    pc._calls.update(next=0, abort=[]); pc._plc.st.update(fault=False)
    QTimer.singleShot(1400, lambda: setattr(pc, "_stop_pending", True))
    pc._heater_wait(600.0)
    assert pc._calls["next"] == 0 and pc._calls["abort"] == [] and pc._calls["failed"] == []


def test_T78_give_up_ends_wait_early(pc):
    QTimer.singleShot(1300, lambda: pc._hold.update(gave_up="도달 시점 출력이 상한에 붙어 있어 고정하지 않음"))
    QTimer.singleShot(1400, lambda: pc._hold.update(state="holding", kind="dac", holding=True, value=1176))
    pc.request_hold_force_dac.connect(lambda mv: None)
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["failed"] and "상한에 붙어" in c["failed"][0]["reason"] and c["next"] == 1
