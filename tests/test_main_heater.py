# -*- coding: utf-8 -*-
"""T6~T12, T20~T21 — main.MainDialog(offscreen). PLC 포트 없음이 정상. 챗·ERP·CSV 는 스텁."""
import threading
import time
from unittest.mock import MagicMock

import pytest
from PyQt6.QtCore import QObject, QThread, QTimer, QEventLoop, pyqtSlot as Slot, Qt

from conftest import make_heater_st

import main as MAIN


def spin(ms=50):
    lp = QEventLoop(); QTimer.singleShot(ms, lp.quit); lp.exec()


@pytest.fixture(scope="module")
def win(qapp):
    MAIN.QMessageBox = MagicMock()
    w = MAIN.MainDialog()
    w.chat_chk = MagicMock()
    w.erp = MagicMock()
    MAIN.append_chk_csv_row = lambda r: True
    w.heater_atmosphere.release = MagicMock()
    w.heater_atmosphere.is_active = MagicMock(return_value=False)
    yield w


@pytest.fixture
def fresh(win):
    """테스트마다 상태를 초기화한다."""
    w = win
    w._heater_run_prev_view = False
    w._heater_fault_off_sent = False
    w._heater_pending = None
    w._heater_stale = False
    w._heater_status_t = 0.0
    w._heater_chat_t0 = 0.0
    w._atm_hold = False
    w.process_running = False; w.csv_mode = False
    w.chat_chk.reset_mock(); MAIN.QMessageBox.reset_mock()
    w._heater_logger.start = MagicMock(return_value=None)
    w._heater_logger.stop = MagicMock()
    w._heater_logger.write_row = MagicMock()
    w.heater_atmosphere.release.reset_mock()
    w.heater_atmosphere.is_active.return_value = False
    w.heater_ramp.stop(restore_rate=False)
    w.plc_controller._heater_last = {}
    w._set_heater_button_view(False, "ON")
    w.ui.heater_onoff_button.setEnabled(True)
    return w


def feed(w, st):
    """PLC 폴링을 흉내낸다 — 캐시(get_heater_status)와 화면 갱신을 함께."""
    w.plc_controller._heater_last = dict(st)
    w.update_heater_display(dict(st))


def test_T6_button_follows_run_unless_pending(fresh):
    w = fresh; btn = w.ui.heater_onoff_button
    for run in (True, False, True, False):
        feed(w, make_heater_st(run=run))
        assert btn.isChecked() is run and btn.text() == ("OFF" if run else "ON")
    w._heater_pending = ("manual_on", 300.0); w._show_heater_pending_button()
    feed(w, make_heater_st(run=False))
    assert btn.isChecked() is True and btn.text() == "취소"     # pending 이면 손대지 않는다
    w._heater_pending = None


def test_T7_reentry_interlock_and_empty_target_send_nothing(fresh):
    w = fresh; btn = w.ui.heater_onoff_button
    emitted = []
    w.request_heater_run.connect(lambda v: emitted.append(v))
    try:
        feed(w, make_heater_st(run=False, itl=False))
        btn.setChecked(True)          # 사용자 클릭 → toggled(True) → 인터락 차단
        spin()
        assert emitted == [] and btn.isChecked() is False and btn.text() == "ON"
        assert MAIN.QMessageBox.warning.called
        # 목표값 빈칸
        MAIN.QMessageBox.reset_mock()
        feed(w, make_heater_st(run=False, itl=True))
        w.ui.heater_sv_edit.setText("")
        btn.setChecked(True)
        spin()
        assert emitted == [] and btn.isChecked() is False and btn.text() == "ON"
    finally:
        w.request_heater_run.disconnect()
        w.request_heater_run.connect(w.plc_controller.set_heater_run)


def test_T8_fault_rising_edge_sends_run_off_once(fresh):
    w = fresh; emitted = []
    w.request_heater_run.connect(lambda v: emitted.append(v))
    try:
        feed(w, make_heater_st(run=True, fault=False))
        feed(w, make_heater_st(run=True, fault=True))
        assert emitted == [False]
        for _ in range(5):
            feed(w, make_heater_st(run=True, fault=True))
        assert emitted == [False]                          # 에피소드당 1회
        feed(w, make_heater_st(run=True, fault=False))
        feed(w, make_heater_st(run=True, fault=True))
        assert emitted == [False, False]                   # 해제 후 재발 → 다시 1회
    finally:
        w.request_heater_run.disconnect()
        w.request_heater_run.connect(w.plc_controller.set_heater_run)


def test_T9_reset_guard_when_run(fresh):
    w = fresh; resets = []
    w.request_heater_reset.connect(lambda: resets.append(1))
    try:
        feed(w, make_heater_st(run=True, fault=True))
        w._on_heater_reset_clicked()
        assert resets == [] and MAIN.QMessageBox.warning.called
        assert not MAIN.QMessageBox.question.called
    finally:
        w.request_heater_reset.disconnect()
        w.request_heater_reset.connect(w.plc_controller.reset_heater_fault)


class _FakePlc(QObject):
    def __init__(self):
        super().__init__(); self.arrived = []
    @Slot(float)
    def set_heater_target(self, v): self.arrived.append(("target", v))
    @Slot(bool)
    def set_heater_run(self, on): self.arrived.append(("run", on))


def test_T10_process_target_then_run_order(fresh, qapp):
    w = fresh
    fake = _FakePlc(); th = QThread(); fake.moveToThread(th); th.start()
    w.heater_ramp.start = lambda *a, **k: False         # 램프를 못 쓰면 목표를 직접 보낸다
    w.request_heater_target.disconnect(); w.request_heater_run.disconnect()
    w.request_heater_target.connect(fake.set_heater_target, Qt.ConnectionType.QueuedConnection)
    w.request_heater_run.connect(fake.set_heater_run, Qt.ConnectionType.QueuedConnection)
    try:
        def emitter():
            w.process_controller.set_heater_target.emit(600.0)
            w.process_controller.set_heater_run.emit(True)
        t = threading.Thread(target=emitter); t.start(); t.join()
        for _ in range(20):
            spin(50)
            if len(fake.arrived) >= 2:
                break
        assert fake.arrived == [("target", 600.0), ("run", True)]
    finally:
        w.request_heater_target.disconnect(); w.request_heater_run.disconnect()
        w.request_heater_target.connect(w.plc_controller.set_heater_target)
        w.request_heater_run.connect(w.plc_controller.set_heater_run)
        th.quit(); th.wait(1000)
        w.heater_ramp.start = type(w.heater_ramp).start.__get__(w.heater_ramp)


def test_T11_single_edge_computation_feeds_all_consumers(fresh):
    w = fresh
    w.heater_atmosphere.is_active.return_value = True
    seq = [False, False, True, True, True, False, False, True, False]
    for run in seq:
        feed(w, make_heater_st(run=run, pv=50.0))       # PV ≤ 해제 임계 → 하강 엣지마다 release 1회
    assert w._heater_logger.start.call_count == 2
    assert w._heater_logger.stop.call_count == 2
    cards = [c.args[0] for c in w.chat_chk.notify_heater_run.call_args_list]
    assert cards == [True, False, True, False]
    assert w.heater_atmosphere.release.call_count == 2
    # 첫 폴링이 run True 인 시퀀스 [T,T,F]
    w._heater_run_prev_view = False
    w._heater_logger.start.reset_mock(); w._heater_logger.stop.reset_mock(); w.chat_chk.reset_mock()
    for run in (True, True, False):
        feed(w, make_heater_st(run=run, pv=50.0))
    assert w._heater_logger.start.call_count == 1 and w._heater_logger.stop.call_count == 1
    assert [c.args[0] for c in w.chat_chk.notify_heater_run.call_args_list] == [True, False]


def test_T12_stale_display_and_restore(fresh):
    w = fresh
    feed(w, make_heater_st(run=False))
    w._heater_status_t = time.monotonic() - 6.0
    w._refresh_heater_progress()
    assert "응답 없음" in w.ui.heater_status_label.text()
    assert w.ui.heater_onoff_button.isEnabled() is False
    assert w._heater_stale is True
    feed(w, make_heater_st(run=False))
    assert w._heater_stale is False
    assert "응답 없음" not in w.ui.heater_status_label.text()
    assert w.ui.heater_onoff_button.isEnabled() is True


def test_T20_stale_restores_original_stylesheet_exactly(fresh):
    w = fresh; ui = w.ui
    feed(w, make_heater_st(run=False))
    pv0, sv0 = ui.heater_pv_edit.styleSheet(), ui.heater_sv_big.styleSheet()
    assert "#1f2937" in pv0 and "#6b7280" in sv0          # UI.py 원본
    w._heater_status_t = time.monotonic() - 6.0
    w._refresh_heater_progress()
    assert w._heater_stale is True
    pv1, sv1 = ui.heater_pv_edit.styleSheet(), ui.heater_sv_big.styleSheet()
    assert MAIN.HEATER_STALE_FG in pv1 and MAIN.HEATER_STALE_FG in sv1
    # color 만 바뀌고 나머지(font-size 등)는 그대로
    assert pv1.replace(MAIN.HEATER_STALE_FG, "#1f2937") == pv0
    assert sv1.replace(MAIN.HEATER_STALE_FG, "#6b7280") == sv0
    feed(w, make_heater_st(run=False))
    assert w._heater_stale is False
    assert ui.heater_pv_edit.styleSheet() == pv0 and ui.heater_sv_big.styleSheet() == sv0
    assert w._heater_style_orig == {}
    # 원본이 다른 스타일이어도(예: 사용자 정의) 그 스타일로 복원된다
    custom = "QLineEdit {color: red; font-size: 10pt;}"
    ui.heater_pv_edit.setStyleSheet(custom)
    w._heater_set_stale(True, 7)
    assert ui.heater_pv_edit.styleSheet() == f"QLineEdit {{color: {MAIN.HEATER_STALE_FG}; font-size: 10pt;}}"
    w._heater_set_stale(False, 0)
    assert ui.heater_pv_edit.styleSheet() == custom
    ui.heater_pv_edit.setStyleSheet(pv0)


def _erp(w, on):
    """ERP 명령 1건을 드레인 타이머 경로로 실행하고 (ok, reason) 을 돌려준다."""
    w.erp.rejected = False
    w.erp.pop_commands.return_value = [{"id": 1, "command": "HEATER_ONOFF", "args": {"on": on}}]
    w.erp.cmd_result.reset_mock()
    w._erp_cmd_timer.timeout.emit()
    w.erp.pop_commands.return_value = []
    args = w.erp.cmd_result.call_args.args
    return bool(args[1]), (args[2] if len(args) > 2 else "")


def test_T21_erp_heater_onoff_pending_counts_as_on(fresh):
    w = fresh
    calls = []
    orig = w._on_heater_onoff_toggled
    w._on_heater_onoff_toggled = lambda v: calls.append(v)
    try:
        feed(w, make_heater_st(run=False))
        w._heater_pending = ("manual_on", 300.0); w._show_heater_pending_button()
        ok, why = _erp(w, True)
        assert ok is False and "준비 중" in why and calls == []
        ok, why = _erp(w, False)                             # 준비 중 OFF = 취소 경로
        assert ok is True and calls == [False]
        w._heater_pending = None
        ok, why = _erp(w, False)
        assert ok is False and "이미 OFF" in why
        feed(w, make_heater_st(run=True))
        ok, why = _erp(w, True)
        assert ok is False and "이미 ON" in why
        assert calls == [False]
    finally:
        w._on_heater_onoff_toggled = orig
        w._heater_pending = None
        w.erp.pop_commands.return_value = []


def test_T24_stale_regex_leaves_background_color(fresh):
    w = fresh; ui = w.ui
    orig = ui.heater_sv_big.styleSheet()
    custom = "QLabel {background-color: #fff; color: #111;}"
    ui.heater_sv_big.setStyleSheet(custom)
    w._heater_set_stale(True, 7)
    assert ui.heater_sv_big.styleSheet() == f"QLabel {{background-color: #fff; color: {MAIN.HEATER_STALE_FG};}}"
    w._heater_set_stale(False, 0)
    assert ui.heater_sv_big.styleSheet() == custom
    ui.heater_sv_big.setStyleSheet(orig)


# ───────────────────────── TC1/TC2 표시 ─────────────────────────
def _hold_tc2(w, sv2=1052.3):
    """HeaterHold 를 tc2 holding 으로 놓는다(상태기 내부 dict 직접 설정 — 표시 테스트용)."""
    h = w.heater_hold._h
    h.update(state="holding", kind="tc2", sv2=sv2, final=600.0)


def test_T34_tc2_label_and_status_text(fresh):
    w = fresh; ui = w.ui
    assert ui.heater_pv_title.text() == "TC1"
    feed(w, make_heater_st(run=False, pv2=None))
    assert ui.heater_pv2_label.text() == "TC2 --.-"
    feed(w, make_heater_st(run=True, pv2=1052.3))
    assert ui.heater_pv2_label.text() == "TC2 1052.3 °C"
    assert "#6b7280" in ui.heater_pv2_label.styleSheet()
    # TC2 추종 중: 목표 표시 + 운전색, 상태 문구, SV 는 D00012(TC1 목표), 편차는 TC1 − 최종 목표
    w.heater_hold.mode = "off"                       # tick 이 상태를 건드리지 않게
    _hold_tc2(w)
    w.heater_hold.tick = lambda st, f=None: None
    try:
        feed(w, make_heater_st(run=True, pv=600.0, sv=600.0, cur_sv=1052.3, pv2=1052.3, pv_sel_eff=True))
        assert ui.heater_pv2_label.text() == "TC2 1052.3 → 1052.3"
        assert "#2e7d32" in ui.heater_pv2_label.styleSheet()
        assert ui.heater_status_label.text() == "운전 중 · TC2 추종"
        assert ui.heater_sv_big.text() == "600.0"
        assert ui.heater_dev_label.text() == "Δ+0.0"
    finally:
        del w.heater_hold.tick
        w.heater_hold._h = w.heater_hold._fresh()
        w.heater_hold.mode = "dac"
    feed(w, make_heater_st(run=True, pv2=1052.3))
    assert "#6b7280" in ui.heater_pv2_label.styleSheet()
    assert ui.heater_status_label.text() == "운전 중"


def test_T35_stale_includes_pv2_label_and_restores(fresh):
    w = fresh; ui = w.ui
    feed(w, make_heater_st(run=False, pv2=1052.3))
    orig = ui.heater_pv2_label.styleSheet()
    w._heater_status_t = time.monotonic() - 6.0
    w._refresh_heater_progress()
    assert w._heater_stale and MAIN.HEATER_STALE_FG in ui.heater_pv2_label.styleSheet()
    feed(w, make_heater_st(run=False, pv2=1052.3))
    assert ui.heater_pv2_label.styleSheet() == orig


def test_T36_lcd_children_no_overlap_and_inside(win):
    from PyQt6.QtCore import QRect
    lcd = win.ui.heater_lcd
    kids = [c for c in lcd.children() if hasattr(c, "geometry") and c.parent() is lcd]
    box = QRect(0, 0, lcd.width(), lcd.height())
    names = [k.objectName() for k in kids]
    assert "heater_pv2_label" in names and lcd.width() == 200 and lcd.height() == 156
    for k in kids:
        assert box.contains(k.geometry()), (k.objectName(), k.geometry().getRect())
    for i, a in enumerate(kids):
        for b in kids[i + 1:]:
            assert not a.geometry().intersects(b.geometry()), (a.objectName(), b.objectName())
    # SV 행과 그 아래는 그대로
    g = win.ui.heater_sv_big.geometry(); assert (g.x(), g.y()) == (30, 50)
    g = win.ui.heater_sv_small.geometry(); assert (g.x(), g.y()) == (8, 54)


# ───────────────────────── PLC 링크 ↔ 화면 ─────────────────────────
def test_T37_plc_link_down_up_ui(fresh):
    w = fresh; ui = w.ui
    w._plc_link_up = False; w._on_plc_link(True)          # 기준: 업 상태
    title0 = w.windowTitle()
    ui.Rotary_button.setChecked(True); ui.Door_Button.setChecked(True)
    w.set_indicator("Air", True)
    feed(w, make_heater_st(run=False))
    w._on_plc_link(False)
    names = [n for n in list(MAIN.PLC_COIL_MAP) + ["Door_Button"] if hasattr(ui, n)]   # Doorup/Doordn 은 Door_Button 하나
    assert len(names) >= 14 and "Door_Button" in names
    for n in names:
        b = getattr(ui, n)
        assert b.isChecked() is False and b.isEnabled() is False, n
    assert "#9e9e9e" in ui.Air_Indicator.styleSheet()
    assert w._erp_indicators["Air"] is True                 # 링크 다운 중 마지막 값 유지(거짓 OFF 금지)
    assert w.windowTitle() == title0 + " — PLC 연결 끊김"
    assert w._heater_stale is True and ui.heater_onoff_button.isEnabled() is False
    w._on_plc_link(False)                                   # 같은 값 재호출 — 제목 접미사가 겹치지 않는다
    assert w.windowTitle() == title0 + " — PLC 연결 끊김"
    w._on_plc_link(True)
    for n in names:
        assert getattr(ui, n).isEnabled() is True, n
    assert w.windowTitle() == title0
    assert w._heater_stale is True                          # 히터 stale 은 폴링이 푼다
    feed(w, make_heater_st(run=False))
    assert w._heater_stale is False
    w.set_indicator("Air", False)
    assert "#d6252f" in ui.Air_Indicator.styleSheet()


def test_T57_heater_stop_card_icon_ok_vs_fault(fresh, monkeypatch):
    """히터 종료 카드: 이상이면 ❌(FAIL), 아니면 ✅(SUCCESS). 시작은 ℹ️(INFO)."""
    from controller.chat_notifier import ChatNotifier
    w = fresh
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "off")
    posted = []
    real = ChatNotifier.__new__(ChatNotifier)
    real._post_card = lambda title, subtitle="", status="INFO", fields=None, urgent=False, route_params=None:         posted.append((title, status, dict(fields or {})))
    w.chat_chk = real
    try:
        for kw, why in ((dict(ot=True, fault=True), "이상 — 과온"), (dict(tc_err=True, fault=True), "이상 — 온도센서"),
                        (dict(wd_err=True, fault=True), "이상 — 통신 워치독"), (dict(fault=True), "이상")):
            posted.clear()
            feed(w, make_heater_st(run=True)); feed(w, make_heater_st(run=False, **kw))
            assert posted[0][:2] == ("히터 시작", "INFO")
            assert posted[1][0] == "히터 종료" and posted[1][1] == "FAIL" and posted[1][2]["사유"] == why, posted
            assert list(posted[1][2]) == ["마지막 TC1", "마지막 TC2", "운전 시간", "사유", "특이사항"]
        posted.clear()
        feed(w, make_heater_st(run=True)); feed(w, make_heater_st(run=False))
        assert posted[1][1] == "SUCCESS" and posted[1][2]["사유"] == "정지"
        posted.clear(); w.process_running = True; w._chat_reset_run_state(); w._chk_process_ok = True
        feed(w, make_heater_st(run=True)); feed(w, make_heater_st(run=False))
        assert posted[1][1] == "SUCCESS" and posted[1][2]["사유"] == "공정 종료"
    finally:
        w.process_running = False
        w.chat_chk = MagicMock()


def test_T58_heater_cards_tc1_tc2_fields(fresh, monkeypatch):
    from controller.chat_notifier import ChatNotifier
    w = fresh
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "off")
    posted = []
    real = ChatNotifier.__new__(ChatNotifier)
    real._post_card = lambda title, subtitle="", status="INFO", fields=None, urgent=False, route_params=None: \
        posted.append((title, status, dict(fields or {})))
    w.chat_chk = real
    try:
        w.ui.heater_sv_edit.setText("600")
        feed(w, make_heater_st(run=True, pv=598.7, pv2=1052.3))
        feed(w, make_heater_st(run=False, pv=590.0, pv2=None))
        assert posted[0][0] == "히터 시작" and posted[0][1] == "INFO"
        assert list(posted[0][2]) == ["목표", "램프", "현재 TC1", "현재 TC2"]
        assert posted[0][2]["현재 TC1"] == "598.7°C" and posted[0][2]["현재 TC2"] == "1052.3°C"
        assert posted[1][0] == "히터 종료" and posted[1][1] == "SUCCESS"
        assert list(posted[1][2]) == ["마지막 TC1", "마지막 TC2", "운전 시간", "사유", "특이사항"]
        assert posted[1][2]["마지막 TC1"] == "590.0°C" and posted[1][2]["마지막 TC2"] == "--.-"
        assert posted[1][2]["특이사항"] == "없음"
        posted.clear(); w.process_running = True
        monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "tc2")                     # 도달 카드는 유지 모드 결과를 싣는다
        w.heater_hold._h.update(state="holding", kind="tc2", sv2=1052.3)        # 엄격 캡처로 tc2 진입한 상태
        w.heater_hold.engaged_relaxed = False
        w.process_controller.heater_reached.emit({"pv": 599.6, "pv2": 1051.0, "target": 600.0, "took_sec": 65, "next": "압력 안정화"})
        spin(50)
        assert posted == [("히터 도달", "SUCCESS", {"목표": "600.0°C", "도달 TC1": "599.6°C", "도달 TC2": "1051.0°C",
                                                    "유지 모드": "TC2 추종 (SV2 1052.3°C)",
                                                    "승온 소요": "1분 5초", "다음 단계": "압력 안정화"})]
        posted.clear()
        w.process_controller.heater_reached.emit({"pv": 599.6, "pv2": None, "target": 600.0, "took_sec": 5, "next": ""})
        spin(50)
        assert posted[0][2]["도달 TC2"] == "--.-" and posted[0][2]["다음 단계"] == "-"
        assert not any("온도" in k for c in posted for k in c[2])       # "도달 온도"/"마지막 온도"/"현재" 키 없음
    finally:
        w.process_running = False
        w.chat_chk = MagicMock()
        w.heater_hold._h = w.heater_hold._fresh()
    import inspect, controller.process_controller as PC
    assert "\"pv2\": _pv2" in inspect.getsource(PC.SputterProcessController._heater_wait)


def test_T59_plc_link_initial_down_no_warning_then_transition_warns(fresh, monkeypatch):
    w = fresh
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lvl, msg: logs.append((lvl, msg)))
    w._plc_link_up = True
    w._on_plc_link(False, initial=True)                       # 생성자 경로
    assert logs == [] and w._plc_link_up is False
    w._on_plc_link(True)
    w._on_plc_link(False)
    assert [m for l, m in logs if l == "경고"] == ["PLC 링크 다운 — 수동 조작 잠금, 표시 초기화"]
    w._on_plc_link(True)


# ═══════════════ 도달 카드에 유지 모드 결과 합치기 ═══════════════
def _reached(w):
    w.chat_chk.notify_heater_reached.reset_mock(); w.chat_chk.notify_heater_alert.reset_mock()
    w.process_controller.heater_reached.emit({"pv": 599.6, "pv2": 893.4, "target": 600.0, "took_sec": 65, "next": "압력 안정화"})
    spin(50)
    assert w.chat_chk.notify_heater_reached.call_count == 1
    args, kw = w.chat_chk.notify_heater_reached.call_args
    return kw.get("ok"), args[1]["유지 모드"], list(args[1])


@pytest.fixture
def hold_card(fresh, monkeypatch):
    w = fresh
    w.process_running = True; w._chat_reset_run_state()
    h = w.heater_hold
    h._h = h._fresh(); h.engaged_relaxed = False; h.no_margin_warned = False
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "tc2")
    yield w
    w.process_running = False; h._h = h._fresh(); h.engaged_relaxed = False; h.no_margin_warned = False


def _set_hold(w, kind, relaxed=False, **kw):
    w.heater_hold._h.update(state="holding", kind=kind, **kw); w.heater_hold.engaged_relaxed = relaxed


def test_T84_reached_card_strict_tc2(hold_card):
    w = hold_card; _set_hold(w, "tc2", sv2=893.4)
    ok, txt, keys = _reached(w)
    assert ok is True and txt == "TC2 추종 (SV2 893.4°C)"
    assert keys[:4] == ["목표", "도달 TC1", "도달 TC2", "유지 모드"]         # "도달 TC2" 바로 다음
    assert w.chat_chk.notify_heater_alert.call_count == 0


def test_T84_reached_card_relaxed_tc2(hold_card):
    w = hold_card; _set_hold(w, "tc2", relaxed=True, sv2=893.4)
    w._on_heater_hold_failed({"action": "tc2_relaxed", "reason": "유지 모드 진입 대기 600s 초과", "sv2": 893.4, "why": "정착 창 미확보"})
    assert w.chat_chk.notify_heater_alert.call_count == 0                  # 별도 카드 없음
    ok, txt, _ = _reached(w)
    assert ok is False and txt == "TC2 추종 — 완화 조건 (SV2 893.4°C, 정착 창 미확보)"
    assert w._hold_fail_info is None                                        # 쓴 뒤 비운다


def test_T84_reached_card_dac_fallback(hold_card):
    w = hold_card; _set_hold(w, "dac", relaxed=True, value=1113)
    w._on_heater_hold_failed({"action": "dac", "reason": "…", "tc2_why": "TC2 값 없음(D00011=-1) — DAC 상한 고정으로 대체"})
    assert w.chat_chk.notify_heater_alert.call_count == 0
    ok, txt, _ = _reached(w)
    assert ok is False and txt == "DAC 상한 고정 1113 — TC2 추종 불가 (TC2 값 없음(D00011=-1) — DAC 상한 고정으로 대체)"


def test_T84_reached_card_proceed_and_truncation(hold_card):
    w = hold_card
    long = "유지 모드 진입 대기 600s 초과 (상태: arming) — 완화 tc2 불가(TC2 표본 부족(0개 < 4)) / dac 불가(측정 창 평균도 현재 MV 도 없음)"
    w._on_heater_hold_failed({"action": "proceed", "reason": long})
    assert w.chat_chk.notify_heater_alert.call_count == 0
    ok, txt, _ = _reached(w)
    assert ok is False and txt.startswith("진입 실패 — 경고 후 진행 (") and txt.endswith("…)")
    assert len(txt) <= len("진입 실패 — 경고 후 진행 (") + 61 + 1


def test_T84_reached_card_mode_off_and_dac_setting(hold_card, monkeypatch):
    w = hold_card
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "off")
    ok, txt, _ = _reached(w)
    assert ok is True and txt == "사용 안 함 (HEATER_HOLD_MODE=off)"
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "dac"); _set_hold(w, "dac", value=1113)
    ok, txt, _ = _reached(w)
    assert ok is True and txt == "DAC 상한 고정 1113 (설정값)"


def test_T84_reached_card_no_margin_appends(hold_card):
    w = hold_card; _set_hold(w, "tc2", sv2=893.4); w.heater_hold.no_margin_warned = True
    ok, txt, _ = _reached(w)
    assert ok is False and txt == "TC2 추종 (SV2 893.4°C) · 출력 여유 없음(MV ≥ 상한 98%)"


def test_T84_abort_still_sends_alert_card_once(hold_card):
    w = hold_card
    w._on_heater_hold_failed({"action": "abort", "reason": "둘 다 실패"})
    w._on_heater_hold_failed({"action": "abort", "reason": "둘 다 실패"})
    assert w.chat_chk.notify_heater_alert.call_count == 1
    assert w.chat_chk.notify_heater_alert.call_args.args[0] == "히터 유지 모드 진입 실패"
    assert w._hold_fail_info is None


def test_T84_previous_process_failure_not_carried_over(hold_card):
    w = hold_card
    w._on_heater_hold_failed({"action": "proceed", "reason": "앞 공정 실패"})
    w._chat_reset_run_state()                                               # 다음 공정 시작
    _set_hold(w, "tc2", sv2=893.4)
    ok, txt, _ = _reached(w)
    assert ok is True and txt == "TC2 추종 (SV2 893.4°C)"


def test_T87_saturated_card_without_clamp(fresh):
    w = fresh
    w.chat_chk.notify_heater_alert.reset_mock()
    w._on_heater_saturated({"pv": 529.0, "pv2": None, "mv": 1031, "sec": 120.0, "clamp": None, "src": "",
                            "owner": "hold_dac", "limit": 1031})
    args, kw = w.chat_chk.notify_heater_alert.call_args
    assert args[0] == "히터 DAC 출력 포화" and kw["ok"] is False
    assert args[2]["클램프"] == "없음 (유지 모드가 D00018 소유, 현재 상한 1031)"
    assert args[2]["주의"] == "TC2 없음 · OT2 과온 보호 없음 — 즉시 확인 필요" and args[2]["TC2"] == "--.-"
    w._on_heater_saturated({"pv": 529.0, "pv2": 900.0, "mv": 1200, "sec": 120.0, "clamp": 1077, "src": "포화 직전 60초 평균",
                            "owner": "guard", "limit": 1200})
    args, _ = w.chat_chk.notify_heater_alert.call_args
    assert args[2]["클램프"] == "D00018 ← 1077 (포화 직전 60초 평균)" and "주의" not in args[2]


# ═══════════════ 종료 카드: 런 중 이상 이벤트 요약 ═══════════════
@pytest.fixture
def stop_card(fresh, monkeypatch):
    from controller.chat_notifier import ChatNotifier
    w = fresh
    posted = []
    real = ChatNotifier.__new__(ChatNotifier)
    real._post_card = lambda title, subtitle="", status="INFO", fields=None, urgent=False, route_params=None: \
        posted.append((title, status, dict(fields or {})))
    w.chat_chk = real
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "tc2")
    h = w.heater_hold; h._h = h._fresh(); h.engaged_relaxed = False; h.no_margin_warned = False
    w._heater_run_events = []; w._heater_run_engaged = False; w._heater_run_arrived = False; w._heater_run_gave_up = None
    w._posted = posted
    yield w
    w.chat_chk = MagicMock(); w.process_running = False
    h._h = h._fresh(); h.engaged_relaxed = False


def _run(w, arrive=True):
    """RUN 상승 엣지 → (arrive 면 도달 래치를 세운다) → 테스트가 이벤트를 넣고 → _stop 으로 하강 엣지."""
    w._posted.clear()
    feed(w, make_heater_st(run=True))
    if arrive:
        _arrive(w)
    return w


def _arrive(w):
    """목표 도달 폴링 1회: PV=SV=램프=600 → heater_hold 도달 래치 → _heater_run_arrived."""
    feed(w, make_heater_st(run=True, pv=600.0, sv=600.0, sv_ramp=600.0, cur_sv=600.0, mv=1000))
    assert w.heater_hold.arrived and w._heater_run_arrived


def _stop(w, **kw):
    feed(w, make_heater_st(run=False, **kw))
    card = [c for c in w._posted if c[0] == "히터 종료"][-1]
    return card[1], card[2]["특이사항"], card[2]


def _engaged(w, kind="tc2", relaxed=False):
    w.heater_hold.engaged_relaxed = relaxed
    if kind == "tc2":
        w.heater_hold._h.update(sv2=893.4)
    else:
        w.heater_hold._h.update(value=1113)
    w._on_heater_hold_engaged(kind)


def test_T88_normal_run_success(stop_card):
    w = _run(stop_card); _engaged(w, "tc2")
    status, note, f = _stop(w)
    assert status == "SUCCESS" and note == "없음" and f["사유"] == "정지"
    assert list(f) == ["마지막 TC1", "마지막 TC2", "운전 시간", "사유", "특이사항"]


def test_T88_dac_fallback_marks_fail_single_label(stop_card):
    w = _run(stop_card)
    w._on_heater_hold_failed({"action": "dac", "reason": "…", "tc2_why": "TC2 값 없음"}); _engaged(w, "dac", relaxed=True)
    status, note, _ = _stop(w)
    assert status == "FAIL" and note == "유지 모드 DAC 폴백"                   # 한 사건 = 라벨 1개
    assert w._hold_fail_info is not None                                     # 도달 카드용 정보는 그대로 남는다
    w = _run(stop_card)
    w._on_heater_hold_failed({"action": "tc2_relaxed", "reason": "…", "sv2": 893.4}); _engaged(w, "tc2", relaxed=True)
    assert _stop(w)[:2] == ("FAIL", "유지 모드 완화 진입")


def test_T88_proceed_and_abort_labels(stop_card):
    w = _run(stop_card)
    w._on_heater_hold_failed({"action": "proceed", "reason": "…"})
    assert _stop(w)[:2] == ("FAIL", "유지 모드 진입 실패 — 경고 후 진행")
    w = _run(stop_card)
    w._on_heater_hold_failed({"action": "abort", "reason": "…"})
    assert _stop(w)[:2] == ("FAIL", "유지 모드 진입 실패 — 공정 중단")


def test_T88_not_arrived_runs_are_not_flagged(stop_card):
    """도달한 적 없는 런: 짧은 수동 런 / 목표 전에 정지 → ✅ "없음" (유지 모드는 도달이 전제)."""
    w = _run(stop_card, arrive=False)                                        # 켰다 바로 끔
    assert _stop(w)[:2] == ("SUCCESS", "없음")
    w = _run(stop_card, arrive=False)
    for pv in (100.0, 200.0, 300.0):                                         # 승온 중 정지
        feed(w, make_heater_st(run=True, pv=pv, sv=600.0, sv_ramp=pv + 5, cur_sv=pv + 5))
    assert not w._heater_run_arrived
    assert _stop(w, pv=300.0)[:2] == ("SUCCESS", "없음")


def test_T88_arrived_but_not_engaged_is_flagged_with_reason(stop_card):
    w = _run(stop_card)
    assert _stop(w)[:2] == ("FAIL", "유지 모드 미진입")
    w = _run(stop_card)
    w.heater_hold._h["gave_up"] = "도달 시점 출력이 상한에 붙어 있어 고정하지 않음 (평균 MV 1181 ≥ 1200×0.98) — 히터가 목표를 유지할 여유가 없습니다"
    feed(w, make_heater_st(run=True, pv=600.0, sv=600.0, sv_ramp=600.0, cur_sv=600.0, mv=1181))   # 폴링이 사유를 보관
    status, note, _ = _stop(w)
    assert status == "FAIL" and note.startswith("유지 모드 미진입(도달 시점 출력이 상한에 붙어 있어 고정하지 않음") and note.endswith("…)")
    assert len(note) <= len("유지 모드 미진입(") + 61 + 1


def test_T88_mode_off_never_flags_not_engaged(stop_card, monkeypatch):
    monkeypatch.setattr(MAIN, "HEATER_HOLD_MODE", "off")
    w = _run(stop_card)
    assert _stop(w)[:2] == ("SUCCESS", "없음")


def test_T88_arrived_flag_resets_between_runs(stop_card):
    w = _run(stop_card)
    assert w._heater_run_arrived
    _stop(w)
    w = _run(stop_card, arrive=False)
    assert w._heater_run_arrived is False
    assert _stop(w)[:2] == ("SUCCESS", "없음")


def test_T88_demotion_saturation_no_margin(stop_card):
    w = _run(stop_card); _engaged(w, "tc2")
    w._on_heater_hold_alert("demoted", {"why": "TC2 값 없음(D00011=-1)", "value": 1059, "source": "강등 직전 MV"})
    assert _stop(w)[:2] == ("FAIL", "TC2 상실 → DAC 강등")
    w = _run(stop_card); _engaged(w, "tc2")
    w._on_heater_saturated({"pv": 529.0, "pv2": None, "mv": 1031, "sec": 120.0, "clamp": None, "owner": "hold_dac", "limit": 1031})
    assert _stop(w)[:2] == ("FAIL", "DAC 출력 포화")
    w = _run(stop_card); _engaged(w, "tc2")
    w._on_heater_hold_alert("no_margin", {"mv": 1181, "limit": 1200, "why": "…"})
    assert _stop(w)[:2] == ("FAIL", "출력 여유 없음(MV ≥ 상한 98%)")


def test_T88_plc_fault_still_fail_with_reason(stop_card):
    w = _run(stop_card); _engaged(w, "tc2")
    status, note, f = _stop(w, fault=True, ot=True)
    assert status == "FAIL" and f["사유"] == "이상 — 과온" and note == "없음"


def test_T88_more_than_four_events_and_dedup(stop_card):
    w = _run(stop_card)
    for _ in range(3):
        w._on_heater_hold_alert("no_margin", {})                    # 같은 이벤트 3번 → 라벨 1개
    w._on_heater_hold_failed({"action": "proceed", "reason": "…"})
    w._on_heater_hold_alert("demoted", {}); w._on_heater_hold_alert("demote_failed", {})
    w._on_heater_saturated({"clamp": None})
    w._on_heater_hold_alert("demoted", {"why": "x"}); w.heater_hold.alert.emit("no_margin", {})   # 중복
    status, note, _ = _stop(w)                                               # 진입 실패 라벨이 있으니 "미진입" 은 안 붙는다
    ev = w._heater_run_events
    assert len(ev) == 5 and ev.count("출력 여유 없음(MV ≥ 상한 98%)") == 1 and "유지 모드 미진입" not in ev
    assert note == " · ".join(ev[:4]) + " 외 1건" and status == "FAIL"


def test_T88_events_reset_on_next_run(stop_card):
    w = _run(stop_card); w._on_heater_saturated({"clamp": 1077})
    assert _stop(w)[0] == "FAIL"
    w = _run(stop_card); _engaged(w, "tc2")                           # 다음 런
    status, note, _ = _stop(w)
    assert status == "SUCCESS" and note == "없음"


def test_T88_manual_run_reflects_demotion_and_saturation(stop_card):
    """수동 런(공정 아님): heater_hold_failed 는 없지만 alert/saturated/engaged 는 그대로 온다."""
    w = _run(stop_card); assert w.process_running is False
    _engaged(w, "tc2", relaxed=True)                                   # 완화 진입
    w.heater_hold.alert.emit("demoted", {"why": "래더가 TC2 제어를 해제(M0004B OFF)", "value": 1059})
    w.heater_sat.saturated.emit({"pv": 500.0, "pv2": None, "mv": 1059, "sec": 120.0, "clamp": None, "owner": "hold_dac", "limit": 1059})
    spin(50)
    status, note, _ = _stop(w)
    assert status == "FAIL" and note == "유지 모드 완화 진입 · TC2 상실 → DAC 강등 · DAC 출력 포화"


def test_T88_0922_replay_not_engaged_plus_saturation(stop_card):
    """09-22: 유지 모드 미진입 + DAC 포화 25분, PLC 트립 없음 → 종료 카드 ❌ (당시에는 ✅ 였다)."""
    w = _run(stop_card)
    w._on_heater_saturated({"pv": 529.0, "pv2": 952.0, "mv": 1200, "sec": 120.0, "clamp": 1077, "src": "포화 직전 60초 평균", "owner": "guard", "limit": 1200})
    status, note, f = _stop(w, pv=559.6, pv2=952.0)
    assert status == "FAIL" and note == "DAC 출력 포화 · 유지 모드 미진입" and f["사유"] == "정지"   # 두 건만
    assert f["마지막 TC1"] == "559.6°C"


# ═══════════════ 종료 카드 "사유": 정상 완료 vs 중단 구분 ═══════════════
def _proc(w, **flags):
    """공정 소유 히터 런: 상승 엣지 → 도달 → 종료 플래그 세팅 → 하강 엣지."""
    w.process_running = True
    w._chat_reset_run_state()                                   # _chk_process_ok 는 공정 시작에서 True
    w._chk_process_ok = True
    w = _run(w)
    _engaged(w, "tc2")
    for k, v in flags.items():
        setattr(w, k, v)
    return w


def test_T91_normal_completion_reason(stop_card):
    w = _proc(stop_card)
    status, note, f = _stop(w)
    assert (status, f["사유"], note) == ("SUCCESS", "공정 종료", "없음")


def test_T91_user_stop_is_success_with_distinct_reason(stop_card):
    w = _proc(stop_card, _chat_user_stopped=True, _chk_process_ok=False)
    status, note, f = _stop(w)
    assert (status, f["사유"], note) == ("SUCCESS", "공정 중단 — 사용자 STOP", "없음")


def test_T91_emergency_and_fault_and_error_are_fail(stop_card):
    w = _proc(stop_card, _chat_emergency_stopped=True, _chk_process_ok=False)
    status, note, f = _stop(w)
    assert (status, f["사유"], note) == ("FAIL", "공정 중단 — 비상 정지", "공정 중단 — 비상 정지")
    w = _proc(stop_card, _fault_abort_active=True, _chk_process_ok=False)
    status, note, f = _stop(w)
    assert (status, f["사유"], note) == ("FAIL", "공정 중단 — 장비 이상", "공정 중단 — 장비 이상")
    w = _proc(stop_card, _chk_process_ok=False)
    status, note, f = _stop(w)
    assert (status, f["사유"], note) == ("FAIL", "공정 중단 — 오류", "공정 중단 — 오류")
    short = "MFC 압력 이상"
    w = _proc(stop_card, _chk_process_ok=False, _chat_fail_reason=short)
    status, note, f = _stop(w)
    assert (status, f["사유"]) == ("FAIL", f"공정 중단 — 오류 ({short})") and note == f["사유"]
    long = "RF Pulse 설정 불일치(듀티): 요청 20kHz·50% / 장비 20kHz·80% — 공정 중단(RFPULSE_VERIFY_PULSE_CONFIG)"
    w = _proc(stop_card, _chk_process_ok=False, _chat_fail_reason=long)
    status, note, f = _stop(w)
    assert status == "FAIL" and f["사유"].startswith("공정 중단 — 오류 (RF Pulse 설정 불일치") and f["사유"].endswith("…)")
    assert len(f["사유"]) == len("공정 중단 — 오류 (") + 61 + 1 and note == f["사유"]


def test_T91_plc_fault_wins_over_process_flags(stop_card):
    w = _proc(stop_card, _chat_user_stopped=True, _chk_process_ok=False)
    status, note, f = _stop(w, fault=True, ot=True)
    assert (status, f["사유"], note) == ("FAIL", "이상 — 과온", "없음")


def test_T91_manual_and_recipe_reasons_unchanged(stop_card, monkeypatch):
    w = _run(stop_card); _engaged(w, "tc2")                      # 수동 런(process_running False)
    assert _stop(w)[2]["사유"] == "정지"
    w = stop_card
    monkeypatch.setattr(w.heater_recipe, "is_running", lambda: True, raising=False)
    monkeypatch.setattr(w.heater_recipe, "current_step_no", lambda: 1, raising=False)
    monkeypatch.setattr(w.heater_recipe, "total_steps", lambda: 3, raising=False)
    w = _run(w); _engaged(w, "tc2")
    assert _stop(w)[2]["사유"] == "레시피 종료"


def test_T91_0923_replay_user_stop_during_ramp(stop_card):
    """2026-09-23 06:26 CeO2 #2-1: 승온 81초 만에 사용자 STOP → ✅ "공정 중단 — 사용자 STOP", 특이사항 없음."""
    w = stop_card
    w.process_running = True; w._chat_reset_run_state(); w._chk_process_ok = True
    w._posted.clear()
    feed(w, make_heater_st(run=True, pv=40.0, sv=600.0, sv_ramp=45.0, cur_sv=45.0))
    for pv in (50.0, 60.0, 69.8):                                 # 승온 중(도달 전)
        feed(w, make_heater_st(run=True, pv=pv, sv=600.0, sv_ramp=pv + 5, cur_sv=pv + 5, pv2=89.6))
    w._chat_user_stopped = True; w._chk_process_ok = False        # _on_sputter_stop_clicked
    status, note, f = _stop(w, pv=69.8, pv2=89.6)
    assert (status, f["사유"], note) == ("SUCCESS", "공정 중단 — 사용자 STOP", "없음")
    assert f["마지막 TC1"] == "69.8°C" and f["마지막 TC2"] == "89.6°C"
