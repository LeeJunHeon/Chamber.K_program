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


def test_T57_heater_stop_card_icon_ok_vs_fault(fresh):
    """히터 종료 카드: 이상이면 ❌(FAIL), 아니면 ✅(SUCCESS). 시작은 ℹ️(INFO)."""
    from controller.chat_notifier import ChatNotifier
    w = fresh
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
            assert list(posted[1][2]) == ["마지막 TC1", "마지막 TC2", "운전 시간", "사유"]
        posted.clear()
        feed(w, make_heater_st(run=True)); feed(w, make_heater_st(run=False))
        assert posted[1][1] == "SUCCESS" and posted[1][2]["사유"] == "정지"
        posted.clear(); w.process_running = True
        feed(w, make_heater_st(run=True)); feed(w, make_heater_st(run=False))
        assert posted[1][1] == "SUCCESS" and posted[1][2]["사유"] == "공정 종료"
    finally:
        w.process_running = False
        w.chat_chk = MagicMock()


def test_T58_heater_cards_tc1_tc2_fields(fresh):
    from controller.chat_notifier import ChatNotifier
    w = fresh
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
        assert list(posted[1][2]) == ["마지막 TC1", "마지막 TC2", "운전 시간", "사유"]
        assert posted[1][2]["마지막 TC1"] == "590.0°C" and posted[1][2]["마지막 TC2"] == "--.-"
        posted.clear(); w.process_running = True
        w.process_controller.heater_reached.emit({"pv": 599.6, "pv2": 1051.0, "target": 600.0, "took_sec": 65, "next": "압력 안정화"})
        spin(50)
        assert posted == [("히터 도달", "INFO", {"목표": "600.0°C", "도달 TC1": "599.6°C", "도달 TC2": "1051.0°C",
                                                 "승온 소요": "1분 5초", "다음 단계": "압력 안정화"})]
        posted.clear()
        w.process_controller.heater_reached.emit({"pv": 599.6, "pv2": None, "target": 600.0, "took_sec": 5, "next": ""})
        spin(50)
        assert posted[0][2]["도달 TC2"] == "--.-" and posted[0][2]["다음 단계"] == "-"
        assert not any("온도" in k for c in posted for k in c[2])       # "도달 온도"/"마지막 온도"/"현재" 키 없음
    finally:
        w.process_running = False
        w.chat_chk = MagicMock()
    import inspect, controller.process_controller as PC
    assert "\"pv2\": _pv2" in inspect.getsource(PC.SputterProcessController._heater_wait)

