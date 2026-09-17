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
