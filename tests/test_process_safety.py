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
