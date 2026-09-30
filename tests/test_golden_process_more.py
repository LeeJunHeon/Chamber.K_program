# -*- coding: utf-8 -*-
"""골든(동작 고정) — 기존 01~15b 가 다루지 않는 공정 종료 경로.

하니스·기록 방식은 test_golden_process 와 같다(check 스냅샷 포함). 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  16a CSV 2스텝 중 1스텝에서 critical_error ("CSV 공정 실패로 중단됨" 경로)
  16b CSV 2스텝 중 1스텝에서 장치 연결 실패
  17a CSV 스텝 진행 중 ALL STOP
  17b CSV 딜레이 스텝 중 ALL STOP
  18a 수동 공정 중 PLC 재기동 감지(_on_plc_restarted)
  18b CSV 딜레이 중 PLC 재기동 감지
  18c 대기 중 PLC 재기동 감지
"""
import re

import pytest

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, csv_row, delay_row, _load_local   # noqa: F401

_RESTART_WHY = "마커 불일치(D00100 0 != 0x5A5A)"


def s16a_csv_step1_critical_error(h):
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w.process_controller.critical_error.emit("O2 유량 이탈: 설정 10.0 sccm / 실측 3.1 sccm (허용 ±10%)")
    h.flush()
    h.check("critical_error 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s16b_csv_step1_connection_failed(h):
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.w.process_controller.connection_failed.emit("MFC 장치에 연결할 수 없습니다.")
    h.flush()
    h.check("연결 실패 후", snapshot=True)


def s17a_csv_step_all_stop(h):
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w.ui.ALL_STOP_button.click()
    h.flush()
    h.check("ALL STOP 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s17b_csv_delay_all_stop(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.advance(2)
    h.w.ui.ALL_STOP_button.click()
    h.flush()
    h.check("딜레이 중 ALL STOP 뒤", snapshot=True)
    h.w.process_controller.finished.emit()          # 늦게 온 finished — 무시되는지
    h.flush()
    h.check("늦은 finished 뒤")


def s18a_manual_plc_restarted(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w._on_plc_restarted(_RESTART_WHY)
    h.flush()
    h.check("PLC 재기동 감지 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s18b_csv_delay_plc_restarted(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.advance(2)
    h.w._on_plc_restarted(_RESTART_WHY)
    h.flush()
    h.check("딜레이 중 PLC 재기동 감지 뒤", snapshot=True)


def s18c_idle_plc_restarted(h):
    h.check("대기", snapshot=True)
    h.w._on_plc_restarted(_RESTART_WHY)
    h.flush()
    h.check("PLC 재기동 감지 뒤", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_more(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
