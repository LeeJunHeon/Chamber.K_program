# -*- coding: utf-8 -*-
"""골든(동작 고정) — STOP·ALL STOP·종료 처리·설비 이상 중단·재기동의 기존 골든 밖 경로.

하니스·기록 방식은 test_golden_process 와 같다(check 스냅샷 포함). 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  23a 수동 공정 중 설비 이상 중단(여러 줄 상세) → 공정 정지 → 종료
  23b 같은 공정에서 설비 이상 두 번(두 번째는 "추가 사유" 로그, 상세 카드 1장)
  23c CSV 스텝 진행 중 설비 이상 → 리스트 취소 플래그 → 스텝 종료 뒤 정리
  23d CSV 딜레이 중 설비 이상 → "CSV 공정 중단됨(설비 이상)"
  23e 대기 중 설비 이상(아무 동작 없음)
  23f 대기 중 ALL STOP(장치 정지만)
  23g 히터 레시피 실행 중 수동 공정 ALL STOP(히터 레시피 정지 포함)
  23h 정상 종료인데 ChK CSV 저장 실패(append_chk_csv_row 가 False)
  23i 정상 종료인데 ChK CSV 처리 중 예외
  23j 대기 중 STOP
  23k CSV 스텝 진행 중 PLC 재기동 감지 → 스텝 정지 → 종료 처리에서 리스트 취소
"""
import re

import pytest

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, csv_row, delay_row, _load_local   # noqa: F401

_FAULT = "히터 TC1 급락: 600.0 → 420.0°C (30초)"
_DETAIL = "TC1 420.0°C / 목표 600.0°C\nTC2 880.0°C\nMV 1200 (100%)"


def _manual(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()


def s23a_manual_fault_abort(h):
    _manual(h)
    h.w._abort_process_by_fault(_FAULT, detail=_DETAIL)
    h.flush()
    h.check("설비 이상 중단 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s23b_manual_fault_twice(h):
    _manual(h)
    h.w._abort_process_by_fault(_FAULT, detail=_DETAIL)
    h.w._abort_process_by_fault("메인밸브 닫힘 (M00003 OFF)", detail="MV=OFF ITL=ON")
    h.flush()
    h.check("설비 이상 두 번 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s23c_csv_step_fault(h):
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w._abort_process_by_fault(_FAULT, detail=_DETAIL)
    h.flush()
    h.check("설비 이상 직후", snapshot=True)
    h.ctrl_finish()
    h.check("스텝 종료 뒤", snapshot=True)


def s23d_csv_delay_fault(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.advance(2)
    h.w._abort_process_by_fault(_FAULT, detail=_DETAIL)
    h.flush()
    h.check("딜레이 중 설비 이상 뒤", snapshot=True)


def s23e_idle_fault(h):
    h.check("대기", snapshot=True)
    h.w._abort_process_by_fault(_FAULT, detail=_DETAIL)
    h.flush()
    h.check("설비 이상 뒤", snapshot=True)


def s23f_idle_all_stop(h):
    h.check("대기", snapshot=True)
    h.w.ui.ALL_STOP_button.click()
    h.flush()
    h.check("ALL STOP 뒤", snapshot=True)


def s23g_manual_all_stop_with_heater_recipe(h):
    h.mp.setattr(h.w.heater_recipe, "is_running", lambda: True, raising=False)
    h.mp.setattr(h.w.heater_recipe, "stop", h.sink.heater.recipe_stop)   # 상태를 남기지 않고 호출만 기록
    _manual(h)
    h.w.ui.ALL_STOP_button.click()
    h.flush()
    h.check("ALL STOP 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s23h_chk_csv_save_failed(h):
    h.sink.csv_row.return_value = False
    _manual(h)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s23i_chk_csv_exception(h):
    h.sink.csv_row.side_effect = OSError("NAS 경로에 쓸 수 없습니다(가짜)")
    _manual(h)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s23j_idle_stop(h):
    h.check("대기", snapshot=True)
    h.w._on_sputter_stop_clicked()
    h.flush()
    h.check("STOP 뒤", snapshot=True)


def s23k_csv_step_plc_restarted(h):
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w._on_plc_restarted("마커 불일치(D00100 0 != 0x5A5A)")
    h.flush()
    h.check("PLC 재기동 감지 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("스텝 종료 뒤", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_stop(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
