# -*- coding: utf-8 -*-
"""골든(동작 고정) — CSV 리스트 진행·딜레이의 기존 골든 밖 경로.

하니스·기록 방식은 test_golden_process 와 같다(check 스냅샷 포함). 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  22a "delay 0s" 행이 들어 있는 레시피 끝까지 (0초 딜레이 스킵)
  22b 히터 레시피 실행 중 + 히터값 없는 CSV 레시피 시작 (스텝마다 "[히터] 히터 레시피가 제어 중입니다" 로그)
  22c 긴 딜레이(delay 2h) 몇 틱 진행 뒤 STOP (남은 시간이 h:mm:ss 로 표시)
  22d 마지막 행이 딜레이인 레시피 끝까지 (딜레이 완료 → CSV 공정 완료)
  22e 공정 이름이 빈 행이 들어 있는 레시피 (표시 이름 "STEP i/n")
"""
import re

import pytest

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, csv_row, delay_row, _load_local   # noqa: F401


def _run_to_end(h, steps):
    for _ in range(steps):
        h.ctrl_run()
        h.ctrl_finish()


def s22a_zero_delay_skipped(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 0s"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("STEP 1 종료 → 0초 딜레이 스킵 → STEP 3", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s22b_heater_recipe_running_csv_without_heater(h):
    h.mp.setattr(h.w.heater_recipe, "is_running", lambda: True, raising=False)
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.check("STEP 1 시작", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("STEP 2 시작", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s22c_long_delay_ticks_then_stop(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 2h"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("딜레이 시작", snapshot=True)
    h.advance(3)
    h.check("딜레이 3초 뒤", snapshot=True)
    h.clock_ms += 3600 * 1000                    # 한 시간 건너뛴 뒤 한 틱
    h.advance(1)
    h.check("딜레이 1시간+4초 뒤", snapshot=True)
    h.w.ui.Sputter_Stop_Button.click()
    h.flush()
    h.check("STOP 뒤", snapshot=True)


def s22d_last_row_is_delay(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 3s")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("딜레이 시작", snapshot=True)
    h.advance(3)
    h.check("딜레이 끝 → 리스트 완료", snapshot=True)


def s22e_blank_process_names(h):
    _load_local(h, [csv_row(""), csv_row("S2"), csv_row("")])
    h.w.ui.Sputter_Start_Button.click()
    h.check("STEP 1 시작", snapshot=True)
    _run_to_end(h, 3)
    h.check("리스트 완료", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_csv_flow(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
