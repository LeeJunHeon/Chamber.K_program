# -*- coding: utf-8 -*-
"""골든(동작 고정) — 기존 골든이 다루지 않는 공정 시작·레시피 적재 경로.

하니스·기록 방식은 test_golden_process 와 같다(check 스냅샷 포함). 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  19a 히터 레시피 실행 중 + 적재 레시피에 히터값 있음 → "시작 불가"
  19b 히터 레시피 실행 중 + 수동 시작(허용, "[히터] 히터 레시피가 제어 중입니다" 로그)
  19c 공정 중에 다시 시작 요청(원격 PROCESS_START) → "이미 공정이 진행 중입니다"
  19d 히터 가스·압력이 잡혀 있을 때 Start → 막힘
  19e 첫 행이 딜레이인 레시피 적재(미리보기 "(대기 스텝)") 뒤 Start
  19f 없는 파일 적재 → "파일 오류"
  19g 읽는 중 예외(load_table 예외) → "CSV 읽기 오류"
  19h 유효 행이 없는 레시피 → "CSV 비어있음"
  19i 공정 중 레시피 적재 → "변경 불가"
"""
import re

import pytest

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, csv_row, delay_row, _load_local, _REMOTE_ARGS   # noqa: F401

import main as MAIN


def _heater_recipe_running(h):
    h.mp.setattr(h.w.heater_recipe, "is_running", lambda: True, raising=False)


def s19a_heater_recipe_running_recipe_has_heater(h):
    _heater_recipe_running(h)
    _load_local(h, [csv_row("S1"), csv_row("S2", use_heater="1", heater_temp="300")])
    h.w.ui.Sputter_Start_Button.click()
    h.flush()
    h.check("Start 뒤", snapshot=True)


def s19b_heater_recipe_running_manual_start(h):
    _heater_recipe_running(h)
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.flush()
    h.check("Start 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s19c_remote_start_while_running(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.remote("PROCESS_START", dict(_REMOTE_ARGS, dcPower=150))
    h.check("원격 재시작 요청 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s19d_start_blocked_by_heater_gas(h):
    h.mp.setattr(h.w.heater_atmosphere, "is_active", lambda: True)
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.flush()
    h.check("Start 뒤", snapshot=True)


def s19e_recipe_first_row_delay(h):
    _load_local(h, [delay_row("delay 3s"), csv_row("S1")])
    h.w.ui.Sputter_Start_Button.click()
    h.check("Start 뒤(딜레이)", snapshot=True)
    h.advance(3)
    h.check("딜레이 끝 → STEP 2", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s19f_load_missing_file(h):
    h.w._start_csv_process_from_path(str(h.tmp / "없는_레시피.csv"))
    h.flush()
    h.check("적재 뒤", snapshot=True)


def s19g_load_table_raises(h):
    path = h.write_csv([csv_row("S1")])

    def _broken(*a, **k):
        raise RuntimeError("시트를 읽을 수 없습니다(가짜)")
    h.mp.setattr(MAIN, "load_table", _broken)
    h.w._start_csv_process_from_path(path)
    h.flush()
    h.check("적재 뒤", snapshot=True)
    h.w.ui.Sputter_Start_Button.click()          # 실패한 파일 경로가 남아 있으면 Start 가 다시 읽는다
    h.flush()
    h.check("Start 뒤", snapshot=True)


def s19h_recipe_without_valid_rows(h):
    blank = {c: "" for c in csv_row("S1")}
    path = h.write_csv([blank, dict(blank, **{"G2 Target": "  "})])
    h.w._start_csv_process_from_path(path)
    h.flush()
    h.check("적재 뒤", snapshot=True)
    h.w.ui.Sputter_Start_Button.click()
    h.flush()
    h.check("Start 뒤", snapshot=True)


def s19i_load_while_running(h):
    path = h.write_csv([csv_row("S1")])
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w._start_csv_process_from_path(path)
    h.flush()
    h.check("로컬 적재 시도 뒤", snapshot=True)
    h.remote("RECIPE_PROCESS_RUN", {"rows": [csv_row("W1")]})
    h.check("원격 적재 시도 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_start(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
