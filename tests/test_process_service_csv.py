# -*- coding: utf-8 -*-
"""core.process_service — Qt 없이, CSV 다음 스텝·딜레이 틱·리스트 즉시 정리를 부른 순서로 본다."""
import ast
import os

import pytest

from core.process_service import ProcessService
from core.state import ProcessState
from test_process_service_start import FakePorts, ROW

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARAMS = {"process_name": "S1", "use_heater": False, "heater_temp": 0.0, "dc_power": 100.0}

CANCEL_TAIL = ["delay_timer_stop", "delay_clock_clear", "set_buttons", "stage",
               "reset_process_ui_fields", "close_process_log"]


def _svc(st, **ret):
    ret.setdefault("build_csv_params", dict(PARAMS))
    ports = FakePorts(**ret)
    return ProcessService(st, ports), ports


def _csv(rows, idx=-1, **kw):
    return ProcessState(csv_mode=True, csv_rows=list(rows), csv_index=idx, csv_file_path="C:/r/a.csv", **kw)


STEP_CALLS = ["build_csv_params", "reset_stats", "apply_params_to_ui", "open_process_log",
              "heater_recipe_running", "log_heater_header", "log", "chat_reset_run_state",
              "chat_notify_started", "log", "stage", "set_buttons", "clear_plc_fault",
              "erp_run_start", "request_start"]


# ───────────────────────── 다음 스텝 ─────────────────────────
def test_next_step_normal_row():
    st = _csv([dict(ROW)])
    svc, p = _svc(st)
    svc.start_next_csv_step()
    assert p.names() == STEP_CALLS
    c = p.calls
    assert c[6] == ["log", "정보", "=== CHK CSV STEP 1/1 시작 ==="]
    assert c[9] == ["log", "Process", "CSV 공정 리스트 1/1 실행: CSV 1/1 - S1"]
    assert c[10] == ["stage", "CSV 1/1 - S1"] and c[11] == ["set_buttons", False, True, False]
    assert c[13] == ["erp_run_start", "CSV 1/1 - S1", PARAMS]
    assert (st.csv_index, st.current_name, st.running, st.step_ok) == (0, "CSV 1/1 - S1", True, True)
    assert st.last_params == PARAMS and st.last_params is not c[-1][1]


def test_next_step_blank_name_uses_step_label_and_heater_recipe_log():
    st = _csv([dict(ROW)])
    svc, p = _svc(st, build_csv_params={"process_name": "", "use_heater": False}, heater_recipe_running=True)
    svc.start_next_csv_step()
    assert p.calls[5] == ["log", "정보", "[히터] 히터 레시피가 제어 중입니다. 이번 공정은 히터를 제어하지 않습니다."]
    assert st.current_name == "CSV 1/1 - STEP1"                       # E7: 이름 없는 스텝은 "STEP{n}"


def test_next_step_delay_row():
    st = _csv([{"Process_name": " delay 5s "}, dict(ROW)])
    svc, p = _svc(st, chat_enabled=True)
    svc.start_next_csv_step()
    assert p.names() == ["delay_timer_stop", "delay_clock_start", "set_buttons", "log", "stage",
                         "chat_enabled", "chat_text", "delay_timer_start"]
    assert p.calls[2] == ["set_buttons", False, True, None]            # 레시피 선택 버튼은 건드리지 않는다
    assert p.calls[3] == ["log", "Process", "CSV DELAY STEP 1/2 시작: delay 5s (총 00:05)"]
    assert p.calls[4] == ["stage", "CSV 1/2 - delay 5s (남은 00:05)"]
    assert p.calls[6] == ["chat_text", "⏳ CHK 딜레이 시작: CSV 1/2 - delay 5s | 총 00:05"]
    assert (st.delay_active, st.delay_total_sec, st.delay_remaining_sec, st.delay_name, st.running) == \
        (True, 5, 5, "delay 5s", True)
    assert st.current_name == "CSV 1/2 - delay 5s"


def test_next_step_zero_delay_skipped_then_next_row():
    st = _csv([{"Process_name": "delay 0s"}, dict(ROW)])
    svc, p = _svc(st)
    svc.start_next_csv_step()
    assert p.calls[0] == ["log", "정보", "CSV DELAY 스킵: delay 0s (0초)"]
    assert p.names()[1:] == STEP_CALLS and st.csv_index == 1


def test_next_step_blank_row_skipped():
    st = _csv([{"#": "1", "Process_name": " "}, dict(ROW)])
    svc, p = _svc(st)
    svc.start_next_csv_step()
    assert p.calls[0] == ["log", "정보", "CSV 빈 행 스킵: index=1"] and st.csv_index == 1


def test_next_step_param_error_chat_then_cancel_then_notice():
    st = _csv([dict(ROW), dict(ROW)])
    svc, p = _svc(st, build_csv_params=ValueError("wp 오류"), chat_enabled=True)
    svc.start_next_csv_step()
    assert p.names() == ["build_csv_params", "log", "chat_reset_run_state", "chat_notify_failed_now",
                         "chat_notify_finished"] + CANCEL_TAIL + ["notice"]
    assert p.calls[1] == ["log", "ERROR", "CSV 레시피 오류로 리스트 공정을 중단합니다: wp 오류"]
    assert p.calls[3] == ["chat_notify_failed_now", "CSV 레시피 오류: wp 오류", False]
    assert p.calls[4] == ["chat_notify_finished", False]
    assert p.calls[-4] == ["stage", "CSV 레시피 오류로 중단"]
    assert p.calls[-1] == ["notice", "process", "critical", "CSV 레시피 오류",
                           "CSV 1번째 행 파라미터가 잘못되었습니다:\nwp 오류"]
    assert st.step_ok is False and st.csv_mode is False and st.csv_rows == [] and st.running is False


def test_next_step_after_last_row_finishes_list():
    st = _csv([dict(ROW)], idx=0, running=True, current_name="CSV 1/1 - S1", last_params={"x": 1})
    svc, p = _svc(st)
    svc.start_next_csv_step()
    assert p.names() == ["set_buttons", "stage", "reset_process_ui_fields", "close_process_log", "notice"]
    assert p.calls[0] == ["set_buttons", True, False, True] and p.calls[1] == ["stage", "CSV 공정 완료"]
    assert p.calls[-1] == ["notice", "process", "information", "CSV 공정 완료", "CSV에 있는 모든 공정을 완료했습니다."]
    assert (st.csv_mode, st.csv_rows, st.csv_index, st.csv_file_path, st.current_name, st.last_params, st.running) == \
        (False, [], -1, None, "", None, False)


@pytest.mark.parametrize("kw", [dict(csv_mode=False), dict(csv_rows=[]), dict(csv_cancelled=True)])
def test_next_step_ignored(kw):
    base = dict(csv_mode=True, csv_rows=[dict(ROW)], csv_index=-1)
    base.update(kw)
    st = ProcessState(**base)
    svc, p = _svc(st)
    svc.start_next_csv_step()
    assert p.names() == ["log"] and "무시합니다" in p.calls[0][2] and st.csv_index == -1


# ───────────────────────── 딜레이 틱 ─────────────────────────
def _in_delay(**kw):
    st = _csv([{"Process_name": "delay 10s"}, dict(ROW)], idx=0, running=True, delay_active=True,
              delay_total_sec=10, delay_remaining_sec=10, delay_name="delay 10s",
              current_name="CSV 1/2 - delay 10s")
    for k, v in kw.items():
        setattr(st, k, v)
    return st


def test_tick_with_clock():
    st = _in_delay()
    svc, p = _svc(st, delay_elapsed_ms=3500)
    svc.on_delay_tick()
    assert p.calls == [["delay_elapsed_ms"], ["stage", "CSV 1/2 - delay 10s (남은 00:07)"]]
    assert st.delay_remaining_sec == 7


def test_tick_without_clock_counts_down_by_one():
    st = _in_delay(delay_remaining_sec=5)
    svc, p = _svc(st, delay_elapsed_ms=None)
    svc.on_delay_tick()
    assert st.delay_remaining_sec == 4 and p.calls[-1] == ["stage", "CSV 1/2 - delay 10s (남은 00:04)"]


def test_tick_done_then_next_step():
    st = _in_delay()
    svc, p = _svc(st, delay_elapsed_ms=10_000, chat_enabled=True)
    svc.on_delay_tick()
    assert p.names()[:5] == ["delay_elapsed_ms", "delay_timer_stop", "log", "chat_enabled", "chat_text"]
    assert p.calls[2] == ["log", "정보", "CSV DELAY 완료: delay 10s"]
    assert p.calls[4] == ["chat_text", "✅ CHK 딜레이 완료: CSV 1/2 - delay 10s"]
    assert p.names()[5:] == STEP_CALLS                      # 이어서 다음 스텝(2번째 행)
    assert st.delay_active is False and st.csv_index == 1 and st.running is True


def test_tick_chat_disabled_skips_text():
    st = _in_delay()
    svc, p = _svc(st, delay_elapsed_ms=10_000, chat_enabled=False)
    svc.on_delay_tick()
    assert "chat_text" not in p.names() and "chat_enabled" in p.names()


@pytest.mark.parametrize("kw", [dict(csv_mode=False), dict(csv_cancelled=True), dict(delay_active=False)])
def test_tick_after_cancel_only_stops_timer(kw):
    st = _in_delay(**kw)
    svc, p = _svc(st, delay_elapsed_ms=10_000)
    svc.on_delay_tick()
    assert p.names() == ["delay_timer_stop"]


# ───────────────────────── 리스트 즉시 정리 ─────────────────────────
def _running_list(**kw):
    return _in_delay(finish_handled=False, **kw)


def test_cancel_with_reason():
    st = _running_list()
    svc, p = _svc(st, chat_enabled=True)
    svc.cancel_csv_list_now("CSV 공정 취소", reason="ALL STOP(비상 정지)으로 중단")
    assert p.names() == ["chat_enabled", "chat_notify_failed_now", "chat_notify_finished"] + CANCEL_TAIL
    assert p.calls[1] == ["chat_notify_failed_now", "ALL STOP(비상 정지)으로 중단", False]
    assert p.calls[-4] == ["set_buttons", True, False, True] and p.calls[-3] == ["stage", "CSV 공정 취소"]
    assert p.names()[-1] == "close_process_log"             # 로그 파일 닫기가 맨 끝
    assert st.finish_handled is True


@pytest.mark.parametrize("user_stopped,adds_error", [(False, True), (True, False)])
def test_cancel_without_reason_depends_on_user_stop(user_stopped, adds_error):
    st = _running_list()
    svc, p = _svc(st, chat_enabled=True, chat_user_stopped=user_stopped)
    svc.cancel_csv_list_now()
    exp = ["chat_enabled", "chat_user_stopped"] + (["chat_add_error"] if adds_error else []) + ["chat_notify_finished"]
    assert p.names() == exp + CANCEL_TAIL
    if adds_error:
        assert p.calls[2] == ["chat_add_error", "CSV 공정 취소됨"]


def test_cancel_names_card_with_stage_text_when_name_empty():
    st = _running_list(current_name="  ")
    seen = []
    svc, p = _svc(st, chat_enabled=True, chat_notify_finished=lambda: seen.append(st.current_name))
    svc.cancel_csv_list_now("CSV 공정 취소됨")
    assert seen == ["CSV 공정 취소됨"] and st.current_name == ""      # 카드에는 보정 이름, 정리 뒤에는 비움


@pytest.mark.parametrize("notify,enabled,first", [(False, True, []), (True, False, ["chat_enabled"])])
def test_cancel_no_chat(notify, enabled, first):
    st = _running_list()
    svc, p = _svc(st, chat_enabled=enabled)
    svc.cancel_csv_list_now("X", notify_chat=notify)
    assert p.names() == first + CANCEL_TAIL
    assert st.finish_handled is False


def test_cancel_chat_error_swallowed_cleanup_continues():
    st = _running_list()
    svc, p = _svc(st, chat_enabled=True, chat_notify_finished=RuntimeError("챗 죽음"))
    svc.cancel_csv_list_now("X", reason="R")
    assert p.names()[-len(CANCEL_TAIL):] == CANCEL_TAIL
    assert st.finish_handled is False                       # 카드가 실패하면 표시도 세우지 않는다(try 범위 그대로)


def test_cancel_resets_list_and_delay_state():
    st = _running_list(csv_cancelled=True)
    svc, p = _svc(st)
    svc.cancel_csv_list_now()
    assert (st.delay_active, st.delay_total_sec, st.delay_remaining_sec, st.delay_name) == (False, 0, 0, "")
    assert (st.csv_cancelled, st.csv_mode, st.csv_rows, st.csv_index, st.csv_file_path, st.running) == \
        (False, False, [], -1, None, False)


# ───────────────────────── 정적 검사 ─────────────────────────
def test_import_rules_cover_new_code():
    """core import 규칙 검사(test_process_service_start.test_core_import_rules)가 보는 파일에 이번 코드가 들어 있다."""
    src = open(os.path.join(ROOT, "core", "process_service.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    names = {f.name for f in ast.walk(tree) if isinstance(f, ast.FunctionDef)}
    assert {"start_next_csv_step", "start_delay_step", "on_delay_tick", "cancel_csv_list_now"} <= names
    rec = open(os.path.join(ROOT, "core", "recipe.py"), encoding="utf-8").read()
    assert "def fmt_hms" in rec
    for f in ("process_service.py", "recipe.py"):
        tree = ast.parse(open(os.path.join(ROOT, "core", f), encoding="utf-8").read())
        mods = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names] + \
               [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        assert not [m for m in mods if m.split(".")[0] in ("PyQt6", "UI", "main", "controller", "device", "reporter")
                    or m in ("lib.logger", "lib.heater_logger", "lib.recipe_io")], f
