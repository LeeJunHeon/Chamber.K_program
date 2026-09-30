# -*- coding: utf-8 -*-
"""core.process_service — Qt 없이, STOP·ALL STOP·설비 이상·종료 처리·오류·재기동을 부른 순서로 본다."""
import ast
import os

import pytest

from core.process_service import ProcessService
from core.state import ProcessState
from test_process_service_start import FakePorts, ROW

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CANCEL_TAIL = ["delay_timer_stop", "delay_clock_clear", "set_buttons", "stage",
               "reset_process_ui_fields", "close_process_log"]


def _svc(st, **ret):
    base = dict(process_controller_present=True, chat_fault_detail_sent=False, chat_enabled=True,
                chat_user_stopped=False, build_chk_csv_row={"Process Name": "X"}, append_chk_csv_row=True)
    base.update(ret)
    ports = FakePorts(**base)
    return ProcessService(st, ports), ports


def _manual(**kw):
    return ProcessState(running=True, step_ok=True, current_name="Single CHK", finish_handled=False, **kw)


def _csv_step(**kw):
    return ProcessState(running=True, csv_mode=True, csv_rows=[dict(ROW), dict(ROW)], csv_index=0,
                        csv_file_path="C:/r/a.csv", step_ok=True, current_name="CSV 1/2 - S1",
                        finish_handled=False, **kw)


def _csv_delay(**kw):
    return _csv_step(delay_active=True, delay_total_sec=10, delay_remaining_sec=8,
                     delay_name="delay 10s", **kw)


def _csv_between(**kw):
    st = _csv_step(**kw)
    st.running = False
    return st


# ───────────────────────── STOP ─────────────────────────
def test_stop_manual():
    st = _manual()
    svc, p = _svc(st)
    svc.stop()
    assert p.names() == ["mark_user_stopped", "chat_add_error", "status_message",
                         "process_controller_present", "request_stop"]
    assert p.calls[1] == ["chat_add_error", "사용자 STOP"] and p.calls[2] == ["status_message", "경고", "STOP 버튼 클릭됨"]
    assert st.step_ok is False and st.running is True        # 정리는 finished 가 한다


def test_stop_csv_step_sets_cancel_flag_then_stops_controller():
    st = _csv_step()
    svc, p = _svc(st)
    svc.stop()
    assert p.names() == ["mark_user_stopped", "chat_add_error", "status_message", "log",
                         "process_controller_present", "request_stop"]
    assert p.calls[3] == ["log", "정보", "사용자 STOP → CSV 리스트 전체 취소 플래그 설정"]
    assert st.csv_cancelled is True


def test_stop_csv_delay_cancels_list_now():
    st = _csv_delay()
    svc, p = _svc(st, chat_user_stopped=True)
    svc.stop()
    assert p.names() == ["mark_user_stopped", "chat_add_error", "status_message", "log", "log",
                         "chat_enabled", "chat_user_stopped", "chat_notify_finished"] + CANCEL_TAIL
    assert p.calls[4] == ["log", "정보", "CSV Delay 중 STOP → 딜레이 즉시 중단 및 리스트 공정 취소"]
    assert "request_stop" not in p.names() and st.csv_mode is False


def test_stop_csv_between_steps_cancels_list_now():
    st = _csv_between()
    svc, p = _svc(st, chat_user_stopped=True)
    svc.stop()
    assert p.calls[4] == ["log", "정보", "CSV STEP 사이 STOP → 리스트 공정 즉시 취소"]
    assert p.names()[-len(CANCEL_TAIL):] == CANCEL_TAIL and "request_stop" not in p.names()


def test_stop_idle_only_marks_and_logs():
    st = ProcessState()
    svc, p = _svc(st)
    svc.stop()
    assert p.names() == ["mark_user_stopped", "chat_add_error", "status_message", "process_controller_present"]


# ───────────────────────── ALL STOP ─────────────────────────
def test_all_stop_idle_only_devices():
    st = ProcessState()
    svc, p = _svc(st)
    svc.all_stop()
    assert p.names() == ["heater_recipe_running", "plc_emergency_stop", "dc_emergency_off", "rfpulse_stop"]
    assert st.step_ok is False


def test_all_stop_idle_with_heater_recipe():
    svc, p = _svc(ProcessState(), heater_recipe_running=True)
    svc.all_stop()
    assert p.names()[:2] == ["heater_recipe_running", "heater_recipe_stop"] and p.calls[1][1] == "비상 정지"


def test_all_stop_manual_order_flags_before_plc():
    st = _manual()
    svc, p = _svc(st)
    seen = {}
    p.ret["plc_emergency_stop"] = lambda: seen.update(step_ok=st.step_ok,
                                                      marked="mark_emergency_stopped" in p.names())
    svc.all_stop()
    assert p.names() == ["heater_recipe_running", "mark_emergency_stopped", "plc_emergency_stop",
                         "dc_emergency_off", "chat_notify_failed_now", "request_stop", "rfpulse_stop"]
    assert seen == {"step_ok": False, "marked": True}        # 실패 표시가 PLC 비상정지보다 먼저
    assert p.calls[4] == ["chat_notify_failed_now", "ALL STOP(비상 정지)으로 중단", False]


def test_all_stop_csv_step_flags_list_and_stops_controller():
    st = _csv_step()
    svc, p = _svc(st)
    svc.all_stop()
    assert p.names() == ["heater_recipe_running", "mark_emergency_stopped", "plc_emergency_stop",
                         "dc_emergency_off", "chat_notify_failed_now", "request_stop", "rfpulse_stop"]
    assert st.csv_cancelled is True and st.csv_mode is True   # 정리는 finished 의 취소 분기가 한다


def test_all_stop_csv_delay_cancels_list_without_controller_stop():
    st = _csv_delay()
    svc, p = _svc(st)
    svc.all_stop()
    assert p.names() == ["heater_recipe_running", "mark_emergency_stopped", "plc_emergency_stop",
                         "dc_emergency_off", "chat_notify_failed_now",
                         "chat_enabled", "chat_notify_failed_now", "chat_notify_finished"] + CANCEL_TAIL + ["rfpulse_stop"]
    assert p.calls[-4] == ["stage", "CSV 공정 취소"]
    assert "request_stop" not in p.names()


def test_all_stop_device_errors_do_not_stop_the_sequence():
    st = _manual()
    svc, p = _svc(st, heater_recipe_running=RuntimeError("x"), plc_emergency_stop=RuntimeError("plc"),
                  dc_emergency_off=RuntimeError("dc"), chat_notify_failed_now=RuntimeError("chat"),
                  request_stop=RuntimeError("stop"))
    svc.all_stop()
    assert p.names()[-1] == "rfpulse_stop" and "request_stop" in p.names()


# ───────────────────────── 설비 이상 ─────────────────────────
def test_fault_idle_does_nothing():
    svc, p = _svc(ProcessState())
    svc.abort_by_fault("R", "D")
    assert p.calls == []


def test_fault_manual_detail_card_once():
    st = _manual()
    svc, p = _svc(st)
    svc.abort_by_fault("TC1 급락", "a\nb")
    assert p.names() == ["mark_fault_abort", "log", "log", "log", "chat_add_error", "chat_notify_failed_now",
                         "chat_fault_detail_sent", "mark_chat_fault_detail_sent", "chat_send_fault_detail",
                         "process_controller_present", "request_stop"]
    assert p.calls[1:4] == [["log", "경고", "[공정 중단] TC1 급락"], ["log", "경고", "  a"], ["log", "경고", "  b"]]
    assert p.calls[8] == ["chat_send_fault_detail", "TC1 급락", "a\nb"] and st.step_ok is False
    svc, p = _svc(st, chat_fault_detail_sent=True)
    svc.abort_by_fault("MV 닫힘")
    assert "chat_send_fault_detail" not in p.names()
    assert ["log", "경고", "[공정 중단] 추가 사유(챗 중복 발송 안 함): MV 닫힘"] in p.calls


def test_fault_csv_step_flags_list():
    st = _csv_step()
    svc, p = _svc(st)
    svc.abort_by_fault("R")
    assert ["log", "정보", "설비 이상 → CSV 리스트 전체 취소 플래그 설정"] in p.calls
    assert p.names()[-2:] == ["process_controller_present", "request_stop"] and st.csv_cancelled is True


@pytest.mark.parametrize("make,extra_log", [(_csv_delay, True), (_csv_between, False)])
def test_fault_csv_delay_or_between_cancels_list(make, extra_log):
    st = make()
    svc, p = _svc(st)
    svc.abort_by_fault("R")
    if extra_log:
        assert ["log", "정보", "CSV Delay 중 설비 이상 → 딜레이 중단 및 리스트 취소"] in p.calls
    assert p.names()[-len(CANCEL_TAIL):] == CANCEL_TAIL and "request_stop" not in p.names()
    assert ["stage", "CSV 공정 중단됨(설비 이상)"] in p.calls


# ───────────────────────── 종료 처리 ─────────────────────────
@pytest.mark.parametrize("active,suffix", [(False, " (시작된 공정 없음)"), (True, "")])
def test_finished_duplicate_ignored(active, suffix):
    st = ProcessState(finish_handled=True, running=active)
    svc, p = _svc(st)
    svc.on_finished()
    assert p.calls == [["log", "정보", "finished 무시 — 이번 공정의 종료 처리는 이미 끝났음" + suffix]]


FIN_HEAD = ["status_message", "clear_erp_meas", "chat_notify_finished"]
SINGLE_TAIL = ["reset_stats", "set_buttons", "stage", "reset_process_ui_fields", "close_process_log"]


def test_finished_manual_ok_writes_chk_row():
    st = _manual(heater_claimed=True)
    svc, p = _svc(st)
    svc.on_finished()
    assert p.names() == FIN_HEAD + ["build_chk_csv_row", "append_chk_csv_row", "log"] + SINGLE_TAIL
    assert p.calls[0] == ["status_message", "정보", "프로세스 종료중."]
    assert p.calls[2] == ["chat_notify_finished", True] and p.calls[5] == ["log", "정보", "ChK CSV 로그 저장 완료"]
    assert p.calls[-3] == ["stage", "공정 종료"]
    assert (st.finish_handled, st.heater_claimed, st.step_ok, st.running) == (True, False, False, False)


@pytest.mark.parametrize("ret,msg", [
    (dict(append_chk_csv_row=False), ["log", "경고", "ChK CSV 로그 저장 실패"]),
    (dict(append_chk_csv_row=OSError("NAS")), ["log", "경고", "ChK CSV 로그 처리 중 예외 발생: OSError('NAS')"]),
])
def test_finished_chk_row_failure_and_exception(ret, msg):
    st = _manual()
    svc, p = _svc(st, **ret)
    svc.on_finished()
    assert msg in p.calls and p.names()[-len(SINGLE_TAIL):] == SINGLE_TAIL


def test_finished_manual_failed_no_chk_row():
    st = _manual()
    st.step_ok = False
    svc, p = _svc(st)
    svc.on_finished()
    assert p.calls[2] == ["chat_notify_finished", False]
    assert ["log", "정보", "이번 공정은 비정상 종료 → ChK CSV에 기록하지 않음"] in p.calls
    assert "build_chk_csv_row" not in p.names()


def test_finished_csv_cancelled_branch():
    st = _csv_step(csv_cancelled=True)
    svc, p = _svc(st)
    svc.on_finished()
    assert p.names()[-5:] == ["reset_stats", "set_buttons", "stage", "reset_process_ui_fields", "close_process_log"]
    assert ["stage", "CSV 공정 취소됨"] in p.calls
    assert (st.csv_cancelled, st.csv_mode, st.csv_rows, st.running) == (False, False, [], False)


def test_finished_csv_ok_goes_to_next_step():
    st = _csv_step()
    svc, p = _svc(st)
    svc.start_next_csv_step = lambda: p._r("start_next_csv_step")
    svc.on_finished()
    assert p.names()[-2:] == ["reset_stats", "start_next_csv_step"] and st.running is False


def test_finished_csv_failed_ends_list():
    st = _csv_step()
    st.step_ok = False
    svc, p = _svc(st)
    svc.on_finished()
    assert ["log", "경고", "CSV 공정 중 실패 발생 → 다음 공정을 실행하지 않고 리스트 공정을 종료합니다."] in p.calls
    assert ["stage", "CSV 공정 실패로 중단됨"] in p.calls and p.names()[-1] == "close_process_log"
    assert st.csv_mode is False and st.running is False


# ───────────────────────── 오류 ─────────────────────────
def test_critical_error_marks_then_notice():
    st = _manual()
    svc, p = _svc(st)
    svc.on_critical_error("Ar 이탈")
    assert p.calls == [["chat_notify_failed_now", "Ar 이탈", False],
                       ["notice", "process", "critical", "공정 중단", "공정이 중단되었습니다.\n\n사유: Ar 이탈"]]
    assert st.step_ok is False and st.running is True        # 종료 처리는 finished 가 한다


def test_connection_failed_finishes_before_notice():
    st = _manual()
    svc, p = _svc(st)
    svc.on_connection_failed("MFC 연결 불가")
    assert p.names()[0] == "chat_notify_failed_now" and p.names()[1:4] == FIN_HEAD
    assert p.calls[3] == ["chat_notify_finished", False]
    assert p.calls[-1] == ["notice", "process", "critical", "연결 실패", "MFC 연결 불가"]
    assert p.names()[-2] == "close_process_log"              # 정리 뒤에 알린다


# ───────────────────────── 재기동 ─────────────────────────
@pytest.mark.parametrize("make", [_csv_delay, _csv_between])
def test_restart_csv_delay_or_between_cancels_list(make):
    st = make()
    svc, p = _svc(st)
    svc.on_restart_required("PLC 재기동 감지")
    assert p.calls[0] == ["chat_notify_failed_now", "PLC 재기동 감지", False]
    assert p.calls[1:4] == [["chat_enabled"], ["chat_notify_failed_now", "PLC 재기동 감지", False],
                            ["chat_notify_finished", False]]
    assert ["stage", "CSV 공정 취소"] in p.calls and "request_stop" not in p.names()


def test_restart_csv_step_flags_and_stops():
    st = _csv_step()
    svc, p = _svc(st)
    svc.on_restart_required("PLC 재기동 감지")
    assert p.names() == ["chat_notify_failed_now", "process_controller_present", "request_stop"]
    assert st.csv_cancelled is True and st.step_ok is False


def test_restart_manual_and_default_reason():
    st = _manual()
    svc, p = _svc(st)
    svc.on_restart_required("   ")
    assert p.calls == [["chat_notify_failed_now", "PLC 통신 이상(재시작)", False],
                       ["process_controller_present"], ["request_stop"]]


# ───────────────────────── 정적 검사 ─────────────────────────
def test_import_rules_cover_new_code():
    """core import 규칙 검사(test_process_service_start.test_core_import_rules)가 보는 파일에 이번 코드가 들어 있다."""
    path = os.path.join(ROOT, "core", "process_service.py")
    tree = ast.parse(open(path, encoding="utf-8").read())
    names = {f.name for f in ast.walk(tree) if isinstance(f, ast.FunctionDef)}
    assert {"stop", "all_stop", "abort_by_fault", "on_finished", "on_critical_error",
            "on_connection_failed", "on_restart_required"} <= names
    mods = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names] + \
           [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert not [m for m in mods if m.split(".")[0] in ("PyQt6", "UI", "main", "controller", "device", "reporter")
                or m in ("lib.logger", "lib.heater_logger", "lib.recipe_io")]
