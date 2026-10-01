# -*- coding: utf-8 -*-
"""T123~ ERP 원격 명령 중에는 경고창을 띄우지 않고 그 문구를 실패 사유로 보고한다.
로컬(사람 조작)에서는 지금과 똑같이 창이 뜬다."""
import os
from unittest.mock import MagicMock

import pytest
from PyQt6.QtCore import QTimer, QEventLoop

from conftest import make_heater_st
from test_main_heater import win, fresh, feed, spin   # noqa: F401  (픽스처 재사용)

import main as MAIN


class _MB:
    """QMessageBox 대역 — 호출 횟수만 센다. StandardButton 은 진짜를 쓴다."""
    def __init__(self):
        from PyQt6.QtWidgets import QMessageBox as _Q
        self.StandardButton = _Q.StandardButton
        self.calls = []

    def _rec(self, kind):
        def f(parent, title, text, *a, **k):
            self.calls.append((kind, title, text))
            return self.StandardButton.No
        return f

    def __getattr__(self, name):
        if name in ("warning", "critical", "information", "question"):
            return self._rec(name)
        raise AttributeError(name)

    def count(self, kind=None):
        return len([c for c in self.calls if kind is None or c[0] == kind])


@pytest.fixture
def erp(fresh, monkeypatch):
    w = fresh
    mb = _MB()
    monkeypatch.setattr(MAIN, "QMessageBox", mb)
    monkeypatch.setattr(MAIN, "set_process_log_file", lambda **k: None)
    w.erp = MagicMock()
    w.erp.rejected = False
    w.erp.pop_commands.return_value = []
    w._mb = mb
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w.csv_file_path = None; w.csv_rows = []
    w._remote_exec = False; w._remote_alerts = []
    yield w
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w.erp = MagicMock()


def run_remote(w, command, args=None):
    """ERP 명령 1건을 드레인 경로로 실행하고 (ok, 사유) 를 돌려준다."""
    w._mb.calls.clear()
    w.erp.cmd_result.reset_mock()
    w.erp.pop_commands.return_value = [{"id": 1, "command": command, "args": args or {}}]
    w._erp_cmd_timer.timeout.emit()
    w.erp.pop_commands.return_value = []
    a = w.erp.cmd_result.call_args.args
    return bool(a[1]), (a[2] if len(a) > 2 else "")


def _params(w, **over):
    ui = w.ui
    ui.Ar_gas_radio.setChecked(True); ui.O2_gas_radio.setChecked(False)
    ui.Ar_flow_edit.setPlainText(over.get("ar", "20"))
    ui.working_pressure_edit.setPlainText(over.get("wp", "5"))
    ui.dc_power_checkbox.setChecked(True); ui.DC_power_edit.setPlainText(over.get("dc", "100"))
    ui.rf_power_checkbox.setChecked(False); ui.rf_pulse_checkbox.setChecked(False)
    ui.Shutter_delay_edit.setPlainText(over.get("sd", "1"))
    ui.process_time_edit.setPlainText(over.get("pt", "1"))
    ui.G1_checkbox.setChecked(True); ui.G1_edit.setPlainText("CeO2"); ui.G2_checkbox.setChecked(False)


# ───────────────────────── 공정 시작 경로 ─────────────────────────
def test_T123_main_valve_closed_and_unreadable(erp, monkeypatch):
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (False, True))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and w._mb.count() == 0
    assert why.startswith("공정 시작 불가: 메인밸브가 열려 있지 않아") and "MV=OFF" in why and "\n" not in why
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (None, None))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and w._mb.count() == 0 and "메인밸브 상태를 읽을 수 없습니다" in why


def test_T124_already_running_and_atmosphere_busy(erp, monkeypatch):
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    w.process_running = True
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and w._mb.count() == 0 and "이미 공정이 진행 중입니다" in why
    w.process_running = False
    w.heater_atmosphere.is_active.return_value = True
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and w._mb.count() == 0 and "히터 가스·압력이 잡혀 있습니다" in why
    w.heater_atmosphere.is_active.return_value = False


def test_T125_bad_process_params(erp, monkeypatch):
    w = erp; _params(w, ar="")
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    ok, why = run_remote(w, "PROCESS_START", {"useAr": True, "arFlow": ""})
    assert ok is False and w._mb.count() == 0
    assert why.startswith("입력 오류: 공정 파라미터가 잘못되었습니다") and "Ar 가스 유량" in why


def test_T126_process_start_silent_failure_is_reported(erp, monkeypatch):
    """창도 예외도 없이 조용히 return 하는 경로 — 결과로 판정한다."""
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    monkeypatch.setattr(w, "_handle_remote_manual_start", lambda inputs: None)
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and why == "공정이 시작되지 않았습니다 (장비 로그 확인)"
    monkeypatch.setattr(w, "_handle_remote_manual_start", lambda inputs: setattr(w, "process_running", True))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is True and why == ""


# ───────────────────────── 레시피 적재/시작 ─────────────────────────
def _rows(n=1, bad=False):
    r = {"Process_name": "S1", "Ar": "1", "Ar_flow": "20", "O2": "0", "O2_flow": "0",
         "working_pressure": "5", "process_time": "1", "shutter_delay": "1",
         "use_rf_power": "0", "rf_power": "0", "use_dc_power": "1", "dc_power": "100",
         "use_rf_pulse": "0", "rf_pulse_power": "0", "rf_pulse_freq": "", "rf_pulse_duty": "",
         "use_dc_delay": "0", "use_heater": "0", "heater_temp": "0", "heater_ramp": "0",
         "gun1": "1", "gun2": "0", "G1 Target": "CeO2", "G2 Target": ""}
    if bad:
        r["working_pressure"] = "abc"
    return [dict(r) for _ in range(n)]


def test_T127_recipe_load_failures(erp, monkeypatch):
    w = erp
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": []})
    assert ok is False and "레시피 행이 없습니다" in why and w._mb.count() == 0
    # 파일 없음
    monkeypatch.setattr(MAIN.Path, "exists", lambda self: False)
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows()})
    assert ok is False and w._mb.count() == 0 and "선택한 CSV 파일을 찾을 수 없습니다" in why
    monkeypatch.undo()
    # 읽기 오류
    monkeypatch.setattr(MAIN, "load_table", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("깨진 파일")))
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows()})
    assert ok is False and w._mb.count() == 0 and "CSV 읽기 오류" in why and "깨진 파일" in why
    monkeypatch.undo()
    # 빈 파일(유효 행 없음)
    monkeypatch.setattr(MAIN, "load_table", lambda *a, **k: [])
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows()})
    assert ok is False and w._mb.count() == 0 and "유효한 공정 행이 없습니다" in why


def test_T128_recipe_load_empty_and_bad_first_row(erp, monkeypatch):
    w = erp
    monkeypatch.setattr(w.proc, "load_csv_list", lambda: False)
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows()})
    assert ok is False and w._mb.count() == 0 and why == "레시피를 적재하지 못했습니다 (장비 로그 확인)"
    monkeypatch.undo()
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows(bad=True)})
    assert ok is False and w._mb.count() == 0 and "첫 번째 공정 파라미터가 잘못되었습니다" in why


def test_T129_recipe_load_rejected_during_process(erp):
    w = erp
    w.process_running = True
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows()})
    assert ok is False and w._mb.count() == 0 and "공정 진행 중에는 CSV 파일을 변경할 수 없습니다" in why
    w.process_running = False


def test_T130_recipe_process_start_first_step_error_cancels_list(erp, monkeypatch):
    w = erp
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    ok, why = run_remote(w, "RECIPE_PROCESS_RUN", {"rows": _rows(2)})
    assert ok is True and w.csv_rows                       # 적재는 정상
    monkeypatch.setattr(w, "_build_params_from_csv_row",
                        lambda row: (_ for _ in ()).throw(ValueError("working_pressure 오류")))
    ok, why = run_remote(w, "RECIPE_PROCESS_START")
    assert ok is False and w._mb.count() == 0
    assert "CSV 레시피 오류" in why and "working_pressure 오류" in why
    assert w.csv_mode is False and w.csv_cancelled is False    # 리스트 정리는 기존 흐름 그대로
    assert not w.process_running


# ───────────────────────── 히터 ─────────────────────────
def test_T131_heater_sv_input_errors(erp, monkeypatch):
    w = erp
    feed(w, make_heater_st(run=False, itl=True))
    w.ui.heater_sv_edit.setText("")
    ok, why = run_remote(w, "HEATER_ONOFF", {"on": True})
    assert ok is False and w._mb.count() == 0 and "히터 목표 온도가 설정되지 않았습니다" in why
    w.ui.heater_sv_edit.setText("abc")
    ok, why = run_remote(w, "HEATER_ONOFF", {"on": True})
    assert ok is False and w._mb.count() == 0 and "숫자가 아닙니다" in why
    w.ui.heater_sv_edit.setText(str(MAIN.HEATER_MAX_TEMP + 100))
    ok, why = run_remote(w, "HEATER_ONOFF", {"on": True})
    assert ok is False and w._mb.count() == 0
    assert "입력 오류" in why and "\n" not in why


def test_T132_heater_on_rejected_when_process_uses_mfc(erp, monkeypatch):
    w = erp
    feed(w, make_heater_st(run=False, itl=True))
    w.ui.heater_sv_edit.setText("300")
    w.ui.heater_ar_check.setChecked(True); w.ui.heater_ar_flow_edit.setText("20")
    w.ui.heater_wp_edit.setText("5")
    w.process_running = True
    reverted = []
    monkeypatch.setattr(w, "_revert_heater_onoff", lambda: reverted.append(1))
    ok, why = run_remote(w, "HEATER_ONOFF", {"on": True})
    assert ok is False and w._mb.count() == 0 and ("가스" in why or "공정" in why)
    assert reverted == [1]                                   # 창 다음 흐름(되돌리기)은 그대로
    w.process_running = False


# ───────────────────────── 로컬(사람 조작)은 그대로 ─────────────────────────
def test_T133_local_still_shows_dialog(erp, monkeypatch):
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (False, True))
    w._mb.calls.clear()
    w._handle_start_process()                                 # 원격이 아닌 직접 호출
    assert w._mb.count("warning") == 1 and w._mb.calls[0][1] == "공정 시작 불가"
    assert w._remote_alerts == []
    w._mb.calls.clear()
    w.heater_atmosphere.is_active.return_value = True
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    w._handle_start_process()
    assert w._mb.count("warning") == 1
    w.heater_atmosphere.is_active.return_value = False


def test_T134_next_command_not_blocked_after_failure(erp, monkeypatch):
    """실패 직후 다음 원격 명령이 '대화상자가 열려 있습니다' 로 거부되지 않는다."""
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (False, True))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False
    from PyQt6.QtWidgets import QApplication
    assert QApplication.activeModalWidget() is None
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    monkeypatch.setattr(w, "_handle_remote_manual_start", lambda inputs: setattr(w, "process_running", True))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is True and "대화상자" not in why


def test_T135_information_only_is_success(erp, monkeypatch):
    w = erp
    monkeypatch.setattr(w, "_handle_remote_manual_start",
                        lambda inputs: (w._alert("information", "안내", "참고 사항"), setattr(w, "process_running", True)))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is True and why == "" and w._mb.count() == 0


# ═══════════════ T136~ 출처(local/erp)별 알림 · 자동 알림 순서 · 히터 레시피 경로 통일 ═══════════════
def _notices(w):
    return [c.args for c in w.erp.notice.call_args_list]


def test_T136_origin_is_recorded_by_starter(erp, monkeypatch):
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    started = []
    monkeypatch.setattr(w, "request_process_start",
                        type("S", (), {"emit": staticmethod(lambda p: started.append(p))})())
    w._proc_origin = "erp"
    w._handle_start_process()                                  # 노트북 Start
    assert w._proc_origin == "local" and started
    w.process_running = False
    run_remote(w, "PROCESS_START")
    assert w._proc_origin == "erp"
    # 가드에 막힌 시작은 출처를 바꾸지 않는다
    w.process_running = False; w._proc_origin = "local"
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (False, True))
    run_remote(w, "PROCESS_START")
    assert w._proc_origin == "local"


def test_T137_heater_origin(erp, monkeypatch):
    w = erp
    feed(w, make_heater_st(run=False, itl=True))
    w.ui.heater_sv_edit.setText("300")
    w.ui.heater_ar_check.setChecked(False); w.ui.heater_o2_check.setChecked(False)
    monkeypatch.setattr(w, "_heater_manual_go", lambda v: None)
    w._heater_origin = "erp"
    w._on_heater_onoff_toggled(True)                           # 노트북 ON
    assert w._heater_origin == "local"
    run_remote(w, "HEATER_ONOFF", {"on": True})
    assert w._heater_origin == "erp"


def test_T138_critical_error_order_and_origin(erp, monkeypatch):
    """실패 표시·정리가 먼저, 창은 마지막(QTimer). 창을 띄우기 전에 종료 처리가 돌아도 실패로 남는다."""
    w = erp
    w._proc_origin = "local"; w.process_running = True; w._chat_reset_run_state(); w._chk_process_ok = True
    monkeypatch.setattr(w, "_chat_notify_failed_now", lambda *a, **k: None)
    w._handle_critical_error("RF 이상")
    assert w._chk_process_ok is False                          # 창보다 먼저
    assert w._mb.count() == 0                                  # 아직 창 없음(singleShot 대기)
    n = _notices(w)[-1]
    assert n[0] == "error" and n[1] == "공정 중단" and n[3] == "local" and n[4] == "process"
    spin(50)
    assert w._mb.count("critical") == 1 and w._mb.calls[-1][1] == "공정 중단"
    # erp 출처면 창이 없다
    w._mb.calls.clear(); w.erp.notice.reset_mock()
    w._proc_origin = "erp"; w._chk_process_ok = True
    w._handle_critical_error("MFC 이상")
    spin(50)
    assert w._mb.count() == 0 and w._chk_process_ok is False
    assert _notices(w)[-1][3] == "erp"


def test_T139_connection_failure_and_csv_notices_order(erp, monkeypatch):
    w = erp; w._proc_origin = "local"
    done = []
    monkeypatch.setattr(w.proc, "on_finished", lambda: done.append(w._mb.count()))
    monkeypatch.setattr(w, "_chat_notify_failed_now", lambda *a, **k: None)
    w._handle_connection_failure("PLC 연결 실패")
    assert done == [0] and w._chk_process_ok is False           # 정리가 창보다 먼저
    spin(50)
    assert w._mb.count("critical") == 1
    # CSV 완료
    w._mb.calls.clear()
    w.csv_mode = True; w.csv_rows = [{"Process_name": "A"}]; w.csv_index = 0
    w._start_next_csv_step()
    assert w.csv_mode is False and w._mb.count() == 0
    spin(50)
    assert w._mb.count("information") == 1
    assert _notices(w)[-1][1] == "CSV 공정 완료"


def test_T140_csv_row_error_cancels_before_notice(erp, monkeypatch):
    w = erp; w._proc_origin = "local"
    cancels = []
    monkeypatch.setattr(w.proc, "cancel_csv_list_now", lambda *a, **k: cancels.append(w._mb.count()))
    monkeypatch.setattr(w, "_build_params_from_csv_row",
                        lambda row: (_ for _ in ()).throw(ValueError("wp 오류")))
    w.csv_mode = True; w.csv_rows = _rows(2); w.csv_index = -1
    w._start_next_csv_step()
    assert cancels == [0]                                      # 정리가 창보다 먼저
    spin(50)
    assert w._mb.count("critical") == 1 and "CSV 레시피 오류" in w._mb.calls[-1][1]
    w.csv_mode = False; w.csv_rows = []


def test_T141_notice_suppressed_inside_remote_command(erp, monkeypatch):
    w = erp
    w.erp.notice.reset_mock()
    monkeypatch.setattr(w, "_handle_remote_manual_start",
                        lambda inputs: (w._notice("process", "warning", "안내 제목", "본문"),
                                 setattr(w, "process_running", True)))
    ok, why = run_remote(w, "PROCESS_START")
    spin(50)
    assert ok is True and why == ""                            # 알림은 실패 사유가 아니다
    assert w._mb.count() == 0 and w.erp.notice.call_count == 0
    assert w._remote_notes and w._remote_notes[0][1] == "안내 제목"


def test_T142_remote_recipe_stop_reports_success(erp, monkeypatch):
    """공정 밖 히터 레시피 정지 — _on_heater_recipe_finished 의 문구가 실패로 둔갑하지 않는다."""
    w = erp
    monkeypatch.setattr(w.heater_recipe, "is_running", lambda: True, raising=False)
    monkeypatch.setattr(w.heater_recipe, "was_user_stopped", lambda: True, raising=False)

    def _stop(reason=""):
        w._on_heater_recipe_finished(False, "사용자 중단")
    monkeypatch.setattr(w.heater_recipe, "stop", _stop, raising=False)
    ok, why = run_remote(w, "RECIPE_HEATER_STOP")
    assert ok is True and why == "" and w._mb.count() == 0


def test_T143_remote_heater_recipe_uses_common_path(erp, monkeypatch):
    w = erp
    rows = [{"step": 1, "target_c": 300, "ramp_c_per_min": 6, "soak_min": 10,
             "use_ar": 1, "ar_flow": 20, "use_o2": 0, "o2_flow": 0, "wp_mtorr": 5}]
    calls = {"guard": 0, "start": 0, "ramp_stop": 0, "panel": 0, "rebuild": 0}
    monkeypatch.setattr(w.heater_recipe, "load", lambda p: True, raising=False)
    monkeypatch.setattr(w.heater_recipe, "recipe_gas",
                        lambda: {"use_ar": True, "ar_flow": 20.0, "sp1": 5.0}, raising=False)
    monkeypatch.setattr(w.heater_recipe, "describe_gas", lambda: "Ar 20 sccm", raising=False)
    monkeypatch.setattr(w.heater_recipe, "steps", lambda: [], raising=False)
    running = {"v": False}
    monkeypatch.setattr(w.heater_recipe, "is_running", lambda: running["v"], raising=False)

    def _start():
        calls["start"] += 1; running["v"] = True; return True
    monkeypatch.setattr(w.heater_recipe, "start", _start, raising=False)
    monkeypatch.setattr(w, "_apply_recipe_gas_to_panel", lambda g: calls.__setitem__("panel", calls["panel"] + 1))
    monkeypatch.setattr(w, "_rebuild_heater_step_list", lambda: calls.__setitem__("rebuild", calls["rebuild"] + 1))
    monkeypatch.setattr(w, "_heater_gas_wanted", lambda: True)
    monkeypatch.setattr(w, "_heater_gas_start_guard",
                        lambda: (calls.__setitem__("guard", calls["guard"] + 1), True)[1])
    monkeypatch.setattr(w.heater_atmosphere, "is_ready", lambda: False, raising=False)
    ok, why = run_remote(w, "RECIPE_HEATER_RUN", {"rows": rows})
    assert ok is True and w._mb.count() == 0                   # 확인창 없이(confirm=False)
    assert calls["panel"] == 1 and calls["rebuild"] == 1 and calls["guard"] == 1   # 가스가 무시되지 않는다
    assert w._heater_pending and w._heater_pending[0] == "recipe_start" and w._heater_origin == "erp"
    # 가스가 없으면 램프 정지 후 바로 start
    w._heater_pending = None
    monkeypatch.setattr(w, "_heater_gas_wanted", lambda: False)
    monkeypatch.setattr(w.heater_ramp, "stop",
                        lambda *a, **k: calls.__setitem__("ramp_stop", calls["ramp_stop"] + 1), raising=False)
    ok, why = run_remote(w, "RECIPE_HEATER_RUN", {"rows": rows})
    assert ok is True and calls["ramp_stop"] == 1 and calls["start"] == 1


def test_T144_remote_heater_recipe_blocked_by_process(erp):
    w = erp
    w.process_running = True; w._process_heater_claimed = True
    rows = [{"step": 1, "target_c": 300, "ramp_c_per_min": 6, "soak_min": 10}]
    ok, why = run_remote(w, "RECIPE_HEATER_RUN", {"rows": rows})
    assert ok is False and w._mb.count() == 0 and "현재 공정이 히터를 제어하고 있습니다" in why
    w.process_running = False; w._process_heater_claimed = False


def test_T145_local_recipe_path_asks_once(erp, monkeypatch):
    w = erp
    monkeypatch.setattr(w.heater_recipe, "load", lambda p: True, raising=False)
    monkeypatch.setattr(w.heater_recipe, "is_running", lambda: False, raising=False)
    monkeypatch.setattr(w.heater_recipe, "recipe_gas", lambda: None, raising=False)
    monkeypatch.setattr(w.heater_recipe, "describe_gas", lambda: "-", raising=False)
    monkeypatch.setattr(w.heater_recipe, "steps", lambda: [], raising=False)
    monkeypatch.setattr(w, "_heater_gas_wanted", lambda: False)
    started = []
    monkeypatch.setattr(w.heater_recipe, "start", lambda: started.append(1) or True, raising=False)
    monkeypatch.setattr(w.heater_ramp, "stop", lambda *a, **k: None, raising=False)
    w._run_heater_recipe_file("x.csv", confirm=True)           # _MB.question → No
    assert w._mb.count("question") == 1 and started == []
