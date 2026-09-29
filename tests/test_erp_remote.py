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
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and w._mb.count() == 0
    assert why.startswith("입력 오류: 공정 파라미터가 잘못되었습니다") and "Ar 가스 유량" in why


def test_T126_process_start_silent_failure_is_reported(erp, monkeypatch):
    """창도 예외도 없이 조용히 return 하는 경로 — 결과로 판정한다."""
    w = erp; _params(w)
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    monkeypatch.setattr(w, "_handle_start_process", lambda: None)
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is False and why == "공정이 시작되지 않았습니다 (장비 로그 확인)"
    monkeypatch.setattr(w, "_handle_start_process", lambda: setattr(w, "process_running", True))
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
    monkeypatch.setattr(w, "_load_csv_process_list", lambda: False)
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
    monkeypatch.setattr(w, "_handle_start_process", lambda: setattr(w, "process_running", True))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is True and "대화상자" not in why


def test_T135_information_only_is_success(erp, monkeypatch):
    w = erp
    monkeypatch.setattr(w, "_handle_start_process",
                        lambda: (w._alert("information", "안내", "참고 사항"), setattr(w, "process_running", True)))
    ok, why = run_remote(w, "PROCESS_START")
    assert ok is True and why == "" and w._mb.count() == 0
