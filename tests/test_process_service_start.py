# -*- coding: utf-8 -*-
"""core.process_service — Qt 없이, 부른 순서를 기록하는 가짜 ports 로 시작·적재 흐름을 본다.
그리고 core/*.py 의 import 규칙(ast)."""
import ast
import dataclasses
import glob
import os

import pytest

from core.params import ManualInputs
from core.process_service import ProcessPorts, ProcessService
from core.state import ProcessState

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MANUAL = ManualInputs(
    use_ar=True, ar_flow_text="20", use_o2=False, o2_flow_text="",
    working_pressure_text="5", use_dc=True, dc_power_text="100",
    use_rf=False, rf_power_text="", offset_text="6.79", param_text="1.0395",
    use_rf_pulse=False, rfp_power_text="", rfp_freq_text="", rfp_duty_text="",
    shutter_delay_text="1", process_time_text="1",
    use_g1=True, g1_name_text="CeO2", use_g2=False, g2_name_text="", use_dc_delay=False)

ROW = {"Process_name": "S1", "Ar": "1", "Ar_flow": "20", "working_pressure": "5",
       "process_time": "1", "use_dc_power": "1", "dc_power": "100"}


class FakePorts:
    """ProcessPorts 대역 — 부른 순서를 calls 에 [이름, 인자…] 로 남긴다. 반환값은 속성으로 정한다."""

    def __init__(self, **ret):
        self.calls = []
        self.ret = dict(heater_recipe_running=False, heater_gas_guard=True, main_valve_open=True,
                        command_origin="local", is_closing=False, read_manual_inputs=MANUAL,
                        load_table=[dict(ROW)], build_csv_params={"process_name": "S1", "dc_power": 100.0})
        self.ret.update(ret)

    def _r(self, name, *args):
        self.calls.append([name, *args])
        v = self.ret.get(name)
        if isinstance(v, BaseException):
            raise v
        if callable(v) and not isinstance(v, (list, dict)):
            return v()
        return v

    def alert(self, kind, title, text): self._r("alert", kind, title, text)
    def log(self, level, msg): self._r("log", level, msg)
    def stage(self, text): self._r("stage", text)
    def set_buttons(self, start, stop, select_csv=None): self._r("set_buttons", start, stop, select_csv)
    def is_closing(self): return self._r("is_closing")
    def read_manual_inputs(self): return self._r("read_manual_inputs")
    def apply_params_to_ui(self, params): self._r("apply_params_to_ui", params)
    def build_csv_params(self, row): return self._r("build_csv_params", row)
    def open_process_log(self, prefix): self._r("open_process_log", prefix)
    def reset_stats(self): self._r("reset_stats")
    def load_table(self, path, preferred_sheets): return self._r("load_table", path, preferred_sheets)
    def command_origin(self): return self._r("command_origin")
    def main_valve_open(self): return self._r("main_valve_open")
    def clear_plc_fault(self): self._r("clear_plc_fault")
    def request_start(self, params): self._r("request_start", params)
    def heater_recipe_running(self): return self._r("heater_recipe_running")
    def heater_gas_guard(self): return self._r("heater_gas_guard")
    def log_heater_header(self, params): self._r("log_heater_header", params)
    def chat_reset_run_state(self): self._r("chat_reset_run_state")
    def chat_notify_started(self, params, name): self._r("chat_notify_started", params, name)
    def erp_run_start(self, name, params): self._r("erp_run_start", name, params)
    # 3b 에서 추가된 port
    def chat_enabled(self): return self._r("chat_enabled")
    def chat_text(self, msg): self._r("chat_text", msg)
    def chat_notify_failed_now(self, reason, send_text=False): self._r("chat_notify_failed_now", reason, send_text)
    def chat_add_error(self, text): self._r("chat_add_error", text)
    def chat_notify_finished(self, ok): self._r("chat_notify_finished", ok)
    def chat_user_stopped(self): return self._r("chat_user_stopped")
    def notice(self, source, kind, title, text): self._r("notice", source, kind, title, text)
    def reset_process_ui_fields(self): self._r("reset_process_ui_fields")
    def close_process_log(self): self._r("close_process_log")
    def delay_timer_start(self): self._r("delay_timer_start")
    def delay_timer_stop(self): self._r("delay_timer_stop")
    def delay_clock_start(self): self._r("delay_clock_start")
    def delay_elapsed_ms(self): return self._r("delay_elapsed_ms")
    def delay_clock_clear(self): self._r("delay_clock_clear")
    # 3c 에서 추가된 port
    def status_message(self, level, msg): self._r("status_message", level, msg)
    def mark_user_stopped(self): self._r("mark_user_stopped")
    def mark_emergency_stopped(self): self._r("mark_emergency_stopped")
    def mark_fault_abort(self): self._r("mark_fault_abort")
    def chat_fault_detail_sent(self): return self._r("chat_fault_detail_sent")
    def mark_chat_fault_detail_sent(self): self._r("mark_chat_fault_detail_sent")
    def chat_send_fault_detail(self, reason, detail): self._r("chat_send_fault_detail", reason, detail)
    def process_controller_present(self): return self._r("process_controller_present")
    def request_stop(self): self._r("request_stop")
    def plc_emergency_stop(self): self._r("plc_emergency_stop")
    def dc_emergency_off(self): self._r("dc_emergency_off")
    def rfpulse_stop(self): self._r("rfpulse_stop")
    def heater_recipe_stop(self, reason): self._r("heater_recipe_stop", reason)
    def clear_erp_meas(self): self._r("clear_erp_meas")
    def build_chk_csv_row(self): return self._r("build_chk_csv_row")
    def append_chk_csv_row(self, row): return self._r("append_chk_csv_row", row)

    def names(self):
        return [c[0] for c in self.calls]


def _svc(state=None, **ret):
    ports = FakePorts(**ret)
    return ProcessService(state or ProcessState(), ports), ports


def test_fake_ports_covers_protocol():
    wanted = {n for n in vars(ProcessPorts) if not n.startswith("_")}
    assert wanted <= set(vars(FakePorts))


# ───────────────────────── start(): 가드 ─────────────────────────
def test_guard1_heater_recipe_and_recipe_has_heater():
    st = ProcessState(csv_file_path="C:/r/a.csv",
                      csv_rows=[{"use_heater": "0"}, {"use_heater": "1", "heater_temp": "300"}])
    svc, p = _svc(st, heater_recipe_running=True)
    svc.start()
    assert p.names() == ["heater_recipe_running", "alert"]
    assert p.calls[1][1:3] == ["warning", "시작 불가"]
    assert st.origin == "local" and st.csv_mode is False


def test_guard1_passes_when_recipe_has_no_heater_then_guard2_running():
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[{"use_heater": "0"}], running=True, origin="x")
    svc, p = _svc(st, heater_recipe_running=True)
    svc.start()
    assert p.names() == ["heater_recipe_running", "alert"]
    assert p.calls[1][1:] == ["warning", "경고", "이미 공정이 진행 중입니다."]
    assert st.origin == "x"


def test_guard3_heater_gas_then_guard4_main_valve():
    st = ProcessState(origin="x")
    svc, p = _svc(st, heater_gas_guard=False)
    svc.start()
    assert p.names() == ["heater_recipe_running", "heater_gas_guard"] and st.origin == "x"
    svc, p = _svc(st, main_valve_open=False)
    svc.start()
    assert p.names() == ["heater_recipe_running", "heater_gas_guard", "main_valve_open"] and st.origin == "x"


@pytest.mark.parametrize("origin", ["local", "erp"])
def test_origin_recorded_after_all_guards_then_plc_fault_cleared(origin):
    st = ProcessState(origin="x")
    svc, p = _svc(st, command_origin=origin)
    svc.start()
    assert p.names()[:5] == ["heater_recipe_running", "heater_gas_guard", "main_valve_open",
                             "command_origin", "clear_plc_fault"]
    assert st.origin == origin


# ───────────────────────── start(): 수동 ─────────────────────────
@pytest.mark.parametrize("heater_running", [False, True])
def test_manual_start_order_and_state(heater_running):
    st = ProcessState()
    svc, p = _svc(st, heater_recipe_running=heater_running)
    svc.start()
    exp = ["heater_recipe_running", "heater_gas_guard", "main_valve_open", "command_origin",
           "clear_plc_fault", "read_manual_inputs", "reset_stats", "open_process_log", "log",
           "heater_recipe_running"] + (["log"] if heater_running else []) + [
           "log_heater_header", "chat_reset_run_state", "chat_notify_started", "erp_run_start",
           "request_start", "set_buttons"]
    assert p.names() == exp
    c = {cl[0]: cl for cl in p.calls}
    assert c["open_process_log"] == ["open_process_log", "CHK"]
    assert p.calls[8] == ["log", "정보", "=== CHK 공정 시작 ==="]
    if heater_running:
        assert p.calls[10] == ["log", "정보", "[히터] 히터 레시피가 제어 중입니다. 이번 공정은 히터를 제어하지 않습니다."]
    params = c["request_start"][1]
    assert c["chat_notify_started"][1:] == [params, "Single CHK"]
    assert c["erp_run_start"][1:] == ["Single CHK", params]
    assert c["set_buttons"] == ["set_buttons", False, True, False]
    assert (st.current_name, st.step_ok, st.running, st.heater_claimed) == ("Single CHK", True, True, False)
    assert st.last_params == params and st.last_params is not params     # 복사본


def test_manual_erp_failure_is_swallowed():
    st = ProcessState()
    svc, p = _svc(st, erp_run_start=RuntimeError("ERP 죽음"))
    svc.start()
    assert p.names()[-2:] == ["request_start", "set_buttons"] and st.running is True


def test_manual_input_error():
    st = ProcessState()
    svc, p = _svc(st, read_manual_inputs=dataclasses.replace(MANUAL, use_ar=False))
    svc.start()
    assert p.names() == ["heater_recipe_running", "heater_gas_guard", "main_valve_open", "command_origin",
                         "clear_plc_fault", "read_manual_inputs", "alert"]
    assert p.calls[-1][1:] == ["warning", "입력 오류",
                               "공정 파라미터가 잘못되었습니다:\nAr 또는 O2 가스를 하나 이상 선택해야 합니다."]
    assert st.running is False and st.current_name == "" and st.step_ok is False


# ───────────────────────── start(): CSV 분기 ─────────────────────────
def test_csv_branch_load_then_mode_then_next_step():
    st = ProcessState(csv_file_path="C:/r/a.csv")
    rows = [{"#": "1", "Process_name": ""}, dict(ROW)]
    svc, p = _svc(st, load_table=rows)
    seen = {}
    p.ret["start_next_csv_step"] = lambda: seen.update(mode=st.csv_mode, rows=list(st.csv_rows))
    svc.start_next_csv_step = lambda: p._r("start_next_csv_step")   # 3b: 다음 스텝은 port 가 아니라 서비스 메서드
    svc.start()
    assert p.names() == ["heater_recipe_running", "heater_gas_guard", "main_valve_open", "command_origin",
                         "clear_plc_fault", "load_table", "start_next_csv_step"]
    assert p.calls[5] == ["load_table", "C:/r/a.csv", ("Recipe", "recipe", "공정", "Sheet1")]
    assert seen == {"mode": True, "rows": [dict(ROW)]}        # load → csv_mode → 다음 스텝 순서
    assert st.csv_index == -1


def test_csv_branch_load_failure_stops():
    st = ProcessState(csv_file_path="C:/r/a.csv")
    svc, p = _svc(st, load_table=[])
    svc.start()
    assert p.names()[-2:] == ["load_table", "alert"] and "start_next_csv_step" not in p.names()
    assert st.csv_mode is False


# ───────────────────────── load_recipe_file ─────────────────────────
@pytest.fixture
def recipe_file(tmp_path):
    f = tmp_path / "recipe.csv"
    f.write_text("x", encoding="utf-8")
    return str(f)


def test_load_recipe_closing_and_active_and_empty_path(recipe_file):
    svc, p = _svc(is_closing=True)
    svc.load_recipe_file(recipe_file)
    assert p.names() == ["is_closing"]
    svc, p = _svc(ProcessState(delay_active=True))
    svc.load_recipe_file(recipe_file)
    assert p.names() == ["is_closing", "alert"] and p.calls[1][2] == "변경 불가"
    svc, p = _svc()
    svc.load_recipe_file("")
    assert p.names() == ["is_closing"]


def test_load_recipe_missing_file_keeps_old_path(tmp_path):
    st = ProcessState(csv_file_path="C:/r/old.csv")
    svc, p = _svc(st)
    svc.load_recipe_file(str(tmp_path / "없음.csv"))
    assert p.names() == ["is_closing", "alert"] and p.calls[1][1:3] == ["warning", "파일 오류"]
    assert st.csv_file_path == "C:/r/old.csv"


def test_load_recipe_preview_first_row(recipe_file):
    st = ProcessState()
    svc, p = _svc(st)
    svc.load_recipe_file(recipe_file)
    assert p.names() == ["is_closing", "log", "load_table", "is_closing", "build_csv_params", "is_closing",
                         "apply_params_to_ui", "stage"]
    assert p.calls[1] == ["log", "정보", f"CSV 공정 리스트 파일 선택: {recipe_file}"]
    assert p.calls[-1] == ["stage", "CSV 공정: 1/1 - S1"]
    assert st.csv_file_path == recipe_file and st.csv_rows == [dict(ROW)]


def test_load_recipe_first_row_delay_and_bad_first_row(recipe_file):
    svc, p = _svc(load_table=[{"Process_name": " delay 3s "}, dict(ROW)])
    svc.load_recipe_file(recipe_file)
    assert p.names()[-1] == "stage" and "build_csv_params" not in p.names()
    assert p.calls[-1] == ["stage", "CSV 공정: 1/2 - delay 3s (대기 스텝)"]
    svc, p = _svc(build_csv_params=ValueError("값 오류"))
    svc.load_recipe_file(recipe_file)
    assert p.names()[-3:] == ["build_csv_params", "alert", "stage"]
    assert p.calls[-2][1:] == ["warning", "CSV 레시피 오류", "첫 번째 공정 파라미터가 잘못되었습니다:\n값 오류"]
    assert p.calls[-1] == ["stage", "CSV 공정: 1/1 - (오류)"]


def test_load_recipe_closing_during_load(recipe_file):
    seq = iter([False, True])
    svc, p = _svc(is_closing=lambda: next(seq))
    svc.load_recipe_file(recipe_file)
    assert p.names() == ["is_closing", "log", "load_table", "is_closing"]


# ───────────────────────── load_csv_list ─────────────────────────
def test_load_csv_list_branches():
    svc, p = _svc(ProcessState())
    assert svc.load_csv_list() is False and p.calls == [["alert", "warning", "CSV 없음", "먼저 CSV 파일을 선택해 주세요."]]
    st = ProcessState(csv_file_path="C:/r/a.csv")
    svc, p = _svc(st, load_table=RuntimeError("깨짐"))
    assert svc.load_csv_list() is False
    assert p.calls[-1] == ["alert", "critical", "CSV 읽기 오류", "CSV 파일을 읽는 중 오류가 발생했습니다.\n\n깨짐"]
    svc, p = _svc(st, load_table=[{"#": "1"}, {"Ar": " "}])
    assert svc.load_csv_list() is False and p.calls[-1][2] == "CSV 비어있음"
    st.csv_index = 5
    svc, p = _svc(st, load_table=[{"#": "1"}, dict(ROW)])
    assert svc.load_csv_list() is True and st.csv_rows == [dict(ROW)] and st.csv_index == -1


# ───────────────────────── 정적 검사 ─────────────────────────
_FORBIDDEN = ("PyQt6", "UI", "main", "controller", "device", "reporter",
              "lib.logger", "lib.heater_logger", "lib.recipe_io")


def test_core_import_rules():
    bad = []
    files = glob.glob(os.path.join(ROOT, "core", "*.py"))
    assert {os.path.basename(f) for f in files} >= {"params.py", "state.py", "recipe.py", "process_service.py"}
    for f in files:
        tree = ast.parse(open(f, encoding="utf-8").read(), f)
        for node in ast.walk(tree):
            mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                    else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
            for m in mods:
                if any(m == x or m.startswith(x + ".") for x in _FORBIDDEN):
                    bad.append((os.path.basename(f), m))
                if m.split(".")[0] == "lib" and m != "lib.config":
                    bad.append((os.path.basename(f), m))
    assert bad == []
