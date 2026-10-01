# -*- coding: utf-8 -*-
"""integrations.erp_commands — Qt 없이, 부른 순서를 기록하는 가짜 host 로 원격 명령 처리를 본다."""
import ast
import csv
import glob
import os
import tempfile

import pytest

from core.params import ManualInputs
from integrations.erp_commands import (ErpCommandHost, ErpCommandRunner, PLC_BUTTONS, manual_inputs_from_args,
                                       PROCESS_RECIPE_COLS, HEATER_RECIPE_COLS, alert_reason, write_recipe_csv)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Widget:
    """입력칸 대역 — 부른 메서드를 host.calls 에 남긴다. plain=False 면 setText 만 있다."""

    def __init__(self, host, name, plain=True):
        self._h, self._n = host, name
        if plain:
            self.setPlainText = lambda s: self._h.calls.append(["w.setPlainText", self._n, s])

    def setText(self, s):
        self._h.calls.append(["w.setText", self._n, s])

    def setChecked(self, v):
        self._h.calls.append(["w.setChecked", self._n, v])


class FakeHost:
    def __init__(self, **kw):
        self.calls = []
        self.alerts, self.notes = [], []
        self.rejected = False
        self.shown = False
        self.cmds = []
        self.modal = False
        self.widgets = {}
        self.rows = []
        self.path = ""
        self.active = False
        self.pending = None
        self.recipe_running = False
        self.heater_names = set()
        self.on_exec = {}                   # 명령 이름 → 실행 중에 부를 함수(경고창·알림 흉내)
        self.__dict__.update(kw)

    def _r(self, *c):
        self.calls.append(list(c))

    def erp_rejected(self): self._r("erp_rejected"); return self.rejected
    def erp_pop_commands(self): self._r("erp_pop_commands"); c, self.cmds = self.cmds, []; return c
    def erp_cmd_result(self, *args): self._r("erp_cmd_result", *args)
    def rejected_shown(self): self._r("rejected_shown"); return self.shown
    def set_rejected_shown(self, v): self._r("set_rejected_shown", v); self.shown = v
    def log(self, level, msg): self._r("log", level, msg)
    def modal_open(self): self._r("modal_open"); return self.modal
    def begin_remote(self): self._r("begin_remote"); self.alerts, self.notes = [], []; self.remote = True
    def end_remote(self): self._r("end_remote"); self.remote = False
    def remote_alerts(self): return self.alerts
    def remote_notes(self): return self.notes
    def widget(self, name): return self.widgets.get(name)
    def start_process(self): self._r("start_process"); self._fire("start")
    def current_rf_cal(self): self._r("current_rf_cal"); return getattr(self, "rf_cal", ("6.79", "1.0395"))
    def start_manual(self, inputs): self._r("start_manual", inputs); self._fire("start")
    def stop_process(self): self._r("stop_process")
    def all_stop(self): self._r("all_stop")
    def load_recipe_file(self, path): self._r("load_recipe_file", path); self._fire("load", path)
    def csv_rows(self): return self.rows
    def csv_file_path(self): return self.path
    def process_active(self): return self.active
    def heater_pending(self): return self.pending
    def heater_recipe_running(self): return self.recipe_running

    def exec_heater_command(self, name, args):
        self._r("exec_heater_command", name, args)
        if name in self.heater_names:
            self._fire(name)
            return True
        return False

    def _fire(self, key, *a):
        fn = self.on_exec.get(key)
        if fn:
            fn(*a)

    def names(self):
        return [c[0] for c in self.calls]


def test_fake_host_covers_protocol():
    wanted = {n for n in vars(ErpCommandHost) if not n.startswith("_")}
    assert wanted <= set(vars(FakeHost))


# ───────────────────────── exec_one ─────────────────────────
def test_plc_button_on_off_and_missing():
    h = FakeHost()
    h.widgets = {"MV_button": Widget(h, "MV_button")}
    r = ErpCommandRunner(h)
    r.exec_one({"command": "MV_button", "args": {"on": 1}})
    r.exec_one({"command": "MV_button"})                           # args 없음 → False
    assert h.calls == [["w.setChecked", "MV_button", True], ["w.setChecked", "MV_button", False]]
    with pytest.raises(RuntimeError, match="^버튼 없음: Vent_button$"):
        r.exec_one({"command": "Vent_button", "args": {"on": True}})
    assert "Door_Button" in PLC_BUTTONS and "ION_button" in PLC_BUTTONS and len(PLC_BUTTONS) == 14


def test_process_start_builds_inputs_and_starts_without_touching_widgets():
    """B1: 원격 수동 시작은 입력칸에 먼저 쓰지 않는다 — 인자로 입력값을 만들어 host.start_manual 로 넘긴다."""
    h = FakeHost(rf_cal=("6.79", "1.0395"))
    h.widgets = {"G1_checkbox": Widget(h, "G1_checkbox"), "G1_edit": Widget(h, "G1_edit")}
    ErpCommandRunner(h).exec_one({"command": "PROCESS_START",
                                  "args": {"useG1": True, "g1": "CeO2", "useAr": 1, "arFlow": 20}})
    assert h.names() == ["current_rf_cal", "start_manual"]
    inp = h.calls[-1][1]
    assert isinstance(inp, ManualInputs)
    assert (inp.use_g1, inp.g1_name_text, inp.use_ar, inp.ar_flow_text) == (True, "CeO2", True, "20")
    assert (inp.offset_text, inp.param_text) == ("6.79", "1.0395")


def test_manual_inputs_from_args_missing_keys_none_and_calibration():
    inp = manual_inputs_from_args({}, "6.79", "1.0395")
    assert all(getattr(inp, f) is False for f in ("use_g1", "use_g2", "use_ar", "use_o2", "use_rf",
                                                  "use_rf_pulse", "use_dc", "use_dc_delay"))
    assert all(getattr(inp, f) == "" for f in ("g1_name_text", "g2_name_text", "ar_flow_text", "o2_flow_text",
                                               "working_pressure_text", "rf_power_text", "rfp_power_text",
                                               "rfp_freq_text", "rfp_duty_text", "dc_power_text",
                                               "shutter_delay_text", "process_time_text"))
    assert (inp.offset_text, inp.param_text) == ("6.79", "1.0395")
    inp = manual_inputs_from_args({"useRf": 0, "useDc": "yes", "rfPower": None, "dcPower": 100.5,
                                   "offset": " ", "param": 2}, "6.79", "1.0395")
    assert (inp.use_rf, inp.use_dc, inp.rf_power_text, inp.dc_power_text) == (False, True, "", "100.5")
    assert (inp.offset_text, inp.param_text) == ("6.79", "2")     # 빈칸 offset → 장비 값, param → 덮어씀
    assert manual_inputs_from_args(None, "a", "b").offset_text == "a"


def test_stop_and_all_stop():
    h = FakeHost()
    r = ErpCommandRunner(h)
    r.exec_one({"command": "PROCESS_STOP"})
    r.exec_one({"command": "ALL_STOP"})
    assert h.calls == [["stop_process"], ["all_stop"]]


@pytest.fixture
def systemp(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    return tmp_path


def test_recipe_process_run_writes_csv_records_path_and_loads(systemp):
    h = FakeHost()
    c = {"command": "RECIPE_PROCESS_RUN", "args": {"rows": [{"Process_name": "W1", "Ar": "1", "x": "무시"},
                                                             {"Process_name": "W2"}]}}
    ErpCommandRunner(h).exec_one(c)
    path = os.path.join(str(systemp), "vanam_recipe", "process_web.csv")
    assert c["_csv_path"] == path and h.calls == [["load_recipe_file", path]]
    raw = open(path, "rb").read()
    assert raw.startswith(b"\xef\xbb\xbf")                           # utf-8-sig
    rows = list(csv.DictReader(open(path, encoding="utf-8-sig", newline="")))
    assert list(rows[0]) == PROCESS_RECIPE_COLS
    assert rows[0]["Process_name"] == "W1" and rows[0]["Ar"] == "1" and rows[1]["Ar"] == ""


def test_recipe_process_run_without_rows_writes_nothing(systemp):
    h = FakeHost()
    for args in ({"rows": []}, {}, None):
        with pytest.raises(RuntimeError, match="^레시피 행이 없습니다$"):
            ErpCommandRunner(h).exec_one({"command": "RECIPE_PROCESS_RUN", "args": args})
    assert not os.path.exists(os.path.join(str(systemp), "vanam_recipe")) and h.calls == []


def test_write_recipe_csv_heater_cols(systemp):
    p = write_recipe_csv([{"step": 1, "target_c": 300}], HEATER_RECIPE_COLS, "heater_web.csv")
    assert p == os.path.join(str(systemp), "vanam_recipe", "heater_web.csv")
    assert open(p, encoding="utf-8-sig").read().splitlines()[0] == ",".join(HEATER_RECIPE_COLS)


def test_recipe_process_start():
    h = FakeHost(rows=[])
    with pytest.raises(RuntimeError, match="^적재된 레시피가 없습니다. 먼저 레시피를 적재하세요.$"):
        ErpCommandRunner(h).exec_one({"command": "RECIPE_PROCESS_START"})
    h.rows = [{"Process_name": "S1"}]
    ErpCommandRunner(h).exec_one({"command": "RECIPE_PROCESS_START"})
    assert h.calls == [["start_process"]]


def test_heater_commands_are_delegated():
    h = FakeHost(heater_names={"HEATER_SV"})
    r = ErpCommandRunner(h)
    r.exec_one({"command": "HEATER_SV", "args": {"value": 300}})
    assert h.calls == [["exec_heater_command", "HEATER_SV", {"value": 300}]]
    with pytest.raises(RuntimeError, match="^허용되지 않은 명령: NOPE$"):
        r.exec_one({"command": "NOPE", "args": {"a": 1}})
    assert h.calls[-1] == ["exec_heater_command", "NOPE", {"a": 1}]


# ───────────────────────── drain ─────────────────────────
def test_drain_rejected_then_resumed_each_once():
    h = FakeHost(rejected=True)
    r = ErpCommandRunner(h)
    r.drain(); r.drain()
    h.rejected = False
    r.drain(); r.drain()
    logs = [c for c in h.calls if c[0] == "log"]
    assert logs == [["log", "WARN", "[ERP] 다른 챔버K 프로그램이 ERP 에 연결되어 있어 보고를 잠시 멈춥니다. "
                                  "60초마다 재연결을 시도합니다."],
                    ["log", "정보", "[ERP] 보고를 재개했습니다."]]
    assert h.calls[:4] == [["erp_rejected"], ["rejected_shown"], ["set_rejected_shown", True], ["log", "WARN", logs[0][2]]]


def test_drain_nothing_to_do():
    h = FakeHost()
    ErpCommandRunner(h).drain()
    # if (rejected and not shown) → elif (not rejected and shown): 옮기기 전 식의 평가 순서 그대로
    assert h.names() == ["erp_rejected", "erp_rejected", "rejected_shown", "erp_pop_commands"]


def test_drain_modal_rejects_all_without_running():
    h = FakeHost(modal=True, cmds=[{"id": 1, "command": "ALL_STOP"}, {"id": 2, "command": "PROCESS_STOP"}])
    ErpCommandRunner(h).drain()
    msg = "장비에 확인 대화상자가 열려 있습니다. 현장에서 닫아주세요."
    assert h.calls[-3:] == [["erp_cmd_result", 1, False, msg], ["erp_cmd_result", 2, False, msg],
                            ["log", "WARN", "[원격] 대화상자가 열려 있어 명령을 거부했습니다"]]
    assert "all_stop" not in h.names() and "begin_remote" not in h.names()


def test_drain_success_reports_true():
    h = FakeHost(cmds=[{"id": 7, "command": "PROCESS_STOP"}])
    ErpCommandRunner(h).drain()
    assert h.calls[-5:] == [["begin_remote"], ["stop_process"], ["end_remote"],
                            ["log", "정보", "[원격] PROCESS_STOP 실행"], ["erp_cmd_result", 7, True]]


def test_drain_alert_reason_wins_and_info_is_logged():
    h = FakeHost(cmds=[{"id": 8, "command": "PROCESS_START", "args": {}}], active=True)
    h.on_exec["start"] = lambda: (h.alerts.append(("information", "안내", "a\n b")),
                                  h.alerts.append(("warning", "공정 시작 불가", "메인밸브가\n닫혀 있습니다")))
    ErpCommandRunner(h).drain()
    assert h.calls[-3:] == [["log", "정보", "[원격] PROCESS_START 안내 — 안내: a b"],
                            ["log", "ERROR", "[원격] PROCESS_START 실패: 공정 시작 불가: 메인밸브가 닫혀 있습니다"],
                            ["erp_cmd_result", 8, False, "공정 시작 불가: 메인밸브가 닫혀 있습니다"]]


def test_drain_silent_failure_uses_note_text_when_present():
    h = FakeHost(cmds=[{"id": 9, "command": "RECIPE_PROCESS_START"}], rows=[{"x": 1}], active=False)
    h.on_exec["start"] = lambda: h.notes.append(("critical", "CSV 레시피 오류", "1번째 행\n 오류"))
    ErpCommandRunner(h).drain()
    assert h.calls[-1] == ["erp_cmd_result", 9, False, "CSV 레시피 오류: 1번째 행 오류"]
    h = FakeHost(cmds=[{"id": 10, "command": "PROCESS_START", "args": {}}], active=False)
    ErpCommandRunner(h).drain()
    assert h.calls[-1] == ["erp_cmd_result", 10, False, "공정이 시작되지 않았습니다 (장비 로그 확인)"]


def test_drain_exception_ends_remote_and_continues():
    h = FakeHost(cmds=[{"id": 1, "command": "BAD"}, {"id": 2, "command": "ALL_STOP"}])
    ErpCommandRunner(h).drain()
    i = h.names().index("exec_heater_command")
    assert h.names()[i - 1:i + 2] == ["begin_remote", "exec_heater_command", "end_remote"]
    assert ["erp_cmd_result", 1, False, "허용되지 않은 명령: BAD"] in h.calls
    assert h.calls[-1] == ["erp_cmd_result", 2, True] and h.remote is False


def test_drain_swallows_host_errors():
    class Boom(FakeHost):
        def erp_pop_commands(self):
            raise RuntimeError("리포터 죽음")
    ErpCommandRunner(Boom()).drain()                       # 예외가 밖으로 나오지 않는다


# ───────────────────────── silent_failure / alert_reason ─────────────────────────
def test_silent_failure_branches():
    r = ErpCommandRunner(FakeHost(active=False))
    assert r.silent_failure("PROCESS_START", {}) == "공정이 시작되지 않았습니다 (장비 로그 확인)"
    assert ErpCommandRunner(FakeHost(active=True)).silent_failure("RECIPE_PROCESS_START", {}) == ""
    assert r.silent_failure("RECIPE_HEATER_RUN", {}) == "히터 레시피를 시작하지 못했습니다 (장비 로그 확인)"
    assert ErpCommandRunner(FakeHost(pending=("recipe_start", None))).silent_failure("RECIPE_HEATER_RUN", {}) == ""
    assert ErpCommandRunner(FakeHost(recipe_running=True)).silent_failure("RECIPE_HEATER_RUN", {}) == ""
    h = FakeHost(rows=[{"a": 1}], path="C:/t/process_web.csv")
    assert ErpCommandRunner(h).silent_failure("RECIPE_PROCESS_RUN", {"_csv_path": "C:/t/process_web.csv"}) == ""
    assert ErpCommandRunner(h).silent_failure("RECIPE_PROCESS_RUN", {"_csv_path": "C:/t/other.csv"}) == \
        "레시피를 적재하지 못했습니다 (장비 로그 확인)"
    assert ErpCommandRunner(FakeHost(rows=[])).silent_failure("RECIPE_PROCESS_RUN", {}) == \
        "레시피를 적재하지 못했습니다 (장비 로그 확인)"
    assert r.silent_failure("MV_button", {}) == ""


def test_alert_reason():
    assert alert_reason([]) == ""
    assert alert_reason([("information", "안내", "무시")]) == ""
    assert alert_reason([("warning", "경고", "이미\n  공정 중"), ("critical", "", "치명  오류")]) == \
        "경고: 이미 공정 중 / 치명 오류"


# ───────────────────────── 정적 검사 ─────────────────────────
def test_integrations_import_rules():
    files = glob.glob(os.path.join(ROOT, "integrations", "*.py"))
    assert any(f.endswith("erp_commands.py") for f in files)
    bad = []
    for f in files:
        tree = ast.parse(open(f, encoding="utf-8").read(), f)
        for node in ast.walk(tree):
            mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                    else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
            for m in mods:
                if m.split(".")[0] in ("PyQt6", "UI", "main") or m == "lib.logger" or m.startswith("lib.logger."):
                    bad.append((os.path.basename(f), m))
            if isinstance(node, ast.ImportFrom) and node.module == "tempfile":
                bad.append((os.path.basename(f), "from tempfile import …"))     # tempfile 은 모듈째
    assert bad == []
