# -*- coding: utf-8 -*-
"""골든(동작 고정) — 공정·원격 명령·ERP 보고.

리팩토링 단계마다 "동작이 한 글자도 바뀌지 않았는지"를 증명한다. 기대값은 지금 코드가 실제로 내는 결과다
(이상해 보여도 그대로 고정한다. 동작을 바꾸는 단계에서 골든을 명시적으로 갱신한다).

  갱신:  CHK_UPDATE_GOLDEN=1 python -m pytest tests/test_golden_process.py
  저장:  tests/golden/<시나리오>.json

하니스
  - 오프스크린 MainDialog(test_main_heater.win) 한 개를 재사용한다. 공정 컨트롤러는 스레드로 돌리지 않는다.
  - MainDialog 가 내보내는 요청 신호(request_process_start/stop, 비상정지, PLC 포트, 히터 RUN 등)는 가로채 기록만 한다.
    컨트롤러가 내는 신호(stage_monitor, tick, finished, critical_error, connection_failed …)는 테스트가 순서대로 흉내 낸다.
  - 실제 이벤트 루프는 돌리지 않는다. QTimer.singleShot 은 가짜 큐에 쌓였다가 flush() 때 순서대로 실행되고,
    QMetaObject.invokeMethod(장치 스레드 호출)는 기록만, QElapsedTimer 는 가짜 시계를 읽는다.
  - 채팅·ERP·로그·스테이지·경고창·CSV 행·요청 신호는 MagicMock 하나(sink)에 모여 호출 순서 그대로 한 기록이 된다.
  - 컨트롤러가 그 params 로 만드는 스텝 목록은 스레드 없이 start_process_flow(→ _build_steps)를 직접 불러 얻는다.
  - 시각·임시 경로·주소처럼 매번 바뀌는 값은 치환한다.
"""
import copy
import difflib
import enum
import json
import os
import re
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from PyQt6.QtCore import QTimer
from PyQt6.QtGui import QCloseEvent
from PyQt6.QtWidgets import QMessageBox as _RealMB

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)

import main as MAIN
import controller.process_controller as PC

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"
UPDATE = os.environ.get("CHK_UPDATE_GOLDEN") == "1"

SB = _RealMB.StandardButton

# MainDialog → 컨트롤러/장치로 나가는 요청 신호와 원래 받는 쪽(teardown 에서 되돌린다)
_OUT_SIGNALS = {
    "request_process_start": lambda w: w.process_controller.start_process_flow,
    "request_process_stop": lambda w: w.process_controller.stop_process,
    "request_plc_emergency_stop": lambda w: w.plc_controller.on_emergency_stop,
    "clear_plc_fault": lambda w: w.plc_controller.clear_fault_latch,
    "request_plc_port_update": lambda w: w.plc_controller.update_port_state,
    "request_heater_run": lambda w: w.plc_controller.set_heater_run,
    "request_heater_target": lambda w: w.plc_controller.set_heater_target,
}

_CSV_COLS = ["Process_name", "Ar", "Ar_flow", "O2", "O2_flow",
             "working_pressure", "process_time", "shutter_delay",
             "use_rf_power", "rf_power", "use_dc_power", "dc_power",
             "use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty",
             "use_dc_delay", "use_heater", "heater_temp", "heater_ramp",
             "gun1", "gun2", "G1 Target", "G2 Target"]


def csv_row(name="S1", **over):
    r = {"Process_name": name, "Ar": "1", "Ar_flow": "20", "O2": "0", "O2_flow": "0",
         "working_pressure": "5", "process_time": "1", "shutter_delay": "1",
         "use_rf_power": "0", "rf_power": "0", "use_dc_power": "1", "dc_power": "100",
         "use_rf_pulse": "0", "rf_pulse_power": "0", "rf_pulse_freq": "", "rf_pulse_duty": "",
         "use_dc_delay": "0", "use_heater": "0", "heater_temp": "0", "heater_ramp": "0",
         "gun1": "1", "gun2": "0", "G1 Target": "CeO2", "G2 Target": ""}
    r.update(over)
    return r


def delay_row(text):
    return {c: "" for c in _CSV_COLS} | {"Process_name": text}


# ───────────────────────── 가짜 Qt 조각 ─────────────────────────
class _MsgBox:
    """QMessageBox 대역 — 창 대신 sink.msgbox.<종류>(제목, 본문) 로 기록. question 의 답은 answer."""
    StandardButton = SB

    def __init__(self, h):
        self._h = h
        self.answer = SB.No

    def _rec(self, kind, title, text):
        getattr(self._h.sink.msgbox, kind)(title, text)

    def warning(self, parent, title, text, *a, **k):
        self._rec("warning", title, text); return SB.Ok

    def critical(self, parent, title, text, *a, **k):
        self._rec("critical", title, text); return SB.Ok

    def information(self, parent, title, text, *a, **k):
        self._rec("information", title, text); return SB.Ok

    def question(self, parent, title, text, *a, **k):
        self._rec("question", title, text); return self.answer


class _FakeElapsed:
    """QElapsedTimer 대역 — 하니스 시계(ms)를 읽는다."""
    harness = None

    def __init__(self):
        self._t0 = None

    def start(self):
        self._t0 = self.harness.clock_ms

    def elapsed(self):
        return self.harness.clock_ms - (self._t0 or 0)


class _FakeQMetaObject:
    harness = None

    @staticmethod
    def invokeMethod(obj, name, *a, **k):
        _FakeQMetaObject.harness.sink.invoke(type(obj).__name__, str(name))
        return True


class _FakeThread:
    def __init__(self, h, name):
        self._h, self._name = h, name

    def objectName(self):
        return self._name

    def quit(self):
        self._h.sink.thread.quit(self._name)

    def wait(self, ms=None):
        self._h.sink.thread.wait(self._name, ms)
        return True


# ───────────────────────── 하니스 ─────────────────────────
class Harness:
    def __init__(self, w, tmp_path, monkeypatch):
        self.w = w
        self.tmp = tmp_path
        self.mp = monkeypatch
        self.sink = MagicMock(name="sink")
        self.sink.erp.rejected = False
        self.sink.erp.pop_commands.return_value = []
        self.sink.csv_row.return_value = True
        self.clock_ms = 0
        self.pending = []           # 가짜 singleShot 큐 [(ms, fn)]
        self.mv = (True, True)      # read_main_valve_state
        self.msgbox = _MsgBox(self)
        self._restore = []

    # ---- 설치 / 해제 ----
    def install(self):
        w, mp, sink = self.w, self.mp, self.sink
        h = self
        w.chat_chk = sink.chat
        w.erp = sink.erp
        mp.setattr(MAIN, "log_message_to_monitor", sink.log)
        mp.setattr(MAIN, "append_chk_csv_row", sink.csv_row)
        mp.setattr(MAIN, "set_process_log_file", sink.logfile.set)
        mp.setattr(MAIN, "clear_process_log_file", sink.logfile.clear)
        mp.setattr(MAIN, "clear_heater_log_file", sink.logfile.clear_heater)
        mp.setattr(MAIN, "QMessageBox", self.msgbox)
        _FakeElapsed.harness = self
        _FakeQMetaObject.harness = self
        mp.setattr(MAIN, "QElapsedTimer", _FakeElapsed)
        mp.setattr(MAIN, "QMetaObject", _FakeQMetaObject)

        class _QT(QTimer):
            @staticmethod
            def singleShot(ms, *rest):
                fn = rest[-1]
                sink.timer.singleShot(ms, getattr(fn, "__name__", type(fn).__name__))
                h.pending.append((ms, fn))
        mp.setattr(MAIN, "QTimer", _QT)
        mp.setattr(w.plc_controller, "read_main_valve_state", lambda: self.mv)
        mp.setattr(tempfile, "gettempdir", lambda: str(self.tmp / "systemp"))
        # 컨트롤러 스텝 목록: 스레드 없이 실제 start_process_flow 를 부른다(장치 연결만 통과시킨다)
        mp.setattr(PC, "_invoke_connect", lambda obj, name: True)

        for sig_name, orig in _OUT_SIGNALS.items():
            sig = getattr(w, sig_name)
            try:
                sig.disconnect()
            except TypeError:
                pass
            if sig_name == "request_process_start":
                slot = self._on_process_start
            else:
                slot = getattr(sink.sig, sig_name)
            sig.connect(slot)
            self._restore.append((sig, orig(w)))

        self._stage_slot = lambda: sink.stage(w.ui.stage_monitor.toPlainText())
        w.ui.stage_monitor.textChanged.connect(self._stage_slot)

    def uninstall(self):
        w = self.w
        try:
            w.ui.stage_monitor.textChanged.disconnect(self._stage_slot)
        except TypeError:
            pass
        for sig, orig in self._restore:
            try:
                sig.disconnect()
            except TypeError:
                pass
            sig.connect(orig)

    # ---- 기록 ----
    def _on_process_start(self, params):
        p = copy.deepcopy(dict(params))
        self.sink.sig.request_process_start(p)
        self.sink.steps(build_steps(p))

    def check(self, label, snapshot=False):
        w, ui = self.w, self.w.ui
        self.sink.check(label, {
            "stage": ui.stage_monitor.toPlainText(),
            "start_enabled": ui.Sputter_Start_Button.isEnabled(),
            "stop_enabled": ui.Sputter_Stop_Button.isEnabled(),
            "select_csv_enabled": ui.select_csv_button.isEnabled(),
            "process_running": bool(w.process_running),
            "csv_mode": bool(w.csv_mode),
            "csv_index": w.csv_index,
            "csv_rows": len(w.csv_rows or []),
            "csv_delay_active": bool(w._csv_delay_active),
            "proc_origin": w._proc_origin,
            "current_process_name": w.current_process_name,
        })
        if snapshot:
            w._erp_snap_timer.timeout.emit()          # → sink.erp.update_state(state)

    # ---- 흉내 ----
    def flush(self):
        """가짜 singleShot 큐를 비운다(지연 순, 같은 지연은 등록 순)."""
        while self.pending:
            self.pending.sort(key=lambda t: t[0])
            _ms, fn = self.pending.pop(0)
            fn()

    def remote(self, command, args=None, cid=1):
        self.sink.erp.pop_commands.return_value = [{"id": cid, "command": command, "args": args or {}}]
        self.w._erp_cmd_timer.timeout.emit()
        self.sink.erp.pop_commands.return_value = []
        self.flush()

    def advance(self, sec):
        """가짜 시계를 1초씩 움직이며 CSV 딜레이 틱을 흉내 낸다."""
        for _ in range(int(sec)):
            self.clock_ms += 1000
            self.w._on_csv_delay_tick()
        self.flush()

    def ctrl_run(self, steps=None):
        """컨트롤러가 공정을 진행하는 동안 내는 신호 — 시작 알림, 스테이지 몇 개, 셔터/공정 틱, 계측값."""
        w = self.w
        pc = w.process_controller
        pc.status_message.emit("정보", "Sputtering 공정을 시작합니다.")
        st = steps if steps is not None else self.last_steps()
        n = len(st)
        pick = [0] + [i for i, s in enumerate(st) if s["timer_purpose"] in ("shutter", "process")]
        for i in pick:
            pc.stage_monitor.emit(f"[{i + 1}/{n}] {st[i]['message']}")
            if st[i]["timer_purpose"] == "shutter":
                pc.shutter_delay_tick.emit(st[i]["duration_sec"])
                w.mfc_controller.update_flow.emit("Ar", 20.4)
                w.mfc_controller.update_pressure.emit("5.1")
                w.dcpower_controller.update_dc_status_display.emit(100.2, 351.0, 0.285)
                pc.shutter_delay_tick.emit(0)
            elif st[i]["timer_purpose"] == "process":
                pc.process_time_tick.emit(st[i]["duration_sec"])
                w.mfc_controller.update_flow.emit("Ar", 19.8)
                w.mfc_controller.update_pressure.emit("4.9")
                w.dcpower_controller.update_dc_status_display.emit(99.8, 349.0, 0.286)
                self.check("공정 중", snapshot=True)
                pc.process_time_tick.emit(0)

    def ctrl_finish(self):
        """컨트롤러 종료 시퀀스 → finished."""
        pc = self.w.process_controller
        pc.status_message.emit("정보", "종료 시퀀스를 실행합니다.")
        pc.stage_monitor.emit("M.S. close...")
        pc.finished.emit()
        self.flush()

    def last_steps(self):
        for c in reversed(self.sink.mock_calls):
            if c[0] == "steps":
                return c[1][0]
        return []

    def write_csv(self, rows, name="recipe.csv"):
        import csv
        p = self.tmp / name
        with open(p, "w", encoding="utf-8-sig", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=_CSV_COLS)
            wr.writeheader()
            for r in rows:
                wr.writerow({c: r.get(c, "") for c in _CSV_COLS})
        return str(p)

    def set_manual_ui(self, **over):
        ui = self.w.ui
        ui.Ar_gas_radio.setChecked(True); ui.O2_gas_radio.setChecked(False)
        ui.Ar_flow_edit.setPlainText(over.get("ar", "20"))
        ui.working_pressure_edit.setPlainText(over.get("wp", "5"))
        ui.dc_power_checkbox.setChecked(True); ui.DC_power_edit.setPlainText(over.get("dc", "100"))
        ui.rf_power_checkbox.setChecked(False); ui.rf_pulse_checkbox.setChecked(False)
        ui.Shutter_delay_edit.setPlainText(over.get("sd", "1"))
        ui.process_time_edit.setPlainText(over.get("pt", "1"))
        ui.G1_checkbox.setChecked(True); ui.G1_edit.setPlainText("CeO2"); ui.G2_checkbox.setChecked(False)

    # ---- 결과 ----
    def result(self):
        out = []
        for name, args, kwargs in self.sink.mock_calls:
            if "__" in name:            # __bool__ 등 매직 메서드는 뺀다
                continue
            e = [name, self.norm(list(args))]
            if kwargs:
                e.append(self.norm(dict(kwargs)))
            out.append(e)
        return {"timeline": out}

    def norm(self, v):
        if isinstance(v, dict):
            return {str(k): self.norm(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [self.norm(x) for x in v]
        if isinstance(v, enum.Enum):
            return self.scrub(str(v.value))
        if v is None or isinstance(v, (bool, int, float)):
            return v
        if isinstance(v, (str, Path)):
            return self.scrub(str(v))
        return "<" + type(v).__name__ + ">"

    _TS = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?|\d{8}_\d{6}")
    _ADDR = re.compile(r"0x[0-9A-Fa-f]{6,}")
    _TMP_PATH = re.compile(r"<TMP>[^\s\"']*")

    def scrub(self, s):
        for base in {str(self.tmp), str(self.tmp).replace("\\", "/")}:
            s = s.replace(base, "<TMP>")
        s = self._TMP_PATH.sub(lambda m: m.group(0).replace("\\", "/"), s)   # OS 구분자 차이 제거
        s = self._TS.sub("<TS>", s)
        return self._ADDR.sub("<ADDR>", s)


def build_steps(params):
    """컨트롤러가 이 params 로 만드는 스텝 목록 — 스레드 없이 실제 start_process_flow(→ _build_steps)."""
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    c._is_connected = lambda obj: True
    c._invoke_self = lambda name: None
    c._next_step = lambda: None
    c.start_process_flow(dict(params))
    out = [{"action": s.action.value, "message": s.message,
            "params": (list(s.params) if isinstance(s.params, tuple) else s.params),
            "value": s.value, "duration_sec": s.duration_sec,
            "timer_purpose": s.timer_purpose, "polling": s.polling} for s in c._steps]
    c.deleteLater()
    return out


def _reset_window(w):
    """시나리오마다 같은 출발점 — 단독 실행과 전체 실행의 결과가 같아야 한다."""
    w._is_closing = False
    w._csv_dialog_open = False
    w._stop_csv_delay_timer()
    w._csv_delay_active = False
    w._csv_delay_total_sec = 0; w._csv_delay_remaining_sec = 0; w._csv_delay_name = ""
    w.csv_file_path = None; w.csv_rows = []; w.csv_index = -1
    w.csv_mode = False; w.csv_cancelled = False
    w.process_running = False
    w.current_process_name = ""; w._last_params = None
    w._reset_chk_stats(); w._chk_process_ok = False
    w._chat_reset_run_state()
    w._finish_handled = True; w._erp_run_ended = True
    w._proc_origin = "local"; w._heater_origin = "local"
    w._remote_exec = False; w._remote_alerts = []; w._remote_notes = []
    w._erp_rejected_shown = False; w._erp_snap_err = False
    w._erp_valves = {}; w._erp_indicators = {}; w._erp_meas = {}
    w._plc_bits.clear(); w._mv_itl_timer.stop()
    w._process_heater_claimed = False
    w._plc_link_up = False
    w._on_plc_link(True)
    w.ui.stage_monitor.setPlainText("")
    w._reset_process_ui_fields()
    w.ui.Sputter_Start_Button.setEnabled(True)
    w.ui.Sputter_Stop_Button.setEnabled(False)
    w.ui.select_csv_button.setEnabled(True)


@pytest.fixture
def H(fresh, tmp_path, monkeypatch):
    w = fresh
    _reset_window(w)
    h = Harness(w, tmp_path, monkeypatch)
    h.install()
    yield h
    h.uninstall()
    w.chat_chk = MagicMock(); w.erp = MagicMock()
    _reset_window(w)
    w._heater_ui_timer.start()


# ───────────────────────── 시나리오 ─────────────────────────
def s01_manual_complete(h):
    h.set_manual_ui()
    h.check("시작 전", snapshot=True)
    h.w.ui.Sputter_Start_Button.click()
    h.flush()
    h.check("시작 직후", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s02_manual_user_stop(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w.ui.Sputter_Stop_Button.click()
    h.flush()
    h.check("STOP 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s03_manual_all_stop(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w.ui.ALL_STOP_button.click()
    h.flush()
    h.check("ALL STOP 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def _critical(h):
    pc = h.w.process_controller
    pc.critical_error.emit("Ar 유량 이탈: 설정 20.0 sccm / 실측 12.3 sccm (허용 ±10%)")
    h.flush()
    h.check("critical_error 직후", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s04a_critical_error_local(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    _critical(h)


_REMOTE_ARGS = {"useG1": True, "g1": "CeO2", "useG2": False, "useAr": True, "arFlow": 20,
                "useO2": False, "workingPressure": 5, "useRf": False, "useDc": True, "dcPower": 100,
                "shutterDelay": 1, "processTime": 1}


def s04b_critical_error_erp(h):
    h.remote("PROCESS_START", dict(_REMOTE_ARGS))
    h.ctrl_run()
    _critical(h)


def s05_connection_failed(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.w.process_controller.connection_failed.emit("DC Power 장치에 연결할 수 없습니다.")
    h.flush()
    h.check("연결 실패 후", snapshot=True)


def _load_local(h, rows):
    h.w._start_csv_process_from_path(h.write_csv(rows))
    h.flush()
    h.check("적재 후", snapshot=True)


def s06_csv_two_steps(h):
    _load_local(h, [csv_row("S1"), csv_row("S2", dc_power="150", working_pressure="3")])
    h.w.ui.Sputter_Start_Button.click()
    h.check("STEP 1 시작", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("STEP 1 종료 → STEP 2", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s07_csv_with_delay(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 3s"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("딜레이 시작", snapshot=True)
    h.advance(1)
    h.check("딜레이 1초 뒤", snapshot=True)
    h.advance(2)
    h.check("딜레이 끝 → STEP 3", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s08a_csv_stop_during_delay(h):
    _load_local(h, [csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.advance(2)
    h.w.ui.Sputter_Stop_Button.click()
    h.flush()
    h.check("딜레이 중 STOP 뒤", snapshot=True)
    h.w.process_controller.finished.emit()          # 늦게 온 finished — 무시되는지
    h.flush()
    h.check("늦은 finished 뒤")


def s08b_csv_stop_during_step(h):
    _load_local(h, [csv_row("S1"), csv_row("S2")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.w.ui.Sputter_Stop_Button.click()
    h.flush()
    h.check("STEP 1 중 STOP 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s09_csv_row2_value_error(h):
    _load_local(h, [csv_row("S1"), csv_row("S2", working_pressure="abc")])
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("2번째 행 오류 뒤", snapshot=True)


def s10a_remote_process_start_ok(h):
    h.remote("PROCESS_START", dict(_REMOTE_ARGS))
    h.check("원격 시작 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s10b_remote_process_start_mv_closed(h):
    h.mv = (False, True)
    h.remote("PROCESS_START", dict(_REMOTE_ARGS))
    h.check("거부 뒤", snapshot=True)


def s11a_remote_recipe_run_start_ok(h):
    h.remote("RECIPE_PROCESS_RUN", {"rows": [csv_row("W1"), csv_row("W2", dc_power="120")]}, cid=1)
    h.check("원격 적재 뒤", snapshot=True)
    h.remote("RECIPE_PROCESS_START", {}, cid=2)
    h.check("원격 시작 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s11b_remote_recipe_bad_file(h):
    h.remote("RECIPE_PROCESS_RUN", {"rows": [csv_row("W1", working_pressure="abc")]}, cid=1)
    h.check("원격 적재 뒤", snapshot=True)
    h.remote("RECIPE_PROCESS_START", {}, cid=2)
    h.check("원격 시작 뒤", snapshot=True)


def s12a_remote_process_stop(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.remote("PROCESS_STOP")
    h.check("원격 STOP 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s12b_remote_all_stop(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.remote("ALL_STOP")
    h.check("원격 ALL STOP 뒤", snapshot=True)
    h.ctrl_finish()
    h.check("종료 후", snapshot=True)


def s13a_loaded_recipe_local_start(h):
    _load_local(h, [csv_row("R1", dc_power="100")])
    h.set_manual_ui(dc="150", wp="3")               # 화면에 다른 수동값을 넣어도
    h.w.ui.Sputter_Start_Button.click()             # 적재된 레시피가 돈다(지금 동작)
    h.check("시작 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def s13b_loaded_recipe_remote_process_start(h):
    _load_local(h, [csv_row("R1", dc_power="100")])
    args = dict(_REMOTE_ARGS, dcPower=150, workingPressure=3)
    h.remote("PROCESS_START", args)                 # 레시피가 적재돼 있으면 원격 수동 시작은 거부(B1) — 적재·입력칸 그대로
    h.check("거부 뒤", snapshot=True)


def s14_plc_link_down_up(h):
    w = h.w

    def link_state(label):
        h.sink.link(label, {
            "title_link_down": w.windowTitle().endswith(w.PLC_LINK_DOWN_TITLE),
            "widgets": [[x.objectName(), x.isEnabled(), x.isChecked()] for x in w._plc_link_widgets()],
        })
    w.ui.MV_button.blockSignals(True); w.ui.MV_button.setChecked(True); w.ui.MV_button.blockSignals(False)
    link_state("링크 업(출발)")
    h.check("링크 업(출발)", snapshot=True)
    w._on_plc_link(False)
    h.flush()
    link_state("링크 다운")
    h.check("링크 다운", snapshot=True)
    w._on_plc_link(True)
    h.flush()
    link_state("링크 업(복구)")
    h.check("링크 업(복구)", snapshot=True)


def s15a_close_during_process(h):
    h.set_manual_ui()
    h.w.ui.Sputter_Start_Button.click()
    ev = QCloseEvent()
    h.w.closeEvent(ev)
    h.flush()
    h.sink.close_event_accepted(ev.isAccepted())
    h.check("종료 요청 뒤")


def s15b_close_idle_yes(h):
    w = h.w
    for attr, name in (("process_thread", "ProcessThread"), ("plc_thread", "PLCThread"),
                       ("mfc_thread", "MFCThread"), ("dcpower_thread", "DCPowerThread"),
                       ("rfpower_thread", "RFPowerThread"), ("rfpulse_thread", "RFPulseThread")):
        h.mp.setattr(w, attr, _FakeThread(h, name))
    # 히터 쪽은 히터 단계에서 따로 고정한다 — 여기서는 호출 순서만 남기고 상태를 남기지 않는다(다음 시나리오에 새지 않게)
    h.mp.setattr(w.heater_recipe, "stop", h.sink.heater.recipe_stop)
    h.mp.setattr(w.heater_ramp, "stop", h.sink.heater.ramp_stop)
    h.msgbox.answer = SB.Yes
    ev = QCloseEvent()
    w.closeEvent(ev)
    h.flush()
    h.sink.close_event_accepted(ev.isAccepted())
    h.check("종료 확인 Yes 뒤")


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


# ───────────────────────── 비교 ─────────────────────────
def _dump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=1) + "\n"


def check_golden(name, actual):
    path = GOLDEN_DIR / f"{name}.json"
    text = _dump(actual)
    if UPDATE:
        GOLDEN_DIR.mkdir(exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        return
    if not path.exists():
        pytest.fail(f"골든 없음: {path.name} — CHK_UPDATE_GOLDEN=1 로 만든다")
    with open(path, encoding="utf-8") as f:
        expected = f.read()
    if expected != text:
        diff = list(difflib.unified_diff(expected.splitlines(), text.splitlines(),
                                         f"golden/{path.name}", "현재", lineterm="", n=4))
        shown = "\n".join(diff[:120]) + ("\n… (%d줄 더)" % (len(diff) - 120) if len(diff) > 120 else "")
        pytest.fail(f"골든과 다름: {path.name}\n{shown}", pytrace=False)


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
