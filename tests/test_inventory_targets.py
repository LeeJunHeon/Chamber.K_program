# -*- coding: utf-8 -*-
"""재고관리 장착 타겟으로 빈 타겟 이름 채우기 — integrations/inventory_targets 단위 시험과 실제 창 공정 흐름 시험.
네트워크는 쓰지 않는다(fetch 는 가짜 함수, conftest 가 urlopen 을 막는다)."""
import csv
import os
from unittest.mock import MagicMock

import pytest

import integrations.inventory_targets as IT
from integrations.inventory_targets import InventoryTargetReader, fill_target_names, gun_of, parse_slots
from core.process_service import ProcessService
from core.state import ProcessState
from test_process_service_start import FakePorts, MANUAL
from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)

import main as MAIN
import lib.config as CFG
import lib.logger as LG

SIO2 = 'Silicon Dioxide (SiO2) 2", 0.125"'
CEO2 = 'Cerium Dioxide (CeO2) 2", 0.125"'
SIO2_TEXT = f"{SIO2} (102)"
CEO2_TEXT = f"{CEO2} (153)"

# 실제 응답 모양(다른 챔버 섞임, itemName null, targetUnitId null, 이름에 쉼표·큰따옴표, 같은 건 둘)
SLOTS = [
    {"locationName": "Sputter 1 - Chamber 1 - Gun 1", "targetUnitId": "9", "itemName": "Ti 2\"",
     "barcodeCode": "B9", "materialCode": "Ti", "purity": "4N", "loadedAt": "2026-11-20T01:00:00.000Z"},
    {"locationName": "Sputter 2 - Chamber K - Gun 1", "targetUnitId": "77", "itemName": "Old SiO2",
     "barcodeCode": "B77", "materialCode": "SiO2", "purity": "4N", "loadedAt": "2026-10-01T01:00:00.000Z"},
    {"locationName": "Chamber K - Gun 1", "targetUnitId": "102", "itemName": SIO2,
     "barcodeCode": "B102", "materialCode": "SiO2", "purity": "4N", "loadedAt": "2026-11-25T01:00:00.000Z"},
    {"locationName": "Sputter 2 - Chamber K - Gun 2", "targetUnitId": "153", "itemName": CEO2,
     "barcodeCode": "B153", "materialCode": "CeO2", "purity": "3N", "loadedAt": "2026-11-24T01:00:00.000Z"},
    {"locationName": "Chamber K2 - Gun 2", "targetUnitId": "5", "itemName": "Not me", "loadedAt": "2026-11-26T01:00:00.000Z"},
    {"locationName": "Sputter 1 - Chamber 2 - Gun 2", "targetUnitId": None, "itemName": None, "loadedAt": None},
]


# ───────────────────────── 자리 이름 ─────────────────────────
@pytest.mark.parametrize("loc,n", [
    ("Chamber K - Gun 1", 1), ("Sputter 2 - Chamber K - Gun 2", 2), ("chamber k-gun1", 1),
    ("  CHAMBER   K  -  GUN  2 ", 2),
    ("Sputter 1 - Chamber 2 - Gun 1", None), ("Chamber K2 - Gun 1", None), ("Chamber K - Gun 3", None),
    ("Chamber 1 - Gun 1", None), ("Chamber K", None), ("", None), (None, None),
])
def test_gun_of(loc, n):
    assert gun_of(loc) == n


# ───────────────────────── parse ─────────────────────────
def test_parse_real_shape():
    m = parse_slots(SLOTS)
    assert m["g1"]["text"] == SIO2_TEXT                       # 같은 건 둘 → loadedAt 이 늦은 것
    assert m["g1"]["name"] == SIO2 and m["g1"]["unitId"] == "102" and m["g1"]["barcode"] == "B102"
    assert m["g1"]["loadedAt"] == "2026-11-25T01:00:00.000Z"
    assert m["g2"]["text"] == CEO2_TEXT                       # Chamber K2 는 섞이지 않는다


def test_parse_null_item_and_unit():
    m = parse_slots([{"locationName": "Chamber K - Gun 1", "targetUnitId": None, "itemName": "Cu 2\""},
                     {"locationName": "Chamber K - Gun 2", "targetUnitId": "8", "itemName": None}])
    assert m["g1"]["text"] == 'Cu 2"'                         # 번호가 없으면 itemName 만
    assert m["g2"] is None                                    # 자리에 타겟 없음


@pytest.mark.parametrize("bad", [{"slots": []}, "x", None, 3])
def test_parse_not_array_fails(bad):
    with pytest.raises(ValueError):
        parse_slots(bad)


def test_http_fetch_goes_through_blocked_urlopen():
    """conftest 의 urlopen 차단이 그대로 걸린다(부를 때 urllib.request.urlopen 을 찾는다)."""
    with pytest.raises(OSError, match="blocked by tests"):
        IT.http_fetch("https://example.invalid/inventory/api/chamber-slots", 1)


def test_http_fetch_status_and_size(monkeypatch):
    class _Res:
        def __init__(self, status, body): self.status, self._b = status, body
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self, n=-1): return self._b[:n] if n >= 0 else self._b
    seen = {}

    def _open(req, timeout=None):
        seen.update(url=req.full_url, accept=req.get_header("Accept"), timeout=timeout)
        return seen["res"]
    monkeypatch.setattr(IT.urllib.request, "urlopen", _open)
    seen["res"] = _Res(200, b'[{"locationName": "Chamber K - Gun 1", "itemName": "A", "targetUnitId": 1}]')
    assert IT.http_fetch("https://h/x", 3)[0]["itemName"] == "A"
    assert seen["accept"] == "application/json" and seen["timeout"] == 3
    seen["res"] = _Res(401, b"[]")
    with pytest.raises(ValueError, match="HTTP 401"):
        IT.http_fetch("https://h/x", 3)
    seen["res"] = _Res(200, b"[" + b" " * (IT.MAX_BODY_BYTES + 10) + b"]")
    with pytest.raises(ValueError, match="2 MB"):
        IT.http_fetch("https://h/x", 3)


# ───────────────────────── reader(가짜 시계) ─────────────────────────
class _Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


def _reader(results, max_age=300):
    clock = _Clock()
    it = iter(results)

    def fetch(url, timeout):
        assert timeout == 3
        r = next(it)
        if isinstance(r, BaseException):
            raise r
        return r
    return InventoryTargetReader("https://vanam.synology.me/inventory/api/chamber-slots", 60, 3, max_age,
                                 fetch=fetch, clock=clock), clock


def test_reader_before_first_read():
    r, _ = _reader([])
    assert r.current() == (None, "아직 읽기 전") and r.pop_events() == []
    assert r.host == "vanam.synology.me"


def test_reader_success_failure_recovery_change():
    other = [dict(SLOTS[2], itemName="New", targetUnitId="200")]
    r, clock = _reader([SLOTS, OSError("timed out"), OSError("again"), SLOTS, other])
    assert r.read_once() is True
    m, why = r.current()
    assert why == "" and m["g1"]["text"] == SIO2_TEXT and m["g2"]["text"] == CEO2_TEXT
    assert r.pop_events() == [("changed", {"g1": SIO2_TEXT, "g2": CEO2_TEXT})]
    # 실패 — 이전 값 유지, 사건 1번(이어진 실패는 다시 쌓지 않음)
    clock.t += 60
    assert r.read_once() is False
    clock.t += 60
    assert r.read_once() is False
    assert r.current()[0]["g1"]["text"] == SIO2_TEXT
    ev = r.pop_events()
    assert len(ev) == 1 and ev[0][0] == "failed" and "timed out" in ev[0][1]
    assert r.last_error() == "OSError: again"
    # 회복 1번, 같은 값이면 바뀜 사건 없음
    clock.t += 60
    assert r.read_once() is True
    assert r.pop_events() == [("recovered", None)] and r.last_error() is None
    # 값 바뀜
    assert r.read_once() is True
    assert r.pop_events() == [("changed", {"g1": "New (200)", "g2": None})]


def test_reader_max_age():
    r, clock = _reader([SLOTS, OSError("down")], max_age=300)
    r.read_once()
    clock.t += 300
    assert r.current()[0] is not None                         # 딱 max_age 는 아직 쓴다
    r.read_once()                                             # 실패
    clock.t += 1
    m, why = r.current()
    assert m is None and why == "재고관리를 5분째 읽지 못함"


def test_reader_thread_start_stop():
    import threading
    got = threading.Event()

    def fetch(url, timeout):
        got.set()
        return SLOTS
    r = InventoryTargetReader("https://h/x", 3600, 3, 300, fetch=fetch)
    r.start()
    assert got.wait(5)
    r.stop()
    assert r._thread is not None and r._thread.daemon
    r._thread.join(5)
    assert not r._thread.is_alive() and r.current()[0]["g1"]["text"] == SIO2_TEXT


# ───────────────────────── 채우기(R2·R3) ─────────────────────────
MOUNTED = parse_slots(SLOTS)


def test_fill_only_used_and_empty():
    p = {"use_g1": True, "g1_target_name": "", "use_g2": False, "g2_target_name": ""}
    assert fill_target_names(p, MOUNTED) == [(1, SIO2_TEXT)]
    assert p["g1_target_name"] == SIO2_TEXT and p["g2_target_name"] == ""


def test_fill_keeps_input_and_unused():
    p = {"use_g1": True, "g1_target_name": "CeO2 내가 씀", "use_g2": False, "g2_target_name": "x"}
    assert fill_target_names(p, MOUNTED) == []
    assert p == {"use_g1": True, "g1_target_name": "CeO2 내가 씀", "use_g2": False, "g2_target_name": "x"}


def test_fill_no_target_or_stale():
    p = {"use_g1": True, "g1_target_name": "", "use_g2": True, "g2_target_name": "  "}
    assert fill_target_names(p, {"g1": None, "g2": None}) == []
    assert fill_target_names(p, None) == []
    assert p["g1_target_name"] == "" and p["g2_target_name"] == "  "


# ───────────────────────── 서비스(가짜 ports) ─────────────────────────
class _RecPorts(FakePorts):
    def fill_targets(self, params):
        self.calls.append(["fill_targets", dict(params)])
        return ["[타겟] 문장"]


def test_service_manual_order():
    ports = _RecPorts()
    svc = ProcessService(ProcessState(), ports)
    svc.start(MANUAL)
    n = ports.names()
    assert n.index("show_manual_inputs") < n.index("fill_targets") < n.index("reset_stats") < n.index("open_process_log")
    logs = [c for c in ports.calls if c[0] == "log"]
    i = logs.index(["log", "정보", "=== CHK 공정 시작 ==="])
    assert logs[i + 1] == ["log", "정보", "[타겟] 문장"]


def test_service_csv_order():
    ports = _RecPorts()
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[{"Process_name": "S1", "Ar": "1"}])
    ProcessService(st, ports).start()
    n = ports.names()
    assert n.index("build_csv_params") < n.index("fill_targets") < n.index("reset_stats") \
        < n.index("apply_params_to_ui") < n.index("open_process_log")
    logs = [c for c in ports.calls if c[0] == "log"]
    i = logs.index(["log", "정보", "=== CHK CSV STEP 1/1 시작 ==="])
    assert logs[i + 1] == ["log", "정보", "[타겟] 문장"]


# ───────────────────────── 실제 창 ─────────────────────────
class FakeReader:
    """창에 끼우는 가짜 reader — 스레드 없이 값·사유·사건을 시험이 정한다."""
    host = "vanam.synology.me"

    def __init__(self, mounted=None, why=""):
        self.mounted, self.why, self.events = mounted, why, []
        self.started = self.stopped = 0

    def start(self): self.started += 1
    def stop(self, wait_s=1.0): self.stopped += 1
    def current(self): return (dict(self.mounted) if self.mounted else None), self.why
    def last_ok_wall(self): return 1764100800.0 if self.mounted else None
    def pop_events(self):
        ev, self.events = self.events, []
        return ev


@pytest.fixture
def tw(fresh, monkeypatch, tmp_path):
    """창 + 가짜 reader. 공정 로그 파일은 tmp, 장치 시작 요청은 가로챈다."""
    w = fresh
    monkeypatch.setattr(MAIN, "QMessageBox", MagicMock())
    w.chat_chk = MagicMock()
    w.erp = MagicMock()
    w.erp.rejected = False
    w.erp.pop_commands.return_value = []
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w.csv_file_path = None; w.csv_rows = []
    w._remote_exec = False; w._remote_alerts = []
    monkeypatch.setattr(w.plc_controller, "read_main_valve_state", lambda: (True, True))
    started = []
    monkeypatch.setattr(w, "request_process_start",
                        type("S", (), {"emit": staticmethod(lambda p: started.append(p))})())
    monkeypatch.setattr(LG, "NAS_PROCESS_LOG_DIR", tmp_path / "proc")
    w._started = started
    fake = FakeReader(MOUNTED)
    w._setup_inventory_targets(reader=fake)
    yield w
    w._inventory_timer.stop()
    w._inventory_reader = None
    w._inventory_timer = None
    LG.clear_process_log_file()
    w.process_running = False; w.csv_mode = False; w._csv_delay_active = False
    w.csv_file_path = None; w.csv_rows = []
    w._reset_process_ui_fields()
    w.erp = MagicMock()


def _set_manual(w, g1=""):
    ui = w.ui
    ui.Ar_gas_radio.setChecked(True); ui.O2_gas_radio.setChecked(False)
    ui.Ar_flow_edit.setPlainText("20"); ui.working_pressure_edit.setPlainText("5")
    ui.dc_power_checkbox.setChecked(True); ui.DC_power_edit.setPlainText("100")
    ui.rf_power_checkbox.setChecked(False); ui.rf_pulse_checkbox.setChecked(False)
    ui.Shutter_delay_edit.setPlainText("1"); ui.process_time_edit.setPlainText("1")
    ui.G1_checkbox.setChecked(True); ui.G1_edit.setPlainText(g1)
    ui.G2_checkbox.setChecked(False); ui.G2_edit.setPlainText("")


def _process_log_text():
    p = LG._current_log_file
    assert p is not None and os.path.exists(p)
    return open(p, encoding="utf-8").read()


def _assert_recorded(w, text, tmp_path, monkeypatch):
    """st.last_params · 구글챗 시작 params · erp_run_start params · ChK_log.csv 행이 모두 text 이고, 공정 로그에 [타겟] 줄."""
    assert w._last_params["g1_target_name"] == text
    chat = w.chat_chk.notify_process_started.call_args.args[0]
    assert chat["g1_target_name"] == text and chat["G1_target_name"] == text
    assert w.erp.run_start.call_args.args[1]["g1_target_name"] == text
    assert w._started[-1]["g1_target_name"] == text
    row = w._build_chk_csv_row()
    assert row["G1 Target"] == text and row["G2 Target"] == ""
    assert f"[타겟] G1 타겟 이름을 재고관리 장착 타겟으로 기록합니다: {text}" in _process_log_text()
    # ChK_log.csv 에 쓰고 csv 모듈로 다시 읽는다(쉼표·큰따옴표가 그대로)
    path = tmp_path / "ChK_log.csv"
    monkeypatch.setattr(LG, "CHK_CSV_PATH", str(path))
    assert LG.append_chk_csv_row(row) is True
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[-1]["G1 Target"] == text


def test_window_manual_local(tw, tmp_path, monkeypatch):
    w = tw
    _set_manual(w, g1="")
    w._handle_start_process()
    assert w._started, "공정이 시작되지 않음"
    _assert_recorded(w, SIO2_TEXT, tmp_path, monkeypatch)
    assert w.ui.G1_edit.toPlainText() == SIO2_TEXT            # 공정 중 화면도 기록과 같게
    assert w.ui.G2_edit.toPlainText() == ""                   # 안 쓰는 건은 그대로


def test_window_manual_input_kept(tw):
    w = tw
    _set_manual(w, g1="내 타겟")
    w._handle_start_process()
    assert w._last_params["g1_target_name"] == "내 타겟"
    assert "[타겟]" not in _process_log_text()


def test_window_remote_manual(tw, tmp_path, monkeypatch):
    w = tw
    _set_manual(w, g1="화면값")                                 # 원격은 화면 칸을 쓰지 않는다
    args = {"useAr": True, "arFlow": "20", "workingPressure": "5", "useDc": True, "dcPower": "100",
            "shutterDelay": "1", "processTime": "1", "useG1": True}
    w.erp.pop_commands.return_value = [{"id": 1, "command": "PROCESS_START", "args": args}]
    w._erp_cmd_timer.timeout.emit()
    w.erp.pop_commands.return_value = []
    a = w.erp.cmd_result.call_args.args
    assert a[1] is True, a
    _assert_recorded(w, SIO2_TEXT, tmp_path, monkeypatch)
    assert w.ui.G1_edit.toPlainText() == SIO2_TEXT


def test_window_recipe_row(tw, tmp_path, monkeypatch):
    w = tw
    row = {"Process_name": "S1", "Ar": "1", "Ar_flow": "20", "O2": "0", "O2_flow": "0",
           "working_pressure": "5", "process_time": "1", "shutter_delay": "1",
           "use_rf_power": "0", "rf_power": "0", "use_dc_power": "1", "dc_power": "100",
           "use_rf_pulse": "0", "rf_pulse_power": "0", "rf_pulse_freq": "", "rf_pulse_duty": "",
           "use_dc_delay": "0", "use_heater": "0", "heater_temp": "0", "heater_ramp": "0",
           "gun1": "1", "gun2": "0", "G1 Target": "", "G2 Target": ""}
    w.csv_file_path = str(tmp_path / "r.csv")
    w.csv_rows = [row]
    w._handle_start_process()
    assert w._started, "공정이 시작되지 않음"
    _assert_recorded(w, SIO2_TEXT, tmp_path, monkeypatch)
    assert "=== CHK CSV STEP 1/1 시작 ===" in _process_log_text()


def test_window_stale_leaves_blank(tw):
    w = tw
    w._inventory_reader.mounted, w._inventory_reader.why = None, "재고관리를 7분째 읽지 못함"
    _set_manual(w, g1="")
    w._handle_start_process()
    assert w._last_params["g1_target_name"] == ""
    assert w._build_chk_csv_row()["G1 Target"] == ""
    assert "[타겟] G1 타겟 이름은 빈칸으로 기록합니다(재고관리를 7분째 읽지 못함)" in _process_log_text()


def test_window_no_target_leaves_blank(tw):
    w = tw
    w._inventory_reader.mounted = {"g1": None, "g2": MOUNTED["g2"]}
    _set_manual(w, g1="")
    w._handle_start_process()
    assert w._last_params["g1_target_name"] == ""
    assert "[타겟] G1 타겟 이름은 빈칸으로 기록합니다(재고관리에 장착 타겟 없음)" in _process_log_text()


def test_window_hints_and_events(tw):
    w = tw
    logs = []
    w._inventory_reader.events = [("changed", {"g1": SIO2_TEXT, "g2": None}), ("failed", "OSError: timed out"),
                                  ("recovered", None)]
    orig = MAIN.log_message_to_monitor
    MAIN.log_message_to_monitor = lambda lv, m: (logs.append(m), orig(lv, m))
    try:
        w._poll_inventory_targets()                           # 메인 스레드 타이머가 부르는 슬롯
    finally:
        MAIN.log_message_to_monitor = orig
    assert logs == [f"[타겟] 재고관리 장착 타겟 — G1: {SIO2_TEXT}, G2: 없음",
                    "[타겟] 재고관리에서 장착 타겟을 읽지 못했습니다(vanam.synology.me, OSError: timed out). 공정은 그대로 진행합니다",
                    "[타겟] 재고관리에서 장착 타겟을 다시 읽었습니다"]
    ui = w.ui
    assert ui.G1_edit.placeholderText() == SIO2_TEXT and ui.G2_edit.placeholderText() == CEO2_TEXT
    assert ui.G1_edit.toolTip().startswith(f"재고관리 장착 타겟: {SIO2_TEXT} · 칸을 비워 두면 이 이름으로 기록됩니다 · 읽은 시각 ")
    assert ui.G1_edit.toPlainText() == "" and ui.G2_edit.toPlainText() == ""
    geo = ui.G1_edit.geometry()
    # 공정 끝 초기화 뒤에도 안내가 남는다
    w._reset_process_ui_fields()
    assert ui.G1_edit.placeholderText() == SIO2_TEXT and ui.G1_edit.toPlainText() == ""
    assert ui.G1_edit.geometry() == geo
    # 오래되면 안내를 비우고 툴팁에 사유
    w._inventory_reader.mounted, w._inventory_reader.why = None, "재고관리를 6분째 읽지 못함"
    w._poll_inventory_targets()
    assert ui.G1_edit.placeholderText() == "" and "재고관리를 6분째 읽지 못함" in ui.G1_edit.toolTip()


def test_window_timer_is_main_thread_qtimer(tw):
    from PyQt6.QtCore import QTimer, QThread
    w = tw
    assert isinstance(w._inventory_timer, QTimer) and w._inventory_timer.isActive()
    assert w._inventory_timer.thread() is QThread.currentThread()
    assert w._inventory_reader.started == 1


# ───────────────────────── 꺼짐(R7) ─────────────────────────
def test_disabled_makes_nothing(win, monkeypatch):
    w = win
    made = []
    monkeypatch.setattr(MAIN, "InventoryTargetReader", lambda *a, **k: made.append(a))
    for enabled, url, dev in ((False, "https://h/x", False), ("true", "https://h/x", False), (1, "https://h/x", False),
                              (True, "", False), (True, "https://h/x", True)):
        monkeypatch.setattr(CFG, "INVENTORY_TARGETS_ENABLED", enabled)
        monkeypatch.setattr(CFG, "INVENTORY_API_URL", url)
        monkeypatch.setattr(MAIN, "DEV_MODE", dev)
        w._setup_inventory_targets()
        assert w._inventory_reader is None and w._inventory_timer is None
        p = {"use_g1": True, "g1_target_name": "", "use_g2": True, "g2_target_name": ""}
        assert w.proc.ports.fill_targets(p) == []
        assert p == {"use_g1": True, "g1_target_name": "", "use_g2": True, "g2_target_name": ""}
    assert made == []


def test_enabled_builds_reader_from_config(win, monkeypatch):
    w = win
    made = []

    class _R(FakeReader):
        def __init__(self, *a, **k):
            super().__init__()
            made.append(a)
    monkeypatch.setattr(MAIN, "InventoryTargetReader", _R)
    monkeypatch.setattr(CFG, "INVENTORY_TARGETS_ENABLED", True)
    monkeypatch.setattr(CFG, "INVENTORY_API_URL", "https://vanam.synology.me/inventory/api/chamber-slots")
    monkeypatch.setattr(MAIN, "DEV_MODE", False)
    try:
        w._setup_inventory_targets()
        assert made == [("https://vanam.synology.me/inventory/api/chamber-slots",
                         CFG.INVENTORY_REFRESH_SEC, CFG.INVENTORY_TIMEOUT_SEC, CFG.INVENTORY_MAX_AGE_SEC)]
        assert w._inventory_reader.started == 1
        w._stop_inventory_targets()
        assert w._inventory_reader.stopped == 1 and not w._inventory_timer.isActive()
    finally:
        w._stop_inventory_targets()
        w._inventory_reader = None
        w._inventory_timer = None


def test_config_defaults_and_ranges(monkeypatch, capsys):
    monkeypatch.setattr(CFG, "INVENTORY_TARGETS_ENABLED", "yes")
    monkeypatch.setattr(CFG, "INVENTORY_API_URL", "  https://h/x  ")
    monkeypatch.setattr(CFG, "INVENTORY_REFRESH_SEC", 5)
    monkeypatch.setattr(CFG, "INVENTORY_TIMEOUT_SEC", "3")
    monkeypatch.setattr(CFG, "INVENTORY_MAX_AGE_SEC", 600)
    CFG._validate_inventory_config()
    assert CFG.INVENTORY_TARGETS_ENABLED is False and CFG.INVENTORY_API_URL == "https://h/x"
    assert (CFG.INVENTORY_REFRESH_SEC, CFG.INVENTORY_TIMEOUT_SEC, CFG.INVENTORY_MAX_AGE_SEC) == (60, 3, 600)
    out = capsys.readouterr().out
    assert "INVENTORY_REFRESH_SEC 5 → 60" in out and "INVENTORY_TIMEOUT_SEC '3' → 3" in out


def test_config_user_json_has_keys():
    import json
    from lib.config_loader import _JSON_PATH
    with open(_JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)                                   # 엄격한 JSON
    assert data["INVENTORY_TARGETS_ENABLED"] is True
    assert data["INVENTORY_API_URL"] == "https://vanam.synology.me/inventory/api/chamber-slots"
    assert (data["INVENTORY_REFRESH_SEC"], data["INVENTORY_TIMEOUT_SEC"], data["INVENTORY_MAX_AGE_SEC"]) == (60, 3, 300)


def test_module_imports_stdlib_only():
    import ast
    tree = ast.parse(open(IT.__file__, encoding="utf-8").read())
    mods = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            mods |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom):
            mods.add((n.module or "").split(".")[0])
    assert mods <= {"__future__", "json", "re", "threading", "time", "urllib", "typing"}
