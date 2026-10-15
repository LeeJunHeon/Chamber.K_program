# -*- coding: utf-8 -*-
"""베이스 압력(base_pressure, Torr) — 레시피 열·수동 입력칸·ERP 원격·상태 보고.
2026-10-15: 값이 명시된 공정만 베이스 대기. 열 없음·빈칸·공백·0 → 0.0(대기 안 함), 잘못된 값은 시작 거부."""
import csv
import os

import pytest
from PyQt6.QtCore import Qt

import main as MAIN
from core.params import (ManualInputs, build_manual_params, build_csv_params, parse_base_pressure_text,
                         check_base_pressure, fmt_base_pressure)
from integrations.erp_commands import manual_inputs_from_args, PROCESS_RECIPE_COLS, write_recipe_csv
from integrations.erp_state import build_state
from lib.recipe_io import load_table
from test_main_heater import win  # noqa: F401
from test_erp_state import FakeSrc

ROW = {"Process_name": "S1", "Ar": "1", "Ar_flow": "20", "working_pressure": "5", "process_time": "1",
       "use_dc_power": "1", "dc_power": "100"}
MANUAL = dict(use_ar=True, ar_flow_text="20", use_o2=False, o2_flow_text="", working_pressure_text="5",
              use_dc=True, dc_power_text="100", use_rf=False, rf_power_text="", offset_text="", param_text="",
              use_rf_pulse=False, rfp_power_text="", rfp_freq_text="", rfp_duty_text="",
              shutter_delay_text="0", process_time_text="1", use_g1=False, g1_name_text="", use_g2=False,
              g2_name_text="", use_dc_delay=False)


# ───────── 검사·형식 ─────────
@pytest.mark.parametrize("text,want", [("", 0.0), ("   ", 0.0), (" \n", 0.0), ("1e-5", 1e-5), ("1.0E-05", 1e-5),
                                       (" 2.5e-7 ", 2.5e-7), ("0", 0.0), ("1e-9", 1e-9), ("1e-3", 1e-3)])
def test_manual_text_accepted(text, want):
    assert parse_base_pressure_text(text) == pytest.approx(want)


@pytest.mark.parametrize("text", ["1e-2", "5e-10", "-1e-5", "abc", "1e-55", "5e-3"])
def test_manual_text_rejected(text):
    with pytest.raises(ValueError):
        parse_base_pressure_text(text)


def test_range_message_and_format():
    with pytest.raises(ValueError, match=r"\[S1\] base_pressure 5\.0E-03 Torr 는 범위 밖입니다 \(비우거나 0 = 대기 안 함, 1E-09 ~ 1E-03 Torr\)"):
        check_base_pressure(5e-3, "S1")
    assert fmt_base_pressure(1e-5) == "1.0E-05" and fmt_base_pressure(0) == "" and fmt_base_pressure(0.0) == ""


# ───────── 레시피(CSV·xlsx) ─────────
def test_csv_column_missing_means_no_wait():
    assert build_csv_params(dict(ROW), "6.79", "1.0395")["base_pressure"] == 0.0


@pytest.mark.parametrize("v,want", [("1.0E-05", 1e-5), ("1e-5", 1e-5), (3e-6, 3e-6), (1e-5, 1e-5),
                                    ("0", 0.0), (0, 0.0), ("", 0.0), ("   ", 0.0), (None, 0.0)])
def test_csv_column_values(v, want):
    assert build_csv_params(dict(ROW, base_pressure=v), "6.79", "1.0395")["base_pressure"] == pytest.approx(want)


@pytest.mark.parametrize("v,match", [("5e-3", "범위 밖"), ("-1e-5", "범위 밖"), ("1e-55", "범위 밖"),
                                     ("x", "CSV 'base_pressure' 값이 숫자가 아닙니다"),
                                     ("abc", "CSV 'base_pressure' 값이 숫자가 아닙니다")])
def test_csv_column_rejected(v, match):
    with pytest.raises(ValueError, match=match):
        build_csv_params(dict(ROW, base_pressure=v), "6.79", "1.0395")


def test_xlsx_numeric_cell(tmp_path):
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Recipe"
    hdr = ["Process_name", "Ar", "Ar_flow", "base_pressure", "working_pressure", "process_time", "use_dc_power", "dc_power"]
    ws.append(hdr)
    ws.append(["S1", "T", 20, 2e-6, 5, 1, "T", 100])
    p = tmp_path / "r.xlsx"
    wb.save(p)
    rows = load_table(str(p), ["Recipe"])
    assert build_csv_params(rows[0], "6.79", "1.0395")["base_pressure"] == pytest.approx(2e-6)


def test_sample_recipe_has_column_before_working_pressure():
    import glob
    import openpyxl
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    f = [p for p in glob.glob(os.path.join(root, "recipes", "*.xlsx")) if "공정" in p][0]
    hdr = [c.value for c in openpyxl.load_workbook(f, read_only=True)["Recipe"][1]]
    assert hdr[hdr.index("base_pressure") + 1] == "working_pressure"


# ───────── ERP 원격 ─────────
def test_erp_manual_args_base_pressure():
    a = {"useAr": True, "arFlow": "20", "workingPressure": "5", "useDc": True, "dcPower": "100",
         "processTime": "1", "shutterDelay": "0"}
    assert build_manual_params(manual_inputs_from_args(a, "", ""))["base_pressure"] == 0.0          # 없음 → 대기 안 함
    assert build_manual_params(manual_inputs_from_args(dict(a, basePressure=""), "", ""))["base_pressure"] == 0.0
    assert build_manual_params(manual_inputs_from_args(dict(a, basePressure=None), "", ""))["base_pressure"] == 0.0
    assert build_manual_params(manual_inputs_from_args(dict(a, basePressure="2.0E-06"), "", ""))["base_pressure"] == pytest.approx(2e-6)
    with pytest.raises(ValueError, match="범위 밖"):
        build_manual_params(manual_inputs_from_args(dict(a, basePressure="1e-2"), "", ""))


def test_erp_recipe_rows_and_csv_column(tmp_path, monkeypatch):
    cols = PROCESS_RECIPE_COLS
    assert cols[cols.index("base_pressure") + 1] == "working_pressure"
    import tempfile
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    path = write_recipe_csv([dict(ROW, base_pressure="3e-6")], cols, "process_web_t.csv")
    with open(path, encoding="utf-8-sig", newline="") as f:
        r = next(csv.DictReader(f))
    assert r["base_pressure"] == "3e-6"
    assert build_csv_params(r, "6.79", "1.0395")["base_pressure"] == pytest.approx(3e-6)
    st = build_state(FakeSrc(rows=[dict(ROW, base_pressure="3e-6")], path="C:/r/a.csv"))
    keys = list(st["csvRecipe"]["rows"][0])
    assert keys[keys.index("base_pressure") + 1] == "working_pressure"
    assert st["csvRecipe"]["rows"][0]["base_pressure"] == "3e-6"


def test_state_group_has_base_pressure():
    st = build_state(FakeSrc(texts={"basePressure_edit": "1.0E-05", "working_pressure_edit": "5"}))
    items = [g for g in st["groups"] if g["label"] == "압력 · 시간"][0]["items"]
    labels = [i["label"] for i in items]
    assert labels[labels.index("Base P") + 1] == "Working P"
    assert items[labels.index("Base P")] == {"label": "Base P", "setpoint": "1.0E-05", "unit": "Torr"}


# ───────── 화면(UI) ─────────
def test_ui_geometry_label_and_tab_order(win):
    ui = win.ui
    g = lambda w: (w.x(), w.y(), w.width(), w.height())
    assert g(ui.basePressure_edit) == (735, 210, 131, 31) and g(ui.working_pressure_edit) == (875, 210, 131, 31)
    assert ui.basePressure_label.pos().x() == 735 and ui.basePressure_label.pos().y() == 190
    assert ui.working_pressure_label.pos().x() == 875 and ui.working_pressure_label.pos().y() == 190
    assert ui.basePressure_label.text() == "Base pressure [Torr]"
    # 탭 순서: base → working (라벨처럼 포커스를 받지 않는 위젯은 건너뛴다)
    w = ui.basePressure_edit.nextInFocusChain()
    while w.focusPolicy() == Qt.FocusPolicy.NoFocus:
        w = w.nextInFocusChain()
    assert w is ui.working_pressure_edit
    # 네 위젯이 서로·다른 위젯과 겹치지 않는다
    mine = [ui.basePressure_label, ui.basePressure_edit, ui.working_pressure_label, ui.working_pressure_edit]
    from PyQt6.QtWidgets import QWidget
    others = [c for c in win.findChildren(QWidget) if c.parent() is win and c not in mine and c.isVisibleTo(win)]
    for m in mine:
        hits = [o.objectName() for o in others if o.geometry().intersects(m.geometry())]
        assert hits == [], (m.objectName(), hits)


def test_ui_read_reset_apply(win):
    ui = win.ui
    ui.basePressure_edit.setPlainText("2.0E-06")
    assert win._read_manual_inputs().base_pressure_text == "2.0E-06"
    ui.basePressure_edit.setPlainText("")
    assert build_manual_params(ManualInputs(**MANUAL, base_pressure_text=win._read_manual_inputs().base_pressure_text))["base_pressure"] == 0.0
    win._apply_params_to_ui({"base_pressure": 3e-6, "sp1_set": 5.0})
    assert ui.basePressure_edit.toPlainText() == "3.0E-06"
    win._apply_params_to_ui({"base_pressure": 0.0, "sp1_set": 5.0})                # 레시피 적재 0 → 빈칸
    assert ui.basePressure_edit.toPlainText() == ""
    ui.basePressure_edit.setPlainText("2.0E-06")
    win._reset_process_ui_fields()
    assert ui.basePressure_edit.toPlainText() == ""                                   # 초기화 뒤 빈칸
    assert ("base_pressure_text", "basePressure_edit") in MAIN.MainDialog._MANUAL_INPUT_WIDGETS


def test_ui_out_of_range_blocks_start():
    with pytest.raises(ValueError, match="범위 밖"):
        build_manual_params(ManualInputs(**MANUAL, base_pressure_text="1e-2"))


def test_state_base_p_blank_when_screen_blank():
    st = build_state(FakeSrc(texts={"basePressure_edit": "", "working_pressure_edit": "5"}))
    items = [g for g in st["groups"] if g["label"] == "압력 · 시간"][0]["items"]
    assert [i for i in items if i["label"] == "Base P"][0]["setpoint"] == ""


def test_ui_initial_blank_with_placeholder_and_tooltip(qapp):
    from PyQt6.QtWidgets import QDialog
    from UI import Ui_Dialog
    d = QDialog()
    ui = Ui_Dialog()
    ui.setupUi(d)
    e = ui.basePressure_edit
    assert e.toPlainText() == "" and e.placeholderText() == "비우면 대기 안 함"
    assert e.toolTip() == ("베이스 압력[Torr] — 값이 있으면 가스 도입 전 이온 게이지로 이 값 이하가 될 때까지 "
                           "기다림(1E-9 ~ 1E-3). 비우거나 0 이면 대기 안 함")
    d.deleteLater()


def test_no_default_base_pressure_setting():
    import json
    import lib.config as CFG
    import core.params as P
    import controller.process_controller as PC
    assert not any(hasattr(m, "PROCESS_DEFAULT_BASE_PRESSURE") for m in (CFG, P, PC, MAIN))
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "config_user.json"), encoding="utf-8") as f:
        assert "PROCESS_DEFAULT_BASE_PRESSURE" not in json.load(f)


def test_config_loader_ignores_unknown_key(tmp_path, monkeypatch):
    """옛 config_user.json 에 PROCESS_DEFAULT_BASE_PRESSURE 가 남아 있어도 무해 — 로더는 get(key) 로만 읽는다."""
    import lib.config_loader as CL
    p = tmp_path / "config_user.json"
    p.write_text('{"PROCESS_DEFAULT_BASE_PRESSURE": 1e-5, "IG_BASE_WAIT_TIMEOUT_SEC": 1800}', encoding="utf-8")
    monkeypatch.setattr(CL, "_JSON_PATH", str(p))
    assert CL.load_user_config()["IG_BASE_WAIT_TIMEOUT_SEC"] == 1800


# ───────── 스텝: 값이 명시된 공정만 IG 스텝 ─────────
STEP_BASE = dict(use_ar_gas=True, use_o2_gas=False, ar_flow=20.0, o2_flow=0.0, sp1_set=5.0,
                 process_time=1.0, shutter_delay=0.0, dc_power=0.0, rf_power=0.0)
IG3 = ["IG_FILAMENT_ON", "IG_BASE_WAIT", "IG_FILAMENT_OFF"]
NO_WAIT = ("정보", "베이스 압력 대기 안 함 — base_pressure 비어 있음(이온 게이지 사용 안 함)")


def _ig_steps(params, ig_on=True):
    from unittest.mock import MagicMock
    from controller.process_controller import SputterProcessController
    c = SputterProcessController(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    c.set_ig_snapshot_provider((lambda: None) if ig_on else None)
    msgs = []
    c.status_message.connect(lambda lv, m: msgs.append((lv, m)))
    acts = [s.action.name for s in c._build_steps(dict(STEP_BASE, **params))]
    c.deleteLater()
    return [a for a in acts if a.startswith("IG_")], msgs


@pytest.mark.parametrize("row_over", [{}, {"base_pressure": ""}, {"base_pressure": "   "}, {"base_pressure": "0"}])
def test_recipe_blank_or_zero_has_no_ig_steps(row_over, qapp):
    p = build_csv_params(dict(ROW, **row_over), "6.79", "1.0395")
    assert p["base_pressure"] == 0.0
    ig, msgs = _ig_steps({"base_pressure": p["base_pressure"]})
    assert ig == [] and msgs.count(NO_WAIT) == 1 and not any(lv == "경고" for lv, _ in msgs)


@pytest.mark.parametrize("v", ["1e-5", "1.0E-05", 1e-5])
def test_recipe_value_waits(v, qapp):
    p = build_csv_params(dict(ROW, base_pressure=v), "6.79", "1.0395")
    ig, msgs = _ig_steps({"base_pressure": p["base_pressure"]})
    assert ig == IG3 and NO_WAIT not in msgs


def test_params_without_key_or_blank_string_no_wait(qapp):
    for over in ({}, {"base_pressure": None}, {"base_pressure": ""}, {"base_pressure": "  "}):
        ig, msgs = _ig_steps(over)
        assert ig == [] and NO_WAIT in msgs


def test_screen_blank_start_no_wait(qapp):
    p = build_manual_params(ManualInputs(**MANUAL, base_pressure_text=""))
    assert _ig_steps({"base_pressure": p["base_pressure"]})[0] == []


def test_erp_value_waits_and_missing_does_not(qapp):
    a = {"useAr": True, "arFlow": "20", "workingPressure": "5", "useDc": True, "dcPower": "100",
         "processTime": "1", "shutterDelay": "0"}
    for args, want in ((a, []), (dict(a, basePressure=""), []), (dict(a, basePressure="1e-5"), IG3)):
        p = build_manual_params(manual_inputs_from_args(args, "", ""))
        assert _ig_steps({"base_pressure": p["base_pressure"]})[0] == want


def test_ig_disabled_with_value_warns_and_skips(qapp):
    ig, msgs = _ig_steps({"base_pressure": 1e-5}, ig_on=False)
    assert ig == [] and msgs == [("경고", "베이스 압력 대기 생략 — 이온 게이지 꺼짐(IG_ENABLED=false 또는 IG_PORT 없음)")]


def test_ig_disabled_and_blank_is_info_only(qapp):
    ig, msgs = _ig_steps({"base_pressure": 0.0}, ig_on=False)
    assert ig == [] and msgs == [NO_WAIT]
