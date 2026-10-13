# -*- coding: utf-8 -*-
"""베이스 압력(base_pressure, Torr) — 레시피 열·수동 입력칸·ERP 원격·상태 보고."""
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
@pytest.mark.parametrize("text,want", [("", 1e-5), ("1e-5", 1e-5), ("1.0E-05", 1e-5), (" 2.5e-7 ", 2.5e-7),
                                       ("0", 0.0), ("1e-9", 1e-9), ("1e-3", 1e-3)])
def test_manual_text_accepted(text, want):
    assert parse_base_pressure_text(text) == pytest.approx(want)


@pytest.mark.parametrize("text", ["1e-2", "5e-10", "-1e-5", "abc"])
def test_manual_text_rejected(text):
    with pytest.raises(ValueError):
        parse_base_pressure_text(text)


def test_range_message_and_format():
    with pytest.raises(ValueError, match=r"\[S1\] base_pressure 5\.0E-03 Torr 는 범위 밖입니다 \(0 = 대기 생략, 1E-09 ~ 1E-03 Torr\)"):
        check_base_pressure(5e-3, "S1")
    assert fmt_base_pressure(1e-5) == "1.0E-05" and fmt_base_pressure(0) == "0"


# ───────── 레시피(CSV·xlsx) ─────────
def test_csv_column_missing_uses_default():
    assert build_csv_params(dict(ROW), "6.79", "1.0395")["base_pressure"] == pytest.approx(1e-5)


@pytest.mark.parametrize("v,want", [("1.0E-05", 1e-5), ("1e-5", 1e-5), (3e-6, 3e-6), ("0", 0.0), ("", 1e-5)])
def test_csv_column_values(v, want):
    assert build_csv_params(dict(ROW, base_pressure=v), "6.79", "1.0395")["base_pressure"] == pytest.approx(want)


@pytest.mark.parametrize("v,match", [("5e-3", "범위 밖"), ("x", "CSV 'base_pressure' 값이 숫자가 아닙니다")])
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
    assert build_manual_params(manual_inputs_from_args(a, "", ""))["base_pressure"] == pytest.approx(1e-5)
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
    assert build_manual_params(ManualInputs(**MANUAL, base_pressure_text=win._read_manual_inputs().base_pressure_text))["base_pressure"] == pytest.approx(1e-5)
    win._apply_params_to_ui({"base_pressure": 3e-6, "sp1_set": 5.0})
    assert ui.basePressure_edit.toPlainText() == "3.0E-06"
    win._reset_process_ui_fields()
    assert ui.basePressure_edit.toPlainText() == "1.0E-05"
    assert ("base_pressure_text", "basePressure_edit") in MAIN.MainDialog._MANUAL_INPUT_WIDGETS


def test_ui_out_of_range_blocks_start():
    with pytest.raises(ValueError, match="범위 밖"):
        build_manual_params(ManualInputs(**MANUAL, base_pressure_text="1e-2"))
