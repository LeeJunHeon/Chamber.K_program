# -*- coding: utf-8 -*-
"""core.params — Qt 없이 직접 불러 20·21 골든(화면 경로로 찍은 결과)과 같은지 본다.
warn 콜백 순서, check_rfpulse_range 경계값, core 의 import 규칙도 확인한다."""
import ast
import dataclasses
import glob
import json
import os
import subprocess
import sys

import pytest

from core.params import ManualInputs, build_manual_params, build_csv_params, check_rfpulse_range

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN = os.path.join(ROOT, "tests", "golden")


def _golden(name):
    with open(os.path.join(GOLDEN, name + ".json"), encoding="utf-8") as f:
        return {c["case"]: c for c in json.load(f)["cases"]}


def typed(d):
    return [[k, type(v).__name__, v] for k, v in d.items()]


# ───────────────────────── 수동 입력 (20_params_manual) ─────────────────────────
BASE = ManualInputs(
    use_ar=True, ar_flow_text="20", use_o2=False, o2_flow_text="",
    working_pressure_text="5", use_dc=True, dc_power_text="100",
    use_rf=False, rf_power_text="", offset_text="6.79", param_text="1.0395",
    use_rf_pulse=False, rfp_power_text="", rfp_freq_text="", rfp_duty_text="",
    shutter_delay_text="1", process_time_text="1",
    use_g1=True, g1_name_text="CeO2", use_g2=False, g2_name_text="", use_dc_delay=False)

_RFP = dict(use_rf_pulse=True, use_dc=False)

MANUAL = {
    "기본(Ar+DC)": {},
    "Ar+O2": dict(use_o2=True, o2_flow_text="10"),
    "Ar 유량 abc": dict(ar_flow_text="abc"),
    "Ar 유량 0": dict(ar_flow_text="0"),
    "RF 체크 + RF 파워 빈칸": dict(use_rf=True, rf_power_text=""),
    "RF 해제 + offset·param 빈칸(기본값)": dict(offset_text="", param_text=""),
    "RF 해제 + offset abc": dict(offset_text="abc"),
    "RF Pulse 파워 빈칸 + 왼쪽 RF 칸 값(흔한 실수)": dict(_RFP, rfp_power_text="", rf_power_text="300"),
    "RF Pulse 듀티 50.7": dict(_RFP, rfp_power_text="100", rfp_duty_text="50.7"),
    "DC 체크 + 값 빈칸 + RF 켬": dict(dc_power_text="", use_rf=True, rf_power_text="150"),
    "G1·G2 이름 앞뒤 공백": dict(g1_name_text="  CeO2 ", use_g2=True, g2_name_text=" Ti  "),
    "겹침: 압력 빈칸 + shutter delay 음수": dict(working_pressure_text="", shutter_delay_text="-1"),
    "겹침: RF Pulse 범위 위반 + 압력 빈칸": dict(_RFP, rfp_power_text="100", rfp_freq_text="40",
                                            working_pressure_text=""),
}


@pytest.mark.parametrize("case", list(MANUAL))
def test_manual_matches_golden(case):
    g = _golden("20_params_manual")[case]
    inp = dataclasses.replace(BASE, **MANUAL[case])
    if g["params"]:
        assert typed(build_manual_params(inp)) == g["params"][0]
        assert g["alerts"] == []
    else:
        with pytest.raises((ValueError, TypeError)) as ei:
            build_manual_params(inp)
        [(kind, title, text)] = g["alerts"]
        assert (kind, title) == ("warning", "입력 오류")
        assert text == f"공정 파라미터가 잘못되었습니다:\n{ei.value}"


def test_manual_inputs_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        BASE.use_ar = False     # type: ignore[misc]


# ───────────────────────── CSV 행 (21_params_csv) ─────────────────────────
def _row(**over):
    r = {"Process_name": "S1", "Ar": "1", "Ar_flow": "20", "O2": "0", "O2_flow": "0",
         "working_pressure": "5", "process_time": "1", "shutter_delay": "1",
         "use_rf_power": "0", "rf_power": "0", "use_dc_power": "1", "dc_power": "100",
         "use_rf_pulse": "0", "rf_pulse_power": "0", "rf_pulse_freq": "", "rf_pulse_duty": "",
         "use_dc_delay": "0", "use_heater": "0", "heater_temp": "0", "heater_ramp": "0",
         "gun1": "1", "gun2": "0", "G1 Target": "CeO2", "G2 Target": ""}
    r.update(over)
    return r


def _old_row():
    r = _row()
    for k in ("use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty",
              "use_heater", "heater_temp", "heater_ramp"):
        r.pop(k)
    return r


_HT = {"use_heater": "1", "heater_temp": "300"}

CSV = {
    "기본 행": (_row(), "6.79", "1.0395"),
    "Ar_flow abc": (_row(Ar_flow="abc"), "6.79", "1.0395"),
    "use_rf_power=1 + UI offset abc": (_row(use_rf_power="1", rf_power="150"), "abc", "1.0395"),
    "use_rf_power=1 + UI offset 빈칸": (_row(use_rf_power="1", rf_power="150"), "", "1.0395"),
    "use_heater=1 + ramp 3": (_row(**dict(_HT, heater_ramp="3")), "6.79", "1.0395"),
    "ramp 3 경고 뒤 RF Pulse 오류": (_row(**dict(_HT, heater_ramp="3", use_rf_pulse="1", rf_pulse_power="")),
                               "6.79", "1.0395"),
    "use_heater=0 + temp·ramp 값 있음": (_row(use_heater="0", heater_temp="300", heater_ramp="3"), "6.79", "1.0395"),
    "참 표기 2": (_row(Ar="2"), "6.79", "1.0395"),
    "RF Pulse·히터 컬럼이 없는 옛 행": (_old_row(), "6.79", "1.0395"),
    "값이 None 인 칸": (_row(O2_flow=None, rf_pulse_freq=None, **{"G2 Target": None, "shutter_delay": None}),
                   "6.79", "1.0395"),
}


@pytest.mark.parametrize("case", list(CSV))
def test_csv_matches_golden(case):
    g = _golden("21_params_csv")[case]
    row, off, par = CSV[case]
    warns = []
    try:
        got = {"params": typed(build_csv_params(row, off, par, lambda lv, m: warns.append([lv, m])))}
    except Exception as e:
        got = {"error": [type(e).__name__, str(e)]}
    exp = {k: g[k] for k in ("params", "error") if k in g}
    assert got == exp
    assert warns == g["logs"]         # 화면 경로에서는 on_status_message → 로그 한 줄


def test_warn_comes_before_later_errors_and_is_optional():
    """경고는 뒤쪽 검사(RF Pulse)보다 먼저 한 번 나간다. warn=None 이어도 결과는 같다."""
    order = []
    row = _row(**dict(_HT, heater_ramp="3", use_rf_pulse="1", rf_pulse_power=""))
    with pytest.raises(ValueError) as ei:
        build_csv_params(row, "6.79", "1.0395", lambda lv, m: order.append(("warn", lv, m)))
    order.append(("error", str(ei.value)))
    assert [o[0] for o in order] == ["warn", "error"]
    assert order[0][1] == "히터(경고)"
    ok = _row(**dict(_HT, heater_ramp="3"))
    assert build_csv_params(ok, "6.79", "1.0395") == build_csv_params(ok, "6.79", "1.0395", lambda *a: None)


# ───────────────────────── check_rfpulse_range 경계 ─────────────────────────
@pytest.mark.parametrize("freq,duty", [
    (None, None), (0.001, None), (30, None), (None, 1), (None, 99),
    (10, 16), (10, 84),          # 10 kHz: 주기 100 µs — ON/OFF 가 정확히 16 µs
    (5, 50),
])
def test_rfpulse_range_ok(freq, duty):
    check_rfpulse_range(freq, duty, "T")


@pytest.mark.parametrize("freq,duty,part", [
    (0.0009, None, "주파수"), (30.001, None, "주파수"), (40, None, "주파수"),
    (None, 0, "듀티"), (None, 100, "듀티"),
    (10, 15, "최소 ON/OFF"), (10, 85, "최소 ON/OFF"), (20, 80, "최소 ON/OFF"),
])
def test_rfpulse_range_rejects(freq, duty, part):
    with pytest.raises(ValueError) as ei:
        check_rfpulse_range(freq, duty, "T")
    msg = str(ei.value)
    assert msg.startswith("[T] RF Pulse")
    assert part in msg or (part == "최소 ON/OFF" and "16µs" in msg)


# ───────────────────────── 정적 검사 ─────────────────────────
_FORBIDDEN = ("PyQt6", "UI", "main", "controller", "device", "reporter", "lib.logger", "lib.heater_logger")


def _imports(path):
    tree = ast.parse(open(path, encoding="utf-8").read(), path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name
        elif isinstance(node, ast.ImportFrom):
            yield ("." * node.level) + (node.module or "")


def test_core_imports_no_ui_or_devices():
    files = glob.glob(os.path.join(ROOT, "core", "*.py"))
    assert any(f.endswith("params.py") for f in files)
    bad = []
    for f in files:
        for mod in _imports(f):
            if any(mod == x or mod.startswith(x + ".") for x in _FORBIDDEN):
                bad.append((os.path.basename(f), mod))
            if mod.startswith("lib") and mod != "lib.config":
                bad.append((os.path.basename(f), mod))
    assert bad == []


def test_core_params_imports_without_qt():
    """별도 프로세스에서 core.params 만 불러도 PyQt6·main 이 딸려 오지 않는다."""
    code = ("import sys; import core.params; "
            "print(sorted(m for m in sys.modules if m.split('.')[0] in "
            "('PyQt6', 'main', 'UI', 'controller', 'device', 'reporter') or m in ('lib.logger', 'lib.heater_logger')))")
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip().splitlines()[-1] == "[]"
