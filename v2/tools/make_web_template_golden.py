"""기본 템플릿 골든 만들기(사용자 지시 5-8 ③) — 웹 W-4 에서 TS 로 옮긴 행 검사·params·기본 템플릿 계산이 장비와 같은지 확인용.

python -m v2.tools.make_web_template_golden   → v2/tests/golden/web_template_cases.json 을 다시 쓴다.
경우마다 {name, row(recipe.PROCESS_COLUMNS 값), cfg(검사·템플릿에 쓰는 키만 — CFG_KEYS), igEnabled, rfpulsePresent,
          checkErrors(recipe.check_process_row 오류), checkWarnings(경고), params(process_steps.row_to_params),
          blocks(blocks.template_blocks)}. 검사 오류가 있는 행은 params·blocks 가 null.
igEnabled = cfg.IG_ENABLED, rfpulsePresent = cfg.RFPULSE_ENABLED(process_engine._start_row 와 같게).
시험(test_web_template_golden)이 파일 = 지금 코드로 만든 것인지 본다 — 다르면 이 스크립트로 다시 만든다.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from v2.common.paramdef import defaults
from v2.chk import blocks as BK
from v2.chk import params as P
from v2.chk import recipe as RC
from v2.chk import process_steps as PSt

OUT = Path(__file__).resolve().parents[1] / "tests" / "golden" / "web_template_cases.json"

# 검사(check_process_row)·params(row_to_params)·템플릿(template_blocks)이 읽는 설정 키 — 시험이 실제로 읽은 키와 같은지 본다
CFG_KEYS: tuple = (
    "MFC_AR_FS_SCCM", "MFC_O2_FS_SCCM", "BASE_PRESSURE_MIN_TORR", "BASE_PRESSURE_MAX_TORR", "DC_MAX_POWER", "RF_MAX_POWER",
    "RFPULSE_MAX_POWER", "RFPULSE_DUTY_MIN", "RFPULSE_DUTY_MAX", "RFPULSE_PULSE_FREQ_MIN_HZ", "RFPULSE_PULSE_FREQ_MAX_HZ",
    "RFPULSE_PULSE_MIN_EDGE_US", "RFPULSE_ENABLED", "IG_ENABLED", "HEATER_ENABLED", "HEATER_MAX_TEMP",
    "HEATER_RECIPE_RAMP_MAX_C_PER_MIN", "HEATER_RAMP_RATE_C_PER_MIN", "RF_CAL_OFFSET", "RF_CAL_GAIN",
    "GAS_STABILIZE_SEC", "SP2_MTORR", "SP3_MTORR", "DC_POWER_DELAY_SEC",
)

_BASE = {"process_name": "base", "ar": True, "ar_flow": 20.0, "working_pressure": 3.0, "process_time": 10.0,
         "use_dc": True, "dc_power": 100.0, "gun1": True}


def _row(**kw) -> Dict[str, Any]:
    r = dict(_BASE)
    for k, v in kw.items():
        if v is None:
            r.pop(k, None)
        else:
            r[k] = v
    return r


# (이름, 행, 설정 바꿈)
CASES: List[tuple] = [
    ("dc_ar_gun1", _row(), {}),
    ("o2_only_gun2", _row(ar=None, ar_flow=None, o2=True, o2_flow=8.5, gun1=None, gun2=True), {}),
    ("ar_o2_both_guns", _row(o2=True, o2_flow=2.0, gun2=True), {}),
    ("no_gun", _row(gun1=None), {}),
    ("heater_on", _row(use_heater=True, heater_temp=300.0, heater_ramp=20.0), {}),
    ("heater_ramp_default", _row(use_heater=True, heater_temp=250.0), {}),
    ("heater_ramp_zero_default", _row(use_heater=True, heater_temp=250.0, heater_ramp=0.0), {}),   # 0 = HEATER_RAMP_RATE_C_PER_MIN
    ("heater_ramp_below_6",_row(use_heater=True, heater_temp=250.0, heater_ramp=3.0), {}),
    ("base_pressure_ig_on", _row(base_pressure=5e-6), {}),
    ("base_pressure_ig_off", _row(base_pressure=5e-6), {"IG_ENABLED": False}),
    ("no_base_pressure_ig_off", _row(), {"IG_ENABLED": False}),
    ("base_pressure_zero", _row(base_pressure=0), {}),              # 0 = 빈 값(zeroIsEmpty) — 블록이 no_base_pressure 와 같음
    ("base_pressure_zero_ig_off", _row(base_pressure=0.0), {"IG_ENABLED": False}),
    ("base_pressure_empty_str", _row(base_pressure=""), {}),
    ("err_base_pressure_negative", _row(base_pressure=-1e-6), {}),
    ("err_base_pressure_below_min", _row(base_pressure=1e-10), {}),
    ("err_base_pressure_over_max", _row(base_pressure=5e-3), {}),
    ("rf_only", _row(use_dc=None, dc_power=None, use_rf=True, rf_power=150.0), {}),
    ("dc_rf", _row(use_rf=True, rf_power=80.0, gun2=True), {}),
    ("rf_pulse_freq_duty", _row(use_dc=None, dc_power=None, use_rf_pulse=True, rf_pulse_power=200.0,
                                rf_pulse_freq=10.5, rf_pulse_duty=40), {"RFPULSE_ENABLED": True}),
    ("rf_pulse_no_freq", _row(use_dc=None, dc_power=None, use_rf_pulse=True, rf_pulse_power=120.0),
     {"RFPULSE_ENABLED": True}),
    ("dc_rf_pulse", _row(use_rf_pulse=True, rf_pulse_power=50.0, rf_pulse_freq=5, rf_pulse_duty=50),
     {"RFPULSE_ENABLED": True}),
    ("wp_below_sp2", _row(working_pressure=2.0), {}),
    ("wp_between_sp2_sp3", _row(working_pressure=8.0), {}),
    ("wp_above_sp3", _row(working_pressure=20.0), {}),
    ("dc_delay", _row(use_dc_delay=True), {}),
    ("dc_delay_zero_cfg", _row(use_dc_delay=True), {"DC_POWER_DELAY_SEC": 0}),
    ("shutter_delay", _row(shutter_delay=1.5), {}),
    ("process_time_zero_shutter_only", _row(process_time=0.0, shutter_delay=2.0), {}),
    ("fractional_minutes", _row(process_time=0.25, shutter_delay=0.01), {}),
    ("err_no_gas", _row(ar=None, ar_flow=None), {}),
    ("err_no_power", _row(use_dc=None, dc_power=None), {}),
    ("err_time_zero", _row(process_time=0.0), {}),
    ("err_dc_over_max", _row(dc_power=900.0), {}),
    ("err_rf_pulse_disabled", _row(use_rf_pulse=True, rf_pulse_power=50.0), {}),
    ("err_rf_pulse_edge", _row(use_dc=None, dc_power=None, use_rf_pulse=True, rf_pulse_power=100.0,
                               rf_pulse_freq=25, rf_pulse_duty=10), {"RFPULSE_ENABLED": True}),
    ("err_heater_disabled", _row(use_heater=True, heater_temp=200.0), {"HEATER_ENABLED": False}),
    ("err_missing_working_pressure", _row(working_pressure=None), {}),
    ("warn_dc_delay_without_dc", _row(use_dc=None, dc_power=None, use_rf=True, rf_power=100.0, use_dc_delay=True), {}),
]


class _Track(dict):
    """읽은 키를 적는 설정(CFG_KEYS 가 맞는지 확인용)."""

    def __init__(self, *a, **kw) -> None:
        super().__init__(*a, **kw)
        self.read: set = set()

    def __getitem__(self, k):
        self.read.add(k)
        return super().__getitem__(k)

    def get(self, k, default=None):
        self.read.add(k)
        return super().get(k, default)


def one(name: str, row: Dict[str, Any], over: Dict[str, Any], track: set = None) -> Dict[str, Any]:
    full = _Track(defaults(P.PARAMS), **over)
    ig, rfp = bool(full["IG_ENABLED"]), bool(full["RFPULSE_ENABLED"])
    clean, errs, warns = RC.check_process_row(row, full)
    params = blocks = None
    if not errs:
        params = PSt.row_to_params(clean, full)
        blocks = BK.template_blocks(params, full, ig_enabled=ig, rfpulse_present=rfp)
    if track is not None:
        track |= full.read
    return {"name": name, "row": row, "cfg": {k: dict.__getitem__(full, k) for k in CFG_KEYS},
            "igEnabled": ig, "rfpulsePresent": rfp, "checkErrors": list(errs), "checkWarnings": list(warns),
            "params": params, "blocks": blocks}


def build(track: set = None) -> Dict[str, Any]:
    return {"_설명": "기본 템플릿 골든(v2.tools.make_web_template_golden 이 만든다 — 손으로 고치지 않음). 웹 W-4 TS 계산 확인용",
            "cases": [one(n, r, o, track) for n, r, o in CASES]}


def to_json(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False, indent=1, allow_nan=False) + "\n"


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(to_json(build()), encoding="utf-8", newline="\n")
    print(f"{OUT} — {len(CASES)} 경우")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
