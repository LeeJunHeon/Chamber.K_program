"""챔버K 레시피 형식과 행 검사.

공정 레시피: 웹이 JSON 행 목록으로 보낸다(값 타입 그대로 — bool 은 true/false, 숫자는 숫자).
  v1 CSV/xlsx 의 문자열 bool("1", "yes", "o" …)은 웹 가져오기에서 바꾼다 — 장비는 받지 않는다.
히터 레시피: 스텝 목록 + 첫 행 가스 설정.
범위는 ParamDef 키(param_min/param_max)나 숫자로 정한다. 검사 함수는 설정 한 벌(cfg)을 받는다.

v1 근거: core/params.py build_csv_params(257)·build_manual_params(112), core/recipe.py, controller/heater_recipe.py load(551).
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class Col:
    name: str
    type: str                 # str / bool / float / int
    unit: str
    required: str             # always / if:<bool열> / optional
    min: Optional[float] = None
    max: Optional[float] = None
    param_min: Optional[str] = None
    param_max: Optional[str] = None
    min_exclusive: bool = False
    v1_name: str = ""
    desc: str = ""


PROCESS_COLUMNS: tuple = (
    Col("process_name", "str", "", "optional", v1_name="Process_name", desc="'delay N[s|m|h|d]' 이면 딜레이 행. 비면 STEP{n}"),
    Col("ar", "bool", "", "optional", v1_name="Ar"),
    Col("ar_flow", "float", "sccm", "if:ar", min=0.0, min_exclusive=True, v1_name="Ar_flow"),
    Col("o2", "bool", "", "optional", v1_name="O2"),
    Col("o2_flow", "float", "sccm", "if:o2", min=0.0, min_exclusive=True, v1_name="O2_flow"),
    Col("base_pressure", "float", "Torr", "optional", param_min="BASE_PRESSURE_MIN_TORR",
        param_max="BASE_PRESSURE_MAX_TORR", v1_name="base_pressure", desc="0·빈 값이면 베이스 대기 안 함"),
    Col("working_pressure", "float", "mTorr", "always", min=0.0, max=1000.0, min_exclusive=True,
        v1_name="working_pressure", desc="상한 1000 mTorr = GPC3000 FS 1 Torr 가정(bench 확인)"),
    Col("process_time", "float", "min", "always", min=0.0, v1_name="process_time"),
    Col("shutter_delay", "float", "min", "optional", min=0.0, v1_name="shutter_delay"),
    Col("use_dc", "bool", "", "optional", v1_name="use_dc_power"),
    Col("dc_power", "float", "W", "if:use_dc", min=0.0, min_exclusive=True, param_max="DC_MAX_POWER", v1_name="dc_power"),
    Col("use_dc_delay", "bool", "", "optional", v1_name="use_dc_delay"),
    Col("use_rf", "bool", "", "optional", v1_name="use_rf_power"),
    Col("rf_power", "float", "W", "if:use_rf", min=0.0, min_exclusive=True, param_max="RF_MAX_POWER", v1_name="rf_power"),
    Col("use_rf_pulse", "bool", "", "optional", v1_name="use_rf_pulse"),
    Col("rf_pulse_power", "float", "W", "if:use_rf_pulse", min=0.0, min_exclusive=True, param_max="RFPULSE_MAX_POWER",
        v1_name="rf_pulse_power"),
    Col("rf_pulse_freq", "float", "kHz", "optional", min=0.0, min_exclusive=True, v1_name="rf_pulse_freq",
        desc="v1 처럼 kHz(소수 허용). 장비에는 Hz = round(kHz × 1000)(v1 controller/process_controller.py:868-869). "
             "범위는 Hz 로 RFPULSE_PULSE_FREQ_MIN_HZ~MAX_HZ(v1 core/params.py:92-99). 빈 값이면 장비값 유지"),
    Col("rf_pulse_duty", "int", "%", "optional", param_min="RFPULSE_DUTY_MIN", param_max="RFPULSE_DUTY_MAX",
        v1_name="rf_pulse_duty"),
    Col("use_heater", "bool", "", "optional", v1_name="use_heater"),
    Col("heater_temp", "float", "°C", "if:use_heater", min=0.0, min_exclusive=True, param_max="HEATER_MAX_TEMP",
        v1_name="heater_temp"),
    Col("heater_ramp", "float", "°C/min", "optional", min=0.0, param_max="HEATER_RECIPE_RAMP_MAX_C_PER_MIN",
        v1_name="heater_ramp", desc="0·빈 값이면 HEATER_RAMP_RATE_C_PER_MIN, 6 미만은 6 으로(경고)"),
    Col("gun1", "bool", "", "optional", v1_name="gun1"),
    Col("gun2", "bool", "", "optional", v1_name="gun2"),
    Col("g1_target", "str", "", "optional", v1_name="G1 Target"),
    Col("g2_target", "str", "", "optional", v1_name="G2 Target"),
)

HEATER_STEP_COLUMNS: tuple = (
    Col("target_c", "float", "°C", "always", min=0.0, param_max="HEATER_MAX_TEMP", v1_name="target_c"),
    Col("ramp_c_per_min", "float", "°C/min", "optional", min=0.0, min_exclusive=True,
        param_max="HEATER_RECIPE_RAMP_MAX_C_PER_MIN", v1_name="ramp_c_per_min", desc="ramp_min 과 함께 쓰지 않음. 둘 다 비면 기본"),
    Col("ramp_min", "float", "min", "optional", min=0.0, min_exclusive=True, v1_name="ramp_min"),
    Col("soak_min", "float", "min", "optional", min=0.0, v1_name="soak_min"),
    Col("repeat", "int", "회", "optional", min=1.0, param_max="HEATER_RECIPE_MAX_REPEAT", v1_name="repeat"),
)

HEATER_GAS_COLUMNS: tuple = (
    Col("use_ar", "bool", "", "optional", v1_name="use_ar"),
    Col("ar_flow", "float", "sccm", "if:use_ar", min=0.0, min_exclusive=True, v1_name="ar_flow"),
    Col("use_o2", "bool", "", "optional", v1_name="use_o2"),
    Col("o2_flow", "float", "sccm", "if:use_o2", min=0.0, min_exclusive=True, v1_name="o2_flow"),
    Col("wp_mtorr", "float", "mTorr", "optional", min=0.0, max=1000.0, min_exclusive=True, v1_name="wp_mtorr",
        desc="가스를 쓰면 필수"),
)

DELAY_RE = re.compile(r"^\s*delay\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$", re.IGNORECASE)   # v1 core/recipe.py:12
_UNIT_SEC = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}


def delay_seconds(name: Any) -> Optional[float]:
    if not isinstance(name, str):
        return None
    m = DELAY_RE.match(name)
    if not m:
        return None
    return float(m.group(1)) * _UNIT_SEC[m.group(2).lower()]


def _bound(col: Col, cfg: Mapping[str, Any], which: str) -> Optional[float]:
    if which == "min":
        return cfg[col.param_min] if col.param_min else col.min
    return cfg[col.param_max] if col.param_max else col.max


def _check_col(col: Col, v: Any, cfg: Mapping[str, Any]) -> Optional[str]:
    if col.type == "bool":
        return None if isinstance(v, bool) else f"{col.name}: true/false 여야 함"
    if col.type == "str":
        return None if isinstance(v, str) else f"{col.name}: 문자열이어야 함"
    if isinstance(v, bool) or not isinstance(v, (int, float)) or (isinstance(v, float) and math.isnan(v)):
        return f"{col.name}: 숫자여야 함"
    if col.type == "int" and not (isinstance(v, int) or float(v).is_integer()):
        return f"{col.name}: 정수여야 함"
    lo, hi = _bound(col, cfg, "min"), _bound(col, cfg, "max")
    if lo is not None and (v <= lo if col.min_exclusive else v < lo):
        return f"{col.name}: {v} {'≤' if col.min_exclusive else '<'} {lo}"
    if hi is not None and v > hi:
        return f"{col.name}: {v} > {hi}"
    return None


def _empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _check_cols(cols: tuple, row: Mapping[str, Any], cfg: Mapping[str, Any]) -> list:
    errs = []
    known = {c.name for c in cols}
    for k in row:
        if k not in known:
            errs.append(f"{k}: 모르는 열")
    for c in cols:
        v = row.get(c.name)
        need = c.required == "always" or (c.required.startswith("if:") and row.get(c.required[3:]) is True)
        if _empty(v):
            if need:
                errs.append(f"{c.name}: 값 필요")
            continue
        e = _check_col(c, v, cfg)
        if e:
            errs.append(e)
    return errs


def pulse_freq_hz(freq_khz: Any) -> Optional[int]:
    """레시피 rf_pulse_freq(kHz) → 장비 Hz — v1 controller/process_controller.py:868-869 int(round(kHz × 1000))."""
    return None if _empty(freq_khz) else int(round(float(freq_khz) * 1000.0))


def pulse_edge_violation(freq_hz: float, duty_pct: float, min_edge_us: float) -> Optional[str]:
    """ON·OFF 시간 ≥ min_edge_us (CESAR 명령 96). v1 lib/config.py rfpulse_pulse_edge_violation."""
    if min_edge_us <= 0 or freq_hz <= 0:
        return None
    on_us = duty_pct / 100.0 / freq_hz * 1e6
    off_us = (100.0 - duty_pct) / 100.0 / freq_hz * 1e6
    if on_us >= min_edge_us - 1e-6 and off_us >= min_edge_us - 1e-6:
        return None
    return f"{freq_hz:g} Hz·{duty_pct:g}% → ON {on_us:.1f} µs / OFF {off_us:.1f} µs < {min_edge_us:g} µs"


def check_process_row(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> tuple:
    """(정리된 행 또는 None, 오류 목록, 경고 목록). 딜레이 행은 {'delay_s': 초}."""
    warns: list = []
    name = row.get("process_name")
    d = delay_seconds(name)
    if d is not None:
        extra = [k for k, v in row.items() if k != "process_name" and not _empty(v) and v is not False]
        if extra:
            return None, [f"딜레이 행에 다른 값: {extra}"], warns
        return {"delay_s": d, "process_name": name}, [], warns

    errs = _check_cols(PROCESS_COLUMNS, row, cfg)
    if errs:
        return None, errs, warns
    r = dict(row)
    if not (r.get("ar") is True or r.get("o2") is True):
        errs.append("가스: Ar 또는 O2 중 하나 이상")
    pt, sd = float(r.get("process_time") or 0), float(r.get("shutter_delay") or 0)
    if pt <= 0 and sd <= 0:
        errs.append("시간: process_time 또는 shutter_delay 중 하나는 0 보다 커야 함")
    if not any(r.get(k) is True for k in ("use_dc", "use_rf", "use_rf_pulse")):
        errs.append("파워: DC·RF·RF Pulse 중 하나 이상")
    if r.get("use_dc_delay") is True and r.get("use_dc") is not True:
        warns.append("use_dc_delay 는 DC 를 쓸 때만 — 무시")
        r["use_dc_delay"] = False
    bp = r.get("base_pressure")
    if not _empty(bp) and bp != 0 and not cfg["IG_ENABLED"]:
        errs.append("base_pressure: 이온 게이지가 꺼져 있음(IG_ENABLED=false)")
    if r.get("use_rf_pulse") is True and not cfg.get("RFPULSE_ENABLED", True):
        errs.append("RF Pulse: RFPULSE_ENABLED=false")
    if r.get("use_rf_pulse") is True:
        f, du = r.get("rf_pulse_freq"), r.get("rf_pulse_duty")
        if not _empty(f):
            hz = float(f) * 1000.0                      # v1 check_rfpulse_range: kHz × 1000 을 Hz 범위와 비교
            lo, hi = float(cfg["RFPULSE_PULSE_FREQ_MIN_HZ"]), float(cfg["RFPULSE_PULSE_FREQ_MAX_HZ"])
            if not lo <= hz <= hi:
                errs.append(f"RF Pulse: 주파수 {float(f):g} kHz 가 범위 {lo / 1000.0:g}~{hi / 1000.0:g} kHz 밖")
        if not _empty(f) and not _empty(du):
            e = pulse_edge_violation(float(pulse_freq_hz(f)), float(du), cfg["RFPULSE_PULSE_MIN_EDGE_US"])
            if e:
                errs.append("RF Pulse: " + e)
    if r.get("use_heater") is True:
        if not cfg["HEATER_ENABLED"]:
            errs.append("히터: HEATER_ENABLED=false")
        ramp = r.get("heater_ramp")
        if _empty(ramp) or ramp == 0:
            r["heater_ramp"] = cfg["HEATER_RAMP_RATE_C_PER_MIN"]
        elif ramp < 6.0:
            warns.append(f"heater_ramp {ramp} < 6 → 6 (래더 분해능)")
            r["heater_ramp"] = 6.0
    return (None if errs else r), errs, warns


def check_heater_recipe(steps: list, gas: Optional[Mapping[str, Any]], cfg: Mapping[str, Any]) -> tuple:
    """(오류 목록, 경고 목록). v1 controller/heater_recipe.py load 규칙."""
    errs, warns = [], []
    if not steps:
        errs.append("스텝 없음")
    if len(steps) > cfg["HEATER_RECIPE_MAX_STEPS"]:
        errs.append(f"스텝 {len(steps)} > HEATER_RECIPE_MAX_STEPS {cfg['HEATER_RECIPE_MAX_STEPS']}")
    for i, s in enumerate(steps, 1):
        for e in _check_cols(HEATER_STEP_COLUMNS, s, cfg):
            errs.append(f"스텝 {i}: {e}")
        if not _empty(s.get("ramp_c_per_min")) and not _empty(s.get("ramp_min")):
            errs.append(f"스텝 {i}: ramp_c_per_min 과 ramp_min 을 함께 쓸 수 없음")
        r = s.get("ramp_c_per_min")
        if isinstance(r, (int, float)) and not isinstance(r, bool) and 0 < r < 6:
            warns.append(f"스텝 {i}: 램프 {r} < 6 — 파이썬 램프로 실행")
    if gas:
        errs += [f"가스: {e}" for e in _check_cols(HEATER_GAS_COLUMNS, gas, cfg)]
        if (gas.get("use_ar") is True or gas.get("use_o2") is True) and _empty(gas.get("wp_mtorr")):
            errs.append("가스: 가스를 쓰면 wp_mtorr 필요")
    return errs, warns
