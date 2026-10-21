"""챔버K 블록 공정 — 레시피 = 블록 목록(JSON). 블록 = {"type", "params", "label"(선택), "stage"(선택)}, 반복은 "blocks".

사용자 결정 2026-10-20(5-4)
- 블록 순서·조합은 자유 — 순서를 막거나 순서 때문에 더 검사하는 규칙은 없다. 검사는 블록 값의 범위(파라미터 상한 연결)와
  설정에서 꺼진 장비(HEATER_ENABLED·IG_ENABLED·RFPULSE_ENABLED)뿐.
- "시작 전 정리"·"정상 끝 정지"도 일반 블록 — 기본 템플릿에 들어 있고 빼거나 바꿀 수 있다.
  정상 끝 정지가 없는 목록은 공정이 끝날 때 켜져 있는 출력(DC·RF·RF Pulse·유량·히터)을 경고 이벤트로만 남긴다(끄지 않음).
- 운전 중 장비 보호(SafetyEngine 규칙)는 순서와 무관 — 적용 구간은 공정 상태 이름이 아니라 블록·장비 상태:
  무출력(S31A·S33A) = 그 장비가 램프 중, 두절·반사파(S30·S34C·S32) = 그 장비 출력 명령 뒤,
  이탈 감시(S31C·S33B·S34A·S38A·S38B) = 지금 블록의 watch(대기·메인·조건 대기 블록이 정한다, 그 밖의 블록은 WATCH_DEFAULT).

기본 템플릿(template_blocks): v1 행(recipe.PROCESS_COLUMNS) → v1 _build_steps(controller/process_controller.py:486-682)와
같은 순서의 블록 목록. 펼치면 지금(5-3)과 같은 스텝이 나온다 — 골든이 이 템플릿으로 비교한다.

펼치기(expand): 블록 → process_steps.ProcessStep 목록(실행은 지금 StepRunner·드라이버 경로 그대로).
  스텝마다 block(블록 순번 — insts 의 번호)과 ref(공정 중 수정 항목 키 — 5-4 ③)를 단다.
  반복은 펼칠 때 풀어 쓴다(중첩 가능). 블록 번호는 위에서부터 "3", 반복 안은 "5.2#2"(5번 반복의 2번째 블록, 2회째).
  값을 비운 항목의 설정 기본값("cfg:키")은 펼칠 때(공정 시작 때) 설정에서 읽는다 — 지금 행 공정과 같다.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from v2.chk.process_steps import (ProcessStep, build_gas_intro_steps, build_gas_pre_steps, channels_of, heater_used,
                                  base_pressure_of)
from v2.chk.recipe import pulse_edge_violation

S = ProcessStep

# ───────────── 감시(이탈 판정) ─────────────
# 항목 → 규칙. 기본값은 v1 과 같은 판정: RF·RF Pulse 이탈은 v1 도 유지 중 내내(RFpower.py:150-167, RFpulse.py:1240),
# DC 이탈은 셔터 딜레이부터(DCpower.py:659-675 arm), 유량·압력 이탈은 메인 공정에서만(MFC.py:1300-1342).
WATCH_ITEMS: tuple = (
    ("dc", "S31C", "DC 파워 이탈"),
    ("rf", "S33B", "RF 파워 이탈"),
    ("rfp", "S34A", "RF Pulse 순방향 이탈"),
    ("flow", "S38A", "가스 유량 이탈(경고)"),
    ("pressure", "S38B", "압력 이탈(경고)"),
)
WATCH_DEFAULT: Dict[str, bool] = {"dc": False, "rf": True, "rfp": True, "flow": False, "pressure": False}
WATCH_MAIN: Dict[str, bool] = {"dc": True, "rf": True, "rfp": True, "flow": True, "pressure": True}

# 조건 대기 블록이 볼 수 있는 스냅샷 값(snapshot_fields.columns() 의 열 — 시험이 확인)
COND_FIELDS: tuple = (
    ("mfc.pressure_mtorr", "챔버 압력", "mTorr"), ("mfc.ar_sccm", "Ar 유량", "sccm"), ("mfc.o2_sccm", "O2 유량", "sccm"),
    ("heater.pv", "히터 온도(TC1)", "°C"), ("heater.pv2", "히터 온도(TC2)", "°C"), ("ig.ion_torr", "이온 게이지 압력", "Torr"),
    ("dc.p", "DC 파워", "W"), ("dc.v", "DC 전압", "V"), ("dc.i", "DC 전류", "A"),
    ("rf.fwd_w", "RF 순방향", "W"), ("rf.ref_w", "RF 반사", "W"), ("rfp.fwd_w", "RF Pulse 순방향", "W"),
    ("rfp.ref_w", "RF Pulse 반사", "W"),
)
COND_OPS: tuple = (("le", "≤ 값"), ("ge", "≥ 값"), ("near", "값 ± 허용"))

SHUTTERS: tuple = (("g1", "S1_button", "Gun Shutter 1"), ("g2", "S2_button", "Gun Shutter 2"), ("ms", "MS_button", "Main Shutter"))
SP_DEFAULT_KEY = {2: "SP2_MTORR", 3: "SP3_MTORR"}

REPEAT_MAX = 100
MAX_DEPTH = 4
MAX_EXPANDED_STEPS = 3000
LABEL_MAX = 60


# ───────────── 정의 표 ─────────────
@dataclass(frozen=True)
class BParam:
    name: str
    type: str                          # float / int / bool / enum / str / watch / list / steps
    unit: str = ""
    default: Any = None                # None = 비움 / "cfg:키" = 펼칠 때 설정값
    required: bool = False
    min: Optional[float] = None
    max: Optional[float] = None
    param_min: Optional[str] = None    # 범위를 파라미터 값에서(× param_scale)
    param_max: Optional[str] = None
    param_scale: float = 1.0
    min_exclusive: bool = False
    enum: Optional[tuple] = None
    desc: str = ""

    def bounds(self, cfg: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        lo = float(cfg[self.param_min]) * self.param_scale if self.param_min else self.min
        hi = float(cfg[self.param_max]) * self.param_scale if self.param_max else self.max
        return lo, hi

    def public(self) -> dict:
        return {"name": self.name, "type": self.type, "unit": self.unit, "default": self.default,
                "required": self.required, "min": self.min, "max": self.max, "paramMin": self.param_min,
                "paramMax": self.param_max, "paramScale": self.param_scale, "minExclusive": self.min_exclusive,
                "enum": list(self.enum) if self.enum else None, "desc": self.desc}


@dataclass(frozen=True)
class BlockDef:
    type: str
    label: str
    params: tuple
    stage: str                         # 화면·기록용 공정 상태 이름(process_states) — 기본값
    stages: tuple = ()                 # "stage" 로 고를 수 있는 다른 이름(기본 템플릿이 v1 상태 이름을 내려고)
    watch: Optional[Dict[str, bool]] = None   # 감시 값을 쓰는 블록이면 그 기본값
    container: bool = False            # 반복 — 안에 "blocks"
    steps: str = ""                    # 펼친 스텝(사람이 읽는 것)
    v1: str = ""
    desc: str = ""

    def public(self) -> dict:
        return {"type": self.type, "label": self.label, "params": [p.public() for p in self.params],
                "stage": self.stage, "stages": list(self.stages), "watch": dict(self.watch) if self.watch else None,
                "container": self.container, "steps": self.steps, "v1": self.v1, "desc": self.desc}


P = BParam
_ONOFF = ("open", "close")
_ON_TIMEOUT = ("continue", "abort")
_WATCH = lambda d: P("watch", "watch", "", None, desc="이탈 감시 켜기/끄기 {dc, rf, rfp, flow, pressure} — 빈 항목은 블록 기본값")

BLOCK_DEFS: tuple = (
    BlockDef("pre_clean", "시작 전 정리", (
        P("dc", "bool", default=True, desc="DC 출력 OFF"), P("rf", "bool", default=True, desc="RF 출력 OFF(즉시 0)"),
        P("rfpulse", "bool", default=True, desc="RF Pulse OFF(RFPULSE_ENABLED 일 때만)"),
        P("settle_sec", "float", "s", "cfg:PRE_POWER_OFF_SETTLE_SEC", min=0.0, max=30.0, desc="OFF 뒤 대기(정수 초로 반올림)")),
        "PRE_CLEAN", steps="DC_POWER_STOP · RF_POWER_STOP · RF_PULSE_STOP · DELAY", v1="pc:514-519",
        desc="이전 공정 잔류 출력 정리"),
    BlockDef("gas_prep", "가스 준비", (
        P("ar", "bool", default=False), P("o2", "bool", default=False)),
        "GAS_PRE", steps="채널별 FLOW_OFF · MFC 밸브 열기", v1="pc:522 build_gas_pre_steps"),
    BlockDef("heater_ramp", "히터 승온", (
        P("target_c", "float", "°C", required=True, min=0.0, min_exclusive=True, param_max="HEATER_MAX_TEMP"),
        P("ramp_c_per_min", "float", "°C/min", None, min=0.0, param_max="HEATER_RECIPE_RAMP_MAX_C_PER_MIN",
          desc="비우거나 0 이면 HEATER_RAMP_RATE_C_PER_MIN, 6 미만은 6(래더 분해능)")),
        "HEATER_RAMP", steps="HEATER_RAMP(D00020) · HEATER_SET(감속 접근·RUN)", v1="pc:534-545",
        desc="HEATER_ENABLED 가 꺼져 있으면 시작 거부(DEVICE_DISABLED)"),
    BlockDef("heater_wait", "히터 도달·유지 대기", (),
        "HEATER_RAMP", ("HEATER_HOLD_WAIT",), steps="HEATER_WAIT — ±HEATER_SOAK_TOLERANCE 를 HEATER_SOAK_TIME_SEC, 유지 모드 진입",
        v1="pc:546-550, 1295-1530", desc="지금 히터 목표(마지막 승온 블록·공정 중 수정)에 도달·유지할 때까지. 목표가 없으면 경고 뒤 다음"),
    BlockDef("base_wait", "베이스 압력 대기(IG)", (
        P("target_torr", "float", "Torr", required=True, param_min="BASE_PRESSURE_MIN_TORR", param_max="BASE_PRESSURE_MAX_TORR"),),
        "BASE_WAIT", steps="IG_FILAMENT_ON · IG_BASE_WAIT · IG_FILAMENT_OFF", v1="pc:1036-1273",
        desc="IG_ENABLED 가 꺼져 있으면 시작 거부(DEVICE_DISABLED)"),
    BlockDef("gas_intro", "가스 도입", (
        P("ar", "bool", default=False), P("ar_flow", "float", "sccm", None, min=0.0, min_exclusive=True, desc="ar 이면 필요"),
        P("o2", "bool", default=False), P("o2_flow", "float", "sccm", None, min=0.0, min_exclusive=True, desc="o2 이면 필요"),
        P("zero", "bool", default=True, desc="MFC·압력 영점")),
        "GAS_INTRO", steps="MFC_ZEROING · PS_ZEROING · 차단밸브 열기 · FLOW_SET · FLOW_ON(유량 안정화)",
        v1="pc:560 build_gas_intro_steps"),
    BlockDef("pressure", "압력 설정", (
        P("sp", "int", "", required=True, min=1, max=4, desc="설정점 번호"),
        P("value", "float", "mTorr", None, min=0.0, max=1000.0, min_exclusive=True,
          desc="비우면 SP2·SP3 은 SP2_MTORR·SP3_MTORR, SP1 은 필요(레시피 작업 압력)"),
        P("set", "bool", default=True, desc="S{n} 쓰기(되읽기)"), P("select", "bool", default=True, desc="D{n} 선택(R7 확인)"),
        P("wait", "bool", default=True, desc="도달 대기"),
        P("tol_ratio", "float", "비율", "cfg:PRESSURE_WAIT_TOL_RATIO", min=0.01, max=0.5),
        P("stable_sec", "float", "s", "cfg:PRESSURE_WAIT_STABLE_SEC", min=0.0, max=600.0),
        P("timeout_sec", "float", "s", None, min=0.0, max=7200.0, min_exclusive=True,
          desc="비우면 SP1 은 SP1_TIMEOUT_SEC, 그 밖은 PRESSURE_STAGE_TIMEOUT_SEC"),
        P("on_timeout", "enum", "", None, enum=_ON_TIMEOUT, desc="비우면 SP1 은 abort(SP1_TIMEOUT), 그 밖은 continue(경고)")),
        "PRESSURE_SP1", ("PRESSURE_SP2", "PRESSURE_SP3", "PRESSURE_SP4", "GAS_STABILIZE"),
        steps="SPn_SET · SPn_ON · WAIT_PRESSURE", v1="pc:565-630", desc="stage 를 비우면 PRESSURE_SP{n}"),
    BlockDef("valve", "MFC 밸브", (P("action", "enum", "", required=True, enum=("open", "close", "hold"),
                                    desc="O 열기 / C 닫기 / H 그 자리 정지 [MFC p.74]"),),
        "VALVE", steps="VALVE_OPEN(R6 확인) · VALVE_CLOSE(R6 확인) · VALVE_HOLD", v1="MFC.py:1172-1211(O·C)"),
    BlockDef("shutter", "셔터", (
        P("action", "enum", "", required=True, enum=_ONOFF),
        P("targets", "list", "", required=True, enum=tuple(s[0] for s in SHUTTERS), desc="g1·g2·ms 중 하나 이상(이 순서로)")),
        "SHUTTER", ("GUN_SHUTTER", "MAIN"), steps="PLC_CMD(S1·S2·MS) — 쓰기 뒤 0.8 s", v1="pc:582-597"),
    BlockDef("power_on", "파워 켜기", (
        P("dc_power", "float", "W", None, min=0.0, min_exclusive=True, param_max="DC_MAX_POWER"),
        P("rf_power", "float", "W", None, min=0.0, min_exclusive=True, param_max="RF_MAX_POWER"),
        P("rfp_power", "float", "W", None, min=0.0, min_exclusive=True, param_max="RFPULSE_MAX_POWER"),
        P("rfp_freq_khz", "float", "kHz", None, param_min="RFPULSE_PULSE_FREQ_MIN_HZ", param_max="RFPULSE_PULSE_FREQ_MAX_HZ",
          param_scale=0.001, desc="장비에는 Hz = round(kHz × 1000). 비우면 장비값 유지"),
        P("rfp_duty", "int", "%", None, param_min="RFPULSE_DUTY_MIN", param_max="RFPULSE_DUTY_MAX"),
        P("wait", "bool", default=True, desc="목표 도달까지 기다림"),
        P("timeout_sec", "float", "s", None, min=0.0, max=14400.0, min_exclusive=True,
          desc="비우면 process_states.FORMULAS['POWER_WAIT']")),
        "POWER_WAIT", steps="POWER_WAIT(도달 대기) 또는 POWER_START(켜기만)", v1="pc:941-997",
        desc="DC·RF·RF Pulse 중 값이 있는 것을 함께 시작. 이미 켜진 장비는 다시 켜지 않고 목표만 바꾼다"),
    BlockDef("power_off", "파워 끄기", (
        P("dc", "bool", default=False), P("rf", "bool", default=False, desc="RF 는 램프다운 뒤 OFF"),
        P("rfpulse", "bool", default=False)),
        "POWER_OFF", steps="DC_POWER_STOP · RF_RAMP_OFF · RF_PULSE_STOP", v1="pc:1924-1981"),
    BlockDef("gas_off", "가스 끄기", (
        P("ar", "bool", default=False), P("o2", "bool", default=False), P("close_valves", "bool", default=True, desc="차단밸브 닫기")),
        "GAS_OFF", steps="FLOW_OFF · 차단밸브 닫기", v1="heater_atmosphere 해제 순서"),
    BlockDef("heater_off", "히터 끄기", (), "HEATER_OFF", steps="HEATER_OFF(RUN=0·램프 원복)", v1="pc _heater_off"),
    BlockDef("wait", "대기", (P("sec", "float", "s", required=True, min=0.0, max=7 * 86400.0), _WATCH(None)),
        "WAIT", ("GAS_STABILIZE", "DC_DELAY", "SHUTTER_DELAY"), watch=dict(WATCH_DEFAULT), steps="DELAY",
        v1="pc:565-579·632-657", desc="공정 중 수정 process.skip_wait 로 바로 끝낼 수 있다"),
    BlockDef("main", "메인", (
        P("sec", "float", "s", required=True, min=0.0, max=7 * 86400.0, min_exclusive=True),
        P("close", "bool", default=True, desc="끝에 메인 셔터 닫기(기본 템플릿은 정지 순서가 닫는다)"), _WATCH(None)),
        "MAIN", watch=dict(WATCH_MAIN), steps="PLC_CMD MS 열기 · DELAY · [MS 닫기]", v1="pc:659-680",
        desc="공정 중 수정 process.main_remaining 으로 남은 시간을 바꾼다"),
    BlockDef("cond_wait", "조건 대기", (
        P("field", "enum", "", required=True, enum=tuple(f[0] for f in COND_FIELDS)),
        P("op", "enum", "", required=True, enum=tuple(o[0] for o in COND_OPS)),
        P("value", "float", "", required=True),
        P("tol", "float", "", 0.0, min=0.0, desc="near 의 허용(같은 단위)"),
        P("stable_sec", "float", "s", 0.0, min=0.0, max=3600.0, desc="조건이 이 시간 이어지면 통과"),
        P("timeout_sec", "float", "s", required=True, min=0.0, max=7 * 86400.0, min_exclusive=True),
        P("on_timeout", "enum", "", "abort", enum=_ON_TIMEOUT, desc="abort = COND_WAIT_TIMEOUT 중단, continue = 경고 뒤 다음"),
        _WATCH(None)),
        "COND_WAIT", watch=dict(WATCH_DEFAULT), steps="COND_WAIT", v1="v2",
        desc="값을 모르는 tick(stale·fail)은 판정을 바꾸지 않는다(judge.Persist)"),
    BlockDef("repeat", "반복", (P("count", "int", "회", required=True, min=1, max=REPEAT_MAX),),
        "", container=True, steps="안의 블록을 count 번(중첩 가능)", v1="v2"),
    BlockDef("end_stop", "정상 끝 정지", (
        P("skip", "steps", "", None, desc="건너뛸 정지 순서 단계 번호(safety_rules.STOP_SEQUENCE). 순서는 STOP_SEQUENCE_ORDER"),),
        "STOPPING", steps="SafetyEngine.end_row — 지금 행 끝 정지 순서", v1="pc:1785-2019",
        desc="기본 템플릿의 마지막. 중간에 두면 정지 순서 뒤 다음 블록으로"),
)


def by_type() -> Dict[str, BlockDef]:
    return {b.type: b for b in BLOCK_DEFS}


# 안전 규칙이 블록 종류를 모를 때(SimpleProcess — 상태 이름만 정하는 시험) 상태 이름 → 블록 종류
STAGE_TO_TYPE: Dict[str, str] = {
    "PRE_CLEAN": "pre_clean", "GAS_PRE": "gas_prep", "HEATER_RAMP": "heater_wait", "HEATER_HOLD_WAIT": "heater_wait",
    "BASE_WAIT": "base_wait", "GAS_INTRO": "gas_intro", "GAS_STABILIZE": "wait", "GUN_SHUTTER": "shutter",
    "POWER_WAIT": "power_on", "PRESSURE_SP1": "pressure", "PRESSURE_SP2": "pressure", "PRESSURE_SP3": "pressure",
    "PRESSURE_SP4": "pressure", "DC_DELAY": "wait", "SHUTTER_DELAY": "wait", "MAIN": "main", "WAIT": "wait",
    "COND_WAIT": "cond_wait", "VALVE": "valve", "SHUTTER": "shutter", "POWER_OFF": "power_off", "GAS_OFF": "gas_off",
    "HEATER_OFF": "heater_off",
}


def watch_for_state(state: str) -> Dict[str, bool]:
    """기본 템플릿의 감시(상태 이름 기준) — SimpleProcess 가 쓴다. 템플릿 블록의 watch 와 같다."""
    w = dict(WATCH_DEFAULT)
    if state == "SHUTTER_DELAY":
        w["dc"] = True
    if state == "MAIN":
        w = dict(WATCH_MAIN)
    return w


# ───────────── 검사 ─────────────
def _empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and not (isinstance(v, float) and math.isnan(v))


def _check_param(p: BParam, v: Any, cfg: Mapping[str, Any], stop_nos: tuple) -> Optional[str]:
    t = p.type
    if t == "bool":
        return None if isinstance(v, bool) else "true/false 여야 함"
    if t == "str":
        return None if isinstance(v, str) else "문자열이어야 함"
    if t == "enum":
        return None if v in (p.enum or ()) else f"{list(p.enum or ())} 중 하나"
    if t == "list":
        if not isinstance(v, list) or not v or any(x not in (p.enum or ()) for x in v) or len(set(v)) != len(v):
            return f"{list(p.enum or ())} 중 하나 이상(중복 없음)"
        return None
    if t == "steps":
        if not isinstance(v, list) or any(not isinstance(x, int) or isinstance(x, bool) or x not in stop_nos for x in v):
            return f"정지 순서 단계 번호 목록({min(stop_nos)}~{max(stop_nos)})"
        return None
    if t == "watch":
        if not isinstance(v, dict) or any(k not in WATCH_DEFAULT or not isinstance(x, bool) for k, x in v.items()):
            return f"{{{', '.join(WATCH_DEFAULT)}}} 의 true/false"
        return None
    if not _num(v):
        return "숫자여야 함"
    if t == "int" and not float(v).is_integer():
        return "정수여야 함"
    lo, hi = p.bounds(cfg)
    if lo is not None and (v <= lo if p.min_exclusive else v < lo - 1e-12):
        return f"{v} {'≤' if p.min_exclusive else '<'} {lo:g}"
    if hi is not None and v > hi + 1e-12:
        return f"{v} > {hi:g}"
    return None


def _check_one(b: Any, path: str, cfg: Mapping[str, Any], depth: int, errs: list, warns: list, stop_nos: tuple) -> Optional[dict]:
    defs = by_type()
    if not isinstance(b, dict):
        errs.append(f"블록 {path}: 객체여야 함")
        return None
    d = defs.get(b.get("type"))
    if d is None:
        errs.append(f"블록 {path}: 모르는 종류 {b.get('type')!r}")
        return None
    pre = f"블록 {path}({d.type})"
    extra = set(b) - {"type", "params", "label", "stage", "blocks"}
    if extra:
        errs.append(f"{pre}: 모르는 키 {sorted(extra)}")
    params = b.get("params") or {}
    if not isinstance(params, dict):
        errs.append(f"{pre}: params 는 객체")
        return None
    known = {p.name for p in d.params}
    for k in params:
        if k not in known:
            errs.append(f"{pre}: {k}: 모르는 값")
    out: Dict[str, Any] = {}
    for p in d.params:
        v = params.get(p.name)
        if _empty(v):
            if p.required:
                errs.append(f"{pre}: {p.name}: 값 필요")
            out[p.name] = p.default
            continue
        e = _check_param(p, v, cfg, stop_nos)
        if e:
            errs.append(f"{pre}: {p.name}: {e}")
        out[p.name] = v
    label = b.get("label")
    if label is not None and (not isinstance(label, str) or len(label) > LABEL_MAX):
        errs.append(f"{pre}: label 은 {LABEL_MAX}자 이하 문자열")
    stage = b.get("stage")
    if stage is not None and stage not in (d.stage,) + d.stages:
        errs.append(f"{pre}: stage 는 {[d.stage] + list(d.stages)} 중 하나")
    nb = {"type": d.type, "params": out, "label": label or "", "stage": stage or ""}
    # 값 사이(같은 블록 안) — 순서 규칙이 아니라 값의 짝
    t = d.type
    if t == "gas_intro":
        for g in ("ar", "o2"):
            if out[g] is True and _empty(out[f"{g}_flow"]):
                errs.append(f"{pre}: {g}_flow: {g} 이면 값 필요")
    if t == "pressure" and out["set"] is True and _empty(out["value"]) and out["sp"] not in SP_DEFAULT_KEY:
        errs.append(f"{pre}: value: SP{out['sp']} 는 값 필요")
    if t == "heater_ramp" or t == "heater_wait" or t == "heater_off":
        if not cfg["HEATER_ENABLED"]:
            errs.append(f"{pre}: 히터: HEATER_ENABLED=false")
    if t == "heater_ramp":
        r = out["ramp_c_per_min"]
        if _num(r) and 0 < r < 6.0:
            warns.append(f"{pre}: ramp_c_per_min {r} < 6 → 6 (래더 분해능)")
    if t == "base_wait" and not cfg["IG_ENABLED"]:
        errs.append(f"{pre}: base_pressure: 이온 게이지가 꺼져 있음(IG_ENABLED=false)")
    if t == "cond_wait" and str(out.get("field") or "").startswith("ig.") and not cfg["IG_ENABLED"]:
        errs.append(f"{pre}: 조건 값 {out['field']}: 이온 게이지가 꺼져 있음(IG_ENABLED=false)")
    if t == "power_on":
        if _num(out["rfp_power"]) and not cfg.get("RFPULSE_ENABLED", True):
            errs.append(f"{pre}: RF Pulse: RFPULSE_ENABLED=false")
        f, du = out["rfp_freq_khz"], out["rfp_duty"]
        if _num(f) and _num(du):
            e = pulse_edge_violation(float(round(float(f) * 1000.0)), float(du), cfg["RFPULSE_PULSE_MIN_EDGE_US"])
            if e:
                errs.append(f"{pre}: RF Pulse: {e}")
    if d.container:
        kids = b.get("blocks")
        if depth >= MAX_DEPTH:
            errs.append(f"{pre}: 반복 중첩은 {MAX_DEPTH}단까지")
        elif not isinstance(kids, list) or not kids:
            errs.append(f"{pre}: blocks: 블록 목록 필요")
        else:
            nb["blocks"] = [x for x in (_check_one(k, f"{path}.{i}", cfg, depth + 1, errs, warns, stop_nos)
                                        for i, k in enumerate(kids, 1)) if x is not None]
    elif "blocks" in b:
        errs.append(f"{pre}: blocks 는 반복 블록만")
    return nb


def check_blocks(blocks: Any, cfg: Mapping[str, Any]) -> tuple:
    """(정리된 블록 목록 또는 None, 오류, 경고). 값 범위·꺼진 장비만 본다 — 순서·조합 규칙은 없다(사용자 결정 2026-10-20)."""
    from v2.chk.safety_rules import STOP_SEQUENCE
    stop_nos = tuple(s.no for s in STOP_SEQUENCE)
    errs: list = []
    warns: list = []
    if not isinstance(blocks, list) or not blocks:
        return None, ["블록 목록이 비었다"], warns
    out = [x for x in (_check_one(b, str(i), cfg, 0, errs, warns, stop_nos) for i, b in enumerate(blocks, 1)) if x is not None]
    if not errs:
        try:
            steps, _ = expand(out, cfg)
        except ValueError as e:
            errs.append(str(e))
    return (None if errs else out), errs, warns


def uses_of(blocks: List[dict]) -> Dict[str, Any]:
    """블록 목록이 쓰는 장비 — 시작 거부(DEVICE_DISABLED·LINK_DOWN)·안전 규칙 적용·정지 순서 조건."""
    u = {"dc": False, "rf": False, "rfp": False, "heater": False, "ig": False, "ar": False, "o2": False}

    def walk(bs):
        for b in bs:
            t, p = b["type"], b["params"]
            if t == "power_on":
                u["dc"] |= _num(p.get("dc_power")) and p["dc_power"] > 0
                u["rf"] |= _num(p.get("rf_power")) and p["rf_power"] > 0
                u["rfp"] |= _num(p.get("rfp_power")) and p["rfp_power"] > 0
            elif t == "heater_ramp":
                u["heater"] |= _num(p.get("target_c")) and p["target_c"] > 0
            elif t == "base_wait":
                u["ig"] = True
            elif t == "cond_wait" and str(p.get("field", "")).startswith("ig."):
                u["ig"] = True
            elif t in ("gas_intro", "gas_prep", "gas_off"):
                u["ar"] |= p.get("ar") is True
                u["o2"] |= p.get("o2") is True
            if b.get("blocks"):
                walk(b["blocks"])
    walk(blocks)
    return u


# ───────────── 기본 템플릿(v1 행) ─────────────
def template_blocks(p: Dict[str, Any], cfg: Mapping[str, Any], *, ig_enabled: bool, rfpulse_present: bool = True) -> List[dict]:
    """v1 행 params(process_steps.row_to_params) → 기본 템플릿 블록 목록. v1 _build_steps(486-682)의 순서·조건 그대로."""
    B = lambda t, label="", stage="", **params: {"type": t, "params": params, "label": label, "stage": stage}
    channels, _ = channels_of(p)
    ar, o2 = 1 in channels, 2 in channels
    default_flow = float(p.get("mfc_flow", 0.0) or 0.0)
    sp1 = float(p.get("sp1_set", 0.0) or 0.0)
    out = [B("pre_clean", dc=True, rf=True, rfpulse=bool(rfpulse_present), settle_sec=None),
           B("gas_prep", ar=ar, o2=o2)]
    if heater_used(p):
        temp = float(p["heater_temp"])
        ramp = float(p.get("heater_ramp") or 0.0) or float(cfg["HEATER_RAMP_RATE_C_PER_MIN"])
        out += [B("heater_ramp", target_c=temp, ramp_c_per_min=ramp), B("heater_wait")]
    bp = base_pressure_of(p)
    if bp > 0 and ig_enabled:
        out.append(B("base_wait", target_torr=bp))
    out.append(B("gas_intro", ar=ar, ar_flow=float(p.get("ar_flow", default_flow) or 0.0) if ar else None,
                 o2=o2, o2_flow=float(p.get("o2_flow", default_flow) or 0.0) if o2 else None, zero=True))
    stab = int(round(cfg["GAS_STABILIZE_SEC"]))
    out.append(B("pressure", "SP4", "GAS_STABILIZE", sp=4, value=None, set=False, select=True, wait=False,
                 tol_ratio=None, stable_sec=None, timeout_sec=None, on_timeout=None))
    out.append(B("wait", f"압력 안정화 대기({stab}초)", "GAS_STABILIZE", sec=stab, watch=None))
    guns = [g for g, k in (("g1", "use_g1"), ("g2", "use_g2")) if p.get(k)]
    if guns:
        out.append(B("shutter", "건 셔터 열기", "GUN_SHUTTER", action="open", targets=guns))
    dc_w, rf_w = float(p.get("dc_power", 0.0) or 0.0), float(p.get("rf_power", 0.0) or 0.0)
    rfp_w = float(p.get("rf_pulse_power") or 0) if p.get("use_rf_pulse") else 0.0
    fhz = p.get("rf_pulse_freq_hz")
    out.append(B("power_on", dc_power=dc_w or None, rf_power=rf_w or None, rfp_power=rfp_w or None,
                 rfp_freq_khz=(fhz / 1000.0) if (rfp_w and fhz is not None) else None,
                 rfp_duty=p.get("rf_pulse_duty") if rfp_w else None, wait=True, timeout_sec=None))
    sp3, sp2 = float(cfg["SP3_MTORR"]), float(cfg["SP2_MTORR"])
    for n, spv, lab in ((3, sp3, "Stage1(SP3)"), (2, sp2, "Stage2(SP2)")):
        if sp1 < spv:
            out.append(B("pressure", lab, f"PRESSURE_SP{n}", sp=n, value=spv, set=True, select=True, wait=True,
                         tol_ratio=None, stable_sec=None, timeout_sec=None, on_timeout=None))
    out.append(B("pressure", f"Stage3(SP1={sp1:.2f})", "PRESSURE_SP1", sp=1, value=sp1, set=True, select=True, wait=True,
                 tol_ratio=None, stable_sec=None, timeout_sec=None, on_timeout=None))
    dcd = int(cfg["DC_POWER_DELAY_SEC"])
    if p.get("use_dc_delay") and dc_w > 0.0 and dcd > 0:
        out.append(B("wait", f"DC Power 안정화 대기 {dcd}s", "DC_DELAY", sec=dcd, watch=None))
    sd = max(0, int(math.ceil(float(p.get("shutter_delay", 0.0) or 0.0) * 60)))
    if sd > 0:
        out.append(B("wait", f"Shutter Delay {sd}s", "SHUTTER_DELAY", sec=sd, watch={"dc": True}))
    pt = max(0, int(math.ceil(float(p.get("process_time", 0.0) or 0.0) * 60)))
    if float(p.get("process_time", 0.0) or 0.0) > 0 and pt > 0:
        out.append(B("main", sec=pt, close=False, watch=None))
    out.append(B("end_stop", skip=None))
    return out


# ───────────── 펼치기 ─────────────
@dataclass
class BlockInst:
    index: int
    no: str
    type: str
    label: str
    stage: str
    params: Dict[str, Any]
    watch: Dict[str, bool] = field(default_factory=dict)
    ended: bool = False

    def record(self) -> dict:
        return {"no": self.no, "type": self.type, "label": self.label, "stage": self.stage, "params": dict(self.params)}


def _cfgv(v: Any, cfg: Mapping[str, Any]) -> Any:
    if isinstance(v, str) and v.startswith("cfg:"):
        return cfg[v[4:]]
    return v


def _dur(x: float):
    x = float(x)
    return int(x) if x.is_integer() else x


class _Ctx:
    def __init__(self, cfg, ig_enabled, rfpulse_present):
        self.cfg, self.ig_enabled, self.rfpulse_present = cfg, ig_enabled, rfpulse_present
        self.steps: List[ProcessStep] = []
        self.insts: List[BlockInst] = []
        self.heater_target: Optional[float] = None


def expand(blocks: List[dict], cfg: Mapping[str, Any], *, ig_enabled: Optional[bool] = None,
           rfpulse_present: Optional[bool] = None) -> tuple:
    """블록 → (스텝 목록, 블록 목록). 블록마다 스텝이 하나 이상(값이 없어 할 일이 없으면 NOP — 블록 시작·끝 기록용)."""
    ctx = _Ctx(cfg, bool(cfg["IG_ENABLED"]) if ig_enabled is None else ig_enabled,
               bool(cfg.get("RFPULSE_ENABLED", True)) if rfpulse_present is None else rfpulse_present)
    _expand_list(ctx, blocks, "")
    return ctx.steps, ctx.insts


def _expand_list(ctx: _Ctx, blocks: List[dict], prefix: str, it: str = "") -> None:
    for i, b in enumerate(blocks, 1):
        no = f"{prefix}{i}"
        if b["type"] == "repeat":
            n = int(b["params"]["count"])
            for k in range(1, n + 1):
                _expand_list(ctx, b.get("blocks") or [], f"{no}.", f"#{k}" if not it else f"{it}.{k}")
            continue
        _expand_block(ctx, b, no + it)
        if len(ctx.steps) > MAX_EXPANDED_STEPS:
            raise ValueError(f"펼친 스텝 {len(ctx.steps)} > {MAX_EXPANDED_STEPS} — 반복 횟수를 줄일 것")


def _expand_block(ctx: _Ctx, b: dict, no: str) -> None:
    d = by_type()[b["type"]]
    c = ctx.cfg
    p = {}
    for bp in d.params:                                  # 비운 값은 기본값, "cfg:키" 는 지금 설정에서
        v = b["params"].get(bp.name)
        p[bp.name] = _cfgv(bp.default if _empty(v) else v, c)
    stage = b.get("stage") or d.stage
    if b["type"] == "pressure" and not b.get("stage"):
        stage = f"PRESSURE_SP{int(p['sp'])}"
    label = b.get("label") or ""
    watch = dict(WATCH_DEFAULT)
    if d.watch is not None:
        watch = dict(d.watch)
        watch.update(p.get("watch") or {})
    inst = BlockInst(len(ctx.insts), no, b["type"], label, stage, p, watch)
    out: List[ProcessStep] = []
    t = b["type"]
    if t == "pre_clean":
        if p["dc"]:
            out.append(S("DC_POWER_STOP", "PRE: DC Power OFF"))
        if p["rf"]:
            out.append(S("RF_POWER_STOP", "PRE: RF Power OFF"))
        if p["rfpulse"] and ctx.rfpulse_present:
            out.append(S("RF_PULSE_STOP", "PRE: RF Pulse OFF"))
        out.append(S("DELAY", "PRE: Power OFF settle", duration_sec=int(round(float(p["settle_sec"])))))
    elif t == "gas_prep":
        out += build_gas_pre_steps([ch for ch, g in ((1, "ar"), (2, "o2")) if p[g]])
    elif t == "heater_ramp":
        temp = float(p["target_c"])
        ramp = float(p["ramp_c_per_min"] or 0.0) or float(c["HEATER_RAMP_RATE_C_PER_MIN"])
        ramp = max(ramp, 6.0) if ramp > 0 else ramp
        ctx.heater_target = temp
        out.append(S("HEATER_RAMP", f"히터 램프 속도 {ramp:.0f}°C/min 설정", value=ramp, ref="heater.ramp"))
        out.append(S("HEATER_SET", f"히터 목표 {temp:.1f}°C 설정 — 승온 시작", value=temp, ref="heater.target"))
    elif t == "heater_wait":
        out.append(S("HEATER_WAIT", f"히터 온도 도달 대기 (±{c['HEATER_SOAK_TOLERANCE']:.1f}°C, "
                     f"{int(c['HEATER_SOAK_TIME_SEC'])}s 유지)", value=ctx.heater_target, ref="heater.target"))
    elif t == "base_wait":
        bp = float(p["target_torr"])
        out += [S("IG_FILAMENT_ON", "이온 게이지 필라멘트 ON"), S("IG_BASE_WAIT", f"베이스 압력 대기 (목표 {bp:.1E} Torr)", value=bp),
                S("IG_FILAMENT_OFF", "이온 게이지 필라멘트 OFF")]
    elif t == "gas_intro":
        chans, flows, btns = [], {}, []
        for ch, g, btn in ((1, "ar", "Ar_Button"), (2, "o2", "O2_Button")):
            if p[g]:
                chans.append(ch)
                btns.append(btn)
                flows[ch] = float(p[f"{g}_flow"] or 0.0)
        steps = build_gas_intro_steps(chans, flows, btns)
        if not p["zero"]:
            steps = [s for s in steps if s.params[0] not in ("MFC_ZEROING", "PS_ZEROING")]
        for s in steps:
            if s.action == "MFC_CMD" and s.params[0] in ("FLOW_SET", "FLOW_ON"):
                s.ref = "mfc.ar_flow" if s.params[1]["channel"] == 1 else "mfc.o2_flow"
        out += steps
    elif t == "pressure":
        n = int(p["sp"])
        v = p["value"]
        if _empty(v) and n in SP_DEFAULT_KEY:
            v = c[SP_DEFAULT_KEY[n]]
        v = None if _empty(v) else float(v)
        ref = "mfc.wp" if n == 1 else ""
        tol = float(p["tol_ratio"] if p["tol_ratio"] is not None else c["PRESSURE_WAIT_TOL_RATIO"])
        stable = float(p["stable_sec"] if p["stable_sec"] is not None else c["PRESSURE_WAIT_STABLE_SEC"])
        to = float(p["timeout_sec"] if p["timeout_sec"] is not None else
                   (c["SP1_TIMEOUT_SEC"] if n == 1 else c["PRESSURE_STAGE_TIMEOUT_SEC"]))
        fail = (p["on_timeout"] or ("abort" if n == 1 else "continue")) == "abort"
        tag = label or f"SP{n}"
        bare = stage == "GAS_STABILIZE"                  # v1 문구: "SP4 ON"
        if p["set"] and v is not None:
            out.append(S("MFC_CMD", f"SP{n}_SET={v:.2f} 설정" if bare else f"{tag}: SP{n}_SET={v:.2f} 설정",
                         (f"SP{n}_SET", {"value": v}), ref=ref))
        if p["select"]:
            out.append(S("MFC_CMD", f"SP{n} ON" if bare else f"{tag}: SP{n}_ON", (f"SP{n}_ON", {})))
        if p["wait"] and v is not None:
            a = {"target": v, "tolerance_ratio": tol, "stable_sec": stable, "timeout_sec": to, "fail_on_timeout": fail}
            if fail and n != 1:
                a["fail_code"] = "PRESSURE_WAIT_TIMEOUT"
            out.append(S("MFC_CMD", f"{tag}: 압력 도달 대기 (target={v:.2f}, ±{tol * 100:.0f}%, timeout={int(to)}s"
                         + (", FAIL ON TIMEOUT" if fail else "") + ")", ("WAIT_PRESSURE", a), ref=ref))
    elif t == "valve":
        cmd = {"open": "VALVE_OPEN", "close": "VALVE_CLOSE", "hold": "VALVE_HOLD"}[p["action"]]
        out.append(S("MFC_CMD", {"open": "MFC Valve Open", "close": "MFC Valve Close", "hold": "MFC Valve Hold"}[p["action"]],
                     (cmd, {})))
    elif t == "shutter":
        on = p["action"] == "open"
        for key, btn, name in SHUTTERS:
            if key in p["targets"]:
                out.append(S("PLC_CMD", f"{name} {'Open' if on else 'Close'}", (btn, on)))
    elif t == "power_on":
        dc, rf, rfp = (float(p[k]) if _num(p[k]) else 0.0 for k in ("dc_power", "rf_power", "rfp_power"))
        f = p["rfp_freq_khz"]
        spec = {"dc": dc, "rf": rf, "rfp": rfp, "freq_hz": int(round(float(f) * 1000.0)) if (rfp and _num(f)) else None,
                "duty": int(p["rfp_duty"]) if (rfp and _num(p["rfp_duty"])) else None,
                "timeout": float(p["timeout_sec"]) if _num(p["timeout_sec"]) else None, "wait": bool(p["wait"])}
        if dc or rf or rfp or p["wait"]:
            out.append(S("POWER_WAIT" if p["wait"] else "POWER_START",
                         "파워 목표치 도달 대기" if p["wait"] else "파워 켜기(도달 대기 안 함)", spec=spec))
    elif t == "power_off":
        if p["dc"]:
            out.append(S("DC_POWER_STOP", "DC Power OFF"))
        if p["rf"]:
            out.append(S("RF_RAMP_OFF", "RF 램프다운 → OFF"))
        if p["rfpulse"] and ctx.rfpulse_present:
            out.append(S("RF_PULSE_STOP", "RF Pulse OFF"))
    elif t == "gas_off":
        for ch, g in ((1, "ar"), (2, "o2")):
            if p[g]:
                out.append(S("MFC_CMD", f"Ch{ch} Flow OFF", ("FLOW_OFF", {"channel": ch})))
        if p["close_valves"]:
            for g, btn in (("ar", "Ar_Button"), ("o2", "O2_Button")):
                if p[g]:
                    out.append(S("PLC_CMD", f"{'Ar' if g == 'ar' else 'O2'} Valve Close", (btn, False)))
    elif t == "heater_off":
        out.append(S("HEATER_OFF", "히터 끄기"))
    elif t == "wait":
        sec = _dur(p["sec"])
        sh = stage == "SHUTTER_DELAY"
        out.append(S("DELAY", label or f"대기 {sec}s", duration_sec=sec, timer_purpose="shutter" if sh else None,
                     polling=sh, spec={"skippable": True}))
    elif t == "main":
        sec = _dur(p["sec"])
        out.append(S("PLC_CMD", "Main Shutter Open", ("MS_button", True)))
        out.append(S("DELAY", f"메인 공정 {sec}s 진행", duration_sec=sec, timer_purpose="process", polling=True,
                     ref="process.main_remaining", spec={"main": True}))
        if p["close"]:
            out.append(S("PLC_CMD", "Main Shutter Close", ("MS_button", False)))
    elif t == "cond_wait":
        a = {k: p[k] for k in ("field", "op", "value", "tol", "stable_sec", "timeout_sec", "on_timeout")}
        out.append(S("COND_WAIT", label or f"조건 대기 {p['field']} {p['op']} {p['value']}", a,
                     spec={"skippable": True}))
    elif t == "end_stop":
        out.append(S("END_STOP", "정상 끝 정지 순서", spec={"skip": list(p.get("skip") or [])}, internal=True))
    if not out:
        out.append(S("NOP", label or d.label, internal=True))
    for s in out:
        s.state, s.block = stage, inst.index
    ctx.insts.append(inst)
    ctx.steps += out


def params_summary(blocks: List[dict], name: str, cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """블록 레시피의 run_start params(화면·기록용 요약) — v1 행 params 와 같은 키에 첫 블록 값을 넣는다."""
    u = uses_of(blocks)
    first: Dict[str, Any] = {}

    def walk(bs):
        for b in bs:
            for k, v in b["params"].items():
                first.setdefault((b["type"], k), v)
            if b.get("blocks"):
                walk(b["blocks"])
    walk(blocks)
    g = lambda t, k, d=0.0: first.get((t, k)) if _num(first.get((t, k))) else d
    return {"process_name": name, "blocks": True, "use_ar_gas": u["ar"], "use_o2_gas": u["o2"],
            "ar_flow": g("gas_intro", "ar_flow"), "o2_flow": g("gas_intro", "o2_flow"),
            "dc_power": g("power_on", "dc_power"), "rf_power": g("power_on", "rf_power"),
            "use_rf_pulse": u["rfp"], "rf_pulse_power": g("power_on", "rfp_power"),
            "use_heater": u["heater"], "heater_temp": g("heater_ramp", "target_c"),
            "rf_offset": float(cfg["RF_CAL_OFFSET"]), "rf_param": float(cfg["RF_CAL_GAIN"])}
