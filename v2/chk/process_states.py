"""챔버K 공정 상태 기계 — 상태, 순서, 한도 시간, 전이.

v1 근거: controller/process_controller.py _build_steps(486-682), core/process_service.py, core/state.py.
v1 은 중첩 QEventLoop 로 기다렸다. v2 는 매 tick 스냅샷 하나를 받아 상태를 진행하고, 대기 시간은 실제 시각으로 센다.

한도 시간(timeout)
- param  : ParamDef 키 하나가 한도
- formula: 여러 파라미터로 계산(FORMULAS 설명). 계산에 쓰는 키는 keys 에 적는다
- none   : 한도 없음(레시피 시간만큼 진행 — DELAY 류)
on_timeout: "abort:<사유 코드>" 또는 "proceed"
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Kind(str, Enum):
    IDLE = "idle"           # 공정 없음
    ACTIVE = "active"       # 공정 진행
    STOPPING = "stopping"   # 정지 순서 실행 중
    END = "end"             # 끝(잠깐 머문 뒤 IDLE/LOADED/CSV_BETWEEN)


@dataclass(frozen=True)
class StateDef:
    name: str
    label: str
    kind: Kind
    timeout: str = "none"            # none / param / formula
    keys: tuple = ()                 # 한도·조건에 쓰는 ParamDef 키
    on_timeout: str = ""             # abort:<코드> / proceed
    skip_if: str = ""                # 이 상태를 건너뛰는 조건(사람이 읽는 설명)
    v1: str = ""                     # v1 근거
    desc: str = ""


S = StateDef
A, I, ST, E = Kind.ACTIVE, Kind.IDLE, Kind.STOPPING, Kind.END

STATES: tuple = (
    S("IDLE", "대기", I, v1="core/state.py Phase.IDLE"),
    S("LOADED", "레시피 적재됨", I, v1="core/state.py Phase.LOADED"),
    S("PRE_CLEAN", "시작 전 전원 OFF", A, "param", ("PRE_POWER_OFF_SETTLE_SEC",), "proceed",
      v1="pc:514-519", desc="DC·RF·RF Pulse OFF 뒤 대기(이전 공정 잔류 방지)"),
    S("GAS_PRE", "배기 개방", A, "param", ("STOP_FLOW_OFF_TIMEOUT_SEC",), "abort:MFC_CMD_FAILED",
      v1="pc:522 build_gas_pre_steps", desc="채널별 FLOW_OFF, MFC 밸브 열기"),
    S("HEATER_RAMP", "히터 승온", A, "formula", ("HEATER_WAIT_TIMEOUT_SEC", "HEATER_WAIT_TIMEOUT_MARGIN_SEC",
      "HEATER_WAIT_TIMEOUT_MAX_SEC", "HEATER_SOAK_TOLERANCE", "HEATER_SOAK_TIME_SEC", "HEATER_CASCADE_TI_SEC"),
      "abort:HEATER_WAIT_TIMEOUT",
      skip_if="레시피 행 use_heater 가 아님", v1="pc:534-550, 1295-1329",
      desc="램프 설정·목표 설정 → ±SOAK_TOLERANCE 를 SOAK_TIME 연속 + 램프 목표 도달"),
    S("HEATER_HOLD_WAIT", "히터 유지 모드 진입 대기", A, "param", ("HEATER_HOLD_WAIT_SEC", "HEATER_HOLD_FAIL_ACTION"),
      "abort:HEATER_HOLD_FAILED", skip_if="히터 미사용 또는 HEATER_HOLD_MODE=off", v1="pc:1444-1530",
      desc="폴백: 완화 tc2 → dac → HEATER_HOLD_FAIL_ACTION(abort/proceed)"),
    S("BASE_WAIT", "베이스 압력 대기", A, "param", ("IG_BASE_WAIT_TIMEOUT_SEC", "IG_BASE_STABLE_SEC", "IG_SETTLE_SEC",
      "IG_VALUE_MAX_AGE_SEC", "IG_MAX_RELIGHTS", "IG_COMM_LOSS_ABORT_SEC", "IG_FILAMENT_ON_TIMEOUT_SEC"),
      "abort:BASE_WAIT_TIMEOUT", skip_if="base_pressure 비었음(0)", v1="pc:1036-1273",
      desc="필라멘트 ON → 방출 뒤 SETTLE 제외 → 이온 ≤ 목표 STABLE_COUNT 연속 → 필라멘트 OFF"),
    S("GAS_INTRO", "가스 도입", A, "param", ("HEATER_GAS_MFC_TIMEOUT_SEC",), "abort:MFC_CMD_FAILED",
      v1="pc:560 build_gas_intro_steps", desc="MFC 영점·압력 영점·차단밸브 열기·유량 설정·FLOW ON"),
    S("GAS_STABILIZE", "압력 안정화", A, "param", ("GAS_STABILIZE_SEC",), "proceed", v1="pc:565-579",
      desc="SP4 ON 뒤 자연압 안정화"),
    S("GUN_SHUTTER", "건 셔터 열기", A, v1="pc:582-597", skip_if="gun1·gun2 모두 아님"),
    S("POWER_WAIT", "파워 목표 도달 대기", A, "formula", ("POWER_WAIT_TIMEOUT_SEC", "POWER_WAIT_TIMEOUT_MAX_SEC",
      "POWER_WAIT_EST_FACTOR", "POWER_WAIT_EST_MARGIN_SEC", "DC_RAMP_STEP_A", "RF_RAMP_STEP_W", "RF_REFP_WAIT_SEC"),
      "abort:POWER_WAIT_TIMEOUT", v1="pc:941-997", desc="DC·RF·RF Pulse 를 함께 시작, 모두 도달할 때까지"),
    S("PRESSURE_SP3", "압력 1단계(SP3)", A, "param", ("PRESSURE_STAGE_TIMEOUT_SEC", "SP3_MTORR",
      "PRESSURE_WAIT_TOL_RATIO", "PRESSURE_WAIT_STABLE_SEC"), "proceed", skip_if="WP ≥ SP3_MTORR", v1="pc:617-619"),
    S("PRESSURE_SP2", "압력 2단계(SP2)", A, "param", ("PRESSURE_STAGE_TIMEOUT_SEC", "SP2_MTORR",
      "PRESSURE_WAIT_TOL_RATIO", "PRESSURE_WAIT_STABLE_SEC"), "proceed", skip_if="WP ≥ SP2_MTORR", v1="pc:621-623"),
    S("PRESSURE_SP1", "작업 압력(SP1=WP)", A, "param", ("SP1_TIMEOUT_SEC", "PRESSURE_WAIT_TOL_RATIO",
      "PRESSURE_WAIT_STABLE_SEC"), "abort:SP1_TIMEOUT", v1="pc:625-630"),
    S("DC_DELAY", "DC 안정화 대기", A, "param", ("DC_POWER_DELAY_SEC",), "proceed",
      skip_if="use_dc_delay 아님 또는 DC 미사용 또는 DC_POWER_DELAY_SEC=0", v1="pc:632-644",
      desc="이 동안 DC ±이탈 판정 꺼짐"),
    S("SHUTTER_DELAY", "셔터 딜레이", A, skip_if="shutter_delay = 0", v1="pc:646-657",
      desc="레시피 시간만큼. DC ±이탈 감시 시작(arm)"),
    S("HEATER_SETTLE", "셔터 전 히터 정착", A, "param", ("HEATER_SHUTTER_SETTLE_TOL_C", "HEATER_SHUTTER_SETTLE_SEC",
      "HEATER_SHUTTER_SETTLE_MAX_SEC", "HEATER_SHUTTER_SETTLE_FAIL_ACTION"), "proceed",
      skip_if="히터 미사용·HEATER_CTRL_MODE=tc1·process_time = 0·HEATER_SHUTTER_SETTLE_MAX_SEC = 0", v1="pc HEATER_SETTLE(3328872)",
      desc="TC1 이 목표 ±TOL 안에 SEC 연속이면 통과(승온 대기 통과 뒤부터 셈). 상한 초과는 FAIL_ACTION — abort 면 HEATER_SETTLE_FAILED"),
    S("MAIN", "메인 공정", A, skip_if="process_time = 0", v1="pc:659-680", desc="Main 셔터 열기 → 레시피 시간"),
    # 블록 공정(blocks.BLOCK_DEFS)의 기본 상태 이름 — 기본 템플릿은 위의 v1 이름(stage)을 쓴다
    S("PRESSURE_SP4", "압력 설정(SP4)", A, v1="v2 블록", desc="압력 블록 sp=4"),
    S("VALVE", "MFC 밸브", A, v1="v2 블록"),
    S("SHUTTER", "셔터", A, v1="v2 블록"),
    S("POWER_OFF", "파워 끄기", A, v1="v2 블록", desc="RF 는 램프다운 뒤 OFF"),
    S("GAS_OFF", "가스 끄기", A, v1="v2 블록"),
    S("HEATER_OFF", "히터 끄기", A, v1="v2 블록"),
    S("WAIT", "대기", A, v1="v2 블록"),
    S("COND_WAIT", "조건 대기", A, v1="v2 블록", desc="한도는 블록 값 timeout_sec(on_timeout abort → COND_WAIT_TIMEOUT)"),
    S("CSV_DELAY", "레시피 딜레이 행", A, v1="core/process_service.py 딜레이", desc="delay N 행 — 장비 조작 없이 시간만"),
    S("CSV_BETWEEN", "레시피 스텝 사이", A, v1="core/state.py Phase.CSV_BETWEEN"),
    S("STOPPING", "정지 순서 실행", ST, "formula", ("STOP_RF_RAMPDOWN_TIMEOUT_SEC", "STOP_RFPULSE_OFF_TIMEOUT_SEC",
      "STOP_FLOW_OFF_TIMEOUT_SEC", "STOP_EXHAUST_WAIT_SEC"), "proceed", v1="pc:1785-2019",
      desc="safety_rules.STOP_SEQUENCE. 단계마다 한도, 넘어도 다음 단계로(반드시 하는 것은 계속)"),
    S("DONE", "정상 종료", E, v1="process_service.on_finished"),
    S("ABORTED", "중단 종료", E, v1="process_service.on_finished"),
)

# 기본 템플릿(v1 행)의 진행 순서 — skip_if 가 맞으면 건너뛴다. 블록 레시피는 블록 순서대로(순서 규칙 없음)
STEP_SEQUENCE: tuple = (
    "PRE_CLEAN", "GAS_PRE", "HEATER_RAMP", "HEATER_HOLD_WAIT", "BASE_WAIT", "GAS_INTRO", "GAS_STABILIZE",
    "GUN_SHUTTER", "POWER_WAIT", "PRESSURE_SP3", "PRESSURE_SP2", "PRESSURE_SP1", "DC_DELAY", "SHUTTER_DELAY",
    "HEATER_SETTLE", "MAIN",
)

FORMULAS: dict = {
    "HEATER_RAMP": "min(max(HEATER_WAIT_TIMEOUT_SEC, 예상 램프 초 + HEATER_WAIT_TIMEOUT_MARGIN_SEC), "
                   "HEATER_WAIT_TIMEOUT_MAX_SEC). 예상 램프 = |목표 − PV| / 램프 속도. "
                   "2단 제어(HEATER_CTRL_MODE=cascade)면 + 2 × HEATER_CASCADE_TI_SEC(바깥 루프 수렴 여유)",
    "POWER_WAIT": "min(max(POWER_WAIT_TIMEOUT_SEC, 예상 × POWER_WAIT_EST_FACTOR + POWER_WAIT_EST_MARGIN_SEC), "
                  "POWER_WAIT_TIMEOUT_MAX_SEC). 예상 = max(DC 전류 램프, RF 목표/RF_RAMP_STEP_W + 3×RF_REFP_WAIT_SEC, "
                  "RF Pulse 20 s)",
    "STOPPING": "단계별 한도의 합(약 171 s) — 각 단계는 한도를 넘어도 다음으로",
}


class Event(str, Enum):
    LOAD = "load"                 # 레시피 적재
    CLEAR = "clear"               # 적재 해제
    START = "start"               # 공정 시작(수동 또는 레시피)
    STEP_DONE = "step_done"       # 현재 상태 완료 → 순서상 다음(건너뛰기 포함)
    STATE_TIMEOUT = "timeout"     # 한도 초과 → on_timeout
    ROW_DONE = "row_done"         # 레시피 행 하나 끝(MAIN 또는 CSV_DELAY 완료)
    NEXT_ROW = "next_row"         # 다음 행 시작
    STOP = "stop"                 # 사용자 STOP
    ALL_STOP = "all_stop"         # ALL STOP
    ABORT = "abort"               # 안전 규칙·장비 오류
    STOP_DONE = "stop_done"       # 정지 순서 끝
    SETTLED = "settled"           # 끝 상태에서 다음 대기 상태로


ACTIVE_STATES = tuple(s.name for s in STATES if s.kind is Kind.ACTIVE)


@dataclass(frozen=True)
class Transition:
    src: str          # 상태 이름 또는 "*ACTIVE"(모든 진행 상태)
    event: Event
    dst: str
    guard: str = ""   # 조건(사람이 읽는 설명)


T = Transition
TRANSITIONS: tuple = (
    T("IDLE", Event.LOAD, "LOADED", "레시피 검사 통과"),
    T("LOADED", Event.LOAD, "LOADED", "다른 레시피로 교체"),
    T("LOADED", Event.CLEAR, "IDLE"),
    T("IDLE", Event.START, "PRE_CLEAN", "수동 공정 — 시작 조건(safety_rules START_GUARDS) 통과"),
    T("LOADED", Event.START, "PRE_CLEAN", "첫 행이 공정 행"),
    T("LOADED", Event.START, "CSV_DELAY", "첫 행이 delay 행"),
    T("*ACTIVE", Event.STEP_DONE, "*NEXT", "STEP_SEQUENCE 에서 skip_if 가 아닌 다음 상태"),
    T("*ACTIVE", Event.STATE_TIMEOUT, "*ON_TIMEOUT", "proceed → 다음, abort → STOPPING"),
    T("MAIN", Event.ROW_DONE, "STOPPING", "행 정상 끝 → 정지 순서(가스·전원 정리) 뒤 다음 행"),
    T("SHUTTER_DELAY", Event.ROW_DONE, "STOPPING", "process_time = 0 인 행"),
    T("CSV_DELAY", Event.ROW_DONE, "CSV_BETWEEN"),
    T("CSV_BETWEEN", Event.NEXT_ROW, "PRE_CLEAN", "다음 행이 공정 행"),
    T("CSV_BETWEEN", Event.NEXT_ROW, "CSV_DELAY", "다음 행이 delay 행"),
    T("CSV_BETWEEN", Event.STEP_DONE, "DONE", "남은 행 없음"),
    T("*ACTIVE", Event.STOP, "STOPPING", "사용자 STOP — 남은 행 취소"),
    T("*ACTIVE", Event.ALL_STOP, "STOPPING", "ALL STOP — 남은 행 취소(PLC·DC·RF Pulse 는 즉시 직접 OFF)"),
    T("*ACTIVE", Event.ABORT, "STOPPING", "안전 규칙 — 남은 행 취소"),
    T("CSV_DELAY", Event.STOP, "ABORTED", "딜레이 중 STOP — 정리할 장비 없음"),
    T("CSV_BETWEEN", Event.STOP, "ABORTED"),
    T("STOPPING", Event.STOP_DONE, "CSV_BETWEEN", "행 정상 끝이고 남은 행 있음"),
    T("STOPPING", Event.STOP_DONE, "DONE", "정상 끝이고 남은 행 없음"),
    T("STOPPING", Event.STOP_DONE, "ABORTED", "STOP·ALL STOP·중단"),
    T("STOPPING", Event.ALL_STOP, "STOPPING", "정지 중 ALL STOP — 직접 OFF 를 더 하고 순서는 계속"),
    T("DONE", Event.SETTLED, "LOADED", "레시피가 적재돼 있으면"),
    T("DONE", Event.SETTLED, "IDLE"),
    T("ABORTED", Event.SETTLED, "LOADED", "적재된 레시피 유지"),
    T("ABORTED", Event.SETTLED, "IDLE"),
)


def by_name() -> dict:
    return {s.name: s for s in STATES}


def timeout_code(state: str) -> Optional[str]:
    s = by_name()[state]
    if s.on_timeout.startswith("abort:"):
        return s.on_timeout.split(":", 1)[1]
    return None
