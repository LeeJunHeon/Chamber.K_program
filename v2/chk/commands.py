"""챔버K 명령 종류·사유 코드·시작 조건·공정 상태별 허용 표.

경로
- web      : vanam-process — 공정·레시피·히터·PLC 조작·설정
- hmi      : 노트북 로컬 화면 — STOP·ALL STOP·Buzz Stop·히터 정지만(로그인 없음)
- internal : 안전 엔진·공정 엔진 — 안전 조치(safety_rules 의 action)

v1 근거: integrations/erp_commands.py(명령·거부 규칙), main.py:_erp_exec_heater_command, core/process_service.py.
v1 은 공정 중에도 원격 PLC 버튼(MV·Vent·Door·Turbo …)을 실행했다 — v2 는 PLC_BUTTONS.remote_in_process 로 막는다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from v2.common.commands import ArgDef, COMMON_REASONS, Reason, Source
from v2.chk.process_states import ACTIVE_STATES

WEB, HMI, INT = Source.WEB, Source.HMI, Source.INTERNAL

# ───────────── 사유 코드(챔버K) ─────────────
CHK_REASONS: tuple = (
    # 시작·명령 거부
    Reason("PROCESS_RUNNING", "공정 진행 중", "reject"),
    Reason("PROCESS_NOT_RUNNING", "진행 중인 공정 없음", "reject"),
    Reason("RECIPE_NOT_LOADED", "적재된 레시피 없음", "reject"),
    Reason("RECIPE_LOADED", "레시피가 적재돼 있음(수동 시작 불가)", "reject"),
    Reason("RECIPE_INVALID", "레시피 검사 실패", "reject"),
    Reason("MV_NOT_OPEN", "메인 밸브가 열려 있지 않음", "reject"),
    Reason("MV_INTERLOCK_OFF", "MV 인터록 OFF", "reject"),
    Reason("MV_STATE_UNKNOWN", "메인 밸브 상태를 모름(PLC 값 없음·오래됨)", "reject"),
    Reason("HEATER_GAS_ACTIVE", "히터 가스·압력 유지 중(MFC 사용 중)", "reject"),
    Reason("HEATER_RECIPE_CONFLICT", "히터 레시피 실행 중인데 공정 레시피에 히터 행", "reject"),
    Reason("HEATER_RECIPE_RUNNING", "히터 레시피 실행 중", "reject"),
    Reason("HEATER_RECIPE_NOT_RUNNING", "실행 중인 히터 레시피 없음", "reject"),
    Reason("HEATER_DISABLED", "히터 기능 꺼짐", "reject"),
    Reason("HEATER_ITL_OFF", "히터 인터록 미충족", "reject"),
    Reason("HEATER_FAULT_ACTIVE", "히터 이상 상태", "reject"),
    Reason("HEATER_NOT_FAULT", "히터 이상이 아님(리셋 불필요)", "reject"),
    Reason("HEATER_RUN_ON", "히터가 켜져 있음", "reject"),
    Reason("HEATER_ALREADY", "이미 요청한 상태", "reject"),
    Reason("HEATER_GAS_NOT_ACTIVE", "유지 중인 히터 가스 없음", "reject"),
    Reason("HEATER_OWNED_BY_PROCESS", "공정이 히터를 쓰는 중", "reject"),
    Reason("IG_DISABLED", "이온 게이지 꺼짐(base_pressure 공정 불가)", "reject"),
    Reason("REMOTE_LOCKED_IN_PROCESS", "공정 중 원격 조작 금지 버튼", "reject"),
    Reason("DOOR_CONFLICT", "도어 올림·내림 동시 요청", "reject"),
    Reason("CONFIG_H_IN_PROCESS", "공정 중 H 등급 설정 변경 금지", "reject"),
    # 공정 중단(abort)
    Reason("MV_CLOSED", "공정 중 메인 밸브 닫힘", "abort"),
    Reason("MV_INTERLOCK_LOST", "공정 중 MV 인터록 해제 지속", "abort"),
    Reason("PLC_COMM_LOST", "PLC 통신 두절", "abort"),
    Reason("PLC_RESTARTED", "PLC 재기동 감지", "abort"),
    Reason("MFC_COMM_LOST", "MFC 통신 두절", "abort"),
    Reason("MFC_CMD_FAILED", "MFC 명령 실패", "abort"),
    Reason("DC_COMM_LOST", "DC 통신 두절", "abort"),
    Reason("DC_NO_OUTPUT", "DC 램프업 무출력", "abort"),
    Reason("DC_LOW_CURRENT", "DC 저전류 지속", "abort"),
    Reason("DC_POWER_DEVIATION", "DC 파워 이탈 지속", "abort"),
    Reason("RF_REFLECTED_HIGH", "RF 반사파 대기 시간 초과", "abort"),
    Reason("RF_NO_OUTPUT", "RF 램프업 무출력", "abort"),
    Reason("RF_POWER_DEVIATION", "RF 파워 이탈 지속", "abort"),
    Reason("RFP_COMM_LOST", "RF Pulse 통신 두절", "abort"),
    Reason("RFP_START_FAILED", "RF Pulse 시작 실패", "abort"),
    Reason("RFP_FORWARD_DEVIATION", "RF Pulse 순방향 이탈 연속", "abort"),
    Reason("RFP_REFLECTED_HIGH", "RF Pulse 반사파 초과 연속", "abort"),
    Reason("RFP_CONFIG_MISMATCH", "RF Pulse 펄스 설정 되읽기 불일치", "abort"),
    Reason("RFP_FAULT", "RF Pulse 장비 이상(CSR/상태)", "abort"),
    Reason("POWER_WAIT_TIMEOUT", "파워 목표 도달 시간 초과", "abort"),
    Reason("SP1_TIMEOUT", "작업 압력 도달 시간 초과", "abort"),
    Reason("HEATER_WAIT_TIMEOUT", "히터 승온 시간 초과", "abort"),
    Reason("HEATER_HOLD_FAILED", "히터 유지 모드 진입 실패", "abort"),
    Reason("HEATER_FAULT", "히터 이상(OT/TC/WD)", "abort"),
    Reason("HEATER_TC_BAD", "히터 TC 이상 지속", "abort"),
    Reason("HEATER_OUTPUT_DEAD", "히터 출력 죽음 지속", "abort"),
    Reason("HEATER_RUN_OFF_EXTERNAL", "히터가 외부에서 꺼짐", "abort"),
    Reason("BASE_WAIT_TIMEOUT", "베이스 압력 대기 시간 초과", "abort"),
    Reason("IG_COMM_LOST", "이온 게이지 두절(베이스 대기 중)", "abort"),
    Reason("IG_FILAMENT_FAILED", "이온 게이지 필라멘트 켜기 실패", "abort"),
    Reason("CONNECTION_FAILED", "장비 연결 실패", "abort"),
    Reason("HEATER_GAS_FAILED", "히터 가스 준비 실패", "abort"),
    # 경고·정보
    Reason("FLOW_DEVIATION", "가스 유량 이탈(경고만)", "info"),
    Reason("PRESSURE_DEVIATION", "압력 이탈(경고만)", "info"),
    Reason("DC_OFF_UNCONFIRMED", "DC OFF 확인 안 됨 — 재시도 중", "info"),
    Reason("RFP_OFF_UNCONFIRMED", "RF Pulse OFF 확인 안 됨", "info"),
    Reason("HEATER_SATURATED", "히터 DAC 포화 — 상한 클램프", "info"),
    Reason("HEATER_WD_DELAY", "히터 하트비트 지연", "info"),
    Reason("HEATER_OUTPUT_DEAD_WARN", "히터 출력 죽음(경고)", "info"),
    Reason("HEATER_RESIDUAL_RUN", "이전 세션 히터 RUN 남음 — 정리함", "info"),
    Reason("HEATER_HOLD_DEMOTED", "히터 유지 모드 tc2 → dac 강등", "info"),
    Reason("HEATER_TC1_DEVIATION", "TC1 편차 큼", "info"),
    Reason("PLC_DISCONNECTED", "PLC 끊김 지속(알림)", "info"),
    Reason("PLC_RECOVERED_SAFE", "PLC 복구 뒤 안전 상태 다시 적용", "info"),
    Reason("COMM_LONG_OUTAGE", "장비 장기 두절", "info"),
    Reason("LINK_STALE", "장비 값이 오래됨 — 화면 잠금", "info"),
)

ALL_REASONS: tuple = COMMON_REASONS + CHK_REASONS


def reason_codes() -> set:
    return {r.code for r in ALL_REASONS}


# ───────────── 시작·명령 조건(precondition) ─────────────
@dataclass(frozen=True)
class Pre:
    id: str
    desc: str
    reason: str          # 실패 사유 코드
    v1: str = ""


PRECONDITIONS: tuple = (
    Pre("NOT_RUNNING", "공정 진행 중이 아님", "PROCESS_RUNNING", "process_service.start"),
    Pre("RUNNING", "공정 진행 중", "PROCESS_NOT_RUNNING"),
    Pre("RECIPE_LOADED", "레시피 적재됨", "RECIPE_NOT_LOADED", "erp_commands RECIPE_PROCESS_START"),
    Pre("NO_RECIPE_LOADED", "레시피 적재 안 됨(수동 시작)", "RECIPE_LOADED", "process_service.start 원격 수동"),
    Pre("MV_OPEN", "MV(M00003) 켜짐 — 값이 신선해야 함", "MV_NOT_OPEN", "main.py:3675"),
    Pre("MV_ITL_ON", "MV_INTERLOCK(M00032) 켜짐", "MV_INTERLOCK_OFF", "main.py:3675"),
    Pre("PLC_FRESH", "PLC 값 품질 ok, 나이 ≤ HEATER_STALE_SEC", "MV_STATE_UNKNOWN", "main.py:3675(읽기 실패도 거부)"),
    Pre("PLC_LINK_UP", "PLC 통신 정상", "LINK_DOWN", "erp_commands:207"),
    Pre("NO_HEATER_GAS", "히터 가스·압력 유지 중이 아님", "HEATER_GAS_ACTIVE", "main.py:2743"),
    Pre("HEATER_RECIPE_NO_CONFLICT", "히터 레시피 실행 중이면 공정 레시피에 히터 행 없음", "HEATER_RECIPE_CONFLICT",
        "process_service.start"),
    Pre("HEATER_ENABLED", "HEATER_ENABLED", "HEATER_DISABLED"),
    Pre("HEATER_ITL", "히터 인터록(M00041) 정상", "HEATER_ITL_OFF", "main.py:1677"),
    Pre("HEATER_NO_FAULT", "히터 이상(M00042) 아님", "HEATER_FAULT_ACTIVE"),
    Pre("HEATER_IS_FAULT", "히터 이상 상태", "HEATER_NOT_FAULT", "main.py:3260"),
    Pre("HEATER_RUN_OFF", "히터 RUN 꺼짐(리셋 전)", "HEATER_RUN_ON", "main.py:3260"),
    Pre("HEATER_RECIPE_IDLE", "히터 레시피 실행 중이 아님", "HEATER_RECIPE_RUNNING", "erp HEATER_SV/ONOFF"),
    Pre("HEATER_RECIPE_ACTIVE", "히터 레시피 실행 중", "HEATER_RECIPE_NOT_RUNNING"),
    Pre("HEATER_NOT_PROCESS_OWNED", "공정이 히터를 쓰지 않음", "HEATER_OWNED_BY_PROCESS"),
    Pre("HEATER_GAS_ACTIVE", "히터 가스 유지 중", "HEATER_GAS_NOT_ACTIVE"),
    Pre("IG_FOR_BASE", "base_pressure 행이 있으면 IG_ENABLED", "IG_DISABLED"),
    Pre("RECIPE_VALID", "레시피 모든 행 검사 통과(recipe.check_process_row)", "RECIPE_INVALID", "process_service 적재 검사"),
    Pre("CONFIG_LIVE_ONLY_IN_PROCESS", "공정 중이면 apply=live 키만 즉시 적용, H 등급 변경은 거부", "CONFIG_H_IN_PROCESS"),
)


# ───────────── PLC 버튼(원격) ─────────────
@dataclass(frozen=True)
class PlcButton:
    target: str            # 명령 인자 값
    addr: str              # devices.PLC_ADDRS 이름
    label: str
    remote_in_process: bool   # 공정 중 원격 조작 허용
    pending_confirm: bool = False


PLC_BUTTONS: tuple = (
    PlcButton("rotary", "rotary", "Rotary 펌프", False, True),
    PlcButton("rv", "rv", "RV", True),
    PlcButton("fv", "fv", "FV", True),
    PlcButton("mv", "mv", "Main 밸브", False, True),
    PlcButton("vent", "vent", "Vent", False, True),
    PlcButton("turbo", "turbo", "Turbo 펌프", False, True),
    PlcButton("door_up", "door_up", "도어 올림", False, True),
    PlcButton("door_down", "door_down", "도어 내림", False, True),
    PlcButton("ar_valve", "ar_valve", "Ar 차단밸브", True),
    PlcButton("o2_valve", "o2_valve", "O2 차단밸브", True),
    PlcButton("ms", "ms", "Main 셔터", True),
    PlcButton("s1", "s1", "Gun 1 셔터", True),
    PlcButton("s2", "s2", "Gun 2 셔터", True),
    PlcButton("buzz_stop", "buzz_stop", "부저 정지", True),
    PlcButton("ion", "ion_remote_on", "이오나이저", True),
)


# ───────────── 명령 정의 ─────────────
ANY_STATE = "*"
IDLE_STATES = ("IDLE", "LOADED")


@dataclass(frozen=True)
class CommandDef:
    kind: str
    label: str
    sources: tuple
    role: str                     # admin / operator / system
    states: tuple                 # 허용 상태(ANY_STATE 는 전부)
    requires: tuple = ()          # PRECONDITIONS id
    args: tuple = ()
    desc: str = ""
    v1: str = ""


def _a(name, type_, required=True, **kw) -> ArgDef:
    return ArgDef(name=name, type=type_, required=required, **kw)


C = CommandDef
STOPPABLE = ACTIVE_STATES + ("STOPPING",)

COMMANDS: tuple = (
    # 정지 — 로컬 화면에서도
    C("PROCESS_STOP", "공정 STOP", (WEB, HMI), "operator", STOPPABLE, v1="PROCESS_STOP"),
    C("ALL_STOP", "ALL STOP", (WEB, HMI, INT), "operator", (ANY_STATE,), desc="공정 여부와 무관 — safety_rules.ALL_STOP_SEQUENCE",
      v1="ALL_STOP"),
    C("BUZZ_STOP", "부저 정지", (WEB, HMI), "operator", (ANY_STATE,), ("PLC_LINK_UP",), v1="BuzzStop 버튼"),
    C("HEATER_STOP", "히터 정지", (WEB, HMI), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      desc="히터 레시피 정지 + RUN=0. 공정이 히터를 쓰는 중이면 공정도 중단(HEATER_FAULT 아님 — USER_STOP)"),
    # 공정
    C("PROCESS_START", "수동 공정 시작", (WEB,), "operator", ("IDLE",),
      ("NOT_RUNNING", "NO_RECIPE_LOADED", "PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS", "IG_FOR_BASE", "RECIPE_VALID"),
      (_a("row", "obj", desc="recipe.PROCESS_COLUMNS 한 행(히터 열은 무시 — 수동 공정은 히터를 쓰지 않음)"),),
      v1="PROCESS_START"),
    C("RECIPE_LOAD", "공정 레시피 적재", (WEB,), "operator", IDLE_STATES, ("RECIPE_VALID", "IG_FOR_BASE"),
      (_a("name", "str"), _a("rows", "rows", desc="recipe.PROCESS_COLUMNS 행 목록")), v1="RECIPE_PROCESS_RUN"),
    C("RECIPE_CLEAR", "레시피 적재 해제", (WEB,), "operator", ("LOADED",), v1="RECIPE_CLEAR"),
    C("RECIPE_START", "적재 레시피 시작", (WEB,), "operator", ("LOADED",),
      ("RECIPE_LOADED", "PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS", "HEATER_RECIPE_NO_CONFLICT"),
      v1="RECIPE_PROCESS_START"),
    # 히터
    C("HEATER_SV", "히터 목표 온도", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("value", "float", min=0.0, param_max="HEATER_MAX_TEMP"),), v1="HEATER_SV"),
    C("HEATER_ON", "히터 켜기(가스 선택)", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_ITL", "HEATER_NO_FAULT", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("value", "float", min=0.0, param_max="HEATER_MAX_TEMP"), _a("use_ar", "bool", False), _a("ar_flow", "float", False, min=0.0),
       _a("use_o2", "bool", False), _a("o2_flow", "float", False, min=0.0), _a("wp_mtorr", "float", False, min=0.0)),
      desc="가스를 고르면 히터 가스 시퀀스(NOT_RUNNING 일 때만 — 공정과 MFC 공유 금지)", v1="HEATER_ONOFF on"),
    C("HEATER_OFF", "히터 끄기", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      v1="HEATER_ONOFF off"),
    C("HEATER_RESET", "히터 이상 리셋", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP", "HEATER_IS_FAULT", "HEATER_RUN_OFF"),
      v1="HEATER_RESET"),
    C("HEATER_GAS_RELEASE", "히터 가스 해제", (WEB,), "operator", IDLE_STATES, ("HEATER_GAS_ACTIVE",), v1="HEATER_GAS_RELEASE"),
    C("HEATER_RECIPE_RUN", "히터 레시피 시작", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_ITL", "HEATER_NO_FAULT", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("name", "str"), _a("steps", "rows", desc="recipe.HEATER_STEP_COLUMNS"), _a("gas", "obj", False, desc="recipe.HEATER_GAS_COLUMNS")),
      v1="RECIPE_HEATER_RUN"),
    C("HEATER_RECIPE_STOP", "히터 레시피 정지", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",), v1="RECIPE_HEATER_STOP"),
    C("HEATER_RECIPE_HOLD", "히터 레시피 일시정지/재개", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",),
      (_a("on", "bool"),), v1="HEATER_RECIPE_HOLD"),
    C("HEATER_RECIPE_SKIP", "히터 레시피 스텝 건너뛰기", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",),
      v1="HEATER_RECIPE_STEP"),
    # PLC 조작 — admin
    C("PLC_BUTTON", "PLC 버튼", (WEB,), "admin", (ANY_STATE,), ("PLC_LINK_UP",),
      (_a("target", "enum", enum=tuple(b.target for b in PLC_BUTTONS)), _a("on", "bool")),
      desc="공정 중에는 PLC_BUTTONS.remote_in_process=False 인 버튼 거부(REMOTE_LOCKED_IN_PROCESS)", v1="PLC 버튼 14종"),
    C("IG_FILAMENT", "이온 게이지 필라멘트", (WEB,), "operator", IDLE_STATES, (), (_a("on", "bool"),),
      desc="공정 중에는 공정 엔진이 필라멘트를 다룬다"),
    # 설정
    C("CONFIG_APPLY", "설정 적용", (WEB,), "admin", (ANY_STATE,), ("CONFIG_LIVE_ONLY_IN_PROCESS",),
      (_a("version", "int", min=1), _a("values", "obj"), _a("schemaHash", "str")),
      desc="검사 실패면 버전 전체 거부. idle/restart 키는 대기로 두고 config_ack 로 알림"),
    # 내부(안전·공정 엔진) 조치 — safety_rules 의 action 이 이것들을 쓴다
    C("PROCESS_ABORT", "공정 중단", (INT,), "system", STOPPABLE, (), (_a("reason", "str"),)),
    C("START_REJECT", "시작 거부", (INT,), "system", (ANY_STATE,), (), (_a("reason", "str"),)),
    C("DC_SAFE_OFF", "DC 출력 OFF(확인형)", (INT,), "system", (ANY_STATE,)),
    C("RF_SAFE_OFF", "RF DAC 0·enable OFF", (INT,), "system", (ANY_STATE,)),
    C("RFP_SAFE_OFF", "RF Pulse RF OFF(확인형)", (INT,), "system", (ANY_STATE,)),
    C("RF_RAMP_DOWN", "RF 램프다운", (INT,), "system", (ANY_STATE,)),
    C("HEATER_FORCE_OFF", "히터 RUN=0(링크 없으면 보류 뒤 적용)", (INT,), "system", (ANY_STATE,)),
    C("HEATER_PUSH_CONFIG", "히터 한계값 쓰기·되읽기", (INT,), "system", (ANY_STATE,)),
    C("HEATER_CLAMP_MV", "히터 MV 상한 클램프", (INT,), "system", (ANY_STATE,), (), (_a("value", "int"),)),
    C("HEATER_GAS_HOLD", "냉각 보호 가스 유지", (INT,), "system", (ANY_STATE,)),
    C("HEATER_RESIDUAL_CLEAR", "남은 RUN 정리(RUN=0, SV=PV)", (INT,), "system", (ANY_STATE,)),
    C("GAS_FLOW_OFF", "MFC 전체 유량 OFF", (INT,), "system", (ANY_STATE,)),
    C("GAS_EXHAUST", "MFC 밸브 열기(배기)", (INT,), "system", (ANY_STATE,)),
    C("GAS_VALVES_CLOSE", "Ar·O2 차단밸브 닫기", (INT,), "system", (ANY_STATE,)),
    C("SHUTTERS_CLOSE", "셔터 닫기(MS·S1·S2)", (INT,), "system", (ANY_STATE,), (), (_a("which", "str", False),)),
    C("MV_CLOSE", "메인 밸브 닫기", (INT,), "system", (ANY_STATE,)),
    C("PLC_OUTPUTS_OFF", "PLC 출력 코일 OFF(펌프 유지 여부는 ALL_STOP_KEEP_PUMPS)", (INT,), "system", (ANY_STATE,)),
    C("IG_FILAMENT_OFF", "이온 게이지 필라멘트 OFF", (INT,), "system", (ANY_STATE,)),
    C("BLACKBOX_DUMP", "PLC 블랙박스 파일로", (INT,), "system", (ANY_STATE,)),
    C("LOCK_CONTROLS", "조작 잠금(값 오래됨·링크 끊김)", (INT,), "system", (ANY_STATE,)),
    C("PLC_MARKER_WRITE", "PLC 재기동 마커 쓰기", (INT,), "system", (ANY_STATE,)),
    C("NOTIFY", "이벤트·알람 보내기", (INT,), "system", (ANY_STATE,), (), (_a("level", "str"), _a("code", "str"))),
)


def by_kind() -> dict:
    return {c.kind: c for c in COMMANDS}


def check_command(kind: str, source: Source, state: str, args: Optional[dict] = None,
                  in_process: Optional[bool] = None) -> Optional[str]:
    """표만으로 판정할 수 있는 거부 사유(경로·상태·원격 버튼 인터록). 통과면 None.
    PRECONDITIONS(장비 값 필요)는 core 가 따로 평가한다."""
    c = by_kind().get(kind)
    if c is None:
        return "UNKNOWN_COMMAND"
    if source not in c.sources:
        return "SOURCE_NOT_ALLOWED"
    if ANY_STATE not in c.states and state not in c.states:
        return "STATE_NOT_ALLOWED"
    if kind == "PLC_BUTTON":
        args = args or {}
        btn = {b.target: b for b in PLC_BUTTONS}.get(args.get("target"))
        if btn is None or not isinstance(args.get("on"), bool):
            return "BAD_ARGS"
        running = (state in STOPPABLE) if in_process is None else in_process
        if running and not btn.remote_in_process:
            return "REMOTE_LOCKED_IN_PROCESS"
    return None
