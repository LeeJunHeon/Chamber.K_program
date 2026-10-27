"""챔버K 명령 종류·사유 코드·시작 조건·공정 상태별 허용 표.

경로
- web      : vanam-process — 공정·레시피·히터·PLC 조작·설정
- hmi      : 노트북 로컬 화면 — STOP·ALL STOP·Buzz Stop·히터 정지 + 공정 중 값 수정(PROCESS_ADJUST source local, 5-4)(로그인 없음)
- internal : 안전 엔진·공정 엔진 — 안전 조치(safety_rules 의 action)

v1 근거: integrations/erp_commands.py(명령·거부 규칙), main.py:_erp_exec_heater_command, core/process_service.py.
공정 중 원격 PLC 버튼(MV·Vent·Door·Turbo …)은 v1 처럼 모두 허용한다(사용자 결정 2026-10-20) — 실행한 것은 공정 기록에
PLC_MANUAL_IN_PROCESS(info)로 남긴다. "정말 하시겠습니까?" 확인은 웹 단계에서.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from v2.common.commands import ArgDef, COMMON_REASONS, Reason, Source
from v2.chk import params as P
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
    Reason("IG_ATM_FILAMENT_ON", "대기압(PLC ATM 센서) — 이온 게이지 필라멘트 켜기 금지", "reject"),
    Reason("IG_FILAMENT_FAILED_CMD", "필라멘트 명령 결과 확인 실패", "fail"),
    Reason("COMMAND_NOT_ALLOWED", "장비에 보내지 않는 명령(예: 이온 게이지 디개스 C·D)", "reject"),
    Reason("DOOR_CONFLICT", "도어 올림·내림 동시 요청", "reject"),
    Reason("CONFIG_H_IN_PROCESS", "공정 중 H 등급 설정 변경 금지", "reject"),
    Reason("CLEANUP_RUNNING", "공정 밖 STOP 정리 중 — 끝난 뒤에 시작", "reject"),
    Reason("ADJUST_NOT_USED", "공정 중 수정: 이번 공정에서 쓰지 않는 장비·항목", "reject"),
    Reason("ADJUST_OUT_OF_RANGE", "공정 중 수정: 범위 밖(레시피와 같은 상한·펄스 ON/OFF 시간)", "reject"),
    Reason("ADJUST_NOT_APPLICABLE", "공정 중 수정: 지금 블록에 해당 없음(대기 끝내기는 대기·조건 대기 블록만)", "reject"),
    Reason("ADJUST_BUSY", "공정 중 수정: 장비가 시작 중(RF Pulse START) — 끝난 뒤에", "reject"),
    # 공정 중단(abort)
    Reason("MV_CLOSED", "공정 중 메인 밸브 닫힘", "abort"),
    Reason("MV_INTERLOCK_LOST", "공정 중 MV 인터록 해제 지속", "abort"),
    Reason("PLC_COMM_LOST", "PLC 통신 두절", "abort"),
    Reason("PLC_RESTARTED", "PLC 재기동 감지", "abort"),
    Reason("MFC_COMM_LOST", "MFC 통신 두절", "abort"),
    Reason("MFC_CMD_FAILED", "MFC 명령 실패", "abort"),
    Reason("FLOW_STABILIZE_TIMEOUT", "FLOW ON 뒤 유량 안정화 시간 초과", "abort"),
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
    Reason("COND_WAIT_TIMEOUT", "조건 대기 블록 한도 초과(on_timeout=abort)", "abort"),
    Reason("PRESSURE_WAIT_TIMEOUT", "압력 블록(SP2~SP4) 도달 한도 초과(on_timeout=abort)", "abort"),
    Reason("PROGRAM_EXIT", "프로그램 강제 종료(콘솔 창 닫기·로그오프·윈도우 종료) — 파워 출력만 끄고 PLC 릴레이는 유지(결정 4)",
           "abort"),
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
    Reason("RF_RESIDUAL_OUTPUT", "시작 때 RF 출력이 남아 있음 — 램프다운 뒤 끔", "info"),
    Reason("RF_ENABLE_RESIDUAL", "시작 때 RF enable 만 켜져 있음(v1 정상 종료 흔적) — 끔", "info"),
    Reason("DC_RESIDUAL_OUTPUT", "시작 때 DC 출력이 켜져 있음 — 끔", "info"),
    Reason("RFP_RESIDUAL_OUTPUT", "시작 때 RF Pulse 출력이 켜져 있음", "info"),
    Reason("GAS_RESIDUAL_FLOW", "시작 때 가스가 흐르고 있음 — 끄지 않음(확인)", "info"),
    Reason("PLC_RECONNECTED", "PLC 끊김 알림 뒤 링크 복구", "info"),
    Reason("DC_RECOVERED_SAFE", "DC 통신 복구 뒤 출력 OFF 다시(공정 없을 때)", "info"),
    Reason("RFP_RECOVERED_SAFE", "RF Pulse 통신 복구 뒤 RF OFF 다시(공정 없을 때)", "info"),
    Reason("MV_INTERLOCK_BLIP", "MV 인터록이 지속 시간 안에 다시 참(기록만)", "info"),
    Reason("COMM_LONG_OUTAGE", "장비 장기 두절", "info"),
    Reason("LINK_STALE", "장비 값이 오래됨 — 화면 잠금", "info"),
    Reason("PLC_MANUAL_IN_PROCESS", "공정 중 PLC 버튼 실행(기록 — 대상·켬/끔·출처·시각)", "info"),
    Reason("OUTPUTS_LEFT_ON", "정상 끝 정지에서 뺀 단계로 남긴 출력(히터·유량·차단밸브·셔터·MFC 밸브·필라멘트) — 일부러 남긴 것", "info"),
    Reason("AUTO_END_STOP", "목록 끝이 정상 끝 정지가 아니어서 기본 끝 정지를 덧붙임", "info"),
    Reason("HEATER_HANDOVER", "정상 끝 정지에서 히터를 남김 — 소유를 공정 → 수동(웹 히터 카드)으로", "info"),
    Reason("OUTPUTS_CLEANUP", "공정 밖 STOP — 지난 공정이 남긴 출력 정리(단계별 결과)", "info"),
    Reason("HEATER_NO_TARGET", "히터 도달 대기 블록인데 히터 목표 없음 — 건너뜀", "info"),
    Reason("RF_OFF_FAILED", "파워 끄기 블록의 RF OFF 결과 오류", "info"),
    Reason("PROCESS_ADJUST", "공정 중 값 수정 기록(장비·항목·이전 → 새 값·출처·결과)", "info"),
    Reason("ADJUST_SETTLE_TIMEOUT", "공정 중 수정 뒤 새 목표에 한도 안에 못 감 — 이탈 판정 다시 켬", "info"),
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
    Pre("IG_ON_NOT_ATM", "필라멘트 켜기(on=true)일 때 PLC ATM 센서가 꺼져 있고 값이 신선", "IG_ATM_FILAMENT_ON",
        "v2 (사용자 결정 2026-10-15)"),
)


# ───────────── PLC 버튼(원격) ─────────────
@dataclass(frozen=True)
class PlcButton:
    target: str            # 명령 인자 값
    addr: str              # devices.PLC_ADDRS 이름
    label: str


# v1 lib/config.py:464-480 PLC_COIL_MAP 15개와 같다(ALL STOP 은 이 코일을 전부 끈다)
PLC_BUTTONS: tuple = (
    PlcButton("rotary", "rotary", "Rotary 펌프"),
    PlcButton("rv", "rv", "RV"),
    PlcButton("fv", "fv", "FV"),
    PlcButton("mv", "mv", "Main 밸브"),
    PlcButton("vent", "vent", "Vent"),
    PlcButton("turbo", "turbo", "Turbo 펌프"),
    PlcButton("door_up", "door_up", "도어 올림"),
    PlcButton("door_down", "door_down", "도어 내림"),
    PlcButton("ar_valve", "ar_valve", "Ar 차단밸브"),
    PlcButton("o2_valve", "o2_valve", "O2 차단밸브"),
    PlcButton("ms", "ms", "Main 셔터"),
    PlcButton("s1", "s1", "Gun 1 셔터"),
    PlcButton("s2", "s2", "Gun 2 셔터"),
    PlcButton("buzz_stop", "buzz_stop", "부저 정지"),
    PlcButton("ion", "ion_remote_on", "이오나이저"),
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
    # 정지 — 로컬 화면에서도. bench 모드에서는 worker 가 BENCH_READ_ONLY 로 거부하므로 로컬 화면은
    # "읽기 전용 — 조작은 장비 앞면으로" 를 표시해야 한다(로컬 화면 단계 할 일)
    C("PROCESS_STOP", "공정 STOP", (WEB, HMI), "operator", (ANY_STATE,),
      desc="공정이 없으면 지난 공정이 남긴 출력만 정리(OUTPUTS_CLEANUP), 없으면 아무것도 하지 않고 성공(v1 과 같게) — 로컬 화면 STOP 도 같다",
      v1="PROCESS_STOP"),
    C("ALL_STOP", "ALL STOP", (WEB, HMI, INT), "operator", (ANY_STATE,), desc="공정 여부와 무관 — safety_rules.ALL_STOP_SEQUENCE",
      v1="ALL_STOP"),
    C("BUZZ_STOP", "부저 정지", (WEB, HMI), "operator", (ANY_STATE,), ("PLC_LINK_UP",), v1="BuzzStop 버튼"),
    C("HEATER_STOP", "히터 정지", (WEB, HMI), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      desc="히터 레시피 정지 + RUN=0. 공정이 히터를 쓰는 중이면 공정도 중단(HEATER_FAULT 아님 — USER_STOP)"),
    # 공정
    C("PROCESS_START", "수동 공정 시작", (WEB,), "operator", IDLE_STATES,
      ("NOT_RUNNING", "NO_RECIPE_LOADED", "PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS", "IG_FOR_BASE", "RECIPE_VALID"),
      (_a("row", "obj", False, desc="recipe.PROCESS_COLUMNS 한 행(히터 열은 무시 — 수동 공정은 히터를 쓰지 않음)"),
       _a("blocks", "rows", False, desc="블록 목록(blocks.BLOCK_DEFS) — row 대신"),
       _a("name", "str", False, desc="run 이름(run_start·공정 기록) — 비우면 행의 process_name, 그것도 없으면 Single CHK")),
      desc="row 또는 blocks 하나", v1="PROCESS_START"),
    C("RECIPE_LOAD", "공정 레시피 적재", (WEB,), "operator", IDLE_STATES, ("RECIPE_VALID", "IG_FOR_BASE"),
      (_a("name", "str"), _a("rows", "rows", False, desc="행 목록 — v1 행(recipe.PROCESS_COLUMNS)·딜레이 행·"
                                                         "블록 행 {process_name, blocks}"),
       _a("blocks", "rows", False, desc="블록 목록 하나(= 블록 행 하나) — rows 대신")),
      desc="rows 또는 blocks 하나. 블록 검사는 값 범위·꺼진 장비만(순서 규칙 없음)", v1="RECIPE_PROCESS_RUN"),
    C("RECIPE_CLEAR", "레시피 적재 해제", (WEB,), "operator", (ANY_STATE,),
      desc="적재가 없으면 아무것도 하지 않고 성공, 공정 중이면 PROCESS_RUNNING(v1 B5)", v1="RECIPE_CLEAR"),
    C("RECIPE_START", "적재 레시피 시작", (WEB,), "operator", IDLE_STATES,
      ("RECIPE_LOADED", "PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS", "HEATER_RECIPE_NO_CONFLICT"),
      v1="RECIPE_PROCESS_START"),
    # 히터
    C("HEATER_SV", "히터 목표 온도", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("value", "float", min=0.0, param_max="HEATER_MAX_TEMP"),), v1="HEATER_SV"),
    C("HEATER_ON", "히터 켜기(가스 선택)", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_ITL", "HEATER_NO_FAULT", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("value", "float", min=0.0, param_max="HEATER_MAX_TEMP"), _a("use_ar", "bool", False),
       _a("ar_flow", "float", False, min=0.0, param_max="MFC_AR_FS_SCCM"),
       _a("use_o2", "bool", False), _a("o2_flow", "float", False, min=0.0, param_max="MFC_O2_FS_SCCM"),
       _a("wp_mtorr", "float", False, min=0.0)),
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
    C("HEATER_RECIPE_STOP", "히터 레시피 정지", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      desc="레시피가 없어도 안전 종료(RUN=0·램프 복구·SV=PV) — v1 main.py:1953", v1="RECIPE_HEATER_STOP"),
    C("HEATER_RECIPE_HOLD", "히터 레시피 일시정지/재개", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",),
      (_a("on", "bool"),), v1="HEATER_RECIPE_HOLD"),
    C("HEATER_RECIPE_SKIP", "히터 레시피 스텝 건너뛰기", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",),
      v1="HEATER_RECIPE_STEP"),
    # PLC 조작 — operator(관리자 + 그 장비 담당자, 사용자 결정 2026-10-22 — 웹이 role 로 정한다)
    C("PLC_BUTTON", "PLC 버튼", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      (_a("target", "enum", enum=tuple(b.target for b in PLC_BUTTONS)), _a("on", "bool")),
      desc="공정 중에도 모두 허용(v1 과 같게) — 공정 중 실행은 공정 기록에 PLC_MANUAL_IN_PROCESS(info)", v1="PLC 버튼 15종"),
    C("IG_FILAMENT", "이온 게이지 필라멘트", (WEB,), "operator", IDLE_STATES, ("IG_ON_NOT_ATM",), (_a("on", "bool"),),
      desc="유휴 상태에서만(공정 중에는 공정 엔진만 A/B). 켜기는 ATM 센서가 켜져 있으면 거부. "
           "무응답 유예 IG_FILAMENT_SWITCH_QUIET_SEC, B 뒤 IG_RELIGHT_LOCKOUT_SEC 안의 A 는 늦춤"),
    C("PROCESS_ADJUST", "공정 중 값 수정", (WEB, HMI), "operator", (ANY_STATE,), ("RUNNING",),
      (_a("device", "enum", enum=("mfc", "dc", "rf", "rfpulse", "heater", "process")), _a("item", "str"),
       _a("value", "any", desc="blocks.ADJUST_ITEMS 의 형식·범위"), _a("source", "enum", False, enum=("web", "local"),
                                                                     desc="비우면 경로(web·hmi → local)")),
      desc="이번 공정의 그 값을 바꾼다 — 지금 쓰는 블록이면 바로(드라이버 경로·되읽기), 아직 안 온 블록이면 그 블록이 새 값으로. "
           "공정 밖·정지 중 PROCESS_NOT_RUNNING. 결과 기록 ProcEvent adjust(PROCESS_ADJUST)", v1="v2"),
    # 설정
    C("CONFIG_APPLY", "설정 적용", (WEB,), "admin", (ANY_STATE,), ("CONFIG_LIVE_ONLY_IN_PROCESS",),
      (_a("version", "int", min=1), _a("values", "obj"), _a("schemaHash", "str")),
      desc="검사 실패면 버전 전체 거부. idle/restart 키는 대기로 두고 config_ack 로 알림. 이력(누가·언제·이전 → 새 값)은 "
           "config_history.jsonl + config_ack changes"),
    C("CONFIG_ROLLBACK", "이전 설정으로 되돌리기", (WEB,), "admin", (ANY_STATE,), ("CONFIG_LIVE_ONLY_IN_PROCESS",),
      (_a("version", "int", False, min=1, desc="새 버전 번호(웹이 정함 — 지금 버전보다 커야 함, 아니면 CONFIG_VERSION_STALE). "
                                                "비우면 지금 버전 + 1"),),
      desc="ConfigStore.rollback — 직전 버전(config_prev.json) 값을 새 버전으로 적용. 결과 config_ack(kind rollback, version, values)"),
    # 내부(안전·공정 엔진) 조치 — safety_rules 의 action 이 이것들을 쓴다
    C("PROCESS_ABORT", "공정 중단", (INT,), "system", STOPPABLE, (), (_a("reason", "str"),)),
    C("START_REJECT", "시작 거부", (INT,), "system", (ANY_STATE,), (), (_a("reason", "str"),)),
    C("DC_SAFE_OFF", "DC 출력 OFF(확인형)", (INT,), "system", (ANY_STATE,)),
    C("RF_SAFE_OFF", "RF DAC 0·enable OFF", (INT,), "system", (ANY_STATE,)),
    C("RFP_SAFE_OFF", "RF Pulse RF OFF(확인형)", (INT,), "system", (ANY_STATE,)),
    C("RF_RAMP_DOWN", "RF 램프다운", (INT,), "system", (ANY_STATE,)),
    C("HEATER_FORCE_OFF", "히터 RUN=0(링크 없으면 보류 뒤 적용)", (INT,), "system", (ANY_STATE,)),
    C("HEATER_RECIPE_ABORT", "히터 레시피 중단", (INT,), "system", (ANY_STATE,), (), (_a("reason", "str"),)),
    C("HEATER_PUSH_CONFIG", "히터 한계값 쓰기·되읽기", (INT,), "system", (ANY_STATE,)),
    C("HEATER_CLAMP_MV", "히터 MV 상한 클램프", (INT,), "system", (ANY_STATE,), (), (_a("value", "int"),)),
    C("HEATER_GAS_HOLD", "냉각 보호 가스 유지", (INT,), "system", (ANY_STATE,)),
    C("HEATER_RESIDUAL_CLEAR", "남은 RUN 정리(RUN=0, SV=PV)", (INT,), "system", (ANY_STATE,)),
    C("GAS_FLOW_OFF", "MFC 전체 유량 OFF", (INT,), "system", (ANY_STATE,)),
    C("GAS_EXHAUST", "MFC 밸브 열기(배기)", (INT,), "system", (ANY_STATE,)),
    C("GAS_VALVES_CLOSE", "Ar·O2 차단밸브 닫기", (INT,), "system", (ANY_STATE,)),
    C("SHUTTERS_CLOSE", "셔터 닫기(MS·S1·S2)", (INT,), "system", (ANY_STATE,), (), (_a("which", "str", False),)),
    C("MV_CLOSE", "메인 밸브 닫기", (INT,), "system", (ANY_STATE,)),
    C("PLC_OUTPUTS_OFF", "PLC 비상 정지(버튼 코일 전부·히터 RUN·D00040·M00200 OFF)", (INT,), "system", (ANY_STATE,)),
    C("IG_FILAMENT_OFF", "이온 게이지 필라멘트 OFF", (INT,), "system", (ANY_STATE,)),
    C("BLACKBOX_DUMP", "PLC 블랙박스 파일로", (INT,), "system", (ANY_STATE,)),
    C("LOCK_CONTROLS", "조작 잠금(값 오래됨·링크 끊김)", (INT,), "system", (ANY_STATE,)),
    C("PLC_MARKER_WRITE", "PLC 재기동 마커 쓰기", (INT,), "system", (ANY_STATE,)),
    C("NOTIFY", "이벤트·알람 보내기", (INT,), "system", (ANY_STATE,), (), (_a("level", "str"), _a("code", "str"))),
)


def by_kind() -> dict:
    return {c.kind: c for c in COMMANDS}


def check_command(kind: str, source: Source, state: str, args: Optional[dict] = None) -> Optional[str]:
    """표만으로 판정할 수 있는 거부 사유(경로·상태). 통과면 None. 인자는 check_args(설정값 필요),
    PRECONDITIONS(장비 값 필요)는 core 가 따로 평가한다."""
    c = by_kind().get(kind)
    if c is None:
        return "UNKNOWN_COMMAND"
    if source not in c.sources:
        return "SOURCE_NOT_ALLOWED"
    if ANY_STATE not in c.states and state not in c.states:
        return "STATE_NOT_ALLOWED"
    return None


SV_LIMIT_KINDS = ("HEATER_SV", "HEATER_ON")      # value 상한 = min(HEATER_MAX_TEMP, PLC 보고 상한 D00013 — 0 보다 클 때)(v1)


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _unit(key: Optional[str]) -> str:
    if not key:
        return ""
    d = next((p for p in P.PARAMS if p.key == key), None)
    return d.unit if d is not None else ""


def check_args(c: CommandDef, args: Mapping[str, Any], cfg: Mapping[str, Any],
               sv_limit: Optional[float] = None) -> Optional[str]:
    """명령 표(ArgDef)로 인자 검사 — 틀리면 사람이 읽는 이유 문구(core 가 BAD_ARGS + detail), 통과면 None.
    required(값 없음·null 은 빠진 것), type(float·int = bool 이 아닌 유한한 숫자, int 는 정수값 / bool / str / enum /
    obj = dict / rows = list / any 는 통과), min·max, paramMin·paramMax(그 설정의 지금 값). 모르는 인자는 무시.
    HEATER_SV·HEATER_ON 의 value 는 PLC 가 보고한 상한(sv_limit, 0 보다 크면)과 HEATER_MAX_TEMP 중 작은 것까지(v1 main.py
    _read_heater_sv_input). 형식 밖 값의 뜻(PROCESS_ADJUST value·"둘 중 하나")은 core·엔진이 본다."""
    for a in c.args:
        v = args.get(a.name)
        if v is None:
            if a.required:
                return f"{a.name} — 값 필요"
            continue
        t = a.type
        if t in ("float", "int"):
            if not _num(v):
                return f"{a.name} {v!r} — 숫자여야 함"
            if t == "int" and not float(v).is_integer():
                return f"{a.name} {v!r} — 정수여야 함"
        elif t == "bool" and not isinstance(v, bool):
            return f"{a.name} {v!r} — true/false 여야 함"
        elif t == "str" and not isinstance(v, str):
            return f"{a.name} {v!r} — 문자열이어야 함"
        elif t == "enum" and not (isinstance(v, str) and v in (a.enum or ())):
            return f"{a.name} {v!r} — {'·'.join(a.enum or ())} 중 하나"
        elif t == "obj" and not isinstance(v, dict):
            return f"{a.name} — 객체(dict)여야 함"
        elif t == "rows" and not isinstance(v, list):
            return f"{a.name} — 목록이어야 함"
        if t not in ("float", "int"):
            continue
        lo = float(cfg[a.param_min]) if a.param_min else a.min
        hi = float(cfg[a.param_max]) if a.param_max else a.max
        src = [k for k in (a.param_min, a.param_max) if k]
        if c.kind in SV_LIMIT_KINDS and a.name == "value" and _num(sv_limit) and sv_limit > 0 \
                and (hi is None or sv_limit < hi):
            hi, src = float(sv_limit), ["PLC 상한 D00013"]
        if (lo is None or v >= lo) and (hi is None or v <= hi):
            continue
        unit = _unit(a.param_max or a.param_min)
        why = f" ({'·'.join(src)})" if src else ""
        if hi is None:
            return f"{a.name} {v:g} — {lo:g}{' ' + unit if unit else ''} 이상{why}"
        if lo is None:
            return f"{a.name} {v:g} — {hi:g}{' ' + unit if unit else ''} 이하{why}"
        return f"{a.name} {v:g} — 범위 {lo:g} ~ {hi:g}{' ' + unit if unit else ''}{why}"
    return None


def check_ig_filament(source, state: str, on: bool, plc_atm: Optional[bool], plc_fresh: bool) -> Optional[str]:
    """웹 IG_FILAMENT 판정: 경로·상태(유휴만) → 켜기면 ATM 센서(값이 신선해야). 통과면 None."""
    r = check_command("IG_FILAMENT", source, state, {"on": on})
    if r:
        return r
    if on and (not plc_fresh or plc_atm is None):
        return "IG_ATM_FILAMENT_ON"            # ATM 을 모르면 켜지 않는다
    if on and plc_atm:
        return "IG_ATM_FILAMENT_ON"
    return None
