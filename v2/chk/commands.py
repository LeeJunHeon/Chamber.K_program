"""챔버K 명령 종류·사유 코드·시작 조건·공정 상태별 허용 표.

경로
- web      : vanam-process — 공정·레시피·히터·PLC 조작·설정
- hmi      : 노트북 로컬 화면(로그인 없음, 127.0.0.1). 받는 명령은 CommandDef.hmi 로 나뉜다(5-20 ②).
             always  — 늘 받음: STOP·ALL STOP·부저 정지·히터 정지, 공정 중 값 바꾸기(PROCESS_ADJUST source local, 5-4)·
                       건너뛰기·일시정지·재개(5-15)
             operate — 설정 LOCAL_HMI_CONTROL 이 '보기와 정지만'(view_stop)이 아니면 받음(HMI_OPERATE: 공정 시작·적재 레시피
                       시작·해제·순서 고치기·PLC 버튼·필라멘트·히터 명령·수동 조작). view_stop 이면 HMI_CONTROL_OFF 로 거부.
                       잠금(LOCAL_HMI_RELOCK_MIN)은 노트북 화면이 한다 — 장비는 hello 로 설정·잠금 시간·지금 받는 명령을 알린다
             그 밖(설정 적용·되돌리기·레시피 적재·내부 조치)은 노트북에서 받지 않는다(SOURCE_NOT_ALLOWED)
- internal : 안전 엔진·공정 엔진 — 안전 조치(safety_rules 의 action)

v1 근거: integrations/erp_commands.py(명령·거부 규칙), main.py:_erp_exec_heater_command, core/process_service.py.
공정 중 원격 PLC 버튼(MV·Vent·Door·Turbo …)은 v1 처럼 모두 허용한다(사용자 결정 2026-10-20) — 실행한 것은 공정 기록에
PLC_MANUAL_IN_PROCESS(info)로 남긴다. "정말 하시겠습니까?" 확인은 웹 단계에서.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
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
    Reason("IG_DISABLED", "이온 게이지 꺼짐(베이스 압력 대기 불가)", "reject"),
    Reason("IG_ATM_FILAMENT_ON", "대기압 상태라 이온 게이지 필라멘트를 켤 수 없음(PLC ATM 센서)", "reject"),
    Reason("IG_FILAMENT_FAILED_CMD", "필라멘트 명령 결과 확인 실패", "fail"),
    Reason("COMMAND_NOT_ALLOWED", "장비에 보내지 않는 명령(예: 이온 게이지 디개스 C·D)", "reject"),
    Reason("HMI_CONTROL_OFF", "노트북 조작이 꺼져 있음(설정 '노트북 조작'이 보기와 정지만). 웹에서 하거나 설정을 바꿀 것", "reject"),
    Reason("DOOR_CONFLICT", "도어 올림·내림 동시 요청", "reject"),
    Reason("CONFIG_H_IN_PROCESS", "공정 중 H 등급 설정 변경 금지", "reject"),
    Reason("CLEANUP_RUNNING", "공정 밖 STOP 정리 중이라 끝난 뒤에 시작할 수 있음", "reject"),
    Reason("ADJUST_NOT_USED", "공정 중 수정: 이번 공정에서 쓰지 않는 장비·항목", "reject"),
    Reason("ADJUST_OUT_OF_RANGE", "공정 중 수정: 범위 밖(레시피와 같은 상한·펄스 ON/OFF 시간)", "reject"),
    Reason("ADJUST_NOT_APPLICABLE", "공정 중 수정: 지금 블록에 해당 없음(대기 끝내기는 대기·조건 대기 블록만)", "reject"),
    Reason("ADJUST_BUSY", "공정 중 수정: RF Pulse 가 시작하는 중이라 끝난 뒤에 할 수 있음", "reject"),
    Reason("SKIP_STALE", "건너뛰기: 화면이 본 단계·판이 지금과 달라 받지 않음. 다시 보고 누를 것", "reject"),
    Reason("PAUSE_STALE", "일시정지·재개: 화면이 본 단계·판이 지금과 달라 받지 않음. 다시 보고 누를 것", "reject"),
    Reason("EDIT_STALE", "순서 고치기: 화면이 본 판이 지금과 달라 받지 않음. 다시 받아 고칠 것", "reject"),
    Reason("SKIP_NOT_ALLOWED", "이 단계는 건너뛸 수 없음(끄기·닫기·정상 끝 정지 등)", "reject"),
    Reason("PAUSE_NOT_ALLOWED", "이 단계는 멈출 수 없음(바로 끝나는 명령 등)", "reject"),
    Reason("EDIT_TOUCHES_STARTED", "순서 고치기: 시작했거나 끝난 블록과 그 앞 순서는 바꿀 수 없음", "reject"),
    Reason("PROCESS_PAUSED", "이미 일시정지 중", "reject"),
    Reason("PROCESS_NOT_PAUSED", "일시정지 중이 아님", "reject"),
    Reason("MANUAL_ACTIVE", "수동 조작 중이라 받지 않음. 수동 조작을 먼저 끝낼 것", "reject"),
    Reason("MANUAL_BUSY", "수동 조작: 앞 조작이 진행 중이라 끝난 뒤에 할 수 있음", "reject"),
    Reason("MANUAL_BLOCK_NOT_ALLOWED", "수동 조작에서 받지 않는 조작(가스·압력·밸브·셔터·파워만)", "reject"),
    Reason("MANUAL_NOT_ACTIVE", "수동 조작 중이 아님", "reject"),
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
    Reason("COND_WAIT_TIMEOUT", "조건 대기 한도 초과(공정 중단)", "abort"),
    Reason("PRESSURE_WAIT_TIMEOUT", "압력 블록(SP2~SP4) 도달 한도 초과(공정 중단)", "abort"),
    Reason("SP_DEVICE_VALUE_UNKNOWN", "압력 설정점: 장비에 저장된 값을 읽지 못해 시작을 거부하거나 그 단계가 실패함(값 출처가 장비 값일 때)", "abort"),
    Reason("HEATER_TC1_DETACHED", "2단 제어 보정이 오래 너무 커서 히터 TC1 탈락이 의심됨", "abort"),
    Reason("HEATER_SETTLE_FAILED", "셔터 전 히터가 정착하지 않은 채 대기 상한을 넘음(공정 중단 설정)", "abort"),
    Reason("PAUSE_TIMEOUT", "일시정지가 한도를 넘어 정지 순서를 실행함", "abort"),
    Reason("PROGRAM_EXIT", "프로그램 강제 종료(콘솔 창 닫기·로그오프·윈도우 종료)로 파워 출력만 끄고 PLC 릴레이는 그대로 둠(결정 4)",
           "abort"),
    # 경고·정보
    Reason("FLOW_DEVIATION", "가스 유량 이탈(경고만)", "info"),
    Reason("PRESSURE_DEVIATION", "압력 이탈(경고만)", "info"),
    Reason("SP_DEVICE_CHANGED", "압력 설정점 장비 값이 시작 때와 달라 새로 읽은 값을 목표로 씀", "info"),
    Reason("DC_OFF_UNCONFIRMED", "DC 꺼짐이 확인되지 않아 다시 시도하는 중", "info"),
    Reason("RFP_OFF_UNCONFIRMED", "RF Pulse 꺼짐이 확인되지 않음", "info"),
    Reason("HEATER_SATURATED", "히터 DAC 가 포화해 상한으로 제한함", "info"),
    Reason("HEATER_WD_DELAY", "히터 하트비트 지연", "info"),
    Reason("HEATER_OUTPUT_DEAD_WARN", "히터 출력 죽음(경고)", "info"),
    Reason("HEATER_RESIDUAL_RUN", "이전 세션의 히터 RUN 이 남아 있어 정리함", "info"),
    Reason("HEATER_HOLD_DEMOTED", "히터 유지 모드 tc2 → dac 강등", "info"),
    Reason("HEATER_TC1_DEVIATION", "TC1 편차 큼", "info"),
    Reason("PLC_DISCONNECTED", "PLC 끊김 지속(알림)", "info"),
    Reason("PLC_RECOVERED_SAFE", "PLC 복구 뒤 안전 상태 다시 적용", "info"),
    Reason("RF_RESIDUAL_OUTPUT", "시작 때 RF 출력이 남아 있음(램프다운한 뒤 끄기)", "info"),
    Reason("RF_ENABLE_RESIDUAL", "시작 때 RF enable 만 켜져 있음(끄기, v1 정상 종료 흔적)", "info"),
    Reason("DC_RESIDUAL_OUTPUT", "시작 때 DC 출력이 켜져 있음(끄기)", "info"),
    Reason("RFP_RESIDUAL_OUTPUT", "시작 때 RF Pulse 출력이 켜져 있음", "info"),
    Reason("GAS_RESIDUAL_FLOW", "시작 때 가스가 흐르고 있지만 끄지 않음(확인 필요)", "info"),
    Reason("PLC_RECONNECTED", "PLC 끊김 알림 뒤 링크 복구", "info"),
    Reason("DC_RECOVERED_SAFE", "DC 통신 복구 뒤 출력 OFF 다시(공정 없을 때)", "info"),
    Reason("RFP_RECOVERED_SAFE", "RF Pulse 통신 복구 뒤 RF OFF 다시(공정 없을 때)", "info"),
    Reason("MV_INTERLOCK_BLIP", "MV 인터록이 지속 시간 안에 다시 참(기록만)", "info"),
    Reason("COMM_LONG_OUTAGE", "장비 장기 두절", "info"),
    Reason("LINK_STALE", "장비 값이 오래되어 화면 조작을 잠금", "info"),
    Reason("PLC_MANUAL_IN_PROCESS", "공정 중 PLC 버튼 실행 기록(대상·켜기/끄기·출처·시각)", "info"),
    Reason("OUTPUTS_LEFT_ON", "정상 끝 정지에서 뺀 단계 때문에 일부러 남긴 출력(히터·유량·차단밸브·셔터·MFC 밸브·필라멘트)", "info"),
    Reason("AUTO_END_STOP", "목록 끝이 정상 끝 정지가 아니어서 기본 끝 정지를 덧붙임", "info"),
    Reason("HEATER_HANDOVER", "정상 끝 정지에서 히터를 남겨 소유를 공정에서 수동(웹 히터 카드)으로 넘김", "info"),
    Reason("OUTPUTS_CLEANUP", "공정 밖 STOP 으로 지난 공정이 남긴 출력을 정리함(단계별 결과)", "info"),
    Reason("HEATER_NO_TARGET", "히터 도달 대기 블록인데 히터 목표가 없어 건너뜀", "info"),
    Reason("RF_OFF_FAILED", "파워 끄기 블록의 RF OFF 결과 오류", "info"),
    Reason("PROCESS_ADJUST", "공정 중 값 수정 기록(장비·항목·이전 → 새 값·출처·결과)", "info"),
    Reason("ADJUST_SETTLE_TIMEOUT", "공정 중 수정 뒤 한도 안에 새 목표에 도달하지 않음(이탈 판정 다시 켜기)", "info"),
    Reason("HEATER_TC2_LOST", "히터 TC2 를 잃어 TC1 제어로 바꿈(히터는 계속 운전)", "info"),
    Reason("HEATER_SV2_CAPPED", "히터 TC2 목표 상한 때문에 목표 도달 불가", "info"),
    Reason("HEATER_TC2_TC1_DEVIATION", "TC2 단독 제어 중 TC1 이 목표에서 멀다(기록용, 공정은 계속)", "info"),
    Reason("HEATER_DEMOTE_FAILED", "직전 출력이 없어 히터 출력 상한을 고정하지 못하고 TC1 제어로 바꿈", "info"),
    Reason("HEATER_SETTLE_SKIPPED", "2단 제어가 돌고 있지 않아 셔터 전 히터 정착 확인을 생략함", "info"),
    Reason("HEATER_SETTLE_TIMEOUT", "셔터 전 히터가 정착하지 않아 경고를 남기고 셔터를 열어 진행함", "info"),
    Reason("HEATER_SETTLE_NOT_CHECKED", "셔터 전 히터 정착 확인 없이 진행(사용자 건너뛰기)", "info"),
    Reason("PROCESS_SKIP", "공정 중 단계 건너뛰기 기록(누가·단계·정리한 것)", "info"),
    Reason("PROCESS_PAUSE", "공정 중 일시정지 기록(누가·단계·남은 시간)", "info"),
    Reason("PROCESS_RESUME", "일시정지 재개 기록(누가·멈춘 시간)", "info"),
    Reason("PROCESS_EDIT", "공정 중 순서 고치기 기록(판 번호·누가·바뀐 것·전체 블록)", "info"),
    Reason("PAUSE_LONG", "일시정지가 길어져 곧 정지 순서를 실행함", "info"),
    # 수동 조작(5-20 ③)
    Reason("MANUAL_BLOCK", "수동 조작 기록(누가·웹/노트북·조작·결과)", "info"),
    Reason("MANUAL_END", "수동 조작 끝내기(정지 순서로 모두 끄기)", "info"),
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
    Pre("NO_RECIPE_LOADED", "레시피가 적재되지 않음(수동 시작)", "RECIPE_LOADED", "process_service.start 원격 수동"),
    Pre("MV_OPEN", "메인 밸브가 켜져 있고 값이 신선해야 함(M00003)", "MV_NOT_OPEN", "main.py:3675"),
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
    PlcButton("rotary", "rotary", "Rotary"),
    PlcButton("rv", "rv", "RV"),
    PlcButton("fv", "fv", "FV"),
    PlcButton("mv", "mv", "MV"),
    PlcButton("vent", "vent", "Vent"),
    PlcButton("turbo", "turbo", "Turbo"),
    PlcButton("door_up", "door_up", "Door 올림"),
    PlcButton("door_down", "door_down", "Door 내림"),
    PlcButton("ar_valve", "ar_valve", "Ar 밸브"),
    PlcButton("o2_valve", "o2_valve", "O₂ 밸브"),
    PlcButton("ms", "ms", "Main 셔터"),
    PlcButton("s1", "s1", "Gun1 셔터"),
    PlcButton("s2", "s2", "Gun2 셔터"),
    PlcButton("buzz_stop", "buzz_stop", "부저 정지"),
    PlcButton("ion", "ion_remote_on", "Ionizer"),
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
    bench_ok: bool = False        # bench(장비 쓰기 금지)에서도 받는 명령 — 장비에 쓰지 않는다(5-14, core 가 이 값을 본다)
    hmi: str = ""                 # 노트북 경로: always(늘) / operate(LOCAL_HMI_CONTROL ≠ view_stop 일 때) / ""(받지 않음) — 5-20 ②


def _a(name, type_, required=True, **kw) -> ArgDef:
    return ArgDef(name=name, type=type_, required=required, **kw)


C = CommandDef
STOPPABLE = ACTIVE_STATES + ("STOPPING",)

COMMANDS: tuple = (
    # 정지 — 로컬 화면에서도. bench 모드에서는 worker 가 BENCH_READ_ONLY 로 거부하므로 로컬 화면은
    # "읽기 전용 — 조작은 장비 앞면으로" 를 표시해야 한다(로컬 화면 단계 할 일)
    C("PROCESS_STOP", "공정 STOP", (WEB, HMI), "operator", (ANY_STATE,),
      desc="공정을 멈춘다. 공정이 없으면 지난 공정이 남긴 출력만 정리하고, 남은 것이 없으면 아무것도 하지 않고 성공한다(v1 과 같게). 로컬 화면 STOP 도 "
           "같다(OUTPUTS_CLEANUP)",
      v1="PROCESS_STOP"),
    C("ALL_STOP", "ALL STOP", (WEB, HMI, INT), "operator", (ANY_STATE,), desc="공정 여부와 상관없이 모든 출력을 "
                                                                              "끈다(safety_rules.ALL_STOP_SEQUENCE)",
      v1="ALL_STOP"),
    C("BUZZ_STOP", "부저 정지", (WEB, HMI), "operator", (ANY_STATE,), ("PLC_LINK_UP",), v1="BuzzStop 버튼",
        desc="PLC 의 부저 정지 출력을 켠다. PLC 통신이 없으면 거부되고, 공정 중에 누르면 공정 기록에 남는다."),
    C("HEATER_STOP", "히터 정지", (WEB, HMI), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      desc="히터 레시피를 멈추고 RUN 을 0 으로 한다. 공정이 히터를 쓰는 중이면 공정도 중단한다(이상이 아닌 사용자 정지 USER_STOP)"),
    # 공정
    C("PROCESS_START", "수동 공정 시작", (WEB,), "operator", IDLE_STATES,
      ("NOT_RUNNING", "NO_RECIPE_LOADED", "PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS", "IG_FOR_BASE", "RECIPE_VALID"),
      (_a("row", "obj", False, desc="레시피 한 행이다. 수동 공정은 히터를 쓰지 않아 히터 열은 무시한다(recipe.PROCESS_COLUMNS)"),
       _a("blocks", "rows", False, desc="row 대신 쓰는 블록 목록이다(blocks.BLOCK_DEFS)"),
       _a("name", "str", False, desc="실행 이름(공정 기록)이다. 비우면 행의 process_name, 그것도 없으면 Single CHK 이다")),
      desc="한 행 공정 또는 블록 레시피 하나를 바로 시작한다(row·blocks)", v1="PROCESS_START"),
    C("RECIPE_LOAD", "공정 레시피 적재", (WEB,), "operator", IDLE_STATES, ("RECIPE_VALID", "IG_FOR_BASE"),
      (_a("name", "str"), _a("rows", "rows", False, desc="행 목록이다. v1 행·딜레이 행·블록 행 {process_name, blocks} 을 쓸 수 "
                                                         "있다(recipe.PROCESS_COLUMNS)"),
       _a("blocks", "rows", False, desc="rows 대신 쓰는 블록 목록 하나이다(블록 행 하나와 같다)")),
      desc="공정 레시피(행 목록 또는 블록 레시피 하나)를 장비에 올린다. 블록 레시피 검사는 값 범위·꺼진 장비만 본다(순서 규칙 없음, rows·blocks)", v1="RECIPE_PROCESS_RUN"),
    C("RECIPE_CLEAR", "레시피 적재 해제", (WEB,), "operator", (ANY_STATE,),
      desc="적재가 없으면 아무것도 하지 않고 성공, 공정 중이면 PROCESS_RUNNING(v1 B5)", v1="RECIPE_CLEAR"),
    C("RECIPE_START", "적재 레시피 시작", (WEB,), "operator", IDLE_STATES,
      ("RECIPE_LOADED", "PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS", "HEATER_RECIPE_NO_CONFLICT"),
      v1="RECIPE_PROCESS_START",
        desc="적재한 공정 레시피를 첫 행부터 차례로 시작한다. Main 밸브가 열려 있고 MV 인터록이 켜져 있어야 하며, 히터 가스 유지 중이거나 히터 레시피가 도는데 레시피에 히터 행이 있으면 "
             "거부된다."),
    # 히터
    C("HEATER_SV", "히터 목표 온도", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("value", "float", min=0.0, param_max="HEATER_MAX_TEMP"),), v1="HEATER_SV",
        desc="히터 목표 온도를 바꾼다. 히터가 켜져 있으면 기본 램프 속도로 새 목표를 따라가고, 꺼져 있으면 목표만 PLC 에 써 두며, 히터 레시피 중이거나 공정이 히터를 쓰는 중이면 "
             "거부된다."),
    C("HEATER_ON", "히터 켜기", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_ITL", "HEATER_NO_FAULT", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("value", "float", min=0.0, param_max="HEATER_MAX_TEMP"), _a("use_ar", "bool", False),
       _a("ar_flow", "float", False, min=0.0, param_max="MFC_AR_FS_SCCM"),
       _a("use_o2", "bool", False), _a("o2_flow", "float", False, min=0.0, param_max="MFC_O2_FS_SCCM"),
       _a("wp_mtorr", "float", False, min=0.0)),
      desc="히터를 켠다. 가스를 고르면 히터 가스 시퀀스를 실행하는데, 공정과 MFC 를 함께 쓸 수 없어 공정이 없을 때만 된다(NOT_RUNNING)", v1="HEATER_ONOFF on"),
    C("HEATER_OFF", "히터 끄기", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      v1="HEATER_ONOFF off",
        desc="수동으로 켠 히터를 끈다(히터 레시피 중이거나 공정이 히터를 쓰는 중이면 거부). 히터 가스를 쓰고 있었다면 온도가 '냉각 보호 가스 해제 온도' 이하가 될 때까지 가스·압력을 "
             "유지하고, 가스 준비 중이었다면 준비를 취소하고 가스를 해제한다."),
    C("HEATER_RESET", "히터 이상 리셋", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP", "HEATER_IS_FAULT", "HEATER_RUN_OFF"),
      v1="HEATER_RESET",
        desc="히터 이상(과온·TC 이상·워치독) 래치를 푸는 리셋 신호를 PLC 에 보낸다. 히터 이상 상태가 아니거나 히터 운전이 켜져 있으면 거부된다."),
    C("HEATER_GAS_RELEASE", "히터 가스 해제", (WEB,), "operator", IDLE_STATES, ("HEATER_GAS_ACTIVE",), v1="HEATER_GAS_RELEASE",
        desc="히터용으로 넣은 가스·압력을 해제한다. 유량을 끄고 MFC 밸브를 연 뒤 차단밸브를 닫으며, 공정이 돌지 않을 때만 받는다."),
    C("HEATER_RECIPE_RUN", "히터 레시피 시작", (WEB,), "operator", (ANY_STATE,),
      ("HEATER_ENABLED", "PLC_LINK_UP", "HEATER_ITL", "HEATER_NO_FAULT", "HEATER_RECIPE_IDLE", "HEATER_NOT_PROCESS_OWNED"),
      (_a("name", "str"), _a("steps", "rows", desc="recipe.HEATER_STEP_COLUMNS"), _a("gas", "obj", False, desc="recipe.HEATER_GAS_COLUMNS")),
      v1="RECIPE_HEATER_RUN",
        desc="히터 레시피를 검사한 뒤 첫 스텝부터 실행한다. 가스를 고르면 가스·압력을 먼저 준비한 뒤 시작하며, 히터 인터록이 정상이 아니거나 히터 이상이거나 공정이 히터를 쓰는 중이면 "
             "거부된다."),
    C("HEATER_RECIPE_STOP", "히터 레시피 정지", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      desc="레시피가 없어도 안전하게 끝낸다(RUN=0·램프 복구·SV=PV, v1 main.py:1953)", v1="RECIPE_HEATER_STOP"),
    C("HEATER_RECIPE_HOLD", "히터 레시피 일시정지/재개", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",),
      (_a("on", "bool"),), v1="HEATER_RECIPE_HOLD",
        desc="실행 중인 히터 레시피를 일시정지하거나 재개한다. 일시정지 동안 레시피 시계(SOAK 남은 시간 포함)가 멈추고 승온 중이면 목표를 지금 위치에 세우며, 히터는 켜진 채 유지된다."),
    C("HEATER_RECIPE_SKIP", "히터 레시피 단계 건너뛰기", (WEB,), "operator", (ANY_STATE,), ("HEATER_RECIPE_ACTIVE",),
      v1="HEATER_RECIPE_STEP",
        desc="실행 중인 히터 레시피의 지금 스텝을 끝내고 다음 스텝을 바로 시작한다. 마지막 스텝이면 레시피를 마치고, '레시피 끝 온도 유지'가 꺼져 있으면 히터를 끈다."),
    # PLC 조작 — operator(관리자 + 그 장비 담당자, 사용자 결정 2026-10-22 — 웹이 role 로 정한다)
    C("PLC_BUTTON", "PLC 버튼", (WEB,), "operator", (ANY_STATE,), ("PLC_LINK_UP",),
      (_a("target", "enum", enum=tuple(b.target for b in PLC_BUTTONS)), _a("on", "bool")),
      desc="공정 중에도 모두 허용한다(v1 과 같게). 공정 중 실행은 공정 기록에 남긴다(PLC_MANUAL_IN_PROCESS info)", v1="PLC 버튼 15종"),
    C("IG_FILAMENT", "Ion Gauge Filament", (WEB,), "operator", IDLE_STATES, ("IG_ON_NOT_ATM",), (_a("on", "bool"),),
      desc="유휴 상태에서만(공정 중에는 공정 엔진만 A/B). 켜기는 ATM 센서가 켜져 있으면 거부. "
           "무응답 유예 IG_FILAMENT_SWITCH_QUIET_SEC, B 뒤 IG_RELIGHT_LOCKOUT_SEC 안의 A 는 늦춤"),
    C("PROCESS_ADJUST", "공정 중 값 수정", (WEB, HMI), "operator", (ANY_STATE,), ("RUNNING",),
      (_a("device", "enum", enum=("mfc", "dc", "rf", "rfpulse", "heater", "process")), _a("item", "str"),
       _a("value", "any", desc="blocks.ADJUST_ITEMS 의 형식·범위"), _a("source", "enum", False, enum=("web", "local"),
                                                                     desc="비우면 경로(web·hmi → local)")),
      desc="이번 공정의 그 값을 바꾼다. 지금 쓰는 블록이면 바로 바꾸고(드라이버 경로·되읽기), 아직 오지 않은 블록이면 그 블록이 새 값을 쓴다. 공정 밖이나 정지 중이면 "
           "받지 않는다(PROCESS_NOT_RUNNING, 기록 ProcEvent adjust·PROCESS_ADJUST)", v1="v2"),
    # 공정 중 건너뛰기·일시정지·순서 고치기(5-15) — 판단은 장비(process_engine), 규칙은 blocks.BlockDef skip·pause
    C("PROCESS_SKIP", "지금 단계 건너뛰기", (WEB, HMI), "operator", ACTIVE_STATES, (),
      (_a("no", "str", desc="지금 단계 번호이다. 반복 안이면 그 안쪽 번호, 딜레이 행은 delay 이다(state.run.current)"),
       _a("revision", "int", min=0, desc="지금 판 번호(state.run.revision, 딜레이 행 0)")),
      desc="지금 단계를 끝내고 다음으로 간다. 정리는 그 단계의 정의를 따른다(기다림만 끝내기·Main 셔터 닫기·필라멘트 끄기·보낸 명령 그대로). 화면이 본 단계가 다르거나 "
           "건너뛸 수 없는 단계이거나 정지 순서 중이면 받지 않는다(SKIP_STALE·SKIP_NOT_ALLOWED·STATE_NOT_ALLOWED, 기록 ProcEvent skip·PROCESS_SKIP)",
      v1="v2"),
    C("PROCESS_PAUSE", "지금 단계 일시정지", (WEB, HMI), "operator", ACTIVE_STATES, (),
      (_a("no", "str", desc="지금 단계 번호"), _a("revision", "int", min=0, desc="지금 판 번호")),
      desc="지금 단계를 멈춘다. 대기·딜레이는 남은 시간을, 메인은 Main 셔터를 닫고 남은 시간을, 도달 대기는 판정과 시간 한도를 멈춘다. 안전 규칙·STOP·ALL "
           "STOP·공정 중 수정은 그대로이며, 한도를 넘으면 정지 순서를 실행한다(PROCESS_PAUSE_MAX_SEC·PAUSE_TIMEOUT)", v1="v2"),
    C("PROCESS_RESUME", "일시정지 재개", (WEB, HMI), "operator", ACTIVE_STATES, (),
      (_a("revision", "int", min=0, desc="지금 판 번호"),),
      desc="멈춘 단계를 이어서 한다. 메인 단계는 셔터를 다시 열고, 도달 대기는 안정 유지 시간을 처음부터 잰다", v1="v2"),
    C("PROCESS_EDIT", "진행 중 순서 고치기", (WEB,), "operator", ACTIVE_STATES, (),
      (_a("revision", "int", min=1, desc="지금 판 번호"),
       _a("blocks", "rows", desc="지금 행의 새 블록 전체 목록이다. 장비가 실행하는 정리 목록을 고친 것이다(state.run.editBase.blocks, 5-16)")),
      desc="아직 시작하지 않은 블록만 추가·삭제·순서·값을 바꾼다. 시작한 블록과 그 앞은 그대로여야 하며(키 순서·기본값 생략·0/0.0 차이는 같다고 본다), 적재 때와 같은 "
           "검사 뒤 판을 1 올리고 지금 블록은 끊지 않고 남은 부분을 새 목록으로 실행한다(EDIT_TOUCHES_STARTED·check_blocks, 기록 ProcEvent "
           "edit·PROCESS_EDIT)", v1="v2"),
    # 수동 조작(5-20 ③) — 블록을 하나씩 실행하는 공정(공정 엔진·블록 실행기·감시·안전 규칙·정지 순서를 그대로 쓴다)
    C("MANUAL_BLOCK", "수동 조작", (WEB,), "operator", (ANY_STATE,),
      ("PLC_FRESH", "MV_OPEN", "MV_ITL_ON", "NO_HEATER_GAS"),
      (_a("block", "obj", desc="조작 하나이다. 가스·압력·밸브·셔터·파워 조작만 쓸 수 있고 값 검사는 블록 레시피와 "
                               "같다({type, params, label?}, blocks.MANUAL_BLOCK_TYPES·check_blocks)"),),
      desc="가스·압력·밸브·셔터·파워 조작 하나를 실행한다. 공정이 없으면(대기·적재 없음) 수동 조작을 시작하며, 시작 조건·검사는 공정 시작과 같고 끝 정지는 붙이지 않는다. "
           "조작이 끝나면 다음 조작을 기다리고 감시·안전 규칙은 공정과 같다. 앞 조작이 진행 중이거나 공정 중이거나 수동 조작에서 받지 않는 조작이면 받지 "
           "않는다(MANUAL_WAIT·MANUAL_BUSY·PROCESS_RUNNING·MANUAL_BLOCK_NOT_ALLOWED, 기록 ProcEvent manual·MANUAL_BLOCK)",
      v1="v2"),
    C("MANUAL_END", "수동 조작 끝내기", (WEB,), "operator", (ANY_STATE,), (),
      desc="STOP 과 같은 정지 순서로 가스·파워·셔터를 모두 끄고 수동 조작을 끝낸다. 수동 조작 중이 아니면 받지 않고, 정지 순서 중이면 아무것도 하지 않고 "
           "성공한다(run_end done·MANUAL_END·MANUAL_NOT_ACTIVE)", v1="v2"),
    # 설정
    C("CONFIG_APPLY", "설정 적용", (WEB,), "admin", (ANY_STATE,), ("CONFIG_LIVE_ONLY_IN_PROCESS",),
      (_a("version", "int", min=1), _a("values", "obj"), _a("schemaHash", "str")),
      desc="검사 실패면 버전 전체 거부. idle/restart 키는 대기로 두고 config_ack 로 알림. 이력(누가·언제·이전 → 새 값)은 "
           "config_history.jsonl + config_ack changes"),
    C("CONFIG_ROLLBACK", "이전 설정으로 되돌리기", (WEB,), "admin", (ANY_STATE,), ("CONFIG_LIVE_ONLY_IN_PROCESS",),
      (_a("version", "int", False, min=1, desc="새 버전 번호로 웹이 정하며 지금 버전보다 커야 한다. 비우면 지금 버전 + 1 이다(아니면 "
                                               "CONFIG_VERSION_STALE)"),),
      desc="직전 버전 설정 값을 새 버전으로 적용한다. 결과는 config_ack 로 알린다(ConfigStore.rollback·config_prev.json, kind "
           "rollback·version·values)"),
    # 내부(안전·공정 엔진) 조치 — safety_rules 의 action 이 이것들을 쓴다
    C("PROCESS_ABORT", "공정 중단", (INT,), "system", STOPPABLE, (), (_a("reason", "str"),)),
    C("START_REJECT", "시작 거부", (INT,), "system", (ANY_STATE,), (), (_a("reason", "str"),)),
    C("DC_SAFE_OFF", "DC 출력 끄기", (INT,), "system", (ANY_STATE,)),
    C("RF_SAFE_OFF", "RF 출력 끄기", (INT,), "system", (ANY_STATE,)),
    C("RFP_SAFE_OFF", "RF Pulse 출력 끄기", (INT,), "system", (ANY_STATE,)),
    C("RF_RAMP_DOWN", "RF 천천히 내리기", (INT,), "system", (ANY_STATE,)),
    C("HEATER_FORCE_OFF", "히터 운전 끄기", (INT,), "system", (ANY_STATE,)),
    C("HEATER_RECIPE_ABORT", "히터 레시피 중단", (INT,), "system", (ANY_STATE,), (), (_a("reason", "str"),)),
    C("HEATER_PUSH_CONFIG", "히터 한계값 쓰기", (INT,), "system", (ANY_STATE,)),
    C("HEATER_CLAMP_MV", "히터 출력 상한 걸기", (INT,), "system", (ANY_STATE,), (), (_a("value", "int"),)),
    C("HEATER_GAS_HOLD", "냉각 보호 가스 유지", (INT,), "system", (ANY_STATE,)),
    C("HEATER_RESIDUAL_CLEAR", "남은 히터 운전 정리", (INT,), "system", (ANY_STATE,)),
    C("GAS_FLOW_OFF", "MFC 유량 모두 끄기", (INT,), "system", (ANY_STATE,)),
    C("GAS_EXHAUST", "MFC 밸브 열기", (INT,), "system", (ANY_STATE,)),
    C("GAS_VALVES_CLOSE", "Ar·O₂ 밸브 닫기", (INT,), "system", (ANY_STATE,)),
    C("SHUTTERS_CLOSE", "셔터 모두 닫기", (INT,), "system", (ANY_STATE,), (), (_a("which", "str", False),)),
    C("MV_CLOSE", "MV 닫기", (INT,), "system", (ANY_STATE,)),
    C("PLC_OUTPUTS_OFF", "PLC 비상 정지 명령", (INT,), "system", (ANY_STATE,),
      desc="버튼 코일 전부·히터 RUN·D00040·M00200 OFF"),
    C("IG_FILAMENT_OFF", "Filament 끄기", (INT,), "system", (ANY_STATE,)),
    C("BLACKBOX_DUMP", "PLC 블랙박스 저장", (INT,), "system", (ANY_STATE,)),
    C("LOCK_CONTROLS", "조작 잠금", (INT,), "system", (ANY_STATE,)),
    C("PLC_MARKER_WRITE", "PLC 재기동 표시 쓰기", (INT,), "system", (ANY_STATE,)),
    C("NOTIFY", "기록·알람 보내기", (INT,), "system", (ANY_STATE,), (), (_a("level", "str"), _a("code", "str"))),
)

# bench 에서도 받는 명령(장비에 쓰지 않음) — 이 목록 한 곳만(core·묶음 commands[].benchOk 가 같은 값을 본다)
_BENCH_OK = frozenset({"RECIPE_LOAD", "RECIPE_CLEAR", "CONFIG_APPLY", "CONFIG_ROLLBACK"})
assert _BENCH_OK <= {c.kind for c in COMMANDS}
COMMANDS = tuple(replace(c, bench_ok=c.kind in _BENCH_OK) for c in COMMANDS)

# 노트북 조작(5-20 ②) — 설정 LOCAL_HMI_CONTROL 이 view_stop 이 아니면 노트북에서도 받는 명령(관리자 설정·내부 조치는 넣지 않는다).
# 위 표에서 hmi 경로가 있는 명령(정지·공정 중 값 바꾸기·건너뛰기·일시정지·재개)은 always — 설정과 상관없이 늘 받는다.
HMI_CONTROL_VALUES = ("view_stop", "unlock", "always")
HMI_OPERATE = ("PROCESS_START", "RECIPE_START", "RECIPE_CLEAR", "PROCESS_EDIT", "PLC_BUTTON", "IG_FILAMENT",
               "HEATER_SV", "HEATER_ON", "HEATER_OFF", "HEATER_RESET", "HEATER_GAS_RELEASE", "HEATER_RECIPE_RUN",
               "HEATER_RECIPE_STOP", "HEATER_RECIPE_HOLD", "HEATER_RECIPE_SKIP", "MANUAL_BLOCK", "MANUAL_END")
assert set(HMI_OPERATE) <= {c.kind for c in COMMANDS}
# 수동 조작 중(5-20 ③): 공정을 새로 시작하는 명령은 MANUAL_ACTIVE, 필라멘트는 공정이 없을 때처럼(허용 상태를 대기로 본다)
MANUAL_EXCLUSIVE = ("PROCESS_START", "RECIPE_LOAD", "RECIPE_START", "RECIPE_CLEAR", "PROCESS_EDIT")
IDLE_IN_MANUAL = ("IG_FILAMENT",)
COMMANDS = tuple(replace(c, hmi="always") if HMI in c.sources else
                 replace(c, hmi="operate", sources=c.sources + (HMI,)) if c.kind in HMI_OPERATE else c for c in COMMANDS)
assert all(c.role == "operator" for c in COMMANDS if c.hmi == "operate")      # 관리자 설정·내부 조치는 노트북에서 받지 않는다


def by_kind() -> dict:
    return {c.kind: c for c in COMMANDS}


def hmi_allows(c: CommandDef, hmi_control: Optional[str]) -> bool:
    """노트북 경로에서 이 명령을 지금 받는가 — hmi_control = 설정 LOCAL_HMI_CONTROL 값(모르면 view_stop 처럼)."""
    return c.hmi == "always" or (c.hmi == "operate" and hmi_control in ("unlock", "always"))


def hmi_kinds(hmi_control: Optional[str]) -> list:
    """노트북 경로에서 지금 받는 명령 종류(hello.commandKinds) — 표 순서."""
    return [c.kind for c in COMMANDS if hmi_allows(c, hmi_control)]


def check_command(kind: str, source: Source, state: str, args: Optional[dict] = None,
                  hmi_control: Optional[str] = None) -> Optional[str]:
    """표만으로 판정할 수 있는 거부 사유(경로·상태). 통과면 None. 인자는 check_args(설정값 필요),
    PRECONDITIONS(장비 값 필요)는 core 가 따로 평가한다. 노트북(hmi) 경로의 operate 명령은 hmi_control(설정
    LOCAL_HMI_CONTROL)이 view_stop 이거나 없으면 HMI_CONTROL_OFF."""
    c = by_kind().get(kind)
    if c is None:
        return "UNKNOWN_COMMAND"
    if source not in c.sources:
        return "SOURCE_NOT_ALLOWED"
    if source == HMI and not hmi_allows(c, hmi_control):
        return "HMI_CONTROL_OFF"
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


def check_ig_filament(source, state: str, on: bool, plc_atm: Optional[bool], plc_fresh: bool,
                      hmi_control: Optional[str] = None) -> Optional[str]:
    """IG_FILAMENT 판정: 경로·상태(유휴만) → 켜기면 ATM 센서(값이 신선해야). 통과면 None."""
    r = check_command("IG_FILAMENT", source, state, {"on": on}, hmi_control)
    if r:
        return r
    if on and (not plc_fresh or plc_atm is None):
        return "IG_ATM_FILAMENT_ON"            # ATM 을 모르면 켜지 않는다
    if on and plc_atm:
        return "IG_ATM_FILAMENT_ON"
    return None
