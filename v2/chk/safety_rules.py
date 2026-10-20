"""챔버K 안전 규칙 표 — 1단계 보고 A-5 의 S1~S45 + v2 추가 규칙 + STOP·ALL STOP 순서.

평가 위치(where)
- plc250       : PLC worker 의 250 ms 내부 폴링마다(MV·인터록·히터 비트처럼 빨라야 하는 것)
- tick         : 1초 tick 마다(스냅샷 한 행으로 판정)
- event:<이름> : 사건 때(링크 복구, 첫 폴링, RUN 하강 …)
- state:<상태> : 공정 상태 기계의 그 상태 안(한도·판정)
- command      : 명령 검사 때(거부)
- config       : 설정 검사 때
- worker       : 장비 worker 가 늘 지키는 동작(maintain)

종류(kind)
- trip     : 조건이 지속되면 조치(중단·OFF)
- reject   : 명령·시작·설정을 거부
- notify   : 경고·알림만
- recover  : 복구 때 안전 상태 다시 적용
- maintain : 늘 하는 동작(하트비트·보류·잠금). 조치가 없을 수 있다
- guard    : 다른 규칙의 판정을 막는 조건(이중 중단 방지 등)

조치(actions)는 commands.COMMANDS 의 내부 명령(kind)과 인자다. 사유는 commands 의 사유 코드.
values 는 snapshot_fields.columns() 의 열 이름이다(시험이 존재를 확인한다).

지속 시간(duration)은 실제 시각이다(4-3 결정): v1 은 장비마다 다른 주기로 횟수를 셌으므로 v2 는 v1 에서 실제로 걸리던
시간(횟수 × v1 판정 주기)을 *_SEC 파라미터로 쓴다(params 의 v1_period_s). 판정은 v2.common.judge.Persist —
값이 ok·late 인 표본만 판정에 쓰고, stale·fail·suppressed tick 은 판정을 바꾸지 않는다(시작 시각을 지우지도 확정하지도 않음).
통신이 끊긴 경우는 따로 두절 규칙(S6·S30·S34C·S39B)이 잡는다.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Act:
    kind: str
    args: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Rule:
    id: str
    name: str
    kind: str
    where: str
    when: str                     # 조건(사람이 읽는 식)
    values: tuple = ()            # 쓰는 스냅샷 열
    params: tuple = ()            # 쓰는 ParamDef 키
    duration: str = "0"           # 지속 시간: "0"(즉시) / ParamDef 키 / 설명
    states: tuple = ("*",)        # 평가하는 공정 상태("*" 전부, "*ACTIVE" 진행 상태 전부)
    actions: tuple = ()
    release: str = ""             # 해제 조건
    v1: str = ""
    pending_confirm: bool = False
    needs: str = ""               # 래더·배선 변경 필요
    on_hold: bool = False         # 보류(사용자 결정) — 구현하지 않음


def _abort(code):
    return Act("PROCESS_ABORT", {"reason": code})


def _rej(code):
    return Act("START_REJECT", {"reason": code})


def _note(level, code):
    return Act("NOTIFY", {"level": level, "code": code})


R = Rule
ACT = ("*ACTIVE",)

RULES: tuple = (
    # ── MV·인터록 ──
    R("S1", "메인 밸브 시작 인터록", "reject", "command",
      "시작(PROCESS_START·RECIPE_START·HEATER_ON 가스) 때 plc.mv 와 plc.mv_interlock 이 모두 참이고 PLC 값이 신선",
      ("plc.mv", "plc.mv_interlock", "plc.q", "plc.age_s"), ("HEATER_STALE_SEC",),
      actions=(_rej("MV_NOT_OPEN"), _rej("MV_INTERLOCK_OFF"), _rej("MV_STATE_UNKNOWN")),
      v1="main.py:3675 _check_main_valve_open (읽기 실패도 거부)"),
    R("S2", "공정 중 MV 닫힘", "trip", "plc250", "plc.mv 가 거짓(첫 값 포함)", ("plc.mv",), duration="0", states=ACT,
      actions=(_abort("MV_CLOSED"),), v1="main.py:3842 _on_plc_bit_changed"),
    R("S3", "MV 인터록 해제 지속", "trip", "plc250", "plc.mv_interlock 거짓이 지속", ("plc.mv_interlock",),
      ("MV_INTERLOCK_ABORT_MS",), "MV_INTERLOCK_ABORT_MS", ACT, (_abort("MV_INTERLOCK_LOST"),),
      release="지속 시간 안에 다시 참이면 기록만(info)", v1="main.py:3878 _mv_itl_timer"),
    R("S4", "정지 중 MV 판정 해제", "guard", "plc250", "STOPPING·ALL STOP·중단 처리 중에는 S2·S3 를 평가하지 않음",
      states=("STOPPING",), v1="main.py:3864 (이중 중단 방지)"),
    R("S5", "MFC 단일 사용", "reject", "command", "공정과 히터 가스는 서로 시작을 막는다(히터 가스 상태 IDLE 이 아니면 공정 시작 거부, 공정 중이면 히터 가스 시작 거부)",
      actions=(_rej("HEATER_GAS_ACTIVE"), _rej("PROCESS_RUNNING")), v1="main.py:2743, 3154, heater_atmosphere:405"),
    # ── PLC 통신 ──
    R("S6", "PLC 통신 두절 중단", "trip", "tick", "plc.age_s ≥ PLC_COMM_LOSS_ABORT_SEC", ("plc.q", "plc.age_s"),
      ("PLC_COMM_LOSS_ABORT_SEC",), "PLC_COMM_LOSS_ABORT_SEC", ACT,
      (_abort("PLC_COMM_LOST"), Act("BLACKBOX_DUMP")), v1="device/PLC.py:410-440"),
    R("S7", "PLC 끊김 알림", "notify", "tick", "plc.age_s ≥ PLC_DISCONNECT_NOTIFY_SEC(단절당 1회)", ("plc.age_s",),
      ("PLC_DISCONNECT_NOTIFY_SEC",), "PLC_DISCONNECT_NOTIFY_SEC", ("*",),
      (_note("error", "PLC_DISCONNECTED"), Act("BLACKBOX_DUMP")), release="링크 복구 때 info 1회", v1="device/PLC.py:757-769"),
    R("S8", "PLC 블랙박스", "maintain", "plc250", "성공한 PLC 폴링마다 원시 값 링버퍼(BLACKBOX_SECONDS)", ("plc.q",),
      ("BLACKBOX_SECONDS",), v1="device/PLC.py:177, 549-572"),
    R("S9", "PLC 재기동 감지", "trip", "plc250", "plc.marker_ok 거짓 2회 연속(마커 D 주소가 0 이면 끔)", ("plc.marker_ok",),
      ("PLC_SESSION_MARK_REG",), "2회 연속", ("*",),
      (Act("HEATER_PUSH_CONFIG"), Act("PLC_MARKER_WRITE"), _abort("PLC_RESTARTED"), _note("error", "PLC_RESTARTED")),
      release="공정 중이 아니면 중단 없이 재설정·알림만", v1="device/PLC.py:575-620, main.py:1537"),
    R("S10", "PLC 복구 뒤 안전 재적용", "recover", "event:plc_link_up", "두절로 공정을 중단했고 지금 공정이 없음",
      ("plc.q",), states=("IDLE", "LOADED", "ABORTED"),
      actions=(Act("RF_SAFE_OFF"), Act("HEATER_FORCE_OFF"), Act("GAS_VALVES_CLOSE"), _note("warn", "PLC_RECOVERED_SAFE")),
      v1="main.py:1551-1590"),
    R("S11", "링크 끊김·값 오래됨 화면 잠금", "maintain", "tick", "장비 품질이 ok 가 아니고 나이 ≥ HEATER_STALE_SEC 면 그 장비 조작 잠금·회색",
      ("plc.q", "plc.age_s"), ("HEATER_STALE_SEC",), actions=(Act("LOCK_CONTROLS"),), v1="main.py:3884"),
    R("S12", "링크 없을 때 히터 OFF 보류", "maintain", "worker", "RUN=0 요청이 실패하거나 링크가 없으면 보류 → 링크 복구 즉시 적용",
      actions=(Act("HEATER_FORCE_OFF"),), v1="device/PLC.py:1276, 319, 391"),
    R("S13", "접속 때 히터 한계값 쓰기·되읽기", "maintain", "event:plc_link_up", "링크 업·재기동·설정 적용 때 히터 한계값을 쓰고 되읽어 비교",
      params=("HEATER_SV_LIMIT_C", "HEATER_MV_LIMIT", "HEATER_RAMP_RATE_C_PER_MIN", "HEATER_HOLDBACK_C", "HEATER_OT_LIMIT_C",
              "HEATER_OT2_LIMIT_C", "HEATER_SLOW_ZONE_C", "HEATER_SLOW_RATE_C_PER_MIN"),
      actions=(Act("HEATER_PUSH_CONFIG"),), release="되읽기 불일치면 VERIFY_MISMATCH 알림·히터 명령 거부", v1="device/PLC.py:852-931"),
    # ── 히터 ──
    R("S14", "히터 워치독 하트비트", "maintain", "worker",
      "PLC worker 가 HEATER_WD_PERIOD_MS 마다 D00014 증가. 링크가 끊기면 쓰지 않는다(래더 TON 이 히터를 끄게). 지연 > HEATER_WD_DELAY_WARN_SEC 면 알림",
      ("plc.wd_counter",), ("HEATER_WD_PERIOD_MS", "HEATER_WD_DELAY_WARN_SEC"), actions=(_note("warn", "HEATER_WD_DELAY"),),
      v1="device/PLC.py:1110-1163, 724"),
    R("S15", "시작 때 남은 RUN 정리", "recover", "event:plc_first_poll", "첫 폴링에서 heater.run 참",
      ("heater.run",), actions=(Act("HEATER_RESIDUAL_CLEAR"), _note("warn", "HEATER_RESIDUAL_RUN")), v1="device/PLC.py:1081, main.py:3588"),
    R("S16", "히터 이상", "trip", "plc250", "heater.fault 상승(원인 OT > TC > WD). 첫 폴링 래치는 구분",
      ("heater.fault", "heater.ot", "heater.tc_err", "heater.wd_err"), states=("*",),
      actions=(Act("HEATER_FORCE_OFF"), _abort("HEATER_FAULT"), Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_FAULT"})),
      release="공정·히터 레시피가 히터를 쓰지 않으면 RUN OFF·알림만", v1="device/PLC.py:1054, main.py:3572, 3606"),
    R("S17", "히터 출력 죽음", "notify", "plc250", "heater.run 참 · fault 거짓 · heater.mv < HEATER_MV_MIN 이 HEATER_OUT_DEAD_SEC 동안(유예 뒤)",
      ("heater.run", "heater.fault", "heater.mv"), ("HEATER_MV_MIN", "HEATER_OUT_DEAD_SEC", "HEATER_OUT_DEAD_GRACE_SEC"),
      "HEATER_OUT_DEAD_SEC", actions=(_note("warn", "HEATER_OUTPUT_DEAD_WARN"),), v1="device/PLC.py:1040"),
    R("S18A", "히터 레시피: 출력 죽음 지속", "trip", "tick", "S17 조건이 HEATER_OUT_DEAD_LIMIT_SEC 지속(레시피 실행 중)",
      ("heater.run", "heater.mv"), ("HEATER_OUT_DEAD_LIMIT_SEC",), "HEATER_OUT_DEAD_LIMIT_SEC",
      actions=(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_OUTPUT_DEAD"}),), v1="controller/heater_recipe.py:1161"),
    R("S18B", "히터 레시피: 외부 RUN OFF", "trip", "tick", "레시피 실행 중 heater.run 거짓 지속", ("heater.run",),
      ("HEATER_RUN_OFF_LIMIT_SEC",), "HEATER_RUN_OFF_LIMIT_SEC",
      actions=(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_RUN_OFF_EXTERNAL"}),), v1="controller/heater_recipe.py:1136"),
    R("S18C", "히터 레시피: TC 이상 지속", "trip", "tick", "레시피 실행 중 heater.tc_err 또는 PV=-1 지속", ("heater.tc_err", "heater.pv"),
      ("HEATER_TC_BAD_LIMIT_SEC",), "HEATER_TC_BAD_LIMIT_SEC",
      actions=(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_TC_BAD"}),), v1="controller/heater_recipe.py:1190"),
    R("S18D", "히터 레시피: 스텝 램프 시간 초과", "trip", "tick", "RAMP 스텝 경과 > min(max(HEATER_WAIT_TIMEOUT_SEC, 예상+여유), 상한) — HOLD 중에도",
      ("heater.pv", "heater.sv"), ("HEATER_WAIT_TIMEOUT_SEC", "HEATER_WAIT_TIMEOUT_MARGIN_SEC", "HEATER_WAIT_TIMEOUT_MAX_SEC"),
      actions=(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_WAIT_TIMEOUT"}),), v1="controller/heater_recipe.py:1221"),
    R("S18E", "공정 승온 대기 한도", "trip", "state:HEATER_RAMP", "process_states.FORMULAS['HEATER_RAMP'] 초과. heater.fault 면 즉시 중단(S16)",
      ("heater.pv", "heater.sv", "heater.sv_ramp"), ("HEATER_WAIT_TIMEOUT_SEC", "HEATER_WAIT_TIMEOUT_MARGIN_SEC",
                                                     "HEATER_WAIT_TIMEOUT_MAX_SEC", "HEATER_SOAK_TOLERANCE", "HEATER_SOAK_TIME_SEC",
                                                     "HEATER_RAMP_DONE_TOL_C"),
      "formula", ("HEATER_RAMP",), (_abort("HEATER_WAIT_TIMEOUT"),), v1="controller/process_controller.py:1295-1329"),
    R("S19", "히터 시작 인터록", "reject", "command", "heater.itl 거짓 또는 heater.fault 참이면 히터 ON·레시피 시작 거부",
      ("heater.itl", "heater.fault"), actions=(_rej("HEATER_ITL_OFF"), _rej("HEATER_FAULT_ACTIVE")), v1="main.py:1677, heater_recipe:805"),
    R("S20", "히터 리셋 보호", "reject", "command", "heater.run 참이면 리셋 거부(웹 확인창 필수)", ("heater.run",),
      actions=(_rej("HEATER_RUN_ON"),), v1="main.py:3260"),
    R("S21", "히터 목표 상한", "reject", "command", "목표 > min(HEATER_MAX_TEMP, PLC D00013) 거부", ("heater.sv",),
      ("HEATER_MAX_TEMP", "HEATER_SV_LIMIT_C"), actions=(_rej("BAD_ARGS"),), v1="main.py:1593"),
    R("S22", "히터 유지 모드", "maintain", "tick",
      "도달 래치 → 창 안정 → dac(D00018 고정) / tc2(D00035 → M0004A → M0004B). TC2 상실 시 dac 로 강등. 값이 되돌아가면 재적용",
      ("heater.pv", "heater.sv", "heater.mv", "heater.pv2", "heater.pv_sel_eff", "heater.hold"),
      ("HEATER_HOLD_MODE", "HEATER_HOLD_MV_ENTER_TOL_C", "HEATER_HOLD_MV_ENTER_SEC", "HEATER_HOLD_MV_ARRIVE_TOL_C",
       "HEATER_HOLD_MV_DRIFT_PV_C", "HEATER_HOLD_MV_DRIFT_MV", "HEATER_HOLD_TC2_MARGIN_C", "HEATER_HOLD_TC2_DRIFT_C",
       "HEATER_HOLD_DRIFT_PV_REL", "HEATER_HOLD_DRIFT_MV_REL", "HEATER_HOLD_DRIFT_TC2_REL", "HEATER_HOLD_ENGAGE_TIMEOUT_SEC",
       "HEATER_HOLD_REAPPLY_SEC", "HEATER_MV_SAT_REL"),
      actions=(_note("warn", "HEATER_HOLD_DEMOTED"),), v1="controller/heater_hold.py"),
    R("S23", "공정의 유지 모드 대기·폴백", "trip", "state:HEATER_HOLD_WAIT",
      "HEATER_HOLD_WAIT_SEC 안에 holding 이 아니면 완화 tc2 → dac → HEATER_HOLD_FAIL_ACTION", ("heater.hold",),
      ("HEATER_HOLD_WAIT_SEC", "HEATER_HOLD_FAIL_ACTION"), "HEATER_HOLD_WAIT_SEC", ("HEATER_HOLD_WAIT",),
      (_abort("HEATER_HOLD_FAILED"),), release="FAIL_ACTION=proceed 면 경고만", v1="controller/process_controller.py:1444-1530"),
    R("S24", "DAC 포화 감시", "trip", "tick", "heater.run 참·유지 모드 아님·heater.mv ≥ HEATER_MV_SAT_REL × mv_limit 지속",
      ("heater.run", "heater.mv", "heater.mv_limit", "heater.hold"),
      ("HEATER_MV_SAT_REL", "HEATER_MV_SAT_SEC", "HEATER_SAT_PRE_WINDOW_SEC", "HEATER_SAT_FALLBACK_REL"), "HEATER_MV_SAT_SEC",
      actions=(Act("HEATER_CLAMP_MV"), _note("warn", "HEATER_SATURATED")), release="RUN OFF·SV 변경 때 원복. dac 유지 중이면 알림만",
      v1="controller/heater_saturation.py"),
    R("S25", "히터 감속 접근·홀드백", "maintain", "tick", "SV ≤ sv_ramp + LEAD, 남은 거리에 비례해 감속",
      ("heater.sv", "heater.sv_ramp", "heater.pv"),
      ("HEATER_APPROACH_ZONE_C", "HEATER_APPROACH_MIN_RATE_C_PER_MIN", "HEATER_APPROACH_LEAD_C"), v1="controller/heater_ramp.py:174"),
    R("S26", "냉각 보호 가스 유지", "maintain", "event:heater_run_falling", "공정 밖에서 RUN 하강 때 heater.pv > HEATER_GAS_HOLD_RELEASE_C",
      ("heater.run", "heater.pv"), ("HEATER_GAS_HOLD_RELEASE_C",), states=("IDLE", "LOADED"),
      actions=(Act("HEATER_GAS_HOLD"),), release="heater.pv ≤ HEATER_GAS_HOLD_RELEASE_C 면 해제. PV 를 모르면 해제 안 함",
      v1="main.py:2676"),
    R("S27", "히터 가스 시퀀스 실패 자동 해제", "trip", "event:heater_gas_failed", "가스 준비 단계 실패·시간 초과",
      params=("HEATER_GAS_SP1_TIMEOUT_SEC", "HEATER_GAS_MFC_TIMEOUT_SEC"),
      actions=(Act("GAS_FLOW_OFF"), Act("GAS_EXHAUST"), Act("GAS_VALVES_CLOSE"), _note("error", "HEATER_GAS_FAILED")),
      release="해제 단계는 실패해도 계속, 밸브는 반드시 닫음", v1="controller/heater_atmosphere.py:351, 395"),
    R("S28", "히터 값 오래됨", "maintain", "tick", "heater 품질 ok 아님·나이 ≥ HEATER_STALE_SEC 면 히터 표시 회색·버튼 잠금",
      ("heater.q", "heater.age_s"), ("HEATER_STALE_SEC",), actions=(Act("LOCK_CONTROLS"),), v1="main.py:2968"),
    # ── DC ──
    R("S29", "DC OFF 확인형", "maintain", "event:dc_off", "OUTP OFF 뒤 OUTP? 를 DC_OFF_VERIFY_TRIES 회 확인. 미확인이면 백오프 재시도",
      ("dc.out",), ("DC_OFF_VERIFY_TRIES", "DC_OFF_RETRY_START_SEC", "COMM_OUTAGE_LOG_SEC"),
      actions=(Act("DC_SAFE_OFF"), _note("error", "DC_OFF_UNCONFIRMED")), release="확인되면 info", v1="device/DCpower.py:273-379"),
    R("S30", "DC 통신 두절", "trip", "tick", "DC 사용 공정에서 dc.age_s ≥ DC_COMM_LOSS_ABORT_SEC", ("dc.q", "dc.age_s"),
      ("DC_COMM_LOSS_ABORT_SEC",), "DC_COMM_LOSS_ABORT_SEC", ACT, (_abort("DC_COMM_LOST"), Act("DC_SAFE_OFF")),
      release="복구 때 공정이 없으면 DC_SAFE_OFF 다시(DC_RECOVERED_SAFE). 판정은 DC 를 켠 뒤(POWER_WAIT 출력 명령 이후)만",
      v1="device/DCpower.py:240-253·242-243·454-458, main.py:1488-1497"),
    R("S31A", "DC 램프업 무출력", "trip", "tick", "dc.i_set ≥ DC_FAIL_ISET_A 이고 dc.p ≤ DC_FAIL_POWER_W", ("dc.i_set", "dc.p"),
      ("DC_FAIL_ISET_A", "DC_FAIL_POWER_W", "DC_FAIL_SEC"), "DC_FAIL_SEC", ("POWER_WAIT",),
      (_abort("DC_NO_OUTPUT"),), v1="device/DCpower.py:604-612"),
    R("S31B", "DC 저전류", "trip", "tick", "유지 중 dc.i ≤ DC_MIN_CURRENT_ABORT", ("dc.i",),
      ("DC_MIN_CURRENT_ABORT", "DC_MIN_CURRENT_ABORT_SEC"), "DC_MIN_CURRENT_ABORT_SEC", ACT,
      (_abort("DC_LOW_CURRENT"),), v1="device/DCpower.py:631"),
    R("S31C", "DC 파워 이탈", "trip", "tick", "|dc.p − set_w| > max(DC_TOLERANCE_W, set_w × DC_POWER_ERROR_RATIO) — SHUTTER_DELAY·MAIN 에서만",
      ("dc.p", "dc.set_w"), ("DC_POWER_ERROR_RATIO", "DC_POWER_ERROR_SEC", "DC_TOLERANCE_W"), "DC_POWER_ERROR_SEC",
      ("SHUTTER_DELAY", "MAIN"), (_abort("DC_POWER_DEVIATION"),), v1="device/DCpower.py:659-675"),
    # ── RF(아날로그) ──
    R("S32", "RF 반사파", "trip", "tick", "rf.ref_w ≥ RF_REFP_ABORT_THRESHOLD 지속", ("rf.ref_w",),
      ("RF_REFP_ABORT_THRESHOLD", "RF_REFP_WAIT_SEC"), "RF_REFP_WAIT_SEC", ACT,
      (_abort("RF_REFLECTED_HIGH"), Act("RF_RAMP_DOWN"), Act("RF_SAFE_OFF")),
      release="RF 를 켠 뒤만. 대기 중에는 S33A·S33B 판정 멈춤. 중단은 정지 순서의 램프다운 뒤 OFF",
      v1="device/RFpower.py:79-81·91-106·210-214"),
    R("S33A", "RF 램프업 무출력", "trip", "tick", "rf.dac ≥ RF_FAIL_DAC_COUNTS 이고 rf.fwd_w ≤ RF_FAIL_FWD_W", ("rf.dac", "rf.fwd_w"),
      ("RF_FAIL_DAC_COUNTS", "RF_FAIL_FWD_W", "RF_FAIL_SEC"), "RF_FAIL_SEC", ("POWER_WAIT",),
      (_abort("RF_NO_OUTPUT"), Act("RF_RAMP_DOWN"), Act("RF_SAFE_OFF")), release="반사파 대기 중에는 판정 멈춤",
      v1="device/RFpower.py:91-104·118-131"),
    R("S33B", "RF 파워 이탈", "trip", "tick", "유지 중 |rf.fwd_w − set_w| > max(RF_TOLERANCE_W, set_w × RF_POWER_ERROR_RATIO)",
      ("rf.fwd_w", "rf.set_w"), ("RF_POWER_ERROR_RATIO", "RF_POWER_ERROR_SEC", "RF_TOLERANCE_W"),
      "RF_POWER_ERROR_SEC", ACT, (_abort("RF_POWER_DEVIATION"), Act("RF_RAMP_DOWN"), Act("RF_SAFE_OFF")),
      release="반사파 대기 중에는 판정 멈춤", v1="device/RFpower.py:91-104·150-167"),
    # ── RF Pulse ──
    R("S34A", "RF Pulse 순방향 이탈", "trip", "tick", "|rfp.fwd_w − set_w| ≥ set_w × RFPULSE_FORP_TOLERANCE_PERCENT %",
      ("rfp.fwd_w", "rfp.set_w"), ("RFPULSE_FORP_TOLERANCE_PERCENT", "RFPULSE_FORP_SEC"),
      "RFPULSE_FORP_SEC", ACT, (_abort("RFP_FORWARD_DEVIATION"), Act("RFP_SAFE_OFF")), v1="device/RFpulse.py:1240"),
    R("S34B", "RF Pulse 반사파", "trip", "tick", "rfp.ref_w ≥ RFPULSE_REFP_LIMIT_WATTS", ("rfp.ref_w",),
      ("RFPULSE_REFP_LIMIT_WATTS", "RFPULSE_REFP_SEC"), "RFPULSE_REFP_SEC", ACT,
      (_abort("RFP_REFLECTED_HIGH"), Act("RFP_SAFE_OFF")), v1="device/RFpulse.py:1267"),
    R("S34C", "RF Pulse 통신 두절", "trip", "tick", "RF Pulse 사용 공정에서 rfp.age_s ≥ RFPULSE_COMM_LOSS_ABORT_SEC",
      ("rfp.q", "rfp.age_s"), ("RFPULSE_COMM_LOSS_ABORT_SEC", "RFPULSE_COMM_REOPEN_SEC"), "RFPULSE_COMM_LOSS_ABORT_SEC", ACT,
      (_abort("RFP_COMM_LOST"), Act("RFP_SAFE_OFF")),
      release="복구 때 공정이 없으면 RFP_SAFE_OFF 다시(RFP_RECOVERED_SAFE). 판정은 RF Pulse START 이후만",
      v1="device/RFpulse.py:789-814·984·1150-1161, main.py:1511-1517"),
    R("S34D", "RF Pulse 시작 실패", "trip", "event:rfp_start_failed", "START 명령 CSR ≠ 0 또는 무응답",
      actions=(_abort("RFP_START_FAILED"), Act("RFP_SAFE_OFF")), states=("POWER_WAIT",), v1="device/RFpulse.py:1001"),
    R("S35", "펄스 설정 규칙·되읽기", "reject", "command", "레시피 검사: 주파수·듀티 범위, ON/OFF ≥ RFPULSE_PULSE_MIN_EDGE_US. "
      "시작 뒤 193/196 되읽기 불일치면 RFPULSE_VERIFY_PULSE_CONFIG 에 따라 중단",
      ("rfp.freq_hz", "rfp.duty_pct"), ("RFPULSE_PULSE_MIN_EDGE_US", "RFPULSE_PULSE_FREQ_MIN_HZ", "RFPULSE_PULSE_FREQ_MAX_HZ",
                                        "RFPULSE_DUTY_MIN", "RFPULSE_DUTY_MAX", "RFPULSE_VERIFY_PULSE_CONFIG"),
      actions=(_rej("RECIPE_INVALID"), _abort("RFP_CONFIG_MISMATCH")), v1="core/params.py:84-107, main.py:4321"),
    # ── 공정 한도 ──
    R("S36", "파워 대기 절대 한도", "trip", "state:POWER_WAIT", "process_states.FORMULAS['POWER_WAIT'] 초과",
      params=("POWER_WAIT_TIMEOUT_SEC", "POWER_WAIT_TIMEOUT_MAX_SEC", "POWER_WAIT_EST_FACTOR", "POWER_WAIT_EST_MARGIN_SEC"),
      duration="formula", states=("POWER_WAIT",), actions=(_abort("POWER_WAIT_TIMEOUT"),), v1="controller/process_controller.py:941"),
    R("S37", "SP1 압력 미도달", "trip", "state:PRESSURE_SP1", "SP1_TIMEOUT_SEC 안에 ±PRESSURE_WAIT_TOL_RATIO 를 STABLE_COUNT 연속 못 함",
      ("mfc.pressure_mtorr",), ("SP1_TIMEOUT_SEC", "PRESSURE_WAIT_TOL_RATIO", "PRESSURE_WAIT_STABLE_SEC"), "SP1_TIMEOUT_SEC",
      ("PRESSURE_SP1",), (_abort("SP1_TIMEOUT"),), v1="controller/process_controller.py:626"),
    R("S38A", "가스 유량 이탈", "notify", "tick", "|유량 − 설정| > 설정 × FLOW_ERROR_TOLERANCE 연속", ("mfc.ar_sccm", "mfc.o2_sccm"),
      ("FLOW_ERROR_TOLERANCE", "FLOW_ERROR_SEC"), "FLOW_ERROR_SEC", ("MAIN",),
      (_note("warn", "FLOW_DEVIATION"),), release="경고만 — 공정 계속", v1="device/MFC.py:1300-1311"),
    R("S38B", "압력 이탈", "notify", "tick", "|압력 − SP1| > SP1 × MFC_PRESSURE_WARN_RATIO 연속", ("mfc.pressure_mtorr",),
      ("MFC_PRESSURE_WARN_RATIO", "MFC_PRESSURE_WARN_SEC"), "MFC_PRESSURE_WARN_SEC", ("MAIN",),
      (_note("warn", "PRESSURE_DEVIATION"),), release="경고만 — 공정 계속", v1="device/MFC.py:1333-1342"),
    R("S39A", "베이스 압력 대기 한도", "trip", "state:BASE_WAIT", "IG_BASE_WAIT_TIMEOUT_SEC 안에 도달 못 함",
      ("ig.ion_torr", "ig.filament", "ig.emission"), ("IG_BASE_WAIT_TIMEOUT_SEC", "IG_BASE_STABLE_SEC", "IG_SETTLE_SEC",
                                                    "IG_VALUE_MAX_AGE_SEC"),
      "IG_BASE_WAIT_TIMEOUT_SEC", ("BASE_WAIT",), (_abort("BASE_WAIT_TIMEOUT"), Act("IG_FILAMENT_OFF")),
      v1="controller/process_controller.py:1175"),
    R("S39B", "베이스 대기 중 IG 두절", "trip", "tick", "ig.age_s ≥ IG_COMM_LOSS_ABORT_SEC (suppressed 구간 제외)", ("ig.q", "ig.age_s"),
      ("IG_COMM_LOSS_ABORT_SEC",), "IG_COMM_LOSS_ABORT_SEC", ("BASE_WAIT",), (_abort("IG_COMM_LOST"),),
      v1="controller/process_controller.py:1179"),
    R("S39C", "필라멘트 켜기 실패", "trip", "state:BASE_WAIT", "IG_FILAMENT_ON_TIMEOUT_SEC 안에 filament·emission 이 안 보임, 재점등 IG_MAX_RELIGHTS 초과",
      ("ig.filament", "ig.emission", "ig.err_code"), ("IG_FILAMENT_ON_TIMEOUT_SEC", "IG_MAX_RELIGHTS"), "IG_FILAMENT_ON_TIMEOUT_SEC",
      ("BASE_WAIT",), (_abort("IG_FILAMENT_FAILED"), Act("IG_FILAMENT_OFF")), v1="controller/process_controller.py:1077-1099, 1247"),
    # ── 프로그램·명령·설정 ──
    R("S40", "종료 가드", "reject", "event:shutdown_request", "공정 진행·히터 가스 유지 중이면 프로그램 종료(서비스 정지) 거부",
      states=("*",), actions=(_rej("PROCESS_RUNNING"), _rej("HEATER_GAS_ACTIVE")), v1="main.py:4467 closeEvent"),
    R("S41", "종료 때 히터 RUN=0", "maintain", "event:shutdown", "프로그램이 끝날 때 RUN=0 시도(2 s 안)", ("heater.run",),
      actions=(Act("HEATER_FORCE_OFF"),), v1="device/PLC.py:331"),
    R("S42", "원격 명령 가드", "reject", "command", "commands.check_command — 경로·상태, PLC 링크 없으면 PLC 명령 거부(LINK_DOWN). "
      "공정 중 PLC 버튼은 v1 처럼 모두 허용하고 공정 기록에 PLC_MANUAL_IN_PROCESS(info)로 남긴다(확인은 웹 단계)",
      actions=(_rej("LINK_DOWN"), _rej("STATE_NOT_ALLOWED"), _note("info", "PLC_MANUAL_IN_PROCESS")),
      v1="integrations/erp_commands.py:207, 268 (공정 중 PLC 버튼 허용 — 사용자 결정 2026-10-20 v1 과 같게)"),
    R("S43", "시리얼 열기·닫기 격리", "maintain", "worker", "포트 열기·닫기를 제한 시간 있는 별도 스레드에서. 재연결은 COMM_RECONNECT_* 백오프",
      params=("COMM_RECONNECT_START_MS", "COMM_RECONNECT_MAX_MS", "COMM_LONG_OUTAGE_SEC", "COMM_PROBE_MS"),
      v1="lib/serial_guard.py, lib/comm_policy.py (2026-09-16 사고)"),
    R("S44", "같은 알림 폭주 억제", "maintain", "worker", "같은 이벤트 코드는 활성 동안 1번, 해제 때 1번(웹 서버가 챗으로 보냄)",
      v1="main.py:4751 (30 s 게이트)"),
    R("S45", "시리얼 포트 중복", "reject", "config", "params.CROSS_RULES X22", params=("PLC_PORT", "MFC_PORT", "DC_PORT",
      "RFPULSE_PORT", "IG_PORT"), actions=(_rej("CONFIG_INVALID"),), v1="main.py:1277 (v1 은 경고만)"),
    # ── v2 추가 ──
    R("S46", "PC 하트비트 → 출력 끄기(래더)", "maintain", "worker",
      "PLC worker 가 1초마다 PC_ALIVE(=D00014 공용) 증가. 래더가 약 3 s 변화 없으면 RF DAC 0·enable OFF, Ar/O2 OFF(+배선 시 RF·RF Pulse·DC 인터록)",
      ("plc.wd_counter",), ("HEATER_WD_PERIOD_MS",), v1="devices.PC_DEAD_PLAN PD1~PD6", pending_confirm=True,
      needs="래더 변경(PD1~PD3), 배선(PD4~PD6) — 보류(2026-10-15). 만들 때 v1 이 D00014 를 3 s 마다 쓰는 동안은 "
            "판정 시간 10 s 이상", on_hold=True),
    R("S47", "베이스 압력은 이온 게이지 필요", "reject", "command", "base_pressure 행이 있는데 IG_ENABLED 거짓이면 적재·시작 거부",
      params=("IG_ENABLED",), actions=(_rej("IG_DISABLED"),), v1="v1 은 경고만 하고 베이스 대기를 건너뜀(process_controller:1036)"),
    R("S48", "공정 중 H 등급 설정 금지", "reject", "config", "공정 진행 중 CONFIG_APPLY 에 H 등급 변경이 있으면 거부, idle 키는 대기",
      actions=(_rej("CONFIG_H_IN_PROCESS"),), v1="v2"),
    R("S49", "RF Pulse 인터록 열림", "trip", "tick", "RF Pulse 사용 공정에서 rfp.itl_open 참", ("rfp.itl_open",), states=ACT,
      actions=(_abort("RFP_FAULT"), Act("RFP_SAFE_OFF")),
      v1="v2 (CESAR 162 byte2 b7, p.114) — v1 에 없음. 열림 즉시 중단은 v2 방식 유지(사용자 2026-10-20)"),
    R("S51", "시작 때 남은 출력 정리", "recover", "event:first_read",
      "v2 가 켜진 뒤 장비별 첫 정상 읽기(재연결 때는 아님)에서, 공정·히터 레시피가 진행 중이 아니면: "
      "PLC rf.dac > 0 → RF 램프다운 → RF_SAFE_OFF + RF_RESIDUAL_OUTPUT(warn), D40 0·enable 만 → RF_SAFE_OFF + RF_ENABLE_RESIDUAL"
      "(info, v1 정상 종료 흔적 — 챗 안 보냄) / DC 연결 때 OUTP? 1 → DC_SAFE_OFF + DC_RESIDUAL_OUTPUT / RF Pulse(켜져 있을 때) "
      "출력 켜짐·Host → RFP_SAFE_OFF + RFP_RESIDUAL_OUTPUT, 앞면 모드면 끄지 않고 warn / MFC 채널 ON 이거나 유량 > 0.1 sccm → "
      "GAS_RESIDUAL_FLOW(warn 만). 진행 중이면 끄지 않고 warn 만. 펌프·밸브·셔터는 건드리지 않는다(히터는 S15). "
      "prod·sim 에서만 — bench 는 아무것도 쓰지 않는다(가드)",
      ("rf.dac", "rf.dac_enable", "rf.fwd_w", "dc.out", "rfp.out_on", "mfc.ar_sccm", "mfc.o2_sccm"),
      actions=(Act("RF_RAMP_DOWN"), Act("RF_SAFE_OFF"), Act("DC_SAFE_OFF"), Act("RFP_SAFE_OFF"),
               _note("warn", "RF_RESIDUAL_OUTPUT"), _note("info", "RF_ENABLE_RESIDUAL"), _note("warn", "DC_RESIDUAL_OUTPUT"),
               _note("warn", "RFP_RESIDUAL_OUTPUT"), _note("warn", "GAS_RESIDUAL_FLOW")),
      v1="v1 은 RF enable 을 비상정지에서만 끔(PLC.send_rfpower_command) — 10/16 bench. 확정(사용자 2026-10-20): "
         "전원 출력만 끄고 PLC 코일(펌프·밸브·가스 차단밸브 등)은 그대로"),
    R("S50", "DC 하드 보호 설정", "maintain", "event:dc_connect", "연결 때 VOLT:OVP=DC_MAX_VOLTAGE, CURR:OCP=DC_MAX_CURRENT, 설정·되읽기(DC_SET_PROTECTION). FACT:LAST-STA 는 읽기만(FULLY 면 경고)",
      params=("DC_MAX_VOLTAGE", "DC_MAX_CURRENT"), v1="v2 (devices.PC_DEAD_PLAN PD7)", pending_confirm=True,
      needs="bench 에서 명령 동작 확인"),
)


# ───────────── STOP·ALL STOP 순서 ─────────────
@dataclass(frozen=True)
class SeqStep:
    no: int
    act: Act
    timeout_param: str = ""      # 이 단계 한도(넘어도 다음 단계로)
    must: bool = True            # 앞 단계에서 예외가 나도 반드시 실행
    when: str = ""               # 조건
    desc: str = ""
    nowait: bool = False         # 보내기만 하고 다음 단계로(결과는 뒤의 AWAIT 단계가 받는다)


# v1 _stop_impl·_power_off_sequence(controller/process_controller.py:1785-1981): 필라멘트 OFF 1회(기다리지 않음) → MS 닫기 →
# 히터 OFF → DC OFF·RF Pulse OFF 를 보내 놓고(켰을 때만, 기다리지 않음) RF 램프다운 → RF Pulse 완료 대기 → 유량 OFF → 배기 →
# 건 셔터 → 차단밸브. v2 는 RF 램프다운 뒤 enable 도 OFF(v2 방식 유지), 끝에서 DC OFF 결과를 받아 기록한다.
STOP_SEQUENCE: tuple = (
    SeqStep(1, Act("IG_FILAMENT_OFF"), "", False, "이번 공정에서 필라멘트를 켰으면",
            "베이스 대기 정리 — 1회 요청, 결과를 기다리지 않음(v1 _ig_cancel)", nowait=True),
    SeqStep(2, Act("SHUTTERS_CLOSE", {"which": "ms"}), "", True, "", "Main 셔터 닫기"),
    SeqStep(3, Act("HEATER_FORCE_OFF"), "", True, "공정이 히터를 쓰는 행이면", "히터 RUN=0, 램프 원복"),
    SeqStep(4, Act("DC_SAFE_OFF"), "", True, "이번 행에서 DC 를 켰으면", "DC OFF(확인형) — 보내고 다음으로", nowait=True),
    SeqStep(5, Act("RFP_SAFE_OFF"), "", True, "이번 행에서 RF Pulse 를 켰으면", "RF Pulse OFF — 보내고 다음으로(램프다운 없음)",
            nowait=True),
    SeqStep(6, Act("RF_RAMP_DOWN"), "STOP_RF_RAMPDOWN_TIMEOUT_SEC", True, "RF 사용 중",
            "RF_RAMP_DOWN_STEP_COUNTS 씩 0.5 s 간격, 한도 넘으면 즉시 0"),
    SeqStep(7, Act("RF_SAFE_OFF"), "", True, "", "DAC 0·enable OFF"),
    SeqStep(8, Act("AWAIT", {"no": 5}), "STOP_RFPULSE_OFF_TIMEOUT_SEC", True, "", "RF Pulse OFF 완료 대기"),
    SeqStep(9, Act("GAS_FLOW_OFF"), "STOP_FLOW_OFF_TIMEOUT_SEC", True, "", "FLOW OFF 확인(R69)"),
    SeqStep(10, Act("GAS_EXHAUST"), "STOP_EXHAUST_WAIT_SEC", True, "", "MFC 밸브 열기(배기)"),
    SeqStep(11, Act("SHUTTERS_CLOSE", {"which": "s1,s2"}), "", True, "", "건 셔터 닫기"),
    SeqStep(12, Act("GAS_VALVES_CLOSE"), "", True, "", "Ar·O2 차단밸브 닫기"),
    SeqStep(13, Act("AWAIT", {"no": 4}), "", True, "", "DC OFF 결과(기록)"),
)

# ALL STOP — 말 그대로 전부 끈다(사용자 결정 2026-10-20, v1 과 같게). 각 단계 독립(앞 단계 예외와 무관).
# v1 순서(core/process_service.py:746-812): 히터 레시피 중단 → 중단 표시 → PLC 비상 정지(device/PLC.py:1390-1447 — 버튼 코일
# 전부 OFF, 히터 RUN=0, DAC enable OFF) → DC 직접 OFF → 공정 정지 순서(유량 OFF·MFC 밸브 열기·필라멘트) → RF Pulse 정지.
# v2 가 더한 것: PLC 비상 정지에 D00040=0. RF 는 램프다운 없이 즉시 0(이상 중단의 램프다운과 다름).
ALL_STOP_SEQUENCE: tuple = (
    SeqStep(1, Act("HEATER_RECIPE_ABORT", {"reason": "ALL_STOP"}), "", True, "히터 레시피 실행 중", "히터 레시피 중단"),
    SeqStep(2, Act("PROCESS_ABORT", {"reason": "ALL_STOP"}), "", True, "공정 진행 중",
            "중단 표시를 PLC OFF 보다 먼저(S4 — MV 닫힘 이중 중단 방지). 공정은 STOPPING 으로"),
    SeqStep(3, Act("PLC_OUTPUTS_OFF"), "", True, "",
            "PLC 비상 정지: 버튼 코일 15개 전부 OFF(펌프·밸브 포함) → 히터 RUN=0 → D00040=0 → M00200=0(D00040=0 만 v2 추가)"),
    SeqStep(4, Act("DC_SAFE_OFF"), "", True, "", "DC OFF 직결 확인형 — 보내고 다음 단계로(v1 dc_emergency_off)", nowait=True),
    SeqStep(5, Act("GAS_FLOW_OFF"), "STOP_FLOW_OFF_TIMEOUT_SEC", True, "", "MFC 유량 OFF"),
    SeqStep(6, Act("GAS_EXHAUST"), "STOP_EXHAUST_WAIT_SEC", True, "공정 진행 중이었으면", "MFC 밸브 열기(v1 공정 정지 순서)"),
    SeqStep(7, Act("IG_FILAMENT_OFF"), "", False, "필라멘트 켜져 있으면", "이온 게이지 필라멘트 OFF"),
    SeqStep(8, Act("RFP_SAFE_OFF"), "", True, "", "RF Pulse OFF 직결 — 공정 여부와 무관(v1 맨 끝 rfpulse_stop)"),
    SeqStep(9, Act("AWAIT", {"no": 4}), "", True, "", "DC OFF 결과"),
)

# 공정 시작 전 확인 규칙(process_states 의 START 전이 guard)
START_GUARDS: tuple = ("S1", "S5", "S19", "S35", "S47")


def by_id() -> dict:
    return {r.id: r for r in RULES}
