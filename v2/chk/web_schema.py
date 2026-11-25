"""웹 설정 내보내기(사용자 지시 5-4 ④) — 웹이 이 묶음 하나로 설정·레시피·블록 화면을 그린다.

묶음(JSON 하나, bundle()):
  version · hash(정의 내용의 sha256 — 현재값 제외) · paramSchemaHash(paramdef.schema_hash — CONFIG_APPLY 의 schemaHash)
  params        파라미터 정의(키·이름·묶음·단위·형식·범위·허용값·기본값·현재값(current 실행 값·stored 저장 값·overridden)·
                안전 등급·적용 시점·역할·v1 근거·설명·
                웹 잠금 webLocked — 웹에서 못 바꾸는 키, 버전 4)
                버전 5: uplink 설정(UPLINK_CLOCK_SKEW_SEC·UPLINK_DOWN_SEC·UPLINK_IN_SIM), 공정 기록 by(요청자), COMMAND_EXPIRED
                버전 6: snapshot(1초 스냅샷 열 정의), PLC_BUTTON role operator, UPLINK_SUMMARY_PERIOD_SEC 설명(공정 중만)
                버전 7: recipe 열 zeroIsEmpty(숫자 0 = 빈 값 — base_pressure 0 은 베이스 대기 안 함, 범위 검사 안 함),
                cmd_result.detail = 거부 이유 문구(명령 인자는 commands.check_args — 형식·범위 밖은 BAD_ARGS)
                버전 8: blocks 에 웹 블록 검사 계산용 blockKeys·empty·valueRules(값 사이·꺼진 장비 규칙 표)·spDefaultKey·
                pressureStage·heaterRampMin·pulseEdge·autoEndStopLabel(골든 tests/golden/web_block_cases.json).
                float 형식 설정값은 늘 float(current·hello·config_ack 가 700.0). state.heaterRecipe.held(일시정지 중)
                버전 9: 블록 label·stage 의 빈 글·공백만 = 값 없음(template_blocks 블록을 그대로 다시 검사해도 통과).
                valueRules.uses(조건·문구가 쓰는 값 — 하나라도 빠졌거나 틀리면 규칙을 건너뜀), need_sp_value 문구
                "value: SP{sp} 이면 값 필요", type 이 비면 "type 필요". PROCESS_START name(run 이름 — 비우면 process_name·Single CHK).
                행 검사: 공백만 있는 선택 값도 없는 값(예전에는 shutter_delay "  " 등이 예외)
                버전 10: params[].stored(저장 값 — config_active 의 값, 없으면 기본값 — 웹이 첫 설정 버전·CONFIG_APPLY 에
                쓰는 값)·overridden(노트북 덮어쓰기가 정한 키), current 는 실행 값(덮어쓰기 반영) 그대로. 묶음 머리·hello 에
                overriddenKeys. CONFIG_ROLLBACK version(새 버전 번호를 웹이 정함). state.adjust(공정 중 수정 항목의 지금 값·
                레시피 값·수정 여부·scope) — 이벤트 adjust 와 같은 항목(adjust.items)
                버전 11: 화면용 이름·도움말(블록 값 label·help·advanced·enumLabels, 블록 help·group·summary·groups, 정지 단계 label,
                공정 중 수정 label·help, 레시피 열 label·help·group, 스냅샷 detail·desc·enumLabels·블록 desc, eventKinds,
                paramGroups) — 동작·문구는 그대로. 개발용 정보(설정 키·레지스터·코드 위치)는 desc 등에 그대로 남는다
                버전 12: 스냅샷 이온 게이지 상태 열 ig.ion_state·ig.a_state·ig.b_state(enumLabels)와 값 열의 stateColumn
                (값이 비어 있고 상태가 ok 가 아니면 상태 이름을 보인다), commands[].benchOk(bench 에서도 받는 명령),
                eventKinds safety bench_skip(bench 에서 안전 조치를 보내지 않음)
                버전 13: 히터 2단(캐스케이드) 제어 설정 HEATER_CTRL_MODE·HEATER_TC2_MAP·HEATER_CASCADE_*·HEATER_SHUTTER_SETTLE_*
                (키 사이 규칙 X26), 블록 heater_settle·상태 HEATER_SETTLE, 스냅샷 heater.ctrl_mode·casc_state·r1·bias·bias_ff·
                bias_int·r1_lag·tc1_ema·sv2_map(heater.sv2 는 평소 화면), blocks.defs[].skip·pause(건너뛰기·일시정지 규칙과 문구),
                명령 PROCESS_SKIP·PROCESS_PAUSE·PROCESS_RESUME·PROCESS_EDIT, 설정 PROCESS_PAUSE_MAX_SEC, 스냅샷 proc.paused·
                proc.revision, events skip·pause·resume·edit·pause_long(run_end summary), eventKinds 그 종류, eventSources(출처 이름),
                장비 이름 DC Power·RF Power
                버전 14: 순서 고치기 기준 목록 state.run.editBase {revision, blocks}(지금 판의 맨 위 블록 정리 목록 — v1 행·장비 앞면
                시작·한 행 바로 시작도) — PROCESS_EDIT blocks 는 이것을 고친 것, 비교는 정리 모양. 블록 검사가 정리된 목록의
                "cfg:키"(설정 기본값)를 비운 값과 같게 받음(정리 목록을 그대로 다시 보내도 통과)
                버전 15: v1 최신(main 12ecc2d) 반영 + 통신 설정. 히터 HEATER_CTRL_MODE 에 tc2(TC2 단독 제어, 기본값)·설정
                HEATER_TC2_TC1_WARN_C·HEATER_TC2_TC1_WARN_SEC·HEATER_TC2_HOLDBACK_C·규칙 X27, 매핑 600:846·700:982, 기본 램프 6,
                스냅샷 heater.ctrl_mode enumLabels(tc2)·사유 HEATER_TC2_TC1_DEVIATION, tc2·tc1 기본 템플릿에는 heater_settle 없음.
                MFC 설정점 5: pressure 블록 sp 1~5·spDefaultKey "5"·상태 PRESSURE_SP5·설정 SP5_MTORR·SP5_STABILIZE_SEC
                (WP > SP5_MTORR 이면 SP4 → SP5 설정·선택 → 고정 대기 → SP1), MFC_SP1_VERIFY_TOL → MFC_SP_VERIFY_TOL(0.01),
                스냅샷 mfc.valve_pct(밸브 위치 %, 자기 품질 열)·mfc.sp_active 0~5, dropped MFC_STATE_LOG_*.
                params[].choices(int 형식의 허용 숫자 목록 — 있으면 숫자 선택 목록으로 그리고 값은 숫자로 보낸다): *_BAUD·
                PLC_STOPBITS. 새 설치값 PLC_PARITY·PLC_STOPBITS·DC_DELIMITER 와 통신 시간·횟수 키, paramGroups comm_plc·comm_mfc·
                comm_dc·comm_rfpulse·comm_ig(장비별 통신 묶음), 규칙 X22(꺼진 장비의 포트 제외)·X42
                버전 16(5-18): 스냅샷 필드 enumLabelsBy {column: 기준 열, labels: {기준 값: {값: 이름}}} — 기준 열 값에 따라 바뀌는
                이름(그 줄에 없는 값은 enumLabels). heater.casc_state 는 heater.ctrl_mode = tc2 이면 running "TC2 제어(단독)"·
                frozen "TC2 제어(단독) · 셔터 열림(유지)"(v1 STATE_TEXT_TC2). enumLabels 는 그대로(옛 화면은 예전처럼 보인다)
                설정 MFC_READ_PLAN 설명을 드라이버와 같게(R5·R60·R6, 번갈아 = R60 짝수 초·R6 홀수 초, R6 양보)
                스냅샷 mfc.sp_sel_mtorr "압력 설정"(지금 선택된 설정점 sp_active 1~5 에 마지막으로 쓴 값 — SP1·SP2·SP3·SP5 는 되읽기
                확인값, SP4 는 쓴 SP4_MTORR, 선택이 없으면 값 없음 — 화면의 압력 목표). mfc.sp_mtorr 는 뜻 그대로(SP1 확인값)·
                이름 "작업 압력 설정(SP1)"·detail
                버전 17(5-19): 설정 MFC_FLOW_STABLE_TIMEOUT_SEC 기본 30 → 100 s·범위 5~600(v1 563f772 MFC_STABILIZATION_TIMEOUT_S)
                압력 설정점 값 출처(5-19 ③): 설정 SP2_SOURCE·SP3_SOURCE·SP4_SOURCE·SP5_SOURCE(enum program·device, params[].enumLabels
                {"program": "프로그램 값", "device": "장비 값"} — 기본 SP4 = device, 나머지 program, v1 과 같게), MFC_SP_REFRESH_SEC(comm_mfc).
                기본 템플릿: 장비 값 단계는 set=false·value 비움, 넣을지 비교(WP < SP3·WP < SP2·WP > SP5)는 장비 값(스냅샷
                mfc.sp{n}_dev_mtorr)으로 — 모르면 시작 거부 SP_DEVICE_VALUE_UNKNOWN("SP{n}: 장비에 저장된 값을 읽지 못함"). 골든
                web_template_cases 의 경우에 devSp({번호: mTorr})·templateError({code, detail}) — 기본(SP4 장비 값)으로 기존 경우의
                SP4 블록 set 이 false. pressure 블록: set 이름 "프로그램 값을 장비에 쓰기"(고급 아님), set=false 면 도달 대기 목표 =
                장비 값(펼친 WAIT_PRESSURE args device_sp·target null, 문구 "target=장비 값 SP{n}"), valueRules 에 kind "empty"
                (조건이면 그 값을 비워야)·조건 "<값>=false" — device_sp_no_value. 스냅샷 mfc.sp1_dev_mtorr~sp5_dev_mtorr(detail),
                mfc.sp_sel_mtorr = 쓴 값 → 없으면 장비 값. 사유 SP_DEVICE_VALUE_UNKNOWN·SP_DEVICE_CHANGED, eventKinds mfc sp_device
                버전 18(5-20): 읽기 계획 되돌아오기(①) — 설정 MFC_READ_PLAN_*·RFPULSE_READ_PLAN_*(SLOW_N·SLOW_M·SLOW_PCT·FAST_PCT·
                RETURN_SEC·HOLD_SEC, comm_mfc·comm_rfpulse, 규칙 X28·X29), 스냅샷 mfc.read_plan·rfp.read_plan(full "매초 읽기"·
                alternate "번갈아 읽기(2초마다)" — 이 행을 읽은 계획), mfc.valve_read(read·alternate·yielded "명령을 처리하느라 이번
                읽기를 건너뜀"·no_reply). read_plan 이벤트는 두 방향(번갈아로·매초로)
                노트북 조작(②): 설정 LOCAL_HMI_CONTROL(view_stop "보기와 정지만"·unlock "잠금을 푼 뒤 조작"(기본)·always "항상 조작")·
                LOCAL_HMI_RELOCK_MIN(10 분), commands[].hmi = "always"(노트북에서 늘)·"operate"(LOCAL_HMI_CONTROL ≠ view_stop 일 때)·
                null(받지 않음) — operate 명령은 sources 에 "hmi" 가 붙는다. 사유 HMI_CONTROL_OFF. hello 에 hmiControl·hmiRelockMin,
                hello.commandKinds = 노트북 경로에서 지금 받는 명령(설정에 따라)
                수동 조작(③): 명령 MANUAL_BLOCK {block}·MANUAL_END(웹·노트북 operate), 상태 MANUAL_WAIT "수동 대기"(진행 중),
                manual {commands, blockTypes, waitState, label, desc}, 사유 MANUAL_ACTIVE·MANUAL_BUSY·MANUAL_BLOCK_NOT_ALLOWED·
                MANUAL_NOT_ACTIVE(거부)·MANUAL_BLOCK·MANUAL_END(info), 이벤트 manual, run_start·run_end data manual(·by·source),
                스냅샷 proc.manual, state.run.manual {label "수동 조작 중", waiting, by, source, executed, outputs[{key, label,
                value?, unit?}]}
                화면 문구(④): 설정·스냅샷·블록·명령·사유·상태·이벤트 이름과 설명을 한 문장으로('—' 조각·'안 함·못 함·안 됨'·'켬·끔'
                없음, 코드 이름은 끝에), 빈 설명 채움(설정 121·스냅샷 71·블록 값 23·명령 9). 키·값·숫자·단위는 그대로. 블록 값 검사
                문구(valueRules msg)는 골든에 묶여 그대로
                버전 19(5-21): 스냅샷 rf.out_on "RF 출력"(D40 > 0 또는 순방향 ≥ 5 W — devices.rf_output_on, 시작 보호와 같은 기준.
                M00200 만 켜진 v1 종료 흔적은 꺼짐, 표시용)
                수동 조작의 말(③): 안전 규칙으로 멈춤 사건 문구 "수동 조작 중단", run_end summary manual·endText("수동 조작 끝"·
                "수동 조작 중단"), 수동 조작은 proc.remain_s 를 내지 않음
                사건 종류(④): eventKinds[].desc(그 기록의 뜻·운전자가 할 일), 이름을 운전자 말로(EVENT_LABEL — 예 "블록" → "공정 단계",
                "눈금" → "압력·유량 범위 확인", "읽기 계획" → "MFC 읽는 방식 바뀜")
                노트북 설정 보기(⑤): 로컬 /api/v1/schema 의 params(current·pending·stored·overridden·default·desc, 비밀 값은 비움)와
                state.configPending 으로 충분 — 더한 값 없음. 노트북 경로의 CONFIG_APPLY·CONFIG_ROLLBACK 은 지금처럼 받지 않음
                버전 20(5-22): 웹 연결 사건 이름표(①) — eventSources "uplink"(웹 연결), eventKinds uplink_up·uplink_down·
                uplink_auth_failed·uplink_conflict·uplink_gap·uplink_spool_loaded·uplink_spool_error·uplink_spool_full·uplink_too_large·
                uplink_bad_command(이름·설명·수준). 기록에 남는 code(UPLINK_*)는 그대로
                명령 이름(②): MANUAL_BLOCK "수동 조작"(전 "수동 조작: 블록 하나 실행"), 진행 중 건너뛰기·일시정지 "지금 단계 …",
                내부 정지 명령 이름을 운전자 말로(예 "DC 출력 끄기(꺼짐 확인)"), 수동 조작·건너뛰기·일시정지 사유 이름의 '블록' → 조작·단계.
                명령 종류(kind)·사유 코드는 그대로. 블록 레시피 편집 명령(PROCESS_EDIT)의 '블록'은 레시피 용어라 그대로
                버전 21(5-23): 화면 이름을 용어표 하나로(①) — 장비·부품·신호는 영문(MV·Door·Water·Ionizer·Ion Gauge·Filament·
                Forward·Reflected), Ar 밸브·Gun1 셔터·인터락·SP·정지 순서, 블록과 같은 상태 이름, 괄호 없는 명령 이름. 식별자는 그대로
                snapshot 필드 help(②, 운전자용 설명 — desc 는 개발용 그대로), reasons[].label 짧게·help(③), eventKinds desc 다듬음(④)
                버전 22(웹 W-21): 블록·상태 이름 '가스 주입'·'증착'·'공정 끝'·'Shutter Delay', 설정·명령·필드 설명의 옛 말을 용어표 말로
                (Ion Gauge·Filament·Forward·Reflected·인터락·SP·MV·천천히 올리기/내리기), 건너뛰기·일시정지 문구의 '블록' → 단계.
                스텝·사건 message(v1 대조)·키·값·숫자는 그대로
                버전 23(W-22): 수동 조작 규칙 사유(GAS_ALREADY_ON·SHUTTER_ALREADY·MANUAL_INTERRUPT·ZERO_SKIPPED), RF 표시 필드
                (rf.cmd_on·rf.uncommanded — rf.out_on 은 호환으로 남김)·사건(명령 없는 RF 출력·Reflected 대기), 새 설정(RF_OPERATING_MAX_W·
                RF_UNCOMMANDED_FWD_W·RF_REFP_HYST_W·ZERO_MIN_IDLE_SEC·INVENTORY_API_URL, RF_REF_FULL_SCALE_W 기본 600), 컨트롤러 값
                (devParams 구역·DEVICE_PARAMS_READ·DEVICE_PARAM_WRITE·state.devParams), 웹 레시피 사본(RECIPE_LOAD·HEATER_RECIPE_RUN 의
                id·updatedAt, 노트북 RECIPE_LOAD, state.recipeCopy), 장착 타겟(state.targets·사건 출처 inventory)
  crossRules    키 사이 규칙 설명 · dropped v2 에서 뺀 v1 키
  commands      명령(종류·경로·역할·허용 상태·조건·인자·benchOk) · preconditions · reasons(사유 코드) · plcButtons.
                benchOk = bench(장비 쓰기 금지)에서도 받는 명령 — false 인 명령은 bench 에서 BENCH_READ_ONLY 로 거부된다
  recipe        행 레시피 열(rf_pulse_freq 는 kHz, zeroIsEmpty 열은 0 = 빈 값) · 히터 레시피 열
  blocks        블록 정의(종류·값·단위·범위·기본값·설명) + 감시 항목·조건 값·한도·값 사이 규칙(valueRules)
  adjust        공정 중 수정 항목(장비 → 세부, 형식·범위·적용 방식·판정)
  stopSequence  STOP·이상 중단·행 끝 정지 단계(고정 — 정상 끝 정지만 end_stop skip, 파워 단계 제외) · allStopSequence(고정)
  states        공정 상태 이름 · events 웹이 받는 공정 기록 이벤트(종류·data 키)
  eventKinds    event 메시지의 (context.source, context.kind) → 화면 이름·level(main 평소 로그 / detail 상세에서만).
                warn·error·critical 은 표와 상관없이 늘 보이고, 표에 없는 짝은 detail 로 본다
  eventSources  event 메시지 context.source → 화면 출처 이름(DC Power·RF Pulse …)
  paramGroups   설정(params[].group) 묶음 key → 화면 이름
  snapshot      1초 스냅샷 열 정의(웹 화면 라벨·단위·형식·그래프) — tick·process 필드, 장비 블록(이름·라벨·worker·
                품질 열 <블록>.q·<블록>.age_s) → 필드(column 전체 이름·형식 fmt 는 파이썬 format spec 그대로 .1f·.2E·d).
                ownQuality 필드는 <column>.q·<column>.age_s 품질 열을 더 갖는다. 열 집합 = hello.snapshotColumns.
                stateColumn(값 열) = 같은 블록 상태 열 — 화면 규칙: 값이 비어 있고 그 상태가 ok 가 아니면 상태 열의
                enumLabels 이름을 값 자리에 보인다(장비 종류를 따로 보지 않는다)
                enumLabelsBy(버전 16) = 다른 열 값에 따라 바뀌는 이름 — 화면 규칙: labels[그 열 값][이 값] 이 있으면 그것, 없으면 enumLabels
범위가 파라미터 값에 묶인 항목(paramMin·paramMax·paramScale)은 웹이 params 의 현재값으로 계산한다.
hash 는 정의만으로 계산한다(현재값·대기값 제외) — 내용이 바뀌면 시험(test_web_schema)이 알고 EXPORT_VERSION 을 올리게 한다.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Optional

from v2.common.paramdef import Scope, schema_hash
from v2.common.snapshot import TICK_COLUMNS
from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import dev_params as DP
from v2.chk import params as P
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk import safety_rules as SR
from v2.chk import snapshot_fields as SF

EXPORT_VERSION = 23

# 웹이 받는 공정 기록 이벤트(ProcEvent kind → data 키) — 웹 단계에서 그대로 event·run_* 메시지로 올린다
EVENTS: tuple = (
    ("run_start", "행 시작(수동 조작이면 manual·by·source — 5-20 ③)", ("name", "params", "run_id", "blocks", "manual", "by",
                                                                   "source")),
    ("steps", "펼친 스텝 목록(골든 비교 단위)", ("steps",)),
    ("block", "블록 시작·끝(phase start/end, result ok/failed/aborted/error, reason)",
     ("no", "type", "label", "stage", "params", "phase", "result", "reason", "index", "total", "elapsed_s", "t", "run_id")),
    ("adjust", "공정 중 수정(PROCESS_ADJUST) — result rejected/scheduled/applied → confirmed/failed",
     ("device", "item", "old", "new", "source", "by", "result", "reason", "t", "run_id", "state", "block", "level")),
    ("state", "상태 전이", ("from", "to", "t")),
    ("warn", "경고(OUTPUTS_LEFT_ON·COND_WAIT_TIMEOUT·ADJUST_SETTLE_TIMEOUT …)", ()),
    ("reject", "적재·시작 거부(블록 검사 오류 목록)", ("errors",)),
    ("plc_manual", "공정 중 PLC 버튼(PLC_MANUAL_IN_PROCESS)", ("target", "on", "source", "by", "t", "run_id", "state", "level")),
    ("stop_rest", "공정 끝 중 STOP·이상 중단 — 뺀 단계를 이어서 실행(남김 취소)", ("steps", "level")),
    ("skip", "블록 건너뛰기(PROCESS_SKIP)", ("by", "source", "no", "type", "label", "kind", "cleaned", "pausedSec",
                                            "run_id", "revision", "t", "level")),
    ("pause", "일시정지(PROCESS_PAUSE)", ("by", "source", "no", "type", "label", "kind", "remainS", "cleaned",
                                       "run_id", "revision", "t", "level")),
    ("resume", "재개(PROCESS_RESUME)", ("by", "source", "no", "type", "label", "kind", "pausedSec", "cleaned",
                                      "run_id", "revision", "t", "level")),
    ("edit", "순서 고치기(PROCESS_EDIT) — 판마다 전체 목록", ("by", "source", "summary", "from", "count", "blocks", "warnings",
                                                         "insts", "run_id", "revision", "t", "level")),
    ("pause_long", "일시정지 오래(PAUSE_LONG — 한도 5분 전)", ("no", "pausedSec", "maxSec", "level")),
    ("manual", "수동 조작 블록(MANUAL_BLOCK — 누가·web/local·블록·결과)·끝내기(MANUAL_END)",
     ("by", "source", "no", "type", "label", "params", "result", "reason", "run_id", "t", "level")),
    ("run_end", "행 끝 — summary: 실행된 순서(executed)·마지막 판·건너뛴/멈춘 횟수(수동 조작이면 manual)",
     ("name", "status", "reason", "summary", "manual")),
    ("refp_wait", "RF Reflected 대기 시작·끝(S32 — 한 번씩, W-22)", ("phase", "ref_w", "threshold", "max_s", "why", "level", "run_id")),
    ("targets", "레시피 타겟 이름을 재고관리 장착 타겟으로 채움(TARGETS_FILLED, W-22)", ("g1", "g2", "level", "run_id")),
    ("list_done", "레시피 리스트 끝", ()),
    ("list_cancel", "레시피 리스트 취소", ()),
)


# 사건 종류의 운전자 말 이름(5-21 ④) — 위 표의 이름을 바꾼 것만. 표의 이름은 시험·옛 이름과 맞추려 그대로 둔다
EVENT_LABEL: Dict[tuple, str] = {
    ("safety", "abort"): "안전 규칙으로 멈춤", ("process", "block"): "공정 단계", ("process", "steps"): "공정 순서 전체", ("process", "step"): "세부 동작",
    ("process", "skip"): "단계 건너뜀", ("process", "edit"): "공정 순서 고침", ("process", "auto_end_stop"): "끝 정리 단계 덧붙임",
    ("process", "stop_rest"): "남은 정지 단계 실행", ("process", "message"): "장비 동작 알림",
    ("safety", "blip"): "안전 조건 잠깐 벗어남", ("safety", "also"): "정지 중 다른 안전 규칙 발동",
    ("safety", "step"): "정지 단계 진행", ("safety", "error"): "정지 단계 실패", ("safety", "seq_start"): "정지 순서 시작",
    ("safety", "seq_done"): "정지 순서 끝", ("safety", "bench_skip"): "읽기 전용이라 보내지 않음",
    ("plc", "marker_written"): "PLC 재기동 표시 씀", ("plc", "heater_wd_delay"): "히터 워치독 늦음",
    ("plc", "heater_wd_recovered"): "히터 워치독 복구", ("plc", "blackbox"): "PLC 값 기록 저장",
    ("plc", "heater_residual_run"): "히터 운전 상태가 남아 있음",
    ("mfc", "fs"): "유량 범위 확인", ("mfc", "fs_not_100"): "유량 범위가 100 이 아님", ("mfc", "scale"): "압력·유량 범위 확인",
    ("mfc", "verify_ok"): "설정 확인됨", ("mfc", "verify_mismatch"): "설정이 다르게 읽힘", ("mfc", "sp_readback"): "압력 SP 확인",
    ("mfc", "remote_state"): "원격 조작 상태", ("mfc", "sp_device"): "장비에 저장된 압력 SP", ("mfc", "read_plan"): "MFC 읽는 방식 바뀜",
    ("mfc", "params_read"): "컨트롤러 값 읽음", ("mfc", "param_write"): "컨트롤러 값 바꿈",
    ("process", "targets"): "장착 타겟 채움", ("inventory", "targets_read_failed"): "장착 타겟 읽기 실패",
    ("inventory", "targets_read_ok"): "장착 타겟 읽기 복구", ("inventory", "targets_changed"): "장착 타겟 바뀜",
    ("dc", "identity"): "장비 확인", ("dc", "syst_err"): "장비 자체 오류 기록", ("dc", "last_state"): "전원 복귀 때 출력 설정",
    ("dc", "protection"): "과전압·과전류 보호 설정", ("dc", "out_check"): "출력 켜짐 확인",
    ("rfp", "identity"): "장비 확인", ("rfp", "auto_off"): "Reflected 자동 차단 설정", ("rfp", "auto_off_read"): "Reflected 자동 차단 읽음",
    ("rfp", "csr_recovery"): "명령 거부 뒤 다시 시도", ("rfp", "config_readback"): "펄스 설정 확인", ("rfp", "read_plan"): "RF Pulse 읽는 방식 바뀜",
    ("ig", "identity_ok"): "장비 확인", ("ig", "parse_error"): "응답을 읽을 수 없음",
}
# 사건 종류 설명(5-21 ④) — 그 기록이 무슨 뜻이고 운전자가 할 일이 있는지. ("*", kind) 는 장비 연결(worker) 사건 공통
EVENT_DESC: Dict[tuple, str] = {
    ("process", "run_start"): "공정이나 수동 조작을 시작했습니다. 누가 어떤 값으로 시작했는지 남깁니다.",
    ("process", "run_end"): "공정이나 수동 조작이 끝났습니다. 정상이 아니면 사유를 확인하세요.",
    ("process", "state"): "공정 상태가 바뀌었습니다.",
    ("process", "block"): "공정의 한 단계가 시작되거나 끝났습니다. 끝난 단계는 결과를 남깁니다.",
    ("process", "steps"): "이번 공정에서 실행할 세부 동작 전체 목록입니다. 확인용 기록입니다.",
    ("process", "step"): "세부 동작 하나를 실행했습니다. 확인용 기록입니다.",
    ("process", "warn"): "공정은 계속되지만 확인할 일이 생겼습니다. 내용을 읽고 필요하면 조치하세요.",
    ("process", "adjust"): "공정 중에 값을 바꿨습니다. 누가 무엇을 어떻게 바꿨는지 남깁니다.",
    ("process", "skip"): "진행 중인 단계를 건너뛰었습니다. 건너뛰며 정리한 것을 남깁니다.",
    ("process", "pause"): "진행 중인 단계를 일시정지했습니다. 안전 감시는 계속됩니다.",
    ("process", "resume"): "일시정지했던 단계를 이어서 진행합니다.",
    ("process", "edit"): "아직 시작하지 않은 단계의 순서나 값을 고쳤습니다.",
    ("process", "pause_long"): "일시정지가 곧 한도에 이릅니다. 재개할지 멈출지 정하세요.",
    ("process", "manual"): "수동 조작 하나를 실행했거나 수동 조작을 끝냈습니다.",
    ("process", "plc_manual"): "공정 중에 PLC 버튼을 눌렀습니다. 누가 무엇을 켜고 껐는지 남깁니다.",
    ("process", "list_done"): "적재한 레시피의 모든 행이 끝났습니다.",
    ("process", "list_cancel"): "적재한 레시피가 중간에 취소되었습니다. 남은 행은 실행하지 않습니다.",
    ("process", "stop_rest"): "끝 정리 중 멈춤 요청이 와서 남겨 두려던 출력도 모두 끕니다.",
    ("process", "auto_end_stop"): "레시피 끝에 정리 단계가 없어 기본 끝 정리를 덧붙였습니다.",
    ("process", "reject"): "공정을 시작하지 않았습니다. 사유를 고친 뒤 다시 시작하세요.",
    ("process", "base_reached"): "챔버가 목표 베이스 압력에 도달했습니다.",
    ("process", "heater_handover"): "공정이 끝났지만 히터를 켠 채 넘겼습니다. 히터 카드에서 끄세요.",
    ("process", "refp_wait"): "RF Reflected 가 기준을 넘어 올리기를 멈추고 기다리기 시작했거나 끝났습니다. 오래 이어지면 공정이 멈춥니다.",
    ("plc", "rf_uncommanded"): "프로그램이 켜지 않았는데 RF 출력이 나옵니다. RF 전원 앞면(LOCAL)에서 켰는지 확인하세요.",
    ("plc", "rf_uncommanded_end"): "명령 없이 나오던 RF 출력이 멈췄습니다.",
    ("process", "cleanup_start"): "지난 공정이 남긴 출력을 끄기 시작했습니다.",
    ("process", "cleanup"): "지난 공정이 남긴 출력을 모두 정리했습니다.",
    ("process", "message"): "공정 중 장비 제어가 남긴 알림입니다. 경고면 내용을 확인하세요.",
    ("safety", "fired"): "안전 규칙이 걸렸습니다. 공정이 멈추거나 출력이 꺼질 수 있으니 원인을 확인하세요.",
    ("safety", "released"): "걸렸던 안전 조건이 풀렸습니다.",
    ("safety", "blip"): "안전 조건을 잠깐 벗어났다가 돌아왔습니다. 공정은 계속됩니다.",
    ("safety", "also"): "멈추는 중에 다른 안전 규칙도 걸렸습니다. 첫 사유가 결과로 남습니다.",
    ("safety", "abort"): "안전 규칙 때문에 멈췄습니다. 사유를 확인한 뒤 다시 시작하세요.",
    ("safety", "seq_start"): "정지 순서를 시작했습니다.",
    ("safety", "seq_done"): "정지 순서가 끝났습니다.",
    ("safety", "step"): "정지 순서의 한 단계를 실행했습니다.",
    ("safety", "error"): "정지 단계 하나가 실패했습니다. 그 장비가 꺼졌는지 장비 앞면에서 확인하세요.",
    ("safety", "bench_skip"): "읽기 전용이라 장비에 명령을 보내지 않았습니다. 조작은 장비 앞면에서 하세요.",
    ("heater", "reached"): "히터가 목표 온도에 도달했습니다.",
    ("heater", "engaged"): "히터가 목표 온도를 유지하는 방식으로 바뀌었습니다.",
    ("heater", "give_up"): "히터 유지 방식에 들어가지 못했습니다. 온도가 안정적인지 확인하세요.",
    ("heater", "alert"): "히터 제어에 확인할 일이 생겼습니다. 내용을 읽고 필요하면 히터를 멈추세요.",
    ("heater", "saturated"): "히터 출력이 상한에 닿아 더 올릴 수 없습니다.",
    ("heater", "gas"): "히터 가스의 준비·유지·해제 상태가 바뀌었습니다.",
    ("heater", "recipe_step"): "히터 레시피가 다음 단계로 넘어갔습니다.",
    ("heater", "recipe_finished"): "히터 레시피가 끝났습니다. 결과와 사유를 확인하세요.",
    ("heater", "message"): "히터 제어가 남긴 알림입니다.",
    ("plc", "link_up"): "PLC 와 다시 연결되었습니다.",
    ("plc", "link_down"): "PLC 연결이 끊겼습니다. PLC 와 케이블을 확인하세요.",
    ("plc", "plc_restarted"): "PLC 가 다시 켜졌습니다. 진행 중인 공정은 멈추고 히터 한계값을 다시 씁니다.",
    ("plc", "marker_written"): "PLC 재기동을 알아보는 표시를 썼습니다. 확인용 기록입니다.",
    ("plc", "heater_config_ok"): "PLC 의 히터 한계값을 되읽어 확인했습니다.",
    ("plc", "heater_config_mismatch"): "PLC 의 히터 한계값이 설정과 다릅니다. 히터를 쓰기 전에 확인하세요.",
    ("plc", "heater_residual_run"): "프로그램을 켤 때 남아 있던 히터 운전을 정리했습니다.",
    ("plc", "heater_off_pending"): "PLC 연결이 없어 히터 끄기를 기다립니다. 연결되면 바로 끕니다.",
    ("plc", "heater_off_applied"): "기다리던 히터 끄기를 적용했습니다.",
    ("plc", "heater_off_retry"): "히터 끄기를 다시 보냈습니다.",
    ("plc", "heater_wd_delay"): "히터 워치독 쓰기가 늦었습니다. 이어지면 PLC 가 히터를 끕니다.",
    ("plc", "heater_wd_recovered"): "히터 워치독 쓰기가 정상으로 돌아왔습니다.",
    ("plc", "blackbox"): "이상 직전 PLC 값을 파일로 저장했습니다. 원인 분석에 씁니다.",
    ("mfc", "fs"): "MFC 유량 범위를 읽었습니다. 확인용 기록입니다.",
    ("mfc", "fs_not_100"): "MFC 유량 범위가 100 이 아닙니다. 공정 전에 유량 단위를 확인하세요.",
    ("mfc", "scale"): "MFC 압력·유량 범위를 설정과 대조했습니다. 다르면 공정을 시작하지 않습니다.",
    ("mfc", "verify_ok"): "MFC 에 쓴 값을 되읽어 확인했습니다.",
    ("mfc", "verify_mismatch"): "MFC 에 쓴 값이 다르게 읽혔습니다. 공정이 멈출 수 있습니다.",
    ("mfc", "sp_readback"): "압력 SP 를 쓴 뒤 되읽은 값입니다. 확인용 기록입니다.",
    ("mfc", "remote_state"): "MFC 원격 조작 상태를 읽었습니다. 정보로만 남깁니다.",
    ("mfc", "sp_device"): "장비에 저장된 압력 SP 값을 읽었습니다.",
    ("mfc", "read_plan"): "MFC 읽기 속도에 따라 읽는 방식을 바꿨습니다.",
    ("process", "targets"): "레시피에 타겟 이름이 비어 있어 재고관리의 장착 타겟으로 채웠습니다. 공정 기록에 남습니다.",
    ("inventory", "targets_read_failed"): "재고관리에서 장착 타겟을 읽지 못했습니다. 공정은 그대로 진행합니다.",
    ("inventory", "targets_read_ok"): "재고관리에서 장착 타겟을 다시 읽었습니다.",
    ("inventory", "targets_changed"): "재고관리의 Gun1·Gun2 장착 타겟이 바뀌었습니다.",
    ("uplink", "uplink_recipes"): "웹 레시피 사본을 웹과 맞췄습니다. 노트북에서 이 사본으로 레시피를 고릅니다.",
    ("uplink", "uplink_recipes_failed"): "웹 레시피 사본을 받지 못했습니다. 노트북은 마지막 사본으로 고릅니다.",
    ("mfc", "params_read"): "MFC 컨트롤러에 저장된 값을 읽었습니다. 장비 설정 보기의 컨트롤러 값에 보입니다.",
    ("mfc", "param_write"): "관리자가 MFC 컨트롤러 값을 바꿨습니다. 되읽은 값이 다르면 실패로 남습니다.",
    ("dc", "identity"): "DC Power 의 형명·버전을 읽었습니다.",
    ("dc", "syst_err"): "DC Power 가 남긴 오류 기록을 읽었습니다.",
    ("dc", "last_state"): "전원이 돌아올 때 DC 출력 설정을 읽었습니다. 출력이 켜지는 설정이면 앞면에서 바꾸세요.",
    ("dc", "protection"): "DC 과전압·과전류 보호 설정을 읽거나 썼습니다.",
    ("dc", "trip"): "DC 보호가 걸려 출력이 나오지 않습니다. 원인을 확인하고 앞면에서 해제하세요.",
    ("dc", "out_check"): "DC 출력이 켜졌는지 확인했습니다.",
    ("dc", "off_confirmed"): "DC 출력이 꺼진 것을 확인했습니다.",
    ("dc", "off_unconfirmed"): "DC 꺼짐을 확인하지 못해 다시 시도합니다. 이어지면 앞면을 확인하세요.",
    ("dc", "off_unverified"): "이번 실행에서 DC 를 켠 적이 없어 꺼짐 확인을 생략했습니다.",
    ("rfp", "identity"): "RF Pulse 의 형명·용량·버전을 읽었습니다.",
    ("rfp", "auto_off"): "Reflected 가 이어지면 장비가 스스로 끄는 시간을 쓰고 확인했습니다.",
    ("rfp", "auto_off_read"): "읽기 전용이라 Reflected 자동 차단 설정을 읽기만 했습니다.",
    ("rfp", "csr_recovery"): "RF Pulse 가 명령을 받지 않아 상태를 바로잡은 뒤 다시 보냈습니다.",
    ("rfp", "config_readback"): "RF Pulse 를 켠 뒤 펄스 주파수·듀티를 되읽어 확인했습니다.",
    ("rfp", "read_plan"): "RF Pulse 읽기 속도에 따라 읽는 방식을 바꿨습니다.",
    ("rfp", "retry"): "RF Pulse 가 응답하지 않거나 거부해 명령을 다시 보냈습니다.",
    ("rfp", "adjusted"): "공정 중 바꾼 RF Pulse 값을 장비에 적용했습니다.",
    ("rfp", "off_confirmed"): "RF Pulse 출력이 꺼진 것을 확인했습니다.",
    ("rfp", "off_unconfirmed"): "RF Pulse 꺼짐을 확인하지 못했습니다. 장비 앞면을 확인하세요.",
    ("ig", "identity_ok"): "Ion Gauge 응답을 확인했습니다.",
    ("ig", "filament_sent"): "Filament 켜기·끄기 명령을 보냈습니다.",
    ("ig", "filament_result"): "Filament 가 실제로 켜지거나 꺼졌는지 확인했습니다.",
    ("ig", "parse_error"): "Ion Gauge 응답을 읽을 수 없습니다. 이어지면 연결을 확인하세요.",
    ("*", "connected"): "장비와 연결되었습니다.",
    ("*", "connect_failed"): "장비 포트를 열 수 없습니다. 케이블·포트 번호·다른 프로그램 사용을 확인하세요.",
    ("*", "connect_retry"): "장비 연결을 다시 시도합니다.",
    ("*", "disconnected"): "장비 연결이 끊겼습니다. 케이블과 장비 전원을 확인하세요.",
    ("*", "long_outage"): "장비 연결이 오래 끊겨 있습니다. 케이블과 장비 전원을 확인하세요.",
    ("*", "timeout"): "장비가 정해진 시간 안에 응답하지 않았습니다.",
    ("*", "COMMAND_BLOCKED"): "허용되지 않은 명령이라 장비에 보내지 않았습니다.",
    ("uplink", "uplink_up"): "웹 연결이 돌아왔습니다. 쌓인 기록을 이어서 보냅니다.",
    ("uplink", "uplink_down"): "웹과 연결이 끊겼습니다. 공정은 그대로이고 기록은 장비에 쌓입니다.",
    ("uplink", "uplink_auth_failed"): "웹이 장비 토큰을 받지 않습니다. 장비 관리에서 장비 사용 여부와 토큰을 확인하세요.",
    ("uplink", "uplink_conflict"): "웹이 이 실행을 받지 않습니다. 같은 장비 이름의 다른 실행이 있는지 확인하세요.",
    ("uplink", "uplink_gap"): "웹으로 보낼 기록 일부를 놓쳤습니다. 장비 기록 파일에는 남아 있습니다.",
    ("uplink", "uplink_spool_loaded"): "지난 실행이 보내지 못한 기록을 이어서 보냅니다.",
    ("uplink", "uplink_spool_error"): "웹으로 보낼 기록을 디스크에 저장하지 못했습니다. 디스크 공간을 확인하세요.",
    ("uplink", "uplink_spool_full"): "미전송 보관함이 가득 차 중요도가 낮은 기록부터 버렸습니다. 웹 연결을 확인하세요.",
    ("uplink", "uplink_too_large"): "너무 큰 기록이라 웹에 보내지 않았습니다. 장비 기록 파일에는 남아 있습니다.",
    ("uplink", "uplink_bad_command"): "읽을 수 없는 웹 명령이라 실행하지 않았습니다.",
}


# 설정 묶음(params[].group) → 화면 이름
PARAM_GROUPS: tuple = (
    ("install", "설치·장비"), ("secret", "비밀값"),
    # 장비별 통신(5-17): 포트·속도·패리티·종결 문자·응답 시간·재시도 — params.COMM_GROUPS
    ("comm_plc", "PLC 통신"), ("comm_mfc", "MFC 통신"), ("comm_dc", "DC Power 통신"), ("comm_rfpulse", "RF Pulse 통신"),
    ("comm_ig", "Ion Gauge 통신"), ("comm", "통신 공통"),
    ("tick", "1초 읽기"), ("plc", "PLC"),
    ("mfc", "MFC"), ("dc", "DC Power"), ("rf", "RF Power"), ("stop", "정지 순서"), ("rfpulse", "RF Pulse"),
    ("ig", "Ion Gauge"), ("heater", "히터"), ("heater_plc", "히터 PLC 설정"), ("heater_ramp", "히터 승온"),
    ("heater_hold", "히터 유지 모드"), ("heater_recipe", "히터 레시피"), ("heater_gas", "히터 가스"),
    ("process", "공정"), ("uplink", "웹 연결"), ("record", "기록"),
)

# event 메시지 (context.source, context.kind) → (화면 이름, level). hub.on_event 가 정하는 source:
#   process(ProcEvent) · safety(SafetyEvent) · heater(HeaterEvent) · 드라이버 이벤트 클래스 이름(dc·ig·mfc·plc·rfp) ·
#   worker·통신 가드 이벤트는 worker 이름(plc·mfc·dc·rfpulse·ig).
#   main = 사람이 공정 흐름을 따라가는 데 필요한 것, detail = 스텝 하나하나·되읽기·식별·자기 진단 등(상세에서만).
#   run_start·run_end 는 event 가 아니라 run_start·run_end 메시지로 나가지만 표에는 둔다(ProcEvent 종류 전부).
_M, _D = "main", "detail"
# event 메시지 context.source → 화면 출처 이름(5-15 — 장비 이름은 DC Power·RF Power·RF Pulse)
EVENT_SOURCES: tuple = (
    ("process", "공정"), ("safety", "안전"), ("heater", "히터"), ("plc", "PLC"), ("mfc", "MFC"), ("dc", "DC Power"),
    ("rfp", "RF Pulse"), ("rfpulse", "RF Pulse"), ("ig", "Ion Gauge"), ("uplink", "웹 연결"), ("inventory", "재고관리"),
)
_WORKERS = ("plc", "mfc", "dc", "rfpulse", "ig")
EVENT_KINDS: tuple = (
    ("process", "run_start", "공정 시작", _M), ("process", "run_end", "공정 끝", _M),
    ("process", "state", "상태 바뀜", _M), ("process", "block", "블록", _M),
    ("process", "steps", "펼친 스텝 목록", _D), ("process", "step", "스텝", _D),
    ("process", "warn", "공정 경고", _M), ("process", "adjust", "공정 중 수정", _M),
    ("process", "skip", "블록 건너뜀", _M), ("process", "pause", "일시정지", _M), ("process", "resume", "재개", _M),
    ("process", "edit", "순서 고침", _M), ("process", "pause_long", "일시정지가 길어져 곧 정지", _M),
    ("process", "manual", "수동 조작", _M),
    ("process", "plc_manual", "공정 중 PLC 버튼", _M), ("process", "list_done", "레시피 끝", _M),
    ("process", "list_cancel", "레시피 취소", _M), ("process", "stop_rest", "남은 정지 단계 실행", _M),
    ("process", "auto_end_stop", "자동 끝 정지 덧붙임", _M), ("process", "reject", "시작 거부", _M),
    ("process", "base_reached", "베이스 압력 도달", _M), ("process", "heater_handover", "히터 켠 채 넘김", _M),
    ("process", "cleanup_start", "남은 출력 정리 시작", _M), ("process", "cleanup", "남은 출력 정리 끝", _M),
    ("process", "message", "장비 알림", _D),
    ("safety", "fired", "안전 규칙 발동", _M), ("safety", "released", "안전 규칙 해제", _M),
    ("safety", "blip", "안전 규칙 순간 발동", _D), ("safety", "also", "이미 정지 중에 추가 발동", _D),
    ("safety", "abort", "공정 중단", _M), ("safety", "seq_start", "정지 순서 시작", _M),
    ("safety", "seq_done", "정지 순서 끝", _M), ("safety", "step", "정지 단계 진행", _D),
    ("safety", "error", "정지 단계 오류", _M), ("safety", "bench_skip", "bench 라서 보내지 않음", _M),
    ("heater", "reached", "히터 목표 도달", _M), ("heater", "engaged", "유지 모드 진입", _M),
    ("heater", "give_up", "유지 모드 포기", _M), ("heater", "alert", "히터 경고", _M),
    ("heater", "saturated", "히터 출력 포화", _M), ("heater", "gas", "히터 가스", _M),
    ("heater", "recipe_step", "히터 레시피 단계", _M), ("heater", "recipe_finished", "히터 레시피 끝", _M),
    ("heater", "message", "히터 알림", _D),
    ("plc", "link_up", "장비 연결 복구", _M), ("plc", "link_down", "장비 연결 끊김", _M),
    ("plc", "plc_restarted", "PLC 재기동 감지", _M), ("plc", "marker_written", "재기동 마커 씀", _D),
    ("plc", "heater_config_ok", "히터 설정 확인", _D), ("plc", "heater_config_mismatch", "히터 설정 다름", _M),
    ("plc", "heater_residual_run", "히터 RUN 남음", _M), ("plc", "heater_off_pending", "히터 끄기 보류", _M),
    ("plc", "heater_off_applied", "히터 끄기 적용", _M), ("plc", "heater_off_retry", "히터 끄기 다시", _D),
    ("process", "refp_wait", "Reflected 대기", _M),
    ("plc", "rf_uncommanded", "명령 없는 RF 출력", _M), ("plc", "rf_uncommanded_end", "명령 없는 RF 출력 끝", _M),
    ("plc", "heater_wd_delay", "워치독 쓰기 지연", _D), ("plc", "heater_wd_recovered", "워치독 쓰기 복구", _D),
    ("plc", "blackbox", "블랙박스 기록", _D),
    ("mfc", "fs", "풀스케일", _D), ("mfc", "fs_not_100", "풀스케일 100 아님", _M), ("mfc", "scale", "눈금", _D),
    ("mfc", "verify_ok", "되읽기 확인", _D), ("mfc", "verify_mismatch", "되읽기 다름", _M),
    ("mfc", "sp_readback", "설정점 되읽기", _D), ("mfc", "remote_state", "원격 상태", _D),
    ("mfc", "sp_device", "설정점 장비 값", _D),
    ("mfc", "read_plan", "읽기 계획", _D),
    ("mfc", "params_read", "컨트롤러 값 읽기", _D), ("mfc", "param_write", "컨트롤러 값 바꾸기", _M),
    ("process", "targets", "장착 타겟 채움", _M),
    ("inventory", "targets_read_failed", "장착 타겟 읽기 실패", _M), ("inventory", "targets_read_ok", "장착 타겟 읽기 복구", _D),
    ("inventory", "targets_changed", "장착 타겟 바뀜", _D),
    ("dc", "identity", "장비 식별", _D), ("dc", "syst_err", "자기 진단", _D), ("dc", "last_state", "마지막 상태", _D),
    ("dc", "protection", "보호 설정", _D), ("dc", "trip", "보호 트립", _M), ("dc", "out_check", "출력 확인", _D),
    ("dc", "off_confirmed", "꺼짐 확인", _D), ("dc", "off_unconfirmed", "꺼짐 미확인", _M),
    ("dc", "off_unverified", "꺼짐 확인 생략", _D),
    ("rfp", "identity", "장비 식별", _D), ("rfp", "auto_off", "자동 꺼짐", _M), ("rfp", "auto_off_read", "자동 꺼짐 읽기", _D),
    ("rfp", "csr_recovery", "명령 응답 복구", _D), ("rfp", "config_readback", "설정 되읽기", _D),
    ("rfp", "read_plan", "읽기 계획", _D), ("rfp", "retry", "다시 시도", _D), ("rfp", "adjusted", "공정 중 수정 적용", _D),
    ("rfp", "off_confirmed", "꺼짐 확인", _D), ("rfp", "off_unconfirmed", "꺼짐 미확인", _M),
    ("ig", "identity_ok", "장비 식별", _D), ("ig", "filament_sent", "Filament 명령", _D),
    ("ig", "filament_result", "Filament 결과", _M), ("ig", "parse_error", "응답 해석 실패", _D),
) + tuple(x for w in _WORKERS for x in (
    (w, "connected", "장비 연결됨", _D), (w, "connect_failed", "연결 실패", _D), (w, "connect_retry", "연결 다시 시도", _D),
    (w, "disconnected", "장비 연결 끊김", _M), (w, "long_outage", "장비 연결 오래 끊김", _M), (w, "timeout", "응답 없음", _D),
    (w, "COMMAND_BLOCKED", "명령 막힘", _M),
)) + (
    # 웹 연결(5-22 ① — uplink._note: source "uplink", kind = 코드 소문자. 기록에 남는 code 는 UPLINK_* 그대로)
    ("uplink", "uplink_up", "웹 연결 복구", _M), ("uplink", "uplink_down", "웹 연결 끊김", _M),
    ("uplink", "uplink_auth_failed", "웹이 장비 토큰을 거부함", _M), ("uplink", "uplink_conflict", "웹이 이 실행을 받지 않음", _M),
    ("uplink", "uplink_gap", "웹으로 보낼 기록 일부를 놓침", _M), ("uplink", "uplink_spool_loaded", "미전송 기록 이어서 보냄", _D),
    ("uplink", "uplink_spool_error", "미전송 보관함 쓰기 실패", _M), ("uplink", "uplink_spool_full", "미전송 보관함이 가득 참", _M),
    ("uplink", "uplink_too_large", "너무 큰 기록을 보내지 않음", _D), ("uplink", "uplink_bad_command", "읽을 수 없는 웹 명령을 버림", _D),
    ("uplink", "uplink_recipes", "웹 레시피 사본 맞춤", _D), ("uplink", "uplink_recipes_failed", "웹 레시피 사본 받기 실패", _M),
)


def _param(p, values: Optional[Mapping[str, Any]], pending: Mapping[str, Any],
           stored: Optional[Mapping[str, Any]] = None, overridden: frozenset = frozenset()) -> dict:
    secret = p.scope is Scope.SECRET
    if secret or values is None:
        st = ov = None
    else:
        st = stored.get(p.key, p.default) if stored is not None else values.get(p.key)
        ov = p.key in overridden
    return {"key": p.key, "label": p.label, "group": p.group, "unit": p.unit, "type": p.type.value,
            "min": p.min, "max": p.max, "enum": list(p.enum) if p.enum else None, "pattern": p.pattern,
            "choices": list(p.choices) if p.choices else None,
            "enumLabels": dict(p.enum_labels) if p.enum_labels else None,     # 5-19 — 고르는 값의 화면 이름
            "default": None if secret else p.default,
            "current": None if (secret or values is None) else values.get(p.key),
            "stored": st, "overridden": ov,
            "pending": None if secret else pending.get(p.key),
            "safety": p.safety.value, "apply": p.apply.value, "scope": p.scope.value, "role": p.role.value,
            "v1Key": p.v1_key, "v1Source": p.v1_source, "v1Default": p.v1_default, "pendingConfirm": p.pending_confirm,
            "webLocked": p.web_locked,
            "desc": p.desc, "note": p.note}


def _arg(a) -> dict:
    return {"name": a.name, "type": a.type, "required": a.required, "min": a.min, "max": a.max,
            "paramMin": a.param_min, "paramMax": a.param_max, "enum": list(a.enum) if a.enum else None, "desc": a.desc}


def _col(c) -> dict:
    return {"name": c.name, "type": c.type, "unit": c.unit, "required": c.required, "min": c.min, "max": c.max,
            "paramMin": c.param_min, "paramMax": c.param_max, "minExclusive": c.min_exclusive,
            "zeroIsEmpty": c.zero_is_empty, "v1Name": c.v1_name,
            "desc": c.desc, "label": c.label, "help": c.help, "group": c.group}


def _seq(s) -> dict:
    return {"no": s.no, "act": s.act.kind, "args": dict(s.act.args), "timeoutParam": s.timeout_param or None,
            "must": s.must, "when": s.when, "desc": s.desc, "nowait": s.nowait, "label": s.label}


def _field(f, column: str) -> dict:
    return {"column": column, "name": f.name, "type": f.type.value, "unit": f.unit, "label": f.label, "fmt": f.fmt,
            "summary": f.summary, "ownQuality": f.own_quality, "detail": f.detail, "desc": f.desc,
            "help": f.help or SF.FIELD_HELP.get(column, ""),          # 5-23 ② 운전자용 설명(평소 보기)
            "enumLabels": dict(f.enum_labels) if f.enum_labels else None,
            "stateColumn": f"{column.rsplit('.', 1)[0]}.{f.state}" if f.state else None,
            "enumLabelsBy": _labels_by(f, column)}


def _labels_by(f, column: str) -> Optional[dict]:
    """FieldDef.enum_labels_by → {"column": 기준 열, "labels": {기준 값: {값: 이름}}} — 5-18(버전 16)."""
    if not f.enum_labels_by:
        return None
    by, rows = f.enum_labels_by
    return {"column": f"{column.rsplit('.', 1)[0]}.{by}", "labels": {k: dict(v) for k, v in rows}}


def snapshot() -> Dict[str, Any]:
    return {"tick": [_field(f, f.name) for f in TICK_COLUMNS],
            "process": [_field(f, f.name) for f in SF.PROCESS_FIELDS],
            "blocks": [{"name": b.name, "label": b.label, "desc": b.desc, "worker": SF.BLOCK_WORKER[b.name],
                        "quality": [f"{b.name}.q", f"{b.name}.age_s"],
                        "fields": [_field(f, f"{b.name}.{f.name}") for f in b.fields]} for b in SF.BLOCKS],
            "columns": SF.columns()}


def definitions() -> Dict[str, Any]:
    """현재값을 뺀 정의 전부(hash 대상)."""
    return {
        "params": [_param(p, None, {}) for p in P.PARAMS],
        "crossRules": [{"id": r.id, "keys": list(r.keys), "desc": r.desc} for r in P.CROSS_RULES],
        "dropped": [{"key": d.key, "reason": d.reason, "v1Source": d.v1_source, "replacedBy": d.replaced_by}
                    for d in P.DROPPED],
        "commands": [{"kind": c.kind, "label": c.label, "sources": [s.value for s in c.sources], "role": c.role,
                      "states": list(c.states), "requires": list(c.requires), "args": [_arg(a) for a in c.args],
                      "desc": c.desc, "v1": c.v1, "benchOk": c.bench_ok, "hmi": c.hmi or None} for c in C.COMMANDS],
        "preconditions": [{"id": p.id, "desc": p.desc, "reason": p.reason, "v1": p.v1} for p in C.PRECONDITIONS],
        "reasons": [{"code": r.code, "label": r.label, "category": r.category, "help": r.help or C.REASON_HELP.get(r.code, "")}
                    for r in C.ALL_REASONS],
        "plcButtons": [{"target": b.target, "addr": b.addr, "label": b.label} for b in C.PLC_BUTTONS],
        "recipe": {"processColumns": [_col(c) for c in RC.PROCESS_COLUMNS],
                   "heaterStepColumns": [_col(c) for c in RC.HEATER_STEP_COLUMNS],
                   "heaterGasColumns": [_col(c) for c in RC.HEATER_GAS_COLUMNS],
                   "delayPattern": RC.DELAY_RE.pattern,
                   "blockRow": "{process_name?, blocks: [블록...]} — 행 목록에 v1 행·딜레이 행과 섞을 수 있다"},
        "blocks": {"defs": [d.public() for d in BK.BLOCK_DEFS],
                   "groups": [{"key": k, "label": lab} for k, lab in BK.BLOCK_GROUPS],
                   "watchItems": [{"item": i, "rule": r, "label": lab} for i, r, lab in BK.WATCH_ITEMS],
                   "watchDefault": dict(BK.WATCH_DEFAULT), "watchMain": dict(BK.WATCH_MAIN),
                   "condFields": [{"field": f, "label": lab, "unit": u} for f, lab, u in BK.COND_FIELDS],
                   "condOps": [{"op": o, "label": lab} for o, lab in BK.COND_OPS],
                   "shutters": [{"target": t, "label": lab} for t, _, lab in BK.SHUTTERS],
                   "limits": {"repeatMax": BK.REPEAT_MAX, "maxDepth": BK.MAX_DEPTH, "maxExpandedSteps": BK.MAX_EXPANDED_STEPS,
                              "labelMax": BK.LABEL_MAX},
                   "rules": "값 범위·꺼진 장비만 검사 — 순서·조합 규칙 없음(사용자 결정 2026-10-20)",
                   "blockKeys": list(BK.BLOCK_KEYS),
                   "empty": "빈 값 = null 또는 공백뿐인 문자열(숫자 0 은 빈 값 아님). params 가 없거나 거짓 값([]·\"\")이면 {}. "
                            "label·stage 도 빈 값이면 없는 것과 같다(정리된 블록은 \"\")",
                   "valueRules": [r.public() for r in BK.VALUE_RULES],
                   "spDefaultKey": {str(n): k for n, k in BK.SP_DEFAULT_KEY.items()},
                   "pressureStage": "stage 를 비우면 PRESSURE_SP{sp}",
                   "heaterRampMin": BK.HEATER_RAMP_MIN,
                   "pulseEdge": {"param": "RFPULSE_PULSE_MIN_EDGE_US", "freqHz": "round(rfp_freq_khz × 1000)",
                                 "rule": "ON = duty/100/Hz×1e6 µs, OFF = (100−duty)/100/Hz×1e6 µs — 둘 다 ≥ 최소 − 1e-6. "
                                         "최소 ≤ 0 이거나 Hz ≤ 0 이면 검사 안 함"},
                   "autoEndStopLabel": BK.AUTO_END_STOP_LABEL},
        "manual": {"commands": ["MANUAL_BLOCK", "MANUAL_END"], "blockTypes": list(BK.MANUAL_BLOCK_TYPES),
                   "waitState": "MANUAL_WAIT", "label": "수동 조작 중",
                   "desc": "블록을 하나씩 실행하는 공정(5-20 ③). 첫 블록이 시작하고(수동 공정 시작과 같은 조건), 블록이 끝나면 "
                           "수동 대기. 감시·안전 규칙·정지 순서는 공정과 같다. 끝내기(MANUAL_END)·STOP·ALL STOP 은 같은 정지 "
                           "순서로 모두 끈다. state.run.manual = {label, waiting, by, source, executed, outputs}"},
        # 컨트롤러 값(W-22 E — 묶음 23). 바꾸기 범위: sp 는 0 ~ MFC_PRESSURE_FS_TORR × 1000 mTorr(max null)
        "devParams": {"read": "DEVICE_PARAMS_READ", "write": "DEVICE_PARAM_WRITE",
                      "writeItems": [{"item": k, "label": v["label"], "nos": [n for n in v["nos"] if n is not None],
                                      "unit": v["unit"], "min": v["min"], "max": v["max"], "int": v["int"]}
                                     for k, v in DP.WRITE_ITEMS.items()],
                      "spTypes": {str(k): v for k, v in DP.SP_TYPE.items()},
                      "autoModes": {str(k): v for k, v in DP.AUTO_MODE.items()},
                      "limitLabels": {str(k): v for k, v in DP.LIMIT_LABEL.items()},
                      "mfcQueries": [f"R{n}" for n in DP.MFC_QUERIES],
                      "igItems": {f"E{k}": v for k, v in DP.IG_ITEMS.items()},
                      "state": "state.devParams = {mfc: {state none·reading·waiting·done·failed, progress {done,total}, writing, "
                               "readAt, values {sp[{no,pct,mtorr,type,typeText,leadS,gainPct}], autoMode, autoModeText, valveType, "
                               "direction, rangeCode, rangeText, unitCode, unitText, fsTorr, limits[{no,label,pct}], flowFs, "
                               "flowUnits, status, version, raw, by, why, missing}, write {item,no,label,target,old,new,ok,"
                               "reason,by,at,cmd,readback}}, ig: {readAt, autoFilamentState, spProtectOff, autoFilament, "
                               "autoFilamentSpTorr, ionSp1Torr, ionSp2Torr, gasFactor, sensitivity, gaugeASpTorr, gaugeBSpTorr, raw}}"},
        "adjust": {"command": "PROCESS_ADJUST", "items": [a.public() for a in BK.ADJUST_ITEMS],
                   "settleParam": "ADJUST_SETTLE_MAX_SEC"},
        "stopSequence": {"stopFixed": True, "steps": [_seq(s) for s in SR.STOP_SEQUENCE],
                         "endStopMustKeep": list(SR.END_STOP_MUST_KEEP),
                         "autoEndStop": "펼친 마지막 스텝이 정상 끝 정지가 아니면 기본 끝 정지(뺀 단계 없음, label "
                                        f"'{BK.AUTO_END_STOP_LABEL}', auto)를 덧붙인다 — 공정이 끝날 때 파워는 항상 끈다",
                         "desc": "STOP·이상 중단은 표 전체(고정). 정상 끝 정지(end_stop 블록)만 skip 으로 1·2·3·9·10·11·12 를 뺄 수 있다"},
        "allStopSequence": {"fixed": True, "steps": [_seq(s) for s in SR.ALL_STOP_SEQUENCE]},
        "states": [{"name": s.name, "label": s.label, "kind": s.kind.value} for s in PS.STATES],
        "events": [{"kind": k, "label": lab, "data": list(keys)} for k, lab, keys in EVENTS],
        "eventKinds": [{"source": s, "kind": k, "label": EVENT_LABEL.get((s, k), lab), "level": lv,
                        "desc": EVENT_DESC.get((s, k)) or EVENT_DESC.get(("*", k)) if s in _WORKERS else EVENT_DESC.get((s, k))}
                       for s, k, lab, lv in EVENT_KINDS],
        "eventSources": [{"source": s, "label": lab} for s, lab in EVENT_SOURCES],
        "paramGroups": [{"key": k, "label": lab} for k, lab in PARAM_GROUPS],
        "snapshot": snapshot(),
    }


def content_hash(defs: Optional[Dict[str, Any]] = None) -> str:
    raw = json.dumps(defs if defs is not None else definitions(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def bundle(values: Optional[Mapping[str, Any]] = None, pending: Optional[Mapping[str, Any]] = None,
           config_version: Optional[int] = None, stored: Optional[Mapping[str, Any]] = None,
           overridden=()) -> Dict[str, Any]:
    """웹으로 보내는 묶음. values = 지금 실행 값(ConfigStore.values), pending = 대기 중 값(idle·restart),
    stored = 저장 값(ConfigStore.stored — 없으면 실행 값과 같다고 봄), overridden = 덮어쓰기가 정한 키."""
    d = definitions()
    h = content_hash(d)
    ov = frozenset(overridden or ())
    d["params"] = [_param(p, values, dict(pending or {}), stored, ov) for p in P.PARAMS]
    return dict({"version": EXPORT_VERSION, "hash": h, "paramSchemaHash": schema_hash(P.PARAMS, P.CROSS_RULES),
                 "configVersion": config_version, "overriddenKeys": sorted(ov)}, **d)


def bundle_for(values: Mapping[str, Any], store, config_version: Optional[int]) -> Dict[str, Any]:
    """실행 값 + ConfigStore(대기 값·저장 값·덮어쓰기 키)로 묶음 — 로컬 /api/v1/schema·hello webSchema 가 같이 쓴다."""
    if store is None:
        return bundle(values, {}, config_version)
    return bundle(values, dict(store.pending_idle, **store.pending_restart), config_version, store.stored(),
                  store.overridden())


def to_json(b: Dict[str, Any]) -> str:
    return json.dumps(b, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
