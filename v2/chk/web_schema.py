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
  paramGroups   설정(params[].group) 묶음 key → 화면 이름
  snapshot      1초 스냅샷 열 정의(웹 화면 라벨·단위·형식·그래프) — tick·process 필드, 장비 블록(이름·라벨·worker·
                품질 열 <블록>.q·<블록>.age_s) → 필드(column 전체 이름·형식 fmt 는 파이썬 format spec 그대로 .1f·.2E·d).
                ownQuality 필드는 <column>.q·<column>.age_s 품질 열을 더 갖는다. 열 집합 = hello.snapshotColumns.
                stateColumn(값 열) = 같은 블록 상태 열 — 화면 규칙: 값이 비어 있고 그 상태가 ok 가 아니면 상태 열의
                enumLabels 이름을 값 자리에 보인다(장비 종류를 따로 보지 않는다)
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
from v2.chk import params as P
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk import safety_rules as SR
from v2.chk import snapshot_fields as SF

EXPORT_VERSION = 13

# 웹이 받는 공정 기록 이벤트(ProcEvent kind → data 키) — 웹 단계에서 그대로 event·run_* 메시지로 올린다
EVENTS: tuple = (
    ("run_start", "행 시작", ("name", "params", "run_id", "blocks")),
    ("steps", "펼친 스텝 목록(골든 비교 단위)", ("steps",)),
    ("block", "블록 시작·끝(phase start/end, result ok/failed/aborted/error, reason)",
     ("no", "type", "label", "stage", "params", "phase", "result", "reason", "index", "total", "elapsed_s", "t", "run_id")),
    ("adjust", "공정 중 수정(PROCESS_ADJUST) — result rejected/scheduled/applied → confirmed/failed",
     ("device", "item", "old", "new", "source", "by", "result", "reason", "t", "run_id", "state", "block", "level")),
    ("state", "상태 전이", ("from", "to", "t")),
    ("warn", "경고(OUTPUTS_LEFT_ON·COND_WAIT_TIMEOUT·ADJUST_SETTLE_TIMEOUT …)", ()),
    ("reject", "적재·시작 거부(블록 검사 오류 목록)", ("errors",)),
    ("plc_manual", "공정 중 PLC 버튼(PLC_MANUAL_IN_PROCESS)", ("target", "on", "source", "by", "t", "run_id", "state", "level")),
    ("stop_rest", "정상 끝 정지 중 STOP·이상 중단 — 뺀 단계를 이어서 실행(남김 취소)", ("steps", "level")),
    ("skip", "블록 건너뛰기(PROCESS_SKIP)", ("by", "source", "no", "type", "label", "kind", "cleaned", "pausedSec",
                                            "run_id", "revision", "t", "level")),
    ("pause", "일시정지(PROCESS_PAUSE)", ("by", "source", "no", "type", "label", "kind", "remainS", "cleaned",
                                       "run_id", "revision", "t", "level")),
    ("resume", "재개(PROCESS_RESUME)", ("by", "source", "no", "type", "label", "kind", "pausedSec", "cleaned",
                                      "run_id", "revision", "t", "level")),
    ("edit", "순서 고치기(PROCESS_EDIT) — 판마다 전체 목록", ("by", "source", "summary", "from", "count", "blocks", "warnings",
                                                         "insts", "run_id", "revision", "t", "level")),
    ("pause_long", "일시정지 오래(PAUSE_LONG — 한도 5분 전)", ("no", "pausedSec", "maxSec", "level")),
    ("run_end", "행 끝 — summary: 실행된 순서(executed)·마지막 판·건너뛴/멈춘 횟수", ("name", "status", "reason", "summary")),
    ("list_done", "레시피 리스트 끝", ()),
    ("list_cancel", "레시피 리스트 취소", ()),
)


# 설정 묶음(params[].group) → 화면 이름
PARAM_GROUPS: tuple = (
    ("install", "설치·장비"), ("secret", "비밀값"), ("tick", "1초 읽기"), ("comm", "통신"), ("plc", "PLC"),
    ("mfc", "MFC"), ("dc", "DC Power"), ("rf", "RF Power"), ("stop", "정지 순서"), ("rfpulse", "RF Pulse"),
    ("ig", "이온 게이지"), ("heater", "히터"), ("heater_plc", "히터 PLC 설정"), ("heater_ramp", "히터 승온"),
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
    ("process", "공정"), ("safety", "안전 규칙"), ("heater", "히터"), ("plc", "PLC"), ("mfc", "MFC"), ("dc", "DC Power"),
    ("rfp", "RF Pulse"), ("rfpulse", "RF Pulse"), ("ig", "이온 게이지"),
)
_WORKERS = ("plc", "mfc", "dc", "rfpulse", "ig")
EVENT_KINDS: tuple = (
    ("process", "run_start", "공정 시작", _M), ("process", "run_end", "공정 끝", _M),
    ("process", "state", "상태 바뀜", _M), ("process", "block", "블록", _M),
    ("process", "steps", "펼친 스텝 목록", _D), ("process", "step", "스텝", _D),
    ("process", "warn", "공정 경고", _M), ("process", "adjust", "공정 중 수정", _M),
    ("process", "skip", "블록 건너뜀", _M), ("process", "pause", "일시정지", _M), ("process", "resume", "재개", _M),
    ("process", "edit", "순서 고침", _M), ("process", "pause_long", "일시정지 오래 — 곧 정지", _M),
    ("process", "plc_manual", "공정 중 PLC 버튼", _M), ("process", "list_done", "레시피 끝", _M),
    ("process", "list_cancel", "레시피 취소", _M), ("process", "stop_rest", "남은 정지 단계 실행", _M),
    ("process", "auto_end_stop", "자동 끝 정지 덧붙임", _M), ("process", "reject", "시작 거부", _M),
    ("process", "base_reached", "베이스 압력 도달", _M), ("process", "heater_handover", "히터 켠 채 넘김", _M),
    ("process", "cleanup_start", "남은 출력 정리 시작", _M), ("process", "cleanup", "남은 출력 정리 끝", _M),
    ("process", "message", "장비 알림", _D),
    ("safety", "fired", "안전 규칙 발동", _M), ("safety", "released", "안전 규칙 해제", _M),
    ("safety", "blip", "안전 규칙 순간 발동", _D), ("safety", "also", "이미 정지 중 — 추가 발동", _D),
    ("safety", "abort", "공정 중단", _M), ("safety", "seq_start", "정지 순서 시작", _M),
    ("safety", "seq_done", "정지 순서 끝", _M), ("safety", "step", "정지 단계 진행", _D),
    ("safety", "error", "정지 단계 오류", _M), ("safety", "bench_skip", "bench — 보내지 않음", _M),
    ("heater", "reached", "히터 목표 도달", _M), ("heater", "engaged", "유지 모드 진입", _M),
    ("heater", "give_up", "유지 모드 포기", _M), ("heater", "alert", "히터 경고", _M),
    ("heater", "saturated", "히터 출력 포화", _M), ("heater", "gas", "히터 가스", _M),
    ("heater", "recipe_step", "히터 레시피 스텝", _M), ("heater", "recipe_finished", "히터 레시피 끝", _M),
    ("heater", "message", "히터 알림", _D),
    ("plc", "link_up", "장비 연결 복구", _M), ("plc", "link_down", "장비 연결 끊김", _M),
    ("plc", "plc_restarted", "PLC 재기동 감지", _M), ("plc", "marker_written", "재기동 마커 씀", _D),
    ("plc", "heater_config_ok", "히터 설정 확인", _D), ("plc", "heater_config_mismatch", "히터 설정 다름", _M),
    ("plc", "heater_residual_run", "히터 RUN 남음", _M), ("plc", "heater_off_pending", "히터 끄기 보류", _M),
    ("plc", "heater_off_applied", "히터 끄기 적용", _M), ("plc", "heater_off_retry", "히터 끄기 다시", _D),
    ("plc", "heater_wd_delay", "워치독 쓰기 지연", _D), ("plc", "heater_wd_recovered", "워치독 쓰기 복구", _D),
    ("plc", "blackbox", "블랙박스 기록", _D),
    ("mfc", "fs", "풀스케일", _D), ("mfc", "fs_not_100", "풀스케일 100 아님", _M), ("mfc", "scale", "눈금", _D),
    ("mfc", "verify_ok", "되읽기 확인", _D), ("mfc", "verify_mismatch", "되읽기 다름", _M),
    ("mfc", "sp_readback", "설정점 되읽기", _D), ("mfc", "remote_state", "원격 상태", _D),
    ("mfc", "read_plan", "읽기 계획", _D),
    ("dc", "identity", "장비 식별", _D), ("dc", "syst_err", "자기 진단", _D), ("dc", "last_state", "마지막 상태", _D),
    ("dc", "protection", "보호 설정", _D), ("dc", "trip", "보호 트립", _M), ("dc", "out_check", "출력 확인", _D),
    ("dc", "off_confirmed", "꺼짐 확인", _D), ("dc", "off_unconfirmed", "꺼짐 미확인", _M),
    ("dc", "off_unverified", "꺼짐 확인 생략", _D),
    ("rfp", "identity", "장비 식별", _D), ("rfp", "auto_off", "자동 꺼짐", _M), ("rfp", "auto_off_read", "자동 꺼짐 읽기", _D),
    ("rfp", "csr_recovery", "명령 응답 복구", _D), ("rfp", "config_readback", "설정 되읽기", _D),
    ("rfp", "read_plan", "읽기 계획", _D), ("rfp", "retry", "다시 시도", _D), ("rfp", "adjusted", "공정 중 수정 적용", _D),
    ("rfp", "off_confirmed", "꺼짐 확인", _D), ("rfp", "off_unconfirmed", "꺼짐 미확인", _M),
    ("ig", "identity_ok", "장비 식별", _D), ("ig", "filament_sent", "필라멘트 명령", _D),
    ("ig", "filament_result", "필라멘트 결과", _M), ("ig", "parse_error", "응답 해석 실패", _D),
) + tuple(x for w in _WORKERS for x in (
    (w, "connected", "장비 연결됨", _D), (w, "connect_failed", "연결 실패", _D), (w, "connect_retry", "연결 다시 시도", _D),
    (w, "disconnected", "장비 연결 끊김", _M), (w, "long_outage", "장비 연결 오래 끊김", _M), (w, "timeout", "응답 없음", _D),
    (w, "COMMAND_BLOCKED", "명령 막힘", _M),
))


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
            "enumLabels": dict(f.enum_labels) if f.enum_labels else None,
            "stateColumn": f"{column.rsplit('.', 1)[0]}.{f.state}" if f.state else None}


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
                      "desc": c.desc, "v1": c.v1, "benchOk": c.bench_ok} for c in C.COMMANDS],
        "preconditions": [{"id": p.id, "desc": p.desc, "reason": p.reason, "v1": p.v1} for p in C.PRECONDITIONS],
        "reasons": [{"code": r.code, "label": r.label, "category": r.category} for r in C.ALL_REASONS],
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
        "eventKinds": [{"source": s, "kind": k, "label": lab, "level": lv} for s, k, lab, lv in EVENT_KINDS],
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
