"""웹 설정 내보내기(사용자 지시 5-4 ④) — 웹이 이 묶음 하나로 설정·레시피·블록 화면을 그린다.

묶음(JSON 하나, bundle()):
  version · hash(정의 내용의 sha256 — 현재값 제외) · paramSchemaHash(paramdef.schema_hash — CONFIG_APPLY 의 schemaHash)
  params        파라미터 정의(키·이름·묶음·단위·형식·범위·허용값·기본값·현재값·안전 등급·적용 시점·역할·v1 근거·설명)
  crossRules    키 사이 규칙 설명 · dropped v2 에서 뺀 v1 키
  commands      명령(종류·경로·역할·허용 상태·조건·인자) · preconditions · reasons(사유 코드) · plcButtons
  recipe        행 레시피 열(rf_pulse_freq 는 kHz) · 히터 레시피 열
  blocks        블록 정의(종류·값·단위·범위·기본값·설명) + 감시 항목·조건 값·한도
  adjust        공정 중 수정 항목(장비 → 세부, 형식·범위·적용 방식·판정)
  stopSequence  STOP·이상 중단·행 끝 정지 단계(고정 — 정상 끝 정지만 end_stop skip, 파워 단계 제외) · allStopSequence(고정)
  states        공정 상태 이름 · events 웹이 받는 공정 기록 이벤트(종류·data 키)
범위가 파라미터 값에 묶인 항목(paramMin·paramMax·paramScale)은 웹이 params 의 현재값으로 계산한다.
hash 는 정의만으로 계산한다(현재값·대기값 제외) — 내용이 바뀌면 시험(test_web_schema)이 알고 EXPORT_VERSION 을 올리게 한다.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Optional

from v2.common.paramdef import Scope, schema_hash
from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk import safety_rules as SR

EXPORT_VERSION = 3

# 웹이 받는 공정 기록 이벤트(ProcEvent kind → data 키) — 웹 단계에서 그대로 event·run_* 메시지로 올린다
EVENTS: tuple = (
    ("run_start", "행 시작", ("name", "params", "run_id", "blocks")),
    ("steps", "펼친 스텝 목록(골든 비교 단위)", ("steps",)),
    ("block", "블록 시작·끝(phase start/end, result ok/failed/aborted/error, reason)",
     ("no", "type", "label", "stage", "params", "phase", "result", "reason", "index", "total", "elapsed_s", "t", "run_id")),
    ("adjust", "공정 중 수정(PROCESS_ADJUST) — result rejected/scheduled/applied → confirmed/failed",
     ("device", "item", "old", "new", "source", "result", "reason", "t", "run_id", "state", "block", "level")),
    ("state", "상태 전이", ("from", "to", "t")),
    ("warn", "경고(OUTPUTS_LEFT_ON·COND_WAIT_TIMEOUT·ADJUST_SETTLE_TIMEOUT …)", ()),
    ("reject", "적재·시작 거부(블록 검사 오류 목록)", ("errors",)),
    ("plc_manual", "공정 중 PLC 버튼(PLC_MANUAL_IN_PROCESS)", ("target", "on", "source", "t", "run_id", "state", "level")),
    ("stop_rest", "정상 끝 정지 중 STOP·이상 중단 — 뺀 단계를 이어서 실행(남김 취소)", ("steps", "level")),
    ("run_end", "행 끝", ("name", "status", "reason")),
    ("list_done", "레시피 리스트 끝", ()),
    ("list_cancel", "레시피 리스트 취소", ()),
)


def _param(p, values: Optional[Mapping[str, Any]], pending: Mapping[str, Any]) -> dict:
    secret = p.scope is Scope.SECRET
    return {"key": p.key, "label": p.label, "group": p.group, "unit": p.unit, "type": p.type.value,
            "min": p.min, "max": p.max, "enum": list(p.enum) if p.enum else None, "pattern": p.pattern,
            "default": None if secret else p.default,
            "current": None if (secret or values is None) else values.get(p.key),
            "pending": None if secret else pending.get(p.key),
            "safety": p.safety.value, "apply": p.apply.value, "scope": p.scope.value, "role": p.role.value,
            "v1Key": p.v1_key, "v1Source": p.v1_source, "v1Default": p.v1_default, "pendingConfirm": p.pending_confirm,
            "desc": p.desc, "note": p.note}


def _arg(a) -> dict:
    return {"name": a.name, "type": a.type, "required": a.required, "min": a.min, "max": a.max,
            "paramMin": a.param_min, "paramMax": a.param_max, "enum": list(a.enum) if a.enum else None, "desc": a.desc}


def _col(c) -> dict:
    return {"name": c.name, "type": c.type, "unit": c.unit, "required": c.required, "min": c.min, "max": c.max,
            "paramMin": c.param_min, "paramMax": c.param_max, "minExclusive": c.min_exclusive, "v1Name": c.v1_name,
            "desc": c.desc}


def _seq(s) -> dict:
    return {"no": s.no, "act": s.act.kind, "args": dict(s.act.args), "timeoutParam": s.timeout_param or None,
            "must": s.must, "when": s.when, "desc": s.desc, "nowait": s.nowait}


def definitions() -> Dict[str, Any]:
    """현재값을 뺀 정의 전부(hash 대상)."""
    return {
        "params": [_param(p, None, {}) for p in P.PARAMS],
        "crossRules": [{"id": r.id, "keys": list(r.keys), "desc": r.desc} for r in P.CROSS_RULES],
        "dropped": [{"key": d.key, "reason": d.reason, "v1Source": d.v1_source, "replacedBy": d.replaced_by}
                    for d in P.DROPPED],
        "commands": [{"kind": c.kind, "label": c.label, "sources": [s.value for s in c.sources], "role": c.role,
                      "states": list(c.states), "requires": list(c.requires), "args": [_arg(a) for a in c.args],
                      "desc": c.desc, "v1": c.v1} for c in C.COMMANDS],
        "preconditions": [{"id": p.id, "desc": p.desc, "reason": p.reason, "v1": p.v1} for p in C.PRECONDITIONS],
        "reasons": [{"code": r.code, "label": r.label, "category": r.category} for r in C.ALL_REASONS],
        "plcButtons": [{"target": b.target, "addr": b.addr, "label": b.label} for b in C.PLC_BUTTONS],
        "recipe": {"processColumns": [_col(c) for c in RC.PROCESS_COLUMNS],
                   "heaterStepColumns": [_col(c) for c in RC.HEATER_STEP_COLUMNS],
                   "heaterGasColumns": [_col(c) for c in RC.HEATER_GAS_COLUMNS],
                   "delayPattern": RC.DELAY_RE.pattern,
                   "blockRow": "{process_name?, blocks: [블록...]} — 행 목록에 v1 행·딜레이 행과 섞을 수 있다"},
        "blocks": {"defs": [d.public() for d in BK.BLOCK_DEFS],
                   "watchItems": [{"item": i, "rule": r, "label": lab} for i, r, lab in BK.WATCH_ITEMS],
                   "watchDefault": dict(BK.WATCH_DEFAULT), "watchMain": dict(BK.WATCH_MAIN),
                   "condFields": [{"field": f, "label": lab, "unit": u} for f, lab, u in BK.COND_FIELDS],
                   "condOps": [{"op": o, "label": lab} for o, lab in BK.COND_OPS],
                   "shutters": [{"target": t, "label": lab} for t, _, lab in BK.SHUTTERS],
                   "limits": {"repeatMax": BK.REPEAT_MAX, "maxDepth": BK.MAX_DEPTH, "maxExpandedSteps": BK.MAX_EXPANDED_STEPS,
                              "labelMax": BK.LABEL_MAX},
                   "rules": "값 범위·꺼진 장비만 검사 — 순서·조합 규칙 없음(사용자 결정 2026-10-20)"},
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
    }


def content_hash(defs: Optional[Dict[str, Any]] = None) -> str:
    raw = json.dumps(defs if defs is not None else definitions(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def bundle(values: Optional[Mapping[str, Any]] = None, pending: Optional[Mapping[str, Any]] = None,
           config_version: Optional[int] = None) -> Dict[str, Any]:
    """웹으로 보내는 묶음. values = 지금 실행 값(ConfigStore.values), pending = 대기 중 값(idle·restart)."""
    d = definitions()
    h = content_hash(d)
    d["params"] = [_param(p, values, dict(pending or {})) for p in P.PARAMS]
    return dict({"version": EXPORT_VERSION, "hash": h, "paramSchemaHash": schema_hash(P.PARAMS, P.CROSS_RULES),
                 "configVersion": config_version}, **d)


def to_json(b: Dict[str, Any]) -> str:
    return json.dumps(b, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
