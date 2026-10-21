"""웹 설정 내보내기 묶음(사용자 지시 5-4 ④) — 웹이 이것 하나로 설정·레시피·블록·공정 중 수정 화면을 그린다.

내용(정의)이 바뀌면 PINNED 해시가 달라져 이 시험이 실패한다 — web_schema.EXPORT_VERSION 을 올리고 PINNED 를 새 값으로.
"""
import json

from v2.common.paramdef import defaults, schema_hash
from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import safety_rules as SR
from v2.chk import web_schema as W

PINNED = (1, "8df8279804e7d9ddcea4c8c0c673f7bde11f5b6f4d10b2e4c237c747a12e847e")
TOP = ["version", "hash", "paramSchemaHash", "configVersion", "params", "crossRules", "dropped", "commands", "preconditions",
       "reasons", "plcButtons", "recipe", "blocks", "adjust", "stopSequence", "allStopSequence", "states", "events"]


def test_version_and_hash_pinned():
    assert (W.EXPORT_VERSION, W.content_hash()) == PINNED, (
        "웹 내보내기 정의가 바뀌었다 — EXPORT_VERSION 을 올리고 PINNED 를 (새 버전, 새 해시)로: "
        f"{W.content_hash()}")


def test_bundle_shape_and_values():
    vals = dict(defaults(P.PARAMS), SP4_MTORR=25.0)
    b = W.bundle(vals, {"FLOW_ERROR_SEC": 9}, 7)
    raw = W.to_json(b)
    assert json.loads(raw)["hash"] == b["hash"] == W.content_hash()       # 현재값은 해시에 들어가지 않는다
    assert list(b) == TOP and b["configVersion"] == 7
    assert b["paramSchemaHash"] == schema_hash(P.PARAMS, P.CROSS_RULES)
    by = {p["key"]: p for p in b["params"]}
    assert set(by) == {p.key for p in P.PARAMS}
    assert by["SP4_MTORR"]["current"] == 25.0 and by["SP4_MTORR"]["default"] == 30.0 and by["FLOW_ERROR_SEC"]["pending"] == 9
    for p in P.PARAMS:
        d = by[p.key]
        assert {"label", "group", "unit", "type", "min", "max", "default", "current", "safety", "apply", "v1Source", "desc"} <= set(d)
        if p.scope.value == "secret":
            assert d["default"] is None and d["current"] is None and d["pending"] is None
    assert {c["kind"] for c in b["commands"]} == {c.kind for c in C.COMMANDS} >= {"PROCESS_ADJUST", "CONFIG_ROLLBACK"}
    assert {r["code"] for r in b["reasons"]} == C.reason_codes()
    assert [d["type"] for d in b["blocks"]["defs"]] == [d.type for d in BK.BLOCK_DEFS]
    assert len(b["adjust"]["items"]) == len(BK.ADJUST_ITEMS)
    assert [s["no"] for s in b["stopSequence"]["steps"]] == [s.no for s in SR.STOP_SEQUENCE]
    assert b["stopSequence"]["configKey"] == "STOP_SEQUENCE_ORDER" and b["allStopSequence"]["fixed"]
    rf = [c for c in b["recipe"]["processColumns"] if c["name"] == "rf_pulse_freq"][0]
    assert rf["unit"] == "kHz"


def test_every_param_reference_resolves_from_bundle_alone():
    """웹이 묶음만으로 범위를 계산할 수 있다 — paramMin·paramMax 는 묶음 params 의 키, 계산값이 장비 쪽과 같다."""
    vals = defaults(P.PARAMS)
    b = W.bundle(vals)
    cur = {p["key"]: p["current"] for p in b["params"]}
    refs = []
    for d in b["blocks"]["defs"]:
        refs += [(p, d["type"]) for p in d["params"]]
    refs += [(a["param"], a["key"]) for a in b["adjust"]["items"]]
    refs += [(c, "recipe") for c in b["recipe"]["processColumns"]]
    for c in b["commands"]:
        refs += [(a, c["kind"]) for a in c["args"]]
    for p, where in refs:
        for k in (p.get("paramMin"), p.get("paramMax")):
            assert k is None or k in cur, (where, k)
    # 블록 값 범위: 웹 계산 = 장비 계산
    for d in BK.BLOCK_DEFS:
        for p in d.params:
            lo = cur[p.param_min] * p.param_scale if p.param_min else p.min
            hi = cur[p.param_max] * p.param_scale if p.param_max else p.max
            assert (lo, hi) == p.bounds(vals), (d.type, p.name)
    for ev in b["events"]:
        assert ev["kind"] and isinstance(ev["data"], list)
