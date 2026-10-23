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

PINNED = (7, "2de1dfe978373a982095532e2e0fb43190fbbfe944da9eba52901a0b7586d7ec")
TOP = ["version", "hash", "paramSchemaHash", "configVersion", "params", "crossRules", "dropped", "commands", "preconditions",
       "reasons", "plcButtons", "recipe", "blocks", "adjust", "stopSequence", "allStopSequence", "states", "events", "snapshot"]


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
    assert {k for k, d in by.items() if d["webLocked"]} == {"RUN_MODE", "EQUIPMENT_ID", "UPLINK_IN_SIM"}   # 웹에서 못 바꾸는 키
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
    ss = b["stopSequence"]
    assert ss["stopFixed"] is True and "configKey" not in ss and ss["endStopMustKeep"] == [4, 5, 6, 7, 8, 13]
    assert "자동 끝 정지" in ss["autoEndStop"] and b["allStopSequence"]["fixed"]
    assert "STOP_SEQUENCE_ORDER" not in by
    codes = {r["code"]: r for r in b["reasons"]}
    assert {"AUTO_END_STOP", "HEATER_HANDOVER", "OUTPUTS_CLEANUP", "CLEANUP_RUNNING"} <= set(codes)
    assert "뺀 단계" in codes["OUTPUTS_LEFT_ON"]["label"]
    gi = [d for d in b["blocks"]["defs"] if d["type"] == "gas_intro"][0]["params"]
    assert {p["name"]: p["paramMax"] for p in gi if p["name"].endswith("_flow")} == {"ar_flow": "MFC_AR_FS_SCCM",
                                                                                     "o2_flow": "MFC_O2_FS_SCCM"}
    es = [d for d in b["blocks"]["defs"] if d["type"] == "end_stop"][0]["params"][0]
    assert "뺄 수 없음" in es["desc"]
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


def test_snapshot_definitions_match_hello_columns(tmp_path):
    """묶음 snapshot 의 열 집합 = hello.snapshotColumns(같은 순서) — 웹이 라벨·단위·형식·그래프를 열 이름으로 찾는다."""
    from v2.common.clock import VirtualClock
    from v2.chk import snapshot_fields as SF
    from v2.chk.sim.rig import ChkRig
    sn = W.bundle(defaults(P.PARAMS))["snapshot"]
    cols = [f["column"] for f in sn["tick"]] + [f["column"] for f in sn["process"]]
    for b in sn["blocks"]:
        cols += b["quality"]
        for f in b["fields"]:
            cols.append(f["column"])
            if f["ownQuality"]:
                cols += [f["column"] + ".q", f["column"] + ".age_s"]
    rig = ChkRig(VirtualClock(start_wall=1_760_000_000.0), tmp_path)
    assert cols == sn["columns"] == rig.hub.hello_data()["snapshotColumns"]
    by = {f["column"]: f for b in sn["blocks"] for f in b["fields"]}
    assert by["ig.ion_torr"]["fmt"] == ".2E" and by["heater.pv"]["unit"] == "°C" and by["heater.pv"]["fmt"] == ".1f"
    assert by["heater.mv"]["fmt"] == "d" and by["mfc.ar_sccm"]["ownQuality"] and by["rf.ref_w"]["summary"] == "max"
    assert {b["name"]: b["worker"] for b in sn["blocks"]} == SF.BLOCK_WORKER
    assert all("source" not in f for b in sn["blocks"] for f in b["fields"])
    assert {f["type"] for f in by.values()} <= {"float", "int", "bool", "str"}


def test_plc_button_role_operator():
    """PLC 버튼은 장비 담당자도(사용자 결정 2026-10-22) — 웹은 묶음의 role 로 권한을 정한다(관리자 + 그 장비 담당자)."""
    b = W.bundle()
    roles = {c["kind"]: c["role"] for c in b["commands"]}
    assert roles["PLC_BUTTON"] == "operator" and roles["CONFIG_APPLY"] == roles["CONFIG_ROLLBACK"] == "admin"
