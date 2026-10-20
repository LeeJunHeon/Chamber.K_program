"""안전 규칙 표 시험 — S1~S45 모두 있음, 참조(파라미터·명령·사유·스냅샷 열·상태) 존재, STOP·ALL STOP 순서."""
import re

from v2.common.commands import Source
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import process_states as PS
from v2.chk import safety_rules as SR
from v2.chk import snapshot_fields as SF

PARAM_KEYS = set(P.by_key())
COLS = set(SF.columns())
CODES = C.reason_codes()
CMDS = C.by_kind()
STATES = set(PS.by_name())
KINDS = {"trip", "reject", "notify", "recover", "maintain", "guard"}
WHERE = re.compile(r"^(plc250|tick|command|config|worker|event:[a-z_]+|state:[A-Z_0-9]+)$")


def _check_act(a):
    c = CMDS.get(a.kind)
    assert c is not None, a.kind
    assert Source.INTERNAL in c.sources, a.kind
    for k in ("reason", "code"):
        if k in a.args:
            assert a.args[k] in CODES, (a.kind, a.args[k])


def test_ids_unique_and_s1_to_s45_present():
    ids = [r.id for r in SR.RULES]
    assert len(ids) == len(set(ids))
    base = {re.match(r"S(\d+)", i).group(1) for i in ids}
    assert {str(n) for n in range(1, 46)} <= base


def test_rule_refs_exist():
    for r in SR.RULES:
        assert r.kind in KINDS, r.id
        assert WHERE.match(r.where), (r.id, r.where)
        if r.where.startswith("state:"):
            assert r.where[6:] in STATES, r.id
        assert set(r.params) <= PARAM_KEYS, (r.id, set(r.params) - PARAM_KEYS)
        assert set(r.values) <= COLS, (r.id, set(r.values) - COLS)
        for s in r.states:
            assert s in ("*", "*ACTIVE") or s in STATES, (r.id, s)
        if r.duration not in ("0", "formula") and re.fullmatch(r"[A-Z][A-Z0-9_]+", r.duration):
            assert r.duration in PARAM_KEYS, (r.id, r.duration)
        for a in r.actions:
            _check_act(a)
        if r.kind in ("trip", "reject", "notify", "recover"):
            assert r.actions, r.id
        assert r.v1, r.id


def test_trip_rules_abort_or_off():
    off = {"PROCESS_ABORT", "HEATER_RECIPE_ABORT", "DC_SAFE_OFF", "RF_SAFE_OFF", "RFP_SAFE_OFF", "HEATER_FORCE_OFF",
           "HEATER_CLAMP_MV", "GAS_FLOW_OFF", "GAS_VALVES_CLOSE", "HEATER_PUSH_CONFIG", "IG_FILAMENT_OFF"}
    for r in SR.RULES:
        if r.kind == "trip":
            assert {a.kind for a in r.actions} & off, r.id


def test_state_timeouts_match_rules():
    """상태 기계에서 중단 사유가 있는 상태 한도는 규칙 표에도 있다."""
    rule_codes = {a.args.get("reason") for r in SR.RULES for a in r.actions if a.kind == "PROCESS_ABORT"}
    for s in PS.STATES:
        code = PS.timeout_code(s.name)
        if code and code not in ("MFC_CMD_FAILED",):
            assert code in rule_codes, (s.name, code)


def test_stop_sequence():
    nos = [s.no for s in SR.STOP_SEQUENCE]
    assert nos == sorted(nos) and len(nos) == len(set(nos))
    kinds = [s.act.kind for s in SR.STOP_SEQUENCE]
    # v1 순서: MS 닫기 → 히터 OFF → 전원 OFF(펄스 먼저, RF 램프다운) → 유량 OFF → 배기 → 건 셔터 → 차단밸브
    assert kinds.index("HEATER_FORCE_OFF") < kinds.index("DC_SAFE_OFF")
    assert kinds.index("RFP_SAFE_OFF") < kinds.index("RF_RAMP_DOWN") < kinds.index("RF_SAFE_OFF")
    assert kinds.index("RF_SAFE_OFF") < kinds.index("GAS_FLOW_OFF") < kinds.index("GAS_EXHAUST") < kinds.index("GAS_VALVES_CLOSE")
    # 예외가 나도 반드시: 가스 차단·히터 OFF·전원 OFF
    must = {s.act.kind for s in SR.STOP_SEQUENCE if s.must}
    assert {"HEATER_FORCE_OFF", "DC_SAFE_OFF", "RFP_SAFE_OFF", "RF_SAFE_OFF", "GAS_FLOW_OFF", "GAS_VALVES_CLOSE"} <= must
    for s in SR.STOP_SEQUENCE:
        _check_act(s.act)
        assert not s.timeout_param or s.timeout_param in PARAM_KEYS


def test_all_stop_sequence():
    kinds = [s.act.kind for s in SR.ALL_STOP_SEQUENCE]
    assert kinds.index("PROCESS_ABORT") < kinds.index("PLC_OUTPUTS_OFF")      # S4 이중 중단 방지
    # v1 순서(core/process_service.py:746-812): PLC 비상 정지 → DC 직접 OFF → 공정 정지 순서 → RF Pulse
    assert kinds.index("PLC_OUTPUTS_OFF") < kinds.index("DC_SAFE_OFF") < kinds.index("GAS_FLOW_OFF") < kinds.index("RFP_SAFE_OFF")
    assert {"PLC_OUTPUTS_OFF", "DC_SAFE_OFF", "RFP_SAFE_OFF", "GAS_FLOW_OFF", "IG_FILAMENT_OFF"} <= set(kinds)
    assert "RF_RAMP_DOWN" not in kinds                                          # ALL STOP 은 RF 즉시 0
    for s in SR.ALL_STOP_SEQUENCE:
        if s.act.kind != "AWAIT":
            _check_act(s.act)
        assert not s.timeout_param or s.timeout_param in PARAM_KEYS
    assert "ALL_STOP_KEEP_PUMPS" not in P.by_key()


def test_start_guards_exist():
    ids = SR.by_id()
    for g in SR.START_GUARDS:
        assert ids[g].kind == "reject"


def test_all_stop_turns_everything_off():
    """사용자 결정 2026-10-20: ALL STOP = 말 그대로 전부(v1 과 같게) — 펌프 유지 목록·파라미터 없음."""
    assert not hasattr(SR, "ALL_STOP_KEEP_COILS")
    step = [s for s in SR.ALL_STOP_SEQUENCE if s.act.kind == "PLC_OUTPUTS_OFF"][0]
    assert "펌프·밸브 포함" in step.desc and "D00040=0" in step.desc


def test_s46_on_hold():
    r = SR.by_id()["S46"]
    assert r.on_hold and r.pending_confirm and "10 s 이상" in r.needs
    assert [x.id for x in SR.RULES if x.on_hold] == ["S46"]
