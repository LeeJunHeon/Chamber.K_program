"""스냅샷·명령·공정 상태·레시피·메시지 명세 시험 — 이름 중복 없음, 참조 존재, 허용 표 동작."""
import pytest

from v2.common.commands import COMMON_REASONS, Source
from v2.common.messages import ENVELOPE, MESSAGES
from v2.common.paramdef import defaults
from v2.common.snapshot import Quality
from v2.chk import commands as C
from v2.chk import devices as D
from v2.chk import params as P
from v2.chk import process_states as PS
from v2.chk import recipe as R
from v2.chk import snapshot_fields as SF

PARAM_KEYS = set(P.by_key())
CFG = defaults(P.PARAMS)


# ───────────── 스냅샷 ─────────────
def test_snapshot_columns_unique():
    cols = SF.columns()
    assert len(cols) == len(set(cols))
    assert cols[:4] == ["ts", "seq", "jitter_ms", "skipped"]
    for b in SF.BLOCKS:
        names = [f.name for f in b.fields]
        assert len(names) == len(set(names)), b.name
        assert "." not in "".join(names)


def test_snapshot_plc_sources_exist():
    plc_names = {a.name for a in D.PLC_ADDRS}
    for b in SF.BLOCKS:
        for f in b.fields:
            if f.source.startswith("plc:"):
                assert f.source[4:].split()[0] in plc_names, (b.name, f.name, f.source)


def test_snapshot_blocks_map_to_devices():
    keys = {d.key for d in D.DEVICES}
    assert set(SF.BLOCK_WORKER) == {b.name for b in SF.BLOCKS}
    assert set(SF.BLOCK_WORKER.values()) <= keys


def test_quality_values():
    assert {q.value for q in Quality} == {"ok", "late", "stale", "fail", "off", "suppressed"}


# ───────────── 사유 코드·명령 ─────────────
def test_reason_codes_unique():
    codes = [r.code for r in C.ALL_REASONS]
    assert len(codes) == len(set(codes))
    assert all(r.category in ("reject", "abort", "fail", "info") for r in C.ALL_REASONS)


def test_command_table_refs():
    kinds = [c.kind for c in C.COMMANDS]
    assert len(kinds) == len(set(kinds))
    pre_ids = {p.id for p in C.PRECONDITIONS}
    assert len(pre_ids) == len(C.PRECONDITIONS)
    states = set(PS.by_name())
    for c in C.COMMANDS:
        assert set(c.requires) <= pre_ids, c.kind
        for s in c.states:
            assert s == C.ANY_STATE or s in states, (c.kind, s)
        for a in c.args:
            for k in (a.param_min, a.param_max):
                assert k is None or k in PARAM_KEYS, (c.kind, k)
        assert c.role in ("admin", "operator", "system")
        if Source.INTERNAL in c.sources and len(c.sources) == 1:
            assert c.role == "system"
    for p in C.PRECONDITIONS:
        assert p.reason in C.reason_codes(), p.id


def test_hmi_only_stop_commands():
    hmi = {c.kind for c in C.COMMANDS if Source.HMI in c.sources}
    assert hmi == {"PROCESS_STOP", "ALL_STOP", "BUZZ_STOP", "HEATER_STOP"}


def test_plc_buttons_map_to_addrs():
    names = {a.name for a in D.PLC_ADDRS if "W" in a.access}
    for b in C.PLC_BUTTONS:
        assert b.addr in names, b.target
    locked = {b.target for b in C.PLC_BUTTONS if not b.remote_in_process}
    assert locked == {"rotary", "mv", "vent", "turbo", "door_up", "door_down"}
    assert all(b.pending_confirm for b in C.PLC_BUTTONS if not b.remote_in_process)


@pytest.mark.parametrize("kind,src,state,args,expect", [
    ("PLC_BUTTON", Source.WEB, "MAIN", {"target": "mv", "on": False}, "REMOTE_LOCKED_IN_PROCESS"),
    ("PLC_BUTTON", Source.WEB, "POWER_WAIT", {"target": "vent", "on": True}, "REMOTE_LOCKED_IN_PROCESS"),
    ("PLC_BUTTON", Source.WEB, "STOPPING", {"target": "door_up", "on": True}, "REMOTE_LOCKED_IN_PROCESS"),
    ("PLC_BUTTON", Source.WEB, "IDLE", {"target": "mv", "on": True}, None),
    ("PLC_BUTTON", Source.WEB, "MAIN", {"target": "s1", "on": False}, None),
    ("PLC_BUTTON", Source.WEB, "IDLE", {"target": "nope", "on": True}, "BAD_ARGS"),
    ("PLC_BUTTON", Source.HMI, "IDLE", {"target": "mv", "on": True}, "SOURCE_NOT_ALLOWED"),
    ("ALL_STOP", Source.HMI, "MAIN", {}, None),
    ("ALL_STOP", Source.HMI, "IDLE", {}, None),
    ("PROCESS_STOP", Source.HMI, "BASE_WAIT", {}, None),
    ("PROCESS_STOP", Source.WEB, "IDLE", {}, "STATE_NOT_ALLOWED"),
    ("PROCESS_START", Source.WEB, "MAIN", {}, "STATE_NOT_ALLOWED"),
    ("PROCESS_START", Source.HMI, "IDLE", {}, "SOURCE_NOT_ALLOWED"),
    ("RECIPE_START", Source.WEB, "IDLE", {}, "STATE_NOT_ALLOWED"),
    ("DC_SAFE_OFF", Source.WEB, "IDLE", {}, "SOURCE_NOT_ALLOWED"),
    ("NOPE", Source.WEB, "IDLE", {}, "UNKNOWN_COMMAND"),
])
def test_check_command(kind, src, state, args, expect):
    assert C.check_command(kind, src, state, args) == expect


def test_check_command_codes_registered():
    codes = C.reason_codes()
    for x in ("UNKNOWN_COMMAND", "SOURCE_NOT_ALLOWED", "STATE_NOT_ALLOWED", "BAD_ARGS", "REMOTE_LOCKED_IN_PROCESS"):
        assert x in codes


# ───────────── 공정 상태 기계 ─────────────
def test_states_and_transitions():
    names = [s.name for s in PS.STATES]
    assert len(names) == len(set(names))
    codes = C.reason_codes()
    for s in PS.STATES:
        assert set(s.keys) <= PARAM_KEYS, (s.name, set(s.keys) - PARAM_KEYS)
        if s.timeout == "param":
            assert s.keys
        if s.timeout == "formula":
            assert s.name in PS.FORMULAS
        if s.on_timeout:
            assert s.on_timeout == "proceed" or (s.on_timeout.startswith("abort:") and PS.timeout_code(s.name) in codes)
    for st in PS.STEP_SEQUENCE:
        assert PS.by_name()[st].kind is PS.Kind.ACTIVE
    special = {"*ACTIVE", "*NEXT", "*ON_TIMEOUT"}
    for t in PS.TRANSITIONS:
        assert t.src in names or t.src in special
        assert t.dst in names or t.dst in special


def test_every_active_state_can_stop():
    stop_src = {t.src for t in PS.TRANSITIONS if t.event in (PS.Event.STOP, PS.Event.ALL_STOP, PS.Event.ABORT)}
    assert "*ACTIVE" in stop_src


def test_end_states_settle():
    for end in ("DONE", "ABORTED"):
        assert any(t.src == end and t.event is PS.Event.SETTLED for t in PS.TRANSITIONS)


# ───────────── 레시피 ─────────────
def test_recipe_column_refs():
    for cols in (R.PROCESS_COLUMNS, R.HEATER_STEP_COLUMNS, R.HEATER_GAS_COLUMNS):
        names = [c.name for c in cols]
        assert len(names) == len(set(names))
        for c in cols:
            for k in (c.param_min, c.param_max):
                assert k is None or k in PARAM_KEYS, (c.name, k)
            if c.required.startswith("if:"):
                assert c.required[3:] in names


GOOD = {
    "process_name": "CeO2 #1", "ar": True, "ar_flow": 20.0, "o2": False, "base_pressure": 5e-6,
    "working_pressure": 2.0, "process_time": 30.0, "shutter_delay": 5.0, "use_dc": True, "dc_power": 250.0,
    "use_heater": True, "heater_temp": 600.0, "heater_ramp": 0, "gun1": True, "g1_target": "CeO2",
}


def test_good_row():
    row, errs, warns = R.check_process_row(GOOD, CFG)
    assert errs == [] and row["heater_ramp"] == CFG["HEATER_RAMP_RATE_C_PER_MIN"]


def test_delay_row():
    row, errs, _ = R.check_process_row({"process_name": "delay 10m"}, CFG)
    assert errs == [] and row["delay_s"] == 600


@pytest.mark.parametrize("patch,needle", [
    ({"ar": False}, "가스"),
    ({"ar_flow": 0.0}, "ar_flow"),
    ({"ar_flow": None}, "ar_flow"),
    ({"working_pressure": 0.0}, "working_pressure"),
    ({"process_time": 0.0, "shutter_delay": 0.0}, "시간"),
    ({"use_dc": False}, "파워"),
    ({"dc_power": 600.0}, "dc_power"),
    ({"base_pressure": 1e-2}, "base_pressure"),
    ({"heater_temp": 750.0}, "heater_temp"),
    ({"heater_ramp": 61.0}, "heater_ramp"),
    ({"ar": "1"}, "ar"),
    ({"unknown": 1}, "모르는 열"),
    ({"use_rf_pulse": True, "rf_pulse_power": 100.0, "rf_pulse_freq": 20000, "rf_pulse_duty": 80}, "RF Pulse"),
])
def test_bad_rows(patch, needle):
    row = dict(GOOD)
    row.update(patch)
    out, errs, _ = R.check_process_row(row, CFG)
    assert out is None and any(needle in e for e in errs), errs


def test_base_pressure_needs_ig():
    cfg = dict(CFG, IG_ENABLED=False)
    _, errs, _ = R.check_process_row(GOOD, cfg)
    assert any("이온 게이지" in e for e in errs)


def test_heater_ramp_below_6_corrected():
    row, errs, warns = R.check_process_row(dict(GOOD, heater_ramp=3.0), CFG)
    assert errs == [] and row["heater_ramp"] == 6.0 and warns


def test_heater_recipe():
    steps = [{"target_c": 300.0, "ramp_c_per_min": 12.0, "soak_min": 10.0}, {"target_c": 25.0, "soak_min": 30.0}]
    assert R.check_heater_recipe(steps, {"use_ar": True, "ar_flow": 10.0, "wp_mtorr": 5.0}, CFG)[0] == []
    errs, _ = R.check_heater_recipe([{"target_c": 300.0, "ramp_c_per_min": 12.0, "ramp_min": 5.0}], None, CFG)
    assert errs
    errs, _ = R.check_heater_recipe([{"target_c": 300.0, "repeat": 2}], None, CFG)
    assert errs   # HEATER_RECIPE_MAX_REPEAT=1 (pending_confirm)
    errs, _ = R.check_heater_recipe([{"target_c": 300.0}], {"use_ar": True, "ar_flow": 10.0}, CFG)
    assert any("wp_mtorr" in e for e in errs)


# ───────────── 메시지 ─────────────
def test_messages():
    types = [m.type for m in MESSAGES]
    assert len(types) == len(set(types))
    assert {"hello", "state", "snapshot_summary", "event", "run_start", "run_end", "cmd_result",
            "config_ack", "command"} <= set(types)
    for m in MESSAGES:
        names = [f.name for f in m.fields]
        assert len(names) == len(set(names)), m.type
        assert m.direction in ("up", "down")
    assert [f.name for f in ENVELOPE] == ["id", "seq", "ts", "type", "data"]


def test_common_reasons_not_shadowed():
    common = {r.code for r in COMMON_REASONS}
    chk = {r.code for r in C.CHK_REASONS}
    assert not common & chk
