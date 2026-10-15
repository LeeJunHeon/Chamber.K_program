"""파라미터 표 시험 — v1 json 대조, 범위·키 사이 규칙, 등급 규칙, 스키마."""
import json
import re
from pathlib import Path

import pytest

from v2.common.paramdef import (
    Apply, PType, Role, Safety, Scope, check_config, check_value, defaults, schema_dict, schema_hash,
)
from v2.chk import params as P

REPO = Path(__file__).resolve().parents[2]
V1_JSON = REPO / "config_user.json"


def _v1_json() -> dict:
    """v1 로더와 같은 방식: 문자열 밖 // 주석 제거 → json → "//" 로 시작하는 키 제외. 파일은 읽기만."""
    text = V1_JSON.read_text(encoding="utf-8")
    out, in_str, esc, i = [], False, False, 0
    while i < len(text):
        c = text[i]
        if in_str:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
            out.append(c)
        elif text.startswith("//", i):
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        else:
            out.append(c)
        i += 1
    data = json.loads("".join(out))
    return {k: v for k, v in data.items() if not k.startswith("//")}


def test_v1_json_has_99_entries():
    assert len(_v1_json()) == 99


def test_every_v1_json_key_is_param_or_dropped():
    keymap = P.json_key_map()
    dropped = {d.key for d in P.DROPPED}
    missing = [k for k in _v1_json() if k not in keymap and k not in dropped]
    assert not missing, missing


def test_defaults_equal_v1_operating_values():
    """default 는 지금 운영값(json)이어야 한다."""
    keymap = P.json_key_map()
    defs = P.by_key()
    diff = []
    for k, v in _v1_json().items():
        if k in keymap:
            p = defs[keymap[k]]
            want = v * p.v1_period_s if p.v1_period_s else v          # 횟수 → 시간(횟수 × v1 판정 주기)
            if p.default != want or isinstance(p.default, bool) != isinstance(v, bool):
                diff.append((k, v, p.default))
    assert not diff, diff


def test_keys_unique_and_named():
    keys = [p.key for p in P.PARAMS]
    assert len(keys) == len(set(keys))
    assert all(re.fullmatch(r"[A-Z][A-Z0-9_]*", k) for k in keys)
    dropped = [d.key for d in P.DROPPED]
    assert len(dropped) == len(set(dropped))
    assert not set(keys) & set(dropped)
    v1keys = [p.v1_key for p in P.PARAMS if p.v1_key]
    assert len(v1keys) == len(set(v1keys))
    assert not set(v1keys) & set(keys)


@pytest.mark.parametrize("p", P.PARAMS, ids=lambda p: p.key)
def test_default_passes_own_check(p):
    assert check_value(p, p.default) is None


@pytest.mark.parametrize("p", P.PARAMS, ids=lambda p: p.key)
def test_def_is_consistent(p):
    assert p.label and p.desc and p.group
    if p.type is PType.ENUM:
        assert p.enum and p.default in p.enum
    if p.type in (PType.INT, PType.FLOAT) and p.min is not None and p.max is not None:
        assert p.min <= p.max
    if p.type is PType.BOOL or p.type is PType.ENUM or p.type is PType.STR:
        assert p.min is None and p.max is None
    # H 등급: admin 전용, 공정 중 변경 금지
    if p.safety is Safety.H:
        assert p.role is Role.ADMIN and p.apply is not Apply.LIVE
    # 설치값·비밀값: 재시작, admin
    if p.scope is not Scope.PROCESS:
        assert p.apply is Apply.RESTART and p.role is Role.ADMIN
    assert p.v1_source


def test_all_defaults_pass_config_check():
    assert check_config(P.PARAMS, P.CROSS_RULES, defaults(P.PARAMS)) == []


def test_cross_rule_keys_exist():
    keys = set(P.by_key())
    ids = [r.id for r in P.CROSS_RULES]
    assert len(ids) == len(set(ids))
    for r in P.CROSS_RULES:
        assert set(r.keys) <= keys, (r.id, set(r.keys) - keys)


@pytest.mark.parametrize("key,bad", [
    ("HEATER_MV_LIMIT", 1400),          # > ABS_MAX(1300)
    ("HEATER_OT_LIMIT_C", 710.0),       # < SV_LIMIT + 20
    ("HEATER_MAX_TEMP", 720.0),         # > SV_LIMIT
    ("HEATER_WD_PERIOD_MS", 4000),      # 범위 밖(> 3000)
    ("PLC_SESSION_MARK_REG", 12),       # 사용 중 주소
    ("RFPULSE_ADDR", 32),               # 0~31
    ("IG_PORT", "COM9"),                # PLC 와 중복
])
def test_bad_values_rejected(key, bad):
    vals = defaults(P.PARAMS)
    vals[key] = bad
    assert check_config(P.PARAMS, P.CROSS_RULES, vals)


@pytest.mark.parametrize("key,bad", [
    ("HEATER_ENABLED", "false"),        # v1 은 문자열 "false" 를 참으로 읽었다
    ("HEATER_MV_MIN", "400"),
    ("PLC_RETRY_COUNT", True),          # bool 은 int 로 받지 않는다
    ("HEATER_HOLD_MODE", "TC2"),        # 대소문자 정확히
    ("DC_CONTROL_GAIN", -1.0),          # v1 은 음수도 허용했다
])
def test_strict_types(key, bad):
    assert check_value(P.by_key()[key], bad)


def test_unknown_or_missing_key_rejected():
    vals = defaults(P.PARAMS)
    vals["NOPE"] = 1
    del vals["HEATER_MV_MIN"]
    errs = check_config(P.PARAMS, P.CROSS_RULES, vals)
    assert any("NOPE" in e for e in errs) and any("HEATER_MV_MIN" in e for e in errs)


def test_prod_needs_uplink_url():
    vals = defaults(P.PARAMS)
    vals["RUN_MODE"] = "prod"
    assert check_config(P.PARAMS, P.CROSS_RULES, vals)


def test_secrets_not_in_schema():
    sch = schema_dict(P.PARAMS, P.CROSS_RULES)
    secrets = [x for x in sch["params"] if x["scope"] == "secret"]
    assert secrets and all(x["default"] is None for x in secrets)
    assert len(schema_hash(P.PARAMS, P.CROSS_RULES)) == 64


def test_pending_confirm_marked():
    pend = {p.key for p in P.PARAMS if p.pending_confirm}
    assert {"ALL_STOP_KEEP_PUMPS", "HEATER_RECIPE_HOLD_AT_END", "HEATER_RECIPE_MAX_REPEAT"} <= pend


def test_comm_install_params():
    p = P.by_key()
    for k in ("PLC_FAIL_STREAK", "MFC_FAIL_STREAK", "DC_FAIL_STREAK", "RFPULSE_FAIL_STREAK", "IG_FAIL_STREAK"):
        assert p[k].default == 3 and p[k].scope is Scope.INSTALL and p[k].role is Role.ADMIN and p[k].apply is Apply.RESTART
    assert p["PORT_OPEN_TIMEOUT_SEC"].default == 2.0 and p["CONNECT_INIT_TIMEOUT_SEC"].default == 5.0
    assert p["NAS_TODAY_COPY_SEC"].default == 600



def test_count_params_converted_to_seconds():
    """안전·판정용 횟수 파라미터는 없다 — 시간(*_SEC) 기본값 = v1 횟수 × v1 판정 주기."""
    keys = set(P.by_key())
    for old in ("FLOW_ERROR_MAX_COUNT", "MFC_PRESSURE_WARN_COUNT", "DC_POWER_ERROR_MAX_COUNT", "DC_MIN_CURRENT_ABORT_COUNT",
                "DC_FAIL_MAX_TICKS", "RF_POWER_ERROR_MAX_COUNT", "RF_FAIL_MAX_TICKS", "RFPULSE_FORP_CONSECUTIVE_LIMIT",
                "RFPULSE_REFP_CONSECUTIVE_LIMIT", "IG_BASE_STABLE_COUNT", "HEATER_OUT_DEAD_COUNT",
                "PRESSURE_WAIT_STABLE_COUNT"):
        assert old not in keys
    conv = {p.key: (p.v1_default, p.v1_period_s, p.default) for p in P.PARAMS if p.v1_period_s}
    assert len(conv) == 12
    for k, (n, period, sec) in conv.items():
        assert k.endswith("_SEC") and sec == pytest.approx(n * period), k
    assert conv["RFPULSE_REFP_SEC"][2] == 15.0 and conv["FLOW_ERROR_SEC"][2] == 10.0
    assert P.by_key()["RFPULSE_REFP_AUTO_OFF_SEC"].default > conv["RFPULSE_REFP_SEC"][2]
    left = [k for k in keys if re.search(r"_(MAX_COUNT|CONSECUTIVE_LIMIT|STABLE_COUNT|DEAD_COUNT|MAX_TICKS)$", k)]
    assert left == []
