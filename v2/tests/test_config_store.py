"""실행 모드·설정 저장소 시험 — 임시 폴더에만 쓴다."""
import json

import pytest

from v2.common.config_store import ACTIVE, OVERRIDE, PREV, ConfigError, ConfigStore
from v2.common.mode import RunMode, policy, resolve_mode
from v2.common.paramdef import defaults, schema_hash
from v2.chk import params as P

SH = schema_hash(P.PARAMS, P.CROSS_RULES)
SECRETS = {p.key for p in P.PARAMS if p.scope.value == "secret"}


def _web_values(**changes):
    v = {k: x for k, x in defaults(P.PARAMS).items() if k not in SECRETS}
    v.update(changes)
    return v


def _store(tmp_path):
    return ConfigStore(tmp_path, P.PARAMS, P.CROSS_RULES, SH)


# ───────── 모드 ─────────
def test_mode_default_sim_and_policies():
    assert resolve_mode(None) is RunMode.SIM and resolve_mode("") is RunMode.SIM
    assert resolve_mode("prod") is RunMode.PROD
    with pytest.raises(ValueError):
        resolve_mode("production")
    assert not policy(RunMode.BENCH).device_writes and policy(RunMode.BENCH).real_ports
    assert not policy(RunMode.SIM).real_ports and not policy(RunMode.SIM).uplink


# ───────── 읽기 ─────────
def test_fresh_install_defaults_sim(tmp_path):
    s = _store(tmp_path)
    vals = s.load()
    assert s.version == 0 and s.source == "defaults" and vals["RUN_MODE"] == "sim"
    assert not (tmp_path / ACTIVE).exists()


def test_apply_and_reload(tmp_path):
    s = _store(tmp_path)
    s.load()
    r = s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False)
    assert r.status == "applied" and r.applied_keys == ["FLOW_ERROR_SEC"]
    assert s.values["FLOW_ERROR_SEC"] == 7
    s2 = _store(tmp_path)
    assert s2.load()["FLOW_ERROR_SEC"] == 7 and s2.version == 1 and s2.source == "active"
    r = s.apply(2, _web_values(FLOW_ERROR_SEC=8), SH, busy=False)
    assert r.status == "applied" and (tmp_path / PREV).exists()
    assert json.loads((tmp_path / PREV).read_text(encoding="utf-8"))["version"] == 1


@pytest.mark.parametrize("patch,reason", [
    ({"HEATER_MV_LIMIT": 1400}, "CONFIG_INVALID"),
    ({"HEATER_ENABLED": "false"}, "CONFIG_INVALID"),
    ({"UPLINK_TOKEN": "x"}, "CONFIG_SECRET_FROM_WEB"),
])
def test_apply_rejects_whole_version(tmp_path, patch, reason):
    s = _store(tmp_path)
    s.load()
    before = dict(s.values)
    r = s.apply(1, _web_values(FLOW_ERROR_SEC=7, **patch), SH, busy=False)
    assert r.status == "rejected" and r.reason == reason
    assert s.values == before and not (tmp_path / ACTIVE).exists()      # 부분 적용 없음


def test_apply_version_and_hash_checks(tmp_path):
    s = _store(tmp_path)
    s.load()
    assert s.apply(1, _web_values(), "x" * 64, busy=False).reason == "CONFIG_HASH_MISMATCH"
    assert s.apply(1, _web_values(), SH, busy=False).status == "applied"
    assert s.apply(1, _web_values(), SH, busy=False).reason == "CONFIG_VERSION_STALE"


def test_busy_rules(tmp_path):
    s = _store(tmp_path)
    s.load()
    # H 등급 변경은 공정 중 거부(S48)
    r = s.apply(1, _web_values(DC_POWER_ERROR_SEC=6.0), SH, busy=True)
    assert r.reason == "CONFIG_H_IN_PROCESS"
    # live 는 즉시, idle 은 대기
    r = s.apply(1, _web_values(FLOW_ERROR_SEC=7, DC_SMALL_ERROR_GAIN=0.6), SH, busy=True)
    assert r.status == "pending_idle"
    assert r.applied_keys == ["FLOW_ERROR_SEC"] and r.pending_keys == ["DC_SMALL_ERROR_GAIN"]
    assert s.values["FLOW_ERROR_SEC"] == 7 and s.values["DC_SMALL_ERROR_GAIN"] == 0.5
    assert s.on_idle() == ["DC_SMALL_ERROR_GAIN"] and s.values["DC_SMALL_ERROR_GAIN"] == 0.6


def test_restart_keys_wait_for_restart(tmp_path):
    s = _store(tmp_path)
    s.load()
    r = s.apply(1, _web_values(MFC_PORT="COM20"), SH, busy=False)
    assert r.status == "pending_restart" and s.values["MFC_PORT"] == "COM10"
    assert _store(tmp_path).load()["MFC_PORT"] == "COM20"


def test_corrupt_active_falls_back_to_prev(tmp_path):
    s = _store(tmp_path)
    s.load()
    s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False)
    s.apply(2, _web_values(FLOW_ERROR_SEC=8), SH, busy=False)
    p = tmp_path / ACTIVE
    p.write_text(p.read_text(encoding="utf-8")[:-40], encoding="utf-8")      # 잘린 파일
    s2 = _store(tmp_path)
    assert s2.load()["FLOW_ERROR_SEC"] == 7 and s2.source == "prev" and s2.version == 1
    assert any("사용 불가" in n for n in s2.notes)


def test_tampered_value_detected(tmp_path):
    s = _store(tmp_path)
    s.load()
    s.apply(1, _web_values(), SH, busy=False)
    s.apply(2, _web_values(FLOW_ERROR_SEC=8), SH, busy=False)
    p = tmp_path / ACTIVE
    obj = json.loads(p.read_text(encoding="utf-8"))
    obj["values"]["HEATER_MV_LIMIT"] = 1299            # 범위 안이지만 해시가 다름
    p.write_text(json.dumps(obj), encoding="utf-8")
    s2 = _store(tmp_path)
    s2.load()
    assert s2.source == "prev" and s2.values["HEATER_MV_LIMIT"] == 1200


def test_both_broken_refuses_start(tmp_path):
    (tmp_path / ACTIVE).write_text("{", encoding="utf-8")
    (tmp_path / PREV).write_text("[]", encoding="utf-8")
    with pytest.raises(ConfigError):
        _store(tmp_path).load()
    (tmp_path / PREV).unlink()
    with pytest.raises(ConfigError):                     # 기본값으로 조용히 돌지 않는다
        _store(tmp_path).load()


def test_active_missing_uses_prev(tmp_path):
    s = _store(tmp_path)
    s.load()
    s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False)
    s.apply(2, _web_values(FLOW_ERROR_SEC=8), SH, busy=False)
    (tmp_path / ACTIVE).unlink()                         # 바꿔치기 중간에 멈춘 경우
    s2 = _store(tmp_path)
    assert s2.load()["FLOW_ERROR_SEC"] == 7 and s2.source == "prev"


def test_schema_change_fills_new_keys(tmp_path):
    s = _store(tmp_path)
    s.load()
    s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False)
    # 예전 코드가 저장한 파일처럼: 키 하나 없고, 모르는 키 하나 있고, 해시 다름
    import v2.common.config_store as cs
    p = tmp_path / ACTIVE
    obj = json.loads(p.read_text(encoding="utf-8"))
    del obj["values"]["NAS_SYNC_PERIOD_SEC"]
    obj["values"]["OLD_KEY"] = 1
    obj["schemaHash"] = "old"
    obj["contentHash"] = cs._content_hash(obj["version"], "old", obj["values"])
    p.write_text(json.dumps(obj), encoding="utf-8")
    s2 = _store(tmp_path)
    vals = s2.load()
    assert vals["FLOW_ERROR_SEC"] == 7 and vals["NAS_SYNC_PERIOD_SEC"] == 60 and "OLD_KEY" not in vals
    assert any("정의가 바뀜" in n for n in s2.notes)


def test_override_install_and_secret(tmp_path):
    (tmp_path / OVERRIDE).write_text(json.dumps({"values": {
        "RUN_MODE": "bench", "MFC_PORT": "COM21", "UPLINK_URL": "https://example.invalid/x", "UPLINK_TOKEN": "t"}}),
        encoding="utf-8")
    s = _store(tmp_path)
    vals = s.load()
    assert vals["RUN_MODE"] == "bench" and vals["MFC_PORT"] == "COM21" and vals["UPLINK_TOKEN"] == "t"
    r = s.apply(1, _web_values(MFC_PORT="COM22", RUN_MODE="prod"), SH, busy=False)
    assert r.reason == "CONFIG_LOCKED_KEY" and "RUN_MODE" in r.errors[0]     # 저장 sim·실행 bench 어느 쪽도 아님 — 잠금
    r = s.apply(1, _web_values(MFC_PORT="COM22"), SH, busy=False)           # 웹은 저장 값(RUN_MODE=sim)을 보낸다
    assert r.status == "applied" and r.overridden_keys == ["MFC_PORT", "RUN_MODE"] and not s.pending_restart
    assert _store(tmp_path).load()["MFC_PORT"] == "COM21"        # 덮어쓰기가 이긴다
    assert s.stored()["MFC_PORT"] == "COM22" and s.stored()["RUN_MODE"] == "sim"


def test_web_sends_stored_values_no_pending_restart(tmp_path):
    """5-12 ① — 웹은 묶음 params[].stored 를 첫 설정 버전으로 두고 그대로 보낸다: 덮어쓰기 키는 overriddenKeys 로 알리고
    pending_restart 없음(실행 값은 덮어쓰기 그대로). 예전 웹처럼 current(덮어쓰기 값)를 보내도 잠금 키는 저장 값을 그대로 둔다."""
    from v2.chk import web_schema as W
    (tmp_path / OVERRIDE).write_text(json.dumps({"values": {
        "RUN_MODE": "bench", "MFC_PORT": "COM21", "LOCAL_HMI_PORT": 18000, "UPLINK_TOKEN": "t"}}), encoding="utf-8")
    s = _store(tmp_path)
    cfg = s.load()
    b = W.bundle_for(cfg, s, s.version)
    by = {p["key"]: p for p in b["params"]}
    assert b["overriddenKeys"] == ["LOCAL_HMI_PORT", "MFC_PORT", "RUN_MODE"]                  # 비밀값은 빼고
    assert (by["MFC_PORT"]["current"], by["MFC_PORT"]["stored"], by["MFC_PORT"]["overridden"]) == \
        ("COM21", defaults(P.PARAMS)["MFC_PORT"], True)
    assert (by["RUN_MODE"]["current"], by["RUN_MODE"]["stored"]) == ("bench", "sim")
    assert (by["FLOW_ERROR_SEC"]["stored"], by["FLOW_ERROR_SEC"]["overridden"]) == (by["FLOW_ERROR_SEC"]["current"], False)
    assert all(by[k]["current"] is None and by[k]["stored"] is None and by[k]["overridden"] is None for k in SECRETS)
    web = {k: p["stored"] for k, p in by.items() if k not in SECRETS}                         # 웹 import = stored
    r = s.apply(1, web, SH, busy=False)
    assert r.status == "applied" and r.pending_keys == [] and not s.pending_restart and not r.changes
    assert r.overridden_keys == ["LOCAL_HMI_PORT", "MFC_PORT", "RUN_MODE"]
    r = s.apply(2, dict(web, FLOW_ERROR_SEC=7, LOCAL_HMI_PORT=8711), SH, busy=False)         # 덮어쓴 install 키를 바꿔도
    assert r.status == "applied" and not s.pending_restart and s.values["LOCAL_HMI_PORT"] == 18000
    assert s.stored()["LOCAL_HMI_PORT"] == 8711 and "LOCAL_HMI_PORT" in r.overridden_keys
    old_web = {k: p["current"] for k, p in by.items() if k not in SECRETS}                    # 예전 웹(current = 덮어쓰기 값)
    r = s.apply(3, old_web, SH, busy=False)
    assert r.status == "applied" and not s.pending_restart and s.stored()["RUN_MODE"] == "sim"   # 잠금 키는 저장 값 유지
    assert json.loads((tmp_path / ACTIVE).read_text(encoding="utf-8"))["values"]["RUN_MODE"] == "sim"
    assert "RUN_MODE" not in {c["key"] for c in r.changes}
    assert W.bundle_for(s.values, s, s.version)["params"][0]["key"]                         # 묶음은 그대로 만들어진다


@pytest.mark.parametrize("patch", [{"RUN_MODE": "prod"},
                                   {"RUN_MODE": "bench"}, {"EQUIPMENT_ID": "CHK9"}])
def test_run_mode_and_equipment_id_locked_from_web(tmp_path, patch):
    """덮어쓰기 파일에 없어도 웹에서 실행 모드(sim → prod 등)·장비 ID 를 바꾸지 않는다 — 나머지 설치 키는 restart 대기."""
    s = _store(tmp_path)
    s.load()
    before = dict(s.values)
    r = s.apply(1, _web_values(**patch), SH, busy=False)
    assert r.status == "rejected" and r.reason == "CONFIG_LOCKED_KEY"
    assert s.values == before and not (tmp_path / ACTIVE).exists()
    assert s.apply(1, _web_values(MFC_PORT="COM22"), SH, busy=False).status == "pending_restart"   # 다른 설치 키는 그대로


def test_rollback_with_locked_key_changed_is_rejected(tmp_path):
    from v2.common.config_store import _content_hash
    s = _store(tmp_path)
    s.load()
    assert s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False).status == "applied"
    assert s.apply(2, _web_values(FLOW_ERROR_SEC=8), SH, busy=False).status == "applied"
    prev = json.loads((tmp_path / PREV).read_text(encoding="utf-8"))   # 손으로 고친 이전 파일(장비 ID 가 다름)
    prev["values"]["EQUIPMENT_ID"] = "CHK9"
    prev["contentHash"] = _content_hash(prev["version"], prev["schemaHash"], prev["values"])
    (tmp_path / PREV).write_text(json.dumps(prev), encoding="utf-8")
    r = s.rollback(busy=False, by="web")
    assert r.status == "rejected" and r.reason == "CONFIG_LOCKED_KEY" and s.values["EQUIPMENT_ID"] == "CHK2"


@pytest.mark.parametrize("vals", [
    {"FLOW_ERROR_SEC": 3},          # process 키는 덮어쓰기 불가
    {"RUN_MODE": "production"},           # 허용값 아님
    {"NOPE": 1},
])
def test_bad_override_refuses_start(tmp_path, vals):
    (tmp_path / OVERRIDE).write_text(json.dumps({"values": vals}), encoding="utf-8")
    with pytest.raises(ConfigError):
        _store(tmp_path).load()


def test_prod_without_url_refused(tmp_path):
    (tmp_path / OVERRIDE).write_text(json.dumps({"values": {"RUN_MODE": "prod"}}), encoding="utf-8")
    with pytest.raises(ConfigError):
        _store(tmp_path).load()


# ───────── 이력·되돌리기(5-4 ④) ─────────
def test_history_records_who_when_old_new(tmp_path):
    s = _store(tmp_path)
    s.load()
    r = s.apply(1, _web_values(FLOW_ERROR_SEC=7, SP4_MTORR=25.0), SH, busy=False, by="kim@vanam")
    assert r.status == "applied" and r.version == 1 and r.kind == "apply"
    assert {c["key"]: (c["old"], c["new"]) for c in r.changes} == {"FLOW_ERROR_SEC": (defaults(P.PARAMS)["FLOW_ERROR_SEC"], 7),
                                                                     "SP4_MTORR": (30.0, 25.0)}
    bad = s.apply(2, _web_values(HEATER_MV_LIMIT=1400), SH, busy=False, by="lee@vanam")
    assert bad.status == "rejected"
    h = s.history()
    assert [(x["version"], x["by"], x["status"]) for x in h] == [(1, "kim@vanam", "applied"), (2, "lee@vanam", "rejected")]
    assert h[1]["reason"] == "CONFIG_INVALID" and h[0]["at"] and not any(c["key"] in SECRETS for x in h for c in x["changes"])


def test_rollback_to_previous_version(tmp_path):
    s = _store(tmp_path)
    s.load()
    assert s.rollback(False).reason == "CONFIG_NO_PREVIOUS"
    s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False, by="a")
    s.apply(2, _web_values(FLOW_ERROR_SEC=9, DC_MAX_POWER=400.0), SH, busy=False, by="b")
    r = s.rollback(busy=True, by="c")                                    # 공정 중 — H 등급(DC_MAX_POWER) 변경은 거부(S48)
    assert r.status == "rejected" and r.reason == "CONFIG_H_IN_PROCESS"
    r = s.rollback(busy=False, by="c")
    assert r.status == "applied" and r.kind == "rollback" and r.version == 3
    assert r.values["FLOW_ERROR_SEC"] == 7 and s.values["FLOW_ERROR_SEC"] == 7 and s.values["DC_MAX_POWER"] == \
        defaults(P.PARAMS)["DC_MAX_POWER"]
    assert {c["key"] for c in r.changes} == {"FLOW_ERROR_SEC", "DC_MAX_POWER"}
    s2 = _store(tmp_path)
    assert s2.load()["FLOW_ERROR_SEC"] == 7 and s2.version == 3
    assert [x["kind"] for x in s.history()] == ["apply", "apply", "rollback:1", "rollback:1"]


def test_rollback_version_from_web(tmp_path):
    """5-12 ③ — 새 버전 번호를 웹이 정한다(version). 지금 버전보다 작거나 같으면 CONFIG_VERSION_STALE, 없으면 지금 + 1."""
    s = _store(tmp_path)
    s.load()
    s.apply(1, _web_values(FLOW_ERROR_SEC=7), SH, busy=False)
    s.apply(2, _web_values(FLOW_ERROR_SEC=9), SH, busy=False)
    for v in (2, 1):
        r = s.rollback(busy=False, by="w", version=v)
        assert (r.status, r.reason, r.version, r.kind) == ("rejected", "CONFIG_VERSION_STALE", v, "rollback")
    assert s.version == 2 and s.values["FLOW_ERROR_SEC"] == 9
    r = s.rollback(busy=False, by="w", version=7)                    # 웹에 거부·무응답 버전 3~6 이 있어도 겹치지 않게
    assert r.status == "applied" and r.version == 7 and s.version == 7 and s.values["FLOW_ERROR_SEC"] == 7
    r = s.rollback(busy=False, by="w")                               # 없으면 지금 + 1
    assert r.status == "applied" and r.version == 8 and s.values["FLOW_ERROR_SEC"] == 9
    assert _store(tmp_path).load()["FLOW_ERROR_SEC"] == 9
    assert [(x["version"], x["status"], x["reason"]) for x in s.history()][2:] == [
        (2, "rejected", "CONFIG_VERSION_STALE"), (1, "rejected", "CONFIG_VERSION_STALE"),
        (7, "applied", None), (8, "applied", None)]


def test_removed_stop_sequence_order_in_saved_file(tmp_path):
    """5-5 — 지운 키 STOP_SEQUENCE_ORDER 가 저장 파일에 남아 있으면: 해시가 다르니 정의가 바뀐 파일로 읽고 그 키는 버린다(notes).
    웹이 옛 해시로 보내면 CONFIG_HASH_MISMATCH, 새 해시인데 그 키가 있으면 CONFIG_INVALID(정의되지 않은 키)."""
    import v2.common.config_store as cs
    s = _store(tmp_path)
    s.load()
    s.apply(1, _web_values(), SH, busy=False)
    p = tmp_path / ACTIVE
    obj = json.loads(p.read_text(encoding="utf-8"))
    obj["values"]["STOP_SEQUENCE_ORDER"] = "2,1"
    obj["schemaHash"] = "5-4"
    obj["contentHash"] = cs._content_hash(obj["version"], "5-4", obj["values"])
    p.write_text(json.dumps(obj), encoding="utf-8")
    s2 = _store(tmp_path)
    vals = s2.load()
    assert "STOP_SEQUENCE_ORDER" not in vals and s2.source == "active"
    assert any("STOP_SEQUENCE_ORDER" in n for n in s2.notes)
    assert s2.apply(2, _web_values(STOP_SEQUENCE_ORDER="2,1"), "5-4", busy=False).reason == "CONFIG_HASH_MISMATCH"
    assert s2.apply(2, _web_values(STOP_SEQUENCE_ORDER="2,1"), SH, busy=False).reason == "CONFIG_INVALID"


# ───────── float 형식 키는 늘 float(사용자 지시 5-10 ②) ─────────
def _int_float_key():
    from v2.common.paramdef import PType
    return next(p.key for p in P.PARAMS if p.type is PType.INT and p.scope.value == "process")


def test_float_keys_become_float_on_apply_save_and_reject_text(tmp_path):
    from v2.chk import recipe as RC
    ik = _int_float_key()
    s = _store(tmp_path)
    s.load()
    r = s.apply(1, _web_values(HEATER_MAX_TEMP=650), SH, busy=False)          # 웹 JSON 은 650.0 을 650 으로 보낸다
    assert r.status == "applied"
    assert type(s.values["HEATER_MAX_TEMP"]) is float and type(s.persisted["HEATER_MAX_TEMP"]) is float
    assert type(s.values[ik]) is int                                          # int 형식 키는 그대로
    saved = json.loads((tmp_path / ACTIVE).read_text(encoding="utf-8"))["values"]
    assert type(saved["HEATER_MAX_TEMP"]) is float
    assert [c for c in r.changes if c["key"] == "HEATER_MAX_TEMP"] == [{"key": "HEATER_MAX_TEMP", "old": 700.0, "new": 650.0}]
    assert type(r.changes[0]["new"]) is float
    row = {"process_name": "x", "ar": True, "ar_flow": 20.0, "working_pressure": 3.0, "process_time": 1.0,
           "use_dc": True, "dc_power": 100.0, "use_heater": True, "heater_temp": 900.0}
    _, errs, _ = RC.check_process_row(row, s.values)
    assert "heater_temp: 900.0 > 650.0" in errs                               # "650" 이 아니라 묶음 형식대로


def test_float_keys_from_old_int_file_and_override(tmp_path):
    from v2.common.config_store import _content_hash
    vals = _web_values(HEATER_MAX_TEMP=600)
    assert type(vals["HEATER_MAX_TEMP"]) is int
    (tmp_path / ACTIVE).write_text(json.dumps({"version": 3, "schemaHash": SH, "savedAt": "x", "values": vals,
                                               "contentHash": _content_hash(3, SH, vals)}), encoding="utf-8")
    fs = int(defaults(P.PARAMS)["MFC_AR_FS_SCCM"])
    (tmp_path / OVERRIDE).write_text(json.dumps({"values": {"MFC_AR_FS_SCCM": fs}}), encoding="utf-8")
    s = _store(tmp_path)
    v = s.load()
    assert s.source == "active" and v["HEATER_MAX_TEMP"] == 600.0 and type(v["HEATER_MAX_TEMP"]) is float
    assert type(s.persisted["HEATER_MAX_TEMP"]) is float
    assert type(v["MFC_AR_FS_SCCM"]) is float and type(s.override["MFC_AR_FS_SCCM"]) is float


def test_float_keys_on_rollback_and_bool_is_not_number(tmp_path):
    from v2.common.paramdef import as_float
    s = _store(tmp_path)
    s.load()
    assert s.apply(1, _web_values(HEATER_MAX_TEMP=650), SH, busy=False).status == "applied"
    assert s.apply(2, _web_values(HEATER_MAX_TEMP=640), SH, busy=False).status == "applied"
    r = s.rollback(busy=False)
    assert r.status == "applied" and type(r.values["HEATER_MAX_TEMP"]) is float and r.values["HEATER_MAX_TEMP"] == 650.0
    assert type(s.values["HEATER_MAX_TEMP"]) is float
    assert as_float(P.PARAMS, {"HEATER_MAX_TEMP": True}) == {"HEATER_MAX_TEMP": True}
    r = s.apply(9, _web_values(HEATER_MAX_TEMP=True), SH, busy=False)
    assert r.reason == "CONFIG_INVALID"
    assert all(type(v) is float for k, v in defaults(P.PARAMS).items()
               if next(p for p in P.PARAMS if p.key == k).type.value == "float")


def test_float_keys_in_config_ack_cfg_and_bundle(tmp_path):
    import threading
    from v2.chk import web_schema as W
    from v2.chk.system import ConfigApply
    s = _store(tmp_path)
    cfg = dict(s.load())
    acks = []
    ca = ConfigApply(s, cfg, lambda: False, threading.Lock(), acks.append)
    assert ca.apply({"version": 1, "values": _web_values(HEATER_MAX_TEMP=650), "schemaHash": SH}, "web") is None
    assert type(cfg["HEATER_MAX_TEMP"]) is float                              # 실행 설정(hello·묶음 current 의 원본)
    assert '"new": 650.0' in json.dumps(acks[0]["changes"])
    cur = {p["key"]: p["current"] for p in W.bundle(cfg)["params"]}
    assert type(cur["HEATER_MAX_TEMP"]) is float and '"current":650.0' in W.to_json(W.bundle(cfg))
    assert ca.apply({"version": 2, "values": _web_values(HEATER_MAX_TEMP=640), "schemaHash": SH}, "web") is None
    assert ca.rollback({}, "web") is None
    assert acks[-1]["kind"] == "rollback" and '"HEATER_MAX_TEMP": 650.0' in json.dumps(acks[-1]["values"])
    assert type(cfg["HEATER_MAX_TEMP"]) is float and cfg["HEATER_MAX_TEMP"] == 650.0
