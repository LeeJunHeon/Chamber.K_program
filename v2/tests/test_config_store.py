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
    r = s.apply(1, _web_values(MFC_PORT="COM22"), SH, busy=False)
    assert r.overridden_keys == ["MFC_PORT", "RUN_MODE"]       # 웹의 RUN_MODE=sim 도 노트북 bench 에 가려짐
    assert _store(tmp_path).load()["MFC_PORT"] == "COM21"        # 덮어쓰기가 이긴다


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
