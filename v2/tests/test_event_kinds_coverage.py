"""5-22 ① — 장비가 웹으로 내는 사건은 모두 묶음 eventKinds 에 이름·설명·수준이 있다(가상 시계·시뮬레이터·가짜 웹만).

- 코드에서 사건을 내는 곳(test_web_labels._code_event_kinds — 웹 연결 uplink 포함)이 모두 표에 있다
- sim 으로 여러 경우를 돌려(정상 공정·STOP·시작 거부·수동 조작·웹 토큰 거부·409·웹 끊김과 복구) 허브로 나간 사건의
  (source, kind) 가 모두 표에 있고, source·kind 가 비지 않는다. 기록에 남는 code 는 그대로(UPLINK_* 등)
"""
import re

from v2.chk import web_schema as W
from v2.chk import uplink as U
from v2.tests.test_manual import M, SCENARIO
from v2.tests.test_process_engine import FAST, R, SHORT
from v2.tests.test_uplink import FakeWeb, _rig_uplink
from v2.tests.test_web_labels import _code_event_kinds

KINDS = {(e["source"], e["kind"]): e for e in W.bundle()["eventKinds"]}


def _events(hub):
    return [m["data"] for m in list(hub.recent) if m["type"] == "event"]


def _check(evs):
    assert evs
    bad = [(e["code"], (e.get("context") or {}).get("source"), (e.get("context") or {}).get("kind")) for e in evs
           if not (e.get("context") or {}).get("source") or not (e.get("context") or {}).get("kind")
           or ((e["context"]["source"], e["context"]["kind"]) not in KINDS)]
    assert bad == [], bad[:10]
    return {(e["context"]["source"], e["context"]["kind"]) for e in evs}


def test_code_event_sites_all_named():
    code = _code_event_kinds()
    up = {k for s, k in code if s == "uplink"}
    assert up == {"uplink_up", "uplink_down", "uplink_auth_failed", "uplink_conflict", "uplink_gap", "uplink_spool_loaded",
                  "uplink_spool_error", "uplink_spool_full", "uplink_too_large", "uplink_bad_command",
                  "uplink_recipes", "uplink_recipes_failed"}                     # W-22 F 웹 레시피 사본
    assert sorted(code - set(KINDS)) == []
    for k in code:
        e = KINDS[k]
        assert e["label"] and e["desc"] and e["level"] in ("main", "detail"), k
        assert not re.search(r"[A-Z0-9]+_[A-Z0-9_]+|\blocal\b", e["label"]), (k, e["label"])     # 코드·영문 local 없음


def test_sim_process_runs_events_all_named(tmp_path):
    seen = set()
    # 정상 공정 → 끝
    r = R(tmp_path / "a", dict(FAST))
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(160)
    seen |= _check(_events(r.rig.hub))
    # STOP 으로 멈춤
    r = R(tmp_path / "b", dict(FAST))
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.on(30, r.cmd("STOP"))
    r.run(90)
    seen |= _check(_events(r.rig.hub))
    # 시작 거부(진공 조건 없음)
    r = R(tmp_path / "c", dict(FAST))
    for k in ("mv", "turbo", "rotary", "fv"):
        r.plc.coils[__import__("v2.chk.sim.plc_sim", fromlist=["A"]).A[k]] = False
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(20)
    seen |= _check(_events(r.rig.hub))
    # 수동 조작(블록 하나씩 → 끝내기)
    m = M(tmp_path / "d")
    m.blocks(SCENARIO)
    m.run(400)
    seen |= _check(_events(m.rig.hub))
    assert ("process", "state") in seen and ("process", "manual") in seen and ("safety", "seq_start") in seen


def test_sim_uplink_events_all_named(tmp_path, monkeypatch):
    monkeypatch.setattr(U, "AUTH_RETRY_S", 0.5)
    monkeypatch.setattr(U, "CONFLICT_RETRY_S", 0.3)
    web = FakeWeb(keepalive_s=0.5).up()
    try:
        rig, up = _rig_uplink(tmp_path / "a", web, token="wrong")          # 토큰 거부
        up.start()
        try:
            assert web.wait(lambda: any(e["code"] == "UPLINK_AUTH_FAILED" for e in _events(rig.hub)), 4)
        finally:
            up.stop()
        evs = _events(rig.hub)
        rig, up = _rig_uplink(tmp_path / "b", web)                          # 409 → 끊김(500) → 복구
        up.start()
        try:
            web.status = web.sse_status = 409
            assert web.wait(lambda: any(e["code"] == "UPLINK_CONFLICT" for e in _events(rig.hub)), 4)
            web.status, web.sse_status = 500, None
            assert web.wait(lambda: any(e["code"] == "UPLINK_DOWN" for e in _events(rig.hub)), 12)
            web.status = None
            assert web.wait(lambda: any(e["code"] == "UPLINK_UP" for e in _events(rig.hub)), 14)
        finally:
            up.stop()
        evs += _events(rig.hub)
    finally:
        web.down()
    seen = _check(evs)
    assert {("uplink", "uplink_auth_failed"), ("uplink", "uplink_conflict"), ("uplink", "uplink_down"),
            ("uplink", "uplink_up")} <= seen
    assert {e["code"] for e in evs if e["context"]["source"] == "uplink"} >= {"UPLINK_AUTH_FAILED", "UPLINK_DOWN"}
    assert KINDS[("uplink", "uplink_auth_failed")]["label"] == "웹이 장비 토큰을 거부함"
