"""W-22 F·G — 웹 레시피 사본·노트북 적재, 재고관리 장착 타겟. 가상 시계·시뮬레이터·가짜 웹·가짜 HTTP(루프백)만.

F: 웹 연결이 붙을 때·웹 알림(event: recipes) 때 사본을 받는다(장비 토큰), 파일에 남아 다시 켜도 그대로, 웹이 꺼지면 마지막 사본.
   RECIPE_LOAD {id, updatedAt} — 사본의 행을 장비가 다시 검사해 적재, 다르면 RECIPE_COPY_STALE, 없으면 RECIPE_COPY_MISSING,
   노트북은 이 길만(행 직접은 BAD_ARGS). 히터 레시피도 사본에서. 공정 기록(run_start params)에 레시피 id·웹 수정 시각.
G: 자리 이름 'Chamber K'·'Gun N'(조각 셋 — '-' 로 둘로 나누지 않음), 빈 타겟 이름만 채움(레시피 값이 먼저), 실패는 info 하나·공정 계속.
"""
import http.server
import json
import threading
import time

import pytest

from v2.common.commands import Source
from v2.chk import commands as C
from v2.chk import targets as TG
from v2.chk.recipe_copy import RecipeCopy
from v2.tests.test_process_engine import FAST, R, SHORT
from v2.tests.test_uplink import FakeWeb, _rig_uplink

T0 = "2026-11-25T01:00:00.000Z"
T1 = "2026-11-25T02:00:00.000Z"
ROW = dict(SHORT, process_name="copy-row")
WEB_RECIPES = [
    {"id": "r1", "kind": "process", "name": "Ti 증착", "rows": [ROW], "gas": None, "updatedAt": T0, "updatedBy": "kim@x",
     "check": {"errors": 0, "warnings": 1, "first": "경고 하나"}},
    {"id": "h1", "kind": "heater", "name": "히터 200", "rows": [{"target_c": 50.0, "ramp_c_per_min": 10.0, "soak_min": 0.1}],
     "gas": None, "updatedAt": T0, "check": {"errors": 0, "warnings": 0, "first": None}},
    {"id": "bad", "kind": "process", "name": "", "rows": [], "updatedAt": T0},          # 모양이 다름 — 버린다
]


# ───────── 사본 파일 ─────────
def test_copy_file_roundtrip_and_summary(tmp_path):
    rc = RecipeCopy(tmp_path)
    assert rc.summary()["count"] == 0 and rc.state() == {"syncedAt": None, "count": 0, "error": None}
    assert rc.replace(WEB_RECIPES, T1, "CHK2") == 2
    again = RecipeCopy(tmp_path)                                         # 다시 켜도 마지막 사본
    s = again.summary()
    assert s["syncedAt"] == T1 and s["count"] == 2 and [r["id"] for r in s["recipes"]] == ["h1", "r1"]
    r1 = next(r for r in s["recipes"] if r["id"] == "r1")
    assert r1["rowCount"] == 1 and r1["updatedAt"] == T0 and r1["check"]["warnings"] == 1 and "rows" not in r1
    assert again.get("r1")["rows"] == [ROW] and again.get("bad") is None
    again.failed("웹 레시피를 받지 못함(timeout)")
    assert again.state()["error"].startswith("웹 레시피를 받지 못함") and again.get("r1") is not None   # 사본은 그대로


# ───────── 웹 연결로 받기 ─────────
def test_uplink_syncs_on_connect_and_on_notify(tmp_path):
    web = FakeWeb().up()
    try:
        web.recipes = list(WEB_RECIPES[:1])
        rig, up = _rig_uplink(tmp_path, web)
        up.start()
        try:
            assert web.wait(lambda: rig.recipe_copy.state()["count"] == 1, 10)
            assert web.recipe_gets and web.recipe_gets[0]["equipment"] == rig.cfg["EQUIPMENT_ID"]
            assert all(a == f"Bearer {web.token}" for a in web.auth_seen)                 # 장비 토큰(사용자 로그인 없음)
            web.recipes = list(WEB_RECIPES)
            assert web.wait(lambda: web.notify_recipes() > 0, 5)
            assert web.wait(lambda: rig.recipe_copy.state()["count"] == 2, 10)
            web.recipes_status = 500                                                      # 웹 오류 — 마지막 사본 그대로
            web.notify_recipes()
            assert web.wait(lambda: bool(rig.recipe_copy.state()["error"]), 10)
            assert rig.recipe_copy.state()["count"] == 2
            ev = [m["data"] for m in rig.hub.recent if m["type"] == "event" and m["data"]["context"]["source"] == "uplink"]
            codes = [e["code"] for e in ev]
            assert codes.count("UPLINK_RECIPES") >= 2 and "UPLINK_RECIPES_FAILED" in codes
            assert all(e["level"] == "info" for e in ev if e["code"].startswith("UPLINK_RECIPES"))
        finally:
            up.stop()
    finally:
        web.down()


# ───────── 사본에서 적재 ─────────
def _rig_with_copy(tmp_path, cfg=None):
    r = R(tmp_path, dict(FAST, **(cfg or {})))
    r.rig.recipe_copy.replace(WEB_RECIPES, T1, "CHK2")
    return r


def test_load_from_copy_web_and_notebook(tmp_path):
    r = _rig_with_copy(tmp_path / "a", {"LOCAL_HMI_CONTROL": "unlock"})
    out = {}
    core = r.rig.core
    r.on(3, lambda: out.setdefault("stale", core.command("RECIPE_LOAD", {"id": "r1", "updatedAt": T1}, source=Source.HMI)))
    r.on(3, lambda: out.setdefault("missing", core.command("RECIPE_LOAD", {"id": "nope"}, source=Source.HMI)))
    r.on(3, lambda: out.setdefault("kind", core.command("RECIPE_LOAD", {"id": "h1"}, source=Source.HMI)))
    r.on(3, lambda: out.setdefault("rows", core.command("RECIPE_LOAD", {"name": "x", "rows": [ROW]}, source=Source.HMI)))
    r.on(4, lambda: out.setdefault("ok", core.command("RECIPE_LOAD", {"id": "r1", "updatedAt": T0}, source=Source.HMI)))
    r.on(5, lambda: out.setdefault("start", core.command("RECIPE_START", {}, source=Source.HMI)))
    r.run(12)
    assert out["stale"].reason == "RECIPE_COPY_STALE" and T0 in out["stale"].detail
    assert out["missing"].reason == "RECIPE_COPY_MISSING" and out["kind"].reason == "RECIPE_COPY_MISSING"
    assert out["rows"].reason == "BAD_ARGS" and "사본" in out["rows"].detail                # 노트북은 편집하지 않는다
    assert out["ok"].ok and out["start"].ok
    st = r.ev("run_start")[0].data
    assert r.rig.process.recipe_name in ("Ti 증착", "")                  # 적재 이름 = 사본 이름(시작 뒤 리스트 진행)
    assert st["params"]["recipe_id"] == "r1" and st["params"]["recipe_updated_at"] == T0
    run_start = [m["data"] for m in r.rig.hub.recent if m["type"] == "run_start"][0]
    assert run_start["recipeName"] == "Ti 증착" and run_start["params"]["recipe_updated_at"] == T0
    assert "RECIPE_LOAD" in C.hmi_kinds("unlock") and {"RECIPE_COPY_MISSING", "RECIPE_COPY_STALE"} <= C.reason_codes()


def test_load_copy_rechecks_rows_on_device(tmp_path):
    bad = dict(WEB_RECIPES[0], id="r2", rows=[dict(ROW, dc_power=99999.0)])           # 웹 검사와 상관없이 장비가 다시 검사
    r = R(tmp_path, dict(FAST))
    r.rig.recipe_copy.replace([bad], T1, "CHK2")
    out = {}
    r.on(3, lambda: out.setdefault("x", r.rig.core.command("RECIPE_LOAD", {"id": "r2", "updatedAt": T0})))
    r.run(5)
    assert out["x"].reason == "RECIPE_INVALID" and out["x"].detail


def test_web_rows_load_records_origin_and_heater_from_copy(tmp_path):
    r = _rig_with_copy(tmp_path, {"HEATER_ENABLED": True})
    out = {}
    r.on(3, lambda: out.setdefault("web", r.rig.core.command("RECIPE_LOAD", {"name": "웹", "rows": [ROW], "id": "r1",
                                                                             "updatedAt": T0})))
    r.on(4, r.cmd("RECIPE_START"))
    r.on(6, lambda: out.setdefault("heater", r.rig.core.command("HEATER_RECIPE_RUN", {"id": "h1", "updatedAt": T0})))
    r.on(6, lambda: out.setdefault("hstale", r.rig.core.command("HEATER_RECIPE_RUN", {"id": "h1", "updatedAt": T1})))
    r.on(6, lambda: out.setdefault("hnone", r.rig.core.command("HEATER_RECIPE_RUN", {})))
    r.run(10)
    assert out["web"].ok
    assert r.ev("run_start")[0].data["params"]["recipe_updated_at"] == T0
    assert out["hstale"].reason == "RECIPE_COPY_STALE" and out["hnone"].reason == "BAD_ARGS"
    assert out["heater"].reason not in ("RECIPE_COPY_MISSING", "RECIPE_COPY_STALE", "BAD_ARGS"), out["heater"]


# ───────── 장착 타겟(재고관리) ─────────
SLOTS = [
    {"locationName": "Sputter 2 - Chamber K - Gun 1", "targetUnitId": 12, "itemName": "Ti 99.99%", "barcodeCode": "T-0012",
     "materialCode": "TI", "purity": "4N", "loadedAt": "2026-11-20T00:00:00Z"},
    {"locationName": "Sputter 2 - Chamber K - Gun 2", "targetUnitId": 31, "itemName": "Cu", "barcodeCode": "C-0031",
     "materialCode": "CU", "purity": "5N", "loadedAt": "2026-11-21T00:00:00Z"},
    {"locationName": "Sputter 2 - Chamber K2 - Gun 1", "targetUnitId": 99, "itemName": "다른 챔버", "loadedAt": "2026-11-22"},
    {"locationName": "Sputter 1 - Chamber 12 - Gun 1", "targetUnitId": 98, "itemName": "챔버12", "loadedAt": "2026-11-22"},
]


def test_gun_parse_keeps_three_piece_names():
    assert TG.gun_of("Sputter 2 - Chamber K - Gun 1") == 1 and TG.gun_of("Sputter 2 - Chamber K - Gun 2") == 2
    assert TG.gun_of("chamber k gun2") == 2 and TG.gun_of("Chamber K - Gun 3") is None
    assert TG.gun_of("Sputter 2 - Chamber K2 - Gun 1") is None and TG.gun_of("Chamber 12 - Gun 1") is None
    assert TG.gun_of("Sputter 2 - Gun 1") is None and TG.gun_of(None) is None
    # v1 처럼 '-' 로 둘로 나누면 "Sputter 2 " / " Chamber K - Gun 1" — 세 조각 이름에서 Gun 을 못 찾는 것과 다르다
    assert "Sputter 2 - Chamber K - Gun 1".split("-", 1)[0].strip() == "Sputter 2"
    m = TG.parse_slots(SLOTS)
    assert m["g1"]["text"] == "Ti 99.99% (#12)" and m["g1"]["barcode"] == "T-0012" and m["g2"]["text"] == "Cu (#31)"
    assert TG.parse_slots({"slots": SLOTS[:1]})["g2"] is None
    assert TG.parse_slots([dict(SLOTS[0], itemName=None)])["g1"] is None                  # 빈 자리
    with pytest.raises(ValueError):
        TG.parse_slots("nope")


class _Slots(http.server.BaseHTTPRequestHandler):
    body = json.dumps(SLOTS).encode()
    delay = 0.0
    code = 200
    hits = 0

    def log_message(self, *a):
        pass

    def do_GET(self):
        type(self).hits += 1
        time.sleep(type(self).delay)
        self.send_response(type(self).code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(type(self).body)


@pytest.fixture
def slots_server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Slots)
    _Slots.delay, _Slots.code, _Slots.hits = 0.0, 200, 0
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}/inventory/api/chamber-slots"
    srv.shutdown()
    srv.server_close()


def test_reader_fake_http_ok_timeout_and_failure_once(slots_server):
    evs = []
    tr = TG.TargetReader(slots_server, on_event=evs.append, timeout_s=0.5)
    assert TG.is_loopback(slots_server) and not TG.is_loopback("https://vanam.synology.me/inventory/api/chamber-slots")
    assert tr.read_once() and tr.view()["g1"]["text"] == "Ti 99.99% (#12)" and tr.view()["error"] is None
    assert [e.kind for e in evs] == ["targets_changed"]
    _Slots.delay = 1.0                                                      # 제한 시간 넘김 → 실패, 예전 값 그대로
    assert not tr.read_once() and not tr.read_once()
    assert tr.view()["g1"]["text"] == "Ti 99.99% (#12)" and tr.view()["error"]
    fails = [e for e in evs if e.kind == "targets_read_failed"]
    assert len(fails) == 1 and fails[0].reason is None and "공정은 그대로" in fails[0].detail   # 이어지는 실패는 한 번만, info
    _Slots.delay, _Slots.code = 0.0, 500
    assert not tr.read_once() and len([e for e in evs if e.kind == "targets_read_failed"]) == 1
    _Slots.code = 200
    assert tr.read_once() and evs[-1].kind == "targets_read_ok"


def test_reader_thread_refresh_wait(slots_server):
    tr = TG.TargetReader(slots_server, period_s=60.0, timeout_s=0.5)
    tr.start()
    try:
        deadline = time.monotonic() + 5
        while tr.view()["readAt"] is None and time.monotonic() < deadline:
            time.sleep(0.02)
        n = _Slots.hits
        t0 = time.monotonic()
        assert tr.refresh_wait(3.0) and _Slots.hits == n + 1 and time.monotonic() - t0 < 3.0
        _Slots.delay = 0.4
        assert tr.refresh_wait(3.0)                                         # 기다리는 동안 읽기가 끝남
    finally:
        tr.stop()


def _fake_targets(m):
    class F:
        mounted = m

        def view(self):
            return {"g1": m.get("g1"), "g2": m.get("g2"), "readAt": 1_760_000_000.0, "error": None, "host": "127.0.0.1"}

        def refresh_wait(self, s):
            return True
    return F()


def test_process_start_fills_empty_targets_only(tmp_path):
    r = R(tmp_path / "a", dict(FAST))
    tr = _fake_targets(TG.parse_slots(SLOTS))
    r.rig.targets = r.rig.core.targets = tr
    row = dict(SHORT, gun1=True, gun2=True, g1_target="", g2_target="Al (레시피)")
    r.on(3, r.cmd("PROCESS_START", {"row": row}))
    r.run(8)
    p = r.ev("run_start")[0].data["params"]
    assert p["g1_target_name"] == "Ti 99.99% (#12)" and p["g2_target_name"] == "Al (레시피)"      # 레시피 값이 먼저
    ev = r.ev("targets")
    assert len(ev) == 1 and ev[0].code == "TARGETS_FILLED" and ev[0].data["g1"] == "Ti 99.99% (#12)" and "g2" not in ev[0].data
    st = r.rig.hub.last_state["data"]["targets"]
    assert st["g1"]["text"] == "Ti 99.99% (#12)" and st["readAt"].endswith("Z")
    # 쓰지 않는 Gun 은 채우지 않는다
    r = R(tmp_path / "b", dict(FAST))
    r.rig.targets = r.rig.core.targets = tr
    r.on(3, r.cmd("PROCESS_START", {"row": dict(SHORT, gun1=True, gun2=False)}))
    r.run(8)
    p = r.ev("run_start")[0].data["params"]
    assert p["g1_target_name"] == "Ti 99.99% (#12)" and p["g2_target_name"] == ""


def test_failed_read_keeps_process_going(tmp_path, slots_server):
    r = R(tmp_path, dict(FAST))
    _Slots.code = 500
    tr = TG.TargetReader(slots_server, on_event=r.rig._ev, timeout_s=0.3)
    tr.start()
    r.rig.targets = r.rig.core.targets = tr
    try:
        r.on(3, r.cmd("PROCESS_START", {"row": dict(SHORT, gun1=True)}))
        r.run(10)
    finally:
        tr.stop()
    assert all(x.ok for x in r.results)
    assert r.ev("run_start") and r.ev("run_start")[0].data["params"]["g1_target_name"] == ""
    ev = [m["data"] for m in r.rig.hub.recent if m["type"] == "event" and m["data"]["context"]["source"] == "inventory"]
    assert [e["code"] for e in ev] == ["TARGETS_READ_FAILED"] and ev[0]["level"] == "info"
    assert r.rig.hub.last_state["data"]["targets"]["error"]


def test_app_sim_never_reads_real_inventory(tmp_path):
    """sim 은 루프백 주소만(진짜 재고관리에 묻지 않음) — app.start_targets."""
    from v2.chk import app as A
    a = A.App.__new__(A.App)
    import logging
    a.log = logging.getLogger("test-targets")
    a.cfg = {"INVENTORY_API_URL": "https://vanam.synology.me/inventory/api/chamber-slots", "RUN_MODE": "sim"}

    class S:
        targets = None
        clock = type("Ck", (), {"wall": staticmethod(time.time)})()

        def _ev(self, e):
            pass
    s = S()
    s.core = type("Core", (), {"targets": None})()
    a.targets = None
    a.start_targets(s)
    assert s.targets is None and a.targets is None
    a.cfg["INVENTORY_API_URL"] = ""
    a.start_targets(s)
    assert s.targets is None
