"""웹 상시 연결(uplink, 사용자 지시 5-7) — 127.0.0.1 의 가짜 웹(fake_web)으로, 실패를 조절하며.

- 실제 시계·조립 sim(UPLINK_IN_SIM + -SIM 장비 ID): hello 맨 앞·state 1 s 마다·메시지 빠짐없이 순서대로·acked 뒤 보관함 비움,
  SSE 명령·같은 id 두 번(SSE·POST 응답)·만료·CONFIG_APPLY(by)·needWebSchema·짧은 공정·비밀값 안 나옴,
  웹 30 s 내림 → 다시 올라감, 프로그램 재시작 → 지난 실행 메시지도 올라감.
- 시험대(ChkRig, tick 없이) + Uplink: 401·409·413·500·응답 지연, 보관함 상한, 느린 소비자·UPLINK_GAP, bench.
- 단위: 다시 시도 간격, 보관함(조각 지움·다시 읽기·다시 쓰기), snapshot_summary.
"""
import datetime as dt
import json
import socket
import threading
import time

import pytest

from v2.common.commands import Source
from v2.common.paramdef import defaults
from v2.chk import params as P
from v2.chk import uplink as U
from v2.chk.app import EXIT_CONFIG, EXIT_OK, App
from v2.chk.sim.plc_sim import A
from v2.chk.system import param_schema_hash
from v2.tests.fake_web import FakeWeb

SECRETS = {p.key for p in P.PARAMS if p.scope.value == "secret"}


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _iso(sec_from_now: float) -> str:
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=sec_from_now)).isoformat().replace("+00:00", "Z")


def _cmd(cid, kind, args=None, by="op@test", exp=60.0):
    return {"id": cid, "kind": kind, "args": args or {}, "requestedBy": by, "requestedAt": _iso(0), "expiresAt": _iso(exp)}


def _cfg_dir(tmp_path, web, **extra):
    d = tmp_path / "config"
    d.mkdir(exist_ok=True)
    vals = dict({"LOCAL_DATA_DIR": str(tmp_path / "data"), "LOCAL_HMI_STATIC_DIR": str(tmp_path / "hmi"),
                 "LOCAL_HMI_PORT": _free_port(), "UPLINK_IN_SIM": True, "EQUIPMENT_ID": "CHK2-SIM",
                 "UPLINK_URL": web.url, "UPLINK_TOKEN": web.token}, **extra)
    (d / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    return d


def _start(tmp_path, web, lines=None, **extra):
    app = App(str(_cfg_dir(tmp_path, web, **extra)), print_fn=(lines.append if lines is not None else (lambda s: None)))
    assert app.start() == EXIT_OK
    for k in ("mv", "turbo", "rotary", "fv"):
        app.sims["plc"].coils[A[k]] = True
    return app


def _web_cmd_results(web, cid):
    return [m["data"] for m in web.messages("cmd_result") if m["data"]["cmdId"] == cid]


def _hub_up(hub):
    """허브가 만든(웹으로 갈) 메시지 — state·hello 제외."""
    return [e for e in list(hub.recent) if e["type"] not in ("hello",)]


# ───────────── 단위 ─────────────
def test_retry_delay_policy():
    assert U.retry_delay(401, 1) == 60.0 and U.retry_delay(401, 9) == 60.0
    assert U.retry_delay(409, 3) == 10.0
    mid = lambda: 0.5                                             # 흔들림 가운데
    assert [U.retry_delay(500, n, mid) for n in range(1, 8)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    assert U.retry_delay(None, 1, lambda: 0.0) == pytest.approx(0.8) and U.retry_delay(None, 1, lambda: 1.0) == pytest.approx(1.2)
    assert U.retry_delay(None, 20, lambda: 1.0) == 30.0          # 흔들림으로도 30 s 를 넘지 않는다


def _env(i, type_="event", level="info", size=0):
    return {"id": f"m{i:05d}", "instance": "x", "seq": i, "ts": "2026-10-21T00:00:00.000Z", "type": type_,
            "data": {"level": level, "code": "C", "message": "x" * size}}


def test_spool_ack_deletes_files_and_survives_restart(tmp_path):
    sp = U.Spool(tmp_path / "up", 10_000_000)
    for i in range(1, 2001):
        sp.put(_env(i, size=200))
    assert sp.count == 2000 and sp.files() >= 2                     # 조각 여러 개
    b = sp.batch(500, 10_000_000)
    assert [k for k, _ in b] == [f"m{i:05d}" for i in range(1, 501)]  # 오래된 것부터
    assert sp.batch(10, 1000)[0][0] == "m00001" and len(sp.batch(10, 1000)) <= 4   # 바이트 상한
    sp.ack([k for k, _ in b] + ["없는 id"])
    assert sp.count == 1500
    sp.close()
    sp2 = U.Spool(tmp_path / "up", 10_000_000)                         # 다시 켬 — 남은 것만, 순서대로
    assert sp2.loaded == 1500 and sp2.batch(1, 10_000_000)[0][0] == "m00501"
    sp2.ack([k for k, _ in sp2.batch(5000, 100_000_000)])
    assert sp2.count == 0 and sp2.files() == 0 and sp2.live_bytes == 0   # 모두 지우면 조각 파일도 없다
    sp2.put(_env(9999))
    assert sp2.files() == 1


def test_spool_partial_acks_are_compacted(tmp_path):
    sp = U.Spool(tmp_path / "up", 400_000)
    keep = []
    for i in range(1, 20001):                                        # 조각마다 하나씩만 남기고 지운다
        sp.put(_env(i, level="warn", size=150))
        if i % 300 == 0:
            keep.append(f"m{i:05d}")
        else:
            sp.ack([f"m{i:05d}"])
    assert sp.count == len(keep) and sp.disk_bytes() <= 2 * 400_000 + U.SEG_BYTES + 400
    assert [k for k, _ in sp.batch(10_000, 10_000_000)] == keep          # 다시 써도 순서 유지
    sp.close()
    assert U.Spool(tmp_path / "up", 400_000).loaded == len(keep)


def test_spool_full_drops_low_priority_first(tmp_path):
    sp = U.Spool(tmp_path / "up", 60_000)
    high = []
    for i in range(1, 401):
        t = ("event", "info") if i % 4 else (("run_end", None) if i % 8 else ("event", "warn"))
        if i % 10 == 0:
            t = ("snapshot_summary", None)
        sp.put(_env(i, t[0], t[1] or "info", size=300))
        if U.priority(_env(i, t[0], t[1] or "info")) == U.HIGH:
            high.append(f"m{i:05d}")
    left = [k for k, _ in sp.batch(10_000, 10_000_000)]
    assert sp.dropped > 0 and sp.live_bytes <= 60_000
    assert set(high) <= set(left)                                      # 높은 것은 하나도 안 버림
    assert sp.dropped_by_type["event"] + sp.dropped_by_type["snapshot_summary"] == sp.dropped
    for i in range(401, 800):                                          # 높은 것만으로 차면 그때는 오래된 높은 것부터
        sp.put(_env(i, "run_end", size=300))
    assert sp.live_bytes <= 60_000 and sp.batch(1, 10**7)[0][0] != "m00004"


def test_snapshot_summary_aligned_periods():
    s = U.Summary()
    out = []
    t0 = 1_760_000_040.0                                              # 정각(60 의 배수)
    for k in range(125):
        row = {"ts": "x", "seq": k, "dc.p": float(k), "plc.mv": True, "plc.q": "ok" if k % 4 else "stale",
               "plc.age_s": 0.1 * (k % 5), "mfc.q": "ok", "mfc.age_s": 0.0, "proc.state": "RUNNING", "proc.run_id": "r1"}
        r = s.add(t0 + 30 + k, row, 60)
        if r:
            out.append(r)
    assert len(out) == 2
    a, b = out
    assert a["from"].endswith(":00.000Z") and a["rows"] == 30 and b["rows"] == 60    # 첫 구간은 시작부터 정각까지
    assert b["values"]["dc.p"] == {"last": 89.0, "mean": 59.5, "min": 30.0, "max": 89.0}
    assert "plc.mv" not in b["values"] and "seq" not in b["values"] and "proc.state" not in b["values"]
    assert b["quality"]["plc"] == {"okRatio": 0.75, "maxAge": 0.4} and b["quality"]["mfc"]["okRatio"] == 1.0
    assert a["runId"] == b["runId"] == "r1"


def test_snapshot_summary_only_while_process_runs():
    """공정 밖 구간은 요약 없음 — 구간 안 행에 proc.run_id 가 하나라도 있으면 내고 runId 는 구간의 마지막 run id(사용자 결정 2026-10-22)."""
    s = U.Summary()
    t0 = 1_760_000_040.0
    plan = ([None] * 60                                              # 구간 1: 공정 없음
            + [None] * 20 + ["ra"] * 30 + [None] * 10                # 구간 2: 중간에 공정 ra
            + ["ra"] * 25 + [None] * 5 + ["rb"] * 30                 # 구간 3: ra 끝 → rb
            + [None] * 60                                            # 구간 4: 공정 없음
            + [None])                                                # 구간 4 를 닫는 행
    out = []
    for k, rid in enumerate(plan):
        r = s.add(t0 + k, {"dc.p": 1.0, "dc.q": "ok", "dc.age_s": 0.0, "proc.run_id": rid}, 60)
        if r:
            out.append(r)
    assert [(o["runId"], o["rows"], o["from"]) for o in out] == [
        ("ra", 60, U.utc_iso(t0 + 60)), ("rb", 60, U.utc_iso(t0 + 120))]


# ───────────── 실제 시계 — 조립 sim + 가짜 웹 ─────────────
def test_uplink_app_normal_commands_schema_and_secrets(tmp_path):
    from v2.chk import web_schema as WS
    web = FakeWeb(keepalive_s=1.0).up()
    lines = []
    app = _start(tmp_path, web, lines)
    up, hub = app.uplink, app.system.hub
    calls = []
    real = app.system.core.command

    def counting(kind, args=None, **kw):
        calls.append((kind, kw.get("cid")))
        return real(kind, args, **kw)
    app.system.core.command = counting
    try:
        assert web.wait(lambda: web.states >= 2 and web.sse_connects >= 1, 8)
        first = next(p for p in web.posts if p["code"] == 200)
        assert first["types"][0] == "hello" and first["instance"] == hub.instance
        h = web.messages("hello")[0]["data"]
        assert h["runMode"] == "sim" and h["equipment"] == "CHK2-SIM" and h["instance"] == hub.instance
        assert abs(U.parse_iso(h["deviceUtc"]) - time.time()) < 5 and "webSchema" not in h
        assert web.sse_queries[0] == {"equipment": "CHK2-SIM", "instance": hub.instance}
        s0, t0 = web.states, time.monotonic()
        time.sleep(5.0)
        rate = (web.states - s0) / (time.monotonic() - t0)
        assert 0.7 < rate < 1.3, rate                                   # state 는 POST(1 s)마다 하나
        assert all(p["types"].count("state") <= 1 for p in web.posts if p.get("types"))
        # SSE 명령 → cmd_result(by = requestedBy)
        web.push(_cmd("c-stop", "PROCESS_STOP", by="a@test"))
        assert web.wait(lambda: _web_cmd_results(web, "c-stop"), 5)
        r = _web_cmd_results(web, "c-stop")[0]
        assert (r["status"], r["source"], r["by"], r["kind"]) == ("accepted", "web", "a@test", "PROCESS_STOP")
        # 같은 id 를 SSE·POST 응답으로 두 번 → 한 번만 실행, 결과는 두 번 같게
        dup = _cmd("c-dup", "PROCESS_STOP")
        web.push(dup)
        assert web.wait(lambda: _web_cmd_results(web, "c-dup"), 5)
        web.post_commands = [dup]
        assert web.wait(lambda: len(_web_cmd_results(web, "c-dup")) == 2, 5)
        a, b = _web_cmd_results(web, "c-dup")
        assert a == b and calls.count(("PROCESS_STOP", "c-dup")) == 1 and up.dup_cmds == 1
        # 만료 → COMMAND_EXPIRED(실행 안 함)
        web.push(_cmd("c-old", "PROCESS_STOP", exp=-30))
        assert web.wait(lambda: _web_cmd_results(web, "c-old"), 5)
        r = _web_cmd_results(web, "c-old")[0]
        assert (r["status"], r["reasonCode"]) == ("rejected", "COMMAND_EXPIRED") and ("PROCESS_STOP", "c-old") not in calls
        web.push(_cmd("c-skew", "PROCESS_STOP", exp=-2))                 # 허용 시계 차(5 s) 안 — 실행
        assert web.wait(lambda: _web_cmd_results(web, "c-skew"), 5)
        assert _web_cmd_results(web, "c-skew")[0]["status"] == "accepted"
        # CONFIG_APPLY → config_ack(by = requestedBy)
        vals = {k: x for k, x in defaults(P.PARAMS).items() if k not in SECRETS}
        vals.update(FLOW_ERROR_SEC=3, RUN_MODE="sim", EQUIPMENT_ID="CHK2-SIM", UPLINK_IN_SIM=True)
        web.push(_cmd("c-cfg", "CONFIG_APPLY", {"version": 1, "values": vals, "schemaHash": param_schema_hash()},
                      by="admin@test"))
        assert web.wait(lambda: web.messages("config_ack"), 5)
        ack = web.messages("config_ack")[0]["data"]
        assert (ack["status"], ack["by"], ack["appliedKeys"]) == ("applied", "admin@test", ["FLOW_ERROR_SEC"])
        assert app.system.cfg["FLOW_ERROR_SEC"] == 3 and _web_cmd_results(web, "c-cfg")[0]["by"] == "admin@test"
        # needWebSchema → webSchema 를 실은 hello 한 번 더
        web.need_schema = True
        assert web.wait(lambda: any("webSchema" in m["data"] for m in web.messages("hello")), 5)
        ws = [m["data"]["webSchema"] for m in web.messages("hello") if "webSchema" in m["data"]]
        assert len(ws) == 1 and ws[0]["hash"] == WS.content_hash() == h["webSchemaHash"]
        # 짧은 공정(SSE PROCESS_START) — run_start·run_end
        web.push(_cmd("c-run", "PROCESS_START", {"blocks": [{"type": "wait", "params": {"sec": 2}}]}, by="run@test"))
        assert web.wait(lambda: web.messages("run_end"), 40)
        rs, re_ = web.messages("run_start")[0]["data"], web.messages("run_end")[0]["data"]
        assert rs["by"] == "run@test" and rs["runId"] == re_["runId"] and re_["result"] == "done"
        # 빠짐없이·순서대로·한 번(웹 저장은 id 로) — 허브가 만든 메시지가 모두 웹에, seq 순서로
        time.sleep(1.5)
        assert web.wait(lambda: up.spool.count == 0, 5)
        want = [e["id"] for e in _hub_up(hub)]
        got = {e["id"]: e for e in web.messages()}
        assert want and set(want) <= set(got)
        seqs = [e["seq"] for e in web.messages() if e["type"] != "hello" and e["instance"] == hub.instance]
        assert seqs == sorted(seqs) and web.dups == 0
        assert up.spool.files() == 0                                      # acked 뒤 보관함 비움(조각 파일도)
        st = hub.last_state["data"]["uplink"]
        assert st["enabled"] and st["connected"] and st["queued"] <= 5 and st["lastError"] is None and st["dropped"] == 0
        assert st["host"] == "127.0.0.1"
    finally:
        app.shutdown()
        web.down()
    # 비밀값(토큰·주소 경로)이 로그·콘솔·메시지·보관함 어디에도 없다
    log = (tmp_path / "data" / "logs" / "chk_v2.log").read_text(encoding="utf-8")
    msgs = json.dumps(web.messages(), ensure_ascii=False)
    spool = "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "data" / "uplink").glob("*"))
    for text in (log, "\n".join(lines), msgs, spool):
        assert web.token not in text and web.url not in text and "/dev/chk" not in text
    assert "웹 연결 127.0.0.1(http)" in lines and any("https 가 아님" in s for s in lines)
    assert web.auth_seen and all(a == f"Bearer {web.token}" for a in web.auth_seen)


@pytest.mark.slow
def test_uplink_outage_30s_and_restart_keep_messages(tmp_path):
    web = FakeWeb(keepalive_s=1.0).up()
    app = _start(tmp_path, web)
    hub = app.system.hub
    app.system.cfg["UPLINK_DOWN_SEC"] = 10                              # live 키 — 같은 사전
    try:
        assert web.wait(lambda: web.messages("hello") and web.sse_connects, 8)
        n_hello = len(web.messages("hello"))
        web.down()                                                       # 웹 30 s 내림
        t0 = time.monotonic()
        k = 0
        while time.monotonic() - t0 < 30:
            app.system.core.command("PROCESS_STOP", {}, cid=f"o-{k}", source=Source.HMI)
            k += 1
            time.sleep(3)
        st = app.uplink.status()
        assert not st["connected"] and st["lastError"] and st["queued"] >= k
        codes = [e["data"]["code"] for e in hub.recent if e["type"] == "event"]
        assert "UPLINK_DOWN" in codes and "UPLINK_UP" not in codes
        web.up()                                                         # 같은 포트로 올림
        want = [e["id"] for e in _hub_up(hub)]
        assert web.wait(lambda: set(want) <= {m["id"] for m in web.messages()}, 40), "끊긴 동안 쌓인 메시지가 다 올라가지 않음"
        assert len([d for d in (m["data"] for m in web.messages("cmd_result")) if d["cmdId"].startswith("o-")]) == k
        assert web.wait(lambda: any(e["data"]["code"] == "UPLINK_UP" for e in web.messages("event")), 10)
        assert len(web.messages("hello")) > n_hello                      # 다시 연결 → hello 맨 앞
        again = [p for p in web.posts if p["code"] == 200 and p.get("types")]
        assert any(p["types"][0] == "hello" for p in again[1:])
        assert web.wait(lambda: app.uplink.spool.count == 0 and app.uplink.status()["connected"], 35)
        # 프로그램 재시작 — 웹이 받지 않는 동안(503) 쌓인 것이 다음 실행에서 올라간다
        web.status = 503
        time.sleep(1.5)
        mark = hub.seq
        for i in range(3):
            app.system.core.command("PROCESS_STOP", {}, cid=f"r-{i}")
        time.sleep(1.0)
    finally:
        app.shutdown()
    old_inst = hub.instance
    pending = [e["id"] for e in _hub_up(hub) if e["seq"] > mark]
    assert len(pending) >= 3 and (tmp_path / "data" / "uplink").exists()
    web.status = None
    app2 = _start(tmp_path, web)
    try:
        assert app2.uplink.spool.loaded >= len(pending)
        assert web.wait(lambda: set(pending) <= {m["id"] for m in web.messages()}, 15)
        olds = [m for m in web.messages() if m["id"] in pending]
        assert {m["instance"] for m in olds} == {old_inst} != {app2.system.hub.instance}
        assert any(p.get("instance") == app2.system.hub.instance for p in web.posts)
        assert web.wait(lambda: app2.uplink.spool.count == 0, 10)
    finally:
        app2.shutdown()
        web.down()


def test_sim_default_no_uplink_and_sim_id_rule(tmp_path):
    web = FakeWeb()
    lines = []
    d = tmp_path / "a"
    d.mkdir()
    app = App(str(_cfg_dir(d, web, UPLINK_IN_SIM=False, EQUIPMENT_ID="CHK2")), print_fn=lines.append)
    assert app.start() == EXIT_OK
    try:
        assert app.uplink is None and "웹 연결 끔(sim)" in lines
        assert app.system.hub.state_data({})["uplink"] == {"enabled": False}
    finally:
        app.shutdown()
    out = []
    d = tmp_path / "b"
    d.mkdir()
    bad = App(str(_cfg_dir(d, web, EQUIPMENT_ID="CHK2")), print_fn=out.append)       # UPLINK_IN_SIM 인데 -SIM 아님
    assert bad.start() == EXIT_CONFIG and bad.server is None and bad.system is None
    assert any("-SIM 으로 끝나지 않음" in s for s in out)


# ───────────── 시험대 + Uplink(tick 없이) ─────────────
def _rig_uplink(tmp_path, web, token=None, mode="sim", **cfg):
    from v2.chk.sim.rig import ChkRig
    from v2.common.clock import VirtualClock
    rig = ChkRig(VirtualClock(start_wall=time.time()), tmp_path, mode=mode,
                 cfg=dict({"UPLINK_INTERVAL_MS": 500, "UPLINK_TIMEOUT_SEC": 2.0, "UPLINK_DOWN_SEC": 5}, **cfg))
    up = U.Uplink(rig, web.url, web.token if token is None else token, tmp_path / "data")
    rig.uplink = up
    return rig, up


def _codes(rig, code):
    return [e for e in list(rig.hub.recent) if e["type"] == "event" and e["data"]["code"] == code]


def _info(rig, i):
    rig.hub._event("info", "T", None, f"m{i}", {"source": "test"})


def test_uplink_http_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(U, "AUTH_RETRY_S", 0.5)
    monkeypatch.setattr(U, "CONFLICT_RETRY_S", 0.3)
    web = FakeWeb(keepalive_s=0.5).up()
    # 401 — 경고 한 번, AUTH_RETRY_S 마다만 다시
    rig, up = _rig_uplink(tmp_path / "a", web, token="wrong")
    up.start()
    try:
        time.sleep(2.2)
        n401 = sum(1 for p in web.posts if p["code"] == 401)
        assert 3 <= n401 <= 6 and len(_codes(rig, "UPLINK_AUTH_FAILED")) == 2     # POST·SSE 각각 한 번
        assert up.status()["lastError"] == "HTTP 401" and not up.status()["connected"]
    finally:
        up.stop()
    web.posts.clear()
    rig, up = _rig_uplink(tmp_path / "b", web)
    up.start()
    try:
        # 409 — 경고 한 번, 10 s(여기서는 0.3 s) 마다
        web.status, web.sse_status = 409, 409
        time.sleep(1.5)
        assert len(_codes(rig, "UPLINK_CONFLICT")) == 2 and sum(1 for p in web.posts if p["code"] == 409) >= 3
        web.status = web.sse_status = None
        assert web.wait(lambda: up.status()["lastError"] is None and up.status()["connected"], 3)
        # 500 — 1→2→4 s(흔들림 ±20 %), UPLINK_DOWN_SEC(5 s) 넘으면 UPLINK_DOWN, 되면 UPLINK_UP
        web.posts.clear()
        web.status = 500
        time.sleep(7.5)
        ts = [p["t"] for p in web.posts if p["code"] == 500]
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        assert len(gaps) >= 2 and 0.75 <= gaps[0] <= 1.35 and 1.55 <= gaps[1] <= 2.5, gaps
        assert len(_codes(rig, "UPLINK_DOWN")) == 1 and up.status()["lastError"] == "HTTP 500"
        web.status = None
        assert web.wait(lambda: _codes(rig, "UPLINK_UP"), 12)                  # 지금 간격은 8 s(±20 %)
        hello_after = [p for p in web.posts if p["code"] == 200][0]
        assert hello_after["types"][0] == "hello"                              # 다시 연결 → hello 맨 앞
        # 응답 지연(UPLINK_TIMEOUT_SEC 2 s 넘음) → timeout
        web.delay_s = 3.0
        assert web.wait(lambda: up.status()["lastError"] == "timeout", 6)
        web.delay_s = 0.0
        assert web.wait(lambda: up.status()["lastError"] is None, 8)
        # 413 — 묶음을 나눠 다시, 모두 저장
        web.max_messages = 4
        ids = []
        for i in range(30):
            _info(rig, i)
            ids.append(rig.hub.recent[-1]["id"])
        assert web.wait(lambda: set(ids) <= {m["id"] for m in web.messages()}, 10)
        assert any(p["code"] == 413 for p in web.posts) and up.batch_n <= 3
        assert all(p["n"] <= 4 for p in web.posts if p["code"] == 200)
    finally:
        up.stop()
        web.down()


def test_uplink_spool_full_and_slow_consumer_gap(tmp_path):
    web = FakeWeb(keepalive_s=0.5)                                          # 웹 내려 둔 채
    web.port = _free_port()
    rig, up = _rig_uplink(tmp_path, web)
    up.spool.max_bytes = 40_000
    up.start()
    try:
        for i in range(300):
            _info(rig, i)
            if i % 50 == 0:
                rig.hub._event("warn", "W", None, f"w{i}", {"source": "test"})
        time.sleep(1.0)
        full = _codes(rig, "UPLINK_SPOOL_FULL")
        assert full and up.spool.dropped > 0 and up.spool.live_bytes <= 40_000
        kept = {json.loads(ln)["data"]["message"] for _, ln in up.spool.batch(10_000, 10**8)}
        assert {f"w{i}" for i in range(0, 300, 50)} <= kept                    # warn 은 버리지 않음
        assert up.status()["dropped"] == up.spool.dropped
    finally:
        up.stop()
    # 느린 소비자 — 허브가 끊으면 마지막 seq 로 다시 붙어 링 버퍼에서 이어 받는다
    rig, up = _rig_uplink(tmp_path / "g", web)
    up.sub_queue = 5
    gate = threading.Event()
    real_put = up.spool.put

    def slow_put(env):
        gate.wait(5)
        return real_put(env)
    up.spool.put = slow_put
    up.start()
    try:
        want = []
        for i in range(40):                                                  # 링 버퍼(500) 안 — 빠짐 없음
            _info(rig, i)
            want.append(rig.hub.recent[-1]["id"])
        gate.set()
        assert web.wait(lambda: up.spool.count >= 40, 5)
        assert up.resubscribes >= 1 and rig.hub.dropped_subs >= 1 and not _codes(rig, "UPLINK_GAP")
        got = [k for k, _ in up.spool.batch(10_000, 10**8)]
        assert [k for k in got if k in want] == want
        gate.clear()
        for i in range(700):                                                 # 링 버퍼보다 많이 → UPLINK_GAP
            _info(rig, i)
        gate.set()
        assert web.wait(lambda: _codes(rig, "UPLINK_GAP"), 5)
        g = _codes(rig, "UPLINK_GAP")[0]["data"]["context"]
        assert g["toSeq"] >= g["fromSeq"]
    finally:
        gate.set()
        up.stop()


def test_bench_uploads_but_write_commands_read_only(tmp_path):
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = _rig_uplink(tmp_path, web, mode="bench")
    up.start()
    try:
        _info(rig, 1)
        ev = rig.hub.recent[-1]["id"]
        assert web.wait(lambda: ev in {m["id"] for m in web.messages()} and web.sse_connects, 5)
        assert web.messages("hello")[0]["data"]["runMode"] == "bench"
        web.push(_cmd("b-stop", "PROCESS_STOP"))
        web.push(_cmd("b-load", "RECIPE_LOAD", {"name": "x", "blocks": [{"type": "wait", "params": {"sec": 1}}]}))
        assert web.wait(lambda: _web_cmd_results(web, "b-stop") and _web_cmd_results(web, "b-load"), 5)
        assert _web_cmd_results(web, "b-stop")[0]["reasonCode"] == "BENCH_READ_ONLY"
        assert _web_cmd_results(web, "b-load")[0]["status"] == "accepted"       # 장비에 쓰지 않는 명령은 받는다
    finally:
        up.stop()
        web.down()


def _result_delays(web, up, n, prefix):
    out = []
    for i in range(n):
        time.sleep(0.13 * (i % 7) + 0.05)                               # POST 주기 안 여러 자리에서
        cid = f"{prefix}{i}"
        t0 = time.monotonic()
        assert web.push(_cmd(cid, "PROCESS_STOP"))
        assert web.wait(lambda: _web_cmd_results(web, cid), 3), cid
        out.append(time.monotonic() - t0)
    return out


def test_high_message_posts_right_away_but_not_during_backoff(tmp_path):
    """HIGH 메시지(cmd_result) → 직전 POST 성공·200 ms 지남이면 1 s 주기를 기다리지 않고 바로 POST. 실패 대기 중에는 깨우지 않는다."""
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = _rig_uplink(tmp_path, web, UPLINK_INTERVAL_MS=1000)
    up.start()
    try:
        assert web.wait(lambda: web.sse_connects and any(p["code"] == 200 for p in web.posts), 5)
        e0 = up.early_posts
        d = _result_delays(web, up, 8, "fast-")
        assert max(d) < 0.6 and sum(d) / len(d) < 0.4, d                 # 1 s 주기면 평균 0.5 s·최대 1 s
        assert up.early_posts - e0 >= 6
        # 정보 사건(LOW)은 깨우지 않는다 — 다음 주기에
        web.wait(lambda: False, 0.3)
        n0 = len(web.posts)
        _info(rig, 1)
        time.sleep(0.15)
        assert len(web.posts) == n0 or web.posts[-1]["t"] - web.posts[n0 - 1]["t"] > 0.8
        # 실패 대기 중(500 백오프 1→2 s)에는 HIGH 가 와도 POST 하지 않는다
        web.status = 500
        assert web.wait(lambda: sum(1 for p in web.posts if p["code"] == 500) >= 2, 5)
        t_fail = [p["t"] for p in web.posts if p["code"] == 500][-1]
        for i in range(3):
            assert web.push(_cmd(f"bo-{i}", "PROCESS_STOP"))
        time.sleep(1.2)                                                  # 두 번째 실패 뒤 대기 2 s(±20 %) ≥ 1.6 s
        assert not [p for p in web.posts if p["t"] > t_fail], "백오프 중에 POST 함"
        web.status = None
        assert web.wait(lambda: all(_web_cmd_results(web, f"bo-{i}") for i in range(3)), 8)
    finally:
        up.stop()
        web.down()
    t0 = time.monotonic()
    rig2, up2 = _rig_uplink(tmp_path / "s", web)                         # 멈출 때는 바로 끝난다(웹 없음)
    up2.start()
    rig2.hub._event("warn", "T", None, "w", {"source": "test"})
    up2.stop()
    assert time.monotonic() - t0 < 3.0 and not any(t.is_alive() for t in up2._threads)


def test_nan_and_inf_become_null_in_spool_and_post(tmp_path):
    """NaN·inf → 보관함 줄·POST 본문 모두 표준 JSON(null). 웹 JSON.parse 는 NaN 을 못 읽어 400 → 보관함이 멈추기 때문."""
    from v2.chk import local_server as LS
    from v2.common.messages import dumps_json
    strict = lambda b: json.loads(b, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
    bad = {"a": float("nan"), "b": [float("inf"), -float("inf"), 1.5], "c": {"d": float("nan")}, "e": "NaN"}
    for raw in (dumps_json(bad), LS._dumps(bad), U._dumps(bad)):
        assert strict(raw) == {"a": None, "b": [None, None, 1.5], "c": {"d": None}, "e": "NaN"}
    sp = U.Spool(tmp_path / "sp", 1_000_000)
    sp.put(dict(_env(1), data={"level": "warn", "v": float("nan")}))
    (seg,) = (tmp_path / "sp").glob("seg-*.jsonl")
    assert strict(seg.read_bytes().strip())["data"]["v"] is None
    sp.close()
    web = FakeWeb(keepalive_s=0.5).up()
    rig, up = _rig_uplink(tmp_path / "w", web)
    up.start()
    try:
        rig.hub._event("warn", "NAN_TEST", None, "nan", {"source": "test", "v": float("nan"), "w": float("inf")})
        eid = rig.hub.recent[-1]["id"]
        rig.hub.publish("state", {"snapshot": {"dc.p": float("nan"), "ig.ion_torr": float("inf"), "proc.run_id": None}})
        assert web.wait(lambda: eid in {m["id"] for m in web.messages()} and web.states >= 1, 5)
        ctx = web.messages("event")[-1]["data"]["context"]
        assert ctx["v"] is None and ctx["w"] is None
        assert web.posts and all(p["code"] == 200 for p in web.posts)       # 400 없음
    finally:
        up.stop()
        web.down()
