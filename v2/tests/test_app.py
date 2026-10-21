"""실행 프로그램(사용자 지시 5-6) — 조립·진입점·로컬 서버·장비 메시지·설정 적용(웹 경로)·종료 처리.

COM 포트·외부 네트워크 없이(sim, 127.0.0.1 만). 기록·설정은 임시 폴더에만.
- 실제 시계: 조립 sim 을 임시 폴더로 시작 → hello·schema → stream 에서 2 s 안에 state → POST PROCESS_STOP(HMI) ok·cmd_result →
  POST CONFIG_APPLY(HMI) SOURCE_NOT_ALLOWED → 정상 종료(포트·스레드 정리). 같은 포트로 두 번째 실행은 바로 끝(장비 포트 안 엶).
- 가상 시계(ChkRig — 같은 조립): 설정 적용(live·idle·restart·S48·rollback·config_ack·이력·실행 모드·장비 ID 잠금),
  종료(Ctrl+C 거부·두 번 확인·강제 종료).
"""
import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest

from v2.common.commands import Source
from v2.common.config_store import HISTORY
from v2.common.paramdef import defaults
from v2.chk import params as P
from v2.chk.app import CONFIRM_HEATER, EXIT_OK, EXIT_RUNNING, REFUSE_PROCESS, App, git_version
from v2.chk.hub import CLOSED, MessageHub
from v2.chk.safety import View
from v2.chk.sim.plc_sim import A
from v2.chk.system import param_schema_hash
from v2.tests.test_left_outputs import E_BLOCKS
from v2.tests.test_process_engine import FAST, R, SHORT

SECRETS = {p.key for p in P.PARAMS if p.scope.value == "secret"}


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _cfg_dir(tmp_path, port, **extra):
    d = tmp_path / "config"
    d.mkdir()
    vals = dict({"LOCAL_DATA_DIR": str(tmp_path / "data"), "LOCAL_HMI_STATIC_DIR": str(tmp_path / "hmi"),
                 "LOCAL_HMI_PORT": port}, **extra)
    (d / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    return d


def _get(base, path):
    return json.load(urllib.request.urlopen(base + path, timeout=5))


def _post(base, body):
    req = urllib.request.Request(base + "/api/v1/command", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=5))


def _sse(resp, until, limit_s):
    """SSE 를 읽어 (event, envelope) 목록 — until(목록)이 참이거나 limit_s 가 지나면 끝."""
    out, ev, t0 = [], None, time.monotonic()
    while time.monotonic() - t0 < limit_s:
        line = resp.readline().decode("utf-8").rstrip("\n")
        if line.startswith("event: "):
            ev = line[7:]
        elif line.startswith("data: "):
            out.append((ev, json.loads(line[6:])))
            if until(out):
                break
    return out


# ───────────── 실제 시계 — 조립 sim 시작·로컬 서버·정상 종료 ─────────────
def test_app_sim_start_api_and_clean_shutdown(tmp_path):
    port = _free_port()
    lines = []
    app = App(str(_cfg_dir(tmp_path, port)), print_fn=lines.append)
    assert app.start() == EXIT_OK
    base = f"http://127.0.0.1:{port}"
    try:
        assert lines[0].startswith("챔버K v2 — 모드 sim · 장비 CHK2 · 버전 ")
        h = _get(base, "/api/v1/hello")
        assert h["runMode"] == "sim" and h["equipment"] == "CHK2" and h["program"] == "chk-v2"
        assert h["configVersion"] == 0 and h["webSchemaHash"] and "PROCESS_STOP" in h["commandKinds"]
        assert "CONFIG_APPLY" not in h["commandKinds"] and h["snapshotColumns"][0] == "ts"
        sch = _get(base, "/api/v1/schema")
        assert sch["hash"] == h["webSchemaHash"] and sch["configVersion"] == 0
        assert any(p["key"] == "LOCAL_HMI_STATIC_DIR" for p in sch["params"])
        page = urllib.request.urlopen(base + "/", timeout=5).read().decode("utf-8")
        assert "로컬 화면 파일 없음" in page and "/api/v1/hello" in page
        with pytest.raises(urllib.error.HTTPError):
            urllib.request.urlopen(base + "/../config/local_override.json", timeout=5)
        stream = urllib.request.urlopen(base + "/api/v1/stream", timeout=5)
        got = _sse(stream, lambda o: any(e == "state" for e, _ in o), 2.0)
        st = [d for e, d in got if e == "state"]
        assert st, "2 s 안에 state 없음"
        assert st[0]["data"]["procState"] == "IDLE" and "plc" in st[0]["data"]["links"]
        r = _post(base, {"id": "c-stop", "kind": "PROCESS_STOP", "args": {}})
        assert r == {"id": "c-stop", "ok": True, "reason": None}
        r2 = _post(base, {"id": "c-cfg", "kind": "CONFIG_APPLY", "args": {"version": 1, "values": {}, "schemaHash": "x"}})
        assert r2 == {"id": "c-cfg", "ok": False, "reason": "SOURCE_NOT_ALLOWED"}
        got = _sse(stream, lambda o: sum(1 for e, _ in o if e == "cmd_result") >= 2, 3.0)
        res = {d["data"]["cmdId"]: d["data"] for e, d in got if e == "cmd_result"}
        assert res["c-stop"]["status"] == "accepted" and res["c-stop"]["source"] == "hmi"
        assert res["c-cfg"]["status"] == "rejected" and res["c-cfg"]["reasonCode"] == "SOURCE_NOT_ALLOWED"
        stream.close()
        assert app.exit_check() == ("ok", "")
        assert app.on_ctrl_c() == "exit"
    finally:
        app.shutdown()
    time.sleep(0.3)
    names = {t.name for t in threading.enumerate()}
    assert not {n for n in names if n.startswith(("worker-", "tick", "local-server", "plc-fast", "recorder"))}, names
    assert not any(w._thread.is_alive() for w in app.system.workers.values() if w._thread is not None)
    s = socket.socket()                                           # 포트가 풀렸다
    s.bind(("127.0.0.1", port))
    s.close()
    log = (tmp_path / "data" / "logs" / "chk_v2.log").read_text(encoding="utf-8")
    assert "히터 RUN=0 쓰기: " in log and "시간 초과" not in log
    assert "조치 실패" not in log and " ERROR " not in log            # S41 쓰기가 끝난 뒤 포트를 닫는다(CANCELLED 없음)


def test_second_instance_on_same_port_exits_before_opening_device_ports(tmp_path):
    port = _free_port()
    first = App(str(_cfg_dir(tmp_path, port)), print_fn=lambda s: None)
    assert first.load_config() and first.bind_server()                # 첫 실행이 포트를 잡고 있다
    try:
        out = []
        second = App(str(tmp_path / "config"), print_fn=out.append)
        t0 = time.monotonic()
        assert second.start() == EXIT_RUNNING
        assert time.monotonic() - t0 < 2.0
        assert second.system is None and second.sims is None              # 조립 전에 끝 — 장비 포트(시뮬레이터)를 열지 않음
        assert any("이미 실행 중" in s for s in out)
    finally:
        first.server.stop()


def test_git_version_reads_files_only(tmp_path):
    g = tmp_path / ".git"
    (g / "refs" / "heads" / "rewrite").mkdir(parents=True)
    (g / "HEAD").write_text("ref: refs/heads/rewrite/v2\n")
    (g / "refs" / "heads" / "rewrite" / "v2").write_text("96284e6d15421599cdc51dc03170813ed987c7f2\n")
    assert git_version(tmp_path) == "rewrite/v2@96284e6"
    (g / "refs" / "heads" / "rewrite" / "v2").unlink()
    (g / "packed-refs").write_text("# pack\nabcdef0123456789 refs/heads/rewrite/v2\n")
    assert git_version(tmp_path) == "rewrite/v2@abcdef0"
    assert git_version(tmp_path / "none") == "unknown"


# ───────────── 장비 메시지 — 링 버퍼·느린 소비자 ─────────────
def test_hub_ring_buffers_and_slow_consumer_does_not_block(tmp_path):
    r = R(tmp_path, FAST)
    hub = r.rig.hub
    slow = hub.subscribe(maxsize=5, name="slow")                  # 읽지 않는 소비자
    t_close = []
    r.rig.loop.tick_hooks.append(lambda t: t_close.append(time.perf_counter()))
    r.run(30)
    assert slow.closed and slow.reason == "slow" and slow not in hub.subs and hub.dropped_subs >= 1
    assert len(hub.recent) <= hub.recent.maxlen and r.rig.loop.rows == 30
    assert r.rig.events.maxlen and r.rig.process.events.maxlen and r.rig.safety.engine.events.maxlen
    assert r.rig.loop.jitters.maxlen and r.rig.safety.poll_eval_s.maxlen and r.rig.heater.events.maxlen
    seqs = [e["seq"] for e in hub.recent]
    assert seqs == sorted(seqs) and hub.last_state["type"] == "state"
    # 다시 붙으면: 최신 state 하나 + Last-Event-ID 뒤의 최근 메시지
    late = hub.subscribe(last_seq=seqs[-3])
    first = [late.get(0.1) for _ in range(3)]
    assert first[0]["type"] == "state" and [e["seq"] for e in first[1:]] == seqs[-2:]
    hub.close()
    assert late.get(0.1) is not None and late.closed


def test_hub_ring_buffer_is_bounded():
    class _S:
        class clock:
            @staticmethod
            def wall():
                return 1_760_000_000.0
    hub = MessageHub(_S(), recent_keep=50)
    for i in range(500):
        hub.publish("event", {"i": i})
    assert len(hub.recent) == 50 and hub.recent[0]["data"]["i"] == 450 and hub.seq == 500


def test_hub_maps_events(tmp_path):
    r = R(tmp_path, FAST)
    hub = r.rig.hub
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(130)
    types = [e["type"] for e in hub.recent]
    assert "run_start" in types and "run_end" in types and "cmd_result" in types
    rs = next(e for e in hub.recent if e["type"] == "run_start")["data"]
    re_ = next(e for e in hub.recent if e["type"] == "run_end")["data"]
    assert rs["runId"] == re_["runId"] and re_["result"] == "done" and rs["configVersion"] == 0
    ev = [e["data"] for e in hub.recent if e["type"] == "event"]
    assert all({"level", "code", "ruleId", "message", "context"} <= set(d) for d in ev)
    blocks = [d for d in ev if d["context"].get("kind") == "block"]
    assert blocks and blocks[0]["context"]["source"] == "process" and "no" in blocks[0]["context"]["data"]
    assert any(d["context"].get("source") == "safety" for d in ev)
    assert hub.recent[0]["type"] == "hello"                                   # 첫 메시지
    assert all(d["ruleId"] is None or d["ruleId"].startswith("S") for d in ev)
    assert any(d["context"].get("seq") == "row_end" for d in ev)              # 정지 순서 이름은 context.seq
    for e in hub.recent:
        assert set(e) == {"id", "seq", "ts", "type", "data"} and e["ts"].endswith("Z")


# ───────────── 설정 적용(웹 경로 — ChkCore CONFIG_APPLY·CONFIG_ROLLBACK) ─────────────
def _vals(**ch):
    v = {k: x for k, x in defaults(P.PARAMS).items() if k not in SECRETS}
    v.update(ch)
    return v


def _apply(rig, version, **ch):
    return rig.core.command("CONFIG_APPLY", {"version": version, "values": _vals(**ch), "schemaHash": param_schema_hash(),
                                             "requestedBy": "admin@test"}, source=Source.WEB)


def _acks(rig):
    return [e["data"] for e in rig.hub.recent if e["type"] == "config_ack"]


def test_config_live_key_is_used_by_safety_rule_at_once(tmp_path):
    from v2.chk.sim.rig import ChkRig
    from v2.common.clock import VirtualClock
    c = VirtualClock()
    rig = ChkRig(c, tmp_path, simple_process=True)
    p = rig.process
    p.state, p.flow_set, p.watch = "MAIN", {"ar": 20.0, "o2": None}, {"flow": True}

    def fire_after(sec_limit):
        rig.safety.engine.reset_rule("S38A")
        for k in range(sec_limit + 2):
            v = View({"mfc.ar_sccm": 10.0}, 1000.0 + k, {"mfc": "ok"}, {"mfc": 0.0})
            if "S38A" in rig.safety.engine.evaluate("tick", v.t, v, p):
                return k
        return None
    assert fire_after(15) == 10                                  # 기본 FLOW_ERROR_SEC 10 s
    res = _apply(rig, 1, FLOW_ERROR_SEC=3)
    assert res.ok and rig.cfg["FLOW_ERROR_SEC"] == 3              # 공정 중(MAIN)이어도 live 키는 바로
    ack = _acks(rig)[-1]
    assert ack["status"] == "applied" and ack["appliedKeys"] == ["FLOW_ERROR_SEC"] and ack["by"] == "admin@test"
    assert ack["changes"] == [{"key": "FLOW_ERROR_SEC", "old": 10.0, "new": 3}] and ack["kind"] == "apply"
    assert fire_after(15) == 3                                   # 다음 판정부터 새 값


def test_config_idle_restart_h_rollback_history(tmp_path):
    r = R(tmp_path, FAST)
    rig = r.rig
    got = {}
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))

    def during():
        got["idle"] = _apply(rig, 1, HEATER_HOLD_REAPPLY_SEC=9.0, LOCAL_HMI_PORT=8711)
        got["cfg_during"] = rig.cfg["HEATER_HOLD_REAPPLY_SEC"]
        got["state_pending"] = rig.hub.state_data({})["configPending"]
        got["h"] = _apply(rig, 2, HEATER_HOLD_REAPPLY_SEC=9.0, LOCAL_HMI_PORT=8711, DC_MAX_POWER=400.0)
        got["busy"] = rig.busy()
    r.when(lambda: rig.process.state == "MAIN", during)
    r.when(lambda: "idle" in got and rig.process.state == "IDLE" and not rig.store.pending_idle,
           lambda: got.setdefault("after", (rig.cfg["HEATER_HOLD_REAPPLY_SEC"], rig.heater.hold.reapply_sec)))
    r.run(140)
    assert got["busy"] and got["idle"].ok and got["cfg_during"] == 5.0
    assert got["state_pending"] == {"idle": {"HEATER_HOLD_REAPPLY_SEC": 9.0}, "restart": {"LOCAL_HMI_PORT": 8711}}
    assert got["h"].reason == "CONFIG_H_IN_PROCESS"                                   # S48
    assert got["after"] == (9.0, 9.0)                                                 # 공정이 끝나면 적용(생성 때 복사 안 함)
    acks = _acks(rig)
    assert [a["status"] for a in acks] == ["pending_restart", "rejected", "applied"]
    assert acks[0]["pendingKeys"] == ["HEATER_HOLD_REAPPLY_SEC", "LOCAL_HMI_PORT"] and acks[0]["appliedKeys"] == []
    assert acks[1]["reasonCode"] == "CONFIG_H_IN_PROCESS" and acks[2]["kind"] == "idle"
    assert acks[2]["appliedKeys"] == ["HEATER_HOLD_REAPPLY_SEC"] and acks[2]["pendingKeys"] == ["LOCAL_HMI_PORT"]
    assert rig.cfg["LOCAL_HMI_PORT"] == 8710 and rig.store.pending_restart == {"LOCAL_HMI_PORT": 8711}   # 재시작 때
    # 되돌리기(공정 밖) — 이전 버전이 없으면 CONFIG_NO_PREVIOUS, 두 번 적용한 뒤에는 직전 값으로
    assert rig.core.command("CONFIG_ROLLBACK", {}, source=Source.WEB).reason == "CONFIG_NO_PREVIOUS"
    assert _apply(rig, 2, HEATER_HOLD_REAPPLY_SEC=7.0, LOCAL_HMI_PORT=8711).ok and rig.cfg["HEATER_HOLD_REAPPLY_SEC"] == 7.0
    assert rig.core.command("CONFIG_ROLLBACK", {"requestedBy": "admin@test"}, source=Source.WEB).ok
    ack = _acks(rig)[-1]
    assert ack["kind"] == "rollback" and ack["version"] == 3 and ack["values"]["HEATER_HOLD_REAPPLY_SEC"] == 9.0
    assert rig.cfg["HEATER_HOLD_REAPPLY_SEC"] == 9.0 and rig.store.version == 3
    hist = [json.loads(x) for x in (tmp_path / "config" / HISTORY).read_text(encoding="utf-8").splitlines()]
    assert [(h["version"], h["status"], h["kind"]) for h in hist] == [
        (1, "pending_restart", "apply"), (2, "rejected", "apply"), (2, "pending_restart", "apply"),
        (3, "pending_restart", "rollback:1")]
    assert hist[0]["by"] == "admin@test" and {"key": "LOCAL_HMI_PORT", "old": 8710, "new": 8711} in hist[0]["changes"]
    # HMI(로컬 화면)·비밀값은 받지 않는다
    assert rig.core.command("CONFIG_APPLY", {"version": 9, "values": {}, "schemaHash": ""}, source=Source.HMI).reason \
        == "SOURCE_NOT_ALLOWED"
    v = _vals()
    v["UPLINK_TOKEN"] = "x"
    assert rig.core.command("CONFIG_APPLY", {"version": 9, "values": v, "schemaHash": param_schema_hash()}).reason \
        == "CONFIG_SECRET_FROM_WEB"
    # 실행 모드·장비 ID 는 웹에서 바꾸지 않는다(덮어쓰기에 없어도) — config_ack rejected
    assert _apply(rig, 9, RUN_MODE="prod").reason == "CONFIG_LOCKED_KEY"
    assert _apply(rig, 9, EQUIPMENT_ID="CHK9").reason == "CONFIG_LOCKED_KEY"
    ack = _acks(rig)[-1]
    assert ack["status"] == "rejected" and ack["reasonCode"] == "CONFIG_LOCKED_KEY" and "EQUIPMENT_ID" in ack["errors"][0]
    assert rig.cfg["RUN_MODE"] == "sim" and rig.cfg["EQUIPMENT_ID"] == "CHK2" and rig.store.version == 3


def test_sims_start_at_clock_time():
    """실제 시계(부팅 뒤 초)로 sim 을 만들면 첫 질의가 바로 답한다 — MfcSim·CesarSim 이 0 에서 시작하면 첫 응답 때
    부팅 뒤 초를 0.1 s 씩 따라잡느라 연결 때 질의(R33·R34·R74·R75)가 시간 초과 → MFC_SCALE_UNREAD 였다."""
    from v2.chk.sim.rig import make_sims
    t0 = 500_000.0
    _, sims = make_sims(t0)
    assert all(getattr(x, "t", t0) == t0 for x in sims.values())
    w = time.perf_counter()
    assert sims["mfc"].respond(b"R33\r", t0 + 0.01)
    assert time.perf_counter() - w < 0.1
    # 받은 요청 기록은 길이 제한(시험 하니스 SIM_LOG_KEEP, 실행 프로그램 sim 은 SIM_APP_LOG_KEEP)
    from v2.chk.app import SIM_APP_LOG_KEEP
    from v2.common.simdev import SIM_LOG_KEEP
    assert sims["plc"].frames.maxlen == sims["mfc"].received.maxlen == SIM_LOG_KEEP
    _, small = make_sims(t0, log_keep=SIM_APP_LOG_KEEP)
    for x in small.values():
        logs = [getattr(x, n) for n in ("rx_log", "received", "frames") if hasattr(x, n)]
        assert logs and all(q.maxlen == SIM_APP_LOG_KEEP for q in logs)
    for k in range(SIM_APP_LOG_KEEP + 50):
        small["mfc"].respond(b"R5", t0 + 1 + k * 0.1)
    assert len(small["mfc"].received) == SIM_APP_LOG_KEEP


def test_local_override_key_is_not_changed_from_web(tmp_path):
    from v2.chk.system import new_store
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "local_override.json").write_text(json.dumps({"values": {"LOCAL_HMI_PORT": 18000}}), encoding="utf-8")
    st = new_store(d)
    st.load()
    res = st.apply(1, _vals(LOCAL_HMI_PORT=8711), param_schema_hash(), busy=False)
    assert res.overridden_keys == ["LOCAL_HMI_PORT"] and st.values["LOCAL_HMI_PORT"] == 18000 and not st.pending_restart


# ───────────── 종료 처리 ─────────────
def _app(r):
    out = []
    return App(system=r.rig, clock=r.c, print_fn=out.append), out


def test_ctrl_c_refused_during_process_and_force_exit_turns_power_off(tmp_path):
    r = R(tmp_path, FAST)
    app, out = _app(r)
    rows = [{"process_name": "P", "blocks": [{"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}},
                                              {"type": "power_on", "params": {"dc_power": 100.0}},
                                              {"type": "wait", "params": {"sec": 120}}]}]
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "x", "rows": rows}))
    r.on(3, r.cmd("RECIPE_START"))
    got = {}

    def at_wait():
        got["before"] = (r.rig.sims["dc"].out, r.plc.coils[A["ar_valve"]], r.rig.sims["mfc"].on[0])
        got["ctrl_c"] = app.on_ctrl_c()
        got["pumps"] = {k: r.plc.coils[A[k]] for k in ("mv", "turbo", "rotary", "fv", "ar_valve", "mfc_valve")
                        if k in A}
        got["res"] = app.force_exit("CTRL_CLOSE")
    r.when(lambda: r.rig.process.state == "WAIT" and r.rig.process.dcc.holding, at_wait)
    r.run(140)
    assert got["before"] == (True, True, True)
    assert got["ctrl_c"] == "refused" and any(REFUSE_PROCESS in s for s in out)
    res = got["res"]
    assert res["sent"] == ["DC OFF", "RF DAC 0·enable OFF", "히터 RUN=0"] and res["process_was_running"]
    assert set(res["done"]) == set(res["sent"]) and res["elapsed_s"] < 3.5
    assert r.rig.sims["dc"].out is False and r.plc.regs[A["rf_dac"]] == 0 and not r.plc.coils[A["rf_dac_enable"]]
    assert r.plc.coils[A["heater_run"]] is False
    assert {k: r.plc.coils[A[k]] for k in got["pumps"]} == got["pumps"]        # PLC 릴레이(펌프·밸브·차단밸브) 그대로
    assert r.rig.sims["mfc"].on[0]                                              # MFC 그대로
    ends = [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")]
    assert ends == [("error", "PROGRAM_EXIT")] and r.ev("list_cancel")
    assert app.force_exit("CTRL_CLOSE") is None                                 # 한 번만


def test_ctrl_c_with_heater_on_needs_second_press_within_5s(tmp_path):
    r = R(tmp_path, FAST)
    app, out = _app(r)
    r.on(3, r.cmd("HEATER_ON", {"value": 40.0}))
    seq = []
    r.on(8, lambda: seq.append(app.on_ctrl_c()))
    r.on(15, lambda: seq.append(app.on_ctrl_c()))           # 7 s 뒤 — 창 지남, 다시 확인
    r.on(18, lambda: seq.append(app.on_ctrl_c()))           # 3 s 안 — 종료
    r.run(20)
    assert seq == ["confirm", "confirm", "exit"] and sum(CONFIRM_HEATER in s for s in out) == 2


def test_ctrl_c_refused_while_heater_gas_or_end_stop(tmp_path):
    r = R(tmp_path, FAST)
    app, out = _app(r)
    r.on(3, r.cmd("PROCESS_START", {"blocks": E_BLOCKS}))
    got = {}
    r.when(lambda: r.rig.process.state == "STOPPING", lambda: got.setdefault("stopping", app.exit_check()))
    r.run(300)
    assert got["stopping"] == ("refuse", REFUSE_PROCESS)
    assert app.exit_check()[0] == "confirm"                   # 정상 끝 정지에서 히터를 남겼다(히터 운전 중)


def test_bench_hello_shows_read_only(tmp_path):
    from v2.chk.sim.rig import ChkRig
    from v2.common.clock import VirtualClock
    rig = ChkRig(VirtualClock(), tmp_path, mode="bench")
    h = rig.hub.hello_data()
    assert h["runMode"] == "bench" and h["writesAllowed"] is False


def test_subscriber_closed_returns_closed():
    class _S:
        class clock:
            @staticmethod
            def wall():
                return 0.0
    hub = MessageHub(_S())
    s = hub.subscribe()
    s.close()
    assert s.get(0.01) is CLOSED


def test_local_override_example_is_valid(tmp_path):
    import shutil
    from v2.chk.app import repo_root
    from v2.chk.system import new_store
    shutil.copy(repo_root() / "local_override.example.json", tmp_path / "local_override.json")
    v = new_store(tmp_path).load()
    assert v["RUN_MODE"] == "sim" and v["UPLINK_TOKEN"] == "" and v["LOCAL_HMI_PORT"] == 8710
    cmd = (repo_root() / "run_chk_v2.cmd").read_bytes()
    assert b"\r\n" in cmd and "닫지 마세요".encode() in cmd and b"-m v2.chk --config-dir" in cmd
    assert b'cd /d "%~dp0"' in cmd and b"py -3.13" in cmd and b"C:\\Users" not in cmd     # PC 별 경로 없음


@pytest.mark.skipif(__import__("sys").platform != "win32", reason="Windows 전용")
def test_session_end_window_calls_force_exit(caplog):
    import ctypes
    from ctypes import wintypes
    from v2.chk.app import WM_ENDSESSION, WM_QUERYENDSESSION
    u = ctypes.WinDLL("user32")
    u.SendMessageW.restype = ctypes.c_ssize_t
    u.SendMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
    for _ in range(2):                                    # 닫고 다시 만들어도(창 클래스 등록 해제) 새 처리기로
        app = App(print_fn=lambda s: None)
        calls = []
        app.force_exit = lambda why="": calls.append(why)
        hwnd = app.start_session_window()
        assert hwnd
        assert u.SendMessageW(hwnd, WM_QUERYENDSESSION, 0, 0) == 1           # 거부하지 않음
        u.SendMessageW(hwnd, WM_ENDSESSION, 0, 0)                             # 취소된 종료 — 아무것도 안 함
        u.SendMessageW(hwnd, WM_ENDSESSION, 1, 0)
        assert calls == ["WM_ENDSESSION"]
        app.stop_session_window()                                             # 스레드가 끝난다(conftest 가 남은 스레드 확인)
        assert app._session is None
    assert not [r for r in caplog.records if "감시 실패" in r.getMessage()]


def test_ctrl_c_refused_while_heater_gas_held(tmp_path):
    from v2.chk.app import REFUSE_GAS
    r = R(tmp_path, FAST)
    app, out = _app(r)
    r.on(3, r.cmd("HEATER_ON", {"value": 40.0, "use_ar": True, "ar_flow": 20.0, "wp_mtorr": 5.0}))
    got = {}
    r.when(lambda: r.rig.heater.atm.is_active() and "c" not in got, lambda: got.setdefault("c", app.on_ctrl_c()))
    r.run(15)
    assert all(x is None or getattr(x, "ok", True) for x in r.results[:1])
    assert got["c"] == "refused" and any(REFUSE_GAS in s for s in out)


def test_stream_connection_limit(tmp_path):
    from v2.chk.local_server import LocalServer
    from v2.chk.sim.rig import ChkRig
    from v2.common.clock import VirtualClock
    rig = ChkRig(VirtualClock(), tmp_path)
    srv = LocalServer(_free_port(), str(tmp_path / "hmi"), sse_max=1, keepalive_s=0.2)
    srv.bind()
    srv.attach(rig)
    srv.start()
    base = f"http://127.0.0.1:{srv.port}"
    try:
        rig.hub.publish("event", {"level": "info", "code": "X", "ruleId": None, "message": "m", "context": {}})
        s1 = urllib.request.urlopen(base + "/api/v1/stream", timeout=5)
        assert s1.readline().startswith(b"id: ")
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(base + "/api/v1/stream", timeout=5)
        assert ei.value.code == 503
        lines = [s1.readline() for _ in range(6)]
        assert b": keep-alive\n" in lines                                     # keep-alive 주석
        s1.close()
    finally:
        rig.hub.close()
        srv.stop()
