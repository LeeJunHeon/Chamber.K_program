"""W-22 E — 컨트롤러 값(GPC3000 MFC 저장값 읽기·바꾸기, Ion Gauge E 응답 설정 항목 읽기). 가상 시계·시뮬레이터만(실제 포트 없음).

- 연결 뒤 한 번 읽기(연결 때 읽은 질의는 다시 묻지 않음), DEVICE_PARAMS_READ 로 다시 읽기, 1초 읽기는 그대로(행 품질·읽기 계획)
- 공정 중(idle 아님)이면 미룸(waiting) → 끝나면 이어서
- 바꾸기: M·X·V·P·S 쓰기 → 되읽기 비교 → param_write 이벤트(누가·항목·이전 → 새 값), 다르면 실패 이벤트(DEVICE_PARAM_MISMATCH)
- 명령 검사: 관리자·대기 중만, 수동 조작·히터 레시피·히터 가스·bench·노트북 거부, 범위
"""
import pytest

from v2.common.commands import Source
from v2.common.paramdef import defaults
from v2.chk import commands as C
from v2.chk import dev_params as DP
from v2.chk import params as P
from v2.chk.drivers.ig import parse_status
from v2.tests.test_mfc import Rig
from v2.tests.test_process_engine import FAST, R, SHORT

CFG = defaults(P.PARAMS)
PARAM_ONLY = {f"R{n}" for n in DP.MFC_QUERIES} - {"R1", "R2", "R3", "R4", "R10"}   # SP 값은 주기 갱신도 읽는다


def _sent(r, t0=-1.0):
    return [d.decode().strip() for tt, d in r.sim.received if tt >= t0]


def _write(r, item, no, value, by="admin@x"):
    box = []
    r.d.param_write(item, no, value, by=by, on_done=lambda n, why, res: (box.append(why), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    return box[0]


def test_read_after_connect_uses_spare_time_only():
    r = Rig()
    assert r.d.params_state == "reading" and r.d.params_view is None
    t0 = r.c.monotonic()
    rows = r.run(30)
    v = r.d.params_view
    assert r.d.params_state == "done" and v is not None and v["why"] == "연결" and v["missing"] == []
    assert [s["gainPct"] for s in v["sp"]] == [100] * 5 and [s["typeText"] for s in v["sp"]] == ["압력"] * 5
    assert v["autoMode"] == 1 and v["autoModeText"] == "PID" and v["valveType"] == 1 and v["direction"] == 0
    assert (v["rangeCode"], v["unitCode"], v["fsTorr"]) == (11, 1, 1.0) and v["unitText"] == "mTorr"
    assert [x["pct"] for x in v["limits"]] == [0.0, 100.0, 0.0, 100.0] and v["flowFs"] == [100.0, 100.0]
    assert v["flowUnits"] == ["SCCM", "SCCM"] and v["status"] == "M006" and v["version"] == "H3 1.00"
    assert v["sp"][3]["mtorr"] == 30.0                                 # 시뮬레이터 SP4 3.00 %FS = 30 mTorr
    sent = _sent(r, t0)
    for n in DP.CONNECT_REUSE:                                         # 연결 때 읽은 질의는 다시 묻지 않는다
        if n not in (1, 2, 3, 4, 10):
            assert f"R{n}" not in sent, n
    assert {f"R{n}" for n in DP.MFC_QUERIES if n not in DP.CONNECT_REUSE} <= set(sent)
    # 1초 읽기는 그대로 — 행마다 압력 ok, 읽기 계획은 매초 전부(번갈아 읽기로 바뀌지 않음)
    assert all(x["mfc.q"] == "ok" and x["mfc.read_plan"] == "full" for x in rows)
    assert "read_plan" not in r.kinds() and r.sim.dropped == 0
    ev = [e for e in r.events if e.kind == "params_read"]
    assert len(ev) == 1 and ev[0].reason is None and "컨트롤러 값을 읽었습니다(연결)" in ev[0].detail
    r.done()


def test_reread_on_request_and_deferred_while_busy():
    r = Rig()
    r.run(30)
    busy = [True]
    r.d.idle = lambda: not busy[0]
    t0 = r.c.monotonic()
    r.d.params_request(by="op@x", why="다시 읽기")
    assert r.d.params_state == "waiting"
    r.run(10)
    assert r.d.params_state == "waiting" and not (set(_sent(r, t0)) & PARAM_ONLY)     # 공정 중 — 묻지 않는다
    assert r.d.params_progress() == {"done": 0, "total": len(DP.MFC_QUERIES)}
    busy[0] = False
    r.run(60)                                                           # 1초 읽기가 남긴 시간에만 — 질의 35개면 약 35초
    assert r.d.params_state == "done" and r.d.params_view["by"] == "op@x"
    sent = _sent(r, t0)
    assert {f"R{n}" for n in DP.MFC_QUERIES} <= set(sent)               # 다시 읽기는 전부
    assert sum(1 for e in r.events if e.kind == "params_read") == 2
    r.done()


@pytest.mark.parametrize("item,no,value,cmd,query,raw", [
    ("gain", 1, 120, "M1 120", "R46", "M1+120"),
    ("lead", 2, 0.5, "X2 0.50", "R42", "X2+0.50"),
    ("auto", None, 1, "V1", "R51", "V1"),
    ("limit", 2, 90.0, "P2 90.00", "R12", "P2+90.00"),
    ("sp", 3, 15.0, "S3 1.50", "R3", "S3+001.50"),
])
def test_write_readback_and_event(item, no, value, cmd, query, raw):
    r = Rig()
    if item == "auto":
        r.sim.info[51] = "V0"                                           # 장비 앞면에서 Self-Tuning — 원격으로는 PID 로만(W-23 H5)
    r.run(30)
    old = DP.mfc_value(r.d.params_view, item, no)
    t0 = r.c.monotonic()
    assert _write(r, item, no, value) is None
    io = [x for x in _sent(r, t0) if x == cmd or x == query]
    assert io[:2] == [cmd, query]
    assert r.sim._query(query) == raw
    assert DP.mfc_value(r.d.params_view, item, no) == pytest.approx(value)   # 화면 값도 되읽은 값으로
    w = r.d.params_write
    assert w["ok"] and w["by"] == "admin@x" and w["cmd"] == cmd and w["new"] == pytest.approx(value) and w["old"] == old
    ev = [e for e in r.events if e.kind == "param_write"]
    assert len(ev) == 1 and ev[0].reason is None
    assert ev[0].detail.startswith("컨트롤러 값을 바꿨습니다 · admin@x · ") and "→" in ev[0].detail
    if item == "sp":
        assert r.d.sp_device[3] == 15.0 and r.d.sp_written[3] == 15.0
    assert not r.d.params_writing
    r.done()


def test_write_mismatch_fails_with_event():
    r = Rig()
    r.run(30)
    r.sim.ignore_writes = {"M1"}                                        # 장비가 바꾸지 않음 → 되읽기 다름
    t0 = r.c.monotonic()
    why = _write(r, "gain", 1, 150)
    assert why == "VERIFY_MISMATCH"
    sent = _sent(r, t0)
    assert sent.count("R46") == CFG["MFC_VERIFY_TRIES"] and sent.count("M1 150") == CFG["MFC_VERIFY_TRIES"]
    ev = [e for e in r.events if e.kind == "param_write"]
    assert len(ev) == 1 and ev[0].reason == "DEVICE_PARAM_MISMATCH"
    assert "바꾸지 못했습니다" in ev[0].detail and "100 % → 목표 150 %" in ev[0].detail and "되읽은 값 100 %" in ev[0].detail
    assert r.d.params_write["ok"] is False and r.d.params_write["new"] == 100
    r.done()


def test_bench_reads_but_never_writes():
    r = Rig(writes=False)
    r.run(30)
    assert r.d.params_state == "done" and r.d.params_view["missing"] == []      # 질의만 — bench 허용 목록 안
    t0 = r.c.monotonic()
    assert _write(r, "gain", 1, 120) == "BENCH_READ_ONLY"
    assert not [x for x in _sent(r, t0) if not x.startswith("R")]
    r.done()


def test_check_write_ranges():
    ok = [("gain", 1, 1), ("gain", 5, 9999), ("lead", 1, 0.01), ("lead", 5, 2.0), ("auto", None, 1),
          ("limit", 4, 100.0), ("limit", 1, 0.0), ("sp", 5, 1000.0), ("sp", 1, 0.0)]
    for item, no, v in ok:
        assert DP.check_write(item, no, v, 1.0)[0] is None, (item, no, v)
    bad = [("gain", 1, 0), ("gain", 1, 10000), ("gain", 1, 1.5), ("lead", 1, 0.005), ("lead", 1, 2.01), ("lead", 1, 0.123),
           ("auto", None, 2), ("auto", None, 0), ("limit", 1, 101), ("limit", 1, -1), ("limit", 5, 1), ("sp", 1, 1000.1), ("sp", 6, 1),
           ("gain", None, 10), ("nope", 1, 1), ("gain", 1, True), ("gain", 1, "10")]
    for item, no, v in bad:
        assert DP.check_write(item, no, v, 1.0)[0], (item, no, v)
    assert DP.check_write("lead", 1, 0.5, 1.0) == (None, 1, 0.5) and DP.check_write("gain", 2, 7.0, 1.0) == (None, 2, 7)
    # 쓰는 글자는 cmd_guard 허용 정규식과 같은 꼴
    from v2.chk import cmd_guard as G
    for item, no, v in ok:
        cmd = DP.write_plan(item, no, DP.check_write(item, no, v, 1.0)[2], 1.0)[0]
        assert G.mfc_check((cmd + "\r").encode(), "prod") is None, cmd


def test_ig_items_parsed_read_only():
    lines = ["0", "0", "0", "0", "0", "0", "0", "0", "0", "2", "0", "0", "0", "0", "1", "1", "10 -2", "50 -4", "20 -3",
             "100", "100", "15 -1", "25 -2", "0 0", "63 -4", "-900 0"]
    r = parse_status(lines)
    assert r.auto_filament_state == 2 and r.sp_protect_off and r.auto_filament
    assert r.sp_auto_filament == pytest.approx(1e-3) and r.sp_ion1 == pytest.approx(5e-5) and r.sp_ion2 == pytest.approx(2e-4)
    assert r.sp_a == pytest.approx(1.5e-2) and r.sp_b == pytest.approx(2.5e-3) and r.gas_factor == 1.0 and r.sensitivity == 10.0
    v = DP.ig_view(r, "2026-11-25T00:00:00.000Z")
    assert v["autoFilament"] is True and v["raw"]["E17"] == "10 -2" and v["readAt"].startswith("2026-11-25")
    bad = list(lines)
    bad[17] = "xx"                                                        # 셋포인트 형식 다름 — 그 값만 모름(압력 읽기는 그대로)
    r2 = parse_status(bad)
    assert r2.sp_ion1 is None and r2.a_torr == pytest.approx(6.3e-3)
    assert not any(c.kind.startswith("IG_") and c.kind.endswith("WRITE") for c in C.COMMANDS)   # IG 값 바꾸는 명령 없음


# ───────── 명령 검사(조립 — ChkRig) ─────────
def _args(**kw):
    return dict({"device": "mfc", "item": "gain", "no": 1, "value": 120}, **kw)


def test_command_table():
    k = C.by_kind()
    r, w = k["DEVICE_PARAMS_READ"], k["DEVICE_PARAM_WRITE"]
    assert r.role == "operator" and r.bench_ok and r.sources == (Source.WEB,) and r.hmi == ""
    assert w.role == "admin" and not w.bench_ok and w.sources == (Source.WEB,) and w.states == C.IDLE_STATES and w.hmi == ""
    assert "DEVICE_PARAM_WRITE" in C.MANUAL_EXCLUSIVE and "DEVICE_PARAM_WRITE" not in C.HMI_OPERATE
    assert {"DEVICE_PARAM_BUSY", "DEVICE_PARAM_MISMATCH"} <= C.reason_codes()
    for code in ("DEVICE_PARAM_BUSY", "DEVICE_PARAM_MISMATCH"):
        assert C.REASON_HELP[code].endswith("요.") or C.REASON_HELP[code].endswith("다.")


def test_core_write_and_rejects(tmp_path):
    r = R(tmp_path / "a", dict(FAST))
    out = {}
    r.on(40, lambda: out.setdefault("hmi", r.rig.core.command("DEVICE_PARAM_WRITE", _args(), source=Source.HMI)))
    r.on(40, lambda: out.setdefault("range", r.rig.core.command("DEVICE_PARAM_WRITE", _args(value=0))))
    r.on(40, lambda: out.setdefault("lead", r.rig.core.command("DEVICE_PARAM_WRITE", _args(item="lead", value=0.123))))
    r.on(41, lambda: out.setdefault("ok", r.rig.core.command("DEVICE_PARAM_WRITE", _args(), by="admin@x")))
    r.on(41, lambda: out.setdefault("busy", r.rig.core.command("DEVICE_PARAM_WRITE", _args(no=2))))
    r.on(60, lambda: out.setdefault("read", r.rig.core.command("DEVICE_PARAMS_READ", {}, by="op@x")))
    r.on(62, r.cmd("PROCESS_START", {"row": SHORT}))
    r.on(70, lambda: out.setdefault("running", r.rig.core.command("DEVICE_PARAM_WRITE", _args())))
    r.run(80)
    assert out["hmi"].reason == "SOURCE_NOT_ALLOWED"
    assert out["range"].reason == "BAD_ARGS" and "범위 1 ~ 9999 %" in out["range"].detail
    assert out["lead"].reason == "BAD_ARGS" and "소수 둘째 자리" in out["lead"].detail
    assert out["ok"].ok and out["busy"].reason == "DEVICE_PARAM_BUSY"
    assert out["read"].ok and out["running"].reason == "STATE_NOT_ALLOWED"
    hub = r.rig.hub
    dp = hub.last_state["data"]["devParams"]
    assert dp["mfc"]["write"]["ok"] and dp["mfc"]["write"]["by"] == "admin@x" and dp["mfc"]["write"]["at"].endswith("Z")
    assert dp["mfc"]["state"] == "waiting"                              # 다시 읽기 — 공정 중이라 미룸
    assert dp["mfc"]["values"]["sp"][0]["gainPct"] == 120 and dp["mfc"]["readAt"].endswith("Z")
    assert dp["ig"] is not None and "autoFilament" in dp["ig"]
    evs = [m["data"] for m in hub.recent if m["type"] == "event" and (m["data"]["context"] or {}).get("kind") == "param_write"]
    assert len(evs) == 1 and evs[0]["context"]["source"] == "mfc" and evs[0]["level"] == "info"


def test_core_rejects_manual_bench_heater(tmp_path):
    r = R(tmp_path / "m", dict(FAST))
    out = {}
    r.on(30, r.cmd("MANUAL_BLOCK", {"block": {"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}}}))
    r.on(40, lambda: out.setdefault("manual", r.rig.core.command("DEVICE_PARAM_WRITE", _args())))
    r.run(45)
    assert out["manual"].reason == "MANUAL_ACTIVE"
    # 히터 가스·히터 레시피
    r = R(tmp_path / "h", dict(FAST))
    r.on(30, lambda: setattr(r.rig.heater.atm, "is_active", lambda: True))
    r.on(31, lambda: out.setdefault("gas", r.rig.core.command("DEVICE_PARAM_WRITE", _args())))
    r.run(35)
    assert out["gas"].reason == "HEATER_GAS_ACTIVE"
    # bench — 읽기는 받고 바꾸기는 거부
    from v2.common.clock import VirtualClock
    from v2.chk.sim.rig import ChkRig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path / "b", mode="bench")
    assert rig.start()
    try:
        rig.loop.run(30)
        assert rig.core.command("DEVICE_PARAM_WRITE", _args()).reason == "BENCH_READ_ONLY"
        assert rig.core.command("DEVICE_PARAMS_READ", {}).ok
        rig.loop.run(60)
        assert rig.mfc.params_state == "done"
    finally:
        rig.stop()


# ───────── W-23 H6 자동 다시 읽기·바뀜 경고 ─────────
def test_reread_after_run_and_change_warning():
    r = Rig(cfg={"DEVICE_PARAMS_REFRESH_MIN": 0})
    r.run(30)
    assert r.d.params_state == "done"
    busy = [True]
    r.d.idle = lambda: not busy[0]
    r.run(3)                                                            # 공정 중(읽지 않음)
    r.sim.info[46] = "M1+400"                                           # 장비 앞면에서 바꿈
    busy[0] = False
    r.run(60)
    reads = [e for e in r.events if e.kind == "params_read"]
    assert len(reads) == 2 and "공정 끝" in reads[1].detail
    ch = [e for e in r.events if e.kind == "params_changed"]
    assert len(ch) == 1 and ch[0].reason == "DEVICE_PARAMS_CHANGED"
    assert ch[0].detail == "컨트롤러 값이 바뀌었습니다 · SP1 Gain 100 → 400 — 장비 앞면에서 바꿨는지 확인하세요"
    r.done()


def test_periodic_reread_and_program_writes_not_warned():
    r = Rig(cfg={"DEVICE_PARAMS_REFRESH_MIN": 1})
    r.run(30)
    assert _write(r, "gain", 2, 150) is None                            # 프로그램이 쓴 값 — 다음 읽기에서 경고하지 않음
    r.sim.sp[0] = 7.0                                                   # SP1 도 공정이 쓴 것처럼
    r.d._prog_touched.add(1)
    r.run(120)
    reads = [e for e in r.events if e.kind == "params_read"]
    assert len(reads) >= 2 and "주기" in reads[1].detail
    assert not [e for e in r.events if e.kind == "params_changed"]
    assert P.by_key()["DEVICE_PARAMS_REFRESH_MIN"].default == 10
    r.done()


def test_diff_words_for_codes():
    """W-23 G1 — 바뀜 경고도 코드가 아닌 말(밸브 종류·제어 방향·센서 범위)."""
    base = {23: "J1", 32: "N0", 33: "E11", 34: "F01"}
    a = DP.mfc_view(dict(base), 1.0)
    b = DP.mfc_view({**base, 23: "J2", 32: "N1"}, 1.0)
    d = DP.diff_views(a, b)
    assert ("밸브 종류", "Standard 253", "Fast 253") in d and ("제어 방향", "Direct", "Reverse") in d
    assert [x for x in DP._diff_items(a) if x[0] == 33][0][2] == "1 Torr"
