"""MFC(GPC3000) 세트포인트 값 설정 SP1·SP2·SP3·SP5 — 한 검증 경로(5-17, v1 fa85e15·a78b5ce·f5780ca).

v1 tests/test_mfc_sp_set.py 의 경우(정상·재전송·5회 실패·접두어·R10·SP1 만 기준값·SPn_ON·SP5 공정 단계)를 v2 시험대
(가상 시계 + MFC 시뮬레이터)로 옮긴 것. v1 을 import 하지 않는다.
쓰기 "Sn 값"(%FS) → 읽기(SP1~SP3 = R1~R3, SP5 = R10 — R5 는 압력) → 응답 "Sn+값" 의 접두어·값(±MFC_SP_VERIFY_TOL 0.01) 확인.
값이 읽혔는데 다르면 Sn 을 다시 쓰고, 무응답·형식 다름이면 다시 읽기만, 최대 MFC_VERIFY_TRIES(5)회.
"""
import pytest

from v2.common.paramdef import defaults
from v2.chk import blocks as BK
from v2.chk import cmd_guard as G
from v2.chk import params as P
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk.drivers.mfc import SP_READ_CMD, SP_VERIFIED, parse_pressure, parse_sp_reply, sp_cmd
from v2.chk.process_steps import build_process_steps, row_to_params
from v2.tests.test_mfc import Rig

CFG = defaults(P.PARAMS)


def _io(r, t0):
    """t0 뒤에 장비가 받은 명령(연결 초기화·tick 읽기 없음 — 이 시험대는 tick 을 돌리지 않는다)."""
    return [d.decode().strip() for tt, d in r.sim.received if tt >= t0]


def _set(r, n, mtorr):
    t0 = r.c.monotonic()
    res = r.seq(r.d.set_sp, n, mtorr)
    return res, _io(r, t0)


# (e) 명령 표 — SP5 읽기는 R10(R5 는 압력), 허용 오차 0.01, SP4 는 한 검증 경로 밖
def test_commands_table():
    assert SP_READ_CMD == {1: "R1", 2: "R2", 3: "R3", 4: "R4", 5: "R10"} and SP_VERIFIED == (1, 2, 3, 5)
    assert sp_cmd(5, 50.0, 1.0)[0] == "S5 5.00"
    p = P.by_key()
    assert p["MFC_SP_VERIFY_TOL"].default == 0.01 and "MFC_SP1_VERIFY_TOL" not in p
    assert p["SP5_MTORR"].default == 50.0 and p["SP5_STABILIZE_SEC"].default == 60.0
    assert (p["MFC_QUERY_RETRIES"].default, p["MFC_VERIFY_RETRY_MS"].default, p["MFC_VERIFY_TRIES"].default,
            p["MFC_LINK_RETRY_MS"].default) == (5, 1000, 5, 1000)                       # 예전 고정 상수와 같은 값


# (a) SP2·SP3·SP5·SP1 정상 검증 — 쓰기 한 번, 읽기 한 번, 원본 응답 기록
@pytest.mark.parametrize("n,rd,ui,hw", [(2, "R2", 5.0, "0.50"), (3, "R3", 15.0, "1.50"), (5, "R10", 50.0, "5.00"),
                                        (1, "R1", 2.0, "0.20")])
def test_set_verified(n, rd, ui, hw):
    r = Rig()
    res, io = _set(r, n, ui)
    assert res is None and io == [f"S{n} {hw}", rd]
    raw = r.sim._query(rd)
    assert raw.startswith(f"S{n}+") and float(raw[2:]) == float(hw)                    # 시뮬레이터 R10 = 'S5+005.00'
    assert [e.detail for e in r.events if e.kind == "sp_readback"] == [f"S{n} 검증 1/5: {rd} 응답={raw!r}"]
    assert [e for e in r.events if e.kind == "verify_ok" and f"{rd}={raw}" in e.detail]
    r.done()


def test_tolerance_and_whitespace():
    r = Rig()
    r.sim.canned["R10"] = ["  S5+5.01 "]                              # 공백 무시, 차이 0.01 = 허용 오차 끝(통과)
    res, io = _set(r, 5, 50.0)
    assert res is None and io == ["S5 5.00", "R10"]
    r.done()


# (b) 값 불일치 → 재전송 뒤 통과 / 응답 없음·빈 응답 → 다시 읽기만
def test_mismatch_resends_then_ok():
    r = Rig()
    r.sim.canned["R3"] = ["S3+1.20", "S3+1.50"]
    t0 = r.c.monotonic()
    res, io = _set(r, 3, 15.0)
    assert res is None and io == ["S3 1.50", "R3", "S3 1.50", "R3"]
    assert r.c.monotonic() - t0 >= CFG["MFC_VERIFY_RETRY_MS"] / 1000.0                  # 다시 하기 전 대기(v1 MFC_DELAY_MS)
    r.done()


def test_no_reply_rereads_without_resend():
    r = Rig(cfg={"MFC_QUERY_RETRIES": 0})                             # 질의 재전송(통신층) 없이 — 검증 회차만 본다
    r.sim.canned["R2"] = [None, "", "S2+0.50"]
    res, io = _set(r, 2, 5.0)
    assert res is None and io == ["S2 0.50", "R2", "R2", "R2"]
    assert [e.detail for e in r.events if e.kind == "sp_readback"][:2] == ["S2 검증 1/5: R2 응답=None",
                                                                         "S2 검증 2/5: R2 응답=''"]
    r.done()


def test_query_retries_param_resends_unanswered_query():
    r = Rig()                                                         # 기본 MFC_QUERY_RETRIES 5 — 무응답 질의는 통신층이 다시
    r.sim.canned["R2"] = [None, None, "S2+0.50"]
    res, io = _set(r, 2, 5.0)
    assert res is None and io == ["S2 0.50", "R2", "R2", "R2"]
    assert len([e for e in r.events if e.kind == "sp_readback"]) == 1                   # 검증 회차는 1회
    r.done()


def test_mismatch_beyond_tol_is_not_ok():
    r = Rig()
    r.sim.canned["R10"] = ["S5+5.02"] * 5                             # 0.02 > 0.01
    res, _ = _set(r, 5, 50.0)
    assert res == "VERIFY_MISMATCH"
    r.done()


# (c) 5회 실패 → 실패 사유에 기대값·마지막 원본 응답
def test_five_failures_reason_has_expected_and_last_reply():
    r = Rig(cfg={"MFC_QUERY_RETRIES": 0})
    r.sim.canned["R2"] = ["S2+0.40"] * 3 + [None, "S2+0.41"]
    res, io = _set(r, 2, 5.0)
    assert res == "VERIFY_MISMATCH"
    assert io.count("R2") == 5 and io.count("S2 0.50") == 1 + 3      # 첫 전송 + 값 불일치 3번 재전송(무응답 뒤는 읽기만)
    why = [e for e in r.events if e.kind == "verify_mismatch"][-1]
    assert why.reason == "VERIFY_MISMATCH" and "S2 0.50" in why.detail and "'S2+0.41'" in why.detail and "5회" in why.detail
    assert len([e for e in r.events if e.kind == "sp_readback"]) == 5                   # 매 시도의 원본 응답
    r.done()


def test_verify_tries_param():
    r = Rig(cfg={"MFC_VERIFY_TRIES": 2, "MFC_VERIFY_RETRY_MS": 200})
    r.sim.ignore_writes = {"S3"}
    t0 = r.c.monotonic()
    res, io = _set(r, 3, 15.0)
    assert res == "VERIFY_MISMATCH" and io == ["S3 1.50", "R3", "S3 1.50", "R3"]
    assert r.c.monotonic() - t0 < 1.5                                 # 대기 200 ms 한 번(예전 고정 1 s 아님)
    r.done()


# (d) 접두어가 다른 응답은 불일치(다른 명령의 응답) — 값이 맞아 보여도 통과하지 않고, 다시 쓰지도 않는다
@pytest.mark.parametrize("reply", ["P+0.20", "S1+0.20", "+0.20", "0.20", "M110"])
def test_wrong_prefix_not_ok(reply):
    r = Rig()
    r.sim.canned["R2"] = [reply] * 4 + ["S2+0.20"]
    res, io = _set(r, 2, 2.0)
    assert res is None and io.count("R2") == 5 and io.count("S2 0.20") == 1
    r.done()


def test_wrong_prefix_only_fails():
    r = Rig()
    r.sim.canned["R1"] = ["P+0.20"] * 5
    res, _ = _set(r, 1, 2.0)
    assert res == "VERIFY_MISMATCH" and "'P+0.20'" in [e for e in r.events if e.kind == "verify_mismatch"][-1].detail
    assert r.d.sp_mtorr is None                                       # 확인 못 한 값은 기준값으로 쓰지 않는다
    r.done()


def test_pressure_parser_unchanged():
    """압력 읽기(R5) 파서는 접두어를 보지 않는다(그대로). 설정점 읽기 파서만 "Sn" 을 본다."""
    assert parse_pressure("P+0.20") == 0.20 and parse_pressure("0.33") == 0.33
    assert parse_sp_reply("P+0.20", 2) is None and parse_sp_reply(" S2+0.20 ", 2) == 0.20
    assert parse_sp_reply("S5+005.00", 5) == 5.0 and parse_sp_reply("S1+0.20", 2) is None and parse_sp_reply(None, 1) is None
    assert parse_sp_reply("S2", 2) is None and parse_sp_reply("s3+1.50", 3) == 1.5


# (f) 압력 이탈 기준(스냅샷 mfc.sp_mtorr)은 SP1 만 — SP2·SP3·SP5(중간 단계)는 건드리지 않는다
def test_only_sp1_sets_pressure_setpoint():
    r = Rig()
    assert _set(r, 1, 2.0)[0] is None and r.d.sp_mtorr == 2.0
    for n, ui in ((2, 5.0), (3, 15.0), (5, 50.0), (4, 30.0)):
        assert _set(r, n, ui)[0] is None and r.d.sp_mtorr == 2.0
    assert _set(r, 1, 8.0)[0] is None and r.d.sp_mtorr == 8.0
    r.done()


# SPn_ON — R7 'MXYZ' 의 두 번째 글자(SP5 추가, SP1~4 그대로)
@pytest.mark.parametrize("n", [1, 4, 5])
def test_sp_on_r7(n):
    r = Rig()
    t0 = r.c.monotonic()
    assert r.seq(r.d.select_sp, n) is None and r.d.sp_active == n
    assert _io(r, t0) == [f"D{n}", "R7"] and r.sim._query("R7")[:2] == f"M{n}"
    r.done()


def test_sp5_on_wrong_select_fails():
    r = Rig()
    r.sim.canned["R7"] = ["M400"] * 5
    t0 = r.c.monotonic()
    assert r.seq(r.d.select_sp, 5) == "VERIFY_MISMATCH" and r.d.sp_active is None
    assert _io(r, t0) == ["D5"] + ["R7"] * 5
    r.done()


def test_sp_number_out_of_range():
    r = Rig()
    with pytest.raises(ValueError):
        r.d.set_sp(6, 5.0)
    with pytest.raises(ValueError):
        r.d.select_sp(0)
    r.done()


def test_sp5_pressure_control_through_chamber():
    """시뮬레이터: S5·D5 로 압력 제어가 되고(50 mTorr), 스냅샷 mfc.sp_active = 5."""
    r = Rig()
    assert r.seq(r.d.set_flow, 1, 20.0) is None and r.seq(r.d.set_onoff, "1000") is None
    assert r.seq(r.d.set_sp, 5, 50.0) is None and r.sim.sp[4] == 5.0
    assert r.seq(r.d.select_sp, 5) is None
    r.c.wait_until(r.c.monotonic() + 120.0)
    row = r.run(2)[-1]
    assert row["mfc.sp_active"] == 5 and row["mfc.pressure_mtorr"] == pytest.approx(50.0, rel=0.05)
    r.done()


# 통신 가드 — prod 는 S5·D5 허용, bench 는 질의만(R10 은 bench 질의 목록에 있다), D6·S6 은 어디서도 안 됨
def test_cmd_guard_sp5():
    for cmd in (b"S5 5.00\r", b"D5\r", b"R10\r"):
        assert G.mfc_check(cmd, "prod") is None and G.mfc_check(cmd, "sim") is None
    assert G.mfc_check(b"R10\r", "bench") is None
    assert "bench" in G.mfc_check(b"S5 5.00\r", "bench") and "bench" in G.mfc_check(b"D5\r", "bench")
    assert "금지" in G.mfc_check(b"D6\r", "prod") and G.mfc_check(b"S6 1.00\r", "prod") is not None
    assert G.mfc_check(b"S0 1.00\r", "prod") is not None


# 공정 스텝 — WP > 50 이면 SP4 → … → 파워 → SP5(50) 설정·선택 → 60초 고정 대기 → SP1, 50 이하면 예전과 같다
def _steps(wp, **over):
    cfg = dict(CFG, **over)
    row = {"process_name": "t", "ar": True, "ar_flow": 20.0, "working_pressure": float(wp), "process_time": 10.0,
           "shutter_delay": 5.0, "use_dc": True, "dc_power": 100.0, "gun1": True}
    ok, errs, _ = RC.check_process_row(row, cfg)
    assert not errs
    return build_process_steps(row_to_params(ok, cfg), cfg, ig_enabled=True, rfpulse_present=False)


def _cmds(steps):
    return [s.params[0] for s in steps if s.action == "MFC_CMD"]


def test_process_sp5_stage():
    s60 = _steps(60)
    cmds = _cmds(s60)
    i4, i5, i1 = cmds.index("SP4_ON"), cmds.index("SP5_SET"), cmds.index("SP1_SET")
    assert i4 < i5 < i1 and cmds[i5:i5 + 2] == ["SP5_SET", "SP5_ON"]
    msgs = [s.message for s in s60]
    j = msgs.index("Stage0(SP5): SP5_ON")
    assert msgs[j - 1] == "Stage0(SP5): SP5_SET=50.00 설정" and s60[j - 1].params == ("SP5_SET", {"value": 50.0})
    assert s60[j - 2].action == "POWER_WAIT"                          # 파워 도달 뒤(v1 순서)
    # SP5_ON 바로 뒤가 60초 DELAY(도달 판정 없음), 그 다음이 SP1_SET
    d = s60[j + 1]
    assert (d.action, d.message, d.duration_sec) == ("DELAY", "Stage0(SP5): 압력 안정화 대기(60초)", 60)
    assert s60[j + 2].params[0] == "SP1_SET"
    assert {s.state for s in s60[j - 1:j + 2]} == {"PRESSURE_SP5"}
    # SP5 뒤의 WAIT_PRESSURE 는 SP1 의 것 하나뿐(목표 60, 한도 300 s, 넘으면 중단)
    waits = [s.params[1] for s in s60[j - 1:] if s.action == "MFC_CMD" and s.params[0] == "WAIT_PRESSURE"]
    assert len(waits) == 1 and waits[0]["target"] == 60.0 and waits[0]["timeout_sec"] == 300.0
    assert waits[0]["fail_on_timeout"] is True
    assert not any(s.action == "MFC_CMD" and s.params[0] == "WAIT_PRESSURE" and s.params[1]["target"] == 50.0 for s in s60)
    assert "SP3_SET" not in cmds and "SP2_SET" not in cmds
    for wp in (2, 8, 20, 50):                                         # WP 가 정확히 50 이면 단계 없음(v1 과 같게)
        assert not any("SP5" in s.message for s in _steps(wp))
    # SP5 3스텝(SET·ON·DELAY)을 빼면 WP 60 의 목록은 SP5 가 없던 때와 같은 모양(SP4 → … → POWER_WAIT → SP1)
    unit = lambda ss: [(s.action, s.params[0] if s.params else None, s.state) for s in ss]
    assert unit([s for s in s60 if "SP5" not in s.message]) == unit(_steps(20))
    assert len(s60) == len(_steps(20)) + 3


def test_process_sp5_follows_config():
    s = _steps(45, SP5_MTORR=40.0, SP5_STABILIZE_SEC=30.0)
    j = [x.message for x in s].index("Stage0(SP5): SP5_ON")
    assert s[j - 1].params == ("SP5_SET", {"value": 40.0}) and s[j + 1].duration_sec == 30
    assert s[j + 1].message == "Stage0(SP5): 압력 안정화 대기(30초)"
    assert not any("SP5" in x.message for x in _steps(40, SP5_MTORR=40.0))


def test_sp5_block_and_state_definitions():
    assert BK.SP_DEFAULT_KEY[5] == "SP5_MTORR"
    d = BK.by_type()["pressure"]
    sp = [p for p in d.params if p.name == "sp"][0]
    assert (sp.min, sp.max) == (1, 5)
    seq = PS.STEP_SEQUENCE
    assert seq.index("POWER_WAIT") < seq.index("PRESSURE_SP5") < seq.index("PRESSURE_SP3")
    ok, errs, _ = BK.check_blocks([{"type": "pressure", "params": {"sp": 5}},                       # 값 비움 = SP5_MTORR
                                   {"type": "wait", "params": {"sec": 60}, "stage": "PRESSURE_SP5"}], CFG)
    assert errs == [] and ok
    _, errs, _ = BK.check_blocks([{"type": "pressure", "params": {"sp": 6, "value": 5.0}}], CFG)
    assert errs


# 전체 공정(sim, 가상 시계) — WP 60: SP4 → 파워 → SP5 단계(약 60초 고정 대기) → SP1, WP 5: 예전 순서 그대로
def _run_process(tmp_path, wp, n):
    from v2.tests.test_process_engine import FAST, R
    r = R(tmp_path, FAST)
    row = {"process_name": f"WP{wp:g}", "ar": True, "ar_flow": 20.0, "working_pressure": float(wp), "process_time": 0.1,
           "shutter_delay": 0.1, "use_dc": True, "dc_power": 100.0, "gun1": True}
    r.on(3, r.cmd("PROCESS_START", {"row": row}))
    r.run(n)
    assert all(x.ok for x in r.results), r.results
    assert [e.data["status"] for e in r.ev("run_end")] == ["done"]
    sent = [d.decode().strip() for _, d in r.rig.sims["mfc"].received]
    return r, r.states(), [x for x in sent if not x.startswith("R")], [x for x in sent if x.startswith("R")]


def test_full_process_wp60_has_sp5_stage(tmp_path):
    r, st, writes, queries = _run_process(tmp_path, 60, 260)
    assert [s for s, _ in st] == ["PRE_CLEAN", "GAS_PRE", "GAS_INTRO", "GAS_STABILIZE", "GUN_SHUTTER", "POWER_WAIT",
                                  "PRESSURE_SP5", "PRESSURE_SP1", "SHUTTER_DELAY", "MAIN", "STOPPING", "DONE", "IDLE"]
    t = dict(st)
    assert t["PRESSURE_SP1"] - t["PRESSURE_SP5"] == pytest.approx(62, abs=1.6)       # S5·R10·D5·R7 + 고정 대기 60 s
    i = writes.index("S4 3.00")
    assert writes[i:i + 6] == ["S4 3.00", "D4", "S5 5.00", "D5", "S1 6.00", "D1"]
    assert queries.count("R10") == 1 and queries.count("R1") == 1 and queries.count("R4") == 1
    assert not [e for e in r.rig.safety.engine.events if e.kind in ("fired", "abort", "also") and e.rule != "S44"]


def test_full_process_wp5_unchanged(tmp_path):
    _, st, writes, queries = _run_process(tmp_path, 5, 200)
    assert [s for s, _ in st] == ["PRE_CLEAN", "GAS_PRE", "GAS_INTRO", "GAS_STABILIZE", "GUN_SHUTTER", "POWER_WAIT",
                                  "PRESSURE_SP3", "PRESSURE_SP1", "SHUTTER_DELAY", "MAIN", "STOPPING", "DONE", "IDLE"]
    i = writes.index("S4 3.00")
    assert writes[i:i + 6] == ["S4 3.00", "D4", "S3 1.50", "D3", "S1 0.50", "D1"]
    assert "R10" not in queries and not any(w.startswith(("S5", "D5")) for w in writes)
    assert queries.count("R3") == 1                                                  # SP3 도 한 검증 경로(R3 되읽기)
