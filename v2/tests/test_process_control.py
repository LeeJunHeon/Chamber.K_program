"""공정 중 건너뛰기·일시정지·순서 고치기(5-15) — 시험대(ChkRig·가상 시계)와 블록 정의.

블록 종류마다 허용/거부와 정리 동작, 멈춤 한도 STOP(PAUSE_TIMEOUT), STALE 거부, 시작한 블록 고치기 거부, 다시 검사 실패 시 판 유지,
사용 장비 늘어남(꺼진 장비면 거부), 반복 안쪽, 정지 순서 중 거부, 기록(원래·판·실행 순서), 딜레이 행, 섞어서 한 행 끝까지.
"""
import pytest

from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import web_schema as W
from v2.chk.sim.plc_sim import A
from v2.common.paramdef import defaults
from v2.chk import params as P
from v2.tests.test_process_engine import R

B = lambda t, **p: {"type": t, "params": p}


def _cmd(r, kind, **a):
    return lambda: r.rig.core.command(kind, a)


def _res(r):
    return [x for x in r.results if hasattr(x, "reason")]


def _ends(r):
    return [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")]


def _cur(r):
    return r.rig.process.run_state(False)


# ───────────────────────── 규칙 표 ─────────────────────────
RULES = {   # 종류: (건너뛰기, 멈춤) — 값 조건이 있는 것은 아래에서 따로
    "pre_clean": ("cmd", ""), "gas_prep": ("cmd", ""), "heater_ramp": ("cmd", ""), "heater_wait": ("wait", "judge"),
    "heater_settle": ("settle", "judge"), "base_wait": ("base", "judge"), "gas_intro": ("cmd", ""),
    "pressure": ("wait", "judge"), "valve": ("cmd", ""), "shutter": ("cmd", ""), "power_on": ("cmd", "judge"),
    "power_off": ("", ""), "gas_off": ("", ""), "heater_off": ("", ""), "wait": ("wait", "timer"),
    "main": ("main", "main"), "cond_wait": ("wait", "judge"), "repeat": ("", ""), "end_stop": ("", ""),
}


def test_rules_table_covers_every_block_and_bundle_exports_it():
    assert set(RULES) == {d.type for d in BK.BLOCK_DEFS}
    defs = {d["type"]: d for d in W.bundle(defaults(P.PARAMS))["blocks"]["defs"]}
    for t, (sk, pk) in RULES.items():
        d = BK.by_type()[t]
        assert (d.skip, d.pause) == (sk, pk), t
        assert defs[t]["skip"]["kind"] == (sk or None) and defs[t]["pause"]["kind"] == (pk or None)
        assert (d.skip_text if sk else d.skip_no) and (d.pause_text if pk else d.pause_no), t      # 버튼·확인창 문구
    assert BK.control("valve", {"action": "open"}, "skip")[0] and not BK.control("valve", {"action": "close"}, "skip")[0]
    assert BK.control("shutter", {"action": "open"}, "skip")[0] and not BK.control("shutter", {"action": "close"}, "skip")[0]
    assert BK.control("pressure", {"wait": True}, "pause")[0] and not BK.control("pressure", {"wait": False}, "pause")[0]
    assert not BK.control("power_on", {"wait": False}, "pause")[0]
    assert BK.control(BK.DELAY_NO, {}, "skip")[1] == "wait" and BK.control(BK.DELAY_NO, {}, "pause")[1] == "timer"


def test_commands_defined_operator_bench_off_and_paths():
    by = C.by_kind()
    for k in ("PROCESS_SKIP", "PROCESS_PAUSE", "PROCESS_RESUME", "PROCESS_EDIT"):
        c = by[k]
        assert c.role == "operator" and c.bench_ok is False and "STOPPING" not in c.states
        assert C.Source.WEB in c.sources
        assert (C.Source.HMI in c.sources) == (k != "PROCESS_EDIT")
    codes = C.reason_codes()
    assert {"SKIP_STALE", "PAUSE_STALE", "EDIT_STALE", "SKIP_NOT_ALLOWED", "PAUSE_NOT_ALLOWED", "EDIT_TOUCHES_STARTED",
            "PAUSE_TIMEOUT", "PROCESS_SKIP", "PROCESS_PAUSE", "PROCESS_RESUME", "PROCESS_EDIT"} <= codes
    kinds = {(e["source"], e["kind"]) for e in W.bundle(defaults(P.PARAMS))["eventKinds"]}
    assert {("process", k) for k in ("skip", "pause", "resume", "edit", "pause_long")} <= kinds


# ───────────────────────── 대기·메인 ─────────────────────────
def test_wait_pause_freezes_remaining_and_skip_goes_next(tmp_path):
    r = R(tmp_path)
    blocks = [B("wait", sec=60), B("wait", sec=20)]
    r.on(3, _cmd(r, "PROCESS_START", blocks=blocks))
    got = {}
    r.on(13, lambda: got.setdefault("before", r.rig.process.remain_s()))
    r.on(14, _cmd(r, "PROCESS_PAUSE", no="1", revision=1))
    r.on(40, lambda: got.update(during=r.rig.process.remain_s(), state=_cur(r), snap=r.rig.process.snapshot_fields()))
    r.on(50, _cmd(r, "PROCESS_RESUME", revision=1))
    r.on(52, lambda: got.setdefault("after", r.rig.process.remain_s()))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "2", lambda: got.setdefault("t2", r.c.monotonic()))
    r.run(130)
    assert [x.reason for x in _res(r)] == [None, None, None]
    assert got["during"] == got["before"] - 1 or got["during"] == got["before"]       # 멈춘 동안 남은 시간 그대로
    assert got["state"]["paused"]["no"] == "1" and got["state"]["paused"]["kind"] == "timer"
    assert got["snap"]["proc.paused"] is True and got["snap"]["proc.state"] == "WAIT"   # 상태 이름은 그대로
    assert abs(got["after"] - got["during"]) <= 2
    ex = r.ev("run_end")[0].data["summary"]["executed"]
    assert ex[0]["result"] == "done" and 35 <= ex[0]["pausedSec"] <= 37
    pe = [e for e in r.rig.process.events if e.kind in ("pause", "resume")]
    assert [e.code for e in pe] == ["PROCESS_PAUSE", "PROCESS_RESUME"] and 35 <= pe[1].data["pausedSec"] <= 37


def test_skip_wait_records_skipped_and_summary(tmp_path):
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("wait", sec=600), B("wait", sec=3)]))
    r.on(10, _cmd(r, "PROCESS_SKIP", no="1", revision=1))
    r.run(40)
    assert _ends(r) == [("done", None)]
    s = r.ev("run_end")[0].data["summary"]
    assert [(x["no"], x["result"]) for x in s["executed"]][:2] == [("1", "skipped"), ("2", "done")]
    assert s["skips"] == 1 and s["pauses"] == 0 and s["revision"] == 1
    sk = [e for e in r.rig.process.events if e.kind == "skip"][0]
    assert sk.code == "PROCESS_SKIP" and sk.data["kind"] == "wait" and sk.data["no"] == "1"


def test_main_pause_closes_and_reopens_shutter_skip_closes(tmp_path):
    r = R(tmp_path)
    plc = r.plc
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("main", sec=120, close=False), B("main", sec=300, close=False),
                                              B("wait", sec=2)]))
    got = {}
    r.on(10, lambda: got.setdefault("open", plc.coils[A["ms"]]))
    r.on(11, _cmd(r, "PROCESS_PAUSE", no="1", revision=1))
    r.on(15, lambda: got.setdefault("paused", plc.coils[A["ms"]]))
    r.on(20, _cmd(r, "PROCESS_RESUME", revision=1))
    r.on(24, lambda: got.setdefault("resumed", plc.coils[A["ms"]]))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "2", lambda: None)
    r.on(140, _cmd(r, "PROCESS_SKIP", no="2", revision=1))
    r.on(144, lambda: got.setdefault("skipped", plc.coils[A["ms"]]))
    r.run(170)
    assert (got["open"], got["paused"], got["resumed"], got["skipped"]) == (True, False, True, False)
    ex = r.ev("run_end")[0].data["summary"]["executed"]
    assert [x["result"] for x in ex][:3] == ["done", "skipped", "done"]
    sk = [e for e in r.rig.process.events if e.kind == "skip"][0]
    assert sk.data["cleaned"] == ["Main 셔터 닫음"]


def test_pause_max_warns_then_stops(tmp_path):
    r = R(tmp_path, {"PROCESS_PAUSE_MAX_SEC": 330.0})
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("wait", sec=60)]))
    r.on(5, _cmd(r, "PROCESS_PAUSE", no="1", revision=1))
    r.run(400)
    assert _ends(r) == [("error", "PAUSE_TIMEOUT")]
    pl = [e for e in r.rig.process.events if e.kind == "pause_long"]
    assert len(pl) == 1 and pl[0].code == "PAUSE_LONG" and 30 <= pl[0].data["pausedSec"] <= 32


# ───────────────────────── 거부 ─────────────────────────
def test_stale_and_not_allowed_and_not_paused(tmp_path):
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("power_off", dc=True), B("wait", sec=60), B("gas_off", ar=True)]))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "2", lambda: [
        r.results.append(r.rig.core.command(k, a)) for k, a in (
            ("PROCESS_SKIP", {"no": "1", "revision": 1}), ("PROCESS_SKIP", {"no": "2", "revision": 7}),
            ("PROCESS_PAUSE", {"no": "3", "revision": 1}), ("PROCESS_RESUME", {"revision": 1}),
            ("PROCESS_EDIT", {"revision": 3, "blocks": [B("wait", sec=1)]}),
            ("PROCESS_PAUSE", {"no": "2", "revision": 1}), ("PROCESS_PAUSE", {"no": "2", "revision": 1}))])
    r.when(lambda: _cur(r) and _cur(r)["current"] == "3", lambda: [
        r.results.append(r.rig.core.command(k, a)) for k, a in (
            ("PROCESS_SKIP", {"no": "3", "revision": 1}), ("PROCESS_PAUSE", {"no": "3", "revision": 1}))])
    r.on(40, _cmd(r, "PROCESS_SKIP", no="2", revision=1))           # 멈춘 블록도 건너뛴다
    r.run(80)
    assert [x.reason for x in _res(r)] == [None, "SKIP_STALE", "SKIP_STALE", "PAUSE_STALE", "PROCESS_NOT_PAUSED",
                                           "EDIT_STALE", None, "PROCESS_PAUSED", None, "SKIP_NOT_ALLOWED",
                                           "PAUSE_NOT_ALLOWED"]
    assert "끄기·닫기 블록은 건너뛸 수 없음" in [x.detail for x in _res(r)]
    assert _ends(r) == [("done", None)]


def test_rejected_while_stopping_and_outside_process(tmp_path):
    r = R(tmp_path)
    r.on(2, _cmd(r, "PROCESS_SKIP", no="1", revision=1))
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("wait", sec=2), B("end_stop"), B("wait", sec=2)]))
    r.when(lambda: r.rig.process.state == "STOPPING", lambda: [
        r.results.append(r.rig.core.command(k, a)) for k, a in (
            ("PROCESS_SKIP", {"no": "2", "revision": 1}), ("PROCESS_PAUSE", {"no": "2", "revision": 1}),
            ("PROCESS_EDIT", {"revision": 1, "blocks": [B("wait", sec=1)]}))])
    r.run(300)
    assert [x.reason for x in _res(r)] == ["STATE_NOT_ALLOWED", None] + ["STATE_NOT_ALLOWED"] * 3


# ───────────────────────── 순서 고치기 ─────────────────────────
def test_edit_rules_and_records(tmp_path):
    r = R(tmp_path, {"HEATER_ENABLED": False})
    orig = [B("wait", sec=30), B("wait", sec=600), B("wait", sec=5)]
    r.on(3, _cmd(r, "PROCESS_START", blocks=orig))
    good = [B("wait", sec=30), B("wait", sec=600), B("wait", sec=2), B("wait", sec=1)]
    r.on(8, _cmd(r, "PROCESS_EDIT", revision=1, blocks=[B("wait", sec=31)] + orig[1:]))        # 시작한 블록 고침
    r.on(9, _cmd(r, "PROCESS_EDIT", revision=1, blocks=orig[:1] + [B("wait", sec=-1)]))       # 검사 실패
    r.on(10, _cmd(r, "PROCESS_EDIT", revision=1, blocks=orig + [B("heater_ramp", target_c=100.0)]))   # 꺼진 장비
    r.on(11, _cmd(r, "PROCESS_EDIT", revision=1, blocks=good))
    r.on(12, _cmd(r, "PROCESS_EDIT", revision=1, blocks=good))                                  # 판이 올라갔다
    r.on(40, _cmd(r, "PROCESS_SKIP", no="2", revision=2))
    r.run(80)
    assert [x.reason for x in _res(r)] == [None, "EDIT_TOUCHES_STARTED", "RECIPE_INVALID", "DEVICE_DISABLED", None, "EDIT_STALE",
                                           None]
    assert _ends(r) == [("done", None)]
    run_start = r.ev("run_start")[0].data
    assert [b["params"]["sec"] for b in run_start["blocks"][:3]] == [30, 600, 5]      # 원래 레시피 그대로
    ed = [e for e in r.rig.process.events if e.kind == "edit"]
    assert len(ed) == 1 and ed[0].code == "PROCESS_EDIT" and ed[0].data["revision"] == 2
    assert [(b["type"], b["params"]["sec"]) for b in ed[0].data["blocks"]] == [("wait", 30), ("wait", 600), ("wait", 2),
                                                                              ("wait", 1)]
    assert ed[0].data["from"] == 3 and "3번부터" in ed[0].data["summary"]
    ex = r.ev("run_end")[0].data["summary"]["executed"]
    assert [(x["no"], x["result"], x["revision"]) for x in ex] == [("1", "done", 1), ("2", "skipped", 2), ("3", "done", 2),
                                                                  ("4", "done", 2), ("5", "done", 2)]
    assert r.ev("run_end")[0].data["summary"]["revision"] == 2


def test_edit_adds_device_uses_and_value_changes_apply(tmp_path):
    """사용 장비가 늘면 새 목록 기준(쓰는 장비·안전 규칙 구간), 공정 중 수정 값은 남은 블록이 쓰는 항목이면 새 목록 값."""
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("wait", sec=40), B("main", sec=5)]))
    r.on(5, _cmd(r, "PROCESS_ADJUST", device="process", item="main_remaining", value=90.0))
    r.on(8, _cmd(r, "PROCESS_EDIT", revision=1, blocks=[B("wait", sec=40), B("gas_intro", ar=True, ar_flow=10.0, zero=False),
                                                         B("main", sec=7)]))
    got = {}
    r.on(9, lambda: got.update(uses=dict(r.rig.process.uses), ov=dict(r.rig.process.overrides)))
    r.run(120)
    assert [x.reason for x in _res(r)] == [None, None, None]
    assert got["uses"]["ar"] is True and "process.main_remaining" not in got["ov"]
    ex = r.ev("run_end")[0].data["summary"]["executed"]
    assert [x["type"] for x in ex][:3] == ["wait", "gas_intro", "main"]
    main_end = [e for e in r.rig.process.events if e.kind == "block" and e.data.get("type") == "main"
                and e.data.get("phase") == "end"][0]
    assert 7.0 <= main_end.data["elapsed_s"] <= 12.0                               # 새 목록 값 7 s + 셔터 열고 닫기(수정 90 s 는 지움)


def test_repeat_inner_skip_and_running_repeat_is_fixed(tmp_path):
    r = R(tmp_path)
    rep = {"type": "repeat", "params": {"count": 2}, "blocks": [B("wait", sec=300), B("wait", sec=2)]}
    r.on(3, _cmd(r, "PROCESS_START", blocks=[rep, B("wait", sec=1)]))
    r.on(8, lambda: r.results.append(r.rig.core.command("PROCESS_SKIP", {"no": _cur(r)["current"], "revision": 1})))
    r.on(9, _cmd(r, "PROCESS_EDIT", revision=1, blocks=[dict(rep, params={"count": 1}), B("wait", sec=1)]))
    r.on(12, _cmd(r, "PROCESS_EDIT", revision=1, blocks=[rep, B("wait", sec=2), B("wait", sec=1)]))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "1.1#2",
           lambda: r.results.append(r.rig.core.command("PROCESS_SKIP", {"no": "1.1#2", "revision": _cur(r)["revision"]})))
    r.run(60)
    assert [x.reason for x in _res(r)] == [None, None, "EDIT_TOUCHES_STARTED", None, None]
    ex = [(x["no"], x["result"]) for x in r.ev("run_end")[0].data["summary"]["executed"]]
    assert ex[:4] == [("1.1#1", "skipped"), ("1.2#1", "done"), ("1.1#2", "skipped"), ("1.2#2", "done")]
    assert [n for n, _ in ex[4:6]] == ["2", "3"]


# ───────────────────────── 블록 종류별 정리 ─────────────────────────
def test_cond_wait_pause_resets_stable_and_power_on_skip_keeps_output(tmp_path):
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[
        B("cond_wait", field="heater.pv", op="ge", value=900.0, timeout_sec=30.0, on_timeout="abort"),
        B("power_on", dc_power=100.0), B("wait", sec=3)]))
    r.on(10, _cmd(r, "PROCESS_PAUSE", no="1", revision=1))
    r.on(70, _cmd(r, "PROCESS_RESUME", revision=1))                # 멈춘 60 s 는 한도(30 s)에 들어가지 않는다
    r.on(80, _cmd(r, "PROCESS_SKIP", no="1", revision=1))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "2" and r.rig.process.dc_on,
           lambda: r.results.append(r.rig.core.command("PROCESS_SKIP", {"no": "2", "revision": 1})))
    got = {}
    r.on(110, lambda: got.setdefault("dc", (r.rig.process.dc_on, r.rig.process.dcc.state)))
    r.run(112)
    assert [x.reason for x in _res(r)] == [None, None, None, None, None]
    assert got["dc"][0] is True                                      # 보낸 파워는 그대로(목표로 계속)
    assert not [e for e in r.ev("run_end") if e.data["reason"] == "COND_WAIT_TIMEOUT"]


def test_settle_skip_warns_and_base_skip_turns_filament_off(tmp_path):
    r = R(tmp_path, {"IG_ENABLED": True})
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("base_wait", target_torr=1e-9), B("heater_settle"), B("wait", sec=2)]))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "1" and r.rig.process.filament_by_process
           and r.rig.process.runner.step.action == "IG_BASE_WAIT",
           lambda: r.results.append(r.rig.core.command("PROCESS_SKIP", {"no": "1", "revision": 1})))
    r.run(120)
    assert [x.reason for x in _res(r)][:1] == [None]
    sk = [e for e in r.rig.process.events if e.kind == "skip"][0]
    assert sk.data["kind"] == "base" and sk.data["cleaned"] == ["이 공정이 켠 필라멘트 끔"]
    assert r.rig.process.filament_by_process is False


def test_delay_row_pause_and_skip(tmp_path):
    r = R(tmp_path)
    rows = [{"process_name": "a", "blocks": [B("wait", sec=2)]}, {"process_name": "delay 300s"}, {"process_name": "b", "blocks": [B("wait", sec=2)]}]
    r.on(3, _cmd(r, "RECIPE_LOAD", name="d", rows=rows))
    r.on(4, _cmd(r, "RECIPE_START"))
    r.when(lambda: r.rig.process.state == "CSV_DELAY", lambda: [
        r.results.append(r.rig.core.command(k, a)) for k, a in (
            ("PROCESS_PAUSE", {"no": "1", "revision": 0}), ("PROCESS_PAUSE", {"no": "delay", "revision": 0}))])
    r.on(60, lambda: r.results.append(r.rig.process.run_state(True)))
    r.on(61, _cmd(r, "PROCESS_SKIP", no="delay", revision=0))
    r.run(90)
    res = [x for x in r.results if x is not None]
    assert [x.reason for x in res if hasattr(x, "reason")][:5] == [None, None, "PAUSE_STALE", None, None]
    st = [x for x in res if isinstance(x, dict)][0]
    assert st["current"] == "delay" and st["paused"]["kind"] == "timer" and st["blocks"][0]["status"] == "paused"
    assert [e.data["status"] for e in r.ev("run_end")] == ["done", "done"]
    assert r.ev("list_done")


# ───────────────────────── state·기록 ─────────────────────────
def test_state_run_blocks_full_only_when_changed_or_every_10s(tmp_path):
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("wait", sec=40), B("power_off", dc=True)]))
    states = []
    sub = {}

    def grab():
        if "s" not in sub:
            sub["s"] = r.rig.hub.subscribe()
        while True:
            x = sub["s"].get(0)
            if x is None:
                return
            if isinstance(x, dict) and x.get("type") == "state":
                states.append(x["data"])
    r.rig.loop.tick_hooks.append(lambda t: grab() if t.seq >= 2 else None)
    r.run(30)
    run = [s["run"] for s in states if s.get("run")]
    full = [x for x in run if "blocks" in x]
    assert run and len(full) < len(run)
    b = full[-1]["blocks"]
    assert [(x["no"], x["status"], x["skip"], x["pause"]) for x in b[:2]] == [("1", "running", True, True),
                                                                              ("2", "waiting", False, False)]
    assert b[0]["durationS"] == 40 and b[1]["skipText"] == "끄기·닫기 블록은 건너뛸 수 없음"


def test_hub_run_end_summary_has_executed(tmp_path):
    r = R(tmp_path)
    sent = []
    real = r.rig.hub.publish
    r.rig.hub.publish = lambda t, d: (sent.append((t, d)) if t == "run_end" else None, real(t, d))[1]
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("wait", sec=2)]))
    r.run(20)
    sm = [d for t, d in sent][-1]["summary"]
    assert sm["name"] and sm["executed"][0]["result"] == "done" and sm["revision"] == 1 and sm["skips"] == 0


# ───────────────────────── 섞어서 끝까지 ─────────────────────────
def test_mixed_skip_pause_edit_whole_row(tmp_path):
    r = R(tmp_path)
    blocks = [B("gas_intro", ar=True, ar_flow=20.0, zero=False), B("pressure", sp=1, value=5.0),
              B("wait", sec=600), B("main", sec=60, close=True), B("gas_off", ar=True)]
    r.on(3, _cmd(r, "PROCESS_START", blocks=blocks))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "3", lambda: [
        r.results.append(r.rig.core.command("PROCESS_EDIT", {"revision": 1, "blocks": blocks[:3] + [
            B("main", sec=30, close=True), B("wait", sec=2), B("gas_off", ar=True)]})),
        r.results.append(r.rig.core.command("PROCESS_SKIP", {"no": "3", "revision": 2}))])
    r.when(lambda: _cur(r) and _cur(r)["current"] == "4" and r.plc.coils[A["ms"]], lambda: [
        r.results.append(r.rig.core.command("PROCESS_PAUSE", {"no": "4", "revision": 2}))])
    r.when(lambda: r.rig.process.paused is not None and r.c.monotonic() - r.rig.process.paused["t0"] >= 20,
           lambda: r.results.append(r.rig.core.command("PROCESS_RESUME", {"revision": 2})))
    r.run(400)
    assert [x.reason for x in _res(r)] == [None, None, None, None, None]
    assert _ends(r) == [("done", None)]
    s = r.ev("run_end")[0].data["summary"]
    ex = [(x["type"], x["result"], x["revision"]) for x in s["executed"]]
    assert ex[:6] == [("gas_intro", "done", 1), ("pressure", "done", 1), ("wait", "skipped", 1), ("main", "done", 2),
                      ("wait", "done", 2), ("gas_off", "done", 2)]
    assert s["skips"] == 1 and s["pauses"] == 1 and s["revision"] == 2 and s["pausedSec"] >= 19
    main = [x for x in s["executed"] if x["type"] == "main"][0]
    assert 49 <= main["end"] - main["start"] <= 55 and main["pausedSec"] >= 19


@pytest.mark.cascade
def test_settle_skip_warns_not_checked(tmp_path):
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("heater_ramp", target_c=200.0), B("heater_settle", sec=600.0, max_sec=3600.0),
                                              B("wait", sec=2)]))
    r.when(lambda: _cur(r) and _cur(r)["current"] == "2" and r.rig.process.runner.cur.issued,
           lambda: r.results.append(r.rig.core.command("PROCESS_SKIP", {"no": "2", "revision": 1})))
    r.run(40)
    assert [x.reason for x in _res(r)] == [None, None]
    w = [e for e in r.rig.process.events if e.kind == "warn" and e.code == "HEATER_SETTLE_NOT_CHECKED"]
    assert len(w) == 1 and r.rig.process.settle_track is None
    assert [x["result"] for x in r.ev("run_end")[0].data["summary"]["executed"]][:2] == ["done", "skipped"]


def test_heater_wait_pause_shifts_wait_clock(tmp_path):
    r = R(tmp_path)
    r.on(3, _cmd(r, "PROCESS_START", blocks=[B("heater_ramp", target_c=300.0), B("heater_wait")]))
    got = {}
    r.when(lambda: _cur(r) and _cur(r)["current"] == "2" and r.rig.heater.proc.get("t0") is not None,
           lambda: [got.setdefault("t0", r.rig.heater.proc["t0"]),
                    r.results.append(r.rig.core.command("PROCESS_PAUSE", {"no": "2", "revision": 1}))])
    r.when(lambda: r.rig.process.paused is not None and r.c.monotonic() - r.rig.process.paused["t0"] >= 60,
           lambda: [r.results.append(r.rig.core.command("PROCESS_RESUME", {"revision": 1})),
                    got.setdefault("t0b", r.rig.heater.proc["t0"]), got.setdefault("phase", r.rig.heater.proc["phase"])])
    r.run(120)
    assert [x.reason for x in _res(r)] == [None, None, None]
    assert 59.0 <= got["t0b"] - got["t0"] <= 61.5 and got["phase"] == "RAMP"
