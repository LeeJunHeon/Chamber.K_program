"""SafetyEngine — 규칙마다 성립·해제·상태 밖 미발동, 우선순위·run_end 사유, 정지 순서의 필수 단계, 거부 검사.

장비 없이: 가짜 처리기(호출만 기록)와 직접 만든 값(View)으로. 실제 드라이버·시뮬레이터 시험은 test_chk_safety.
"""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.safety_engine import Step, StepRunner
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import safety as S
from v2.chk import safety_rules as SR
from v2.chk.safety_rules import Act

CFG = defaults(P.PARAMS)
BLOCKS = S.DEVICE_BLOCKS
T0 = 1_760_000_000.0


class Dev:
    last = None


class H:
    """엔진 + 가짜 처리기. calls = [(조치, 인자)]."""

    def __init__(self, **proc):
        self.c = VirtualClock(start_wall=T0)
        self.c.register()
        self.p = S.SimpleProcess(**proc)
        self.s = S.ChkSafety(self.c, CFG, self.p, Dev(), Dev(), Dev(), Dev(), Dev())
        self.e = self.s.engine
        self.calls = []
        for k in list(self.e.handlers):
            if k != "PROCESS_ABORT":
                self.e.handlers[k] = self._rec(k)

    def _rec(self, k):
        def h(a, done):
            self.calls.append((k, a))
            done(None)
        return h

    def view(self, t, vals=None, q=None, ages=None):
        qq = {b: "ok" for b in BLOCKS}
        qq.update(q or {})
        aa = {b: 0.0 for b in BLOCKS}
        aa.update(ages or {})
        return S.View(dict(vals or {}), T0 + t, qq, aa)

    def feed(self, where, t, vals=None, q=None, ages=None):
        v = self.view(t, vals, q, ages)
        return self.e.evaluate(where, v.t, v, self.p)

    def ev(self, kind, rule=None):
        return [e for e in self.e.events if e.kind == kind and (rule is None or e.rule == rule)]

    def settle(self):
        self.c.wait_until(None, lambda: not self.e.busy and len(self.e.seq_results) >= len(self.e._threads))
        for th in self.e._threads:
            th.join(5)


# (규칙, where, 공정 설정, 참 값, 거짓 값, 성립까지 초, 표본 간격, 상태 밖)
DEAD = {"heater.run": True, "heater.fault": False, "heater.mv": 0}
CASES = [
    ("S2", "plc250", dict(state="MAIN"), {"plc.mv": False}, {"plc.mv": True}, 0.0, 0.25, "IDLE"),
    ("S3", "plc250", dict(state="MAIN"), {"plc.mv_interlock": False}, {"plc.mv_interlock": True}, 1.0, 0.25, "IDLE"),
    ("S16", "plc250", dict(), {"heater.fault": True}, {"heater.fault": False}, 0.0, 0.25, None),
    ("S17", "plc250", dict(), DEAD, dict(DEAD, **{"heater.mv": 900}), 5.6, 0.25, None),     # 유예 5 s + 0.6 s
    ("S18A", "tick", dict(heater_recipe_running=True), DEAD, dict(DEAD, **{"heater.mv": 900}), 15.0, 1.0, "recipe"),
    ("S18B", "tick", dict(heater_recipe_running=True), {"heater.run": False}, {"heater.run": True}, 5.0, 1.0, "recipe"),
    ("S18C", "tick", dict(heater_recipe_running=True), {"heater.tc_err": True, "heater.pv": 25.0},
     {"heater.tc_err": False, "heater.pv": 25.0}, 60.0, 1.0, "recipe"),
    ("S31A", "tick", dict(state="POWER_WAIT", uses_dc=True), {"dc.i_set": 0.3, "dc.p": 0.5},
     {"dc.i_set": 0.3, "dc.p": 50.0}, 15.0, 1.0, "MAIN"),
    ("S31B", "tick", dict(state="MAIN", uses_dc=True, dc_holding=True), {"dc.i": 0.01}, {"dc.i": 0.3}, 10.0, 1.0, "IDLE"),
    ("S31C", "tick", dict(state="MAIN", uses_dc=True, dc_set_w=100.0), {"dc.p": 80.0}, {"dc.p": 100.0}, 5.0, 1.0,
     "POWER_WAIT"),
    ("S32", "tick", dict(state="MAIN", uses_rf=True), {"rf.ref_w": 5.0}, {"rf.ref_w": 1.0}, 60.0, 1.0, "IDLE"),
    ("S33A", "tick", dict(state="POWER_WAIT", uses_rf=True), {"rf.dac": 500, "rf.fwd_w": 0.0},
     {"rf.dac": 500, "rf.fwd_w": 50.0}, 10.0, 1.0, "MAIN"),
    ("S33B", "tick", dict(state="MAIN", uses_rf=True, rf_holding=True, rf_set_w=100.0), {"rf.fwd_w": 80.0},
     {"rf.fwd_w": 100.0}, 5.0, 1.0, "IDLE"),
    ("S34A", "tick", dict(state="MAIN", uses_rfp=True, rfp_set_w=100.0), {"rfp.out_on": True, "rfp.fwd_w": 80.0},
     {"rfp.out_on": True, "rfp.fwd_w": 100.0}, 15.0, 1.0, "IDLE"),
    ("S34B", "tick", dict(state="MAIN", uses_rfp=True), {"rfp.ref_w": 25.0}, {"rfp.ref_w": 2.0}, 15.0, 1.0, "IDLE"),
    ("S38A", "tick", dict(state="MAIN", flow_set={"ar": 20.0, "o2": None}), {"mfc.ar_sccm": 15.0}, {"mfc.ar_sccm": 20.0},
     10.0, 1.0, "GAS_STABILIZE"),
    ("S38B", "tick", dict(state="MAIN", sp1_mtorr=5.0), {"mfc.pressure_mtorr": 7.0}, {"mfc.pressure_mtorr": 5.0},
     10.0, 1.0, "PRESSURE_SP1"),
    ("S49", "tick", dict(state="MAIN", uses_rfp=True), {"rfp.itl_open": True}, {"rfp.itl_open": False}, 0.0, 1.0, "IDLE"),
]


@pytest.mark.parametrize("rid,where,proc,true_v,false_v,hold,dt,outside", CASES, ids=[c[0] for c in CASES])
def test_rule_fires_after_hold_and_releases(rid, where, proc, true_v, false_v, hold, dt, outside):
    h = H(**proc)
    t, fired_at = 0.0, None
    while t <= hold + 3 * dt and fired_at is None:
        if rid in h.feed(where, t, true_v):
            fired_at = t
        t += dt
    assert fired_at is not None and fired_at == pytest.approx(hold, abs=dt + 1e-9), (rid, fired_at)
    assert h.ev("fired", rid) and len(h.ev("fired", rid)) == 1
    h.feed(where, t, true_v)                                  # 활성 동안 다시 발동하지 않는다(S44)
    assert len(h.ev("fired", rid)) == 1
    rule = next(r for r in h.e.rules if r.id == rid)
    if rule.cls == "abort":                                    # 중단 → 공정이 STOPPING 으로 — 규칙은 상태 밖이 된다
        code = [x.args["reason"] for x in rule.acts(h.view(t), h.p) if x.kind == "PROCESS_ABORT"][0]
        assert h.e.run_reason == code and h.p.state in ("STOPPING", "ABORTED")
    else:
        h.feed(where, t + dt, false_v)
        assert h.ev("released", rid)
    # 상태 밖(또는 적용 조건 거짓)이면 발동하지 않음
    if outside is not None:
        h2 = H(**dict(proc, **({"heater_recipe_running": False} if outside == "recipe" else {"state": outside})))
        for k in range(int((hold + 3) / dt) + 2):
            assert rid not in h2.feed(where, k * dt, true_v)


PLAIN = [c for c in CASES if c[5] > 0 and c[0] not in ("S17", "S18A")]     # 유예가 있는 규칙은 아래 따로


@pytest.mark.parametrize("rid,where,proc,true_v,false_v,hold,dt,outside", PLAIN, ids=[c[0] for c in PLAIN])
def test_stale_samples_do_not_reset_or_confirm(rid, where, proc, true_v, false_v, hold, dt, outside):
    """모름(stale·fail) 표본은 판정을 바꾸지 않는다 — 지우지도, 확정하지도 않음."""
    h = H(**proc)
    block = next(iter(true_v)).split(".")[0]
    h.feed(where, 0.0, true_v)
    h.feed(where, hold / 2, false_v, q={block: "stale"})          # 모름 — since 그대로
    assert rid not in h.feed(where, hold + 2 * dt, {}, q={block: "fail"})   # 모름만으로는 확정 안 함
    assert rid in h.feed(where, hold + 3 * dt, true_v)


AGE_CASES = [("S6", "plc", "PLC_COMM_LOSS_ABORT_SEC", dict(state="MAIN"), "IDLE"),
             ("S7", "plc", "PLC_DISCONNECT_NOTIFY_SEC", dict(), None),
             ("S30", "dc", "DC_COMM_LOSS_ABORT_SEC", dict(state="MAIN", uses_dc=True), "IDLE"),
             ("S34C", "rfp", "RFPULSE_COMM_LOSS_ABORT_SEC", dict(state="MAIN", uses_rfp=True), "IDLE"),
             ("S39B", "ig", "IG_COMM_LOSS_ABORT_SEC", dict(state="BASE_WAIT"), "MAIN")]


@pytest.mark.parametrize("rid,block,key,proc,outside", AGE_CASES, ids=[c[0] for c in AGE_CASES])
def test_comm_loss_rules_by_age(rid, block, key, proc, outside):
    h = H(**proc)
    lim = CFG[key]
    fired = None
    for k in range(1, int(lim) + 3):
        if rid in h.feed("tick", k, {}, q={block: "fail"}, ages={block: float(k)}):
            fired = k
            break
    assert fired == int(lim)
    if rid == "S7":                                             # 알림 — 링크 복구 때 해제 + PLC_RECONNECTED
        h.feed("tick", lim + 2, {}, q={block: "ok"}, ages={block: 0.0})
        assert h.ev("released", rid) and [a["code"] for k, a in h.calls if k == "NOTIFY"][-1] == "PLC_RECONNECTED"
    else:
        assert h.p.state in ("STOPPING", "ABORTED") and h.e.run_reason
    if outside:
        h2 = H(**dict(proc, state=outside))
        assert rid not in h2.feed("tick", 1, {}, q={block: "fail"}, ages={block: lim + 5})


def test_heater_dead_grace_and_stale():
    """S17: RUN 상승 뒤 유예(5 s) 동안은 판정하지 않고, 그 뒤 0.6 s. 중간의 모름 표본은 판정을 바꾸지 않는다."""
    h = H()
    for k in range(0, 21):                                     # 0~5 s 유예
        assert "S17" not in h.feed("plc250", k * 0.25, DEAD)
    h.feed("plc250", 5.25, {}, q={"heater": "stale"})
    assert "S17" not in h.feed("plc250", 5.5, DEAD)
    assert "S17" in h.feed("plc250", 5.75, DEAD)               # 5.0 부터 0.6 s 넘김(5.25 모름은 무시)


def test_s39b_ignores_suppressed():
    h = H(state="BASE_WAIT")
    assert "S39B" not in h.feed("tick", 1, {}, q={"ig": "suppressed"}, ages={"ig": 120.0})


def test_s3_blip_is_info_only():
    h = H(state="MAIN")
    h.feed("plc250", 0.0, {"plc.mv_interlock": False})
    h.feed("plc250", 0.5, {"plc.mv_interlock": True})
    assert h.ev("blip", "S3") and h.ev("blip", "S3")[0].code == "MV_INTERLOCK_BLIP"
    assert not h.ev("fired", "S3") and h.p.state == "MAIN"


def test_s4_skip_when_stopping():
    h = H(state="MAIN")
    h.e.stopping = "stop"
    assert "S2" not in h.feed("plc250", 0.0, {"plc.mv": False})
    assert "S3" not in h.feed("plc250", 5.0, {"plc.mv_interlock": False})


def test_s16_actions_depend_on_use():
    h = H(state="MAIN", uses_heater=True, heater_recipe_running=True)
    h.feed("plc250", 0.0, {"heater.fault": True, "heater.ot": True})
    h.settle()
    kinds = [k for k, _ in h.calls]
    assert kinds[0] == "HEATER_FORCE_OFF" and "HEATER_RECIPE_ABORT" in kinds
    assert h.e.run_reason == "HEATER_FAULT" and h.p.state == "ABORTED"
    note = [a for k, a in h.calls if k == "NOTIFY"][0]
    assert note["code"] == "HEATER_FAULT" and note["detail"] == "원인 OT"
    h2 = H(state="MAIN", uses_heater=False)                   # 히터를 안 쓰면 RUN OFF·알림만
    h2.feed("plc250", 0.0, {"heater.fault": True})
    assert h2.e.run_reason is None and h2.p.state == "MAIN"
    assert [k for k, _ in h2.calls] == ["HEATER_FORCE_OFF", "NOTIFY"]


# ───────── 이벤트 규칙 ─────────
def test_event_rules():
    h = H(state="MAIN")
    assert h.s.event("plc_restarted") == ["S9"] and h.e.run_reason == "PLC_RESTARTED"
    h = H(state="IDLE")
    assert h.s.event("plc_restarted") == ["S9"] and h.e.run_reason is None
    assert [a["code"] for k, a in h.calls if k == "NOTIFY"] == ["PLC_RESTARTED"]
    assert h.s.event("plc_restarted") == ["S9"]                # 이벤트 규칙은 매번 다시 무장

    h = H(state="ABORTED", comm_aborted=True)
    assert h.s.event("plc_link_up") == ["S10"] and not h.p.comm_aborted
    assert [k for k, _ in h.calls][:3] == ["RF_SAFE_OFF", "HEATER_FORCE_OFF", "GAS_VALVES_CLOSE"]
    assert h.s.event("plc_link_up") == []                     # 두절 중단이 없었으면 안 함

    h = H()
    assert h.s.event("plc_first_poll") == ["S15"] and h.calls[0][0] == "HEATER_RESIDUAL_CLEAR"

    h = H(state="IDLE")
    assert h.s.event("heater_run_falling", h.view(0, {"heater.pv": 150.0})) == ["S26"]
    assert h.calls == [("HEATER_GAS_HOLD", {})]
    assert H(state="IDLE").s.event("heater_run_falling", h.view(0, {"heater.pv": 50.0})) == []
    assert H(state="MAIN").s.event("heater_run_falling", h.view(0, {"heater.pv": 150.0})) == []

    h = H()
    assert h.s.event("heater_gas_failed") == ["S27"]
    assert [k for k, _ in h.calls] == ["GAS_FLOW_OFF", "GAS_EXHAUST", "GAS_VALVES_CLOSE", "NOTIFY"]

    h = H(state="POWER_WAIT", uses_rfp=True)
    h.s.on_rfp_start_done("rfp:start", "RFP_START_FAILED", [])
    assert h.e.run_reason == "RFP_START_FAILED" and h.calls[0][0] == "RFP_SAFE_OFF"
    assert H(state="MAIN").s.event("rfp_start_failed") == []

    h = H()
    assert h.s.event("shutdown") == ["S41"] and h.calls == [("HEATER_FORCE_OFF", {})]


# ───────── 우선순위·사유 ─────────
def test_priority_abort_before_notify():
    """같은 평가에서 S38B(알림, 10 s 째)와 S49(중단, 즉시)가 함께 성립 — 중단·OFF 가 알림보다 먼저."""
    h = H(state="MAIN", uses_rfp=True, sp1_mtorr=5.0)
    for k in range(0, 10):
        assert h.feed("tick", k, {"mfc.pressure_mtorr": 9.0, "rfp.itl_open": False}) == []
    out = h.feed("tick", 10, {"mfc.pressure_mtorr": 9.0, "rfp.itl_open": True})
    assert out == ["S49", "S38B"]
    kinds = [k for k, _ in h.calls]                            # (정지 순서 스레드의 단계가 사이에 끼어도 된다)
    assert kinds.index("RFP_SAFE_OFF") < kinds.index("NOTIFY")
    assert h.e.run_reason == "RFP_FAULT"


def test_same_eval_aborts_first_reason_others_events():
    h = H(state="MAIN", uses_rfp=True, uses_dc=True)
    out = h.feed("tick", 0, {"rfp.itl_open": True}, q={"dc": "fail"}, ages={"dc": 20.0})
    assert out == ["S30", "S49"]                               # 같은 등급은 표 순서
    assert h.e.run_reason == "DC_COMM_LOST"
    also = h.ev("also", "S49")
    assert also and also[0].code == "RFP_FAULT"
    # 두 규칙의 OFF 조치는 둘 다 실행
    assert {"DC_SAFE_OFF", "RFP_SAFE_OFF"} <= {k for k, _ in h.calls}


def test_all_stop_outranks_abort():
    h = H(state="MAIN", uses_rfp=True)
    h.s.all_stop()
    assert h.e.run_reason == "ALL_STOP"
    assert h.e.abort("RFP_FAULT", rule="S49") is False          # ALL STOP 중 중단 — 이벤트만, 정지 순서 다시 안 함
    assert h.e.run_reason == "ALL_STOP" and h.ev("also", "S49")
    h.settle()
    assert h.p.state == "ABORTED" and "abort" not in h.e.seq_results


# ───────── 정지 순서 ─────────
def _runner(handlers):
    c = VirtualClock(start_wall=T0)
    c.register()
    evs = []
    return c, StepRunner(c, handlers, evs.append), evs


def test_step_runner_must_steps_run_after_exception():
    calls = []

    def ok(name):
        return lambda a, done: (calls.append(name), done(None))

    def boom(a, done):
        calls.append("boom")
        raise RuntimeError("일부러")
    c, r, evs = _runner({"A": ok("A"), "B": boom, "C": ok("C"), "D": ok("D"), "E": ok("E")})
    res = r.run("stop", [Step(1, Act("A"), must=False), Step(2, Act("B")), Step(3, Act("C"), must=False),
                         Step(4, Act("D")), Step(5, Act("E"))])
    assert calls == ["A", "boom", "D", "E"]
    assert [x[2] for x in res][:3] == ["ok", "error:RuntimeError: 일부러", "skipped"]
    assert [e.kind for e in evs].count("step") == 5 and evs[-1].kind == "seq_done"


def test_step_runner_timeout_moves_on():
    calls = []
    c, r, evs = _runner({"SLOW": lambda a, done: calls.append("slow"),          # done 을 안 부름
                         "NEXT": lambda a, done: (calls.append("next"), done(None))})
    t0 = c.monotonic()
    res = r.run("stop", [Step(1, Act("SLOW"), timeout_s=3.0), Step(2, Act("NEXT"))])
    assert calls == ["slow", "next"] and res[0][2] == "timeout" and c.monotonic() - t0 == pytest.approx(3.0)


def test_stop_and_all_stop_sequences_from_table():
    h = H(state="MAIN", uses_heater=True, filament_by_process=True)
    assert h.s.stop()
    h.settle()
    kinds = [k for k, _ in h.calls]
    assert kinds == ["IG_FILAMENT_OFF", "SHUTTERS_CLOSE", "HEATER_FORCE_OFF", "DC_SAFE_OFF", "RFP_SAFE_OFF",
                     "RF_SAFE_OFF", "GAS_FLOW_OFF", "GAS_EXHAUST", "SHUTTERS_CLOSE", "GAS_VALVES_CLOSE"]
    assert h.p.state == "DONE" and h.e.run_reason is None and not h.e.busy
    h = H(state="MAIN")
    h.s.all_stop()
    h.settle()
    kinds = [k for k, _ in h.calls]
    # v1 순서: PLC 비상 정지 → DC 직접 OFF → 공정 정지 순서(유량 OFF·MFC 밸브 열기) → RF Pulse(사용자 결정 2026-10-20)
    assert kinds == ["PLC_OUTPUTS_OFF", "DC_SAFE_OFF", "GAS_FLOW_OFF", "GAS_EXHAUST", "RFP_SAFE_OFF"]
    assert h.calls[0][1] == {}                                  # 펌프 유지 옵션 없음 — 전부 끈다
    assert ("abort", "ALL_STOP") in h.p.log and h.p.state == "ABORTED"
    res = dict((n, st) for n, _, st in h.e.seq_results["all_stop"])
    assert res[4] == "sent" and res[9] == "ok"                  # DC 는 보내고 다음 단계로, 끝에서 결과 확인
    h = H(state="IDLE")
    h.s.all_stop()
    h.settle()
    assert [k for k, _ in h.calls] == ["PLC_OUTPUTS_OFF", "DC_SAFE_OFF", "GAS_FLOW_OFF", "RFP_SAFE_OFF"]   # 공정 없으면 배기 없음


def test_stop_sequence_exception_still_runs_must_steps():
    h = H(state="MAIN", uses_heater=True, filament_by_process=True)

    def boom(a, done):
        raise RuntimeError("셔터 쓰기 예외")
    h.e.handlers["SHUTTERS_CLOSE"] = boom
    h.e.handlers["DC_SAFE_OFF"] = lambda a, done: done("DC_OFF_UNCONFIRMED")
    h.s.stop()
    h.settle()
    kinds = [k for k, _ in h.calls]
    assert kinds == ["IG_FILAMENT_OFF", "HEATER_FORCE_OFF", "RFP_SAFE_OFF", "RF_SAFE_OFF", "GAS_FLOW_OFF", "GAS_EXHAUST",
                     "GAS_VALVES_CLOSE"]
    res = dict((n, st) for n, _, st in h.e.seq_results["stop"])
    assert res[2].startswith("error:") and res[4] == "error:DC_OFF_UNCONFIRMED" and res[11] == "ok"


def test_all_stop_exception_skips_only_optional():
    h = H(state="MAIN", filament_by_process=True)
    h.e.handlers["PLC_OUTPUTS_OFF"] = lambda a, done: (_ for _ in ()).throw(RuntimeError("PLC 예외"))
    h.s.all_stop()
    h.settle()
    kinds = [k for k, _ in h.calls]
    assert kinds == ["DC_SAFE_OFF", "GAS_FLOW_OFF", "GAS_EXHAUST", "RFP_SAFE_OFF"]
    assert dict((n, st) for n, _, st in h.e.seq_results["all_stop"])[7] == "skipped"     # 필라멘트(필수 아님)


# ───────── 거부 검사·잠금 ─────────
def test_check_start_guards():
    h = H()
    good = {"plc.mv": True, "plc.mv_interlock": True, "heater.itl": True, "heater.fault": False}
    assert h.s.check_start(h.view(0, good), True) is None
    assert h.s.check_start(h.view(0, dict(good, **{"plc.mv": False})), False) == "MV_NOT_OPEN"
    assert h.s.check_start(h.view(0, dict(good, **{"plc.mv_interlock": False})), False) == "MV_INTERLOCK_OFF"
    assert h.s.check_start(h.view(0, good, q={"plc": "stale"}, ages={"plc": 6.0}), False) == "MV_STATE_UNKNOWN"
    assert h.s.check_start(h.view(0, dict(good, **{"heater.fault": True})), True) == "HEATER_FAULT_ACTIVE"
    assert h.s.check_start(h.view(0, dict(good, **{"heater.fault": True})), False) is None
    h.p.heater_gas_state = "HOLD"
    assert h.s.check_start(h.view(0, good), False) == "HEATER_GAS_ACTIVE"
    assert h.s.check_shutdown() == "HEATER_GAS_ACTIVE"
    h = H(state="MAIN")
    assert h.s.check_heater_start(h.view(0, good)) == "PROCESS_RUNNING" and h.s.check_shutdown() == "PROCESS_RUNNING"
    assert H().s.check_heater_start(h.view(0, dict(good, **{"heater.itl": False}))) == "HEATER_ITL_OFF"
    assert H().s.check_shutdown() is None
    codes = C.reason_codes()
    for x in ("MV_NOT_OPEN", "MV_INTERLOCK_OFF", "MV_STATE_UNKNOWN", "HEATER_FAULT_ACTIVE", "HEATER_ITL_OFF",
              "HEATER_GAS_ACTIVE", "PROCESS_RUNNING"):
        assert x in codes


def test_locks_s11_s28():
    h = H()
    row = {"ts": "2026-10-15T12:00:00"}
    for b in BLOCKS:
        row[f"{b}.q"], row[f"{b}.age_s"] = "ok", 0.0
    row["plc.q"], row["plc.age_s"] = "stale", 6.0
    row["heater.q"], row["heater.age_s"] = "fail", 2.0
    row["ig.q"], row["ig.age_s"] = "suppressed", 9.0
    h.s.on_row(row)
    assert h.s.locks == {"plc"}


# ───────── 표와의 대응 ─────────
def test_every_table_rule_has_a_home():
    ids = {r.id for r in SR.RULES}
    assert set(S.IMPLEMENTED_IN) == ids
    engine_ids = {r.id for r in S.build_rules(CFG)}
    for rid, where in S.IMPLEMENTED_IN.items():
        if where.startswith("engine"):
            assert rid in engine_ids, rid
    assert S.IMPLEMENTED_IN["S46"] == "on_hold" and "S46" not in engine_ids
    by = SR.by_id()
    for r in S.build_rules(CFG):
        assert not by[r.id].on_hold
        t = by[r.id]
        if r.id in S.WHERE_DIFFERS:
            continue
        if t.where in ("plc250", "tick"):
            assert r.where == t.where, r.id
        elif t.where.startswith("event:"):
            assert r.where == t.where[6:], r.id


def test_all_codes_exist():
    codes = C.reason_codes()
    h = H(state="MAIN", uses_heater=True, heater_recipe_running=True)
    v = h.view(0, {})
    for r in S.build_rules(CFG):
        for a in r.acts(v, h.p) + r.on_release(v, h.p):
            for k in ("reason", "code"):
                if k in a.args:
                    assert a.args[k] in codes, (r.id, a.args[k])
        assert not r.blip_code or r.blip_code in codes
    for k in h.s.engine.handlers:
        assert k in C.by_kind(), k
