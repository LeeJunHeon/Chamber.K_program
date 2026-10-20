"""HeaterEngine — v1 히터 제어 루프 이식 확인.

단위(장비 없이): 감속 접근 프로파일 식, HeaterHold(v1 tests/test_heater_hold.py 의 T25·T26b·T28·T29·T65b·T67·T80 시나리오를
다시 씀), 포화 감시, 레시피 단계(RAMP/SOAK/COOL·HOLD·SKIP·repeat 거부).
시험대(실제 드라이버 + PLC 시뮬레이터 열 모델): 수동 감속 접근, 공정 목표·유지 요청(RAMP→SOAK→HOLD_WAIT→도달),
히터 가스 준비·냉각 유지·해제.
"""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.chk import params as P
from v2.chk.heater_engine import (HeaterHold, RecipeRunner, RampProfiler, SaturationGuard, approach_rate,
                                  parse_recipe_steps, ramp_minutes, solve_rate_for_minutes)
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig

CFG = defaults(P.PARAMS)


class Port:
    def __init__(self):
        self.ev = []

    def target(self, c): self.ev.append(("sv", round(c, 1)))
    def ramp(self, n): self.ev.append(("ramp", n))
    def run(self, on): self.ev.append(("run", on))
    def mv_limit(self, v): self.ev.append(("mv", v))
    def sv2(self, c): self.ev.append(("sv2", round(c, 1)))
    def pv_sel(self, on): self.ev.append(("sel", on))


# ───────── 프로파일(lib/heater_profile) ─────────
def test_profile_matches_v1_formulas():
    assert approach_rate(30, 12, 20, 1) == 12                       # 구간 밖
    assert approach_rate(10, 12, 20, 1) == pytest.approx(1 + 11 * 0.5)
    assert approach_rate(0, 12, 20, 1) == 1
    assert ramp_minutes(20, 12, 20, 1) == pytest.approx(4.52, abs=0.01)   # v1 docstring: 접근 구간에만 4.52분
    assert ramp_minutes(100, 12, 20, 1) == pytest.approx(80 / 12 + 4.52, abs=0.01)
    r = solve_rate_for_minutes(100, 20, 20, 1)
    assert ramp_minutes(100, r, 20, 1) == pytest.approx(20, abs=1e-6) and r > 100 / 20


# ───────── HeaterHold(v1 T 시나리오) ─────────
class HH:
    def __init__(self, mode="tc2", **cfg):
        self.t = 1000.0
        c = dict(CFG, HEATER_HOLD_MODE=mode, HEATER_MV_LIMIT=1200, HEATER_MV_MIN=400, HEATER_HOLD_MV_ENTER_TOL_C=3.0,
                 HEATER_HOLD_MV_ENTER_SEC=60, HEATER_HOLD_MV_ARRIVE_TOL_C=1.0, HEATER_HOLD_MV_DRIFT_PV_C=0.5,
                 HEATER_HOLD_MV_DRIFT_MV=15, HEATER_HOLD_TC2_MARGIN_C=50.0, HEATER_HOLD_TC2_DRIFT_C=1.0, **cfg)
        self.port, self.msgs, self.events = Port(), [], []
        self.h = HeaterHold(c, self.port, lambda l, m: self.msgs.append(m), self.events.append, lambda: self.t)

    def step(self, dt=1.0, final=600.0, **kw):
        self.t += dt
        st = dict(ok=True, run=True, fault=False, pv=600.0, sv=600.0, sv_ramp=600.0, mv=800, mv_limit=1200, pv2=1052.3,
                  sv2=0.0, sv2_max=1100.0, ot2_limit=1150.0, pv_sel_eff=False)
        st.update(kw)
        if st["pv"] is None:
            st["ok"] = False
        self.h.tick(st, final)

    def arm(self, n=70, **kw):
        for _ in range(n):
            self.step(**kw)
            if self.h.state not in ("idle", "arming"):
                return


def test_hold_T25_tc2_engage_order():
    H = HH("tc2")
    H.arm()
    assert H.port.ev == [("sv2", 1052.3)] and H.h.state == "engaging_sv2"
    H.step(sv2=0.0)
    assert H.port.ev == [("sv2", 1052.3)]                          # 되읽기 전에는 pv_sel 없음
    H.step(sv2=1052.3)
    assert H.port.ev[-1] == ("sel", True) and H.h.state == "engaging_sel"
    H.step(sv2=1052.3, pv_sel_eff=True)
    assert H.h.is_holding() and H.h.kind == "tc2"
    for _ in range(301):
        H.step(pv=470.0, sv2=1052.3, pv_sel_eff=True)               # 120 °C 넘는 편차 — 5분 뒤 안내 1회, 해제 아님
    assert H.h.is_holding() and sum("TC1 편차" in m for m in H.msgs) == 1


def test_hold_T26b_tc2_lost_demotes_to_dac():
    H = HH("tc2")
    H.arm()
    H.step(sv2=1052.3)
    H.step(sv2=1052.3, pv_sel_eff=True)
    H.step(pv2=None, sv2=1052.3, pv_sel_eff=True, mv=1059)
    assert H.h.is_holding() and H.h.kind == "dac" and ("mv", 1059) in H.port.ev
    assert H.port.ev.index(("sel", False)) < H.port.ev.index(("sv2", 0.0)) < H.port.ev.index(("mv", 1059))
    assert any(e.kind == "alert" and e.data["kind"] == "demoted" for e in H.events)


def test_hold_T28_dac_when_ladder_unsupported_and_T29_off():
    H = HH("tc2")
    H.arm(sv2_max=0.0)
    assert H.h.is_holding() and H.h.kind == "dac" and H.port.ev == [("mv", 800)]
    H.step(sv2_max=0.0, run=False)
    assert H.h.state == "idle" and H.port.ev[-1] == ("mv", 1200)    # 해제 → 상한 원복
    H = HH("off")
    H.arm()
    assert H.h.state == "idle" and H.port.ev == []


def test_hold_T65b_saturated_dac_gives_up():
    H = HH("dac")
    for _ in range(61):
        H.step(mv=1190)
    assert H.h.gave_up and sum(e.kind == "give_up" for e in H.events) == 1 and H.port.ev == []   # 고정하지 않음


def test_hold_T67_T80_force_paths():
    H = HH("tc2")
    for _ in range(30):                                            # 링버퍼만 쌓이고 창은 드리프트로 못 끝남
        H.step(mv=800 + (_ % 2) * 200)
    assert H.h.force_tc2_hold() and H.h.state == "engaging_sv2" and H.h.engaged_relaxed
    H = HH("dac")
    assert not H.h.force_dac_hold(None) and "없음" in H.h.last_force_reason
    assert H.h.force_dac_hold(900) and H.port.ev == [("mv", 900)] and H.h.kind == "dac"


# ───────── 포화 감시 ─────────
def test_saturation_clamps_to_pre_window_mean_and_releases():
    t = [0.0]
    port, ev = Port(), []
    g = SaturationGuard(dict(CFG, HEATER_MV_LIMIT=1200, HEATER_MV_SAT_SEC=120), port, lambda l, m: None, ev.append, lambda: t[0])
    st = dict(run=True, fault=False, sv=600.0, pv=590.0, pv2=None, mv_limit=1200)
    for k in range(60):
        t[0] = k
        g.tick(dict(st, mv=900), False)
    for k in range(60, 182):
        t[0] = k
        g.tick(dict(st, mv=1200), False)
    assert g.clamped == 900 and port.ev == [("mv", 900)]
    assert ev and ev[0].code == "HEATER_SATURATED" and "TC2 없음" in ev[0].detail
    t[0] = 183
    g.tick(dict(st, sv=610.0, mv=1200), False)                     # 목표 변경 → 원복
    assert g.clamped is None and port.ev[-1] == ("mv", 1200)


# ───────── 레시피 단계 ─────────
def _runner(status):
    t = [0.0]
    port, ev = Port(), []
    ramp = RampProfiler(CFG, port, lambda: status, lambda l, m: None, lambda: t[0])
    r = RecipeRunner(CFG, port, ramp, lambda: status, lambda l, m: None, ev.append, lambda: t[0])
    return t, port, ev, r


def test_recipe_phases_ramp_soak_cool_and_finish():
    st = dict(run=True, pv=25.0, sv_ramp=25.0, fault=False, itl=True)
    t, port, ev, r = _runner(st)
    steps = parse_recipe_steps([{"target_c": 100, "ramp_c_per_min": 30, "soak_min": 1}, {"target_c": 25, "soak_min": 2}], CFG)
    assert steps[-1].is_cooldown and steps[0].describe() == "100°C · RAMP 30°C/min · SOAK 1분"
    r.load("R", steps)
    assert r.start() and r.phase == "RAMP" and ("run", True) in port.ev
    st.update(pv=99.0, sv_ramp=100.0)
    r.on_status(st)
    assert r.phase == "SOAK"
    t[0] += 61
    r.tick(st)
    assert r.phase == "COOL" and port.ev[-2:] == [("run", True), ("sv", 25.0)]
    t[0] += 121
    r.tick(st)
    assert not r.is_running() and r.finished == (True, "", "레시피 완료") and ("run", False) in port.ev


def test_recipe_hold_freezes_clock_and_skip():
    st = dict(run=True, pv=50.0, sv_ramp=50.0, fault=False, itl=True)
    t, port, ev, r = _runner(st)
    r.load("R", parse_recipe_steps([{"target_c": 50, "soak_min": 1}, {"target_c": 60, "soak_min": 1}], CFG))
    r.start()
    r.on_status(st)
    assert r.phase == "SOAK"
    r.hold()
    t[0] += 500                                                    # 정지 중에는 레시피 시계가 안 흐른다
    r.tick(st)
    r.resume()
    t[0] += 30
    r.tick(st)
    assert r.idx == 0 and r.phase == "SOAK"
    r.skip()
    assert r.idx == 1 and r.phase == "RAMP"


def test_recipe_repeat_rejected_and_step_timeout():
    with pytest.raises(ValueError):
        parse_recipe_steps([{"target_c": 100, "repeat": 2}], CFG)    # HEATER_RECIPE_MAX_REPEAT=1(v1 과 같음)
    st = dict(run=True, pv=25.0, sv_ramp=25.0, fault=False, itl=True)
    t, port, ev, r = _runner(st)
    r.load("R", parse_recipe_steps([{"target_c": 100, "ramp_c_per_min": 30, "soak_min": 0}], CFG))
    r.start()
    t[0] += r.step_timeout + 1
    r.tick(st)
    assert r.finished[1] == "HEATER_WAIT_TIMEOUT"                    # S18D


# ───────── 시험대 ─────────
def rig(tmp_path, **cfg):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = ChkRig(c, tmp_path, cfg=cfg)
    r.plc_sim = r.sims["plc"]
    return c, r


@pytest.mark.slow
def test_rig_manual_ramp_approach_and_hold(tmp_path):
    c, r = rig(tmp_path, HEATER_HOLD_MODE="dac")
    rows = []
    r.loop.sinks.append(rows.append)
    r.loop.tick_hooks.append(lambda t: t.seq == 2 and r.heater.manual_on(60.0))
    assert r.start()
    try:
        r.loop.run(600)
    finally:
        r.stop()
    pv = [x["heater.pv"] for x in rows]
    reached = next(i for i, x in enumerate(pv) if x is not None and x >= 59.5)
    assert max(pv) < 61.5                                           # 감속 접근 — 큰 오버슈트 없음
    svs = [x["heater.sv"] for x in rows[3:reached]]
    assert svs == sorted(svs) and svs[0] < 30                       # SV 를 파이썬이 조금씩 밀어 올림
    assert r.heater.hold.is_holding() and r.heater.hold.kind == "dac"
    assert rows[-1]["heater.hold"] == "holding_dac"


@pytest.mark.slow
def test_rig_process_target_and_hold_wait(tmp_path):
    c, r = rig(tmp_path)                                            # HEATER_HOLD_MODE 기본 tc2
    seen = []

    def hook(t):
        if t.seq == 2:
            r.heater.process_ramp(30.0)
            r.heater.process_set(60.0)
        elif t.seq > 2 and not seen or (seen and seen[-1] not in ("reached",) and not isinstance(seen[-1], tuple)):
            res = r.heater.process_wait_tick(c.monotonic())
            seen.append(res if res != "wait" else r.heater.process_phase)
    r.loop.tick_hooks.append(hook)
    assert r.start()
    try:
        r.loop.run(900)
    finally:
        r.stop()
    phases = [p for i, p in enumerate(seen) if i == 0 or p != seen[i - 1]]
    assert phases[:3] == ["RAMP", "SOAK", "HOLD_WAIT"] and phases[-1] == "reached", phases
    assert r.heater.hold.is_holding() and r.heater.hold.kind == "tc2"
    assert r.plc_sim.coils[A["heater_pv_sel_eff"]] is True


@pytest.mark.slow
def test_rig_heater_gas_ready_hold_and_release(tmp_path):
    c, r = rig(tmp_path, HEATER_GAS_HOLD_RELEASE_C=50.0)
    plc = r.sims["plc"]
    log = []

    def hook(t):
        if t.seq == 2:
            assert r.heater.manual_on(60.0, {"use_ar": True, "ar_flow": 20.0, "wp_mtorr": 5.0}) is None
        if t.seq == 400:
            r.heater.manual_off()
        log.append((t.seq, r.heater.gas_state, r.heater.atm_hold))
    r.loop.tick_hooks.append(hook)
    assert r.start()
    try:
        r.loop.run(900)
    finally:
        r.stop()
    states = [s for _, s, _ in log]
    ready = states.index("READY")
    assert "PREPARING" in states[:ready] and plc.coils[A["ar_valve"]] is False      # 끝에서는 해제됨
    assert any(h for q, s, h in log if q > 400)                                    # OFF 뒤 냉각 유지
    assert log[-1][1] == "IDLE" and not log[-1][2]
    on = [q for q, s, h in log if s == "READY"]
    assert on and on[0] < 120                                                        # 가스 준비 뒤 히터 ON
