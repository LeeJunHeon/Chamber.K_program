# -*- coding: utf-8 -*-
"""T68~T72 — controller/heater_saturation.HeaterSaturationGuard (clock 주입, 시그널 기록)."""
import pytest

from conftest import make_heater_st
from controller.heater_saturation import HeaterSaturationGuard, MV_SAT_FALLBACK_REL


class G:
    def __init__(self, sat_sec=120):
        self.t = 1000.0
        self.g = HeaterSaturationGuard(mv_limit=1200, mv_min=400, sat_sec=sat_sec, clock=lambda: self.t)
        self.ev = []; self.msgs = []; self.sat = []
        self.g.request_mv_limit.connect(lambda v: self.ev.append(v))
        self.g.message.connect(lambda l, m: self.msgs.append((l, m)))
        self.g.saturated.connect(lambda d: self.sat.append(d))

    def step(self, dt=1.0, hold=False, kind=None, **kw):
        self.t += dt
        st = make_heater_st(**{"run": True, "pv": 600.0, "sv": 600.0, "mv": 1000, **kw})
        self.g.tick(st, hold, kind)


def test_T68_saturation_warn_and_clamp_once():
    g = G()
    for _ in range(60):
        g.step(mv=1040)                     # 포화 직전 60초 이력
    for _ in range(120):
        g.step(mv=1200)                      # 첫 tick 이 t0 → 119초 경과
    assert g.ev == [] and g.sat == []
    g.step(mv=1200)                          # 120초 연속
    assert g.ev == [1040] and len(g.sat) == 1 and g.sat[0]["clamp"] == 1040
    w = [m for l, m in g.msgs if l == "히터(경고)"]
    assert len(w) == 1 and "TC1 600.0" in w[0] and "MV 1200" in w[0] and "120초째" in w[0]
    for _ in range(300):
        g.step(mv=1040)                      # 같은 에피소드 — 반복 발사 없음
    assert g.ev == [1040] and len(g.sat) == 1 and len([1 for l, _ in g.msgs if l == "히터(경고)"]) == 1


def test_T69_no_history_uses_fallback_and_clamps():
    g = G(sat_sec=10)
    for _ in range(11):
        g.step(mv=1200)
    assert g.ev == [int(MV_SAT_FALLBACK_REL * 1200)] and "이력 없음" in g.sat[0]["src"]


def test_T70_hold_active_does_nothing():
    g = G(sat_sec=10)
    for _ in range(60):
        g.step(mv=1200, hold=True, kind="tc2")
    assert g.ev == [] and g.sat == [] and g.msgs == []
    # hold 가 풀린 뒤에는 새로 센다
    for _ in range(11):
        g.step(mv=1200)
    assert len(g.sat) == 1


def test_T71_release_on_run_off_target_change_and_hold_entry():
    g = G(sat_sec=10)
    for _ in range(11):
        g.step(mv=1200)
    assert g.ev[-1] == 1080
    g.step(run=False)
    assert g.ev[-1] == 1200 and any("원복 (운전 OFF)" in m for _, m in g.msgs)
    g2 = G(sat_sec=10)
    for _ in range(11):
        g2.step(mv=1200)
    g2.step(mv=1200, sv=650.0)
    assert g2.ev[-1] == 1200 and any("목표 변경" in m for _, m in g2.msgs)
    g3 = G(sat_sec=10)
    for _ in range(11):
        g3.step(mv=1200)
    g3.step(mv=1200, hold=True, kind="tc2")
    assert g3.ev[-1] == 1200 and any("유지 모드 진입" in m for _, m in g3.msgs)
    g4 = G(sat_sec=10)
    for _ in range(11):
        g4.step(mv=1200)
    g4.step(mv=1200, hold=True, kind="dac")
    assert g4.ev[-1] != 1200 and any("이어받음" in m for _, m in g4.msgs)     # dac 유지가 D00018 소유 — 덮어쓰지 않는다


def test_T72_0922_scenario_caught_within_120s():
    """12:50:56 부터 MV 1200 연속(직전 60초 평균 1077) → 120초 안에 경고 + 클램프."""
    g = G()
    for _ in range(60):
        g.step(mv=1077, pv=600.0, pv2=893.4)
    t_sat = g.t
    while not g.sat:
        g.step(mv=1200, pv=560.0, pv2=920.0)
        assert g.t - t_sat <= 121
    assert g.t - t_sat == pytest.approx(121.0) and g.ev == [1077]      # 첫 포화 tick + 120초
    assert "TC2 920.0" in g.msgs[-1][1]


def test_T72b_demoted_dac_hold_is_not_overwritten_by_guard():
    """tc2 → dac 강등으로 dac holding 이 되면 포화 감시는 소유권을 넘기고 D00018 을 덮어쓰지 않는다."""
    from controller.heater_hold import HeaterHold
    g = G(sat_sec=10)
    hold = HeaterHold("tc2", mv_limit=1200, mv_min=400, enter_tol_c=3.0, enter_sec=60, arrive_tol_c=1.0,
                      drift_pv_c=0.5, drift_mv=15, tc2_margin_c=50.0, tc2_drift_c=1.0, clock=lambda: g.t)
    hev = []; hold.request_mv_limit.connect(lambda v: hev.append(v))
    hold.request_sv2.connect(lambda v: None); hold.request_pv_sel.connect(lambda v: None)
    base = dict(run=True, pv=600.0, sv=600.0, sv_ramp=600.0, cur_sv=600.0, mv=1200, pv2=893.4,
                sv2=0.0, sv2_max=1100.0, ot2_limit=1150.0, pv_sel_eff=False)
    def tick(**kw):
        g.t += 1.0
        st = make_heater_st(**{**base, **kw})
        g.g.tick(st, hold.is_holding(), hold.kind)
        hold.tick(st, 600.0)
    for _ in range(70):
        tick(mv=1000)                                    # 도달·창 → tc2 캡처
        if hold.state == "engaging_sv2":
            break
    assert hold.state == "engaging_sv2"
    tick(mv=1000, sv2=hold.sv2); tick(mv=1000, sv2=hold.sv2, pv_sel_eff=True)
    assert hold.is_holding() and hold.kind == "tc2"
    for _ in range(30):
        tick(mv=1200, sv2=hold.sv2, pv_sel_eff=True)     # tc2 유지 중 MV 상한 — hold_active 라 감시는 판정하지 않는다
    assert g.ev == []
    tick(mv=1000, sv2=hold.sv2, pv_sel_eff=True, pv2=None)   # TC2 상실 → dac 강등(강등 직전 MV 1000)
    assert hold.is_holding() and hold.kind == "dac" and hev == [1000]
    for _ in range(60):
        tick(mv=1000, mv_limit=1000, pv2=None, pv_sel_eff=False)      # PLC 가 D00018=1000 을 비춘다
    assert g.ev == [] and hold.value == 1000 and hev == [1000]   # 감시는 dac 유지의 D00018 을 건드리지 않는다


def _guard(sat_sec=10):
    return G(sat_sec=sat_sec)


def test_T86_dac_hold_saturation_warns_without_clamp():
    """hold_kind='dac', 실제 상한(D00018) 1031, MV 1031 연속 → 경고 1회 + saturated(clamp=None, owner='hold_dac'), 쓰기 없음."""
    g = _guard()
    for _ in range(12):
        g.step(hold=True, kind="dac", mv=1031, mv_limit=1031, pv2=None)
    assert g.ev == [] and len(g.sat) == 1
    d = g.sat[0]
    assert d["clamp"] is None and d["owner"] == "hold_dac" and d["limit"] == 1031
    w = [m for l, m in g.msgs if l == "히터(경고)"]
    assert len(w) == 1 and "상한(1031)" in w[0] and "클램프 없음(유지 모드가 D00018 1031 을 소유)" in w[0]
    assert "TC2 없음 · OT2 과온 보호 없음 — 즉시 확인 필요" in w[0]
    for _ in range(100):
        g.step(hold=True, kind="dac", mv=1031, mv_limit=1031, pv2=None)      # 같은 에피소드 — 반복 없음
    assert g.ev == [] and len(g.sat) == 1
    # 설정 상한(1200) 기준이었다면 1031 은 86% 라 판정 자체가 없었다
    g2 = _guard()
    for _ in range(12):
        g2.step(hold=True, kind="dac", mv=1031, mv_limit=None)
    assert g2.sat == []


def test_T86b_tc2_hold_emits_nothing():
    g = _guard()
    for _ in range(60):
        g.step(hold=True, kind="tc2", mv=1200, mv_limit=1200)
    assert g.ev == [] and g.sat == [] and g.msgs == []


def test_T86c_no_hold_uses_actual_limit_and_clamps():
    g = _guard()
    for _ in range(30):
        g.step(mv=900, mv_limit=1031)                    # 이력
    for _ in range(12):
        g.step(mv=1031, mv_limit=1031)                   # 실제 상한 1031 기준으로 포화
    assert g.ev == [900] and g.sat[0]["clamp"] == 900 and g.sat[0]["owner"] == "guard" and g.sat[0]["limit"] == 1031
    assert "상한(1031)" in g.msgs[-1][1]
