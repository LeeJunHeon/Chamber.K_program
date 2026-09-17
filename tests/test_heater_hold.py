# -*- coding: utf-8 -*-
"""T25~T30 — controller/heater_hold.HeaterHold 단독(MainDialog 없이, clock 주입).
T30 은 옛 main._heater_mv_hold_tick(커밋 c15af43)을 git 에서 꺼내 같은 시나리오로 돌려 발행이 같음을 증명한다."""
import os
import re
import subprocess
import textwrap
import types

import pytest

from conftest import make_heater_st
from controller.heater_hold import HeaterHold

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OLD_COMMIT = "c15af43"


class Harness:
    """HeaterHold + 시계 + 발행 기록."""
    def __init__(self, mode="tc2", **kw):
        self.t = 1000.0
        args = dict(mv_limit=1200, mv_min=400, enter_tol_c=3.0, enter_sec=60, arrive_tol_c=1.0,
                    drift_pv_c=0.5, drift_mv=15, tc2_margin_c=50.0, tc2_drift_c=1.0)
        args.update(kw)
        self.h = HeaterHold(mode, clock=lambda: self.t, **args)
        self.ev = []          # ("mv", v) / ("sv2", v) / ("sel", v)
        self.msgs = []
        self.h.request_mv_limit.connect(lambda v: self.ev.append(("mv", v)))
        self.h.request_sv2.connect(lambda v: self.ev.append(("sv2", v)))
        self.h.request_pv_sel.connect(lambda v: self.ev.append(("sel", v)))
        self.h.message.connect(lambda l, m: self.msgs.append((l, m)))

    def step(self, dt=1.0, final=600.0, **st_kw):
        self.t += dt
        st = make_heater_st(**st_kw)
        self.h.tick(st, final)
        return st


def _stable(**kw):
    """도달·안정 상태의 st 인자: PV=SV=램프=600, MV 800, TC2 1052.3"""
    d = dict(run=True, pv=600.0, sv=600.0, sv_ramp=600.0, cur_sv=600.0, mv=800, mv_limit=1200,
             pv2=1052.3, sv2=0.0, sv2_max=1100.0, ot2_limit=1150.0, pv_sel_eff=False)
    d.update(kw); return d


def _arm(H, n=70, **kw):
    """도달 뒤 안정 상태를 최대 n초 — 창이 끝나 캡처(상태 전이)되면 멈춘다."""
    for _ in range(n):
        H.step(**_stable(**kw))
        if H.h.state not in ("idle", "arming"):
            return


def test_T25_tc2_engage_order():
    H = Harness("tc2")
    _arm(H)
    assert H.ev == [("sv2", 1052.3)] and H.h.state == "engaging_sv2"
    H.step(**_stable(sv2=0.0)); H.step(**_stable(sv2=0.0))
    assert H.ev == [("sv2", 1052.3)]                          # 되읽기 전에는 pv_sel 없음
    H.step(**_stable(sv2=1052.3))
    assert H.ev == [("sv2", 1052.3), ("sel", True)] and H.h.state == "engaging_sel"
    H.step(**_stable(sv2=1052.3, pv_sel_eff=False))
    assert H.h.state == "engaging_sel"
    H.step(**_stable(sv2=1052.3, pv_sel_eff=True))
    assert H.h.state == "holding" and H.h.kind == "tc2" and H.h.is_holding()
    assert any("TC2 추종 시작 — TC1 600.0 / TC2 1052.3 (SV2 1052.3 고정)" in m for _, m in H.msgs)
    assert H.h.log_tuple() == (1, None)
    # 유지 중 TC1 이 튀어도 해제하지 않는다 — 5분 뒤 안내 1회, 복귀 안내 1회
    for _ in range(301):
        H.step(**_stable(pv=576.6, sv2=1052.3, pv_sel_eff=True))
    assert H.h.state == "holding"
    assert sum("TC2 추종 중 TC1 편차 -23.4°C (5분 경과) — 추종 유지" in m for _, m in H.msgs) == 1
    for _ in range(3):
        H.step(**_stable(pv=600.0, sv2=1052.3, pv_sel_eff=True))
    assert sum("TC1 편차 정상 복귀" in m for _, m in H.msgs) == 1
    # 재적용: D00035 가 되돌아간 상태 5초 이상
    for _ in range(7):
        H.step(**_stable(sv2=0.0, pv_sel_eff=True))
    assert H.ev[-1] == ("sv2", 1052.3) and H.ev.count(("sv2", 1052.3)) == 2
    assert H.h.state == "holding"


def _engage(H):
    _arm(H)
    H.step(**_stable(sv2=1052.3))
    H.step(**_stable(sv2=1052.3, pv_sel_eff=True))
    assert H.h.state == "holding"
    H.ev.clear()


@pytest.mark.parametrize("kw,final,why", [
    (dict(), 650.0, "목표 변경"),
    (dict(pv2=None), 600.0, "TC2 값 없음"),
    (dict(fault=True), 600.0, "히터 이상"),
    (dict(run=False), 600.0, "운전 OFF"),
    (dict(pv_sel_eff=False), 600.0, "M0004B OFF"),
])
def test_T26_tc2_release_order(kw, final, why):
    H = Harness("tc2")
    _engage(H)
    H.step(final=final, **_stable(**{"sv2": 1052.3, "pv_sel_eff": True, **kw}))
    assert H.ev == [("sel", False), ("sv2", 0.0)]              # pv_sel(False) 가 먼저
    assert H.h.state == "idle"
    assert any("TC2 추종 해제 → TC1 제어 복귀" in m and why in m for _, m in H.msgs)


def test_T27_tc2_clamp_to_ot2_minus_margin():
    H = Harness("tc2")
    _arm(H, pv2=1120.0)
    assert H.ev == [("sv2", 1100.0)]
    assert any("TC2 목표를 OT2−마진으로 제한 1120.0 → 1100.0°C" in m for _, m in H.msgs)


def test_T28_fallback_to_dac_when_ladder_unsupported_or_no_tc2():
    H = Harness("tc2")
    _arm(H, sv2_max=0.0)
    assert H.ev == [("mv", 800)] and H.h.kind == "dac" and H.h.state == "holding"
    assert sum("래더가 TC2 추종을 지원하지 않음(D00036=0) — DAC 상한 고정으로 대체" in m for _, m in H.msgs) == 1
    assert H.h.log_tuple() == (1, 800)
    H.step(**_stable(run=False))
    assert H.ev[-1] == ("mv", 1200)
    # TC2 없음
    H2 = Harness("tc2")
    _arm(H2, pv2=None)
    assert H2.ev == [("mv", 800)]
    assert sum("TC2 값 없음(D00011=-1) — DAC 상한 고정으로 대체" in m for _, m in H2.msgs) == 1


def test_T29_mode_off_and_engage_timeout_rollback():
    H = Harness("off")
    _arm(H)
    assert H.ev == [] and H.h.state == "idle"
    # 타임아웃: D00035 되읽기가 5초 안에 안 맞는다
    H = Harness("tc2")
    _arm(H)
    for _ in range(6):
        H.step(**_stable(sv2=0.0))
    assert H.ev == [("sv2", 1052.3), ("sel", False), ("sv2", 0.0)]
    assert H.h.state == "idle" and H.h._h["arrived"] is True
    assert sum("TC2 추종 진입 실패" in m for l, m in H.msgs if l == "히터(경고)") == 1
    # M0004B 단계 타임아웃
    H = Harness("tc2")
    _arm(H)
    H.step(**_stable(sv2=1052.3))
    for _ in range(6):
        H.step(**_stable(sv2=1052.3, pv_sel_eff=False))
    assert H.ev == [("sv2", 1052.3), ("sel", True), ("sel", False), ("sv2", 0.0)]


# ───────────────────────── T30 dac 회귀: 옛 코드와 발행 비교 ─────────────────────────
def _old_hold_class():
    src = subprocess.run(["git", "show", f"{OLD_COMMIT}:main.py"], cwd=ROOT,
                         capture_output=True).stdout.decode("utf-8")
    a = src.index("    def _mvhold_release(")
    b = src.index("    # ==================== 히터 시작/종료 구글챗 카드")
    body = textwrap.dedent(src[a:b])
    ns = {}
    clock = {"t": 0.0}
    fake_time = types.SimpleNamespace(monotonic=lambda: clock["t"])
    ns.update(time=fake_time, HEATER_HOLD_MV_AFTER_REACH=True, HEATER_MV_LIMIT=1200, HEATER_MV_MIN=400,
              HEATER_HOLD_MV_ENTER_TOL_C=3.0, HEATER_HOLD_MV_ENTER_SEC=60, HEATER_HOLD_MV_ARRIVE_TOL_C=1.0,
              HEATER_HOLD_MV_DRIFT_PV_C=0.5, HEATER_HOLD_MV_DRIFT_MV=15)
    from lib.config import heater_est_current
    ns["heater_est_current"] = heater_est_current
    exec(body, ns)

    class Old:
        def __init__(self):
            self.msgs = []; self.ev = []
            ns["log_message_to_monitor"] = lambda l, m: self.msgs.append((l, m))
            self._mvhold = {'state': 'idle', 'samples': [], 't0': 0.0, 'sv': None, 'value': None, 'last_push': 0.0,
                            'arrived': False, 'arr_sv': None, 'floor_warned': False, 'limit_warned': False,
                            'drift_log_t': 0.0}
            self.request_heater_mv_limit = types.SimpleNamespace(emit=lambda v: self.ev.append(("mv", v)))
            self._final = None
        def _heater_final_target(self, st): return self._final
        _mvhold_release = ns["_mvhold_release"]
        _heater_mv_hold_tick = ns["_heater_mv_hold_tick"]
        def tick(self, st, final):
            self._final = final; self._heater_mv_hold_tick(st)
        @property
        def state(self): return self._mvhold['state']
    return Old, clock


SCENARIOS = {
    "정상 캡처→재적용→목표 변경 해제": (
        [(1.0, _stable(), 600.0)] * 70
        + [(1.0, _stable(mv_limit=800), 600.0)] * 10
        + [(1.0, _stable(mv_limit=1200), 600.0)] * 7
        + [(1.0, _stable(sv=650.0, sv_ramp=650.0), 650.0)] * 2),
    "드리프트 재측정 뒤 캡처": (
        [(1.0, _stable(mv=700 + i * 3), 600.0) for i in range(70)]
        + [(1.0, _stable(mv=900), 600.0)] * 70),
    "상한과 같아 거부": [(1.0, _stable(mv=1200), 600.0)] * 130,
    "바닥이라 거부": [(1.0, _stable(mv=410), 600.0)] * 130,
    "미도달(PV 605)→운전 OFF": [(1.0, _stable(pv=605.0), 600.0)] * 70 + [(1.0, _stable(run=False), 600.0)],
    "램프 중(중간 SV)→도달→캡처": (
        [(1.0, _stable(sv=590.0, sv_ramp=590.0, pv=590.0), 600.0)] * 30
        + [(1.0, _stable(), 600.0)] * 70),
    "유지 중 이상": [(1.0, _stable(), 600.0)] * 70 + [(1.0, _stable(fault=True), 600.0)] * 3,
    "PV 이탈로 창 리셋 뒤 재캡처": (
        [(1.0, _stable(), 600.0)] * 40 + [(1.0, _stable(pv=605.0), 600.0)] * 5
        + [(1.0, _stable(), 600.0)] * 70),
}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_T30_dac_regression_matches_old_code(name):
    Old, clock = _old_hold_class()
    old = Old()
    new = Harness("dac")
    old_states, new_states = [], []
    clock["t"] = new.t
    for dt, kw, final in SCENARIOS[name]:
        clock["t"] += dt
        old.tick(make_heater_st(**kw), final)
        old_states.append((old.state, old._mvhold['value'], old._mvhold['arrived']))
        new.step(dt, final, **kw)
        new_states.append((new.h.state, new.h.value, new.h._h['arrived']))
    assert new.ev == old.ev, (name, new.ev, old.ev)
    assert new_states == old_states, name
    # dac 전용 문구도 그대로(도달/대기취소 공용 문구만 다르다)
    def dac_msgs(ms):
        return [m for _, m in ms if "DAC 상한" in m and "측정 시작" not in m and "대기 취소" not in m]
    assert dac_msgs(new.msgs) == dac_msgs(old.msgs), name
    assert all("지그" not in m for _, m in new.msgs)
