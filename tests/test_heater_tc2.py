# -*- coding: utf-8 -*-
"""HEATER_CTRL_MODE=tc2(TC2 단독 제어) — 2026-11-02 사용자 결정.

test_heater_cascade 의 열 모델(Plant·PID·Sim)을 그대로 쓰되, 플랜트의 참 매핑과 제어 매핑을 새 기본 매핑(NEW_MAP)으로 둔다.
  · tc2 모드: SV2 = map(r1), 바깥 루프 없음 — TC2 가 SV2 에 붙고, 매핑이 어긋난 만큼 TC1 이 목표에서 벗어나야 정상.
  · 같은 시나리오를 cascade 로도 돌려 나란히 비교한다(도달 시간 차이 포함).
  · 셔터·TC2 상실·TC1 탈락(편차 안내만)·레시피 HOLD/RESUME·목표 하강·TC2 홀드백·출발점(TC2 역함수)·학습 파일 무접촉.
공정 컨트롤러(HEATER_WAIT tc2 판정·HEATER_SETTLE 없음·셔터 직전 기록), main 연결(TC1 편차 안내 카드·LCD 편차·CSV),
Tc2Map.inverse, 설정 검증도 여기서 본다. 결과표는 `pytest -s tests/test_heater_tc2.py` 로 볼 수 있다.
"""
import importlib
import json
import time
from unittest.mock import MagicMock

import pytest
from PyQt6.QtCore import QTimer

import controller.process_controller as PC
from controller.heater_cascade import Tc2Map
from controller.process_controller import ActionType
from conftest import make_heater_st
from test_heater_cascade import Plant, Sim, _interp, SV2_MAX

NEW_MAP = [[25, 25], [50, 92], [100, 170], [150, 226], [200, 290], [250, 365], [300, 437], [400, 569], [500, 705],
           [600, 846], [700, 982]]
TOL = 2.0                     # HEATER_SOAK_TOLERANCE
RESULTS = []                  # 비교표


# ───────────────────────── 시뮬레이터 ─────────────────────────
class Plant2(Plant):
    """참 매핑 = NEW_MAP + offset·clip((TC1−25)/275, 0, 1)."""

    def f_true(self, t1):
        return _interp(NEW_MAP, t1) + self.offset * min(1.0, max(0.0, (t1 - 25.0) / 275.0))


def _sim(tmp_path, mode, offset=0.0, **kw):
    s = Sim(tmp_path, map_points=NEW_MAP, mode=mode, **kw)
    s.pl = Plant2(offset=offset)
    return s


def _arrive(rec, ok, need=60):
    since = None
    for r in rec:
        if ok(r):
            since = since if since is not None else r[0]
            if r[0] - since >= need:
                return since
        else:
            since = None
    return None


def _metrics(s, target, tail_sec=1200):
    rec = s.rec
    sv2f = min(s.c.map.value(target), SV2_MAX)
    tail = [r for r in rec if r[0] >= rec[-1][0] - tail_sec]
    t2 = [r[2] for r in tail]; t1 = [r[1] for r in tail]
    done = lambda r: r[4] is not None and r[4] >= target - 0.05
    if s.c.tc2_only:      # 도달 = 그 모드의 HEATER_WAIT 판정(tc2: r1 도달 + |TC2 − SV2| ≤ TOL 60 s)
        a = _arrive(rec, lambda r: done(r) and abs(r[2] - sv2f) <= TOL)
    else:                 # cascade: r1 도달 + |TC1 − 목표| ≤ TOL 60 s
        a = _arrive(rec, lambda r: done(r) and abs(r[1] - target) <= TOL)
    return {"err2": abs(sum(t2) / len(t2) - sv2f), "over2": max(r[2] for r in rec) - sv2f, "pp2": max(t2) - min(t2),
            "arrive_min": None if a is None else a / 60.0, "dev1": sum(t1) / len(t1) - target,
            "over1": max(r[1] for r in rec) - target, "sv2_final": sv2f}


def _ramp(tmp_path, mode, target, offset, rate=6.0, tail_min=90, **kw):
    s = _sim(tmp_path, mode, offset, **kw)
    s.c.start(target, rate, "sim"); s.run = True
    s.runfor((target - 25.0) / rate * 60 + tail_min * 60)
    return s, _metrics(s, target)


def _wait_timeout_min(target, rate=6.0):
    """공정 HEATER_WAIT 타임아웃(tc2 — TI×2 여유 없음): max(HEATER_WAIT_TIMEOUT_SEC, 예상 승온 + 30분), 상한 WAIT_TIMEOUT_MAX_SEC."""
    from lib.heater_profile import ramp_minutes
    need = ramp_minutes(target - 25.0, rate) * 60.0
    return min(max(float(PC.HEATER_WAIT_TIMEOUT_SEC), need + PC.WAIT_TIMEOUT_MARGIN_SEC), PC.WAIT_TIMEOUT_MAX_SEC) / 60.0


def _expected_tc1(target, offset):
    """TC2 가 map(목표) 에 붙을 때의 참 TC1 — f_true(TC1) = map(목표)."""
    pl = Plant2(offset=offset)
    return pl.finv(_interp(NEW_MAP, target))


@pytest.mark.parametrize("offset", [0.0, 25.0, -25.0])
@pytest.mark.parametrize("target", [100, 300, 500, 600, 700])
def test_sim_tc2_vs_cascade(tmp_path, target, offset):
    s2, m2 = _ramp(tmp_path, "tc2", target, offset, tail_min=150)
    sc, mc = _ramp(tmp_path, "cascade", target, offset, tail_min=150, learned=tmp_path / "casc_learned.json")
    RESULTS.append((target, offset, m2, mc))
    assert s2.c.state == 'running' and s2.c.bias == 0.0
    assert not (tmp_path / "learned.json").exists()                     # tc2 는 학습 파일을 쓰지 않는다
    assert m2["err2"] <= 0.5 and m2["pp2"] <= 1.0 and m2["over2"] <= 3.0, m2
    assert m2["arrive_min"] is not None and m2["arrive_min"] <= _wait_timeout_min(target), m2   # 공정 HEATER_WAIT 안
    # TC1 은 매핑이 어긋난 만큼 벗어나야 정상(제어 대상이 아니다)
    assert m2["dev1"] == pytest.approx(_expected_tc1(target, offset) - target, abs=1.0), m2
    assert not [a for a in s2.alerts if a[1] in ("tc1_detached",)]


# ───────────────────────── 시나리오 ─────────────────────────
def _held(tmp_path, target=600, offset=0.0, **kw):
    s, _ = _ramp(tmp_path, "tc2", target, offset, tail_min=120, **kw)
    return s


def test_tc2_shutter_open_holds_sv2_and_close_resumes(tmp_path):
    s = _held(tmp_path)
    sv2 = s.D[35]
    s.shutter = True; s.tc1_off = -100.0                  # 셔터 열림: TC1 이 약 100°C 낮게 읽힌다
    t_open = s.t
    s.runfor(20 * 60)
    seg = [r for r in s.rec if r[0] > t_open]
    assert s.c.state == 'frozen' and all(r[6] == sv2 for r in seg)
    assert max(r[2] for r in seg) - min(r[2] for r in seg) < 1.0
    assert not [a for a in s.alerts if a[1] == "tc1_deviation"]          # 셔터 열림 중에는 안내하지 않는다
    assert any("메인 셔터 열림 — SV2 846.0 유지" in m for _, _, m in s.msgs)
    s.c.start(650, 6.0); s.runfor(120)                                   # 상승 목표는 셔터가 닫힐 때까지 보류
    assert s.c.r1 == pytest.approx(600.0) and s.D[35] == sv2
    s.shutter = False; s.tc1_off = 0.0
    s.runfor(60)
    assert s.c.state == 'running' and s.c.r1 > 600.0
    assert any("메인 셔터 닫힘 — TC2 제어 재개" in m for _, _, m in s.msgs)


def test_tc2_lost_shutter_closed_falls_back_to_tc1(tmp_path):
    s = _held(tmp_path, 300)
    order = len(s.writes)
    s.tc2_bad = True
    s.runfor(5)
    assert s.c.state == 'tc1' and [a[1] for a in s.alerts] == ["tc2_lost"]
    w = [(k, v) for _, k, v in s.writes[order:]]
    assert w[:2] == [('M4A', False), (35, 0.0)]                           # 해제 순서 M0004A OFF → D00035=0
    s.runfor(20 * 60)
    m = sum(r[1] for r in s.rec[-300:]) / 300
    assert abs(m - 300.0) <= 2.0                                         # 래더 TC1 경로가 같은 r1 을 따라간다


def test_tc2_lost_shutter_open_fixes_dac(tmp_path):
    s = _held(tmp_path, 600)
    s.shutter = True; s.tc1_off = -100.0
    s.runfor(10)
    mv = int(s.mv)
    s.tc2_bad = True
    s.runfor(3)
    assert s.c.state == 'dac' and abs(s.D[18] - mv) <= 25
    assert [a[1] for a in s.alerts] == ["demoted"]
    s.shutter = False; s.tc1_off = 0.0
    s.runfor(3)
    assert s.D[18] == 1200 and s.c.state == 'tc1'                        # 상한 원복, TC2 없음 → TC1 제어


def test_tc2_tc1_detached_only_notice_control_unaffected(tmp_path):
    """TC1 이 시료대에서 떨어져 낮게 고정 — 제어(SV2·TC2)는 그대로, 편차 안내 1회만, TC1 탈락 경보·중단 없음."""
    s = _held(tmp_path, 600)
    sv2 = s.D[35]
    s.tc1_stuck = 420.0
    t0 = s.t
    s.runfor(119)
    assert not [a for a in s.alerts if a[1] == "tc1_deviation"]          # 120 s 전에는 없다
    s.runfor(30 * 60)
    dev = [a for a in s.alerts if a[1] == "tc1_deviation"]
    assert len(dev) == 1 and 120 <= dev[0][0] - t0 <= 125
    assert "제어에는 영향 없음 — TC1 위치/접촉 확인" in dev[0][2]["text"]
    assert not [a for a in s.alerts if a[1] == "tc1_detached"]
    seg = [r for r in s.rec if r[0] > t0]
    assert all(r[6] == sv2 for r in seg) and max(abs(r[2] - sv2) for r in seg) < 1.0


def test_tc2_tc1_none_no_notice(tmp_path):
    s = _held(tmp_path, 600)
    real = s.st
    s.st = lambda: dict(real(), pv=None)
    s.runfor(10 * 60)
    assert not [a for a in s.alerts if a[1] == "tc1_deviation"] and s.c.state == 'running'


def test_tc2_recipe_hold_resume_and_lower_target(tmp_path):
    s = _sim(tmp_path, "tc2")
    s.c.start(400, 6.0, "sim"); s.run = True
    s.runfor(20 * 60)
    s.c.hold()
    r = s.c.r1
    s.runfor(10 * 60)
    assert s.c.r1 == pytest.approx(r) and s.c.is_held()
    s.c.resume(); s.runfor(60)
    assert s.c.r1 > r + 3.0
    s.runfor(90 * 60)
    assert s.c.r1_done(400)                                              # 레시피 RAMP 완료 판정(r1_done(목표))
    s.c.start(250, 6.0); s.runfor(3)
    assert s.c.r1 == pytest.approx(250.0) and s.D[35] == pytest.approx(365.0)   # 하강은 즉시
    s.runfor(60 * 60)
    m = sum(r[2] for r in s.rec[-600:]) / 600
    assert abs(m - 365.0) <= 1.0


def test_tc2_holdback_uses_tc2(tmp_path):
    """DAC 상한을 낮춰 TC2 가 SV2 를 못 따라가게 하면 r1 이 멈춘다(TC1 은 쓰지 않는다 — TC1 을 일부러 높게 읽혀도)."""
    s = _sim(tmp_path, "tc2")
    s.D[18] = 560                                  # 출력 상한 — TC2 가 몇백 도에서 막힌다
    s.tc1_off = +200.0                             # TC1 을 앞서게 읽혀도 홀드백은 TC2 로만 판단
    s.c.start(600, 6.0, "sim"); s.run = True
    s.runfor(150 * 60)
    assert s.c.r1 < 500.0
    gap = s.D[35] - s.pl.tc2
    assert gap <= s.c.map.slope(s.c.r1) * s.c.tc2_holdback + 2.0          # HEATER_TC2_HOLDBACK_C(20) 기준


def test_tc2_start_point_from_tc2_inverse(tmp_path):
    """히터를 켤 때 출발점은 TC2 의 매핑 역함수 — TC1 이 떨어져 낮게 읽혀도(30°C) TC2 569 면 r1 400 에서 출발."""
    s = _sim(tmp_path, "tc2")
    s.pl.h = s.pl.tc2 = 569.0; s.pl.tc1 = 400.0
    s.pl.q2.clear(); s.pl.q2.extend([569.0] * 21); s.pl.q1.clear(); s.pl.q1.extend([400.0] * 31)
    s.tc1_stuck = 30.0
    s.c.tick(s.st())                               # 꺼진 상태 폴링(_last_st)
    s.c.start(600, 6.0, "sim")
    assert s.writes[-1][1:] == (12, 400.0)         # RUN 전 D00012 ← 출발점
    assert any("출발 TC2 569.0 (TC1 환산 400.0)" in m and "TC1 은 기록만" in m and "→ SV2 846.0 (매핑)" in m
               for _, _, m in s.msgs)
    s.run = True
    s.runfor(2)
    assert s.c.r1 == pytest.approx(400.0, abs=0.3)
    # 상온 아래 TC2 는 상온으로
    s2 = _sim(tmp_path, "tc2")
    s2.pl.tc2 = 10.0
    s2.c.tick(s2.st()); s2.c.start(100, 6.0); s2.run = True; s2.runfor(1)
    assert s2.c.r1 >= 25.0


def test_tc2_does_not_touch_learned_file(tmp_path):
    lf = tmp_path / "learned.json"
    lf.write_text(json.dumps({"points": [{"tc1": 600.0, "tc2": 900.0, "t": "x"}]}), encoding="utf-8")
    before = lf.read_bytes(); mt = lf.stat().st_mtime_ns
    s = _sim(tmp_path, "tc2", learned=lf)
    s.c.start(300, 6.0, "sim"); s.run = True
    s.runfor(90 * 60)
    assert lf.read_bytes() == before and lf.stat().st_mtime_ns == mt
    assert s.c.bias_ff == 0.0 and s.c.bias_int == 0.0 and "학습" not in s.c.bias_source
    assert not any("매핑 학습" in m or "학습점" in m for _, _, m in s.msgs)


def test_tc2_status_logs_and_csv_fields(tmp_path):
    s = _held(tmp_path, 300)
    assert s.c.status_text() == f"TC2 제어(단독) · SV2 {s.D[35]:.1f}"
    lines = [m for _, _, m in s.msgs if m.startswith("[TC2 단독] TC2 ") and "(기록) / MV " in m]
    assert lines and "/ SV2 437.0 / r1 300.0 / TC1 " in lines[-1] and lines[-1].endswith("%")
    f = s.c.log_fields()
    assert f["bias_c"] == "" and f["bias_ff_c"] == "" and f["bias_int_c"] == "" and f["r1_c"] == "300.0"
    assert s.c.kind == 'cascade' and s.c.is_holding() and s.c.sv2 == 437.0       # 화면·ERP·포화 감시 질의는 cascade 와 같다
    snap = s.c.snapshot()
    assert snap["mode"] == "tc2" and snap["bias"] == 0.0 and snap["r1_done"] is True


def test_zz_print_comparison_table():
    if not RESULTS:
        pytest.skip("비교 시나리오가 돌지 않았다")
    print("\n[tc2 vs cascade] 목표·매핑 어긋남별 — tc2: TC2 오차(SV2 대비)·TC2 오버슈트·정착 p-p·도달·TC1 편차 / cascade: TC1 기준")
    print("  (도달 = 그 모드의 HEATER_WAIT 판정 60 s 연속. 시뮬레이터 PLC 추종이 실제보다 느려 도달 시간은 보수적인 값)")
    print(f"{'목표':>4} {'매핑':>5} | {'tc2 오차':>7} {'TC2 오버':>7} {'p-p':>5} {'도달분':>6} {'TC1 편차':>7} |"
          f" {'casc TC1오버':>10} {'TC1 오차':>7} {'도달분':>6} {'TC1편차':>7} | {'도달 차(tc2−casc)':>16}")
    for target, off, m2, mc in sorted(RESULTS, key=lambda x: (x[0], x[1])):
        a2, ac = m2["arrive_min"], mc["arrive_min"]
        d = "-" if (a2 is None or ac is None) else f"{a2 - ac:+.1f}"
        print(f"{target:>4} {off:>+5.0f} | {m2['err2']:>7.2f} {m2['over2']:>+7.2f} {m2['pp2']:>5.2f}"
              f" {('-' if a2 is None else f'{a2:.1f}'):>6} {m2['dev1']:>+7.1f} |"
              f" {mc['over1']:>+10.2f} {abs(mc['dev1']):>7.2f} {('-' if ac is None else f'{ac:.1f}'):>6} {mc['dev1']:>+7.1f}"
              f" | {d:>16}")


# ───────────────────────── Tc2Map.inverse ─────────────────────────
def test_map_inverse_roundtrip_and_extrapolation():
    m = Tc2Map(NEW_MAP)
    for t1 in [25, 37.5, 50, 99, 100, 175, 300, 450, 600, 650, 700]:
        assert m.inverse(m.value(t1)) == pytest.approx(t1, abs=1e-9)
    for t2 in [25, 60, 170, 437, 846, 900, 982]:
        assert m.value(m.inverse(t2)) == pytest.approx(t2, abs=1e-9)
    assert m.inverse(1118) == pytest.approx(800.0)                        # 700 위: 마지막 구간 기울기(136/100)
    assert m.inverse(-42) == pytest.approx(25 + (-67) * 25 / 67)          # 25 아래: 첫 구간 기울기(67/25)
    assert m.inverse(m.value(800.0)) == pytest.approx(800.0)


# ───────────────────────── 공정 컨트롤러 ─────────────────────────
from test_process_safety import pc   # noqa: E402,F401  (픽스처 재사용)


def _tc2_snap(pc, **kw):
    pc._hold["casc"] = dict({"active": True, "state": "running", "engaged": True, "final": 600.0, "r1": 590.0,
                             "r1_done": False, "sv2": 846.0, "bias": 0.0, "mode": "tc2"}, **kw)


@pytest.mark.tc2
def test_heater_wait_tc2_judges_tc2_not_tc1(pc):
    """TC1 이 목표에서 30°C 떨어져 있어도 r1 도달 + |TC2 − SV2| ≤ TOL 이면 통과. 바깥 루프 여유(TI×2)는 더하지 않는다."""
    pc._plc.st.update(pv=570.0, pv2=845.2, sv_ramp=846.0, cur_sv=846.0)
    _tc2_snap(pc)
    QTimer.singleShot(1200, lambda: pc._hold["casc"].update(r1=600.0, r1_done=True))
    reached = []
    pc.heater_reached.connect(lambda d: reached.append(d))
    t0 = time.monotonic()
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["force"] == []
    assert time.monotonic() - t0 >= 2.0
    assert not any("TI×2" in m for _, m in c["msgs"])
    assert any("히터 도달 — TC2 845.2 (SV2 846.0) · TC1 570.0 (목표 대비 -30.0, 기록용)" in m for _, m in c["msgs"])
    assert reached and reached[0]["record"] == "TC2 845.2 (SV2 846.0) · TC1 570.0 (목표 대비 -30.0, 기록용)"
    assert any("TC2 제어 확인 (SV2 846.0°C, TC2 단독)" in m for _, m in c["msgs"])


@pytest.mark.tc2
def test_heater_wait_tc2_waits_while_tc2_off_band(pc, monkeypatch):
    """TC1 이 목표에 정확히 있어도 TC2 가 SV2 에서 벗어나 있으면 통과하지 않는다(타임아웃)."""
    monkeypatch.setattr(PC, "HEATER_WAIT_TIMEOUT_SEC", 2.0)
    monkeypatch.setattr(PC, "WAIT_TIMEOUT_MARGIN_SEC", 0.0)
    pc._plc.st.update(pv=600.0, pv2=830.0)
    _tc2_snap(pc, r1=600.0, r1_done=True)
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 0 and len(c["abort"]) == 1 and "timeout" in c["abort"][0]


def _steps(monkeypatch, mode, **over):
    monkeypatch.setattr(PC, "HEATER_CTRL_MODE", mode)
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    c._is_connected = lambda obj: True
    c._invoke_self = lambda name: None
    c._next_step = lambda: None
    c.set_hold_state_provider(lambda: {"casc": {"active": False}})
    p = dict(process_note="t", use_dc=True, dc_power=100.0, sp1_set=5.0, shutter_delay=5.0, process_time=10.0,
             use_g1=True, use_heater=True, heater_temp=600.0, heater_ramp=6.0, ar_flow=20.0)
    p.update(over)
    c.start_process_flow(p)
    out = [(s.action, s.message) for s in c._steps]
    c.deleteLater()
    return out


@pytest.mark.tc2
def test_tc2_step_list_has_no_heater_settle(qapp, monkeypatch):
    tc2 = _steps(monkeypatch, "tc2")
    assert ActionType.HEATER_SETTLE not in [a for a, _ in tc2]
    assert tc2 == _steps(monkeypatch, "tc1")                              # 스텝 목록은 tc1 과 같다
    assert ActionType.HEATER_SETTLE in [a for a, _ in _steps(monkeypatch, "cascade")]


@pytest.mark.tc2
def test_tc2_record_line_before_main_shutter(pc, monkeypatch):
    from controller.process_controller import ProcessStep
    pc._plc.st.update(pv=571.3, pv2=846.4)
    _tc2_snap(pc, r1=600.0, r1_done=True)
    pc._heater_used = True
    sent = []
    pc.update_plc_port.connect(lambda b, v: sent.append((b, v)))
    monkeypatch.setattr(PC.QTimer, "singleShot", staticmethod(lambda ms, fn: None))
    pc._steps = [ProcessStep(ActionType.PLC_CMD, "Main Shutter Open", params=('MS_button', True))]
    pc._idx = -1
    type(pc)._next_step(pc)                         # 픽스처가 덮은 _next_step 대신 실제 실행
    msgs = [m for _, m in pc._calls["msgs"]]
    assert "셔터 열기 직전 기록 — TC2 846.4 (SV2 846.0) · TC1 571.3" in msgs
    assert sent == [("MS_button", True)]


# ───────────────────────── main 연결 ─────────────────────────
from test_main_heater import win, fresh   # noqa: E402,F401


def _casc_reset(w, monkeypatch):
    c = w.heater_casc
    c._reset()
    clock = [1000.0]
    monkeypatch.setattr(c, "_now", lambda: clock[0])
    monkeypatch.setattr(c, "learned_path", "__no_such_dir__/l.json")
    return c, clock


def _engage(w, c, clock, pv=100.0, pv2=170.0, target=300.0):
    w.update_heater_display(make_heater_st(run=False, pv=pv, sv=pv, pv2=pv2))
    w._heater_manual_go(target)
    st = dict(run=True, pv=pv, sv=pv, pv2=pv2, sv_ramp=pv, cur_sv=pv, mv=520)
    w.update_heater_display(make_heater_st(**st))
    sv2 = c._sv2_cmd
    clock[0] += 0.2
    w.update_heater_display(make_heater_st(**st, sv2=sv2))
    clock[0] += 0.2
    st.update(sv2=sv2, pv_sel_eff=True, sv_ramp=sv2, cur_sv=sv2, pv_ctrl=pv2)
    w.update_heater_display(make_heater_st(**st))
    return st


@pytest.mark.tc2
def test_main_tc2_manual_lcd_dev_is_tc2_minus_sv2(fresh, monkeypatch):
    w = fresh
    c, clock = _casc_reset(w, monkeypatch)
    assert c.tc2_only and w._hold_view() is c
    st = _engage(w, c, clock, pv=88.0, pv2=168.5)
    assert c.engaged() and not w.heater_ramp.is_active()
    assert w.ui.heater_sv_big.text() == f"{c.r1:.1f}"                     # SV 큰 숫자 = r1(TC1 기준 목표)
    assert w.ui.heater_sv2_label.text() == f"→ {c.sv2:.1f}"
    assert w.ui.heater_dev_label.text() == f"Δ{168.5 - c.sv2:+.1f}"   # 편차 = TC2 − SV2
    assert w._erp_heater_dev_ok == (abs(168.5 - c.sv2) <= 2.0)
    assert w.ui.heater_status_label.text() == f"운전 중 · TC2 제어(단독) · SV2 {c.sv2:.1f}"
    assert "bias" not in w._erp_heater and w._erp_heater["cur_sv"] == round(c.r1, 1)
    assert w._hold_snapshot["casc"]["mode"] == "tc2" and w._hold_snapshot["mode"] == "off"
    order = []
    w.request_heater_pv_sel.connect(lambda b: order.append(("M0004A", b)))
    c.request_sv2.connect(lambda v: order.append(("D00035", v)))
    w.request_heater_run.connect(lambda b: order.append(("RUN", b)))
    w._on_heater_onoff_toggled(False)
    assert order[:3] == [("M0004A", False), ("D00035", 0.0), ("RUN", False)]


@pytest.mark.tc2
def test_main_tc2_deviation_alert_card_and_run_event(fresh, monkeypatch):
    w = fresh
    c, clock = _casc_reset(w, monkeypatch)
    cards = []
    chat = MagicMock()
    chat.notify_heater_alert.side_effect = lambda title, ctx, fields, ok=True: cards.append((title, fields, ok))
    monkeypatch.setattr(w, "chat_chk", chat)
    monkeypatch.setattr(w, "_chat_hold_alert_sent", set(), raising=False)
    monkeypatch.setattr(w.plc_controller, "get_heater_status",
                        lambda: make_heater_st(run=True, pv=560.0, pv2=846.0, sv2=846.0))
    w._heater_run_events = []
    aborted = []
    monkeypatch.setattr(w, "_abort_process_by_fault", lambda *a: aborted.append(a))
    c.alert.emit("tc1_deviation", {"text": "TC1 편차 큼(기록용) — … 제어에는 영향 없음 — TC1 위치/접촉 확인",
                                    "pv": 560.0, "pv2": 846.0, "sv2": 846.0, "final": 600.0, "dev": -40.0})
    c.alert.emit("tc1_deviation", {"text": "again", "final": 600.0, "dev": -40.0})   # 같은 종류 카드는 1장
    assert "TC1 편차 큼(기록용)" in w._heater_run_events
    assert len(cards) == 1
    title, fields, ok = cards[0]
    assert title == "히터 TC1 편차 큼(기록용)" and ok is False
    assert fields["조치"].startswith("공정 계속 — 제어에는 영향 없음") and "bias" not in fields
    assert fields["편차"] == "-40.0°C" and aborted == []


@pytest.mark.tc2
def test_main_tc2_csv_row(fresh, monkeypatch):
    w = fresh
    c, clock = _casc_reset(w, monkeypatch)
    _engage(w, c, clock)
    rows = []
    monkeypatch.setattr(w._heater_logger, "write_row", lambda *a, **k: rows.append((a, k)))
    w._heater_log_row(make_heater_st(run=True, pv=100.0, pv2=170.0))
    a, k = rows[0]
    assert k["ctrl_mode"] == "tc2" and k["casc"]["bias_c"] == "" and k["casc"]["bias_ff_c"] == ""
    assert k["casc"]["bias_int_c"] == "" and k["casc"]["engaged"] is True


# ───────────────────────── 설정 검증 ─────────────────────────
def _validate(monkeypatch, **vals):
    import lib.config as CFG
    for k, v in vals.items():
        monkeypatch.setattr(CFG, k, v)
    CFG._validate_heater_cascade_config()
    return CFG


@pytest.mark.parametrize("raw,want", [("tc2", "tc2"), ("cascade", "cascade"), ("tc1", "tc1"), (" TC2 ", "tc2"),
                                      ("pid", "tc2"), ("", "tc2"), (None, "tc2")])
def test_config_ctrl_mode(monkeypatch, capsys, raw, want):
    CFG = _validate(monkeypatch, HEATER_CTRL_MODE=raw)
    assert CFG.HEATER_CTRL_MODE == want
    if raw not in ("tc2", "cascade", "tc1", " TC2 "):
        assert "허용: tc2 / cascade / tc1" in capsys.readouterr().out


@pytest.mark.parametrize("key,raw,want", [("HEATER_TC2_TC1_WARN_C", 1.0, 3.0), ("HEATER_TC2_TC1_WARN_C", 500, 100.0),
                                          ("HEATER_TC2_TC1_WARN_C", "x", 15.0), ("HEATER_TC2_TC1_WARN_C", 20, 20.0),
                                          ("HEATER_TC2_TC1_WARN_SEC", 5, 30.0), ("HEATER_TC2_TC1_WARN_SEC", 9999, 1800.0),
                                          ("HEATER_TC2_TC1_WARN_SEC", None, 120.0), ("HEATER_TC2_TC1_WARN_SEC", 300, 300.0),
                                          ("HEATER_TC2_HOLDBACK_C", 1.0, 2.0), ("HEATER_TC2_HOLDBACK_C", 500, 100.0),
                                          ("HEATER_TC2_HOLDBACK_C", "x", 20.0), ("HEATER_TC2_HOLDBACK_C", 15, 15.0)])
def test_config_tc1_warn_clamp(monkeypatch, key, raw, want):
    CFG = _validate(monkeypatch, **{key: raw})
    assert getattr(CFG, key) == want


def test_config_new_map_passes_and_bad_map_falls_back(monkeypatch):
    CFG = _validate(monkeypatch, HEATER_TC2_MAP=NEW_MAP)
    assert CFG.HEATER_TC2_MAP == [[float(a), float(b)] for a, b in NEW_MAP]
    CFG = _validate(monkeypatch, HEATER_TC2_MAP=[[25, 25], [600, 500], [700, 400]])     # TC2 감소 → 기본값
    assert CFG.HEATER_TC2_MAP == [[float(a), float(b)] for a, b in NEW_MAP]


def test_repo_config_user_json_values():
    with open(importlib.import_module("lib.config_loader")._JSON_PATH, encoding="utf-8") as f:
        d = json.load(f)
    assert d["HEATER_CTRL_MODE"] == "tc2" and d["HEATER_TC2_MAP"] == NEW_MAP
    assert d["HEATER_TC2_TC1_WARN_C"] == 15.0 and d["HEATER_TC2_TC1_WARN_SEC"] == 120 and d["HEATER_TC2_HOLDBACK_C"] == 20.0
    assert any("tc2     = TC2 단독 제어" in k for k in d) and any("되돌리기" in k for k in d)
