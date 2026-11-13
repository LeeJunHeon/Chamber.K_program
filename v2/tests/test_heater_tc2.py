"""HEATER_CTRL_MODE=tc2(TC2 단독 제어, 기본) — v1 tests/test_heater_tc2.py(8776a64)를 v2 로 옮긴 것(v1 import 없음).

test_heater_cascade 의 열 모델(Plant·PID·Sim)을 그대로 쓰되, 플랜트의 참 매핑과 제어 매핑을 새 기본 매핑(NEW_MAP)으로 둔다.
  · tc2 모드: SV2 = map(r1), 바깥 루프 없음 — TC2 가 SV2 에 붙고, 매핑이 어긋난 만큼 TC1 이 목표에서 벗어나야 정상.
  · 같은 시나리오를 cascade 로도 돌려 나란히 비교한다(도달 시간 차이 포함).
  · 셔터·TC2 상실·TC1 탈락(편차 안내만)·레시피 HOLD/RESUME·목표 하강·TC2 홀드백·출발점(TC2 역함수)·학습 파일 무접촉.
v2 쪽: HeaterEngine 의 승온 대기 tc2 판정(TI×2 여유 없음)·도달 기록, 기본 템플릿에 HEATER_SETTLE 없음, 셔터 직전 기록 한 줄,
TC1 편차 안내 사건(공정 계속), 스냅샷 열, 시험대(ChkRig) 수동 ON/OFF 쓰기 순서, 설정 정의(허용값·범위·기본 매핑·키 사이 규칙).
v1 의 main 화면(LCD·챗 카드·CSV) 시험은 v2 대상이 아니다 — 같은 값은 스냅샷 열·사건으로 본다.
결과표는 `pytest -s v2/tests/test_heater_tc2.py` 로 볼 수 있다.
"""
import json

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import check_config, check_value, defaults
from v2.chk import blocks as BK
from v2.chk import heater_cascade as HC
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.heater_cascade import Tc2Map, ramp_minutes
from v2.chk.process_steps import row_to_params
from v2.chk.sim.plc_sim import A
from v2.tests.test_heater_cascade import SV2_MAX, Plant, Sim, _interp
from v2.tests.test_heater_cascade_engine import _engine, _rig, _run, _writes

pytestmark = pytest.mark.ctrl_cfg          # 설정값 그대로(기본 tc2)

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
    s = Sim(tmp_path, HEATER_TC2_MAP=HC.map_text(NEW_MAP), HEATER_CTRL_MODE=mode, **kw)
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
    t2 = [r[2] for r in tail]
    t1 = [r[1] for r in tail]
    done = lambda r: r[4] is not None and r[4] >= target - 0.05
    if s.c.tc2_only:      # 도달 = 그 모드의 승온 대기 판정(tc2: r1 도달 + |TC2 − SV2| ≤ TOL 60 s)
        a = _arrive(rec, lambda r: done(r) and abs(r[2] - sv2f) <= TOL)
    else:                 # cascade: r1 도달 + |TC1 − 목표| ≤ TOL 60 s
        a = _arrive(rec, lambda r: done(r) and abs(r[1] - target) <= TOL)
    return {"err2": abs(sum(t2) / len(t2) - sv2f), "over2": max(r[2] for r in rec) - sv2f, "pp2": max(t2) - min(t2),
            "arrive_min": None if a is None else a / 60.0, "dev1": sum(t1) / len(t1) - target,
            "over1": max(r[1] for r in rec) - target, "sv2_final": sv2f}


def _ramp(tmp_path, mode, target, offset, rate=6.0, tail_min=90, **kw):
    s = _sim(tmp_path, mode, offset, **kw)
    s.c.start(target, rate, "sim")
    s.run = True
    s.runfor((target - 25.0) / rate * 60 + tail_min * 60)
    return s, _metrics(s, target)


def _wait_timeout_min(target, rate=6.0):
    """공정 승온 대기 한도(tc2 — TI×2 여유 없음): max(HEATER_WAIT_TIMEOUT_SEC, 예상 승온 + 여유), 상한 MAX."""
    d = defaults(P.PARAMS)
    need = ramp_minutes(target - 25.0, rate, d["HEATER_APPROACH_ZONE_C"], d["HEATER_APPROACH_MIN_RATE_C_PER_MIN"]) * 60.0
    return min(max(float(d["HEATER_WAIT_TIMEOUT_SEC"]), need + d["HEATER_WAIT_TIMEOUT_MARGIN_SEC"]),
               d["HEATER_WAIT_TIMEOUT_MAX_SEC"]) / 60.0


def _expected_tc1(target, offset):
    """TC2 가 map(목표) 에 붙을 때의 참 TC1 — f_true(TC1) = map(목표)."""
    return Plant2(offset=offset).finv(_interp(NEW_MAP, target))


@pytest.mark.parametrize("offset", [0.0, 25.0, -25.0])
@pytest.mark.parametrize("target", [100, 300, 500, 600, 700])
def test_sim_tc2_vs_cascade(tmp_path, target, offset):
    s2, m2 = _ramp(tmp_path, "tc2", target, offset, tail_min=150)
    sc, mc = _ramp(tmp_path, "cascade", target, offset, tail_min=150, learned=tmp_path / "casc_learned.json")
    RESULTS.append((target, offset, m2, mc))
    assert s2.c.tc2_only and not sc.c.tc2_only
    assert s2.c.state == "running" and s2.c.bias == 0.0
    assert not (tmp_path / "learned.json").exists()                     # tc2 는 학습 파일을 쓰지 않는다
    assert m2["err2"] <= 0.5 and m2["pp2"] <= 1.0 and m2["over2"] <= 3.0, m2
    assert m2["arrive_min"] is not None and m2["arrive_min"] <= _wait_timeout_min(target), m2   # 공정 승온 대기 안
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
    s.shutter = True
    s.tc1_off = -100.0                                    # 셔터 열림: TC1 이 약 100°C 낮게 읽힌다
    t_open = s.t
    s.runfor(20 * 60)
    seg = [r for r in s.rec if r[0] > t_open]
    assert s.c.state == "frozen" and all(r[6] == sv2 for r in seg)
    assert max(r[2] for r in seg) - min(r[2] for r in seg) < 1.0
    assert not [a for a in s.alerts if a[1] == "tc1_deviation"]          # 셔터 열림 중에는 안내하지 않는다
    assert any("메인 셔터 열림 — SV2 846.0 유지" in m for _, _, m in s.msgs)
    s.c.start(650, 6.0)
    s.runfor(120)                                                        # 상승 목표는 셔터가 닫힐 때까지 보류
    assert s.c.r1 == pytest.approx(600.0) and s.D[35] == sv2
    s.shutter = False
    s.tc1_off = 0.0
    s.runfor(60)
    assert s.c.state == "running" and s.c.r1 > 600.0
    assert any("메인 셔터 닫힘 — TC2 제어 재개" in m and "적분" not in m for _, _, m in s.msgs)


def test_tc2_lost_shutter_closed_falls_back_to_tc1(tmp_path):
    s = _held(tmp_path, 300)
    order = len(s.writes)
    s.tc2_bad = True
    s.runfor(5)
    assert s.c.state == "tc1" and [a[1] for a in s.alerts] == ["tc2_lost"]
    w = [(k, v) for _, k, v in s.writes[order:]]
    assert w[:2] == [("M4A", False), (35, 0.0)]                           # 해제 순서 M0004A OFF → D00035=0
    s.runfor(20 * 60)
    m = sum(r[1] for r in s.rec[-300:]) / 300
    assert abs(m - 300.0) <= 2.0                                         # 래더 TC1 경로가 같은 r1 을 따라간다


def test_tc2_lost_shutter_open_fixes_dac(tmp_path):
    s = _held(tmp_path, 600)
    s.shutter = True
    s.tc1_off = -100.0
    s.runfor(10)
    mv = int(s.mv)
    s.tc2_bad = True
    s.runfor(3)
    assert s.c.state == "dac" and abs(s.D[18] - mv) <= 25
    assert [a[1] for a in s.alerts] == ["demoted"]
    s.shutter = False
    s.tc1_off = 0.0
    s.runfor(3)
    assert s.D[18] == 1200 and s.c.state == "tc1"                        # 상한 원복, TC2 없음 → TC1 제어


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
    assert dev[0][2]["dev"] == pytest.approx(-180.0) and dev[0][2]["final"] == 600.0
    assert not [a for a in s.alerts if a[1] == "tc1_detached"]
    seg = [r for r in s.rec if r[0] > t0]
    assert all(r[6] == sv2 for r in seg) and max(abs(r[2] - sv2) for r in seg) < 1.0


def test_tc2_tc1_warn_keys_are_read_live(tmp_path):
    """HEATER_TC2_TC1_WARN_C·SEC 는 쓰는 때 읽는다 — 편차 30 은 기준 15 면 안내, 50 이면 안내 없음."""
    s = _held(tmp_path, 300, HEATER_TC2_TC1_WARN_C=50.0, HEATER_TC2_TC1_WARN_SEC=30.0)
    s.tc1_stuck = 270.0
    s.runfor(10 * 60)
    assert not [a for a in s.alerts if a[1] == "tc1_deviation"]
    s.c.cfg["HEATER_TC2_TC1_WARN_C"] = 15.0
    t0 = s.t
    s.runfor(60)
    dev = [a for a in s.alerts if a[1] == "tc1_deviation"]
    assert len(dev) == 1 and 30 <= dev[0][0] - t0 <= 33


def test_tc2_tc1_none_no_notice(tmp_path):
    s = _held(tmp_path, 600)
    real = s.st
    s.st = lambda: dict(real(), pv=None)
    s.runfor(10 * 60)
    assert not [a for a in s.alerts if a[1] == "tc1_deviation"] and s.c.state == "running"


def test_tc2_recipe_hold_resume_and_lower_target(tmp_path):
    s = _sim(tmp_path, "tc2")
    s.c.start(400, 6.0, "sim")
    s.run = True
    s.runfor(20 * 60)
    s.c.hold()
    r = s.c.r1
    s.runfor(10 * 60)
    assert s.c.r1 == pytest.approx(r) and s.c.is_held()
    s.c.resume()
    s.runfor(60)
    assert s.c.r1 > r + 3.0
    s.runfor(90 * 60)
    assert s.c.r1_done(400)                                              # 레시피 RAMP 완료 판정(r1_done(목표))
    s.c.start(250, 6.0)
    s.runfor(3)
    assert s.c.r1 == pytest.approx(250.0) and s.D[35] == pytest.approx(365.0)   # 하강은 즉시
    s.runfor(60 * 60)
    m = sum(r[2] for r in s.rec[-600:]) / 600
    assert abs(m - 365.0) <= 1.0


def test_tc2_holdback_uses_tc2(tmp_path):
    """DAC 상한을 낮춰 TC2 가 SV2 를 못 따라가게 하면 r1 이 멈춘다(TC1 은 쓰지 않는다 — TC1 을 일부러 높게 읽혀도)."""
    s = _sim(tmp_path, "tc2")
    s.D[18] = 560                                  # 출력 상한 — TC2 가 몇백 도에서 막힌다
    s.tc1_off = +200.0                             # TC1 을 앞서게 읽혀도 홀드백은 TC2 로만 판단
    s.c.start(600, 6.0, "sim")
    s.run = True
    s.runfor(150 * 60)
    assert s.c.r1 < 500.0
    gap = s.D[35] - s.pl.tc2
    assert s.c.tc2_holdback == 20.0                                       # HEATER_TC2_HOLDBACK_C 기본(HEATER_HOLDBACK_C 10 과 별개)
    assert gap <= s.c.map.slope(s.c.r1) * s.c.tc2_holdback + 2.0


def test_tc2_start_point_from_tc2_inverse(tmp_path):
    """히터를 켤 때 출발점은 TC2 의 매핑 역함수 — TC1 이 떨어져 낮게 읽혀도(30°C) TC2 569 면 r1 400 에서 출발."""
    s = _sim(tmp_path, "tc2")
    s.pl.h = s.pl.tc2 = 569.0
    s.pl.tc1 = 400.0
    s.pl.q2.clear()
    s.pl.q2.extend([569.0] * 21)
    s.pl.q1.clear()
    s.pl.q1.extend([400.0] * 31)
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
    s2.c.tick(s2.st())
    s2.c.start(100, 6.0)
    s2.run = True
    s2.runfor(1)
    assert s2.c.r1 >= 25.0


def test_tc2_does_not_touch_learned_file(tmp_path):
    lf = tmp_path / "learned.json"
    lf.write_text(json.dumps({"points": [{"tc1": 600.0, "tc2": 900.0, "t": "x"}]}), encoding="utf-8")
    before = lf.read_bytes()
    mt = lf.stat().st_mtime_ns
    s = _sim(tmp_path, "tc2", learned=lf)
    s.c.start(300, 6.0, "sim")
    s.run = True
    s.runfor(90 * 60)
    assert lf.read_bytes() == before and lf.stat().st_mtime_ns == mt
    assert s.c.bias_ff == 0.0 and s.c.bias_int == 0.0 and "학습" not in s.c.bias_source
    assert not any("매핑 학습" in m or "학습점" in m for _, _, m in s.msgs)


def test_tc2_status_logs_and_snapshot_fields(tmp_path):
    s = _held(tmp_path, 300)
    assert s.c.status_text() == f"TC2 제어(단독) · SV2 {s.D[35]:.1f}"
    assert all(m.startswith("[TC2 단독] ") for _, _, m in s.msgs)
    lines = [m for _, _, m in s.msgs if m.startswith("[TC2 단독] TC2 ") and "(기록) / MV " in m]
    assert lines and "/ SV2 437.0 / r1 300.0 / TC1 " in lines[-1] and lines[-1].endswith("%")
    f = s.c.fields()                                                    # v1 log_fields: bias 열만 빈 칸, 나머지는 그대로
    assert f["heater.bias"] is None and f["heater.bias_ff"] is None and f["heater.bias_int"] is None
    assert f["heater.r1"] == 300.0 and f["heater.sv2_map"] == 437.0 and f["heater.casc_state"] == "running"
    assert s.c.kind == "cascade" and s.c.is_holding() and s.c.sv2 == 437.0       # 화면·포화 감시 질의는 cascade 와 같다
    snap = s.c.snapshot()
    assert snap["mode"] == "tc2" and snap["bias"] == 0.0 and snap["r1_done"] is True
    s.c.stop("시험")
    assert any("종료 (시험) — r1 300.0 / SV2 437.0 · TC1 제어 복귀" in m for _, _, m in s.msgs)


def test_cascade_mode_unchanged_by_tc2_branches(tmp_path):
    """같은 Sim 을 cascade 로 — 문구 말머리·bias 열·학습이 예전 그대로."""
    s, _ = _ramp(tmp_path, "cascade", 300, 0.0, tail_min=60)
    assert not s.c.tc2_only and s.c.snapshot()["mode"] == "cascade"
    assert all(m.startswith("[캐스케이드] ") for _, _, m in s.msgs)
    assert s.c.status_text().startswith("TC2 제어 · bias ") and s.c.fields()["heater.bias"] is not None
    assert (tmp_path / "learned.json").exists()


def test_zz_print_comparison_table():
    if not RESULTS:
        pytest.skip("비교 시나리오가 돌지 않았다")
    print("\n[tc2 vs cascade] 목표·매핑 어긋남별 — tc2: TC2 오차(SV2 대비)·TC2 오버슈트·정착 p-p·도달·TC1 편차 / cascade: TC1 기준")
    print("  (도달 = 그 모드의 승온 대기 판정 60 s 연속. 시뮬레이터 PLC 추종이 실제보다 느려 도달 시간은 보수적인 값)")
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


# ───────────────────────── HeaterEngine(가짜 PLC) — v1 공정 컨트롤러 시험 ─────────────────────────
def _tc2_engine(pv, pv2, **snap):
    h, aborts = _engine()
    assert h.tc2_only and h.tc2_pid and not h.cascade
    h.st = {"pv": pv, "pv2": pv2, "run": True, "fault": False, "sv_ramp": 846.0}
    h.process_ramp(6.0)
    h.process_set(600.0)
    cs = dict({"active": True, "state": "running", "engaged": True, "final": 600.0, "r1": 590.0, "r1_done": False,
               "sv2": 846.0, "bias": 0.0, "mode": "tc2"}, **snap)
    h.casc.snapshot = lambda: cs
    h.casc.engaged = lambda: True
    h.casc._sv2_cmd = 846.0
    h.casc._state = "running"
    return h, cs, aborts


def test_heater_wait_tc2_judges_tc2_not_tc1():
    """TC1 이 목표에서 30°C 떨어져 있어도 r1 도달 + |TC2 − SV2| ≤ TOL 이 HEATER_SOAK_TIME_SEC 연속이면 통과.
    바깥 루프 여유(TI×2)는 더하지 않는다."""
    h, cs, _ = _tc2_engine(570.0, 845.2)
    assert h.process_wait_tick(0.0) == "wait" and h.proc["phase"] == "RAMP"       # r1 이 아직 목표 전
    cs.update(r1=600.0, r1_done=True)
    assert h.process_wait_tick(10.0) == "wait" and h.proc["phase"] == "SOAK"
    assert h.process_wait_tick(69.0) == "wait"
    assert h.process_wait_tick(70.0) == "reached"
    msgs = [e.detail for e in h.events if e.kind == "message"]
    rec = "TC2 845.2 (SV2 846.0) · TC1 570.0 (목표 대비 -30.0, 기록용)"
    assert any("히터 목표 도달(TC2 기준) — 60s 유지 확인 중" in m for m in msgs)
    assert any(f"TC2 단독 제어 확인 (SV2 846.0°C, TC2 단독) — 히터 도달 {rec}" in m for m in msgs)
    assert [e.detail for e in h.events if e.kind == "reached"] == [f"600.0°C TC2 단독 제어 — {rec}"]


def test_heater_wait_tc2_timeout_has_no_ti_margin_and_cascade_keeps_it():
    h, _, _ = _tc2_engine(25.0, 25.0)
    h.process_wait_tick(0.0)
    hc, _ = _engine(HEATER_CTRL_MODE="cascade")
    hc.st = dict(h.st)
    hc.process_ramp(6.0)
    hc.process_set(600.0)
    hc.process_wait_tick(0.0)
    ht, _ = _engine(HEATER_CTRL_MODE="tc1")
    ht.st = dict(h.st)
    ht.process_ramp(6.0)
    ht.process_set(600.0)
    ht.process_wait_tick(0.0)
    assert h.proc["timeout"] == ht.proc["timeout"]                         # tc2 = tc1 과 같은 한도
    assert hc.proc["timeout"] - h.proc["timeout"] == pytest.approx(2 * 600.0)   # cascade 만 TI×2
    assert not ht.casc.is_active() and h.casc.is_active() and not h.ramp.is_active()


def test_heater_wait_tc2_waits_while_tc2_off_band():
    """TC1 이 목표에 정확히 있어도 TC2 가 SV2 에서 벗어나 있으면 통과하지 않는다(한도 초과 → HEATER_WAIT_TIMEOUT)."""
    h, cs, _ = _tc2_engine(600.0, 830.0, r1=600.0, r1_done=True)
    assert h.process_wait_tick(0.0) == "wait"
    assert h.process_wait_tick(600.0) == "wait" and h.proc["phase"] == "RAMP"
    assert h.process_wait_tick(h.proc["timeout"] + 1.0) == ("fail", "HEATER_WAIT_TIMEOUT")
    # 띠 안에 들어왔다 나가면 연속 시간이 처음부터
    h2, cs2, _ = _tc2_engine(600.0, 846.0, r1=600.0, r1_done=True)
    h2.process_wait_tick(0.0)
    assert h2.proc["phase"] == "SOAK"
    h2.st["pv2"] = 840.0
    h2.process_wait_tick(30.0)
    assert h2.proc["phase"] == "RAMP"
    h2.st["pv2"] = 846.5
    h2.process_wait_tick(40.0)
    assert h2.process_wait_tick(99.0) == "wait" and h2.process_wait_tick(100.0) == "reached"
    # TC2·SV2 가 없으면(TC2 상실) 통과하지 않는다. TC1 이 없어도(None) 판정에는 영향 없다
    h3, cs3, _ = _tc2_engine(None, 846.0, r1=600.0, r1_done=True)
    h3.process_wait_tick(0.0)
    assert h3.process_wait_tick(60.0) == "reached"
    h4, cs4, _ = _tc2_engine(600.0, None, r1=600.0, r1_done=True)
    h4.process_wait_tick(0.0)
    assert h4.process_wait_tick(60.0) == "wait" and h4.proc["phase"] == "RAMP"


def test_tc2_hold_mode_is_off_and_manual_routes():
    h, _ = _engine(proc_active=False)
    assert h.hold.mode == "off"                                            # 유지 모드(HeaterHold)는 쓰지 않는다
    h.st = {"pv": 30.0, "pv2": 569.0, "run": False, "itl": True, "fault": False}
    h.casc.tick(dict(h.st))
    assert h.manual_on(600.0) is None and h.casc.is_active() and not h.ramp.is_active()
    assert ("sv", 400.0) in h.plc.calls                                    # 출발점 = TC2 569 의 역매핑(TC1 30 이 아니라)
    h.st["run"] = True
    h.set_sv(250.0)
    assert h.casc.target() == 250.0
    h.plc.calls.clear()
    h.manual_off()
    assert h.casc.state == "idle" and h.plc.calls[-1] == ("run", False)


def test_tc2_deviation_alert_is_event_only_no_abort():
    """TC1 편차 안내 → 사건(경고) 1건 HEATER_TC2_TC1_DEVIATION. 공정·레시피는 멈추지 않는다(v1 챗 카드 1장·런 이벤트)."""
    h, aborts = _engine()
    h.owner = "process"
    h._casc_alert("tc1_deviation", {"text": "TC1 편차 큼(기록용) — … 제어에는 영향 없음 — TC1 위치/접촉 확인",
                                    "pv": 560.0, "pv2": 846.0, "sv2": 846.0, "final": 600.0, "dev": -40.0})
    h._flush()
    al = [e for e in h.events if e.kind == "alert"]
    assert len(al) == 1 and al[0].code == "HEATER_TC2_TC1_DEVIATION" and al[0].level == "warn"
    assert al[0].detail.startswith("TC1 편차 큼(기록용) — ") and al[0].data["dev"] == -40.0      # 이름을 두 번 쓰지 않는다
    assert al[0].data["action"] is None and aborts == []
    from v2.chk import commands as C
    assert any(r.code == "HEATER_TC2_TC1_DEVIATION" for r in C.ALL_REASONS)


# ───────────────────────── 기본 템플릿 — HEATER_SETTLE 없음 ─────────────────────────
def _template_steps(**over):
    cfg = dict(defaults(P.PARAMS), **over)
    row = {"process_name": "T", "use_heater": True, "heater_temp": 600.0, "heater_ramp": 6.0, "ar": True, "ar_flow": 20.0,
           "working_pressure": 5.0, "use_dc": True, "dc_power": 100.0, "shutter_delay": 5.0, "process_time": 10.0}
    p = row_to_params(row, cfg)
    steps, _ = BK.expand(BK.template_blocks(p, cfg, ig_enabled=False), cfg, ig_enabled=False)
    return [(s.action, s.message) for s in steps if not s.internal]


def test_tc2_step_list_has_no_heater_settle():
    tc2 = _template_steps()
    assert "HEATER_SETTLE" not in [a for a, _ in tc2]
    assert tc2 == _template_steps(HEATER_CTRL_MODE="tc1")                  # 스텝 목록은 tc1 과 같다
    casc = _template_steps(HEATER_CTRL_MODE="cascade")
    assert "HEATER_SETTLE" in [a for a, _ in casc]
    assert [x for x in casc if x[0] != "HEATER_SETTLE"] == tc2


# ───────────────────────── 시험대(ChkRig) ─────────────────────────
def test_rig_tc2_manual_on_off_write_order_and_snapshot(tmp_path):
    """수동 ON 300 → D00012 ← 출발점, RUN → D00035 ← map(r1) → M0004A → TC2 제어. OFF 는 M0004A OFF → D00035=0 → RUN OFF.
    스냅샷: ctrl_mode tc2, casc_state running, bias 열 값 없음, 유지 모드 off."""
    c, rig = _rig(tmp_path)
    plc = rig.sims["plc"]
    assert plc.tc2_map is not None                                         # 시험대 TC2 가 매핑을 따른다(tc2 도)
    rows, mark = [], {}
    real = rig.hub.on_row
    rig.hub.on_row = lambda row: (rows.append(dict(row)), real(row))[1]

    def on():
        mark["n0"] = len(plc.frames)
        assert rig.core.command("HEATER_ON", {"value": 300.0}).reason is None

    def off():
        mark["row"] = dict(rows[-1])
        mark["n1"] = len(plc.frames)
        assert rig.core.command("HEATER_OFF", {}).reason is None
    _run(rig, 50, {3: on, 40: off})
    first = _writes(plc, mark["n0"])
    names = [k for k, _ in first]
    assert first[names.index("D00012")][1] == 25.0                          # 출발점 = TC2 25 의 역매핑(상온)
    assert names.index("RUN") < names.index("D00035") < names.index("M0004A")
    r = mark["row"]
    assert r["heater.ctrl_mode"] == "tc2" and r["heater.casc_state"] == "running" and r["heater.pv_sel_eff"] is True
    assert r["heater.r1"] > 25.0 and r["heater.sv2"] == pytest.approx(Tc2Map(NEW_MAP).value(r["heater.r1"]), abs=1.0)
    assert r["heater.sv2_map"] is not None
    assert r["heater.bias"] is None and r["heater.bias_ff"] is None and r["heater.bias_int"] is None
    assert r["heater.hold"] == "off"
    last = _writes(plc, mark["n1"])
    assert last[:3] == [("M0004A", False), ("D00035", 0.0), ("RUN", False)]
    assert rig.heater.casc.state == "idle"
    hev = [e.detail for e in rig.heater.events if e.kind == "message"]
    assert any(m.startswith("[TC2 단독] 시작 수동: 목표 TC1 300.0°C → SV2 437.0 (매핑)") for m in hev)


@pytest.mark.slow
def test_rig_tc2_process_row_reaches_logs_before_shutter_and_runs_main(tmp_path):
    """v1 행(히터 100 °C·셔터 딜레이·메인) 끝까지 — 승온 대기는 TC2 − SV2 판정, 셔터 전 정착 단계 없음, 메인 셔터 열기 직전
    TC1·TC2 한 줄, 끝나면 히터 해제."""
    c, rig = _rig(tmp_path)
    row = {"process_name": "T2", "use_heater": True, "heater_temp": 100.0, "ar": True, "ar_flow": 20.0,
           "working_pressure": 5.0, "use_dc": True, "dc_power": 100.0, "shutter_delay": 0.5, "process_time": 0.5}
    _run(rig, 3600, {3: lambda: rig.core.command("RECIPE_LOAD", {"name": "t2", "rows": [row]}),
                     4: lambda: rig.core.command("RECIPE_START", {})})
    ev = [e for e in rig.process.events]
    ends = [(e.data["status"], e.data["reason"]) for e in ev if e.kind == "run_end"]
    assert ends == [("done", None)], [e.detail for e in ev if e.kind in ("warn",)]
    steps = [e.detail for e in ev if e.kind == "step"]
    assert not any("셔터 전 히터 정착" in d for d in steps)
    i = next(k for k, d in enumerate(steps) if "Main Shutter Open" in d)
    assert "Shutter Delay" in steps[i - 1]
    msgs = [e.detail for e in ev if e.kind == "message"]
    line = [m for m in msgs if m.startswith("셔터 열기 직전 기록 — TC2 ")]
    assert len(line) == 1 and "(SV2 170.0) · TC1 " in line[0]
    hev = [e.detail for e in rig.heater.events if e.kind == "message"]
    assert any("TC2 단독 제어 확인 (SV2 170.0°C, TC2 단독) — 히터 도달 TC2 " in m for m in hev)
    assert not any("캐스케이드" in m for m in hev)
    assert not [e for e in ev if e.kind == "warn" and e.code and e.code.startswith("HEATER_SETTLE")]
    assert rig.heater.casc.state == "idle"                     # 정지 순서가 히터를 끄며 해제


def test_rig_tc2_settle_block_is_skipped_with_warning(tmp_path):
    """사용자가 넣은 heater_settle 블록 — tc2 에서는 경고 뒤 다음(HEATER_SETTLE_SKIPPED, cascade 전용)."""
    c, rig = _rig(tmp_path)
    blocks = [{"type": "heater_ramp", "params": {"target_c": 60.0}}, {"type": "heater_settle", "params": {}},
              {"type": "wait", "params": {"sec": 1}}]
    _run(rig, 60, {3: lambda: rig.core.command("PROCESS_START", {"blocks": blocks})})
    ev = rig.process.events
    assert [(e.data["status"], e.data["reason"]) for e in ev if e.kind == "run_end"] == [("done", None)]
    assert [e.code for e in ev if e.kind == "warn" and e.code == "HEATER_SETTLE_SKIPPED"] == ["HEATER_SETTLE_SKIPPED"]


# ───────────────────────── 설정 정의 ─────────────────────────
def test_param_defs_follow_v1_6e0685b():
    d, by = defaults(P.PARAMS), P.by_key()
    assert d["HEATER_CTRL_MODE"] == "tc2" and by["HEATER_CTRL_MODE"].enum == ("tc2", "cascade", "tc1")
    assert HC.parse_map(d["HEATER_TC2_MAP"]) == [tuple(map(float, x)) for x in NEW_MAP]
    assert d["HEATER_RAMP_RATE_C_PER_MIN"] == 6.0                           # v1 bb2b0a3
    for key, dflt, lo, hi in (("HEATER_TC2_TC1_WARN_C", 15.0, 3.0, 100.0), ("HEATER_TC2_TC1_WARN_SEC", 120.0, 30.0, 1800.0),
                              ("HEATER_TC2_HOLDBACK_C", 20.0, 2.0, 100.0)):
        p = by[key]
        assert (p.default, p.min, p.max) == (dflt, lo, hi)
        assert check_value(p, lo) is None and check_value(p, hi) is None
        assert check_value(p, lo - 0.5) and check_value(p, hi + 1)
    assert check_config(P.PARAMS, P.CROSS_RULES, d) == []


@pytest.mark.parametrize("raw,ok", [("tc2", True), ("cascade", True), ("tc1", True), (" TC2 ", False), ("pid", False),
                                    ("", False), (None, False)])
def test_ctrl_mode_param_is_strict_and_reader_falls_back_to_tc2(raw, ok):
    """설정 검사는 허용값만(v2 는 틀린 값을 고쳐 쓰지 않고 거부). 읽는 함수는 v1 과 같이 틀린 값을 tc2 로."""
    p = P.by_key()["HEATER_CTRL_MODE"]
    assert (check_value(p, raw) is None) is ok
    want = raw if ok else ("tc2" if raw != " TC2 " else "tc2")
    assert HC.ctrl_mode({"HEATER_CTRL_MODE": raw}) == want
    assert HC.ctrl_mode({}) == "tc2"
    assert HC.tc2_pid({"HEATER_CTRL_MODE": "tc2"}) and HC.tc2_pid({"HEATER_CTRL_MODE": "cascade"})
    assert not HC.tc2_pid({"HEATER_CTRL_MODE": "tc1"})


def test_cross_rule_x27_tc2_target_limit_must_fit_under_sv2_cap():
    d = defaults(P.PARAMS)
    bad = dict(d, HEATER_HOLD_TC2_MARGIN_C=200.0)                           # 상한 950 < map(700) 982
    errs = check_config(P.PARAMS, P.CROSS_RULES, bad)
    assert any(e.startswith("[X27]") for e in errs)
    assert not any(e.startswith("[X27]") for e in check_config(P.PARAMS, P.CROSS_RULES, dict(bad, HEATER_CTRL_MODE="cascade")))
    assert not any(e.startswith("[X27]") for e in check_config(P.PARAMS, P.CROSS_RULES, dict(bad, HEATER_CTRL_MODE="tc1")))
    errs = check_config(P.PARAMS, P.CROSS_RULES, dict(d, HEATER_TC2_MAP="25:25,300:445,200:500"))
    assert any(e.startswith("[X26]") for e in errs) and not any(e.startswith("[X27]") for e in errs)


def test_snapshot_enum_labels_cover_tc2():
    f = {x.name: x for x in SF.HEATER_BLOCK.fields}
    assert dict(f["ctrl_mode"].enum_labels) == {"tc2": "TC2 단독 제어", "cascade": "2단 제어(TC2 + TC1 보정)",
                                               "tc1": "TC1 직접 제어"}
    assert set(dict(f["ctrl_mode"].enum_labels)) == set(HC.CTRL_MODES)
    assert dict(f["casc_state"].enum_labels) == HC.STATE_TEXT and "TC2 제어(단독)" in f["casc_state"].desc
    assert set(HC.STATE_TEXT_TC2) <= set(HC.STATE_TEXT)
