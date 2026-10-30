"""heater_cascade(5-15) — v1 tests/test_heater_cascade.py(3328872)를 v2 로 옮긴 것(v1 import 없음).

[플랜트] v1 과 같은 단순 열 모델: MV(DAC) → 히터 H → TC2(순수지연 20 s + 1차 15 s) → TC1(순수지연 30 s + 1차 35 s).
  출력 = VARITAP 위상제어 실효 전력, 열손실 = 선형 + 복사 항, 정상 상태 TC2 = 기본 매핑 + offset·clip((TC1−25)/275, 0, 1).
[PLC] PID 는 Kp 3·Ti 300 s 위치형 PI. 래더 TC2 경로(M0004A ∧ TC2 정상 ∧ RUN ∧ D00035>0 → M0004B)와 TC1 경로를 흉내 낸다.
수치(오버슈트·도달 시간·정착 p-p·SV2 하한)는 v1 시험과 같은 기준으로 본다 — 같은 플랜트·같은 제어 계산이면 같은 값이 나온다.
결과표는 `pytest -s v2/tests/test_heater_cascade.py` 로 볼 수 있다.

v2 에 더한 것: HeaterEngine 연결(수동·목표 바꾸기·공정 도달 대기·유지 모드 대기·TC1 탈락 조치·히터 레시피), 시험대(ChkRig)
PLC 쓰기 순서(v1 test_plc_comm T120 · 골든 28a 의 v2 형식), 학습 파일 가져오기 도구.
"""
import json
import math
from collections import deque

import pytest

from v2.common.paramdef import defaults
from v2.chk import heater_cascade as HC
from v2.chk import params as P
from v2.chk.heater_cascade import ALARM_HOLD_SEC, ENGAGE_TIMEOUT_SEC, FfCurve, HeaterCascade, SettleTracker, Tc2Map

pytestmark = pytest.mark.cascade

MAP = [[25, 25], [300, 445], [400, 570], [500, 695], [600, 825]]       # v1 시험 매핑(예전 기본값)
OT2 = 1150.0
SV2_MAX = OT2 - 50.0          # D00036 = D00034 − 500
PID_SPAN = 1200.0 / 0.29      # Kp 3 를 정규화하는 폭(°C)
HOLDBACK = 10.0

RESULTS = []


def _cfg(**kw):
    c = dict(defaults(P.PARAMS))
    c.update(HEATER_TC2_MAP=HC.map_text(MAP), HEATER_CASCADE_TI_SEC=600.0, HEATER_CASCADE_LAG_SEC=90.0,
             HEATER_CASCADE_BIAS_RATE_MAX=3.0, HEATER_CASCADE_BIAS_LIMIT_C=150.0, HEATER_CASCADE_BIAS_ALARM_C=100.0,
             HEATER_CASCADE_ALARM_ACTION="abort", HEATER_HOLDBACK_C=HOLDBACK, HEATER_HOLD_TC2_MARGIN_C=50.0,
             HEATER_MV_LIMIT=1200, HEATER_MV_MIN=400, HEATER_SOAK_TOLERANCE=2.0, HEATER_SOAK_TIME_SEC=60,
             HEATER_APPROACH_ZONE_C=20.0, HEATER_APPROACH_MIN_RATE_C_PER_MIN=1.0)
    c.update(kw)
    return c


class Port:
    """HeaterPort 대신 — 쓰기를 순서대로 남기고 받은 쪽(w)에 넘긴다."""

    def __init__(self, w):
        self.w = w

    def target(self, c):
        self.w(12, round(float(c), 1))

    def sv2(self, c, urgent=False):
        self.w(35, float(c))

    def pv_sel(self, on, urgent=False):
        self.w("M4A", bool(on))

    def mv_limit(self, v):
        self.w(18, int(v))

    def ramp(self, counts):
        self.w(20, int(counts))


# ───────────────────────── 플랜트 / PLC(v1 그대로) ─────────────────────────
def _interp(pts, x):
    for i in range(len(pts) - 1):
        if x <= pts[i + 1][0]:
            (x0, y0), (x1, y1) = pts[i], pts[i + 1]
            break
    else:
        (x0, y0), (x1, y1) = pts[-2], pts[-1]
    return y0 + (y1 - y0) / (x1 - x0) * (x - x0)


class Plant:
    A = 0.01433
    B = 2.32e-11
    PMAX = 230.0
    C = 2.0
    D2 = 20
    T2 = 15.0
    D1 = 30
    T1 = 35.0

    def __init__(self, offset=0.0, t0=25.0):
        self.offset = offset
        self.h = self.tc2 = self.tc1 = t0
        self.q2 = deque([t0] * (self.D2 + 1))
        self.q1 = deque([t0] * (self.D1 + 1))

    def f_true(self, t1):
        return _interp(MAP, t1) + self.offset * min(1.0, max(0.0, (t1 - 25.0) / 275.0))

    def finv(self, t2):
        lo, hi = -100.0, 1500.0
        for _ in range(40):
            m = (lo + hi) / 2
            if self.f_true(m) < t2:
                lo = m
            else:
                hi = m
        return (lo + hi) / 2

    def loss(self, t):
        return self.A * (t - 25) + self.B * ((t + 273) ** 4 - 298.0 ** 4)

    @staticmethod
    def frac(mv):
        a = math.pi * min(1.0, max(0.0, (1600.0 - mv) / 1200.0))
        return (math.pi - a + math.sin(2 * a) / 2) / math.pi

    def step(self, mv):
        p = self.PMAX * self.frac(mv) if mv > 400 else 0.0
        self.h += (p - self.loss(self.h)) / self.C
        self.q2.append(self.h)
        self.tc2 += (self.q2.popleft() - self.tc2) / (self.T2 + 1.0)
        self.q1.append(self.finv(self.tc2))
        self.tc1 += (self.q1.popleft() - self.tc1) / (self.T1 + 1.0)


class PID:
    def __init__(self, kp=3.0, ti=300.0, span=PID_SPAN):
        self.kp, self.ti, self.span = kp, ti, span
        self.i = 0.0

    def step(self, sv, pv, mv_limit):
        umax = (mv_limit - 400) / 12.0
        e = (sv - pv) / self.span * 100.0
        p = self.kp * e
        i_new = self.i + self.kp * e / self.ti
        u = p + i_new
        if 0.0 <= u <= umax or (u > umax and e < 0) or (u < 0 and e > 0):
            self.i = i_new
        return 400 + min(umax, max(0.0, p + self.i)) * 12.0


class Sim:
    """1 s 스텝. 캐스케이드 tick 도 1 s(실기는 250 ms 폴링이지만 계산·쓰기는 1 s 주기다)."""

    def __init__(self, tmp_path, offset=0.0, learned=None, **cfg):
        self.t = 0.0
        self.pl = Plant(offset=offset)
        self.pid = PID()
        self.D = {12: 25.0, 35: 0.0, 18: 1200, 19: 25.0, 20: 0}
        self.M4A = self.M4B = self.run = self.shutter = False
        self.tc1_off = 0.0
        self.tc1_stuck = None
        self.tc2_bad = False
        self.mv = 400.0
        self.writes = []
        self.msgs, self.alerts = [], []
        self.c = HeaterCascade(_cfg(**cfg), Port(self._w), lambda l, m: self.msgs.append((self.t, l, m)),
                               lambda k, d: self.alerts.append((self.t, k, d)), lambda: self.t,
                               learned_path=learned or (tmp_path / "learned.json"))
        self.rec = []

    def _w(self, k, v):
        self.writes.append((self.t, k, v))
        if k == "M4A":
            self.M4A = bool(v)
        else:
            self.D[k] = v

    def tc1_meas(self):
        if self.tc1_stuck is not None:
            return self.tc1_stuck
        return self.pl.tc1 + self.tc1_off

    def st(self):
        return dict(run=self.run, fault=False, pv=round(self.tc1_meas(), 1),
                    pv2=None if self.tc2_bad else round(self.pl.tc2, 1),
                    sv=self.D[12], sv2=self.D[35], sv2_max=SV2_MAX, ot2_limit=OT2,
                    pv_sel_eff=self.M4B, mv=int(self.mv), mv_limit=self.D[18], shutter_open=self.shutter)

    def step(self):
        prev = self.M4B
        if not self.run:
            self.M4A = False
        self.M4B = self.M4A and not self.tc2_bad and self.run and self.D[35] > 0
        tc1m = self.tc1_meas()
        if self.M4B:
            sv, pvc = min(self.D[35], SV2_MAX), self.pl.tc2
            self.D[19] = sv
        else:
            if prev:
                self.D[19] = tc1m
            tgt = self.D[12]
            if self.D[19] < tgt and (self.D[19] - tc1m) <= HOLDBACK:
                self.D[19] = min(tgt, self.D[19] + 0.3)
            elif self.D[19] > tgt:
                self.D[19] = tgt
            sv, pvc = self.D[19], tc1m
        if self.run:
            self.mv = self.pid.step(sv, pvc, self.D[18])
        else:
            self.mv = 400.0
            self.pid.i = 0.0
        self.pl.step(self.mv)
        self.t += 1.0
        self.c.tick(self.st())
        self.rec.append((self.t, self.pl.tc1, self.pl.tc2, self.mv, self.c.r1, self.c.bias, self.D[35], self.c.state))

    def runfor(self, sec):
        for _ in range(int(sec)):
            self.step()

    def run_until(self, cond, max_sec):
        for _ in range(int(max_sec)):
            self.step()
            if cond():
                return True
        return False


def _metrics(rec, target, t_from=0.0, tail_sec=1200):
    seg = [r for r in rec if r[0] >= t_from]
    tc1 = [r[1] for r in seg]
    tail = [r[1] for r in seg if r[0] >= seg[-1][0] - tail_sec]
    arr, since = None, None
    for r in seg:
        if abs(r[1] - target) <= 2.0:
            since = since if since is not None else r[0]
            if r[0] - since >= 60:
                arr = since - t_from
                break
        else:
            since = None
    return {"over": max(tc1) - target, "pp": max(tail) - min(tail),
            "err": abs(sum(tail) / len(tail) - target), "arrive_min": None if arr is None else arr / 60.0}


def _ramp_run(tmp_path, target, offset, rate=6.0, tail_min=150, **kw):
    s = Sim(tmp_path, offset=offset, **kw)
    s.c.start(target, rate, "sim")
    s.run = True
    s.runfor((target - 25.0) / rate * 60 + tail_min * 60)
    return s, _metrics(s.rec, target)


def _row(case, m):
    RESULTS.append((case, m["over"], m["pp"], m["err"], m["arrive_min"]))


# ───────────────────────── 시뮬레이터 타당성 ─────────────────────────
def _direct_loop(target_tc1, use_tc2, minutes=90):
    pl, pid = Plant(), PID()
    h = pl.f_true(target_tc1)
    pl.h = pl.tc2 = h
    pl.tc1 = float(target_tc1)
    pl.q2 = deque([h] * len(pl.q2))
    pl.q1 = deque([float(target_tc1)] * len(pl.q1))
    lo, hi = 400.0, 1600.0
    for _ in range(60):
        m = (lo + hi) / 2
        lo, hi = (m, hi) if pl.PMAX * pl.frac(m) < pl.loss(h) else (lo, m)
    pid.i = (lo - 400.0) / 12.0
    sv = (h if use_tc2 else float(target_tc1)) + 5.0
    xs = []
    for _ in range(int(minutes * 60)):
        pl.step(pid.step(sv, pl.tc2 if use_tc2 else pl.tc1, 1200))
        xs.append(pl.tc2 if use_tc2 else pl.tc1)
    tail = xs[-2400:]
    mu = sum(tail) / len(tail)
    zc = [i for i in range(1, len(tail)) if (tail[i - 1] - mu) * (tail[i] - mu) < 0]
    period = 2.0 * (zc[-1] - zc[0]) / (len(zc) - 1) if len(zc) > 2 else 0.0
    return max(tail) - min(tail), period


def test_sim_reproduces_tc1_direct_oscillation():
    pp300, per300 = _direct_loop(300, False)
    pp100, _ = _direct_loop(100, False)
    pp600, _ = _direct_loop(600, False)
    tc2 = {t: _direct_loop(t, True)[0] for t in (100, 300, 600, 700)}
    RESULTS.append((f"[비교] TC1 직접 제어 300°C (주기 {per300:.0f}s)", None, pp300, None, None))
    assert pp300 >= 8.0 and 200.0 <= per300 <= 350.0
    assert pp100 <= 1.0 and pp600 <= 1.0
    assert all(pp <= 0.5 for pp in tc2.values()), tc2


# ───────────────────────── a) 목표 온도별 ─────────────────────────
@pytest.mark.parametrize("offset", [0.0, 25.0])
@pytest.mark.parametrize("target", [100, 300, 500, 600, 700])
def test_a_targets_same_params(tmp_path, target, offset):
    s, m = _ramp_run(tmp_path, target, offset)
    _row(f"a) 25→{target} map{offset:+.0f}", m)
    assert s.c.state == "running"
    assert m["err"] <= 1.0 and m["pp"] <= 2.0 and m["over"] <= 5.0, m


@pytest.mark.parametrize("target", [100, 300, 500, 600, 700])
def test_a_targets_map_minus25_first_run(tmp_path, target):
    s, m = _ramp_run(tmp_path, target, -25.0)
    _row(f"a) 25→{target} map-25 (첫 운전)", m)
    assert m["err"] <= 1.0 and m["pp"] <= 2.0 and m["over"] <= 8.0, m


@pytest.mark.parametrize("target", [300, 600])
def test_a_learned_rerun(tmp_path, target):
    learned = tmp_path / "learned.json"
    s1, m1 = _ramp_run(tmp_path, target, -25.0, learned=learned)
    assert learned.exists() and any("매핑 학습" in m for _, _, m in s1.msgs)
    s2, m2 = _ramp_run(tmp_path, target, -25.0, learned=learned)
    _row(f"a) 25→{target} map-25 (학습 후 냉간 재운전)", m2)
    assert "학습 보정: [TC1" in s2.c.bias_source and "상온 0" in s2.c.bias_source
    assert min(r[6] for r in s2.rec if r[7] == "running") > 1.0          # SV2 가 하한 1.0 에 붙지 않는다
    assert m2["err"] <= 1.0 and m2["pp"] <= 2.0 and m2["over"] <= m1["over"] - 1.0, (m1, m2)
    assert m2["arrive_min"] <= m1["arrive_min"] + 8.0, (m1, m2)


@pytest.mark.parametrize("offset", [-40.0, 40.0])
@pytest.mark.parametrize("target", [300, 600])
def test_b_map_off_40_converges(tmp_path, target, offset):
    s, m = _ramp_run(tmp_path, target, offset, tail_min=200)
    _row(f"b) 25→{target} map{offset:+.0f}", m)
    assert m["err"] <= 1.0 and m["pp"] <= 2.0, m


# ───────────────────────── c) 셔터 ─────────────────────────
def test_c_shutter_freezes_bias_and_sv2(tmp_path):
    s, _ = _ramp_run(tmp_path, 600, 25.0, tail_min=120)
    b0, sv2_0 = s.c.bias, s.D[35]
    s.shutter = True
    s.tc1_off = -100.0
    t_open = s.t
    s.runfor(30 * 60)
    seg = [r for r in s.rec if r[0] > t_open]
    assert s.c.state == "frozen"
    assert all(abs(r[5] - b0) < 1e-9 for r in seg)
    assert all(abs(r[6] - sv2_0) < 1e-9 for r in seg)
    assert max(r[2] for r in seg) - min(r[2] for r in seg) < 1.0
    s.shutter = False
    s.tc1_off = 0.0
    t_close = s.t
    s.runfor(60 * 60)
    m = _metrics(s.rec, 600, t_from=t_close)
    _row("c) 600 셔터 30분 열림 → 닫힘 후 60분", m)
    assert s.c.state == "running"
    assert abs(s.c.bias - b0) < 2.0 and m["err"] <= 1.0 and m["pp"] <= 2.0


def test_c_shutter_holds_rising_target_until_closed(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    s.shutter = True
    s.tc1_off = -100.0
    s.runfor(10)
    sv2 = s.D[35]
    s.c.start(400, 6.0)
    s.runfor(300)
    assert s.c.r1 == pytest.approx(300.0) and s.D[35] == sv2
    s.c.start(250, 6.0)
    s.runfor(5)
    assert s.c.r1 == pytest.approx(250.0) and s.D[35] < sv2


# ───────────────────────── d) TC1 탈락 ─────────────────────────
def test_d_tc1_detached_alarm(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    s.tc1_stuck = 240.0
    t0 = s.t
    assert s.run_until(lambda: any(k == "tc1_detached" for _, k, _ in s.alerts), 3 * 3600)
    t_alarm, _, d = [a for a in s.alerts if a[1] == "tc1_detached"][0]
    t_cross = next(r[0] for r in s.rec if r[0] > t0 and abs(r[5]) >= 100.0)
    RESULTS.append(("d) TC1 탈락(TC1 300→240 고정)", None, None, None, (t_alarm - t0) / 60.0))
    assert t_alarm - t_cross == pytest.approx(ALARM_HOLD_SEC, abs=2.0)
    assert d["action"] == "abort" and "TC1 이 시료대에서 떨어졌거나" in d["text"] and "예상 TC2" in d["text"]
    assert abs(s.c.bias) <= 150.0 + 1e-9


# ───────────────────────── e) TC2 이상 ─────────────────────────
def test_e_tc2_lost_shutter_closed_falls_back_to_tc1(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    n = len(s.writes)
    s.tc2_bad = True
    s.runfor(3)
    w = [(k, v) for _, k, v in s.writes[n:]]
    assert w[:2] == [("M4A", False), (35, 0.0)], "해제 순서: M0004A OFF → D00035=0"
    assert s.c.state == "tc1" and any(k == "tc2_lost" for _, k, _ in s.alerts)
    t0 = s.t
    s.runfor(30 * 60)
    assert s.run and s.D[12] == pytest.approx(300.0)
    tc1 = [r[1] for r in s.rec if r[0] > t0]
    assert min(tc1) > 280.0 and max(tc1) < 320.0


def test_e_tc2_lost_shutter_open_fixes_dac(tmp_path):
    s, _ = _ramp_run(tmp_path, 600, 0.0, tail_min=90)
    s.shutter = True
    s.tc1_off = -100.0
    s.runfor(60)
    mv_before = int(s.mv)
    n = len(s.writes)
    s.tc2_bad = True
    s.runfor(3)
    w = [(k, v) for _, k, v in s.writes[n:]]
    assert w[:2] == [("M4A", False), (35, 0.0)]
    assert s.c.state == "dac" and s.c.kind == "dac"
    assert abs(s.D[18] - mv_before) <= 2 and any(k == "demoted" for _, k, _ in s.alerts)
    assert s.c.sat_hold() == (True, "dac")
    s.runfor(600)
    assert s.D[18] == s.c.dac_value
    s.shutter = False
    s.tc1_off = 0.0
    s.runfor(3)
    assert s.D[18] == 1200 and s.c.state == "tc1"


def test_e_tc2_recovers_reengages(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    s.tc2_bad = True
    s.runfor(5)
    assert s.c.state == "tc1"
    s.tc2_bad = False
    assert s.run_until(lambda: s.c.state == "running", 200)
    assert any("TC2 제어 재시도" in m for _, _, m in s.msgs)


def test_e_reengage_limit(tmp_path):
    """재진입 시도는 한 운전에 REENGAGE_MAX 번까지(래더가 계속 거부하면 M0004A 를 흔들지 않는다)."""
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st(sv2_max=0.0))
    assert c.state == "tc1"
    for _ in range(HC.REENGAGE_MAX + 2):
        for _ in range(int(HC.REENGAGE_RETRY_SEC + HC.REENGAGE_OK_SEC) + 2):
            t[0] += 1.0
            c.tick(_st())
        t[0] += HC.ENGAGE_TIMEOUT_SEC + 1.0
        c.tick(_st(sv2=0.0))                               # D00035 되읽기가 끝내 안 맞는다
    assert c._reengage_n == HC.REENGAGE_MAX and c.state == "tc1"


# ───────────────────────── f) HOLD / 하강 / 운전 중 목표 변경 ─────────────────────────
def test_f_hold_resume_and_retarget(tmp_path):
    s = Sim(tmp_path, offset=0.0)
    s.c.start(300, 6.0, "레시피")
    s.run = True
    assert s.run_until(lambda: (s.c.r1 or 0) >= 150.0, 3 * 3600)
    s.c.hold()
    r_hold = s.c.r1
    s.runfor(10 * 60)
    assert s.c.r1 == pytest.approx(r_hold)
    s.c.resume()
    s.runfor(5)
    assert s.c.r1 > r_hold and s.c.r1 - r_hold < 1.0
    assert s.run_until(lambda: (s.c.r1 or 0) >= 250.0, 3 * 3600)
    s.c.start(400, 6.0)
    assert s.c.state == "running" and s.c.target() == 400.0
    t_up = s.t
    s.runfor(150 * 60)
    m_up = _metrics(s.rec, 400, t_from=t_up)
    _row("f) 운전 중 목표 300→400", m_up)
    assert m_up["err"] <= 1.0 and m_up["pp"] <= 2.0 and m_up["over"] <= 5.0
    s.c.start(200, 6.0)
    assert s.c.r1 == pytest.approx(200.0)
    t_dn = s.t
    s.runfor(150 * 60)
    m_dn = _metrics(s.rec, 200, t_from=t_dn)
    assert m_dn["err"] <= 1.0 and m_dn["pp"] <= 2.0


# ───────────────────────── 핸드셰이크·해제 순서·폴백(플랜트 없이) ─────────────────────────
def _bare(tmp_path, **kw):
    t = [0.0]
    ev, msgs, alerts = [], [], []
    names = {12: "sv", 35: "sv2", "M4A": "sel", 18: "lim", 20: "ramp"}

    def w(k, v):
        if k != 20:
            ev.append((names[k], v))
    c = HeaterCascade(_cfg(), Port(w), lambda l, m: msgs.append((l, m)), lambda k, d: alerts.append((k, d)),
                      lambda: t[0], learned_path=kw.pop("learned", tmp_path / "l.json"))
    c.msgs, c.alerts = msgs, alerts
    return c, ev, t


def _st(**kw):
    d = dict(run=True, fault=False, pv=100.0, pv2=140.0, sv=100.0, sv2=0.0, sv2_max=SV2_MAX, ot2_limit=OT2,
             pv_sel_eff=False, mv=500, mv_limit=1200, shutter_open=False)
    d.update(kw)
    return d


def test_handshake_engage_and_release_order(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.tick(_st(run=False))
    c.start(300, 6.0)
    c.tick(_st(run=False))
    assert c.state == "armed"
    c.tick(_st())
    assert c.state == "engaging_sv2"
    sv2 = [v for k, v in ev if k == "sv2"][-1]
    assert sv2 == pytest.approx(round(Tc2Map(MAP).value(100.0), 1))
    c.tick(_st(sv2=sv2))
    assert c.state == "engaging_sel" and ev[-1] == ("sel", True)
    c.tick(_st(sv2=sv2, pv_sel_eff=True))
    assert c.state == "running" and c.engaged()
    ev.clear()
    c.stop("테스트")
    assert ev == [("sel", False), ("sv2", 0.0)] and c.state == "idle"


def test_engage_timeout_falls_back_to_tc1(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st())
    assert c.state == "engaging_sv2"
    t[0] = ENGAGE_TIMEOUT_SEC + 0.5
    c.tick(_st(sv2=0.0))
    assert c.state == "tc1" and ev[-2:] == [("sel", False), ("sv2", 0.0)]
    assert [k for k, _ in c.alerts] == ["tc2_lost"]


def test_ladder_without_tc2_support_uses_tc1(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st(sv2_max=0.0))
    assert c.state == "tc1" and not any(k == "sel" and v for k, v in ev)


def test_run_off_releases(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st())
    sv2 = ev[-1][1]
    c.tick(_st(sv2=sv2))
    c.tick(_st(sv2=sv2, pv_sel_eff=True))
    ev.clear()
    c.tick(_st(run=False, sv2=sv2))
    assert ev == [("sel", False), ("sv2", 0.0)] and c.state == "idle"


def test_start_writes_d12_from_pv_before_run(tmp_path):
    """RUN 을 켜기 전에 D00012 를 출발점(지금 TC1)으로 내려 둔다(TC1 폴백 경로가 옛 목표로 달리지 않게)."""
    c, ev, t = _bare(tmp_path)
    c.tick(_st(run=False, pv=123.4))
    c.start(300, 6.0)
    assert ev == [("sv", 123.4)]


def _learned(p, *pts):
    p.write_text(json.dumps({"points": [{"tc1": a, "tc2": b, "t": f"2026-10-29 0{i}:00:00"}
                                        for i, (a, b) in enumerate(pts)]}), encoding="utf-8")


def test_ff_curve_points_0_1_2():
    f0 = FfCurve(25.0)
    assert [f0.value(t) for t in (10, 25, 300, 900)] == [0.0, 0.0, 0.0, 0.0] and f0.value(None) == 0.0
    f1 = FfCurve(25.0, [(600.0, 15.0)])
    assert f1.value(25) == 0.0 and f1.value(0) == 0.0
    assert f1.value(312.5) == pytest.approx(7.5) and f1.value(600) == pytest.approx(15.0)
    assert f1.value(700) == pytest.approx(15.0) and f1.value(1000) == pytest.approx(15.0)
    f2 = FfCurve(25.0, [(600.0, 15.0), (300.0, -10.0)])
    assert f2.value(25) == 0.0
    assert f2.value(162.5) == pytest.approx(-5.0)
    assert f2.value(450) == pytest.approx(2.5)
    assert f2.value(800) == pytest.approx(15.0)
    assert FfCurve(25.0, [(20.0, 9.0)]).points == [(25.0, 0.0)]


def test_ff_from_learned_file_and_excessive_point_ignored(tmp_path):
    p = tmp_path / "learned.json"
    c, _, _ = _bare(tmp_path, learned=p)
    c.start(600, 6.0)
    assert c.bias == 0.0 and c.bias_source.startswith("학습점 없음 → ff 0")
    c.stop()
    _learned(p, (300.0, 455.0), (600.0, 840.0), (450.0, 700.0))
    c.start(620, 6.0)
    assert c._ff.points == [(25.0, 0.0), (300.0, pytest.approx(10.0)), (450.0, pytest.approx(67.5)),
                            (600.0, pytest.approx(15.0))]
    assert "목표 620 에서 ff +15.0" in c.bias_source and "상온 0" in c.bias_source and "적분 보정 0" in c.bias_source
    c.stop()
    _learned(p, (300.0, 455.0), (600.0, 840.0), (450.0, 750.0))
    c.msgs.clear()
    c.start(600, 6.0)
    assert [x for x, _ in c._ff.points] == [25.0, 300.0, 600.0]
    assert any(lv == "warn" and "학습점 무시(보정 과대)" in m and "TC1 450" in m for lv, m in c.msgs)
    c._r1 = 25.0
    assert c.bias_ff == 0.0 and c.bias_int == 0.0 and c.bias == 0.0
    c._r1 = 450.0
    c._b_int = -2.0
    assert c.bias_ff == pytest.approx(12.5) and c.bias == pytest.approx(10.5)
    snap = c.snapshot()
    assert snap["bias"] == pytest.approx(10.5) and snap["bias_ff"] == pytest.approx(12.5)
    assert snap["bias_int"] == pytest.approx(-2.0)


def test_learn_blocked_after_alarm_or_excess_bias(tmp_path):
    learned = tmp_path / "learned.json"
    s = Sim(tmp_path, offset=0.0, learned=learned)
    s.c.start(300, 6.0, "sim")
    s.run = True
    s.runfor(60)
    s.c._alarm_sent = True
    s.runfor((300 - 25) / 6.0 * 60 + 60 * 60)
    assert not learned.exists()
    skip = [m for _, _, m in s.msgs if "매핑 학습 안 함" in m]
    assert len(skip) == 1 and "TC1 탈락 경보" in skip[0]
    c, _, _ = _bare(tmp_path)
    c.start(300, 6.0)
    c._r1 = 300.0
    assert c._learn_block_reason() is None
    c._b_int = 99.9
    assert c._learn_block_reason() is None
    c._b_int = 100.0
    assert "보정 과대" in c._learn_block_reason()
    c._b_int = -100.0
    assert "보정 과대" in c._learn_block_reason()


def test_learn_without_path_keeps_nothing(tmp_path):
    """학습 파일 자리가 없으면(조립 없는 단독 사용) 읽지도 쓰지도 않는다."""
    c = HeaterCascade(_cfg(), Port(lambda k, v: None), lambda l, m: None, lambda k, d: None, lambda: 0.0)
    c.start(300, 6.0)
    c._learn(300.0, 445.0)
    assert c.bias_source.startswith("학습점 없음") and not list(tmp_path.iterdir())


def test_map_interp_and_extrapolation():
    m = Tc2Map(MAP)
    assert m.value(300) == pytest.approx(445.0)
    assert m.value(350) == pytest.approx(507.5)
    assert m.value(700) == pytest.approx(955.0)
    assert m.slope(10) == pytest.approx(420.0 / 275.0)


def test_map_param_validation_and_default_is_v1_b0a945a():
    d = defaults(P.PARAMS)
    assert HC.parse_map(d["HEATER_TC2_MAP"]) == [(25, 25), (50, 92), (100, 170), (150, 226), (200, 290), (250, 365),
                                                 (300, 437), (400, 569), (500, 705), (600, 842)]
    assert HC.map_problem(d["HEATER_TC2_MAP"]) is None
    assert "2개 이상" in HC.map_problem("25:25")
    assert "증가" in HC.map_problem("25:25,300:445,200:500")
    assert "증가" in HC.map_problem("25:25,300:445,400:440")
    from v2.common.paramdef import check_config
    errs = check_config(P.PARAMS, P.CROSS_RULES, dict(d, HEATER_TC2_MAP="25:25,300:445,200:500"))
    assert any(e.startswith("[X26]") for e in errs)
    assert d["HEATER_CTRL_MODE"] == "cascade" and d["HEATER_SHUTTER_SETTLE_MAX_SEC"] == 900.0


def test_snapshot_fields_follow_v1_csv_columns(tmp_path):
    """v1 히터 CSV 캐스케이드 열(r1_c·r1_lag_c·tc1_ema_c·sv2_map_c·bias_c·casc_state·bias_ff_c·bias_int_c)이
    스냅샷 heater 블록 열로 — 꺼져 있으면 값 없음."""
    c, ev, t = _bare(tmp_path)
    f = c.fields()
    assert f["heater.casc_state"] == "idle" and f["heater.bias"] is None and f["heater.r1"] is None
    c.start(300, 6.0)
    c.tick(_st())
    sv2 = ev[-1][1]
    c.tick(_st(sv2=sv2))
    c.tick(_st(sv2=sv2, pv_sel_eff=True))
    for _ in range(3):
        t[0] += 1.0
        c.tick(_st(sv2=c._sv2_cmd, pv_sel_eff=True))
    f = c.fields()
    assert f["heater.casc_state"] == "running" and f["heater.r1"] > 100.0 and f["heater.sv2_map"] is not None
    assert f["heater.bias"] == pytest.approx(f["heater.bias_ff"] + f["heater.bias_int"])
    from v2.chk import snapshot_fields as SF
    cols = {f"heater.{x.name}" for x in SF.HEATER_BLOCK.fields}
    assert set(f) <= cols
    lab = dict(next(x for x in SF.HEATER_BLOCK.fields if x.name == "casc_state").enum_labels)
    assert lab == HC.STATE_TEXT


# ───────────────────────── g) #8-3 재현: 가스 도입 뒤 TC1 하락 → 셔터 전 정착 확인 ─────────────────────────
def test_g_83_gas_drop_then_shutter_settle(tmp_path):
    s = Sim(tmp_path, offset=0.0)
    s.c.start(600, 6.0, "sim")
    s.run = True
    s.runfor((600 - 25) / 6.0 * 60 + 90 * 60)
    assert abs(s.pl.tc1 - 600.0) < 0.1 and abs(s.pl.tc2 - s.D[35]) < 0.1
    tr = SettleTracker(600.0, 2.0, 60.0, clock=lambda: s.t)

    def feed():
        tr.feed(s.st()["pv"], final_ok=s.c.r1_done(600))
    for _ in range(60):
        s.step()
        feed()
    s.pl.offset += 4.0 * Tc2Map(MAP).slope(600.0)
    t_gas = s.t
    for _ in range(420):
        s.step()
        feed()
    t_enter, pv_enter = s.t, s.st()["pv"]
    opened = tr.ok() or s.run_until(lambda: (feed() or tr.ok()), 900)
    t_open, pv_open = s.t, s.st()["pv"]
    seg = [r for r in s.rec if r[0] > t_gas]
    t_min, tc1_min = min(((r[0], r[1]) for r in seg), key=lambda x: x[1])
    back = next(r[0] for r in seg if r[0] > t_min and abs(round(r[1], 1) - 600.0) <= 2.0)
    RESULTS.append(("g) #8-3 가스 −4°C: 셔터 진입 편차→열 때 편차", pv_enter - 600.0, None, abs(pv_open - 600.0),
                    (t_open - t_enter) / 60.0))
    assert opened and abs(pv_open - 600.0) <= 2.0
    assert pv_enter < 598.0
    assert t_open - back >= 60.0 - 1.0
    assert t_open - t_enter < 900.0


def test_settle_tracker_rules():
    t = [0.0]
    tr = SettleTracker(600.0, 2.0, 60.0, clock=lambda: t[0])
    tr.feed(599.0)
    t[0] = 59.0
    tr.feed(601.9)
    assert not tr.ok()
    t[0] = 60.0
    tr.feed(600.5)
    assert tr.ok() and tr.held_sec() == 60.0
    tr.feed(None)                                     # TC1 None(센서 이상) — 연속이 끊긴다
    assert not tr.ok() and tr.held_sec() == 0.0
    tr.feed(600.0, final_ok=False)                    # 캐스케이드 최종 목표가 공정 목표와 다르면 끊긴다
    assert tr.since is None


def test_zz_print_summary_table():
    def f(v, fmt="{:+.2f}"):
        return "-" if v is None else fmt.format(v)
    lines = [f"{'시나리오':<40} {'오버슈트':>9} {'정착 p-p':>9} {'최종 오차':>9} {'도달(분)':>9}"]
    for case, over, pp, err, arr in RESULTS:
        lines.append(f"{case:<40} {f(over):>9} {f(pp, '{:.2f}'):>9} {f(err, '{:.2f}'):>9} {f(arr, '{:.1f}'):>9}")
    print("\n" + "\n".join(lines))


# ───────────────────────── 히터 레시피 러너 연결 ─────────────────────────
def test_recipe_runner_uses_cascade():
    """러너는 캐스케이드에 start/hold/resume/stop 만 부르고(tick 은 엔진의 PLC 폴링), RAMP 끝은 r1 도달로 본다.
    종료 시 캐스케이드 해제가 RUN OFF 보다 먼저다. 운전 중 D00012 는 캐스케이드가 쥔다(러너가 쓰지 않음)."""
    from v2.chk.heater_engine import RampProfiler, RecipeRunner, RecipeStep
    order = []

    class FakePort:
        def target(self, c):
            order.append(("D00012", c))

        def ramp(self, n):
            pass

        def run(self, on):
            order.append(("RUN", on))

    class FakeCasc:
        def __init__(self):
            self.r1, self.final, self.active, self.held = None, None, False, False

        def start(self, t, rate, label=""):
            order.append(("start", t, rate))
            self.final, self.active = float(t), True
            self.r1 = 100.0 if self.r1 is None else min(self.r1, float(t))

        def hold(self):
            order.append(("hold",))
            self.held = True

        def resume(self):
            if self.held:
                order.append(("resume",))
            self.held = False

        def stop(self, why=""):
            order.append(("stop", why))
            self.active = False

        def is_active(self):
            return self.active

        def target(self):
            return self.final if self.active else None

        def r1_done(self, target=None):
            return self.active and self.r1 == self.final and (target is None or target == self.final)

    cfg = _cfg()
    t = [0.0]
    st = dict(run=True, itl=True, fault=False, pv=100.0, sv=100.0, sv_ramp=100.0)
    port, fc = FakePort(), FakeCasc()
    ramp = RampProfiler(cfg, port, lambda: st, lambda l, m: None, lambda: t[0])
    r = RecipeRunner(cfg, port, ramp, lambda: st, lambda l, m: None, lambda e: None, lambda: t[0])
    r.casc_fn = lambda: fc
    r.load("rc", [RecipeStep(1, 300.0, 6.0, 0.0), RecipeStep(2, 200.0, 6.0, 0.0)])
    assert r.start()
    assert ("start", 300.0, 6.0) in order and ("RUN", True) in order
    assert not any(k == "D00012" for k, *_ in order)
    r.hold()
    r.resume()
    assert ("hold",) in order and ("resume",) in order
    r.on_status(dict(st))
    assert r.phase == "RAMP"                                   # r1 이 아직 목표 전 — SOAK 로 넘어가지 않는다
    fc.r1 = 300.0
    r.on_status(dict(st))                                      # r1 도달 → SOAK(0분)
    r.tick(st)                                                 # → 스텝 2
    assert ("start", 200.0, 6.0) in order
    order.clear()
    r.stop("테스트")
    assert [o[0] for o in order][:2] == ["stop", "RUN"] and order[1] == ("RUN", False)


# ───────────────────────── 학습 파일 가져오기 ─────────────────────────
def test_import_learned_reads_v1_file_only(tmp_path):
    src = tmp_path / "v1" / "heater_tc2_map_learned.json"
    src.parent.mkdir()
    _learned(src, (600.0, 840.0), (300.0, 450.0))
    before = src.read_bytes()
    dst = tmp_path / "data" / HC.LEARNED_FILE
    assert HC.import_learned(src, dst) == 2
    assert src.read_bytes() == before                          # 읽기만
    pts = json.loads(dst.read_text(encoding="utf-8"))["points"]
    assert [p["tc1"] for p in pts] == [300.0, 600.0]
    with pytest.raises(FileExistsError):
        HC.import_learned(src, dst)                            # 이미 있으면 덮지 않는다
    assert HC.import_learned(src, dst, force=True) == 2
    from v2.tools import import_heater_map as T
    out = []
    assert T.main([str(src), "--data-dir", str(tmp_path / "data2")], print_fn=out.append) == 0
    assert (tmp_path / "data2" / HC.LEARNED_FILE).exists() and any("2점" in s for s in out)
    assert T.main([str(src), "--data-dir", str(tmp_path / "data2")], print_fn=out.append) == 1
