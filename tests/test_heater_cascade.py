# -*- coding: utf-8 -*-
"""controller/heater_cascade — 2단(캐스케이드) 제어를 단순 열 모델 시뮬레이터로 검증한다.

[플랜트] MV(DAC 카운트) → 히터 열평형 H → TC2(순수지연 20 s + 1차 15 s) → TC1(순수지연 30 s + 1차 35 s).
  · 출력: VARITAP 위상제어 실효 전력 (π − α + sin2α/2)/π, α = π·(1600 − MV)/1200 (lib.config.heater_est_current 와 같은 식).
  · 열손실: 선형 + 복사 항 a(T−25) + b((T+273)⁴ − 298⁴) — 고온일수록 손실이 커져 둔해진다.
  · 정상 상태 TC2 = f_true(TC1) = 기본 매핑 + offset·clip((TC1−25)/275, 0, 1) (상온에서는 어긋남 0).
[PLC] PID 는 Kp 3·Ti 300 s 위치형 PI(정규화 폭 PID_SPAN). 래더 TC2 경로(M0004A ∧ TC2 정상 ∧ RUN ∧ D00035>0 → M0004B,
  SV = min(D00035, D00036))와 TC1 경로(D00019 가 D00012 를 향해 래더 램프 + 홀드백, M0004B 하강 엣지에서 D00019 ← TC1)를 흉내 낸다.
[보정] PID_SPAN·열용량은 실측 세 가지를 재현하도록 맞췄다: TC1 직접 제어가 300°C 에서 지속 진동(주기 약 270 s, 실측 260 s),
  600°C 에서 안정, TC2 루프는 전 온도에서 안정 — test_sim_reproduces_tc1_direct_oscillation 이 매번 확인한다.

결과표는 `pytest -s tests/test_heater_cascade.py` 로 볼 수 있다.
"""
import math
from collections import deque

import pytest

from controller.heater_cascade import HeaterCascade, Tc2Map, ENGAGE_TIMEOUT_SEC, ALARM_HOLD_SEC

MAP = [[25, 25], [300, 445], [400, 570], [500, 695], [600, 825]]
OT2 = 1150.0
SV2_MAX = OT2 - 50.0          # D00036 = D00034 − 500
PID_SPAN = 1200.0 / 0.29      # Kp 3 를 정규화하는 폭(°C) — 위 [보정]
HOLDBACK = 10.0               # 장비 config_user.json 값

RESULTS = []                  # 요약표


# ───────────────────────── 플랜트 / PLC ─────────────────────────
def _interp(pts, x):
    for i in range(len(pts) - 1):
        if x <= pts[i + 1][0]:
            (x0, y0), (x1, y1) = pts[i], pts[i + 1]
            break
    else:
        (x0, y0), (x1, y1) = pts[-2], pts[-1]
    return y0 + (y1 - y0) / (x1 - x0) * (x - x0)


class Plant:
    A = 0.01433; B = 2.32e-11; PMAX = 230.0; C = 2.0
    D2 = 20; T2 = 15.0; D1 = 30; T1 = 35.0

    def __init__(self, offset=0.0, t0=25.0):
        self.offset = offset
        self.h = self.tc2 = self.tc1 = t0
        self.q2 = deque([t0] * (self.D2 + 1)); self.q1 = deque([t0] * (self.D1 + 1))

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
    """PLC PID 근사 — 위치형 PI, 출력 0~100% → DAC 400~1600, 상한 D00018, 적분 클램프."""
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
    """1 s 스텝. 캐스케이드 tick 도 1 s(실기는 200 ms 폴링이지만 계산·쓰기는 1 s 주기다)."""

    def __init__(self, tmp_path, offset=0.0, learned=None, **ck):
        self.t = 0.0
        self.pl = Plant(offset=offset)
        self.pid = PID()
        self.D = {12: 25.0, 35: 0.0, 18: 1200, 19: 25.0}
        self.M4A = self.M4B = self.run = self.shutter = False
        self.tc1_off = 0.0; self.tc1_stuck = None; self.tc2_bad = False
        self.mv = 400.0
        self.writes = []
        kw = dict(map_points=MAP, ti_sec=600, lag_sec=90, bias_rate_max=3.0, bias_limit_c=150.0,
                  bias_alarm_c=100.0, alarm_action="abort", holdback_c=HOLDBACK, tc2_margin_c=50.0,
                  mv_limit=1200, mv_min=400, soak_tol_c=2.0, soak_sec=60,
                  learned_path=str(learned or (tmp_path / "learned.json")))
        kw.update(ck)
        self.c = HeaterCascade(clock=lambda: self.t, **kw)
        self.c.request_target.connect(lambda v: self._w(12, v))
        self.c.request_sv2.connect(lambda v: self._w(35, v))
        self.c.request_pv_sel.connect(lambda b: self._w('M4A', b))
        self.c.request_mv_limit.connect(lambda v: self._w(18, v))
        self.msgs, self.alerts = [], []
        self.c.message.connect(lambda l, m: self.msgs.append((self.t, l, m)))
        self.c.alert.connect(lambda k, d: self.alerts.append((self.t, k, d)))
        self.rec = []

    def _w(self, k, v):
        self.writes.append((self.t, k, v))
        if k == 'M4A':
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
                self.D[19] = tc1m                       # M0004B 하강 엣지: D00019 ← TC1
            tgt = self.D[12]
            if self.D[19] < tgt and (self.D[19] - tc1m) <= HOLDBACK:
                self.D[19] = min(tgt, self.D[19] + 0.3)  # 래더 램프(18°C/min) + 홀드백
            elif self.D[19] > tgt:
                self.D[19] = tgt
            sv, pvc = self.D[19], tc1m
        if self.run:
            self.mv = self.pid.step(sv, pvc, self.D[18])
        else:
            self.mv = 400.0; self.pid.i = 0.0
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
                arr = since - t_from; break
        else:
            since = None
    return {"over": max(tc1) - target, "pp": max(tail) - min(tail),
            "err": abs(sum(tail) / len(tail) - target), "arrive_min": None if arr is None else arr / 60.0}


def _ramp_run(tmp_path, target, offset, rate=6.0, tail_min=150, **kw):
    s = Sim(tmp_path, offset=offset, **kw)
    s.c.start(target, rate, "sim"); s.run = True
    s.runfor((target - 25.0) / rate * 60 + tail_min * 60)
    return s, _metrics(s.rec, target)


def _row(case, m):
    RESULTS.append((case, m["over"], m["pp"], m["err"], m["arrive_min"]))


# ───────────────────────── 시뮬레이터 타당성 ─────────────────────────
def _direct_loop(target_tc1, use_tc2, minutes=90):
    """PID 한 루프만 돌린다 — 목표 근처 평형에서 출발해 SV 를 +5°C 계단으로 주고 마지막 40분의 흔들림을 잰다.
    use_tc2=False: 기존 TC1 직접 제어(PV=TC1) / True: TC2 루프(PV=TC2, SV=f_true(TC1 목표))."""
    pl, pid = Plant(), PID()
    h = pl.f_true(target_tc1)
    pl.h = pl.tc2 = h; pl.tc1 = float(target_tc1)
    pl.q2 = deque([h] * len(pl.q2)); pl.q1 = deque([float(target_tc1)] * len(pl.q1))
    lo, hi = 400.0, 1600.0
    for _ in range(60):                          # 평형 MV
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
    """시뮬레이터 타당성: 기존 TC1 직접 제어(Kp 3·Ti 300)가 300°C 에서 지속 진동(실측 288↔311°C, 주기 약 260 s)하고
    100·600°C 에서는 안정, TC2 루프는 전 온도에서 안정해야 한다(실측 유지 모드 TC2 표준편차 0.15~0.2°C)."""
    pp300, per300 = _direct_loop(300, False)
    pp100, _ = _direct_loop(100, False)
    pp600, _ = _direct_loop(600, False)
    tc2 = {t: _direct_loop(t, True)[0] for t in (100, 300, 600, 700)}
    RESULTS.append((f"[비교] TC1 직접 제어 300°C (주기 {per300:.0f}s)", None, pp300, None, None))
    RESULTS.append(("[비교] TC1 직접 제어 100°C", None, pp100, None, None))
    RESULTS.append(("[비교] TC1 직접 제어 600°C", None, pp600, None, None))
    for t, pp in tc2.items():
        RESULTS.append((f"[비교] TC2 루프 (TC1 {t}°C 상당)", None, pp, None, None))
    assert pp300 >= 8.0 and 200.0 <= per300 <= 350.0
    assert pp100 <= 1.0 and pp600 <= 1.0
    assert all(pp <= 0.5 for pp in tc2.values()), tc2


# ───────────────────────── a) 목표 온도별 — 같은 파라미터 ─────────────────────────
@pytest.mark.parametrize("offset", [0.0, 25.0])
@pytest.mark.parametrize("target", [100, 300, 500, 600, 700])
def test_a_targets_same_params(tmp_path, target, offset):
    s, m = _ramp_run(tmp_path, target, offset)
    _row(f"a) 25→{target} map{offset:+.0f}", m)
    assert s.c.state == 'running'
    assert m["err"] <= 1.0 and m["pp"] <= 2.0 and m["over"] <= 5.0, m


@pytest.mark.parametrize("target", [100, 300, 500, 600, 700])
def test_a_targets_map_minus25_first_run(tmp_path, target):
    """기본 매핑이 실제보다 25°C 높은(TC1 이 넘치는 쪽) 첫 운전 — 수렴은 같지만 오버슈트가 5°C 를 넘을 수 있다(최대 약 7°C).
    램프 중에는 열 지연과 매핑 오차를 구별할 수 없어 TI 600 s 로는 도착 전에 다 걷어 내지 못한다(보고서에 수치로 남긴다)."""
    s, m = _ramp_run(tmp_path, target, -25.0)
    _row(f"a) 25→{target} map-25 (첫 운전)", m)
    assert m["err"] <= 1.0 and m["pp"] <= 2.0 and m["over"] <= 8.0, m


@pytest.mark.parametrize("target", [300, 600])
def test_a_learned_rerun(tmp_path, target):
    """첫 운전이 학습 파일에 (목표, SV2) 를 남기고, 다음 운전은 그 값을 초기 bias 로 쓴다.
    냉간 시동에서는 학습 bias(−25)가 상온 구간 SV2 를 바닥(1.0)에 붙여 r1 이 홀드백에 걸리고, 그동안 bias 가 0 쪽으로
    되감겨 결과가 첫 운전과 거의 같다 — 도움이 되는 것은 같은 목표 근처에서 다시 켜는 경우다(표에 수치만 남긴다)."""
    learned = tmp_path / "learned.json"
    s1, m1 = _ramp_run(tmp_path, target, -25.0, learned=learned)
    assert learned.exists() and any("매핑 학습" in m for _, _, m in s1.msgs)
    s2, m2 = _ramp_run(tmp_path, target, -25.0, learned=learned)
    _row(f"a) 25→{target} map-25 (학습 후 냉간 재운전)", m2)
    assert "목표 근처 학습점" in s2.c.bias_source
    assert m2["err"] <= 1.0 and m2["pp"] <= 2.0 and m2["over"] <= 8.0, m2


# ───────────────────────── b) 매핑 ±40 ─────────────────────────
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
    s.shutter = True; s.tc1_off = -100.0          # 셔터 열림: TC1 이 약 100°C 낮게 읽힌다
    t_open = s.t
    s.runfor(30 * 60)
    seg = [r for r in s.rec if r[0] > t_open]
    assert s.c.state == 'frozen'
    assert all(abs(r[5] - b0) < 1e-9 for r in seg), "셔터 열림 동안 bias 가 변하면 안 된다"
    assert all(abs(r[6] - sv2_0) < 1e-9 for r in seg), "셔터 열림 동안 SV2 가 변하면 안 된다"
    assert max(r[2] for r in seg) - min(r[2] for r in seg) < 1.0      # TC2 는 그대로 붙들린다
    s.shutter = False; s.tc1_off = 0.0
    t_close = s.t
    s.runfor(60 * 60)
    m = _metrics(s.rec, 600, t_from=t_close)
    _row("c) 600 셔터 30분 열림 → 닫힘 후 60분", m)
    assert s.c.state == 'running'
    assert abs(s.c.bias - b0) < 2.0 and m["err"] <= 1.0 and m["pp"] <= 2.0


def test_c_shutter_holds_rising_target_until_closed(tmp_path):
    """셔터가 열린 동안에는 상승 목표도 보류(SV2 고정), 하강은 즉시 받는다."""
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    s.shutter = True; s.tc1_off = -100.0
    s.runfor(10)
    sv2 = s.D[35]
    s.c.start(400, 6.0); s.runfor(300)
    assert s.c.r1 == pytest.approx(300.0) and s.D[35] == sv2
    s.c.start(250, 6.0); s.runfor(5)
    assert s.c.r1 == pytest.approx(250.0) and s.D[35] < sv2


# ───────────────────────── d) TC1 탈락 ─────────────────────────
def test_d_tc1_detached_alarm(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    s.tc1_stuck = 240.0                           # TC1 이 시료대에서 떨어져 실제보다 낮게 읽힌다
    t0 = s.t
    ok = s.run_until(lambda: any(k == "tc1_detached" for _, k, _ in s.alerts), 3 * 3600)
    assert ok
    t_alarm, _, d = [a for a in s.alerts if a[1] == "tc1_detached"][0]
    t_cross = next(r[0] for r in s.rec if r[0] > t0 and abs(r[5]) >= 100.0)
    RESULTS.append(("d) TC1 탈락(TC1 300→240 고정)", None, None, None, (t_alarm - t0) / 60.0))
    assert t_alarm - t_cross == pytest.approx(ALARM_HOLD_SEC, abs=2.0)
    assert d["action"] == "abort" and "TC1 이 시료대에서 떨어졌거나" in d["text"] and "예상 TC2" in d["text"]
    assert abs(s.c.bias) <= 150.0 + 1e-9         # bias 상한이 SV2 를 묶는다


# ───────────────────────── e) TC2 이상 ─────────────────────────
def test_e_tc2_lost_shutter_closed_falls_back_to_tc1(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    n = len(s.writes)
    s.tc2_bad = True
    s.runfor(3)
    w = [(k, v) for _, k, v in s.writes[n:]]
    assert w[:2] == [('M4A', False), (35, 0.0)], "해제 순서: M0004A OFF → D00035=0"
    assert s.c.state == 'tc1' and any(k == "tc2_lost" for _, k, _ in s.alerts)
    t0 = s.t
    s.runfor(30 * 60)                            # 히터는 계속 운전 — 래더 TC1 제어가 D00012(=r1) 을 따라간다
    assert s.run and s.D[12] == pytest.approx(300.0)
    tc1 = [r[1] for r in s.rec if r[0] > t0]
    assert min(tc1) > 280.0 and max(tc1) < 320.0
    RESULTS.append(("e) TC2 상실(셔터 닫힘) → TC1 제어 30분", max(tc1) - 300.0, max(tc1) - min(tc1), None, None))


def test_e_tc2_lost_shutter_open_fixes_dac(tmp_path):
    s, _ = _ramp_run(tmp_path, 600, 0.0, tail_min=90)
    s.shutter = True; s.tc1_off = -100.0
    s.runfor(60)
    mv_before = int(s.mv)
    n = len(s.writes)
    s.tc2_bad = True
    s.runfor(3)
    w = [(k, v) for _, k, v in s.writes[n:]]
    assert w[:2] == [('M4A', False), (35, 0.0)]
    assert s.c.state == 'dac' and s.c.kind == 'dac'
    assert abs(s.D[18] - mv_before) <= 2 and any(k == "demoted" for _, k, _ in s.alerts)
    assert s.c.sat_hold() == (True, 'dac')
    s.runfor(600)
    assert s.D[18] == s.c.dac_value              # 셔터가 열려 있는 동안 고정 유지
    s.shutter = False; s.tc1_off = 0.0
    s.runfor(3)
    assert s.D[18] == 1200 and s.c.state == 'tc1'   # TC2 가 여전히 없으면 상한 원복 + TC1 제어


def test_e_tc2_recovers_reengages(tmp_path):
    s, _ = _ramp_run(tmp_path, 300, 0.0, tail_min=90)
    s.tc2_bad = True; s.runfor(5)
    assert s.c.state == 'tc1'
    s.tc2_bad = False
    assert s.run_until(lambda: s.c.state == 'running', 200)
    assert any("TC2 제어 재시도" in m for _, _, m in s.msgs)


# ───────────────────────── f) HOLD / 하강 / 운전 중 목표 변경 ─────────────────────────
def test_f_hold_resume_and_retarget(tmp_path):
    s = Sim(tmp_path, offset=0.0)
    s.c.start(300, 6.0, "레시피"); s.run = True
    assert s.run_until(lambda: (s.c.r1 or 0) >= 150.0, 3 * 3600)
    s.c.hold(); r_hold = s.c.r1
    s.runfor(10 * 60)
    assert s.c.r1 == pytest.approx(r_hold)          # HOLD 동안 r1 고정(멈춘 시간은 계산에 쓰지 않는다)
    s.c.resume(); s.runfor(5)
    assert s.c.r1 > r_hold and s.c.r1 - r_hold < 1.0   # 재개 때 껑충 뛰지 않는다
    assert s.run_until(lambda: (s.c.r1 or 0) >= 250.0, 3 * 3600)
    s.c.start(400, 6.0)                              # 운전 중 상승 변경 — 같은 운전에서 이어 간다
    assert s.c.state == 'running' and s.c.target() == 400.0
    t_up = s.t
    s.runfor(150 * 60)
    m_up = _metrics(s.rec, 400, t_from=t_up)
    _row("f) 운전 중 목표 300→400", m_up)
    assert m_up["err"] <= 1.0 and m_up["pp"] <= 2.0 and m_up["over"] <= 5.0
    s.c.start(200, 6.0)                              # 하강 — r1 을 바로 내린다
    assert s.c.r1 == pytest.approx(200.0)
    t_dn = s.t
    s.runfor(150 * 60)
    m_dn = _metrics(s.rec, 200, t_from=t_dn)
    _row("f) 목표 하강 400→200", dict(m_dn, over=min(r[1] for r in s.rec if r[0] > t_dn + 1800) - 200))
    assert m_dn["err"] <= 1.0 and m_dn["pp"] <= 2.0


# ───────────────────────── 핸드셰이크·해제 순서·폴백 (플랜트 없이) ─────────────────────────
def _bare(tmp_path, **kw):
    t = [0.0]
    c = HeaterCascade(map_points=MAP, ti_sec=600, lag_sec=90, bias_rate_max=3.0, bias_limit_c=150.0,
                      bias_alarm_c=100.0, alarm_action="abort", holdback_c=10.0, tc2_margin_c=50.0,
                      mv_limit=1200, mv_min=400, soak_tol_c=2.0, soak_sec=60,
                      learned_path=str(kw.pop("learned", tmp_path / "l.json")), clock=lambda: t[0])
    ev = []
    c.request_sv2.connect(lambda v: ev.append(("sv2", v)))
    c.request_pv_sel.connect(lambda b: ev.append(("sel", b)))
    c.request_target.connect(lambda v: ev.append(("sv", v)))
    c.request_mv_limit.connect(lambda v: ev.append(("lim", v)))
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
    c.tick(_st(run=False)); assert c.state == 'armed'
    c.tick(_st()); assert c.state == 'engaging_sv2'
    sv2 = [v for k, v in ev if k == "sv2"][-1]
    assert sv2 == pytest.approx(round(Tc2Map(MAP).value(100.0), 1))
    c.tick(_st(sv2=sv2)); assert c.state == 'engaging_sel' and ev[-1] == ("sel", True)
    c.tick(_st(sv2=sv2, pv_sel_eff=True)); assert c.state == 'running' and c.engaged()
    ev.clear()
    c.stop("테스트")
    assert ev == [("sel", False), ("sv2", 0.0)] and c.state == 'idle'


def test_engage_timeout_falls_back_to_tc1(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st()); assert c.state == 'engaging_sv2'
    t[0] = ENGAGE_TIMEOUT_SEC + 0.5
    c.tick(_st(sv2=0.0))                          # D00035 되읽기가 끝내 안 맞는다
    assert c.state == 'tc1' and ev[-2:] == [("sel", False), ("sv2", 0.0)]


def test_ladder_without_tc2_support_uses_tc1(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st(sv2_max=0.0))
    assert c.state == 'tc1' and not any(k == "sel" and v for k, v in ev)


def test_run_off_releases(tmp_path):
    c, ev, t = _bare(tmp_path)
    c.start(300, 6.0)
    c.tick(_st()); sv2 = ev[-1][1]
    c.tick(_st(sv2=sv2)); c.tick(_st(sv2=sv2, pv_sel_eff=True))
    ev.clear()
    c.tick(_st(run=False, sv2=sv2))
    assert ev == [("sel", False), ("sv2", 0.0)] and c.state == 'idle'


def test_initial_bias_sources(tmp_path):
    import json
    p = tmp_path / "learned.json"
    c, _, _ = _bare(tmp_path, learned=p)
    c.start(600, 6.0)
    assert c.bias == 0.0 and "학습점 없음" in c.bias_source
    c.stop()
    p.write_text(json.dumps({"points": [{"tc1": 300.0, "tc2": 455.0, "t": "2026-10-01 10:00:00"},
                                        {"tc1": 600.0, "tc2": 840.0, "t": "2026-09-01 10:00:00"}]}), encoding="utf-8")
    c.start(620, 6.0)                             # ±50 안에 600 학습점
    assert c.bias == pytest.approx(15.0) and "목표 근처 학습점" in c.bias_source
    c.stop()
    c.start(450, 6.0)                             # 근처 없음 → 가장 최근(300, 10-01)
    assert c.bias == pytest.approx(10.0) and "가장 최근 학습점" in c.bias_source


def test_map_interp_and_extrapolation():
    m = Tc2Map(MAP)
    assert m.value(300) == pytest.approx(445.0)
    assert m.value(350) == pytest.approx(507.5)
    assert m.value(700) == pytest.approx(955.0)       # 끝 구간 기울기 1.3 으로 외삽
    assert m.slope(10) == pytest.approx(420.0 / 275.0)


def test_csv_logger_columns(tmp_path, monkeypatch):
    """기존 열 이름·순서는 그대로, 캐스케이드 열은 끝에. TC2 제어 중 ramp_sv_c / cur_sv_c 에는 r1 이 들어간다."""
    import csv
    import lib.heater_logger as HL
    monkeypatch.setattr(HL, "NAS_HEATER_LOG_DIR", tmp_path)
    lg = HL.HeaterCsvLogger()
    path = lg.start("T")
    st = _st(sv_ramp=812.3, cur_sv=812.3, sv2=812.3, pv=598.0, pv2=810.0)
    casc = {"r1_c": "600.0", "r1_lag_c": "599.0", "tc1_ema_c": "598.1", "sv2_map_c": "825.0", "bias_c": "-12.70",
            "casc_state": "running", "shutter_open": 0, "engaged": True}
    lg.write_row(st, "n", (1, None), "cascade", wd_age=0.5, ctrl_mode="cascade", casc=casc)
    lg.stop()
    rows = list(csv.reader(open(path, encoding="utf-8-sig")))
    head, row = rows[0], dict(zip(rows[0], rows[1]))
    assert head[:len(HL.COLUMNS) - 8] == HL.COLUMNS[:len(HL.COLUMNS) - 8]
    assert head[-8:] == ["ctrl_mode", "r1_c", "r1_lag_c", "tc1_ema_c", "sv2_map_c", "bias_c", "casc_state", "shutter_open"]
    assert row["ramp_sv_c"] == "600.0" and row["cur_sv_c"] == "600.0" and row["sv2_c"] == "812.3"
    assert row["bias_c"] == "-12.70" and row["hold_kind"] == "cascade" and row["ctrl_mode"] == "cascade"


def test_zz_print_summary_table():
    """요약표(pytest -s 로 본다). 같은 세션에서 위 시나리오가 돈 뒤에 찍힌다."""
    def f(v, fmt="{:+.2f}"):
        return "-" if v is None else fmt.format(v)
    lines = [f"{'시나리오':<40} {'오버슈트':>9} {'정착 p-p':>9} {'최종 오차':>9} {'도달(분)':>9}"]
    for case, over, pp, err, arr in RESULTS:
        lines.append(f"{case:<40} {f(over):>9} {f(pp, '{:.2f}'):>9} {f(err, '{:.2f}'):>9} {f(arr, '{:.1f}'):>9}")
    print("\n" + "\n".join(lines))


# ───────────────────────── 히터 레시피 러너 연결 ─────────────────────────
def test_recipe_runner_uses_cascade(qapp, tmp_path):
    """러너는 캐스케이드에 start/hold/resume/stop 만 부르고(tick 은 main 폴링), RAMP 끝은 r1 도달로 본다.
    종료 시 캐스케이드 해제(M0004A OFF → D00035=0)가 RUN OFF 보다 먼저다."""
    from PyQt6.QtCore import QObject, pyqtSignal as Signal
    from controller.heater_recipe import HeaterRecipeRunner
    from conftest import make_heater_st

    class FakePlc(QObject):
        update_heater_status = Signal(dict)

        def __init__(self):
            super().__init__()
            self.st = make_heater_st(run=True, pv=100.0, sv=100.0)

        def get_heater_status(self):
            return dict(self.st)

    order = []

    class FakeCasc:
        def __init__(self):
            self.r1, self.final, self.active, self.held = None, None, False, False

        def start(self, t, rate, label=""):
            order.append(("start", t, rate)); self.final, self.active = float(t), True
            self.r1 = 100.0 if self.r1 is None else min(self.r1, float(t))

        def hold(self):
            order.append(("hold",)); self.held = True

        def resume(self):
            if self.held:
                order.append(("resume",))
            self.held = False

        def stop(self, why=""):
            order.append(("stop", why)); self.active = False

        def is_active(self):
            return self.active

        def target(self):
            return self.final if self.active else None

        def r1_done(self, target=None):
            return self.active and self.r1 == self.final and (target is None or target == self.final)

    plc, fc = FakePlc(), FakeCasc()
    r = HeaterRecipeRunner(plc)
    r.set_cascade(fc)
    r.request_run.connect(lambda b: order.append(("RUN", b)))
    r.request_target.connect(lambda v: order.append(("D00012", v)))
    p = tmp_path / "rc.csv"
    p.write_text("step,target_c,ramp_c_per_min,soak_min\n1,300,6,0\n2,200,6,0\n", encoding="utf-8")
    assert r.load(str(p)) and r.start()
    assert ("start", 300.0, 6.0) in order and ("RUN", True) in order
    assert not any(k == "D00012" for k, *_ in order)            # 운전 중 D00012 는 캐스케이드가 쥔다
    r.hold(); r.resume()
    assert ("hold",) in order and ("resume",) in order
    plc.update_heater_status.emit(dict(plc.st))                 # r1 이 아직 목표 전 — SOAK 로 넘어가지 않는다
    assert r.progress().get("phase") == "ramp"
    fc.r1 = 300.0
    plc.update_heater_status.emit(dict(plc.st))                 # r1 도달 → SOAK(0분) → 다음 틱에 스텝 2
    r._on_tick()
    assert ("start", 200.0, 6.0) in order
    order.clear()
    r.stop("테스트")
    assert [o[0] for o in order][:2] == ["stop", "RUN"] and order[1] == ("RUN", False)
