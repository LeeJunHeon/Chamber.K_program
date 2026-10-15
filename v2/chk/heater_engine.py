"""챔버K HeaterEngine — 히터 제어 루프를 v1 동작 그대로 옮긴 것(사용자 결정 5-2: 옮긴 뒤 개선은 따로).

v1 근거(읽고 다시 씀, import 안 함): lib/heater_profile.py, controller/heater_ramp.py(RampProfiler),
controller/heater_hold.py(HeaterHold), controller/heater_saturation.py, controller/heater_recipe.py(HeaterRecipeRunner),
controller/heater_atmosphere.py, main.py 히터 경로(수동 ON/OFF·목표·가스 유지, update_heater_display 순서),
controller/process_controller.py _heater_wait·_heater_hold_wait·_hold_force·_heater_off.

ProcessEngine 과 분리(결정 2): 공정은 request_target(목표·속도)·process_wait_tick(도달·유지 확인)·process_off 로만 요청한다.
히터 단독 운전(수동·히터 레시피·히터 가스)도 여기서 맡는다. 단계 표기: RAMP / SOAK / COOL — 사용자가 모르는 중간 단계를 만들지
않는다(감속 접근은 RAMP 안의 동작, 공정의 유지 모드 진입 대기는 SOAK 뒤의 확인).

호출 시점(v1 과 같은 주기)
- on_poll(values): PLC 폴링마다(250 ms, v1 200 ms) — 포화 감시 → 유지 모드 → 히터 RUN 하강 가스 유지. v1 update_heater_display 순서.
- on_tick(view): 1초 tick 마다 — 감속 접근 램프·히터 레시피·히터 가스 시퀀스·공정 도달 대기.
두 경로는 다른 스레드(PLC worker / tick 루프)라 잠금 하나로 묶는다. PLC 쓰기는 드라이버 요청(큐)만 — 기다리지 않는다.

안전 규칙과 겹치지 않게: 히터 이상(S16)·외부 RUN OFF(S18B)·TC 이상(S18C)·출력 죽음(S18A)은 SafetyEngine 이 판정하고
HEATER_RECIPE_ABORT 로 이 엔진을 부른다(v1 레시피 러너 안의 같은 판정은 옮기지 않음). 레시피 스텝 램프 시간 초과(S18D)·
포화(S24)·유지 모드(S22)·감속 접근(S25)은 이 엔진이 한다.
pending_confirm: HEATER_RECIPE_MAX_REPEAT=1(repeat > 1 거부), HEATER_RECIPE_HOLD_AT_END=false.
"""
from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.chk.process_steps import (StepRunner, build_gas_intro_steps, build_gas_pre_steps, build_gas_release_steps,
                                  build_pressure_stage_steps)

HOLD_DRIFT_DEFAULTS = ("HEATER_HOLD_DRIFT_PV_REL", "HEATER_HOLD_DRIFT_MV_REL", "HEATER_HOLD_DRIFT_TC2_REL")


@dataclass
class HeaterEvent:
    kind: str            # message / engaged / give_up / alert / saturated / recipe_step / recipe_finished / gas / reached
    level: str = "info"
    detail: str = ""
    code: Optional[str] = None
    data: Optional[dict] = None


# ───────────── 감속 접근 프로파일(lib/heater_profile) ─────────────
def approach_rate(dist: float, rate: float, zone: float, rmin: float) -> float:
    if rate <= rmin or zone <= 0 or dist >= zone:
        return float(rate)
    return rmin + (rate - rmin) * max(0.0, dist) / zone


def ramp_minutes(span: float, rate: float, zone: float, rmin: float) -> float:
    span, rate = abs(float(span)), float(rate)
    if rate <= 0 or span <= 0:
        return 0.0
    if zone <= 0 or rate <= rmin:
        return span / rate
    d1 = min(span, zone)
    b = (rate - rmin) / zone
    return (span - d1) / rate + math.log((rmin + b * d1) / rmin) / b


def solve_rate_for_minutes(span: float, minutes: float, zone: float, rmin: float) -> float:
    span, minutes = abs(float(span)), float(minutes)
    if minutes <= 0 or span <= 0:
        return 0.0
    r0 = span / minutes
    if zone <= 0 or r0 <= rmin:
        return r0
    lo, hi = r0, r0 * 10.0
    while ramp_minutes(span, hi, zone, rmin) > minutes and hi < 600.0:
        hi *= 2.0
    hi = min(hi, 600.0)
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if ramp_minutes(span, mid, zone, rmin) > minutes:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def ramp_raw(c_per_min: float) -> int:
    """°C/min → 래더 카운트/초(1 = 6 °C/min)."""
    return max(1, int(round(float(c_per_min) / 6.0)))


# ───────────── PLC 쪽 요청(드라이버 큐) ─────────────
class HeaterPort:
    def __init__(self, plc) -> None:
        self.plc = plc

    def target(self, c: float) -> None:
        self.plc.heater_set_sv(round(float(c), 1))

    def ramp(self, counts: int) -> None:
        self.plc.heater_set_reg("heater_ramp_rate", int(counts))

    def run(self, on: bool) -> None:
        self.plc.heater_run(bool(on))

    def mv_limit(self, v: int) -> None:
        self.plc.heater_set_reg("heater_mv_limit", int(v))

    def sv2(self, c: float) -> None:
        self.plc.heater_set_reg("heater_sv2", int(round(float(c) * 10)))

    def pv_sel(self, on: bool) -> None:
        self.plc.heater_set_pv_sel(bool(on))


# ───────────── RampProfiler(controller/heater_ramp.py) ─────────────
class RampProfiler:
    def __init__(self, cfg, port: HeaterPort, status: Callable[[], dict], msg: Callable[[str, str], None],
                 clock: Callable[[], float]) -> None:
        self.cfg, self.port, self._status, self._msg, self._now = cfg, port, status, msg, clock
        self._active = False
        self._target: Optional[float] = None
        self._rate = 0.0
        self._sv_cmd: Optional[float] = None
        self._t_last = 0.0
        self._label = ""

    def is_active(self) -> bool:
        return self._active

    def target(self) -> Optional[float]:
        return self._target if self._active else None

    def _zr(self):
        return float(self.cfg["HEATER_APPROACH_ZONE_C"]), float(self.cfg["HEATER_APPROACH_MIN_RATE_C_PER_MIN"])

    def start(self, target: float, rate: float, label: str = "") -> bool:
        st = self._status() or {}
        svr, pv = st.get("sv_ramp"), st.get("pv")
        base = svr if (st.get("run") and svr is not None) else (pv if pv is not None else svr)
        if base is None:
            self._msg("warn", "[히터] 램프: 현재 온도를 읽을 수 없어 감속 접근을 쓸 수 없습니다")
            return False
        base, target, rate = float(base), float(target), float(rate)
        if target <= base + 0.05 or rate <= 0:
            return False
        counts = max(2, int(math.ceil(rate / 6.0)) + 1)          # 래더는 파이썬 푸시보다 항상 빠르게
        self.port.ramp(counts)
        self._sv_cmd = base
        self.port.target(round(base, 1))                         # RUN 전에 출발점으로
        self._target, self._rate, self._label = target, rate, label or ""
        self._t_last = self._now()
        self._active = True
        z, m = self._zr()
        self._msg("info", f"[히터] 램프{(' ' + self._label) if self._label else ''}: {base:.1f}→{target:.1f}°C · "
                          f"{rate:g}°C/min · 접근 {z:g}°C→{m:g}°C/min · 예상 {ramp_minutes(target - base, rate, z, m):.1f}분")
        return True

    def tick(self) -> Optional[bool]:
        """1초마다. 끝났으면 True."""
        if not self._active:
            return None
        st = self._status() or {}
        svr = st.get("sv_ramp")
        if svr is None:
            return None
        svr = float(svr)
        now = self._now()
        dt = min(max(now - self._t_last, 0.0), 5.0)
        self._t_last = now
        target = float(self._target)
        z, m = self._zr()
        r_now = approach_rate(max(0.0, target - svr), self._rate, z, m)
        base = max(float(self._sv_cmd), svr - 0.05)
        new = min(target, base + r_now * dt / 60.0, svr + float(self.cfg["HEATER_APPROACH_LEAD_C"]))
        new = max(new, float(self._sv_cmd))
        if round(new, 1) != round(float(self._sv_cmd), 1):
            self.port.target(round(new, 1))
        self._sv_cmd = new
        if self._sv_cmd >= target - 1e-6 and svr >= target - 0.15:
            self._active = False
            self._msg("info", f"[히터] 램프 완료 {target:.1f}°C")
            return True
        return False

    def hold(self) -> None:
        if not self._active:
            return
        svr = (self._status() or {}).get("sv_ramp")
        if svr is not None:
            self.port.target(round(float(svr), 1))
            self._sv_cmd = float(svr)

    def resume(self) -> None:
        if self._active:
            self._t_last = self._now()

    def stop(self, restore_rate: bool = True) -> None:
        was = self._active
        self._active, self._target = False, None
        if restore_rate and was:
            self.port.ramp(ramp_raw(self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]))


# ───────────── HeaterHold(controller/heater_hold.py) ─────────────
class HeaterHold:
    def __init__(self, cfg, port: HeaterPort, msg: Callable[[str, str], None], emit: Callable[[HeaterEvent], None],
                 clock: Callable[[], float]) -> None:
        c = cfg
        self.mode = str(c["HEATER_HOLD_MODE"])
        self.mv_limit, self.mv_min = int(c["HEATER_MV_LIMIT"]), int(c["HEATER_MV_MIN"])
        self.enter_tol, self.enter_sec = float(c["HEATER_HOLD_MV_ENTER_TOL_C"]), float(c["HEATER_HOLD_MV_ENTER_SEC"])
        self.arrive_tol = float(c["HEATER_HOLD_MV_ARRIVE_TOL_C"])
        self.drift_pv, self.drift_mv = float(c["HEATER_HOLD_MV_DRIFT_PV_C"]), float(c["HEATER_HOLD_MV_DRIFT_MV"])
        self.tc2_margin, self.tc2_drift = float(c["HEATER_HOLD_TC2_MARGIN_C"]), float(c["HEATER_HOLD_TC2_DRIFT_C"])
        self.rel_pv, self.rel_mv, self.rel_tc2 = (float(c[k]) for k in HOLD_DRIFT_DEFAULTS)
        self.mv_sat_rel = float(c["HEATER_MV_SAT_REL"])
        self.engage_timeout = float(c["HEATER_HOLD_ENGAGE_TIMEOUT_SEC"])
        self.reapply_sec = float(c["HEATER_HOLD_REAPPLY_SEC"])
        self.dev_warn_c, self.dev_warn_sec = float(c["HEATER_TC1_DEV_WARN_C"]), float(c["HEATER_TC1_DEV_WARN_SEC"])
        self.dev_ok_c = float(c["HEATER_TC1_DEV_OK_C"])
        self.cfg, self.port, self._msg_fn, self._emit, self._now = cfg, port, msg, emit, clock
        self._h = self._fresh()
        self._ring: deque = deque()
        self._last_st: dict = {}
        self.engaged_relaxed = False
        self.no_margin_warned = False
        self.last_force_reason = ""
        self.last_force_source = ""

    @staticmethod
    def _fresh() -> dict:
        return {'state': 'idle', 'kind': None, 'samples': [], 't0': 0.0, 'sv': None, 'value': None, 'sv2': None,
                'final': None, 'last_push': 0.0, 'engage_t0': 0.0, 'engage_warned': False, 'arrived': False,
                'arr_sv': None, 'floor_warned': False, 'limit_warned': False, 'drift_log_t': 0.0, 'alt_reason': None,
                'dev_t0': 0.0, 'dev_warned': False, 'reapply_t0': 0.0, 'last_window_mv': None, 'last_sv': None,
                'gave_up': None}

    @property
    def state(self) -> str:
        return self._h['state']

    @property
    def kind(self):
        return self._h['kind'] if self._h['state'] != 'idle' else None

    @property
    def value(self):
        return self._h['value']

    @property
    def sv2(self):
        return self._h['sv2']

    def is_holding(self) -> bool:
        return self._h['state'] == 'holding'

    @property
    def arrived(self) -> bool:
        return bool(self._h['arrived'])

    @property
    def gave_up(self):
        return self._h['gave_up']

    def _msg(self, level: str, text: str) -> None:
        self._msg_fn(level, text)

    def effective_gates(self, tc2_mean=None):
        eff_pv = max(self.drift_pv, self.rel_pv * self.enter_tol)
        eff_mv = max(self.drift_mv, self.rel_mv * self.mv_limit)
        eff_tc2 = max(self.tc2_drift, self.rel_tc2 * float(tc2_mean)) if tc2_mean else self.tc2_drift
        return eff_pv, eff_mv, eff_tc2

    def ring_tc2_mean(self):
        vals = [r[3] for r in self._ring if r[3] is not None]
        return (sum(vals) / len(vals), len(vals)) if vals else (None, 0)

    def force_tc2_hold(self) -> bool:
        h, st = self._h, self._last_st
        if h['state'] == 'holding':
            return True
        if self.mode == 'off':
            self.last_force_reason = "유지 모드 꺼짐(HEATER_HOLD_MODE=off)"
            return False
        kind, alt = self._effective_kind(st)
        if kind != 'tc2':
            self.last_force_reason = alt or "TC2 추종 불가"
            return False
        mean2, n2 = self.ring_tc2_mean()
        if mean2 is None or n2 < 4:
            self.last_force_reason = f"TC2 표본 부족({n2}개 < 4)"
            return False
        pv_now = st.get('pv') if st.get('pv') is not None else self._ring[-1][2]
        sv = h['last_sv']
        if sv is None or pv_now is None or abs(float(pv_now) - float(sv)) > self.enter_tol:
            self.last_force_reason = f"TC1 이 목표 근처가 아님(|TC1−SV| > {self.enter_tol:g})"
            return False
        if h['state'] in ('engaging_sv2', 'engaging_sel'):
            return True
        cap = float(st.get('ot2_limit') or 0.0) - self.tc2_margin
        sv2 = round(min(mean2, cap), 1)
        if mean2 > cap:
            self._msg("info", f"TC2 목표를 OT2−마진으로 제한 {mean2:.1f} → {sv2:.1f}°C")
        h.update(state='engaging_sv2', kind='tc2', sv2=sv2, final=h['arr_sv'] if h['arr_sv'] is not None else sv,
                 value=None, engage_t0=self._now(), samples=[], t0=0.0, gave_up=None, engage_warned=False)
        self.engaged_relaxed = True
        self.port.sv2(float(sv2))
        self._msg("warn", f"완화 조건으로 TC2 추종 진입 — SV2 {sv2:.1f}°C (정착 창 미확보, 최근 {self.enter_sec:.0f}초 평균 {n2}표본)")
        return True

    def force_dac_hold(self, mv_value=None, prefer_current: bool = False) -> bool:
        h = self._h
        if h['state'] == 'holding':
            return True
        cur = None
        if mv_value is not None:
            try:
                cur = float(mv_value)
            except Exception:
                cur = None
        if prefer_current:
            src, self.last_force_source = ((cur, "강등 직전 MV") if cur is not None
                                           else (h['last_window_mv'], "마지막 측정 창 평균"))
        else:
            src, self.last_force_source = ((h['last_window_mv'], "마지막 측정 창 평균") if h['last_window_mv'] is not None
                                           else (cur, "현재 MV"))
        if src is None:
            self.last_force_reason = "측정 창 평균도 현재 MV 도 없음"
            return False
        if h['state'] in ('engaging_sv2', 'engaging_sel'):
            self.port.pv_sel(False)
            self.port.sv2(0.0)
        value = max(self.mv_min + 20, min(self.mv_limit, int(round(src))))
        h.update(state='holding', kind='dac', sv=h['last_sv'], value=value, last_push=self._now(), samples=[], t0=0.0,
                 engage_t0=0.0, gave_up=None)
        self.engaged_relaxed = True
        self.port.mv_limit(value)
        self._msg("warn", f"유지 모드 강제 진입 — DAC 상한 {value} 고정 ({self.last_force_source})")
        self._emit(HeaterEvent("engaged", detail="dac"))
        return True

    def _reset(self, keep_arrived: bool) -> None:
        h = self._h
        keep = {k: h[k] for k in ('arrived', 'arr_sv', 'limit_warned', 'floor_warned', 'drift_log_t')}
        lw, lsv = h['last_window_mv'], h['last_sv']
        self._h = self._fresh()
        self._h.update(last_window_mv=lw, last_sv=lsv)
        if keep_arrived:
            self._h.update(keep)

    def release(self, why: str) -> None:
        h = self._h
        st_, kind = h['state'], h['kind']
        if st_ == 'holding':
            if kind == 'dac':
                self.port.mv_limit(self.mv_limit)
                self._msg("info", f"DAC 상한 고정 해제 → {self.mv_limit} 원복 ({why})")
            else:
                self.port.pv_sel(False)
                self.port.sv2(0.0)
                self._msg("info", f"TC2 추종 해제 → TC1 제어 복귀 ({why})")
        elif st_ in ('engaging_sv2', 'engaging_sel'):
            self.port.pv_sel(False)
            self.port.sv2(0.0)
            self._msg("info", f"TC2 추종 진입 취소 → TC1 제어 유지 ({why})")
        elif h['samples']:
            self._msg("info", f"유지 모드 대기 취소 ({why})")
        self._reset(keep_arrived=False)
        self.no_margin_warned = False

    def _effective_kind(self, st: dict):
        if self.mode == 'off':
            return None, None
        if self.mode == 'dac':
            return 'dac', None
        sv2_max, ot2 = float(st.get('sv2_max') or 0.0), float(st.get('ot2_limit') or 0.0)
        if sv2_max <= 0 or ot2 <= 0:
            return 'dac', "래더가 TC2 추종을 지원하지 않음(D00036=0) — DAC 상한 고정으로 대체"
        if st.get('pv2') is None:
            return 'dac', "TC2 값 없음(D00011=-1) — DAC 상한 고정으로 대체"
        return 'tc2', None

    def tick(self, st: dict, final_target=None) -> None:
        try:
            self._tick(st, final_target)
        except Exception as e:
            self._msg("warn", f"유지 모드 처리 예외: {e!r}")

    def _tick(self, st: dict, final_target) -> None:
        h = self._h
        now = self._now()
        self._last_st = st
        if self.mode == 'off':
            if h['state'] == 'holding' or h['state'].startswith('engaging'):
                self.release("기능 꺼짐")
            elif h['state'] != 'idle' or h['samples'] or h['arrived']:
                self._reset(keep_arrived=False)
            return
        run, fault = bool(st.get('run')), bool(st.get('fault'))
        sv, pv, svr, mv = st.get('sv'), st.get('pv'), st.get('sv_ramp'), st.get('mv')
        if sv is not None:
            h['last_sv'] = float(sv)
        if not run:
            if h['state'] != 'idle' or h['samples']:
                self.release("운전 OFF")
            h['last_window_mv'] = None
            self._ring.clear()
            return
        if fault:
            if h['state'] != 'idle' or h['samples']:
                self.release("히터 이상")
            return
        try:
            final = float(final_target) if final_target is not None else (float(sv) if sv is not None else None)
        except Exception:
            final = float(sv) if sv is not None else None
        if h['state'] in ('engaging_sv2', 'engaging_sel'):
            self._tick_engaging(st, now, final)
            return
        if h['state'] == 'holding':
            if h['kind'] == 'dac':
                self._tick_holding_dac(st, now, sv)
            else:
                self._tick_holding_tc2(st, now, final, pv)
            return
        base_ok = (bool(st.get('ok')) and pv is not None and sv is not None and svr is not None and mv is not None
                   and float(sv) > 0)
        ramp_done = (base_ok and final is not None and abs(float(svr) - float(sv)) <= 0.05
                     and abs(float(sv) - final) <= 0.05)
        if base_ok and final is not None and h['arrived'] and h['arr_sv'] is not None and abs(final - float(h['arr_sv'])) > 0.05:
            h.update(arrived=False, arr_sv=None, samples=[], t0=0.0, state='idle')
            self.no_margin_warned = False
        if ramp_done and not h['arrived'] and abs(float(pv) - final) <= self.arrive_tol:
            h['arrived'] = True
            h['arr_sv'] = final
            self._msg("info", f"목표 도달 확인 — 유지 모드 측정 시작 (TC1 {float(pv):.1f} / SV {final:.1f})")
        ok = ramp_done and bool(h['arrived']) and abs(float(pv) - float(sv)) <= self.enter_tol
        if ok:
            _pv2 = st.get('pv2')
            self._ring.append((now, float(mv), float(pv), None if _pv2 is None else float(_pv2)))
            while self._ring and (now - self._ring[0][0]) > self.enter_sec:
                self._ring.popleft()
        if not ok:
            if h['samples']:
                h['samples'], h['t0'], h['state'], h['alt_reason'] = [], 0.0, 'idle', None
            h['limit_warned'] = False
            h['floor_warned'] = False
            return
        kind, alt = self._effective_kind(st)
        if alt is not None and h['alt_reason'] != alt:
            h['alt_reason'] = alt
            self._msg("info", alt)
        elif alt is None:
            h['alt_reason'] = None
        if not h['samples']:
            h['t0'], h['state'], h['gave_up'] = now, 'arming', None
        h['kind'] = kind
        pv2 = st.get('pv2')
        h['samples'].append((float(mv), float(pv), None if pv2 is None else float(pv2)))
        if (now - h['t0']) < self.enter_sec:
            return
        n = len(h['samples'])
        if n < 4:
            return
        half = n // 2
        first, second = h['samples'][:half], h['samples'][half:]
        avg = lambda xs, i: sum(s[i] for s in xs) / len(xs)
        d_mv, d_pv = avg(second, 0) - avg(first, 0), avg(second, 1) - avg(first, 1)
        d_pv2 = 0.0
        tc2_mean = None
        if kind == 'tc2':
            f2 = [s[2] for s in first if s[2] is not None]
            s2 = [s[2] for s in second if s[2] is not None]
            if f2 and s2:
                d_pv2 = sum(s2) / len(s2) - sum(f2) / len(f2)
            vals2 = [s[2] for s in h['samples'] if s[2] is not None]
            tc2_mean = (sum(vals2) / len(vals2)) if vals2 else None
        eff_pv, eff_mv, eff_tc2 = self.effective_gates(tc2_mean)
        h['last_window_mv'] = sum(s[0] for s in h['samples']) / n
        if abs(d_pv) > eff_pv or abs(d_mv) > eff_mv or (kind == 'tc2' and abs(d_pv2) > eff_tc2):
            h['samples'], h['t0'] = [], now
            if (now - h['drift_log_t']) >= 30.0:
                h['drift_log_t'] = now
                extra = f" / TC2 드리프트 {d_pv2:+.2f}°C(허용 {eff_tc2:.1f})" if kind == 'tc2' else ""
                self._msg("info", f"목표 근처지만 아직 정착 전 — 재측정 (PV 드리프트 {d_pv:+.2f}°C(허용 {eff_pv:.1f}) / "
                                  f"MV 드리프트 {d_mv:+.1f}(허용 {eff_mv:.0f}){extra})")
            return
        value = int(round(sum(s[0] for s in h['samples']) / n))
        if value >= self.mv_sat_rel * float(self.mv_limit):
            if kind == 'dac':
                why = (f"도달 시점 출력이 상한에 붙어 있어 고정하지 않음 (평균 MV {value} ≥ {self.mv_limit}×{self.mv_sat_rel:g}) "
                       f"— 히터가 목표를 유지할 여유가 없습니다")
                if not h['limit_warned']:
                    h['limit_warned'] = True
                    self._msg("warn", why)
                    h['gave_up'] = why
                    self._emit(HeaterEvent("give_up", "warn", why))
                h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)
                return
            if not h['limit_warned']:
                h['limit_warned'] = True
                why = (f"도달 시점 출력이 상한의 {self.mv_sat_rel * 100:.0f}% 이상입니다 (평균 MV {value} / 상한 {self.mv_limit}) "
                       f"— 히터에 여유가 없습니다. TC2 추종으로 현재 상태를 고정합니다")
                self._msg("warn", why)
                self.no_margin_warned = True
                self._emit(HeaterEvent("alert", "warn", why, data={"kind": "no_margin", "mv": value}))
        if value <= self.mv_min + 20:
            if not h['floor_warned']:
                h['floor_warned'] = True
                self._msg("warn", f"도달 시점 출력이 최소치라 고정하지 않음 (평균 MV {value} ≤ {self.mv_min + 20}) "
                                  f"— PID 가 출력을 내지 않는 상태입니다")
            h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)
            return
        h['floor_warned'] = False
        if kind == 'dac':
            h['limit_warned'] = False
            self._capture_dac(h, now, value, pv, sv)
        else:
            self._capture_tc2(h, st, now, final)

    def _capture_dac(self, h: dict, now: float, value: int, pv, sv) -> None:
        raw = value
        value = max(self.mv_min + 20, min(self.mv_limit, value))
        if value != raw:
            self._msg("info", f"DAC 상한 고정값 클램프 {raw} → {value} (PLC 하한 HEATER_MV_MIN+20)")
        h.update(state='holding', kind='dac', sv=float(sv), value=value, last_push=now, samples=[], t0=0.0)
        self.engaged_relaxed = False
        self.port.mv_limit(value)
        self._msg("info", f"히터 DAC 상한 {value} 고정 (PV {float(pv):.1f} / SV {float(sv):.1f})")
        self._emit(HeaterEvent("engaged", detail="dac"))

    def _tick_holding_dac(self, st: dict, now: float, sv) -> None:
        h = self._h
        if sv is None or h['sv'] is None or abs(float(sv) - float(h['sv'])) > 0.05:
            self.release(f"목표 변경 {h['sv']} → {sv}")
            return
        cur = st.get('mv_limit')
        if cur is not None and int(cur) != int(h['value']) and (now - h['last_push']) >= self.reapply_sec:
            h['last_push'] = now
            self.port.mv_limit(int(h['value']))
            self._msg("info", f"DAC 상한 재적용 {h['value']} (읽힌 D00018={cur})")

    def _capture_tc2(self, h: dict, st: dict, now: float, final) -> None:
        vals = [s[2] for s in h['samples'] if s[2] is not None]
        if not vals:
            h.update(state='idle', samples=[], t0=0.0)
            return
        avg2 = sum(vals) / len(vals)
        cap = float(st.get('ot2_limit') or 0.0) - self.tc2_margin
        sv2 = round(min(avg2, cap), 1)
        if avg2 > cap:
            self._msg("info", f"TC2 목표를 OT2−마진으로 제한 {avg2:.1f} → {sv2:.1f}°C")
        h.update(state='engaging_sv2', kind='tc2', sv2=sv2, final=final, value=None, engage_t0=now, samples=[], t0=0.0)
        self.engaged_relaxed = False
        self.port.sv2(float(sv2))

    def _demote_to_dac(self, why: str) -> None:
        was = self._h['state']
        self.port.pv_sel(False)
        self.port.sv2(0.0)
        self._reset(keep_arrived=True)
        ok = self.force_dac_hold(None if not self._last_st else self._last_st.get('mv'), prefer_current=True)
        base = f"{why} — TC2 추종 → DAC 상한 고정으로 강등"
        if ok:
            self._emit(HeaterEvent("alert", "warn", base, "HEATER_HOLD_DEMOTED", {"kind": "demoted", "from": was}))
            self._msg("warn", f"{base} (D00018 {self._h['value']}, {self.last_force_source}). OT2 과온 보호도 함께 사라졌습니다")
        else:
            self._msg("warn", f"{base} 실패({self.last_force_reason}) → TC1 제어로 복귀")
            self._emit(HeaterEvent("alert", "warn", base, "HEATER_HOLD_DEMOTED", {"kind": "demote_failed", "from": was}))

    def _tick_engaging(self, st: dict, now: float, final) -> None:
        h = self._h
        why = self._tc2_release_reason(st, final, check_eff=False)
        if why:
            if self._tc2_lost(why):
                self._demote_to_dac(why)
            else:
                self.release(why)
            return
        if h['state'] == 'engaging_sv2':
            back = st.get('sv2')
            if back is not None and abs(float(back) - float(h['sv2'])) <= 0.05:
                h['state'], h['engage_t0'] = 'engaging_sel', now
                self.port.pv_sel(True)
                return
        elif h['state'] == 'engaging_sel':
            if bool(st.get('pv_sel_eff')):
                h.update(state='holding', last_push=now, dev_t0=0.0, dev_warned=False, reapply_t0=0.0)
                self._msg("info", f"TC2 추종 시작 — TC1 {float(st.get('pv')):.1f} / TC2 {float(st.get('pv2')):.1f} "
                                  f"(SV2 {float(h['sv2']):.1f} 고정)")
                self._emit(HeaterEvent("engaged", detail="tc2"))
                return
        if (now - h['engage_t0']) > self.engage_timeout:
            step = "D00035 되읽기" if h['state'] == 'engaging_sv2' else "M0004B 확인"
            warn = not h['engage_warned']
            self.port.pv_sel(False)
            self.port.sv2(0.0)
            self._reset(keep_arrived=True)
            self._h['engage_warned'] = True
            if warn:
                self._msg("warn", f"TC2 추종 진입 실패 — {step} {self.engage_timeout:.0f}초 초과, TC1 제어로 되돌림")

    @staticmethod
    def _tc2_lost(why: str) -> bool:
        return why.startswith("TC2 값 없음") or why.startswith("래더가 TC2 제어를 해제")

    def _tc2_release_reason(self, st: dict, final, check_eff: bool):
        h = self._h
        if final is None or h['final'] is None or abs(float(final) - float(h['final'])) > 0.05:
            return f"목표 변경 {h['final']} → {final}"
        if st.get('pv2') is None:
            return "TC2 값 없음(D00011=-1)"
        if check_eff and not bool(st.get('pv_sel_eff')):
            return "래더가 TC2 제어를 해제(M0004B OFF)"
        return None

    def _tick_holding_tc2(self, st: dict, now: float, final, pv) -> None:
        h = self._h
        why = self._tc2_release_reason(st, final, check_eff=True)
        if why:
            if self._tc2_lost(why):
                self._demote_to_dac(why)
            else:
                self.release(why)
            return
        back = st.get('sv2')
        if back is not None and abs(float(back) - float(h['sv2'])) > 0.05:
            if h['reapply_t0'] == 0.0:
                h['reapply_t0'] = now
            elif (now - h['reapply_t0']) >= self.reapply_sec and (now - h['last_push']) >= self.reapply_sec:
                h['last_push'], h['reapply_t0'] = now, 0.0
                self.port.sv2(float(h['sv2']))
                self._msg("info", f"TC2 목표 재적용 {float(h['sv2']):.1f}°C (읽힌 D00035={float(back):.1f})")
        else:
            h['reapply_t0'] = 0.0
        if pv is None or final is None:
            return
        dev = float(pv) - float(final)
        if abs(dev) > self.dev_warn_c:
            if h['dev_t0'] == 0.0:
                h['dev_t0'] = now
            elif (now - h['dev_t0']) >= self.dev_warn_sec and not h['dev_warned']:
                h['dev_warned'] = True
                self._msg("info", f"TC2 추종 중 TC1 편차 {dev:+.1f}°C — 추종 유지")
        elif abs(dev) <= self.dev_ok_c:
            if h['dev_warned']:
                self._msg("info", "TC1 편차 정상 복귀")
            h['dev_warned'], h['dev_t0'] = False, 0.0


# ───────────── DAC 포화 감시(controller/heater_saturation.py) ─────────────
class SaturationGuard:
    def __init__(self, cfg, port: HeaterPort, msg: Callable[[str, str], None], emit: Callable[[HeaterEvent], None],
                 clock: Callable[[], float]) -> None:
        self.mv_limit, self.mv_min = int(cfg["HEATER_MV_LIMIT"]), int(cfg["HEATER_MV_MIN"])
        self.sat_sec, self.rel = float(cfg["HEATER_MV_SAT_SEC"]), float(cfg["HEATER_MV_SAT_REL"])
        self.pre_window, self.fallback = float(cfg["HEATER_SAT_PRE_WINDOW_SEC"]), float(cfg["HEATER_SAT_FALLBACK_REL"])
        self.port, self._msg, self._emit, self._now = port, msg, emit, clock
        self._hist: deque = deque()
        self._sat_since = 0.0
        self._clamped = None
        self._warned_hold = False
        self._sv = None

    @property
    def clamped(self):
        return self._clamped

    def _release(self, why: str, restore: bool = True) -> None:
        if self._clamped is not None:
            if restore:
                self.port.mv_limit(self.mv_limit)
                self._msg("info", f"DAC 포화 클램프 해제 → {self.mv_limit} 원복 ({why})")
            else:
                self._msg("info", f"DAC 포화 클램프 종료 — 유지 모드가 D00018 을 이어받음 ({why})")
        self._clamped, self._sat_since, self._warned_hold = None, 0.0, False
        self._hist.clear()

    def tick(self, st: dict, hold_active: bool, hold_kind=None) -> None:
        try:
            self._tick(st, bool(hold_active), hold_kind)
        except Exception as e:
            self._msg("warn", f"DAC 포화 감시 예외: {e!r}")

    def _actual_limit(self, st: dict) -> float:
        v = st.get('mv_limit')
        return float(v) if v is not None and float(v) > 0 else float(self.mv_limit)

    def _tick(self, st: dict, hold_active: bool, hold_kind) -> None:
        now = self._now()
        run, fault, sv, mv = bool(st.get('run')), bool(st.get('fault')), st.get('sv'), st.get('mv')
        if not run:
            self._release("운전 OFF")
            self._sv = None
            return
        if fault:
            self._release("히터 이상")
            return
        if hold_active:
            if self._clamped is not None:
                self._release("유지 모드 진입", restore=(hold_kind != 'dac'))
            if hold_kind != 'dac':
                self._sat_since, self._warned_hold = 0.0, False
                return
            if mv is None or self._warned_hold:
                return
            if float(mv) >= self.rel * self._actual_limit(st):
                if not self._sat_since:
                    self._sat_since = now
                elif (now - self._sat_since) >= self.sat_sec:
                    self._warned_hold = True
                    self._announce(st, now, clamp=None, owner="hold_dac")
            else:
                self._sat_since = 0.0
            return
        if sv is not None:
            if self._sv is not None and abs(float(sv) - float(self._sv)) > 0.05:
                self._release(f"목표 변경 {self._sv} → {sv}")
            self._sv = float(sv)
        if mv is None:
            return
        mv = float(mv)
        self._hist.append((now, mv))
        keep_from = (self._sat_since if self._sat_since else now) - self.pre_window
        while self._hist and self._hist[0][0] < keep_from:
            self._hist.popleft()
        if self._clamped is not None:
            return
        if mv >= self.rel * self._actual_limit(st):
            if not self._sat_since:
                self._sat_since = now
            elif (now - self._sat_since) >= self.sat_sec:
                self._saturate(st, now)
        else:
            self._sat_since = 0.0

    def _saturate(self, st: dict, now: float) -> None:
        pre = [m for t, m in self._hist if (self._sat_since - self.pre_window) <= t < self._sat_since]
        if pre:
            raw, src = sum(pre) / len(pre), f"포화 직전 {self.pre_window:.0f}초 평균"
        else:
            raw, src = self.fallback * float(self.mv_limit), f"이력 없음 → 상한×{self.fallback:g}"
        value = max(self.mv_min + 20, min(self.mv_limit, int(round(raw))))
        self._clamped = value
        self._announce(st, now, clamp=value, owner="guard", src=src)
        self.port.mv_limit(value)

    def _announce(self, st: dict, now: float, clamp, owner: str, src: str = "") -> None:
        sec = now - self._sat_since
        limit = int(self._actual_limit(st))
        tail = (f" → D00018 을 {clamp}({src})로 클램프" if clamp is not None
                else f" — 클램프 없음(유지 모드가 D00018 {limit} 을 소유)")
        if st.get('pv2') is None:
            tail += " · TC2 없음 · OT2 과온 보호 없음 — 즉시 확인 필요"
        self._msg("warn", f"DAC 출력 포화 — {sec:.0f}초째 상한({limit})에 물려 있음: TC1 {st.get('pv')} / MV {st.get('mv')}{tail}")
        self._emit(HeaterEvent("saturated", "warn", tail, "HEATER_SATURATED",
                               {"pv": st.get('pv'), "pv2": st.get('pv2'), "mv": st.get('mv'), "sec": sec, "clamp": clamp,
                                "owner": owner, "limit": limit}))


# ───────────── 히터 레시피(controller/heater_recipe.py) ─────────────
@dataclass
class RecipeStep:
    index: int
    target_c: float
    ramp_c_per_min: float
    soak_min: float
    ramp_min: Optional[float] = None
    is_cooldown: bool = False
    resolved_rate: float = 0.0

    def describe(self) -> str:
        if self.is_cooldown:
            return f"{self.target_c:g}°C · COOL · {self.soak_min:g}분 대기"
        if self.ramp_min:
            return f"{self.target_c:g}°C · RAMP {self.ramp_min:g}분 · SOAK {self.soak_min:g}분"
        return f"{self.target_c:g}°C · RAMP {self.ramp_c_per_min:g}°C/min · SOAK {self.soak_min:g}분"


def parse_recipe_steps(rows: List[dict], cfg) -> List[RecipeStep]:
    """recipe.check_heater_recipe 를 통과한 행 → 스텝. repeat > HEATER_RECIPE_MAX_REPEAT(=1, pending) 는 ValueError.
    마지막 스텝이 HEATER_COOLDOWN_TARGET_C 이하면 냉각 스텝(COOL — 램프 없이 soak 만큼 대기)."""
    out: List[RecipeStep] = []
    for i, r in enumerate(rows, 1):
        rep = int(r.get("repeat") or 1)
        if rep > int(cfg["HEATER_RECIPE_MAX_REPEAT"]):
            raise ValueError(f"{i}행: repeat {rep} > HEATER_RECIPE_MAX_REPEAT {cfg['HEATER_RECIPE_MAX_REPEAT']}")
        ramp = r.get("ramp_c_per_min")
        ramp = float(ramp) if ramp not in (None, "") else (0.0 if r.get("ramp_min") else float(cfg["HEATER_RAMP_RATE_C_PER_MIN"]))
        out.append(RecipeStep(len(out) + 1, float(r["target_c"]), ramp, float(r.get("soak_min") or 0.0),
                              float(r["ramp_min"]) if r.get("ramp_min") not in (None, "") else None))
    if out and out[-1].target_c <= float(cfg["HEATER_COOLDOWN_TARGET_C"]):
        out[-1].is_cooldown = True
    return out


class RecipeRunner:
    IDLE, RAMPING, SOAKING, DONE, ABORTED = "IDLE", "RAMPING", "SOAKING", "DONE", "ABORTED"

    def __init__(self, cfg, port: HeaterPort, ramp: RampProfiler, status: Callable[[], dict],
                 msg: Callable[[str, str], None], emit: Callable[[HeaterEvent], None], clock: Callable[[], float]) -> None:
        self.cfg, self.port, self.ramp, self._status, self._msg, self._emit, self._mono = cfg, port, ramp, status, msg, emit, clock
        self.steps: List[RecipeStep] = []
        self.name = ""
        self.state = self.IDLE
        self.idx = -1
        self.step_started = 0.0
        self.step_timeout = float(cfg["HEATER_WAIT_TIMEOUT_SEC"])
        self.soak_deadline = 0.0
        self.held = False
        self.held_at: Optional[float] = None       # 정지 시작 시각(정지 중이 아니면 None)
        self.paused_total = 0.0
        self.held_target = None
        self.stopped_by_user = False
        self.finished: Optional[tuple] = None        # (ok, 사유 코드, 문구)

    def _zr(self):
        return float(self.cfg["HEATER_APPROACH_ZONE_C"]), float(self.cfg["HEATER_APPROACH_MIN_RATE_C_PER_MIN"])

    def now(self) -> float:
        """레시피 시계 — 일시정지 중에는 흐르지 않는다."""
        base = self.held_at if self.held_at is not None else self._mono()
        return base - self.paused_total

    def is_running(self) -> bool:
        return self.state in (self.RAMPING, self.SOAKING)

    @property
    def phase(self) -> str:
        """RAMP / SOAK / COOL / ''."""
        s = self.current()
        if not self.is_running() or s is None:
            return ""
        if s.is_cooldown:
            return "COOL"
        return "RAMP" if self.state == self.RAMPING else "SOAK"

    def current(self) -> Optional[RecipeStep]:
        return self.steps[self.idx] if 0 <= self.idx < len(self.steps) else None

    def load(self, name: str, steps: List[RecipeStep]) -> None:
        self.name, self.steps, self.state, self.idx, self.finished = name, steps, self.IDLE, -1, None

    def start(self) -> bool:
        if self.is_running() or not self.steps:
            return False
        st = self._status() or {}
        self.idx, self.state, self.stopped_by_user, self.finished = -1, self.IDLE, False, None
        self._clear_hold()
        self.paused_total, self.held_at = 0.0, None
        self._msg("info", f"레시피 시작: {len(self.steps)}스텝")
        self._advance(st)
        return True

    def stop(self, reason: str = "중단") -> None:
        was = self.is_running()
        self.stopped_by_user = True
        self.state = self.ABORTED
        self._clear_hold()
        self._safe_shutdown()
        if was:
            self._msg("info", f"레시피 중단: {reason}")
            self._finish(False, "USER_STOP", reason)

    def abort(self, code: str, text: str = "") -> None:
        if not self.is_running():
            return
        self.state = self.ABORTED
        self._clear_hold()
        self._safe_shutdown()
        self._msg("error", f"레시피 중단: {text or code}")
        self._finish(False, code, text or code)

    def _finish(self, ok: bool, code: str, text: str) -> None:
        self.finished = (ok, code, text)
        self._emit(HeaterEvent("recipe_finished", "info" if ok else "warn", text, None if ok else code,
                               {"ok": ok, "name": self.name}))

    def _clear_hold(self) -> None:
        if self.held and self.held_at is not None:
            self.paused_total += max(0.0, self._mono() - self.held_at)
        self.held, self.held_at, self.held_target = False, None, None

    def hold(self) -> bool:
        if not self.is_running():
            return False
        if self.held:
            return True
        self.held, self.held_at = True, self._mono()
        st = self._status() or {}
        if self.state == self.RAMPING:
            if self.ramp.is_active():
                self.held_target = None
                self.ramp.hold()
            else:
                s = self.current()
                self.held_target = float(s.target_c) if s is not None else None
                hold_at = st.get("sv_ramp")
                if hold_at is None or float(hold_at) <= 0:
                    hold_at = st.get("pv") if st.get("pv") is not None else self.held_target
                if hold_at is not None:
                    self.port.target(float(hold_at))
        self._msg("info", f"스텝 {self.idx + 1} 일시정지")
        return True

    def resume(self) -> bool:
        if not self.held:
            return False
        saved = self.held_target
        self._clear_hold()
        if self.state == self.RAMPING:
            if self.ramp.is_active():
                self.ramp.resume()
            elif saved is not None:
                self.port.target(float(saved))
        self._msg("info", f"스텝 {self.idx + 1} 재개")
        return True

    def skip(self) -> bool:
        if not self.is_running():
            return False
        if self.held:
            self._clear_hold()
        self._msg("info", f"스텝 {self.idx + 1} 건너뜀 (사용자)")
        self._advance(self._status() or {})
        return True

    def _safe_shutdown(self, keep_running: bool = False) -> None:
        self.ramp.stop(restore_rate=False)
        if not keep_running:
            self.port.run(False)
        self.port.ramp(ramp_raw(self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]))
        if not keep_running:
            pv = (self._status() or {}).get("pv")
            if pv is not None:
                self.port.target(float(pv))

    def _calc_step_timeout(self, s: RecipeStep, from_temp: float) -> float:
        base = float(self.cfg["HEATER_WAIT_TIMEOUT_SEC"])
        if s.is_cooldown:
            return base
        z, m = self._zr()
        if s.ramp_min:
            need = float(s.ramp_min) * 60.0
        else:
            if s.resolved_rate <= 0:
                return base
            need = ramp_minutes(abs(s.target_c - from_temp), s.resolved_rate, z, m) * 60.0
        return min(max(base, need + float(self.cfg["HEATER_WAIT_TIMEOUT_MARGIN_SEC"])),
                   float(self.cfg["HEATER_WAIT_TIMEOUT_MAX_SEC"]))

    def _advance(self, st: dict) -> None:
        self._clear_hold()
        self.ramp.stop(restore_rate=False)
        self.idx += 1
        if self.idx >= len(self.steps):
            self._complete(st)
            return
        s = self.steps[self.idx]
        self.soak_deadline = 0.0
        self.state = self.RAMPING
        self.step_started = self.now()
        pv = st.get("pv")
        from_temp = float(pv) if pv is not None else (self.steps[self.idx - 1].target_c if self.idx > 0 else 0.0)
        z, m = self._zr()
        if s.ramp_min:
            s.resolved_rate = solve_rate_for_minutes(abs(s.target_c - from_temp), float(s.ramp_min), z, m)
        else:
            s.resolved_rate = float(s.ramp_c_per_min)
        self.step_timeout = self._calc_step_timeout(s, from_temp)
        rate = s.resolved_rate
        if s.is_cooldown or rate <= 0:
            self.port.ramp(ramp_raw(rate or self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]))
            self.port.target(float(s.target_c))
        elif s.target_c > from_temp + 0.05:
            if not self.ramp.start(float(s.target_c), rate, label=f"스텝 {self.idx + 1}"):
                self._msg("warn", f"스텝 {self.idx + 1}: 감속 접근을 쓸 수 없어 래더 램프로 진행합니다")
                self.port.ramp(ramp_raw(rate))
                self.port.target(float(s.target_c))
        else:
            self.port.ramp(ramp_raw(rate))
            self.port.target(float(s.target_c))
        self.port.run(True)
        self._emit(HeaterEvent("recipe_step", detail=f"{self.idx + 1}/{len(self.steps)} {s.describe()}",
                               data={"step": self.idx + 1, "total": len(self.steps), "phase": self.phase}))
        if s.is_cooldown:
            self._enter_soak(s)

    def _enter_soak(self, s: RecipeStep) -> None:
        self.ramp.stop(restore_rate=False)
        self.state = self.SOAKING
        self.port.target(float(s.target_c))
        self.soak_deadline = self.now() + s.soak_min * 60.0

    def _complete(self, st: dict) -> None:
        self.state = self.DONE
        self._clear_hold()
        last = self.steps[-1] if self.steps else None
        hold = bool(self.cfg["HEATER_RECIPE_HOLD_AT_END"]) and last is not None and not last.is_cooldown
        if hold:
            self.port.target(float(last.target_c))
        self._safe_shutdown(keep_running=hold)
        self._msg("info", "레시피 완료" + (" (히터 계속 운전)" if hold else ""))
        self._finish(True, "", "레시피 완료")

    def on_status(self, st: dict) -> None:
        """PLC 값마다 — RAMP 끝(sv_ramp 가 목표에 닿음) → SOAK. 이상·외부 OFF·TC 는 SafetyEngine(S16·S18A~C)."""
        if self.state != self.RAMPING or self.held:
            return
        s = self.current()
        pv, svr = st.get("pv"), st.get("sv_ramp")
        if s is None or pv is None:
            return
        if svr is not None:
            arrived = abs(float(svr) - s.target_c) <= float(self.cfg["HEATER_RAMP_DONE_TOL_C"])
        else:
            arrived = abs(float(pv) - s.target_c) <= float(self.cfg["HEATER_SOAK_TOLERANCE"])
        if arrived:
            self._msg("info", f"스텝 {self.idx + 1}: {s.target_c:g}°C RAMP 완료 (현재 {float(pv):.1f}°C)")
            self._enter_soak(s)

    def tick(self, st: dict) -> None:
        if self.held:
            return
        now = self.now()
        if self.state == self.RAMPING:
            self.ramp.tick()
            if now - self.step_started > self.step_timeout:
                self.abort("HEATER_WAIT_TIMEOUT", f"스텝 {self.idx + 1} RAMP 시간 초과 ({self.step_timeout:.0f}초)")   # S18D
        elif self.state == self.SOAKING:
            if self.soak_deadline - now <= 0:
                self._advance(st)


# ───────────── 히터 가스(controller/heater_atmosphere.py) ─────────────
class Atmosphere:
    IDLE, PREPARING, READY, RELEASING = "IDLE", "PREPARING", "READY", "RELEASING"

    def __init__(self, cfg, clock, mfc, plc, msg, on_failed: Callable[[str], None]) -> None:
        self.cfg, self.clock, self.mfc, self.plc, self._msg, self._on_failed = cfg, clock, mfc, plc, msg, on_failed
        self.state = self.IDLE
        self.runner: Optional[StepRunner] = None
        self.channels: List[int] = []
        self.buttons: List[str] = []
        self.params: Dict[str, Any] = {}

    def is_active(self) -> bool:
        return self.state in (self.PREPARING, self.READY, self.RELEASING)

    def start(self, use_ar: bool, use_o2: bool, ar_flow: float, o2_flow: float, sp1: float) -> Optional[str]:
        """받아들이면 None, 아니면 거부 문구."""
        if self.state != self.IDLE:
            return f"이미 동작 중({self.state})"
        if not (use_ar or use_o2):
            return "가스를 하나 이상 선택"
        if (use_ar and ar_flow <= 0) or (use_o2 and o2_flow <= 0) or sp1 <= 0:
            return "유량·압력은 0 보다 커야 함"
        self.channels, self.buttons, flows = [], [], {}
        if use_ar:
            self.channels.append(1), self.buttons.append("Ar_Button")
            flows[1] = ar_flow
        if use_o2:
            self.channels.append(2), self.buttons.append("O2_Button")
            flows[2] = o2_flow
        self.params = {"use_ar": use_ar, "use_o2": use_o2, "ar_flow": ar_flow, "o2_flow": o2_flow, "sp1": sp1}
        steps = (build_gas_pre_steps(self.channels, "") + build_gas_intro_steps(self.channels, flows, self.buttons, "")
                 + build_pressure_stage_steps(1, sp1, f"히터 분위기(SP1={sp1:.2f})", self.cfg,
                                              float(self.cfg["HEATER_GAS_SP1_TIMEOUT_SEC"]), fail_on_timeout=True))
        self.runner = StepRunner(self.clock, steps, self.mfc, self.plc, name="heater_gas")
        self.state = self.PREPARING
        self._msg("info", f"[가스] 도입 시작 · 목표 {sp1:.2f} mTorr ({len(steps)}단계)")
        return None

    def release(self, reason: str) -> None:
        if self.state in (self.IDLE, self.RELEASING):
            return
        self.state = self.RELEASING
        self._msg("info", f"[가스] 해제: {reason}")
        self.runner = StepRunner(self.clock, build_gas_release_steps(self.channels, self.buttons), self.mfc, self.plc,
                                 name="heater_gas_release", continue_on_fail=True)

    def tick(self, view) -> None:
        if self.runner is None or self.state in (self.IDLE, self.READY):
            return
        self.runner.tick(view, self.clock.monotonic())
        if self.runner.failed:
            why = self.runner.failed
            self._msg("error", f"[가스] 실패: {why}")
            self.state, self.runner = self.IDLE, None
            self._on_failed(why)          # → SafetyEngine S27(유량 OFF·배기·차단밸브 닫기) — 여기서 따로 해제하지 않는다
        elif self.runner.finished:
            if self.state == self.RELEASING:
                self.state, self.runner = self.IDLE, None
                self._msg("info", "[가스] 해제 완료")
            else:
                self.state = self.READY
                self._msg("info", f"[가스] 준비 완료 — {self.params.get('sp1', 0.0):.2f} mTorr 유지")


# ───────────── 엔진 ─────────────
class HeaterEngine:
    def __init__(self, clock, cfg: Dict[str, Any], plc, mfc, on_event: Optional[Callable[[HeaterEvent], None]] = None,
                 process_active: Callable[[], bool] = lambda: False,
                 on_gas_failed: Callable[[str], None] = lambda why: None) -> None:
        self.clock, self.cfg, self.plc, self.mfc = clock, cfg, plc, mfc
        self.on_event = on_event
        self.process_active = process_active
        self.port = HeaterPort(plc)
        self._lock = threading.RLock()
        self.st: Dict[str, Any] = {}
        mono = clock.monotonic
        self.ramp = RampProfiler(cfg, self.port, lambda: self.st, self._msg, mono)
        self.hold = HeaterHold(cfg, self.port, self._msg, self._emit, mono)
        self.sat = SaturationGuard(cfg, self.port, self._msg, self._emit, mono)
        self.recipe = RecipeRunner(cfg, self.port, self.ramp, lambda: self.st, self._msg, self._emit, mono)
        self.atm = Atmosphere(cfg, clock, mfc, plc, self._msg, self._gas_failed)
        self._on_gas_failed = on_gas_failed
        self.owner: Optional[str] = None           # None / manual / recipe / process
        self.pending: Optional[tuple] = None        # 가스 준비 뒤 할 일 ("manual_on", v) / ("recipe",)
        self.atm_hold = False                       # 냉각 보호 가스 유지 중
        self._prev_run: Optional[bool] = None
        self.proc: Dict[str, Any] = {}              # 공정 도달·유지 대기 상태
        self.events: List[HeaterEvent] = []

    # ── 공용 ──
    def _emit(self, e: HeaterEvent) -> None:
        self.events.append(e)
        if self.on_event:
            try:
                self.on_event(e)
            except Exception:
                pass

    def _msg(self, level: str, text: str) -> None:
        self._emit(HeaterEvent("message", level, text))

    def _gas_failed(self, why: str) -> None:
        self.pending = None
        self._emit(HeaterEvent("gas", "error", f"히터 가스 실패: {why}", "HEATER_GAS_FAILED"))
        if self.recipe.is_running():
            self.recipe.abort("HEATER_GAS_FAILED", f"히터 가스 실패: {why}")
        self._on_gas_failed(why)

    @staticmethod
    def status_of(values: Dict[str, Any]) -> Dict[str, Any]:
        """PLC 폴링 값(heater.*) → v1 get_heater_status 모양."""
        g = lambda k: values.get(f"heater.{k}")
        st = {k: g(k) for k in ("pv", "pv2", "sv", "cur_sv", "sv_ramp", "sv2", "mv", "mv_limit", "run", "itl", "fault",
                                "ot", "tc_err", "wd_err", "pv_sel_eff", "sv_limit", "ot2_limit", "sv2_max")}
        st["ok"] = st["pv"] is not None
        return st

    def final_target(self, st: dict):
        """v1 _heater_final_target: 램프 목표 → 레시피 현재 스텝 목표 → st['sv']."""
        t = self.ramp.target()
        if t is not None:
            return float(t)
        s = self.recipe.current() if self.recipe.is_running() else None
        if s is not None:
            return float(s.target_c)
        return st.get("sv")

    @property
    def hold_label(self) -> str:
        if self.hold.is_holding():
            return f"holding_{self.hold.kind}"
        return "arming" if self.hold.state not in ("idle",) else "off"

    @property
    def recipe_running(self) -> bool:
        return self.recipe.is_running()

    @property
    def gas_state(self) -> str:
        return self.atm.state

    # ── PLC 폴링(250 ms) ──
    def on_poll(self, poll) -> None:
        with self._lock:
            st = self.status_of(poll.values)
            self.st = st
            run = bool(st.get("run"))
            edge = "rise" if run and self._prev_run is False else ("fall" if self._prev_run and not run else None)
            self._prev_run = run
            self.sat.tick(st, self.hold.is_holding(), self.hold.kind)
            self.hold.tick(st, self.final_target(st))
            self.recipe.on_status(st)
            self._gas_hold_tick(st, edge)

    def _gas_hold_tick(self, st: dict, edge) -> None:
        """v1 main._heater_gas_hold_tick — RUN 하강에서 뜨거우면 가스 유지, PV ≤ HEATER_GAS_HOLD_RELEASE_C 면 해제."""
        thr = float(self.cfg["HEATER_GAS_HOLD_RELEASE_C"])
        pv = st.get("pv")
        if self.process_active():
            return
        if edge == "fall" and self.atm.is_active():
            if pv is not None and pv <= thr:
                self.atm.release("히터 OFF")
            else:
                self.gas_hold()
        if st.get("run"):
            self.atm_hold = False
        if self.atm_hold:
            if self.atm.state != Atmosphere.READY:
                self.atm_hold = False
            elif pv is not None and pv <= thr:
                self.atm_hold = False
                self._msg("info", f"[히터] PV {pv:.1f}°C ≤ {thr:g}°C — 가스·압력 해제")
                self.atm.release(f"냉각 완료 {pv:.1f}°C")

    def gas_hold(self) -> None:
        """S26 HEATER_GAS_HOLD — 히터 가스가 잡혀 있으면 냉각될 때까지 유지(멱등)."""
        with self._lock:
            if self.atm.is_active() and not self.atm_hold:
                self.atm_hold = True
                self._msg("info", f"[히터] 히터 OFF — 가스·압력은 PV {self.cfg['HEATER_GAS_HOLD_RELEASE_C']:g}°C 이하가 될 때까지 유지")

    # ── 1초 tick ──
    def on_tick(self, view) -> None:
        with self._lock:
            if self.recipe.is_running():
                self.recipe.tick(self.st)
                if not self.recipe.is_running() and self.owner == "recipe":
                    self.owner = None
            elif self.ramp.is_active():
                self.ramp.tick()
            self.atm.tick(view)
            if self.pending and self.atm.state == Atmosphere.READY:
                what, self.pending = self.pending, None
                if what[0] == "manual_on":
                    self._manual_go(what[1])
                elif what[0] == "recipe":
                    self.recipe.start()

    # ── 수동 ──
    def manual_on(self, value: float, gas: Optional[dict] = None) -> Optional[str]:
        """HEATER_ON. 가스를 고르면 가스 준비 뒤 켠다(v1 _on_heater_onoff_toggled). 거부면 사유 코드."""
        with self._lock:
            if self.recipe.is_running():
                return "HEATER_RECIPE_RUNNING"
            if gas and (gas.get("use_ar") or gas.get("use_o2")) and self.atm.state != Atmosphere.READY:
                if self.process_active():
                    return "PROCESS_RUNNING"
                why = self.atm.start(bool(gas.get("use_ar")), bool(gas.get("use_o2")), float(gas.get("ar_flow") or 0),
                                     float(gas.get("o2_flow") or 0), float(gas.get("wp_mtorr") or 0))
                if why:
                    return "BAD_ARGS"
                self.owner, self.pending = "manual", ("manual_on", float(value))
                return None
            self.owner = "manual"
            self._manual_go(float(value))
            return None

    def _manual_go(self, v: float) -> None:
        if not self.ramp.start(v, float(self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]), "수동"):
            self.port.target(v)
        self.port.run(True)

    def manual_off(self) -> None:
        with self._lock:
            if self.pending is not None:
                self.pending = None
                self.atm.release("사용자 취소")
                return
            self.ramp.stop()
            self.port.run(False)
            if self.owner == "manual":
                self.owner = None

    def set_sv(self, v: float) -> None:
        with self._lock:
            if self.st.get("run"):
                self.ramp.stop(restore_rate=False)
                if not self.ramp.start(v, float(self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]), "수동"):
                    self.port.target(v)
            else:
                self.ramp.stop()
                self.port.target(v)

    def gas_release(self) -> None:
        with self._lock:
            self.atm_hold = False
            self.atm.release("사용자 해제")

    # ── 히터 레시피 ──
    def recipe_run(self, name: str, steps: List[RecipeStep], gas: Optional[dict] = None) -> Optional[str]:
        with self._lock:
            if self.recipe.is_running():
                return "HEATER_RECIPE_RUNNING"
            self.ramp.stop()
            self.recipe.load(name, steps)
            self.owner = "recipe"
            if gas and (gas.get("use_ar") or gas.get("use_o2")) and self.atm.state != Atmosphere.READY:
                why = self.atm.start(bool(gas.get("use_ar")), bool(gas.get("use_o2")), float(gas.get("ar_flow") or 0),
                                     float(gas.get("o2_flow") or 0), float(gas.get("wp_mtorr") or 0))
                if why:
                    return "BAD_ARGS"
                self.pending = ("recipe",)
                return None
            self.recipe.start()
            return None

    def recipe_stop(self, reason: str = "중단") -> None:
        with self._lock:
            if self.pending and self.pending[0] == "recipe":
                self.pending = None
                self.atm.release("레시피 취소")
            self.recipe.stop(reason)
            if self.owner == "recipe":
                self.owner = None

    def recipe_abort(self, code: str) -> None:
        """HEATER_RECIPE_ABORT(SafetyEngine S16·S18A~C, ALL STOP)."""
        with self._lock:
            if code == "ALL_STOP":
                self.recipe_stop("비상 정지")
            else:
                self.recipe.abort(code)
            if self.owner == "recipe" and not self.recipe.is_running():
                self.owner = None

    # ── 공정 요청(ProcessEngine) ──
    def process_ramp(self, rate_c: float) -> None:
        with self._lock:
            self.proc = {"rate": float(rate_c or 0.0)}

    def process_set(self, target: float) -> None:
        """HEATER_SET — 감속 접근으로 올리고 RUN(v1 _on_process_heater_target·_on_process_heater_run)."""
        with self._lock:
            self.owner = "process"
            rate = self.proc.get("rate") or float(self.cfg["HEATER_RAMP_RATE_C_PER_MIN"])
            self.ramp.stop(restore_rate=False)
            if not self.ramp.start(float(target), rate, "공정"):
                self.port.target(float(target))
            self.port.run(True)
            self.proc.update(target=float(target), phase="RAMP", t0=None)

    def process_wait_tick(self, now: float):
        """HEATER_WAIT — v1 _heater_wait + _heater_hold_wait. 'wait' | 'reached' | ('fail', 코드).
        phase: RAMP(목표 ±tol 밖·램프 중) → SOAK(±tol 을 HEATER_SOAK_TIME_SEC 유지 확인) → HOLD_WAIT(유지 모드 진입 대기·폴백)."""
        with self._lock:
            p, c, st = self.proc, self.cfg, self.st
            target = float(p["target"])
            if p.get("t0") is None:
                p["t0"] = now
                rate = p.get("rate") or float(c["HEATER_RAMP_RATE_C_PER_MIN"])
                pv = st.get("pv")
                to = float(c["HEATER_WAIT_TIMEOUT_SEC"])
                if pv is not None:
                    need = ramp_minutes(abs(target - float(pv)), rate, float(c["HEATER_APPROACH_ZONE_C"]),
                                        float(c["HEATER_APPROACH_MIN_RATE_C_PER_MIN"])) * 60.0
                    to = min(max(to, need + float(c["HEATER_WAIT_TIMEOUT_MARGIN_SEC"])), float(c["HEATER_WAIT_TIMEOUT_MAX_SEC"]))
                p["timeout"] = to
            if p["phase"] in ("RAMP", "SOAK"):
                if st.get("fault"):
                    return ("fail", "HEATER_FAULT")
                if now - p["t0"] > p["timeout"]:
                    return ("fail", "HEATER_WAIT_TIMEOUT")                    # S18E
                pv, svr = st.get("pv"), st.get("sv_ramp")
                ramp_done = True if svr is None else abs(float(svr) - target) <= float(c["HEATER_RAMP_DONE_TOL_C"])
                if pv is not None and ramp_done and abs(float(pv) - target) <= float(c["HEATER_SOAK_TOLERANCE"]):
                    if p["phase"] == "RAMP":
                        p["phase"], p["band_t0"] = "SOAK", now
                    elif now - p["band_t0"] >= float(c["HEATER_SOAK_TIME_SEC"]):
                        return self._hold_wait_enter(now)
                else:
                    p["phase"] = "RAMP"
                return "wait"
            if p["phase"] == "HOLD_WAIT":
                return self._hold_wait_tick(now)
            return "reached" if p["phase"] == "DONE" else ("fail", "HEATER_HOLD_FAILED")

    def _hold_wait_enter(self, now: float):
        p, c = self.proc, self.cfg
        mode, wait = str(c["HEATER_HOLD_MODE"]), float(c["HEATER_HOLD_WAIT_SEC"])
        if mode == "off" or wait <= 0 or self.hold.is_holding():
            p["phase"] = "DONE"
            self._emit(HeaterEvent("reached", detail=f"{p['target']:.1f}°C"))
            return "reached"
        p.update(phase="HOLD_WAIT", hold_t0=now, force=None, force_t0=None)
        self._msg("info", f"목표 도달 — 유지 모드({mode}) 진입 대기 시작 (상한 {wait:.0f}s)")
        return "wait"

    def _hold_wait_tick(self, now: float):
        p, c, st = self.proc, self.cfg, self.st
        if st.get("fault"):
            return ("fail", "HEATER_FAULT")
        if self.hold.is_holding():
            p["phase"] = "DONE"
            self._emit(HeaterEvent("reached", detail=f"{p['target']:.1f}°C 유지 모드 {self.hold.kind}"))
            return "reached"
        f = p.get("force")
        if f is None:
            if not self.hold.gave_up and now - p["hold_t0"] < float(c["HEATER_HOLD_WAIT_SEC"]):
                return "wait"
            ok = self.hold.force_tc2_hold()                                # ① 완화 tc2
            p.update(force="tc2" if ok else "dac_try", force_t0=now, tc2_why=None if ok else self.hold.last_force_reason)
            if ok:
                return "wait"
            f = "dac_try"
        if f == "tc2":
            if now - p["force_t0"] <= 2 * float(c["HEATER_HOLD_ENGAGE_TIMEOUT_SEC"]) + 3.0:
                return "wait"
            p["tc2_why"] = "핸드셰이크 실패(holding 전이 없음)"
            f = "dac_try"
        if f == "dac_try":
            ok = self.hold.force_dac_hold(st.get("mv"))                    # ② dac
            if ok:
                p["phase"] = "DONE"
                self._emit(HeaterEvent("alert", "warn", f"TC2 추종 불가({p.get('tc2_why')}) → DAC 상한 강제 고정 후 진행",
                                       "HEATER_HOLD_DEMOTED"))
                return "reached"
            action = str(c["HEATER_HOLD_FAIL_ACTION"])                     # ③
            self._emit(HeaterEvent("alert", "warn", f"유지 모드 진입 실패 — tc2 불가({p.get('tc2_why')}) / "
                                   f"dac 불가({self.hold.last_force_reason}) → {action}", "HEATER_HOLD_FAILED"))
            if action == "proceed":
                p["phase"] = "DONE"
                return "reached"
            p["phase"] = "FAILED"
            return ("fail", "HEATER_HOLD_FAILED")
        return "wait"

    @property
    def process_phase(self) -> str:
        return self.proc.get("phase", "")

    def process_off(self) -> None:
        """v1 _heater_off — RUN=0, 램프 속도 원복(공정이 히터를 썼을 때만 부른다)."""
        with self._lock:
            self.ramp.stop(restore_rate=False)
            self.port.run(False)
            self.port.ramp(ramp_raw(self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]))
            if self.owner == "process":
                self.owner = None
            self.proc = {}
