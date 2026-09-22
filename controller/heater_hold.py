# controller/heater_hold.py
"""목표 도달 후 유지 모드 상태기 — DAC 상한 고정("dac") / TC2 추종("tc2").

main.update_heater_display 가 PLC 폴링(200ms)마다 tick(st, final_target) 을 부른다.
결정은 전부 여기서 하고, PLC 쓰기는 시그널로만 요청한다(request_mv_limit / request_sv2 / request_pv_sel).
main 은 생성·시그널 연결·tick 호출·상태 조회만 한다.

공용 도달 판정(두 모드 동일, 2026-09-16 실측 3건 반영):
  램프 종료 → |PV−final| ≤ ARRIVE_TOL 에 한 번 들어와야 잰다(도달 래치) → |PV−SV| ≤ ENTER_TOL 이
  ENTER_SEC 연속·표본 4개 이상 → 창 전·후반 평균 드리프트(PV/MV, tc2 는 TC2 도) 안이면 캡처.
  평균 MV ≥ 상한이면 고정하지 않고(평형 미도달), ≤ MV_MIN+20 이면 고정하지 않는다(바닥 캡처).
dac : D00018 ← 창 평균 MV. 해제 시 HEATER_MV_LIMIT 원복.
tc2 : D00035 ← min(창 평균 TC2, OT2−마진) → 되읽기 일치 → M0004A ON → M0004B 확인 → holding.
      해제는 반드시 M0004A OFF 먼저, 그다음 D00035=0.

폴백 순서(2026-09-22, process_controller 가 HEATER_HOLD_WAIT_SEC 안에 holding 을 못 보면):
  1) 엄격 캡처 성공 → tc2 (정상)   2) force_tc2_hold(): 최근 ENTER_SEC 링버퍼 TC2 평균으로 완화 tc2 진입
  3) tc2 불가(센서 없음/래더 미지원/핸드셰이크 실패) → force_dac_hold()   4) 그것도 불가 → HEATER_HOLD_FAIL_ACTION
  TC2 추종에 필요한 것은 정밀한 SV2 가 아니라 대략 맞는 SV2 다(894°C 에서 ±10°C = 1%). 드리프트 게이트는 SV2 정밀도를
  높이는 장치이지 추종의 전제 조건이 아니다. dac 고정은 PID 입력이 TC1 인 채로의 정전력 운전이라 최후 수단이다.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable, Optional

from PyQt6.QtCore import QObject, pyqtSignal as Signal

from lib.config import heater_est_current

# ── 진입 게이트의 유효 허용치(config 값은 하한, 아래 비율이 실제 판정 폭) — 2026-09-21/22 실측 근거 ──
#  09-21(CeO2 1-6): 창 드리프트 PV +0.47 / MV +8.9 / TC2 -0.50 → 절대 게이트(0.5/15/1.0)를 6% 차이로 통과(운).
#  09-22(CeO2 1-7): 6개 창 전부 탈락(PV -1.01~+1.21, MV ±13~35, TC2 -3.6~+13.9) → 유지 모드 실패 → 셔터 개방 뒤
#  DAC 1200 포화 25분. 이 루프의 평상시 진동폭(MV p-p 35~83, PV p-p 2.4~3.6)이 게이트보다 컸다.
#  캡처하는 값은 "TC1 을 목표에 붙들던 열원의 세기"라 몇십 카운트·몇 도의 오차는 무의미하다 — 입구는 넓게.
#  절대값이 아니라 ENTER_TOL / MV_LIMIT / TC2 실측에 대한 비율이라 목표 온도·시료대(TC2 위치)가 바뀌어도 같이 따라간다.
DRIFT_PV_REL  = 0.5   # 이미 |PV-SV| <= ENTER_TOL 을 요구한다. 그 절반이면 정착으로 본다(기본 1.5°C)
DRIFT_MV_REL  = 0.05  # 상한의 5%(기본 60카운트). 이 루프는 평상시 p-p 35~83 카운트로 흔들린다
DRIFT_TC2_REL = 0.02  # TC2 890°C 에서 약 18°C. SV2 로 쓸 값에 2% 오차는 무의미하다
#  6°C/min 램프 중이면 60초 창의 전·후반 평균차가 약 3°C 라 PV 게이트(1.5°C)에서 여전히 탈락한다.
MV_SAT_REL    = 0.98  # 창 평균 MV 가 상한의 98% 이상이면 "히터가 목표를 유지할 능력이 없다" — 재시도로 해결되지 않는다(give_up)

ENGAGE_TIMEOUT_SEC = 5.0     # engaging 각 단계 대기 상한  # 미확인: M0004A ON 뒤 래더가 M0004B 를 올리기까지의 스캔 지연(5초면 충분하다고 가정)
REAPPLY_SEC        = 5.0     # 유지 중 값이 되돌아가 있으면 이만큼 지난 뒤 재적용
# tc2 유지 중 TC1 편차 안내 임계. 메인 셔터 개방 뒤 TC1 이 TC2 와 100°C 이상 벌어지는 것은 정상 물리 현상이다
#  (2026-09-21 정상 런 실측 -110.9°C). 20.0 은 모든 정상 공정에서 경고가 떠 실제 이상을 가렸다(2026-09-22 → 120.0).
#  안내일 뿐이다 — 해제 사유나 공정 중단 사유가 아니다.
TC1_DEV_WARN_C     = 120.0
TC1_DEV_WARN_SEC   = 300.0
TC1_DEV_OK_C       = 10.0


class HeaterHold(QObject):
    request_mv_limit = Signal(int)
    request_sv2      = Signal(float)
    request_pv_sel   = Signal(bool)
    message          = Signal(str, str)     # (레벨, 문구) → main 이 log_message_to_monitor 에 연결
    engaged          = Signal(str)          # 'tc2' | 'dac' — holding 전이 순간 1회 (완화 경로 여부는 engaged_relaxed 로 조회)
    give_up          = Signal(str)          # 진입 불가 확정(dac: 창 평균 MV 가 상한 98% 이상) 사유 1회. mode='off' 는 해당 없음
    alert            = Signal(str, dict)    # 챗 카드가 필요한 사건: "demoted"(tc2→dac 강등) / "demote_failed" / "no_margin"(MV≥98%)

    def __init__(self, mode: str, *, mv_limit: int, mv_min: int,
                 enter_tol_c: float, enter_sec: float, arrive_tol_c: float,
                 drift_pv_c: float, drift_mv: float,
                 tc2_margin_c: float, tc2_drift_c: float,
                 clock: Callable[[], float] = time.monotonic, parent=None):
        super().__init__(parent)
        self.mode = str(mode)
        self.mv_limit = int(mv_limit); self.mv_min = int(mv_min)
        self.enter_tol = float(enter_tol_c); self.enter_sec = float(enter_sec)
        self.arrive_tol = float(arrive_tol_c)
        self.drift_pv = float(drift_pv_c); self.drift_mv = float(drift_mv)
        self.tc2_margin = float(tc2_margin_c); self.tc2_drift = float(tc2_drift_c)
        self._now = clock
        self._h = self._fresh()
        # 완화 캡처용 링버퍼 — 드리프트 실패로 samples 가 비워져도 남는다. ok(램프 완료·arrived·|PV-SV|≤ENTER_TOL) 동안만 적재
        self._ring: deque = deque()          # (t, mv, pv, pv2)
        self._last_st: dict = {}
        self.engaged_relaxed: bool = False   # 마지막 holding 진입이 완화 경로(force_tc2_hold)였는가
        self.last_force_reason: str = ""     # force_*_hold 가 False 를 돌려준 사유

    # ───────────────────────── 상태 ─────────────────────────
    @staticmethod
    def _fresh() -> dict:
        return {'state': 'idle', 'kind': None, 'samples': [], 't0': 0.0, 'sv': None, 'value': None,
                'sv2': None, 'final': None, 'last_push': 0.0, 'engage_t0': 0.0, 'engage_warned': False,
                'arrived': False, 'arr_sv': None, 'floor_warned': False, 'limit_warned': False,
                'drift_log_t': 0.0, 'alt_reason': None, 'dev_t0': 0.0, 'dev_warned': False,
                'reapply_t0': 0.0, 'last_window_mv': None, 'last_sv': None, 'gave_up': None}

    @property
    def state(self) -> str:
        return self._h['state']

    @property
    def kind(self) -> Optional[str]:
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
    def gave_up(self):
        """진입 불가 확정 사유(give_up 발행 뒤 보관). 다시 arming 하거나 해제되면 None."""
        return self._h['gave_up']

    def effective_gates(self, tc2_mean=None):
        """(eff_pv, eff_mv, eff_tc2) — config 하한과 비율 허용치 중 큰 쪽."""
        eff_pv = max(self.drift_pv, DRIFT_PV_REL * self.enter_tol)
        eff_mv = max(self.drift_mv, DRIFT_MV_REL * self.mv_limit)
        eff_tc2 = max(self.tc2_drift, DRIFT_TC2_REL * float(tc2_mean)) if tc2_mean else self.tc2_drift
        return eff_pv, eff_mv, eff_tc2

    def ring_tc2_mean(self):
        vals = [r[3] for r in self._ring if r[3] is not None]
        return (sum(vals) / len(vals), len(vals)) if vals else (None, 0)

    def force_tc2_hold(self) -> bool:
        """공정이 정착 창을 기다리다 포기했을 때: 최근 ENTER_SEC 링버퍼의 TC2 평균으로 tc2 추종에 완화 진입한다.
        드리프트 게이트·MV 상한/바닥 판정은 건너뛴다. 조건(전부): _effective_kind=='tc2', 유효 TC2 표본 ≥4,
        지금(마지막 폴링) |TC1−SV| ≤ ENTER_TOL. 성공하면 engaging_sv2 → engaging_sel 핸드셰이크를 그대로 탄다(True 반환은
        핸드셰이크 시작이지 holding 이 아니다). 실패 사유는 last_force_reason."""
        h = self._h; st = self._last_st
        if h['state'] == 'holding':
            return True
        if self.mode == 'off':
            self.last_force_reason = "유지 모드 꺼짐(HEATER_HOLD_MODE=off)"; return False
        kind, alt = self._effective_kind(st)
        if kind != 'tc2':
            self.last_force_reason = (alt or "TC2 추종 불가")
            return False
        mean2, n2 = self.ring_tc2_mean()
        if mean2 is None or n2 < 4:
            self.last_force_reason = f"TC2 표본 부족({n2}개 < 4)"; return False
        pv_now = st.get('pv') if st.get('pv') is not None else self._ring[-1][2]   # 지금 TC1(마지막 폴링)
        sv = h['last_sv']
        if sv is None or pv_now is None or abs(float(pv_now) - float(sv)) > self.enter_tol:
            self.last_force_reason = f"TC1 이 목표 근처가 아님(|TC1−SV| > {self.enter_tol:g})"; return False
        if h['state'] in ('engaging_sv2', 'engaging_sel'):
            return True                                      # 이미 핸드셰이크 중
        cap = float(st.get('ot2_limit') or 0.0) - self.tc2_margin
        sv2 = round(min(mean2, cap), 1)
        if mean2 > cap:
            self._msg("히터", f"TC2 목표를 OT2−마진으로 제한 {mean2:.1f} → {sv2:.1f}°C")
        h.update(state='engaging_sv2', kind='tc2', sv2=sv2, final=h['arr_sv'] if h['arr_sv'] is not None else sv,
                 value=None, engage_t0=self._now(), samples=[], t0=0.0, gave_up=None, engage_warned=False)
        self.engaged_relaxed = True
        self.request_sv2.emit(float(sv2))
        self._msg("히터(경고)", f"완화 조건으로 TC2 추종 진입 — SV2 {sv2:.1f}°C (정착 창 미확보, 최근 {self.enter_sec:.0f}초 평균 {n2}표본)")
        return True

    def force_dac_hold(self, mv_value=None) -> bool:
        """공정이 유지 모드 진입을 기다리다 포기했을 때: 마지막 측정 창의 평균 MV(없으면 인자의 현재 MV)로
        dac 유지에 강제 진입한다. 값이 없으면 False. 클램프는 _capture_dac 과 같다(MV_MIN+20 ~ MV_LIMIT)."""
        h = self._h
        if h['state'] == 'holding':
            return True
        src = h['last_window_mv']
        if src is None and mv_value is not None:
            try:
                src = float(mv_value)
            except Exception:
                src = None
        if src is None:
            self.last_force_reason = "측정 창 평균도 현재 MV 도 없음"
            return False
        if h['state'] in ('engaging_sv2', 'engaging_sel'):
            self.request_pv_sel.emit(False); self.request_sv2.emit(0.0)      # tc2 진입 중이면 순서대로 되돌린다
        sv = h['last_sv']
        value = max(int(self.mv_min) + 20, min(int(self.mv_limit), int(round(src))))
        h.update(state='holding', kind='dac', sv=sv, value=value, last_push=self._now(), samples=[], t0=0.0,
                 engage_t0=0.0, gave_up=None)
        self.engaged_relaxed = True
        self.request_mv_limit.emit(value)
        self._msg("히터(경고)", f"유지 모드 강제 진입 — DAC 상한 {value} 고정 (≒{heater_est_current(value):.0f}A, "
                              f"{'마지막 측정 창 평균' if h['last_window_mv'] is not None else '현재 MV'})")
        self.engaged.emit('dac')
        return True

    def log_tuple(self):
        """heater_logger 의 (hold, hold_mv) — hold 는 어느 모드든 holding 이면 1, hold_mv 는 dac 값."""
        h = self._h
        return (1 if h['state'] == 'holding' else 0, h['value'] if h['kind'] == 'dac' else None)

    def _msg(self, level: str, text: str) -> None:
        self.message.emit(level, text)

    def _reset(self, keep_arrived: bool) -> None:
        h = self._h
        arrived, arr_sv = h['arrived'], h['arr_sv']
        limit_w, floor_w = h['limit_warned'], h['floor_warned']
        drift_t = h['drift_log_t']
        lw, lsv = h['last_window_mv'], h['last_sv']
        self._h = self._fresh()
        self._h.update(last_window_mv=lw, last_sv=lsv)       # force_dac_hold 가 쓸 마지막 창 평균은 남긴다
        if keep_arrived:
            self._h.update(arrived=arrived, arr_sv=arr_sv, limit_warned=limit_w,
                           floor_warned=floor_w, drift_log_t=drift_t)

    # ───────────────────────── 해제 ─────────────────────────
    def release(self, why: str) -> None:
        """유지/진입 중이면 PLC 값을 원복하고 idle 로. tc2 는 M0004A OFF 가 D00035=0 보다 먼저다."""
        h = self._h
        st_, kind = h['state'], h['kind']
        if st_ == 'holding':
            if kind == 'dac':
                self.request_mv_limit.emit(int(self.mv_limit))
                self._msg("히터", f"DAC 상한 고정 해제 → {int(self.mv_limit)} 원복 ({why})")
            else:
                self.request_pv_sel.emit(False)
                self.request_sv2.emit(0.0)
                self._msg("히터", f"TC2 추종 해제 → TC1 제어 복귀 ({why})")
        elif st_ in ('engaging_sv2', 'engaging_sel'):
            self.request_pv_sel.emit(False)
            self.request_sv2.emit(0.0)
            self._msg("히터", f"TC2 추종 진입 취소 → TC1 제어 유지 ({why})")
        elif h['samples']:
            self._msg("히터", f"유지 모드 대기 취소 ({why})")
        # 해제 뒤 다시 운전하면 경고들을 다시 낼 수 있어야 하고, 도달 래치도 처음부터 다시 잰다
        self._reset(keep_arrived=False)

    # ───────────────────────── kind ─────────────────────────
    def _effective_kind(self, st: dict):
        """(kind, 대체 사유). mode tc2 인데 래더/센서가 못 받쳐 주면 dac 로 대체한다."""
        if self.mode == 'off':
            return None, None
        if self.mode == 'dac':
            return 'dac', None
        try:
            sv2_max = float(st.get('sv2_max') or 0.0); ot2 = float(st.get('ot2_limit') or 0.0)
        except Exception:
            sv2_max, ot2 = 0.0, 0.0
        if sv2_max <= 0 or ot2 <= 0:
            return 'dac', "래더가 TC2 추종을 지원하지 않음(D00036=0) — DAC 상한 고정으로 대체"
        if st.get('pv2') is None:
            return 'dac', "TC2 값 없음(D00011=-1) — DAC 상한 고정으로 대체"
        return 'tc2', None

    # ───────────────────────── tick ─────────────────────────
    def tick(self, st: dict, final_target=None) -> None:
        try:
            self._tick(st, final_target)
        except Exception as e:
            self._msg("경고", f"유지 모드 처리 예외: {e!r}")

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

        run = bool(st.get('run')); fault = bool(st.get('fault'))
        sv = st.get('sv'); pv = st.get('pv'); svr = st.get('sv_ramp'); mv = st.get('mv')
        if sv is not None:
            h['last_sv'] = float(sv)

        # ── 해제 조건(공용) ──
        if not run:
            if h['state'] != 'idle' or h['samples']:
                self.release("운전 OFF")
            h['last_window_mv'] = None                        # 다음 운전에 이전 창 평균을 물고 가지 않는다
            self._ring.clear()
            return
        if fault:
            if h['state'] != 'idle' or h['samples']:
                self.release("히터 이상")
            return

        # 최종 목표(램프 중에는 st['sv'] 가 중간값일 수 있다)
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

        # ── 도달/진입 판정(공용, 기존 로직 그대로) ──
        base_ok = (bool(st.get('ok')) and pv is not None and sv is not None and svr is not None and mv is not None
                   and float(sv) > 0)
        ramp_done = (base_ok and final is not None
                     and abs(float(svr) - float(sv)) <= 0.05
                     and abs(float(sv) - final) <= 0.05)
        if base_ok and final is not None and h['arrived'] and h['arr_sv'] is not None and abs(final - float(h['arr_sv'])) > 0.05:
            h.update(arrived=False, arr_sv=None, samples=[], t0=0.0, state='idle')
        if ramp_done and not h['arrived'] and abs(float(pv) - final) <= self.arrive_tol:
            h['arrived'] = True; h['arr_sv'] = final
            self._msg("히터", f"목표 도달 확인 — 유지 모드 측정 시작 (TC1 {float(pv):.1f} / SV {final:.1f})")

        ok = (ramp_done and bool(h['arrived']) and abs(float(pv) - float(sv)) <= self.enter_tol)
        if ok:
            # 완화 캡처용 링버퍼(최근 ENTER_SEC) — samples 와 달리 드리프트 실패로 비우지 않는다
            _pv2 = st.get('pv2')
            self._ring.append((now, float(mv), float(pv), None if _pv2 is None else float(_pv2)))
            while self._ring and (now - self._ring[0][0]) > self.enter_sec:
                self._ring.popleft()
        if not ok:
            if h['samples']:
                h['samples'] = []; h['t0'] = 0.0; h['state'] = 'idle'; h['alt_reason'] = None
            h['limit_warned'] = False; h['floor_warned'] = False
            return
        kind, alt = self._effective_kind(st)
        if alt is not None and h['alt_reason'] != alt:
            h['alt_reason'] = alt
            self._msg("히터", alt)
        elif alt is None:
            h['alt_reason'] = None
        if not h['samples']:
            h['t0'] = now; h['state'] = 'arming'; h['gave_up'] = None
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
        d_mv = sum(s[0] for s in second) / len(second) - sum(s[0] for s in first) / len(first)
        d_pv = sum(s[1] for s in second) / len(second) - sum(s[1] for s in first) / len(first)
        d_pv2 = 0.0
        if kind == 'tc2':
            f2 = [s[2] for s in first if s[2] is not None]; s2 = [s[2] for s in second if s[2] is not None]
            if f2 and s2:
                d_pv2 = sum(s2) / len(s2) - sum(f2) / len(f2)
        tc2_mean = None
        if kind == 'tc2':
            vals2 = [s[2] for s in h['samples'] if s[2] is not None]
            tc2_mean = (sum(vals2) / len(vals2)) if vals2 else None
        eff_pv, eff_mv, eff_tc2 = self.effective_gates(tc2_mean)
        # 드리프트로 탈락하는 창도 "측정한 창"이다 — force_dac_hold 가 쓸 마지막 창 평균 MV 는 여기서 갱신한다
        h['last_window_mv'] = sum(s[0] for s in h['samples']) / n
        if abs(d_pv) > eff_pv or abs(d_mv) > eff_mv or (kind == 'tc2' and abs(d_pv2) > eff_tc2):
            h['samples'] = []; h['t0'] = now                # 처음부터 다시 잰다 (arming/arrived 유지)
            if (now - h['drift_log_t']) >= 30.0:
                h['drift_log_t'] = now
                extra = f" / TC2 드리프트 {d_pv2:+.2f}°C(허용 {eff_tc2:.1f})" if kind == 'tc2' else ""
                self._msg("히터", f"목표 근처지만 아직 정착 전 — 재측정 (PV 드리프트 {d_pv:+.2f}°C(허용 {eff_pv:.1f}) / "
                                  f"MV 드리프트 {d_mv:+.1f}(허용 {eff_mv:.0f}){extra})")
            return
        value = int(round(sum(s[0] for s in h['samples']) / n))
        if value >= MV_SAT_REL * float(self.mv_limit):
            if kind == 'dac':
                # 상한 근처에서 상한을 고정하는 것은 의미가 없다 — 재시도로 해결되지 않는다. 공정이 알 수 있게 give_up
                why = (f"도달 시점 출력이 상한에 붙어 있어 고정하지 않음 (평균 MV {value} ≥ {int(self.mv_limit)}×{MV_SAT_REL:g}) "
                       f"— 히터가 목표를 유지할 여유가 없습니다")
                if not h['limit_warned']:
                    h['limit_warned'] = True
                    self._msg("히터(경고)", why)
                    h['gave_up'] = why
                    self.give_up.emit(why)
                h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)   # arrived 유지
                return
            # tc2: 여유가 없을수록 능동 조절이 더 필요하다 — 포기하지 않고 현재 상태를 TC2 로 고정한다(안내 1회)
            #  (목표 도달 실패는 승온 타임아웃이 판단할 일이지 유지 모드가 판단할 일이 아니다)
            if not h['limit_warned']:
                h['limit_warned'] = True
                why = (f"도달 시점 출력이 상한의 {MV_SAT_REL * 100:.0f}% 이상입니다 (평균 MV {value} / 상한 {int(self.mv_limit)}) "
                       f"— 히터에 여유가 없습니다. TC2 추종으로 현재 상태를 고정합니다")
                self._msg("히터(경고)", why)
                self.alert.emit("no_margin", {"mv": value, "limit": int(self.mv_limit), "why": why})
        if value <= int(self.mv_min) + 20:
            if not h['floor_warned']:
                h['floor_warned'] = True
                self._msg("히터(경고)", f"도달 시점 출력이 최소치라 고정하지 않음 (평균 MV {value} ≤ {int(self.mv_min) + 20}) "
                                     f"— PID 가 출력을 내지 않는 상태입니다")
            h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)   # arrived 유지
            return
        h['floor_warned'] = False
        if kind == 'dac':
            h['limit_warned'] = False
            self._capture_dac(h, now, value, pv, sv)
        else:
            self._capture_tc2(h, st, now, final, pv)

    # ───────────────────────── dac ─────────────────────────
    def _capture_dac(self, h: dict, now: float, value: int, pv, sv) -> None:
        # PLC.set_heater_mv_limit 과 같은 식으로 클램프해서 저장한다(안 맞추면 5초마다 재적용이 반복된다)
        raw_value = value
        value = max(int(self.mv_min) + 20, min(int(self.mv_limit), value))
        if value != raw_value:
            self._msg("히터", f"DAC 상한 고정값 클램프 {raw_value} → {value} (PLC 하한 HEATER_MV_MIN+20)")
        h.update(state='holding', kind='dac', sv=float(sv), value=value, last_push=now, samples=[], t0=0.0)
        self.engaged_relaxed = False
        self.request_mv_limit.emit(value)
        amps = heater_est_current(value)
        self._msg("히터", f"히터 DAC 상한 {value} 고정 (≒{amps:.0f}A, PV {float(pv):.1f} / SV {float(sv):.1f})")
        self.engaged.emit('dac')

    def _tick_holding_dac(self, st: dict, now: float, sv) -> None:
        h = self._h
        if sv is None or h['sv'] is None or abs(float(sv) - float(h['sv'])) > 0.05:
            self.release(f"목표 변경 {h['sv']} → {sv}")
            return
        # 유지 중 재적용 — PLC 재기동 등으로 D00018 이 되돌아간 경우(5초 간격)
        try:
            cur_lim = int(st.get('mv_limit')) if st.get('mv_limit') is not None else None
        except Exception:
            cur_lim = None
        if cur_lim is not None and cur_lim != int(h['value']) and (now - h['last_push']) >= REAPPLY_SEC:
            h['last_push'] = now
            self.request_mv_limit.emit(int(h['value']))
            self._msg("히터", f"DAC 상한 재적용 {h['value']} (읽힌 D00018={cur_lim})")

    # ───────────────────────── tc2 ─────────────────────────
    def _capture_tc2(self, h: dict, st: dict, now: float, final, pv) -> None:
        vals = [s[2] for s in h['samples'] if s[2] is not None]
        if not vals:
            h.update(state='idle', samples=[], t0=0.0)
            return
        avg2 = sum(vals) / len(vals)
        cap = float(st.get('ot2_limit') or 0.0) - self.tc2_margin
        sv2 = round(min(avg2, cap), 1)
        if avg2 > cap:
            self._msg("히터", f"TC2 목표를 OT2−마진으로 제한 {avg2:.1f} → {sv2:.1f}°C")
        # 미확인: D00035 는 RUN 중에도 언제든 써도 되는지(래더가 M0004B 일 때만 읽는다고 가정), D00036 클램프는 래더가 한다
        h.update(state='engaging_sv2', kind='tc2', sv2=sv2, final=final, value=None,
                 engage_t0=now, samples=[], t0=0.0)
        self.engaged_relaxed = False
        self.request_sv2.emit(float(sv2))

    def _demote_to_dac(self, why: str) -> None:
        """tc2 유지/진입 중 TC2 를 못 쓰게 됐다(D00011=-1 / M0004B OFF): TC1 제어로 조용히 복귀하면 셔터가 열린 공정에서
        PID 가 즉시 DAC 를 상한까지 민다(2026-09-22). M0004A OFF → D00035=0 순서로 tc2 를 끊은 뒤 dac 유지로 강등한다.
        OT2 과온 보호도 함께 사라지므로 반드시 알린다. dac 도 불가하면 그때만 완전 해제하되 역시 알린다."""
        h = self._h
        was = h['state']
        self.request_pv_sel.emit(False)
        self.request_sv2.emit(0.0)
        self._reset(keep_arrived=True)
        ok = self.force_dac_hold(None if not self._last_st else self._last_st.get('mv'))
        base = f"{why} — TC2 추종 → DAC 상한 고정으로 강등"
        if ok:
            self.alert.emit("demoted", {"why": why, "value": self._h['value'], "from": was})
            self._msg("히터(경고)", f"{base} (D00018 {self._h['value']}). OT2 과온 보호도 함께 사라졌습니다. TC2 배선을 확인하십시오")
        else:
            self._msg("히터(경고)", f"{base} 실패({self.last_force_reason}) → TC1 제어로 복귀. 출력 상한이 {int(self.mv_limit)} 그대로입니다 — 확인 필요")
            self.alert.emit("demote_failed", {"why": why, "reason": self.last_force_reason, "from": was})

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
            try:
                back = float(st.get('sv2'))
            except Exception:
                back = None
            if back is not None and abs(back - float(h['sv2'])) <= 0.05:
                h['state'] = 'engaging_sel'; h['engage_t0'] = now
                self.request_pv_sel.emit(True)
                return
        elif h['state'] == 'engaging_sel':
            if bool(st.get('pv_sel_eff')):
                h.update(state='holding', last_push=now, dev_t0=0.0, dev_warned=False, reapply_t0=0.0)
                pv = st.get('pv'); pv2 = st.get('pv2')
                self._msg("히터", f"TC2 추종 시작 — TC1 {float(pv):.1f} / TC2 {float(pv2):.1f} (SV2 {float(h['sv2']):.1f} 고정)")
                self.engaged.emit('tc2')
                return
        if (now - h['engage_t0']) > ENGAGE_TIMEOUT_SEC:
            step = "D00035 되읽기" if h['state'] == 'engaging_sv2' else "M0004B 확인"
            warn = not h['engage_warned']
            self.request_pv_sel.emit(False)
            self.request_sv2.emit(0.0)
            self._reset(keep_arrived=True)
            self._h['engage_warned'] = True
            if warn:
                self._msg("히터(경고)", f"TC2 추종 진입 실패 — {step} {ENGAGE_TIMEOUT_SEC:.0f}초 초과, TC1 제어로 되돌림")

    @staticmethod
    def _tc2_lost(why: str) -> bool:
        """(가) TC2 를 못 쓰게 된 사유(강등 대상) vs (나) 목표 변경/RUN OFF/fault(기존 해제)."""
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
        # 재적용 — D00035 가 되돌아간 상태가 5초 이상이면 다시 쓴다
        try:
            back = float(st.get('sv2'))
        except Exception:
            back = None
        if back is not None and abs(back - float(h['sv2'])) > 0.05:
            if h['reapply_t0'] == 0.0:
                h['reapply_t0'] = now
            elif (now - h['reapply_t0']) >= REAPPLY_SEC and (now - h['last_push']) >= REAPPLY_SEC:
                h['last_push'] = now; h['reapply_t0'] = 0.0
                self.request_sv2.emit(float(h['sv2']))
                self._msg("히터", f"TC2 목표 재적용 {float(h['sv2']):.1f}°C (읽힌 D00035={back:.1f})")
        else:
            h['reapply_t0'] = 0.0
        # TC1 편차 안내(해제 사유가 아니다 — 셔터로 TC1 이 튀는 상황이 바로 이 경우)
        if pv is None or final is None:
            return
        dev = float(pv) - float(final)
        if abs(dev) > TC1_DEV_WARN_C:
            if h['dev_t0'] == 0.0:
                h['dev_t0'] = now
            elif (now - h['dev_t0']) >= TC1_DEV_WARN_SEC and not h['dev_warned']:
                h['dev_warned'] = True
                self._msg("히터", f"TC2 추종 중 TC1 편차 {dev:+.1f}°C ({TC1_DEV_WARN_SEC / 60:.0f}분 경과) — 추종 유지")
        elif abs(dev) <= TC1_DEV_OK_C:
            if h['dev_warned']:
                self._msg("히터", "TC1 편차 정상 복귀")
            h['dev_warned'] = False; h['dev_t0'] = 0.0
