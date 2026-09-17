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
"""
from __future__ import annotations

import time
from typing import Callable, Optional

from PyQt6.QtCore import QObject, pyqtSignal as Signal

from lib.config import heater_est_current

ENGAGE_TIMEOUT_SEC = 5.0     # engaging 각 단계 대기 상한  # 미확인: M0004A ON 뒤 래더가 M0004B 를 올리기까지의 스캔 지연(5초면 충분하다고 가정)
REAPPLY_SEC        = 5.0     # 유지 중 값이 되돌아가 있으면 이만큼 지난 뒤 재적용
TC1_DEV_WARN_C     = 20.0    # tc2 유지 중 TC1 편차 안내 임계
TC1_DEV_WARN_SEC   = 300.0
TC1_DEV_OK_C       = 10.0


class HeaterHold(QObject):
    request_mv_limit = Signal(int)
    request_sv2      = Signal(float)
    request_pv_sel   = Signal(bool)
    message          = Signal(str, str)     # (레벨, 문구) → main 이 log_message_to_monitor 에 연결

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

    # ───────────────────────── 상태 ─────────────────────────
    @staticmethod
    def _fresh() -> dict:
        return {'state': 'idle', 'kind': None, 'samples': [], 't0': 0.0, 'sv': None, 'value': None,
                'sv2': None, 'final': None, 'last_push': 0.0, 'engage_t0': 0.0, 'engage_warned': False,
                'arrived': False, 'arr_sv': None, 'floor_warned': False, 'limit_warned': False,
                'drift_log_t': 0.0, 'alt_reason': None, 'dev_t0': 0.0, 'dev_warned': False,
                'reapply_t0': 0.0}

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
        self._h = self._fresh()
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
        if self.mode == 'off':
            if h['state'] == 'holding' or h['state'].startswith('engaging'):
                self.release("기능 꺼짐")
            elif h['state'] != 'idle' or h['samples'] or h['arrived']:
                self._reset(keep_arrived=False)
            return

        run = bool(st.get('run')); fault = bool(st.get('fault'))
        sv = st.get('sv'); pv = st.get('pv'); svr = st.get('sv_ramp'); mv = st.get('mv')

        # ── 해제 조건(공용) ──
        if not run:
            if h['state'] != 'idle' or h['samples']:
                self.release("운전 OFF")
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
            h['t0'] = now; h['state'] = 'arming'
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
        if abs(d_pv) > self.drift_pv or abs(d_mv) > self.drift_mv or (kind == 'tc2' and abs(d_pv2) > self.tc2_drift):
            h['samples'] = []; h['t0'] = now                # 처음부터 다시 잰다 (arming/arrived 유지)
            if (now - h['drift_log_t']) >= 30.0:
                h['drift_log_t'] = now
                extra = f" / TC2 드리프트 {d_pv2:+.2f}°C" if kind == 'tc2' else ""
                self._msg("히터", f"목표 근처지만 아직 정착 전 — 재측정 (PV 드리프트 {d_pv:+.2f}°C / MV 드리프트 {d_mv:+.1f}{extra})")
            return
        value = int(round(sum(s[0] for s in h['samples']) / n))
        if value >= int(self.mv_limit):
            if not h['limit_warned']:
                h['limit_warned'] = True
                self._msg("히터(경고)", f"경고: 도달 시점 출력이 상한과 같아 고정하지 않음 (평균 MV {value} ≥ {int(self.mv_limit)})")
            h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)   # arrived 유지
            return
        if value <= int(self.mv_min) + 20:
            if not h['floor_warned']:
                h['floor_warned'] = True
                self._msg("히터(경고)", f"도달 시점 출력이 최소치라 고정하지 않음 (평균 MV {value} ≤ {int(self.mv_min) + 20}) "
                                     f"— PID 가 출력을 내지 않는 상태입니다")
            h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)   # arrived 유지
            return
        h['limit_warned'] = False; h['floor_warned'] = False
        if kind == 'dac':
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
        self.request_mv_limit.emit(value)
        amps = heater_est_current(value)
        self._msg("히터", f"히터 DAC 상한 {value} 고정 (≒{amps:.0f}A, PV {float(pv):.1f} / SV {float(sv):.1f})")

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
        self.request_sv2.emit(float(sv2))

    def _tick_engaging(self, st: dict, now: float, final) -> None:
        h = self._h
        why = self._tc2_release_reason(st, final, check_eff=False)
        if why:
            self.release(why); return
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
            self.release(why); return
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
