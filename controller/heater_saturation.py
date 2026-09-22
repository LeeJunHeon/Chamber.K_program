# controller/heater_saturation.py
"""DAC 포화 감시 — 유지 모드(HeaterHold)와 독립된 안전망. HEATER_HOLD_MODE 가 무엇이든(off 포함) 항상 돈다.

2026-09-22(CeO2 1-7): 유지 모드 진입에 실패한 채 메인 셔터가 열려 TC1 이 600→529°C 로 급락 → PID 와인드업 →
DAC 1200(상한) 포화 25분, TC2 894→952°C. 히터 관련 로그·챗 0건 — 포화를 보는 코드가 없었다.
이 감시가 있었다면 12:52 경 잡혔다(실제 발견 13:21).

동작(main.update_heater_display 가 heater_hold.tick 직전에 tick(st, hold_active) 를 부른다):
  · hold_active(유지 모드가 이미 출력을 묶고 있다) 면 판정도 클램프도 하지 않는다 — D00018 을 두 주체가 쓰지 않는다.
  · RUN 중 hold 없이 mv >= MV_SAT_REL×mv_limit 가 sat_sec 연속이면 포화 1회: 경고 로그 + saturated(dict)(챗 카드용)
    + D00018 ← 포화 직전 60초 평균 MV(이력 없으면 MV_SAT_FALLBACK_REL×상한), MV_MIN+20~MV_LIMIT 로 클램프.
  · 해제(RUN OFF / fault / 목표 변경 / 유지 모드 진입) → HEATER_MV_LIMIT 원복 + 로그 1줄.
    단 유지 모드가 dac 로 D00018 을 잡은 경우에는 쓰지 않는다(그쪽이 소유자).
PLC 쓰기는 직접 하지 않는다 — heater_hold 와 같이 request_mv_limit 시그널로만 요청한다.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable

from PyQt6.QtCore import QObject, pyqtSignal as Signal

from lib.config import heater_est_current

MV_SAT_REL          = 0.98    # heater_hold.MV_SAT_REL 과 같은 기준 — 상한의 98% 이상이면 "상한에 물려 있다"
MV_SAT_FALLBACK_REL = 0.9     # 포화 직전 이력이 없을 때 클램프 값(상한의 90%)
PRE_WINDOW_SEC      = 60.0    # 클램프 값을 잡는 "포화 직전" 창


class HeaterSaturationGuard(QObject):
    request_mv_limit = Signal(int)
    message          = Signal(str, str)     # (레벨, 문구)
    saturated        = Signal(dict)         # 포화 판정 1회 {"pv","pv2","mv","sec","clamp"} → main 이 챗 카드

    def __init__(self, *, mv_limit: int, mv_min: int, sat_sec: float,
                 clock: Callable[[], float] = time.monotonic, parent=None):
        super().__init__(parent)
        self.mv_limit = int(mv_limit); self.mv_min = int(mv_min); self.sat_sec = float(sat_sec)
        self._now = clock
        self._hist: deque = deque()          # (t, mv) 최근 PRE_WINDOW_SEC + 포화 구간
        self._sat_since = 0.0                # 상한에 붙기 시작한 시각(0 = 아님)
        self._clamped = None                 # 내가 쓴 D00018 값(None = 안 씀)
        self._sv = None                      # 목표 변경 감지용

    # ───────── 조회 ─────────
    @property
    def clamped(self):
        return self._clamped

    def _msg(self, level: str, text: str) -> None:
        self.message.emit(level, text)

    def _release(self, why: str, restore: bool = True) -> None:
        if self._clamped is not None:
            if restore:
                self.request_mv_limit.emit(int(self.mv_limit))
                self._msg("히터", f"DAC 포화 클램프 해제 → {int(self.mv_limit)} 원복 ({why})")
            else:
                self._msg("히터", f"DAC 포화 클램프 종료 — 유지 모드가 D00018 을 이어받음 ({why})")
        self._clamped = None
        self._sat_since = 0.0
        self._hist.clear()

    # ───────── tick ─────────
    def tick(self, st: dict, hold_active: bool, hold_kind=None) -> None:
        try:
            self._tick(st, bool(hold_active), hold_kind)
        except Exception as e:
            self._msg("경고", f"DAC 포화 감시 예외: {e!r}")

    def _tick(self, st: dict, hold_active: bool, hold_kind) -> None:
        now = self._now()
        run = bool(st.get('run')); fault = bool(st.get('fault'))
        sv = st.get('sv'); mv = st.get('mv')
        if not run:
            self._release("운전 OFF"); self._sv = None; return
        if fault:
            self._release("히터 이상"); return
        if hold_active:
            # 유지 모드가 출력을 묶고 있다 — 판정도 클램프도 하지 않는다. dac 유지면 D00018 은 그쪽 소유
            self._release("유지 모드 진입", restore=(hold_kind != 'dac'))
            return
        if sv is not None:
            if self._sv is not None and abs(float(sv) - float(self._sv)) > 0.05:
                self._release(f"목표 변경 {self._sv} → {sv}")
            self._sv = float(sv)
        if mv is None:
            return
        mv = float(mv)
        self._hist.append((now, mv))
        keep_from = (self._sat_since if self._sat_since else now) - PRE_WINDOW_SEC
        while self._hist and self._hist[0][0] < keep_from:
            self._hist.popleft()
        if self._clamped is not None:
            return                            # 이번 에피소드는 이미 처리했다(해제 조건까지 유지)
        if mv >= MV_SAT_REL * float(self.mv_limit):
            if not self._sat_since:
                self._sat_since = now
            elif (now - self._sat_since) >= self.sat_sec:
                self._saturate(st, now)
        else:
            self._sat_since = 0.0

    def _saturate(self, st: dict, now: float) -> None:
        pre = [m for t, m in self._hist if (self._sat_since - PRE_WINDOW_SEC) <= t < self._sat_since]
        if pre:
            raw = sum(pre) / len(pre); src = f"포화 직전 {PRE_WINDOW_SEC:.0f}초 평균"
        else:
            raw = MV_SAT_FALLBACK_REL * float(self.mv_limit); src = f"이력 없음 → 상한×{MV_SAT_FALLBACK_REL:g}"
        value = max(int(self.mv_min) + 20, min(int(self.mv_limit), int(round(raw))))
        sec = now - self._sat_since
        pv = st.get('pv'); pv2 = st.get('pv2'); mv = st.get('mv')
        t = lambda v: "--.-" if v is None else f"{float(v):.1f}"
        self._clamped = value
        self._msg("히터(경고)",
                  f"DAC 출력 포화 — {sec:.0f}초째 상한({int(self.mv_limit)})에 물려 있음: TC1 {t(pv)} / TC2 {t(pv2)} / MV {mv}"
                  f" → D00018 을 {value}(≒{heater_est_current(value):.0f}A, {src})로 클램프")
        self.request_mv_limit.emit(value)
        self.saturated.emit({"pv": pv, "pv2": pv2, "mv": mv, "sec": sec, "clamp": value, "src": src})
