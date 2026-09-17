# lib/comm_policy.py
# -*- coding: utf-8 -*-
"""시리얼 장치 공통 재연결 정책(스케줄·장기두절·알림). Qt 의존 없음.

PLC·MFC·DC·RFpulse 네 장치가 같은 클래스의 인스턴스를 쓴다. 장치별로 다른 것은
'언제 재연결을 시작하는가'(트리거)뿐이고, 얼마나 기다렸다 다시 시도하는가·언제 장기두절로
보는가·언제 알리고 얼마나 자주 로그를 남기는가는 여기서 한 번만 정한다.

2026-09-16 사고: PLC 가 3초마다 같은 Serial 객체를 close/open(매번 ~30초 블로킹) 하며
13.6시간 반복 → 드라이버 락업. 재시도 간격은 지수 백오프로 벌리고, 장기두절이면 프로브
주기(기본 60초)로 고정한다.
"""
from __future__ import annotations
import time
from typing import Callable


class ReconnectPolicy:
    def __init__(self, start_ms: int, max_ms: int, long_outage_sec: float,
                 probe_ms: int, log_every_sec: float,
                 now: Callable[[], float] = time.monotonic):
        self.start_ms = int(start_ms)
        self.max_ms = int(max_ms)
        self.long_outage_sec = float(long_outage_sec)
        self.probe_ms = int(probe_ms)
        self.log_every_sec = float(log_every_sec)
        self._now = now
        self._fail_count = 0              # 첫 실패부터 연속 실패 수(백오프 지수)
        self._first_fail_t = None         # 첫 실패 시각(None = 단절 아님)
        self._alerted = False             # 장기두절 알림을 이미 냈는가
        self._last_log_t = None           # 장기두절 중 마지막 로그 시각

    # ---------- 이벤트 ----------
    def on_failure(self) -> int:
        """실패를 기록하고 다음 재시도까지의 ms 를 돌려준다.
        첫 실패부터 start×2^n(max 캡). 장기두절이면 probe_ms 고정."""
        t = self._now()
        if self._first_fail_t is None:
            self._first_fail_t = t
        if self.is_long_outage():
            return self.probe_ms
        delay = min(self.start_ms * (2 ** self._fail_count), self.max_ms)
        self._fail_count += 1
        return int(delay)

    def on_success(self) -> None:
        """연결 복구 — 전부 리셋."""
        self._fail_count = 0
        self._first_fail_t = None
        self._alerted = False
        self._last_log_t = None

    # ---------- 조회 ----------
    def is_long_outage(self) -> bool:
        return (self._first_fail_t is not None
                and (self._now() - self._first_fail_t) >= self.long_outage_sec)

    def take_long_outage_alert(self) -> bool:
        """장기두절당 1회만 True."""
        if self.is_long_outage() and not self._alerted:
            self._alerted = True
            return True
        return False

    def should_log(self) -> bool:
        """장기두절 전에는 항상 True. 장기두절 중에는 log_every_sec 마다 True."""
        if not self.is_long_outage():
            return True
        t = self._now()
        if self._last_log_t is None or (t - self._last_log_t) >= self.log_every_sec:
            self._last_log_t = t
            return True
        return False

    def outage_sec(self) -> float:
        return 0.0 if self._first_fail_t is None else (self._now() - self._first_fail_t)

    def in_outage(self) -> bool:
        return self._first_fail_t is not None
