"""이온 게이지 시뮬레이터 — Duniway Terranova 934 (RS-232 9600 8N1, 대문자 한 글자 명령).

응답 형식(매뉴얼 표 3.4 + v1 실측): 줄 끝 LF CR("\\n\\r"), E 는 26줄 뒤 빈 줄 하나 더. 압력 "가수 지수"(가수 × 10^지수).
타이밍(2026-10-13~14 v1 실측을 그대로):
  E 응답 평소 250~300 ms(0.25 / 0.275 / 0.30 순환), 방출 확인 뒤 레인지 찾기 RANGE_SEARCH_S(7.3 s) 동안 860 ms
  A 뒤 A_QUIET_S(5.9 s) 무응답 → 끝나면 필라멘트·방출 켜짐 → 레인지 찾기(이온 값 0 → 8E-4 → 9.9E-5 → 9.9E-6 → 실제)
  B 뒤 B_QUIET_S(4.9 s) 무응답(필라멘트가 켜져 있을 때만) → 끝나면 꺼짐
  B 뒤 RELIGHT_S(4 s, 매뉴얼) 안의 A 는 무시
  필라멘트 켜짐에서 압력 > 9.9E-3 Torr → 꺼짐 + 오류 15(S2-3 고압 차단)
  모르는 글자 → "999x". C·D(디개스)는 받으면 기록하고 디개스 표시만 바꾼다(드라이버가 보내면 안 됨 — 시험용)
압력은 공용 챔버 모델에서. 게이지 A·B(컨벡션)는 1E-3 Torr 아래면 가수 0(측정 하한 아래).
"""
from __future__ import annotations

import math
from typing import List, Optional

from v2.common.simdev import SimDevice
from v2.chk.sim.chamber import Chamber

E_LATENCIES = (0.25, 0.275, 0.30)
RANGE_LATENCY = 0.86
F_LATENCY = 0.10
A_QUIET_S = 5.9
B_QUIET_S = 4.9
RANGE_SEARCH_S = 7.3
RELIGHT_S = 4.0
TRIP_TORR = 9.9e-3
_RANGE_STEPS = ((0.0, None), (1.5, 8e-4), (3.0, 9.9e-5), (4.5, 9.9e-6))   # (방출 뒤 초, 이온 값) — 그 뒤 실제


def fmt_pair(v: Optional[float]) -> str:
    """값 → '가수 지수'(가수 두 자리 정수). None·0 → '0 0'."""
    if not v:
        return "0 0"
    neg = v < 0
    a = abs(v)
    e = math.floor(math.log10(a))
    m = int(round(a / 10 ** (e - 1)))
    if m >= 100:
        m //= 10
        e += 1
    return f"{'-' if neg else ''}{m} {e - 1}"


class IgSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, start: float = 0.0) -> None:
        super().__init__("ig", latency_s=0.0)
        self.chamber = chamber or Chamber(start)
        self.filament = False
        self.emission = False
        self.degas = False
        self.err = 0
        self.quiet_until = -1.0
        self.pending_state: Optional[tuple] = None   # (시각, filament)
        self.emission_since: Optional[float] = None
        self.last_b: Optional[float] = None
        self.n_e = 0
        self.received: List[tuple] = []
        self.gauge_a_offline = False

    # ───────── 상태 진행 ─────────
    def _advance(self, now: float) -> None:
        self.chamber.advance_to(now)
        if self.pending_state and now >= self.pending_state[0]:
            t, fil = self.pending_state
            self.pending_state = None
            self.filament = self.emission = fil
            self.emission_since = t if fil else None
            if fil:
                self.err = 0
        if self.filament and self.chamber.pressure > TRIP_TORR:
            self.filament = self.emission = False
            self.emission_since = None
            self.err = 15

    def _in_range_search(self, now: float) -> bool:
        return self.emission_since is not None and now - self.emission_since < RANGE_SEARCH_S

    def ion_value(self, now: float) -> Optional[float]:
        if not (self.filament and self.emission):
            return None
        if self._in_range_search(now):
            dt = now - self.emission_since
            val = None
            for t0, v in _RANGE_STEPS:
                if dt >= t0:
                    val = v
            return val if val is not None else None
        return self.chamber.pressure

    def _conv(self) -> str:
        p = self.chamber.pressure
        return "0 -4" if p < 1e-3 else fmt_pair(min(p, 1200.0))

    def e_lines(self, now: float) -> List[str]:
        ion = self.ion_value(now)
        it = ["0"] * 26
        it[0] = "1" if self.degas else "0"
        it[1] = "1" if self.filament else "0"
        it[2] = "1" if self.emission else "0"
        it[4] = str(self.err)
        it[16], it[17], it[18] = "10 -3", "50 -4", "10 -4"         # 셋포인트(가수 × 10^(지수−2))
        it[19], it[20] = "100", "100"                               # 가스 계수 1.00, 감도 10.0
        it[21], it[22] = "10 0", "10 0"
        it[23] = fmt_pair(ion) if ion else "0"
        it[24] = "-900" if self.gauge_a_offline else self._conv()
        it[25] = self._conv()
        return it

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        if now < self.quiet_until:
            return []                                               # A·B 뒤 무응답
        if self._active("no_response", now):
            return []
        out = self.handle(data, now)
        if out is None:
            return []
        lat = self._latency(data, now) + sum(f.extra_s for f in self._active("delay", now))
        if self._active("garbage", now):
            out = b"#" * max(1, len(out) - 2) + out[-2:]
        return [(now + lat, out)]

    def _latency(self, data: bytes, now: float) -> float:
        if data == b"E":
            if self._in_range_search(now):
                return RANGE_LATENCY
            self.n_e += 1
            return E_LATENCIES[self.n_e % len(E_LATENCIES)]
        return F_LATENCY

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        cmd = data.decode("ascii", errors="replace")
        if cmd == "E":
            return ("\n\r".join(self.e_lines(now)) + "\n\r\n\r").encode()
        if cmd == "F":
            ion = self.ion_value(now)
            return ((fmt_pair(ion) if ion else "0") + "\n\r").encode()
        if cmd == "A":
            if self.last_b is not None and now - self.last_b < RELIGHT_S:
                return None                                         # 재점등 금지(경고음만)
            if not self.filament:
                self.quiet_until = now + A_QUIET_S
                self.pending_state = (now + A_QUIET_S, True)
            return None
        if cmd == "B":
            self.last_b = now
            if self.filament:
                self.quiet_until = now + B_QUIET_S
                self.pending_state = (now + B_QUIET_S, False)
            return None
        if cmd in ("C", "D"):
            self.degas = cmd == "C"
            return None
        return b"999x\n\r"

    def commands_seen(self) -> List[str]:
        return [d.decode("ascii", errors="replace") for _, d in self.received]
