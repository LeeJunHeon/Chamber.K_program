"""RF Pulse 시뮬레이터 — AE CESAR 1310(RS-232 9600 8O1, AE Bus, 주소 1).

명령(devices.RFPULSE): 1 RF OFF, 2 RF ON, 3 조절 모드(6 FWD), 8 셋포인트 W(u16), 14 제어 모드(2 Host/4 User/6 Local),
  27 펄싱(0~4), 33 반사 전력 자동 차단 시간(가정: 1바이트 초, 0 = 끔), 93 펄스 주파수 Hz(u24), 96 듀티 %(u16),
  128 기종(ASCII), 130 용량 W(u16), 152 반사 자동 차단 시간, 155 제어 모드, 162 상태 4바이트, 165 순방향 W(u16),
  166 반사 W(u16), 193 주파수, 196 듀티, 198 SW 버전(ASCII).
CSR: Host 가 아니면 설정 명령에 1, RF ON 중 3·14 에 2, 범위 밖 4·50·51, 인터록 열림이면 RF ON 에 5(가정 — 매뉴얼 CSR 5 는
  User Port RF 신호 꺼짐). 체크섬이 틀리면 NAK. 주소가 다르면 무응답.
응답 지연: 처리 PROCESS_S + (요청 + ACK + 응답) 바이트 × 11 비트 / 9600(8O1).
플라즈마(가정 모델): RF ON 이면 순방향 = 셋포인트(1차 지연 0.3 s), 반사 = 순방향 × (점화 0.02 / 점화 전 0.6),
  점화 = 공용 챔버의 점화 조건(가스·압력 범위). 반사 주입(ref_inject_w), 인터록 열림 주입(interlock_open → RF OFF).
시험용: csr_once{명령: CSR}(한 번만 그 CSR), readback_offset(193 되읽기를 틀리게).
"""
from __future__ import annotations

from typing import Dict, List, Optional

from v2.common.aebus import ACK, NAK, build, le, u16, u24, xor
from v2.common.simdev import SimDevice
from v2.chk.sim.chamber import Chamber

PROCESS_S = 0.02
BIT_S = 11.0 / 9600.0
FWD_TAU_S = 0.3
REF_RATIO_LIT = 0.02
REF_RATIO_UNLIT = 0.6


class CesarSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, addr: int = 1) -> None:
        super().__init__("rfpulse", latency_s=0.0)
        self.chamber = chamber or Chamber()
        self.addr = addr
        self.control = 6                     # Local 로 시작 — Host 로 바꿔야 설정 가능
        self.reg_mode = 6
        self.setpoint = 0
        self.pulsing = 0
        self.freq = 1000
        self.duty = 50
        self.out = False
        self.fwd = 0.0
        self.auto_off_s = 0
        self.interlock_open = False
        self.ref_inject_w = 0.0
        self.csr_once: Dict[int, int] = {}
        self.readback_offset = 0
        self.t = 0.0
        self.received: List[tuple] = []

    # ───────── 시각 ─────────
    def _advance(self, now: float) -> None:
        self.chamber.advance_to(now)
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            if self.interlock_open:
                self.out = False
            tgt = float(self.setpoint) if self.out else 0.0
            self.fwd += (tgt - self.fwd) * min(1.0, dt / FWD_TAU_S)
            self.t += dt

    def reflected(self) -> float:
        if self.fwd < 0.5:
            return 0.0
        ratio = REF_RATIO_LIT if self.chamber.can_ignite() else REF_RATIO_UNLIT
        return self.fwd * ratio + self.ref_inject_w

    def status(self) -> bytes:
        b1 = (0x20 if self.out else 0) | (0x40 if self.out else 0) | (0x80 if self.out and abs(self.fwd - self.setpoint) > 5 else 0)
        b2 = 0x80 if self.interlock_open else 0
        return bytes([b1, b2, 0, 0])

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        if self._active("no_response", now):
            return []
        if len(data) < 3 or (data[0] >> 3) != self.addr:
            return []
        if xor(data) != 0:
            return [(now + PROCESS_S + (len(data) + 1) * BIT_S, bytes([NAK]))]
        cmd = data[1]
        n = data[0] & 0x07
        payload = data[3:3 + data[2]] if n == 7 else data[2:2 + n]
        out = self._handle(cmd, payload)
        frame = bytes([ACK]) + build(self.addr, cmd, out)
        if self._active("garbage", now):
            frame = frame[:-1] + bytes([frame[-1] ^ 0x5A])
        lat = PROCESS_S + (len(data) + len(frame)) * BIT_S + sum(f.extra_s for f in self._active("delay", now))
        return [(now + lat, frame)]

    def _csr(self, c: int) -> bytes:
        return bytes([c])

    def _handle(self, cmd: int, d: bytes) -> bytes:
        if cmd in self.csr_once:
            return self._csr(self.csr_once.pop(cmd))
        if cmd < 128:
            if cmd != 14 and self.control != 2:
                return self._csr(1)
            if cmd == 1:
                self.out = False
                return self._csr(0)
            if cmd == 2:
                if self.interlock_open:
                    return self._csr(5)
                self.out = True
                return self._csr(0)
            if cmd == 3:
                if self.out:
                    return self._csr(2)
                self.reg_mode = d[0]
                return self._csr(0 if d[0] in (6, 7, 8) else 4)
            if cmd == 8:
                v = u16(d)
                if v > 1000:
                    return self._csr(4)
                self.setpoint = v
                return self._csr(0)
            if cmd == 14:
                if self.out and d[0] != self.control:
                    self.out = False                 # 매뉴얼: ON 중 제어 모드를 바꾸면 자동 OFF
                self.control = d[0]
                return self._csr(0)
            if cmd == 27:
                if d[0] > 4:
                    return self._csr(4)
                self.pulsing = d[0]
                return self._csr(0)
            if cmd == 33:
                self.auto_off_s = d[0]
                return self._csr(0)
            if cmd == 93:
                v = u24(d)
                if not 1 <= v <= 100000:
                    return self._csr(50)
                self.freq = v
                return self._csr(0)
            if cmd == 96:
                v = u16(d)
                if not 1 <= v <= 99:
                    return self._csr(51)
                self.duty = v
                return self._csr(0)
            return self._csr(99)
        if cmd == 128:
            return b"CESAR 1310"
        if cmd == 130:
            return le(1000, 2)
        if cmd == 152:
            return bytes([self.auto_off_s])
        if cmd == 155:
            return bytes([self.control])
        if cmd == 162:
            return self.status()
        if cmd == 165:
            return le(int(round(self.fwd)), 2)
        if cmd == 166:
            return le(int(round(self.reflected())), 2)
        if cmd == 193:
            return le(self.freq + self.readback_offset, 3)
        if cmd == 196:
            return le(self.duty, 2)
        if cmd == 198:
            return b"1.16"
        return self._csr(99)

    def commands_seen(self) -> List[int]:
        return [d[1] for _, d in self.received if len(d) > 1]

    def set_commands_seen(self) -> List[int]:
        return [c for c in self.commands_seen() if c < 128]
