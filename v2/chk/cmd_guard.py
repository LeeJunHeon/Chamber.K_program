"""통신층 명령 가드 — 포트 바로 위에서 보내려는 바이트를 장비별 허용 목록과 정확히 맞춰 보고, 아니면 포트에 닿기 전에 막는다.

사용자 지시(2026-10-16): 실제 장비는 절대 제어하지 않는다. bench 는 매뉴얼에 질의로 나온 명령만 보낸다.
- bench 목록 = 아래 *_BENCH(매뉴얼 질의뿐). PLC 는 Modbus FC1·FC3 요청 프레임만(국번·CRC 검사).
- prod·sim 목록 = 드라이버가 실제로 쓰는 명령(*_PROD). 장비 설정을 바꾸거나 장비 메모리에 저장하는 명령은 어떤 모드에서도 막는다
  (*_FORBIDDEN — 이름 붙은 사유로 막고, 목록 밖은 '허용 목록 밖'으로 막는다).
막으면 CommandBlocked(사유 COMMAND_BLOCKED) + 이벤트. 드라이버 안의 bench 거부(이중 보호)와 별개로 마지막 관문이다.

근거(인쇄 쪽): DC(ODA EX) 명령표 [DC p.86-87], 질의 설명 [p.88-98]. MFC(GPC3000) 명령 요약 [MFC p.73-79],
질의 표 [p.80-88]. IG(934) 표 3.4 [IG p.53-54]. RF Pulse(CESAR) AE Bus 명령 번호.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common import aebus
from v2.common.modbus_rtu import READ_COILS, READ_REGS, WRITE_COIL, WRITE_COILS, WRITE_REG, WRITE_REGS, crc16
from v2.common.transport import CommandBlocked, Transport
from v2.chk import devices as D

# ───────────── DC(ODA EX, SCPI + LF) ─────────────
DC_BENCH: tuple = (
    "*IDN?", "*SN?", "SYST:VERS?", "OUTP?", "KEYL?", "APPL?", "VOLT?", "CURR?", "VOLT:STEP?", "CURR:STEP?",
    "VOLT:OVP?", "CURR:OCP?", "VOLT:OVP:TRIP?", "CURR:OCP:TRIP?", "VOLT:UVL?", "VOLT:OVL?", "CURR:UCL?", "CURR:OCL?",
    "FACT:LAST-STA?", "FACT:AUTO-CUR?", "FACT:AUTO-LOC?", "FACT:OVP?", "FACT:OCP?", "FACT:ADC?",
    "MEAS:ALL?", "MEAS:VOLT?", "MEAS:CURR?", "SYST:ERR?",
)   # 28개 [DC p.86-98]. FLOW?·POL? 은 바이폴라 전용이라 이 장비는 응답하지 않는다 [p.86, p.91] — 보내지 않음
DC_PROD_QUERIES = frozenset({"*IDN?", "SYST:ERR?", "MEAS:ALL?", "OUTP?", "FACT:LAST-STA?", "VOLT:OVP:TRIP?",
                             "CURR:OCP:TRIP?", "VOLT:OVP?", "CURR:OCP?"})
_NUM = r"\d+(?:\.\d+)?"
DC_PROD_WRITES = (re.compile(r"OUTP (ON|OFF)"), re.compile(r"\*RST"), re.compile(r"\*CLS"),
                  re.compile(rf"APPLy {_NUM},{_NUM}"), re.compile(rf"CURR {_NUM}"))   # v1 과 같은 글자(APPLy, DCpower.py:682)
DC_PROTECTION_WRITES = (re.compile(rf"VOLT:OVP {_NUM}"), re.compile(rf"CURR:OCP {_NUM}"))   # DC_SET_PROTECTION 일 때만
DC_FORBIDDEN = (   # (정규식, 사유) — 장비 설정 변경·메모리 저장
    (re.compile(r"FACT:[A-Z-]+ "), "FACT:* 쓰기(공장·영구 설정) [p.93-95]"),
    (re.compile(r"FACT:LOAD-DEF"), "FACT:LOAD-DEF(공장 설정 되돌리기) [p.95]"),
    (re.compile(r"\*SAV"), "*SAV(메모리 저장) [p.97]"), (re.compile(r"\*RCL"), "*RCL(메모리 불러오기) [p.97]"),
    (re.compile(r"CAL:"), "CAL:*(교정) [p.93]"), (re.compile(r"KEYL "), "KEYL(앞면 잠금) [p.97]"),
    (re.compile(r"SYST:BEEP"), "SYST:BEEP [p.96]"),
    (re.compile(r"(VOLT|CURR):STEP "), "STEP 설정 [p.88-90]"),
    (re.compile(r"VOLT:(UVL|OVL) "), "UVL·OVL 설정 [p.89-90]"), (re.compile(r"CURR:(UCL|OCL)"), "UCL·OCL 설정 [p.91]"),
    (re.compile(r"(VOLT:OVP|CURR:OCP):CLE"), "트립 해제 [p.89-91]"), (re.compile(r"POL"), "POL(바이폴라 전용) [p.91]"),
)

# ───────────── MFC(GPC3000, ASCII + CR) ─────────────
MFC_BENCH_R = (0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 23, 25, 26, 27, 28, 29, 30, 32, 33, 34, 37, 38,
               41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 60, 61, 62, 65, 66, 69, 70, 71, 74, 75)
# 46개, 채널 1·2 만 [MFC p.80-88]. "현재 지원 안 함" R24·R31·R35·R36 과 없는 채널 3·4(R63·R64·R67·R68·R72·R73·R76·R77)는 보내지 않음
MFC_BENCH = tuple(f"R{n}" for n in MFC_BENCH_R)
MFC_PROD_WRITES = (re.compile(rf"Q[1-4] {_NUM}"), re.compile(r"L0[01]{4}"), re.compile(r"L[5-8] 1"), re.compile(r"Z1"),
                   re.compile(rf"S[1-4] {_NUM}"), re.compile(r"D[1-4]"), re.compile(r"[OCH]"))
MFC_FORBIDDEN = (
    (re.compile(r"[EF]"), "E·F(압력 레인지·단위 설정) [MFC p.74] — IG 의 E·F 와 다르다"),
    (re.compile(r"[GJATBNUXMVWPY]"), "센서·밸브·제어 설정(G·J·A·T·B·N·U·X·M·V·W·P·Y) [p.74-77]"),
    (re.compile(r"Z[2-4]"), "Z2~Z4(특수 영점·영점 해제·A/D 교정) [p.75]"),
    (re.compile(r"D6"), "D6"),
    (re.compile(r"Q[5-8]"), "Q5~Q8(유량 설정 + 플래시 기록) [p.78]"),
)

# ───────────── IG(934, 글자 하나) ─────────────
IG_BENCH = ("E", "F", "G", "H")                      # 표 3.4 [IG p.53-54]
IG_PROD = ("A", "B", "E", "F", "G", "H")
IG_FORBIDDEN = {"C": "C(디개스 켜기) [IG p.53]", "D": "D(디개스 끄기) [IG p.53]"}

# ───────────── RF Pulse(CESAR, AE Bus) ─────────────
RFP_BENCH = frozenset({128, 130, 152, 155, 162, 165, 166, 198})
RFP_PROD = frozenset({1, 2, 3, 8, 14, 27, 33, 93, 96, 128, 130, 152, 155, 162, 165, 166, 193, 196, 198})


@dataclass(frozen=True)
class GuardEvent:
    kind: str            # COMMAND_BLOCKED
    device: str
    detail: str
    reason: str = "COMMAND_BLOCKED"


def _line(data: bytes, term: bytes) -> Optional[str]:
    if not data.endswith(term):
        return None
    try:
        return data[: -len(term)].decode("ascii")
    except UnicodeDecodeError:
        return None


def dc_check(data: bytes, mode: str, protection: bool = False) -> Optional[str]:
    s = _line(data, b"\n")
    if s is None:
        return f"형식(LF 로 끝나는 ASCII 한 줄) 아님: {data!r}"
    if mode == "bench":
        return None if s in DC_BENCH else f"bench 허용 질의 아님: {s}"
    for rx, why in DC_FORBIDDEN:
        if rx.match(s):
            return f"금지 명령 — {why}: {s}"
    if s in DC_PROD_QUERIES:
        return None
    writes = DC_PROD_WRITES + (DC_PROTECTION_WRITES if protection else ())
    if any(rx.fullmatch(s) for rx in writes):
        return None
    return f"허용 목록 밖: {s}"


def mfc_check(data: bytes, mode: str) -> Optional[str]:
    s = _line(data, b"\r")
    if s is None:
        return f"형식(CR 로 끝나는 ASCII 한 줄) 아님: {data!r}"
    if mode == "bench":
        return None if s in MFC_BENCH else f"bench 허용 질의 아님: {s}"
    if re.fullmatch(r"R\d+", s):
        return None if s in MFC_BENCH else f"허용 목록 밖 질의: {s}"
    if any(rx.fullmatch(s) for rx in MFC_PROD_WRITES):
        return None
    for rx, why in MFC_FORBIDDEN:
        if rx.match(s):
            return f"금지 명령 — {why}: {s}"
    return f"허용 목록 밖: {s}"


def ig_check(data: bytes, mode: str) -> Optional[str]:
    s = data.decode("ascii", errors="replace")
    if s in IG_FORBIDDEN:
        return f"금지 명령 — {IG_FORBIDDEN[s]}"
    allowed = IG_BENCH if mode == "bench" else IG_PROD
    return None if s in allowed else f"{'bench ' if mode == 'bench' else ''}허용 글자 아님: {data!r}"


def _plc_writable(cfg) -> tuple:
    coils = {a.addr for a in D.PLC_ADDRS if a.kind == "coil" and "W" in a.access}
    regs = {a.addr for a in D.PLC_ADDRS if a.kind == "reg" and "W" in a.access}
    mark = int(cfg.get("PLC_SESSION_MARK_REG") or 0)
    if mark:
        regs.add(mark)                                    # 세션 마커(재기동 감지) — 설정된 D 주소 하나
    return coils, regs


def plc_check(data: bytes, mode: str, slave: int, cfg) -> Optional[str]:
    if len(data) < 8:
        return f"Modbus 요청 프레임이 짧음: {data.hex(' ')}"
    body, crc = data[:-2], data[-2:]
    if crc16(body).to_bytes(2, "little") != crc:
        return f"CRC 다름: {data.hex(' ')}"
    if body[0] != slave:
        return f"국번 {body[0]} ≠ {slave}"
    fc, addr = body[1], (body[2] << 8) | body[3]
    if fc in (READ_COILS, READ_REGS):
        return None if len(data) == 8 else f"읽기 프레임 길이 다름: {data.hex(' ')}"
    if mode == "bench":
        return f"bench 는 FC1·FC3 만 — FC{fc}"
    coils, regs = _plc_writable(cfg)
    if fc == WRITE_COIL:
        return None if addr in coils else f"쓰기 가능(RW) 코일 아님: {addr}"
    if fc == WRITE_REG:
        return None if addr in regs else f"쓰기 가능(RW) 레지스터 아님: {addr}"
    if fc in (WRITE_COILS, WRITE_REGS):
        n = (body[4] << 8) | body[5]
        allowed = coils if fc == WRITE_COILS else regs
        bad = [a for a in range(addr, addr + n) if a not in allowed]
        return None if not bad else f"FC{fc} 범위에 쓰기 가능 아닌 주소 {bad[:5]}"
    return f"모르는 함수 FC{fc}"


def rfp_check(data: bytes, mode: str, addr: int) -> Optional[str]:
    try:
        a, cmd, payload = aebus.parse(data)
    except Exception as e:
        return f"AE Bus 프레임 아님: {data.hex(' ')} ({e})"
    if a != addr:
        return f"주소 {a} ≠ {addr}"
    if mode == "bench":
        return None if cmd in RFP_BENCH and not payload else f"bench 허용 질의 아님: 명령 {cmd}"
    return None if cmd in RFP_PROD else f"허용 목록 밖: 명령 {cmd}"


def checker(device: str, mode: str, cfg: Dict[str, Any]) -> Callable[[bytes], Optional[str]]:
    if device == "dc":
        return lambda b: dc_check(b, mode, bool(cfg.get("DC_SET_PROTECTION")))
    if device == "mfc":
        return lambda b: mfc_check(b, mode)
    if device == "ig":
        return lambda b: ig_check(b, mode)
    if device == "plc":
        return lambda b: plc_check(b, mode, int(cfg.get("PLC_SLAVE_ID", 1)), cfg)
    if device == "rfpulse":
        return lambda b: rfp_check(b, mode, int(cfg.get("RFPULSE_ADDR", 1)))
    raise ValueError(device)


def allowed_query_count(device: str) -> int:
    return {"dc": len(DC_BENCH), "mfc": len(MFC_BENCH), "ig": len(IG_BENCH), "plc": 2, "rfpulse": len(RFP_BENCH)}[device]


class GuardedTransport(Transport):
    """포트 바로 위 — write 만 검사하고 나머지는 그대로 넘긴다."""

    def __init__(self, inner: Transport, device: str, mode: str, cfg: Dict[str, Any],
                 on_event: Optional[Callable[[GuardEvent], None]] = None) -> None:
        super().__init__(inner.clock)
        self.inner, self.device, self.mode = inner, device, mode
        self.check = checker(device, mode, cfg)
        self.on_event = on_event
        self.blocked: list = []

    @property
    def is_open(self) -> bool:
        return self.inner.is_open

    def open(self) -> None:
        self.inner.open()

    def close(self) -> None:
        self.inner.close()

    def reset_input(self) -> None:
        self.inner.reset_input()

    def write(self, data: bytes) -> None:
        why = self.check(bytes(data))
        if why:
            self.blocked.append((bytes(data), why))
            if self.on_event:
                try:
                    self.on_event(GuardEvent("COMMAND_BLOCKED", self.device, why))
                except Exception:
                    pass
            raise CommandBlocked(f"{self.device}: {why}")
        self.inner.write(data)

    def read_until(self, deadline, terminator=None, size=None, cancel=None) -> bytes:
        return self.inner.read_until(deadline, terminator=terminator, size=size, cancel=cancel)

    def __getattr__(self, name):           # 시뮬레이터 전송의 device 등
        return getattr(self.inner, name)
