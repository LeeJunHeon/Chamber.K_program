# device/IonGauge.py
# -*- coding: utf-8 -*-
"""이온 게이지 (Duniway Terranova 934, RS-232 9600 8N1) — Chamber-K. 읽기 전용.

[이 장비로 하는 일]
  압력을 읽어 화면·IG CSV·ERP 상태에 보여 줄 뿐이다. 공정을 멈추거나 다른 장비를 건드리지 않는다.
  필라멘트 켜기/끄기(A/B)·디개스(C/D)는 절대 보내지 않는다 — 보낼 수 있는 글자는 ALLOWED_COMMANDS(E, F)뿐.

[프로토콜 — 매뉴얼 3장 + 2026-10-13 장비 노트북 실측]
  명령: 대문자 한 글자, 줄바꿈 없음. 초당 약 2개 이하, 응답을 받은 뒤 다음 명령.
    E = 상태 26항목, F = 이온 게이지 압력 (G/H = 게이지 A/B 압력 — 쓰지 않는다)
  응답: 줄 끝은 LF 다음 CR("\\n\\r"). E 는 26줄 뒤 빈 줄이 하나 더 온다 → CR/LF 어느 쪽으로든 나누고 빈 줄은 버린다.
  압력: "가수 공백 지수"(공백 개수 일정하지 않음). 값(Torr) = 가수 × 10^지수 — 가수는 정규화돼 있지 않을 수 있다
        ("9       -6" = 9.0E-6, "10      -6" = 1.0E-5). 셋포인트 항목(17·18·19·22·23)은 가수 × 10^(지수-2).
  특수값: 이온 게이지 가수 0 = 꺼짐 / 게이지 A·B 가수 0 = 측정 하한(1E-3 Torr) 아래, -900 = 미연결,
          -999 = 영점 미조정(음수 표시) / "999x" = 잘못된 명령.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

# ===== 프로토콜 상수 =====
ALLOWED_COMMANDS = frozenset({"E", "F"})     # 이 밖의 글자는 보내지 않는다(A~D 필라멘트·디개스 금지)
E_ITEM_COUNT = 26
SETPOINT_ITEMS = frozenset({17, 18, 19, 22, 23})

# 매뉴얼 표 3.2 — 필라멘트 오류 코드(E 의 5번 항목). 화면·로그 문구.
FILAMENT_ERRORS = {
    3: "방출 상실로 꺼짐(원인 불명)",
    4: "필라멘트가 제한 시간 안에 안 켜짐(게이지 불량 가능)",
    5: "자동 시작에서 켜기 실패",
    6: "게이지 커넥터에 고전압 없음",
    7: "필라멘트 전류 없음(필라멘트·케이블 단선 가능)",
    8: "필라멘트 과전류(단락)",
    9: "설정 최대 압력 초과로 꺼짐",
    12: "9.9E-3 Torr 초과로 꺼짐(가스 계수·감도 비표준)",
    14: "9.9E-3 Torr 초과로 꺼짐(이온 전류 부족)",
    15: "9.9E-3 Torr 초과로 꺼짐(고압 차단 스위치)",
    16: "자동 필라멘트 모드에서 정상 꺼짐",
    17: "자동 필라멘트 모드에서 수동 꺼짐",
}

# 압력 상태 → 화면 문구(숫자 대신 이 말을 보여 준다)
STATE_TEXT = {
    "off": "꺼짐",
    "below": "하한 이하",
    "disconnected": "미연결",
    "unzeroed": "영점 미조정",
    "invalid": "값 오류",
}

_PAIR_RE = re.compile(r"-?\d+(\s+-?\d+)?")
_INVALID_RE = re.compile(r"999x", re.IGNORECASE)


class IgParseError(ValueError):
    """응답을 해석할 수 없다(항목 부족·형식 다름·잘못된 명령)."""


# ===== 파서 (시리얼 없이 시험 가능한 순수 함수) =====
def split_lines(data: bytes) -> List[str]:
    """CR/LF 어느 쪽으로든 나누고 앞뒤 공백·빈 줄을 버린다."""
    text = bytes(data or b"").decode("ascii", errors="replace")
    return [s.strip() for s in re.split(r"[\r\n]+", text) if s.strip()]


def is_invalid_command_reply(line: str) -> bool:
    return bool(_INVALID_RE.fullmatch(line.strip()))


def is_complete(data: bytes, n_items: int) -> bool:
    """응답이 다 왔는가 — 항목 n_items 개 이상 + 마지막 바이트가 줄 끝. '999x' 는 그 자체로 완료(오류)."""
    lines = split_lines(data)
    if not lines:
        return False
    ended = bytes(data[-1:]) in (b"\r", b"\n")
    if ended and any(is_invalid_command_reply(s) for s in lines):
        return True
    return ended and len(lines) >= n_items


def parse_pair(text: str) -> Tuple[int, int]:
    """'가수 공백 지수' → (가수, 지수). 지수가 없으면 0 ("0" 한 글자 응답 등)."""
    s = (text or "").strip()
    if is_invalid_command_reply(s):
        raise IgParseError("잘못된 명령 응답(999x)")
    if not _PAIR_RE.fullmatch(s):
        raise IgParseError(f"압력 형식 다름: {s!r}")
    parts = s.split()
    return int(parts[0]), (int(parts[1]) if len(parts) > 1 else 0)


def fmt_torr(v: float) -> str:
    """9e-6 → '9.0E-6', 0.5 → '5.0E-1' (지수 앞 0 없음)."""
    m, e = f"{float(v):.1e}".split("e")
    return f"{m}E{int(e)}"


def pressure_from_pair(text: str, kind: str) -> Tuple[Optional[float], str, str]:
    """압력 한 항목 → (Torr 또는 None, 상태, 화면 문구). kind 'ion' = 이온 게이지, 'conv' = 게이지 A/B."""
    mant, exp = parse_pair(text)
    if kind == "ion":
        if mant == 0:
            return None, "off", STATE_TEXT["off"]
    else:
        if mant == -900:
            return None, "disconnected", STATE_TEXT["disconnected"]
        if mant == -999:
            return None, "unzeroed", STATE_TEXT["unzeroed"]
        if mant == 0:
            return None, "below", STATE_TEXT["below"]
    if mant < 0:
        return None, "invalid", STATE_TEXT["invalid"]
    v = mant * (10.0 ** exp)
    return v, "ok", fmt_torr(v)


def setpoint_from_pair(text: str) -> float:
    """셋포인트 항목(17·18·19·22·23): 가수 × 10^(지수-2)."""
    mant, exp = parse_pair(text)
    return mant * (10.0 ** (exp - 2))


def looks_like_pressure_reply(data: bytes) -> bool:
    """신원 확인(F) — 한 줄짜리 '가수 공백 지수' 형식인가."""
    lines = split_lines(data)
    return len(lines) == 1 and bool(_PAIR_RE.fullmatch(lines[0]))


def filament_error_text(code: Optional[int]) -> str:
    if not code:
        return ""
    return FILAMENT_ERRORS.get(int(code), f"알 수 없는 필라멘트 오류 코드 {int(code)}")


@dataclass
class IgReading:
    """E(상태 26항목) 한 번의 해석 결과."""
    ion_torr: Optional[float]
    a_torr: Optional[float]
    b_torr: Optional[float]
    ion_state: str
    a_state: str
    b_state: str
    ion_text: str
    a_text: str
    b_text: str
    degas_on: bool
    filament_on: bool
    emission_ok: bool
    error_code: int
    error_text: str
    relay_sp1_a: bool          # 4  SP1·게이지 A 릴레이
    relay_sp2_b: bool          # 7  SP2·게이지 B 릴레이
    relay_sp3_ion: bool        # 8  SP3·이온 릴레이
    relay_sp4_ion: bool        # 9  SP4·이온 릴레이
    auto_filament_state: int   # 10
    sp_protect_off: bool       # 15 셋포인트 보호 해제
    auto_filament_enabled: bool  # 16
    sp_auto_filament: float    # 17 (Torr)
    sp_ion1: float             # 18
    sp_ion2: float             # 19
    gas_factor: float          # 20 (100 = 1.00)
    sensitivity: float         # 21 (100 = 10.0)
    sp_a: float                # 22
    sp_b: float                # 23
    t: float                   # 응답 시각(epoch 초)


def parse_status(data: bytes, t: Optional[float] = None) -> IgReading:
    """E 응답 → IgReading. 26항목이 안 되거나 형식이 다르면 IgParseError."""
    lines = split_lines(data)
    if any(is_invalid_command_reply(s) for s in lines):
        raise IgParseError("잘못된 명령 응답(999x)")
    if len(lines) < E_ITEM_COUNT:
        raise IgParseError(f"상태 항목 부족 {len(lines)}/{E_ITEM_COUNT}")
    it = lines[:E_ITEM_COUNT]

    def _int(n: int) -> int:
        s = it[n - 1]
        try:
            return int(s)
        except ValueError:
            raise IgParseError(f"{n}번 항목이 정수가 아님: {s!r}")

    ion = pressure_from_pair(it[23], "ion")
    a = pressure_from_pair(it[24], "conv")
    b = pressure_from_pair(it[25], "conv")
    code = _int(5)
    return IgReading(
        ion_torr=ion[0], a_torr=a[0], b_torr=b[0],
        ion_state=ion[1], a_state=a[1], b_state=b[1],
        ion_text=ion[2], a_text=a[2], b_text=b[2],
        degas_on=_int(1) == 1, filament_on=_int(2) == 1, emission_ok=_int(3) == 1,
        error_code=code, error_text=filament_error_text(code),
        relay_sp1_a=_int(4) == 1, relay_sp2_b=_int(7) == 1,
        relay_sp3_ion=_int(8) == 1, relay_sp4_ion=_int(9) == 1,
        auto_filament_state=_int(10),
        sp_protect_off=_int(15) == 1, auto_filament_enabled=_int(16) == 1,
        sp_auto_filament=setpoint_from_pair(it[16]), sp_ion1=setpoint_from_pair(it[17]),
        sp_ion2=setpoint_from_pair(it[18]),
        gas_factor=_int(20) / 100.0, sensitivity=_int(21) / 10.0,
        sp_a=setpoint_from_pair(it[21]), sp_b=setpoint_from_pair(it[22]),
        t=time.time() if t is None else float(t),
    )


def filament_text(r: Optional[IgReading]) -> str:
    """화면·로그용 필라멘트 문구 — 오류가 있으면 오류 문구가 우선."""
    if r is None:
        return ""
    if r.error_code:
        return f"필라멘트 오류 {r.error_code}: {r.error_text}"
    if not r.filament_on:
        return "필라멘트 꺼짐"
    return "필라멘트 켜짐" + ("" if r.emission_ok else " · 방출 이상")
