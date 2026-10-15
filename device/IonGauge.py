# device/IonGauge.py
# -*- coding: utf-8 -*-
"""이온 게이지 (Duniway Terranova 934, RS-232 9600 8N1) — Chamber-K. 읽기 전용.

[이 장비로 하는 일]
  압력을 읽어 화면·IG CSV·ERP 상태에 보여 준다. 필라멘트 켜기/끄기(A/B)는 공정의 베이스 압력 대기가
  전용 슬롯 filament_on()/filament_off() 로만 보낸다. 디개스(C/D)와 그 밖의 글자는 절대 보내지 않는다.
  A/B 에는 응답이 없다(0 바이트, 2026-10-13 실측) — 결과는 다음 E 의 2번(필라멘트)·3번(방출)·5번(오류)으로 본다.

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

import datetime
import re
import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Tuple

from PyQt6.QtCore import QObject, QTimer, QIODeviceBase, pyqtSignal as Signal, pyqtSlot as Slot
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo

from lib.comm_policy import ReconnectPolicy
from lib.config import (
    IG_PORT, IG_BAUD, IG_FILAMENT_SWITCH_QUIET_SEC, IG_FILAMENT_ON_TIMEOUT_SEC, IG_RESPONSE_TIMEOUT_MS,
    IG_GAUGE_A_NAME, IG_GAUGE_B_NAME, IG_SETTLE_SEC,
    COMM_RECONNECT_START_MS, COMM_RECONNECT_MAX_MS, COMM_LONG_OUTAGE_SEC, COMM_PROBE_MS, COMM_OUTAGE_LOG_SEC,
)

# ===== 프로토콜 상수 =====
QUERY_COMMANDS = frozenset({"E", "F"})        # 조회(응답 있음)
FILAMENT_COMMANDS = frozenset({"A", "B"})     # 필라멘트 ON/OFF(응답 없음) — filament_on()/filament_off() 로만
ALLOWED_COMMANDS = QUERY_COMMANDS | FILAMENT_COMMANDS   # 이 밖의 글자(C·D 디개스 등)는 포트에 닿기 전에 예외
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

# 화면 필라멘트 줄의 짧은 오류 문구(매뉴얼 표 3.2). 표에 없는 코드는 "오류 N".
FILAMENT_ERRORS_SHORT = {
    3: "방출 상실",
    4: "시동 시간 초과",
    5: "자동 시동 실패",
    6: "고전압 없음",
    7: "필라멘트 전류 없음(단선·퓨즈)",
    8: "필라멘트 전류 과다(단락)",
    9: "설정 최대 압력 초과 차단",
    12: "가스계수·감도 비기본값으로 9.9E-3 초과 차단",
    14: "이온 전류 부족으로 9.9E-3 초과 차단",
    15: "9.9E-3 Torr 초과 차단",
    16: "자동 필라멘트 정상 꺼짐",
    17: "자동 모드 중 수동 꺼짐",
}
FILAMENT_ERRORS_NOT_WARN = frozenset({16})     # 정상 동작이라 주의색이 아니다

# 압력 상태 → 화면 문구(숫자 대신 이 말을 보여 준다)
STATE_TEXT = {
    "off": "꺼짐",                      # 이온: 필라멘트 꺼짐 + 가수 0
    "preparing": "측정 준비 중",         # 이온: 필라멘트 켜짐 + 가수 0(꺼짐으로 보이지 않게)
    "below": "< 1.0E-3",                 # 게이지: -5.0E-3 ≤ 값 < 1.0E-3(0·작은 음수·하한 미만 양수). 화면은 "< 1.0E-3 Torr"
                                         #  — 컨벡션 게이지 측정 범위 1E-3 ~ 1200 Torr(매뉴얼), 하한 아래는 영점 잡음이라 숫자로 안 보인다
                                         #  (10/15 실측: 고진공에서 "-1 -4" ↔ "1 -4" 를 오갔다 — 둘 다 같은 '하한 아래')
    "negative": "음수 — 영점·오염 확인",  # 게이지: 값 < -5.0E-3 Torr
    "disconnected": "미연결",            # 게이지: -900
    "unzeroed": "영점 미조정",            # 게이지: -999
    "invalid": "값 오류",                # 형식이 안 맞는 응답 — 이것만 진짜 오류
}
# 매뉴얼: 컨벡션 게이지는 고진공에서 -5.0E-3 ~ +5.0E-3 Torr 를 읽는 것이 정상(벗어나면 오염 가능성)
GAUGE_NEG_OK_TORR = -5.0e-3
# 컨벡션 게이지 측정 하한. 이 미만 양수도 영점 잡음이라 숫자로 보이지 않는다(1.0E-3 부터 숫자)
GAUGE_MIN_TORR = 1.0e-3

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


def raw_pair_text(text: str) -> str:
    """원문(가수 지수) — 공백을 한 칸으로."""
    return " ".join((text or "").split())


def pressure_from_pair(text: str, kind: str, filament_on: bool = False) -> Tuple[Optional[float], str, str]:
    """압력 한 항목 → (Torr 또는 None, 상태, 화면 문구). kind 'ion' = 이온 게이지, 'conv' = 게이지 A/B.
    이온: 가수 0 은 필라멘트가 꺼져 있으면 '꺼짐', 켜져 있으면 '측정 준비 중'. 형식이 다르면 IgParseError(그대로).
    게이지: -5.0E-3 ≤ 값 < 1.0E-3(0·작은 음수·하한 미만 양수)은 '< 1.0E-3'(측정 하한 아래), 1.0E-3 이상만 숫자,
    -5.0E-3 보다 작은 음수는 '음수 — 영점·오염 확인', -900 '미연결', -999 '영점 미조정',
    형식이 안 맞으면 '값 오류(원문)'(이것만 진짜 오류)."""
    raw = raw_pair_text(text)
    if kind == "ion":
        mant, exp = parse_pair(text)
        if mant == 0:
            return (None, "preparing", STATE_TEXT["preparing"]) if filament_on else (None, "off", STATE_TEXT["off"])
        if mant < 0:
            return None, "invalid", f"{STATE_TEXT['invalid']}({raw})"
        v = mant * (10.0 ** exp)
        return v, "ok", fmt_torr(v)
    try:
        mant, exp = parse_pair(text)
    except IgParseError:
        return None, "invalid", f"{STATE_TEXT['invalid']}({raw})"
    if mant == -900:
        return None, "disconnected", STATE_TEXT["disconnected"]
    if mant == -999:
        return None, "unzeroed", STATE_TEXT["unzeroed"]
    if mant == 0:
        return None, "below", STATE_TEXT["below"]
    v = mant * (10.0 ** exp)
    if v < 0:
        if v >= GAUGE_NEG_OK_TORR - 1e-12:        # 경계 -5.0E-3 포함(부동소수 오차 여유)
            return None, "below", STATE_TEXT["below"]
        return None, "negative", STATE_TEXT["negative"]
    if v < GAUGE_MIN_TORR * (1 - 1e-9):           # 경계 1.0E-3 은 숫자("10 -4" 의 부동소수 오차 여유)
        return None, "below", STATE_TEXT["below"]
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
    ion_raw: str = ""          # 24 원문(가수 지수)
    a_raw: str = ""            # 25 원문
    b_raw: str = ""            # 26 원문


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

    fil_on = _int(2) == 1
    ion = pressure_from_pair(it[23], "ion", filament_on=fil_on)
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
        ion_raw=raw_pair_text(it[23]), a_raw=raw_pair_text(it[24]), b_raw=raw_pair_text(it[25]),
    )


def filament_text(r: Optional[IgReading]) -> str:
    """화면·로그용 필라멘트 줄. "필라멘트 켜짐/꺼짐", 오류면 " — 오류 15: 9.9E-3 Torr 초과 차단"(표에 없으면 "오류 N"),
    켜짐인데 방출 이상이면 " · 방출 이상", 디개스 중이면 끝에 " · 디개스 중"."""
    if r is None:
        return ""
    t = "필라멘트 켜짐" if r.filament_on else "필라멘트 꺼짐"
    if r.error_code:
        short = FILAMENT_ERRORS_SHORT.get(int(r.error_code))
        t += f" — 오류 {int(r.error_code)}" + (f": {short}" if short else "")
    elif r.filament_on and not r.emission_ok:
        t += " · 방출 이상"
    if r.degas_on:
        t += " · 디개스 중"
    return t


def filament_warn(r: Optional[IgReading]) -> bool:
    """필라멘트 줄 주의색 — 오류 코드(16 제외) 또는 켜짐인데 방출 이상."""
    if r is None:
        return False
    if r.error_code:
        return int(r.error_code) not in FILAMENT_ERRORS_NOT_WARN
    return bool(r.filament_on and not r.emission_ok)


# ===== 통신 상태 =====
COMM_TEXT = {
    "connecting": "연결 중",
    "ok": "정상",
    "starting": "필라멘트 시동 중",
    "stopping": "필라멘트 끄는 중",
    "disabled": "사용 안 함",
    "no_port": "포트 없음",
    "open_failed": "포트 열기 실패",
    "no_identity": "응답 없음/형식 다름",
    "timeout": "응답 없음",
    "bad_response": "응답 형식 다름",
}


@dataclass
class IgSnapshot:
    """1초에 한 번 main 으로 보내는 상태. reading 은 마지막으로 해석에 성공한 값이다 —
    이번 주기에 새로 받은 값인지는 fresh 로 구분한다(지난 값을 새 값처럼 쓰지 않도록)."""
    comm: str
    comm_text: str
    detail: str
    reading: Optional[IgReading]
    fresh: bool
    last_ok_wall: Optional[float]
    t: float
    emission_since: Optional[float] = None   # 방출이 처음 확인된 시각(epoch). 필라멘트·방출이 꺼지면 None
    filament_starting: bool = False          # A 를 보낸 뒤 무응답 유예 구간(실패로 세지 않는다)
    filament_stopping: bool = False          # B 를 보낸 뒤 무응답 유예 구간

    @property
    def ok(self) -> bool:
        return self.comm == "ok" and self.fresh and self.reading is not None

    def age_sec(self, now: Optional[float] = None) -> Optional[float]:
        if self.last_ok_wall is None:
            return None
        return max(0.0, (time.time() if now is None else now) - self.last_ok_wall)


class IonGaugeController(QObject):
    """Terranova 934 읽기 전용 드라이버. 전용 QThread 에서 돈다.

    [스레드 규칙] 시리얼 포트·타이머·시그널 연결은 전부 워커 스레드 안에서 처음 쓸 때 만든다(_ensure_created).
      __init__ 에서는 Qt 객체를 만들지 않는다 — DCPowerController._outage_probe 처럼 moveToThread 전에
      만든 타이머가 @Slot 없는 메서드에 연결돼 콜백이 메인 스레드에서 도는 문제를 만들지 않기 위해서다.
      타이머·시그널로 불리는 메서드에는 전부 @Slot 을 붙인다.
    [주기] 1초마다 E 한 번. readyRead 로 모으다가 26항목 + 줄 끝이면 완료, timeout_ms 안에 안 오면 실패 1회.
      중첩 이벤트 루프로 기다리지 않는다. 다음 요청 전에 남은 입력을 비운다. 명령 간격은 min_gap_ms 이상.
    [두절] 연속 fail_limit 회 실패하면 포트를 닫고 공통 ReconnectPolicy 일정으로 다시 연다. 다시 열면 F 로
      신원을 확인한 뒤에만 E 를 보낸다. 로그·COMM 이벤트는 상태가 바뀔 때만 낸다(매초 남기지 않는다).
    [필라멘트] filament_on()/filament_off() 로만 A/B 를 보낸다(응답 없음). 진행 중인 E 가 끝난 뒤 보내고,
      보낸 뒤 e_block_sec(1초)는 E 를 보내지 않는다. B 뒤 relight_lockout_sec(5초) 안의 A 는 그만큼 늦춘다
      (매뉴얼: 끈 뒤 4초 안에는 다시 켤 수 없다). A·B 뒤 quiet_sec(IG_FILAMENT_SWITCH_QUIET_SEC) 동안은 E 무응답·
      불완전 응답을 실패로 세지 않고 포트도 닫지 않는다(실측: A 직후 약 4초, 켜진 필라멘트에 B 뒤 약 3.7초 무응답).
      결과는 filament_result.
    [안전] 공정·다른 장비를 직접 건드리지 않는다. 경고창 없음. C·D(디개스)와 그 밖의 글자는 보낼 수 없다.
    """

    snapshot = Signal(object)          # IgSnapshot — 1초마다(연결이 안 돼도) 1번
    status_message = Signal(str, str)  # (레벨, 문구) — 상태가 바뀔 때만
    comm_event = Signal(dict)          # COMM_events.csv 1행(파일 쓰기는 main 스레드)
    filament_result = Signal(str, bool, str)   # ("on"|"off", 성공, 상세 — 실패면 5번 오류 코드와 문구)

    _RX_MAX = 4096

    def __init__(self, port_name: Optional[str] = None, baud: Optional[int] = None, *,
                 poll_ms: int = 1000, timeout_ms: Optional[int] = None, fail_limit: int = 3, min_gap_ms: int = 500,
                 quiet_sec: Optional[float] = None, on_timeout_sec: Optional[float] = None,
                 off_timeout_sec: float = 20.0, relight_lockout_sec: float = 5.0, e_block_sec: float = 1.0,
                 port_factory: Optional[Callable[[QObject], object]] = None,
                 port_lister: Optional[Callable[[], Iterable[str]]] = None,
                 policy: Optional[ReconnectPolicy] = None, parent=None):
        super().__init__(parent)
        self.port_name = str(IG_PORT if port_name is None else port_name).strip()
        self.baud = int(IG_BAUD if baud is None else baud)
        self.poll_ms = int(poll_ms)
        self.timeout_ms = int(IG_RESPONSE_TIMEOUT_MS if timeout_ms is None else timeout_ms)
        self.fail_limit = max(1, int(fail_limit))
        self.min_gap_ms = int(min_gap_ms)
        self.quiet_sec = float(IG_FILAMENT_SWITCH_QUIET_SEC if quiet_sec is None else quiet_sec)
        self.on_timeout_sec = float(IG_FILAMENT_ON_TIMEOUT_SEC if on_timeout_sec is None else on_timeout_sec)
        self.off_timeout_sec = float(off_timeout_sec)
        self.relight_lockout_sec = float(relight_lockout_sec)
        self.e_block_sec = float(e_block_sec)
        self._port_factory = port_factory
        self._port_lister = port_lister
        self._policy = policy or ReconnectPolicy(COMM_RECONNECT_START_MS, COMM_RECONNECT_MAX_MS,
                                                 COMM_LONG_OUTAGE_SEC, COMM_PROBE_MS, COMM_OUTAGE_LOG_SEC)
        # Qt 객체 — 워커 스레드에서 _ensure_created 가 만든다
        self.serial = None
        self._poll_timer: Optional[QTimer] = None
        self._resp_timer: Optional[QTimer] = None
        self._reconnect_timer: Optional[QTimer] = None
        self._fil_timer: Optional[QTimer] = None   # B 뒤 재점등 금지 시간만큼 A 를 늦출 때
        # 상태
        self._rx = bytearray()
        self._pending: Optional[str] = None     # 응답을 기다리는 명령(E/F)
        self._want = False
        self._linked = False                    # 포트 열림 + 신원 확인 완료
        self._comm = "connecting"
        self._detail = ""
        self._fail_streak = 0
        self._outage_t0: Optional[float] = None # 이번 두절의 시작(monotonic). None = 두절 아님
        self._reopened = False                  # 이번 두절 중 포트를 닫았다 다시 열었는가
        self._last_reading: Optional[IgReading] = None
        self._last_ok_wall: Optional[float] = None
        self._last_send_t = -1e9
        self._device_key = None                 # (필라멘트, 방출, 디개스, 오류) — 바뀔 때만 로그
        self._gauge_prev = {}                   # 게이지 A/B: (상태, 화면 문구) — 상태가 바뀔 때만 로그
        # 필라멘트 제어
        self._fil_queue: Optional[str] = None   # 보낼 A/B(아직 안 보냄)
        self._fil_expect: Optional[str] = None  # 결과를 기다리는 동작 "on"/"off"
        self._fil_deadline: Optional[float] = None
        self._last_b_t = -1e9                   # 마지막 B 송신(monotonic)
        self._e_block_until = 0.0               # A/B 뒤 E 를 보내지 않는 시각까지
        self._quiet_until = 0.0                 # A·B 뒤 무응답을 실패로 세지 않는 시각까지
        self._quiet_kind = "starting"           # 유예 중 통신 상태: A 뒤 "starting", B 뒤 "stopping"
        self._emission_since: Optional[float] = None

    # ==================== 지연 생성(워커 스레드) ====================
    def _make_port(self):
        if self._port_factory is not None:
            return self._port_factory(self)
        p = QSerialPort(self)
        p.setBaudRate(self.baud)
        p.setDataBits(QSerialPort.DataBits.Data8)
        p.setParity(QSerialPort.Parity.NoParity)
        p.setStopBits(QSerialPort.StopBits.OneStop)
        p.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
        return p

    def _ensure_created(self) -> None:
        if self.serial is None:
            self.serial = self._make_port()
            self.serial.readyRead.connect(self._on_ready_read)
        if self._poll_timer is None:
            self._poll_timer = QTimer(self)
            self._poll_timer.setInterval(self.poll_ms)
            self._poll_timer.timeout.connect(self._on_poll_tick)
        if self._resp_timer is None:
            self._resp_timer = QTimer(self)
            self._resp_timer.setSingleShot(True)
            self._resp_timer.timeout.connect(self._on_response_timeout)
        if self._reconnect_timer is None:
            self._reconnect_timer = QTimer(self)
            self._reconnect_timer.setSingleShot(True)
            self._reconnect_timer.timeout.connect(self._try_connect)
        if self._fil_timer is None:
            self._fil_timer = QTimer(self)
            self._fil_timer.setSingleShot(True)
            self._fil_timer.timeout.connect(self._try_send_filament)

    def _available_ports(self) -> set:
        if self._port_lister is not None:
            return set(self._port_lister())
        return {p.portName() for p in QSerialPortInfo.availablePorts()}

    # ==================== 연결 ====================
    @Slot()
    def connect_device(self) -> None:
        """프로그램 시작 때 워커 스레드에서 부른다(QueuedConnection). 하드웨어가 없어도 예외를 올리지 않는다."""
        if not self.port_name:
            self._set_comm("disabled", "IG_PORT 가 비어 있음")
            return
        self._ensure_created()
        self._want = True
        self._poll_timer.start()
        self._try_connect()

    @Slot()
    def _try_connect(self) -> None:
        if not self._want or self._linked or self._pending is not None:
            return
        try:
            if not self.serial.isOpen():
                if self.port_name not in self._available_ports():
                    self._link_failed("no_port", f"{self.port_name} 없음")
                    return
                self.serial.setPortName(self.port_name)
                if not self.serial.open(QIODeviceBase.OpenModeFlag.ReadWrite):
                    self._link_failed("open_failed", f"{self.port_name}: {self.serial.errorString()}")
                    return
                if self._outage_t0 is not None:
                    self._reopened = True
            self._send("F")                     # 신원 확인 — 압력 한 줄이 와야 이 포트를 쓴다
        except Exception as e:
            self._link_failed("open_failed", f"{self.port_name}: {e!r}")

    @Slot()
    def cleanup(self) -> None:
        """종료 — 워커 스레드에서 돈다(ig_thread.finished 에 DirectConnection). 메시지는 내지 않는다."""
        self._want = False
        self._linked = False
        self._pending = None
        self._fil_queue = None
        self._fil_expect = None
        self._fil_deadline = None
        for t in (self._poll_timer, self._resp_timer, self._reconnect_timer, self._fil_timer):
            if t is not None:
                try:
                    t.stop()
                except Exception:
                    pass
        try:
            if self.serial is not None and self.serial.isOpen():
                self.serial.close()
        except Exception:
            pass
        self._rx.clear()

    # ==================== 송신 ====================
    @staticmethod
    def _check_command(cmd, filament: bool) -> None:
        """허용 목록 밖(C·D 디개스 등)은 예외. A·B 는 필라멘트 경로로만, E·F 는 조회 경로로만."""
        if not isinstance(cmd, str) or cmd not in ALLOWED_COMMANDS:
            raise ValueError(f"이온 게이지에 보낼 수 없는 명령: {cmd!r} (허용: {sorted(ALLOWED_COMMANDS)})")
        if (cmd in FILAMENT_COMMANDS) != bool(filament):
            raise ValueError(f"이온 게이지 명령 {cmd!r} 은 "
                             + ("filament_on()/filament_off() 로만 보낸다" if cmd in FILAMENT_COMMANDS
                                else "조회 경로로만 보낸다"))

    def _write(self, cmd: str, *, filament: bool = False) -> None:
        """포트에 쓰는 단 한 곳."""
        self._check_command(cmd, filament)
        self.serial.write(cmd.encode("ascii"))

    def _send(self, cmd: str) -> None:
        """조회(E/F) — 응답을 기다린다. 그 밖의 글자는 포트에 닿기 전에 예외."""
        self._check_command(cmd, False)
        try:
            self.serial.clear(QSerialPort.Direction.Input)   # 지난 응답 찌꺼기
        except Exception:
            pass
        try:
            self.serial.readAll()
        except Exception:
            pass
        self._rx.clear()
        self._write(cmd)
        self._pending = cmd
        self._last_send_t = time.monotonic()
        self._resp_timer.start(self.timeout_ms)

    @Slot()
    def _on_poll_tick(self) -> None:
        if not self._want:
            return
        self._check_filament_deadline()
        if not self._linked:
            self._emit_snapshot(fresh=False)    # 끊겨 있어도 1초에 한 번(화면·CSV 가 '끊김'을 그대로 보여 준다)
            return
        if self._pending is not None:
            return
        self._try_send_filament()
        now = time.monotonic()
        if now < self._e_block_until:           # A/B 직후 1초는 E 를 보내지 않는다
            return
        if (now - self._last_send_t) * 1000.0 < self.min_gap_ms:
            return
        self._send("E")

    # ==================== 수신 ====================
    @Slot()
    def _on_ready_read(self) -> None:
        if self.serial is None:
            return
        data = bytes(self.serial.readAll())
        if not data or self._pending is None:
            return                              # 기다리는 명령이 없으면(늦게 온 응답) 버린다
        self._rx.extend(data)
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]
        need = E_ITEM_COUNT if self._pending == "E" else 1
        if not is_complete(bytes(self._rx), need):
            return
        self._resp_timer.stop()
        cmd, buf = self._pending, bytes(self._rx)
        self._pending = None
        self._rx.clear()
        if cmd == "F":
            self._on_identity(buf)
        else:
            self._on_status(buf)

    @Slot()
    def _on_response_timeout(self) -> None:
        cmd, n = self._pending, len(split_lines(bytes(self._rx)))
        self._pending = None
        self._rx.clear()
        if cmd == "F":
            self._link_failed("no_identity", "F 응답 없음" + (f"(받은 줄 {n})" if n else ""))
        elif cmd == "E":
            if self._in_quiet():
                self._quiet_skip()
                return
            self._cycle_failed("timeout", f"{self.timeout_ms} ms 안에 {E_ITEM_COUNT}항목 미수신(받은 줄 {n})")

    def _on_identity(self, buf: bytes) -> None:
        if not looks_like_pressure_reply(buf):
            self._link_failed("no_identity", f"F 응답 형식 다름: {buf[:40]!r}")
            return
        self._linked = True
        self._fail_streak = 0
        if self._outage_t0 is not None and self._reopened:
            self._emit_event("재연결", f"{self.port_name} 다시 열고 신원 확인")
        self._reopened = False
        self._try_send_filament()

    def _on_status(self, buf: bytes) -> None:
        try:
            r = parse_status(buf)
        except IgParseError as e:
            if self._in_quiet():
                self._quiet_skip()
                return
            self._cycle_failed("bad_response", str(e))
            return
        self._fail_streak = 0
        self._last_reading = r
        self._last_ok_wall = r.t
        if r.filament_on and r.emission_ok:
            if self._emission_since is None:
                self._emission_since = r.t      # 방출이 처음 확인된 시각 — 위쪽이 안정화 시간을 센다
        else:
            self._emission_since = None
        if self._policy.in_outage():
            self._policy.on_success()           # 장비가 실제로 응답했을 때만 리셋(포트 열림은 성공이 아니다)
        self._set_comm("ok", "")
        self._log_device_changes(r)
        self._log_gauge_changes(r)
        self._judge_filament(r)
        self._emit_snapshot(fresh=True)
        self._try_send_filament()

    # ==================== 실패·재연결 ====================
    def _cycle_failed(self, kind: str, detail: str) -> None:
        self._fail_streak += 1
        self._set_comm(kind, detail)
        self._emit_snapshot(fresh=False)
        if self._fail_streak >= self.fail_limit:
            self._link_failed(kind, f"연속 {self._fail_streak}회 실패 — 포트를 닫고 다시 연결")

    def _link_failed(self, kind: str, detail: str) -> None:
        self._linked = False
        self._pending = None
        self._fail_streak = 0
        if self._resp_timer is not None:
            self._resp_timer.stop()
        self._rx.clear()
        try:
            if self.serial is not None and self.serial.isOpen():
                self.serial.close()
        except Exception:
            pass
        self._set_comm(kind, detail)
        if not self._want or self._reconnect_timer is None:
            return
        ms = self._policy.on_failure()
        if self._policy.take_long_outage_alert():
            lost = self._policy.outage_sec()
            self.status_message.emit("IG(경고)", f"이온 게이지 통신 장기 두절 {lost:.0f}초 — {ms} ms 간격으로만 재시도")
            self._emit_event("장기두절", f"{lost:.0f}초", lost=lost)
        self._reconnect_timer.start(int(ms))

    def _set_comm(self, kind: str, detail: str) -> None:
        """통신 상태 — 바뀔 때만 로그와 COMM 이벤트를 낸다."""
        prev = self._comm
        self._comm, self._detail = kind, detail
        if kind == prev:
            return
        if kind in ("starting", "stopping"):
            self.status_message.emit("IG", f"이온 게이지 {COMM_TEXT[kind]} — 무응답을 실패로 세지 않음")
            return
        if kind == "ok":
            if prev in ("starting", "stopping") and self._outage_t0 is None:
                return
            if self._outage_t0 is not None:
                lost = time.monotonic() - self._outage_t0
                self._outage_t0 = None
                self._emit_event("복구", f"단절 {lost:.1f}초", lost=lost)
                self.status_message.emit("IG", f"이온 게이지 통신 복구 (단절 {lost:.1f}초)")
            else:
                self.status_message.emit("IG", f"이온 게이지 연결됨 ({self.port_name})")
            return
        text = COMM_TEXT.get(kind, kind)
        if kind == "disabled":
            self.status_message.emit("IG", f"이온 게이지 {text} — {detail}")
            return
        if self._outage_t0 is None:
            self._outage_t0 = time.monotonic()
            self._emit_event("실패", f"{text}: {detail}")
        self.status_message.emit("IG(경고)", f"이온 게이지 {text} — {detail}")

    def _log_gauge_changes(self, r: IgReading) -> None:
        """이온·게이지 A/B 상태 '종류'가 바뀔 때만 1줄(원문 포함). 예) "[IG] 게이지 A < 1.0E-3(0 -4) → 음수 — 영점·오염 확인(-60 -4)".
        하한 아래(0·작은 음수·1.0E-3 미만 양수) 안에서 오가는 것은 같은 종류라 남기지 않는다. 값만 바뀌어도 남기지 않는다.
        첫 값은 주의 종류(음수·영점 미조정·값 오류)일 때만 남긴다(원문 확보용)."""
        for key, name, state, text, raw in (
                ("ion", "IG", r.ion_state, r.ion_text, r.ion_raw),
                ("a", IG_GAUGE_A_NAME, r.a_state, r.a_text, r.a_raw),
                ("b", IG_GAUGE_B_NAME, r.b_state, r.b_text, r.b_raw)):
            shown = _with_unit(text, state) if state == "ok" else _csv_text(state, text, raw)
            prev = self._gauge_prev.get(key)
            self._gauge_prev[key] = (state, shown)
            if prev is None:
                if state in _WARN_STATES:
                    self.status_message.emit("IG(경고)", f"[IG] {name} {shown}")
                continue
            if prev[0] != state:
                self.status_message.emit("IG(경고)" if state in _WARN_STATES else "IG",
                                         f"[IG] {name} {prev[1]} → {shown}")

    def _log_device_changes(self, r: IgReading) -> None:
        key = (r.filament_on, r.emission_ok, r.degas_on, r.error_code)
        if key == self._device_key:
            return
        prev_code = None if self._device_key is None else self._device_key[3]
        self._device_key = key
        level = "IG(경고)" if filament_warn(r) else "IG"
        if r.error_code and not prev_code:
            self.status_message.emit(level, f"[IG] {filament_text(r)}")      # 오류 코드 0 → N
            return
        self.status_message.emit(level, f"이온 게이지 {filament_text(r)}")

    # ==================== 내보내기 ====================
    def _emit_snapshot(self, fresh: bool) -> None:
        self.snapshot.emit(IgSnapshot(
            comm=self._comm, comm_text=COMM_TEXT.get(self._comm, self._comm), detail=self._detail,
            reading=self._last_reading, fresh=bool(fresh), last_ok_wall=self._last_ok_wall, t=time.time(),
            emission_since=self._emission_since,
            filament_starting=self._in_quiet() and self._quiet_kind == "starting",
            filament_stopping=self._in_quiet() and self._quiet_kind == "stopping"))

    # ==================== 필라멘트 ON/OFF ====================
    @Slot()
    def filament_on(self) -> None:
        """필라멘트 켜기(A). 공정의 베이스 압력 대기가 시그널(QueuedConnection)로 부른다."""
        self._request_filament("A")

    @Slot()
    def filament_off(self) -> None:
        """필라멘트 끄기(B)."""
        self._request_filament("B")

    def _request_filament(self, cmd: str) -> None:
        action = "on" if cmd == "A" else "off"
        if self._poll_timer is None or not self._want:
            self.filament_result.emit(action, False, "이온 게이지가 연결돼 있지 않음")
            return
        if self._fil_expect is not None and self._fil_expect != action:
            self._finish_filament(self._fil_expect, False, "다른 필라멘트 요청으로 취소")
        now = time.monotonic()
        self._fil_queue = cmd
        self._fil_expect = action
        # 못 보내는 경우(통신 끊김)를 위한 기한 — 보내면 보낸 시각 기준으로 다시 잡는다
        self._fil_deadline = now + (self.on_timeout_sec + self.relight_lockout_sec if action == "on"
                                    else self.off_timeout_sec)
        self._try_send_filament()

    @Slot()
    def _try_send_filament(self) -> None:
        cmd = self._fil_queue
        if cmd is None or not self._linked or self.serial is None or not self.serial.isOpen():
            return
        if self._pending is not None:           # 진행 중인 E 가 끝난 뒤
            return
        now = time.monotonic()
        if cmd == "A":
            wait = self._last_b_t + self.relight_lockout_sec - now
            if wait > 0:                        # B 뒤 재점등 금지 — 그만큼 늦춘다
                if self._fil_timer is not None and not self._fil_timer.isActive():
                    self._fil_timer.start(int(wait * 1000) + 1)
                return
        self._write(cmd, filament=True)
        self._fil_queue = None
        self._e_block_until = now + self.e_block_sec
        self._quiet_until = now + self.quiet_sec        # A·B 모두 상태가 바뀌는 동안 E 무응답(실측)
        self._quiet_kind = "starting" if cmd == "A" else "stopping"
        if cmd == "A":
            self._fil_deadline = now + self.on_timeout_sec
            self.status_message.emit("IG", "이온 게이지 필라멘트 ON(A) 보냄")
        else:
            self._last_b_t = now
            self._fil_deadline = now + self.off_timeout_sec
            self.status_message.emit("IG", "이온 게이지 필라멘트 OFF(B) 보냄")

    def _judge_filament(self, r: IgReading) -> None:
        if self._fil_expect is None or self._fil_queue is not None:
            return
        if self._fil_expect == "on" and r.filament_on and r.emission_ok:
            self._finish_filament("on", True, "필라멘트 켜짐·방출 정상")
        elif self._fil_expect == "off" and not r.filament_on:
            self._finish_filament("off", True, "필라멘트 꺼짐")

    def _check_filament_deadline(self) -> None:
        if self._fil_expect is None or self._fil_deadline is None or time.monotonic() <= self._fil_deadline:
            return
        action, r = self._fil_expect, self._last_reading
        sent = self._fil_queue is None
        if not sent:
            detail = "명령을 보내지 못함(이온 게이지 통신 끊김)"
        elif action == "on":
            code = r.error_code if r is not None else None
            state = ("" if r is None else
                     f" (필라멘트 {'켜짐' if r.filament_on else '꺼짐'}, 방출 {'정상' if r.emission_ok else '없음'})")
            detail = (f"{self.on_timeout_sec:.0f}초 안에 필라멘트·방출 확인 안 됨{state} — "
                      f"오류 코드 {code if code is not None else '?'}: {filament_error_text(code) or '없음'}")
        else:
            detail = f"{self.off_timeout_sec:.0f}초 안에 필라멘트 꺼짐 확인 안 됨"
        self._finish_filament(action, False, detail)

    def _finish_filament(self, action: str, ok: bool, detail: str) -> None:
        self._fil_expect = None
        self._fil_deadline = None
        self._fil_queue = None
        if self._fil_timer is not None:
            self._fil_timer.stop()
        word = "켜기" if action == "on" else "끄기"
        self.status_message.emit("IG" if ok else "IG(경고)",
                                 f"이온 게이지 필라멘트 {word} {'성공' if ok else '실패'} — {detail}")
        self.filament_result.emit(action, bool(ok), detail)

    def _in_quiet(self) -> bool:
        return time.monotonic() < self._quiet_until

    def _quiet_skip(self) -> None:
        """A·B 직후 유예 구간의 무응답·불완전 응답 — 실패로 세지 않고 포트도 닫지 않는다."""
        self._set_comm(self._quiet_kind, COMM_TEXT[self._quiet_kind])
        self._emit_snapshot(fresh=False)
        self._try_send_filament()

    def _emit_event(self, kind: str, detail: str, lost=None) -> None:
        try:
            self.comm_event.emit({
                "시각": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "종류": kind, "상세": detail,
                "단절초": ("" if lost is None else f"{float(lost):.1f}"),
                "연속실패": self._fail_streak,
            })
        except Exception:
            pass


# ===== 화면·기록 문구 (main 이 쓰는 순수 함수) =====
_WARN_STATES = frozenset({"negative", "unzeroed", "invalid"})   # 게이지·이온 줄 주의색


def _with_unit(text: str, state: str) -> str:
    return f"{text} Torr" if state in ("ok", "below") else text


def _settling(snap: IgSnapshot, now: Optional[float] = None) -> bool:
    r = snap.reading
    return bool(r is not None and r.ion_state == "ok" and r.filament_on and r.emission_ok
                and snap.emission_since is not None
                and ((time.time() if now is None else now) - snap.emission_since) < float(IG_SETTLE_SEC))


def ion_line(snap: IgSnapshot, now: Optional[float] = None) -> Tuple[str, str]:
    """이온 줄 문구('IG ' 제외)와 등급. 방출 확인 후 IG_SETTLE_SEC 이내면 '(안정화 중)' 주의색."""
    r = snap.reading
    text = _with_unit(r.ion_text, r.ion_state)
    if _settling(snap, now):
        return f"{text} (안정화 중)", "warn"
    return text, ("warn" if r.ion_state in _WARN_STATES else "ok")


def gauge_line(state: str, text: str) -> Tuple[str, str]:
    return _with_unit(text, state), ("warn" if state in _WARN_STATES else "ok")


def _view(snap: Optional[IgSnapshot], a_name: str, b_name: str, now: Optional[float] = None):
    """(4줄, 줄별 등급, 전체 등급). 줄 등급: ok / warn / lost(끊김 제목) / dim(끊김 때 나머지)."""
    if snap is None:
        return ["IG 연결 중", "", "", ""], ["lost", "dim", "dim", "dim"], "lost"
    r = snap.reading
    if snap.comm in ("starting", "stopping"):
        return [f"IG {COMM_TEXT[snap.comm]}", "", "", ""], ["warn", "ok", "ok", "ok"], "warn"
    if snap.ok:
        ion, l0 = ion_line(snap, now)
        a, l1 = gauge_line(r.a_state, r.a_text)
        b, l2 = gauge_line(r.b_state, r.b_text)
        l3 = "warn" if filament_warn(r) else "ok"
        levels = [l0, l1, l2, l3]
        return ([f"IG {ion}", f"{a_name} {a}", f"{b_name} {b}", filament_text(r)], levels,
                ("warn" if "warn" in levels else "ok"))
    age = snap.age_sec(now)
    last = "받은 값 없음" if age is None else f"마지막 값 {age:.0f}초 전"
    old = "" if r is None else f"IG {_with_unit(r.ion_text, r.ion_state)}"
    return ["IG 통신 끊김", snap.comm_text, last, old], ["lost", "dim", "dim", "dim"], "lost"


def display_lines(snap: Optional[IgSnapshot], a_name: str, b_name: str,
                  now: Optional[float] = None) -> Tuple[List[str], str]:
    """화면 4줄과 전체 등급('ok' / 'warn' / 'lost')."""
    lines, _levels, level = _view(snap, a_name, b_name, now)
    return lines, level


def line_levels(snap: Optional[IgSnapshot], a_name: str = "", b_name: str = "",
                now: Optional[float] = None) -> List[str]:
    """줄별 색 등급 — ok #222222 / warn #b35c00 / lost #c62828 / dim(끊김 때 나머지 줄) #8a8a8a."""
    return _view(snap, a_name, b_name, now)[1]


def raw_summary(snap: Optional[IgSnapshot]) -> str:
    """툴팁용 원문 한 줄 — 예) "원문 IG 99 -7 · A -5 -4 · B 61 -4 · 오류 코드 0"."""
    r = getattr(snap, "reading", None)
    if r is None:
        return ""
    return f"원문 IG {r.ion_raw} · A {r.a_raw} · B {r.b_raw} · 오류 코드 {r.error_code}"


BELOW_NOTE = "컨벡션 게이지 측정 하한(1E-3 Torr) 아래 — 영점 근처 잡음"


def tooltip_lines(snap: Optional[IgSnapshot]) -> List[str]:
    """툴팁에 덧붙일 줄 — 원문, 게이지 A/B 가 측정 하한 아래면 그 설명."""
    r = getattr(snap, "reading", None)
    if r is None:
        return []
    out = [raw_summary(snap)]
    if "below" in (r.a_state, r.b_state):
        out.append(BELOW_NOTE)
    return out


def _csv_text(state: str, text: str, raw: str) -> str:
    """IG CSV — 정상 양수는 값 그대로, 그 밖은 문구에 원문을 괄호로(값 오류는 이미 원문 포함)."""
    if state == "ok" or state == "invalid":
        return text
    return f"{text}({raw})"


def csv_row(snap: IgSnapshot) -> dict:
    """IG CSV 1행. 통신이 끊긴 주기는 압력·필라멘트 칸을 비운다(지난 값을 새 값처럼 남기지 않는다)."""
    row = {"시각": datetime.datetime.fromtimestamp(snap.t).strftime("%Y-%m-%d %H:%M:%S"),
           "통신 상태": snap.comm_text}
    if snap.ok:
        r = snap.reading
        row.update({"IG(Torr)": _csv_text(r.ion_state, r.ion_text, r.ion_raw),
                    "게이지A(Torr)": _csv_text(r.a_state, r.a_text, r.a_raw),
                    "게이지B(Torr)": _csv_text(r.b_state, r.b_text, r.b_raw),
                    "필라멘트": "켜짐" if r.filament_on else "꺼짐",
                    "방출": "정상" if r.emission_ok else "이상",
                    "디개스": "켜짐" if r.degas_on else "꺼짐",
                    "오류 코드": r.error_code})
    return row


def process_log_line(snap: Optional[IgSnapshot], phase: str, a_name: str, b_name: str,
                     now: Optional[float] = None) -> str:
    """공정 로그 한 줄 — '[IG] 시작 압력 …' / '[IG] 종료 압력 …'."""
    head = f"[IG] {phase} 압력"
    if snap is not None and snap.ok:
        r = snap.reading
        return (f"{head} IG {ion_line(snap, now)[0]}, {a_name} {gauge_line(r.a_state, r.a_text)[0]}, "
                f"{b_name} {gauge_line(r.b_state, r.b_text)[0]} ({filament_text(r)})")
    if snap is None:
        return f"{head} — 아직 받은 값 없음"
    lines, _ = display_lines(snap, a_name, b_name, now)
    return f"{head} — 통신 끊김({snap.comm_text}), {lines[2]}" + (f": {lines[3]}" if lines[3] else "")


def erp_state_dict(snap: Optional[IgSnapshot], enabled: bool, a_name: str, b_name: str,
                   now: Optional[float] = None) -> dict:
    """ERP 상태의 ionGauge 객체. 압력 숫자(Torr)는 이번 주기에 받은 값일 때만 — 끊기면 None 이고
    통신 상태와 마지막 응답 시각(UTC ISO)·경과 초로 알린다. 표시 문자열은 장비 화면과 같다."""
    d = {
        "enabled": bool(enabled), "comm": "disabled", "commText": COMM_TEXT["disabled"],
        "ion": None, "gaugeA": None, "gaugeB": None,
        "ionText": "", "gaugeAText": "", "gaugeBText": "",
        "gaugeAName": a_name, "gaugeBName": b_name,
        "filamentOn": None, "emissionOk": None, "degasOn": None,
        "errorCode": None, "errorText": "", "filamentText": "",
        "lastResponse": None, "ageSec": None,
    }
    if not enabled:
        return d
    if snap is None:
        d.update(comm="connecting", commText=COMM_TEXT["connecting"])
        return d
    d.update(comm=snap.comm, commText=snap.comm_text)
    if snap.last_ok_wall is not None:
        d["lastResponse"] = datetime.datetime.fromtimestamp(
            snap.last_ok_wall, datetime.timezone.utc).isoformat(timespec="seconds")
        d["ageSec"] = int(round(snap.age_sec(now) or 0.0))
    if snap.ok:
        r = snap.reading
        d.update(ion=r.ion_torr, gaugeA=r.a_torr, gaugeB=r.b_torr,
                 ionText=ion_line(snap, now)[0], gaugeAText=gauge_line(r.a_state, r.a_text)[0],
                 gaugeBText=gauge_line(r.b_state, r.b_text)[0],
                 filamentOn=r.filament_on, emissionOk=r.emission_ok, degasOn=r.degas_on,
                 errorCode=r.error_code, errorText=r.error_text, filamentText=filament_text(r))
    return d
