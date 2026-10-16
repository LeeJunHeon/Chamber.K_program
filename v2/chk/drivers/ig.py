"""챔버K 이온 게이지 드라이버 — Duniway Terranova 934. v1 device/IonGauge.py 의 파서·규칙을 다시 쓴 것(import 하지 않음).

명령: 질의 E(상태 26항목)·F(이온 압력 한 줄)만 읽기 경로로, 필라멘트 A(켜기)·B(끄기)는 filament_on/off 로만.
  그 밖의 글자(C·D 디개스 등)는 포트에 닿기 전에 거부(COMMAND_NOT_ALLOWED).
읽기: tick 마다 E 1회(스냅샷 묶기 소스). 응답 대기 중이면 묶기가 보내지 않는다(stale).
  E 응답 제한은 IG_RESPONSE_LIMIT_MS(1200, tick 마감 뒤까지 — X25): 마감(900) 뒤 도착하면 다음 행에 late,
  그동안 다음 tick 의 E 는 보내지 않는다(stale). 제한을 넘기면 fail, 늦게 온 바이트는 다음 질의 전에 비운다.
필라멘트: A·B 에는 응답이 없다(confirms_link=False) — 결과는 다음 E 의 2번(필라멘트)·3번(방출)·5번(오류)으로 판정,
  기한(켜기 IG_FILAMENT_ON_TIMEOUT_SEC, 끄기 IG_FILAMENT_OFF_TIMEOUT_SEC)을 넘으면 실패 이벤트.
  A·B 뒤 IG_FILAMENT_SWITCH_QUIET_SEC 동안은 무응답을 실패로 세지 않는다(worker 억제 → 스냅샷 품질 suppressed),
  A·B 뒤 1 s 는 E 를 보내지 않는다. B 뒤 IG_RELIGHT_LOCKOUT_SEC 안의 A 는 그만큼 늦춘다.
연결: F 로 신원 확인('가수 지수' 한 줄) — 아니면 연결 실패.
bench: A·B 는 worker 가 BENCH_READ_ONLY 로 거부, E·F 는 허용.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common.worker import Request, Result, Worker

QUERY_COMMANDS = frozenset({"E", "F", "G", "H"})       # 질의 [IG p.53-54] — G·H 는 게이지 A·B(bench 설정 읽기)
FILAMENT_COMMANDS = frozenset({"A", "B"})
E_ITEM_COUNT = 26
E_BLOCK_S = 1.0          # A·B 뒤 E 를 보내지 않는 시간(v1 e_block_sec)
GAUGE_NEG_OK_TORR = -5.0e-3
GAUGE_MIN_TORR = 1.0e-3

FILAMENT_ERRORS = {
    3: "방출 상실로 꺼짐", 4: "필라멘트가 제한 시간 안에 안 켜짐", 5: "자동 시작에서 켜기 실패",
    6: "게이지 커넥터에 고전압 없음", 7: "필라멘트 전류 없음(단선 가능)", 8: "필라멘트 과전류(단락)",
    9: "설정 최대 압력 초과로 꺼짐", 12: "9.9E-3 Torr 초과로 꺼짐(가스 계수·감도 비표준)",
    14: "9.9E-3 Torr 초과로 꺼짐(이온 전류 부족)", 15: "9.9E-3 Torr 초과로 꺼짐(고압 차단 스위치)",
    16: "자동 필라멘트 모드에서 정상 꺼짐", 17: "자동 필라멘트 모드에서 수동 꺼짐",
}   # 매뉴얼 표 3.2 [IG p.44]

_PAIR_RE = re.compile(r"-?\d+(\s+-?\d+)?")
_INVALID_RE = re.compile(r"999x", re.IGNORECASE)


class IgParseError(ValueError):
    pass


class IgCommandNotAllowed(ValueError):
    reason = "COMMAND_NOT_ALLOWED"


class IgSkip(Exception):
    """A·B 직후라 이번 tick 에는 E 를 보내지 않음(포트에 닿지 않는다)."""


# ───────── 파서(순수 함수) ─────────
def split_lines(data: bytes) -> List[str]:
    text = bytes(data or b"").decode("ascii", errors="replace")
    return [s.strip() for s in re.split(r"[\r\n]+", text) if s.strip()]


def parse_pair(text: str) -> Tuple[int, int]:
    s = (text or "").strip()
    if _INVALID_RE.fullmatch(s):
        raise IgParseError("잘못된 명령 응답(999x)")
    if not _PAIR_RE.fullmatch(s):
        raise IgParseError(f"압력 형식 다름: {s!r}")
    p = s.split()
    return int(p[0]), (int(p[1]) if len(p) > 1 else 0)


def pressure_from_pair(text: str, kind: str, filament_on: bool = False) -> Tuple[Optional[float], str]:
    """(Torr 또는 None, 상태). 이온: 가수 0 → off/preparing, 음수 → invalid.
    게이지 A·B: -900 disconnected, -999 unzeroed, -5E-3 ≤ 값 < 1E-3 below, 더 작은 음수 negative, 형식 오류 invalid."""
    if kind == "ion":
        m, e = parse_pair(text)
        if m == 0:
            return None, ("preparing" if filament_on else "off")
        if m < 0:
            return None, "invalid"
        return m * 10.0 ** e, "ok"
    try:
        m, e = parse_pair(text)
    except IgParseError:
        return None, "invalid"
    if m == -900:
        return None, "disconnected"
    if m == -999:
        return None, "unzeroed"
    if m == 0:
        return None, "below"
    v = m * 10.0 ** e
    if v < 0:
        return (None, "below") if v >= GAUGE_NEG_OK_TORR - 1e-12 else (None, "negative")
    if v < GAUGE_MIN_TORR * (1 - 1e-9):
        return None, "below"
    return v, "ok"


def setpoint_from_pair(text: str) -> float:
    m, e = parse_pair(text)
    return m * 10.0 ** (e - 2)


def looks_like_pressure_reply(data: bytes) -> bool:
    lines = split_lines(data)
    return len(lines) == 1 and bool(_PAIR_RE.fullmatch(lines[0]))


@dataclass(frozen=True)
class IgReading:
    ion_torr: Optional[float]
    a_torr: Optional[float]
    b_torr: Optional[float]
    ion_state: str
    a_state: str
    b_state: str
    degas: bool
    filament: bool
    emission: bool
    err_code: int
    gas_factor: float
    sensitivity: float
    raw: Tuple[str, str, str]


def parse_status(lines: List[str]) -> IgReading:
    if any(_INVALID_RE.fullmatch(s) for s in lines):
        raise IgParseError("잘못된 명령 응답(999x)")
    if len(lines) < E_ITEM_COUNT:
        raise IgParseError(f"상태 항목 부족 {len(lines)}/{E_ITEM_COUNT}")
    it = lines[:E_ITEM_COUNT]

    def _int(n: int) -> int:
        try:
            return int(it[n - 1])
        except ValueError:
            raise IgParseError(f"{n}번 항목이 정수가 아님: {it[n - 1]!r}")

    fil = _int(2) == 1
    ion = pressure_from_pair(it[23], "ion", fil)
    a = pressure_from_pair(it[24], "conv")
    b = pressure_from_pair(it[25], "conv")
    return IgReading(ion[0], a[0], b[0], ion[1], a[1], b[1], _int(1) == 1, fil, _int(3) == 1, _int(5),
                     _int(20) / 100.0, _int(21) / 10.0, (it[23], it[24], it[25]))


# ───────── 드라이버 ─────────
@dataclass(frozen=True)
class IgEvent:
    kind: str           # identity_ok / filament_sent / filament_result / parse_error
    detail: str
    reason: Optional[str] = None
    ok: Optional[bool] = None


class IgDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[IgEvent], None]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, dict(cfg)
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.last: Optional[IgReading] = None
        self.last_result: Optional[tuple] = None    # (on/off, 성공, 문구, 시각) — 마지막 필라멘트 결과
        self.emission_since: Optional[float] = None    # 방출이 처음 확인된 벽시계 시각(베이스 판정의 안정화 기준)
        self._e_block_until = -1.0
        self._last_b: Optional[float] = None
        self._expect: Optional[str] = None              # "on" / "off"
        self._deadline: Optional[float] = None
        self._sent = False
        worker.on_connect = self._identify

    def _emit(self, kind: str, detail: str, reason: Optional[str] = None, ok: Optional[bool] = None) -> None:
        if self.on_event:
            self.on_event(IgEvent(kind, detail, reason, ok))

    @staticmethod
    def check_command(ch: str, allowed=QUERY_COMMANDS) -> None:
        if ch not in allowed:
            raise IgCommandNotAllowed(f"이온 게이지에 보내지 않는 명령: {ch!r}")

    def send_command(self, ch: str) -> None:
        """외부에서 글자 하나를 보내려는 경로 — 질의(E·F)만, A·B 는 filament_on/off 로만, 그 밖은 거부.
        거부는 포트에 닿기 전에(worker 에 넣지도 않는다)."""
        self.check_command(ch)
        self.worker.submit(Request("read", lambda s: self._query(s, ch), deadline=self.clock.monotonic() + 2.0,
                                   name=f"ig:{ch}"))

    # ───────── 읽기 ─────────
    def _write(self, session, ch: str, allowed) -> None:
        self.check_command(ch, allowed)
        session.write(ch.encode("ascii"))

    def _query(self, session, ch: str) -> List[str]:
        self.check_command(ch, QUERY_COMMANDS)
        session.transport.reset_input()      # 앞 질의의 늦은 응답(마감 뒤 도착)이 섞이지 않게 비우고 보낸다
        self._write(session, ch, QUERY_COMMANDS)
        want = E_ITEM_COUNT if ch == "E" else 1
        lines: List[str] = []
        while len(lines) < want:
            chunk = session.read_until(b"\r")
            got = split_lines(chunk)
            if any(_INVALID_RE.fullmatch(x) for x in got):
                raise IgParseError("잘못된 명령 응답(999x)")
            lines += got
        return lines

    def _identify(self, session) -> None:
        lines = self._query(session, "F")
        if not looks_like_pressure_reply(("\n".join(lines)).encode()):
            raise IgParseError(f"신원 확인(F) 응답 형식 다름: {lines!r}")
        self._emit("identity_ok", f"F → {lines[0]}")

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        if self.clock.monotonic() < self._e_block_until:
            raise IgSkip("A·B 직후 — E 보내지 않음")
        try:
            r = parse_status(self._query(session, "E"))
        except IgParseError as e:
            self._emit("parse_error", str(e), "DEVICE_REJECTED")
            raise
        self.last = r
        if r.filament and r.emission:
            if self.emission_since is None:
                self.emission_since = self.clock.wall()
        else:
            self.emission_since = None
        self._judge(r)
        return {"ig.ion_torr": r.ion_torr, "ig.a_torr": r.a_torr, "ig.b_torr": r.b_torr, "ig.filament": r.filament,
                "ig.emission": r.emission, "ig.degas": r.degas, "ig.err_code": r.err_code}

    # ───────── 필라멘트 ─────────
    def filament_on(self, on_done: Optional[Callable[[Result], None]] = None) -> None:
        self._filament("A", on_done)

    def filament_off(self, on_done: Optional[Callable[[Result], None]] = None) -> None:
        self._filament("B", on_done)

    def _filament(self, ch: str, on_done) -> None:
        now = self.clock.monotonic()
        action = "on" if ch == "A" else "off"
        not_before = None
        if ch == "A" and self._last_b is not None:
            nb = self._last_b + self.cfg["IG_RELIGHT_LOCKOUT_SEC"]
            not_before = nb if nb > now else None
        if self._expect is not None and self._expect != action:
            self._finish(self._expect, False, "다른 필라멘트 요청으로 취소")
        self._expect, self._sent = action, False
        self._deadline = (not_before or now) + (self.cfg["IG_FILAMENT_ON_TIMEOUT_SEC"] if action == "on"
                                                 else self.cfg["IG_FILAMENT_OFF_TIMEOUT_SEC"])

        def fn(session):
            self._write(session, ch, FILAMENT_COMMANDS)
            t = self.clock.monotonic()
            self._sent = True
            self._e_block_until = t + E_BLOCK_S
            self.worker.suppress(t + self.cfg["IG_FILAMENT_SWITCH_QUIET_SEC"],
                                 "필라멘트 시동 중" if ch == "A" else "필라멘트 끄는 중")
            if ch == "B":
                self._last_b = t
            self._deadline = t + (self.cfg["IG_FILAMENT_ON_TIMEOUT_SEC"] if ch == "A"
                                  else self.cfg["IG_FILAMENT_OFF_TIMEOUT_SEC"])
            self._emit("filament_sent", f"{ch} 보냄")

        def done(res: Result) -> None:
            if res.status != "ok":
                self._finish(action, False, f"{ch} 보내지 못함: {res.reason or res.status}")
            if on_done:
                on_done(res)

        self.worker.submit(Request("write", fn, deadline=(not_before or now) + 5.0, on_done=done, name=f"ig:{ch}",
                                   not_before=not_before, confirms_link=False))

    def _judge(self, r: IgReading) -> None:
        if self._expect is None or not self._sent:
            return
        if self._expect == "on" and r.filament and r.emission:
            self._finish("on", True, "필라멘트 켜짐·방출 정상")
        elif self._expect == "off" and not r.filament:
            self._finish("off", True, "필라멘트 꺼짐")

    def _finish(self, action: str, ok: bool, detail: str) -> None:
        self._expect, self._deadline, self._sent = None, None, False
        self.last_result = (action, ok, detail, self.clock.monotonic())    # 공정 엔진이 결과를 본다
        self._emit("filament_result", f"{action}: {detail}", None if ok else "IG_FILAMENT_FAILED_CMD", ok)

    def on_tick(self, tick=None) -> None:
        if self._expect is not None and self._deadline is not None and self.clock.monotonic() > self._deadline:
            r = self.last
            code = r.err_code if r is not None else None
            detail = ("기한 안에 확인 안 됨" + ("" if code is None else
                      f" — 오류 코드 {code}: {FILAMENT_ERRORS.get(code, '없음' if not code else '?')}"))
            self._finish(self._expect, False, detail)
