"""챔버K DC 전원 드라이버 — ODA EX 시리즈 SCPI(종단 LF).

읽기: tick 마다 MEAS:ALL?("V,I") — 스냅샷 묶기 소스(dc.v·dc.i·dc.p, 명령 기준 dc.i_set·dc.out, 공정 목표 dc.set_w).
쓰기 규칙(devices.DC, v1 실측): 설정 명령에는 응답이 없고, 쓰기 뒤 DC_WRITE_QUERY_GAP_MS 안의 질의에는 장비가 답하지 않는다
  → 질의는 마지막 쓰기 + 간격까지 기다렸다 보낸다(요청 마감 안에 못 보내면 그 질의는 실패).
  응답 없는 쓰기는 통신 정상으로 세지 않는다(confirms_link=False) — 응답이 와야 정상.
정전류 제어: ΔI 계산은 공정 엔진이 tick 마다, 드라이버는 CURR 쓰기만. tick 안 순서 t0 MEAS:ALL? → t0 + CURR_WRITE_DELAY_S
  CURR(not_before) — 다음 MEAS 까지 약 900 ms 라 간격 규칙을 지킨다.
OFF 확인(S29): Sequence — OUTP OFF → 0.5 s → OUTP? 최대 DC_OFF_VERIFY_TRIES 회("0" 이면 끝, "1"·무응답이면 OUTP OFF 다시).
  끝까지 확인 못 하면 DC_OFF_UNCONFIRMED. OFF 는 늦추지 않는다(안전이 먼저). OFF 를 쓴 뒤 질의 금지 시간의 측정은
  suppressed(실패·두절 판정에 넣지 않음).
  출력 켜기 시퀀스는 tick 을 주면 그 tick 의 측정 뒤에 시작한다.
연결 때: *IDN? 로 형명 기록, SYST:ERR? 1회. 쓰기 허용(prod·sim)이면 FACT:LAST-STA 를 읽고 SAF 가 아닐 때만 쓰고 되읽기,
  DC_SET_PROTECTION 이면 VOLT:OVP·CURR:OCP 를 쓰고 되읽기(기본 끔 — bench 확인 뒤 켬).
bench: 질의는 *IDN?·SYST:ERR?·MEAS:ALL?·OUTP?·FACT:LAST-STA? 만, 상태를 바꾸는 명령은 모두 보내지 않는다(worker 거부 + 드라이버 이중 보호).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common.transport import Cancelled
from v2.common.worker import SEQ_DONE, Request, Result, Sequence, Worker

BENCH_QUERIES = frozenset({"*IDN?", "SYST:ERR?", "MEAS:ALL?", "OUTP?", "FACT:LAST-STA?"})   # 값만 읽음(5-2)
CURR_WRITE_DELAY_S = 0.1     # tick 안에서 MEAS:ALL? 뒤 CURR 쓰기까지(사용자 결정 2026-10-15)
OFF_STEP_TIMEOUT_S = 1.5     # OFF 확인 질의 하나의 한도(v1 OUTP? 1.5 s)


class DcError(Exception):
    pass


class DcReadOnly(DcError):
    reason = "BENCH_READ_ONLY"


class DcQueryBlocked(DcError):
    """쓰기 뒤 질의 금지 시간이 요청 마감을 넘음 — 보내지 않음."""


class DcOffUnconfirmed(DcError):
    reason = "DC_OFF_UNCONFIRMED"


@dataclass(frozen=True)
class DcEvent:
    kind: str     # identity / syst_err / last_state / protection / off_confirmed / off_unconfirmed
    detail: str
    reason: Optional[str] = None


class DcDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[DcEvent], None]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, dict(cfg)
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.gap_s = cfg["DC_WRITE_QUERY_GAP_MS"] / 1000.0
        self.model: Optional[str] = None
        self.last_state: Optional[str] = None     # FACT:LAST-STA? 값(연결 때)
        self.meas_listeners: list = []            # (값, 읽기 시작 시각) — 정전류 제어
        self.i_set: Optional[float] = None
        self.out: Optional[bool] = None
        self.set_w: Optional[float] = None
        self._last_write = -1e9
        worker.on_connect = self._on_connect

    def _emit(self, kind: str, detail: str, reason: Optional[str] = None) -> None:
        if self.on_event:
            self.on_event(DcEvent(kind, detail, reason))

    # ───────── 줄 입출력 ─────────
    def _write(self, session, cmd: str) -> None:
        if cmd.endswith("?"):
            raise ValueError("질의는 _query 로")
        if not self.writes_allowed:
            raise DcReadOnly(f"bench — 보내지 않음: {cmd}")
        session.write((cmd + "\n").encode("ascii"))
        self._last_write = self.clock.monotonic()

    def _query(self, session, cmd: str) -> str:
        if not self.writes_allowed and cmd not in BENCH_QUERIES:
            raise DcReadOnly(f"bench — 허용 질의 아님: {cmd}")
        ready = self._last_write + self.gap_s
        if ready > self.clock.monotonic():
            if ready > session.deadline:
                raise DcQueryBlocked(f"{cmd}: 쓰기 뒤 {self.gap_s:g} s 질의 금지 — 마감 안에 못 보냄")
            self.clock.sleep_until(ready)
        session.transport.reset_input()
        session.write((cmd + "\n").encode("ascii"))
        return session.read_until(b"\n").decode("ascii", errors="replace").strip()

    # ───────── 읽기 ─────────
    def source_read(self, session, seq: int) -> Dict[str, Any]:
        t0 = self.clock.monotonic()
        raw = self._query(session, "MEAS:ALL?")
        try:
            v, i = (float(x) for x in raw.split(","))
        except ValueError:
            raise DcError(f"MEAS:ALL? 형식 다름: {raw!r}")
        out = {"dc.v": v, "dc.i": i, "dc.p": round(v * i, 3), "dc.i_set": self.i_set, "dc.out": self.out,
               "dc.set_w": self.set_w}
        for f in list(self.meas_listeners):        # 정전류 제어(공정 엔진) — 측정 직후 ΔI, CURR 는 t0 + CURR_WRITE_DELAY_S
            try:
                f(out, t0)
            except Exception:
                pass
        return out

    # ───────── 연결 ─────────
    def _on_connect(self, session) -> None:
        self._last_write = -1e9
        self.model = self._query(session, "*IDN?")
        self._emit("identity", self.model)
        err = self._query(session, "SYST:ERR?")
        self._emit("syst_err", err)
        cur = self._query(session, "FACT:LAST-STA?")
        self.last_state = cur
        if not self.writes_allowed:
            self._emit("last_state", f"FACT:LAST-STA? = {cur} (bench — 읽기만)")
            return
        if cur.upper() != "SAF":
            self._write(session, "FACT:LAST-STA SAF")
            back = self._query(session, "FACT:LAST-STA?")
            if back.upper() == "SAF":
                self._emit("last_state", f"FACT:LAST-STA {cur} → SAF(영구 저장) 확인")
            else:
                self._emit("last_state", f"FACT:LAST-STA 쓰기 뒤 되읽기 {back!r}", "VERIFY_MISMATCH")
        else:
            self._emit("last_state", "FACT:LAST-STA 이미 SAF — 쓰지 않음")
        if self.cfg["DC_SET_PROTECTION"]:
            ovp, ocp = self.cfg["DC_MAX_VOLTAGE"], self.cfg["DC_MAX_CURRENT"]
            self._write(session, f"VOLT:OVP {ovp:.2f}")
            self._write(session, f"CURR:OCP {ocp:.4f}")
            b_ovp = float(self._query(session, "VOLT:OVP?"))
            b_ocp = float(self._query(session, "CURR:OCP?"))
            if abs(b_ovp - ovp) > 0.05 or abs(b_ocp - ocp) > 0.0005:
                self._emit("protection", f"OVP/OCP 되읽기 {b_ovp}/{b_ocp} ≠ {ovp}/{ocp}", "VERIFY_MISMATCH")
            else:
                self._emit("protection", f"OVP {ovp} V·OCP {ocp} A 확인")

    # ───────── 쓰기 ─────────
    def set_current(self, amps: float, tick=None, on_done: Optional[Callable[[Result], None]] = None,
                    not_before: Optional[float] = None) -> None:
        """CURR 쓰기 — tick 이 주어지면 t0 + CURR_WRITE_DELAY_S 뒤(같은 tick 의 MEAS:ALL? 다음). not_before 로 직접 줄 수도."""
        a = max(0.0, min(float(amps), self.cfg["DC_MAX_CURRENT"]))
        nb = (tick.t_mono + CURR_WRITE_DELAY_S) if tick is not None else not_before

        def fn(s):
            self._write(s, f"CURR {a:.4f}")
            self.i_set = a
        self.worker.submit(Request("write", fn, deadline=(nb or self.clock.monotonic()) + 0.8, on_done=on_done,
                                   name="dc:CURR", not_before=nb, confirms_link=False))

    def start_output(self, v_limit: float, i_initial: float,
                     on_done: Optional[Callable[[str, Optional[str], list], None]] = None, tick=None) -> None:
        """*RST → *CLS → APPL v,i → OUTP ON → OUTP? 확인(v1 _initialize_power_supply).
        tick 이 주어지면 그 tick 의 MEAS:ALL? 뒤(t0 + CURR_WRITE_DELAY_S)에 시작 — 쓰기가 측정을 막지 않게."""
        v = max(0.0, min(float(v_limit), self.cfg["DC_MAX_VOLTAGE"]))
        i = max(0.0, min(float(i_initial), self.cfg["DC_MAX_CURRENT"]))

        def w(cmd):
            return lambda s: self._write(s, cmd)

        def on(s):
            self._write(s, "OUTP ON")
            self.out, self.i_set = True, i

        def verify(s):
            if self._query(s, "OUTP?") != "1":
                raise DcError("OUTP ON 뒤 OUTP? 가 1 이 아님")
            return SEQ_DONE
        self.worker.submit_sequence(Sequence("dc:start", [("*RST", w("*RST")), ("*CLS", w("*CLS")),
                                                          ("APPL", w(f"APPL {v:.2f},{i:.4f}")), ("ON", on),
                                                          ("verify", verify)],
                                             gap_s=0.05, step_timeout_s=OFF_STEP_TIMEOUT_S, on_done=on_done,
                                             confirms_link=False,
                                             start_after=(tick.t_mono + CURR_WRITE_DELAY_S) if tick else None))

    def _suppress_after_off(self) -> None:
        """OFF 를 쓴 뒤 질의 금지 시간 동안의 측정은 실패가 아니라 suppressed — 연속 실패·두절 판정에 넣지 않는다."""
        self.worker.suppress(self._last_write + self.gap_s + 0.1, "DC OFF 확인 중")

    def output_off_confirmed(self, on_done: Optional[Callable[[str, Optional[str], list], None]] = None) -> None:
        tries = self.cfg["DC_OFF_VERIFY_TRIES"]
        answers = []

        def off(s):
            self._write(s, "OUTP OFF")
            self.out = False
            self._suppress_after_off()

        def verify(k):
            def fn(s):
                try:
                    a = self._query(s, "OUTP?")
                except Cancelled:
                    raise
                except Exception as e:                      # 무응답·금지 시간 — 다시 끄고 다음 확인으로
                    a = f"무응답({type(e).__name__})"
                answers.append(a)
                if a == "0":
                    self._emit("off_confirmed", f"OUTP?=0 ({k}회째)")
                    return SEQ_DONE
                if k >= tries:
                    raise DcOffUnconfirmed("OUTP? 응답 " + "/".join(answers))
                self._write(s, "OUTP OFF")
                self._suppress_after_off()
            return fn

        def done(name, reason, results):
            if reason and reason != "BENCH_READ_ONLY":
                self._emit("off_unconfirmed", "DC 출력 OFF 확인 못 함: " + "/".join(answers), "DC_OFF_UNCONFIRMED")
            if on_done:
                on_done(name, "DC_OFF_UNCONFIRMED" if reason and reason != "BENCH_READ_ONLY" else reason, results)

        steps = [("OUTP OFF", off)] + [(f"OUTP? {k}", verify(k)) for k in range(1, tries + 1)]
        self.worker.submit_sequence(Sequence("dc:off", steps, gap_s=self.gap_s, step_timeout_s=OFF_STEP_TIMEOUT_S,
                                             on_done=done, confirms_link=False))
