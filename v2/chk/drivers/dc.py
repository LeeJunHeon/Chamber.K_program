"""챔버K DC 전원 드라이버 — ODA EX 시리즈 SCPI(종단 LF).

읽기: tick 마다 MEAS:ALL?("V,I") — 스냅샷 묶기 소스(dc.v·dc.i·dc.p, 명령 기준 dc.i_set·dc.out, 공정 목표 dc.set_w).
쓰기 규칙(devices.DC, v1 실측): 설정 명령에는 응답이 없고, 쓰기 뒤 DC_WRITE_QUERY_GAP_MS 안의 질의에는 장비가 답하지 않는다
  → 질의는 마지막 쓰기 + 간격까지 기다렸다 보낸다(요청 마감 안에 못 보내면 그 질의는 실패).
  응답 없는 쓰기는 통신 정상으로 세지 않는다(confirms_link=False) — 응답이 와야 정상.
정전류 제어: ΔI 계산은 공정 엔진이 tick 마다, 드라이버는 CURR 쓰기만. tick 안 순서 t0 MEAS:ALL? → t0 + CURR_WRITE_DELAY_S
  CURR(not_before) — 다음 MEAS 까지 약 900 ms 라 간격 규칙을 지킨다.
OFF 확인(S29): Sequence — OUTP OFF → 0.5 s → OUTP? 최대 DC_OFF_VERIFY_TRIES 회("0" 이면 끝, "1"·무응답이면 OUTP OFF 다시).
  끝까지 확인 못 하면 DC_OFF_UNCONFIRMED — 단 이번 실행에서 출력을 켠 적이 없으면(maybe_on 거짓) 경고가 아니라 정보 한 줄
  (v1 _mark_off_unconfirmed, DCpower.py:356-359). OFF 는 늦추지 않는다(안전이 먼저). OFF 를 쓴 뒤 질의 금지 시간의 측정은
  suppressed(실패·두절 판정에 넣지 않음).
  출력 켜기 시퀀스는 tick 을 주면 그 tick 의 측정 뒤에 시작한다.
  OFF 미확인(켠 적 있음)이면 DC_OFF_RETRY_START_SEC 뒤 다시, 실패마다 2배·최대 COMM_OUTAGE_LOG_SEC(v1 DCpower.py:269-270·
  366-379). 그 사이 DC 를 다시 켰으면(공정이 출력 주인) 하지 않는다.
출력 켜기(v1 _initialize_power_supply, DCpower.py:678-687·829-833): *RST → *CLS → APPLy → OUTP ON 을 120 ms 간격으로 보내고
  확인 없이 제어 루프 시작(on_on). 그 뒤 OUTP? 는 기록만(재시도 포함, 실패해도 시작은 성공) — 출력이 정말 안 나오면 S31A 가 중단.
연결 때(매뉴얼 순서): *IDN? → SYST:ERR? 를 +0 까지(최대 10, 전부 이벤트) → OUTP?(dc.out 초기값) → FACT:LAST-STA?(읽기만 —
  FULLY 면 DC_LAST_STATE_FULLY) → VOLT:OVP:TRIP?·CURR:OCP:TRIP?(트립이면 DC_PROTECTION_TRIPPED). 쓰기 허용(prod·sim)이고
  DC_SET_PROTECTION 이면 VOLT:OVP·CURR:OCP 를 쓰고 되읽기(기본 끔 — bench 확인 뒤 켬).
bench: 질의는 *IDN?·SYST:ERR?·MEAS:ALL?·OUTP?·FACT:LAST-STA? 만, 상태를 바꾸는 명령은 모두 보내지 않는다(worker 거부 + 드라이버 이중 보호).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common.transport import Cancelled
from v2.common.worker import SEQ_DONE, Request, Result, Sequence, Worker

BENCH_QUERIES = frozenset({"*IDN?", "*SN?", "SYST:VERS?", "OUTP?", "KEYL?", "APPL?", "VOLT?", "CURR?", "VOLT:STEP?",
                           "CURR:STEP?", "VOLT:OVP?", "CURR:OCP?", "VOLT:OVP:TRIP?", "CURR:OCP:TRIP?", "VOLT:UVL?",
                           "VOLT:OVL?", "CURR:UCL?", "CURR:OCL?", "FACT:LAST-STA?", "FACT:AUTO-CUR?", "FACT:AUTO-LOC?",
                           "FACT:OVP?", "FACT:OCP?", "FACT:ADC?", "MEAS:ALL?", "MEAS:VOLT?", "MEAS:CURR?", "SYST:ERR?"})
# 매뉴얼 질의 28개 [DC p.86-98] — 통신층 가드(cmd_guard.DC_BENCH)와 같은 목록(시험이 확인)
CURR_WRITE_DELAY_S = 0.1     # tick 안에서 MEAS:ALL? 뒤 CURR 쓰기까지(사용자 결정 2026-10-15)
START_GAP_S = 0.12           # v1 _send 의 msleep(120) — 시작 명령 사이(DCpower.py:829-833)
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
        self.out_at_connect: Optional[bool] = None   # 연결 때 OUTP? 값
        self.maybe_on = False                        # 출력이 켜졌을 수 있다(v1 _output_maybe_on) — OFF 확인되면 거짓
        self.running = False                         # 출력을 켰다(start_output) — OFF 요청까지(v1 _is_running)
        self.off_retry_s: Optional[float] = None     # OFF 미확인 에피소드의 다음 재시도 간격(없으면 None)
        self._off_retry_queued = False
        self._off_unconfirmed_since: Optional[float] = None
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
        # 오류 기록을 +0 까지 비운다(최근 것부터, 최대 10, 읽으면 지워짐 [DC p.54, p.96]) — 전부 이벤트로
        for _ in range(10):
            err = self._query(session, "SYST:ERR?")
            self._emit("syst_err", err)
            if err.startswith(("+0,", "0,")):
                break
        self.out = self._query(session, "OUTP?") == "1"            # dc.out 초기값(S51 남은 출력 판단)
        self.out_at_connect = self.out
        if self.out:
            self.maybe_on = True
        cur = self._query(session, "FACT:LAST-STA?")
        self.last_state = cur
        # 읽기만 한다(어떤 모드에서도 쓰지 않음 — 사용자 결정 2026-10-16). 응답은 DISABLE·SAFETY·FULLY [DC p.94, p.78].
        # FULLY 만 전원 차단 전 출력까지 되살린다 → 경고(바꾸는 것은 사람이 앞면 FACTORY 메뉴에서)
        if cur.strip().upper() == "FULLY":
            self._emit("last_state", f"FACT:LAST-STA? = {cur} — 전원이 다시 들어오면 출력까지 되살아남(앞면에서 바꿀 것)",
                       "DC_LAST_STATE_FULLY")
        else:
            self._emit("last_state", f"FACT:LAST-STA? = {cur}")
        for q, name in (("VOLT:OVP:TRIP?", "OVP"), ("CURR:OCP:TRIP?", "OCP")):
            if self._query(session, q) == "1":                         # 트립 중에는 출력이 나오지 않는다 [DC p.89-91]
                self._emit("trip", f"{name} 트립 중 — 출력이 나오지 않음(해제는 사람이 앞면에서)", "DC_PROTECTION_TRIPPED")
        if not self.writes_allowed:
            return
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
            self._write(s, dc_curr_cmd(a))
            self.i_set = a
        self.worker.submit(Request("write", fn, deadline=(nb or self.clock.monotonic()) + 0.8, on_done=on_done,
                                   name="dc:CURR", not_before=nb, confirms_link=False))

    def start_output(self, v_limit: float, i_initial: float,
                     on_done: Optional[Callable[[str, Optional[str], list], None]] = None, tick=None,
                     on_on: Optional[Callable[[], None]] = None) -> None:
        """*RST → *CLS → APPLy v,i → OUTP ON(120 ms 간격, v1 _initialize_power_supply) → on_on(제어 루프 시작) →
        OUTP? 기록만(재시도 DC_OFF_VERIFY_TRIES — 실패해도 시작은 성공).
        tick 이 주어지면 그 tick 의 MEAS:ALL? 뒤(t0 + CURR_WRITE_DELAY_S)에 시작 — 쓰기가 측정을 막지 않게."""
        v = max(0.0, min(float(v_limit), self.cfg["DC_MAX_VOLTAGE"]))
        i = max(0.0, min(float(i_initial), self.cfg["DC_MAX_CURRENT"]))

        def w(cmd):
            return lambda s: self._write(s, cmd)

        def on(s):
            self.maybe_on = True                       # 부분 전송도 켜졌을 수 있다 — 보내기 전에(v1 DCpower.py:684)
            self._write(s, "OUTP ON")
            self.out, self.i_set = True, i
            if on_on:
                on_on()                                # v1: 확인 없이 제어 루프 시작(438-440)

        tries = int(self.cfg["DC_OFF_VERIFY_TRIES"])

        def check(k):
            def fn(s):
                try:
                    a = self._query(s, "OUTP?")
                except Cancelled:
                    raise
                except Exception as e:
                    a = f"무응답({type(e).__name__})"
                if a == "1" or k >= tries:
                    self._emit("out_check", f"OUTP ON 뒤 OUTP? = {a} ({k}회째) — 기록만")
                    return SEQ_DONE
            return fn
        self.running = True
        self.worker.submit_sequence(Sequence("dc:start", [("*RST", w("*RST")), ("*CLS", w("*CLS")),
                                                          ("APPLy", w(dc_appl_cmd(v, i))), ("ON", on)]
                                             + [(f"OUTP? {k}", check(k)) for k in range(1, tries + 1)],
                                             gap_s=START_GAP_S, step_timeout_s=OFF_STEP_TIMEOUT_S, on_done=on_done,
                                             confirms_link=False,
                                             start_after=(tick.t_mono + CURR_WRITE_DELAY_S) if tick else None))

    def _schedule_off_retry(self) -> None:
        """OFF 미확인 재시도 — 첫 간격 DC_OFF_RETRY_START_SEC, 실패마다 2배, 최대 COMM_OUTAGE_LOG_SEC(v1 DCpower.py:366-379)."""
        if self._off_retry_queued:
            return
        cap = float(self.cfg["COMM_OUTAGE_LOG_SEC"])
        start = float(self.cfg["DC_OFF_RETRY_START_SEC"])
        self.off_retry_s = start if self.off_retry_s is None else min(cap, self.off_retry_s * 2.0)
        self._off_retry_queued = True

        def fire(_res) -> None:
            self._off_retry_queued = False
            if self.running or not self.maybe_on:
                return                                     # 다시 켰거나(공정이 주인) 이미 꺼짐 확인 — 하지 않는다
            self.output_off_confirmed(where="OFF 재시도")
        self.worker.submit(Request("write", lambda s: None, name="dc:OFF 재시도 대기", on_done=fire,
                                   not_before=self.clock.monotonic() + self.off_retry_s, confirms_link=False))

    def _suppress_after_off(self) -> None:
        """OFF 를 쓴 뒤 질의 금지 시간 동안의 측정은 실패가 아니라 suppressed — 연속 실패·두절 판정에 넣지 않는다."""
        self.worker.suppress(self._last_write + self.gap_s + 0.1, "DC OFF 확인 중")

    def output_off_confirmed(self, on_done: Optional[Callable[[str, Optional[str], list], None]] = None,
                             where: str = "OFF") -> None:
        tries = self.cfg["DC_OFF_VERIFY_TRIES"]
        self.running = False
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
                    self.maybe_on = False
                    since, self._off_unconfirmed_since, self.off_retry_s = self._off_unconfirmed_since, None, None
                    self._emit("off_confirmed", f"OUTP?=0 ({k}회째, {where})"
                               + (f" — 미확인 {self.clock.monotonic() - since:.0f} s 뒤" if since is not None else ""))
                    return SEQ_DONE
                if k >= tries:
                    raise DcOffUnconfirmed("OUTP? 응답 " + "/".join(answers))
                self._write(s, "OUTP OFF")
                self._suppress_after_off()
            return fn

        def done(name, reason, results):
            if reason and reason != "BENCH_READ_ONLY":
                if not self.maybe_on:                       # 켠 적 없음 — 에피소드가 아니라 정보 한 줄(v1 356-359)
                    self._emit("off_unverified", "DC OFF 생략/미확인(" + "/".join(answers) + ") — 이번 실행에서 출력을 켠 적 없음")
                    reason = None
                else:
                    first = self._off_unconfirmed_since is None
                    if first:
                        self._off_unconfirmed_since = self.clock.monotonic()
                        self._emit("off_unconfirmed", f"DC 출력 OFF 확인 못 함({where}): " + "/".join(answers)
                                   + " — 장비 전면 확인, 통신되면 자동 재시도", "DC_OFF_UNCONFIRMED")
                    reason = "DC_OFF_UNCONFIRMED"
                    self._schedule_off_retry()
            if on_done:
                on_done(name, reason, results)

        steps = [("OUTP OFF", off)] + [(f"OUTP? {k}", verify(k)) for k in range(1, tries + 1)]
        self.worker.submit_sequence(Sequence("dc:off", steps, gap_s=self.gap_s, step_timeout_s=OFF_STEP_TIMEOUT_S,
                                             on_done=done, confirms_link=False))


# ───────── 쓰기 명령 글자(v1 device/DCpower.py 와 같게) ─────────
def dc_appl_cmd(v: float, i: float) -> str:
    """v1 device/DCpower.py:682 f"APPLy {voltage:.2f},{current:.4f}" — 매뉴얼상 APPL 과 같은 명령[DC p.86]이지만 v1 과 같은 글자."""
    return f"APPLy {v:.2f},{i:.4f}"


def dc_curr_cmd(i: float) -> str:
    """v1 device/DCpower.py:590 f"CURR {self.current_current:.4f}"."""
    return f"CURR {i:.4f}"
