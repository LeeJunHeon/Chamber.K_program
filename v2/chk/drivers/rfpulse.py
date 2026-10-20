"""챔버K RF Pulse 드라이버 — AE CESAR 1310(AE Bus, 주소 RFPULSE_ADDR). v1 device/RFpulse.py 를 읽고 다시 쓴 것(import 안 함).

읽기(스냅샷 묶기 소스): 162(상태)·165(순방향)·166(반사), 질의 하나 RFPULSE_QUERY_TIMEOUT_MS(250), 사이 RFPULSE_CMD_GAP_MS.
  RFPULSE_READ_PLAN: full=매초 셋, alternate=166 매초 + 165(짝수 tick)/162(홀수 tick), auto=질의가 제한을 넘으면 번갈아로.
  순방향·상태 필드는 자기 품질(own_quality)이라 번갈아면 stale·나이 1.
START(v1 순서·대기, RFpulse.py:925-960): 14 Host → 1 RF OFF(사전) → 200 ms → 3 조절 모드 6(FWD) → 8 셋포인트 → [93 주파수]
  → [96 듀티] → 27 펄싱 1 → 2 RF ON → 800 ms → 193·196 되읽기. I/O 하나가 worker 요청 하나(그 사이 tick 읽기가 끼어든다).
  단계마다 3회 재시도(v1 _make_cmd retries=3): 백오프 max(150 ms, 간격), CSR 5 는 max(간격 × 1.5, 1200 ms)(v1 745-751).
  응답 한도는 설정 명령 2000 ms, RF ON/OFF 2500 ms(v1 RFPULSE_ACK_TIMEOUT_MS·927-956). CSR 1(Host 아님) → 14 Host 뒤,
  CSR 2(RF ON 중) → 1 RF OFF 뒤 다시(이것도 재시도 한 번). 그 밖의 실패가 남으면 RFP_START_FAILED.
  되읽기는 요청한 항목만 판정, 질의 1회 재시도, 요청 안 한 항목의 실패는 무시(v1 1023-1034·1077-1080). 불일치는
  RFPULSE_VERIFY_PULSE_CONFIG: true 면 RF OFF 하고 실패(RFP_CONFIG_MISMATCH), false 면 경고만.
RF OFF(안전): 1 → CSR 1 이면 14 Host 뒤 다시 1(v1 595-606) → 162 로 출력 꺼짐 확인, 안 되면 한 번 더, 끝내 못 하면 RFP_OFF_UNCONFIRMED.
연결: 128·130·198 로 기종·용량·SW 버전 기록. 쓰기 허용(prod·sim)이면 명령 33 = RFPULSE_REFP_AUTO_OFF_SEC 를 쓰고 152 로 되읽기
  (Host 모드가 아니면 출력이 꺼져 있을 때만 14 Host 로 바꾼 뒤 — 켜져 있으면 건너뛰고 이벤트).
bench: 162·165·166·155·128·130·198·152 질의만(152 = 명령 33 현재 값), 상태를 바꾸는 명령(1~127)은 모두 거부.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common import aebus
from v2.common.transport import Cancelled, ReadTimeout
from v2.common.worker import Request, Result, Worker

RF_OFF, RF_ON, SET_MODE, SET_SETPOINT, SET_CONTROL, SET_PULSING, SET_AUTO_OFF, SET_FREQ, SET_DUTY = 1, 2, 3, 8, 14, 27, 33, 93, 96
Q_MODEL, Q_CAPACITY, Q_AUTO_OFF, Q_CONTROL, Q_STATUS, Q_FWD, Q_REF, Q_FREQ, Q_DUTY, Q_SW = 128, 130, 152, 155, 162, 165, 166, 193, 196, 198
ACK_TIMEOUT_S = 2.0          # v1 RFPULSE_ACK_TIMEOUT_MS(lib/config.py:649) — 설정 명령 CSR 대기
RF_ONOFF_TIMEOUT_S = 2.5     # v1 max(RFPULSE_ACK_TIMEOUT_MS, 2500) — RF ON·OFF
RF_ON_SETTLE_S = 0.8         # v1 RFPULSE_POLL_START_DELAY_AFTER_RF_ON_MS(lib/config.py:657)
START_RETRIES = 3            # v1 _make_cmd(retries=3)
BENCH_QUERIES = frozenset({Q_STATUS, Q_FWD, Q_REF, Q_CONTROL, Q_MODEL, Q_CAPACITY, Q_SW, Q_AUTO_OFF})   # 152: 명령 33 현재 값(읽기만)
CSR_TEXT = {0: "accepted", 1: "wrong control mode(Host 아님)", 2: "RF output is on", 4: "data out of range",
            5: "User Port RF signal off", 7: "active fault(s)", 9: "data byte count", 19: "recipe mode",
            50: "frequency out of range", 51: "duty out of range", 99: "not implemented"}   # [CESAR p.96 / 4-62]


class RfpError(Exception):
    reason = "RFP_FAULT"


class RfpReadOnly(RfpError):
    reason = "BENCH_READ_ONLY"


class RfpCsr(RfpError):
    reason = "RFP_START_FAILED"

    def __init__(self, cmd: int, csr: int) -> None:
        super().__init__(f"명령 {cmd} CSR {csr}({CSR_TEXT.get(csr, '?')})")
        self.cmd, self.csr = cmd, csr


class RfpConfigMismatch(RfpError):
    reason = "RFP_CONFIG_MISMATCH"


@dataclass(frozen=True)
class RfpEvent:
    kind: str     # identity / auto_off / csr_recovery / config_readback / read_plan / off_confirmed / off_unconfirmed
    detail: str
    reason: Optional[str] = None


def parse_status(b: bytes) -> Dict[str, bool]:
    """162 상태 4바이트 [CESAR p.114-115 / 4-80~81]."""
    return {"out_on": bool(b[0] & 0x20), "rf_on_req": bool(b[0] & 0x40), "sp_out_of_tol": bool(b[0] & 0x80),
            "itl_open": bool(b[1] & 0x80), "overtemp": bool(b[1] & 0x08), "current_limit": bool(b[3] & 0x01),
            "ext_fault": bool(b[3] & 0x20), "cex_lock": bool(b[3] & 0x80)}


class RfPulseDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[RfpEvent], None]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, dict(cfg)
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.addr = cfg["RFPULSE_ADDR"]
        self.gap_s = cfg["RFPULSE_CMD_GAP_MS"] / 1000.0
        self.q_timeout_s = cfg["RFPULSE_QUERY_TIMEOUT_MS"] / 1000.0
        self.alternate = cfg["RFPULSE_READ_PLAN"] == "alternate"
        self.model: Optional[str] = None
        self.set_w: Optional[float] = None
        self.out_at_connect: Optional[bool] = None        # 연결 때 RF 출력(162)
        self.control_at_connect: Optional[int] = None     # 연결 때 제어 모드(155, 2 = Host)
        self.freq_hz: Optional[int] = None
        self.duty_pct: Optional[int] = None
        self._last = -1e9
        worker.on_connect = self._on_connect

    def _emit(self, kind: str, detail: str, reason: Optional[str] = None) -> None:
        if self.on_event:
            self.on_event(RfpEvent(kind, detail, reason))

    # ───────── 한 번 주고받기 ─────────
    def _x(self, session, cmd: int, data: bytes = b"", timeout_s: Optional[float] = None) -> bytes:
        if cmd < 128 and not self.writes_allowed:
            raise RfpReadOnly(f"bench — 명령 {cmd} 보내지 않음")
        if cmd >= 128 and not self.writes_allowed and cmd not in BENCH_QUERIES:
            raise RfpReadOnly(f"bench — 허용 질의 아님: {cmd}")
        ready = self._last + self.gap_s
        if ready > self.clock.monotonic():
            self.clock.sleep_until(ready)
        try:
            return aebus.transact(session, self.addr, cmd, data, self.q_timeout_s if timeout_s is None else timeout_s)
        finally:
            self._last = self.clock.monotonic()

    def _exec(self, session, cmd: int, data: bytes = b"", timeout_s: Optional[float] = None) -> int:
        out = self._x(session, cmd, data, timeout_s)
        return out[0] if out else -1

    # ───────── 연결 ─────────
    def _on_connect(self, session) -> None:
        self._last = -1e9
        self.model = self._x(session, Q_MODEL).decode("ascii", errors="replace")
        cap = aebus.u16(self._x(session, Q_CAPACITY))
        sw = self._x(session, Q_SW).decode("ascii", errors="replace")
        self._emit("identity", f"{self.model} · {cap} W · SW {sw}")
        try:                                              # 연결 때 출력·제어 모드(S51 남은 출력 판단 — 읽기만)
            self.out_at_connect = parse_status(self._x(session, Q_STATUS))["out_on"]
            ctl = self._x(session, Q_CONTROL)
            self.control_at_connect = ctl[0] if ctl else None
        except Cancelled:
            raise
        except Exception:
            self.out_at_connect = self.control_at_connect = None
        if not self.writes_allowed:
            try:                                          # bench: 명령 33 현재 값·제어 모드를 읽기만(보고용)
                cur = self._x(session, Q_AUTO_OFF)
                ctl = self._x(session, Q_CONTROL)
                self._emit("auto_off_read", f"명령 33 현재 {cur[0] if cur else '?'} s · 제어 모드 {ctl[0] if ctl else '?'}")
            except Cancelled:
                raise
            except Exception as e:
                self._emit("auto_off_read", f"명령 33 현재 값 못 읽음: {type(e).__name__}")
            return
        sec = int(self.cfg["RFPULSE_REFP_AUTO_OFF_SEC"])
        if self._x(session, Q_CONTROL)[0] != 2:
            # 명령 33 은 Host 모드에서만 받는다(CSR 1). 출력이 켜져 있을 때 제어 모드를 바꾸면 장비가 RF 를 끄므로
            # (매뉴얼 명령 14) 앞면에서 쓰는 중일 수 있는 RF 를 끄지 않도록, 출력이 꺼져 있을 때만 Host 로 바꾼다.
            if parse_status(self._x(session, Q_STATUS))["out_on"]:
                self._emit("auto_off", "RF 출력 중이라 Host 로 바꾸지 않음 — 명령 33 은 다음 연결 때", "STATE_NOT_ALLOWED")
                return
            self._exec(session, SET_CONTROL, b"\x02")
        csr = self._exec(session, SET_AUTO_OFF, bytes([sec]))
        back = self._x(session, Q_AUTO_OFF)
        if csr == 0 and back and back[0] == sec:
            self._emit("auto_off", f"명령 33 반사 전력 자동 차단 {sec} s 확인")
        else:
            self._emit("auto_off", f"명령 33 쓰기 CSR {csr}, 되읽기 {back.hex()} ≠ {sec}", "VERIFY_MISMATCH")

    # ───────── 읽기 ─────────
    def source_read(self, session, seq: int) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        plan = [Q_STATUS, Q_FWD, Q_REF] if not self.alternate else ([Q_REF, Q_FWD] if seq % 2 == 0 else [Q_REF, Q_STATUS])
        for q in plan:
            t0 = self.clock.monotonic()
            try:
                data = self._x(session, q)
            except ReadTimeout:
                # 질의 자체가 제한을 다 쓴 경우만(요청 마감이 먼저 끝난 경우 — 앞 작업에 밀림 — 는 장비가 느린 게 아니다)
                slow = self.clock.monotonic() - t0 >= self.q_timeout_s - 1e-6
                if self.cfg["RFPULSE_READ_PLAN"] == "auto" and not self.alternate and slow:
                    self.alternate = True
                    self._emit("read_plan", f"질의 {q} 가 {self.q_timeout_s * 1000:.0f} ms 를 넘음 — 166 매초, 165·162 번갈아로")
                raise
            if q == Q_STATUS:
                st = parse_status(data)
                out.update({"rfp.out_on": st["out_on"], "rfp.itl_open": st["itl_open"], "rfp.status_raw": data.hex()})
            elif q == Q_FWD:
                out["rfp.fwd_w"] = float(aebus.u16(data))
            else:
                out["rfp.ref_w"] = float(aebus.u16(data))
        out.update({"rfp.set_w": self.set_w, "rfp.freq_hz": self.freq_hz, "rfp.duty_pct": self.duty_pct})
        return out

    # ───────── START(v1 device/RFpulse.py:925-1080 — 단계마다 재시도, I/O 하나가 worker 요청 하나) ─────────
    def _op(self, name: str, gen, on_done=None) -> None:
        """gen: yield ("x", 명령, 데이터, 응답 한도) → ("ok", 바이트) | ("err", 예외 문구) / ("sleep", 초).
        정상 끝나면 성공. RfpError 류를 던지면 그 사유로 실패."""
        if not self.writes_allowed:
            if on_done:
                on_done(f"rfp:{name}", "BENCH_READ_ONLY", [])
            return
        st: Dict[str, Any] = {"nb": None, "results": [], "done": False}

        def finish(reason: Optional[str], why: str = "") -> None:
            if st["done"]:
                return
            st["done"] = True
            if on_done:
                on_done(f"rfp:{name}", reason, st["results"])

        def advance(value=None) -> None:
            try:
                item = gen.send(value)
            except StopIteration:
                finish(None)
                return
            except RfpError as e:
                st["results"].append(Result(Request("write", lambda s: None, name=name), "fail", reason=e.reason,
                                            error=f"{type(e).__name__}: {e}"))
                finish(e.reason, str(e))
                return
            if item[0] == "sleep":
                st["nb"] = self.clock.monotonic() + float(item[1])
                advance(None)
                return
            _, cmd, data, limit = item
            nb, st["nb"] = st["nb"], None
            self.worker.submit(Request("write", lambda s: self._x(s, cmd, data, limit), name=f"rfp:{name}:{cmd}",
                                       not_before=nb, exec_timeout_s=limit + self.gap_s + 0.2, confirms_link=True,
                                       on_done=lambda r: result(r)))

        def result(r: Result) -> None:
            st["results"].append(r)
            if st["done"]:
                return
            if r.status == "ok":
                advance(("ok", r.value))
            elif r.status in ("cancelled", "rejected"):
                finish(r.reason or r.status)
            else:
                advance(("err", r.error or r.reason or r.status))
        advance(None)

    def _exec_retry(self, name: str, cmd: int, data: bytes = b"", limit: float = ACK_TIMEOUT_S):
        """설정 명령 한 단계 — v1 _make_cmd(retries=3)·_fail_inflight·_try_csr_recovery:
        실패(무응답·NAK·CSR ≠ 0)면 max(150 ms, 간격) 뒤(CSR 5 는 max(간격 × 1.5, 1200 ms)) 다시, 3회.
        CSR 1(Host 아님) → 14 Host 뒤, CSR 2(RF ON 중) → 1 RF OFF 뒤 다시 — 이것도 재시도 한 번을 쓴다."""
        tries = 0
        while True:
            r = yield ("x", cmd, data, limit)
            csr = (r[1][0] if r[1] else -1) if r[0] == "ok" else None
            if csr == 0:
                return
            if csr in (1, 2) and cmd not in (SET_CONTROL, RF_OFF) and tries < START_RETRIES:
                fix = SET_CONTROL if csr == 1 else RF_OFF
                f = yield ("x", fix, b"\x02" if fix == SET_CONTROL else b"", RF_ONOFF_TIMEOUT_S if fix == RF_OFF else ACK_TIMEOUT_S)
                self._emit("csr_recovery", f"{name}: CSR {csr}({CSR_TEXT[csr]}) → 명령 {fix} 뒤 다시 ({f[0]})")
                tries += 1
                yield ("sleep", max(0.2, self.gap_s))
                continue
            if tries >= START_RETRIES:
                if csr is None:
                    raise RfpError(f"{name}: 명령 {cmd} 응답 없음·오류({r[1]})")
                raise RfpCsr(cmd, csr)
            tries += 1
            backoff = max(self.gap_s * 1.5, 1.2) if csr == 5 else max(0.15, self.gap_s)
            self._emit("retry", f"{name}: 명령 {cmd} {'CSR ' + str(csr) if csr is not None else r[1]} — {tries}/{START_RETRIES} 다시")
            yield ("sleep", backoff)

    def start(self, target_w: float, freq_hz: Optional[int] = None, duty: Optional[int] = None,
              on_done: Optional[Callable[[str, Optional[str], list], None]] = None, tick=None) -> None:
        """v1 순서와 대기(RFpulse.py:925-960): 14 Host → 1 RF OFF(2.5 s) → 200 ms → 3 FWD → 8 셋포인트 → [93 주파수] → [96 듀티]
        → 27 펄싱 1 → 2 RF ON(2.5 s) → 800 ms → 193·196 되읽기(요청한 항목만 판정, 1회 재시도, 요청 안 한 항목 실패는 무시)."""
        if target_w > self.cfg["RFPULSE_MAX_POWER"]:
            raise ValueError(f"목표 {target_w} W > RFPULSE_MAX_POWER")
        sp = int(round(target_w))

        def query_retry(cmd: int, n: int):
            for _ in range(2):                                   # v1 _enqueue_query(retries=1)
                r = yield ("x", cmd, b"", self.q_timeout_s)
                if r[0] == "ok" and len(r[1]) >= n:
                    return r[1]
            return None

        def gen():
            yield from self._exec_retry("Host", SET_CONTROL, b"\x02")
            yield from self._exec_retry("사전 RF OFF", RF_OFF, b"", RF_ONOFF_TIMEOUT_S)   # RF 가 켜진 채 설정이 바뀌지 않게
            yield ("sleep", 0.2)
            yield from self._exec_retry("조절 모드", SET_MODE, b"\x06")
            yield from self._exec_retry("셋포인트", SET_SETPOINT, aebus.le(sp, 2))
            if freq_hz is not None:
                yield from self._exec_retry("주파수", SET_FREQ, aebus.le(int(freq_hz), 3))
            if duty is not None:
                yield from self._exec_retry("듀티", SET_DUTY, aebus.le(int(duty), 2))
            yield from self._exec_retry("펄싱", SET_PULSING, b"\x01")
            yield from self._exec_retry("RF ON", RF_ON, b"", RF_ONOFF_TIMEOUT_S)
            self.set_w = float(target_w)
            yield ("sleep", RF_ON_SETTLE_S)
            fb = yield from query_retry(Q_FREQ, 3)
            db = yield from query_retry(Q_DUTY, 2)
            f = aebus.u24(fb) if fb is not None else None
            d = aebus.u16(db) if db is not None else None
            self.freq_hz, self.duty_pct = f, d
            bad = []
            if freq_hz is not None:
                if f is None:
                    bad.append("주파수 되읽기 실패")
                elif abs(f - int(freq_hz)) > 1:
                    bad.append(f"주파수 요청 {freq_hz} → {f}")
            if duty is not None:
                if d is None:
                    bad.append("듀티 되읽기 실패")
                elif d != int(duty):
                    bad.append(f"듀티 요청 {duty} → {d}")
            if bad:
                if self.cfg["RFPULSE_VERIFY_PULSE_CONFIG"]:
                    yield ("x", RF_OFF, b"", RF_ONOFF_TIMEOUT_S)
                    self.set_w = 0.0
                    self._emit("config_readback", "펄스 설정 되읽기 불일치 — RF OFF: " + ", ".join(bad), "RFP_CONFIG_MISMATCH")
                    raise RfpConfigMismatch(", ".join(bad))
                self._emit("config_readback", "펄스 설정 되읽기 불일치(경고만): " + ", ".join(bad), "RFP_CONFIG_MISMATCH")
            else:
                self._emit("config_readback", f"되읽기 {f if f is not None else '?'} Hz · {d if d is not None else '?'} %")

        def done(name, reason, results):
            r = reason
            if reason and reason not in ("BENCH_READ_ONLY", "RFP_CONFIG_MISMATCH"):
                r = "RFP_START_FAILED"
            if on_done:
                on_done(name, r, results)
        g = gen()
        if tick is not None:                                     # 같은 tick 의 읽기 뒤에 시작
            def delayed():
                yield ("sleep", max(0.0, tick.t_mono + 0.1 - self.clock.monotonic()))
                yield from g
            self._op("start", delayed(), done)
        else:
            self._op("start", g, done)

    # ───────── RF OFF(안전) ─────────
    def rf_off_confirmed(self, on_done: Optional[Callable[[Any], None]] = None) -> None:
        """1 RF OFF(2.5 s) → CSR 0 → 162 로 출력 꺼짐 확인, 안 되면 한 번 더. CSR 1(Host 아님)이면 14 Host 뒤 다시 RF OFF
        (v1 _try_csr_recovery, RFpulse.py:595-606)."""
        def fn(s):
            for attempt in (1, 2):
                try:
                    csr = self._exec(s, RF_OFF, b"", RF_ONOFF_TIMEOUT_S)
                    if csr == 1:
                        hc = self._exec(s, SET_CONTROL, b"\x02", ACK_TIMEOUT_S)
                        self._emit("csr_recovery", f"RF OFF: CSR 1(Host 아님) → 명령 14 뒤 다시 (CSR {hc})")
                        csr = self._exec(s, RF_OFF, b"", RF_ONOFF_TIMEOUT_S)
                    st = parse_status(self._x(s, Q_STATUS))
                    if csr == 0 and not st["out_on"]:
                        self.set_w = 0.0
                        self._emit("off_confirmed", f"RF OFF 확인({attempt}회째)")
                        return True
                except Cancelled:
                    raise
                except (ReadTimeout, aebus.AeBusError):
                    pass
            self._emit("off_unconfirmed", "RF OFF 확인 못 함 — 장비 전면 확인", "RFP_OFF_UNCONFIRMED")
            raise RfpError("RF OFF 확인 실패")
        self.worker.submit(Request("safety", fn, deadline=self.clock.monotonic() + 3.0, on_done=on_done,
                                   name="rfp:RF OFF", purge=True,
                                   exec_timeout_s=4 * (RF_ONOFF_TIMEOUT_S + self.gap_s) + 2 * (self.q_timeout_s + self.gap_s),
                                   confirms_link=False))
