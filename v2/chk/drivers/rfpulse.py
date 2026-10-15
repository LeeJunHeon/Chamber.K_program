"""챔버K RF Pulse 드라이버 — AE CESAR 1310(AE Bus, 주소 RFPULSE_ADDR). v1 device/RFpulse.py 를 읽고 다시 쓴 것(import 안 함).

읽기(스냅샷 묶기 소스): 162(상태)·165(순방향)·166(반사), 질의 하나 RFPULSE_QUERY_TIMEOUT_MS(250), 사이 RFPULSE_CMD_GAP_MS.
  RFPULSE_READ_PLAN: full=매초 셋, alternate=166 매초 + 165(짝수 tick)/162(홀수 tick), auto=질의가 제한을 넘으면 번갈아로.
  순방향·상태 필드는 자기 품질(own_quality)이라 번갈아면 stale·나이 1.
START(여러 tick 에 걸친 Sequence): 14 Host → 3 조절 모드 6(FWD) → 8 셋포인트 → 27 펄싱 1 → [93 주파수] → [96 듀티] → 2 RF ON
  → 193·196 되읽기. 불일치는 RFPULSE_VERIFY_PULSE_CONFIG: true 면 RF OFF 하고 실패(RFP_CONFIG_MISMATCH), false 면 경고만.
  CSR 자동 복구(v1 과 같은 규칙, START·설정 시퀀스 안에서만): CSR 1(Host 아님) → 14 Host 뒤 그 명령 다시,
  CSR 2(RF ON 중) → 1 RF OFF 뒤 다시 — 한 단계에 한 번, 이벤트를 남긴다. 그 밖의 CSR ≠ 0 → 실패(RFP_START_FAILED).
RF OFF(안전): 1 → CSR 0 → 162 로 출력 꺼짐 확인, 안 되면 한 번 더, 끝내 못 하면 RFP_OFF_UNCONFIRMED.
연결: 128·130·198 로 기종·용량·SW 버전 기록. 쓰기 허용(prod·sim)이면 명령 33 = RFPULSE_REFP_AUTO_OFF_SEC 를 쓰고 152 로 되읽기
  (Host 모드가 아니면 출력이 꺼져 있을 때만 14 Host 로 바꾼 뒤 — 켜져 있으면 건너뛰고 이벤트).
bench: 162·165·166·155·128·130·198 질의만, 상태를 바꾸는 명령(1~127)은 모두 거부.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common import aebus
from v2.common.transport import Cancelled, ReadTimeout
from v2.common.worker import SEQ_DONE, Request, Sequence, Worker

RF_OFF, RF_ON, SET_MODE, SET_SETPOINT, SET_CONTROL, SET_PULSING, SET_AUTO_OFF, SET_FREQ, SET_DUTY = 1, 2, 3, 8, 14, 27, 33, 93, 96
Q_MODEL, Q_CAPACITY, Q_AUTO_OFF, Q_CONTROL, Q_STATUS, Q_FWD, Q_REF, Q_FREQ, Q_DUTY, Q_SW = 128, 130, 152, 155, 162, 165, 166, 193, 196, 198
BENCH_QUERIES = frozenset({Q_STATUS, Q_FWD, Q_REF, Q_CONTROL, Q_MODEL, Q_CAPACITY, Q_SW})
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
        self.freq_hz: Optional[int] = None
        self.duty_pct: Optional[int] = None
        self._last = -1e9
        worker.on_connect = self._on_connect

    def _emit(self, kind: str, detail: str, reason: Optional[str] = None) -> None:
        if self.on_event:
            self.on_event(RfpEvent(kind, detail, reason))

    # ───────── 한 번 주고받기 ─────────
    def _x(self, session, cmd: int, data: bytes = b"") -> bytes:
        if cmd < 128 and not self.writes_allowed:
            raise RfpReadOnly(f"bench — 명령 {cmd} 보내지 않음")
        if cmd >= 128 and not self.writes_allowed and cmd not in BENCH_QUERIES:
            raise RfpReadOnly(f"bench — 허용 질의 아님: {cmd}")
        ready = self._last + self.gap_s
        if ready > self.clock.monotonic():
            self.clock.sleep_until(ready)
        try:
            return aebus.transact(session, self.addr, cmd, data, self.q_timeout_s)
        finally:
            self._last = self.clock.monotonic()

    def _exec(self, session, cmd: int, data: bytes = b"") -> int:
        out = self._x(session, cmd, data)
        return out[0] if out else -1

    # ───────── 연결 ─────────
    def _on_connect(self, session) -> None:
        self._last = -1e9
        self.model = self._x(session, Q_MODEL).decode("ascii", errors="replace")
        cap = aebus.u16(self._x(session, Q_CAPACITY))
        sw = self._x(session, Q_SW).decode("ascii", errors="replace")
        self._emit("identity", f"{self.model} · {cap} W · SW {sw}")
        if not self.writes_allowed:
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
                if self.cfg["RFPULSE_READ_PLAN"] == "auto" and not self.alternate:
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

    # ───────── START ─────────
    def _step(self, name: str, cmd: int, data: bytes = b""):
        """설정 명령 한 단계 + CSR 자동 복구(CSR 1 → Host, CSR 2 → RF OFF, 한 번)."""
        def fn(s):
            csr = self._exec(s, cmd, data)
            if csr in (1, 2) and cmd not in (SET_CONTROL, RF_OFF):
                fix = SET_CONTROL if csr == 1 else RF_OFF
                fcsr = self._exec(s, fix, b"\x02" if fix == SET_CONTROL else b"")
                self._emit("csr_recovery", f"{name}: CSR {csr}({CSR_TEXT[csr]}) → 명령 {fix} 뒤 다시 (CSR {fcsr})")
                csr = self._exec(s, cmd, data)
            if csr != 0:
                raise RfpCsr(cmd, csr)
        return fn

    def start(self, target_w: float, freq_hz: Optional[int] = None, duty: Optional[int] = None,
              on_done: Optional[Callable[[str, Optional[str], list], None]] = None, tick=None) -> None:
        if target_w > self.cfg["RFPULSE_MAX_POWER"]:
            raise ValueError(f"목표 {target_w} W > RFPULSE_MAX_POWER")
        sp = int(round(target_w))
        steps = [("14 Host", self._step("Host", SET_CONTROL, b"\x02")),
                 ("3 FWD", self._step("조절 모드", SET_MODE, b"\x06")),
                 ("8 셋포인트", self._step("셋포인트", SET_SETPOINT, aebus.le(sp, 2))),
                 ("27 펄싱", self._step("펄싱", SET_PULSING, b"\x01"))]
        if freq_hz is not None:
            steps.append(("93 주파수", self._step("주파수", SET_FREQ, aebus.le(int(freq_hz), 3))))
        if duty is not None:
            steps.append(("96 듀티", self._step("듀티", SET_DUTY, aebus.le(int(duty), 2))))

        def rf_on(s):
            self._step("RF ON", RF_ON)(s)
            self.set_w = float(target_w)

        def readback(s):
            f = aebus.u24(self._x(s, Q_FREQ))
            d = aebus.u16(self._x(s, Q_DUTY))
            self.freq_hz, self.duty_pct = f, d
            bad = []
            if freq_hz is not None and abs(f - int(freq_hz)) > 1:
                bad.append(f"주파수 요청 {freq_hz} → {f}")
            if duty is not None and d != int(duty):
                bad.append(f"듀티 요청 {duty} → {d}")
            if bad:
                if self.cfg["RFPULSE_VERIFY_PULSE_CONFIG"]:
                    self._exec(s, RF_OFF)
                    self.set_w = 0.0
                    self._emit("config_readback", "펄스 설정 되읽기 불일치 — RF OFF: " + ", ".join(bad), "RFP_CONFIG_MISMATCH")
                    raise RfpConfigMismatch(", ".join(bad))
                self._emit("config_readback", "펄스 설정 되읽기 불일치(경고만): " + ", ".join(bad), "RFP_CONFIG_MISMATCH")
            else:
                self._emit("config_readback", f"되읽기 {f} Hz · {d} %")
            return SEQ_DONE

        steps += [("2 RF ON", rf_on), ("193·196 되읽기", readback)]

        def done(name, reason, results):
            r = reason
            if reason and reason != "BENCH_READ_ONLY":
                errs = [x.error for x in results if x.status != "ok"]
                r = "RFP_CONFIG_MISMATCH" if any("RfpConfigMismatch" in e for e in errs) else "RFP_START_FAILED"
            if on_done:
                on_done(name, r, results)

        self.worker.submit_sequence(Sequence("rfp:start", steps, gap_s=max(self.gap_s, 0.05),
                                             step_timeout_s=4 * (self.q_timeout_s + self.gap_s) + 0.2, on_done=done,
                                             confirms_link=False,
                                             start_after=(tick.t_mono + 0.1) if tick is not None else None))

    # ───────── RF OFF(안전) ─────────
    def rf_off_confirmed(self, on_done: Optional[Callable[[Any], None]] = None) -> None:
        def fn(s):
            for attempt in (1, 2):
                try:
                    csr = self._exec(s, RF_OFF)
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
                                   name="rfp:RF OFF", purge=True, exec_timeout_s=8 * (self.q_timeout_s + self.gap_s),
                                   confirms_link=False))
