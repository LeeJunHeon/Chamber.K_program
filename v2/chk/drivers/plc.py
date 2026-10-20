"""챔버K PLC 드라이버 — LS XGB 내장 Modbus RTU. RF power(DAC·ADC)와 히터(래더 PID) 포함.

읽기(한 번 = 블록 3개, devices.TICK_PLAN): FC1 0..95, FC1 160..320, FC3 D0..D60.
  - 1초 tick 읽기는 스냅샷 묶기(SnapshotBus)의 소스 read 로, 그 사이 250 ms 내부 폴링은 PlcFastPoller 가 한다.
  - 값 이름·주소는 snapshot_fields 의 plc·heater·rf 블록 source("plc:<devices.PLC_ADDRS 이름>")를 그대로 따른다.
  - 온도 레지스터는 부호 있는 0.1 °C(-1 = 센서 이상 → None). RF: 순방향 ADC 4000 = RF_FWD_FULL_SCALE_W(594.5 W),
    반사 ADC 4000 = RF_REF_FULL_SCALE_W(200 W) — 사용자 확인값.
쓰기는 모두 worker 의 write·safety 요청으로만 — bench 에서는 worker 가 BENCH_READ_ONLY 로 거부하고,
  그와 별도로 이 드라이버의 Modbus 클라이언트도 읽기 전용이 된다(연결 초기화의 쓰기까지 막는 이중 보호).
연결할 때(prod·sim): 보류된 히터 OFF 적용 → 히터 한계값 쓰기 + 되읽기 확인(S13) → 재기동 감지 마커 쓰기.
워치독 하트비트: tick 마다 D00014 를 1 증가해 쓴다. 링크가 끊겼거나 bench 면 쓰지 않는다(PLC 래더가 10 s 뒤 히터를 끄게).
재기동 감지: 폴링마다 마커 D(PLC_SESSION_MARK_REG)를 비교, 2회 연속 다르면 이벤트 + 한계값·마커 다시 쓰기 + 블랙박스.
블랙박스: 폴링(250 ms) 원시 값 BLACKBOX_SECONDS 링버퍼. 링크가 PLC_COMM_LOSS_ABORT_SEC 끊기거나 재기동이면 1회 파일로.
안전 판정은 하지 않는다 — 값·이벤트만 낸다(SafetyEngine 이 판정).
"""
from __future__ import annotations

import collections
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from v2.common.modbus_rtu import ModbusClient, to_signed
from v2.common.worker import Request, Result, Worker
from v2.chk import devices as D
from v2.chk import snapshot_fields as SF

COILS_A = (0, 96)        # FC1 0..95
COILS_B = (160, 161)     # FC1 160..320
REGS = (0, 61)           # FC3 D0..D60

_ADDR = {a.name: a for a in D.PLC_ADDRS}
_TEMP_REGS = {"heater_pv", "heater_pv2", "heater_sv", "heater_cur_sv", "heater_sv_ramp", "heater_sv2",
              "heater_sv_limit", "heater_ot2_limit", "heater_sv2_max"}
CORE_FIELDS = {"heater.hold"}            # 드라이버가 아니라 core(유지 모드 엔진)가 채운다

# 히터 한계값(S13) — (ParamDef 키, D 주소 이름, 변환)
_RATE = lambda c_per_min: max(1, int(round(float(c_per_min) / 6.0)))     # 1 카운트/초 = 6 °C/min
_X10 = lambda c: int(round(float(c) * 10))
HEATER_LIMITS = (
    ("HEATER_SV_LIMIT_C", "heater_sv_limit", _X10),
    ("HEATER_MV_LIMIT", "heater_mv_limit", int),
    ("HEATER_RAMP_RATE_C_PER_MIN", "heater_ramp_rate", _RATE),
    ("HEATER_HOLDBACK_C", "heater_holdback", _X10),
    ("HEATER_OT_LIMIT_C", "heater_ot_limit", _X10),
    ("HEATER_OT2_LIMIT_C", "heater_ot2_limit", _X10),
    ("HEATER_SLOW_ZONE_C", "heater_slow_zone", _X10),
    ("HEATER_SLOW_RATE_C_PER_MIN", "heater_slow_rate", _RATE),
)


@dataclass(frozen=True)
class PlcEvent:
    kind: str          # link_up / link_down / heater_config_ok / heater_config_mismatch / plc_restarted /
                       # heater_residual_run / heater_off_pending / heater_off_applied / blackbox / marker_written
    detail: str
    reason: Optional[str] = None          # 사유 코드(commands)
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PlcPoll:
    t_wall: float
    coils: Dict[int, bool]
    regs: List[int]
    values: Dict[str, Any]


def heater_est_current(mv: Optional[int], cfg: Dict[str, Any]) -> float:
    """위상제어 실효값 추정(표시용) — v1 lib/config.heater_est_current."""
    import math
    if mv is None or mv < cfg["HEATER_MV_MIN"]:
        return 0.0
    span = cfg["HEATER_CURRENT_MV_FULL"] - cfg["HEATER_CURRENT_MV_ZERO"]
    x = (cfg["HEATER_CURRENT_MV_FULL"] - mv) / span
    a = min(max(x, 0.0), 1.0) * math.pi
    return float(cfg["HEATER_CURRENT_SCALE"]) * math.sqrt(max(0.0, (math.pi - a + math.sin(2 * a) / 2) / math.pi))


def rf_power_to_dac(w: float, cfg: Dict[str, Any]) -> int:
    """v1 RFpower._power_to_dac: (W/RF_MAX_POWER)×4000×gain + offset, 0~4000."""
    base = (float(w) / cfg["RF_MAX_POWER"]) * D.RF_DAC_FULL_SCALE
    return max(0, min(D.RF_DAC_FULL_SCALE, int(round(cfg["RF_CAL_OFFSET"] + base * cfg["RF_CAL_GAIN"]))))


class PlcDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[PlcEvent], None]] = None,
                 blackbox_sink: Optional[Callable[[str, List[Dict[str, Any]]], None]] = None,
                 rng: Optional[random.Random] = None) -> None:
        self.clock = clock
        self.worker = worker
        self.cfg = dict(cfg)
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.blackbox_sink = blackbox_sink
        self.rng = rng or random.Random()
        self.slave = cfg["PLC_SLAVE_ID"]
        self.last: Optional[PlcPoll] = None
        self.fast_listeners: List[Callable[[PlcPoll], None]] = []
        self.frame_listeners: List[Callable[[str, bytes], None]] = []
        n = max(1, int(cfg["BLACKBOX_SECONDS"] * 1000 / cfg["PLC_FAST_POLL_MS"]))
        self.blackbox: "collections.deque[PlcPoll]" = collections.deque(maxlen=n)
        self._marker: Optional[int] = None
        self._marker_mismatch = 0
        self._first_poll = True
        self._pending_heater_off = False
        self._wd = 0
        self._rf_set_w: Optional[float] = None
        self._rf_enabled = False
        self._outage_since: Optional[float] = None
        self._outage_dumped = False
        worker.on_connect = self._on_connect
        self._prev_link = False

    # ───────── 공통 ─────────
    def _emit(self, kind: str, detail: str = "", reason: Optional[str] = None, **data) -> None:
        if self.on_event:
            self.on_event(PlcEvent(kind, detail, reason, data))

    def _client(self, session) -> ModbusClient:
        return ModbusClient(session, self.slave, self.cfg["PLC_TXN_TIMEOUT_MS"] / 1000.0,
                            retries=self.cfg["PLC_RETRY_COUNT"],
                            retry_delay_s=self.cfg["PLC_RETRY_DELAY_MS"] / 1000.0,
                            read_only=not self.writes_allowed,
                            on_frame=(self._frame if self.frame_listeners else None))

    def _frame(self, direction: str, data: bytes) -> None:
        for f in self.frame_listeners:
            f(direction, data)

    @property
    def link_up(self) -> bool:
        lo = self.worker.last_ok
        return bool(self.worker.connected and lo is not None and self.clock.monotonic() - lo <= 2.0)

    # ───────── 읽기 ─────────
    def _poll(self, session) -> PlcPoll:
        t0 = self.clock.wall()                    # 표본 시각 = 읽기를 시작한 시각
        c = self._client(session)
        a = c.read_coils(*COILS_A)
        b = c.read_coils(*COILS_B)
        regs = c.read_regs(*REGS)
        coils = {i + COILS_A[0]: v for i, v in enumerate(a)}
        coils.update({i + COILS_B[0]: v for i, v in enumerate(b)})
        poll = PlcPoll(t0, coils, regs, self._decode(coils, regs))
        self._after_poll(poll)
        return poll

    def _raw(self, name: str, coils: Dict[int, bool], regs: List[int]):
        a = _ADDR[name]
        return coils[a.addr] if a.kind == "coil" else regs[a.addr]

    def _decode(self, coils: Dict[int, bool], regs: List[int]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        cfg = self.cfg
        for b in (SF.PLC_BLOCK, SF.HEATER_BLOCK, SF.RF_BLOCK):
            for f in b.fields:
                col = f"{b.name}.{f.name}"
                if not f.source.startswith("plc:"):
                    continue
                name = f.source[4:].split()[0]
                raw = self._raw(name, coils, regs)
                if name in _TEMP_REGS:
                    s = to_signed(raw)
                    out[col] = None if s == -1 else round(s * D.HEATER_TEMP_SCALE_C_PER_COUNT, 1)
                elif name == "rf_fwd_adc":
                    out[col] = round(raw * cfg["RF_FWD_FULL_SCALE_W"] / D.RF_ADC_FULL_SCALE, 2)
                elif name == "rf_ref_adc":
                    out[col] = round(raw * cfg["RF_REF_FULL_SCALE_W"] / D.RF_ADC_FULL_SCALE, 2)
                else:
                    out[col] = bool(raw) if _ADDR[name].kind == "coil" else int(raw)
        out["heater.current_est"] = round(heater_est_current(out["heater.mv"], cfg), 1)
        out["plc.marker_ok"] = None if self._marker is None else (regs[cfg["PLC_SESSION_MARK_REG"]] == self._marker)
        out["rf.set_w"] = self._rf_set_w
        return out

    def _after_poll(self, poll: PlcPoll) -> None:
        self.last = poll
        self.blackbox.append(poll)
        mark = self.cfg["PLC_SESSION_MARK_REG"]
        if self._marker is not None and mark:
            if poll.regs[mark] == self._marker:
                self._marker_mismatch = 0
            else:
                self._marker_mismatch += 1
                if self._marker_mismatch >= 2:
                    got, exp = poll.regs[mark], self._marker
                    self._marker, self._marker_mismatch = None, 0
                    self._emit("plc_restarted", f"마커 D{mark:05d} 기대 0x{exp:04X}, 읽음 0x{got:04X} (2회 연속)",
                               "PLC_RESTARTED")
                    self.dump_blackbox("plc_restarted")
                    self.submit_write("재기동 뒤 히터 한계값·마커", self._init_writes, kind="write")
        if self._first_poll:
            self._first_poll = False
            self._rf_enabled = bool(poll.values.get("rf.dac_enable"))     # 내부 상태를 실제 M00200 으로(v1 종료 흔적 포함)
            if poll.values["heater.run"]:
                self._emit("heater_residual_run", "첫 폴링에서 히터 RUN 이 켜져 있음(이전 세션)", "HEATER_RESIDUAL_RUN",
                           pv=poll.values["heater.pv"])
        for f in self.fast_listeners:
            f(poll)

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        """스냅샷 묶기의 소스 read — 정각 회차 폴링."""
        return dict(self._poll(session).values)

    def fast_poll_request(self) -> Request:
        return Request("read", self._poll, deadline=self.clock.monotonic() + self.cfg["PLC_READ_DEADLINE_MS"] / 1000.0,
                       name="plc:fast")

    # ───────── 연결 초기화(worker 스레드) ─────────
    def _on_connect(self, session) -> None:
        c = self._client(session)
        c.read_regs(self.cfg["PLC_SESSION_MARK_REG"] or 10, 1)       # 프로브 — 응답이 있어야 링크 업
        self._first_poll = True
        self._marker, self._marker_mismatch = None, 0
        if self.writes_allowed:
            self._init_writes(session)

    def _init_writes(self, session) -> None:
        c = self._client(session)
        if self._pending_heater_off:
            c.write_coil(_ADDR["heater_run"].addr, False)
            self._pending_heater_off = False
            self._emit("heater_off_applied", "링크 복구 — 보류된 히터 OFF 적용")
        self.push_heater_limits(c)
        mark = self.cfg["PLC_SESSION_MARK_REG"]
        if mark:
            m = self.rng.randint(0x1000, 0xFFFE)
            c.write_reg(mark, m)
            self._marker, self._marker_mismatch = m, 0
            self._emit("marker_written", f"D{mark:05d}=0x{m:04X}")

    def heater_limit_values(self) -> List[tuple]:
        return [(_ADDR[reg].addr, max(0, min(32767, conv(self.cfg[key]))), key)
                for key, reg, conv in HEATER_LIMITS]

    def push_heater_limits(self, c: ModbusClient) -> List[str]:
        """한계값 쓰기 + 되읽기(S13). 불일치 목록(빈 목록 = 일치)."""
        if not self.cfg["HEATER_ENABLED"]:
            return []
        items = self.heater_limit_values()
        for addr, raw, _k in items:
            c.write_reg(addr, raw)
        lo, hi = min(a for a, _, _ in items), max(a for a, _, _ in items)
        back = c.read_regs(lo, hi - lo + 1)
        mism = [f"{k}: 요청 {raw} → 실제 {back[a - lo]}" for a, raw, k in items if back[a - lo] != raw]
        if mism:
            self._emit("heater_config_mismatch", "히터 한계값 되읽기 불일치: " + ", ".join(mism), "VERIFY_MISMATCH",
                       mismatch=mism)
        else:
            self._emit("heater_config_ok", "히터 한계값 쓰기·되읽기 일치")
        return mism

    # ───────── tick ─────────
    def on_tick(self, tick) -> None:
        """TickLoop 의 tick 훅 — 하트비트, 링크 감시, 두절 블랙박스."""
        up = self.link_up
        now = self.clock.monotonic()
        if up != self._prev_link:
            self._emit("link_up" if up else "link_down", "")
            self._prev_link = up
        if up:
            self._outage_since, self._outage_dumped = None, False
            if self.writes_allowed:
                self.submit_write("하트비트", self._heartbeat)
        else:
            if self._outage_since is None:
                self._outage_since = now
            if (not self._outage_dumped and self.blackbox
                    and now - self._outage_since >= self.cfg["PLC_COMM_LOSS_ABORT_SEC"]):
                self._outage_dumped = True
                self.dump_blackbox("comm_loss")

    def _heartbeat(self, session) -> None:
        self._wd = (self._wd + 1) & 0x7FFF
        self._client(session).write_reg(_ADDR["heater_wd"].addr, self._wd)

    # ───────── 블랙박스 ─────────
    def dump_blackbox(self, reason: str) -> List[Dict[str, Any]]:
        polls = list(self.blackbox)
        if not polls:
            return []
        t0 = polls[0].t_wall
        rows = []
        for p in polls:
            r = {"t_wall": round(p.t_wall, 3), "elapsed_ms": int(round((p.t_wall - t0) * 1000))}
            r.update(p.values)
            rows.append(r)
        self._emit("blackbox", f"{reason}: {len(rows)}행", data_rows=len(rows))
        if self.blackbox_sink:
            self.blackbox_sink(f"plc_{reason}", rows)
        return rows

    # ───────── 쓰기(타입 있음) ─────────
    def submit_write(self, name: str, fn: Callable, kind: str = "write", purge: bool = False,
                     on_done: Optional[Callable[[Result], None]] = None) -> None:
        self.worker.submit(Request(kind, fn, deadline=self.clock.monotonic() + 2.0, on_done=on_done,
                                   name=f"plc:{name}", purge=purge))

    def set_button(self, name: str, on: bool, on_done=None) -> None:
        a = _ADDR[name]
        if a.kind != "coil" or "W" not in a.access:
            raise ValueError(f"{name}: 쓸 수 없는 주소")
        self.submit_write(f"{name}={int(on)}", lambda s: self._client(s).write_coil(a.addr, on), on_done=on_done)

    def heater_set_sv(self, temp_c: float, on_done=None) -> None:
        raw = max(0, min(32767, _X10(temp_c)))
        self.submit_write(f"SV={temp_c}", lambda s: self._client(s).write_reg(_ADDR["heater_sv"].addr, raw), on_done=on_done)

    def heater_run(self, on: bool, on_done=None) -> None:
        if not on and not self.link_up:
            self._pending_heater_off = True
            self._emit("heater_off_pending", "링크 없음 — 히터 OFF 보류(복구 때 적용)")
        kind = "write" if on else "safety"
        self.submit_write(f"RUN={int(on)}", lambda s: self._client(s).write_coil(_ADDR["heater_run"].addr, on),
                          kind=kind, on_done=on_done)

    def heater_reset(self, on_done=None) -> None:
        self.submit_write("RST", lambda s: self._client(s).write_coil(_ADDR["heater_rst"].addr, True), on_done=on_done)

    def heater_set_reg(self, name: str, raw: int, on_done=None) -> None:
        if name not in ("heater_mv_limit", "heater_ramp_rate", "heater_sv2"):
            raise ValueError(name)
        self.submit_write(f"{name}={raw}", lambda s: self._client(s).write_reg(_ADDR[name].addr, int(raw)), on_done=on_done)

    def heater_set_pv_sel(self, on: bool, on_done=None) -> None:
        self.submit_write(f"PV_SEL={int(on)}",
                          lambda s: self._client(s).write_coil(_ADDR["heater_pv_sel"].addr, on), on_done=on_done)

    def rf_set_power(self, watts: float, on_done=None) -> None:
        dac = rf_power_to_dac(watts, self.cfg)

        def fn(s):
            c = self._client(s)
            if dac > 0 and not self._rf_enabled:
                c.write_coil(_ADDR["rf_dac_enable"].addr, True)
                self._rf_enabled = True
            c.write_reg(_ADDR["rf_dac"].addr, dac)
            self._rf_set_w = float(watts)
        self.submit_write(f"RF={watts}W(DAC {dac})", fn, on_done=on_done)

    def rf_off(self, on_done=None) -> None:
        def fn(s):
            c = self._client(s)
            c.write_reg(_ADDR["rf_dac"].addr, 0)
            c.write_coil(_ADDR["rf_dac_enable"].addr, False)
            self._rf_enabled, self._rf_set_w = False, 0.0
        self.submit_write("RF OFF", fn, kind="safety", on_done=on_done)

    def rf_write_dac(self, counts: int, on_done=None) -> None:
        """RF 제어 루프의 DAC 쓰기(write 우선순위) — 0 보다 크면 enable 을 먼저 켠다(v1 send_rfpower_command)."""
        counts = max(0, min(D.RF_DAC_FULL_SCALE, int(counts)))

        def fn(s):
            c = self._client(s)
            if counts > 0 and not self._rf_enabled:
                c.write_coil(_ADDR["rf_dac_enable"].addr, True)
                self._rf_enabled = True
            c.write_reg(_ADDR["rf_dac"].addr, counts)
        self.submit_write(f"RF DAC={counts}", fn, on_done=on_done)

    def rf_set_dac(self, counts: int, on_done=None) -> None:
        """RF DAC 원시값(램프다운 단계용, safety 우선순위). enable 은 건드리지 않는다."""
        counts = max(0, int(counts))

        def fn(s):
            self._client(s).write_reg(_ADDR["rf_dac"].addr, counts)
        self.submit_write(f"RF DAC={counts}", fn, kind="safety", on_done=on_done)

    def coils_off(self, names, on_done=None) -> None:
        """여러 버튼 코일을 OFF — safety 우선순위 한 요청(셔터·차단밸브 닫기 등 정지 순서용)."""
        addrs = [_ADDR[n].addr for n in names]
        for n in names:
            if _ADDR[n].kind != "coil" or "W" not in _ADDR[n].access:
                raise ValueError(f"{n}: 쓸 수 없는 주소")

        def fn(s):
            c = self._client(s)
            for a in addrs:
                c.write_coil(a, False)
        self.submit_write("OFF " + ",".join(names), fn, kind="safety", on_done=on_done)

    def outputs_off(self, on_done=None) -> None:
        """ALL STOP 의 PLC_OUTPUTS_OFF — v1 비상 정지(device/PLC.py:1390-1447)와 같게: 버튼 코일 15개(v1 PLC_COIL_MAP,
        펌프·밸브 포함) 전부 OFF → 히터 RUN=0(HEATER_ENABLED) → D00040=0(v2 추가) → M00200(DAC enable)=0.
        매핑된 코일의 연속 구간만 FC15, 하나면 FC5(매핑 안 된 래더 비트는 건드리지 않는다). v1 처럼 한 쓰기가 실패해도
        나머지를 계속 쓰고, 하나라도 실패하면 결과는 실패 — 히터 RUN=0 을 못 썼으면 보류(다음 통신 성공 때 다시)."""
        from v2.chk.commands import PLC_BUTTONS
        addrs = sorted(_ADDR[b.addr].addr for b in PLC_BUTTONS)
        runs, start, prev = [], addrs[0], addrs[0]
        for a in addrs[1:]:
            if a == prev + 1:
                prev = a
            else:
                runs.append((start, prev))
                start = prev = a
        runs.append((start, prev))
        run_off = {"ok": False}

        def fn(s):
            c = self._client(s)
            errs = []

            def w(what, f, *a):
                try:
                    f(*a)
                    return True
                except Exception as e:          # v1: 구간마다 따로 감싸고 계속
                    errs.append(f"{what}: {e!r}")
                    return False
            for lo, hi in runs:
                if lo == hi:
                    w(f"코일 {lo}", c.write_coil, lo, False)
                else:
                    w(f"코일 {lo}..{hi}", c.write_coils, lo, [False] * (hi - lo + 1))
            if self.cfg["HEATER_ENABLED"]:
                if w("히터 RUN=0", c.write_coil, _ADDR["heater_run"].addr, False):
                    run_off["ok"], self._pending_heater_off = True, False
            w("D00040=0", c.write_reg, _ADDR["rf_dac"].addr, 0)
            if w("M00200=0", c.write_coil, _ADDR["rf_dac_enable"].addr, False):
                self._rf_enabled = False
            self._rf_set_w = 0.0
            if errs:
                raise RuntimeError("ALL STOP 출력 OFF 일부 실패: " + "; ".join(errs))

        def done(res: Result) -> None:
            if not run_off["ok"] and self.cfg["HEATER_ENABLED"] and res.status != "rejected":
                self._pending_heater_off = True          # RUN=0 을 못 썼으면(링크 없음 포함) 보류 — 다음 통신 성공 때 다시
            if on_done:
                on_done(res)
        self.submit_write("ALL STOP 출력 OFF", fn, kind="safety", purge=True, on_done=done)


class PlcFastPoller:
    """250 ms 내부 폴링 — 정각 회차는 스냅샷 묶기가 읽으므로 건너뛴다. 앞 요청이 진행 중이면 보내지 않는다.
    시각은 period 의 정수배(벽시계)에 맞춘 절대 시각, 밀리면 놓친 회차는 건너뛴다."""

    def __init__(self, clock, driver: PlcDriver, period_ms: int) -> None:
        self.clock, self.driver = clock, driver
        self.period = period_ms / 1000.0
        self._stop = False
        self._busy = False
        self._thread = None
        self.sent = 0
        self.skipped_busy = 0

    def start(self) -> None:
        self._thread = self.clock.start_thread(self._run, "plc-fast")

    def stop(self) -> None:
        with self.clock.cond:
            self._stop = True
            self.clock.cond.notify_all()

    def _done(self, _r) -> None:
        with self.clock.cond:
            self._busy = False

    def _run(self) -> None:
        import math
        p = self.period
        n = math.floor(self.clock.wall() / p + 1e-9) + 1
        while True:
            target = n * p - (self.clock.wall() - self.clock.monotonic())
            if self.clock.wait_until(target, lambda: self._stop):
                return
            n_now = math.floor(self.clock.wall() / p + 1e-9)
            slot, n = max(n, n_now), max(n, n_now) + 1
            if abs(slot * p - round(slot * p)) < 1e-9:          # 정각 — 묶기가 읽음
                continue
            if self._busy or not self.driver.worker.connected:
                self.skipped_busy += 1
                continue
            with self.clock.cond:
                self._busy = True
            req = self.driver.fast_poll_request()
            req.on_done = self._done
            self.driver.worker.submit(req)
            self.sent += 1
