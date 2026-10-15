"""챔버K 시뮬레이터 시험대 — 실제 드라이버 5개(PLC·MFC·DC·RF Pulse·IG) + 시뮬레이터 + 공용 챔버 + 묶기 + 기록.

sim 전용(SimTransport). RF power 는 PLC 드라이버 안(DAC·ADC). tick 훅: PLC 하트비트·링크, IG 필라멘트 기한.
안전 규칙(ChkSafety): plc250 = PLC 폴링 직후(fast_listeners), tick = 행이 닫힌 뒤(sink), 드라이버 이벤트 → 이벤트 규칙.
공정 쪽은 SimpleProcess(rig.process) — 시험이 상태·사용 장비를 정한다.
scenario "light": 가스 20 sccm·SP1 5 mTorr(MFC) → 필라멘트 켜기(IG) → DC 0.3 A → RF Pulse 100 W → 끄기 —
  쓰기가 1초 읽기와 섞여도 tick 이 밀리지 않는지 보려는 것(공정 엔진 아님).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from v2.common.paramdef import defaults
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop, iso_local
from v2.common.transport import SimTransport
from v2.common.worker import Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.dc import DcDriver
from v2.chk.drivers.ig import IgDriver
from v2.chk.drivers.mfc import CH_AR, MfcDriver
from v2.chk.drivers.plc import PlcDriver, PlcFastPoller
from v2.chk.drivers.rfpulse import RfPulseDriver
from v2.chk.heater_engine import HeaterEngine
from v2.chk.safety import ChkSafety, SimpleProcess, View
from v2.chk.sim.cesar_sim import CesarSim
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.dc_sim import DcSim
from v2.chk.sim.ig_sim import IgSim
from v2.chk.sim.mfc_sim import MfcSim
from v2.chk.sim.plc_sim import PlcSim


class ChkRig:
    def __init__(self, clock, out_dir: Path, cfg: Optional[Dict[str, Any]] = None, scenario: str = "none") -> None:
        self.clock = clock
        self.cfg = dict(defaults(P.PARAMS), **(cfg or {}))
        c = self.cfg
        self.chamber = Chamber()
        self.sims = {"plc": PlcSim(chamber=self.chamber), "mfc": MfcSim(self.chamber), "dc": DcSim(self.chamber),
                     "rfpulse": CesarSim(self.chamber), "ig": IgSim(self.chamber)}
        streak = {"plc": "PLC_FAIL_STREAK", "mfc": "MFC_FAIL_STREAK", "dc": "DC_FAIL_STREAK",
                  "rfpulse": "RFPULSE_FAIL_STREAK", "ig": "IG_FAIL_STREAK"}
        self.events: List[Any] = []
        self.workers = {k: Worker(k, SimTransport(clock, s), fail_streak=c[streak[k]],
                                  connect_init_timeout_s=c["CONNECT_INIT_TIMEOUT_SEC"], on_event=self._ev)
                        for k, s in self.sims.items()}
        self.recorder: Optional[Recorder] = None
        w = self.workers
        self.plc = PlcDriver(clock, w["plc"], c, True, on_event=self._ev,
                             blackbox_sink=lambda name, rows: self.recorder.put_blackbox(name, rows))
        self.mfc = MfcDriver(clock, w["mfc"], c, True, on_event=self._ev)
        self.dc = DcDriver(clock, w["dc"], c, True, on_event=self._ev)
        self.rfp = RfPulseDriver(clock, w["rfpulse"], c, True, on_event=self._ev)
        self.ig = IgDriver(clock, w["ig"], c, True, on_event=self._ev)
        self.bus = SnapshotBus(clock, SF.BLOCKS, [
            Source("plc", w["plc"], self.plc.source_read, ("plc", "heater", "rf"), c["PLC_READ_DEADLINE_MS"]),
            Source("mfc", w["mfc"], self.mfc.source_read, ("mfc",), c["MFC_READ_DEADLINE_MS"]),
            Source("dc", w["dc"], self.dc.source_read, ("dc",), c["DC_READ_DEADLINE_MS"]),
            Source("rfpulse", w["rfpulse"], self.rfp.source_read, ("rfp",), c["RFPULSE_READ_DEADLINE_MS"]),
            Source("ig", w["ig"], self.ig.source_read, ("ig",), c["IG_RESPONSE_LIMIT_MS"]),
        ], c["TICK_DEADLINE_MS"], SF.PROCESS_FIELDS)
        self.recorder = Recorder(out_dir, self.bus.columns, retention_days=c["SNAPSHOT_RETENTION_DAYS"])
        self.fast = PlcFastPoller(clock, self.plc, c["PLC_FAST_POLL_MS"])
        self.process = SimpleProcess()
        self.heater = HeaterEngine(clock, c, self.plc, self.mfc, on_event=self._hev,
                                   process_active=lambda: self.process.active,
                                   on_gas_failed=lambda why: self.safety.event("heater_gas_failed"))
        self.safety = ChkSafety(clock, c, self.process, self.plc, self.mfc, self.dc, self.rfp, self.ig, on_event=self._sev,
                                heater=self.heater)
        self.plc.fast_listeners.append(self.safety.on_poll)
        self.plc.fast_listeners.append(self.heater.on_poll)
        self.loop = TickLoop(clock, self.bus, [self.recorder.put_row, self.safety.on_row, self._on_row],
                             tick_hooks=[self._tick], extra_fn=self._extra)
        self.scenario = scenario

    def _on_row(self, row) -> None:
        self.heater.on_tick(View.from_row(row))

    def _extra(self, t) -> dict:
        return {"heater.hold": self.heater.hold_label}

    def _hev(self, e) -> None:
        self.events.append(e)
        if self.recorder is not None and e.kind != "message":
            self.recorder.put_event(iso_local(self.clock.wall()), e.level, (e.code or e.kind).upper(), "heater", e.detail)

    def _sev(self, e) -> None:
        self.events.append(e)
        if self.recorder is not None:
            self.recorder.put_event(iso_local(self.clock.wall()), e.level, (e.code or e.kind).upper(),
                                    f"safety:{e.rule}", e.detail)

    def _ev(self, e) -> None:
        self.events.append(e)
        if getattr(self, "safety", None) is not None:
            self.safety.on_driver_event(e)
        if self.recorder is None:
            return
        kind = getattr(e, "kind", "?")
        reason = getattr(e, "reason", None)
        self.recorder.put_event(iso_local(self.clock.wall()), "warn" if reason else "info", (reason or kind).upper(),
                                getattr(e, "device", type(e).__name__), getattr(e, "detail", ""))

    def _tick(self, t) -> None:
        self.plc.on_tick(t)
        self.ig.on_tick(t)
        if self.scenario != "light":
            return
        s = t.seq
        if s == 1:
            self.mfc.set_flow(CH_AR, 20.0)
            self.mfc.set_onoff("1000")
        elif s == 3:
            self.mfc.set_sp(1, 5.0)
            self.mfc.select_sp(1)
            self.ig.filament_on()
        elif s == 20:
            self.dc.start_output(600.0, 0.3, tick=t)
        elif s == 30:
            self.rfp.start(100.0, freq_hz=10000, duty=50, tick=t)
        elif s == 45:
            self.rfp.rf_off_confirmed()
            self.dc.output_off_confirmed()
        elif s == 50:
            self.ig.filament_off()
            self.mfc.all_flow_off()

    def start(self, ready_timeout_s: float = 15.0) -> bool:
        """worker 를 시작하고, 모두 연결·초기화가 끝날 때까지(최대 ready_timeout_s) 기다린 뒤 돌아온다 —
        첫 tick 의 읽기가 연결 초기화 뒤에 줄 서서 실패로 남지 않게. 모두 준비되면 True."""
        self.recorder.start()
        with self.clock.hold():
            for x in self.workers.values():
                x.start()
            self.fast.start()
        return self.clock.wait_until(self.clock.monotonic() + ready_timeout_s,
                                     lambda: all(x.ready for x in self.workers.values()))

    def stop(self) -> None:
        self.fast.stop()
        for x in self.workers.values():
            x.stop()
        self.recorder.stop()
