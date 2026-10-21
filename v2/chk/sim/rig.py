"""챔버K 시뮬레이터 시험대 — 실제 드라이버 5개(PLC·MFC·DC·RF Pulse·IG) + 시뮬레이터 + 공용 챔버 + 묶기 + 기록.

배선은 실행 프로그램과 같은 조립(v2.chk.system.ChkSystem — 5-6 ②). 여기는 시뮬레이터와 SimTransport 만 만든다.
sim 전용(SimTransport). RF power 는 PLC 드라이버 안(DAC·ADC). tick 훅: PLC 하트비트·링크, IG 필라멘트 기한.
안전 규칙(ChkSafety): plc250 = PLC 폴링 직후(fast_listeners), tick = 행이 닫힌 뒤(sink), 드라이버 이벤트 → 이벤트 규칙.
공정 쪽은 ProcessEngine(rig.process), 히터는 HeaterEngine(rig.heater), 명령은 ChkCore(rig.core.command).
simple_process=True 면 SimpleProcess(시험이 상태·사용 장비를 직접 정하는 안전 규칙 시험용).
1초 행이 닫히면: 기록 → 안전 규칙(tick) → 히터(on_tick) → 공정(on_row) 순서.
scenario "light": 가스 20 sccm·SP1 5 mTorr(MFC) → 필라멘트 켜기(IG) → DC 0.3 A → RF Pulse 100 W → 끄기 —
  쓰기가 1초 읽기와 섞여도 tick 이 밀리지 않는지 보려는 것(공정 엔진 아님).
"""
from __future__ import annotations

import collections
from pathlib import Path
from typing import Any, Dict, Optional

from v2.common.paramdef import defaults
from v2.common.transport import SimTransport
from v2.chk import params as P
from v2.chk.drivers.mfc import CH_AR
from v2.chk.sim.cesar_sim import CesarSim
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.dc_sim import DcSim
from v2.chk.sim.ig_sim import IgSim
from v2.chk.sim.mfc_sim import MfcSim
from v2.chk.sim.plc_sim import PlcSim
from v2.chk.system import TEST_RETENTION, ChkSystem, Retention, new_store


def make_sims(start: float = 0.0, log_keep: Optional[int] = None):
    """공용 챔버 + 시뮬레이터 5개 — (chamber, {장비: 시뮬레이터}). start = 시계의 지금(실제 시계는 부팅 뒤 초 —
    0 에서 시작하면 첫 응답 때 그만큼을 0.1 s 씩 따라잡느라 멈춘 것처럼 된다). log_keep = 받은 요청 기록 상한
    (None 이면 시험용 기본 SIM_LOG_KEEP — 실행 프로그램 sim 은 기록이 필요 없어 작게)."""
    chamber = Chamber(start)
    sims = {"plc": PlcSim(start=start, chamber=chamber), "mfc": MfcSim(chamber), "dc": DcSim(chamber),
            "rfpulse": CesarSim(chamber), "ig": IgSim(chamber, start=start)}
    if log_keep is not None:
        for x in sims.values():
            for name in ("rx_log", "received", "frames"):
                if hasattr(x, name):
                    setattr(x, name, collections.deque(getattr(x, name), maxlen=log_keep))
    return chamber, sims


def sim_transports(clock, sims) -> Dict[str, SimTransport]:
    return {k: SimTransport(clock, x) for k, x in sims.items()}


class ChkRig(ChkSystem):
    """시험 하니스 — 시뮬레이터를 만들어 실행 프로그램과 같은 조립(ChkSystem)에 넣는다. 설정 저장소는 out_dir/config.
    retention = 기록 크기(기본 TEST_RETENTION — 결과를 보려고 크게, APP_RETENTION 이면 실행 프로그램과 같게)."""

    def __init__(self, clock, out_dir: Path, cfg: Optional[Dict[str, Any]] = None, scenario: str = "none",
                 simple_process: bool = False, mode: str = "sim", retention: Retention = TEST_RETENTION) -> None:
        self.chamber, self.sims = make_sims(clock.monotonic(), log_keep=retention.sim_log)
        self.scenario = scenario
        out = Path(out_dir)
        super().__init__(clock, dict(defaults(P.PARAMS), **(cfg or {})), out, sim_transports(clock, self.sims), mode=mode,
                         simple_process=simple_process, store=new_store(out / "config"),
                         scenario_hook=self._scenario if scenario == "light" else None, retention=retention)

    def _scenario(self, t) -> None:
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
            if self.enabled["rfpulse"]:
                self.rfp.start(100.0, freq_hz=10000, duty=50, tick=t)
        elif s == 45:
            if self.enabled["rfpulse"]:
                self.rfp.rf_off_confirmed()
            self.dc.output_off_confirmed()
        elif s == 50:
            self.ig.filament_off()
            self.mfc.all_flow_off()
