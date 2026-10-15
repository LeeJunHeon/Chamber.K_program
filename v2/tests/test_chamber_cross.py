"""공용 챔버 모델 교차 시험 — PLC·IG·DC 드라이버가 같은 챔버의 압력·밸브·플라즈마를 서로 맞게 보이는지."""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.dc import DcDriver
from v2.chk.drivers.ig import IgDriver
from v2.chk.drivers.plc import PlcDriver
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.dc_sim import DcSim
from v2.chk.sim.ig_sim import IgSim
from v2.chk.sim.plc_sim import PlcSim

CFG = defaults(P.PARAMS)


def test_plc_ig_dc_share_one_chamber():
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    ch = Chamber()
    sims = {"plc": PlcSim(chamber=ch), "ig": IgSim(ch), "dc": DcSim(ch)}
    w = {k: Worker(k, SimTransport(c, s)) for k, s in sims.items()}
    plc = PlcDriver(c, w["plc"], CFG, True)
    ig = IgDriver(c, w["ig"], CFG, True)
    dc = DcDriver(c, w["dc"], CFG, True)
    with c.hold():
        for x in w.values():
            x.start()
    blocks = (SF.PLC_BLOCK, SF.HEATER_BLOCK, SF.RF_BLOCK, SF.DC_BLOCK, SF.IG_BLOCK)
    bus = SnapshotBus(c, blocks, [
        Source("plc", w["plc"], plc.source_read, ("plc", "heater", "rf"), CFG["PLC_READ_DEADLINE_MS"]),
        Source("dc", w["dc"], dc.source_read, ("dc",), CFG["DC_READ_DEADLINE_MS"]),
        Source("ig", w["ig"], ig.source_read, ("ig",), CFG["IG_READ_DEADLINE_MS"]),
    ], CFG["TICK_DEADLINE_MS"])
    rows = []
    actions = {
        1: lambda: (ch.gas_sccm.__setitem__("ar", 20.0), ig.filament_on()),
        30: "dc_start",
        40: lambda: (plc.set_button("mv", False), plc.set_button("vent", True)),
    }

    def hook(t):
        plc.on_tick(t)
        ig.on_tick(t)
        f = actions.get(t.seq)
        if f == "dc_start":
            dc.start_output(600.0, 0.4, tick=t)                  # 같은 tick 의 측정 뒤에 시작
        elif f:
            f()
    loop = TickLoop(c, bus, [rows.append], tick_hooks=[hook])
    loop.run(130)
    for x in w.values():
        x.stop()
    by = {r["seq"]: r for r in rows}

    # 가스 20 sccm, MV 열림 → 약 2 mTorr: PLC 대기압 아님·인터록 정상, IG 이온 압력, 게이지 A 하한 위
    r = by[28]
    assert r["plc.atm"] is False and r["plc.mv_interlock"] is True
    assert r["ig.filament"] and r["ig.ion_torr"] == pytest.approx(ch_p := 2e-3, rel=0.1)
    assert r["ig.a_torr"] == pytest.approx(2e-3, rel=0.1)
    # DC 점화 — 전압은 압력으로 정해진 유지 전압 + 50·I
    r = by[38]
    vp = 350 + 150 / (2.0 ** 0.5)
    assert r["dc.i"] == pytest.approx(0.4) and r["dc.v"] == pytest.approx(vp + 20, rel=0.02)
    # MV 닫고 Vent → 압력 상승: DC 플라즈마 소멸(무부하), IG 필라멘트 고압 차단(오류 15), 끝내 대기압
    r = by[60]
    assert r["dc.i"] == 0.0 and r["dc.v"] == 600.0
    assert r["ig.filament"] is False and r["ig.err_code"] == 15
    assert r["plc.mv"] is False and r["plc.vent"] is True and r["plc.mv_interlock"] is False
    r = by[129]
    assert r["plc.atm"] is True and r["ig.a_torr"] > 700.0
    bad = [(x["seq"], x["plc.q"], x["dc.q"]) for x in rows if x["seq"] > 3 and (x["plc.q"], x["dc.q"]) != ("ok", "ok")]
    assert bad == []
