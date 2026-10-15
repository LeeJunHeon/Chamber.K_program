"""챔버K 시험대 — 실제 드라이버 5개 + 시뮬레이터 + 공용 챔버 + 묶기 + 기록을 가상 시계로 60 tick."""
import collections
import sqlite3

import pytest

from v2.common.clock import VirtualClock
from v2.chk.sim.rig import ChkRig


def test_five_devices_light_scenario(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, scenario="light")
    rows = []
    rig.loop.sinks.append(rows.append)
    assert rig.start()                                                   # 모두 연결·초기화 뒤 tick 시작
    try:
        rig.loop.run(60)
    finally:
        rig.stop()
    by = {r["seq"]: r for r in rows}
    assert rig.loop.missed == 0 and len(rows) == 60
    bad = collections.Counter((b, r[f"{b}.q"]) for r in rows for b in ("plc", "mfc", "dc", "rfp", "ig")
                              if r[f"{b}.q"] not in ("ok", "suppressed", "late"))
    assert not bad, bad
    assert by[18]["mfc.pressure_mtorr"] == pytest.approx(5.0, rel=0.1)     # SP1 5 mTorr 압력 제어
    assert by[18]["mfc.ar_set"] == 20.0 and by[18]["mfc.sp_active"] == 1
    assert by[25]["dc.i"] == pytest.approx(0.3) and by[25]["dc.out"] is True
    assert by[40]["rfp.out_on"] is True and by[40]["rfp.ref_w"] < 10        # 점화(가스 있음) — 반사 작음
    assert by[40]["ig.filament"] is True and by[40]["ig.ion_torr"] == pytest.approx(5e-3, rel=0.1)
    assert by[58]["dc.out"] is False and by[58]["rfp.out_on"] is False and by[58]["ig.filament"] is False
    kinds = collections.Counter(getattr(e, "kind", "") for e in rig.events)
    assert kinds["verify_mismatch"] == 0 and kinds["off_confirmed"] >= 2 and kinds["heater_config_ok"] == 1
    assert rig.recorder.written == 60 and list(rig.recorder.errors) == []
    db = sqlite3.connect(next(tmp_path.glob("snap_*.sqlite")))
    assert db.execute("SELECT COUNT(*) FROM snapshot").fetchone()[0] == 60
    db.close()
