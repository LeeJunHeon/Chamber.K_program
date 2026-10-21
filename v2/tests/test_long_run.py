"""오래 켜 두기 — 가상 시계 10 시간(사용자 지시 5-7 추가). 실제 시계 10 분(연결·스레드)은 scratch 의 soak 스크립트로 따로.

실행 프로그램과 같은 조립·같은 기록 크기(ChkRig + system.APP_RETENTION, 기본 설정, sim)로 가상 10 시간.
150 s 마다 공정(가스 → DC 100 W → 대기 30 s → 끝 정지), 허브 소비자 하나를 tick 마다 비운다.
매시 gc 뒤 프로세스 전용 메모리(윈도우 PagefileUsage, 리눅스 /proc/self/statm 상주)를 잰다 — 8~10 시간 사이 증가 0.5 MB 안쪽.
환경 변수 CHK_LONGRUN_TABLE 이 있으면 시간별 표를 그 파일에(JSON) 남긴다.
"""
import gc
import json
import os
import time

import pytest

from v2.common.clock import VirtualClock
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig
from v2.chk.system import APP_RETENTION
from v2.tests.conftest import _mem

HOURS = 10
PERIOD_S = 150
BLOCKS = [{"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}},
          {"type": "power_on", "params": {"dc_power": 100.0}},
          {"type": "wait", "params": {"sec": 30}},
          {"type": "end_stop", "params": {}}]


@pytest.mark.slow
def test_virtual_10h_app_retention_memory_flat(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, retention=APP_RETENTION)
    for k in ("mv", "turbo", "rotary", "fv"):
        rig.sims["plc"].coils[A[k]] = True
    rep = rig.retention_report()
    assert rep["system.events"] == rep["process.events"] == rep["safety.events"] == rep["heater.events"] == 1000
    assert all(q.maxlen == APP_RETENTION.sim_log for x in rig.sims.values()
               for q in (getattr(x, n) for n in ("rx_log", "received", "frames") if hasattr(x, n)))
    sub = rig.hub.subscribe(name="drain")
    seen = {"run_end": [], "start": []}
    table = []
    w0 = time.perf_counter()

    def hook(t):
        while True:
            e = sub.get(0)
            if not isinstance(e, dict):
                break
            if e["type"] == "run_end":
                seen["run_end"].append(e["data"]["result"])
        if t.seq % PERIOD_S == 3:
            seen["start"].append(rig.core.command("PROCESS_START", {"blocks": BLOCKS}).reason)
        if t.seq % 3600 == 0:
            gc.collect()
            m = _mem()
            table.append({"h": t.seq // 3600, "private_mb": round(m["private"] / 1048576, 2),
                          "ws_mb": round(m["ws"] / 1048576, 2) if m["ws"] else None,
                          "runs": len(seen["run_end"]), "wall_s": round(time.perf_counter() - w0, 1)})
    rig.loop.tick_hooks.append(hook)
    assert rig.start()
    try:
        rig.loop.run(HOURS * 3600 + 1)                                    # 10 시간째 표본까지
    finally:
        rig.stop()
        sub.close()
    if os.environ.get("CHK_LONGRUN_TABLE"):
        with open(os.environ["CHK_LONGRUN_TABLE"], "w", encoding="utf-8") as f:
            json.dump(table, f, ensure_ascii=False, indent=1)
    n = HOURS * 3600 // PERIOD_S
    assert seen["start"].count(None) >= n - 1, seen["start"][:10]          # 공정이 매번 시작
    assert seen["run_end"].count("done") >= n - 2 and not sub.reason == "slow"
    by_h = {r["h"]: r["private_mb"] for r in table}
    assert len(by_h) == HOURS + 1
    assert by_h[HOURS] - by_h[8] < 0.5, table                              # 8~10 시간 사이 0.5 MB 안쪽
