"""실제 시간 1초 tick 측정 — 실제 드라이버 5개(PLC·MFC·DC·RF Pulse·IG) + 시뮬레이터 + 공용 챔버로 공통 틀을 돌려
jitter·놓친 tick·행 닫힘 지연·장비별 품질을 잰다.

    python -m v2.tools.tick_bench --seconds 60 --mode sim [--scenario light|none] [--rig chk|generic]

기록은 임시 폴더에만 쓴다(끝에 경로 출력). --mode 는 sim 만 받는다(bench·prod 는 실제 포트).
--rig generic 은 3단계의 줄 프로토콜 가짜 장비 5대(v2.tools.sim_rig).
"""
from __future__ import annotations

import argparse
import collections
import sys
import tempfile
import time
from pathlib import Path

from v2.common.clock import RealClock


def pct(xs, p):
    s = sorted(xs)
    if not s:
        return float("nan")
    k = min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="v2 1초 tick 실제 시간 측정(시뮬레이터)")
    ap.add_argument("--seconds", type=int, default=60)
    # sim 만 받는다 — 실수로 실제 장비 모드(bench·prod)로 돌리지 않도록
    ap.add_argument("--mode", default="sim", choices=["sim"])
    ap.add_argument("--rig", default="chk", choices=["chk", "generic"])
    ap.add_argument("--scenario", default="light", choices=["light", "none"])
    a = ap.parse_args(argv)
    out = Path(tempfile.mkdtemp(prefix="v2_tick_bench_"))
    clock = RealClock()
    if a.rig == "chk":
        from v2.chk.sim.rig import ChkRig
        rig = ChkRig(clock, out, scenario=a.scenario)
        blocks = [b.name for b in rig.bus.blocks]
    else:
        from v2.tools.sim_rig import BLOCKS, Rig
        rig = Rig(clock, out)
        blocks = [b.name for b in BLOCKS]
    rows = []
    rig.loop.sinks.append(rows.append)
    rig.start()
    t0 = time.perf_counter()
    try:
        rig.loop.run(a.seconds)
    finally:
        rig.stop()
    wall = time.perf_counter() - t0
    j, lag = rig.loop.jitters, rig.loop.close_lags
    print(f"mode=sim rig={a.rig} scenario={a.scenario if a.rig == 'chk' else '-'} seconds={a.seconds} "
          f"ticks={rig.loop.rows} missed={rig.loop.missed} wall={wall:.1f}s")
    print(f"tick jitter ms : p50={pct(j, 50):.3f} p99={pct(j, 99):.3f} max={max(j):.3f}")
    print(f"row close ms   : p50={pct(lag, 50):.1f} p99={pct(lag, 99):.1f} max={max(lag):.1f}  (마감 900)")
    for b in blocks:
        cnt = collections.Counter(r[f"{b}.q"] for r in rows)
        print(f"quality {b:7s}: " + ", ".join(f"{k}={v}" for k, v in sorted(cnt.items())))
    if a.rig == "chk":
        kinds = collections.Counter(f"{type(e).__name__}.{getattr(e, 'kind', '?')}" for e in rig.events)
        print("events: " + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())))
        last = rows[-1] if rows else {}
        print(f"last row: pressure={last.get('mfc.pressure_mtorr')} mTorr, ig.ion={last.get('ig.ion_torr')}, "
              f"dc.out={last.get('dc.out')}, rfp.out_on={last.get('rfp.out_on')}, heater.pv={last.get('heater.pv')}")
    print(f"recorder: written={rig.recorder.written} dropped={rig.recorder.dropped} errors={len(rig.recorder.errors)}")
    print(f"output: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
