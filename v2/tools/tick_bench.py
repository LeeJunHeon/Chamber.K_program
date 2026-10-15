"""실제 시간 1초 tick 측정 — 가짜 장비 5대로 공통 틀을 돌려 jitter·놓친 tick·행 닫힘 지연을 잰다.

    python -m v2.tools.tick_bench --seconds 60 --mode sim

기록은 임시 폴더에만 쓴다(끝에 경로 출력). --mode 는 sim 만 받는다(bench·prod 는 실제 포트).
"""
from __future__ import annotations

import argparse
import collections
import sys
import tempfile
import time
from pathlib import Path

from v2.common.clock import RealClock
from v2.tools.sim_rig import BLOCKS, Rig


def pct(xs, p):
    s = sorted(xs)
    if not s:
        return float("nan")
    k = min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="v2 1초 tick 실제 시간 측정(가짜 장비)")
    ap.add_argument("--seconds", type=int, default=60)
    # sim 만 받는다 — 실수로 실제 장비 모드(bench·prod)로 돌리지 않도록
    ap.add_argument("--mode", default="sim", choices=["sim"])
    a = ap.parse_args(argv)
    out = Path(tempfile.mkdtemp(prefix="v2_tick_bench_"))
    clock = RealClock()
    rig = Rig(clock, out)
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
    print(f"mode=sim seconds={a.seconds} ticks={rig.loop.rows} missed={rig.loop.missed} wall={wall:.1f}s")
    print(f"tick jitter ms : p50={pct(j, 50):.3f} p99={pct(j, 99):.3f} max={max(j):.3f}")
    print(f"row close ms   : p50={pct(lag, 50):.1f} p99={pct(lag, 99):.1f} max={max(lag):.1f}  (마감 900)")
    for b in BLOCKS:
        cnt = collections.Counter(r[f"{b.name}.q"] for r in rows)
        print(f"quality {b.name:5s}: " + ", ".join(f"{k}={v}" for k, v in sorted(cnt.items())))
    print(f"recorder: written={rig.recorder.written} dropped={rig.recorder.dropped} errors={len(rig.recorder.errors)}")
    print(f"output: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
