"""전체 흐름 — 가상 시계 + 가짜 장비 5대 worker + 스냅샷 묶기 + 기록.
품질·나이·놓친 tick·재연결이 SQLite 에 맞게 남는지. 기록은 임시 폴더에만."""
import sqlite3

from v2.common.clock import VirtualClock
from v2.tools import tick_bench
from v2.tools.sim_rig import Rig, make_devices


def test_full_flow(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.25)        # 첫 tick: monotonic 0.75
    c.register()
    devs = make_devices()
    devs["dc"].add_fault("disconnect", 5.0, 9.0)           # 끊김 → 재연결
    devs["slow"].add_fault("no_response", 15.0, 17.0)      # 무응답 2회
    devs["alt"].add_fault("garbage", 25.0, 26.0)           # 깨진 응답
    rig = Rig(c, tmp_path, devices=devs)

    def stall(row):                                        # seq 20 행(≈21.05 에 닫힘) 뒤 2.3 s 밀림 → 23.35
        if row["seq"] == 20:
            c.advance(2.3)

    rig.loop.sinks.append(stall)
    rig.start()
    try:
        rig.loop.run(30)
    finally:
        rig.stop()
    assert rig.recorder.flush() and list(rig.recorder.errors) == [] and rig.recorder.dropped == 0

    day = sorted(tmp_path.glob("snap_*.sqlite"))
    assert len(day) == 1
    db = sqlite3.connect(day[0])
    db.row_factory = sqlite3.Row
    rows = {r["seq"]: dict(r) for r in db.execute("SELECT * FROM snapshot ORDER BY seq")}
    events = [dict(e) for e in db.execute("SELECT * FROM events")]
    db.close()

    # 놓친 tick
    # 23.35 에 깨어남 → tick 21 놓침, 지나간 가장 최근 정각 tick 22 를 600 ms 늦게
    assert sorted(rows) == [n for n in range(31) if n != 21]
    assert rows[22]["skipped"] == 1 and sum(r["skipped"] for r in rows.values()) == rig.loop.missed == 1
    assert rows[22]["jitter_ms"] == 600 and rows[0]["jitter_ms"] == 0

    # 빠른 장비는 항상 ok
    bad = [(n, r["fast.q"], r["fast.age_s"]) for n, r in rows.items() if (r["fast.q"], r["fast.age_s"]) != ("ok", 0)]
    assert bad == []

    # DC 끊김(5.0~9.0) → fail, 백오프 뒤 재연결되면 ok
    dc = {n: (r["dc.q"], r["dc.age_s"]) for n, r in rows.items()}
    assert dc[4] == ("ok", 0)
    assert all(dc[n][0] == "fail" for n in range(5, 12))      # tick 5.75 끊김 감지 → 1·2·4 s 백오프
    assert dc[12] == ("ok", 0)
    assert dc[11][1] == 7                                      # 마지막 값(seq 4)의 나이
    kinds = [(e["source"], e["code"]) for e in events]
    assert ("dc", "DISCONNECTED") in kinds and ("dc", "CONNECTED") in kinds

    # 느린 장비 무응답(15~17) → fail 두 번, 끊기 전에 복구(연속 3회 미만)
    slow = [rows[n]["slow.q"] for n in (14, 15, 16, 17)]
    assert slow == ["ok", "fail", "fail", "ok"]
    assert ("slow", "DISCONNECTED") not in kinds

    # 번갈아 읽기: 유량은 짝수 tick 에만 — 홀수 tick 은 stale, 나이 1
    assert (rows[2]["alt.flow1.q"], rows[2]["alt.flow1.age_s"]) == ("ok", 0)
    assert (rows[3]["alt.flow1.q"], rows[3]["alt.flow1.age_s"]) == ("stale", 1)
    assert rows[3]["alt.q"] == "ok"
    # 깨진 응답(25~26) → seq 25(25.75) 는 fail, 값은 마지막 값·나이 1
    assert (rows[25]["alt.q"], rows[25]["alt.age_s"], rows[25]["alt.press"]) == ("fail", 1, 2.0)
    assert rows[26]["alt.q"] == "ok"


def test_tick_bench_accepts_only_sim():
    import pytest
    for mode in ("bench", "prod"):
        with pytest.raises(SystemExit) as e:
            tick_bench.main(["--seconds", "1", "--mode", mode])
        assert e.value.code == 2
