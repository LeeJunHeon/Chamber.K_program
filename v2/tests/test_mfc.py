"""MFC 드라이버 + 시뮬레이터 시험(가상 시계). 실제 포트는 열지 않는다."""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.mfc import CH_AR, CH_O2, MfcDriver, parse_pressure, parse_r60, parse_r69, parse_prefixed, parse_valve
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.mfc_sim import MfcSim

CFG = defaults(P.PARAMS)


def test_parsers():
    assert parse_r60("Q0+20.0+5.5+0.0+0.0") == [20.0, 5.5, 0.0, 0.0]
    assert parse_r60("P+0.2") is None
    assert parse_prefixed("Q5+12.3", "Q5") == 12.3 and parse_prefixed("Q6+1", "Q5") is None
    assert parse_pressure("P+0.20") == 0.2 and parse_pressure("S1+30.00") == 30.0
    assert parse_r69("L0 1010") == "1010" and parse_r69("L01100") == "1100"


class Rig:
    def __init__(self, writes=True, sim=None, cfg=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber()
        self.sim = sim or MfcSim(self.ch)
        if sim is not None:
            sim.chamber = self.ch
        self.cfg = dict(CFG, **(cfg or {}))
        self.events = []
        self.w = Worker("mfc", SimTransport(self.c, self.sim), writes_allowed=writes, fail_streak=self.cfg["MFC_FAIL_STREAK"])
        self.d = MfcDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append)
        self.w.start()
        self.c.wait_until(1.5)

    def kinds(self):
        return [e.kind for e in self.events]

    def seq(self, fn, *a):
        box = []
        fn(*a, on_done=lambda n, r, res: (box.append(r), self.c.notify()))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def run(self, n):
        bus = SnapshotBus(self.c, (SF.MFC_BLOCK,), [Source("mfc", self.w, self.d.source_read, ("mfc",),
                                                             self.cfg["MFC_READ_DEADLINE_MS"])], self.cfg["TICK_DEADLINE_MS"])
        rows = []
        TickLoop(self.c, bus, [rows.append]).run(n)
        return rows

    def done(self):
        self.w.stop()


def test_fs_read_and_scaling():
    sim = MfcSim(fs_sccm=(200.0, 50.0, 100.0, 100.0))
    r = Rig(sim=sim)
    assert r.d.fs == {CH_AR: 200.0, CH_O2: 50.0} and r.d.fs_source == "R70/R71"
    ev = [e for e in r.events if e.kind == "fs_not_100"]
    assert ev and ev[0].reason == "MFC_FS_NOT_100" and "사용자 확인" in ev[0].detail
    assert r.seq(r.d.set_flow, CH_AR, 20.0) is None                     # 20 sccm → 10.0 %FS
    assert sim.set_pct[0] == 10.0 and r.d.ar_set == 20.0
    assert r.seq(r.d.set_onoff, "1000") is None
    rows = r.run(6)
    assert rows[-1]["mfc.ar_sccm"] == pytest.approx(20.0, rel=0.02)    # 읽기 %FS → sccm
    assert rows[-1]["mfc.ar_set"] == 20.0
    r.done()


def test_fs_fallback_to_params():
    sim = MfcSim()
    sim.add_fault("garbage", 0.0, 1.0)
    r = Rig(sim=sim, cfg={"MFC_AR_FS_SCCM": 100.0, "MFC_O2_FS_SCCM": 100.0})
    assert r.d.fs_source == "param" and "fs_not_100" not in r.kinds()
    r.done()


def test_alternate_read_quality_and_age():
    r = Rig(cfg={"MFC_READ_PLAN": "alternate"})
    rows = r.run(6)
    for x in rows:
        assert x["mfc.q"] == "ok" and x["mfc.pressure_mtorr"] is not None   # 압력은 매초
        if x["seq"] % 2 == 0:
            assert (x["mfc.ar_sccm.q"], x["mfc.ar_sccm.age_s"]) == ("ok", 0.0)
        else:
            assert (x["mfc.ar_sccm.q"], x["mfc.ar_sccm.age_s"]) == ("stale", 1.0)   # 유량은 격초
    assert r.sim.dropped == 0                                           # 명령 간격 150 ms 를 지킴
    # 밸브 위치(5-17)는 홀수 tick — tick 마다 질의 둘(R5 + R60 또는 R6)
    for x in rows[2:]:
        if x["seq"] % 2 == 1:
            assert (x["mfc.valve_pct"], x["mfc.valve_pct.q"], x["mfc.valve_pct.age_s"]) == (100.0, "ok", 0.0)
        else:
            assert (x["mfc.valve_pct"], x["mfc.valve_pct.q"], x["mfc.valve_pct.age_s"]) == (100.0, "stale", 1.0)
    q = [d.decode().strip() for tt, d in r.sim.received][-12:]
    assert q.count("R5") == 6 and q.count("R60") == 3 and q.count("R6") == 3
    r.done()


# ───────── 밸브 위치 1초 기록(5-17 — v1 12ecc2d 의 MFC 상태 CSV "V" 열) ─────────
def test_parse_valve_needs_v_prefix():
    assert parse_valve("V+100.0") == 100.0 and parse_valve(" V +37.5 ") == 37.5 and parse_valve("V0") == 0.0
    for bad in ("P+0.20", "+50.0", "50.0", "S1+30.00", "M110", "", None, "V", "Vx"):
        assert parse_valve(bad) is None


def test_valve_position_every_second_in_full_plan():
    r = Rig()
    assert "mfc.valve_pct" in [f"mfc.{f.name}" for f in SF.MFC_BLOCK.fields]
    rows = r.run(4)
    assert all((x["mfc.q"], x["mfc.valve_pct"], x["mfc.valve_pct.q"], x["mfc.valve_pct.age_s"]) == ("ok", 100.0, "ok", 0.0)
               for x in rows)                                           # 시작 때 밸브 완전 열림(O)
    assert not r.d.alternate and r.sim.dropped == 0                     # R5·R60·R6 셋 다 1초 안(auto 가 번갈아로 넘기지 않음)
    assert r.seq(r.d.set_flow, CH_AR, 20.0) is None and r.seq(r.d.set_onoff, "1000") is None
    assert r.seq(r.d.set_sp, 1, 5.0) is None and r.seq(r.d.select_sp, 1) is None
    r.c.wait_until(r.c.monotonic() + 40.0)
    x = r.run(2)[-1]
    assert 0.0 < x["mfc.valve_pct"] < 50.0 and x["mfc.valve_pct"] == round(r.sim.valve * 100, 1)   # 압력 제어 — 밸브를 조임
    r.done()


def test_valve_read_failure_does_not_fail_pressure():
    """R6 응답이 "V" 로 시작하지 않거나 없으면 그 tick 의 밸브 값만 stale — 압력·유량·장비 품질은 ok."""
    r = Rig()
    r.sim.canned["R6"] = ["V+100.0", "P+0.20", None, "50.0"]            # 정상 → 접두어 다름 → 무응답 → 접두어 없음 → (평소 응답)
    rows = r.run(5)
    assert all(x["mfc.q"] == "ok" and x["mfc.pressure_mtorr"] is not None and x["mfc.ar_sccm.q"] == "ok" for x in rows)
    assert [x["mfc.valve_pct.q"] for x in rows] == ["ok", "stale", "stale", "stale", "ok"]
    assert [x["mfc.valve_pct.age_s"] for x in rows] == [0.0, 1.0, 2.0, 3.0, 0.0] and rows[3]["mfc.valve_pct"] == 100.0
    assert not r.d.alternate and "read_plan" not in r.kinds()           # R6 무응답 한 번이 읽기 계획을 바꾸지 않는다
    r.done()


def test_valve_read_yields_to_pending_command():
    """v1 원칙 — 기록용 읽기는 공정 명령보다 뒤: 설정 명령이 진행 중인 tick 에는 R6 를 보내지 않는다(압력·유량은 읽는다)."""
    r = Rig()
    r.sim.ignore_writes = {"S2"}                                        # 되읽기 불일치 — 1 s 간격 5회(약 5 s) 동안 명령 진행 중
    box = []
    t0 = r.c.monotonic()
    r.d.set_sp(2, 5.0, on_done=lambda n, why, res: box.append(why))
    rows = r.run(9)
    assert box == ["VERIFY_MISMATCH"] and r.d._ops == 0
    busy = [x for x in rows[:4]]
    assert all(x["mfc.q"] == "ok" and x["mfc.pressure_mtorr"] is not None and x["mfc.ar_sccm.q"] == "ok" for x in rows)
    assert all(x["mfc.valve_pct.q"] == "stale" and x["mfc.valve_pct"] is None for x in busy) and r.d.valve_yielded >= 4
    assert rows[-1]["mfc.valve_pct.q"] == "ok" and rows[-1]["mfc.valve_pct"] == 100.0   # 명령이 끝나면 다시 읽는다
    sent = [d.decode().strip() for tt, d in r.sim.received if tt >= t0]
    assert sent.count("R6") == 9 - r.d.valve_yielded and sent.count("R5") == 9
    r.done()


def test_valve_read_skipped_when_it_would_miss_read_deadline():
    """설정 명령 둘(Q1·R65, L0·R69 — 약 0.6 s)이 읽기 앞에 선 tick: R5·R60 뒤 R6 까지 보내면 읽기 마감(900 ms)을 넘긴다 —
    R6 를 건너뛰어 압력·유량이 그 행에 제때(ok) 들어간다."""
    r = Rig()
    box, rows = [], []
    cb = lambda n, why, res: box.append(why)

    def hook(t):
        if t.seq == 1:                                                  # tick 1 시작 때 명령 둘 — 쓰기가 읽기보다 먼저 나간다
            r.d.set_flow(CH_AR, 20.0, on_done=cb)
            r.d.set_onoff("1000", on_done=cb)
    bus = SnapshotBus(r.c, (SF.MFC_BLOCK,), [Source("mfc", r.w, r.d.source_read, ("mfc",), r.cfg["MFC_READ_DEADLINE_MS"])],
                      r.cfg["TICK_DEADLINE_MS"])
    loop = TickLoop(r.c, bus, [rows.append])
    loop.tick_hooks.append(hook)
    loop.run(4)
    assert box == [None, None]
    assert [x["mfc.q"] for x in rows] == ["ok"] * 4 and all(x["mfc.ar_sccm.q"] == "ok" for x in rows)
    assert [x["mfc.valve_pct.q"] for x in rows] == ["ok", "stale", "ok", "ok"] and r.d.valve_yielded == 1
    r.done()


def test_valve_read_not_blocked_forever_by_lost_command():
    r = Rig()
    r.d._ops, r.d._ops_until = 1, r.c.monotonic() + 2.0                 # 끝 콜백을 못 받은 명령(가정) — 기한 뒤에는 다시 읽는다
    rows = r.run(5)
    assert rows[0]["mfc.valve_pct.q"] != "ok" and rows[-1]["mfc.valve_pct.q"] == "ok"
    r.done()


def test_auto_switches_to_alternate_when_slow():
    sim = MfcSim()
    sim.add_fault("delay", 3.0, 100.0, extra_s=0.24)                   # 질의마다 +240 ms(제한 300 안) → 한 번 읽기 > 마감 60 %
    r = Rig(sim=sim)
    rows = r.run(5)
    assert "read_plan" in r.kinds() and r.d.alternate
    assert all(x["mfc.q"] == "ok" for x in rows)
    r.done()


def test_verify_mismatch_and_ok():
    sim = MfcSim()
    sim.ignore_writes = {"Q1"}
    r = Rig(sim=sim)
    assert r.seq(r.d.set_flow, CH_AR, 10.0) == "VERIFY_MISMATCH"
    ev = [e for e in r.events if e.kind == "verify_mismatch"]
    assert ev and ev[0].reason == "VERIFY_MISMATCH"
    assert [c for c in sim.set_commands_seen()].count("Q1 10.0") == 5    # v1: 되읽기가 다르면 Q 를 다시, 5회(MFC.py:1072-1076)
    assert r.d.ar_set is None
    assert r.seq(r.d.set_flow, CH_O2, 5.0) is None and r.d.o2_set == 5.0
    r.done()


def test_pressure_control_through_chamber():
    r = Rig()
    assert r.seq(r.d.set_flow, CH_AR, 20.0) is None
    assert r.seq(r.d.set_onoff, "1000") is None
    assert r.seq(r.d.set_sp, 1, 5.0) is None and r.d.sp_mtorr == 5.0  # 5 mTorr → 0.50 %FS(FS 1 Torr)
    assert r.sim.sp[0] == 0.5
    assert r.seq(r.d.select_sp, 1) is None and r.d.sp_active == 1
    r.c.wait_until(r.c.monotonic() + 40.0)
    rows = r.run(3)
    assert rows[-1]["mfc.pressure_mtorr"] == pytest.approx(5.0, rel=0.05)   # 밸브가 배기를 줄여 설정점으로
    assert r.ch.throttle < 0.5
    assert r.seq(r.d.valve, True) is None                               # O → R6 100
    r.c.wait_until(r.c.monotonic() + 30.0)
    assert r.run(1)[-1]["mfc.pressure_mtorr"] == pytest.approx(2.0, rel=0.05)   # 완전 열림이면 20 sccm ≈ 2 mTorr
    r.done()


def test_bench_no_writes():
    r = Rig(writes=False)
    box = []
    cb = lambda n, x, res: box.append(x)
    r.d.set_flow(CH_AR, 10.0, on_done=cb)
    r.d.set_onoff("1000", on_done=cb)
    r.d.set_sp(1, 5.0, on_done=cb)
    r.d.valve(False, on_done=cb)
    r.d.zero("Z1", on_done=cb)
    rows = r.run(3)
    assert box == ["BENCH_READ_ONLY"] * 5 and r.sim.set_commands_seen() == []
    assert all(x["mfc.q"] == "ok" for x in rows)
    r.done()


def test_connect_scale_check_matches_and_mismatches():
    """연결 때 R33·R34·R70·R71·R74·R75 대조 [MFC p.83-88] — 다르면 MFC_SCALE_MISMATCH, 못 읽으면 경고만. 값은 바꾸지 않는다.
    R37 은 정보로만(10/20 bench 'M006' — X=0 인데 v1 은 원격 제어가 됐다)."""
    r = Rig()
    ev = [e for e in r.events if e.kind == "scale"]
    assert r.d.scale_mismatch is None and ev and ev[0].reason is None and "1 Torr" in ev[0].detail
    r.done()
    sim = MfcSim()
    sim.info[33], sim.info[34], sim.info[37] = "E06", "F00", "M000"     # 10 Torr 레인지(Torr 단위), R37 X=0
    r = Rig(sim=sim)
    assert "10 Torr" in r.d.scale_mismatch and "Local" not in r.d.scale_mismatch
    assert [e for e in r.events if e.kind == "remote_state"]       # R37 은 정보 이벤트만
    assert [e.reason for e in r.events if e.kind == "scale"] == ["MFC_SCALE_MISMATCH"]
    assert r.cfg["MFC_PRESSURE_FS_TORR"] == 1.0                    # 설정값은 그대로
    r.done()
    sim = MfcSim()
    del sim.info[34]                                               # 단위를 못 읽음
    r = Rig(sim=sim)
    assert r.d.scale_mismatch is None
    assert [e.reason for e in r.events if e.kind == "scale"] == ["MFC_SCALE_UNREAD"]
    r.done()


def test_sp4_written_and_read_back_with_r4():
    """5-4 ④ — SP4_MTORR(30 mTorr) → S4 3.00(%FS, FS 1 Torr) 쓰고 R4 'S4+003.00'(10/20 bench 형식)으로 되읽기."""
    r = Rig()
    t0 = r.c.monotonic()
    assert r.seq(r.d.set_sp, 4, 30.0) is None
    sent = [d for tt, d in r.sim.received if tt >= t0]
    assert sent[:2] == [b"S4 3.00\r", b"R4\r"]
    assert r.sim._query("R4") == "S4+003.00"
    assert [e for e in r.events if e.kind == "verify_ok" and "R4=S4+003.00" in e.detail]
    r.sim.ignore_writes = {"S4"}
    assert r.seq(r.d.set_sp, 4, 15.0) is None                           # SP2~SP4 불일치는 경고만(v1 은 되읽기 없음)
    assert [e for e in r.events if e.kind == "sp_readback" and e.reason == "VERIFY_MISMATCH"]
    r.done()
