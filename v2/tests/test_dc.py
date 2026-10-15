"""DC 전원 드라이버 + 시뮬레이터 시험(가상 시계). 실제 포트는 열지 않는다."""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Request, Status, Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.dc import BENCH_QUERIES, DcDriver, DcQueryBlocked
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.dc_sim import DcSim

CFG = defaults(P.PARAMS)


class Rig:
    def __init__(self, writes=True, sim=None, cfg=None, chamber=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = chamber or Chamber()
        self.sim = sim or DcSim(self.ch)
        self.events = []
        self.cfg = dict(CFG, **(cfg or {}))
        self.w = Worker("dc", SimTransport(self.c, self.sim), writes_allowed=writes, fail_streak=self.cfg["DC_FAIL_STREAK"],
                        connect_init_timeout_s=self.cfg["CONNECT_INIT_TIMEOUT_SEC"])
        self.d = DcDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append)
        self.w.start()
        self.c.wait_until(3.0)

    def kinds(self):
        return [e.kind for e in self.events]

    def read(self, deadline=0.5):
        box = []
        self.w.submit(Request("read", lambda s: self.d.source_read(s, 0), deadline=self.c.monotonic() + deadline,
                              on_done=lambda r: (box.append(r), self.c.notify())))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def seq_wait(self, starter):
        box = []
        starter(on_done=lambda n, r, res: (box.append(r), self.c.notify()))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def done(self):
        self.w.stop()


def test_connect_identity_and_last_state_written_once():
    sim = DcSim(last_sta="DIS")
    r = Rig(sim=sim)
    assert r.d.model == "ODA,EX-600-1,SIM0001,1.00" and "identity" in r.kinds()
    assert sim.last_sta == "SAF" and sim.writes_last_sta == 1
    assert not any(c.startswith(("VOLT:OVP", "CURR:OCP")) for c in sim.commands_seen())   # 보호 설정 기본 끔
    r.done()
    sim.busy_until = -1.0                                             # 새 가상 시계(시각 0)에서 다시 쓰므로
    r2 = Rig(sim=sim)                                                 # 다시 연결 — 이미 SAF 면 쓰지 않음
    assert sim.writes_last_sta == 1 and "이미 SAF" in [e for e in r2.events if e.kind == "last_state"][0].detail
    r2.done()


def test_protection_written_and_verified_when_enabled():
    r = Rig(cfg={"DC_SET_PROTECTION": True})
    assert r.sim.ovp == CFG["DC_MAX_VOLTAGE"] and r.sim.ocp == CFG["DC_MAX_CURRENT"]
    ev = [e for e in r.events if e.kind == "protection"]
    assert ev and ev[0].reason is None
    r.done()


def test_query_waits_after_write():
    r = Rig()
    r.d.set_current(0.2)
    res = r.read(deadline=1.0)
    curr_t = [t for t, d in r.sim.received if d.startswith(b"CURR")][-1]
    meas_t = [t for t, d in r.sim.received if d.startswith(b"MEAS")][-1]
    assert res.status == Status.OK and meas_t - curr_t >= CFG["DC_WRITE_QUERY_GAP_MS"] / 1000 - 1e-9
    assert r.sim.ignored_queries == 0
    r.d.set_current(0.3)
    r.c.wait_until(r.c.monotonic() + 0.05)
    res = r.read(deadline=0.2)                                         # 금지 시간이 마감을 넘음 → 보내지 않음
    assert res.status == Status.FAIL and "DcQueryBlocked" in res.error
    r.done()


def test_tick_order_meas_then_curr():
    r = Rig()
    r.ch.gas_sccm["ar"] = 20.0
    r.c.wait_until(30.0)
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.1, on_done=on_done))
    bus = SnapshotBus(r.c, (SF.DC_BLOCK,), [Source("dc", r.w, r.d.source_read, ("dc",), CFG["DC_READ_DEADLINE_MS"])],
                      CFG["TICK_DEADLINE_MS"])
    rows = []
    amps = iter([0.15, 0.2, 0.25, 0.3, 0.35, 0.4])
    loop = TickLoop(r.c, bus, [rows.append], tick_hooks=[lambda t: r.d.set_current(next(amps), t)])
    loop.run(6)
    assert all(x["dc.q"] == "ok" for x in rows) and r.sim.ignored_queries == 0
    ev = [(t, d[:4]) for t, d in r.sim.received if d.startswith((b"MEAS", b"CURR"))][-12:]
    for (t1, a), (t2, b) in zip(ev, ev[1:]):
        if a == b"MEAS" and b == b"CURR":
            assert 0.09 <= t2 - (t1 - (t1 % 1.0)) <= 0.2                # 같은 tick 의 t0 + 약 100 ms
        if a == b"CURR" and b == b"MEAS":
            assert t2 - t1 >= 0.5                                       # 쓰기 뒤 500 ms 질의 금지
    assert rows[-1]["dc.i"] == pytest.approx(0.35, abs=1e-4)            # 앞 tick 에 쓴 전류가 다음 MEAS 에
    assert rows[-1]["dc.out"] is True and rows[-1]["dc.i_set"] == pytest.approx(0.35)   # 측정 시점의 설정 전류
    r.done()


def test_off_confirm_success():
    r = Rig()
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.1, on_done=on_done))
    assert r.sim.out
    reason = r.seq_wait(r.d.output_off_confirmed)
    assert reason is None and not r.sim.out and "off_confirmed" in r.kinds()
    tail = r.sim.commands_seen()[-2:]
    assert tail == ["OUTP OFF", "OUTP?"]
    r.done()


def test_off_confirm_fails_after_three():
    r = Rig()
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.1, on_done=on_done))
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 30.0)
    reason = r.seq_wait(r.d.output_off_confirmed)
    assert reason == "DC_OFF_UNCONFIRMED"
    ev = [e for e in r.events if e.kind == "off_unconfirmed"]
    assert ev and ev[0].reason == "DC_OFF_UNCONFIRMED"
    cmds = [c for tt, c in zip([x[0] for x in r.sim.received], r.sim.commands_seen()) if tt >= t]
    assert cmds.count("OUTP?") == 3 and cmds.count("OUTP OFF") == 3     # 처음 + 무응답 뒤 두 번 다시
    r.done()


def test_writes_without_reply_not_counted_as_link_ok():
    r = Rig()
    assert r.read().status == Status.OK
    t_ok = r.w.last_ok
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 100.0)
    box = []
    r.d.set_current(0.2, on_done=box.append)
    r.d.set_current(0.3, on_done=box.append)
    r.c.wait_until(None, lambda: len(box) == 2)
    assert all(b.status == Status.OK for b in box) and r.w.last_ok == t_ok   # 쓰기만으로는 정상 아님
    for _ in range(3):
        r.c.wait_until(r.c.monotonic() + 1.0)
        r.read(deadline=0.9)
    assert not r.w.connected                                          # 응답이 없으니 끊김으로
    r.done()


def test_bench_no_writes_queries_allowed():
    sim = DcSim(last_sta="DIS")
    r = Rig(writes=False, sim=sim)
    box = []
    r.d.set_current(0.2, on_done=box.append)
    r.d.start_output(600.0, 0.1, on_done=lambda n, x, res: box.append(x))
    r.d.output_off_confirmed(on_done=lambda n, x, res: box.append(x))
    assert r.read().status == Status.OK
    r.c.wait_until(r.c.monotonic() + 2.0)
    assert sim.set_commands_seen() == [] and sim.writes_last_sta == 0
    assert set(sim.commands_seen()) <= BENCH_QUERIES
    assert [getattr(b, "reason", b) for b in box] == ["BENCH_READ_ONLY"] * 3
    assert "off_unconfirmed" not in r.kinds()
    r.done()


def test_plasma_and_faults_through_driver():
    r = Rig()
    r.ch.gas_sccm["ar"] = 20.0
    r.c.wait_until(30.0)
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.5, on_done=on_done))
    r.c.wait_until(r.c.monotonic() + 1.0)
    v = r.read().value
    vp = 350 + 150 / (r.ch.pressure_mtorr ** 0.5)
    assert v["dc.i"] == pytest.approx(0.5) and v["dc.v"] == pytest.approx(vp + 25, abs=0.01)
    assert v["dc.p"] == pytest.approx(v["dc.v"] * v["dc.i"], abs=0.01)
    r.ch.low_current = True
    assert r.read().value["dc.i"] == pytest.approx(0.05)               # 저전류 주입(S31B 시험용)
    r.ch.low_current = False
    r.seq_wait(r.d.output_off_confirmed)
    r.ch.ignite_fail = True
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.5, on_done=on_done))
    r.c.wait_until(r.c.monotonic() + 1.0)
    v = r.read().value
    assert v["dc.i"] == 0.0 and v["dc.v"] == 600.0                      # 점화 실패 — 무부하(S31A 시험용)
    r.done()
