"""RF Pulse(CESAR 1310) 드라이버 + 시뮬레이터 시험(가상 시계). 실제 포트는 열지 않는다."""
import pytest

from v2.common import aebus
from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.rfpulse import BENCH_QUERIES, RfPulseDriver, parse_status
from v2.chk.sim.cesar_sim import CesarSim
from v2.chk.sim.chamber import Chamber

CFG = defaults(P.PARAMS)


def test_aebus_frames():
    assert aebus.build(1, 14, b"\x02") == bytes.fromhex("09 0E 02 05")       # v1 로그 예시와 같음
    long = aebus.build(1, 200, bytes(range(10)))
    assert long[0] & 7 == 7 and long[2] == 10 and aebus.parse(long) == (1, 200, bytes(range(10)))
    with pytest.raises(aebus.AeChecksumError):
        aebus.parse(long[:-1] + bytes([long[-1] ^ 1]))
    with pytest.raises(ValueError):
        aebus.build(0, 1)                                                     # 브로드캐스트 금지
    st = parse_status(bytes([0x20, 0x80, 0, 0]))
    assert st["out_on"] and st["itl_open"] and not st["overtemp"]


class Rig:
    def __init__(self, writes=True, cfg=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = Chamber()
        self.sim = CesarSim(self.ch)
        self.cfg = dict(CFG, **(cfg or {}))
        self.events = []
        self.w = Worker("rfp", SimTransport(self.c, self.sim), writes_allowed=writes,
                        fail_streak=self.cfg["RFPULSE_FAIL_STREAK"])
        self.d = RfPulseDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append)
        self.w.start()
        self.c.wait_until(1.5)

    def ev(self, kind):
        return [e for e in self.events if e.kind == kind]

    def start(self, *a, **kw):
        box = []
        self.d.start(*a, on_done=lambda n, r, res: (box.append(r), self.c.notify()), **kw)
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def run(self, n):
        bus = SnapshotBus(self.c, (SF.RFPULSE_BLOCK,), [Source("rfpulse", self.w, self.d.source_read, ("rfp",),
                                                                 self.cfg["RFPULSE_READ_DEADLINE_MS"])],
                          self.cfg["TICK_DEADLINE_MS"])
        rows = []
        TickLoop(self.c, bus, [rows.append]).run(n)
        return rows

    def done(self):
        self.w.stop()


def test_identity_and_cmd33_written_and_verified():
    r = Rig()
    assert r.d.model == "CESAR 1310" and r.ev("identity")
    assert r.sim.auto_off_s == CFG["RFPULSE_REFP_AUTO_OFF_SEC"] == 20
    assert r.ev("auto_off")[0].reason is None
    r.done()


def test_full_read_each_tick():
    r = Rig()
    rows = r.run(4)
    for x in rows:
        assert x["rfp.q"] == "ok" and x["rfp.fwd_w.q"] == "ok" and x["rfp.out_on.q"] == "ok"
        assert x["rfp.ref_w"] == 0.0 and x["rfp.out_on"] is False
    r.done()


def test_alternate_plan_quality():
    r = Rig(cfg={"RFPULSE_READ_PLAN": "alternate"})
    rows = r.run(6)
    for x in rows:
        assert x["rfp.q"] == "ok" and x["rfp.ref_w"] is not None             # 반사는 매초
        even = x["seq"] % 2 == 0
        assert x["rfp.fwd_w.q"] == ("ok" if even else "stale")
        assert x["rfp.out_on.q"] == ("stale" if even else "ok")
    r.done()


def test_auto_switches_when_query_slow():
    r = Rig()
    r.sim.add_fault("delay", 3.0, 5.0, extra_s=0.3)                         # 질의 > 250 ms
    rows = r.run(6)
    assert r.ev("read_plan") and r.d.alternate
    assert rows[-1]["rfp.q"] == "ok"
    r.done()


def test_start_sequence_normal_and_plasma_reflection():
    r = Rig()
    assert r.sim.set_commands_seen() == [14, 33]                             # 연결: Local → Host(출력 꺼짐) → 명령 33
    assert r.start(300.0, freq_hz=20000, duty=50) is None
    assert r.sim.out and r.sim.setpoint == 300 and r.sim.freq == 20000 and r.sim.duty == 50 and r.sim.pulsing == 1
    assert r.sim.set_commands_seen()[2:] == [14, 1, 3, 8, 27, 93, 96, 2]       # Host 뒤 사전 RF OFF
    assert r.ev("config_readback")[-1].reason is None
    rows = r.run(3)
    assert rows[-1]["rfp.fwd_w"] == pytest.approx(300.0, abs=1) and rows[-1]["rfp.out_on"] is True
    assert rows[-1]["rfp.ref_w"] == pytest.approx(180.0, abs=2)               # 점화 전(가스 없음) 반사 큼
    assert rows[-1]["rfp.set_w"] == 300.0 and rows[-1]["rfp.freq_hz"] == 20000
    r.ch.gas_sccm["ar"] = 20.0
    r.c.wait_until(r.c.monotonic() + 30.0)
    assert r.run(1)[-1]["rfp.ref_w"] == pytest.approx(6.0, abs=1)            # 점화 — 반사 2 %
    r.done()


@pytest.mark.parametrize("verify,expect_reason,out_after", [(True, "RFP_CONFIG_MISMATCH", False), (False, None, True)])
def test_readback_mismatch(verify, expect_reason, out_after):
    r = Rig(cfg={"RFPULSE_VERIFY_PULSE_CONFIG": verify})
    r.sim.readback_offset = 500
    assert r.start(200.0, freq_hz=10000, duty=40) == expect_reason
    assert r.sim.out is out_after
    assert r.ev("config_readback")[-1].reason == "RFP_CONFIG_MISMATCH"
    r.done()


@pytest.mark.parametrize("cmd,csr,fix", [(3, 1, 14), (8, 2, 1)])
def test_csr_recovery(cmd, csr, fix):
    r = Rig()
    r.sim.csr_once[cmd] = csr
    assert r.start(150.0) is None and r.sim.out
    ev = r.ev("csr_recovery")
    assert len(ev) == 1 and f"CSR {csr}" in ev[0].detail and f"명령 {fix}" in ev[0].detail
    seen = r.sim.set_commands_seen()
    i = seen.index(cmd)
    assert seen[i:i + 3] == [cmd, fix, cmd]                                  # 그 명령 → 복구 → 다시
    r.done()


def test_other_csr_fails_start():
    r = Rig()
    assert r.start(150.0, duty=120) == "RFP_START_FAILED"                    # CSR 51 듀티 범위 밖
    assert not r.sim.out
    r.done()


def test_rf_off_confirmed_and_unconfirmed():
    r = Rig()
    r.start(100.0)
    box = []
    r.d.rf_off_confirmed(on_done=lambda x: (box.append(x), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box[0].status == "ok" and not r.sim.out and r.ev("off_confirmed")
    r.start(100.0)
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 10.0)
    box.clear()
    r.d.rf_off_confirmed(on_done=lambda x: (box.append(x), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    assert box[0].status != "ok" and r.ev("off_unconfirmed")[0].reason == "RFP_OFF_UNCONFIRMED"
    r.done()


def test_bench_no_writes_only_allowed_queries():
    r = Rig(writes=False)
    box = []
    r.d.start(100.0, on_done=lambda n, x, res: box.append(x))
    r.d.rf_off_confirmed(on_done=lambda x: box.append(x.reason))
    rows = r.run(3)
    assert box == ["BENCH_READ_ONLY", "BENCH_READ_ONLY"]
    assert r.sim.set_commands_seen() == [] and r.sim.auto_off_s == 0       # 명령 33 도 안 씀
    assert set(r.sim.commands_seen()) <= BENCH_QUERIES
    assert all(x["rfp.q"] == "ok" for x in rows)
    r.done()


def test_cmd33_skipped_while_rf_on_in_local():
    c = VirtualClock()
    c.register()
    sim = CesarSim()
    sim.out, sim.setpoint = True, 100                                        # 앞면(Local)에서 RF 켜 둔 상태
    events = []
    w = Worker("rfp", SimTransport(c, sim))
    RfPulseDriver(c, w, CFG, True, on_event=events.append)
    w.start()
    c.wait_until(1.0)
    assert sim.out and sim.control == 6 and sim.set_commands_seen() == []   # 제어 모드를 바꾸지 않음(RF 를 끄지 않음)
    assert [e.reason for e in events if e.kind == "auto_off"] == ["STATE_NOT_ALLOWED"]
    w.stop()


def test_short_request_deadline_does_not_switch_plan():
    """요청 마감이 먼저 끝나 생긴 시간 초과(앞 작업에 밀림)는 장비가 느린 것이 아니다 — 읽기 계획을 바꾸지 않는다."""
    from v2.common.worker import Request
    r = Rig()
    box = []
    r.w.submit(Request("read", lambda s: r.d.source_read(s, 0), deadline=r.c.monotonic() + 0.05,
                       on_done=lambda x: (box.append(x), r.c.notify())))
    r.c.wait_until(None, lambda: bool(box))
    assert box[0].status == "timeout" and not r.d.alternate and not r.ev("read_plan")
    r.done()
