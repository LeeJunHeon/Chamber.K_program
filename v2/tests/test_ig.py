"""이온 게이지 드라이버 + 시뮬레이터 시험(가상 시계). 실제 포트는 열지 않는다."""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import SimTransport
from v2.common.worker import Request, Status, Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.ig import (
    IgCommandNotAllowed, IgDriver, IgParseError, looks_like_pressure_reply, parse_pair, parse_status,
    pressure_from_pair, setpoint_from_pair, split_lines,
)
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.ig_sim import A_QUIET_S, B_QUIET_S, IgSim, fmt_pair

CFG = defaults(P.PARAMS)


# ───────── 파서 ─────────
@pytest.mark.parametrize("txt,kind,fil,expect", [
    ("9       -6", "ion", True, (9e-6, "ok")),
    ("10      -6", "ion", True, (1e-5, "ok")),
    ("0", "ion", False, (None, "off")),
    ("0", "ion", True, (None, "preparing")),
    ("-900", "conv", False, (None, "disconnected")),
    ("-999", "conv", False, (None, "unzeroed")),
    ("0 -4", "conv", False, (None, "below")),
    ("-1 -4", "conv", False, (None, "below")),                     # 10/15 실측: 고진공 영점 잡음
    ("-6 -3", "conv", False, (None, "negative")),
    ("20 -4", "conv", False, (2e-3, "ok")),
    ("x1", "conv", False, (None, "invalid")),
])
def test_pressure_parsing(txt, kind, fil, expect):
    v, st = pressure_from_pair(txt, kind, fil)
    assert st == expect[1] and (v == pytest.approx(expect[0]) if expect[0] else v is None)


def test_parse_misc():
    assert setpoint_from_pair("10 -3") == pytest.approx(1e-4)          # 가수 × 10^(지수−2)
    with pytest.raises(IgParseError):
        parse_pair("999x")
    assert split_lines(b"1\n\r\n\r2\n\r") == ["1", "2"]
    assert looks_like_pressure_reply(b"40 -7\n\r") and not looks_like_pressure_reply(b"1\n\r2\n\r")
    with pytest.raises(IgParseError):
        parse_status(["0"] * 25)
    assert fmt_pair(4.0e-6) == "40 -7" and fmt_pair(760.0) == "76 1" and fmt_pair(2e-3) == "20 -4"


# ───────── 드라이버 + 시뮬레이터 ─────────
class Rig:
    def __init__(self, writes=True, chamber=None, sim=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.ch = chamber or Chamber()
        self.sim = sim or IgSim(self.ch)
        self.events = []
        self.w = Worker("ig", SimTransport(self.c, self.sim), writes_allowed=writes, fail_streak=CFG["IG_FAIL_STREAK"])
        self.d = IgDriver(self.c, self.w, CFG, writes, on_event=self.events.append)
        self.w.start()
        self.c.wait_until(0.5)

    def bus(self):
        return SnapshotBus(self.c, (SF.IG_BLOCK,), [Source("ig", self.w, self.d.source_read, ("ig",),
                                                            CFG["IG_RESPONSE_LIMIT_MS"])], CFG["TICK_DEADLINE_MS"])

    def run(self, n, at=None):
        rows = []
        loop = TickLoop(self.c, self.bus(), [rows.append])
        hooks = at or {}

        def hook(row):
            self.d.on_tick()
            f = hooks.get(row["seq"])
            if f:
                f()
        loop.sinks.append(hook)
        loop.run(n)
        return rows

    def results(self, kind="filament_result"):
        return [e for e in self.events if e.kind == kind]

    def done(self):
        self.w.stop()


def test_identity_and_normal_reads():
    r = Rig()
    assert [e.kind for e in r.events] == ["identity_ok"] and r.sim.commands_seen() == ["F"]
    rows = r.run(5)
    assert all(x["ig.q"] == "ok" for x in rows)
    assert all(x["ig.filament"] is False and x["ig.ion_torr"] is None for x in rows)
    assert r.sim.commands_seen()[1:] == ["E"] * 5                       # tick 마다 E 1회
    r.done()


def test_filament_on_quiet_suppressed_then_range_search():
    r = Rig()
    r.ch.pressure_override = 4e-6
    rows = r.run(25, at={1: r.d.filament_on})
    q = [x["ig.q"] for x in rows]
    a_tick = 2                                                          # seq 1 행 뒤에 A → seq 2 부터 무응답
    quiet_rows = [x for x in rows if 2 <= x["seq"] <= 7]
    assert all(x["ig.q"] == "suppressed" for x in quiet_rows), q        # 5.9 s 무응답 = 억제(실패 아님)
    assert r.w.connected and not [e for e in r.events if e.kind == "parse_error"]
    after = [x for x in rows if x["seq"] >= 9]
    assert all(x["ig.q"] == "ok" for x in after), q                     # 레인지 찾기 860 ms 도 마감(900) 안
    assert after[0]["ig.filament"] and after[0]["ig.emission"]
    assert rows[-1]["ig.ion_torr"] == pytest.approx(4e-6)               # 레인지 찾기(7.3 s) 뒤 실제 값
    assert [e.ok for e in r.results()] == [True]
    assert r.d.emission_since is not None
    r.done()


def test_filament_off_and_relight_lockout():
    r = Rig()
    r.ch.pressure_override = 4e-6
    r.run(20, at={1: r.d.filament_on})
    t_b = r.c.monotonic()
    rows = r.run(14, at={0: r.d.filament_off, 1: r.d.filament_on})
    sent = [(t, d) for t, d in r.sim.received if d in (b"A", b"B")]
    (tb, _), (ta, _) = sent[-2], sent[-1]
    assert sent[-2][1] == b"B" and sent[-1][1] == b"A"
    assert ta - tb >= CFG["IG_RELIGHT_LOCKOUT_SEC"] - 1e-9             # B 뒤 5 s 안의 A 는 늦춤
    kinds = [(e.detail.split(":")[0], e.ok) for e in r.results()]
    assert kinds[-2] == ("off", False) and "다른 필라멘트 요청" in r.results()[-2].detail   # 확인 전 A 가 B 판정을 취소(v1 과 같음)
    assert kinds[-1] == ("on", True)
    r.done()


def test_filament_on_failure_reported():
    r = Rig()
    r.ch.pressure_override = 5e-2                                       # 고압 — 켜지자마자 오류 15 로 꺼짐
    r.run(30, at={1: r.d.filament_on})
    res = r.results()
    assert res and res[-1].ok is False and res[-1].reason == "IG_FILAMENT_FAILED_CMD" and "15" in res[-1].detail
    r.done()


def test_forbidden_commands_never_reach_port():
    r = Rig()
    for ch in ("C", "D", "A", "B", "Z", "x"):
        with pytest.raises(IgCommandNotAllowed):
            r.d.send_command(ch)                                        # A·B 도 이 경로로는 안 됨
    r.d.send_command("F")
    r.d.send_command("G")                                               # G·H 는 질의(게이지 A·B 압력) [IG p.54]
    r.c.wait_until(r.c.monotonic() + 1.0)
    r.run(3, at={0: r.d.filament_on})
    assert set(r.sim.commands_seen()) <= {"E", "F", "G", "A", "B"}       # A/B 경로 밖의 쓰기 0
    assert "C" not in r.sim.commands_seen() and "D" not in r.sim.commands_seen()
    r.done()


def test_bench_rejects_filament_allows_queries():
    r = Rig(writes=False)
    box = []
    r.d.filament_on(on_done=box.append)
    r.d.filament_off(on_done=box.append)
    rows = r.run(3)
    assert [b.reason for b in box] == ["BENCH_READ_ONLY", "BENCH_READ_ONLY"]
    assert "A" not in r.sim.commands_seen() and "B" not in r.sim.commands_seen()
    assert all(x["ig.q"] == "ok" for x in rows)                         # E·F 는 허용
    r.done()


def test_late_reply_window_and_limit():
    """900~1200 ms 응답은 다음 행에 late(그동안 E 를 보내지 않음 — stale), 1200 ms 를 넘기면 fail."""
    r = Rig()
    r.sim.add_fault("delay", 2.0, 2.5, extra_s=0.8)                    # seq 1(t 2.0): E 약 1.05~1.1 s
    r.sim.add_fault("delay", 5.0, 5.5, extra_s=1.0)                    # seq 4(t 5.0): E 1.25 s 이상 → 제한 초과
    rows = r.run(8)
    q = {x["seq"]: (x["ig.q"], x["ig.age_s"]) for x in rows}
    assert q[1][0] == "stale"                                           # 보낸 tick — 마감(900 ms)까지 없음
    assert q[2] == ("late", 1.0)                                        # 늦게 온 값이 다음 행에 late
    assert q[3] == ("ok", 0.0)
    assert q[4][0] == "stale" and q[5][0] == "fail" and q[6] == ("ok", 0.0)
    e_times = [round(t, 3) for t, d in r.sim.received if d == b"E"]
    assert 3.0 not in e_times and 6.0 not in e_times                    # 응답을 기다리는 동안 다음 E 를 보내지 않음
    assert r.w.connected and not [e for e in r.events if e.kind == "parse_error"]
    r.done()


def test_identity_failure_disconnects():
    sim = IgSim()
    sim.add_fault("garbage", 0.0, 100.0)
    r = Rig(sim=sim)
    assert not r.w.connected and "identity_ok" not in [e.kind for e in r.events]
    r.done()
