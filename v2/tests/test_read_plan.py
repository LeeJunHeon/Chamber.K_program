"""읽기 계획 되돌아오기(5-20 ①) — MFC·RF Pulse 의 auto 가 한 번 느렸다고 영영 번갈아 읽지 않게.

11/23 bench 사실(시뮬레이터 시간을 맞춤): MFC 응답 p50 34 ms · p99 60 ms, 명령 간격 150 ms 라 셋 다 읽는 한 번은 보통 약 350 ms.
bench 앱은 시작 15초 뒤 '한 번 읽기 664 ms' 한 번으로 끝까지(11.5분) 번갈아 읽었다(밸브 위치 행의 절반이 stale).
가짜 포트(시뮬레이터)와 가상 시계로만 — 실제 포트는 열지 않는다. tick 은 가상 시계의 정수 초에 선다.
"""
import pytest

from v2.chk.drivers.readplan import ReadPlan
from v2.chk.sim.mfc_sim import MfcSim
from v2.tests.test_mfc import Rig as MfcRig
from v2.tests.test_rfpulse import Rig as RfpRig


def _plan_events(r):
    return [e for e in r.events if e.kind == "read_plan"]


class _Track:
    """행마다 (tick 시각, 계획, 판정 시간)을 모은다 — 계획을 바꾼 시각을 행으로 본다."""

    def __init__(self, r, dev):
        self.r, self.dev, self.rows = r, dev, []

    def __call__(self, row):
        self.rows.append((row["seq"], row[f"{self.dev}.read_plan"], self.r.d.read_plan.last_ms, row))


def _run(r, n, dev, block, source):
    from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
    tr = _Track(r, dev)
    bus = SnapshotBus(r.c, (block,), [source], r.cfg["TICK_DEADLINE_MS"])
    TickLoop(r.c, bus, [tr]).run(n)
    return tr


def _mfc_run(r, n):
    from v2.chk import snapshot_fields as SF
    from v2.common.snapshot_bus import Source
    return _run(r, n, "mfc", SF.MFC_BLOCK, Source("mfc", r.w, r.d.source_read, ("mfc",), r.cfg["MFC_READ_DEADLINE_MS"]))


def _rfp_run(r, n):
    from v2.chk import snapshot_fields as SF
    from v2.common.snapshot_bus import Source
    return _run(r, n, "rfp", SF.RFPULSE_BLOCK,
                Source("rfpulse", r.w, r.d.source_read, ("rfp",), r.cfg["RFPULSE_READ_DEADLINE_MS"]))


def _watch(r):
    r.switch_at = []
    orig = r.d.read_plan._switch

    def sw(now, alternate):
        r.switch_at.append((round(now, 2), "alternate" if alternate else "full"))
        return orig(now, alternate)
    r.d.read_plan._switch = sw
    return r


def _next_tick(r):
    import math
    return math.ceil(r.c.monotonic() + 1e-9)


# ───────── 판정기 단독 ─────────
def test_planner_defaults_need_repeated_slow_and_stricter_return():
    from v2.common.paramdef import defaults
    from v2.chk import params as P
    cfg = defaults(P.PARAMS)
    rp = ReadPlan(cfg, "MFC")
    assert rp.mode == "auto" and rp.plan == "full"
    assert (rp.slow_ms(900), rp.fast_ms(900)) == (540.0, 405.0)          # 느린 읽기 > 540 ms, 돌아오기 ≤ 405 ms(더 엄격)
    t = 0.0
    for i in range(30):                                                  # 350 ms 사이에 664 ms 한 번 — 바꾸지 않는다
        assert rp.note(t + i, 664.0 if i == 15 else 350.0, 900) is None
    for i in range(4):                                                   # 느린 읽기 넷(최근 10번 중) — 아직
        assert rp.note(40.0 + i, 700.0, 900) is None
    assert rp.note(44.0, 700.0, 900) == "alternate" and rp.switch_slow == 5   # 다섯 번째에 번갈아로
    # 번갈아 중: 420 ms(느리진 않지만 돌아오는 기준 405 ms 를 넘음)는 빠른 읽기가 아니다 — 돌아오지 않는다
    assert all(rp.note(45.0 + i, 420.0, 900) is None for i in range(120))
    assert rp.plan == "alternate"
    for i in range(60):
        assert rp.note(200.0 + i, 354.0, 900) is None                     # 60초를 채우기 전
    assert rp.note(260.0, 354.0, 900) == "full"
    rp2 = ReadPlan(dict(cfg, MFC_READ_PLAN="full"), "MFC")               # 고정 계획은 판정하지 않는다
    assert all(rp2.note(float(i), 900.0, 900) is None for i in range(30)) and rp2.plan == "full"
    rp3 = ReadPlan(dict(cfg, MFC_READ_PLAN="alternate"), "MFC")
    assert all(rp3.note(float(i), 100.0, 900) is None for i in range(200)) and rp3.plan == "alternate"


def test_planner_not_counted_reads_do_not_move_plan():
    from v2.common.paramdef import defaults
    from v2.chk import params as P
    rp = ReadPlan(defaults(P.PARAMS), "MFC")
    assert all(rp.note(float(i), None, 900) is None for i in range(100)) and rp.plan == "full" and not rp.recent


# ───────── MFC — bench 시간 ─────────
def test_mfc_bench_like_reads_stay_full_with_one_664ms_read():
    """bench: 보통 약 350 ms, 15초쯤 664 ms 한 번 — 끝까지 매초 읽기(밸브 위치가 매초 ok)."""
    sim = MfcSim()
    r = _watch(MfcRig(sim=sim))
    k = _next_tick(r) + 15                                              # 15번째 tick 의 질의마다 +175 ms(제한 300 ms 안)
    sim.add_fault("delay", k - 0.01, k + 0.8, extra_s=0.175)
    tr = _mfc_run(r, 120)
    ms = {seq: m for seq, plan, m, row in tr.rows}
    normal = [m for seq, plan, m, row in tr.rows if m is not None and m < 600]
    assert 330 <= min(normal) and max(normal) <= 380                    # bench 와 같은 약 350 ms
    assert any(600 < m < 750 for m in ms.values())                      # 그 한 번(약 664 ms)
    assert _plan_events(r) == [] and r.switch_at == []
    assert all(plan == "full" for seq, plan, m, row in tr.rows)
    assert all(row["mfc.valve_pct.q"] == "ok" and row["mfc.valve_read"] == "read" for *_, row in tr.rows)
    r.done()


def test_mfc_sustained_slow_switches_then_returns_within_two_minutes():
    sim = MfcSim()
    r = _watch(MfcRig(sim=sim))
    t0 = _next_tick(r)
    sim.add_fault("delay", t0 + 20 - 0.01, t0 + 40 - 0.01, extra_s=0.24)   # 20초 동안 질의마다 +240 ms(한 번 읽기 약 850 ms)
    tr = _mfc_run(r, 200)
    ev = _plan_events(r)
    assert [p for t, p in r.switch_at] == ["alternate", "full"] and len(ev) == 2
    (t_alt, _), (t_full, _) = r.switch_at
    assert t0 + 20 + 4 <= t_alt <= t0 + 30                             # 느린 읽기 다섯 번째(최근 10번 중)에
    assert t_full - t_alt >= 60 and t_full <= t0 + 40 + 120             # 다시 빨라진 뒤 2분 안에 돌아옴
    assert "번갈아" in ev[0].detail and "10번 중 5번" in ev[0].detail and "매초" in ev[1].detail
    alt_rows = [row for seq, plan, m, row in tr.rows if plan == "alternate"]
    assert alt_rows and all(row["mfc.q"] == "ok" for *_, row in tr.rows)
    for row in alt_rows:                                                 # 번갈아: 유량 짝수 초·밸브 위치 홀수 초
        if row["seq"] % 2 == 0:
            assert row["mfc.ar_sccm.q"] == "ok" and row["mfc.valve_read"] == "alternate"
        else:
            assert row["mfc.ar_sccm.q"] == "stale" and row["mfc.valve_read"] == "read"
    assert tr.rows[-1][1] == "full" and tr.rows[-1][3]["mfc.valve_pct.q"] == "ok"
    r.done()


def test_mfc_plan_changes_at_most_once_a_minute():
    """돌아오는 시간을 하한(10 s)으로 줄여도 바꾸는 사이는 60 s 이상(MFC_READ_PLAN_HOLD_SEC 하한 60)."""
    sim = MfcSim()
    r = _watch(MfcRig(sim=sim, cfg={"MFC_READ_PLAN_RETURN_SEC": 10}))
    t0 = _next_tick(r)
    sim.add_fault("delay", t0 + 10 - 0.01, t0 + 20 - 0.01, extra_s=0.24)    # 느림 → 빠름(80 s) → 다시 느림(끝까지)
    sim.add_fault("delay", t0 + 100 - 0.01, t0 + 1000, extra_s=0.24)
    _mfc_run(r, 220)
    ts = [t for t, p in r.switch_at]
    assert [p for t, p in r.switch_at] == ["alternate", "full", "alternate"]
    assert all(b - a >= 60 - 1e-6 for a, b in zip(ts, ts[1:]))
    assert ts[1] >= ts[0] + 60 and ts[2] >= ts[1] + 60                    # 둘 다 HOLD 에 걸려 늦게 바뀜
    r.done()


def test_mfc_r6_no_reply_reads_are_not_counted():
    """R6 무응답(응답 제한 300 ms 를 다 기다림)으로 길어진 읽기(약 600 ms > 540 ms)는 세지 않는다."""
    sim = MfcSim()
    r = _watch(MfcRig(sim=sim))
    sim.canned["R6"] = [None] * 60
    tr = _mfc_run(r, 60)
    assert r.switch_at == [] and _plan_events(r) == []
    assert all(row["mfc.valve_read"] == "no_reply" and row["mfc.valve_pct.q"] == "stale" for *_, row in tr.rows)
    assert all(row["mfc.q"] == "ok" and row["mfc.ar_sccm.q"] == "ok" for *_, row in tr.rows)
    r.done()


def test_mfc_fixed_plans_never_switch():
    sim = MfcSim()
    r = _watch(MfcRig(sim=sim, cfg={"MFC_READ_PLAN": "full"}))
    t0 = _next_tick(r)
    sim.add_fault("delay", t0, t0 + 1000, extra_s=0.24)
    tr = _mfc_run(r, 30)
    assert r.switch_at == [] and all(plan == "full" for seq, plan, m, row in tr.rows)
    r.done()
    r = _watch(MfcRig(cfg={"MFC_READ_PLAN": "alternate"}))
    tr = _mfc_run(r, 80)
    assert r.switch_at == [] and all(plan == "alternate" for seq, plan, m, row in tr.rows)
    r.done()


def test_mfc_yielded_valve_read_is_reported_as_yielded():
    """명령을 처리하느라 R6 를 건너뛴 초는 valve_read = yielded — 화면이 '번갈아 읽는 중' 과 구분한다."""
    r = MfcRig()
    r.d._ops, r.d._ops_until = 1, r.c.monotonic() + 3.0                 # 설정 명령 진행 중(가정)
    tr = _mfc_run(r, 6)
    reads = [row["mfc.valve_read"] for *_, row in tr.rows]
    assert reads[0] == "yielded" and reads[-1] == "read" and all(p == "full" for _, p, _, _ in tr.rows)
    r.done()


# ───────── RF Pulse ─────────
def test_rfpulse_one_query_timeout_does_not_switch():
    r = _watch(RfpRig())
    k = _next_tick(r) + 15
    r.sim.add_fault("delay", k - 0.01, k + 0.05, extra_s=0.3)            # 15번째 tick 의 첫 질의만 > 250 ms(무응답)
    tr = _rfp_run(r, 120)
    assert r.switch_at == [] and _plan_events(r) == []
    assert all(plan == "full" for seq, plan, m, row in tr.rows)
    assert sum(1 for *_, row in tr.rows if row["rfp.q"] != "ok") == 1
    r.done()


def test_rfpulse_sustained_slow_switches_then_returns():
    r = _watch(RfpRig())
    t0 = _next_tick(r)
    r.sim.add_fault("delay", t0 + 20 - 0.01, t0 + 40 - 0.01, extra_s=0.15)   # 질의마다 +150 ms(제한 250 ms 안) — 한 번 약 660 ms
    tr = _rfp_run(r, 200)
    ev = _plan_events(r)
    assert [p for t, p in r.switch_at] == ["alternate", "full"] and len(ev) == 2
    (t_alt, _), (t_full, _) = r.switch_at
    assert t0 + 20 + 2 <= t_alt <= t0 + 30 and "10번 중 3번" in ev[0].detail
    assert t_full - t_alt >= 60 and t_full <= t0 + 40 + 120
    assert all(row["rfp.q"] == "ok" for *_, row in tr.rows)
    alt = [row for seq, plan, m, row in tr.rows if plan == "alternate"]
    assert alt and all(row["rfp.fwd_w.q"] == ("ok" if row["seq"] % 2 == 0 else "stale") for row in alt)
    r.done()


def test_rfpulse_repeated_query_timeouts_switch():
    """질의 응답 제한을 다 쓴 읽기가 이어지면(최근 10번 중 3번) 번갈아로 — 한 번은 아니다."""
    r = _watch(RfpRig())
    t0 = _next_tick(r)
    for k in (t0 + 10, t0 + 13, t0 + 16):                                # 연속 실패(재연결) 아닌 띄엄띄엄 무응답 셋
        r.sim.add_fault("delay", k - 0.01, k + 0.05, extra_s=0.3)
    _rfp_run(r, 25)
    assert [p for t, p in r.switch_at] == ["alternate"] and t0 + 16 <= r.switch_at[0][0] < t0 + 17
    r.done()
