"""PLC 드라이버 + 시뮬레이터 통합 시험(가상 시계, SimTransport). 기록은 임시 폴더에만."""
import csv

import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop
from v2.common.transport import RecordingTransport, ReplayTransport, SimTransport, load_exchanges
from v2.common.worker import Request, Status, Worker
from v2.chk import params as P
from v2.chk import safety_rules as SR
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.plc import CORE_FIELDS, PlcDriver, PlcFastPoller, rf_power_to_dac
from v2.chk.sim.plc_sim import A, PlcSim

CFG = defaults(P.PARAMS)


class Rig:
    def __init__(self, writes=True, sim=None, transport=None, cfg=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.sim = sim or PlcSim()
        self.events, self.boxes = [], []
        self.cfg = dict(CFG, **(cfg or {}))
        tr = transport(self.c, self.sim) if transport else SimTransport(self.c, self.sim)
        self.tr = tr
        self.w = Worker("plc", tr, writes_allowed=writes, fail_streak=self.cfg["PLC_FAIL_STREAK"])
        self.d = PlcDriver(self.c, self.w, self.cfg, writes, on_event=self.events.append,
                           blackbox_sink=lambda n, rows: self.boxes.append((n, rows)))
        self.w.start()
        self.c.wait_until(0.5)

    def kinds(self):
        return [e.kind for e in self.events]

    def read(self):
        box = []
        self.w.submit(Request("read", lambda s: self.d.source_read(s, 0), deadline=self.c.monotonic() + 0.4,
                              on_done=lambda r: (box.append(r), self.c.notify())))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def second(self, n=1):
        """1초씩: 읽기 → tick 훅(하트비트) → 다음 정각까지."""
        for _ in range(n):
            self.read()
            self.d.on_tick(None)
            self.c.wait_until(self.c.monotonic() + 1.0 - (self.c.monotonic() % 1.0))

    def done(self):
        self.w.stop()


# ───────── 읽기·스케일·예산 ─────────
def test_decode_scaling_and_budget():
    sim = PlcSim()
    sim.forced_regs = {A["rf_fwd_adc"]: 4000, A["rf_ref_adc"]: 2000, A["heater_pv"]: 0x1771}   # 600.1 °C
    sim.tc2_fail = True
    r = Rig(sim=sim)
    res = r.read()
    v = res.value
    assert res.status == Status.OK
    assert v["rf.fwd_w"] == 594.5 and v["rf.ref_w"] == 100.0         # 순방향 4000 = 594.5 W, 반사 4000 = 200 W
    assert v["heater.pv2"] is None                                    # -1 → 센서 이상
    assert v["heater.pv"] == 600.1
    assert v["plc.mv"] is True and v["plc.mv_interlock"] is True and v["plc.atm"] is False
    # 한 tick 읽기 시간: 3 트랜잭션, 194 바이트 × 10 / 115200 + 3 × (5 + 2) ms ≈ 37.8 ms
    took_ms = (res.finished - res.started) * 1000
    assert 30.0 < took_ms < 50.0 and took_ms < CFG["PLC_READ_DEADLINE_MS"]
    r.done()


def test_rf_scale_from_adc():
    r = Rig()
    r.d.rf_set_power(300.0)
    r.c.wait_until(5.0)
    v = r.read().value
    dac = rf_power_to_dac(300.0, CFG)
    assert dac == round(6.79 + 300 / 600 * 4000 * 1.0395) and r.sim.reg("rf_dac") == dac and r.sim.coil("rf_dac_enable")
    assert v["rf.dac"] == dac and v["rf.dac_enable"] is True and v["rf.set_w"] == 300.0
    gen_w = dac / 4000 * 600.0                                        # 시뮬레이터 발생기
    assert v["rf.fwd_w"] == pytest.approx(gen_w, abs=0.5)             # ADC 4000 = 594.5 W 로 되돌림
    assert v["rf.ref_w"] == pytest.approx(gen_w * 0.01, abs=0.1)      # ADC 4000 = 200 W
    r.d.rf_off()
    r.c.wait_until(8.0)
    assert r.sim.reg("rf_dac") == 0 and not r.sim.coil("rf_dac_enable")
    r.done()


# ───────── 연결 초기화(S13)·마커 ─────────
def test_connect_pushes_heater_limits_and_marker():
    r = Rig()
    assert "heater_config_ok" in r.kinds() and "marker_written" in r.kinds()
    assert r.sim.reg("heater_sv_limit") == 7000 and r.sim.reg("heater_ot_limit") == 7500
    assert r.sim.reg("heater_ot2_limit") == 11500 and r.sim.reg("heater_mv_limit") == 1200
    assert r.sim.reg("heater_ramp_rate") == 2 and r.sim.reg("heater_slow_rate") == 10
    assert r.sim.reg("heater_holdback") == 100 and r.sim.reg("heater_slow_zone") == 100
    assert r.read().value["plc.marker_ok"] is True
    r.done()


def test_heater_limit_mismatch_reason():
    sim = PlcSim()
    sim.forced_regs = {A["heater_holdback"]: 20}                      # 래더가 다른 값을 강제
    r = Rig(sim=sim)
    ev = [e for e in r.events if e.kind == "heater_config_mismatch"]
    assert ev and ev[0].reason == "VERIFY_MISMATCH" and "HEATER_HOLDBACK_C" in ev[0].detail
    assert r.w.connected                                              # 불일치여도 링크는 유지
    r.done()


def test_restart_detected_and_recovered():
    r = Rig()
    r.second(3)                                                       # 블랙박스에 폴링이 쌓임
    r.sim.restart()
    r.second(3)
    ev = [e for e in r.events if e.kind == "plc_restarted"]
    assert len(ev) == 1 and ev[0].reason == "PLC_RESTARTED"
    assert r.boxes and r.boxes[0][0] == "plc_plc_restarted" and len(r.boxes[0][1]) >= 3
    assert r.sim.reg("heater_sv_limit") == 7000                       # 한계값 다시 씀
    assert r.read().value["plc.marker_ok"] is True                    # 새 마커
    r.done()


# ───────── 하트비트·링크 다운 ─────────
def test_heartbeat_each_tick_and_plc_cuts_heater_when_link_down():
    r = Rig()
    r.d.heater_set_sv(100.0)
    r.d.heater_run(True)
    r.second(5)
    hb = [f for _, f in r.sim.frames if f[1] == 6 and f[2:4] == bytes([0, A["heater_wd"]])]
    assert len(hb) == 5                                               # tick 마다 1번
    vals = [int.from_bytes(f[4:6], "big") for f in hb]
    assert vals == sorted(vals) and len(set(vals)) == 5
    assert r.sim.coil("heater_run")
    t_last = r.c.monotonic()
    r.sim.add_fault("disconnect", t_last, t_last + 60.0)
    n_before = len(hb)
    r.second(12)
    hb2 = [f for _, f in r.sim.frames if f[1] == 6 and f[2:4] == bytes([0, A["heater_wd"]])]
    assert len(hb2) == n_before                                       # 링크 다운이면 쓰지 않음
    assert "link_down" in r.kinds()
    r.sim.advance_to(r.c.monotonic())
    assert not r.sim.coil("heater_run") and r.sim.coil("heater_wd_err")    # PLC 가 10 s 뒤 히터 OFF
    r.done()


def test_comm_loss_blackbox_once():
    r = Rig()
    r.second(4)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 100.0)
    r.second(15)
    assert [b[0] for b in r.boxes] == ["plc_comm_loss"]               # PLC_COMM_LOSS_ABORT_SEC 뒤 1회
    assert len(r.boxes[0][1]) >= 4
    r.done()


def test_pending_heater_off_applied_on_reconnect():
    r = Rig()
    r.d.heater_set_sv(80.0)
    r.d.heater_run(True)
    r.second(2)
    t = r.c.monotonic()
    r.sim.add_fault("disconnect", t, t + 4.0)
    r.second(3)
    r.d.heater_run(False)                                             # 링크 없음 → 보류
    assert "heater_off_pending" in r.kinds()
    r.second(8)                                                       # 재연결
    assert "heater_off_applied" in r.kinds() and not r.sim.coil("heater_run")
    r.done()


def test_residual_run_reported():
    sim = PlcSim()
    sim.coils[A["heater_run"]] = True
    r = Rig(sim=sim)
    r.read()
    ev = [e for e in r.events if e.kind == "heater_residual_run"]
    assert ev and ev[0].reason == "HEATER_RESIDUAL_RUN"
    r.done()


# ───────── bench: 어떤 쓰기도 나가지 않음 ─────────
def test_bench_sends_no_writes():
    r = Rig(writes=False)
    r.second(3)
    r.d.heater_run(True)
    r.d.heater_set_sv(200.0)
    r.d.set_button("vent", True)
    r.d.rf_set_power(100.0)
    r.d.outputs_off(True)
    r.d.heater_reset()
    r.second(2)
    assert r.sim.write_funcs_seen() == []                             # 하트비트·한계값·마커 포함
    v = r.read().value
    assert v["plc.marker_ok"] is None and v["plc.mv"] is True
    assert "heater_config_ok" not in r.kinds() and "marker_written" not in r.kinds()
    r.done()


# ───────── ALL STOP 출력 ─────────
@pytest.mark.parametrize("keep", [True, False])
def test_outputs_off(keep):
    sim = PlcSim()
    r = Rig(sim=sim)
    for n in ("rotary", "rv", "fv", "turbo", "mv", "vent", "door_up", "ar_valve", "o2_valve", "ms", "s1", "s2",
              "door_down", "buzz_stop", "ion_remote_on"):
        sim.coils[A[n]] = True
    sim.coils[12] = sim.coils[13] = True                              # 매핑 안 된 래더 비트
    r.d.outputs_off(keep_pumps=keep)
    r.c.wait_until(r.c.monotonic() + 1.0)
    pumps = [sim.coil(n) for n in SR.ALL_STOP_KEEP_COILS]
    assert pumps == [keep] * 4
    assert not any(sim.coil(n) for n in ("mv", "vent", "door_up", "door_down", "ar_valve", "o2_valve", "ms", "s1", "s2",
                                          "ion_remote_on"))
    assert sim.coils[12] and sim.coils[13]                            # 건드리지 않음
    assert 15 in sim.write_funcs_seen()
    r.done()


# ───────── 안전 규칙이 쓰는 PLC 값 ─────────
def test_driver_provides_every_plc_value_used_by_safety_rules():
    r = Rig()
    v = r.read().value
    blocks = {"plc", "heater", "rf"}
    fields = {f"{b.name}.{f.name}" for b in (SF.PLC_BLOCK, SF.HEATER_BLOCK, SF.RF_BLOCK) for f in b.fields}
    assert fields == set(v) | CORE_FIELDS                             # 블록의 모든 필드
    used = {x for rule in SR.RULES for x in rule.values if x.split(".")[0] in blocks}
    missing = {x for x in used if x not in v and x not in CORE_FIELDS and not x.endswith((".q", ".age_s"))}
    assert missing == set()
    r.done()


# ───────── 묶기 + 빠른 폴링 ─────────
def test_bus_and_fast_poller():
    r = Rig()
    bus = SnapshotBus(r.c, (SF.PLC_BLOCK, SF.HEATER_BLOCK, SF.RF_BLOCK),
                      [Source("plc", r.w, r.d.source_read, ("plc", "heater", "rf"), CFG["PLC_READ_DEADLINE_MS"])],
                      CFG["TICK_DEADLINE_MS"])
    fp = PlcFastPoller(r.c, r.d, CFG["PLC_FAST_POLL_MS"])
    polls = []
    r.d.fast_listeners.append(polls.append)
    fp.start()
    rows = []
    loop = TickLoop(r.c, bus, [rows.append])
    loop.run(4)
    fp.stop()
    assert all(x["plc.q"] == "ok" and x["heater.q"] == "ok" for x in rows)
    assert fp.sent >= 9                                               # 1초에 3번(정각 제외)
    fracs = {round(p.t_wall % 1.0, 2) for p in polls}
    assert fracs == {0.0, 0.25, 0.5, 0.75}                            # 정각(묶기) + 250 ms 내부 폴링
    assert len(polls) >= 12
    r.done()


# ───────── 녹화·재생 ─────────
def test_record_and_replay_reads(tmp_path):
    rec_box = {}

    def rec_transport(c, sim):
        rec_box["t"] = RecordingTransport(SimTransport(c, sim))
        return rec_box["t"]

    r = Rig(writes=False, transport=rec_transport)
    v1 = r.read().value
    r.done()
    path = tmp_path / "plc_rec.jsonl"
    rec_box["t"].save(path)
    ex = load_exchanges(path)
    assert len(ex) == 4 and ex[0].rx and ex[1].t_s >= 0                # 프로브 + 블록 3개
    r2 = Rig(writes=False, transport=lambda c, sim: ReplayTransport(c, ex))
    v2 = r2.read().value
    assert v2 == v1
    r2.done()


def test_recorder_writes_blackbox(tmp_path):
    rec = Recorder(tmp_path, ["ts"])
    rec.start()
    rec.put_blackbox("plc_comm_loss", [{"t_wall": 1.0, "elapsed_ms": 0, "heater.pv": 25.0},
                                       {"t_wall": 1.25, "elapsed_ms": 250, "heater.pv": None}])
    assert rec.flush()
    rec.stop()
    (f,) = rec.blackbox_files
    with open(f, encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["t_wall", "elapsed_ms", "heater.pv"] and rows[2] == ["1.25", "250", ""]


def test_rf_enable_state_follows_first_poll():
    """v1 정상 종료 흔적(M00200 켜짐)이 있으면 드라이버 내부 상태도 켜짐으로 — 첫 RF 쓰기에서 enable 을 다시 쓰지 않는다."""
    sim = PlcSim()
    sim.coils[A["rf_dac_enable"]] = True
    r = Rig(sim=sim)
    r.read()
    assert r.d._rf_enabled is True
    r.done()
    r = Rig()
    r.read()
    assert r.d._rf_enabled is False
    r.done()
