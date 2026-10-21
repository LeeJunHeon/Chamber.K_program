"""ProcessEngine — 실제 드라이버 5개 + 시뮬레이터 + HeaterEngine + SafetyEngine 시험대(가상 시계).

전체 공정(가스 → 히터 → 유지 → 베이스 대기 → 파워 → 압력 → 셔터 → 메인 → 정지 순서)을 끝까지, CSV 리스트·딜레이,
STOP·ALL STOP·안전 중단, 시작 거부(결정 5-2-4)·적재 규칙(v1 B2·B5).
"""
import pytest

from v2.common.clock import VirtualClock
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig

FULL_ORDER = ["LOADED", "PRE_CLEAN", "GAS_PRE", "HEATER_RAMP", "HEATER_HOLD_WAIT", "BASE_WAIT", "GAS_INTRO",
              "GAS_STABILIZE", "GUN_SHUTTER", "POWER_WAIT", "PRESSURE_SP3", "PRESSURE_SP1", "SHUTTER_DELAY", "MAIN",
              "STOPPING", "DONE", "IDLE"]
FULL_ROW = {"process_name": "FULL", "ar": True, "ar_flow": 20.0, "base_pressure": 1e-5, "working_pressure": 5.0,
            "process_time": 1.0, "shutter_delay": 1.0, "use_dc": True, "dc_power": 100.0, "use_heater": True,
            "heater_temp": 60.0, "heater_ramp": 30.0, "gun1": True, "g1_target": "CeO2"}
FAST = {"GAS_STABILIZE_SEC": 5.0}
SHORT = {"process_name": "S", "ar": True, "ar_flow": 20.0, "working_pressure": 5.0, "process_time": 0.1,
         "shutter_delay": 0.1, "use_dc": True, "dc_power": 100.0, "gun1": True}


class R:
    def __init__(self, tmp_path, cfg=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.rig = ChkRig(self.c, tmp_path, cfg=cfg)
        self.plc = self.rig.sims["plc"]
        for k in ("mv", "turbo", "rotary", "fv"):
            self.plc.coils[A[k]] = True
        self.at = {}
        self.when_ = []
        self.results = []
        self.rig.loop.tick_hooks.append(self._hook)

    def _hook(self, t):
        for fn in self.at.pop(t.seq, []):
            self.results.append(fn())
        for w in list(self.when_):
            if w[0]():
                self.when_.remove(w)
                self.results.append(w[1]())

    def when(self, pred, fn):
        """pred 가 처음 참인 tick 에 fn."""
        self.when_.append((pred, fn))

    def on(self, seq, fn):
        self.at.setdefault(seq, []).append(fn)

    def cmd(self, kind, args=None):
        return lambda: self.rig.core.command(kind, args or {})

    def run(self, n):
        assert self.rig.start()
        try:
            self.rig.loop.run(n)
        finally:
            self.rig.stop()

    def ev(self, kind):
        return [e for e in self.rig.events if type(e).__name__ == "ProcEvent" and e.kind == kind]

    def states(self):
        return [(e.data["to"], e.data["t"]) for e in self.ev("state")]


@pytest.mark.slow
def test_full_process_sim(tmp_path):
    r = R(tmp_path)
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "full", "rows": [FULL_ROW]}))
    r.on(3, r.cmd("RECIPE_START"))
    r.run(1250)
    assert all(x.ok for x in r.results), r.results
    st = r.states()
    assert [s for s, _ in st] == FULL_ORDER
    t = dict(st)
    assert t["SHUTTER_DELAY"] - t["PRESSURE_SP1"] >= 6.0                # 압력 안정 6 s 이상
    assert t["MAIN"] - t["SHUTTER_DELAY"] == pytest.approx(60, abs=1.1)
    assert t["STOPPING"] - t["MAIN"] == pytest.approx(60 + 0.8, abs=1.1)  # Main 셔터 0.8 s + 60 s
    # SP4_SET(S4 쓰기·R4 되읽기 — 5-4 ④ SP4_FROM_CONFIG) + SP4 ON + 안정화 60 s
    assert t["GUN_SHUTTER"] - t["GAS_STABILIZE"] == pytest.approx(62, abs=1.1)
    ends = r.ev("run_end")
    assert len(ends) == 1 and ends[0].data["status"] == "done"
    sims, rig = r.rig.sims, r.rig
    assert sims["dc"].out is False and not any(sims["mfc"].on)
    for k in ("ms", "s1", "s2", "ar_valve", "o2_valve"):
        assert r.plc.coils[A[k]] is False, k
    assert r.plc.coils[A["heater_run"]] is False and rig.process.state == "IDLE" and not rig.process.rows
    assert sims["ig"].filament is False
    starts = r.ev("run_start")
    assert starts[0].detail == "CSV 1/1 - FULL" and starts[0].data["params"]["heater_temp"] == 60.0
    steps = [s["action"] for s in r.ev("steps")[0].data["steps"]]
    assert steps.count("IG_BASE_WAIT") == 1 and steps.count("HEATER_WAIT") == 1 and steps[-1] == "DELAY"
    # 진행 상태 규칙이 정상 공정에서 오탐하지 않는다
    assert not [e for e in rig.safety.engine.events if e.kind in ("fired", "abort", "also") and e.rule != "S44"]


def test_csv_list_with_delay_then_done(tmp_path):
    r = R(tmp_path, FAST)
    rows = [SHORT, {"process_name": "delay 5s"}, dict(SHORT, process_name="T", dc_power=80.0)]
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "two", "rows": rows}))
    r.on(3, r.cmd("RECIPE_START"))
    r.run(260)
    names = [e.detail for e in r.ev("run_start")]
    assert names == ["CSV 1/3 - S", "CSV 3/3 - T"]
    assert [e.data["status"] for e in r.ev("run_end")] == ["done", "done"]
    st = r.states()
    seq = [s for s, _ in st]
    i = seq.index("CSV_BETWEEN")
    assert seq[i:i + 3] == ["CSV_BETWEEN", "CSV_DELAY", "PRE_CLEAN"]
    t_delay = [t for s, t in st if s == "CSV_DELAY"][0]
    t_next = [t for s, t in st if s == "PRE_CLEAN"][1]
    assert t_next - t_delay == pytest.approx(5, abs=1.01)
    assert seq[-2:] == ["DONE", "IDLE"] and r.ev("list_done") and not r.rig.process.rows


def test_stop_during_main_and_during_delay(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"row": dict(SHORT, process_time=2.0)}))
    r.when(lambda: r.rig.process.state == "MAIN", r.cmd("PROCESS_STOP"))
    r.run(120)
    ends = r.ev("run_end")
    assert len(ends) == 1 and ends[0].data["status"] == "aborted" and ends[0].data["reason"] == "USER_STOP"
    seq = [s for s, _ in r.states()]
    assert "MAIN" in seq and seq[-3:] == ["STOPPING", "ABORTED", "IDLE"]
    assert r.rig.sims["dc"].out is False
    seqs = r.rig.safety.engine.seq_results
    assert "stop" in seqs and all(s in ("ok", "not_needed", "sent") for _, _, s in seqs["stop"])


def test_stop_and_all_stop_during_delay_cancel_list(tmp_path):
    for kind in ("PROCESS_STOP", "ALL_STOP"):
        r = R(tmp_path / kind, FAST)
        rows = [SHORT, {"process_name": "delay 60s"}, SHORT]
        r.on(3, r.cmd("RECIPE_LOAD", {"name": "d", "rows": rows}))
        r.on(3, r.cmd("RECIPE_START"))
        holder = {}

        def act(r=r, kind=kind):
            holder["state"] = r.rig.process.state
            return r.rig.core.command(kind)
        r.when(lambda r=r: r.rig.process.state == "CSV_DELAY", act)
        r.run(150)
        assert holder["state"] == "CSV_DELAY"
        assert len(r.ev("run_start")) == 1 and [e.data["status"] for e in r.ev("run_end")] == ["done"]
        assert r.ev("list_cancel") and r.rig.process.state == "IDLE" and not r.rig.process.rows
        if kind == "PROCESS_STOP":
            assert "stop" not in r.rig.safety.engine.seq_results          # 딜레이 중에는 정지 순서 없음
        else:
            assert "all_stop" in r.rig.safety.engine.seq_results          # ALL STOP 은 장비를 직접 끈다


def test_safety_abort_ends_run_with_reason(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"row": dict(SHORT, process_time=3.0)}))

    def low():
        r.rig.chamber.low_current = True                                 # 전류 10 % → S31B(저전류 10 s)
    r.on(60, low)
    r.run(120)
    ends = r.ev("run_end")
    assert len(ends) == 1 and ends[0].data["status"] == "error"
    assert ends[0].data["reason"] in ("DC_LOW_CURRENT", "DC_POWER_DEVIATION")
    assert r.rig.process.state == "IDLE" and r.rig.sims["dc"].out is False


def test_start_rejections_and_load_rules(tmp_path):
    r = R(tmp_path, dict(FAST, HEATER_ENABLED=False))
    heat = dict(SHORT, use_heater=True, heater_temp=50.0)
    seq = [("RECIPE_LOAD", {"name": "h", "rows": [heat]}, "DEVICE_DISABLED"),
           ("RECIPE_START", {}, "RECIPE_NOT_LOADED"),
           ("RECIPE_LOAD", {"name": "x", "rows": [SHORT]}, None),
           ("PROCESS_START", {"row": SHORT}, "RECIPE_LOADED"),
           ("RECIPE_LOAD", {"name": "bad", "rows": [dict(SHORT, working_pressure="abc")]}, "RECIPE_INVALID"),
           ("RECIPE_START", {}, "RECIPE_NOT_LOADED"),                     # 실패한 적재는 이전 적재도 해제(v1 B2)
           ("RECIPE_CLEAR", {}, None),                                    # 적재 없음 → 성공(v1 B5)
           ("PROCESS_STOP", {}, None),                                    # 공정 없음 → 성공(v1)
           ("RECIPE_LOAD", {"name": "e", "rows": []}, "RECIPE_INVALID")]
    for i, (k, a, _) in enumerate(seq):
        r.on(3 + i, r.cmd(k, a))

    def mv_closed():
        r.plc.coils[A["mv"]] = False
    r.on(20, mv_closed)
    r.on(22, r.cmd("PROCESS_START", {"row": SHORT}))

    def dc_gone():
        r.plc.coils[A["mv"]] = True
        r.rig.sims["dc"].add_fault("disconnect", r.c.monotonic(), 1e9)
    r.on(24, dc_gone)
    r.on(40, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(42)
    got = [x.reason for x in r.results if x is not None]
    assert got == [e for _, _, e in seq] + ["MV_NOT_OPEN", "LINK_DOWN"], got
    assert not r.ev("run_start")


def test_mfc_scale_mismatch_rejects_start(tmp_path):
    """연결 때 MFC 압력 FS 가 설정과 다르면(R33 E06 = 10 Torr) 공정 시작 거부 MFC_SCALE_MISMATCH(값은 바꾸지 않음)."""
    r = R(tmp_path, FAST)
    r.rig.sims["mfc"].info[33] = "E06"
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.run(5)
    assert [x.reason for x in r.results] == ["MFC_SCALE_MISMATCH"] and not r.ev("run_start")
