"""공정 중 값 수정 PROCESS_ADJUST(사용자 지시 5-4 ③) — 검사 → 드라이버 쓰기 → 판정 기준 갱신 → 기록을 한 곳(ProcessEngine.adjust)에서.

- 공정 밖·정지 중 PROCESS_NOT_RUNNING, 범위 밖 ADJUST_OUT_OF_RANGE, 이번 공정에서 안 쓰는 항목 ADJUST_NOT_USED,
  대기 끝내기는 대기·조건 대기 블록에서만(ADJUST_NOT_APPLICABLE)
- 지금 쓰는 값은 바로(드라이버 경로·되읽기 → confirmed), 아직 안 온 블록은 그 블록이 새 값으로(scheduled)
- 바꾼 순간 이탈 판정 지속 시간을 처음부터, 파워는 새 목표에 다시 도달할 때까지 이탈 판정(S31C·S34A) 쉼
"""
import pytest

from v2.chk import blocks as BK
from v2.tests.test_process_engine import FAST, R, SHORT

LONG = dict(SHORT, process_time=2.0)


def adj(r, device, item, value, **kw):
    return r.cmd("PROCESS_ADJUST", dict({"device": device, "item": item, "value": value}, **kw))


def recs(r):
    return [e.data for e in r.ev("adjust")]


def test_adjust_item_table():
    keys = [(a.device, a.item) for a in BK.ADJUST_ITEMS]
    assert keys == [("mfc", "ar_flow"), ("mfc", "o2_flow"), ("mfc", "wp"), ("dc", "power"), ("rf", "power"),
                    ("rfpulse", "power"), ("rfpulse", "freq"), ("rfpulse", "duty"), ("heater", "target"), ("heater", "ramp"),
                    ("process", "main_remaining"), ("process", "skip_wait")]
    from v2.chk import params as P
    pk = {p.key for p in P.PARAMS}
    for a in BK.ADJUST_ITEMS:
        assert a.param.param_max is None or a.param.param_max in pk


def test_rejections(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, adj(r, "dc", "power", 120.0))                                  # 공정 없음
    r.on(4, r.cmd("PROCESS_START", {"row": LONG}))
    got = {}

    def at_main():
        got["main"] = [r.rig.core.command("PROCESS_ADJUST", a) for a in (
            {"device": "dc", "item": "power", "value": 5000.0},            # DC_MAX_POWER 넘음
            {"device": "rf", "item": "power", "value": 50.0},              # RF 안 씀
            {"device": "heater", "item": "target", "value": 50.0},         # 히터 안 씀
            {"device": "process", "item": "skip_wait", "value": True},     # 메인은 대기 블록이 아님
            {"device": "dc", "item": "volts", "value": 1.0},               # 모르는 항목
            {"device": "dc", "item": "power", "value": "x"},
            {"device": "dc", "item": "power", "value": 120.0, "source": "ai"})]
    r.when(lambda: r.rig.process.state == "MAIN", at_main)
    r.when(lambda: "main" in got, r.cmd("PROCESS_STOP"))
    r.when(lambda: r.rig.process.state == "STOPPING", adj(r, "dc", "power", 120.0))
    r.run(160)
    assert r.results[0].reason == "PROCESS_NOT_RUNNING"
    assert [x.reason for x in got["main"]] == ["ADJUST_OUT_OF_RANGE", "ADJUST_NOT_USED", "ADJUST_NOT_USED",
                                               "ADJUST_NOT_APPLICABLE", "BAD_ARGS", "BAD_ARGS", "BAD_ARGS"]
    assert [x.reason for x in r.results if x is not None][-1] == "PROCESS_NOT_RUNNING"   # 정지 순서 중
    rej = [d for d in recs(r) if d["result"] == "rejected"]
    assert {d["reason"] for d in rej} >= {"ADJUST_OUT_OF_RANGE", "ADJUST_NOT_USED", "PROCESS_NOT_RUNNING"}
    assert all(d["source"] == "web" for d in rej)


def test_dc_power_live_settles_without_false_abort(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"row": LONG}))
    seen = {}

    def go():
        res = r.rig.core.command("PROCESS_ADJUST", {"device": "dc", "item": "power", "value": 160.0}, source=__import__(
            "v2.common.commands", fromlist=["Source"]).Source.HMI)
        seen.update(res=res, settling=r.rig.process.dc_settling, target=r.rig.process.dcc.target)
    r.when(lambda: r.rig.process.state == "MAIN" and r.rig.process.dcc.holding, go)
    r.when(lambda: "res" in seen and r.rig.process.state == "MAIN" and not r.rig.process.dc_settling,
           lambda: seen.setdefault("p", r.rig.process.dcc.target))
    r.run(330)
    assert seen["res"].ok and seen["settling"] is True and seen["target"] == 160.0 and seen["p"] == 160.0
    d = [x for x in recs(r) if x["item"] == "power"][0]
    assert (d["old"], d["new"], d["source"], d["result"]) == (100.0, 160.0, "local", "applied")
    assert [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")] == [("done", None)]
    assert not [e for e in r.rig.safety.engine.events if e.kind in ("fired", "abort")]


def test_flow_live_confirmed_and_wp_scheduled_for_later_block(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"row": LONG}))
    r.when(lambda: r.rig.process.state == "GAS_STABILIZE", adj(r, "mfc", "wp", 7.0))          # SP1 은 아직 — 예약
    r.when(lambda: r.rig.process.state == "MAIN", adj(r, "mfc", "ar_flow", 25.0))
    r.when(lambda: r.rig.process.state == "MAIN", r.cmd("PROCESS_STOP"))
    r.run(200)
    d = recs(r)
    wp = [x for x in d if x["item"] == "wp"]
    assert wp[0]["result"] == "scheduled" and wp[0]["old"] is None
    assert r.rig.mfc.sp_mtorr == 7.0                                       # SP1 블록이 새 값으로 S1 7.00 mTorr
    steps = r.ev("steps")[0].data["steps"]
    assert [s["params"][1]["value"] for s in steps if s["params"] and s["params"][0] == "SP1_SET"] == [5.0]   # 기록은 레시피 값
    fl = [x for x in d if x["item"] == "ar_flow"]
    assert [x["result"] for x in fl] == ["applied", "confirmed"] and fl[0]["old"] == 20.0
    assert abs(r.rig.sims["mfc"].set_pct[0] - 25.0) < 1e-9
    assert r.rig.process.flow_set == {"ar": None, "o2": None}              # 끝난 뒤(값 정리)


def test_flow_change_restarts_deviation_timer(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"row": LONG}))
    got = {}

    def go():
        p = r.rig.safety.engine._p.get("S38A")
        if p is not None:
            p.since = r.c.wall() - 1.0                                     # 이탈 판정이 진행 중인 것처럼
        r.rig.core.command("PROCESS_ADJUST", {"device": "mfc", "item": "ar_flow", "value": 22.0})
        got["since"] = r.rig.safety.engine._p["S38A"].since
    r.when(lambda: r.rig.process.state == "MAIN", go)
    r.when(lambda: "since" in got, r.cmd("PROCESS_STOP"))
    r.run(200)
    assert got["since"] is None


def test_main_remaining_live_and_scheduled_and_skip_wait(tmp_path):
    r = R(tmp_path, FAST)
    bl = [{"type": "wait", "params": {"sec": 300}, "label": "긴 대기"}, {"type": "main", "params": {"sec": 600}}]
    r.on(3, r.cmd("PROCESS_START", {"blocks": bl}))
    r.on(5, adj(r, "process", "main_remaining", 4.0))                      # 메인 전 — 다음 메인 길이
    r.on(6, adj(r, "process", "skip_wait", True))                          # 긴 대기를 바로 끝냄
    r.run(40)
    d = recs(r)
    assert [x["result"] for x in d if x["item"] == "main_remaining"] == ["scheduled", "applied"]
    assert [x["result"] for x in d if x["item"] == "skip_wait"] == ["applied"]
    assert [e.data["status"] for e in r.ev("run_end")] == ["done"]
    ends = [e.data for e in r.ev("block") if e.data["phase"] == "end"]
    assert ends[0]["label"] == "긴 대기" and ends[0]["elapsed_s"] < 10.0      # 300 s 대기를 바로 끝냄
    assert ends[1]["elapsed_s"] == pytest.approx(7.0, abs=1.1)             # 셔터 열기(다음 tick) + 4 s + 셔터 닫기(다음 tick)

    r = R(tmp_path / "live", FAST)
    r.on(3, r.cmd("PROCESS_START", {"blocks": [{"type": "main", "params": {"sec": 600}}]}))
    r.on(10, adj(r, "process", "main_remaining", 3.0))
    r.run(30)
    end = [e.data for e in r.ev("block") if e.data["phase"] == "end"][0]
    assert end["elapsed_s"] == pytest.approx(10.0, abs=1.6)


def test_rf_power_live_ramps_to_new_target(tmp_path):
    r = R(tmp_path, FAST)
    row = dict(LONG, use_dc=False, dc_power=None, use_rf=True, rf_power=40.0)
    r.on(3, r.cmd("PROCESS_START", {"row": row}))
    seen = {}
    r.when(lambda: r.rig.process.state == "MAIN" and r.rig.process.rfc.holding,
           lambda: seen.setdefault("r", r.rig.core.command("PROCESS_ADJUST", {"device": "rf", "item": "power", "value": 30.0})))
    r.when(lambda: "r" in seen and r.rig.process.rfc.state == "RAMPING_UP", lambda: seen.setdefault("ramp", True))
    r.when(lambda: "ramp" in seen and r.rig.process.rfc.holding, lambda: seen.setdefault("held", r.rig.process.rfc.target))
    r.run(330)
    assert seen["r"].ok and seen.get("ramp") and seen["held"] == 30.0
    assert [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")] == [("done", None)]


def test_rfpulse_freq_duty_power_live(tmp_path):
    r = R(tmp_path, dict(FAST, RFPULSE_ENABLED=True))
    row = dict(LONG, use_dc=False, dc_power=None, use_rf_pulse=True, rf_pulse_power=100.0, rf_pulse_freq=10.0, rf_pulse_duty=50)
    r.on(3, r.cmd("PROCESS_START", {"row": row}))
    got = {}

    def go():
        c = r.rig.core.command
        got["r"] = [c("PROCESS_ADJUST", {"device": "rfpulse", "item": "freq", "value": 5.0}),
                    c("PROCESS_ADJUST", {"device": "rfpulse", "item": "duty", "value": 40}),
                    c("PROCESS_ADJUST", {"device": "rfpulse", "item": "power", "value": 120.0}),
                    c("PROCESS_ADJUST", {"device": "rfpulse", "item": "duty", "value": 99})]   # 5 kHz·99 % → OFF 2 µs < 16 µs
    r.when(lambda: r.rig.process.state == "MAIN", go)
    r.when(lambda: "r" in got and len([d for d in recs(r) if d["result"] == "confirmed"]) == 3, r.cmd("PROCESS_STOP"))
    r.run(240)
    assert [x.reason for x in got["r"]] == [None, None, None, "ADJUST_OUT_OF_RANGE"]
    sim = r.rig.sims["rfpulse"]
    assert (sim.freq, sim.duty, sim.setpoint) == (5000, 40, 120)
    conf = [d for d in recs(r) if d["result"] == "confirmed"]
    assert sorted(d["item"] for d in conf) == ["duty", "freq", "power"]
    assert [d["old"] for d in conf if d["item"] == "freq"] == [10.0]
    assert not [e for e in r.rig.safety.engine.events if e.kind == "fired" and e.rule == "S34A"]


@pytest.mark.slow
def test_heater_target_during_wait(tmp_path):
    r = R(tmp_path, FAST)
    bl = [{"type": "heater_ramp", "params": {"target_c": 60.0, "ramp_c_per_min": 30.0}}, {"type": "heater_wait"},
          {"type": "heater_off"}]
    r.on(3, r.cmd("PROCESS_START", {"blocks": bl}))
    r.on(20, adj(r, "heater", "target", 45.0))
    r.on(21, adj(r, "heater", "ramp", 12.0))
    r.run(900)
    d = [x for x in recs(r) if x["device"] == "heater"]
    assert [(x["item"], x["result"]) for x in d] == [("target", "applied"), ("ramp", "applied")]
    assert d[0]["old"] == 60.0
    reached = [e for e in r.rig.events if type(e).__name__ == "HeaterEvent" and e.kind == "reached"]
    assert reached and "45.0" in reached[0].detail
    assert [e.data["status"] for e in r.ev("run_end")] == ["done"]
