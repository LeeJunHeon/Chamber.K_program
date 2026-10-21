"""남긴 출력·히터 넘김·공정 밖 STOP 정리(사용자 결정 2026-10-21, 5-5 ③-3·3-4) — 검토 B·B'·C'·C''.

- 정상 끝 정지에서 히터 단계(3)를 빼면 히터는 켜진 채 소유 manual(HEATER_HANDOVER), 웹 히터 카드 명령이 받아진다
- 뺀 단계로 실제 켜진 채 남은 것은 OUTPUTS_LEFT_ON(info)
- 공정 밖 STOP: 남긴 것 중 지금도 켜진 것만 고정 정지 순서로 정리(OUTPUTS_CLEANUP), 없으면 아무것도 안 함(v1)
- 공정 중 STOP 의 정지 순서는 앞 행이 남긴 히터도 끈다
"""
import pytest

from v2.chk.sim.plc_sim import A
from v2.tests.test_process_engine import FAST, R, SHORT

HEAT = [{"type": "heater_ramp", "params": {"target_c": 40.0, "ramp_c_per_min": 60.0}}, {"type": "wait", "params": {"sec": 3}},
        {"type": "end_stop", "params": {"skip": [3]}}]
GAS = {"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}}


def ended(r, n=1):
    return lambda: len(r.ev("run_end")) >= n and r.rig.process.state in ("IDLE", "LOADED")


def cleanups(r):
    return [e for e in r.ev("cleanup")]


def test_heater_left_on_handed_over_then_stop_cleans(tmp_path):
    """검토 B — skip [3]: 히터 켜진 채 manual 로 넘김 → HEATER_SV 받아짐 → STOP 이 끈다 → 두 번째 STOP 은 아무것도 안 함."""
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"blocks": HEAT}))
    got = {}

    def after():
        got["run"] = r.plc.coils[A["heater_run"]]
        got["owner"] = r.rig.heater.owner
        got["sv"] = r.rig.core.command("HEATER_SV", {"value": 45.0})
        got["stop1"] = r.rig.core.command("PROCESS_STOP")
    r.when(ended(r), after)
    r.when(lambda: "stop1" in got and cleanups(r), lambda: got.setdefault("stop2", r.rig.core.command("PROCESS_STOP")))
    r.run(80)
    assert [e.data["status"] for e in r.ev("run_end")] == ["done"]
    assert got["run"] is True and got["owner"] == "manual" and got["sv"].ok
    assert [e.code for e in r.ev("heater_handover")] == ["HEATER_HANDOVER"]
    left = [e for e in r.ev("warn") if e.code == "OUTPUTS_LEFT_ON"]
    assert len(left) == 1 and left[0].data["outputs"] == ["heater"] and left[0].data["level"] == "info"
    assert got["stop1"].ok and got["stop2"].ok
    assert len(cleanups(r)) == 1 and len(r.ev("cleanup_start")) == 1                  # 두 번째 STOP 은 아무것도 안 함
    st = {s["no"]: s["status"] for s in cleanups(r)[0].data["steps"]}
    assert st[3] == "ok" and st[9] == "not_needed" and st[4] == "not_needed"
    assert r.plc.coils[A["heater_run"]] is False and r.rig.heater.owner is None and not r.rig.process.left


def test_heater_turned_off_by_card_is_not_cleaned(tmp_path):
    """검토 B' — 넘긴 히터를 히터 카드(HEATER_OFF)로 끄면 PROCESS_STOP 은 아무것도 안 함."""
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"blocks": HEAT}))
    got = {}

    def after():
        got["off"] = r.rig.core.command("HEATER_OFF")
    r.when(ended(r), after)
    r.when(lambda: "off" in got and not r.plc.coils[A["heater_run"]],
           lambda: got.setdefault("stop", r.rig.core.command("PROCESS_STOP")))
    r.run(60)
    assert got["off"].ok and got["stop"].ok
    assert not r.ev("cleanup_start") and not cleanups(r) and "cleanup" not in r.rig.safety.engine.seq_results


def test_flow_left_then_stop_turns_flow_and_valves_off(tmp_path):
    """검토 C' — end_stop(skip [9,10,12]) → Ar 유량·차단밸브 남음 → STOP → 유량 OFF·밸브 닫힘 → 두 번째 STOP 은 아무것도 안 함."""
    r = R(tmp_path, FAST)
    bl = [GAS, {"type": "power_on", "params": {"dc_power": 100.0}}, {"type": "wait", "params": {"sec": 3}},
          {"type": "end_stop", "params": {"skip": [9, 10, 12]}}]
    r.on(3, r.cmd("PROCESS_START", {"blocks": bl}))
    got = {}

    def after():
        got["state"] = (r.rig.sims["dc"].out, r.rig.sims["mfc"].on[0], r.plc.coils[A["ar_valve"]])
        got["stop1"] = r.rig.core.command("PROCESS_STOP")
    r.when(ended(r), after)
    r.when(lambda: cleanups(r), lambda: got.setdefault("stop2", r.rig.core.command("PROCESS_STOP")))
    r.run(120)
    assert got["state"] == (False, True, True)                                     # 파워는 꺼지고 유량·밸브는 남음
    left = [e for e in r.ev("warn") if e.code == "OUTPUTS_LEFT_ON"][0]
    assert set(left.data["outputs"]) == {"ar", "mfc_valve", "ar_valve"}
    assert len(cleanups(r)) == 1 and len(r.ev("cleanup_start")) == 1
    assert not any(r.rig.sims["mfc"].on) and r.plc.coils[A["ar_valve"]] is False


def test_stop_in_second_row_also_turns_off_heater_left_by_first_row(tmp_path):
    """검토 C'' — 1행 end_stop(skip [3])으로 히터 남김, 2행(히터 없음) 중 STOP → 정지 순서가 앞 행 히터도 끈다."""
    r = R(tmp_path, FAST)
    rows = [{"process_name": "H", "blocks": HEAT},
            {"process_name": "W", "blocks": [{"type": "wait", "params": {"sec": 60}}]}]
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "two", "rows": rows}))
    r.on(3, r.cmd("RECIPE_START"))
    got = {}

    def stop():
        got["run_before"] = r.plc.coils[A["heater_run"]]
        got["stop"] = r.rig.core.command("PROCESS_STOP")
    r.when(lambda: len(r.ev("run_start")) == 2 and r.rig.process.state == "WAIT", stop)
    r.run(120)
    assert got["run_before"] is True and got["stop"].ok
    assert [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")] == [("done", None), ("aborted", "USER_STOP")]
    res = {n: st for n, _, st in r.rig.safety.engine.seq_results["stop"]}
    assert res[3] == "ok" and r.plc.coils[A["heater_run"]] is False and r.rig.heater.owner is None
    assert not r.rig.process.left


def test_all_stop_clears_left_record(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"blocks": HEAT}))
    got = {}
    r.when(ended(r), lambda: got.setdefault("a", r.rig.core.command("ALL_STOP")))
    r.when(lambda: "a" in got and "all_stop" in r.rig.safety.engine.seq_results,
           lambda: got.setdefault("s", r.rig.core.command("PROCESS_STOP")))
    r.run(60)
    assert got["s"].ok and not r.rig.process.left and not r.rig.heater.handed_over and not r.ev("cleanup_start")
    assert r.plc.coils[A["heater_run"]] is False


def test_stop_outside_process_without_left_does_nothing(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"row": SHORT}))
    r.when(ended(r), r.cmd("PROCESS_STOP"))
    r.run(100)
    assert all(x.ok for x in r.results) and not r.ev("cleanup_start") and not r.ev("heater_handover")
    assert not [e for e in r.ev("warn") if e.code == "OUTPUTS_LEFT_ON"]


E_BLOCKS = [{"type": "heater_ramp", "params": {"target_c": 40.0, "ramp_c_per_min": 60.0}}, GAS,
            {"type": "power_on", "params": {"rf_power": 100.0}}, {"type": "wait", "params": {"sec": 3}},
            {"type": "end_stop", "params": {"skip": [3, 9, 10, 12]}}]


@pytest.mark.parametrize("how", ["stop", "abort"])
def test_stop_during_end_stop_cancels_leaving(tmp_path, how):
    """검토 E(5-6 ①) — 정상 끝 정지(STOPPING, skip [3,9,10,12]) 중 STOP·이상 중단 → 남김 취소: 뺀 단계를 이어서 실행."""
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("PROCESS_START", {"blocks": E_BLOCKS}))
    got = {}

    def act():
        got["stopping"] = r.rig.safety.engine.stopping
        if how == "stop":
            got["res"] = r.rig.core.command("PROCESS_STOP")
        else:
            r.plc.coils[A["mv"]] = False
            got["res"] = r.rig.safety.engine.abort("MV_CLOSED", rule="S2")
    r.when(lambda: r.rig.process.state == "STOPPING" and r.rig.safety.engine.stopping == "row_end", act)
    r.run(300)
    assert got["stopping"] == "row_end"
    want = ("aborted", "USER_STOP") if how == "stop" else ("error", "MV_CLOSED")
    assert [(e.data["status"], e.data["reason"]) for e in r.ev("run_end")] == [want]
    rest = {n: st for n, _, st in r.rig.safety.engine.seq_results["stop_rest"]}
    assert sorted(rest) == [3, 9, 10, 12] and all(st == "ok" for st in rest.values()), rest
    assert r.plc.coils[A["heater_run"]] is False and r.rig.heater.owner is None and not r.rig.heater.handed_over
    assert not any(r.rig.sims["mfc"].on) and r.plc.coils[A["ar_valve"]] is False
    assert not r.rig.process.left and not r.ev("heater_handover")
    assert not [e for e in r.ev("warn") if e.code == "OUTPUTS_LEFT_ON"]
    assert r.ev("stop_rest")[0].data["steps"] == [3, 9, 10, 12]
    ends = [e for e in r.ev("run_end")]
    assert r.rig.safety.engine.stopping is None and r.rig.process.state in ("IDLE", "LOADED") and ends
