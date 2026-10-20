"""5-3 히터 — v1 과 줄 단위로 비교한 검토에서 나온 차이의 재현 시험(고치기 전 실패 → 고친 뒤 통과).

- S15 남은 RUN 정리는 프로그램 실행당 한 번(v1 PLC.py:220·1019·1079)
- RUN=0 쓰기 실패는 보류 → 다음 통신 성공 때 다시(v1 PLC.py:1279-1293·391-394·1018-1021)
- 히터 레시피 시작 거부: 진행 중 리스트의 어느 행이 히터를 쓰면(v1 main.py:2618-2620)
- 히터 레시피 비정상 끝·히터 이상 → 같이 돌던 공정도 중단(v1 main.py:3380·3443·3620-3622)
- 히터 가스 시작은 MV 열림·인터록(v1 main.py:3158·1692·2665), 미뤄진 레시피 시작 전 인터록 확인(heater_recipe.py:805·810, main.py:3226-3230)
- HEATER_ON 이 이미 켜짐·가스 준비 중이면 HEATER_ALREADY(v1 main.py:1865·1867)
- 하트비트 5 s 넘게 밀리면 경고(v1 PLC.py:1145-1159)
- HEATER_RECIPE_STOP 은 레시피가 없어도 안전 종료(v1 main.py:1953, heater_recipe.py:852)
"""
from v2.common.clock import VirtualClock
from v2.chk.heater_engine import HeaterEngine
from v2.chk.sim.plc_sim import A, PlcSim
from v2.tests.test_plc_driver import Rig as PlcRig
from v2.tests.test_process_engine import FAST, R, SHORT
from v2.tests.test_safety_engine import CFG, H

T0 = 1_760_000_000.0
GAS = {"use_ar": True, "ar_flow": 20.0, "wp_mtorr": 5.0}
STEPS = [{"target_c": 40, "ramp_c_per_min": 6, "soak_min": 600}]


def test_residual_run_cleared_once_per_program_run():
    sim = PlcSim()
    sim.coils[A["heater_run"]] = True
    r = PlcRig(sim=sim)
    r.second(2)
    assert r.kinds().count("heater_residual_run") == 1
    t = r.c.monotonic()
    sim.add_fault("disconnect", t, t + 2.0)                  # 가열 중 1~2 s 통신 끊김
    sim.coils[A["heater_run"]] = True
    r.second(8)
    assert r.kinds().count("heater_residual_run") == 1       # 재연결 때 다시 끄지 않는다
    r.done()


def test_heater_off_write_failure_is_retried_on_next_success():
    sim = PlcSim()
    sim.coils[A["heater_run"]] = True
    r = PlcRig(sim=sim)
    r.second(1)
    t = r.c.monotonic()
    sim.add_fault("no_response", t, t + 0.4)                 # 링크는 살아 있는데 RUN=0 쓰기만 실패
    r.d.heater_run(False)
    r.c.wait_until(t + 0.6)
    assert sim.coils[A["heater_run"]] is True and r.d._pending_heater_off
    r.second(3)
    assert sim.coils[A["heater_run"]] is False and not r.d._pending_heater_off
    assert "heater_off_applied" in r.kinds()
    r.done()


def test_heartbeat_delay_warns_once_and_recovers():
    sim = PlcSim()
    r = PlcRig(sim=sim)
    r.second(2)
    t = r.c.monotonic()
    sim.add_fault("disconnect", t, t + 8.0)
    r.second(40)                                             # 재연결 백오프 뒤 회복
    assert r.kinds().count("heater_wd_delay") == 1 and "heater_wd_recovered" in r.kinds()
    r.done()


def test_heater_recipe_rejected_while_list_with_heater_row_runs(tmp_path):
    r = R(tmp_path, FAST)
    heat = dict(SHORT, process_name="H", use_heater=True, heater_temp=40.0)
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "h", "rows": [SHORT, {"process_name": "delay 60s"}, heat]}))
    r.on(3, r.cmd("RECIPE_START"))
    got = {}
    r.when(lambda: r.rig.process.state == "CSV_DELAY",
           lambda: got.setdefault("r", r.rig.core.command("HEATER_RECIPE_RUN", {"name": "x", "steps": STEPS})))
    r.when(lambda: "r" in got, r.cmd("PROCESS_STOP"))
    r.run(160)
    assert got["r"].reason == "HEATER_OWNED_BY_PROCESS"     # 딜레이 중에도 리스트 뒤 행이 히터를 쓴다


def test_heater_recipe_failure_aborts_running_process(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("HEATER_RECIPE_RUN", {"name": "bg", "steps": STEPS}))
    r.on(5, r.cmd("PROCESS_START", {"row": dict(SHORT, process_time=3.0)}))
    r.when(lambda: r.rig.process.state == "MAIN", lambda: r.rig.heater.recipe_abort("HEATER_TC_BAD"))
    r.run(150)
    ends = r.ev("run_end")
    assert [(e.data["status"], e.data["reason"]) for e in ends] == [("error", "HEATER_TC_BAD")]


def test_heater_fault_aborts_process_when_recipe_runs():
    h = H(state="MAIN", heater_recipe_running=True)          # 공정은 히터를 안 쓰지만 레시피가 돌고 있음
    h.feed("plc250", 0, {"heater.fault": True})
    assert h.e.run_reason == "HEATER_FAULT"


def test_heater_gas_needs_main_valve(tmp_path):
    r = R(tmp_path, FAST)
    r.plc.coils[A["mv"]] = False
    r.on(3, r.cmd("HEATER_ON", dict(GAS, value=40.0)))
    r.on(3, r.cmd("HEATER_RECIPE_RUN", {"name": "g", "steps": STEPS, "gas": GAS}))
    r.run(6)
    res = [x.reason for x in r.results]
    assert res == ["MV_NOT_OPEN", "MV_NOT_OPEN"] and r.rig.heater.gas_state == "IDLE"


def test_deferred_recipe_start_checks_interlock_and_releases_gas(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("HEATER_RECIPE_RUN", {"name": "g", "steps": STEPS, "gas": GAS}))
    r.on(6, lambda: r.plc.sensors.__setitem__("water", False))     # 가스 준비 중 히터 인터록 해제
    r.run(150)
    fin = [e for e in r.rig.events if type(e).__name__ == "HeaterEvent" and e.kind == "recipe_finished"]
    assert fin and fin[-1].code == "HEATER_ITL_OFF" and not r.rig.heater.recipe_running
    assert r.rig.heater.gas_state == "IDLE"                  # 가스는 풀었다
    assert r.plc.coils[A["heater_run"]] is False


class _Plc:
    last = None

    def heater_set_reg(self, *a, **k):
        pass

    def heater_set_sv(self, *a, **k):
        pass

    def heater_run(self, *a, **k):
        pass


def test_heater_on_already_on_is_rejected():
    c = VirtualClock(start_wall=T0)
    c.register()
    h = HeaterEngine(c, CFG, _Plc(), None)
    h.st = {"run": True, "pv": 100.0, "itl": True}
    assert h.manual_on(200.0) == "HEATER_ALREADY"
    h.st = {"run": False, "pv": 100.0, "itl": True}
    h.pending = ("manual_on", 200.0)
    assert h.manual_on(200.0) == "HEATER_ALREADY"           # 가스 준비 중


def test_heater_recipe_stop_without_recipe_shuts_down(tmp_path):
    r = R(tmp_path, FAST)
    r.on(3, r.cmd("HEATER_ON", {"value": 40.0}))
    r.on(8, r.cmd("HEATER_RECIPE_STOP"))
    r.run(10)
    assert r.results[0].ok and r.results[1].ok
    assert r.plc.coils[A["heater_run"]] is False
    assert r.plc.regs[A["heater_ramp_rate"]] == max(1, round(CFG["HEATER_RAMP_RATE_C_PER_MIN"] / 6.0))
