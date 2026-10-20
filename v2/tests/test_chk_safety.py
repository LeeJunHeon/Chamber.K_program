"""안전 규칙 — 실제 드라이버 5개 + 시뮬레이터 시험대(가상 시계).

- plc250 조치 지연: 조건이 생긴 시각 → 조치 쓰기가 PLC 에 도착한 시각
- 공정 중 MV 닫힘 → 중단 → STOP 순서 끝까지(장비가 실제로 안전 상태)
- ALL STOP 이 PLC 출력을 전부 끈다(펌프·밸브 포함, 히터 RUN·D00040·M00200 — v1 과 같게, 2026-10-20)
- 정지 순서 단계에서 예외가 나도 필수 단계 실행
"""
import pytest

from v2.common.clock import VirtualClock
from v2.chk.sim.plc_sim import A
from v2.chk.safety import PLC250_LATENCY_EXTRA_S
from v2.chk.sim.rig import ChkRig

PUMPS = ("rotary", "turbo", "rv", "fv")


def make(tmp_path, **proc):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, simple_process=True)
    for k, v in proc.items():
        setattr(rig.process, k, v)
    return c, rig


def at(clock, t, fn):
    """가상 시각 t(monotonic)에 fn 실행."""
    def run():
        clock.register()
        clock.wait_until(t)
        fn()
    clock.start_thread(run, f"at-{t}")


def run(rig, seconds):
    assert rig.start()
    try:
        rig.loop.run(seconds)
    finally:
        rig.stop()


def coil_writes(plc_sim, after_t):
    """after_t 뒤 PLC 에 도착한 코일 쓰기 [(시각, 시작 주소, 개수, 값들)]."""
    out = []
    for t, f in plc_sim.frames:
        if t < after_t or len(f) < 6:
            continue
        fc, addr = f[1], (f[2] << 8) | f[3]
        if fc == 5:
            out.append((t, addr, 1, f[4] == 0xFF))
        elif fc == 15:
            out.append((t, addr, (f[4] << 8) | f[5], None))
    return out


@pytest.mark.parametrize("offset", [0.01, 0.12, 0.24, 0.26, 0.51, 0.74, 0.99])
def test_plc250_heater_fault_action_latency(tmp_path, offset):
    """히터 fault 상승(S16) → HEATER_FORCE_OFF(RUN=0 쓰기)가 PLC 에 도착하기까지 — tick 을 기다리지 않는다."""
    c, rig = make(tmp_path)
    plc = rig.sims["plc"]
    T = 4.0 + offset
    at(c, T, lambda: plc.coils.__setitem__(A["heater_fault"], True))
    run(rig, 7)
    w = [x for x in coil_writes(plc, T) if x[1] == A["heater_run"] and x[3] is False]
    assert w, "RUN=0 쓰기 없음"
    lat = w[0][0] - T
    # 목표(safety.PLC250_LATENCY_EXTRA_S): 폴링 주기 + 35 ms — 최악은 조건이 막 읽힌 직후에 생긴 경우
    assert lat <= rig.cfg["PLC_FAST_POLL_MS"] / 1000.0 + PLC250_LATENCY_EXTRA_S, lat
    fire = rig.safety.engine.last_fire_t["S16"]
    assert w[0][0] - fire <= 0.02             # 감지 → 쓰기 도착(tick 을 기다리지 않음)


@pytest.mark.parametrize("offset", [0.05, 0.3, 0.6, 0.9])
def test_plc250_mv_closed_detect_latency(tmp_path, offset):
    c, rig = make(tmp_path, state="MAIN")
    plc = rig.sims["plc"]
    plc.coils[A["mv"]] = True
    plc.coils[A["ms"]] = True
    T = 3.0 + offset
    at(c, T, lambda: plc.coils.__setitem__(A["mv"], False))
    run(rig, 6)
    fire = rig.safety.engine.last_fire_t["S2"]
    assert 0 < fire - T <= rig.cfg["PLC_FAST_POLL_MS"] / 1000.0 + PLC250_LATENCY_EXTRA_S
    ms = [x for x in coil_writes(plc, T) if x[1] == A["ms"] and x[3] is False]   # STOP 2단계 Main 셔터 닫기
    assert ms and ms[0][0] - fire <= 0.05
    assert rig.safety.engine.run_reason == "MV_CLOSED"


def test_mv_closed_abort_runs_full_stop_sequence(tmp_path):
    c, rig = make(tmp_path, state="MAIN", uses_dc=True, dc_on=True)
    plc, dcs, mfcs = rig.sims["plc"], rig.sims["dc"], rig.sims["mfc"]
    for k in ("mv", "ms", "s1", "s2", "ar_valve", "o2_valve"):
        plc.coils[A[k]] = True

    def hook(t):
        if t.seq == 1:
            rig.mfc.set_flow(1, 20.0)
            rig.mfc.set_onoff("1000")
            rig.dc.start_output(600.0, 0.3, tick=t)
    rig.loop.tick_hooks.append(hook)
    at(c, 8.4, lambda: plc.coils.__setitem__(A["mv"], False))
    run(rig, 30)
    e = rig.safety.engine
    assert e.run_reason == "MV_CLOSED" and rig.process.state == "ABORTED"
    res = {n: st for n, _, st in e.seq_results["abort"]}
    assert res == {1: "not_needed", 2: "ok", 3: "not_needed", 4: "sent", 5: "not_needed", 6: "not_needed", 7: "ok",
                   8: "not_needed", 9: "ok", 10: "ok", 11: "ok", 12: "ok", 13: "ok"}, res
    assert dcs.out is False and not any(mfcs.on)
    for k in ("ms", "s1", "s2", "ar_valve", "o2_valve"):
        assert plc.coils[A[k]] is False, k
    assert not rig.safety.engine.busy


def test_all_stop_turns_off_every_plc_output(tmp_path):
    """ALL STOP = 전부(v1 PLC.on_emergency_stop + D00040=0): 버튼 코일 15개(펌프·밸브 포함)·히터 RUN·D00040·M00200, DC OFF."""
    c, rig = make(tmp_path, state="MAIN", uses_dc=True)
    plc, dcs = rig.sims["plc"], rig.sims["dc"]
    buttons = PUMPS + ("mv", "vent", "door_up", "door_down", "ar_valve", "o2_valve", "ms", "s1", "s2", "buzz_stop", "ion_remote_on")
    for k in buttons + ("heater_run", "rf_dac_enable"):
        plc.coils[A[k]] = True
    plc.regs[A["rf_dac"]] = 500
    rig.loop.tick_hooks.append(lambda t: t.seq == 1 and rig.dc.start_output(600.0, 0.3, tick=t))
    rig.loop.tick_hooks.append(lambda t: t.seq == 4 and rig.safety.all_stop())
    run(rig, 15)
    for k in buttons + ("heater_run", "rf_dac_enable"):
        assert plc.coils[A[k]] is False, k
    assert plc.regs[A["rf_dac"]] == 0 and dcs.out is False
    e = rig.safety.engine
    assert e.run_reason == "ALL_STOP" and rig.process.state == "ABORTED"
    assert all(st in ("ok", "not_needed", "sent") for _, _, st in e.seq_results["all_stop"]), e.seq_results["all_stop"]


def test_stop_sequence_exception_real_devices(tmp_path):
    """셔터 닫기 단계가 예외를 내도 DC OFF·가스 차단 등 필수 단계는 실제 장비에 실행된다."""
    c, rig = make(tmp_path, state="MAIN", uses_dc=True, dc_on=True)
    plc, dcs = rig.sims["plc"], rig.sims["dc"]
    for k in ("mv", "ar_valve", "o2_valve", "ms"):
        plc.coils[A[k]] = True

    def boom(a, done):
        raise RuntimeError("셔터 쓰기 예외(시험)")
    rig.safety.engine.handlers["SHUTTERS_CLOSE"] = boom
    rig.loop.tick_hooks.append(lambda t: t.seq == 1 and rig.dc.start_output(600.0, 0.3, tick=t))
    at(c, 6.3, lambda: plc.coils.__setitem__(A["mv"], False))
    run(rig, 25)
    res = {n: st for n, _, st in rig.safety.engine.seq_results["abort"]}
    assert res[2].startswith("error:RuntimeError") and res[11].startswith("error:RuntimeError")
    assert res[4] == "sent" and res[13] == "ok" and res[9] == "ok" and res[12] == "ok"
    assert dcs.out is False and plc.coils[A["ar_valve"]] is False and plc.coils[A["o2_valve"]] is False
    assert plc.coils[A["ms"]] is True                         # 예외 난 단계만 못 함
    steps = [e for e in rig.events if getattr(e, "kind", "") == "step"]
    assert len(steps) == 13


def test_idle_rig_no_trips(tmp_path):
    """공정이 없으면(IDLE) 60 s 동안 중단·OFF 규칙이 발동하지 않는다(오탐 없음)."""
    c, rig = make(tmp_path)
    run(rig, 60)
    fired = [e for e in rig.safety.engine.events if e.kind == "fired"]
    assert fired == [], fired
    assert rig.safety.locks == set()
