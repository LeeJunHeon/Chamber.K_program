"""PLC 시뮬레이터 래더 모델 시험 — 시각은 advance_to 로 직접 진행(통신 없이)."""
from v2.chk.sim.plc_sim import A, PlcSim


def _run_heater(sim, sv_c, t0=0.0):
    sim.regs[A["heater_sv_limit"]] = 7000
    sim.regs[A["heater_ot_limit"]] = 7500
    sim.regs[A["heater_ramp_rate"]] = 2                  # 12 °C/min
    sim.regs[A["heater_slow_rate"]] = 10
    sim.regs[A["heater_holdback"]] = 100
    sim.regs[A["heater_sv"]] = int(sv_c * 10)
    sim.coils[A["heater_run"]] = True


def _kick(sim, t_end, every=3.0):
    t = sim.t
    while t < t_end:
        t = min(t_end, t + every)
        sim.advance_to(t)
        sim.regs[A["heater_wd"]] = (sim.regs[A["heater_wd"]] + 1) & 0x7FFF     # 마지막 하트비트 = t_end


def test_heater_ramps_and_holds():
    sim = PlcSim()
    _run_heater(sim, 200.0)
    _kick(sim, 60.0)
    pv = sim.reg("heater_pv") / 10
    assert 30.0 < pv < 50.0                               # 12 °C/min → 1분에 약 +12 °C(+ 지연)
    assert sim.reg("heater_sv_ramp") / 10 > pv
    _kick(sim, 3600.0)
    assert abs(sim.reg("heater_pv") / 10 - 200.0) < 3.0   # 목표에 머무름
    assert 400 < sim.reg("heater_mv") <= 1200 and sim.coil("heater_pid_run")


def test_watchdog_trips_after_10s_without_heartbeat():
    sim = PlcSim()
    _run_heater(sim, 100.0)
    _kick(sim, 20.0)
    assert sim.coil("heater_run") and not sim.coil("heater_wd_err")
    sim.advance_to(29.0)                                  # 마지막 하트비트 20.0 + 9 s
    assert sim.coil("heater_run")
    sim.advance_to(30.2)
    assert not sim.coil("heater_run") and sim.coil("heater_wd_err") and sim.coil("heater_fault")
    assert sim.reg("heater_mv") == 0
    sim.coils[A["heater_rst"]] = True                     # 하트비트가 없으면 리셋 안 됨
    sim.advance_to(30.4)
    assert sim.coil("heater_wd_err")
    sim.regs[A["heater_wd"]] += 1
    sim.advance_to(30.5)
    sim.coils[A["heater_rst"]] = True
    sim.advance_to(30.7)
    assert not sim.coil("heater_fault") and not sim.coil("heater_rst")


def test_over_temperature_and_tc_failure():
    sim = PlcSim()
    _run_heater(sim, 600.0)
    sim.regs[A["heater_ot_limit"]] = 500                  # 50 °C 에서 트립
    _kick(sim, 600.0)
    assert sim.coil("heater_ot") and sim.coil("heater_fault") and not sim.coil("heater_run")
    sim2 = PlcSim()
    sim2.tc_fail = True
    sim2.advance_to(0.2)
    assert sim2.reg("heater_pv") == 0xFFFF and sim2.coil("heater_tc_err")


def test_ladder_forced_values():
    sim = PlcSim()
    sim.regs[A["heater_slow_zone"]] = 60
    sim.regs[A["heater_mv_limit"]] = 2000
    sim.regs[A["heater_ot2_limit"]] = 11500
    sim.advance_to(0.1)
    assert sim.reg("heater_slow_zone") == 100 and sim.reg("heater_mv_limit") == 1600
    assert sim.reg("heater_sv2_max") == 11000


def test_rf_dac_to_adc():
    sim = PlcSim()
    sim.regs[A["rf_dac"]] = 2000
    sim.coils[A["rf_dac_enable"]] = True
    sim.advance_to(5.0)
    fwd = sim.reg("rf_fwd_adc") * 594.5 / 4000
    ref = sim.reg("rf_ref_adc") * 200.0 / 4000
    assert abs(fwd - 300.0) < 1.0 and abs(ref - 3.0) < 0.2
    sim.ref_inject_w = 40.0
    sim.advance_to(5.2)
    assert sim.reg("rf_ref_adc") * 200.0 / 4000 > 40.0
    sim.coils[A["rf_dac_enable"]] = False
    sim.advance_to(10.0)
    assert sim.reg("rf_fwd_adc") < 5


def test_mv_interlock_and_pressure():
    sim = PlcSim()
    sim.advance_to(0.1)
    assert sim.coil("mv_interlock") and not sim.coil("atm")
    sim.itl_fault = True
    sim.advance_to(0.2)
    assert not sim.coil("mv_interlock")
    sim.itl_fault = False
    sim.gas_sccm = 20.0
    sim.advance_to(30.0)
    assert abs(sim.pressure - 2e-3) < 2e-4                # 20 sccm → 약 2 mTorr
    sim.coils[A["mv"]] = False
    sim.coils[A["vent"]] = True
    sim.advance_to(200.0)
    assert sim.coil("atm") and not sim.coil("mv_interlock")
    sim.pressure_override = 1e-6
    sim.advance_to(200.1)
    assert sim.pressure == 1e-6


def test_restart_clears_memory():
    sim = PlcSim()
    sim.regs[60] = 0x1234
    sim.regs[A["heater_sv_limit"]] = 7000
    sim.restart()
    assert sim.regs[60] == 0 and sim.reg("heater_sv_limit") == 5000 and not sim.coil("heater_run")
