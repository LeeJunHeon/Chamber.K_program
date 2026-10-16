"""S51 시작 때 남은 출력 정리 — 장비별 첫 정상 읽기(재연결 때는 아님), 진행 중이면 warn 만, bench 는 쓰지 않음."""
from v2.common.clock import VirtualClock
from v2.chk import cmd_guard as G
from v2.chk import process_states as PS
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig


def _rig(tmp_path, residual=("rf",), mode="sim", simple=False, ticks=25, after=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, cfg={"RFPULSE_ENABLED": True}, mode=mode, simple_process=simple)
    s = rig.sims
    for k in ("mv", "turbo", "rotary"):
        s["plc"].coils[A[k]] = True
    if "rf" in residual:
        s["plc"].regs[A["rf_dac"]] = 800
        s["plc"].coils[A["rf_dac_enable"]] = True
    if "en" in residual:
        s["plc"].coils[A["rf_dac_enable"]] = True
    if "dc" in residual:
        s["dc"].out = True
    if "rfp" in residual:
        s["rfpulse"].control, s["rfpulse"].setpoint, s["rfpulse"].out = 2, 50, True
    if "rfp_local" in residual:
        s["rfpulse"].setpoint, s["rfpulse"].out = 50, True
    if "gas" in residual:
        s["mfc"].on[0], s["mfc"].set_pct[0], s["mfc"].flow_pct[0] = True, 20.0, 20.0
    if after:
        after(rig)
    rig.start()
    try:
        rig.loop.run(ticks)
    finally:
        rig.stop()
    return rig


def _codes(rig):
    return [(n["level"], n["code"]) for n in rig.safety.notifications if n["code"].endswith(("RESIDUAL", "OUTPUT", "FLOW"))]


def test_residual_outputs_are_turned_off_and_reported(tmp_path):
    rig = _rig(tmp_path, ("rf", "dc", "rfp", "gas"), ticks=70)
    s = rig.sims
    assert s["plc"].regs[A["rf_dac"]] == 0 and not s["plc"].coils[A["rf_dac_enable"]]       # 램프다운 → enable OFF
    assert not s["dc"].out and not s["rfpulse"].out
    assert s["mfc"].on[0]                                                                    # 가스는 끄지 않는다
    assert set(_codes(rig)) == {("warn", "RF_RESIDUAL_OUTPUT"), ("warn", "DC_RESIDUAL_OUTPUT"),
                                ("warn", "RFP_RESIDUAL_OUTPUT"), ("warn", "GAS_RESIDUAL_FLOW")}
    assert {d for d, _, _ in rig.safety.residual} == {"plc", "dc", "rfp"}
    assert all(s["plc"].coils[A[k]] for k in ("mv", "turbo", "rotary"))                    # 펌프·밸브 안 건드림
    assert ("info", "RF_ENABLE_RESIDUAL") not in _codes(rig)


def test_enable_only_is_info_and_cleared(tmp_path):
    rig = _rig(tmp_path, ("en",))
    assert _codes(rig) == [("info", "RF_ENABLE_RESIDUAL")]
    assert not rig.sims["plc"].coils[A["rf_dac_enable"]]


def test_nothing_left_means_nothing_done(tmp_path):
    rig = _rig(tmp_path, ())
    assert _codes(rig) == [] and rig.safety.residual == []
    assert set(rig.safety.first_done) >= {"plc", "dc", "mfc", "rfp"}


def test_reconnect_does_not_act_again(tmp_path):
    def later(rig):
        def hook(t):
            if t.seq == 12:                                       # 첫 읽기 뒤 DC 끊김 → 재연결 때 출력이 켜져 있어도
                rig.sims["dc"].out = True
                rig.workers["dc"].transport.close()
        rig.loop.tick_hooks.append(hook)
    rig = _rig(tmp_path, (), ticks=40, after=later)
    assert ("warn", "DC_RESIDUAL_OUTPUT") not in _codes(rig) and rig.safety.residual == []


def test_process_running_only_warns(tmp_path):
    active = sorted(PS.ACTIVE_STATES)[0]

    def busy(rig):
        rig.process.state = active
    rig = _rig(tmp_path, ("rf", "dc"), simple=True, after=busy)
    assert set(_codes(rig)) == {("warn", "RF_RESIDUAL_OUTPUT"), ("warn", "DC_RESIDUAL_OUTPUT")}
    assert all("진행 중" in n["detail"] for n in rig.safety.notifications if n["code"].endswith("OUTPUT"))
    assert rig.safety.residual == [] and rig.sims["dc"].out


def test_rfp_front_panel_mode_only_warns(tmp_path):
    rig = _rig(tmp_path, ("rfp_local",))
    assert _codes(rig) == [("warn", "RFP_RESIDUAL_OUTPUT")] and rig.sims["rfpulse"].out


def test_bench_writes_nothing(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, cfg={"RFPULSE_ENABLED": True}, mode="bench")
    log = {k: [] for k in rig.guards}
    for k, g in rig.guards.items():
        orig = g.inner.write
        g.inner.write = lambda data, k=k, orig=orig: (log[k].append(bytes(data)), orig(data))
    rig.sims["plc"].regs[A["rf_dac"]] = 800
    rig.sims["plc"].coils[A["rf_dac_enable"]] = True
    rig.sims["dc"].out = True
    rig.start()
    try:
        rig.loop.run(25)
    finally:
        rig.stop()
    checks = {k: G.checker(k, "bench", rig.cfg) for k in log}
    assert [(k, b) for k, bs in log.items() for b in bs if checks[k](b)] == []
    assert rig.sims["plc"].write_funcs_seen() == [] and rig.sims["dc"].out
    assert rig.sims["plc"].regs[A["rf_dac"]] == 800
