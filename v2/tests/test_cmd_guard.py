"""통신층 명령 가드 — 매뉴얼 질의 목록(bench)·드라이버 명령 목록(prod·sim)·금지 명령.

1) 판정 단위 시험(장비별 허용·금지, 사유).
2) bench 시험대: 모든 쓰기 경로(하트비트·필라멘트·유량·압력·밸브·영점·OFF 시퀀스·DC 시작·RF·버튼·히터·공정 시작)를 불러도
   포트에 나간 바이트 중 bench 목록 밖은 0 바이트.
3) sim(prod 목록): 금지 명령을 드라이버 경로로 보내도 포트에 닿기 전에 막히고(COMMAND_BLOCKED) 연결은 유지.
"""
import pytest

from v2.common.clock import VirtualClock
from v2.common.modbus_rtu import with_crc
from v2.common.paramdef import defaults
from v2.common.worker import Request
from v2.chk import cmd_guard as G
from v2.chk import params as P
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig
from v2.common import aebus

CFG = defaults(P.PARAMS)


def dc(s, mode="prod", prot=False):
    return G.dc_check((s + "\n").encode(), mode, prot)


def mfc(s, mode="prod"):
    return G.mfc_check((s + "\r").encode(), mode)


def test_dc_lists():
    assert len(G.DC_BENCH) == 28 and all(dc(q, "bench") is None for q in G.DC_BENCH)
    assert dc("OUTP OFF", "bench") and dc("FLOW?", "bench") and dc("POL?", "bench")      # 바이폴라 전용 — 보내지 않음
    for ok in ("OUTP ON", "OUTP OFF", "*RST", "*CLS", "APPLy 600.00,0.0100", "CURR 0.3000", "MEAS:ALL?", "FACT:LAST-STA?"):
        assert dc(ok) is None, ok
    for bad, word in (("FACT:LAST-STA SAF", "FACT"), ("FACT:LOAD-DEF", "LOAD-DEF"), ("*SAV 1", "*SAV"), ("*RCL 2", "*RCL"),
                      ("CAL:VOLT MIN", "CAL"), ("KEYL ON", "KEYL"), ("SYST:BEEP", "BEEP"), ("VOLT:STEP 1", "STEP"),
                      ("CURR:STEP 0.1", "STEP"), ("VOLT:UVL 0", "UVL"), ("VOLT:OVL 600", "OVL"), ("CURR:UCL 0", "UCL"),
                      ("CURR:OCL 1", "OCL"), ("VOLT:OVP:CLE", "트립"), ("CURR:OCP:CLE", "트립"), ("POL P", "POL")):
        why = dc(bad)
        assert why and "금지" in why and word in why, (bad, why)
    assert dc("VOLT:OVP 600.00") and dc("VOLT:OVP 600.00", prot=True) is None       # DC_SET_PROTECTION 일 때만
    assert dc("VOLT 10") and "허용 목록 밖" in dc("VOLT 10")
    assert G.dc_check(b"OUTP?\r", "prod")                                              # LF 가 아니면 형식 오류


def test_mfc_lists():
    assert len(G.MFC_BENCH) == 46 and all(mfc(q, "bench") is None for q in G.MFC_BENCH)
    for no in ("R24", "R31", "R35", "R36", "R63", "R64", "R67", "R68", "R72", "R73", "R76", "R77", "Q1 20.0"):
        assert mfc(no, "bench"), no
    for ok in ("Q1 20.0", "Q2 5.5", "L01000", "L5 1", "Z1", "S1 0.50", "D1", "D4", "O", "C", "H", "R69"):
        assert mfc(ok) is None, ok
    for bad in ("E03", "F0", "G2", "J1", "A1", "T1 1", "B1", "N0", "U0", "X1 1", "M1 10", "V50", "W11.000", "W51",
                "P1 10", "Y1 1", "Z2 1", "Z3", "Z4", "D6", "Q5 10.0", "Q8 1"):
        why = mfc(bad)
        assert why and "금지" in why, (bad, why)
    assert "IG 의 E·F 와 다르다" in mfc("E03")


def test_ig_plc_rfp_lists():
    assert G.ig_check(b"E", "bench") is None and G.ig_check(b"A", "bench") and G.ig_check(b"A", "prod") is None
    assert "디개스" in G.ig_check(b"C", "prod") and "디개스" in G.ig_check(b"D", "bench") and G.ig_check(b"Z", "prod")
    rd = with_crc(bytes([1, 3, 0, 0, 0, 61]))
    assert G.plc_check(rd, "bench", 1, CFG) is None and G.plc_check(rd, "bench", 2, CFG)            # 국번
    wr = with_crc(bytes([1, 5, 0, A["mv"], 0xFF, 0]))
    assert "FC1·FC3" in G.plc_check(wr, "bench", 1, CFG) and G.plc_check(wr, "prod", 1, CFG) is None
    assert G.plc_check(with_crc(bytes([1, 5, 0, A["mv_interlock"], 0xFF, 0])), "prod", 1, CFG)       # 읽기 전용 코일
    assert G.plc_check(with_crc(bytes([1, 6, 0, 40, 0, 10])), "prod", 1, CFG) is None                 # D40 RF DAC
    assert G.plc_check(with_crc(bytes([1, 6, 0, 41, 0, 10])), "prod", 1, CFG)                         # D41 MV(읽기)
    assert G.plc_check(with_crc(bytes([1, 6, 0, 60, 0x12, 0x34])), "prod", 1, CFG) is None           # 세션 마커
    assert G.plc_check(with_crc(bytes([1, 16, 0, 12, 0, 3, 6, 0, 1, 0, 2, 0, 3])), "prod", 1, CFG) is None   # D12~D14 모두 쓰기 가능
    assert "[10, 11]" in G.plc_check(with_crc(bytes([1, 16, 0, 10, 0, 3, 6, 0, 1, 0, 2, 0, 3])), "prod", 1, CFG)  # D10(TC1)·D11(TC2) 읽기 전용
    assert G.plc_check(rd[:-1] + bytes([rd[-1] ^ 1]), "prod", 1, CFG)                                # CRC
    q = aebus.build(1, 162)
    assert G.rfp_check(q, "bench", 1) is None and G.rfp_check(aebus.build(1, 1), "bench", 1)
    assert G.rfp_check(aebus.build(1, 1), "prod", 1) is None and G.rfp_check(aebus.build(1, 200), "prod", 1)


# ───────── bench 시험대: 모든 쓰기 경로 → 포트에 나간 bench 목록 밖 0 바이트 ─────────
def _wire_log(rig):
    log = {k: [] for k in rig.guards}
    for k, g in rig.guards.items():
        inner = g.inner
        orig = inner.write

        def w(data, k=k, orig=orig):
            log[k].append(bytes(data))
            orig(data)
        inner.write = w
    return log


def test_bench_rig_all_write_paths_send_zero_bytes_outside_bench_list(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, cfg={"RFPULSE_ENABLED": True}, mode="bench")
    log = _wire_log(rig)
    plc = rig.sims["plc"]
    for k in ("mv", "turbo", "rotary", "fv"):
        plc.coils[A[k]] = True
    results = []

    def all_writes(t):
        if t.seq != 3:
            return
        r, cb = rig, results.append
        r.plc.heater_run(True, on_done=cb)
        r.plc.heater_run(False, on_done=cb)
        r.plc.set_button("mv", False, on_done=cb)
        r.plc.rf_set_power(100.0, on_done=cb)
        r.plc.rf_write_dac(100, on_done=cb)
        r.plc.rf_off(on_done=cb)
        r.plc.coils_off(["ms", "s1"], on_done=cb)
        r.plc.outputs_off(on_done=cb)
        r.plc.heater_set_sv(50.0, on_done=cb)
        r.plc.heater_reset(on_done=cb)
        r.ig.filament_on(on_done=cb)
        r.ig.filament_off(on_done=cb)
        r.mfc.set_flow(1, 20.0)
        r.mfc.set_onoff("1000")
        r.mfc.set_channel(1, True)
        r.mfc.set_sp(1, 5.0)
        r.mfc.select_sp(1)
        r.mfc.valve(True)
        r.mfc.zero("Z1")
        r.dc.start_output(600.0, 0.1)
        r.dc.set_current(0.3, on_done=cb)
        r.dc.output_off_confirmed()
        r.rfp.start(100.0, freq_hz=10000, duty=50)
        r.rfp.rf_off_confirmed(on_done=cb)
        r.heater.manual_on(60.0)
        r.core.command("PROCESS_START", {"row": {"ar": True, "ar_flow": 20.0, "working_pressure": 5.0,
                                                 "process_time": 1.0, "use_dc": True, "dc_power": 100.0}})
        r.safety.all_stop()
    rig.loop.tick_hooks.append(all_writes)
    rig.start()
    try:
        rig.loop.run(40)
    finally:
        rig.stop()
    checks = {k: G.checker(k, "bench", rig.cfg) for k in log}
    outside = [(k, b) for k, bs in log.items() for b in bs if checks[k](b)]
    assert outside == [], outside[:5]
    assert sum(len(v) for v in log.values()) > 50                     # 읽기는 나갔다(헛시험 아님)
    assert plc.write_funcs_seen() == [] and rig.sims["rfpulse"].set_commands_seen() == []
    assert sum(1 for x in results if getattr(x, "reason", None) == "BENCH_READ_ONLY") >= 10
    assert rig.plc.writes_allowed is False and rig.workers["plc"].writes_allowed is False


# ───────── sim(prod 목록): 금지 명령은 드라이버 경로로도 막힌다 ─────────
def test_forbidden_commands_blocked_before_port(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, cfg={"RFPULSE_ENABLED": True})
    log = _wire_log(rig)
    res = {}

    def submit(dev, name, fn):
        rig.workers[dev].submit(Request("write", fn, deadline=c.monotonic() + 3.0, name=name,
                                        on_done=lambda r, name=name: res.__setitem__(name, r)))

    def hook(t):
        if t.seq != 3:
            return
        submit("dc", "*SAV", lambda s: rig.dc._write(s, "*SAV 1"))
        submit("dc", "LAST-STA", lambda s: rig.dc._write(s, "FACT:LAST-STA FUL"))
        submit("mfc", "Q5", lambda s: rig.mfc._send(s, "Q5 10.0"))
        submit("mfc", "E", lambda s: rig.mfc._send(s, "E03"))
        submit("ig", "C", lambda s: s.write(b"C"))
        submit("plc", "itl", lambda s: rig.plc._client(s).write_coil(A["mv_interlock"], True))
        submit("rfpulse", "200", lambda s: s.write(aebus.build(1, 200)))
    rig.loop.tick_hooks.append(hook)
    rig.start()
    try:
        rig.loop.run(8)
    finally:
        rig.stop()
    assert set(res) == {"*SAV", "LAST-STA", "Q5", "E", "C", "itl", "200"}
    assert all(r.reason == "COMMAND_BLOCKED" for r in res.values()), {k: r.reason for k, r in res.items()}
    sent = b"".join(b for bs in log.values() for b in bs)
    for bad in (b"*SAV", b"FACT:LAST-STA F", b"Q5 ", b"E03", aebus.build(1, 200)):
        assert bad not in sent
    assert b"C" not in log["ig"]
    blocked = [e for e in rig.events if getattr(e, "kind", "") == "COMMAND_BLOCKED"]
    assert len(blocked) == 7 and all(w.connected for k, w in rig.workers.items())        # 연결은 유지


def test_driver_bench_lists_match_guard():
    from v2.chk.drivers import dc as DCD, ig as IGD, rfpulse as RFD
    assert DCD.BENCH_QUERIES == set(G.DC_BENCH)
    assert set(IGD.QUERY_COMMANDS) == set(G.IG_BENCH)
    assert set(RFD.BENCH_QUERIES) == set(G.RFP_BENCH)
