"""쓰기 명령 글자 — 같은 입력에서 v2 가 장비에 보내는 바이트가 v1 과 같다(사용자 지시 5-3 ⑤).

v1 을 import 하지 않는다 — v1 소스의 형식(파일:줄)을 아래 기대값 함수로 옮겨 적고, v2 의 명령 글자 함수와
실제 드라이버(시뮬레이터에 보낸 바이트)를 둘 다 비교한다.
"""
import pytest

from v2.chk.drivers.dc import dc_appl_cmd, dc_curr_cmd
from v2.chk.drivers.mfc import flow_cmd, sp_cmd
from v2.tests.test_dc import Rig as DcRig
from v2.tests.test_ig import Rig as IgRig
from v2.tests.test_mfc import Rig as MfcRig

# ───────── v1 형식(그대로 옮김) ─────────
# MFC — lib/config.py:811-834 MFC_COMMANDS, 종단 '\r'(device/MFC.py:355-356 enqueue)
V1_MFC = {
    "SET_ONOFF_MASK": lambda bits: f"L0{bits}",                       # config.py:812
    "MFC_ZEROING": lambda channel: f"L{4+channel} 1",                 # config.py:815
    "FLOW_SET": lambda channel, value: f"Q{channel} {value}",          # config.py:816 — value = float(params["value"]), MFC.py:609
    "VALVE_OPEN": "O", "VALVE_CLOSE": "C", "PS_ZEROING": "Z1",         # config.py:825-827
    "SP1_ON": "D1", "SP2_ON": "D2", "SP3_ON": "D3", "SP4_ON": "D4",    # config.py:828-831
    "SP1_SET": lambda value: f"S1 {value}", "SP2_SET": lambda value: f"S2 {value}",   # config.py:832-835
    "SP3_SET": lambda value: f"S3 {value}", "SP4_SET": lambda value: f"S4 {value}",
}
V1_MFC_PRESSURE_SCALE, V1_MFC_PRESSURE_DECIMALS = 0.1, 2              # config.py:806-807


def v1_sp_value(ui_mtorr):
    """MFC.py:551 _to_hw_pressure(ui × 0.1) → 644·663·686 f"{hw_val:.{dec}f}"."""
    hw = ui_mtorr * V1_MFC_PRESSURE_SCALE
    return f"{hw:.{int(V1_MFC_PRESSURE_DECIMALS)}f}"


def v1_mfc_bytes(cmd):
    return (cmd + "\r").encode("ascii")


# DC — device/DCpower.py, 종단 '\n'(DCpower.py:880 _write_line)
def v1_dc(cmd):
    return (cmd.rstrip("\n") + "\n").encode("ascii")


V1_APPLY = lambda v, i: f"APPLy {v:.2f},{i:.4f}"                       # DCpower.py:682
V1_CURR = lambda i: f"CURR {i:.4f}"                                     # DCpower.py:590
# IG — device/IonGauge.py:539 self.serial.write(cmd.encode("ascii")), 필라멘트 "A"·"B"(779·787)

FLOWS = [20, 20.0, 14, 14.55, 0.5, 99.99, 5.25, 1.0, 100.0]
SPS = [5.0, 15.0, 3.0, 2.5, 7.3, 30.0, 1.25, 0.5]


# ───────── 글자 함수 ─────────
@pytest.mark.parametrize("ch", [1, 2])
@pytest.mark.parametrize("value", FLOWS)
def test_mfc_flow_set_same_as_v1(ch, value):
    cmd, _ = flow_cmd(ch, value, 100.0)                                # FS 100(10/20 bench) — %FS = sccm
    assert cmd == V1_MFC["FLOW_SET"](channel=ch, value=float(value))


@pytest.mark.parametrize("n", [1, 2, 3, 4])
@pytest.mark.parametrize("mtorr", SPS)
def test_mfc_sp_set_same_as_v1(n, mtorr):
    cmd, _ = sp_cmd(n, mtorr, 1.0)                                     # FS 1 Torr(E11·F01, 10/20 bench)
    assert cmd == V1_MFC[f"SP{n}_SET"](value=v1_sp_value(mtorr))


@pytest.mark.parametrize("v,i", [(600.0, 0.1), (600.0, 0.0123), (450.0, 0.25), (599.995, 1.0), (0.0, 0.0)])
def test_dc_apply_and_curr_same_as_v1(v, i):
    assert dc_appl_cmd(v, i) == V1_APPLY(v, i)
    assert dc_curr_cmd(i) == V1_CURR(i)


# ───────── 실제 드라이버가 보낸 바이트 ─────────
def _sent(sim, t0):
    return [d for tt, d in sim.received if tt >= t0 and not d.startswith(b"R")]


def test_mfc_driver_bytes_same_as_v1():
    r = MfcRig()
    t0 = r.c.monotonic()
    assert r.seq(r.d.set_flow, 1, 14.55) is None
    assert r.seq(r.d.set_channel, 1, True) is None
    assert r.seq(r.d.zero, "L5 1") is None
    assert r.seq(r.d.zero, "Z1") is None
    assert r.seq(r.d.set_sp, 1, 5.0) is None
    assert r.seq(r.d.set_sp, 3, 15.0) is None
    assert r.seq(r.d.select_sp, 1) is None
    assert r.seq(r.d.valve, True) is None
    assert r.seq(r.d.set_channel, 1, False) is None
    exp = [V1_MFC["FLOW_SET"](channel=1, value=14.55), V1_MFC["SET_ONOFF_MASK"]("1000"), V1_MFC["MFC_ZEROING"](channel=1),
           V1_MFC["PS_ZEROING"], V1_MFC["SP1_SET"](value=v1_sp_value(5.0)), V1_MFC["SP3_SET"](value=v1_sp_value(15.0)),
           V1_MFC["SP1_ON"], V1_MFC["VALVE_OPEN"], V1_MFC["SET_ONOFF_MASK"]("0000")]
    assert _sent(r.sim, t0) == [v1_mfc_bytes(c) for c in exp]
    r.done()


def test_dc_driver_bytes_same_as_v1():
    r = DcRig()
    t0 = r.c.monotonic()
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.1, on_done=on_done))
    r.d.set_current(0.1234)
    r.c.wait_until(r.c.monotonic() + 1.0)
    assert r.seq_wait(r.d.output_off_confirmed) is None
    sent = [d for tt, d in r.sim.received if tt >= t0 and not d.strip().endswith(b"?")]
    assert sent == [v1_dc(c) for c in ("*RST", "*CLS", V1_APPLY(600.0, 0.1), "OUTP ON", V1_CURR(0.1234), "OUTP OFF")]
    r.done()


def test_ig_filament_bytes_same_as_v1():
    r = IgRig()
    t0 = r.c.monotonic()
    r.d.filament_on()
    r.c.wait_until(r.c.monotonic() + 15.0)
    r.d.filament_off()
    r.c.wait_until(r.c.monotonic() + 15.0)
    sent = [d for tt, d in r.sim.received if tt >= t0 and d in (b"A", b"B")]
    assert sent == ["A".encode("ascii"), "B".encode("ascii")]
    r.w.stop()


# ───────── 10/20 bench 실측 반영 ─────────
def test_bench_2026_10_20_values_recorded():
    import re
    from v2.chk import devices as D
    from v2.chk import inventory as INV
    assert (D.DC_RATED_VOLTAGE_V, D.DC_RATED_CURRENT_A, D.DC_RATED_POWER_W) == (630.0, 2.1, 1200.0)   # EX 600-2
    assert INV.DC_SPEC["EX 600-2"] == (630.0, 2.1)
    assert re.fullmatch(INV.DC_FORMAT["APPL?"][0], '"600.0000,0.1230"')                         # 따옴표 붙은 응답
    sc = INV.mfc_scale("E11", "F01")
    assert sc is not None and abs(sc[2] - 1.0) < 1e-12                                            # 1000 mTorr = 1 Torr
    assert INV.mfc_int("W51", "W5") == 1 and INV.mfc_int("W61", "W6") == 1                        # SCCM
    assert "D00016" in D.PLC.timing_manual and "M006" in D.MFC.timing_manual


def test_mfc_r37_local_is_info_only():
    r = MfcRig()                                         # 시뮬레이터 R37 = 'M006'(10/20 bench)
    assert r.d.scale_mismatch is None
    assert [e for e in r.events if e.kind == "remote_state"]
    r.done()
