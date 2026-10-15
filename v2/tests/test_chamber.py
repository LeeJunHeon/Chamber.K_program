"""공용 챔버 모델 시험 — 압력 1차 모델과 DC 플라즈마 V-I."""
import pytest

from v2.chk.sim.chamber import Chamber


def test_pressure_follows_valves_and_gas():
    ch = Chamber()
    ch.gas_sccm["ar"] = 20.0
    ch.advance_to(30.0)
    assert ch.pressure == pytest.approx(2e-3, rel=0.05)              # 20 sccm ≈ 2 mTorr
    ch.set_valves(mv=False, vent=True)
    ch.advance_to(200.0)
    assert ch.pressure > 700.0
    ch.set_valves(vent=False, rv=True)
    ch.advance_to(400.0)
    assert ch.pressure == pytest.approx(0.05, rel=0.05)


def test_advance_is_idempotent():
    ch = Chamber()
    ch.gas_sccm["ar"] = 10.0
    ch.advance_to(5.0)
    p = ch.pressure
    ch.advance_to(5.0)
    ch.advance_to(3.0)
    assert ch.pressure == p


def test_dc_plasma_vi():
    ch = Chamber()
    assert ch.dc_operating_point(True, 600.0, 0.5) == (600.0, 0.0)  # 가스 없음 → 무부하
    ch.gas_sccm["ar"] = 20.0
    ch.advance_to(30.0)
    v, i = ch.dc_operating_point(True, 600.0, 0.5)
    vp = 350 + 150 / (ch.pressure_mtorr ** 0.5)
    assert i == pytest.approx(0.5) and v == pytest.approx(vp + 25.0) and ch.dc_ignited
    assert ch.dc_operating_point(False, 600.0, 0.5) == (0.0, 0.0) and not ch.dc_ignited
    ch.ignite_fail = True
    assert ch.dc_operating_point(True, 600.0, 0.5) == (600.0, 0.0)
    ch.ignite_fail = False
    ch.low_current = True
    assert ch.dc_operating_point(True, 600.0, 0.5)[1] == pytest.approx(0.05)
    ch.low_current, ch.v_limit = False, True
    v, i = ch.dc_operating_point(True, 600.0, 0.5)
    assert v == 600.0 and 0 <= i < 0.5                                # 전압 한계(CV) — 전류 모자람
