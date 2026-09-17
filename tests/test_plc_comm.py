# -*- coding: utf-8 -*-
"""T3 폴링 블로킹 상한 / T4 cleanup 상한 / T5 잔존 RUN — 가짜 minimalmodbus.Instrument."""
import threading
import time

import pytest

import device.PLC as PLCM
from lib import config as CFG


class FakeSerial:
    def __init__(self, close_sleep=0.0):
        self.is_open = True
        self.close_sleep = close_sleep
        self.close_started = threading.Event()

    def close(self):
        self.close_started.set()
        if self.close_sleep:
            time.sleep(self.close_sleep)
        self.is_open = False

    def reset_input_buffer(self):
        pass


class FakeInstrument:
    """시나리오: fail_reads(모든 read 가 0.5초 자고 예외) / run_bit / pv_raw / write_bit_fail_once."""
    scenario = {}
    ctor_fail = False

    def __init__(self, *a, **k):
        if FakeInstrument.ctor_fail:
            raise OSError("could not open port")
        self.serial = FakeSerial(FakeInstrument.scenario.get("close_sleep", 0.0))
        self.calls = []
        self.close_port_after_each_call = False
        self.clear_buffers_before_each_transaction = True
        self.handle_local_echo = False
        FakeInstrument.scenario.setdefault("instances", []).append(self)

    def _maybe_fail(self):
        if FakeInstrument.scenario.get("fail_reads"):
            time.sleep(0.5)
            raise OSError("No communication with the instrument (no answer)")

    def read_bits(self, addr, count, functioncode=1):
        self.calls.append(("read_bits", addr, count)); self._maybe_fail()
        if addr == CFG.HEATER_COIL_BASE:
            bits = [0] * count
            bits[CFG.HEATER_COIL_RUN - CFG.HEATER_COIL_BASE] = int(FakeInstrument.scenario.get("run_bit", 0))
            bits[1] = 1   # ITL
            return bits
        return [0] * count

    def read_registers(self, addr, count, functioncode=3):
        self.calls.append(("read_registers", addr, count)); self._maybe_fail()
        regs = [0] * count
        if addr == CFG.HEATER_REG_BLOCK_START:
            regs[CFG.HEATER_REG_PV - addr] = int(FakeInstrument.scenario.get("pv_raw", 0))
            regs[CFG.HEATER_REG_SV - addr] = int(FakeInstrument.scenario.get("sv_raw", 0))
        return regs

    def read_register(self, addr, dec=0, functioncode=3, signed=False):
        self.calls.append(("read_register", addr)); self._maybe_fail()
        return 0

    def write_bit(self, addr, value, functioncode=5):
        self.calls.append(("write_bit", addr, int(value)))
        n = FakeInstrument.scenario.get("write_bit_fail_n", 0)
        if n > 0:
            FakeInstrument.scenario["write_bit_fail_n"] = n - 1
            raise OSError("write failed")
        if FakeInstrument.scenario.get("write_bit_fail_always"):
            raise OSError("write failed")

    def write_register(self, addr, value, functioncode=6):
        self.calls.append(("write_register", addr, int(value)))

    def write_bits(self, addr, values):
        self.calls.append(("write_bits", addr, list(values)))


@pytest.fixture
def plc(qapp, monkeypatch):
    FakeInstrument.scenario = {}
    FakeInstrument.ctor_fail = False
    monkeypatch.setattr(PLCM.minimalmodbus, "Instrument", FakeInstrument)
    monkeypatch.setattr(PLCM, "PLC_RETRY_DELAY_MS", 0)
    c = PLCM.PLCController()
    msgs = []
    c.status_message.connect(lambda l, m: msgs.append((l, m)))
    c._msgs = msgs
    c.start_polling()
    c.polling_timer.stop(); c._heater_wd_timer.stop()
    return c


def test_T3_poll_blocking_bound_and_link_down(plc, monkeypatch):
    FakeInstrument.scenario["fail_reads"] = True
    ticks = []
    orig = plc._comm_budget_tick
    monkeypatch.setattr(plc, "_comm_budget_tick", lambda ex, what: (ticks.append(what), orig(ex, what)))
    shots = []
    monkeypatch.setattr(PLCM.QTimer, "singleShot", staticmethod(lambda ms, fn: shots.append((ms, fn))))

    inst = plc.instrument
    t0 = time.monotonic()
    plc._poll_status()
    took = time.monotonic() - t0
    assert took < 2.0, f"_poll_status {took:.2f}s"
    assert len(ticks) == 1
    assert plc._link_down is True
    assert shots and shots[-1][0] == 1000

    # 두 번째 폴링부터는 링크 다운 → 트랜잭션 0회
    n_calls = len(inst.calls)
    plc._poll_status()
    assert len(inst.calls) == n_calls
    assert len(ticks) == 2                       # 예산 판정은 계속 돈다

    # 재연결 예약: 1000 → 2000 → 4000 (포트가 계속 안 열린다)
    FakeInstrument.ctor_fail = True
    delays = [shots[-1][0]]
    for _ in range(2):
        fn = shots[-1][1]; fn()
        delays.append(shots[-1][0])
    assert delays == [1000, 2000, 4000]
    assert plc._link_down is True
    # 포트가 돌아오면 성공 → 링크 복구, 정책 리셋
    FakeInstrument.ctor_fail = False
    FakeInstrument.scenario["fail_reads"] = False
    shots[-1][1]()
    assert plc._link_down is False and plc.instrument is not None
    assert any("재연결(" in m for _, m in plc._msgs)


def test_T4_cleanup_bounded_and_warns_when_off_unconfirmed(plc):
    FakeInstrument.scenario["write_bit_fail_always"] = True
    plc.instrument.serial.close_sleep = 5.0
    t0 = time.monotonic()
    plc.cleanup()
    took = time.monotonic() - t0
    assert took < 2.5, f"cleanup {took:.2f}s"
    assert any("종료 시 히터 OFF 미확인" in m for _, m in plc._msgs)
    assert plc.instrument is None


def test_T4_cleanup_normal_writes_run_off(plc):
    inst = plc.instrument
    plc.cleanup()
    assert ("write_bit", CFG.HEATER_COIL_RUN, 0) in inst.calls
    assert not any("미확인" in m for _, m in plc._msgs)


def test_T5_residual_run_cleared_in_order(plc):
    FakeInstrument.scenario.update(run_bit=1, pv_raw=2750, sv_raw=6000)
    inst = plc.instrument
    published = []; residual = []
    plc.update_heater_status.connect(lambda st: published.append(dict(st)))
    plc.heater_residual.connect(lambda d: residual.append(dict(d)))
    n0 = len(inst.calls)
    plc._poll_status()
    writes = [c for c in inst.calls[n0:] if c[0] in ("write_bit", "write_register")]
    assert writes[:2] == [("write_bit", CFG.HEATER_COIL_RUN, 0), ("write_register", CFG.HEATER_REG_SV, 2750)]
    assert published and published[-1]["run"] is False and published[-1].get("residual_run") is True
    assert residual == [{"pv": pytest.approx(275.0), "sv_old": pytest.approx(600.0)}]
    assert plc._heater_first_poll is False


def test_T5_residual_retry_when_first_write_fails(plc):
    # _mb 가 3회(1+PLC_RETRY_COUNT) 재시도하므로 3회 전부 실패해야 '이번 폴링 실패' 가 된다
    FakeInstrument.scenario.update(run_bit=1, pv_raw=2750, sv_raw=6000, write_bit_fail_n=3)
    inst = plc.instrument
    published = []; residual = []
    plc.update_heater_status.connect(lambda st: published.append(dict(st)))
    plc.heater_residual.connect(lambda d: residual.append(dict(d)))
    plc._poll_status()                     # write_bit 재시도 소진 → 발행 없음
    assert published == [] and residual == []
    assert plc._heater_first_poll is True
    assert any("이전 세션 RUN 정리 실패" in m for _, m in plc._msgs)
    plc._poll_status()                     # 다음 폴링에서 재시도 성공
    assert len(residual) == 1 and published[-1]["run"] is False
    wb = [c for c in inst.calls if c[0] == "write_bit"]
    assert len(wb) == 4                    # 3회 실패 + 다음 폴링 1회 성공
