# -*- coding: utf-8 -*-
"""히터 워치독 하트비트(D00014) — 다른 스레드가 PLC 를 쓰는 중에도 건너뛰지 않는다(2026-10-14 RF 램프업 중 트립).
가짜 Modbus instrument 로만 시험한다(포트·장비 없음)."""
import threading
import time

import pytest
from PyQt6.QtCore import QEventLoop, QMetaObject, QThread, QTimer, Qt

import device.PLC as PLCM
from lib import config as CFG


class FakeSerial:
    is_open = True

    def reset_input_buffer(self):
        pass

    def close(self):
        pass


class WdFake:
    """트랜잭션마다 delay 초가 걸리는 가짜 PLC. D00014 쓰기 시각을 남긴다."""

    def __init__(self, delay=0.025):
        self.serial = FakeSerial()
        self.delay = delay
        self.regs = {}
        self.wd_times = []

    def _t(self):
        if self.delay:
            time.sleep(self.delay)

    def read_bits(self, addr, count, functioncode=1):
        self._t()
        return [0] * count

    def read_registers(self, addr, count, functioncode=3):
        self._t()
        return [self.regs.get(addr + i, 0) for i in range(count)]

    def read_register(self, addr, *a, **k):
        self._t()
        return self.regs.get(addr, 0)

    def write_register(self, addr, value, *a, **k):
        self._t()
        self.regs[addr] = value
        if addr == CFG.HEATER_REG_WD:
            self.wd_times.append(time.monotonic())

    def write_bit(self, addr, value, *a, **k):
        self._t()

    def write_bits(self, addr, values, *a, **k):
        self._t()


def spin(ms):
    loop = QEventLoop()
    QTimer.singleShot(int(ms), loop.quit)
    loop.exec()


def make_ctrl(fake):
    c = PLCM.PLCController()
    c.instrument = fake
    c._is_running = True
    c._ever_connected = True
    c._link_down = False
    return c


def hold_lock(c, seconds, started):
    """다른 스레드(RF 처럼)가 _busy 를 켜고 잠금을 seconds 동안 쥔다."""
    def _run():
        c._busy = True
        c._mutex.lock()
        started.set()
        time.sleep(seconds)
        c._busy = False
        c._mutex.unlock()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def test_kick_waits_for_lock_instead_of_skipping(qapp):
    fake = WdFake(delay=0)
    c = make_ctrl(fake)
    started = threading.Event()
    t = hold_lock(c, 0.3, started)
    started.wait(1)
    assert c._busy is True
    t0 = time.monotonic()
    c._kick_heater_watchdog()                       # 예전 코드는 여기서 _busy 로 return 했다
    t.join()
    assert len(fake.wd_times) == 1 and fake.wd_times[0] - t0 >= 0.25
    assert c.heater_wd_age() < 0.2


def test_kick_retries_after_lock_timeout(qapp):
    fake = WdFake(delay=0)
    c = make_ctrl(fake)
    started = threading.Event()
    t = hold_lock(c, 1.5, started)
    started.wait(1)
    t0 = time.monotonic()
    c._kick_heater_watchdog()                       # 1초 tryLock 실패 → 200 ms 재시도 예약
    assert fake.wd_times == [] and c._heater_wd_retry_timer.isActive()
    assert time.monotonic() - t0 >= 0.95
    spin(1200)                                      # 잠금이 풀린 뒤 재시도가 쓴다
    t.join()
    assert len(fake.wd_times) == 1 and fake.wd_times[0] - t0 >= 1.5


def test_no_heartbeat_while_link_down(qapp):
    fake = WdFake(delay=0)
    c = make_ctrl(fake)
    c._link_down = True
    c._heater_wd_last_ok = time.monotonic() - 5
    c._kick_heater_watchdog()
    c._poll_status()
    assert fake.wd_times == []


def test_poll_forces_heartbeat_after_force_sec(qapp):
    fake = WdFake(delay=0)
    c = make_ctrl(fake)
    c._heater_wd_last_ok = time.monotonic() - 1.0
    c._poll_status()
    assert fake.wd_times == []                      # 2초 전이면 쓰지 않는다
    c._heater_wd_last_ok = time.monotonic() - float(CFG.HEATER_WD_FORCE_SEC)
    v0 = c._heater_wd
    c._poll_status()
    assert len(fake.wd_times) == 1 and c._heater_wd == (v0 + 1) & 0x7FFF
    assert fake.regs[CFG.HEATER_REG_WD] == c._heater_wd and c.heater_wd_age() < 0.2


def run_rf_load(ctrl_cls, seconds, delay=0.025):
    """RF 램프업 흉내: 다른 스레드가 매초 send_rfpower_command(3트랜잭션) + read_rf_feedback(2트랜잭션)을
    직접 부르고, PLC 스레드는 200 ms 폴링 + 3초 하트비트. D00014 쓰기 간격 최댓값(초)을 돌려준다."""
    fake = WdFake(delay=delay)
    c = ctrl_cls()
    c.instrument = fake
    c._is_running = True
    c._ever_connected = True
    c._link_down = False
    th = QThread()
    c.moveToThread(th)
    th.start()
    t_start = time.monotonic()
    QMetaObject.invokeMethod(c.polling_timer, "start", Qt.ConnectionType.QueuedConnection)
    QMetaObject.invokeMethod(c._heater_wd_timer, "start", Qt.ConnectionType.QueuedConnection)
    stop = threading.Event()

    def _rf():
        while not stop.is_set():
            t0 = time.monotonic()
            c.send_rfpower_command(800)
            c.read_rf_feedback()
            stop.wait(max(0.0, 1.0 - (time.monotonic() - t0)))
    rf = threading.Thread(target=_rf, daemon=True)
    rf.start()
    time.sleep(seconds)
    stop.set()
    rf.join(3)
    t_end = time.monotonic()
    QMetaObject.invokeMethod(c.polling_timer, "stop", Qt.ConnectionType.QueuedConnection)
    QMetaObject.invokeMethod(c._heater_wd_timer, "stop", Qt.ConnectionType.QueuedConnection)
    th.quit()
    th.wait(5000)
    times = [t_start] + [t for t in fake.wd_times if t <= t_end] + [t_end]
    return max(b - a for a, b in zip(times, times[1:])), len(fake.wd_times)


def test_heartbeat_gap_under_rf_rampup_load_60s(qapp):
    """재현 시험 — 트랜잭션 40 ms(115200 bps + USB 지연 수준). 같은 부하에서 수정 전 코드(451592a)는
    하트비트를 건너뛰어 최대 간격 3.40초·60초 동안 쓰기 19회, 수정 후 2.65초·39회였다(2026-10-14 개발 PC)."""
    gap, n = run_rf_load(PLCM.PLCController, 60.0, delay=0.04)
    assert n >= 20
    assert gap <= 3.0, f"최대 하트비트 간격 {gap:.2f}초"


# ───────────── 진단 ─────────────
def _recorder(c):
    rec = {"msg": [], "ev": [], "fault": []}
    c.status_message.connect(lambda lv, m: rec["msg"].append((lv, m)))
    c.plc_event.connect(lambda r: rec["ev"].append(r))
    c.heater_fault.connect(lambda why: rec["fault"].append(why))
    return rec


def test_delay_warning_once_and_recovery_once(qapp):
    fake = WdFake(delay=0)
    c = make_ctrl(fake)
    rec = _recorder(c)
    c._heater_wd_last_ok = time.monotonic() - 6.2
    c._check_heater_wd_delay()
    c._check_heater_wd_delay()
    warns = [m for lv, m in rec["msg"] if "하트비트 지연" in m]
    assert len(warns) == 1 and warns[0].startswith("히터 워치독 하트비트 지연 6.") and warns[0].endswith("초 (래더 10초에 트립)")
    assert [lv for lv, m in rec["msg"] if "하트비트 지연" in m] == ["히터(경고)"]   # '히터…' 레벨 → 히터 로그에도
    c._kick_heater_watchdog()
    c._kick_heater_watchdog()
    recov = [m for lv, m in rec["msg"] if "하트비트 회복" in m]
    assert len(recov) == 1 and recov[0].startswith("히터 워치독 하트비트 회복 (지연 최대 6.")
    kinds = [e["종류"] for e in rec["ev"]]
    assert kinds == ["워치독지연", "워치독회복"]


def test_delay_checked_even_when_poll_skipped_by_busy(qapp):
    fake = WdFake(delay=0)
    c = make_ctrl(fake)
    rec = _recorder(c)
    c._busy = True
    c._heater_wd_last_ok = time.monotonic() - 7.0
    c._poll_status()                                # _busy 로 폴링은 건너뛰지만 나이는 본다
    assert any("하트비트 지연 7." in m for _, m in rec["msg"])
    assert fake.wd_times == []


def test_no_delay_warning_below_5s(qapp):
    c = make_ctrl(WdFake(delay=0))
    rec = _recorder(c)
    c._heater_wd_last_ok = time.monotonic() - 4.5
    c._check_heater_wd_delay()
    assert rec["msg"] == [] and rec["ev"] == []


class FaultFake(WdFake):
    def read_bits(self, addr, count, functioncode=1):
        bits = [0] * count
        if addr == CFG.HEATER_COIL_BASE:
            bits[CFG.HEATER_COIL_ITL - addr] = 1
            bits[CFG.HEATER_COIL_FAULT - addr] = 1
            bits[CFG.HEATER_COIL_WD_ERR - addr] = 1
        return bits


def test_trip_message_has_heartbeat_age(qapp):
    c = make_ctrl(FaultFake(delay=0))
    rec = _recorder(c)
    c._heater_wd_last_ok = time.monotonic() - 1.0
    c._poll_status()
    assert rec["fault"] and "히터 통신 워치독 타임아웃 (마지막 하트비트 성공 후 1." in rec["fault"][0]


def test_heater_csv_wd_age_column(tmp_path, monkeypatch):
    import lib.heater_logger as HL
    import csv
    monkeypatch.setattr(HL, "NAS_HEATER_LOG_DIR", tmp_path)
    lg = HL.HeaterCsvLogger()
    p = lg.start("WDTEST")
    lg.write_row({"pv": 600.0, "sv": 600.0}, "", None, None, wd_age=6.24)
    lg.write_row({"pv": 600.0, "sv": 600.0})
    lg.stop()
    with open(p, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0][-1] == "WD age[s]" and len(rows[0]) == len(HL.COLUMNS)
    assert rows[1][-1] == "6.2" and rows[2][-1] == "" and all(len(r) == len(rows[0]) for r in rows)


def test_main_passes_wd_age_to_heater_csv():
    import os
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py"), encoding="utf-8").read()
    assert src.count("self._heater_logger.write_row(") == src.count("wd_age=self._heater_wd_age()") == 2
