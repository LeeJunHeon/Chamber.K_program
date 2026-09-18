# -*- coding: utf-8 -*-
"""T25 — DC/MFC/RFpulse: 포트 open 성공 + 무응답이면 정책은 리셋되지 않고 지연이 계속 는다.
응답(_comm_ok / 응답 라인) 뒤에만 in_outage() False. QSerialPort 는 가짜, QTimer.singleShot 은 기록만."""
import pytest
from PyQt6.QtSerialPort import QSerialPortInfo

import device.DCpower as DCM
import device.MFC as MFCM
import device.RFpulse as RFM


class FakeQSerial:
    """open() 은 성공하지만 아무 응답도 하지 않는 포트."""
    def __init__(self):
        self._open = False; self.opens = 0

    def isOpen(self): return self._open
    def open(self, *a): self._open = True; self.opens += 1; return True
    def close(self): self._open = False
    def setPortName(self, n): pass
    def setDataTerminalReady(self, v): pass
    def setRequestToSend(self, v): pass
    def clear(self, *a): pass
    def errorString(self): return ""
    def baudRate(self): return 9600
    def dataBits(self): return type("E", (), {"value": 8})()
    def parity(self): return "odd"
    def stopBits(self): return type("E", (), {"value": 1})()
    def write(self, b): return len(b)
    def flush(self): pass
    def readAll(self): return b""


class _Port:
    def __init__(self, n): self._n = n
    def portName(self): return self._n


@pytest.fixture
def shots(monkeypatch):
    rec = []
    for mod in (DCM, MFCM, RFM):
        monkeypatch.setattr(mod.QTimer, "singleShot", staticmethod(lambda ms, fn: rec.append((ms, fn))))
    monkeypatch.setattr(QSerialPortInfo, "availablePorts",
                        staticmethod(lambda: [_Port(DCM.DC_PORT), _Port(MFCM.MFC_PORT), _Port(RFM.RFPULSE_PORT)]))
    return rec


def _reconnect_delays(shots):
    return [ms for ms, fn in shots if getattr(fn, "__name__", "") == "_try_reconnect"]


def test_T25_dcpower_silent_port(qapp, shots, monkeypatch):
    monkeypatch.setattr(DCM, "DC_COMM_REOPEN_SEC", 0.0)
    c = DCM.DCPowerController()
    c.serial = FakeQSerial()
    assert c.connect_dcpower_device() is True
    assert not c._policy.in_outage()
    for _ in range(DCM.DC_COMM_FAIL_RECONNECT_STREAK):
        c._comm_fail("timeout")                                  # 3회 무응답 → 닫고 재연결 예약
    assert c._policy.in_outage() and _reconnect_delays(shots) == [1000]
    shots[-1][1]()                                               # 재연결: 포트는 열림, 장비는 침묵
    assert c.serial.isOpen() and c._policy.in_outage()
    for _ in range(DCM.DC_COMM_FAIL_RECONNECT_STREAK):
        c._comm_fail("timeout")
    assert _reconnect_delays(shots) == [1000, 2000]
    shots[-1][1]()
    for _ in range(DCM.DC_COMM_FAIL_RECONNECT_STREAK):
        c._comm_fail("timeout")
    assert _reconnect_delays(shots) == [1000, 2000, 4000]        # 두 번 연속 증가
    c._comm_ok()                                                 # 실제 응답
    assert not c._policy.in_outage()


def test_T25_mfc_silent_port_and_no_reply_path(qapp, shots):
    c = MFCM.MFCController()
    c.serial_mfc = FakeQSerial()
    c._ensure_timers_created()
    c._want_connected = True
    assert c._open_port() is True and not c._policy.in_outage()
    fake = c.serial_mfc
    got = []
    cmd = MFCM.Command("R60", lambda r: got.append(r), 500, 50, "t", retries_left=5, allow_no_reply=False)
    # 명령 무응답 경로(_finish_command(None)): 포트 닫고 스케줄러 예약 — 직접 open 0회
    c._inflight = cmd; opens0 = fake.opens
    c._finish_command(None)
    assert fake.opens == opens0 and not fake.isOpen()
    assert _reconnect_delays(shots) == [1000] and c._policy.in_outage()
    shots[-1][1]()                                               # 예약된 재연결 → open 1회(침묵)
    assert fake.opens == opens0 + 1 and fake.isOpen()
    c._inflight = cmd; c._finish_command(None)
    assert _reconnect_delays(shots) == [1000, 2000]
    shots[-1][1]()
    c._inflight = cmd; c._finish_command(None)
    assert _reconnect_delays(shots) == [1000, 2000, 4000]
    shots[-1][1]()
    c._inflight = cmd; c._finish_command("+1.000")               # 응답 라인
    assert got == ["+1.000"] and not c._policy.in_outage()


def test_T25_rfpulse_silent_port(qapp, shots, monkeypatch):
    c = RFM.RFPulseController()
    c.serial_rfp = FakeQSerial()
    c._want_connected = True
    assert c._open_port() is True and not c._policy.in_outage()
    monkeypatch.setattr(RFM, "RFPULSE_COMM_REOPEN_SEC", 0.0)
    monkeypatch.setattr(RFM, "RFPULSE_COMM_LOSS_ABORT_SEC", 1e9)
    c._comm_last_ok = 0.0; c._comm_last_reopen_t = -1e9
    c._check_comm_budget()                                       # 무응답 → 닫고 스케줄러 예약
    assert not c.serial_rfp.isOpen() and _reconnect_delays(shots) == [1000] and c._policy.in_outage()
    shots[-1][1]()                                               # 포트 열림(침묵)
    assert c.serial_rfp.isOpen() and c._policy.in_outage()
    c._comm_last_reopen_t = -1e9; c._check_comm_budget()
    assert _reconnect_delays(shots) == [1000, 2000]
    shots[-1][1]()
    c._comm_last_reopen_t = -1e9; c._check_comm_budget()
    assert _reconnect_delays(shots) == [1000, 2000, 4000]
    c._comm_ok("poll")
    assert not c._policy.in_outage()


def test_T60_dcpower_connect_on_open_port_is_noop(qapp, shots):
    c = DCM.DCPowerController()
    c.serial = FakeQSerial()
    msgs = []
    c.status_message.connect(lambda l, m: msgs.append(m))
    assert c.connect_dcpower_device() is True and c.serial.opens == 1
    msgs.clear()
    assert c.connect_dcpower_device() is True                 # 이미 열림 → 다시 열지 않고 True
    assert c.serial.opens == 1 and msgs == []
