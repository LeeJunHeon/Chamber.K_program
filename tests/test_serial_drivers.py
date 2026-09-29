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


# ═══════════════ T92~ DC 출력 OFF 추적 · ALL STOP 직결 OFF · 재진입 방지 ═══════════════
class DCFakeSerial(FakeQSerial):
    """DC 드라이버가 쓰는 메서드까지 갖춘 가짜 포트(쓰기는 _write_line 을 가로채므로 여기선 무시)."""
    def bytesAvailable(self): return 0
    def waitForBytesWritten(self, ms): return True
    def errorString(self): return ""


class DCHarness:
    """DCPowerController + 가짜 포트. _readline_blocking 은 스크립트 응답으로, singleShot 은 기록만.
    availablePorts 는 conftest 가 [] 로 막아 둔 상태를 그대로 쓴다(테스트가 필요하면 자기 것으로 덮는다)."""
    def __init__(self, monkeypatch, replies=None, open_port=True, tmp_now=None):
        self.t = 1000.0
        self.c = DCM.DCPowerController()
        self.sent = []; self.msgs = []; self.events = []; self.shots = []
        self.replies = list(replies or [])
        self.c.status_message.connect(lambda l, m: self.msgs.append((l, m)))
        self.c.comm_event.connect(lambda d: self.events.append((d.get("종류"), d.get("상세"))))
        self.unconf = []; self.conf = []
        self.c.off_unconfirmed.connect(lambda w: self.unconf.append(w))
        self.c.off_confirmed.connect(lambda w, sec: self.conf.append((w, sec)))
        monkeypatch.setattr(DCM.QTimer, "singleShot", staticmethod(lambda ms, fn: self.shots.append((ms, fn))))
        monkeypatch.setattr(DCM.time, "monotonic", lambda: self.t)
        monkeypatch.setattr(DCM.time, "sleep", lambda s: None)
        monkeypatch.setattr(self.c, "_write_line", self._write)
        monkeypatch.setattr(self.c, "_readline_blocking", lambda timeout_ms=500: self._read())
        if open_port:
            self.c.serial = DCFakeSerial(); self.c.serial.open()
            self.c._want_connected = True

    def _write(self, line):
        self.sent.append(line)
        self.c._last_write_t = self.t          # 실제 _write_line 이 하는 일(간격 계산의 기준)
        return True

    def _read(self):
        return self.replies.pop(0) if self.replies else None

    def writes(self, kind=None):
        return [w for w in self.sent if kind is None or w.startswith(kind)]


def test_T92_emergency_off_sends_and_verifies(qapp, monkeypatch):
    h = DCHarness(monkeypatch, replies=["0"])
    h.c._is_running = True; h.c.control_timer.start(); h.c._output_maybe_on = True
    h.c.emergency_off()
    assert h.sent == ["OUTP OFF", "OUTP?"]
    assert h.c._is_running is False and h.c.state == "IDLE" and not h.c.control_timer.isActive()
    assert h.c._output_maybe_on is False and h.c._off_unconfirmed is False and h.unconf == []
    assert any("ALL STOP — DC 출력 OFF 요청" in m for _, m in h.msgs)


def test_T93_emergency_off_without_port_is_quiet(qapp, monkeypatch):
    """이번 실행에서 DC 를 쓴 적 없고 포트도 없음(conftest: availablePorts=[]) → 경고·이벤트·시그널 없음."""
    h = DCHarness(monkeypatch, open_port=False)
    h.c.emergency_off()
    assert h.sent == [] and h.unconf == [] and h.events == []
    assert any(l == "DCpower" and "이번 실행에서 DC 출력을 켠 적 없음" in m for l, m in h.msgs)
    assert not any(l.endswith("(경고)") for l, _ in h.msgs)


def test_T94_emergency_off_temp_open_is_restored(qapp, monkeypatch):
    """연결한 적 없음 + 포트는 있으나 무응답 → 포트를 되닫고 _want_connected/_comm_fail_streak 복원, 재연결 미예약."""
    h = DCHarness(monkeypatch, replies=[None, None], open_port=False)
    fake = DCFakeSerial()

    def _connect():
        h.c.serial = fake; fake.open(); h.c._want_connected = True; return True
    monkeypatch.setattr(h.c, "connect_dcpower_device", _connect)
    assert h.c._want_connected is False
    h.c.emergency_off()
    assert fake.isOpen() is False and h.c._want_connected is False and h.c._comm_fail_streak == 0
    assert [ms for ms, fn in h.shots if getattr(fn, "__name__", "") == "_try_reconnect"] == []


def test_T95_off_unconfirmed_episode_retry_and_confirm(qapp, monkeypatch):
    h = DCHarness(monkeypatch, replies=[None, None])
    h.c._output_maybe_on = True
    r = h.c._output_off_confirmed("공정 종료")
    assert r is False and h.unconf == ["공정 종료"] and h.c._off_unconfirmed is True
    assert h.c._outage_probe.isActive() and h.c._off_retry_sec == DCM.DC_OFF_RETRY_START_SEC
    warn = [m for l, m in h.msgs if l == "DCpower(경고)"]
    assert len(warn) == 1 and "OFF 미확인" in warn[0]
    # 재시도 간격: 실패마다 2배, COMM_OUTAGE_LOG_SEC 에서 캡
    seen = [h.c._off_retry_sec]
    for _ in range(8):
        h.t = h.c._off_retry_next
        h.replies = [None, None]
        h.c.serial.open(); h.c._comm_fail_streak = 0        # 무응답 3회면 드라이버가 포트를 닫는다 — 재연결됐다고 본다
        h.c._outage_probe_tick()
        seen.append(h.c._off_retry_sec)
    assert seen[:4] == [10.0, 20.0, 40.0, 80.0] and seen[-1] == float(DCM.COMM_OUTAGE_LOG_SEC)
    assert h.unconf == ["공정 종료"]                       # 에피소드당 1회
    # 정상 응답이 오면 확인 1회 + 플래그 해제 + 프로브 정지
    h.t = h.c._off_retry_next
    h.replies = ["0"]
    h.c._outage_probe_tick()
    assert h.conf and h.conf[0][0] == "OFF 재시도" and h.c._off_unconfirmed is False
    assert h.c._output_maybe_on is False and not h.c._outage_probe.isActive()
    assert ("OFF확인", h.conf and "OFF 재시도: 미확인 %.0f초 뒤" % h.conf[0][1]) in h.events


def test_T96_verify_deferred_while_reading(qapp, monkeypatch):
    h = DCHarness(monkeypatch, replies=["0"])
    h.c._output_maybe_on = True
    h.c._io_depth = 1
    r = h.c._output_off_confirmed("stop_process")
    assert r is None and h.sent == ["OUTP OFF"] and h.shots and h.shots[-1][0] == 50
    h.c._io_depth = 0
    h.shots[-1][1]()
    assert h.sent == ["OUTP OFF", "OUTP?"] and h.c._output_maybe_on is False


def test_T97_verify_aborts_when_new_process_started(qapp, monkeypatch):
    h = DCHarness(monkeypatch, replies=[None])
    h.c._output_maybe_on = True
    h.c._is_running = True                                  # 확인 도중 새 공정이 주인이 됐다
    assert h.c._verify_output_off("stop_process") is None
    assert h.sent == []                                     # 재전송 없음
    h2 = DCHarness(monkeypatch, replies=["1"])
    h2.c._output_maybe_on = True
    def _steal(cmd, timeout_ms=500):
        h2.c._is_running = True
        return "1"
    monkeypatch.setattr(h2.c, "_query", lambda cmd, timeout_ms=500, count_fail=True: (h2.sent.append(cmd), _steal(cmd))[1])
    assert h2.c._verify_output_off("stop_process") is None
    assert h2.sent.count("OUTP OFF") == 0                   # 1 응답이어도 재전송 안 함


def test_T98_start_process_deferred_while_reading(qapp, monkeypatch):
    h = DCHarness(monkeypatch)
    h.c._io_depth = 1
    h.c.start_process(100.0)
    assert h.sent == [] and h.shots and h.shots[-1][0] == 50 and h.c._is_running is False


def test_T99_timer_tick_guards(qapp, monkeypatch):
    h = DCHarness(monkeypatch)
    h.c._is_running = True
    h.c._io_depth = 1
    h.c._on_timer_tick()
    assert h.sent == []                                     # 겹친 틱은 건너뛴다
    h.c._io_depth = 0
    def _read_then_stop():
        h.c.stop_process()                                  # 읽는 도중 정지가 끼어들었다
        return (100.0, 50.0, 2.0)
    monkeypatch.setattr(h.c, "read_dc_power", _read_then_stop)
    h.c._is_running = True
    h.sent.clear()
    h.c._on_timer_tick()
    assert not any(w.startswith("CURR") for w in h.sent)


def test_T100_safe_off_keeps_probe_when_unconfirmed(qapp, monkeypatch):
    h = DCHarness(monkeypatch, replies=[None, None])
    h.c._output_maybe_on = True; h.c._outage_abort = True; h.c._outage_probe.start()
    h.c.safe_off()
    assert h.c._outage_abort is False and h.c._outage_probe.isActive()      # 미확인 → 프로브 유지
    assert ("안전상태재적용", "OUTP OFF 미확인") in h.events
    h2 = DCHarness(monkeypatch, replies=["0"])
    h2.c._output_maybe_on = True; h2.c._outage_abort = True; h2.c._outage_probe.start()
    h2.c.safe_off()
    assert h2.c._outage_abort is False and not h2.c._outage_probe.isActive()
    assert ("안전상태재적용", "OUTP OFF 확인") in h2.events


def test_T101_stop_process_without_output_is_info_only(qapp, monkeypatch):
    """그날 첫 공정의 "PRE: DC Power OFF"(포트 미연결) 가 내던 거짓 경고가 정보 1줄로 바뀐다."""
    h = DCHarness(monkeypatch, open_port=False)
    h.c.stop_process()
    assert h.events == [] and h.unconf == []
    assert not any(l == "DCpower(경고)" for l, _ in h.msgs)
    assert any("이번 실행에서 DC 출력을 켠 적 없음" in m for _, m in h.msgs)


# ═══════════════ T114~ OFF 확인은 명령 간격을 둔 뒤 (2026-09-28 거짓 "OFF 미확인") ═══════════════
class GapHarness(DCHarness):
    """직전 쓰기로부터 DC_CMD_GAP_MS 안에 온 질의는 무시하는 장비(2026-09-28 실기 동작).
    가짜 시계: msleep 이 시간을 흘려보낸다."""
    def __init__(self, monkeypatch, outp="0", **kw):
        super().__init__(monkeypatch, **kw)
        self.outp = outp if isinstance(outp, list) else [outp]
        self.queries = []; self._prev_write_t = 0.0; self._gap_at_write = 1e9
        monkeypatch.setattr(DCM.QThread, "msleep", lambda ms: setattr(self, "t", self.t + ms / 1000.0))
        monkeypatch.setattr(self.c, "_readline_blocking", self._read_gap)

    def _read_gap(self, timeout_ms=500):
        """질의가 '직전 명령으로부터 얼마 뒤' 도착했는지로 응답 여부를 정한다(실기 장비 동작)."""
        self.queries.append(round(self._gap_at_write))
        if self._gap_at_write < DCM.DC_CMD_GAP_MS:
            self.t += timeout_ms / 1000.0          # 무응답 — 타임아웃까지 기다린 셈
            return None
        return self.outp.pop(0) if self.outp else None

    def _write(self, line):                        # 쓰기도 시간을 조금 쓴다
        self.t += 0.01
        self._gap_at_write = (self.t - self._prev_write_t) * 1000.0 if self._prev_write_t else 1e9
        self._prev_write_t = self.t
        return super()._write(line)


def test_T114_stop_process_confirms_on_first_try_with_gap(qapp, monkeypatch):
    h = GapHarness(monkeypatch, outp="0")
    h.c._output_maybe_on = True
    h.c.stop_process()
    assert h.sent == ["OUTP OFF", "OUTP?"]                       # 재전송 없이 한 번에 확인
    assert h.queries[0] >= DCM.DC_CMD_GAP_MS                     # 질의 전 간격 보장
    assert h.unconf == [] and h.c._off_unconfirmed is False and h.c._output_maybe_on is False
    assert h.c._comm_fail_streak == 0                            # 확인 때문에 실패 카운트가 늘지 않는다
    assert h.c.serial.isOpen() and not any(fn for _, fn in h.shots if getattr(fn, "__name__", "") == "_try_reconnect")
    assert any("DC 출력 OFF 확인(OUTP?=0, 공정 종료)" in m for _, m in h.msgs)


def test_T115_old_behavior_would_have_failed(qapp, monkeypatch):
    """간격 없이 바로 물으면(수정 전) 같은 장비가 무응답이다 — 거짓 ❌ 의 원인."""
    h = GapHarness(monkeypatch, outp="0")
    h.c._output_maybe_on = True
    h.c._send_output_off("공정 종료")
    ans = h.c._query("OUTP?", timeout_ms=800, count_fail=False)   # 간격 없이 즉시
    assert ans is None and h.queries[0] < DCM.DC_CMD_GAP_MS


def test_T116_retries_on_one_then_confirms(qapp, monkeypatch):
    h = GapHarness(monkeypatch, outp=["1", "1", "0"])
    h.c._output_maybe_on = True
    assert h.c._output_off_confirmed("공정 종료") is True
    assert h.sent == ["OUTP OFF", "OUTP?", "OUTP OFF", "OUTP?", "OUTP OFF", "OUTP?"]
    assert h.unconf == [] and h.c._comm_fail_streak == 0


def test_T117_all_tries_fail_marks_once_and_counts_one_comm_fail(qapp, monkeypatch):
    h = GapHarness(monkeypatch, outp=[])                          # 항상 무응답
    h.c._output_maybe_on = True
    assert h.c._output_off_confirmed("공정 종료") is False
    assert h.sent.count("OUTP?") == DCM.DC_OFF_VERIFY_TRIES
    assert h.unconf == ["공정 종료"] and h.c._comm_fail_streak == 1     # 확인 실패는 1회만 반영
    detail = [m for l, m in h.msgs if l == "DCpower(경고)" and "OFF 미확인" in m][0]
    assert "None/None/None" in detail


def test_T118_no_send_after_start_during_verify(qapp, monkeypatch):
    h = GapHarness(monkeypatch, outp=[])
    h.c._output_maybe_on = True
    h.c._send_output_off("공정 종료")
    n = len(h.sent)
    h.c._is_running = True
    assert h.c._verify_output_off("공정 종료") is None and len(h.sent) == n


def test_T119_no_double_waitforbyteswritten_and_timeout_error_ignored(qapp, monkeypatch):
    import inspect
    def _code(fn):                                    # 주석은 빼고 코드 줄만 본다
        return chr(10).join(l.split("#")[0] for l in inspect.getsource(fn).splitlines())
    assert "waitForBytesWritten" not in _code(DCM.DCPowerController._send_output_off)
    assert "waitForBytesWritten" not in _code(DCM.DCPowerController.cleanup)
    assert "waitForBytesWritten" in _code(DCM.DCPowerController._write_line)   # 한 곳에서만 한다
    h = DCHarness(monkeypatch)
    h.c._on_serial_error(DCM.QSerialPort.SerialPortError.TimeoutError)
    assert h.msgs == [] and h.events == []


def test_T120_read_dc_power_skips_fallback_after_stop(qapp, monkeypatch):
    h = DCHarness(monkeypatch)
    h.c._is_running = True
    def _q(cmd, timeout_ms=500, count_fail=True):
        h.sent.append(cmd)
        h.c._is_running = False              # 읽는 도중 사용자 STOP
        return None
    monkeypatch.setattr(h.c, "_query", _q)
    assert h.c.read_dc_power() == (None, None, None)
    assert h.sent == ["MEAS:ALL?"]           # 폴백 MEAS:VOLT?/MEAS:CURR? 를 보내지 않는다


def test_T121_query_logs_dropped_bytes(qapp, monkeypatch):
    h = DCHarness(monkeypatch, replies=["0"])
    class _S(DCFakeSerial):
        def __init__(self): super().__init__(); self._n = 1
        def bytesAvailable(self): return self._n
        def readAll(self): self._n = 0; return b"0\r\n"
    h.c.serial = _S(); h.c.serial.open()
    h.c._query("OUTP?")
    assert any(l == "DCpower" and "이전 응답 폐기: '0'" in m for l, m in h.msgs)


def test_T122_where_labels_are_human_readable(qapp, monkeypatch):
    import inspect
    src = inspect.getsource(DCM.DCPowerController)
    assert '_output_off_confirmed("공정 종료")' in src and '"stop_process"' not in src
    for w in ("공정 종료", "ALL STOP", "복구 후 안전 상태", "OFF 재시도"):
        assert f'"{w}"' in src
    h = GapHarness(monkeypatch, outp=[])
    h.c._output_maybe_on = True
    h.c.stop_process()
    assert h.unconf == ["공정 종료"]
