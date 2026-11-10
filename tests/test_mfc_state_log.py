# -*- coding: utf-8 -*-
"""MFC 압력·밸브 위치 1초 CSV — 저우선 읽기(R5·R6)·늦은 응답 차단·CSV 쓰기.

실제 MFCController 를 가짜 시리얼 포트(FakePort)로 돌린다. 시간 상수는 시험이 빠르도록 줄인다(관계는 실제와 같다):
MFC_TIMEOUT 400, 일반 간격 100, 기록 읽기 타임아웃 200·간격 40. 응답은 실측 형식("P-000.06", "P+012.34", "V+100.00", "V+035.20").
"""
import csv
import datetime
import threading
import time

import pytest
from PyQt6.QtCore import QByteArray, QEventLoop, QObject, QTimer, pyqtSignal
from PyQt6.QtSerialPort import QSerialPort

import device.MFC as MFCM
import lib.mfc_state_log as SL
import lib.paths as P

TIMEOUT, GAP, LOG_TIMEOUT, LOG_GAP = 400, 100, 200, 40


def spin(ms):
    loop = QEventLoop()
    QTimer.singleShot(int(ms), loop.quit)
    loop.exec()


def spin_until(cond, ms=3000):
    end = time.monotonic() + ms / 1000.0
    while not cond() and time.monotonic() < end:
        spin(5)
    return cond()


class FakePort(QObject):
    readyRead = pyqtSignal()
    errorOccurred = pyqtSignal(object)
    DataBits, Parity, StopBits = QSerialPort.DataBits, QSerialPort.Parity, QSerialPort.StopBits
    FlowControl, Direction, SerialPortError = QSerialPort.FlowControl, QSerialPort.Direction, QSerialPort.SerialPortError
    rig = None

    def __init__(self, parent=None):
        super().__init__(parent)
        self._open = False
        self._buf = bytearray()

    setBaudRate = setDataBits = setParity = setStopBits = setFlowControl = lambda self, *a: None
    setPortName = setDataTerminalReady = setRequestToSend = clear = lambda self, *a: None

    def open(self, *a):
        self._open = True
        FakePort.rig.opens += 1
        return True

    def isOpen(self):
        return self._open

    def close(self):
        self._open = False

    def errorString(self):
        return ""

    def flush(self):
        return True

    def write(self, b):
        b = bytes(b)
        FakePort.rig.on_write(b.decode().strip())
        return len(b)

    def readAll(self):
        d = bytes(self._buf)
        self._buf.clear()
        return QByteArray(d)

    def feed(self, text):
        self._buf.extend((text + "\r").encode())
        self.readyRead.emit()


class FakeInfo:
    class _P:
        def portName(self):
            return MFCM.MFC_PORT

    @staticmethod
    def availablePorts():
        return [FakeInfo._P()]


class Rig:
    """가짜 장비 — 명령 문자열별 기본 응답(30 ms 안쪽), 명령별 일회성 응답 덮어쓰기(override: [[(지연ms, 문자열), ...], ...])."""

    def __init__(self, c):
        self.c = c
        self.sent = []                 # (시각, 명령, 태그)
        self.opens = 0
        self.mask = "0000"
        self.override = {}
        self.port = None
        self.msgs, self.pressures, self.samples, self.events, self.ok = [], [], [], [], []
        c.status_message.connect(lambda lv, m: self.msgs.append((lv, m)))
        c.update_pressure.connect(self.pressures.append)
        c.state_sample.connect(self.samples.append)
        c.mfc_comm_event.connect(self.events.append)
        c.command_confirmed.connect(self.ok.append)

    def script(self, cmd):
        q = self.override.get(cmd)
        if q:
            return q.pop(0)
        if cmd.startswith("L0"):
            self.mask = cmd[2:]
            return []
        table = {"R69": "L0" + self.mask, "R5": "P+012.34", "R6": "V+100.00", "R7": "M1000",
                 "R60": "Q0+020.00+000.00+000.00+000.00", "R65": "Q5+20.00", "R1": "S1+0.20", "R10": "S5+5.00"}
        if cmd in table:
            return [(25, table[cmd])]
        return []

    def on_write(self, cmd):
        tag = self.c._inflight.tag if self.c._inflight else ""
        self.sent.append((time.monotonic(), cmd, tag))
        for delay, text in self.script(cmd):
            QTimer.singleShot(delay, lambda text=text, port=self.port: self._feed(port, text))

    @staticmethod
    def _feed(port, text):
        try:                                   # cleanup 으로 포트가 지워진 뒤의 늦은 응답은 무시(슬롯 안 예외는 PyQt 가 프로세스를 죽인다)
            port.feed(text)
        except RuntimeError:
            pass

    def log_on(self):
        self.c._state_log_on = True    # 타이머 없이 _state_log_tick 을 직접 부른다

    def tags(self):
        return [t for _, _, t in self.sent]

    def shown(self):
        return " | ".join(m for _, m in self.msgs)


@pytest.fixture
def make(qapp, monkeypatch):
    made = []

    def _make(log=False):
        for k, v in dict(MFC_TIMEOUT=TIMEOUT, MFC_GAP_MS=GAP, MFC_DELAY_MS=100, MFC_DELAY_MS_VALVE=150,
                         MFC_POLLING_INTERVAL_MS=300, MFC_STATE_LOG_ENABLED=log, MFC_STATE_LOG_INTERVAL_S=0.1,
                         MFC_STATE_LOG_GAP_MS=LOG_GAP, MFC_STATE_LOG_TIMEOUT_MS=LOG_TIMEOUT).items():
            monkeypatch.setattr(MFCM, k, v)
        monkeypatch.setattr(MFCM, "QSerialPort", FakePort)
        monkeypatch.setattr(MFCM, "QSerialPortInfo", FakeInfo)
        c = MFCM.MFCController()
        rig = Rig(c)
        FakePort.rig = rig
        assert c.connect_mfc_device() is True
        c._watchdog.stop()
        rig.port = c.serial_mfc
        rig.opens = 0
        made.append(c)
        return rig
    yield _make
    for c in made:
        c.cleanup()
        c.deleteLater()


# ───────────────────────── 끄면 기존과 같다 ─────────────────────────
def test_disabled_has_no_timer_no_traffic(make):
    r = make(log=False)
    r.c.start_state_log()                                # 꺼져 있으면 아무것도 만들지 않는다
    assert r.c._state_log_timer is None and r.c._state_log_on is False
    spin(300)
    assert r.sent == [] and r.samples == []


SEQUENCE_REPLIES = {"R65": "Q5+20.00"}


def _run_sequence(r):
    """대표 명령 시퀀스: 유량 설정·켜기, 세트포인트 설정·검증·켜기, 밸브 열기·확인, 폴링."""
    c = r.c
    c.handle_command("FLOW_SET", {"channel": 1, "value": 20.0})
    c.handle_command("FLOW_ON", {"channel": 1})
    c.handle_command("SP1_SET", {"value": 2.0})
    c.handle_command("SP1_ON", {})
    c.handle_command("VALVE_OPEN", {})
    c.handle_command("set_polling", {"enable": True})
    spin_until(lambda: len(r.sent) >= 16, 6000)
    c.handle_command("set_polling", {"enable": False})


def test_disabled_sequence_order_and_gaps(make):
    r = make(log=False)
    _run_sequence(r)
    cmds = [cmd for _, cmd, _ in r.sent]
    assert cmds[0] == "Q1 20.0" and "O" in cmds and cmds.count("R69") >= 1, cmds
    # 일반 명령 사이 간격은 항상 일반 간격(100 ms) 이상 — 기록 읽기는 없다
    assert all("[LOG" not in t for t in r.tags())
    ts = [t for t, _, _ in r.sent]
    assert all((b - a) * 1000 >= GAP - 5 for a, b in zip(ts, ts[1:]))


def test_enabled_normal_order_unchanged_and_delay_bounded(make):
    """켜도 일반 명령끼리의 순서는 그대로, 일반 명령이 기록 읽기 때문에 늦어지는 시간은 (기록 응답 ≈25) + 기록 간격(40) 안쪽."""
    off = make(log=False)
    _run_sequence(off)
    normal_off = [cmd for _, cmd, t in off.sent if "[LOG" not in t][:14]
    off.c.cleanup()
    on = make(log=True)
    on.c.start_state_log()
    _run_sequence(on)
    on.c.stop_state_log()
    normal_on = [cmd for _, cmd, t in on.sent if "[LOG" not in t][:14]
    assert normal_on == normal_off


# ───────────────────────── 우선순위 ─────────────────────────
def test_normal_command_goes_before_queued_log_read(make):
    r = make(log=True)
    c = r.c
    r.override["R7"] = [[(150, "M1000")]]                  # 앞선 일반 명령이 느리게 끝난다
    c.enqueue("R7", lambda l: None, tag="[BLOCK]", timeout_ms=TIMEOUT, gap_ms=GAP)
    spin(30)
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0, low_priority=True, quiet=True)
    c.enqueue("R60", lambda l: None, tag="[N1]", timeout_ms=TIMEOUT, gap_ms=GAP)
    c.enqueue("R69", lambda l: None, tag="[N2]", timeout_ms=TIMEOUT, gap_ms=GAP)
    assert spin_until(lambda: len(r.sent) >= 4)
    assert r.tags() == ["[BLOCK]", "[N1]", "[N2]", "[LOG R5]"]      # 일반끼리 순서 불변, 기록은 맨 뒤


def test_no_low_priority_pops_left_like_before(make):
    r = make(log=False)
    c = r.c
    c.enqueue("R7", lambda l: None, tag="[A]", timeout_ms=TIMEOUT, gap_ms=GAP)
    spin(5)
    for t in ("[B]", "[C]", "[D]"):
        c.enqueue("R60", lambda l: None, tag=t, timeout_ms=TIMEOUT, gap_ms=GAP)
    assert spin_until(lambda: len(r.sent) >= 4)
    assert r.tags() == ["[A]", "[B]", "[C]", "[D]"]


def test_max_extra_wait_is_one_log_read_plus_log_gap(make):
    r = make(log=True)
    c = r.c
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP,
              retries_left=0, low_priority=True, quiet=True)
    spin(5)
    c.enqueue("R7", lambda l: None, tag="[N]", timeout_ms=TIMEOUT, gap_ms=GAP)                         # 기록 읽기가 나간 직후 들어온 일반 명령
    assert spin_until(lambda: len(r.sent) >= 2)
    wait_ms = (r.sent[1][0] - r.sent[0][0]) * 1000
    assert 25 + LOG_GAP - 10 <= wait_ms <= 25 + LOG_GAP + 80, wait_ms     # 응답 시간 + 기록 간격


def test_max_extra_wait_when_log_read_times_out(make):
    r = make(log=True)
    c = r.c
    r.override["R5"] = [[]]                                               # 무응답
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP,
              retries_left=0, low_priority=True, quiet=True)
    spin(5)
    c.enqueue("R7", lambda l: None, tag="[N]", timeout_ms=TIMEOUT, gap_ms=GAP)
    assert spin_until(lambda: len(r.sent) >= 2, 3000)
    wait_ms = (r.sent[1][0] - r.sent[0][0]) * 1000
    assert TIMEOUT + LOG_GAP - 10 <= wait_ms <= TIMEOUT + LOG_GAP + 90, wait_ms   # 보낸 뒤 MFC_TIMEOUT + 기록 간격


# ───────────────────────── 기록 읽기 타임아웃 · 늦은 응답 차단 ─────────────────────────
def test_log_timeout_no_retry_no_reconnect_no_comm_event(make, monkeypatch):
    r = make(log=True)
    c = r.c
    watch = []
    monkeypatch.setattr(c, "_watch_connection", lambda: watch.append(1))
    r.override["R5"] = [[]]
    got = []
    c.enqueue("R5", got.append, timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, tag="[LOG R5]", retries_left=0,
              low_priority=True, quiet=True)
    spin(LOG_TIMEOUT + 150)
    assert got == [None]
    assert len([s for s in r.sent if s[1] == "R5"]) == 1               # 재시도 없음
    assert c.serial_mfc.isOpen() and r.opens == 0 and watch == []      # 포트 닫기·재연결 없음
    assert r.events == [] and not any("TIMEOUT" in m for _, m in r.msgs)


def test_late_response_after_log_timeout_is_not_taken_by_next_command(make):
    """R6 기록 읽기가 타임아웃(200)이고 보낸 지 280 ms 에 늦은 "V+100.00" — 다음 R69 의 응답이 되어 L0 마스크를 바꾸면 안 된다."""
    r = make(log=True)
    c = r.c
    r.override["R6"] = [[(280, "V+100.00")]]
    c.enqueue("R6", lambda l: None, tag="[LOG R6]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0,
              low_priority=True, quiet=True)
    spin(LOG_TIMEOUT + 10)
    c.handle_command("FLOW_ON", {"channel": 2})                        # R69 → L0 → R69
    assert spin_until(lambda: "FLOW_ON" in r.ok, 3000), r.sent
    l0 = [cmd for _, cmd, _ in r.sent if cmd.startswith("L0")]
    assert l0 == ["L00100"], l0                                         # 오염됐다면 "L01100"
    t_log, t_r69 = r.sent[0][0], r.sent[1][0]
    assert (t_r69 - t_log) * 1000 >= TIMEOUT + LOG_GAP - 10             # 보낸 뒤 MFC_TIMEOUT + 간격이 지나서야 다음 명령


def test_late_response_inside_guard_edge_is_dropped(make):
    """보낸 뒤 MFC_TIMEOUT + 기록 간격 − 20 ms(보호 구간의 끝자락)에 와도 버린다."""
    r = make(log=True)
    c = r.c
    r.override["R5"] = [[(TIMEOUT + LOG_GAP - 20, "P-000.06")]]
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0,
              low_priority=True, quiet=True)
    spin(LOG_TIMEOUT + 10)
    c.enqueue("R5", lambda l: c.update_pressure.emit("POLL:" + l), tag="[POLL PRESS]", timeout_ms=TIMEOUT, gap_ms=GAP)
    spin_until(lambda: any(p.startswith("POLL:") for p in r.pressures), 3000)
    assert [p for p in r.pressures if p.startswith("POLL:")] == ["POLL:P+012.34"]


def test_late_pressure_not_shown_on_poll_and_wait_pressure(make):
    r = make(log=True)
    c = r.c
    r.override["R5"] = [[(280, "P-000.06")]]
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0,
              low_priority=True, quiet=True)
    spin(LOG_TIMEOUT + 10)
    c.handle_command("WAIT_PRESSURE", {"target": 123.4, "tolerance_ratio": 0.1, "stable_count": 1, "timeout_sec": 5})
    assert spin_until(lambda: "WAIT_PRESSURE" in r.ok, 3000)
    assert r.pressures == ["123.40"]                                    # 늦은 P-000.06(−0.60)이 압력으로 안 들어갔다


def test_late_response_does_not_change_r69_mask_via_poll_path(make):
    r = make(log=True)
    c = r.c
    r.override["R5"] = [[(280, "P-000.06")]]
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0,
              low_priority=True, quiet=True)
    spin(LOG_TIMEOUT + 10)
    c.handle_command("FLOW_ON", {"channel": 1})
    assert spin_until(lambda: "FLOW_ON" in r.ok, 3000)
    assert [cmd for _, cmd, _ in r.sent if cmd.startswith("L0")] == ["L01000"]


def test_log_send_failure_gives_up_without_requeue(make, monkeypatch):
    r = make(log=True)
    c = r.c
    got = []
    monkeypatch.setattr(r.port, "write", lambda b: -1)
    c.enqueue("R5", got.append, timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, tag="[LOG R5]", retries_left=0,
              low_priority=True, quiet=True)
    spin(60)
    assert got == [None] and len(c._cmd_q) == 0 and c.serial_mfc.isOpen()


# ───────────────────────── 폴링·WAIT_PRESSURE 판정 불변 ─────────────────────────
def test_polling_helpers_unchanged_by_log_reads(make):
    r = make(log=True)
    c = r.c
    r.override["R7"] = [[(300, "M1000")]]
    c.enqueue("R7", lambda l: None, tag="[BLOCK]", timeout_ms=TIMEOUT, gap_ms=GAP)
    spin(20)
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0, low_priority=True, quiet=True)
    assert c._has_pending_poll() is False                                # 기록 읽기만 있으면 폴링 대기가 아니다
    c.enqueue("R5", lambda l: None, tag="[POLL PRESS]", timeout_ms=TIMEOUT, gap_ms=GAP)
    assert c._has_pending_poll() is True
    assert c._purge_poll_reads_only(cancel_inflight=False, reason="t") == 1
    assert [x.tag for x in c._cmd_q] == ["[LOG R5]"]                    # 기록 읽기는 그대로
    c._cancel_wait_pressure("t")
    assert [x.tag for x in c._cmd_q] == ["[LOG R5]"]


def test_poll_cycle_not_skipped_by_log_read(make):
    r = make(log=True)
    c = r.c
    r.override["R7"] = [[(300, "M1000")]]
    c.enqueue("R7", lambda l: None, tag="[BLOCK]", timeout_ms=TIMEOUT, gap_ms=GAP)
    spin(20)
    c.enqueue("R5", lambda l: None, tag="[LOG R5]", timeout_ms=LOG_TIMEOUT, gap_ms=LOG_GAP, retries_left=0, low_priority=True, quiet=True)
    c._enqueue_poll_cycle()
    assert sorted(x.tag for x in c._cmd_q) == ["[LOG R5]", "[POLL PRESS]", "[POLL R60]"]


# ───────────────────────── quiet ─────────────────────────
def test_quiet_no_messages_no_pressure_update(make):
    r = make(log=True)
    r.log_on()
    c = r.c
    c._state_log_tick()                                                 # 성공 주기(R5 → R6)
    assert spin_until(lambda: len(r.samples) == 1, 2000)
    r.override["R5"] = [[]]
    r.override["R6"] = [[]]
    spin(LOG_GAP + 60)
    c._state_log_tick()                                                 # 타임아웃 주기
    spin(2 * LOG_TIMEOUT + 2 * TIMEOUT + 300)
    shown = r.shown()
    for word in ("[RECV]", "[TIMEOUT]", "[NOTE]", "전송", "R5", "R6", "LOG"):
        assert word not in shown, (word, shown)
    assert r.pressures == [] and r.events == []
    c.enqueue("R7", lambda l: None, tag="[N]", timeout_ms=TIMEOUT, gap_ms=GAP)                          # 일반 명령은 그대로 알린다
    spin(150)
    assert "[RECV]" in r.shown() and any(lv == "MFC > 전송" for lv, _ in r.msgs)


# ───────────────────────── 샘플 값·접두사 ─────────────────────────
def test_sample_values_negative_pressure_and_valve(make):
    r = make(log=True)
    r.log_on()
    r.override["R5"] = [[(25, "P-000.06")]]
    r.override["R6"] = [[(25, "V+035.20")]]
    before = datetime.datetime.now()
    r.c._state_log_tick()
    assert spin_until(lambda: len(r.samples) == 1, 2000)
    s = r.samples[0]
    assert s["pressure"] == pytest.approx(-0.6) and s["valve"] == "035.20"      # 화면 단위(×10), 음수 그대로
    assert before <= s["time"] <= datetime.datetime.now()
    row = SL.make_row(s, 2)
    assert row[1][1:] == ["-0.60", "35.20"] and len(row[1][0]) == len("2026-11-10 09:08:07.012")


def test_sample_default_replies(make):
    r = make(log=True)
    r.log_on()
    r.c._state_log_tick()
    assert spin_until(lambda: len(r.samples) == 1, 2000)
    assert r.samples[0]["pressure"] == pytest.approx(123.4) and r.samples[0]["valve"] == "100.00"
    assert r.tags() == ["[LOG R5]", "[LOG R6]"]


def test_wrong_prefix_counts_as_failure(make):
    r = make(log=True)
    r.log_on()
    r.override["R5"] = [[(25, "V+100.00")]]                              # R5 인데 V → 압력 실패
    r.override["R6"] = [[(25, "P-000.06")]]                              # R6 인데 P → 밸브 실패
    r.c._state_log_tick()
    spin(400)
    assert r.samples == []                                               # 둘 다 실패면 시그널 없음
    assert r.pressures == []


def test_pressure_fail_valve_ok_and_reverse(make):
    r = make(log=True)
    r.log_on()
    r.override["R5"] = [[(25, "V+100.00")]]
    r.c._state_log_tick()
    assert spin_until(lambda: len(r.samples) == 1, 2000)
    assert r.samples[0]["pressure"] is None and r.samples[0]["valve"] == "100.00"
    spin(LOG_GAP + 60)
    r.override["R6"] = [[]]
    r.c._state_log_tick()
    assert spin_until(lambda: len(r.samples) == 2, 3000)
    assert r.samples[1]["valve"] is None and r.samples[1]["pressure"] == pytest.approx(123.4)


def test_r6_skipped_when_normal_command_arrives(make):
    r = make(log=True)
    r.log_on()
    c = r.c
    r.override["R5"] = [[(60, "P+012.34")]]
    c._state_log_tick()
    spin(10)
    c.enqueue("R7", lambda l: None, tag="[N]", timeout_ms=TIMEOUT, gap_ms=GAP)                           # R5 응답 전에 일반 명령이 들어왔다
    assert spin_until(lambda: len(r.samples) == 1, 2000)
    assert r.samples[0]["valve"] is None and "[LOG R6]" not in r.tags()


def test_tick_skips_when_normal_pending_or_log_busy_or_port_closed(make):
    r = make(log=True)
    r.log_on()
    c = r.c
    c.enqueue("R7", lambda l: None, tag="[N]", timeout_ms=TIMEOUT, gap_ms=GAP)
    c._state_log_tick()
    assert [x.tag for x in c._cmd_q] == ["[N]"]
    spin(150)
    c._state_log_tick()
    c._state_log_tick()                                                  # 이미 기록 읽기가 있으면 또 넣지 않는다
    assert [x.tag for x in c._cmd_q].count("[LOG R5]") + r.tags().count("[LOG R5]") == 1
    spin(400)
    c.serial_mfc.close()
    n = len(r.sent)
    c._state_log_tick()
    spin(100)
    assert len(r.sent) == n


def test_cleanup_does_not_requeue_log_reads(make):
    r = make(log=True)
    r.log_on()
    c = r.c
    r.override["R5"] = [[(300, "P+012.34")]]
    c._state_log_tick()
    spin(30)
    c.cleanup()                                                          # 진행 중 읽기의 콜백에 None → 새 명령을 넣지 않는다
    spin(50)
    assert len(c._cmd_q) == 0 and len([s for s in r.sent]) == 1 and r.samples == []


def test_start_stop_state_log_slots(make):
    r = make(log=True)
    c = r.c
    c.start_state_log()
    assert c._state_log_timer is not None and c._state_log_timer.isActive() and c._state_log_on
    assert c._state_log_timer.interval() == 100
    assert spin_until(lambda: len(r.samples) >= 2, 3000)
    c.stop_state_log()
    assert not c._state_log_timer.isActive() and not c._state_log_on
    n = len(r.sent)
    spin(250)
    assert len(r.sent) == n


def test_poll_and_log_coexist_poll_not_starved(make):
    """폴링이 켜진 채 기록이 켜져 있어도 폴링 읽기는 모두 나간다(양보하는 쪽은 기록)."""
    r = make(log=True)
    c = r.c
    c.start_state_log()
    c.handle_command("set_polling", {"enable": True})
    spin(2500)
    c.handle_command("set_polling", {"enable": False})
    tags = r.tags()
    assert tags.count("[POLL PRESS]") >= 3 and tags.count("[POLL R60]") >= 3
    assert tags.count("[LOG R5]") >= 1


# ───────────────────────── CSV ─────────────────────────
def test_row_formats():
    ts = datetime.datetime(2026, 11, 10, 9, 8, 7, 12000)
    assert SL.make_row({"time": ts, "pressure": -0.6, "valve": "100.00"}, 2) == \
        ("20261110", ["2026-11-10 09:08:07.012", "-0.60", "100.00"])
    assert SL.make_row({"time": ts, "pressure": 123.4, "valve": "035.20"}, 2)[1] == ["2026-11-10 09:08:07.012", "123.40", "35.20"]
    assert SL.make_row({"time": ts, "pressure": None, "valve": "0.00"}, 2)[1][1:] == ["", "0.00"]
    assert SL.make_row({"time": ts, "pressure": 5.0, "valve": None}, 3)[1][1:] == ["5.000", ""]
    assert SL.make_row({"time": ts, "pressure": None, "valve": None}, 2) is None
    assert SL.format_pressure(-0.001, 2) == "0.00"


def _read(path):
    raw = path.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
    return list(csv.reader(raw.decode("utf-8-sig").splitlines()))


def _rows(day, n=2, hh=9):
    return [(day, [f"{day[:4]}-{day[4:6]}-{day[6:]} {hh:02d}:00:0{i}.000", "1.00", "100.00"]) for i in range(n)]


def test_writer_header_and_new_file_per_day(tmp_path):
    w = SL.MfcStateCsvWriter(tmp_path / "nas", tmp_path / "loc")
    w.submit(_rows("20261110") + _rows("20261111", 1))
    w.submit(_rows("20261110", 1, hh=10))
    assert w.shutdown(3.0)
    a = _read(tmp_path / "nas" / "MFC_20261110.csv")
    assert a[0] == ["Timestamp", "Pressure_mTorr", "Valve_pct"] and len(a) == 1 + 3
    assert len(_read(tmp_path / "nas" / "MFC_20261111.csv")) == 2
    assert not (tmp_path / "loc").exists() and w.stats() == {"primary": 4, "local": 0, "dropped": 0}


def test_writer_nas_folder_failure_goes_local(tmp_path):
    (tmp_path / "blocker").write_text("x")                               # 이 아래로는 폴더를 만들 수 없다
    w = SL.MfcStateCsvWriter(tmp_path / "blocker" / "mfc", tmp_path / "loc")
    w.submit(_rows("20261110"))
    assert w.shutdown(3.0)
    assert len(_read(tmp_path / "loc" / "MFC_20261110.csv")) == 3
    assert w.stats()["local"] == 2 and len(w.take_notices()) == 1 and w.take_notices() == []     # 경고는 1회


def test_writer_locked_file_same_rows_written_locally_then_nas_returns(tmp_path):
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / "MFC_20261110.csv").mkdir()                                   # 파일 자리에 폴더 → 열 수 없음(엑셀 잠금 흉내)
    w = SL.MfcStateCsvWriter(nas, tmp_path / "loc")
    w.submit(_rows("20261110"))
    assert spin_until(lambda: w.stats()["local"] == 2, 3000)
    assert [r[0] for r in _read(tmp_path / "loc" / "MFC_20261110.csv")][1:] == \
        ["2026-11-10 09:00:00.000", "2026-11-10 09:00:01.000"]           # 같은 행이 같은 이름으로
    (nas / "MFC_20261110.csv").rmdir()                                   # 잠금 풀림 — 다음 묶음부터 NAS
    w.submit(_rows("20261110", 1, hh=10))
    assert w.shutdown(3.0)
    assert len(_read(nas / "MFC_20261110.csv")) == 2 and w.stats()["primary"] == 1
    assert len(_read(tmp_path / "loc" / "MFC_20261110.csv")) == 3        # 로컬에 쌓인 것은 합치지 않는다


def test_writer_drops_and_counts_only_when_local_fails_too(tmp_path):
    (tmp_path / "blocker").write_text("x")
    w = SL.MfcStateCsvWriter(tmp_path / "blocker" / "a", tmp_path / "blocker" / "b")
    w.submit(_rows("20261110"))
    assert w.shutdown(3.0)
    assert w.stats() == {"primary": 0, "local": 0, "dropped": 2} and len(w.take_notices()) == 1


def test_writer_slow_nas_does_not_block_and_shutdown_is_bounded(tmp_path, monkeypatch):
    def slow(base, name, recs, make_dir):
        time.sleep(2.0)
        return False
    monkeypatch.setattr(SL.MfcStateCsvWriter, "_append", staticmethod(slow))
    w = SL.MfcStateCsvWriter(tmp_path / "nas", tmp_path / "loc")
    t0 = time.monotonic()
    assert w.submit(_rows("20261110")) is True
    assert time.monotonic() - t0 < 0.1                                   # 넘기는 쪽은 막히지 않는다
    t0 = time.monotonic()
    assert w.shutdown(0.3) is False                                      # NAS 가 멈추면 기다리지 않고 나온다
    assert time.monotonic() - t0 < 1.0


def test_writer_queue_full_drops_and_counts(tmp_path, monkeypatch):
    gate = threading.Event()
    monkeypatch.setattr(SL.MfcStateCsvWriter, "_append", staticmethod(lambda *a, **k: (gate.wait(3), True)[1]))
    w = SL.MfcStateCsvWriter(tmp_path / "nas", tmp_path / "loc", queue_max=2)
    for _ in range(6):
        w.submit(_rows("20261110", 1))
    assert w.stats()["dropped"] >= 2
    gate.set()
    w.shutdown(2.0)


def test_dev_mode_and_ops_paths():
    assert P.resolve_paths(True).mfc_dir == P.DEV_LOG_ROOT / "mfc"
    assert str(P.resolve_paths(False).mfc_dir).endswith(r"\Logs\CHK\mfc")


# ───────────────────────── main ─────────────────────────
from test_main_heater import win  # noqa: E402,F401  (공용 창 — MFC 상태 기록은 conftest 가 꺼 둔다)
import main as MAIN  # noqa: E402


def test_main_disabled_creates_nothing(win):
    assert win._mfc_log_writer is None and win._mfc_log_timer is None and win._mfc_log_rows == []
    assert win.mfc_controller.serial_mfc is None                          # 시작 때 MFC 를 열지 않는다


@pytest.fixture
def mainlog(win, tmp_path, monkeypatch):
    calls = []

    class FakeMeta:
        @staticmethod
        def invokeMethod(obj, name, *a):
            calls.append(name)
            return True
    monkeypatch.setattr(MAIN, "QMetaObject", FakeMeta)
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lv, m: logs.append((lv, m)))
    win._setup_mfc_state_log(enabled=True, primary_dir=tmp_path / "nas", local_dir=tmp_path / "loc")
    yield win, calls, logs, tmp_path
    try:
        win.mfc_controller.state_sample.disconnect(win._on_mfc_state_sample)
    except TypeError:
        pass
    if win._mfc_log_timer is not None:
        win._mfc_log_timer.stop()
    if win._mfc_log_writer is not None:
        win._mfc_log_writer.shutdown(1.0)
    win._mfc_log_writer = win._mfc_log_timer = None


def test_main_enabled_flow_and_csv(mainlog):
    win, calls, logs, tmp = mainlog
    assert calls == ["connect_mfc_device", "start_state_log"]            # 시작 때 한 번 연결 → 기록 시작(둘 다 MFC 스레드, Queued)
    assert win._mfc_log_timer.interval() == 10000 and win._mfc_log_writer is not None
    t = datetime.datetime(2026, 11, 10, 9, 8, 7, 12000)
    win._on_mfc_state_sample({"time": t, "pressure": -0.6, "valve": "100.00"})
    win._on_mfc_state_sample({"time": t, "pressure": None, "valve": None})      # 둘 다 실패 → 행 없음
    win._on_mfc_state_sample({"time": t + datetime.timedelta(days=1), "pressure": 123.4, "valve": "035.20"})
    assert len(win._mfc_log_rows) == 2
    win._flush_mfc_rows()
    assert win._mfc_log_rows == []
    assert win._mfc_log_writer.shutdown(3.0)
    assert _read(tmp / "nas" / "MFC_20261110.csv")[1] == ["2026-11-10 09:08:07.012", "-0.60", "100.00"]
    assert _read(tmp / "nas" / "MFC_20261111.csv")[1] == ["2026-11-11 09:08:07.012", "123.40", "35.20"]


def test_main_row_buffer_capped(mainlog):
    win, *_ = mainlog
    t = datetime.datetime(2026, 11, 10, 9, 0, 0)
    for _ in range(MAIN.MainDialog._MFC_ROWS_MAX + 100):
        win._on_mfc_state_sample({"time": t, "pressure": 1.0, "valve": "1.00"})
    assert len(win._mfc_log_rows) == MAIN.MainDialog._MFC_ROWS_MAX


def test_main_local_fallback_warns_once_and_never_raises(mainlog):
    win, calls, logs, tmp = mainlog
    (tmp / "blocker").write_text("x")
    win._mfc_log_writer.primary_dir = tmp / "blocker" / "mfc"            # NAS 폴더를 못 만든다
    t = datetime.datetime(2026, 11, 10, 9, 0, 0)
    for _ in range(3):
        win._on_mfc_state_sample({"time": t, "pressure": 1.0, "valve": "1.00"})
        win._flush_mfc_rows()
        assert spin_until(lambda: win._mfc_log_writer._q.empty(), 2000)
        spin(80)
    warn = [m for lv, m in logs if "MFC 상태 CSV" in m]
    assert len(warn) == 1 and "로컬" in warn[0]                            # 10초마다가 아니라 실행당 1회
    assert len(_read(tmp / "loc" / "MFC_20261110.csv")) == 1 + 3


def test_main_stop_is_bounded_when_nas_hangs(mainlog, monkeypatch):
    win, calls, logs, tmp = mainlog
    monkeypatch.setattr(SL.MfcStateCsvWriter, "_append", staticmethod(lambda *a, **k: (time.sleep(2.0), False)[1]))
    t = datetime.datetime(2026, 11, 10, 9, 0, 0)
    win._on_mfc_state_sample({"time": t, "pressure": 1.0, "valve": "1.00"})
    win._flush_mfc_rows()
    t0 = time.monotonic()
    win._stop_mfc_state_log(wait_sec=0.3)                                 # 기록 타이머 정지 + 남은 행 넘김 + 제한 시간 대기
    assert time.monotonic() - t0 < 1.5 and win._mfc_log_writer is None
    assert any("기다리지 않고 종료" in m for _, m in logs)
    win._stop_mfc_state_log()                                             # 두 번째 호출은 아무 일도 없다
