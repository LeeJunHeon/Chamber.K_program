# -*- coding: utf-8 -*-
"""이온 게이지 드라이버(IonGaugeController) — 가짜 포트로 정상·무응답·늦은 응답·재연결·신원 확인 실패·
허용 목록, 그리고 실제 QThread 에서 타이머 콜백과 readyRead 처리가 워커 스레드에서 도는지."""
import threading
import time

import pytest
from PyQt6.QtCore import QObject, QTimer, QEventLoop, QThread, QMetaObject, Qt, pyqtSignal, pyqtSlot

from lib.comm_policy import ReconnectPolicy
from device.IonGauge import IonGaugeController, ALLOWED_COMMANDS
from test_ion_gauge import F_REPLY, E_REPLY


def spin(ms):
    loop = QEventLoop()
    QTimer.singleShot(int(ms), loop.quit)
    loop.exec()


class Script:
    """가짜 포트의 동작과 기록. replies[명령] = bytes | None(무응답) | callable() → bytes|None."""

    def __init__(self, replies=None, delay_ms=20, chunk=0, ports=("COM12",), open_ok=True):
        self.replies = {b"F": F_REPLY, b"E": E_REPLY} if replies is None else replies
        self.delay_ms = delay_ms
        self.chunk = chunk
        self.ports = set(ports)
        self.open_ok = open_ok
        self.writes, self.write_threads, self.read_threads, self.close_threads = [], [], [], []
        self.write_times = []
        self.opens = 0
        self.closes = 0


class FakeIgPort(QObject):
    readyRead = pyqtSignal()

    def __init__(self, script: Script, parent=None):
        super().__init__(parent)
        self.s = script
        self._open = False
        self._out = bytearray()
        self._due = []                  # (도착 시각 monotonic, 바이트)

    # QSerialPort 와 같은 이름의 메서드만 흉내
    def setPortName(self, name): self.name = name
    def isOpen(self): return self._open
    def errorString(self): return "fake open error"

    def open(self, mode):
        self.s.opens += 1
        self._open = bool(self.s.open_ok)
        return self._open

    def close(self):
        self.s.closes += 1
        self.s.close_threads.append(threading.get_ident())
        self._open = False
        self._out.clear()

    def clear(self, direction=None):
        self._out.clear()

    def readAll(self):
        self.s.read_threads.append(threading.get_ident())
        b = bytes(self._out)
        self._out.clear()
        return b

    def write(self, data):
        data = bytes(data)
        self.s.writes.append(data)
        self.s.write_times.append(time.monotonic())
        self.s.write_threads.append(threading.get_ident())
        r = self.s.replies.get(data)
        if callable(r):
            r = r()
        if r:
            step = self.s.chunk or len(r)
            for i in range(0, len(r), step):
                due = time.monotonic() + (self.s.delay_ms + i // step * 5) / 1000.0
                self._due.append((due, r[i:i + step]))
                QTimer.singleShot(self.s.delay_ms + i // step * 5, self._deliver)
        return len(data)

    @pyqtSlot()
    def _deliver(self):
        # 예약 1건 = 조각 1개. Qt 타이머는 조금 일찍 울릴 수 있어 시각 대신 '가장 이른 조각'을 꺼낸다
        if not self._due:
            return
        self._due.sort(key=lambda x: x[0])
        _, c = self._due.pop(0)
        if not self._open:
            return
        self._out.extend(c)
        self.readyRead.emit()


def make(script, **kw):
    kw.setdefault("poll_ms", 100)
    kw.setdefault("timeout_ms", 80)
    kw.setdefault("min_gap_ms", 0)
    kw.setdefault("fail_limit", 3)
    kw.setdefault("policy", ReconnectPolicy(50, 200, 600, 1000, 600))
    c = IonGaugeController("COM12", 9600, port_factory=lambda parent: FakeIgPort(script, parent),
                           port_lister=lambda: script.ports, **kw)
    rec = {"snap": [], "msg": [], "ev": []}
    c.snapshot.connect(lambda s: rec["snap"].append(s))
    c.status_message.connect(lambda lv, m: rec["msg"].append((lv, m)))
    c.comm_event.connect(lambda r: rec["ev"].append(r))
    return c, rec


def test_no_qt_objects_until_connect(qapp):
    c = IonGaugeController("COM12", port_factory=lambda p: FakeIgPort(Script(), p), port_lister=lambda: ["COM12"])
    assert c.serial is None and c._poll_timer is None and c._resp_timer is None and c._reconnect_timer is None
    c.connect_device()
    assert c.serial is not None and c._poll_timer.isActive()
    c.cleanup()


def test_normal_cycle_identity_then_e(qapp):
    s = Script()
    c, rec = make(s)
    c.connect_device()
    spin(550)
    c.cleanup()
    assert s.writes[0] == b"F" and set(s.writes[1:]) == {b"E"} and len(s.writes) >= 3
    fresh = [x for x in rec["snap"] if x.fresh]
    assert fresh and all(x.comm == "ok" and x.ok for x in fresh)
    r = fresh[-1].reading
    assert r.ion_text == "9.0E-6" and r.a_text == "하한 이하" and r.b_text == "6.3E-3" and r.filament_on
    assert fresh[-1].age_sec() < 1.0
    # 상태가 바뀔 때만 로그: 연결 1줄 + 필라멘트 1줄, 매 주기 반복 없음
    assert [m for _, m in rec["msg"]] == ["이온 게이지 연결됨 (COM12)", "이온 게이지 필라멘트 켜짐"]
    assert rec["ev"] == []


def test_chunked_reply(qapp):
    s = Script(chunk=5, delay_ms=10)
    c, rec = make(s, timeout_ms=400, poll_ms=500)
    c.connect_device()
    spin(800)
    c.cleanup()
    assert any(x.fresh and x.reading.ion_text == "9.0E-6" for x in rec["snap"])


def test_no_response_counts_fail_then_reopens_with_identity(qapp):
    s = Script(replies={b"F": F_REPLY, b"E": None})
    c, rec = make(s)
    c.connect_device()
    spin(900)
    c.cleanup()
    assert any(x.comm == "timeout" and not x.fresh for x in rec["snap"])
    assert all(not x.fresh for x in rec["snap"])
    assert s.closes >= 1 and s.opens >= 2                       # 연속 3회 실패 → 닫고 다시 열기
    assert s.writes.count(b"F") >= 2                            # 다시 열면 신원 확인부터
    kinds = [e["종류"] for e in rec["ev"]]
    assert kinds[0] == "실패" and kinds.count("실패") == 1       # 두절당 실패 1행
    assert set(kinds) <= {"실패", "재연결"}
    warn = [m for lv, m in rec["msg"] if "26항목 미수신" in m]
    assert len(warn) == 1                                       # 매초 로그 없음


def test_late_response_is_discarded_and_next_cycle_ok(qapp):
    calls = {"n": 0}

    def e_reply():
        calls["n"] += 1
        return E_REPLY

    s = Script(replies={b"F": F_REPLY, b"E": e_reply}, delay_ms=20)
    c, rec = make(s, poll_ms=200, timeout_ms=80)
    c.connect_device()
    spin(330)                                                   # E 1회 정상(t≈200)
    s.delay_ms = 120                                            # 다음 E(t≈400)는 타임아웃(80 ms) 뒤, 다음 요청(t≈600) 전에 도착
    spin(220)
    s.delay_ms = 20
    spin(450)
    c.cleanup()
    comms = [x.comm for x in rec["snap"]]
    i = comms.index("timeout")
    assert "ok" in comms[:i] and "ok" in comms[i + 1:]
    assert "bad_response" not in comms                          # 늦은 응답이 다음 요청과 섞이지 않았다
    assert [e["종류"] for e in rec["ev"]] == ["실패", "복구"]


def test_port_disappears_and_comes_back(qapp):
    s = Script()
    c, rec = make(s)
    c.connect_device()
    spin(300)
    s.ports = set()
    s.replies = {b"F": None, b"E": None}                        # 뽑혔다
    spin(700)
    assert any(x.comm == "no_port" for x in rec["snap"])
    s.ports = {"COM12"}
    s.replies = {b"F": F_REPLY, b"E": E_REPLY}
    spin(900)
    c.cleanup()
    assert rec["snap"][-1].comm == "ok" and rec["snap"][-1].fresh
    kinds = [e["종류"] for e in rec["ev"]]
    assert kinds == ["실패", "재연결", "복구"], kinds
    assert any("통신 복구" in m for _, m in rec["msg"])


@pytest.mark.parametrize("reply", [b"hello\n\r", b"999x\n\r", None])
def test_identity_failure_never_sends_e(qapp, reply):
    s = Script(replies={b"F": reply, b"E": E_REPLY})
    c, rec = make(s)
    c.connect_device()
    spin(500)
    c.cleanup()
    assert s.writes and set(s.writes) == {b"F"}                 # 신원 확인을 통과하지 못하면 E 를 보내지 않는다
    assert s.closes >= 1
    assert all(x.comm == "no_identity" for x in rec["snap"])
    assert [m for _, m in rec["msg"] if "형식 다름" in m][:1][0].startswith("이온 게이지 응답 없음/형식 다름 — ")
    assert sum("응답 없음/형식 다름" in m for _, m in rec["msg"]) == 1
    assert [e["종류"] for e in rec["ev"]] == ["실패"]


def test_open_failure_and_missing_port(qapp):
    s = Script(open_ok=False)
    c, rec = make(s)
    c.connect_device()
    spin(200)
    c.cleanup()
    assert rec["snap"] and all(x.comm == "open_failed" for x in rec["snap"]) and s.writes == []
    s2 = Script(ports=())
    c2, rec2 = make(s2)
    c2.connect_device()
    spin(200)
    c2.cleanup()
    assert all(x.comm == "no_port" for x in rec2["snap"]) and s2.opens == 0


def test_empty_port_name_disables(qapp):
    c = IonGaugeController("", port_factory=lambda p: FakeIgPort(Script(), p), port_lister=lambda: ["COM12"])
    msgs = []
    c.status_message.connect(lambda lv, m: msgs.append(m))
    c.connect_device()
    assert c.serial is None and msgs == ["이온 게이지 사용 안 함 — IG_PORT 가 비어 있음"]


@pytest.mark.parametrize("cmd", ["A", "B", "C", "D", "G", "H", "e", "EF", "", b"E", None])
def test_query_path_sends_only_e_and_f(qapp, cmd):
    """조회 경로(_send)는 E·F 만 — A·B 는 전용 슬롯으로만, C·D·기타는 어디로도 안 나간다."""
    s = Script()
    c, _ = make(s)
    c.connect_device()
    n = len(s.writes)
    with pytest.raises(ValueError):
        c._send(cmd)
    c.cleanup()
    assert len(s.writes) == n


@pytest.mark.parametrize("cmd", ["C", "D", "E", "F", "G", "a", "AB", "", None])
def test_filament_path_sends_only_a_and_b(qapp, cmd):
    s = Script()
    c, _ = make(s)
    c.connect_device()
    spin(150)
    n = len(s.writes)
    with pytest.raises(ValueError):
        c._write(cmd, filament=True)
    c.cleanup()
    assert len(s.writes) == n
    assert ALLOWED_COMMANDS == frozenset({"E", "F", "A", "B"})


def test_single_write_path_in_source():
    """포트에 쓰는 곳은 _write 한 곳, A·B 는 _try_send_filament(= filament_on/off 슬롯)에서만. C·D 글자 없음."""
    import inspect
    import device.IonGauge as M
    src = inspect.getsource(M)
    assert src.count(".write(") == 1 and "self.serial.write(cmd.encode" in src
    assert src.count("filament=True") == 1
    fil_src = inspect.getsource(M.IonGaugeController._try_send_filament)
    assert "self._write(cmd, filament=True)" in fil_src
    for bad in ('"C"', '"D"', "b\"C\"", "b\"D\""):
        assert bad not in src
    on_src = inspect.getsource(M.IonGaugeController.filament_on)
    off_src = inspect.getsource(M.IonGaugeController.filament_off)
    assert "@Slot()" in on_src and '"A"' in on_src and "@Slot()" in off_src and '"B"' in off_src


# ───────────── 실제 QThread ─────────────
class _Receiver(QObject):
    def __init__(self):
        super().__init__()
        self.snaps = []
        self.threads = []

    @pyqtSlot(object)
    def on_snapshot(self, s):
        self.snaps.append(s)
        self.threads.append(threading.get_ident())


def test_timer_and_ready_read_run_in_worker_thread(qapp):
    s = Script(delay_ms=30)
    worker_ident = {}
    th = QThread()
    th.setObjectName("IonGaugeThreadTest")
    c = IonGaugeController("COM12", 9600, poll_ms=1000, timeout_ms=800,
                           port_factory=lambda parent: FakeIgPort(s, parent), port_lister=lambda: ["COM12"])
    c.moveToThread(th)
    th.started.connect(lambda: worker_ident.setdefault("id", threading.get_ident()), Qt.ConnectionType.DirectConnection)
    th.finished.connect(c.cleanup, Qt.ConnectionType.DirectConnection)
    rcv = _Receiver()
    c.snapshot.connect(rcv.on_snapshot)
    th.start()
    QMetaObject.invokeMethod(c, "connect_device", Qt.ConnectionType.QueuedConnection)
    spin(2400)                                                  # 1초 타이머 2회
    main_ident = threading.get_ident()
    th.quit()
    assert th.wait(3000)
    wid = worker_ident["id"]
    assert wid != main_ident
    assert s.writes[:1] == [b"F"] and s.writes.count(b"E") >= 2
    assert set(s.write_threads) == {wid}                        # 1초 타이머 콜백(_on_poll_tick → _send)
    assert set(s.read_threads) == {wid}                         # readyRead 처리(_on_ready_read → readAll)
    assert s.close_threads == [wid]                             # 종료 정리도 워커에서(finished, DirectConnection)
    assert c._poll_timer.thread() is th and c.serial.thread() is th
    fresh = [x for x in rcv.snaps if x.fresh]
    assert fresh and fresh[-1].reading.ion_text == "9.0E-6"
    assert set(rcv.threads) == {main_ident}                     # 받는 쪽(main)은 main 스레드에서


def test_default_qserialport_path_without_ports(qapp):
    """기본 경로(QSerialPort·QSerialPortInfo) — 시험에서는 conftest 가 포트 목록을 비워 실제 포트를 열지 않는다."""
    from PyQt6.QtSerialPort import QSerialPort
    c = IonGaugeController("COM12", 9600, poll_ms=100, policy=ReconnectPolicy(50, 200, 600, 1000, 600))
    snaps = []
    c.snapshot.connect(lambda s: snaps.append(s))
    c.connect_device()
    assert isinstance(c.serial, QSerialPort) and c.serial.baudRate() == 9600
    assert c.serial.parity() == QSerialPort.Parity.NoParity and c.serial.flowControl() == QSerialPort.FlowControl.NoFlowControl
    spin(250)
    c.cleanup()
    assert snaps and all(x.comm == "no_port" for x in snaps) and not c.serial.isOpen()



# ───────────── 필라멘트 ON/OFF (가짜 934: A·B 에는 응답 없음) ─────────────
from test_ion_gauge import e_reply, E_ITEMS  # noqa: E402


def e_with(fil, emis, code=0, ion="9       -6"):
    items = list(E_ITEMS)
    items[1], items[2], items[4], items[23] = str(int(fil)), str(int(emis)), str(int(code)), ion
    return e_reply(items)


class Gauge934:
    """필라멘트 상태를 가진 가짜 934. A 뒤 start_quiet 초 동안 E 무응답 → 그 뒤 fil=1·emis=1(on_ok 이면).
    B 는 즉시 fil=0. A·B 에는 응답 바이트가 없다."""

    def __init__(self, fil=False, start_quiet=0.3, on_ok=True, fail_code=7):
        self.fil = fil
        self.start_quiet = start_quiet
        self.on_ok = on_ok
        self.fail_code = fail_code
        self.a_t = None

    def replies(self):
        return {b"F": F_REPLY, b"E": self._e, b"A": self._a, b"B": self._b}

    def _a(self):
        self.a_t = time.monotonic()
        return None

    def _b(self):
        self.fil = False
        self.a_t = None
        return None

    def _e(self):
        if self.a_t is not None:
            if time.monotonic() - self.a_t < self.start_quiet:
                return None                                 # 시동 중 무응답
            self.fil = self.on_ok
            self.a_t = None
        if self.fil:
            return e_with(1, 1)
        return e_with(0, 0, code=(self.fail_code if not self.on_ok else 0))


def make_fil(g, **kw):
    s = Script(replies=g.replies())
    kw.setdefault("quiet_sec", 0.8)
    kw.setdefault("on_timeout_sec", 1.5)
    kw.setdefault("off_timeout_sec", 1.0)
    kw.setdefault("relight_lockout_sec", 0.5)
    kw.setdefault("e_block_sec", 0.15)
    kw.setdefault("fail_limit", 2)
    c, rec = make(s, **kw)
    rec["fil"] = []
    c.filament_result.connect(lambda a, ok, d: rec["fil"].append((a, ok, d)))
    return s, c, rec


def test_filament_on_quiet_start_then_success(qapp):
    g = Gauge934(fil=False, start_quiet=0.4)
    s, c, rec = make_fil(g)
    c.connect_device()
    spin(250)
    c.filament_on()
    spin(1300)
    closes = s.closes
    c.cleanup()
    assert b"A" in s.writes and b"B" not in s.writes
    assert closes == 0                                      # 시동 무응답에도 포트를 닫지 않는다
    assert any(x.comm == "starting" and x.filament_starting for x in rec["snap"])
    assert rec["fil"] == [("on", True, "필라멘트 켜짐·방출 정상")]
    assert not any(e["종류"] == "실패" for e in rec["ev"])
    ok = [x for x in rec["snap"] if x.ok and x.reading.filament_on]
    assert ok and ok[-1].emission_since == ok[0].reading.t     # 방출이 처음 확인된 시각을 유지


def test_quiet_expired_counts_failures_again(qapp):
    g = Gauge934(fil=False, start_quiet=99)                # A 뒤 계속 무응답
    s, c, rec = make_fil(g, quiet_sec=0.4, on_timeout_sec=5)
    c.connect_device()
    spin(250)
    c.filament_on()
    spin(1300)
    c.cleanup()
    assert s.closes >= 1                                    # 시동 구간이 지나면 기존대로 실패를 센다
    assert any(x.comm == "timeout" for x in rec["snap"])


def test_a_after_b_is_delayed_by_lockout(qapp):
    g = Gauge934(fil=True)
    s, c, rec = make_fil(g, relight_lockout_sec=0.6)
    c.connect_device()
    spin(250)
    c.filament_off()
    spin(50)
    c.filament_on()
    spin(1800)
    c.cleanup()
    ib, ia = s.writes.index(b"B"), s.writes.index(b"A")
    assert ia > ib and s.write_times[ia] - s.write_times[ib] >= 0.59
    assert rec["fil"][0][0] == "off" and rec["fil"][0][1] is False and "취소" in rec["fil"][0][2]
    assert rec["fil"][-1] == ("on", True, "필라멘트 켜짐·방출 정상")


def test_e_not_sent_for_block_after_a(qapp):
    g = Gauge934(fil=False, start_quiet=0.0)
    s, c, rec = make_fil(g, e_block_sec=0.5)
    c.connect_device()
    spin(250)
    c.filament_on()
    spin(900)
    c.cleanup()
    ia = s.writes.index(b"A")
    nxt = [t for w, t in zip(s.writes[ia + 1:], s.write_times[ia + 1:]) if w == b"E"]
    assert nxt and nxt[0] - s.write_times[ia] >= 0.49


def test_a_waits_for_inflight_e(qapp):
    g = Gauge934(fil=False, start_quiet=0.0)
    s, c, rec = make_fil(g)
    s.delay_ms = 60
    c.connect_device()
    spin(250)
    while c._pending != "E":                                # E 가 나간 직후를 잡는다
        spin(5)
    c.filament_on()
    assert s.writes[-1] == b"E"                             # 아직 A 를 보내지 않았다
    spin(400)
    c.cleanup()
    i = len(s.writes) - 1 - s.writes[::-1].index(b"A")
    assert s.writes[i - 1] == b"E"


def test_filament_on_failure_reports_error_code(qapp):
    g = Gauge934(fil=False, start_quiet=0.1, on_ok=False, fail_code=7)
    s, c, rec = make_fil(g, on_timeout_sec=0.8)
    c.connect_device()
    spin(250)
    c.filament_on()
    spin(1400)
    c.cleanup()
    a, ok, d = rec["fil"][-1]
    assert (a, ok) == ("on", False)
    assert "오류 코드 7" in d and "필라멘트 전류 없음(필라멘트·케이블 단선 가능)" in d


def test_filament_off_success_and_timeout(qapp):
    g = Gauge934(fil=True)
    s, c, rec = make_fil(g)
    c.connect_device()
    spin(250)
    c.filament_off()
    spin(700)
    assert rec["fil"] == [("off", True, "필라멘트 꺼짐")]
    snaps = [x for x in rec["snap"] if x.ok]
    assert snaps[-1].emission_since is None                 # 꺼지면 방출 시각을 지운다
    c.cleanup()
    # B 를 무시하는 장비 — 기한 안에 꺼짐 확인 안 됨
    g2 = Gauge934(fil=True)
    g2._b = lambda: None
    s2, c2, rec2 = make_fil(g2, off_timeout_sec=0.6)
    s2.replies[b"B"] = g2._b
    c2.connect_device()
    spin(250)
    c2.filament_off()
    spin(1200)
    c2.cleanup()
    assert rec2["fil"] == [("off", False, "1초 안에 필라멘트 꺼짐 확인 안 됨")]


def test_filament_request_without_connection_fails_fast(qapp):
    c = IonGaugeController("COM12", port_factory=lambda p: FakeIgPort(Script(), p), port_lister=lambda: [])
    got = []
    c.filament_result.connect(lambda a, ok, d: got.append((a, ok, d)))
    c.filament_on()
    assert got == [("on", False, "이온 게이지가 연결돼 있지 않음")]


class Gauge934B(Gauge934):
    """켜져 있던 필라멘트에 B 를 보내면 stop_quiet 초 동안 E 무응답(2026-10-13 실측 약 3.7초)."""

    def __init__(self, stop_quiet=0.4, **kw):
        super().__init__(**kw)
        self.stop_quiet = stop_quiet
        self.b_t = None

    def _b(self):
        if self.fil:
            self.b_t = time.monotonic()
        self.fil = False
        self.a_t = None
        return None

    def _e(self):
        if self.b_t is not None and time.monotonic() - self.b_t < self.stop_quiet:
            return None
        return super()._e()


def test_b_quiet_no_port_close_then_off_success(qapp):
    """B 뒤 무응답(실측 4초를 0.4초로 축소)에도 포트를 닫지 않고 '끄는 중' — 유예 뒤 fil=0 확인."""
    g = Gauge934B(fil=True, stop_quiet=0.4)
    s, c, rec = make_fil(g, quiet_sec=0.8, off_timeout_sec=2.0)
    c.connect_device()
    spin(250)
    c.filament_off()
    spin(1300)
    closes = s.closes
    c.cleanup()
    assert closes == 0 and b"B" in s.writes
    assert any(x.comm == "stopping" and x.filament_stopping and not x.filament_starting for x in rec["snap"])
    assert rec["fil"] == [("off", True, "필라멘트 꺼짐")]
    assert not any(e["종류"] in ("실패", "재연결") for e in rec["ev"])      # 종료 중 끊김·재연결 이벤트 없음
    assert any("필라멘트 끄는 중 — 무응답을 실패로 세지 않음" in m for _, m in rec["msg"])


def test_b_quiet_expired_counts_failures_again(qapp):
    g = Gauge934B(fil=True, stop_quiet=99)                 # B 뒤 계속 무응답
    s, c, rec = make_fil(g, quiet_sec=0.4, off_timeout_sec=5)
    c.connect_device()
    spin(250)
    c.filament_off()
    spin(1300)
    c.cleanup()
    assert s.closes >= 2                                    # 유예가 끝나면 기존대로 실패 → 닫고 다시 열기(+정리 1)
    assert any(x.comm == "timeout" for x in rec["snap"])


def test_off_confirm_default_is_20s():
    c = IonGaugeController("COM12", port_factory=lambda p: FakeIgPort(Script(), p), port_lister=lambda: [])
    assert c.off_timeout_sec == 20.0
    from lib.config import IG_FILAMENT_SWITCH_QUIET_SEC
    assert c.quiet_sec == float(IG_FILAMENT_SWITCH_QUIET_SEC) == 12.0



def test_response_timeout_default_1200ms_from_config():
    from lib.config import IG_RESPONSE_TIMEOUT_MS
    c = IonGaugeController("COM12", port_factory=lambda p: FakeIgPort(Script(), p), port_lister=lambda: [])
    assert IG_RESPONSE_TIMEOUT_MS == 1200 and c.timeout_ms == 1200


def test_860ms_reply_is_not_a_failure_outside_quiet(qapp):
    """레인지 찾기 중 E 응답 860 ms(2026-10-14 실측) — 유예 구간 밖에서도 실패로 세지 않는다."""
    s = Script(delay_ms=860)
    c, rec = make(s, timeout_ms=None, poll_ms=1500)           # 기본 응답 제한(1200 ms)
    c.connect_device()
    spin(4200)
    c.cleanup()
    assert c.timeout_ms == 1200
    assert any(x.fresh and x.comm == "ok" for x in rec["snap"])
    assert not any(x.comm == "timeout" for x in rec["snap"]) and rec["ev"] == []


def test_over_1200ms_reply_counts_one_failure(qapp):
    calls = {"n": 0}

    def e_reply():
        calls["n"] += 1
        s.delay_ms = 1350 if calls["n"] == 2 else 30          # 두 번째 E 만 1.35초 뒤에 온다
        return E_REPLY

    s = Script(replies={b"F": F_REPLY, b"E": e_reply}, delay_ms=30)
    c, rec = make(s, timeout_ms=None, poll_ms=1600, fail_limit=3)
    c.connect_device()
    spin(5200)
    c.cleanup()
    comms = [x.comm for x in rec["snap"]]
    assert comms.count("timeout") == 1 and "ok" in comms[comms.index("timeout") + 1:]
    assert s.closes <= 1                                     # 1회 실패로는 포트를 닫지 않는다(정리 1회만)
    assert [e["종류"] for e in rec["ev"]] == ["실패", "복구"]
