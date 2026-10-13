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
def test_only_e_and_f_can_be_sent(qapp, cmd):
    s = Script()
    c, _ = make(s)
    c.connect_device()
    n = len(s.writes)
    with pytest.raises(ValueError):
        c._send(cmd)
    c.cleanup()
    assert len(s.writes) == n and ALLOWED_COMMANDS == frozenset({"E", "F"})


def test_single_write_path_in_source():
    """포트에 쓰는 곳은 _send 한 곳뿐(허용 목록 검사를 거치지 않는 쓰기 없음)."""
    import inspect
    import device.IonGauge as M
    src = inspect.getsource(M)
    assert src.count(".write(") == 1 and "self.serial.write(cmd.encode" in src
    for bad in ('"A"', '"B"', '"C"', '"D"', "b\"A\"", "b\"B\""):
        assert bad not in src


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
