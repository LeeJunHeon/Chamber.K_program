# -*- coding: utf-8 -*-
"""T2 — lib/serial_guard: open/close 가 호출 스레드를 timeout 이상 묶지 않는다."""
import threading
import time

from lib.serial_guard import open_guarded, close_guarded


class SlowObj:
    def __init__(self):
        self.closed = threading.Event()

    def close(self):
        self.closed.set()


class SlowClose:
    def __init__(self):
        self.started = threading.Event()

    def close(self):
        self.started.set()
        time.sleep(5.0)


def test_open_guarded_times_out_and_late_object_is_closed():
    made = {}

    def factory():
        time.sleep(5.0)
        made["obj"] = SlowObj()
        return made["obj"]

    t0 = time.monotonic()
    r = open_guarded(factory, 2.0)
    took = time.monotonic() - t0
    assert r is None
    assert took < 2.3
    # 늦게 만들어진 객체는 그 스레드가 스스로 닫는다
    assert "obj" not in made or made["obj"].closed.wait(5.0)
    time.sleep(3.3)
    assert made["obj"].closed.is_set()


def test_open_guarded_returns_object_when_fast_and_none_on_error():
    obj = object()
    assert open_guarded(lambda: obj, 1.0) is obj

    def boom():
        raise OSError("no port")
    assert open_guarded(boom, 1.0) is None


def test_close_guarded_returns_immediately():
    o = SlowClose()
    t0 = time.monotonic()
    close_guarded(o, "slow")
    assert time.monotonic() - t0 < 0.1
    assert o.started.wait(1.0)          # 닫기는 데몬 스레드에서 실제로 시작됐다
