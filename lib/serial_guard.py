# lib/serial_guard.py
# -*- coding: utf-8 -*-
"""블로킹 가능성이 있는 시리얼 open/close 를 호출 스레드에서 떼어 낸다.

2026-09-16 사고: PLC 드라이버가 같은 Serial 객체를 close/open 하며 매번 ~30초 블로킹
(USB-시리얼 어댑터가 사라진 뒤 OS 드라이버 호출이 오래 걸린다) → 워커 스레드 락업 →
종료 시 BlockingQueuedConnection 무한 대기 → "응답 없음".

장치 워커 스레드는 OS 드라이버 호출에 묶이지 않는다. open 은 데몬 스레드에서 돌리고
timeout 안에 안 끝나면 None 을 돌려주며, 늦게 만들어진 객체는 그 스레드가 스스로 닫는다.
close 는 데몬 스레드에 넘기고 즉시 반환한다.
"""
from __future__ import annotations
import logging
import threading
from typing import Callable, Optional, Any, Tuple

_log = logging.getLogger("serial_guard")


def open_guarded(factory: Callable[[], Any], timeout_sec: float) -> Tuple[Optional[Any], Optional[str]]:
    """factory() 를 데몬 스레드에서 실행한다. (객체, None) 또는 (None, 사유) 를 돌려준다.
    사유는 f"timeout {t}s" 또는 repr(예외). 늦게 성공한 객체는 누수 방지를 위해 그 스레드가
    스스로 close() 한다. 호출 스레드는 timeout_sec 이상 묶이지 않는다."""
    lock = threading.Lock()
    box = {"obj": None, "err": None, "done": False, "timed_out": False}

    def _run():
        obj = None
        try:
            obj = factory()
        except Exception as e:                 # 예외는 결과에 담아 호출자가 로그로 남긴다
            with lock:
                box["err"] = e
                box["done"] = True
            return
        with lock:
            box["done"] = True
            if box["timed_out"]:
                late = True
            else:
                box["obj"] = obj
                late = False
        if late:
            _close_quietly(obj, "open_guarded(늦게 완료)")

    t = threading.Thread(target=_run, name="serial-open", daemon=True)
    t.start()
    t.join(timeout_sec)
    with lock:
        if not box["done"]:
            box["timed_out"] = True
            _log.warning("open_guarded: %.1fs 안에 열리지 않음 — 포기(늦게 열리면 스스로 닫음)", timeout_sec)
            return None, f"timeout {timeout_sec:g}s"
        if box["err"] is not None:
            _log.warning("open_guarded: 열기 실패: %r", box["err"])
            return None, repr(box["err"])
        return box["obj"], None


def _close_quietly(obj: Any, label: str) -> None:
    try:
        close = getattr(obj, "close", None)
        if close is None:
            # minimalmodbus.Instrument 처럼 serial 속성을 가진 객체
            ser = getattr(obj, "serial", None)
            close = getattr(ser, "close", None)
        if close is not None:
            close()
    except Exception as e:
        _log.warning("close_guarded(%s): 닫기 실패: %r", label, e)


def close_guarded(obj: Any, label: str = "") -> None:
    """obj.close() 를 데몬 스레드에서 실행하고 즉시 반환한다. 예외는 모듈 로거로 남긴다."""
    if obj is None:
        return
    threading.Thread(target=_close_quietly, args=(obj, label or type(obj).__name__),
                     name="serial-close", daemon=True).start()
