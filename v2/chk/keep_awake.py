"""절전 막기(W-23 H3) — 공정·수동 조작·히터 운전 중에 윈도우가 시스템 절전(대기 모드)에 들어가지 않게 한다.

윈도우 SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) — 부른 스레드가 살아 있는 동안만 유효하므로 이 모듈의 스레드
하나(keep-awake)가 걸고 그 스레드가 푼다(ES_CONTINUOUS 만). 화면 꺼짐(ES_DISPLAY_REQUIRED)은 막지 않는다.
설정 KEEP_AWAKE: run(운전 중만 — 기본) · always(프로그램이 켜져 있는 동안) · off. 윈도우가 아니면 아무것도 하지 않는다.
걸 때·풀 때 사건 한 줄(상세 — keep_awake_on·keep_awake_off). 시험은 가짜 API(api=호출 기록 함수)로.
"""
from __future__ import annotations

import sys
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
CHECK_S = 1.0


@dataclass(frozen=True)
class AwakeEvent:
    kind: str                 # keep_awake_on / keep_awake_off
    detail: str
    reason: Optional[str] = None
    device: str = "app"
    data: Optional[dict] = None


def windows_api() -> Optional[Callable[[int], int]]:
    """윈도우면 kernel32.SetThreadExecutionState, 아니면 None."""
    if not sys.platform.startswith("win"):
        return None
    try:
        import ctypes
        f = ctypes.windll.kernel32.SetThreadExecutionState          # type: ignore[attr-defined]
        f.restype = ctypes.c_uint
        f.argtypes = [ctypes.c_uint]
        return f
    except Exception:
        return None


class KeepAwake:
    def __init__(self, cfg: Dict[str, Any], busy: Callable[[], bool], on_event: Optional[Callable[[AwakeEvent], None]] = None,
                 api: Optional[Callable[[int], int]] = None) -> None:
        self.cfg, self.busy, self.on_event = cfg, busy, on_event
        self.api = api                   # None = 윈도우가 아님(아무것도 하지 않음)
        self.held = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def want(self) -> bool:
        mode = str(self.cfg.get("KEEP_AWAKE") or "run")
        if mode == "always":
            return True
        if mode == "off":
            return False
        try:
            return bool(self.busy())
        except Exception:
            return self.held             # 판단 실패 — 지금 상태 그대로

    def tick(self) -> None:
        """한 번 판단 — keep-awake 스레드가 1초마다 부른다(시험은 직접)."""
        if self.api is None:
            return
        w = self.want()
        if w == self.held:
            return
        self.api(ES_CONTINUOUS | ES_SYSTEM_REQUIRED if w else ES_CONTINUOUS)
        self.held = w
        why = "프로그램이 켜져 있는 동안" if str(self.cfg.get("KEEP_AWAKE")) == "always" else "운전 중"
        if self.on_event:
            self.on_event(AwakeEvent("keep_awake_on" if w else "keep_awake_off",
                                     f"윈도우 절전 막기 {'시작' if w else '끝'}({why})" if w else "윈도우 절전 막기 끝"))

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                self.tick()
                self._stop.wait(CHECK_S)
        finally:
            if self.held and self.api is not None:
                self.api(ES_CONTINUOUS)                   # 같은 스레드에서 푼다
                self.held = False
                if self.on_event:
                    self.on_event(AwakeEvent("keep_awake_off", "윈도우 절전 막기 끝(프로그램 종료)"))

    def start(self) -> None:
        if self.api is None:
            return
        self._thread = threading.Thread(target=self._loop, name="keep-awake", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
