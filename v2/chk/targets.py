"""장착 타겟(W-22 G) — 재고관리 서버의 챔버 자리 목록(Inventory_web_app app/api/chamber-slots, 읽기만)에서 이 장비의
Gun1·Gun2 에 장착된 타겟을 읽는다. 사내망(192.168.0.0/24)은 로그인 없이 읽는다 — 장비 프로그램이 직접 GET.

- 주소: 설치값 INVENTORY_API_URL(비우면 읽지 않음). sim 은 루프백(127.0.0.1·localhost — 시험용 가짜 서버) 주소만 읽는다
  (sim 이 진짜 재고관리에 묻지 않게 — app.start_targets).
- 읽는 때: 시작(연결) 때, INVENTORY_PERIOD_S(5분)마다, 공정 시작 때(최대 START_WAIT_S 3 s 기다림 — core). 자기 스레드,
  요청 하나 시간 제한 INVENTORY_TIMEOUT_S(3 s). 실패하면 info 이벤트 하나(이어지는 실패는 다시 알리지 않음)만 — 공정은 그대로.
- 자리 이름: "Sputter 2 - Chamber K - Gun 1" 처럼 '-' 로 나뉜 조각이 셋 이상일 수 있다 — '-' 로 둘로 나누지 않고
  'Chamber K' 와 'Gun N' 을 따로 찾는다(대소문자·공백 무시). 'Chamber K' 뒤에 글자·숫자가 붙은 이름(Chamber K2 등)은 아니다.
- 필드: locationName·targetUnitId·itemName·barcodeCode·materialCode·purity·loadedAt. 화면·레시피 글은 "itemName (#targetUnitId)".
- 공정 시작 때 레시피의 G1·G2 타겟 이름이 비어 있으면 이 글로 채운다(레시피 값이 먼저) — process_engine._fill_targets.
"""
from __future__ import annotations

import json
import re
import ssl
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

INVENTORY_TIMEOUT_S = 3.0
INVENTORY_PERIOD_S = 300.0
START_WAIT_S = 3.0
_CHAMBER = re.compile(r"(?<![0-9A-Za-z])chamber\s*k(?![0-9A-Za-z])", re.IGNORECASE)
_GUN = re.compile(r"(?<![0-9A-Za-z])gun\s*(\d+)(?![0-9A-Za-z])", re.IGNORECASE)


@dataclass(frozen=True)
class TargetEvent:
    kind: str                 # targets_read_failed / targets_read_ok / targets_changed
    detail: str
    reason: Optional[str] = None
    device: str = "inventory"
    data: Optional[dict] = None


def gun_of(location: Any) -> Optional[int]:
    """자리 이름 → Gun 번호(1·2). 'Chamber K' 와 'Gun N' 이 함께 있어야 한다. 아니면 None."""
    s = str(location or "")
    if not _CHAMBER.search(s):
        return None
    m = _GUN.search(s)
    if not m:
        return None
    n = int(m.group(1))
    return n if n in (1, 2) else None


def _items(data: Any) -> List[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for k in ("slots", "items", "data", "chamberSlots"):
            if isinstance(data.get(k), list):
                return [x for x in data[k] if isinstance(x, dict)]
    raise ValueError("목록 모양이 아님")


def _s(v: Any) -> Optional[str]:
    if v is None:
        return None
    t = str(v).strip()
    return t or None


def parse_slots(data: Any) -> Dict[str, Optional[dict]]:
    """응답 JSON → {"g1": 타겟 또는 None, "g2": …}. 그 Gun 자리가 있는데 타겟이 없으면(itemName 없음) None.
    같은 Gun 자리가 여럿이면 loadedAt 이 가장 늦은 것."""
    out: Dict[str, Optional[dict]] = {"g1": None, "g2": None}
    best: Dict[str, str] = {}
    for it in _items(data):
        n = gun_of(it.get("locationName"))
        if n is None:
            continue
        g = f"g{n}"
        name, unit = _s(it.get("itemName")), _s(it.get("targetUnitId"))
        if not name:
            continue
        at = _s(it.get("loadedAt")) or ""
        if g in best and at <= best[g]:
            continue
        best[g] = at
        out[g] = {"name": name, "unitId": unit, "barcode": _s(it.get("barcodeCode")), "material": _s(it.get("materialCode")),
                  "purity": _s(it.get("purity")), "loadedAt": _s(it.get("loadedAt")), "location": _s(it.get("locationName")),
                  "text": f"{name} (#{unit})" if unit else name}
    return out


def is_loopback(url: str) -> bool:
    h = (urlsplit(url).hostname or "").lower()
    return h in ("127.0.0.1", "localhost", "::1")


def http_fetch(url: str, timeout: float) -> Any:
    """GET → JSON. 프록시 없이 바로(사내망). 로그인·토큰 없음."""
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "chk-v2"})
    handlers: list = [urllib.request.ProxyHandler({})]
    if urlsplit(url).scheme == "https":
        handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    with urllib.request.build_opener(*handlers).open(req, timeout=timeout) as r:
        if r.status != 200:
            raise ValueError(f"HTTP {r.status}")
        return json.loads(r.read(2_000_000).decode("utf-8"))


class TargetReader:
    """재고관리 읽기 스레드 하나. wall = 읽은 시각을 정할 벽시계(시스템 시계), fetch = (url, 제한 s) → JSON(시험은 가짜)."""

    def __init__(self, url: str, wall: Callable[[], float] = time.time, on_event: Optional[Callable[[TargetEvent], None]] = None,
                 fetch: Callable[[str, float], Any] = http_fetch, period_s: float = INVENTORY_PERIOD_S,
                 timeout_s: float = INVENTORY_TIMEOUT_S) -> None:
        self.url, self.wall, self.on_event, self.fetch = url, wall, on_event, fetch
        self.period_s, self.timeout_s = period_s, timeout_s
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._done = threading.Condition(self._lock)
        self._reads = 0                                   # 끝난 읽기 수(기다리기용)
        self.mounted: Dict[str, Optional[dict]] = {"g1": None, "g2": None}
        self.read_at: Optional[float] = None
        self.error: Optional[str] = None
        self._failing = False
        self._thread: Optional[threading.Thread] = None

    @property
    def host(self) -> str:
        return urlsplit(self.url).hostname or ""

    def _emit(self, kind: str, detail: str, data: Optional[dict] = None) -> None:
        if self.on_event:
            try:
                self.on_event(TargetEvent(kind, detail, None, "inventory", data))
            except Exception:
                pass

    def read_once(self) -> bool:
        """한 번 읽기 — 성공이면 True. 실패면 예전 값은 그대로 두고 error 만(첫 실패에 info 이벤트 하나)."""
        try:
            got = parse_slots(self.fetch(self.url, self.timeout_s))
        except Exception as e:
            why = type(e).__name__ if not str(e) else f"{type(e).__name__}: {str(e)[:120]}"
            with self._lock:
                self.error = why
                first = not self._failing
                self._failing = True
                self._reads += 1
                self._done.notify_all()
            if first:
                self._emit("targets_read_failed", f"재고관리에서 장착 타겟을 읽지 못했습니다({self.host}, {why}). 공정은 그대로 진행합니다",
                           {"host": self.host, "error": why})
            return False
        with self._lock:
            changed = got != self.mounted
            recovered = self._failing
            self.mounted, self.read_at, self.error, self._failing = got, self.wall(), None, False
            self._reads += 1
            self._done.notify_all()
        if recovered:
            self._emit("targets_read_ok", f"재고관리에서 장착 타겟을 다시 읽었습니다({self.host})")
        if changed:
            txt = " · ".join(f"{g.upper()} {(m or {}).get('text') or '없음'}" for g, m in got.items())
            self._emit("targets_changed", f"장착 타겟 {txt}", {g: (m or {}).get("text") for g, m in got.items()})
        return True

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.read_once()
            self._wake.wait(self.period_s)
            self._wake.clear()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="targets", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def refresh_wait(self, timeout_s: float = START_WAIT_S) -> bool:
        """공정 시작 때 — 지금 읽게 하고 끝날 때까지 최대 timeout_s 기다린다. 끝났으면 True(성공·실패 상관없이)."""
        if self._thread is None or not self._thread.is_alive():
            return False
        with self._lock:
            n = self._reads
        self._wake.set()
        end = time.monotonic() + timeout_s
        with self._lock:
            while self._reads == n:
                left = end - time.monotonic()
                if left <= 0:
                    return False
                self._done.wait(left)
        return True

    def view(self) -> dict:
        """state.targets — Gun1·Gun2 장착 타겟(이름·번호·바코드 …), 읽은 시각(벽시계 초), 실패."""
        with self._lock:
            return {"g1": self.mounted.get("g1"), "g2": self.mounted.get("g2"), "readAt": self.read_at, "error": self.error,
                    "host": self.host}
