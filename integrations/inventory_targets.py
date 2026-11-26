# integrations/inventory_targets.py
# -*- coding: utf-8 -*-
"""재고관리 장착 타겟 — 재고관리 서버의 챔버 자리 목록(Inventory_web_app app/api/chamber-slots, 읽기만)에서
챔버K Gun1·Gun2 에 장착된 타겟을 읽어, 타겟 이름을 비워 둔 공정의 기록(ChK_log.csv·공정 로그·구글챗·ERP)을 채운다.

- 읽기: 전용 데몬 스레드가 시작 직후 한 번, 그 뒤 refresh_s 마다 읽어 값을 기억한다. 공정 시작은 기억한 값만 쓴다
  (메인 스레드에서 기다리지 않는다 — 히터 램프 같은 메인 스레드 타이머가 밀리지 않게).
- 오래된 값은 쓰지 않는다: 마지막으로 읽기에 성공한 지 max_age_s 를 넘었으면 current() 가 값을 주지 않는다.
- 실패는 공정을 막지 않는다. 사건(첫 실패 1번·회복 1번·값 바뀜)만 쌓아 두고 메인 스레드가 pop_events() 로 꺼내 간다.
- 자리 이름: "Chamber K - Gun 1" / "Sputter 2 - Chamber K - Gun 1" — '-' 로 자르지 않고 'Chamber K' 와 'Gun N' 을 따로 찾는다
  (대소문자·공백 무시, "Chamber K2"·"Chamber 1" 은 아님). rewrite/v2 의 v2/chk/targets.gun_of 와 같은 규칙.
- 채울 글: 챔버12(Chamber.Total_program util/inventory_client.py)와 같은 "itemName (targetUnitId)", 번호가 없으면 itemName.

표준 라이브러리만 쓴다(PyQt6·main·UI·controller·device 를 import 하지 않는다).
"""
from __future__ import annotations

import json
import re
import threading
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

MAX_BODY_BYTES = 2 * 1024 * 1024          # 응답 읽기 상한(2 MB)
GUN_KEYS = ("g1", "g2")

_CHAMBER = re.compile(r"(?<![0-9A-Za-z])chamber\s*k(?![0-9A-Za-z])", re.IGNORECASE)
_GUN = re.compile(r"(?<![0-9A-Za-z])gun\s*(\d+)(?![0-9A-Za-z])", re.IGNORECASE)

Mounted = Dict[str, Optional[dict]]
FetchFn = Callable[[str, float], Any]


def gun_of(location: Any) -> Optional[int]:
    """자리 이름 → 챔버K 의 건 번호(1·2). 'Chamber K' 와 'Gun N' 이 함께 있어야 한다. 아니면 None."""
    s = str(location or "")
    if not _CHAMBER.search(s):
        return None
    m = _GUN.search(s)
    if not m:
        return None
    n = int(m.group(1))
    return n if n in (1, 2) else None


def _s(v: Any) -> Optional[str]:
    """값 → 앞뒤 공백을 뗀 글. None·빈 글이면 None."""
    if v is None:
        return None
    t = str(v).strip()
    return t or None


def target_text(name: str, unit_id: Optional[str]) -> str:
    """기록에 채울 글 — "itemName (targetUnitId)", 번호가 없으면 itemName 만."""
    return f"{name} ({unit_id})" if unit_id else name


def parse_slots(data: Any) -> Mounted:
    """응답 JSON(배열) → {"g1": 타겟 또는 None, "g2": 타겟 또는 None}.

    타겟 = {"text": 채울 글, "name", "unitId", "barcode", "loadedAt", "location"}. 그 건 자리에 타겟이 없으면(itemName null) None.
    같은 건 자리가 둘 이상이면 loadedAt 이 늦은 것(ISO 글 비교). 배열이 아니면 ValueError."""
    if not isinstance(data, list):
        raise ValueError(f"응답이 배열이 아님({type(data).__name__})")
    out: Mounted = {"g1": None, "g2": None}
    best: Dict[str, str] = {}
    for it in data:
        if not isinstance(it, dict):
            continue
        n = gun_of(it.get("locationName"))
        if n is None:
            continue
        key = f"g{n}"
        name = _s(it.get("itemName"))
        if not name:
            continue
        at = _s(it.get("loadedAt")) or ""
        if key in best and at <= best[key]:
            continue
        best[key] = at
        unit = _s(it.get("targetUnitId"))
        out[key] = {"text": target_text(name, unit), "name": name, "unitId": unit,
                    "barcode": _s(it.get("barcodeCode")), "loadedAt": _s(it.get("loadedAt")),
                    "location": _s(it.get("locationName"))}
    return out


def http_fetch(url: str, timeout_s: float) -> Any:
    """GET → JSON. reporter.py 와 같은 방식(urllib.request.urlopen — 부를 때 찾는다). 200 이 아니면 ValueError(HTTP 상태).
    응답은 최대 2 MB 까지만 읽는다. 인증서 검증은 기본(끄지 않음), 전역 소켓 설정은 건드리지 않는다."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as res:
        status = getattr(res, "status", None)
        if status is None:
            status = res.getcode()
        if status != 200:
            raise ValueError(f"HTTP {status}")
        body = res.read(MAX_BODY_BYTES + 1)
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("응답이 2 MB 를 넘음")
    return json.loads(body.decode("utf-8"))


def _reason_of(e: BaseException) -> str:
    """예외 → 짧은 사유 글."""
    msg = str(e).strip()
    return f"{type(e).__name__}: {msg[:120]}" if msg else type(e).__name__


class InventoryTargetReader:
    """재고관리 장착 타겟 읽기 스레드 하나. 값·시각은 lock 으로 보호하고, 메인 스레드는 current()·pop_events() 로만 본다.

    fetch = (url, 제한 s) → JSON(시험은 가짜), clock = 단조 시계(시험은 가짜)."""

    def __init__(self, url: str, refresh_s: float, timeout_s: float, max_age_s: float,
                 fetch: FetchFn = http_fetch, clock: Callable[[], float] = time.monotonic) -> None:
        self.url = url
        self.refresh_s = float(refresh_s)
        self.timeout_s = float(timeout_s)
        self.max_age_s = float(max_age_s)
        self.fetch = fetch
        self.clock = clock
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._mounted: Optional[Mounted] = None          # 마지막으로 읽기에 성공한 값(실패해도 그대로)
        self._ok_at: Optional[float] = None              # 마지막 성공 시각(clock)
        self._ok_wall: Optional[float] = None            # 마지막 성공 시각(벽시계 — 화면 "읽은 시각")
        self._error: Optional[str] = None                # 마지막 실패 사유(성공하면 None)
        self._failing = False
        self._events: List[Tuple[str, Any]] = []

    @property
    def host(self) -> str:
        """주소의 호스트 이름(로그용)."""
        return urlsplit(self.url).hostname or self.url

    # ---------- 읽기 ----------
    def read_once(self) -> bool:
        """한 번 읽는다 — 성공이면 True. 실패면 이전 값은 그대로 두고 사유만 기억한다(첫 실패에만 사건 1번)."""
        try:
            got = parse_slots(self.fetch(self.url, self.timeout_s))
        except Exception as e:
            why = _reason_of(e)
            with self._lock:
                self._error = why
                if not self._failing:
                    self._failing = True
                    self._events.append(("failed", why))
            return False
        now, wall = self.clock(), time.time()
        with self._lock:
            if self._failing:
                self._failing = False
                self._events.append(("recovered", None))
            if self._mounted is None or _texts(self._mounted) != _texts(got):
                self._events.append(("changed", {k: (v["text"] if v else None) for k, v in got.items()}))
            self._mounted = got
            self._ok_at, self._ok_wall = now, wall
            self._error = None
        return True

    def _run(self) -> None:
        """스레드 본문 — 곧바로 한 번, 그 뒤 refresh_s 마다 읽는다(stop() 이면 끝)."""
        while not self._stop.is_set():
            try:
                self.read_once()
            except Exception as e:                       # read_once 는 예외를 삼키지만 혹시 몰라 사유를 남긴다
                with self._lock:
                    self._error = _reason_of(e)
            if self._stop.wait(self.refresh_s):
                break

    def start(self) -> None:
        """읽기 데몬 스레드를 시작한다(이미 돌면 그대로)."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="InventoryTargetReader", daemon=True)
        self._thread.start()

    def stop(self, wait_s: float = 1.0) -> None:
        """스레드를 멈춘다. 읽는 중이면 wait_s 까지만 기다린다(데몬이라 종료를 막지 않는다)."""
        self._stop.set()
        t = self._thread
        if t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(wait_s)

    # ---------- 메인 스레드가 보는 것 ----------
    def current(self) -> Tuple[Optional[Mounted], str]:
        """지금 쓸 수 있는 값과 사유. 마지막 성공이 max_age_s 이내일 때만 값을 준다.

        사유: "" (값 있음) / "아직 읽기 전" / "재고관리를 N분째 읽지 못함"."""
        with self._lock:
            mounted, ok_at = self._mounted, self._ok_at
        if mounted is None or ok_at is None:
            return None, "아직 읽기 전"
        age = self.clock() - ok_at
        if age > self.max_age_s:
            return None, f"재고관리를 {max(1, int(age // 60))}분째 읽지 못함"
        return dict(mounted), ""

    def last_ok_wall(self) -> Optional[float]:
        """마지막으로 읽기에 성공한 벽시계 시각(time.time). 없으면 None."""
        with self._lock:
            return self._ok_wall

    def last_error(self) -> Optional[str]:
        """마지막 실패 사유(마지막 읽기가 성공이면 None)."""
        with self._lock:
            return self._error

    def pop_events(self) -> List[Tuple[str, Any]]:
        """쌓인 사건을 꺼낸다: ("failed", 사유) / ("recovered", None) / ("changed", {"g1": 글 또는 None, "g2": …})."""
        with self._lock:
            ev, self._events = self._events, []
        return ev


def _texts(mounted: Mounted) -> Dict[str, Optional[str]]:
    """값 비교용 — 건별 채울 글만."""
    return {k: (v["text"] if v else None) for k, v in mounted.items()}


def fill_target_names(params: dict, mounted: Optional[Mounted]) -> List[Tuple[int, str]]:
    """params 의 g1/g2_target_name 을 그 자리에서 채우고, 채운 (건 번호, 글) 목록을 돌려준다.

    채우는 조건: 그 건을 쓰고(use_gN 참) 이름이 비어 있고, mounted 에 그 건 타겟이 있을 때만.
    입력값·레시피 값이 있으면 그대로, 쓰지 않는 건은 건드리지 않는다. mounted 가 None 이면 아무것도 하지 않는다."""
    filled: List[Tuple[int, str]] = []
    if not mounted:
        return filled
    for n in (1, 2):
        if not bool(params.get(f"use_g{n}")):
            continue
        key = f"g{n}_target_name"
        if str(params.get(key) or "").strip():
            continue
        tgt = mounted.get(f"g{n}")
        if not tgt or not tgt.get("text"):
            continue
        params[key] = tgt["text"]
        filled.append((n, tgt["text"]))
    return filled
