# lib/mfc_state_log.py
# -*- coding: utf-8 -*-
"""MFC 압력·밸브 위치 1초 CSV(mfc/MFC_YYYYMMDD.csv) — 행 만들기와 백그라운드 쓰기.

파일 쓰기는 화면 스레드·MFC 스레드에서 하지 않는다(대상이 NAS UNC 라 느리거나 멈출 수 있다).
전용 작업자 스레드 하나가 대기열에서 행 묶음을 순서대로 꺼내 쓴다.
  - 평소: 기록 폴더(NAS)에 쓴다.
  - NAS 폴더를 못 만들거나 파일 쓰기가 실패(예: 누가 엑셀로 열어 잠김)하면 같은 파일 이름으로 로컬(./Logs/mfc)에 쓴다.
    NAS 가 돌아오면 다음 묶음부터 다시 NAS 에 쓴다(로컬에 쌓인 것을 합치지는 않는다).
  - 로컬까지 실패할 때만 버리고 개수를 센다.
작업자는 모니터에 경고를 직접 쓰지 않는다 — 개수만 세고, main 이 take_notices() 로 실행당 1회씩만 가져간다.
예외는 밖으로 내보내지 않는다.
"""
import csv
import datetime
import io
import queue
import re
import threading
from pathlib import Path
from typing import List, Optional, Tuple

HEADER = ["Timestamp", "Pressure_mTorr", "Valve_pct"]
_QUEUE_MAX = 360          # 묶음(10초치) 수 상한 — NAS 가 오래 멈춰도 메모리가 늘지 않게


def format_valve(text: Optional[str]) -> str:
    """받은 밸브 값 문자열("+035.20" 등) → 앞의 부호·0 만 정리하고 받은 소수 자리 그대로("35.20"). None → 빈 칸."""
    if text is None:
        return ""
    t = str(text).strip().lstrip("+")
    m = re.match(r"^-?\d+(?:\.(\d+))?$", t)
    if not m:
        return ""
    nd = len(m.group(1) or "")
    return f"{float(t):.{nd}f}"


def format_pressure(ui_val: Optional[float], decimals: int) -> str:
    """압력(화면 단위) → MFC_PRESSURE_DECIMALS 자리 문자열. 음수도 그대로. None → 빈 칸."""
    if ui_val is None:
        return ""
    s = f"{float(ui_val):.{int(decimals)}f}"
    return s[1:] if s.startswith("-") and float(s) == 0.0 else s       # "-0.00" 은 "0.00"


def format_timestamp(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def make_row(sample: dict, decimals: int) -> Optional[Tuple[str, List[str]]]:
    """state_sample dict → (YYYYMMDD, [Timestamp, Pressure, Valve]). 둘 다 비면 None(행 없음).
    sample: {"time": datetime, "pressure": float|None(화면 단위), "valve": str|None(받은 숫자 문자열)}"""
    try:
        p, v = sample.get("pressure"), sample.get("valve")
        if p is None and v is None:
            return None
        dt = sample.get("time") or datetime.datetime.now()
        return dt.strftime("%Y%m%d"), [format_timestamp(dt), format_pressure(p, decimals), format_valve(v)]
    except Exception:
        return None


class MfcStateCsvWriter:
    """행 묶음을 백그라운드 스레드 하나에서 순서대로 쓴다. submit 은 막지 않는다."""

    def __init__(self, primary_dir, local_dir=None, queue_max: int = _QUEUE_MAX):
        self.primary_dir = Path(primary_dir)
        self.local_dir = Path(local_dir) if local_dir is not None else Path.cwd() / "Logs" / "mfc"
        self._q: "queue.Queue" = queue.Queue(maxsize=queue_max)
        self._lock = threading.Lock()
        self.rows_primary = 0
        self.rows_local = 0
        self.rows_dropped = 0
        self._reported = set()
        self._thread = threading.Thread(target=self._run, name="MfcStateCsvWriter", daemon=True)
        self._thread.start()

    # ---------- 호출 쪽(main) ----------
    def submit(self, rows: List[Tuple[str, List[str]]]) -> bool:
        """행 묶음을 넘긴다(막지 않는다). 대기열이 가득이면 그 묶음을 버리고 개수를 센다."""
        if not rows:
            return True
        try:
            self._q.put_nowait(list(rows))
            return True
        except Exception:
            with self._lock:
                self.rows_dropped += len(rows)
            return False

    def take_notices(self) -> List[str]:
        """아직 알리지 않은 경고 문구(로컬로 바꿔 씀·버림) — 각각 실행당 1회만 돌려준다."""
        out = []
        with self._lock:
            if self.rows_local and "local" not in self._reported:
                self._reported.add("local")
                out.append(f"MFC 상태 CSV 를 NAS 에 쓸 수 없어 로컬({self.local_dir})에 저장합니다.")
            if self.rows_dropped and "drop" not in self._reported:
                self._reported.add("drop")
                out.append("MFC 상태 CSV 를 NAS·로컬 모두 쓰지 못해 일부 행을 버렸습니다.")
        return out

    def stats(self) -> dict:
        with self._lock:
            return {"primary": self.rows_primary, "local": self.rows_local, "dropped": self.rows_dropped}

    def shutdown(self, timeout: float = 3.0) -> bool:
        """남은 묶음을 쓰고 끝내도록 요청하고 timeout 초까지만 기다린다. 끝났으면 True(NAS 가 멈추면 False — 기다리지 않고 나간다)."""
        try:
            self._q.put(None, timeout=0.2)
        except Exception:
            pass
        try:
            self._thread.join(max(0.0, float(timeout)))
        except Exception:
            pass
        return not self._thread.is_alive()

    # ---------- 작업자 ----------
    def _run(self) -> None:
        while True:
            try:
                rows = self._q.get()
            except Exception:
                return
            if rows is None:
                return
            try:
                self._write_batch(rows)
            except Exception:
                with self._lock:
                    self.rows_dropped += len(rows)

    def _write_batch(self, rows: List[Tuple[str, List[str]]]) -> None:
        by_day: dict = {}
        for day, fields in rows:
            by_day.setdefault(day, []).append(fields)
        for day, recs in by_day.items():
            name = f"MFC_{day}.csv"
            if self._append(self.primary_dir, name, recs, make_dir=True):
                with self._lock:
                    self.rows_primary += len(recs)
            elif self._append(self.local_dir, name, recs, make_dir=True):
                with self._lock:
                    self.rows_local += len(recs)
            else:
                with self._lock:
                    self.rows_dropped += len(recs)

    @staticmethod
    def _append(base: Path, name: str, recs: List[List[str]], make_dir: bool) -> bool:
        try:
            if make_dir:
                base.mkdir(parents=True, exist_ok=True)
            path = base / name
            new = not path.exists() or path.stat().st_size == 0
            buf = io.StringIO(newline="")
            w = csv.writer(buf, lineterminator="\r\n")
            if new:
                w.writerow(HEADER)
            w.writerows(recs)
            with open(path, "a", newline="", encoding="utf-8-sig" if new else "utf-8") as f:
                f.write(buf.getvalue())
            return True
        except Exception:
            return False
