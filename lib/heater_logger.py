# lib/heater_logger.py
"""히터 상태 CSV 로거.

PLC 폴링(200ms)으로 올라오는 히터 상태 딕셔너리를 일정 주기로 CSV에 남긴다.
솎아내기(주기 판정)는 호출자(main.update_heater_display)가 한다.

저장 위치는 lib/logger.py 의 NAS_LOG_DIR 을 재사용하고, 접근 실패 시
./Logs 로 폴백한다 (set_process_log_file 과 동일한 정책).
"""

from __future__ import annotations

import csv
import datetime
import time
from pathlib import Path
from typing import Optional

from lib.logger import NAS_LOG_DIR

COLUMNS = [
    "timestamp", "elapsed_sec",
    "pv_c", "sv_c", "ramp_sv_c",
    "mv", "mv_limit", "mv_pct", "pid_err", "cur_sv_c", "est_current_a",
    "ramp_rate_c_per_min", "holdback_c",
    "run", "itl", "fault", "ot", "tc_err", "wd_err",
    "note",
]


class HeaterCsvLogger:
    def __init__(self):
        self._fp = None
        self._writer = None
        self._path: Optional[Path] = None
        self._t0 = 0.0
        self._warned = False          # 쓰기 실패 경고는 최초 1회만

    # ---------- 조회 ----------
    @property
    def path(self) -> Optional[Path]:
        return self._path

    def is_open(self) -> bool:
        return self._fp is not None

    # ---------- 수명 ----------
    def start(self, prefix: str = "HEATER") -> Optional[Path]:
        """새 CSV를 열고 헤더를 쓴다. 이미 열려 있으면 그대로 둔다."""
        if self._fp is not None:
            return self._path

        base_dir = NAS_LOG_DIR
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            base_dir = Path.cwd() / "Logs"
            try:
                base_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                return None

        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = base_dir / f"{prefix}_{stamp}.csv"
        try:
            fp = open(path, "w", encoding="utf-8-sig", newline="")
            writer = csv.writer(fp)
            writer.writerow(COLUMNS)
            fp.flush()
        except Exception:
            return None

        self._fp = fp
        self._writer = writer
        self._path = path
        self._t0 = time.monotonic()
        self._warned = False
        return path

    def write_row(self, st: dict, note: str = ""):
        """한 행 기록 후 즉시 flush. 프로그램이 죽어도 데이터는 남는다."""
        if self._fp is None or self._writer is None:
            return

        def _f(key, default=0.0):
            v = st.get(key, default)
            return default if v is None else v

        def _b(key):
            return 1 if st.get(key) else 0

        try:
            pv = st.get('pv')
            self._writer.writerow([
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                f"{time.monotonic() - self._t0:.1f}",
                "" if pv is None else f"{float(pv):.1f}",
                f"{float(_f('sv')):.1f}",
                f"{float(_f('sv_ramp')):.1f}",
                int(_f('mv', 0)),
                int(_f('mv_limit', 0)),
                f"{float(_f('mv_pct')):.1f}",
                int(_f('pid_err', 0)),
                f"{float(_f('cur_sv')):.1f}",
                f"{float(_f('est_current')):.2f}",
                f"{float(_f('ramp_rate')):.0f}",
                f"{float(_f('holdback')):.1f}",
                _b('run'), _b('itl'), _b('fault'),
                _b('ot'), _b('tc_err'), _b('wd_err'),
                note,
            ])
            self._fp.flush()
        except Exception as e:
            if not self._warned:
                self._warned = True
                print(f"[HeaterCsvLogger] 기록 실패(이후 경고 생략): {e}")

    def stop(self):
        if self._fp is not None:
            try:
                self._fp.flush()
                self._fp.close()
            except Exception:
                pass
        self._fp = None
        self._writer = None
        self._path = None
