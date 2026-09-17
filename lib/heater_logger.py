# lib/heater_logger.py
"""히터 상태 CSV 로거.

PLC 폴링(200ms)으로 올라오는 히터 상태 딕셔너리를 일정 주기로 CSV에 남긴다.
솎아내기(주기 판정)는 호출자(main.update_heater_display)가 한다.

저장 위치는 lib/logger.py 의 NAS_HEATER_LOG_DIR(<CHK>/heater)을 쓰고,
접근 실패 시 ./Logs/heater 로 폴백한다 (set_process_log_file 과 동일한 정책).
"""

from __future__ import annotations

import csv
import datetime
import time
from pathlib import Path
from typing import Optional

from lib.logger import NAS_HEATER_LOG_DIR, log_message_to_monitor

# NAS 폴백 경고는 프로그램 실행당 1회만(인스턴스가 아니라 모듈 전역).
_nas_fallback_warned: bool = False

COLUMNS = [
    "timestamp", "elapsed_sec",
    "pv_c", "sv_c", "ramp_sv_c",
    "mv", "mv_limit", "mv_pct", "pid_err", "cur_sv_c", "est_current_a",
    "ramp_rate_c_per_min", "holdback_c",
    "run", "itl", "fault", "ot", "tc_err", "wd_err",
    "note",
    "hold", "hold_mv",        # 유지 모드 holding(0/1, dac·tc2 공통) / dac 고정값(그 외 빈칸)
    "pv2_c", "pv_ctrl_c", "hold_kind", "sv2_c",   # TC2 / PID 가 보는 PV / 유지 종류(dac·tc2) / TC2 목표
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

        base_dir = NAS_HEATER_LOG_DIR
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            _nas_tried = base_dir
            base_dir = Path.cwd() / "Logs" / "heater"
            try:
                base_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                return None
            global _nas_fallback_warned
            if not _nas_fallback_warned:
                _nas_fallback_warned = True
                try:
                    log_message_to_monitor(
                        "경고",
                        f"NAS 히터 로그 폴더를 만들 수 없어 로컬로 저장합니다. "
                        f"시도={_nas_tried} → 사용={base_dir}")
                except Exception:
                    pass

        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = base_dir / f"{prefix}_{stamp}.csv"
        # 로거는 RUN 상승 엣지마다 새 파일을 "w" 로 만든다(이어 쓰기 없음) → 헤더는 항상 현재 COLUMNS 다.
        #  같은 초에 다시 시작해 파일이 이미 있으면(옛 헤더일 수 있다) 덮어쓰지 않고 _2, _3 … 을 붙인다.
        k = 1
        while path.exists():
            k += 1
            path = base_dir / f"{prefix}_{stamp}_{k}.csv"
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

    def write_row(self, st: dict, note: str = "", hold=None, hold_kind=None):
        """한 행 기록 후 즉시 flush. 프로그램이 죽어도 데이터는 남는다.
        hold=(holding 0/1, dac 고정값|None), hold_kind="dac"|"tc2"|None."""
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
                (1 if (hold and hold[0]) else 0),
                ("" if (not hold or hold[1] is None) else int(hold[1])),
                "" if st.get('pv2') is None else f"{float(st['pv2']):.1f}",
                "" if st.get('pv_ctrl') is None else f"{float(st['pv_ctrl']):.1f}",
                ("" if not (hold and hold[0]) else (hold_kind or "")),
                "" if st.get('sv2') is None else f"{float(st['sv2']):.1f}",
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
