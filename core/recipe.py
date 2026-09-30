# core/recipe.py
# -*- coding: utf-8 -*-
"""공정 레시피(CSV/엑셀 행) 해석 — 화면 없이 도는 순수 함수.

main.py 의 MainDialog._CSV_DELAY_RE / _parse_csv_delay_seconds / _fmt_hms / _load_csv_process_list 안의 _has_content /
_csv_list_uses_heater 본문을 그대로 옮겼다. PyQt6·UI·main·controller·device·reporter·lib.logger 를 import 하지 않는다.
"""
import re
from typing import Optional

# ============= CSV Delay (공정 사이 대기) =============
CSV_DELAY_RE = re.compile(r"^\s*delay\s+(\d+(?:\.\d+)?)\s*([smhd])\s*$", re.IGNORECASE)


def parse_delay_seconds(process_name: str) -> Optional[int]:
    """Process_name이 'delay 60m' 같은 형태면 대기 시간(초)을 반환, 아니면 None."""
    if not process_name:
        return None
    m = CSV_DELAY_RE.match(process_name)
    if not m:
        return None

    try:
        num = float(m.group(1))
    except Exception:
        return None

    unit = (m.group(2) or "m").lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
    if mult is None:
        return None

    sec = int(num * mult)
    return max(sec, 0)


def fmt_hms(seconds: int) -> str:
    """초 → "MM:SS" (1시간 이상이면 "H:MM:SS"). 음수·None 은 0. CSV 딜레이 남은 시간 표시용."""
    seconds = max(int(seconds or 0), 0)
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"


def row_has_content(row: dict) -> bool:
    """'#'(번호) 칸을 빼고 값이 하나라도 있는 행인가. 빈 행·번호만 있는 행은 공정 행이 아니다."""
    for k, v in (row or {}).items():
        if (k or "").strip() == "#":
            continue
        if v is None:
            continue
        if str(v).strip() != "":
            return True
    return False


def csv_rows_use_heater(rows) -> bool:
    """적재된 CSV 공정 목록의 어느 행이든 히터를 쓰면 True.

    한 행만 보면 안 된다 — 3번째 행에서 히터를 켜는 레시피가 있을 수 있다.
    판정 규칙은 _build_params_from_csv_row 와 같게 맞춘다. 여기서는 예외를
    던지지 않는다(실제 검증은 그쪽이 한다).
    """
    try:
        rows = rows or []
    except Exception:
        return False
    for row in rows:
        try:
            use = ""
            temp = ""
            for k, v in (row or {}).items():
                key = str(k or "").strip().lower()
                if key == "use_heater":
                    use = str(v or "").strip().lower()
                elif key == "heater_temp":
                    temp = str(v or "").strip()
            if use not in ("1", "y", "yes", "true", "t", "on"):
                continue
            if float(temp) > 0:
                return True
        except Exception:
            continue        # 값이 비었거나 숫자가 아니면 '히터 미사용'으로 본다
    return False
