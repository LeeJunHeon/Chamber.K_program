# lib/logger.py
import datetime
from pathlib import Path
from typing import Optional
import csv  # ★ 추가

from lib.config import CHK_CSV_PATH, CHK_CSV_COLUMNS  # ★ 추가

_monitor_widget = None  # 전역 변수(로그 모니터)

# 이번 공정에서 사용할 로그 파일 경로 (공정 시작 시 설정)
_current_log_file: Optional[Path] = None

# NAS 로그 기본 경로 (UNC 경로)
NAS_LOG_DIR = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK")

def set_monitor_widget(widget):
    """메인 코드에서 로그창 위젯을 한번 등록"""
    global _monitor_widget
    _monitor_widget = widget

def set_process_log_file(prefix: str = "CHK") -> Path:
    """
    공정 시작 시 호출해서, 이번 공정 로그를 기록할 파일을 생성/지정한다.
    - 기본 파일 이름: {prefix}_YYYYmmdd_HHMMSS.txt
    - 기본 경로: \\VanaM_NAS\VanaM_toShare\JH_Lee\Logs
    - NAS 접근 실패 시: 현재 작업 폴더 아래 Logs 폴더에 저장
    """
    global _current_log_file

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # 우선 NAS 경로 시도
    base_dir = NAS_LOG_DIR
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        # NAS에 접근 안 되면 로컬 Logs 폴더로 폴백
        base_dir = Path.cwd() / "Logs"
        base_dir.mkdir(parents=True, exist_ok=True)

    _current_log_file = base_dir / f"{prefix}_{timestamp}.txt"
    return _current_log_file

def log_message_to_monitor(level, message):
    """UI 모니터에 로그"""
    now = datetime.datetime.now().strftime("%H:%M:%S")
    msg = f"[{now}][{level}] {message}"
    _monitor_widget.append(msg)
    # (스크롤 자동 하단으로)
    if hasattr(_monitor_widget, "verticalScrollBar"):
        sb = _monitor_widget.verticalScrollBar()
        sb.setValue(sb.maximum())
    # --- 파일에도 로그 추가 ---
    log_message_to_file(level, message)

def log_message_to_file(level, message):
    """
    현재 설정된 공정 로그 파일(_current_log_file)에 한 줄 추가.
    공정 로그 파일이 아직 없으면:
      - NAS_LOG_DIR/log.txt 시도 후,
      - 실패 시 현재 폴더의 log.txt에 기록.
    """
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] [{level}] {message}\n"

    global _current_log_file

    # 1) 공정 시작 시 set_process_log_file로 지정된 파일이 있으면 그걸 사용
    path: Path
    if _current_log_file is not None:
        path = _current_log_file
    else:
        # 2) 아직 공정 로그 파일이 없으면 기본 log.txt를 사용
        try:
            NAS_LOG_DIR.mkdir(parents=True, exist_ok=True)
            path = NAS_LOG_DIR / "log.txt"
        except Exception:
            path = Path("log.txt")

    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        # 최후의 폴백: 현재 작업 디렉터리의 log.txt
        try:
            with open("log.txt", "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            # 정말 쓸 수 있는 데가 없으면 조용히 무시
            pass

def append_chk_csv_row(row: dict) -> bool:
    """
    Chamber-K 공정 요약 데이터를 CSV(ChK_log.csv)에 한 줄 append.
    - row 는 CHK_CSV_COLUMNS 의 컬럼명을 key 로 갖는 dict 여야 함.
    - 부족한 컬럼은 ""(빈 문자열)로 채워서 기록.
    - NAS 접근 실패 시 로컬 Logs 폴더로 폴백.
    """
    target_path = Path(CHK_CSV_PATH)

    try:
        # 1) NAS 경로 시도
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            use_path = target_path
        except Exception:
            # NAS 폴더 생성 실패 → 로컬 Logs 폴더로 폴백
            local_dir = Path.cwd() / "Logs"
            local_dir.mkdir(parents=True, exist_ok=True)
            use_path = local_dir / "ChK_log.csv"

        first = not use_path.exists()

        with use_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CHK_CSV_COLUMNS)
            if first:
                writer.writeheader()
            writer.writerow({k: row.get(k, "") for k in CHK_CSV_COLUMNS})

        return True

    except Exception as e:
        # 실패하면 모니터에만 경고 남기고 False
        if _monitor_widget is not None:
            log_message_to_monitor("경고", f"ChK CSV 로그 기록 실패: {e!r}")
        return False

