# lib/logger.py
import datetime
from pathlib import Path
from typing import Optional
import csv  # ★ 추가

from lib.config import CHK_CSV_PATH, CHK_CSV_COLUMNS  # ★ 추가

_monitor_widget = None  # 전역 변수(로그 모니터)

# ERP 리포터 훅 (없으면 None, 있어도 로깅 동작에는 영향 없음)
_reporter = None

def set_reporter(r):
    global _reporter
    _reporter = r


# 이번 공정에서 사용할 로그 파일 경로 (공정 시작 시 설정)
_current_log_file: Optional[Path] = None

# 히터 전용 텍스트 로그 경로 (히터 운전 시작 시 설정).
#  히터 CSV 와 같은 이름/타임스탬프로 짝을 이룬다.
_heater_log_file: Optional[Path] = None

# NAS 로그 기본 경로 (UNC 경로)
NAS_LOG_DIR = Path(r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK")

def set_monitor_widget(widget):
    """메인 코드에서 로그창 위젯을 한번 등록"""
    global _monitor_widget
    _monitor_widget = widget

def set_process_log_file(prefix: str = "CHK") -> Path:
    r"""
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

def clear_process_log_file() -> None:
    """공정 로그 파일 지정을 해제한다(공정 종료 시).

    해제하지 않으면 공정이 끝난 뒤의 모든 로그가 이미 끝난 공정의 .txt 에
    계속 덧붙는다(히터만 돌려도 그렇다).
    """
    global _current_log_file
    _current_log_file = None

def set_heater_log_file(path) -> None:
    """히터 전용 텍스트 로그 파일을 지정한다. 경로는 호출부가 정한다
    (히터 CSV 와 같은 이름/타임스탬프로 맞추기 위해)."""
    global _heater_log_file
    try:
        _heater_log_file = Path(path) if path else None
    except Exception:
        _heater_log_file = None

def clear_heater_log_file() -> None:
    global _heater_log_file
    _heater_log_file = None

def log_message_to_monitor(level, message):
    """UI 모니터에 로그.

    이 함수는 어떤 경우에도 예외를 밖으로 내보내지 않는다.
    안전 동작(공정 중단 등) 직전에 불리는 자리가 많아, 로그를 못 남겼다는
    이유로 뒤따르는 처리가 통째로 건너뛰어지면 안 되기 때문이다.
    화면·파일·ERP 세 경로를 각각 감싸서 하나가 죽어도 나머지는 남는다.
    """
    try:
        now = datetime.datetime.now().strftime("%H:%M:%S")
        msg = f"[{now}][{level}] {message}"
    except Exception:
        return

    # --- 화면 위젯 ---
    #  위젯이 아직 등록되지 않았거나(초기화 시점), 종료 중에 C++ 객체가 먼저
    #  파괴된 경우(RuntimeError: wrapped C/C++ object has been deleted) 모두 삼킨다.
    if _monitor_widget is not None:
        try:
            _monitor_widget.append(msg)
            # (스크롤 자동 하단으로)
            if hasattr(_monitor_widget, "verticalScrollBar"):
                sb = _monitor_widget.verticalScrollBar()
                sb.setValue(sb.maximum())
        except Exception:
            pass

    # --- 파일에도 로그 추가 ---
    #  재시작 후에도 남는 유일한 기록이라 화면 출력 실패와 무관하게 시도한다.
    try:
        log_message_to_file(level, message)
    except Exception:
        pass

    # --- ERP 리포터로 전달 (실패해도 무시) ---
    if _reporter is not None:
        try:
            low = str(message)
            lv = "error" if ("ERROR" in low or "FAIL" in low or "실패" in low)                  else ("warn" if ("WARN" in low or "경고" in low) else "info")
            _reporter.event(lv, low)
        except Exception:
            pass

def log_message_to_file(level, message):
    """
    로그 한 줄을 파일에 남긴다.

    라우팅:
      - 공정 로그(_current_log_file)가 있으면 항상 거기에 쓴다.
        히터 메시지도 공정 로그에 남아야 나중에 원인 분석이 된다.
      - level 이 "히터" 로 시작하면 히터 로그(_heater_log_file)에도 쓴다.
      - 둘 다 없으면 NAS_LOG_DIR/log.txt → 실패 시 ./log.txt.
    각 파일 쓰기는 독립적으로 감싸서 한쪽이 실패해도 다른 쪽은 남는다.
    """
    try:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{now}] [{level}] {message}\n"
    except Exception:
        return          # 메시지를 문자열로 못 만들면 남길 것도 없다

    global _current_log_file, _heater_log_file

    def _append(path: Path) -> bool:
        try:
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
            return True
        except Exception:
            return False

    wrote = False

    # 1) 공정 로그가 지정돼 있으면 항상 거기에 남긴다(히터 메시지 포함)
    if _current_log_file is not None:
        wrote = _append(_current_log_file) or wrote

    # 2) 히터 메시지는 히터 전용 로그에도 남긴다
    try:
        is_heater = str(level or "").startswith("히터")
    except Exception:
        is_heater = False
    if is_heater and _heater_log_file is not None:
        wrote = _append(_heater_log_file) or wrote

    if wrote:
        return

    # 3) 지정된 파일이 없거나 전부 실패하면 기본 log.txt
    try:
        NAS_LOG_DIR.mkdir(parents=True, exist_ok=True)
        path = NAS_LOG_DIR / "log.txt"
    except Exception:
        path = Path("log.txt")
    if _append(path):
        return
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

