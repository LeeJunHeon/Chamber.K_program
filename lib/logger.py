# lib/logger.py
import datetime

_monitor_widget = None  # 전역 변수(로그 모니터)

def set_monitor_widget(widget):
    """메인 코드에서 로그창 위젯을 한번 등록"""
    global _monitor_widget
    _monitor_widget = widget

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
    """log.txt에 로그 남기기"""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{now}] [{level}] {message}\n")
