# -*- coding: utf-8 -*-
"""이온 게이지 — MainDialog 쪽: 표시(위치·문구), IG CSV, COMM 이벤트, 공정 로그 한 줄, 끊김 표시, 종료 정리.
실제 IonGaugeController 를 실제 QThread 에서 가짜 포트로 돌린다(시리얼·NAS 접근 없음)."""
import csv
import datetime

import pytest
from PyQt6.QtWidgets import QWidget

import main as MAIN
import lib.logger as LG
from device.IonGauge import IonGaugeController
from lib.comm_policy import ReconnectPolicy
from test_main_heater import win  # noqa: F401  (픽스처 재사용 — 이 창은 이온 게이지가 꺼진 상태로 뜬다)
from test_ion_gauge_driver import Script, FakeIgPort, spin

EXPECTED_LINES = ["IG 9.0E-6 Torr", "게이지 A 하한 이하", "게이지 B 6.3E-3 Torr", "필라멘트 켜짐"]


@pytest.fixture
def ig(win, monkeypatch):
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lv, m: logs.append((lv, m)))
    s = Script(delay_ms=20)
    w = win
    w._setup_ion_gauge(enabled=True, port="COM12", controller_factory=lambda: IonGaugeController(
        "COM12", poll_ms=200, timeout_ms=150, min_gap_ms=0, fail_limit=2,
        policy=ReconnectPolicy(100, 200, 600, 1000, 600),
        port_factory=lambda parent: FakeIgPort(s, parent), port_lister=lambda: s.ports))
    yield w, s, logs
    w._stop_ion_gauge()
    for lab in w._ig_labels:
        lab.deleteLater()
    w.ig_thread = None
    w.ig_controller = None
    w._ig_labels = []


def texts(w):
    """화면 4줄(원문). 라벨 글자는 원문이거나, 칸보다 길면 원문 앞부분 + '…'
    (offscreen 시험 플랫폼에는 글꼴이 없어 폭이 실제보다 크게 잡힌다)."""
    for lab, line in zip(w._ig_labels, w._ig_lines):
        shown = lab.text()
        assert shown == line or (shown.endswith("…") and line.startswith(shown[:-1])), (shown, line)
    return list(w._ig_lines)


def test_disabled_window_has_no_ion_gauge(win):
    assert win.ig_thread is None and win.ig_controller is None and win._ig_labels == []
    win._ig_process_log("시작")                                  # 꺼져 있으면 아무것도 안 한다


def test_display_values_and_position(ig):
    w, s, logs = ig
    assert texts(w)[0] == "IG 연결 중"
    spin(700)
    assert texts(w) == EXPECTED_LINES
    assert w._ig_labels[0].styleSheet() == "color: #222222;"
    assert ("IG", "이온 게이지 연결됨 (COM12)") in logs and ("IG", "이온 게이지 필라멘트 켜짐") in logs
    assert sum(m == "이온 게이지 필라멘트 켜짐" for _, m in logs) == 1   # 매초 로그 없음
    # 위치: 램프 줄 오른쪽 빈 자리, UI.py 의 어떤 위젯과도 겹치지 않는다
    rects = [lab.geometry() for lab in w._ig_labels]
    assert [(r.x(), r.y(), r.width(), r.height()) for r in rects] == [
        (410, 12, 172, 18), (410, 31, 172, 15), (410, 46, 172, 15), (410, 61, 172, 15)]
    others = [c for c in w.findChildren(QWidget) if c.parent() is w and c not in w._ig_labels]
    assert others
    for r in rects:
        hits = [c.objectName() for c in others if c.geometry().intersects(r)]
        assert hits == [], hits


def test_csv_rows_and_process_log_line(ig, monkeypatch):
    w, s, logs = ig
    spin(700)
    lines = []
    monkeypatch.setattr(MAIN, "process_log_active", lambda: True)
    monkeypatch.setattr(MAIN, "log_message_to_file", lambda lv, m: lines.append((lv, m)))
    w._ig_process_log("시작")
    assert lines == [("정보", "[IG] 시작 압력 IG 9.0E-6 Torr, 게이지 A 하한 이하, 게이지 B 6.3E-3 Torr (필라멘트 켜짐)")]
    # ProcessPorts 의 공정 로그 열기/닫기가 한 줄씩 남긴다
    monkeypatch.setattr(MAIN, "set_process_log_file", lambda prefix="CHK": None)
    monkeypatch.setattr(MAIN, "clear_process_log_file", lambda: None)
    w.proc.ports.open_process_log("CHK")
    w.proc.ports.close_process_log()
    assert [m.split(" 압력")[0] for _, m in lines[1:]] == ["[IG] 시작", "[IG] 종료"]
    # 10초마다 쓰는 CSV — 여기서는 직접 비운다
    n = len(w._ig_rows)
    assert n >= 2
    w._flush_ig_rows()
    assert w._ig_rows == []
    path = LG.NAS_IG_LOG_DIR / f"IG_{datetime.date.today():%Y%m%d}.csv"
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == LG.IG_CSV_COLUMNS
    ok = [r for r in rows[1:] if r[-1] == "정상"]
    assert ok and ok[-1][1:] == ["9.0E-6", "하한 이하", "6.3E-3", "켜짐", "정상", "꺼짐", "0", "정상"]


def test_lost_comm_shows_age_and_logs_comm_event(ig):
    w, s, logs = ig
    spin(600)
    assert texts(w) == EXPECTED_LINES
    s.replies = {b"F": None, b"E": None}                         # 응답이 끊겼다
    spin(1500)
    t = texts(w)
    assert t[0] == "IG 통신 끊김" and t[2].startswith("마지막 값 ") and t[2].endswith("초 전")
    assert t[3] == "IG 9.0E-6 Torr"                              # 마지막 값은 회색으로 함께
    assert w._ig_labels[0].styleSheet() == "color: #c62828;" and w._ig_labels[3].styleSheet() == "color: #8a8a8a;"
    path = LG.NAS_COMM_LOG_DIR / "COMM_events.csv"
    with open(path, encoding="utf-8-sig", newline="") as f:
        ig_rows = [r for r in csv.DictReader(f) if r["장비"] == "IG"]
    assert ig_rows and ig_rows[0]["종류"] == "실패"
    warn = [m for lv, m in logs if lv == "IG(경고)"]
    assert 1 <= len(warn) <= 3                                   # 상태가 바뀔 때만
    # 끊긴 주기의 CSV 행은 압력 칸이 비어 있다
    lost = [r for r in w._ig_rows if r["통신 상태"] != "정상"]
    assert lost and all("IG(Torr)" not in r for r in lost)
    # 공정과 무관 — 공정 상태·버튼은 그대로
    assert w.process_running is False and w.ui.Sputter_Start_Button.isEnabled()


def test_stop_runs_cleanup_in_worker_and_flushes(ig):
    w, s, logs = ig
    spin(500)
    th = w.ig_thread
    w._stop_ion_gauge()
    assert th.isFinished() and s.closes >= 1
    assert w._ig_rows == []
    assert not any("스레드" in m for _, m in logs)                # 종료는 조용히
