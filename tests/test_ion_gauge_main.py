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

# 막 연결된 가짜 게이지는 방출이 처음 확인된 지 IG_SETTLE_SEC 안이라 1줄이 '(안정화 중)'(주의색)이다
EXPECTED_LINES = ["IG 9.0E-6 Torr (안정화 중)", "게이지 A < 1.0E-3 Torr", "게이지 B 6.3E-3 Torr", "필라멘트 켜짐"]


@pytest.fixture
def ig(win, monkeypatch):
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lv, m: logs.append((lv, m)))
    s = Script(delay_ms=20)
    w = win
    w._setup_ion_gauge(enabled=True, port="COM12", csv_enabled=True, controller_factory=lambda: IonGaugeController(
        "COM12", poll_ms=200, timeout_ms=150, min_gap_ms=0, fail_limit=2,
        policy=ReconnectPolicy(100, 200, 600, 1000, 600),
        port_factory=lambda parent: FakeIgPort(s, parent), port_lister=lambda: s.ports))
    yield w, s, logs
    w._stop_ion_gauge()
    pc = w.process_controller                               # 공용 창 — 공정 쪽 배선도 되돌린다
    pc.set_ig_snapshot_provider(None)
    for sig in (pc.request_ig_filament_on, pc.request_ig_filament_off):
        try:
            sig.disconnect()
        except TypeError:
            pass
    if w._ig_frame is not None:
        w._ig_frame.deleteLater()                           # 4줄 라벨·제목은 틀의 자식이라 함께 지워진다
        w._ig_frame = None
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
    assert w._ig_labels[0].styleSheet() == "color: #b35c00;" and w._ig_labels[1].styleSheet() == "color: #222222;"
    tip = w._ig_labels[0].toolTip()
    assert "원문 IG 9 -6 · A 0 -4 · B 63 -4 · 오류 코드 0" in tip                 # 툴팁에는 항상 원문
    assert ("IG", "이온 게이지 연결됨 (COM12)") in logs and ("IG", "이온 게이지 필라멘트 켜짐") in logs
    assert sum(m == "이온 게이지 필라멘트 켜짐" for _, m in logs) == 1   # 매초 로그 없음
    # 위치: Process List / Select File 아래 빈 공간(x 555~725, y 345~475) 안의 테두리 틀, UI.py 위젯과 겹치지 않는다
    fr = w._ig_frame
    g = fr.geometry()
    assert (g.x(), g.y(), g.width(), g.height()) == MAIN.IG_FRAME_RECT == (560, 345, 165, 104)
    assert 555 <= g.left() and g.right() < 725 and 345 <= g.top() and g.bottom() < 475
    assert fr.objectName() == "ig_frame" and fr.parent() is w
    assert "border: 1px solid #8a8a8a" in fr.styleSheet() and "border-radius: 4px" in fr.styleSheet()
    assert "background: #ffffff" in fr.styleSheet()
    others = [c for c in w.findChildren(QWidget) if c.parent() is w and c is not fr]
    assert len(others) > 50
    hits = [c.objectName() for c in others if c.geometry().intersects(g)]
    assert hits == [], hits
    title = fr.findChild(QWidget, "ig_title")
    assert title.text() == "Ion Gauge" and title.font().bold()
    for lab in w._ig_labels:                                # 4줄은 틀 안에, 1줄만 크게·굵게
        assert lab.parent() is fr and fr.rect().contains(lab.geometry())
    assert w._ig_labels[0].font().bold() and w._ig_labels[0].font().pointSizeF() > w._ig_labels[1].font().pointSizeF()
    # 옛 위치(램프 줄 오른쪽)에는 아무것도 남지 않는다
    assert not [c for c in others if c.objectName().startswith("ig_")]


def test_csv_rows_and_process_log_line(ig, monkeypatch):
    w, s, logs = ig
    spin(700)
    lines = []
    monkeypatch.setattr(MAIN, "process_log_active", lambda: True)
    monkeypatch.setattr(MAIN, "log_message_to_file", lambda lv, m: lines.append((lv, m)))
    w._ig_process_log("시작")
    assert lines == [("정보", "[IG] 시작 압력 IG 9.0E-6 Torr (안정화 중), 게이지 A < 1.0E-3 Torr, 게이지 B 6.3E-3 Torr (필라멘트 켜짐)")]
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
    assert ok and ok[-1][1:] == ["9.0E-6", "< 1.0E-3(0 -4)", "6.3E-3", "켜짐", "정상", "꺼짐", "0", "정상"]


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


def test_erp_state_has_ion_gauge(ig):
    from integrations.erp_state import build_state
    w, s, logs = ig
    spin(700)
    st = build_state(w.erp_state.src)
    g = st["ionGauge"]
    assert list(st)[-1] == "ionGauge" and g["enabled"] is True and g["comm"] == "ok"
    assert g["ion"] == pytest.approx(9e-6) and g["gaugeA"] is None and g["gaugeB"] == pytest.approx(6.3e-3)
    assert (g["ionText"], g["gaugeAText"], g["gaugeBText"], g["filamentText"]) == (
        "9.0E-6 Torr (안정화 중)", "< 1.0E-3 Torr", "6.3E-3 Torr", "필라멘트 켜짐")
    assert g["ageSec"] is not None and g["ageSec"] <= 1



def test_process_controller_wired_to_ion_gauge(ig):
    """공정 → 시그널 → 이온 게이지 스레드(QueuedConnection) → 포트에 A, 결과는 공정 컨트롤러 슬롯으로."""
    w, s, logs = ig
    pc = w.process_controller
    spin(500)
    assert pc._ig_snap() is w._ig_snap and w._ig_snap is not None
    got = []
    pc._running = True
    pc._ig_expect, pc._ig_ctx = "on", "probe"              # 결과만 받아 본다(ctx 미지정 → 다음 스텝 없음)
    orig = pc._ig_after_result
    pc._ig_after_result = lambda ctx, a, ok, d: got.append((ctx, a, ok))
    try:
        pc.request_ig_filament_on.emit()
        spin(1500)
    finally:
        pc._ig_after_result = orig
        pc._running = False
    assert b"A" in s.writes
    assert got and got[0][:2] == ("probe", "on")            # 가짜 장비는 필라멘트가 이미 켜져 있어 결과 성공


def test_csv_disabled_collects_nothing_and_has_no_timer(win, monkeypatch):
    """IG_CSV_ENABLED=false(기본): 스냅샷이 와도 행이 쌓이지 않고, 10초 쓰기 타이머가 없고, 종료 때도 쓰지 않는다."""
    from lib.config import IG_CSV_ENABLED
    assert IG_CSV_ENABLED is False
    writes = []
    monkeypatch.setattr(MAIN, "append_ig_rows", lambda rows: writes.append(list(rows)) or True)
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lv, m: None)
    s = Script(delay_ms=20)
    w = win
    w._setup_ion_gauge(enabled=True, port="COM12", controller_factory=lambda: IonGaugeController(
        "COM12", poll_ms=200, timeout_ms=150, min_gap_ms=0,
        port_factory=lambda parent: FakeIgPort(s, parent), port_lister=lambda: s.ports))
    try:
        assert w._ig_csv is False and w._ig_flush_timer is None
        spin(800)
        assert w._ig_snap is not None and w._ig_snap.ok          # 표시는 그대로 돈다
        assert w._ig_rows == []
        w._stop_ion_gauge()
        assert writes == []                                      # 종료 때도 쓰기 호출 없음(→ ig 폴더도 안 생김)
    finally:
        w._stop_ion_gauge()
        if w._ig_frame is not None:
            w._ig_frame.deleteLater()
            w._ig_frame = None
        w.ig_thread = None
        w.ig_controller = None
        w._ig_labels = []
        pc = w.process_controller
        pc.set_ig_snapshot_provider(None)
        for sig in (pc.request_ig_filament_on, pc.request_ig_filament_off):
            try:
                sig.disconnect()
            except TypeError:
                pass


def test_csv_enabled_keeps_timer(ig):
    w, s, logs = ig
    assert w._ig_csv is True and w._ig_flush_timer is not None and w._ig_flush_timer.isActive()
