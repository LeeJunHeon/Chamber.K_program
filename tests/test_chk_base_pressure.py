# -*- coding: utf-8 -*-
"""ChK_log.csv 'Base Pressure'·'Main Shutter' 열과 tools/migrate_chk_csv.py 새 스키마."""
import lib.logger as LG
from lib.config import CHK_CSV_COLUMNS
from test_main_heater import win  # noqa: F401


def test_column_order_base_pressure_after_process_name():
    import tools.migrate_chk_csv as M
    assert list(CHK_CSV_COLUMNS[:5]) == ["Timestamp", "Process Name", "Base Pressure", "Main Shutter", "Shutter Delay"]
    assert len(CHK_CSV_COLUMNS) == 21 and CHK_CSV_COLUMNS[-4:] == [
        "RF Pulse: Freq[kHz]", "RF Pulse: Duty[%]", "RF Pulse: For.P", "RF Pulse: Ref. P"]
    # Base Pressure 만 옮겼다 — 나머지는 옛 20열 순서 그대로
    assert [c for c in CHK_CSV_COLUMNS if c != "Base Pressure"] == M.COLS_20
    assert M.COLS_21_END == M.COLS_20 + ["Base Pressure"]


def test_row_keys_follow_column_order(win):
    w = win
    w._reset_chk_stats()
    w.last_process_params = None
    assert list(w._build_chk_csv_row().keys()) == list(CHK_CSV_COLUMNS)


def test_main_shutter_t_only_when_ms_step_ran(win):
    w = win
    pc = w.process_controller
    w._reset_chk_stats()
    assert w._build_chk_csv_row()["Main Shutter"] == "F"           # 메인 공정 없음(Process time 0) — 스텝이 없다
    # 실제 신호 경로(컨트롤러 update_plc_port → main). PLC 쪽 연결은 잠시 떼어 포트 없음 로그가 섞이지 않게
    pc.update_plc_port.disconnect(w.plc_controller.update_port_state)
    try:
        pc.update_plc_port.emit("S1_button", True)                  # 다른 버튼은 무관
        pc.update_plc_port.emit("MS_button", False)                 # 닫기도 무관
        assert w._build_chk_csv_row()["Main Shutter"] == "F"
        pc.update_plc_port.emit("MS_button", True)                  # 'Main Shutter Open' 스텝
        assert w._build_chk_csv_row()["Main Shutter"] == "T"
        pc.update_plc_port.emit("MS_button", False)                 # 종료 시퀀스의 닫기가 T 를 지우지 않는다
        assert w._build_chk_csv_row()["Main Shutter"] == "T"
    finally:
        pc.update_plc_port.connect(w.plc_controller.update_port_state)
    w._reset_chk_stats()                                            # 다음 공정은 다시 F 부터
    assert w._build_chk_csv_row()["Main Shutter"] == "F"


def test_ms_step_exists_only_with_process_time():
    from test_golden_process import build_steps
    base = {"process_name": "x", "use_dc_power": True, "dc_power": 100, "use_ar": True, "ar_flow": 20,
            "sp1_set": 5, "shutter_delay": 0, "use_g1": True, "g1_target_name": "CeO2"}
    has = lambda st: any(s["action"] == "PLC_CMD" and s["params"] == ["MS_button", True] for s in st)
    assert has(build_steps(dict(base, process_time=1.0)))
    assert not has(build_steps(dict(base, process_time=0.0)))


def test_row_written_in_column_order(tmp_path, monkeypatch):
    import csv
    p = tmp_path / "ChK_log.csv"
    monkeypatch.setattr(LG, "CHK_CSV_PATH", str(p))
    row = {c: c.upper() for c in CHK_CSV_COLUMNS}
    assert LG.append_chk_csv_row(dict(reversed(list(row.items()))))
    with p.open(encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows == [list(CHK_CSV_COLUMNS), [c.upper() for c in CHK_CSV_COLUMNS]]


def test_old_21_end_header_file_written_by_name(tmp_path, monkeypatch):
    """마이그레이션 전 파일(옛 21열 — Base Pressure 맨 끝)에는 그 헤더대로, 이름 기준으로 쓴다(어긋난 행 없음)."""
    import csv
    import tools.migrate_chk_csv as M
    p = tmp_path / "ChK_log.csv"
    with p.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(M.COLS_21_END)
    monkeypatch.setattr(LG, "CHK_CSV_PATH", str(p))
    warned = []
    monkeypatch.setattr(LG, "_warn_legacy_chk_header_once", lambda path: warned.append(path))
    row = {c: c.upper() for c in CHK_CSV_COLUMNS}
    assert LG.append_chk_csv_row(row)
    with p.open(encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[1] == [c.upper() for c in M.COLS_21_END] and warned


def test_row_value_from_base_wait(win):
    w = win
    w._reset_chk_stats()
    w.last_process_params = None
    assert w._build_chk_csv_row()["Base Pressure"] == ""          # 대기 생략·이온 게이지 꺼짐
    w.process_controller.ig_base_reached.emit(3.9e-6)              # 공정 스레드 신호(시험에선 같은 스레드)
    assert w._build_chk_csv_row()["Base Pressure"] == "3.90E-06"
    w._reset_chk_stats()                                           # 다음 공정은 다시 빈칸부터
    assert w._build_chk_csv_row()["Base Pressure"] == ""


def test_legacy_header_warning_mentions_base_pressure(tmp_path, monkeypatch):
    msgs = []
    monkeypatch.setattr(LG, "log_message_to_monitor", lambda lv, m: msgs.append(m))
    LG._warn_legacy_chk_header_once(tmp_path / "x.csv")
    assert msgs and "Base Pressure" in msgs[0] and "Main Shutter" in msgs[0] and "migrate_chk_csv" in msgs[0]


def test_migrate_selftest_passes(capsys):
    import tools.migrate_chk_csv as M
    assert M.cmd_selftest() == 0
    out = capsys.readouterr().out
    assert "회귀 테스트 실패 0건" in out and "[FAIL]" not in out
    for label in ("(a) --apply rc=0", "(b) 행 2 Base Pressure 만 4.60E-06 으로 채움", "fill 중복 Timestamp(2개) → rc=2",
                  "fill 일치 0개 → rc=2", "fill 이미 값 있음 → rc=2", "미지 헤더(같은 열·다른 순서) → rc=2",
                  "(c) 재실행은 파일을 바꾸지 않음", "검증: 위치로 옮긴 값(Base Pressure ↔ 끝 열)을 잡음"):
        assert "[OK]   " + label in out, label
