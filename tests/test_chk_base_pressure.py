# -*- coding: utf-8 -*-
"""ChK_log.csv 'Base Pressure' 열과 tools/migrate_chk_csv.py 새 스키마."""
import lib.logger as LG
from lib.config import CHK_CSV_COLUMNS
from test_main_heater import win  # noqa: F401


def test_column_appended_at_end():
    import tools.migrate_chk_csv as M
    assert CHK_CSV_COLUMNS[-1] == "Base Pressure" and len(CHK_CSV_COLUMNS) == 21
    assert list(CHK_CSV_COLUMNS[:20]) == M.COLS_20          # 앞 20열은 그대로(중간에 끼우지 않음)


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
    assert msgs and "Base Pressure" in msgs[0] and "migrate_chk_csv" in msgs[0]


def test_migrate_selftest_passes(capsys):
    import tools.migrate_chk_csv as M
    assert M.cmd_selftest() == 0
    out = capsys.readouterr().out
    assert "회귀 테스트 실패 0건" in out and "20필드 행 Base Pressure" in out
