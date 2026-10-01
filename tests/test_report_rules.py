# -*- coding: utf-8 -*-
"""E 단계 규칙 단위 테스트 — 레시피 임시 파일·적재 검사·ERP 보고 정리."""
import os
import tempfile

import pytest

from integrations.erp_commands import ErpCommandRunner, cleanup_web_recipes
from test_erp_commands import FakeHost


# ───────────────────────── E2 웹 레시피 임시 파일 ─────────────────────────
@pytest.fixture
def systemp(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    return tmp_path


def _web_dir(systemp):
    return systemp / "vanam_recipe"


def _run(h, cid=1):
    c = {"id": cid, "command": "RECIPE_PROCESS_RUN", "args": {"rows": [{"Process_name": "W"}]}}
    ErpCommandRunner(h).exec_one(c)
    return c


@pytest.mark.parametrize("outcome", ["success", "load_failed", "rejected"])
def test_e2_web_file_removed_after_load(outcome, systemp):
    h = FakeHost()
    seen = {}

    def _at_load(path):
        seen["existed"] = os.path.exists(path)
        if outcome != "success":                       # 실패·거부는 경고창 문구로만 남는다(예외 없음)
            h.alerts.append(("warning", "변경 불가" if outcome == "rejected" else "CSV 레시피 오류", "x"))
    h.on_exec["load"] = _at_load
    c = _run(h, cid=11)
    assert seen["existed"] is True and not os.path.exists(c["_csv_path"])
    assert os.listdir(_web_dir(systemp)) == []


def test_e2_web_file_removed_when_load_raises(systemp):
    class Boom(FakeHost):
        def load_recipe_file(self, path, name=""):
            raise RuntimeError("적재 중 예외")
    h = Boom()
    with pytest.raises(RuntimeError, match="적재 중 예외"):
        _run(h, cid=12)
    assert os.listdir(_web_dir(systemp)) == []


def test_e2_old_web_files_cleaned_including_legacy_name(systemp):
    d = _web_dir(systemp)
    d.mkdir()
    for n in ("process_web.csv", "process_web_9.csv", "heater_web.csv", "notes.txt"):
        (d / n).write_text("x", encoding="utf-8")
    seen = {}
    h = FakeHost()
    h.on_exec["load"] = lambda path: seen.update(files=sorted(os.listdir(d)))
    _run(h, cid=13)
    assert seen["files"] == ["heater_web.csv", "notes.txt", "process_web_13.csv"]     # 적재 때는 새 파일만
    assert sorted(os.listdir(d)) == ["heater_web.csv", "notes.txt"]


def test_e2_notebook_recipe_never_removed(systemp, tmp_path):
    local = tmp_path / "노트북" / "process_web_내 레시피.csv"       # 이름이 비슷해도 폴더가 다르면 건드리지 않는다
    local.parent.mkdir()
    local.write_text("x", encoding="utf-8")
    cleanup_web_recipes()
    h = FakeHost(path=str(local))
    _run(h, cid=14)
    assert local.exists()


def test_e2_silent_failure_check_still_compares_paths(systemp):
    """임시 파일을 지워도 적재 결과 확인(_csv_path 와 csv_file_path 비교)은 그대로다."""
    h = FakeHost(rows=[{"a": 1}])
    c = _run(h, cid=15)
    h.path = c["_csv_path"]
    assert ErpCommandRunner(h).silent_failure("RECIPE_PROCESS_RUN", c) == ""
    h.path = "C:/다른/process_web_1.csv"
    assert ErpCommandRunner(h).silent_failure("RECIPE_PROCESS_RUN", c) == "레시피를 적재하지 못했습니다 (장비 로그 확인)"


# 히터 레시피(heater_web.csv) — MainDialog 의 히터 명령 분기(Qt, 이 모듈 전용 창)
from test_main_heater import win  # noqa: E402,F401  (픽스처 재사용)


@pytest.mark.parametrize("fail", [False, True])
def test_e2_heater_web_file_removed_after_run(win, systemp, monkeypatch, fail):
    w = win
    seen = {}

    def _run_file(path, confirm):
        seen["existed"] = os.path.exists(path)
        seen["path"] = path
        if fail:
            raise RuntimeError("레시피 실행 실패(가짜)")
    monkeypatch.setattr(w, "_run_heater_recipe_file", _run_file)
    rows = [{"step": 1, "target_c": 300}]
    if fail:
        with pytest.raises(RuntimeError):
            w._erp_exec_heater_command("RECIPE_HEATER_RUN", {"rows": rows})
    else:
        assert w._erp_exec_heater_command("RECIPE_HEATER_RUN", {"rows": rows}) is True
    assert seen["existed"] is True and seen["path"].endswith("heater_web.csv")
    assert not os.path.exists(seen["path"])


# ───────────────────────── E3 적재할 때 모든 공정 행 검사 ─────────────────────────
from core.process_service import ProcessService   # noqa: E402
from core.state import ProcessState               # noqa: E402
from test_process_service_start import FakePorts, ROW   # noqa: E402


@pytest.fixture
def recipe_file(tmp_path):
    f = tmp_path / "recipe.csv"
    f.write_text("x", encoding="utf-8")
    return str(f)


def _svc_rows(rows, bad=()):
    ports = FakePorts(load_table=list(rows))
    real = ports.check_csv_row

    def _check(row):
        real(row)
        if row.get("Process_name") in bad:
            raise ValueError(f"[{row['Process_name']}] 값 오류")
    ports.check_csv_row = _check
    return ProcessService(ProcessState(), ports), ports


@pytest.mark.parametrize("rows,bad,which", [
    ([dict(ROW, Process_name="S1"), dict(ROW, Process_name="S2")], ("S1",), "첫 번째"),
    ([dict(ROW, Process_name="S1"), {"Process_name": "delay 1m"}, dict(ROW, Process_name="S3")], ("S3",), "3번째"),
    ([{"Process_name": "delay 1m"}, dict(ROW, Process_name="S2")], ("S2",), "2번째"),
])
def test_e3_row_number_and_message(rows, bad, which, recipe_file):
    svc, p = _svc_rows(rows, bad)
    svc.load_recipe_file(recipe_file)
    alert = [c for c in p.calls if c[0] == "alert"][0]
    assert alert[1:3] == ["warning", "CSV 레시피 오류"]
    assert alert[3] == f"{which} 공정 파라미터가 잘못되었습니다:\n[{bad[0]}] 값 오류"
    assert p.names()[-2:] == ["stage", "reset_process_ui_fields"] and svc.st.csv_rows == []   # _load_failed
    assert "build_csv_params" not in p.names() and "apply_params_to_ui" not in p.names()


def test_e3_skips_delay_and_blank_rows(recipe_file):
    rows = [dict(ROW, Process_name="S1"), {"Process_name": "delay 0s"}, {"Process_name": " delay 5m "},
            {"#": "4", "Process_name": "  "}, dict(ROW, Process_name="S5")]
    svc, p = _svc_rows(rows)
    svc.load_recipe_file(recipe_file)
    checked = [c[1]["Process_name"] for c in p.calls if c[0] == "check_csv_row"]
    assert checked == ["S1", "S5"]
    # 내용 없는 행은 파일을 읽을 때 이미 빠진다(load_csv_list) — 검사의 빈 행 건너뛰기는 방어용이다
    assert p.calls[-1] == ["stage", "CSV 공정: 1/4 - S1"]          # 통과하면 기존 미리보기


def test_e3_no_warning_log_during_check(recipe_file):
    """검사는 warn 없이 한다 — 히터 램프 보정 경고는 미리보기(첫 행)에서만 지금처럼 한 번 남는다."""
    from core.params import build_csv_params
    logs = []
    rows = [dict(ROW, Process_name="S1"), dict(ROW, Process_name="S2", use_heater="1", heater_temp="300",
                                               heater_ramp="3")]
    ports = FakePorts(load_table=rows)
    ports.check_csv_row = lambda row: build_csv_params(row, "6.79", "1.0395")
    ports.ret["build_csv_params"] = {"process_name": "S1"}
    ports.log = lambda level, msg: logs.append((level, msg))
    ProcessService(ProcessState(), ports).load_recipe_file(recipe_file)
    assert not [m for lv, m in logs if "heater_ramp" in m]


def test_e3_delay_first_row_still_previews_delay(recipe_file):
    svc, p = _svc_rows([{"Process_name": "delay 3s"}, dict(ROW, Process_name="S2")])
    svc.load_recipe_file(recipe_file)
    assert p.calls[-1] == ["stage", "CSV 공정: 1/2 - delay 3s (대기 스텝)"]
    assert [c[1]["Process_name"] for c in p.calls if c[0] == "check_csv_row"] == ["S2"]


# ───────────────────────── E4 run_end 는 run_start 를 보낸 공정에만 ─────────────────────────
from unittest.mock import MagicMock   # noqa: E402


def test_e4_chat_reset_does_not_touch_erp_run_flag(win):
    w = win
    for v in (True, False):
        w._erp_run_ended = v
        w._chat_reset_run_state()
        assert w._erp_run_ended is v
    w._erp_run_ended = True


def test_e4_erp_run_start_port_opens_record(win):
    w = win
    saved = w.erp
    try:
        w.erp = MagicMock()
        w._erp_run_ended = True
        w.proc.ports.erp_run_start("Single CHK", {"a": 1})
        assert w._erp_run_ended is False
        w.erp.run_start.assert_called_once_with("Single CHK", {"a": 1})
    finally:
        w.erp = saved
        w._erp_run_ended = True


def test_e4_no_run_end_without_run_start(win):
    w = win
    saved = (w.erp, w.chat_chk)
    try:
        w.erp = MagicMock(); w.chat_chk = None
        w._erp_run_ended = True                      # 앞 공정이 이미 끝났다(또는 시작한 적 없음)
        w._chat_reset_run_state()                    # 실행 중 행 오류 경로가 하던 일
        w._chat_notify_finished(False)
        w.erp.run_end.assert_not_called()
        w.proc.ports.erp_run_start("CSV 1/1 - S1", {})   # run_start 를 보내면
        w._chat_notify_finished(True)
        w._chat_notify_finished(True)                # 한 번만
        assert [c.args for c in w.erp.run_end.call_args_list] == [("done",)]
    finally:
        w.erp, w.chat_chk = saved
        w._erp_run_ended = True
