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
