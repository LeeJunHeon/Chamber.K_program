# -*- coding: utf-8 -*-
"""원격 조작 규칙(B단계) — Qt 없이 ProcessService·ErpCommandRunner 를 가짜 ports/host 로 본다."""
import dataclasses

import pytest

from core.process_service import ProcessService
from core.state import ProcessState
from test_process_service_start import FakePorts, MANUAL, ROW


def _svc(st, **ret):
    ports = FakePorts(**ret)
    return ProcessService(st, ports), ports


# ───────────────────────── B1 원격 수동 시작 ─────────────────────────
@pytest.mark.parametrize("kw", [dict(running=True), dict(csv_mode=True, csv_rows=[ROW]), dict(delay_active=True)])
def test_b1_remote_manual_rejected_while_active(kw):
    st = ProcessState(**kw)
    svc, p = _svc(st)
    svc.start(manual_inputs=MANUAL)
    assert p.calls == [["alert", "warning", "경고", "이미 공정이 진행 중입니다."]]


def test_b1_remote_manual_rejected_when_recipe_loaded():
    st = ProcessState(csv_file_path="C:/r/메인 레시피.csv", csv_rows=[ROW])
    svc, p = _svc(st)
    svc.start(manual_inputs=MANUAL)
    assert p.calls == [["alert", "warning", "시작 불가",
                        "장비에 레시피가 적재돼 있습니다(메인 레시피.csv). 레시피로 시작하거나 적재를 해제하세요."]]
    assert st.csv_mode is False and st.origin == "local"


def test_b1_remote_manual_uses_inputs_and_shows_them_after_params_pass():
    st = ProcessState()
    inputs = dataclasses.replace(MANUAL, dc_power_text="150")
    svc, p = _svc(st)
    svc.start(manual_inputs=inputs)
    n = p.names()
    assert "read_manual_inputs" not in n                      # 노트북 입력칸을 읽지 않는다
    i = n.index("show_manual_inputs")
    assert n[i - 1] == "clear_plc_fault" and n[i + 1] == "reset_stats"   # 파라미터 통과 직후, 시작 전
    assert p.calls[i][1] is inputs
    assert [c for c in p.calls if c[0] == "request_start"][0][1]["dc_power"] == 150.0


def test_b1_remote_manual_input_error_does_not_touch_notebook():
    st = ProcessState()
    svc, p = _svc(st)
    svc.start(manual_inputs=dataclasses.replace(MANUAL, use_ar=False))
    assert "show_manual_inputs" not in p.names() and p.names()[-1] == "alert"
    assert p.calls[-1][2] == "입력 오류"


def test_b1_remote_manual_guard_rejection_does_not_touch_notebook():
    svc, p = _svc(ProcessState(), main_valve_open=False)
    svc.start(manual_inputs=MANUAL)
    assert "show_manual_inputs" not in p.names() and p.names()[-1] == "main_valve_open"


def test_b1_local_start_unchanged():
    """노트북 Start(manual_inputs 없음)는 지금처럼 입력칸을 읽고, 입력칸에 다시 쓰지 않는다."""
    svc, p = _svc(ProcessState())
    svc.start()
    assert "read_manual_inputs" in p.names() and "show_manual_inputs" not in p.names()
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[ROW])     # 레시피가 적재돼 있으면 레시피로 시작
    svc, p = _svc(st)
    svc.start()
    assert "alert" not in p.names() and st.csv_mode is True


# ───────────────────────── B2 적재 실패면 적재 상태를 비운다 ─────────────────────────
def _loaded():
    return ProcessState(csv_file_path="C:/r/old.csv", csv_rows=[dict(ROW)], current_name="x", last_params={"a": 1})


@pytest.fixture
def recipe_file(tmp_path):
    f = tmp_path / "recipe.csv"
    f.write_text("x", encoding="utf-8")
    return str(f)


@pytest.mark.parametrize("case", ["missing", "read_error", "no_rows", "bad_first_row"])
def test_b2_failures_clear_previous_recipe(case, recipe_file, tmp_path):
    ret = {"missing": {}, "read_error": dict(load_table=RuntimeError("깨짐")),
           "no_rows": dict(load_table=[{"#": "1"}]), "bad_first_row": dict(build_csv_params=ValueError("wp"))}[case]
    st = _loaded()
    svc, p = _svc(st, **ret)
    svc.load_recipe_file(str(tmp_path / "없음.csv") if case == "missing" else recipe_file)
    assert p.calls[-2] == ["stage", "레시피 적재 실패"] and p.names()[-3] == "alert"
    assert p.names()[-1] == "reset_process_ui_fields"                # 입력칸도 초기 상태로
    assert (st.csv_file_path, st.csv_rows, st.csv_index, st.csv_mode, st.current_name, st.last_params) == \
        (None, [], -1, False, "", None)


@pytest.mark.parametrize("kw,path", [
    (dict(is_closing=True), "C:/r/new.csv"),               # 종료 중
    (dict(), ""),                                          # 경로 없음(대화상자 취소)
])
def test_b2_non_failure_rejections_keep_recipe(kw, path):
    st = _loaded()
    svc, p = _svc(st, **kw)
    svc.load_recipe_file(path)
    assert st.csv_file_path == "C:/r/old.csv" and st.csv_rows == [dict(ROW)]
    assert "stage" not in p.names()


def test_b2_active_process_rejection_keeps_running_recipe(recipe_file):
    st = _loaded()
    st.running = True; st.csv_mode = True
    svc, p = _svc(st)
    svc.load_recipe_file(recipe_file)
    assert p.calls[-1][2] == "변경 불가" and st.csv_file_path == "C:/r/old.csv" and st.csv_rows == [dict(ROW)]


# ───────────────────────── B3 Start 는 적재할 때 읽은 내용 그대로 ─────────────────────────
def test_b3_start_does_not_reread_file():
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)])
    svc, p = _svc(st, load_table=RuntimeError("다시 읽으면 안 된다"))
    svc.start()
    assert "load_table" not in p.names() and st.csv_mode is True and st.csv_index == 0


@pytest.mark.parametrize("rows,blocked", [
    ([dict(ROW)], False),
    ([dict(ROW), dict(ROW, use_heater="1", heater_temp="300")], True),
])
def test_b3_heater_check_and_run_use_same_loaded_rows(rows, blocked):
    """히터 레시피 실행 중 검사는 적재된 행으로 하고, 통과하면 같은 행이 실행된다."""
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=list(rows))
    svc, p = _svc(st, heater_recipe_running=True)
    svc.start()
    if blocked:
        assert p.calls[-1][2] == "시작 불가" and st.csv_mode is False
    else:
        assert st.csv_mode is True and [c for c in p.calls if c[0] == "build_csv_params"][0][1] is st.csv_rows[0]


# ───────────────────────── B4 레시피 이름·원격 레시피 파일 ─────────────────────────
@pytest.mark.parametrize("display,expected", [("웹 레시피 A", "웹 레시피 A"), ("", "recipe.csv")])
def test_b4_recipe_name_set_on_successful_load(display, expected, recipe_file):
    st = ProcessState()
    svc, p = _svc(st)
    svc.load_recipe_file(recipe_file, display)
    assert st.recipe_name == expected and svc.recipe_display_name() == expected


def test_b4_recipe_name_cleared_on_failure_and_clear():
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)], recipe_name="웹 A")
    svc, p = _svc(st)
    svc.load_recipe_file("C:/없는/파일.csv", "웹 B")
    assert st.recipe_name == ""
    st.recipe_name = "X"; st.clear_csv_list()
    assert st.recipe_name == ""
    assert ProcessService(ProcessState(csv_file_path="C:/r/b.csv"), FakePorts()).recipe_display_name() == "b.csv"


def test_b4_cleanup_web_recipes_removes_all_web_files_and_ignores_errors(tmp_path, monkeypatch):
    """적재가 끝나면 웹 레시피 임시 파일은 필요 없다 — process_web*.csv(예전 이름 포함)를 모두 지운다(E 커밋 2)."""
    import os
    import tempfile
    from integrations.erp_commands import cleanup_web_recipes
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    d = tmp_path / "vanam_recipe"
    d.mkdir()
    for n in ("process_web.csv", "process_web_1.csv", "process_web_2.csv", "heater_web.csv", "other.csv"):
        (d / n).write_text("x", encoding="utf-8")
    real_remove = os.remove

    def _remove(path):
        if path.endswith("process_web_2.csv"):
            raise PermissionError("열려 있음")
        real_remove(path)
    monkeypatch.setattr(os, "remove", _remove)
    cleanup_web_recipes()                                           # 지우기 실패는 무시
    assert sorted(os.listdir(d)) == ["heater_web.csv", "other.csv", "process_web_2.csv"]
    monkeypatch.setattr(os, "remove", real_remove)
    cleanup_web_recipes()
    assert sorted(os.listdir(d)) == ["heater_web.csv", "other.csv"]


def test_b4_remote_recipe_run_passes_name_and_id_file(tmp_path, monkeypatch):
    import tempfile
    from integrations.erp_commands import ErpCommandRunner
    from test_erp_commands import FakeHost
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    h = FakeHost()
    ErpCommandRunner(h).exec_one({"id": 77, "command": "RECIPE_PROCESS_RUN",
                                  "args": {"name": "웹 레시피", "rows": [{"Process_name": "W"}]}})
    assert h.calls[-1][0] == "load_recipe_file" and h.calls[-1][1].endswith("process_web_77.csv")
    assert h.calls[-1][2] == "웹 레시피"


def test_b4_erp_state_name_prefers_recipe_name():
    from integrations.erp_state import build_state
    from test_erp_state import FakeSrc
    assert build_state(FakeSrc(rows=[{"a": 1}], path="C:/t/process_web_3.csv", rname="웹 A"))["csvRecipe"]["name"] == "웹 A"
    assert build_state(FakeSrc(rows=[{"a": 1}], path="C:/t/process_web_3.csv"))["csvRecipe"]["name"] == "process_web_3.csv"


# ───────────────────────── B5 RECIPE_CLEAR ─────────────────────────
def test_b5_clear_recipe_loaded():
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)], recipe_name="웹 A")
    svc, p = _svc(st)
    svc.clear_recipe()
    assert p.calls == [["stage", "레시피 적재 해제됨"], ["reset_process_ui_fields"],
                       ["log", "정보", "[레시피] 적재 해제: 웹 A"]]
    assert (st.csv_file_path, st.csv_rows, st.recipe_name) == (None, [], "")


def test_b5_clear_recipe_nothing_loaded():
    st = ProcessState()
    svc, p = _svc(st)
    svc.clear_recipe()
    assert p.calls == [["log", "정보", "[레시피] 적재된 레시피가 없습니다(해제할 것 없음)"]]


@pytest.mark.parametrize("kw", [dict(running=True), dict(csv_mode=True), dict(delay_active=True)])
def test_b5_clear_recipe_rejected_while_active(kw):
    st = ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)], **kw)
    svc, p = _svc(st)
    svc.clear_recipe()
    assert p.calls == [["alert", "warning", "해제 불가", "공정 진행 중에는 레시피 적재를 해제할 수 없습니다."]]
    assert st.csv_file_path == "C:/r/a.csv"


def test_b5_runner_delegates_recipe_clear():
    from integrations.erp_commands import ErpCommandRunner
    from test_erp_commands import FakeHost
    h = FakeHost()
    ErpCommandRunner(h).exec_one({"command": "RECIPE_CLEAR", "args": {}})
    assert h.calls == [["clear_recipe"]]
    assert ErpCommandRunner(h).silent_failure("RECIPE_CLEAR", {}) == ""          # 결과 확인 분기 없음 → 성공


# ───────────────────────── B6 원격 PLC 버튼은 링크가 끊겼을 때만 거부 ─────────────────────────
def test_b6_plc_button_rejected_only_when_link_down():
    from integrations.erp_commands import ErpCommandRunner
    from test_erp_commands import FakeHost, Widget
    h = FakeHost(link=False)
    h.widgets = {"MV_button": Widget(h, "MV_button")}
    with pytest.raises(RuntimeError, match="^PLC 통신이 끊겨 있어 실행할 수 없습니다$"):
        ErpCommandRunner(h).exec_one({"command": "MV_button", "args": {"on": True}})
    assert h.calls == []                                           # 버튼을 건드리지 않는다
    h.link = True
    h.active = True                                                # 공정 중에도 막지 않는다
    ErpCommandRunner(h).exec_one({"command": "MV_button", "args": {"on": True}})
    assert h.calls == [["w.setChecked", "MV_button", True]]


def test_b6_link_check_comes_before_missing_button():
    from integrations.erp_commands import ErpCommandRunner
    from test_erp_commands import FakeHost
    with pytest.raises(RuntimeError, match="PLC 통신이 끊겨"):
        ErpCommandRunner(FakeHost(link=False)).exec_one({"command": "Vent_button", "args": {"on": True}})


def test_b6_drain_reports_link_down_reason():
    from integrations.erp_commands import ErpCommandRunner
    from test_erp_commands import FakeHost
    h = FakeHost(link=False, cmds=[{"id": 4, "command": "Door_Button", "args": {"on": True}}])
    ErpCommandRunner(h).drain()
    assert h.calls[-1] == ["erp_cmd_result", 4, False, "PLC 통신이 끊겨 있어 실행할 수 없습니다"]


# ───────────────────────── 적재 실패·해제 때 입력칸 초기화 ─────────────────────────
@pytest.mark.parametrize("case", ["missing", "read_error", "no_rows", "bad_first_row"])
def test_reset_inputs_on_each_load_failure(case, recipe_file, tmp_path):
    ret = {"missing": {}, "read_error": dict(load_table=RuntimeError("깨짐")),
           "no_rows": dict(load_table=[{"#": "1"}]), "bad_first_row": dict(build_csv_params=ValueError("wp"))}[case]
    svc, p = _svc(ProcessState(), **ret)
    svc.load_recipe_file(str(tmp_path / "없음.csv") if case == "missing" else recipe_file)
    assert p.names()[-2:] == ["stage", "reset_process_ui_fields"] and p.names().count("reset_process_ui_fields") == 1


def test_reset_inputs_on_clear_with_recipe():
    svc, p = _svc(ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)]))
    svc.clear_recipe()
    assert p.names() == ["stage", "reset_process_ui_fields", "log"]


@pytest.mark.parametrize("make,call", [
    (lambda: ProcessState(), "clear"),                                                     # 적재 없음 해제
    (lambda: ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)], running=True), "clear"),  # 공정 중 해제 거부
    (lambda: ProcessState(csv_file_path="C:/r/a.csv", csv_rows=[dict(ROW)], csv_mode=True,
                          running=True), "load"),                                          # 공정 중 적재 거부("변경 불가")
    (lambda: ProcessState(), "load_closing"),                                             # 종료 중
    (lambda: ProcessState(), "load_empty"),                                               # 빈 경로(선택 창 취소)
])
def test_no_reset_inputs_when_nothing_changes(make, call, recipe_file):
    st = make()
    svc, p = _svc(st, is_closing=(call == "load_closing"))
    if call == "clear":
        svc.clear_recipe()
    elif call == "load_empty":
        svc.load_recipe_file("")
    else:
        svc.load_recipe_file(recipe_file)
    assert "reset_process_ui_fields" not in p.names()
