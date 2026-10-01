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
    assert p.calls[-1] == ["stage", "레시피 적재 실패"] and p.names()[-2] == "alert"
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
