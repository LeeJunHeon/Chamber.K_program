# -*- coding: utf-8 -*-
"""core.state — Qt 없이. 기본값, is_active, phase 표, clear_csv_list."""
import dataclasses
import itertools

import pytest

from core.state import Phase, ProcessState

DEFAULTS = {
    "running": False, "csv_file_path": None, "csv_rows": [], "csv_index": -1,
    "csv_mode": False, "csv_cancelled": False, "delay_active": False,
    "delay_total_sec": 0, "delay_remaining_sec": 0, "delay_name": "",
    "current_name": "", "last_params": None, "step_ok": False, "origin": "local",
    "finish_handled": True, "heater_claimed": False,
}


def test_defaults():
    st = ProcessState()
    got = dataclasses.asdict(st)
    assert got == DEFAULTS
    assert [f.name for f in dataclasses.fields(st)] == list(DEFAULTS)      # 필드 16개, 순서까지
    assert {k: type(v) for k, v in got.items()} == {k: type(v) for k, v in DEFAULTS.items()}
    assert ProcessState().csv_rows is not ProcessState().csv_rows            # 인스턴스마다 새 리스트


@pytest.mark.parametrize("running,csv_mode,delay", list(itertools.product((False, True), repeat=3)))
def test_is_active_matches_expression(running, csv_mode, delay):
    st = ProcessState(running=running, csv_mode=csv_mode, delay_active=delay)
    assert st.is_active() is (running or csv_mode or delay)


# (delay_active, csv_mode, running, csv_file_path 있음) → Phase
PHASES = {
    (False, False, False, False): Phase.IDLE,
    (False, False, False, True): Phase.LOADED,
    (False, False, True, False): Phase.MANUAL,
    (False, False, True, True): Phase.MANUAL,
    (False, True, False, False): Phase.CSV_BETWEEN,
    (False, True, False, True): Phase.CSV_BETWEEN,
    (False, True, True, False): Phase.CSV_STEP,
    (False, True, True, True): Phase.CSV_STEP,
    (True, False, False, False): Phase.CSV_DELAY,
    (True, False, False, True): Phase.CSV_DELAY,
    (True, False, True, False): Phase.CSV_DELAY,
    (True, False, True, True): Phase.CSV_DELAY,
    (True, True, False, False): Phase.CSV_DELAY,
    (True, True, False, True): Phase.CSV_DELAY,
    (True, True, True, False): Phase.CSV_DELAY,
    (True, True, True, True): Phase.CSV_DELAY,
}


@pytest.mark.parametrize("key", list(PHASES))
def test_phase_table(key):
    delay, csv_mode, running, has_file = key
    st = ProcessState(delay_active=delay, csv_mode=csv_mode, running=running,
                      csv_file_path=("C:/r/recipe.csv" if has_file else None))
    assert st.phase() is PHASES[key]


def test_phase_values():
    assert [p.value for p in Phase] == ["idle", "loaded", "manual", "csv_step", "csv_delay", "csv_between"]
    assert Phase.CSV_STEP == "csv_step"       # str Enum — 문자열로 그대로 보낼 수 있다


def test_clear_csv_list_changes_only_six_and_uses_new_list():
    rows = [{"Process_name": "S1"}, {"Process_name": "S2"}]
    st = ProcessState(running=True, csv_file_path="C:/r/recipe.csv", csv_rows=rows, csv_index=1,
                      csv_mode=True, csv_cancelled=True, delay_active=True, delay_total_sec=30,
                      delay_remaining_sec=12, delay_name="delay 30s", current_name="CSV 2/2 - S2",
                      last_params={"dc_power": 100.0}, step_ok=True, origin="erp",
                      finish_handled=False, heater_claimed=True)
    before = dataclasses.asdict(st)
    st.clear_csv_list()
    after = dataclasses.asdict(st)
    changed = {k for k in before if before[k] != after[k]}
    assert changed == {"csv_mode", "csv_rows", "csv_index", "csv_file_path", "current_name", "last_params"}
    assert (st.csv_mode, st.csv_rows, st.csv_index, st.csv_file_path, st.current_name, st.last_params) == \
        (False, [], -1, None, "", None)
    assert st.csv_rows is not rows and rows == [{"Process_name": "S1"}, {"Process_name": "S2"}]   # 제자리 clear() 아님
