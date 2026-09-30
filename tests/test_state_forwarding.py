# -*- coding: utf-8 -*-
"""MainDialog 의 옛 이름 16개 ↔ self.proc_state 필드(양방향), 그리고 main.py 에 세 값 or 식이 남지 않았는지."""
import ast
import os

import pytest

from test_main_heater import win   # noqa: F401  (픽스처 재사용)

import main as MAIN

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 옛 이름 → (ProcessState 필드, 시험값 1, 시험값 2)
FORWARD = {
    "process_running": ("running", True, False),
    "csv_file_path": ("csv_file_path", "C:/r/a.csv", "C:/r/b.csv"),
    "csv_rows": ("csv_rows", [{"Process_name": "S1"}], [{"Process_name": "S2"}]),
    "csv_index": ("csv_index", 3, 7),
    "csv_mode": ("csv_mode", True, False),
    "csv_cancelled": ("csv_cancelled", True, False),
    "_csv_delay_active": ("delay_active", True, False),
    "_csv_delay_total_sec": ("delay_total_sec", 60, 90),
    "_csv_delay_remaining_sec": ("delay_remaining_sec", 12, 34),
    "_csv_delay_name": ("delay_name", "delay 1m", "delay 2m"),
    "current_process_name": ("current_name", "CSV 1/2 - S1", "Single CHK"),
    "_last_params": ("last_params", {"dc_power": 100.0}, {"dc_power": 150.0}),
    "_chk_process_ok": ("step_ok", True, False),
    "_proc_origin": ("origin", "erp", "local"),
    "_finish_handled": ("finish_handled", False, True),
    "_process_heater_claimed": ("heater_claimed", True, False),
}


@pytest.mark.parametrize("old", list(FORWARD))
def test_old_name_forwards_both_ways(win, old):
    w = win
    field, v1, v2 = FORWARD[old]
    assert isinstance(getattr(type(w), old), property)
    assert old not in w.__dict__
    saved = getattr(w.proc_state, field)
    try:
        setattr(w, old, v1)                       # 옛 이름으로 쓰면 → proc_state
        assert getattr(w.proc_state, field) is v1 or getattr(w.proc_state, field) == v1
        assert getattr(w, old) == v1
        setattr(w.proc_state, field, v2)          # proc_state 에 쓰면 → 옛 이름
        assert getattr(w, old) == v2
        assert getattr(w, old, "없음") == v2       # getattr(…, 기본값) 형태도 같은 값
        assert old not in w.__dict__
    finally:
        setattr(w.proc_state, field, saved)
    assert getattr(w, old) == saved


def test_mutable_values_are_the_same_object(win):
    """csv_rows 등은 복사가 아니라 같은 객체가 오간다(append 등 제자리 변경도 그대로 보인다)."""
    w = win
    saved = w.proc_state.csv_rows
    try:
        rows = []
        w.csv_rows = rows
        assert w.proc_state.csv_rows is rows and w.csv_rows is rows
    finally:
        w.proc_state.csv_rows = saved


# ───────────────────────── 정적 검사 ─────────────────────────
_TRI = {"process_running", "csv_mode", "_csv_delay_active"}


def _name_of(n):
    """self.X / getattr(self, 'X', d) / bool(…) / not … 을 벗겨 이름을 얻는다."""
    while True:
        if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.Not):
            n = n.operand
            continue
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "bool" and len(n.args) == 1:
            n = n.args[0]
            continue
        break
    if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "self":
        return n.attr
    if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "getattr"
            and len(n.args) >= 2 and isinstance(n.args[1], ast.Constant)):
        return n.args[1].value
    return None


def test_no_three_value_or_expression_left_in_main():
    path = os.path.join(ROOT, "main.py")
    src = open(path, encoding="utf-8").read()
    left = []
    for node in ast.walk(ast.parse(src, path)):
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            names = {_name_of(v) for v in node.values}
            if _TRI <= names:
                left.append((node.lineno, ast.get_source_segment(src, node)[:80]))
    assert left == []


def test_process_active_delegates_to_state(win):
    w = win
    saved = (w.proc_state.running, w.proc_state.csv_mode, w.proc_state.delay_active)
    try:
        w.proc_state.running, w.proc_state.csv_mode, w.proc_state.delay_active = False, False, True
        assert w._process_active() is True
        w.proc_state.delay_active = False
        assert w._process_active() is False
    finally:
        w.proc_state.running, w.proc_state.csv_mode, w.proc_state.delay_active = saved
    assert MAIN.MainDialog._process_active.__qualname__ == "MainDialog._process_active"
