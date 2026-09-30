# -*- coding: utf-8 -*-
"""공정 상태 값 16개의 첫 값과 _process_active() 판정 — ProcessState 로 모으기 전·후 모두 같아야 한다.
첫 테스트는 이 모듈의 새 MainDialog(win)를 만든 직후, 이벤트 루프를 돌리기 전에 읽는다."""
import itertools

from test_main_heater import win   # noqa: F401  (픽스처 재사용 — 이 모듈 전용 새 창)

INITIAL = {
    "process_running": False,
    "csv_file_path": None,
    "csv_rows": [],
    "csv_index": -1,
    "csv_mode": False,
    "csv_cancelled": False,
    "_csv_delay_active": False,
    "_csv_delay_total_sec": 0,
    "_csv_delay_remaining_sec": 0,
    "_csv_delay_name": "",
    "current_process_name": "",
    "_last_params": None,
    "_chk_process_ok": False,
    "_proc_origin": "local",
    "_finish_handled": True,
    "_process_heater_claimed": False,
}


def test_initial_values_right_after_construction(win):
    got = {k: getattr(win, k) for k in INITIAL}
    assert got == INITIAL
    # 타입까지 같아야 한다(0 과 False, [] 와 None 을 가른다)
    assert {k: type(v).__name__ for k, v in got.items()} == {k: type(v).__name__ for k, v in INITIAL.items()}


def test_process_active_matches_expression(win):
    w = win
    saved = (w.process_running, w.csv_mode, w._csv_delay_active)
    try:
        for pr, cm, da in itertools.product((False, True), repeat=3):
            w.process_running, w.csv_mode, w._csv_delay_active = pr, cm, da
            assert w._process_active() == (pr or cm or da), (pr, cm, da)
            assert isinstance(w._process_active(), bool)
    finally:
        w.process_running, w.csv_mode, w._csv_delay_active = saved
