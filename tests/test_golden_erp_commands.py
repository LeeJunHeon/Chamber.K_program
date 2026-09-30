# -*- coding: utf-8 -*-
"""골든(동작 고정) — ERP 원격 명령 처리의 기존 골든 밖 경로.

하니스·기록 방식은 test_golden_process 와 같다. 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  24a 원격 PLC 버튼 명령 — MV_button 켜기·끄기, Door_Button, ION_button
  24b 허용되지 않은 명령
  24c 장비에 모달 창이 떠 있을 때 명령 2건(둘 다 거부)
  24d ERP 연결 거부(erp.rejected True) → 재개(False) 로그
  24e 한 번에 명령 여러 건(순서대로 결과)
  24f RECIPE_PROCESS_RUN 에 행 없음(예외 경로)
  24g 적재된 레시피 없이 RECIPE_PROCESS_START
"""
import re

import pytest

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden   # noqa: F401

import main as MAIN


def _drain(h, cmds):
    """ERP 명령 여러 건을 한 번에 꺼내게 하고 드레인 타이머 1틱을 흉내 낸다."""
    h.sink.erp.pop_commands.return_value = list(cmds)
    h.w._erp_cmd_timer.timeout.emit()
    h.sink.erp.pop_commands.return_value = []
    h.flush()


def _reset_buttons(h):
    """이 파일의 시나리오가 건드리는 PLC 버튼을 조용히(시그널 없이) 끈 상태로 — 앞 시나리오 상태가 새지 않게."""
    for n in ("MV_button", "Door_Button", "ION_button"):
        b = getattr(h.w.ui, n)
        b.blockSignals(True)
        b.setChecked(False)
        b.blockSignals(False)


def _buttons(h, label):
    ui = h.w.ui
    h.sink.buttons(label, {n: getattr(ui, n).isChecked() for n in ("MV_button", "Door_Button", "ION_button")})


def s24a_remote_plc_buttons(h):
    _reset_buttons(h)
    _buttons(h, "출발")
    h.remote("MV_button", {"on": True}, cid=1)
    h.remote("Door_Button", {"on": True}, cid=2)
    h.remote("ION_button", {"on": True}, cid=3)
    _buttons(h, "켠 뒤")
    h.remote("MV_button", {"on": False}, cid=4)
    h.remote("Door_Button", {}, cid=5)                 # on 없음 → False
    _buttons(h, "끈 뒤")
    h.check("버튼 명령 뒤", snapshot=True)


def s24b_unknown_command(h):
    _reset_buttons(h)
    h.remote("FORMAT_C_DRIVE", {"x": 1})
    h.check("거부 뒤")


def s24c_modal_open_rejects_all(h):
    _reset_buttons(h)
    h.mp.setattr(MAIN.QApplication, "activeModalWidget", staticmethod(lambda: object()))
    _drain(h, [{"id": 11, "command": "MV_button", "args": {"on": True}},
               {"id": 12, "command": "PROCESS_START", "args": {}}])
    _buttons(h, "거부 뒤")
    h.check("거부 뒤")


def s24d_erp_rejected_then_resumed(h):
    _reset_buttons(h)
    h.sink.erp.rejected = True
    _drain(h, [])
    _drain(h, [])                                       # 알림은 1회만
    h.sink.erp.rejected = False
    _drain(h, [])
    _drain(h, [])                                       # 재개 알림도 1회만


def s24e_several_commands_in_order(h):
    _reset_buttons(h)
    _drain(h, [{"id": 21, "command": "MV_button", "args": {"on": True}},
               {"id": 22, "command": "PROCESS_STOP", "args": {}},
               {"id": 23, "command": "NOPE", "args": {}},
               {"id": 24, "command": "ALL_STOP", "args": {}},
               {"id": 25, "command": "RECIPE_PROCESS_START", "args": {}}])
    _buttons(h, "뒤")
    h.check("여러 건 뒤", snapshot=True)


def s24f_recipe_run_without_rows(h):
    _reset_buttons(h)
    h.remote("RECIPE_PROCESS_RUN", {"rows": []}, cid=31)
    h.remote("RECIPE_PROCESS_RUN", {}, cid=32)
    h.check("뒤")


def s24g_recipe_start_without_loaded(h):
    _reset_buttons(h)
    h.remote("RECIPE_PROCESS_START", {}, cid=41)
    h.check("뒤", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_erp_commands(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
