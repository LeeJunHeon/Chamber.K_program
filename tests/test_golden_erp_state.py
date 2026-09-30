# -*- coding: utf-8 -*-
"""골든(동작 고정) — ERP 상태 스냅샷에서 기존 골든이 한 번도 찍지 않은 분기.

하니스·기록 방식은 test_golden_process 와 같다(check(…, snapshot=True) 로 상태를 찍는다). 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  25a 타겟 그룹: G2 만 체크(이름 빈칸 → "미입력"), G1·G2 둘 다 체크
  25b PLC 쪽 값이 채워진 상태: 밸브 표시, 표시등(ION_RUN·ION_LAMP·ION_OT 포함), MV_INTERLOCK 비트
  25c 히터 쪽 값이 채워진 상태: 히터 상태 갱신(run·fault·cur_sv …), TC2 추종 중(heater_hold 가 tc2 로 유지 중)
  25d 스냅샷 수집 중 예외 → erp.event("error", …) 1회, 다음 틱엔 다시 보고하지 않음, 원인이 없어진 뒤 상태 보고 재개
시나리오마다 이 파일이 건드리는 값을 기록 없이(시그널 없이) 초기화하고 시작한다 — 앞 시나리오 값이 새지 않게.
"""
import re

import pytest

from conftest import make_heater_st
from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden   # noqa: F401

_BUTTONS = ("MV_button", "Ar_Button", "Door_Button", "G1_checkbox", "G2_checkbox")
_TEXTS = ("G1_edit", "G2_edit", "heater_pv_edit", "heater_status_label", "heater_sv_big",
          "heater_dev_label", "heater_pv2_label")


def _quiet_reset(h):
    """이 파일이 건드리는 위젯·값을 조용히 초기 상태로(기록되지 않게)."""
    w = h.w
    for n in _BUTTONS:
        b = getattr(w.ui, n)
        b.blockSignals(True)
        b.setChecked(False)
        b.blockSignals(False)
    for n in _TEXTS:
        t = getattr(w.ui, n)
        t.blockSignals(True)
        (t.setPlainText if hasattr(t, "setPlainText") else t.setText)("")
        t.blockSignals(False)
    w.ui.heater_sv_edit.setText("")
    w._erp_heater = {}
    w._erp_heater_dev_ok = False
    w._heater_badge = None
    w._erp_valves = {}
    w._erp_indicators = {}
    w._plc_bits.clear()
    w._erp_snap_err = False


def s25a_target_groups(h):
    _quiet_reset(h)
    ui = h.w.ui
    ui.G2_checkbox.setChecked(True)                    # G2 만, 이름 빈칸 → "미입력"
    h.check("G2 만(이름 빈칸)", snapshot=True)
    ui.G1_checkbox.setChecked(True)
    ui.G1_edit.setPlainText("CeO2")
    ui.G2_edit.setPlainText("  Ti ")
    h.check("G1·G2 둘 다", snapshot=True)


class _Tc2Holding:
    """heater_hold 대역 — 스냅샷이 읽는 is_holding()·kind 만."""
    kind = "tc2"

    def is_holding(self):
        return True


def s25b_plc_values(h):
    _quiet_reset(h)
    w = h.w
    w.update_ui_button_display("MV_button", True)
    w.update_ui_button_display("Ar_Button", False)
    w.update_ui_button_display("Doorup_button", True)
    for name, st in (("ION_RUN", True), ("ION_LAMP", False), ("ION_OT", True), ("Air", True)):
        w.set_indicator(name, st)
    w._on_plc_bit_changed("MV_INTERLOCK", True, None)
    h.flush()
    h.check("PLC 값 채움", snapshot=True)
    w.set_indicator("Air", None)                        # 링크 다운 회색 — ERP 값은 마지막 값 그대로
    h.check("표시등 회색 뒤", snapshot=True)


def s25c_heater_values(h):
    _quiet_reset(h)
    w = h.w
    w.update_heater_display(make_heater_st(run=True, pv=598.9, sv=600.0, cur_sv=512.5, pid_err=3,
                                           mv=800, mv_pct=66.7, pv2=893.4))
    h.flush()
    h.check("히터 운전 중", snapshot=True)
    w.update_heater_display(make_heater_st(run=False, fault=True, tc_err=True, pv=420.0, sv=600.0))
    h.flush()
    h.check("히터 이상", snapshot=True)
    h.mp.setattr(w, "heater_hold", _Tc2Holding())
    h.check("TC2 추종 중", snapshot=True)


def s25d_snapshot_error_reported_once(h):
    _quiet_reset(h)
    w = h.w
    broken = {"on": True}
    real = w._heater_output_text

    def _flaky(*a, **k):
        if broken["on"]:
            raise RuntimeError("히터 출력 읽기 실패(가짜)")
        return real(*a, **k)
    h.mp.setattr(w, "_heater_output_text", _flaky)
    h.check("예외 1틱", snapshot=True)
    h.check("예외 2틱", snapshot=True)                   # 다시 보고하지 않는다
    broken["on"] = False
    h.check("원인 사라진 뒤", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_erp_state(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
