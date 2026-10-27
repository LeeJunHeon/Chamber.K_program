# -*- coding: utf-8 -*-
"""HEATER_CTRL_MODE=cascade 경로 — main 연결(수동·공정·화면·ERP)과 공정 컨트롤러 대기, 캐스케이드용 골든.

기존 골든(tc1 롤백 경로)은 그대로 두고, 캐스케이드 경로 골든은 여기서 새로 만든다(28a). 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.
  28a 수동 ON → TC2 제어 진입 → 화면(SV 칸=r1, TC2 줄=TC2 → SV2, 상태 문구) / ERP 상태(tc2Hold·curSv=r1) → 수동 OFF 해제 순서
"""
import time

import pytest
from PyQt6.QtCore import QTimer

import main as MAIN
from conftest import make_heater_st
from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden   # noqa: F401
from test_process_safety import pc   # noqa: F401

pytestmark = pytest.mark.cascade


def _casc_reset(w, monkeypatch):
    c = w.heater_casc
    c._reset()
    clock = [1000.0]
    monkeypatch.setattr(c, "_now", lambda: clock[0])
    return c, clock


def _engage(w, c, clock, pv=100.0, pv2=140.0):
    """수동 ON 흉내: RUN OFF 폴링 → _heater_manual_go → RUN ON 폴링 → D00035 되읽기 → M0004B."""
    w.update_heater_display(make_heater_st(run=False, pv=pv, sv=pv, pv2=pv2))
    w._heater_manual_go(300.0)
    st = dict(run=True, pv=pv, sv=pv, pv2=pv2, sv_ramp=pv, cur_sv=pv, mv=520)
    w.update_heater_display(make_heater_st(**st))
    sv2 = c._sv2_cmd
    clock[0] += 0.2
    w.update_heater_display(make_heater_st(**st, sv2=sv2))
    clock[0] += 0.2
    st.update(sv2=sv2, pv_sel_eff=True, sv_ramp=sv2, cur_sv=sv2, pv_ctrl=pv2)
    w.update_heater_display(make_heater_st(**st))
    return st


def s28a_cascade_manual_on_lcd_erp_off(h):
    w = h.w
    c, clock = _casc_reset(w, h.mp)
    h.mp.setattr(c, "learned_path", str(h.tmp / "learned.json"))
    w.ui.heater_sv_edit.setText("300")
    st = _engage(w, c, clock)
    assert c.engaged()
    for _ in range(30):                               # 30 s — r1 이 6°C/min 으로 3°C 오른다
        clock[0] += 1.0
        w.update_heater_display(make_heater_st(**st))
    h.flush()
    h.check("캐스케이드 TC2 제어 30초", snapshot=True)
    w._on_heater_onoff_toggled(False)
    h.flush()
    h.check("수동 OFF", snapshot=True)


SCENARIOS = {"28a_cascade_manual_on_lcd_erp_off": s28a_cascade_manual_on_lcd_erp_off}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_cascade(name, H, tmp_path):
    H.tmp = tmp_path
    SCENARIOS[name](H)
    check_golden(name, H.result())


# ───────────────────────── main 연결 ─────────────────────────
def test_casc_manual_on_off_routes_and_release_order(fresh, monkeypatch, tmp_path):
    w = fresh
    c, clock = _casc_reset(w, monkeypatch)
    monkeypatch.setattr(c, "learned_path", str(tmp_path / "l.json"))
    order = []
    w.request_heater_pv_sel.connect(lambda b: order.append(("M0004A", b)))
    c.request_sv2.connect(lambda v: order.append(("D00035", v)))
    w.request_heater_run.connect(lambda b: order.append(("RUN", b)))
    st = _engage(w, c, clock)
    assert c.engaged() and not w.heater_ramp.is_active()          # RampProfiler 는 쓰지 않는다
    assert ("RUN", True) in order and ("M0004A", True) in order
    # 화면: SV 칸은 r1(TC1 기준), TC2 줄은 TC2 → SV2, 상태는 TC2 제어
    assert w.ui.heater_sv_big.text() == f"{c.r1:.1f}"
    assert w.ui.heater_pv2_label.text() == f"TC2 140.0 → {c.sv2:.1f}"
    assert w.ui.heater_status_label.text().startswith("운전 중 · TC2 제어 · bias")
    assert w._erp_heater["hold_kind"] == "cascade" and w._erp_heater["cur_sv"] == round(c.r1, 1)
    # 운전 중 목표 변경 — 끊지 않고 목표만
    w.ui.heater_sv_edit.setText("250")
    monkeypatch.setattr(w.plc_controller, "get_heater_status", lambda: make_heater_st(**st))
    w._on_heater_apply_clicked()
    assert c.engaged() and c.target() == 250.0
    order.clear()
    w._on_heater_onoff_toggled(False)
    assert order[:3] == [("M0004A", False), ("D00035", 0.0), ("RUN", False)]
    assert c.state == 'idle'


def test_casc_process_target_and_reached_card(fresh, monkeypatch, tmp_path):
    w = fresh
    c, clock = _casc_reset(w, monkeypatch)
    monkeypatch.setattr(c, "learned_path", str(tmp_path / "l.json"))
    w._process_heater_rate_c = 12.0
    w._on_process_heater_target(500.0)
    assert c.is_active() and c.target() == 500.0 and c.rate() == 12.0 and not w.heater_ramp.is_active()
    assert w._heater_final_target({"sv": 30.0}) == 500.0
    txt, ok = w._heater_hold_result_text()
    assert not ok and "캐스케이드" in txt                         # 아직 TC2 제어 아님
    w._on_process_heater_run(False)
    assert c.state == 'idle'


def test_casc_snapshot_for_process(fresh, monkeypatch, tmp_path):
    w = fresh
    c, clock = _casc_reset(w, monkeypatch)
    monkeypatch.setattr(c, "learned_path", str(tmp_path / "l.json"))
    _engage(w, c, clock)
    snap = w._hold_snapshot
    assert snap["mode"] == "off" and snap["ctrl_mode"] == MAIN.HEATER_CTRL_MODE
    assert snap["casc"]["engaged"] is True and snap["casc"]["final"] == 300.0 and snap["casc"]["r1_done"] is False


# ───────────────────────── 공정 컨트롤러 ─────────────────────────
def _casc_snap(pc, **kw):
    pc._hold["casc"] = dict({"active": True, "state": "running", "engaged": True, "final": 600.0, "r1": 590.0,
                             "r1_done": False, "sv2": 830.0, "bias": 5.0}, **kw)


def test_casc_heater_wait_uses_r1_not_sv_ramp_and_passes_without_hold_fallback(pc):
    pc._plc.st.update(sv_ramp=830.0, cur_sv=830.0)              # TC2 제어 중 D00019 는 SV2 다 — 도달 판정에 쓰면 안 된다
    _casc_snap(pc)
    QTimer.singleShot(1200, lambda: pc._hold["casc"].update(r1=600.0, r1_done=True))
    t0 = time.monotonic()
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 1 and c["abort"] == [] and c["force"] == [] and c["failed"] == []
    assert time.monotonic() - t0 >= 2.0                          # r1 도달(1.2 s) + soak 1 s
    assert any("캐스케이드 TC2 제어 확인 (SV2 830.0°C, bias +5.0°C)" in m for _, m in c["msgs"])
    assert any("바깥 루프 수렴 여유 20분(TI×2)" in m for _, m in c["msgs"])


def test_casc_hold_wait_not_engaged_applies_fail_action(pc):
    _casc_snap(pc, r1=600.0, r1_done=True, engaged=False, state="tc1", sv2=None)
    pc._heater_wait(600.0)
    c = pc._calls
    assert c["next"] == 0 and len(c["abort"]) == 1 and "TC2 제어가 아님(상태 tc1)" in c["abort"][0]
    assert c["failed"][0]["action"] == "abort" and c["force"] == []


def test_casc_hold_wait_engages_late(pc):
    _casc_snap(pc, r1=600.0, r1_done=True, engaged=False, state="engaging_sel", sv2=None)
    QTimer.singleShot(1500, lambda: pc._hold["casc"].update(engaged=True, state="running", sv2=825.0))
    pc._heater_wait(600.0)
    assert pc._calls["next"] == 1 and pc._calls["abort"] == []
