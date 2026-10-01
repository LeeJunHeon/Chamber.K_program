# -*- coding: utf-8 -*-
"""골든 — 원격 조작 규칙(B단계). 하니스·기록 방식은 test_golden_process 와 같다. 갱신은 CHK_UPDATE_GOLDEN=1 일 때만.

  B1 원격 수동 시작(PROCESS_START) — 인자로 입력값을 만들고, 통과한 뒤에만 노트북 입력칸에 보여 준다
    26a 레시피가 적재된 상태에서 원격 수동 시작 → 거부, 노트북 입력칸 전체 그대로
        (거부 자체와 상태 스냅샷은 13b 가 고정한다 — 여기서는 입력칸 22개 전후 비교만)
    26b 일부 키 없이 원격 수동 시작(useRfPulse·dcDelay 키 없음, 노트북은 RF Pulse·DC delay 체크) → 둘 다 끈 채로 시작
    26c offset·param 을 안 보냄 → 장비 값 유지 / 보냄 → 덮어씀
    26d 원격 수동 시작 입력 오류 → 거부, 노트북 입력칸 그대로
  B2 레시피 적재가 실패하면 적재 상태를 비운다
    26e 정상 적재 뒤 실패 적재 4가지(없는 파일·읽기 오류·빈 레시피·첫 행 오류) → 각각 적재 해제
  B3 Start 는 적재할 때 읽은 내용 그대로 실행한다
    26f 적재 뒤 파일 내용을 바꾸고 Start → 적재할 때 내용으로 실행
  B4 레시피 이름과 원격 레시피 파일 이름
    26g 원격 레시피 이름이 상태에 보임(이름 있음 / 없음) — 원격 파일은 명령마다 다른 이름, 예전 파일 정리
  B5 RECIPE_CLEAR (원격 적재 해제)
    26h 대기 중 적재 있음 / 적재 없음 / 공정 중
  B6 원격 PLC 버튼은 PLC 통신이 끊겨 있을 때만 거부
    26i PLC 링크 끊김 중 원격 PLC 버튼 → 거부, 링크 복구 뒤 → 실행
"""
import os
import re

import pytest

import main as MAIN

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, csv_row, _load_local, _REMOTE_ARGS   # noqa: F401

# 수동 입력칸 22개 — (위젯 이름, 체크박스인가)
_FIELDS = (("Ar_gas_radio", True), ("Ar_flow_edit", False), ("O2_gas_radio", True), ("O2_flow_edit", False),
           ("working_pressure_edit", False), ("dc_power_checkbox", True), ("DC_power_edit", False),
           ("rf_power_checkbox", True), ("RF_power_edit", False), ("offset_edit", False), ("param_edit", False),
           ("rf_pulse_checkbox", True), ("rfp_power_edit", False), ("rfp_freq_edit", False),
           ("rfp_duty_edit", False), ("Shutter_delay_edit", False), ("process_time_edit", False),
           ("G1_checkbox", True), ("G1_edit", False), ("G2_checkbox", True), ("G2_edit", False),
           ("dc_delay_checkbox", True))


def _fields(h, label):
    ui = h.w.ui
    h.sink.fields(label, {n: (getattr(ui, n).isChecked() if chk else getattr(ui, n).toPlainText())
                          for n, chk in _FIELDS})


def _notebook(h, **over):
    """노트북 수동 입력칸을 정해진 값으로(기록되는 시그널은 없다 — 스테이지만 기록된다)."""
    vals = dict(Ar_gas_radio=True, Ar_flow_edit="15", O2_gas_radio=False, O2_flow_edit="",
                working_pressure_edit="4", dc_power_checkbox=True, DC_power_edit="80",
                rf_power_checkbox=False, RF_power_edit="", offset_edit="7.10", param_edit="1.1",
                rf_pulse_checkbox=False, rfp_power_edit="", rfp_freq_edit="", rfp_duty_edit="",
                Shutter_delay_edit="2", process_time_edit="3", G1_checkbox=False, G1_edit="",
                G2_checkbox=True, G2_edit="Ti", dc_delay_checkbox=False)
    vals.update(over)
    ui = h.w.ui
    for n, v in vals.items():
        w = getattr(ui, n)
        w.setChecked(v) if isinstance(v, bool) else w.setPlainText(v)


def s26a_loaded_recipe_remote_manual_fields_untouched(h):
    _load_local(h, [csv_row("R1", dc_power="100")])
    _notebook(h)
    _fields(h, "거부 전")
    h.remote("PROCESS_START", dict(_REMOTE_ARGS, dcPower=150, workingPressure=3, offset=9, param=9))
    _fields(h, "거부 뒤")


def s26b_missing_keys_turn_off(h):
    _notebook(h, rf_pulse_checkbox=True, rfp_power_edit="100", dc_delay_checkbox=True)
    _fields(h, "시작 전")
    args = dict(_REMOTE_ARGS)                        # useRfPulse·dcDelay 키 없음
    assert "useRfPulse" not in args and "dcDelay" not in args
    h.remote("PROCESS_START", args)
    _fields(h, "시작 뒤")
    h.check("시작 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()


def s26c_rf_calibration_keep_or_override(h):
    _notebook(h, offset_edit="7.10", param_edit="1.1")
    h.remote("PROCESS_START", dict(_REMOTE_ARGS, useRf=True, rfPower=150), cid=1)    # offset·param 안 보냄
    _fields(h, "보내지 않음 → 장비 값")
    h.ctrl_run()
    h.ctrl_finish()
    h.remote("PROCESS_START", dict(_REMOTE_ARGS, useRf=True, rfPower=150, offset="6.5", param=" 1.2 "), cid=2)
    _fields(h, "보냄 → 덮어씀")
    h.ctrl_run()
    h.ctrl_finish()


def s26d_input_error_fields_untouched(h):
    _notebook(h)
    _fields(h, "거부 전")
    h.remote("PROCESS_START", dict(_REMOTE_ARGS, arFlow="", dcPower="abc"))
    _fields(h, "거부 뒤")
    h.check("거부 뒤", snapshot=True)


def s26e_failed_load_releases_recipe(h):
    good = h.write_csv([csv_row("G1"), csv_row("G2")], name="good.csv")

    def _good():
        h.w._start_csv_process_from_path(good)
        h.check("정상 적재", snapshot=True)

    _good()
    h.w._start_csv_process_from_path(str(h.tmp / "없는_파일.csv"))
    h.flush()
    h.check("없는 파일 뒤", snapshot=True)

    _good()
    real = MAIN.load_table

    def _broken(*a, **k):
        raise RuntimeError("시트를 읽을 수 없습니다(가짜)")
    h.mp.setattr(MAIN, "load_table", _broken)
    h.w._start_csv_process_from_path(good)
    h.flush()
    h.check("읽기 오류 뒤", snapshot=True)
    h.mp.setattr(MAIN, "load_table", real)

    _good()
    blank = {c: "" for c in csv_row("X")}
    h.w._start_csv_process_from_path(h.write_csv([blank], name="empty.csv"))
    h.flush()
    h.check("빈 레시피 뒤", snapshot=True)

    _good()
    h.w._start_csv_process_from_path(h.write_csv([csv_row("B1", working_pressure="abc")], name="bad.csv"))
    h.flush()
    h.check("첫 행 오류 뒤", snapshot=True)


def s26f_start_uses_rows_read_at_load(h):
    path = h.write_csv([csv_row("L1", dc_power="100")], name="edited.csv")
    h.w._start_csv_process_from_path(path)
    h.check("적재", snapshot=True)
    h.write_csv([csv_row("E1", dc_power="250"), csv_row("E2", dc_power="300")], name="edited.csv")   # 적재 뒤 파일 수정
    h.w.ui.Sputter_Start_Button.click()
    h.check("Start 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()
    h.check("리스트 완료", snapshot=True)


def _web_files(h, label):
    d = h.tmp / "systemp" / "vanam_recipe"
    h.sink.files(label, sorted(os.listdir(d)) if d.exists() else [])


def s26g_recipe_names(h):
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 A", "rows": [csv_row("W1")]}, cid=1)
    _web_files(h, "이름 있음 적재 뒤")
    h.check("이름 있음", snapshot=True)
    h.remote("PROCESS_START", dict(_REMOTE_ARGS), cid=2)          # B1 거부 문구에 표시 이름
    h.remote("RECIPE_PROCESS_RUN", {"rows": [csv_row("W2")]}, cid=3)
    _web_files(h, "이름 없음 적재 뒤")                             # 예전 파일(1)은 지우고 적재 중인 것만
    h.check("이름 없음 → 파일 이름", snapshot=True)
    h.w._start_csv_process_from_path(h.write_csv([csv_row("L1")], name="노트북 레시피.csv"))
    h.check("노트북 적재 → 파일 이름", snapshot=True)
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 B", "rows": [csv_row("W4")]}, cid=4)
    _web_files(h, "다시 원격 적재 뒤")                            # 적재 중이던 건 노트북 파일 → 예전 웹 파일 모두 정리


def s26h_recipe_clear(h):
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 C", "rows": [csv_row("W1"), csv_row("W2")]}, cid=1)
    h.check("적재", snapshot=True)
    h.remote("RECIPE_CLEAR", {}, cid=2)                           # 대기 중 적재 있음 → 해제
    h.check("해제 뒤", snapshot=True)
    h.remote("RECIPE_CLEAR", {}, cid=3)                           # 적재 없음 → 아무것도 하지 않고 성공
    h.check("다시 해제 뒤")
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 D", "rows": [csv_row("W3")]}, cid=4)
    h.remote("RECIPE_PROCESS_START", {}, cid=5)
    h.remote("RECIPE_CLEAR", {}, cid=6)                           # 공정 중 → 거부
    h.check("공정 중 해제 요청 뒤", snapshot=True)
    h.ctrl_run()
    h.ctrl_finish()


def s26i_plc_buttons_need_link(h):
    w = h.w
    for n in ("MV_button", "Rotary_button"):                       # 기록 없이 끈 상태로
        b = getattr(w.ui, n)
        b.blockSignals(True); b.setChecked(False); b.blockSignals(False)
    w._on_plc_link(False)
    h.remote("MV_button", {"on": True}, cid=1)                    # 링크 끊김 → 거부, 버튼 그대로
    h.sink.buttons("끊김 중", {"MV_button": w.ui.MV_button.isChecked()})
    w._on_plc_link(True)
    h.remote("MV_button", {"on": True}, cid=2)                    # 복구 뒤 → 실행
    h.sink.buttons("복구 뒤", {"MV_button": w.ui.MV_button.isChecked()})
    h.set_manual_ui()
    w.ui.Sputter_Start_Button.click()
    h.remote("Rotary_button", {"on": True}, cid=3)                # 공정 중에도 막지 않는다
    h.ctrl_run()
    h.ctrl_finish()


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_remote_rules(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
