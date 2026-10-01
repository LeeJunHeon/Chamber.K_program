# -*- coding: utf-8 -*-
"""골든 — 레시피 임시 파일·적재 검사·ERP 보고 정리(E 단계). 하니스·기록 방식은 test_golden_process 와 같다.
갱신은 CHK_UPDATE_GOLDEN=1 일 때만.

  E2 웹 레시피 임시 파일은 적재가 끝나면 바로 지운다
    27a 예전 파일(process_web.csv·process_web_9.csv)이 있는 폴더 → 원격 정상 적재(폴더 비어 있음, 적재는 됨) → Start →
        공정 끝 / 원격 첫 행 오류 적재 → 실패, 폴더 비어 있음 / 정상 적재 → Start → 공정 중 원격 적재 "변경 불가",
        폴더 비어 있음 → 공정 끝 / 노트북에서 고른 파일은 그대로
  E3 적재할 때 모든 공정 행을 검사한다
    27b [S1, delay 1m, S3(오류)] → "3번째 …" 적재 실패·입력칸 초기화 / [delay 1m, S2(오류)] → 적재 실패(전에는 검사 안 함) /
        [S1, delay 1m, S3] 정상 → 적재 성공, 미리보기 그대로
"""
import os
import re

import pytest

from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, csv_row, delay_row   # noqa: F401


def _web_files(h, label):
    d = h.tmp / "systemp" / "vanam_recipe"
    h.sink.files(label, sorted(os.listdir(d)) if d.exists() else [])


def s27a_web_recipe_temp_files(h):
    d = h.tmp / "systemp" / "vanam_recipe"
    d.mkdir(parents=True, exist_ok=True)
    for n in ("process_web.csv", "process_web_9.csv"):            # 예전 이름·남은 파일
        (d / n).write_text("x", encoding="utf-8")
    _web_files(h, "처음")
    # 정상 적재 → 폴더 비어 있음(적재는 됨) → Start → 적재된 행으로 진행 → 끝
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 A", "rows": [csv_row("W1")]}, cid=1)
    _web_files(h, "정상 적재 뒤")
    h.check("정상 적재 뒤", snapshot=True)
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.ctrl_finish()
    h.check("공정 끝", snapshot=True)
    # 첫 행 오류 적재 → 실패, 폴더 비어 있음
    h.remote("RECIPE_PROCESS_RUN", {"rows": [csv_row("B1", working_pressure="abc")]}, cid=2)
    _web_files(h, "첫 행 오류 뒤")
    h.check("첫 행 오류 뒤")
    # 정상 적재 → Start → 공정 중 원격 적재 → "변경 불가", 폴더 비어 있음 → 끝
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 C", "rows": [csv_row("W3")]}, cid=3)
    h.w.ui.Sputter_Start_Button.click()
    h.ctrl_run()
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 D", "rows": [csv_row("W4")]}, cid=4)
    _web_files(h, "공정 중 적재 거부 뒤")
    h.ctrl_finish()
    h.check("공정 끝(2)", snapshot=True)
    # 노트북에서 고른 파일은 지우지 않는다
    local = h.write_csv([csv_row("L1")], name="노트북 레시피.csv")
    h.w._start_csv_process_from_path(local)
    h.remote("RECIPE_PROCESS_RUN", {"name": "웹 E", "rows": [csv_row("W5")]}, cid=5)
    _web_files(h, "노트북 적재 뒤 원격 적재")
    h.sink.local_file("노트북 레시피.csv 남아 있음", os.path.exists(local))


def s27b_validate_all_rows_at_load(h):
    h.w._start_csv_process_from_path(h.write_csv(
        [csv_row("S1"), delay_row("delay 1m"), csv_row("S3", working_pressure="abc")], name="row3_bad.csv"))
    h.flush()
    h.check("3번째 행 오류", snapshot=True)
    h.w._start_csv_process_from_path(h.write_csv(
        [delay_row("delay 1m"), csv_row("S2", working_pressure="abc")], name="delay_first_bad.csv"))
    h.flush()
    h.check("첫 행 딜레이 + 2번째 행 오류", snapshot=True)
    h.w._start_csv_process_from_path(h.write_csv(
        [csv_row("S1"), delay_row("delay 1m"), csv_row("S3")], name="ok.csv"))
    h.flush()
    h.check("정상", snapshot=True)


SCENARIOS = {name[1:]: fn for name, fn in sorted(globals().items())
             if re.match(r"s\d\d[a-z]?_", name) and callable(fn)}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_golden_report_rules(name, H):
    SCENARIOS[name](H)
    check_golden(name, H.result())
