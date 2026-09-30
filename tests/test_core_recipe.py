# -*- coding: utf-8 -*-
"""core.recipe — Qt 없이. 딜레이 행 해석, 빈 행 판정, 레시피 히터 사용 판정."""
import pytest

from core.recipe import CSV_DELAY_RE, csv_rows_use_heater, parse_delay_seconds, row_has_content


@pytest.mark.parametrize("name,sec", [
    ("delay 30s", 30), ("delay 2m", 120), ("delay 1h", 3600), ("delay 1d", 86400),
    ("delay 1.5m", 90), ("delay 0.5s", 0), ("delay 2.9s", 2),        # 초 단위로 내림(int)
    ("DELAY 3M", 180), ("Delay 10S", 10),                            # 대소문자 무관
    ("  delay   5 m  ", 300), ("delay 5m", 300), ("delay 0s", 0),    # 앞뒤·중간 공백
])
def test_parse_delay_seconds_ok(name, sec):
    assert parse_delay_seconds(name) == sec


@pytest.mark.parametrize("name", [
    None, "", "S1", "delay", "delay 5", "delay m", "delay -5m", "delay 5x", "delay5m",
    "wait 5m", "delay 5m extra", "delay 1,5m", "delay 5 min",
])
def test_parse_delay_seconds_not_delay(name):
    assert parse_delay_seconds(name) is None


def test_delay_regex_is_case_insensitive():
    assert CSV_DELAY_RE.match("DeLaY 1H")


@pytest.mark.parametrize("row,expected", [
    ({"#": "3", "Process_name": "", "Ar": None}, False),     # 번호만 → 빈 행
    ({"#": "", "Process_name": "   "}, False),                # 공백만
    ({"Process_name": None, "Ar": None}, False),              # None 만
    ({}, False), (None, False),
    ({"#": "1", "Ar": "0"}, True),                            # "0" 도 값이다
    ({" # ": "1", "Ar": ""}, False),                          # "#" 키 앞뒤 공백도 번호 칸
    ({"#": "1", "G2 Target": " Ti "}, True),
])
def test_row_has_content(row, expected):
    assert row_has_content(row) is expected


@pytest.mark.parametrize("rows,expected", [
    (None, False), ([], False),
    ([{"use_heater": "1", "heater_temp": "300"}], True),
    ([{"USE_HEATER": "Yes", " Heater_Temp ": " 250 "}], True),       # 키 대소문자·공백 무관
    ([{"use_heater": "On", "heater_temp": "1"}], True),
    ([{"use_heater": "t", "heater_temp": "10"}], True),
    ([{"use_heater": "2", "heater_temp": "300"}], False),            # 참 표기 아님
    ([{"use_heater": "1", "heater_temp": "0"}], False),
    ([{"use_heater": "1", "heater_temp": "abc"}], False),            # 숫자 아님 → 미사용
    ([{"use_heater": "1", "heater_temp": ""}], False),
    ([{"use_heater": "1"}], False),                                   # temp 칸 없음
    ([{"use_heater": None, "heater_temp": "300"}], False),
    ([{"use_heater": "0", "heater_temp": "300"}, None, {"use_heater": "1", "heater_temp": "5"}], True),  # 뒤 행도 본다
])
def test_csv_rows_use_heater(rows, expected):
    assert csv_rows_use_heater(rows) is expected
