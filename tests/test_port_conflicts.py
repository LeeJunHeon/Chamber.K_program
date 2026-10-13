# -*- coding: utf-8 -*-
"""RF Pulse 기본 포트(COM13)와 시작 때 시리얼 포트 중복 경고."""
import os
import re

import main as MAIN
from lib.config import serial_port_conflicts
from test_main_heater import win  # noqa: F401  (픽스처 재사용)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_rfpulse_default_port_is_com13():
    src = open(os.path.join(ROOT, "lib", "config.py"), encoding="utf-8").read()
    m = re.search(r"^RFPULSE_PORT\s*=\s*get\('RFPULSE_PORT',\s*\"(COM\d+)\"\)", src, re.M)
    assert m and m.group(1) == "COM13"
    assert "COM12 는 이온 게이지(IG_PORT)가 쓴다" in src


def test_conflicts_pure():
    assert serial_port_conflicts({"MFC_PORT": "COM10", "DC_PORT": "COM11", "PLC_PORT": "COM9",
                                  "RFPULSE_PORT": "COM13", "IG_PORT": "COM12"}) == []
    assert serial_port_conflicts({"RFPULSE_PORT": " com12", "IG_PORT": "COM12 ", "DC_PORT": ""}) == [
        "시리얼 포트 중복: COM12 = RFPULSE_PORT, IG_PORT — config_user.json 확인"]
    assert serial_port_conflicts({"A": "", "B": None, "C": " "}) == []


def _warnings(w, monkeypatch, **ports):
    logs = []
    monkeypatch.setattr(MAIN, "log_message_to_monitor", lambda lv, m: logs.append((lv, m)))
    for k, v in ports.items():
        monkeypatch.setattr(MAIN, k, v)
    w._warn_serial_port_conflicts()
    return logs


def test_startup_warning_one_line_on_conflict(win, monkeypatch):
    logs = _warnings(win, monkeypatch, MFC_PORT="COM10", DC_PORT="COM11", PLC_PORT="COM9",
                     RFPULSE_PORT="COM12", IG_ENABLED=True, IG_PORT="com12")
    assert logs == [("경고", "시리얼 포트 중복: COM12 = RFPULSE_PORT, IG_PORT — config_user.json 확인")]


def test_startup_no_warning_without_conflict(win, monkeypatch):
    assert _warnings(win, monkeypatch, MFC_PORT="COM10", DC_PORT="COM11", PLC_PORT="COM9",
                     RFPULSE_PORT="COM13", IG_ENABLED=True, IG_PORT="COM12") == []
    # 이온 게이지가 꺼져 있으면 IG_PORT 는 보지 않는다
    assert _warnings(win, monkeypatch, MFC_PORT="COM10", DC_PORT="COM11", PLC_PORT="COM9",
                     RFPULSE_PORT="COM12", IG_ENABLED=False, IG_PORT="COM12") == []
