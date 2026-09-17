# -*- coding: utf-8 -*-
"""T13 정적 검사 / T14 줄바꿈."""
import glob
import json
import os
import py_compile
import re
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY_FILES = [f for f in glob.glob(os.path.join(ROOT, "**", "*.py"), recursive=True)
            if ".git" not in f and os.sep + "tests" + os.sep not in f]


def _grep(pattern, files):
    rx = re.compile(pattern)
    hits = []
    for f in files:
        with open(f, encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh, 1):
                if rx.search(line):
                    hits.append(f"{os.path.relpath(f, ROOT)}:{i}: {line.strip()[:80]}")
    return hits


def test_T13_py_compile_all():
    for f in PY_FILES:
        py_compile.compile(f, doraise=True)


def test_T13_removed_symbols_absent():
    files = PY_FILES + [os.path.join(ROOT, "config_user.json")]
    for pat in (r"_reopen_port", r"_reconnect_backoff_ms", r"PLC_COMM_REOPEN_SEC",
                r"DC_RECONNECT_BACKOFF", r"RFPULSE_RECONNECT_BACKOFF", r"MFC_RECONNECT_BACKOFF",
                r"_heater_chat_run_prev", r"_atm_run_prev", r"_heater_run_prev\b",
                r"plc_controller\._heater_last"):
        assert _grep(pat, files) == [], pat


def test_T13_button_setchecked_only_in_helper():
    hits = _grep(r"heater_onoff_button\.setChecked", [os.path.join(ROOT, "main.py")])
    assert len(hits) == 1, hits
    src = open(os.path.join(ROOT, "main.py"), encoding="utf-8").read()
    body = src.split("def _set_heater_button_view")[1].split("\n    def ")[0]
    assert "heater_onoff_button.setChecked" in body


def test_T13_config_json_loads():
    with open(os.path.join(ROOT, "config_user.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    for k in ("COMM_RECONNECT_START_MS", "COMM_RECONNECT_MAX_MS", "COMM_LONG_OUTAGE_SEC",
              "COMM_PROBE_MS", "COMM_OUTAGE_LOG_SEC", "HEATER_STALE_SEC"):
        assert k in cfg
    import lib.config_loader as cl
    assert cl.get("COMM_PROBE_MS", None) == 60000


def test_T14_line_endings_unchanged_vs_head():
    """git diff --stat 에 통째 변경(전 줄 바뀜) 파일이 없어야 한다 = CRLF/LF 전환 없음."""
    out = subprocess.run(["git", "diff", "--numstat", "HEAD~0", "--"], cwd=ROOT,
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        add, dele, name = line.split("\t", 2)
        if add == "-":
            continue
        with open(os.path.join(ROOT, name), "rb") as fh:
            total = fh.read().count(b"\n")
        assert int(dele) < max(1, total * 0.9), f"{name}: 통째 변경 의심 (-{dele}/{total})"
    # 수정 파일은 전부 LF 로 유지(저장소 규칙 .gitattributes eol=lf)
    for name in ("main.py", "device/PLC.py", "device/MFC.py", "device/DCpower.py",
                 "device/RFpulse.py", "lib/config.py", "config_user.json"):
        assert b"\r\n" not in open(os.path.join(ROOT, name), "rb").read(), name
