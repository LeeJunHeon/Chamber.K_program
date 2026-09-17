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


def test_T13_policy_success_not_at_port_open():
    """정책의 on_success 는 포트 열림 지점(start_polling/_reconnect_attempt/connect_dcpower_device/_open_port)
    에 없어야 하고, 우회 경로에서 _try_reconnect() 를 직접 부르지 않아야 한다."""
    import ast
    checks = {
        "device/PLC.py": ("start_polling", "_reconnect_attempt", "_on_link_up"),
        "device/DCpower.py": ("connect_dcpower_device",),
        "device/RFpulse.py": ("_open_port", "_check_comm_budget", "_outage_probe_tick"),
        "device/MFC.py": ("_open_port",),
    }
    for rel, funcs in checks.items():
        src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
        tree = ast.parse(src)
        bodies = {n.name: ast.get_source_segment(src, n) for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name in funcs}
        assert set(bodies) == set(funcs), (rel, set(funcs) - set(bodies))
        for name, body in bodies.items():
            assert "on_success(" not in body, f"{rel}:{name} 에 on_success"
            if name in ("_check_comm_budget", "_outage_probe_tick"):
                assert "self._try_reconnect()" not in body, f"{rel}:{name} 가 _try_reconnect 직접 호출"
    # 응답 지점에는 있어야 하고, MFC 재시도 경로는 스케줄러를 거친다
    for rel, fn in (("device/PLC.py", "_mb"), ("device/DCpower.py", "_comm_ok"),
                    ("device/RFpulse.py", "_comm_ok"), ("device/MFC.py", "_finish_command")):
        src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
        body = src.split(f"def {fn}(")[1].split("\n    def ")[0]
        assert "self._policy.on_success()" in body, f"{rel}:{fn} 에 on_success 없음"
        if fn == "_finish_command":
            assert "self._try_reconnect()" not in body
    # _try_reconnect 는 정의 + _watch_connection 의 singleShot 예약, 이 두 곳뿐이다
    for rel in ("device/RFpulse.py", "device/MFC.py"):
        hits = _grep(r"_try_reconnect", [os.path.join(ROOT, rel)])
        assert len(hits) == 2, hits


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


def test_T13_no_jig_wording_in_new_code():
    """TC1/TC2 작업(2026-09-17)으로 새로 만든/고친 코드·문구에 '지그' 를 쓰지 않는다.
    허용: config_user.json 의 홀드백 이력 주석 1건(기존)."""
    files = PY_FILES + [os.path.join(ROOT, "config_user.json")]
    hits = _grep(r"지그", files)
    assert all(h.startswith("config_user.json:") and "홀드백" in h for h in hits), hits
    assert _grep(r"지그", [os.path.join(ROOT, "controller", "heater_hold.py"),
                          os.path.join(ROOT, "main.py"), os.path.join(ROOT, "UI.py"),
                          os.path.join(ROOT, "device", "PLC.py"), os.path.join(ROOT, "lib", "config.py"),
                          os.path.join(ROOT, "lib", "heater_logger.py")]) == []
