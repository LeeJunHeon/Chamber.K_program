# -*- coding: utf-8 -*-
"""개발 모드 스위치(lib/paths.py).

DEV_MODE 없음/False → 운영 경로·ERP·구글챗 그대로(장비 노트북 동작 불변).
DEV_MODE True       → 기록은 전부 저장소 _dev_logs 아래, ERP·구글챗 비활성.

모듈 상수까지의 배선은 이 노트북의 실제 config_local 과 무관하게 보려고, 가짜 lib.config_local 을
심은 별도 파이썬 프로세스에서 확인한다(conftest 의 경로 격리를 건드리지 않는다).
"""
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from lib import paths as P

ROOT = Path(__file__).resolve().parent.parent

# 리팩토링 전(ops/2026-09-29) 코드에 박혀 있던 운영 경로 그대로
OPS_ROOT = r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK"
OPS_CHK_CSV = r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database\ChK_log.csv"

_FAKE_CFG = dict(ERP_INGEST_URL=" http://erp.example/ingest ", ERP_INGEST_TOKEN="tok",
                 CHAT_WEBHOOK_URL="https://chat.example/a", CHAT_WEBHOOK_PLC_DISCONNECT_URL="https://chat.example/b")


@pytest.mark.parametrize("cfg,expected", [
    (None, False),
    (SimpleNamespace(), False),
    (SimpleNamespace(DEV_MODE=False), False),
    (SimpleNamespace(DEV_MODE="True"), False),     # 정확히 True 일 때만
    (SimpleNamespace(DEV_MODE=True), True),
])
def test_read_dev_mode(cfg, expected):
    assert P.read_dev_mode(cfg) is expected


def test_ops_paths_unchanged():
    p = P.resolve_paths(False)
    assert str(p.nas_log_dir) == OPS_ROOT
    assert str(p.process_dir) == OPS_ROOT + r"\process"
    assert str(p.heater_dir) == OPS_ROOT + r"\heater"
    assert str(p.plc_dir) == OPS_ROOT + r"\plc"
    assert str(p.comm_dir) == OPS_ROOT + r"\comm"
    assert p.chk_csv_path == OPS_CHK_CSV


def test_dev_paths_all_under_dev_logs():
    dev = (ROOT / "_dev_logs").resolve()
    for v in P.resolve_paths(True):
        s = str(v)
        assert "VanaM_NAS" not in s
        assert Path(s).resolve().is_relative_to(dev), s


def test_erp_and_chat_ops_vs_dev():
    cfg = SimpleNamespace(**_FAKE_CFG)
    assert P.erp_settings(cfg, False) == ("http://erp.example/ingest", "tok")
    assert P.chat_webhook(cfg, False) == "https://chat.example/a"
    assert P.chat_webhook(cfg, False, "CHAT_WEBHOOK_PLC_DISCONNECT_URL") == "https://chat.example/b"
    assert P.erp_settings(cfg, True) == ("", "")
    assert P.chat_webhook(cfg, True) == ""
    assert P.chat_webhook(cfg, True, "CHAT_WEBHOOK_PLC_DISCONNECT_URL") == ""
    assert P.erp_settings(None, False) == ("", "")


_PROBE = r"""
import json, sys, types, urllib.request
def _no_net(*a, **k):
    raise OSError("blocked")
urllib.request.urlopen = _no_net
fake = types.ModuleType("lib.config_local")
fake.__dict__.update(json.loads(sys.argv[1]))
import lib
sys.modules["lib.config_local"] = fake
lib.config_local = fake
import lib.paths as P, lib.logger as LG, lib.config as CFG, lib.heater_logger as HL
from controller.chat_notifier import ChatNotifier
from reporter import ErpReporter
from lib import config_local as cfgl
cn = ChatNotifier(P.chat_webhook(cfgl, P.DEV_MODE))
erp = ErpReporter(*P.erp_settings(cfgl, P.DEV_MODE))
print(json.dumps(dict(
    dev=P.DEV_MODE,
    paths=[str(LG.NAS_LOG_DIR), str(LG.NAS_PROCESS_LOG_DIR), str(LG.NAS_HEATER_LOG_DIR),
           str(LG.NAS_PLC_LOG_DIR), str(LG.NAS_COMM_LOG_DIR), str(HL.NAS_HEATER_LOG_DIR),
           CFG.CHK_CSV_PATH, LG.CHK_CSV_PATH],
    chat=[cn.webhook_default, cn.webhook_plc_disconnect],
    erp=erp.enabled,
)))
"""


def _probe(cfg: dict) -> dict:
    env = dict(os.environ, QT_QPA_PLATFORM="offscreen", PYTHONIOENCODING="utf-8")
    out = subprocess.run([sys.executable, "-c", _PROBE, json.dumps(cfg)], cwd=str(ROOT), env=env,
                         capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("extra", [{}, {"DEV_MODE": False}])
def test_wiring_ops_when_dev_mode_off(extra):
    r = _probe(dict(_FAKE_CFG, **extra))
    assert r["dev"] is False
    assert r["paths"] == [OPS_ROOT, OPS_ROOT + r"\process", OPS_ROOT + r"\heater", OPS_ROOT + r"\plc",
                          OPS_ROOT + r"\comm", OPS_ROOT + r"\heater", OPS_CHK_CSV, OPS_CHK_CSV]
    assert r["chat"] == ["https://chat.example/a", "https://chat.example/b"]
    assert r["erp"] is True


def test_wiring_dev_when_dev_mode_on():
    r = _probe(dict(_FAKE_CFG, DEV_MODE=True))
    assert r["dev"] is True
    dev = (ROOT / "_dev_logs").resolve()
    for s in r["paths"]:
        assert "VanaM_NAS" not in s
        assert Path(s).resolve().is_relative_to(dev), s
    assert r["paths"][-1].endswith("ChK_log.csv")
    assert r["chat"] == ["", ""]
    assert r["erp"] is False


def test_chat_notifier_ignores_explicit_url_in_dev_mode(monkeypatch):
    import controller.chat_notifier as CN
    monkeypatch.setattr(CN, "DEV_MODE", True)
    monkeypatch.setattr(CN, "_cfg", SimpleNamespace(**_FAKE_CFG))
    cn = CN.ChatNotifier("https://chat.example/explicit")
    assert cn.webhook_default == "" and cn.webhook_plc_disconnect == ""
    assert cn._resolve_webhook() is None
