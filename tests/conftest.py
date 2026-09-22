# tests/conftest.py
# -*- coding: utf-8 -*-
"""공통 픽스처. 네트워크(구글챗·ERP)는 전부 차단하고, Qt 는 offscreen 으로 띄운다."""
import os
import sys
import urllib.request

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

# ── HTTP 차단: 테스트가 실제 챗/ERP 로 무엇도 보내면 안 된다 ──
_BLOCKED = []


def _blocked_urlopen(req, *a, **k):
    url = getattr(req, "full_url", str(req))
    _BLOCKED.append(url)
    raise OSError(f"blocked by tests: {url}")


urllib.request.urlopen = _blocked_urlopen

import pytest  # noqa: E402


# ── 로그 격리: 테스트 중 어떤 로그도 NAS 운영 폴더로 나가지 않는다 ──
#  2026-09-22 16:33 실기 PC 에서 테스트 중 \VanaM_NAS\...\CHK\heater 에 HEATER_20260922_1633xx.csv/.txt 가 생겼다
#  (conftest.make_heater_st 값 TC1 598.9 / TC2 893.4 / OT 650.0). 실제 챔버는 403°C 승온 중이었다.
_NAS_ATTRS = ("NAS_LOG_DIR", "NAS_PROCESS_LOG_DIR", "NAS_HEATER_LOG_DIR", "NAS_PLC_LOG_DIR", "NAS_COMM_LOG_DIR")


@pytest.fixture(scope="session", autouse=True)
def isolate_logs_and_ports(tmp_path_factory):
    """autouse·session: lib.logger 의 NAS 경로 5개 + 이름으로 바인딩한 모듈(lib.heater_logger)을 tmp 아래로,
    시리얼 포트(COM9 minimalmodbus / COM10·11·13 QSerialPort)도 열지 못하게 막는다. 로컬 폴백(./Logs) 로직은 그대로."""
    from pathlib import Path
    import lib.logger as LG
    import lib.heater_logger as HL
    root = Path(tmp_path_factory.mktemp("chk_logs"))
    mp = pytest.MonkeyPatch()
    for name in _NAS_ATTRS:
        sub = root if name == "NAS_LOG_DIR" else root / name.replace("NAS_", "").replace("_LOG_DIR", "").lower()
        mp.setattr(LG, name, sub)
    mp.setattr(HL, "NAS_HEATER_LOG_DIR", LG.NAS_HEATER_LOG_DIR)
    # ChK_log.csv(공정 요약, lib.config.CHK_CSV_PATH → lib.logger 가 이름으로 바인딩) 도 NAS 다
    import lib.config as CFG
    mp.setattr(CFG, "CHK_CSV_PATH", str(root / "ChK_log.csv"))
    if hasattr(LG, "CHK_CSV_PATH"):
        mp.setattr(LG, "CHK_CSV_PATH", str(root / "ChK_log.csv"))
    os.chdir(root)                                   # ./Logs, ./log.txt 폴백도 tmp 아래로
    # 시리얼: 실기 PC 에서 테스트가 실제 포트를 열면 안 된다(테스트가 필요하면 자기 monkeypatch 로 덮어쓴다)
    import minimalmodbus
    from PyQt6.QtSerialPort import QSerialPortInfo

    def _no_instrument(*a, **k):
        raise OSError("tests: 실제 시리얼 포트 열기 차단")
    mp.setattr(minimalmodbus, "Instrument", _no_instrument)
    mp.setattr(QSerialPortInfo, "availablePorts", staticmethod(lambda: []))
    yield root
    mp.undo()
    os.chdir(ROOT)


@pytest.fixture(scope="session")
def qapp():
    from PyQt6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication(sys.argv)
    yield app


def make_heater_st(run=False, fault=False, itl=True, pv=275.0, sv=275.0, ok=True, **kw):
    st = dict(ok=ok, pv=pv, sv=sv, sv_limit=630.0, cur_sv=sv, pid_err=0, mv=400, mv_pct=0.0,
              mv_limit=1200, limit_pct=100.0, sv_ramp=sv, ramp_rate=12, holdback=10.0,
              ot_limit=650.0, est_current=0.0, run=run, itl=itl, fault=fault, ot=False,
              tc_err=False, wd_err=False, at_done=False, pid_run=run, output_dead=False,
              pv2=None, pv_ctrl=pv, ot2_limit=1150.0, sv2=0.0, sv2_max=1100.0,
              pv_sel=False, pv_sel_eff=False)
    st.update(kw)
    return st
