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
