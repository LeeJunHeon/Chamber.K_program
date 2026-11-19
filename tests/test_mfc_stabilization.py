# -*- coding: utf-8 -*-
"""MFC FLOW_ON 유량 안정화 — 시도 횟수 대신 경과 시간(MFC_STABILIZATION_TIMEOUT_S)으로 시간 초과를 판정한다.

R60 읽기(_read_flow_all_async)와 time.monotonic 을 가짜로 바꿔 _check_flow_stabilization 을 직접 돌린다.
"""
import pytest

import device.MFC as MFCM
import lib.config as CFG


class FakeTimer:
    def __init__(self):
        self.active = False

    def start(self):
        self.active = True

    def stop(self):
        self.active = False

    def isActive(self):
        return self.active


@pytest.fixture
def mfc(qapp, monkeypatch):
    c = MFCM.MFCController()
    clock = {"t": 1000.0}
    flows = {"vals": [0.0, 0.0, 0.0, 0.0]}
    ok, fail = [], []
    monkeypatch.setattr(MFCM.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(c, "_read_flow_all_async", lambda on_done, tag="": on_done(list(flows["vals"])))
    c.stabilization_timer = FakeTimer()
    c.command_confirmed.connect(lambda k: ok.append(k))
    c.command_failed.connect(lambda k, why: fail.append((k, why)))

    def start(ch=1, target=10.0):                       # _set_onoff_mask_and_verify 의 안정화 시작과 같은 상태
        c._stabilizing_channel = ch
        c._stabilizing_target = target
        c.stabilization_attempts = 0
        c._stabilizing_started = MFCM.time.monotonic()
        c.stabilization_timer.start()

    c.t_clock, c.t_flows, c.t_ok, c.t_fail, c.t_start = clock, flows, ok, fail, start
    yield c
    c.deleteLater()


def test_default_is_100s():
    assert CFG.MFC_STABILIZATION_TIMEOUT_S == 100 == MFCM.MFC_STABILIZATION_TIMEOUT_S


def test_no_failure_before_limit(mfc, monkeypatch):
    monkeypatch.setattr(MFCM, "MFC_STABILIZATION_TIMEOUT_S", 100)
    mfc.t_start()
    for _ in range(99):                                  # 옛 한도(30회)를 훨씬 넘겨도 100초 전에는 실패하지 않는다
        mfc.t_clock["t"] += 1.0
        mfc._check_flow_stabilization()
    assert mfc.t_fail == [] and mfc.t_ok == []
    assert mfc.stabilization_timer.isActive() and mfc._stabilizing_channel == 1
    assert mfc.stabilization_attempts == 99


def test_fails_after_limit(mfc, monkeypatch):
    monkeypatch.setattr(MFCM, "MFC_STABILIZATION_TIMEOUT_S", 100)
    mfc.t_start()
    mfc.t_clock["t"] += 99.9
    mfc._check_flow_stabilization()
    assert mfc.t_fail == []
    mfc.t_clock["t"] += 0.1                              # 정확히 100초 경과
    mfc._check_flow_stabilization()
    assert mfc.t_fail == [("FLOW_ON", "유량 안정화 시간 초과(100초)")]
    assert not mfc.stabilization_timer.isActive()
    assert mfc._stabilizing_channel is None and mfc._stabilizing_target == 0.0 and mfc._stabilizing_started is None


def test_judged_by_time_not_attempts(mfc, monkeypatch):
    """확인 주기가 밀려 시도가 적어도 시간이 지나면 실패한다."""
    monkeypatch.setattr(MFCM, "MFC_STABILIZATION_TIMEOUT_S", 20)
    mfc.t_start()
    mfc.t_clock["t"] += 25.0
    mfc._check_flow_stabilization()
    assert mfc.stabilization_attempts == 1
    assert mfc.t_fail == [("FLOW_ON", "유량 안정화 시간 초과(20초)")]


def test_confirmed_when_target_reached(mfc, monkeypatch):
    monkeypatch.setattr(MFCM, "MFC_STABILIZATION_TIMEOUT_S", 100)
    mfc.t_start(ch=2, target=10.0)
    mfc.t_clock["t"] += 1.0
    mfc._check_flow_stabilization()                      # 아직 0
    mfc.t_flows["vals"] = [0.0, 10.5, 0.0, 0.0]          # ±10% 안
    mfc.t_clock["t"] += 1.0
    mfc._check_flow_stabilization()
    assert mfc.t_ok == ["FLOW_ON"] and mfc.t_fail == []
    assert not mfc.stabilization_timer.isActive() and mfc._stabilizing_started is None


def test_flow_off_clears_started(mfc, monkeypatch):
    mfc.t_start(ch=1)
    monkeypatch.setattr(mfc, "enqueue", lambda cmd, cb, **kw: cb("R69 1000") if cmd.strip() == "R69" else None)
    monkeypatch.setattr(mfc, "_set_onoff_mask_and_verify", lambda **kw: None)
    mfc._apply_flow_onoff_with_L0(1, False)
    assert mfc._stabilizing_channel is None and mfc._stabilizing_started is None
    assert not mfc.stabilization_timer.isActive()


@pytest.mark.parametrize("raw,expect", [(100, 100), (10, 10), (600, 600), (45.5, 45.5),
                                        (9, 100), (601, 100), (0, 100), (-5, 100),
                                        ("60", 100), (None, 100), (True, 100), ([], 100)])
def test_config_validation(monkeypatch, raw, expect):
    monkeypatch.setattr(CFG, "MFC_STABILIZATION_TIMEOUT_S", raw)
    CFG._validate_mfc_stabilization_timeout()
    assert CFG.MFC_STABILIZATION_TIMEOUT_S == expect
