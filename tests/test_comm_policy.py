# -*- coding: utf-8 -*-
"""T1 — lib/comm_policy.ReconnectPolicy (now 주입)."""
from lib.comm_policy import ReconnectPolicy


def _policy(clock):
    return ReconnectPolicy(start_ms=1000, max_ms=60000, long_outage_sec=600,
                           probe_ms=60000, log_every_sec=600, now=lambda: clock["t"])


def test_backoff_sequence_and_cap():
    clock = {"t": 0.0}
    p = _policy(clock)
    delays = [p.on_failure() for _ in range(8)]
    assert delays == [1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000]


def test_long_outage_alert_once_probe_and_log_cadence():
    clock = {"t": 0.0}
    p = _policy(clock)
    p.on_failure()
    assert not p.is_long_outage()
    assert p.take_long_outage_alert() is False
    clock["t"] = 600.0
    assert p.is_long_outage()
    assert p.take_long_outage_alert() is True
    assert p.take_long_outage_alert() is False          # 장기두절당 1회
    assert p.on_failure() == 60000                       # 이후 지연은 probe 고정
    assert p.on_failure() == 60000
    # should_log: 장기두절 진입 첫 호출 True, 600초 안에는 False, 600초 뒤 True
    assert p.should_log() is True
    clock["t"] = 900.0
    assert p.should_log() is False
    clock["t"] = 1200.0
    assert p.should_log() is True
    assert abs(p.outage_sec() - 1200.0) < 1e-9


def test_should_log_always_true_before_long_outage():
    clock = {"t": 0.0}
    p = _policy(clock)
    p.on_failure()
    assert all(p.should_log() for _ in range(5))


def test_success_resets_to_start():
    clock = {"t": 0.0}
    p = _policy(clock)
    for _ in range(5):
        p.on_failure()
    clock["t"] = 700.0
    assert p.take_long_outage_alert() is True
    p.on_success()
    assert not p.in_outage()
    assert p.outage_sec() == 0.0
    assert p.on_failure() == 1000                        # 다시 1000 부터
    clock["t"] = 700.0 + 600.0
    assert p.take_long_outage_alert() is True            # 새 두절이면 다시 1회 알림
