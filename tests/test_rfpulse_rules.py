# -*- coding: utf-8 -*-
"""T54~T56 — RF 펄스 최소 ON/OFF 16 µs 규칙: 규칙 함수 / main 시작 전 검사 / RFpulse 리드백 경고."""
import pytest

from lib.config import rfpulse_pulse_edge_violation as viol
import device.RFpulse as RFM


@pytest.mark.parametrize("f,d,bad", [
    (20000, 80, True), (20000, 50, False), (12500, 80, False), (30000, 50, False),
    (30000, 40, True), (1, 50, False), (30000, 48, False), (30000, 47, True),
])
def test_T54_edge_rule(f, d, bad):
    r = viol(f, d)
    assert (r is not None) is bad, (f, d, r)


def test_T54_edge_rule_wording():
    r = viol(20000, 80)
    assert r == "20kHz·80% → ON 40.0µs / OFF 10.0µs, 최소 16µs 미만 — 20kHz 에서 허용 듀티 32~68%, 80% 를 쓰려면 12.5kHz 이하"
    assert "ON 13.3µs" in viol(30000, 40)


def test_T55_main_start_check_only_when_both_given():
    import main as MAIN
    chk = MAIN.MainDialog._check_rfpulse_pulse_range
    with pytest.raises(ValueError, match=r"\[수동 시작\] RF Pulse 설정 20kHz·80% .*32~68%.*12\.5kHz"):
        chk(20.0, 80, "수동 시작")
    chk(20.0, None, "수동 시작"); chk(None, 80, "수동 시작"); chk(20.0, 50, "수동 시작")
    with pytest.raises(ValueError, match="장비 범위"):
        chk(40.0, 50, "수동 시작")


def _readback(qapp, monkeypatch, freq_hz, duty):
    c = RFM.RFPulseController()
    c._req_freq_hz = None; c._req_duty = None; c._stop_requested = False
    resp = {RFM.CMD_REPORT_PULSE_FREQ: bytes([freq_hz & 0xFF, (freq_hz >> 8) & 0xFF, (freq_hz >> 16) & 0xFF]),
            RFM.CMD_REPORT_PULSE_DUTY: bytes([duty & 0xFF, (duty >> 8) & 0xFF])}
    monkeypatch.setattr(c, "_enqueue_query", lambda cmd, data=b"", **kw: kw["callback"](resp[cmd]))
    msgs = []; warns = []
    c.status_message.connect(lambda l, m: msgs.append((l, m)))
    c.pulse_config_warning.connect(lambda m: warns.append(m))
    c._readback_pulse_config()
    return msgs, warns


def test_T56_readback_warns_on_violation_only(qapp, monkeypatch):
    msgs, warns = _readback(qapp, monkeypatch, 20000, 80)
    assert ("RFPulse", "펄스 설정 리드백: 20 kHz · 80%") in msgs
    w = [m for l, m in msgs if l == "RFPulse(경고)"]
    assert len(w) == 1 and w[0].startswith("장비 펄스 설정 20kHz·80% → ON 40.0µs / OFF 10.0µs") and w[0].endswith("— 공정은 계속")
    assert len(warns) == 1 and "32~68%" in warns[0]
    assert not any(l == "재시작" for l, _ in msgs)                 # 요청값이 없으니 검증 불일치도 없다
    msgs, warns = _readback(qapp, monkeypatch, 20000, 50)
    assert warns == [] and not any(l == "RFPulse(경고)" for l, _ in msgs)


def test_T56_readback_verify_mismatch_still_restarts(qapp, monkeypatch):
    c = RFM.RFPulseController()
    c._req_freq_hz = 20000; c._req_duty = 50; c._stop_requested = False
    resp = {RFM.CMD_REPORT_PULSE_FREQ: bytes([0x20, 0x4E, 0]), RFM.CMD_REPORT_PULSE_DUTY: bytes([80, 0])}
    monkeypatch.setattr(c, "_enqueue_query", lambda cmd, data=b"", **kw: kw["callback"](resp[cmd]))
    monkeypatch.setattr(c, "stop_process", lambda: None)
    msgs = []
    c.status_message.connect(lambda l, m: msgs.append((l, m)))
    c._readback_pulse_config()
    assert any(l == "재시작" and "듀티" in m for l, m in msgs)
