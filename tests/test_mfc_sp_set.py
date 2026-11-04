# -*- coding: utf-8 -*-
"""MFC(GPC3000) 세트포인트 값 설정 SP1·SP2·SP3·SP5 — 공통 경로(쓰기 "Sn 값" → 읽기 → "Sn" 접두어·값 확인).

GPC3000 RS-232 매뉴얼: 쓰기 S1~S5 "Sn 값"(% F.S.), 선택 D1~D5, 읽기 SP1~SP4 = R1~R4 · SP5 = R10(R5 는 압력 읽기),
응답 "Sn+값", R7 응답 "MXYZ" 의 X = 선택된 세트포인트. 시리얼 대신 enqueue 를 가짜로 바꿔 읽기 응답을 차례로 준다.
"""
import pytest

import device.MFC as MFCM
import lib.config as CFG


@pytest.fixture
def mfc(qapp, monkeypatch):
    c = MFCM.MFCController()
    sent, replies, ok, fail, msgs, delays = [], {}, [], [], [], []

    def fake_enqueue(cmd_str, on_reply, timeout_ms=0, gap_ms=0, tag="", retries_left=5, allow_no_reply=False):
        cmd = cmd_str.strip()
        sent.append(cmd)
        q = replies.get(cmd)
        on_reply(q.pop(0) if q else None)
    monkeypatch.setattr(c, "enqueue", fake_enqueue)

    def fake_single_shot(ms, fn):
        delays.append(ms)
        fn()
    monkeypatch.setattr(MFCM.QTimer, "singleShot", staticmethod(fake_single_shot))
    c.command_confirmed.connect(lambda k: ok.append(k))
    c.command_failed.connect(lambda k, why: fail.append((k, why)))
    c.status_message.connect(lambda lv, m: msgs.append((lv, m)))
    c.t_sent, c.t_replies, c.t_ok, c.t_fail, c.t_msgs, c.t_delays = sent, replies, ok, fail, msgs, delays
    yield c
    c.deleteLater()


# (e) 명령 표 — SP5 읽기는 R10(R5 는 압력)
def test_commands_table():
    M = CFG.MFC_COMMANDS
    assert M["SP5_SET"](value="5.00") == "S5 5.00" and M["SP5_ON"] == "D5"
    assert (M["READ_SP1_VALUE"], M["READ_SP2_VALUE"], M["READ_SP3_VALUE"], M["READ_SP5_VALUE"]) == ("R1", "R2", "R3", "R10")
    assert M["READ_PRESSURE"] == "R5" and M["SP4_ON"] == "D4"
    assert CFG.MFC_SP_VERIFY_TOL == 0.01 and not hasattr(CFG, "MFC_SP1_VERIFY_TOL")
    assert MFCM.SP_SET_COMMANDS["SP5_SET"] == (5, "SP5_SET", "READ_SP5_VALUE")
    assert "SP4_SET" not in MFCM.SP_SET_COMMANDS


# (a) SP2·SP3·SP5 정상 검증
@pytest.mark.parametrize("cmd,n,rd,ui,hw", [("SP2_SET", 2, "R2", 5.0, "0.50"), ("SP3_SET", 3, "R3", 15.0, "1.50"),
                                            ("SP5_SET", 5, "R10", 50.0, "5.00"), ("SP1_SET", 1, "R1", 2.0, "0.20")])
def test_set_verified(mfc, cmd, n, rd, ui, hw):
    mfc.t_replies[rd] = [f"S{n}+{hw}"]
    mfc.handle_command(cmd, {"value": ui})
    assert mfc.t_sent == [f"S{n} {hw}", rd]
    assert mfc.t_ok == [cmd] and mfc.t_fail == []
    assert any(f"[{cmd} 검증 1/5] {rd} 응답='S{n}+{hw}'" in m for _, m in mfc.t_msgs)   # 원본 응답을 로그로


def test_tolerance_and_whitespace(mfc):
    mfc.t_replies["R10"] = ["  S5+5.01 \r\n"]                        # 공백 무시, 차이 0.01 = 허용오차 끝(통과)
    mfc.handle_command("SP5_SET", {"value": 50.0})
    assert mfc.t_ok == ["SP5_SET"]


# (b) 값 불일치 → 재전송 후 통과 / 응답 없음 → 다시 읽기만
def test_mismatch_resends_then_ok(mfc):
    mfc.t_replies["R3"] = ["S3+1.20", "S3+1.50"]
    mfc.handle_command("SP3_SET", {"value": 15.0})
    assert mfc.t_sent == ["S3 1.50", "R3", "S3 1.50", "R3"]
    assert mfc.t_ok == ["SP3_SET"] and mfc.t_delays == [CFG.MFC_DELAY_MS]


def test_no_reply_rereads_without_resend(mfc):
    mfc.t_replies["R2"] = [None, "", "S2+0.50"]
    mfc.handle_command("SP2_SET", {"value": 5.0})
    assert mfc.t_sent == ["S2 0.50", "R2", "R2", "R2"] and mfc.t_ok == ["SP2_SET"]


def test_mismatch_beyond_tol_is_not_ok(mfc):
    mfc.t_replies["R10"] = ["S5+5.02"] * 5                            # 0.02 > 0.01
    mfc.handle_command("SP5_SET", {"value": 50.0})
    assert mfc.t_ok == [] and len(mfc.t_fail) == 1


# (c) 5회 실패 → command_failed(기대값·마지막 원본 응답)
def test_five_failures_command_failed(mfc):
    mfc.t_replies["R2"] = ["S2+0.40"] * 3 + [None, "S2+0.41"]
    mfc.handle_command("SP2_SET", {"value": 5.0})
    assert mfc.t_ok == [] and [k for k, _ in mfc.t_fail] == ["SP2_SET"]
    why = mfc.t_fail[0][1]
    assert "S2 0.50" in why and "'S2+0.41'" in why and "5회" in why
    assert mfc.t_sent.count("R2") == 5 and mfc.t_sent.count("S2 0.50") == 1 + 3      # 첫 전송 + 값 불일치 3번 재전송


# (d) 접두어 다른 응답은 불일치(다른 명령의 응답) — 값이 맞아 보여도 통과하지 않는다
@pytest.mark.parametrize("reply", ["P+0.20", "S1+0.20", "+0.20", "0.20", "M110"])
def test_wrong_prefix_not_ok(mfc, reply):
    mfc.t_replies["R2"] = [reply] * 4 + ["S2+0.20"]
    mfc.handle_command("SP2_SET", {"value": 2.0})
    assert mfc.t_ok == ["SP2_SET"] and mfc.t_sent.count("R2") == 5 and mfc.t_sent.count("S2 0.20") == 1


def test_wrong_prefix_only_fails(mfc):
    mfc.t_replies["R1"] = ["P+0.20"] * 5
    mfc.handle_command("SP1_SET", {"value": 2.0})
    assert mfc.t_ok == [] and mfc.t_fail and "'P+0.20'" in mfc.t_fail[0][1]


def test_pressure_parser_unchanged(mfc):
    """압력 읽기(R5) 등 다른 곳의 _parse_pressure_value 는 접두어를 보지 않는다(그대로)."""
    assert mfc._parse_pressure_value("P+0.20") == 0.20 and mfc._parse_pressure_value("0.33") == 0.33
    assert mfc._parse_sp_reply("P+0.20", 2) is None and mfc._parse_sp_reply(" S2+0.20 ", 2) == 0.20


# (f) last_pressure_setpoint 는 SP1_SET 만
def test_only_sp1_sets_last_pressure_setpoint(mfc):
    mfc.last_pressure_setpoint = 2.0
    mfc.pressure_error_count = 3
    for cmd, n, rd, ui, hw in (("SP2_SET", 2, "R2", 5.0, "0.50"), ("SP3_SET", 3, "R3", 15.0, "1.50"),
                               ("SP5_SET", 5, "R10", 50.0, "5.00")):
        mfc.t_replies[rd] = [f"S{n}+{hw}"]
        mfc.handle_command(cmd, {"value": ui})
        assert mfc.last_pressure_setpoint == 2.0 and mfc.pressure_error_count == 3
    mfc.t_replies["R1"] = ["S1+0.80"]
    mfc.handle_command("SP1_SET", {"value": 8.0})
    assert mfc.last_pressure_setpoint == 8.0 and mfc.pressure_error_count == 0


# SPn_ON — R7 두 번째 글자(SP5 추가, SP1~4 그대로)
@pytest.mark.parametrize("cmd,ch", [("SP1_ON", "1"), ("SP4_ON", "4"), ("SP5_ON", "5")])
def test_sp_on_r7(mfc, cmd, ch):
    mfc.t_replies["R7"] = [f"M{ch}00"]
    mfc.handle_command(cmd, {})
    assert mfc.t_sent == [CFG.MFC_COMMANDS[cmd], "R7"] and mfc.t_ok == [cmd]


def test_sp5_on_wrong_select_fails(mfc):
    mfc.t_replies["R7"] = ["M400"] * 5
    mfc.handle_command("SP5_ON", {})
    assert mfc.t_ok == [] and mfc.t_fail == [("SP5_ON", "SP5_ON 상태 확인 실패")]


# 공정 스텝 — WP > 50 이면 SP4 → SP5(50) → SP1, 50 이하면 예전과 같다
def _steps(wp):
    from unittest.mock import MagicMock
    import controller.process_controller as PC
    c = PC.SputterProcessController(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    c._is_connected = lambda obj: True
    c._invoke_self = lambda name: None
    c._next_step = lambda: None
    c.start_process_flow(dict(process_note="t", use_dc=True, dc_power=100.0, sp1_set=float(wp), shutter_delay=5.0,
                              process_time=10.0, use_g1=True, use_heater=False, ar_flow=20.0))
    out = [(s.message, s.params) for s in c._steps]
    c.deleteLater()
    return out


def test_process_sp5_stage(qapp):
    s60 = _steps(60)
    cmds = [p[0] for _, p in s60 if p]
    i4, i5, i1 = cmds.index("SP4_ON"), cmds.index("SP5_SET"), cmds.index("SP1_SET")
    assert i4 < i5 < i1 and cmds[i5:i5 + 3] == ["SP5_SET", "SP5_ON", "WAIT_PRESSURE"] and i1 == i5 + 3
    wait = [p for _, p in s60 if p and p[0] == "WAIT_PRESSURE"][0][1]
    assert wait["target"] == 50.0 and wait["timeout_sec"] == 180.0 and wait["fail_on_timeout"] is False
    assert "SP3_SET" not in cmds and "SP2_SET" not in cmds
    assert any(m.startswith("Stage0(SP5): SP5_SET=50.00 설정") for m, _ in s60)
    for wp in (2, 8, 20, 50):
        assert "SP5_SET" not in [p[0] for _, p in _steps(wp) if p]
    # SP5 3스텝을 빼면 WP 60 의 목록은 SP5 가 없던 때와 같은 모양(SP4 → … → POWER_WAIT → SP1)
    rest = [p[0] for _, p in s60 if p and not p[0].startswith("SP5_")
            and not (p[0] == "WAIT_PRESSURE" and p[1]["target"] == 50.0)]
    assert rest == [p[0] for _, p in _steps(20) if p]
