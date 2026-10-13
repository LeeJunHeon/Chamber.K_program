# -*- coding: utf-8 -*-
"""이온 게이지(Terranova 934) — 파서. 시험 고정값은 2026-10-13 장비 노트북 실측 응답 그대로."""
import pytest

from device.IonGauge import (
    IgParseError, parse_status, parse_pair, pressure_from_pair, setpoint_from_pair,
    split_lines, is_complete, looks_like_pressure_reply, fmt_torr, filament_text, filament_error_text,
)

EOL = b"\n\r"
F_REPLY = b"10      -6" + EOL
G_REPLY = b"0       -4" + EOL
H_REPLY = b"61      -4" + EOL
E_ITEMS = ["0", "1", "1", "1", "0", "0", "1", "1", "0", "0", "0", "0", "0", "0", "0", "0",
           "10      -2", "10      -4", "10      -8", "100", "100", "65      -1", "50      0",
           "9       -6", "0       -4", "63      -4"]


def e_reply(items=E_ITEMS, eol=EOL, trailer=True) -> bytes:
    return b"".join(s.encode() + eol for s in items) + (eol if trailer else b"")


E_REPLY = e_reply()


def test_measured_single_replies():
    assert parse_pair(F_REPLY.decode()) == (10, -6)
    assert pressure_from_pair("10      -6", "ion") == (pytest.approx(1.0e-5), "ok", "1.0E-5")
    assert pressure_from_pair("0       -4", "conv")[1:] == ("below", "하한 이하")
    v, st, txt = pressure_from_pair("61      -4", "conv")
    assert v == pytest.approx(6.1e-3) and st == "ok" and txt == "6.1E-3"
    for raw in (F_REPLY, G_REPLY, H_REPLY):
        assert looks_like_pressure_reply(raw) and is_complete(raw, 1)


def test_measured_status_reply():
    r = parse_status(E_REPLY, t=123.0)
    assert r.ion_torr == pytest.approx(9.0e-6) and r.ion_text == "9.0E-6" and r.ion_state == "ok"
    assert r.a_torr is None and r.a_state == "below" and r.a_text == "하한 이하"
    assert r.b_torr == pytest.approx(6.3e-3) and r.b_text == "6.3E-3"
    assert r.filament_on and r.emission_ok and not r.degas_on
    assert r.error_code == 0 and r.error_text == ""
    assert (r.relay_sp1_a, r.relay_sp2_b, r.relay_sp3_ion, r.relay_sp4_ion) == (True, True, True, False)
    assert r.sp_auto_filament == pytest.approx(1.0e-3)
    assert r.sp_ion1 == pytest.approx(1.0e-5) and r.sp_ion2 == pytest.approx(1.0e-9)
    assert r.sp_a == pytest.approx(6.5e-2) and r.sp_b == pytest.approx(0.50)
    assert r.gas_factor == pytest.approx(1.00) and r.sensitivity == pytest.approx(10.0)
    assert not r.sp_protect_off and not r.auto_filament_enabled and r.auto_filament_state == 0
    assert r.t == 123.0
    assert filament_text(r) == "필라멘트 켜짐"
    # 릴레이가 압력·셋포인트 관계와 맞는다: 이온 9E-6 < SP1 1E-5 → ON, > SP2 1E-9 → OFF / B 6.3E-3 < 0.5 → ON
    assert r.ion_torr < r.sp_ion1 and r.ion_torr > r.sp_ion2 and r.b_torr < r.sp_b


def test_status_complete_detection():
    assert is_complete(E_REPLY, 26)
    assert is_complete(e_reply(trailer=False), 26)
    assert not is_complete(E_REPLY[:-20], 26)                 # 26줄 전에 끊김
    no_eol = e_reply(trailer=False)[:-2]                      # 26번째 줄에 줄 끝이 아직 안 옴
    assert not is_complete(no_eol, 26)
    assert not is_complete(b"", 26)


def test_split_into_chunks_reassembles():
    """쪼개져 들어온 응답 — 이어 붙이면 같은 결과, 중간에는 미완료."""
    buf = bytearray()
    done_at = None
    for i in range(0, len(E_REPLY), 7):
        buf.extend(E_REPLY[i:i + 7])
        if done_at is None and is_complete(bytes(buf), 26):
            done_at = len(buf)
    assert done_at is not None and done_at >= len(e_reply(trailer=False)) - 7
    assert parse_status(bytes(buf)).ion_text == "9.0E-6"


@pytest.mark.parametrize("eol", [b"\r\n", b"\n", b"\r", b"\n\r"])
def test_line_ending_variants(eol):
    r = parse_status(e_reply(eol=eol))
    assert r.ion_text == "9.0E-6" and r.b_text == "6.3E-3"
    assert parse_pair(split_lines(b"9       -6" + eol)[0]) == (9, -6)


def test_special_values():
    assert pressure_from_pair("0", "ion")[1:] == ("off", "꺼짐")
    assert pressure_from_pair("0       -5", "ion")[1:] == ("off", "꺼짐")
    assert pressure_from_pair("-900", "conv") == (None, "disconnected", "미연결")
    assert pressure_from_pair("-900     0", "conv") == (None, "disconnected", "미연결")
    assert pressure_from_pair("-999", "conv") == (None, "unzeroed", "영점 미조정")
    assert pressure_from_pair("-5  -3", "conv")[1] == "invalid"
    items = list(E_ITEMS)
    items[23], items[24], items[25] = "0", "-900", "-999"
    r = parse_status(e_reply(items))
    assert (r.ion_torr, r.a_torr, r.b_torr) == (None, None, None)
    assert (r.ion_text, r.a_text, r.b_text) == ("꺼짐", "미연결", "영점 미조정")


def test_invalid_command_reply():
    with pytest.raises(IgParseError, match="999x"):
        parse_pair("999x")
    with pytest.raises(IgParseError, match="999x"):
        parse_status(b"999x" + EOL)
    assert is_complete(b"999x" + EOL, 26)                     # 오류 응답은 그 자체로 끝
    assert not looks_like_pressure_reply(b"999x" + EOL)


def test_fewer_than_26_items_and_bad_format():
    with pytest.raises(IgParseError, match="25/26"):
        parse_status(e_reply(E_ITEMS[:25]))
    bad = list(E_ITEMS); bad[4] = "x"
    with pytest.raises(IgParseError, match="5번"):
        parse_status(e_reply(bad))
    bad = list(E_ITEMS); bad[23] = "9.5 -6"
    with pytest.raises(IgParseError, match="형식"):
        parse_status(e_reply(bad))
    assert not looks_like_pressure_reply(E_REPLY)             # F 신원 확인에 E 응답이 오면 형식 다름
    assert not looks_like_pressure_reply(b"hello" + EOL)


def test_error_codes_and_texts():
    items = list(E_ITEMS); items[1] = "0"; items[4] = "7"
    r = parse_status(e_reply(items))
    assert r.error_code == 7 and "단선" in r.error_text
    assert filament_text(r) == "필라멘트 오류 7: 필라멘트 전류 없음(필라멘트·케이블 단선 가능)"
    assert filament_error_text(99) == "알 수 없는 필라멘트 오류 코드 99"
    items = list(E_ITEMS); items[1] = "0"
    assert filament_text(parse_status(e_reply(items))) == "필라멘트 꺼짐"


def test_setpoint_and_format_helpers():
    assert setpoint_from_pair("50      0") == pytest.approx(0.5)
    assert fmt_torr(9e-6) == "9.0E-6" and fmt_torr(0.5) == "5.0E-1" and fmt_torr(1.234e-10) == "1.2E-10"


# ───────────── 화면·기록 문구(순수 함수) ─────────────
from device.IonGauge import IgSnapshot, display_lines, csv_row, process_log_line  # noqa: E402


def _snap(reading, comm="ok", fresh=True, t=1000.0):
    from device.IonGauge import COMM_TEXT
    return IgSnapshot(comm=comm, comm_text=COMM_TEXT[comm], detail="", reading=reading, fresh=fresh,
                      last_ok_wall=None if reading is None else reading.t, t=t)


def test_display_lines_states():
    r = parse_status(E_REPLY, t=1000.0)
    assert display_lines(_snap(r), "게이지 A", "게이지 B") == (
        ["IG 9.0E-6 Torr", "게이지 A 하한 이하", "게이지 B 6.3E-3 Torr", "필라멘트 켜짐"], "ok")
    items = list(E_ITEMS); items[1] = "0"; items[4] = "4"; items[23], items[24], items[25] = "0", "-900", "-999"
    lines, level = display_lines(_snap(parse_status(e_reply(items), t=1000.0)), "Pirani", "Conv")
    assert lines == ["IG 꺼짐", "Pirani 미연결", "Conv 영점 미조정",
                     "필라멘트 오류 4: 필라멘트가 제한 시간 안에 안 켜짐(게이지 불량 가능)"] and level == "warn"
    lost = _snap(r, comm="timeout", fresh=False)
    assert display_lines(lost, "A", "B", now=1012.4) == (["IG 통신 끊김", "응답 없음", "마지막 값 12초 전", "IG 9.0E-6 Torr"], "lost")
    assert display_lines(_snap(None, comm="no_port", fresh=False), "A", "B")[0] == ["IG 통신 끊김", "포트 없음", "받은 값 없음", ""]
    assert display_lines(None, "A", "B") == (["IG 연결 중", "", "", ""], "lost")


def test_csv_row_and_process_line():
    r = parse_status(E_REPLY, t=1000.0)
    row = csv_row(_snap(r, t=1000.0))
    assert {k: row[k] for k in ("IG(Torr)", "게이지A(Torr)", "게이지B(Torr)", "필라멘트", "방출", "디개스", "오류 코드", "통신 상태")} == {
        "IG(Torr)": "9.0E-6", "게이지A(Torr)": "하한 이하", "게이지B(Torr)": "6.3E-3", "필라멘트": "켜짐",
        "방출": "정상", "디개스": "꺼짐", "오류 코드": 0, "통신 상태": "정상"}
    lost = csv_row(_snap(r, comm="timeout", fresh=False, t=1001.0))
    assert set(lost) == {"시각", "통신 상태"} and lost["통신 상태"] == "응답 없음"      # 끊긴 주기는 압력 칸 비움
    assert process_log_line(_snap(r), "시작", "게이지 A", "게이지 B") == \
        "[IG] 시작 압력 IG 9.0E-6 Torr, 게이지 A 하한 이하, 게이지 B 6.3E-3 Torr (필라멘트 켜짐)"
    assert process_log_line(_snap(r, comm="timeout", fresh=False), "종료", "A", "B", now=1030.0) == \
        "[IG] 종료 압력 — 통신 끊김(응답 없음), 마지막 값 30초 전: IG 9.0E-6 Torr"
    assert process_log_line(None, "시작", "A", "B") == "[IG] 시작 압력 — 아직 받은 값 없음"


def test_ig_log_dir_dev_and_ops():
    import lib.paths as P
    assert P.resolve_paths(True).ig_dir == P.DEV_LOG_ROOT / "ig"
    assert str(P.resolve_paths(False).ig_dir).endswith(r"\Logs\CHK\ig")
