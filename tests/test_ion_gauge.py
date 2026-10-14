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
    assert pressure_from_pair("0       -4", "conv")[1:] == ("below", "< 1.0E-3")
    v, st, txt = pressure_from_pair("61      -4", "conv")
    assert v == pytest.approx(6.1e-3) and st == "ok" and txt == "6.1E-3"
    for raw in (F_REPLY, G_REPLY, H_REPLY):
        assert looks_like_pressure_reply(raw) and is_complete(raw, 1)


def test_measured_status_reply():
    r = parse_status(E_REPLY, t=123.0)
    assert r.ion_torr == pytest.approx(9.0e-6) and r.ion_text == "9.0E-6" and r.ion_state == "ok"
    assert r.a_torr is None and r.a_state == "below" and r.a_text == "< 1.0E-3"
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
    assert pressure_from_pair("-5  -3", "conv")[1:] == ("below", "< 1.0E-3")     # -5.0E-3 = 정상 영점 범위 경계
    items = list(E_ITEMS)
    items[1] = "0"                                                          # 필라멘트 꺼짐 + 이온 0 → 꺼짐
    items[23], items[24], items[25] = "0", "-900", "-999"
    r = parse_status(e_reply(items))
    assert (r.ion_torr, r.a_torr, r.b_torr) == (None, None, None)
    assert (r.ion_text, r.a_text, r.b_text) == ("꺼짐", "미연결", "영점 미조정")
    items[1] = "1"                                                          # 필라멘트 켜짐 + 이온 0 → 측정 준비 중
    assert parse_status(e_reply(items)).ion_text == "측정 준비 중"


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
    assert r.error_code == 7 and "단선" in r.error_text                      # error_text(긴 문구)는 그대로
    assert filament_text(r) == "필라멘트 꺼짐 — 오류 7: 필라멘트 전류 없음(단선·퓨즈)"
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
        ["IG 9.0E-6 Torr", "게이지 A < 1.0E-3 Torr", "게이지 B 6.3E-3 Torr", "필라멘트 켜짐"], "ok")
    items = list(E_ITEMS); items[1] = "0"; items[4] = "4"; items[23], items[24], items[25] = "0", "-900", "-999"
    lines, level = display_lines(_snap(parse_status(e_reply(items), t=1000.0)), "Pirani", "Conv")
    assert lines == ["IG 꺼짐", "Pirani 미연결", "Conv 영점 미조정",
                     "필라멘트 꺼짐 — 오류 4: 시동 시간 초과"] and level == "warn"
    lost = _snap(r, comm="timeout", fresh=False)
    assert display_lines(lost, "A", "B", now=1012.4) == (["IG 통신 끊김", "응답 없음", "마지막 값 12초 전", "IG 9.0E-6 Torr"], "lost")
    assert display_lines(_snap(None, comm="no_port", fresh=False), "A", "B")[0] == ["IG 통신 끊김", "포트 없음", "받은 값 없음", ""]
    assert display_lines(None, "A", "B") == (["IG 연결 중", "", "", ""], "lost")


def test_csv_row_and_process_line():
    r = parse_status(E_REPLY, t=1000.0)
    row = csv_row(_snap(r, t=1000.0))
    assert {k: row[k] for k in ("IG(Torr)", "게이지A(Torr)", "게이지B(Torr)", "필라멘트", "방출", "디개스", "오류 코드", "통신 상태")} == {
        "IG(Torr)": "9.0E-6", "게이지A(Torr)": "< 1.0E-3(0 -4)", "게이지B(Torr)": "6.3E-3", "필라멘트": "켜짐",
        "방출": "정상", "디개스": "꺼짐", "오류 코드": 0, "통신 상태": "정상"}
    lost = csv_row(_snap(r, comm="timeout", fresh=False, t=1001.0))
    assert set(lost) == {"시각", "통신 상태"} and lost["통신 상태"] == "응답 없음"      # 끊긴 주기는 압력 칸 비움
    assert process_log_line(_snap(r), "시작", "게이지 A", "게이지 B") == \
        "[IG] 시작 압력 IG 9.0E-6 Torr, 게이지 A < 1.0E-3 Torr, 게이지 B 6.3E-3 Torr (필라멘트 켜짐)"
    assert process_log_line(_snap(r, comm="timeout", fresh=False), "종료", "A", "B", now=1030.0) == \
        "[IG] 종료 압력 — 통신 끊김(응답 없음), 마지막 값 30초 전: IG 9.0E-6 Torr"
    assert process_log_line(None, "시작", "A", "B") == "[IG] 시작 압력 — 아직 받은 값 없음"


def test_ig_log_dir_dev_and_ops():
    import lib.paths as P
    assert P.resolve_paths(True).ig_dir == P.DEV_LOG_ROOT / "ig"
    assert str(P.resolve_paths(False).ig_dir).endswith(r"\Logs\CHK\ig")


def test_erp_state_dict():
    from device.IonGauge import erp_state_dict
    r = parse_status(E_REPLY, t=1000.0)
    d = erp_state_dict(_snap(r), True, "게이지 A", "게이지 B", now=1002.0)
    assert d["enabled"] is True and d["comm"] == "ok" and d["commText"] == "정상"
    assert d["ion"] == pytest.approx(9e-6) and d["gaugeA"] is None and d["gaugeB"] == pytest.approx(6.3e-3)
    assert (d["ionText"], d["gaugeAText"], d["gaugeBText"]) == ("9.0E-6 Torr", "< 1.0E-3 Torr", "6.3E-3 Torr")
    assert (d["filamentOn"], d["emissionOk"], d["degasOn"], d["errorCode"], d["errorText"]) == (True, True, False, 0, "")
    assert d["filamentText"] == "필라멘트 켜짐" and d["lastResponse"] == "1970-01-01T00:16:40+00:00" and d["ageSec"] == 2
    lost = erp_state_dict(_snap(r, comm="timeout", fresh=False), True, "A", "B", now=1030.0)
    assert lost["comm"] == "timeout" and lost["ion"] is None and lost["ionText"] == "" and lost["filamentOn"] is None
    assert lost["ageSec"] == 30 and lost["lastResponse"] is not None            # 끊기면 숫자 없음 + 마지막 응답 시각
    assert erp_state_dict(None, True, "A", "B")["comm"] == "connecting"
    off = erp_state_dict(None, False, "A", "B")
    assert off["enabled"] is False and off["comm"] == "disabled" and off["ion"] is None


def test_starting_state_display_and_erp():
    from device.IonGauge import erp_state_dict
    r = parse_status(E_REPLY, t=1000.0)
    st = _snap(r, comm="starting", fresh=False)
    assert display_lines(st, "A", "B") == (["IG 필라멘트 시동 중", "", "", ""], "warn")
    d = erp_state_dict(st, True, "A", "B", now=1003.0)
    assert d["comm"] == "starting" and d["commText"] == "필라멘트 시동 중" and d["ion"] is None
    assert _snap(r).emission_since is None and _snap(r).filament_starting is False   # 기본값


def test_stopping_state_display_and_erp():
    from device.IonGauge import erp_state_dict
    r = parse_status(E_REPLY, t=1000.0)
    st = _snap(r, comm="stopping", fresh=False)
    assert display_lines(st, "A", "B") == (["IG 필라멘트 끄는 중", "", "", ""], "warn")
    assert erp_state_dict(st, True, "A", "B")["commText"] == "필라멘트 끄는 중"


# ───────────── 게이지 A/B '값 오류' 원문 ─────────────
def test_broken_gauge_string_is_the_only_real_error():
    assert pressure_from_pair("12a  -4", "conv") == (None, "invalid", "값 오류(12a -4)")
    items = list(E_ITEMS); items[24] = "1.2  -4"
    r = parse_status(e_reply(items), t=1000.0)
    assert r.a_state == "invalid" and r.a_text == "값 오류(1.2 -4)" and r.a_raw == "1.2 -4"
    assert display_lines(_snap(r), "게이지 A", "게이지 B")[0][1] == "게이지 A 값 오류(1.2 -4)"
    assert csv_row(_snap(r))["게이지A(Torr)"] == "값 오류(1.2 -4)"              # 원문이 이미 들어 있어 한 번만
    from device.IonGauge import erp_state_dict
    assert erp_state_dict(_snap(r), True, "A", "B")["gaugeAText"] == "값 오류(1.2 -4)"


# ───────────── 원문 → 화면 / CSV 대응표 ─────────────
from device.IonGauge import line_levels, raw_summary, erp_state_dict, FILAMENT_ERRORS_SHORT  # noqa: E402


def _reading(ion="9       -6", a="0       -4", b="63      -4", fil=1, emis=1, code=0, degas=0):
    items = list(E_ITEMS)
    items[0], items[1], items[2], items[4] = str(degas), str(fil), str(emis), str(code)
    items[23], items[24], items[25] = ion, a, b
    return parse_status(e_reply(items), t=1000.0)


def _view(r, settle_ago=None, now=1000.0):
    sn = IgSnapshot(comm="ok", comm_text="정상", detail="", reading=r, fresh=True, last_ok_wall=r.t, t=r.t,
                    emission_since=(None if settle_ago is None else now - settle_ago))
    lines, _ = display_lines(sn, "게이지 A", "게이지 B", now=now)
    return lines, line_levels(sn, "게이지 A", "게이지 B", now=now), csv_row(sn), erp_state_dict(sn, True, "게이지 A", "게이지 B", now=now)


ION_TABLE = [
    # (원문, fil, emis, 화면 1줄, 등급, CSV IG 칸)
    ("0", 0, 0, "IG 꺼짐", "ok", "꺼짐(0)"),
    ("0", 1, 1, "IG 측정 준비 중", "ok", "측정 준비 중(0)"),
    ("0       -4", 1, 0, "IG 측정 준비 중", "ok", "측정 준비 중(0 -4)"),
    ("8       -4", 1, 1, "IG 8.0E-4 Torr", "ok", "8.0E-4"),
    ("99      -7", 1, 1, "IG 9.9E-6 Torr", "ok", "9.9E-6"),
    ("15      -7", 1, 1, "IG 1.5E-6 Torr", "ok", "1.5E-6"),
    ("10      -6", 1, 1, "IG 1.0E-5 Torr", "ok", "1.0E-5"),
]


@pytest.mark.parametrize("raw,fil,emis,line,level,csv_text", ION_TABLE)
def test_ion_table(raw, fil, emis, line, level, csv_text):
    lines, levels, row, erp = _view(_reading(ion=raw, fil=fil, emis=emis))
    assert lines[0] == line and levels[0] == level and row["IG(Torr)"] == csv_text
    assert erp["ionText"] == line[3:] and "(" not in erp["ionText"]          # ERP 는 화면 문구, 원문 없음


GAUGE_TABLE = [
    # (원문, 화면 2줄, 등급, CSV 칸, 상태)
    ("0", "게이지 A < 1.0E-3 Torr", "ok", "< 1.0E-3(0)", "below"),
    ("-5      -4", "게이지 A < 1.0E-3 Torr", "ok", "< 1.0E-3(-5 -4)", "below"),
    ("-4      -4", "게이지 A < 1.0E-3 Torr", "ok", "< 1.0E-3(-4 -4)", "below"),
    ("-50     -4", "게이지 A < 1.0E-3 Torr", "ok", "< 1.0E-3(-50 -4)", "below"),            # = -5.0E-3 경계
    ("-6      -3", "게이지 A 음수 — 영점·오염 확인", "warn", "음수 — 영점·오염 확인(-6 -3)", "negative"),
    ("-900", "게이지 A 미연결", "ok", "미연결(-900)", "disconnected"),
    ("-999", "게이지 A 영점 미조정", "warn", "영점 미조정(-999)", "unzeroed"),
    ("61      -4", "게이지 A 6.1E-3 Torr", "ok", "6.1E-3", "ok"),
    ("76      1", "게이지 A 7.6E2 Torr", "ok", "7.6E2", "ok"),
    ("x7#  -4", "게이지 A 값 오류(x7# -4)", "warn", "값 오류(x7# -4)", "invalid"),
]


@pytest.mark.parametrize("raw,line,level,csv_text,state", GAUGE_TABLE)
def test_gauge_table(raw, line, level, csv_text, state):
    r = _reading(a=raw)
    lines, levels, row, erp = _view(r)
    assert r.a_state == state and lines[1] == line and levels[1] == level and row["게이지A(Torr)"] == csv_text
    assert erp["gaugeAText"] == line[len("게이지 A "):]
    if state != "invalid":
        assert "(" not in erp["gaugeAText"]                                      # ERP 는 원문 없음


@pytest.mark.parametrize("code", sorted(FILAMENT_ERRORS_SHORT) + [99])
def test_filament_error_codes(code):
    lines, levels, row, erp = _view(_reading(fil=0, emis=0, code=code, ion="0"))
    short = FILAMENT_ERRORS_SHORT.get(code)
    want = f"필라멘트 꺼짐 — 오류 {code}" + (f": {short}" if short else "")
    assert lines[3] == want and erp["filamentText"] == want and row["오류 코드"] == code
    assert levels[3] == ("ok" if code == 16 else "warn")                      # 16(자동 정상 꺼짐)은 주의색 아님


def test_filament_error_examples():
    assert _view(_reading(fil=0, emis=0, code=15, ion="0"))[0][3] == "필라멘트 꺼짐 — 오류 15: 9.9E-3 Torr 초과 차단"
    assert _view(_reading(fil=0, emis=0, code=99, ion="0"))[0][3] == "필라멘트 꺼짐 — 오류 99"


def test_filament_emission_and_degas():
    lines, levels, *_ = _view(_reading(fil=1, emis=0))
    assert lines[3] == "필라멘트 켜짐 · 방출 이상" and levels[3] == "warn"
    lines, levels, *_ = _view(_reading(fil=1, emis=1, degas=1))
    assert lines[3] == "필라멘트 켜짐 · 디개스 중" and levels[3] == "ok"
    lines, levels, *_ = _view(_reading(fil=0, emis=0, ion="0"))
    assert lines[3] == "필라멘트 꺼짐" and levels[3] == "ok"


def test_settling_shown_until_settle_sec_then_gone():
    from lib.config import IG_SETTLE_SEC
    r = _reading(ion="99      -7")
    lines, levels, row, erp = _view(r, settle_ago=float(IG_SETTLE_SEC) - 0.5)
    assert lines[0] == "IG 9.9E-6 Torr (안정화 중)" and levels[0] == "warn"
    assert erp["ionText"] == "9.9E-6 Torr (안정화 중)" and row["IG(Torr)"] == "9.9E-6"   # CSV 는 숫자 그대로
    lines, levels, *_ = _view(r, settle_ago=float(IG_SETTLE_SEC) + 0.1)
    assert lines[0] == "IG 9.9E-6 Torr" and levels[0] == "ok"


def test_raw_summary_for_tooltip():
    r = _reading(ion="99      -7", a="-5      -4", b="61      -4")
    assert raw_summary(_snap(r)) == "원문 IG 99 -7 · A -5 -4 · B 61 -4 · 오류 코드 0"
    assert raw_summary(None) == ""


def test_lost_and_switching_levels():
    r = _reading()
    assert line_levels(_snap(r, comm="timeout", fresh=False), "A", "B") == ["lost", "dim", "dim", "dim"]
    assert line_levels(_snap(r, comm="starting", fresh=False), "A", "B")[0] == "warn"


@pytest.mark.parametrize("raw", ["0", "-4      -4", "-5      -4", "-50     -4"])
def test_below_range_shown_as_less_than_1e3(raw):
    lines, levels, row, erp = _view(_reading(a=raw))
    norm = " ".join(raw.split())
    assert lines[1] == "게이지 A < 1.0E-3 Torr" and levels[1] == "ok"
    assert row["게이지A(Torr)"] == f"< 1.0E-3({norm})" and erp["gaugeAText"] == "< 1.0E-3 Torr"


def test_minus_6e3_still_negative_check():
    lines, levels, row, erp = _view(_reading(a="-6      -3"))
    assert lines[1] == "게이지 A 음수 — 영점·오염 확인" and levels[1] == "warn"
    assert row["게이지A(Torr)"] == "음수 — 영점·오염 확인(-6 -3)"


def test_tooltip_has_below_note():
    from device.IonGauge import tooltip_lines, BELOW_NOTE
    r = _reading(a="-4      -4")
    assert tooltip_lines(_snap(r)) == ["원문 IG 9 -6 · A -4 -4 · B 63 -4 · 오류 코드 0",
                                       "컨벡션 게이지 측정 하한(1E-3 Torr) 아래 — 영점 근처 잡음"] == [raw_summary(_snap(r)), BELOW_NOTE]
    r2 = _reading(a="61      -4", b="63      -4")
    assert tooltip_lines(_snap(r2)) == [raw_summary(_snap(r2))]          # 둘 다 측정 범위 안이면 설명 없음
    assert tooltip_lines(None) == []
