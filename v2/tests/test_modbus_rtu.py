"""Modbus RTU 프로토콜 시험 — 알려진 프레임(Modbus 규격서 예시), 응답 길이·풀기, 예외·CRC·잘린 프레임·재시도."""
import pytest

from v2.common.clock import VirtualClock
from v2.common.modbus_rtu import (
    CrcError, FrameError, ModbusClient, ModbusException, ReadOnlyViolation, crc16, crc_ok, parse_response,
    req_read_coils, req_read_regs, req_write_coil, req_write_coils, req_write_reg, req_write_regs, response_length,
    with_crc,
)
from v2.common.transport import CancelToken, ReadTimeout, SimTransport
from v2.common.worker import Session
from v2.chk.sim.plc_sim import PlcSim

H = bytes.fromhex


@pytest.mark.parametrize("frame", [
    "01 03 00 00 00 0A C5 CD",                      # 국번 1, 레지스터 0~9 읽기
    "11 03 00 6B 00 03 76 87",                      # 규격서 FC3 예시
    "11 05 00 AC FF 00 4E 8B",                      # 규격서 FC5 예시
    "11 01 00 13 00 25 0E 84",                      # 규격서 FC1 예시
])
def test_known_frames(frame):
    b = H(frame)
    assert crc_ok(b) and with_crc(b[:-2]) == b


def test_request_builders():
    assert req_read_regs(0x11, 0x6B, 3) == H("11 03 00 6B 00 03 76 87")
    assert req_write_coil(0x11, 0xAC, True) == H("11 05 00 AC FF 00 4E 8B")
    assert req_read_coils(0x11, 0x13, 0x25) == H("11 01 00 13 00 25 0E 84")
    assert req_write_coil(1, 3, False)[2:6] == H("00 03 00 00")
    assert req_write_reg(1, 14, 0x1234)[2:6] == H("00 0E 12 34")
    f15 = req_write_coils(0x11, 0x13, [1, 0, 1, 1, 0, 0, 1, 1, 1, 0])     # 규격서 FC15 예시 데이터 CD 01
    assert f15[:-2] == H("11 0F 00 13 00 0A 02 CD 01") and crc_ok(f15)
    f16 = req_write_regs(0x11, 1, [0x000A, 0x0102])
    assert f16[:-2] == H("11 10 00 01 00 02 04 00 0A 01 02") and crc_ok(f16)
    with pytest.raises(ValueError):
        req_read_regs(1, 0, 126)


def test_response_length():
    assert response_length(1, 96) == 17 and response_length(1, 161) == 26
    assert response_length(3, 61) == 127
    assert all(response_length(f) == 8 for f in (5, 6, 15, 16))


def test_parse_reads_and_writes():
    coils = with_crc(H("11 01 05 CD 6B B2 0E 1B"))                       # 규격서 FC1 응답(37 비트)
    bits = parse_response(coils, 0x11, 1, 37)
    assert bits[:8] == [True, False, True, True, False, False, True, True] and len(bits) == 37
    regs = with_crc(H("11 03 06 02 2B 00 00 00 64"))
    assert parse_response(regs, 0x11, 3, 3) == [0x022B, 0, 0x64]
    echo = H("11 05 00 AC FF 00 4E 8B")
    assert parse_response(echo, 0x11, 5) == (0xAC, 0xFF00)


def test_parse_errors():
    with pytest.raises(ModbusException) as e:
        parse_response(with_crc(H("11 83 02")), 0x11, 3, 3)
    assert e.value.code == 2
    good = with_crc(H("11 03 02 00 07"))
    with pytest.raises(CrcError):
        parse_response(good[:-1] + bytes([good[-1] ^ 1]), 0x11, 3, 1)
    with pytest.raises(FrameError):
        parse_response(good, 0x12, 3, 1)                 # 국번 다름
    with pytest.raises(FrameError):
        parse_response(good, 0x11, 3, 2)                 # 길이 다름


# ───────── 클라이언트(가짜 PLC 위에서) ─────────
def _client(sim, **kw):
    c = VirtualClock()
    c.register()
    t = SimTransport(c, sim)
    t.open()
    sess = Session(t, c.monotonic() + 5.0, CancelToken(c))
    return c, ModbusClient(sess, 1, **kw)


def test_client_round_trip():
    sim = PlcSim()
    c, cl = _client(sim)
    cl.write_reg(14, 77)
    assert cl.read_regs(14, 1) == [77]
    cl.write_coils(16, [True, True])
    assert cl.read_coils(16, 2) == [True, True]
    cl.write_regs(50, [1, 2])                             # 래더가 쓰지 않는 주소(D36 은 래더 계산값)
    assert cl.read_regs(50, 2) == [1, 2]
    assert cl.transactions == 6


def test_exception_not_retried():
    sim = PlcSim()
    c, cl = _client(sim, retries=3)
    with pytest.raises(ModbusException) as e:
        cl.read_regs(250, 10)                             # 범위 밖
    assert e.value.code == 2 and len(sim.frames) == 1


@pytest.mark.parametrize("fault,err", [("no_response", ReadTimeout), ("truncate", ReadTimeout),
                                       ("bad_crc", CrcError), ("garbage", CrcError)])
def test_faults_after_retries(fault, err):
    sim = PlcSim()
    sim.add_fault(fault, 0.0, 100.0)
    c, cl = _client(sim, retries=2, txn_timeout_s=0.15)
    with pytest.raises(err):
        cl.read_regs(0, 10)
    assert len(sim.frames) == 3                           # 1 + 재시도 2


def test_retry_recovers():
    sim = PlcSim()
    sim.add_fault("no_response", 0.0, 0.1)
    c, cl = _client(sim, retries=1, txn_timeout_s=0.15, retry_delay_s=0.03)
    assert cl.read_regs(14, 1) == [0] and len(sim.frames) == 2


def test_read_only_client_never_sends_writes():
    sim = PlcSim()
    c, cl = _client(sim, read_only=True)
    for f in (lambda: cl.write_reg(14, 1), lambda: cl.write_coil(3, False), lambda: cl.write_coils(0, [False]),
              lambda: cl.write_regs(0, [1])):
        with pytest.raises(ReadOnlyViolation):
            f()
    cl.read_regs(0, 1)
    assert sim.write_funcs_seen() == [] and len(sim.frames) == 1


def test_crc_of_any_frame_with_crc_is_zero():
    for body in (b"\x01\x03\x00\x00\x00\x01", b"\x05" * 40):
        assert crc16(with_crc(body)) == 0
