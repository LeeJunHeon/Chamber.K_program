"""bench 도구 — 가짜 포트(SimSerial)로만. 실제 COM 포트는 열지 않는다.

장비에 나간 바이트를 장비별로 검사해 '쓰기 0'(질의만)을 확인한다.
"""
import pytest
import time

from v2.chk.sim.cesar_sim import CesarSim
from v2.chk.sim.chamber import Chamber
from v2.chk.sim.dc_sim import DcSim
from v2.chk.sim.ig_sim import IgSim
from v2.chk.sim.mfc_sim import MfcSim
from v2.chk.sim.plc_sim import A, PlcSim
from v2.chk.sim.serial_adapter import SimSerial
from v2.tools import bench_probe as B

PORTS = {"plc": "COM9", "mfc": "COM10", "dc": "COM11", "ig": "COM12", "rfpulse": "COM13"}


class Bench:
    def __init__(self, fail_port=None):
        self.origin = time.monotonic()                               # 시뮬레이터 시각 0 = 시험 시작
        ch = Chamber()
        self.sims = {"COM9": PlcSim(chamber=ch), "COM10": MfcSim(ch), "COM11": DcSim(ch),
                     "COM13": CesarSim(ch), "COM12": IgSim(ch)}
        self.ports = {}
        self.fail_port = fail_port
        self.out = []

    def factory(self, s):
        if s.port == self.fail_port:
            raise PermissionError(13, "Access is denied")      # v1 이 포트를 쥐고 있을 때와 같게
        self.ports[s.port] = SimSerial(self.sims[s.port], self.origin)
        return self.ports[s.port]

    def run(self, tmp_path, seconds=2, cfg_over=None, skip=()):
        code = B.run(seconds, PORTS, tmp_path, serial_factory=self.factory, print_fn=self.out.append,
                     cfg_over=cfg_over, skip=skip)
        return code, "\n".join(self.out)

    def writes(self):
        """질의가 아닌 바이트 목록(장비별 형식으로 판정)."""
        bad = []
        for port, ser in self.ports.items():
            for w in ser.written:
                if port == "COM9":
                    ok = len(w) >= 2 and w[1] in (1, 2, 3, 4)               # Modbus 읽기 함수만
                elif port == "COM10":
                    ok = w.startswith(b"R") and w.endswith(b"\r")          # GPC3000 R 질의만
                elif port == "COM11":
                    ok = w.rstrip(b"\n").endswith(b"?")                    # SCPI 질의만
                elif port == "COM12":
                    ok = w in (b"E", b"F", b"G", b"H")                     # 필라멘트 A·B·디개스 C·D 없음
                else:
                    ok = len(w) >= 2 and w[1] >= 128 or w in (b"\x06", b"\x15")   # AE Bus 질의(128~) 또는 ACK/NAK
                if not ok:
                    bad.append((port, w))
        return bad


def test_port_open_failure_stops_without_retry_or_writes(tmp_path):
    b = Bench(fail_port="COM11")
    code, out = b.run(tmp_path)
    assert code == 3
    assert "COM11 를 열 수 없음" in out and "다시 시도하지 않고" in out
    assert all(not s.written for s in b.ports.values())             # 아무것도 보내지 않음
    assert all(s.closed for s in b.ports.values())                  # 먼저 연 포트는 닫음
    assert (next(tmp_path.glob("v2_bench_*")) / "summary.txt").exists()


def test_heater_run_on_stops_with_zero_writes(tmp_path):
    b = Bench()
    b.sims["COM9"].coils[A["heater_run"]] = True
    code, out = b.run(tmp_path)
    assert code == 2 and "히터 RUN 이 켜져 있음" in out and "수집 시작" not in out
    assert b.writes() == [] and b.sims["COM9"].write_funcs_seen() == []


def test_process_trace_stops_with_zero_writes(tmp_path):
    b = Bench()
    b.sims["COM11"].out = True                                        # DC 출력이 켜져 있음
    code, out = b.run(tmp_path)
    assert code == 2 and "DC 출력" in out and "수집 시작" not in out
    assert b.writes() == []
    assert b.sims["COM11"].out is True                                # 끄지도 않는다(쓰기 0)


@pytest.mark.slow
def test_rf_enable_only_continues_with_note(tmp_path):
    """enable(M00200)만 켜지고 D40 0·순방향 0 — v1 정상 종료 흔적: [참고] 를 남기고 계속(10/16 bench 에서 멈췄던 경우)."""
    b = Bench()
    b.sims["COM9"].coils[A["rf_dac_enable"]] = True
    code, out = b.run(tmp_path)
    assert code == 0 and "[참고] M00200 켜짐 · D40 0" in out and "RF(PLC 아날로그): D40 0" in out and b.writes() == []


def test_rf_actual_output_stops(tmp_path):
    b = Bench()
    b.sims["COM9"].coils[A["rf_dac_enable"]] = True
    b.sims["COM9"].regs[A["rf_dac"]] = 300                       # D40 > 0 — 실제 출력
    code, out = b.run(tmp_path)
    assert code == 2 and "RF 출력이 있음(D40 300" in out and "수집 시작" not in out and b.writes() == []


@pytest.mark.slow
def test_normal_run_reads_only_and_writes_summary(tmp_path):
    b = Bench()
    code, out = b.run(tmp_path, seconds=3, cfg_over={"RFPULSE_ENABLED": True})
    assert code == 0
    assert "읽기 전용" in out and "안전 확인 통과" in out and "읽기 전용 가드 켜짐" in out
    assert b.writes() == [] and b.sims["COM9"].write_funcs_seen() == []
    assert b.sims["COM13"].set_commands_seen() == []
    folder = next(tmp_path.glob("v2_bench_*"))
    names = {p.name for p in folder.iterdir()}
    assert "summary.txt" in names and {f"rec_{k}.jsonl" for k in PORTS} <= names
    assert any(n.startswith("snap_") and n.endswith(".sqlite") for n in names)
    assert any(n.endswith(".csv") for n in names)
    s = (folder / "summary.txt").read_text(encoding="utf-8")
    assert "events.jsonl" in names and "inventory.json" in names
    for want in ("[dc] 28개", "[mfc] 46개", "[ig] 29개", "[확인] MFC 압력 레인지 03(1) · 단위 0(Torr) → FS 1 Torr",
                 "[확인] MFC Remote(R37 X=1)", "[확인] DC LAST-STA = DISABLE", "정격 후보 EX 600-1", "[확인] IG 가스 계수 1.00 · 감도 10.0",
                 "명령으로 확인할 수 없는 것", "대기 중 값 범위", "연결 기록", "D00040(U02.03)",
                 "[확인] DC 형명·정격", "[확인] MFC 압력 레인지·작업 압력 상한"):
        assert want in s, want
    assert "형식 다름" not in s
    for want in ("놓친 tick 0", "MFC FS: Ar 100", "DC *IDN?: ODA", "DC FACT:LAST-STA?: DISABLE", "[확인] DC LAST-STA 현재 값", "CESAR 1310", "명령 33 현재",
                 "IG F", "IG E:", "PLC 블록 읽기", "[확인] PLC 트랜잭션 왕복"):
        assert want in s, want
    assert "경고: MFC 풀스케일" not in s
    import re
    assert all(m == "0" for m in re.findall(r"무응답 (\d+)", s)), s      # 종료 때 끊긴 요청은 무응답에 안 셈


@pytest.mark.slow
def test_disabled_and_skipped_devices_are_not_opened(tmp_path):
    """RF Pulse 는 기본 꺼짐(RFPULSE_ENABLED=false, 미연결) — 포트를 열지 않는다. --skip 으로도 뺀다."""
    b = Bench()
    code, out = b.run(tmp_path, skip=("ig",))
    assert code == 0 and "COM13" not in b.ports and "COM12" not in b.ports
    assert "rfpulse 제외(설정 꺼짐 — RFPULSE_ENABLED=false)" in out and "ig 제외(--skip)" in out
    assert b.writes() == []


@pytest.mark.slow
def test_inventory_mismatches_are_warned_not_changed(tmp_path):
    b = Bench()
    b.sims["COM10"].info[33] = "E06"                              # 10 Torr 레인지
    b.sims["COM10"].info[37] = "M000"                             # Local
    b.sims["COM11"].last_sta = "FUL"
    b.sims["COM11"].info["FACT:ADC?"] = "fast"                    # 매뉴얼에 없는 형식
    b.sims["COM11"].info["VOLT:OVP:TRIP?"] = "1"
    b.sims["COM11"].errors = ['-124,"Undefined header"', '-222,"Out of data"']
    code, out = b.run(tmp_path)
    assert code == 0 and b.writes() == []
    for want in ("!!! MFC 압력 레인지 06(10) · 단위 0(Torr) → FS 10 Torr ≠ MFC_PRESSURE_FS_TORR",
                 "!!! MFC Local(R37 X=0)", "!!! DC LAST-STA = FULLY", "!!! DC OVP 트립 중", "'fast'", "형식 다름",
                 "SYST:ERR? 1", 'DC SYST:ERR?(연결 때): -124,"Undefined header"', 'DC SYST:ERR?(연결 때): -222,"Out of data"',
                 "[미확인] MFC 압력 레인지·작업 압력 상한"):
        assert want in out, want
    inv = __import__("json").loads((next(tmp_path.glob("v2_bench_*")) / "inventory.json").read_text(encoding="utf-8"))
    assert any(i["query"] == "R33" and i["raw"] == "E06" for i in inv["items"])


@pytest.mark.slow
def test_no_reply_device_shows_recording(tmp_path):
    b = Bench()
    b.sims["COM12"].add_fault("no_response", 0.0, 1e9)            # IG 가 아무것도 답하지 않음
    code, out = b.run(tmp_path)
    assert "녹화 앞부분 ig" in out and "'F'  →  ''" in out and b.writes() == []


@pytest.mark.slow
def test_mfc_fs_not_100_is_loud(tmp_path):
    b = Bench()
    b.sims["COM10"].fs = [200.0, 100.0, 100.0, 100.0]
    code, out = b.run(tmp_path)
    assert code == 0 and "!!!!!!!! 경고: MFC 풀스케일이 100 이 아닙니다" in out


def test_cli_accepts_only_bench_options():
    import pytest
    with pytest.raises(SystemExit):
        B.main(["--mode", "prod"])                                    # 모드 선택지가 없다(bench 고정)
