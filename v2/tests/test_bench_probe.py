"""bench 도구 — 가짜 포트(SimSerial)로만. 실제 COM 포트는 열지 않는다.

장비에 나간 바이트를 장비별로 검사해 '쓰기 0'(질의만)을 확인한다.
"""
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

    def run(self, tmp_path, seconds=2):
        code = B.run(seconds, PORTS, tmp_path, serial_factory=self.factory, print_fn=self.out.append)
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
                    ok = w in (b"E", b"F")                                 # 필라멘트 A·B·degas 없음
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


def test_rf_dac_enable_is_process_trace(tmp_path):
    b = Bench()
    b.sims["COM9"].coils[A["rf_dac_enable"]] = True
    code, out = b.run(tmp_path)
    assert code == 2 and "RF DAC enable" in out and b.writes() == []


def test_normal_run_reads_only_and_writes_summary(tmp_path):
    b = Bench()
    code, out = b.run(tmp_path, seconds=3)
    assert code == 0
    assert "읽기 전용" in out and "안전 확인 통과" in out
    assert b.writes() == [] and b.sims["COM9"].write_funcs_seen() == []
    assert b.sims["COM13"].set_commands_seen() == []
    folder = next(tmp_path.glob("v2_bench_*"))
    names = {p.name for p in folder.iterdir()}
    assert "summary.txt" in names and {f"rec_{k}.jsonl" for k in PORTS} <= names
    assert any(n.startswith("snap_") and n.endswith(".sqlite") for n in names)
    assert any(n.endswith(".csv") for n in names)
    s = (folder / "summary.txt").read_text(encoding="utf-8")
    for want in ("놓친 tick 0", "MFC FS: Ar 100", "DC *IDN?: ODA", "CESAR 1310", "명령 33 현재",
                 "IG F", "IG E:", "PLC 블록 읽기", "[확인] PLC 트랜잭션 왕복"):
        assert want in s, want
    assert "경고: MFC 풀스케일" not in s


def test_mfc_fs_not_100_is_loud(tmp_path):
    b = Bench()
    b.sims["COM10"].fs = [200.0, 100.0, 100.0, 100.0]
    code, out = b.run(tmp_path)
    assert code == 0 and "!!!!!!!! 경고: MFC 풀스케일이 100 이 아닙니다" in out


def test_cli_accepts_only_bench_options():
    import pytest
    with pytest.raises(SystemExit):
        B.main(["--mode", "prod"])                                    # 모드 선택지가 없다(bench 고정)
