"""시작 보호(5-19 ②) — 실행 프로그램(python -m v2.chk) bench·prod 를 가짜 포트(SimSerial)로만. 실제 COM 포트는 열지 않는다.

- bench: 켜진 장비 포트를 PLC → MFC → 이온 게이지 → RF Pulse → DC 순서로 한 번씩 열고, 하나라도 못 열면 연 포트를 닫고 끝(종료 코드 5).
  연결 뒤 첫 읽기(히터 RUN·RF·DC·RF Pulse·가스 유량)에 걸리면 포트·로컬 서버를 닫고 끝. 모든 경우 장비 쓰기 0 바이트.
- prod: 첫 열기에서 '다른 프로그램이 사용 중'(PermissionError — pyserial 윈도우 접근 거부)이면 끝, '포트 없음'은 재연결로 계속.
- 시작 보호를 통과한 뒤의 재연결은 그대로, sim 은 바뀐 것 없음(test_app).
"""
import json
import socket
import time

import pytest

from v2.chk import startup_guard as SG
from v2.chk.app import EXIT_GUARD, EXIT_OK, App
from v2.chk.sim.plc_sim import A
from v2.common.transport import TransportError
from v2.tests.test_bench_probe import Bench


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Ports(Bench):
    """test_bench_probe.Bench 의 가짜 포트 + 포트별 실패 종류(사용 중 = PermissionError, 없음 = FileNotFoundError)."""

    def __init__(self, busy=(), missing=()):
        super().__init__()
        self.busy, self.missing, self.order = set(busy), set(missing), []

    def factory(self, s):
        self.order.append(s.port)
        if s.port in self.busy:
            raise PermissionError(13, "Access is denied", None, 5)     # pyserial 윈도우: CreateFile ERROR_ACCESS_DENIED
        if s.port in self.missing:
            raise FileNotFoundError(2, "The system cannot find the file specified", None, 2)
        return super().factory(s)


def _app(tmp_path, ports, mode="bench", **extra):
    d = tmp_path / "config"
    d.mkdir(parents=True, exist_ok=True)
    vals = dict({"RUN_MODE": mode, "LOCAL_DATA_DIR": str(tmp_path / "data"), "LOCAL_HMI_STATIC_DIR": str(tmp_path / "hmi"),
                 "LOCAL_HMI_PORT": _free_port(), "NAS_COPY_DIR": ""}, **extra)
    if mode == "prod":
        vals.update(UPLINK_URL="https://example.invalid/x", UPLINK_TOKEN="t")   # X23: prod 는 주소 필요(연결은 시험에서 막음)
    (d / "local_override.json").write_text(json.dumps({"values": vals}), encoding="utf-8")
    out = []
    app = App(str(d), print_fn=out.append, serial_factory=ports.factory)
    if mode == "prod":
        app.start_uplink = lambda s: None                                    # 웹에 연결하지 않는다
    return app, out


def _closed_all(ports):
    return all(s.closed for s in ports.ports.values())


ORDER = ["COM9", "COM10", "COM12", "COM11"]          # PLC → MFC → 이온 게이지 → (RF Pulse 꺼짐) → DC


def test_bench_normal_start_opens_in_order_and_reads_only(tmp_path):
    p = Ports()
    app, out = _app(tmp_path, p)
    try:
        assert app.start() == EXIT_OK
        assert p.order[:4] == ORDER                                           # 시작 보호가 연 순서(그 뒤 다시 열지 않음)
        assert len(p.order) == 4 and not any(s.closed for s in p.ports.values())
        assert any("RF(PLC 아날로그): D40 0" in s for s in out)
        time.sleep(2.0)
    finally:
        app.shutdown()
    assert p.writes() == [] and p.sims["COM9"].write_funcs_seen() == []
    assert _closed_all(p)


@pytest.mark.parametrize("busy,opened_before", [("COM9", []), ("COM11", ["COM9", "COM10", "COM12"]),
                                                ("COM10", ["COM9"])])
def test_bench_port_busy_stops_closes_and_writes_nothing(tmp_path, busy, opened_before):
    p = Ports(busy=(busy,))
    app, out = _app(tmp_path, p)
    t0 = time.monotonic()
    assert app.start() == EXIT_GUARD
    assert time.monotonic() - t0 < 5.0                                        # 다시 시도하지 않는다
    assert p.order == opened_before + [busy]                                  # 실패한 뒤로는 열지 않는다
    assert sorted(p.ports) == sorted(opened_before) and _closed_all(p)        # 연 포트는 모두 닫았다
    assert p.writes() == [] and all(not s.written for s in p.ports.values())
    text = "\n".join(out)
    assert f"포트 {busy} 를 열 수 없음" in text and SG.PORT_HINT in text and "다시 시도하지 않고" in text
    assert app.system is not None and not app.system.started                 # worker 를 시작하지 않았다
    log = (tmp_path / "data" / "logs" / "chk_v2.log").read_text(encoding="utf-8")
    assert f"포트 {busy} 를 열 수 없음" in log


def test_bench_port_missing_also_stops(tmp_path):
    p = Ports(missing=("COM12",))
    app, out = _app(tmp_path, p)
    assert app.start() == EXIT_GUARD
    assert p.order == ["COM9", "COM10", "COM12"] and _closed_all(p) and p.writes() == []
    assert any("이온 게이지 포트 COM12 를 열 수 없음" in s for s in out)


def _set_heater(p):
    p.sims["COM9"].coils[A["heater_run"]] = True


def _set_rf_dac(p):
    p.sims["COM9"].coils[A["rf_dac_enable"]] = True
    p.sims["COM9"].regs[A["rf_dac"]] = 300


def _set_dc(p):
    p.sims["COM11"].out = True


def _set_gas(p):
    m = p.sims["COM10"]
    m.on[0], m.set_pct[0], m.flow_pct[0] = True, 20.0, 20.0


def _set_rfpulse(p):
    p.sims["COM13"].out = True


@pytest.mark.parametrize("setup,why,extra", [
    (_set_heater, "히터 RUN 이 켜져 있음", {}),
    (_set_rf_dac, "RF 출력이 있음(D40 300", {}),
    (_set_dc, "DC 출력(OUTP?)이 켜져 있음", {}),
    (_set_gas, "가스 유량이 있음", {}),
    (_set_rfpulse, "RF Pulse 출력이 켜져 있음", {"RFPULSE_ENABLED": True}),
])
def test_bench_first_read_stops_cleanly_with_zero_writes(tmp_path, setup, why, extra):
    p = Ports()
    setup(p)
    app, out = _app(tmp_path, p, **extra)
    assert app.start() == EXIT_GUARD
    text = "\n".join(out)
    assert why in text and "장비에 아무것도 쓰지 않고" in text
    assert p.writes() == [] and p.sims["COM9"].write_funcs_seen() == []
    assert p.sims["COM13"].set_commands_seen() == []
    time.sleep(0.3)
    assert _closed_all(p)                                                     # 포트를 모두 닫았다
    assert not any(w._thread.is_alive() for w in app.system.workers.values() if w._thread is not None)
    assert app._tick_thread is None and app.server.httpd is not None and app.server._thread is None   # tick·서버는 안 띄움
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", app.server.port), timeout=0.5).close()   # 로컬 서버 포트 닫힘


def test_bench_rf_enable_only_starts_with_note(tmp_path):
    p = Ports()
    p.sims["COM9"].coils[A["rf_dac_enable"]] = True
    app, out = _app(tmp_path, p)
    try:
        assert app.start() == EXIT_OK and any("[참고] M00200 켜짐 · D40 0" in s for s in out)
    finally:
        app.shutdown()
    assert p.writes() == []


def test_prod_busy_port_refuses_missing_port_continues(tmp_path):
    p = Ports(busy=("COM10",))
    app, out = _app(tmp_path / "a", p, mode="prod")
    assert app.start() == EXIT_GUARD
    assert p.order == ["COM9", "COM10"] and _closed_all(p) and all(not s.written for s in p.ports.values())
    assert any("COM10 를 다른 프로그램이 사용 중" in s for s in out)
    q = Ports(missing=("COM12",))
    app2, out2 = _app(tmp_path / "b", q, mode="prod")
    try:
        assert app2.start() == EXIT_OK                                         # 없는 포트는 재연결로 계속(예전 그대로)
        assert not app2.system.workers["ig"].ready and app2.system.workers["plc"].ready
    finally:
        app2.shutdown()


def test_port_in_use_classification():
    """pyserial 윈도우 문구(SerialException 글) · PermissionError · winerror 5 = 사용 중, 없음·시간 초과 = 아님."""
    class SerialException(OSError):
        pass
    busy = SerialException("could not open port 'COM9': PermissionError(13, '액세스가 거부되었습니다.', None, 5)")
    gone = SerialException("could not open port 'COM9': FileNotFoundError(2, '지정된 파일을 찾을 수 없습니다.', None, 2)")
    for cause, want in ((busy, True), (gone, False), (PermissionError(13, "x"), True), (FileNotFoundError(2, "x"), False)):
        try:
            try:
                raise cause
            except OSError as c:
                raise TransportError(f"포트 열기 실패: {c!r}") from c
        except TransportError as e:
            assert SG.port_in_use(e) is want, cause
    assert SG.port_in_use(TransportError("포트 열기 2s 초과")) is False


def test_stop_sequence_bench_rejection_is_info_not_error():
    """bench 에서 정지 순서 단계가 BENCH_READ_ONLY 로 거절되면 'bench — 보내지 않음'(info), 다음 단계도 그대로 —
    prod·sim 의 실패(error·뒤 단계 건너뜀)는 그대로."""
    from v2.common.clock import VirtualClock
    from v2.common.safety_engine import BENCH_SKIP, Act, Step, StepRunner
    for bench in (True, False):
        ev = []
        clk = VirtualClock()
        clk.register()
        r = StepRunner(clk, {"X": lambda a, done: done("BENCH_READ_ONLY")}, ev.append, bench=lambda b=bench: b)
        res = r.run("ALL_STOP", [Step(1, Act("X")), Step(2, Act("X"), must=False), Step(3, Act("X"), nowait=True),
                                 Step(4, Act("AWAIT", {"no": 3}))])
        if bench:
            assert [x[2] for x in res] == [BENCH_SKIP, BENCH_SKIP, "sent", BENCH_SKIP]
            assert all(e.level != "error" for e in ev) and ev[-1].level == "info"
        else:
            assert [x[2] for x in res] == ["error:BENCH_READ_ONLY", "skipped", "sent", "error:BENCH_READ_ONLY"]
            assert [e.level for e in ev if e.kind == "step"].count("error") == 2


@pytest.mark.slow
def test_bench_running_all_commands_and_outputs_turning_on_send_only_allowed_queries(tmp_path):
    """bench 로 시작한 뒤 모든 명령 종류(웹·노트북 화면·내부)를 보내고, 시작 뒤 장비 쪽 출력(히터 RUN·RF·DC·RF Pulse·가스)이
    켜진 상태로 바뀌어도 포트로 나간 것은 허용 질의뿐(5-19 ⑥). 정지·안전 조치의 bench 거절은 ERROR 로 남지 않는다."""
    from v2.chk import commands as C
    from v2.common.commands import Source
    p = Ports()
    app, out = _app(tmp_path, p, RFPULSE_ENABLED=True)
    assert app.start() == EXIT_OK
    s = app.system
    row = {"process_name": "t", "ar": True, "ar_flow": 20.0, "working_pressure": 5.0, "process_time": 0.1, "use_dc": True,
           "dc_power": 50.0, "gun1": True}
    args = {"PROCESS_START": {"row": row}, "RECIPE_LOAD": {"name": "r", "rows": [row]}, "HEATER_SV": {"value": 50.0},
            "HEATER_ON": {"value": 50.0}, "HEATER_RECIPE_HOLD": {"on": True}, "PLC_BUTTON": {"target": "mv", "on": True},
            "IG_FILAMENT": {"on": True}, "PROCESS_ADJUST": {"device": "dc", "item": "power", "value": 60.0},
            "HEATER_CLAMP_MV": {"value": 500}, "PROCESS_ABORT": {"reason": "TEST"}, "NOTIFY": {"level": "info", "code": "T"}}
    try:
        res = []
        for c in C.COMMANDS:
            for src in c.sources:
                res.append((c.kind, s.core.command(c.kind, dict(args.get(c.kind, {})), source=src)))
        assert all(r.ok or r.reason for _, r in res)
        assert [k for k, r in res if r.ok] == ["RECIPE_LOAD", "RECIPE_CLEAR"]          # 장비에 쓰지 않는 명령만 받음
        time.sleep(2.0)
        p.sims["COM9"].coils[A["heater_run"]] = True                              # 시작 뒤 출력이 켜진 상태로 바뀜
        p.sims["COM9"].regs[A["rf_dac"]] = 300
        p.sims["COM11"].out = True
        _set_gas(p)
        p.sims["COM13"].out = True
        time.sleep(12.0)
        for k in ("ALL_STOP", "PROCESS_STOP", "HEATER_STOP"):
            assert s.core.command(k, {}, source=Source.WEB).reason == "BENCH_READ_ONLY"
        time.sleep(2.0)
    finally:
        app.shutdown()
    assert p.writes() == [] and p.sims["COM9"].write_funcs_seen() == [] and p.sims["COM13"].set_commands_seen() == []
    log = (tmp_path / "data" / "logs" / "chk_v2.log").read_text(encoding="utf-8")
    bad = [ln for ln in log.splitlines() if " ERROR " in ln and ("BENCH_READ_ONLY" in ln or "조치 실패" in ln or "→ error" in ln)]
    assert bad == []
