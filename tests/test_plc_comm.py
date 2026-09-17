# -*- coding: utf-8 -*-
"""T3 폴링 블로킹 상한 / T4 cleanup 상한 / T5 잔존 RUN / T15~T19 링크업·정책·pending OFF — 가짜 minimalmodbus.Instrument."""
import threading
import time

import pytest

import device.PLC as PLCM
from lib import config as CFG


class FakeSerial:
    def __init__(self, close_sleep=0.0):
        self.is_open = True
        self.close_sleep = close_sleep
        self.close_started = threading.Event()

    def close(self):
        self.close_started.set()
        if self.close_sleep:
            time.sleep(self.close_sleep)
        self.is_open = False

    def reset_input_buffer(self):
        pass


class FakeInstrument:
    """시나리오: fail_reads(모든 read 가 0.5초 자고 예외) / fail_writes(모든 write 예외) / run_bit / pv_raw / write_bit_fail_n."""
    scenario = {}
    ctor_fail = False

    def __init__(self, *a, **k):
        if FakeInstrument.ctor_fail:
            raise OSError("could not open port")
        self.serial = FakeSerial(FakeInstrument.scenario.get("close_sleep", 0.0))
        self.calls = []
        self.close_port_after_each_call = False
        self.clear_buffers_before_each_transaction = True
        self.handle_local_echo = False
        FakeInstrument.scenario.setdefault("instances", []).append(self)

    def _maybe_fail(self):
        if FakeInstrument.scenario.get("fail_reads"):
            time.sleep(0.5)
            raise OSError("No communication with the instrument (no answer)")

    def read_bits(self, addr, count, functioncode=1):
        self.calls.append(("read_bits", addr, count)); self._maybe_fail()
        if addr == CFG.HEATER_COIL_BASE:
            bits = [0] * count
            bits[CFG.HEATER_COIL_RUN - CFG.HEATER_COIL_BASE] = int(FakeInstrument.scenario.get("run_bit", 0))
            bits[1] = 1   # ITL
            bits[CFG.HEATER_COIL_PV_SEL_EFF - CFG.HEATER_COIL_BASE] = int(FakeInstrument.scenario.get("pv_sel_eff_bit", 0))
            return bits
        return [0] * count

    def read_registers(self, addr, count, functioncode=3):
        self.calls.append(("read_registers", addr, count)); self._maybe_fail()
        regs = [0] * count
        if addr == CFG.HEATER_REG_BLOCK_START:
            regs[CFG.HEATER_REG_PV - addr] = int(FakeInstrument.scenario.get("pv_raw", 0))
            regs[CFG.HEATER_REG_SV - addr] = int(FakeInstrument.scenario.get("sv_raw", 0))
            pv2 = int(FakeInstrument.scenario.get("pv2_raw", -1))
            regs[CFG.HEATER_REG_PV2 - addr] = pv2 & 0xFFFF
            regs[CFG.HEATER_REG_SV2_MAX - addr] = int(FakeInstrument.scenario.get("sv2_max_raw", 0))
            regs[CFG.HEATER_REG_SV2 - addr] = int(FakeInstrument.scenario.get("sv2_raw", 0))
            regs[CFG.HEATER_REG_OT2_LIMIT - addr] = int(FakeInstrument.scenario.get("ot2_raw", 11500))
        return regs

    def read_register(self, addr, dec=0, functioncode=3, signed=False):
        self.calls.append(("read_register", addr)); self._maybe_fail()
        return 0

    def _maybe_fail_write(self):
        if FakeInstrument.scenario.get("fail_writes"):
            raise OSError("No communication with the instrument (no answer)")

    def write_bit(self, addr, value, functioncode=5):
        self.calls.append(("write_bit", addr, int(value))); self._maybe_fail_write()
        n = FakeInstrument.scenario.get("write_bit_fail_n", 0)
        if n > 0:
            FakeInstrument.scenario["write_bit_fail_n"] = n - 1
            raise OSError("write failed")
        if FakeInstrument.scenario.get("write_bit_fail_always"):
            raise OSError("write failed")

    def write_register(self, addr, value, functioncode=6):
        self.calls.append(("write_register", addr, int(value))); self._maybe_fail_write()

    def write_bits(self, addr, values):
        self.calls.append(("write_bits", addr, list(values))); self._maybe_fail_write()


@pytest.fixture
def plc(qapp, monkeypatch):
    FakeInstrument.scenario = {}
    FakeInstrument.ctor_fail = False
    monkeypatch.setattr(PLCM.minimalmodbus, "Instrument", FakeInstrument)
    monkeypatch.setattr(PLCM, "PLC_RETRY_DELAY_MS", 0)
    c = PLCM.PLCController()
    msgs = []
    c.status_message.connect(lambda l, m: msgs.append((l, m)))
    c._msgs = msgs
    c.start_polling()
    c.polling_timer.stop(); c._heater_wd_timer.stop()
    return c


def test_T3_poll_blocking_bound_and_link_down(plc, monkeypatch):
    FakeInstrument.scenario["fail_reads"] = True
    ticks = []
    orig = plc._comm_budget_tick
    monkeypatch.setattr(plc, "_comm_budget_tick", lambda ex, what: (ticks.append(what), orig(ex, what)))
    shots = []
    monkeypatch.setattr(PLCM.QTimer, "singleShot", staticmethod(lambda ms, fn: shots.append((ms, fn))))

    inst = plc.instrument
    t0 = time.monotonic()
    plc._poll_status()
    took = time.monotonic() - t0
    assert took < 2.0, f"_poll_status {took:.2f}s"
    assert len(ticks) == 1
    assert plc._link_down is True
    assert shots and shots[-1][0] == 1000

    # 두 번째 폴링부터는 링크 다운 → 트랜잭션 0회
    n_calls = len(inst.calls)
    plc._poll_status()
    assert len(inst.calls) == n_calls
    assert len(ticks) == 2                       # 예산 판정은 계속 돈다

    # 재연결 예약: 1000 → 2000 → 4000 (포트가 계속 안 열린다)
    FakeInstrument.ctor_fail = True
    delays = [shots[-1][0]]
    for _ in range(2):
        fn = shots[-1][1]; fn()
        delays.append(shots[-1][0])
    assert delays == [1000, 2000, 4000]
    assert plc._link_down is True
    # 포트가 돌아오면 성공 → 링크 복구, 정책 리셋
    FakeInstrument.ctor_fail = False
    FakeInstrument.scenario["fail_reads"] = False
    shots[-1][1]()
    assert plc._link_down is False and plc.instrument is not None
    assert any("재연결(" in m for _, m in plc._msgs)


def test_T4_cleanup_bounded_and_warns_when_off_unconfirmed(plc):
    FakeInstrument.scenario["write_bit_fail_always"] = True
    plc.instrument.serial.close_sleep = 5.0
    t0 = time.monotonic()
    plc.cleanup()
    took = time.monotonic() - t0
    assert took < 2.5, f"cleanup {took:.2f}s"
    assert any("종료 시 히터 OFF 미확인" in m for _, m in plc._msgs)
    assert plc.instrument is None


def test_T4_cleanup_normal_writes_run_off(plc):
    inst = plc.instrument
    plc.cleanup()
    assert ("write_bit", CFG.HEATER_COIL_RUN, 0) in inst.calls
    assert not any("미확인" in m for _, m in plc._msgs)


def test_T5_residual_run_cleared_in_order(plc):
    FakeInstrument.scenario.update(run_bit=1, pv_raw=2750, sv_raw=6000)
    inst = plc.instrument
    published = []; residual = []
    plc.update_heater_status.connect(lambda st: published.append(dict(st)))
    plc.heater_residual.connect(lambda d: residual.append(dict(d)))
    n0 = len(inst.calls)
    plc._poll_status()
    writes = [c for c in inst.calls[n0:] if c[0] in ("write_bit", "write_register")]
    assert writes[:2] == [("write_bit", CFG.HEATER_COIL_RUN, 0), ("write_register", CFG.HEATER_REG_SV, 2750)]
    assert published and published[-1]["run"] is False and published[-1].get("residual_run") is True
    assert residual == [{"pv": pytest.approx(275.0), "sv_old": pytest.approx(600.0)}]
    assert plc._heater_first_poll is False


def test_T5_residual_retry_when_first_write_fails(plc):
    # _mb 가 3회(1+PLC_RETRY_COUNT) 재시도하므로 3회 전부 실패해야 '이번 폴링 실패' 가 된다
    FakeInstrument.scenario.update(run_bit=1, pv_raw=2750, sv_raw=6000, write_bit_fail_n=3)
    inst = plc.instrument
    published = []; residual = []
    plc.update_heater_status.connect(lambda st: published.append(dict(st)))
    plc.heater_residual.connect(lambda d: residual.append(dict(d)))
    plc._poll_status()                     # write_bit 재시도 소진 → 발행 없음
    assert published == [] and residual == []
    assert plc._heater_first_poll is True
    assert any("이전 세션 RUN 정리 실패" in m for _, m in plc._msgs)
    plc._poll_status()                     # 다음 폴링에서 재시도 성공
    assert len(residual) == 1 and published[-1]["run"] is False
    wb = [c for c in inst.calls if c[0] == "write_bit"]
    assert len(wb) == 4                    # 3회 실패 + 다음 폴링 1회 성공


def _shots(monkeypatch):
    shots = []
    monkeypatch.setattr(PLCM.QTimer, "singleShot", staticmethod(lambda ms, fn: shots.append((ms, fn))))
    return shots


def _run_writes(inst):
    return [c for c in inst.calls if c[0] == "write_bit" and c[1] == CFG.HEATER_COIL_RUN]


def test_T15_policy_success_only_on_device_response(plc, monkeypatch):
    """포트가 열려도 장비가 침묵하면 링크 업이 아니고 정책도 리셋되지 않는다(백오프 계속 상승). 첫 응답에서 리셋."""
    shots = _shots(monkeypatch)
    FakeInstrument.scenario.update(fail_reads=True, fail_writes=True)   # 포트는 열리지만 장비 침묵
    plc._poll_status()                                   # 실패 → 링크 다운, 1000ms 예약
    assert shots[-1][0] == 1000
    shots[-1][1]()                                       # 재연결: 포트 열림 → 프로브 실패 → 링크 다운 유지, 2000
    assert plc._link_down is True and plc._policy.in_outage() and shots[-1][0] == 2000
    shots[-1][1]()
    assert plc._link_down is True and shots[-1][0] == 4000
    # 장비가 응답하면 그제야 리셋
    FakeInstrument.scenario.update(fail_reads=False, fail_writes=False)
    shots[-1][1]()
    plc._poll_status()
    assert plc._link_down is False and not plc._policy.in_outage()
    assert plc._policy.on_failure() == 1000


def test_T16_start_polling_open_failure_schedules_policy_not_error(qapp, monkeypatch):
    FakeInstrument.scenario = {}
    FakeInstrument.ctor_fail = True
    monkeypatch.setattr(PLCM.minimalmodbus, "Instrument", FakeInstrument)
    monkeypatch.setattr(PLCM, "PLC_RETRY_DELAY_MS", 0)
    shots = _shots(monkeypatch)
    c = PLCM.PLCController()
    msgs = []; events = []
    c.status_message.connect(lambda l, m: msgs.append((l, m)))
    monkeypatch.setattr(c, "_emit_event", lambda k, d="", lost=None: events.append((k, d)))
    c.start_polling()
    c.polling_timer.stop(); c._heater_wd_timer.stop()
    assert c._is_running is True and c._link_down is True and c._ever_connected is False
    assert not any("연결 실패" in m for _, m in msgs)
    assert any(l == "PLC(경고)" and "포트 열기 실패" in m and "OSError" in m for l, m in msgs)
    assert events and events[0][0] == "열기실패" and "could not open port" in events[0][1]
    assert shots and shots[-1][0] == 1000
    # 연결된 적 없으면 예산 판정·60초 알림 없음
    ticks = []
    monkeypatch.setattr(c, "_comm_budget_tick", lambda ex, what: ticks.append(what))
    c._disconnect_since = time.monotonic() - 120
    c._poll_status()
    assert ticks == [] and c._disconnect_notified is False
    # 포트가 돌아오면 _on_link_up(first=True): 설정 푸시 + 마커 + "연결 성공"
    FakeInstrument.ctor_fail = False
    shots[-1][1]()
    assert c._link_down is False and c._ever_connected is True
    assert any("연결 성공" in m for _, m in msgs)
    assert ("연결", f"{PLCM.PLC_PORT} ID={PLCM.PLC_SLAVE_ID}") in events
    inst = c.instrument
    assert any(cl[0] == "write_register" and cl[1] == PLCM.PLC_SESSION_MARK_REG for cl in inst.calls)
    assert any(cl[0] == "write_register" and cl[1] != PLCM.PLC_SESSION_MARK_REG for cl in inst.calls)  # 설정 푸시


def test_T17_link_up_single_path_on_reconnect(plc, monkeypatch):
    """재연결 성공도 _on_link_up 을 거친다: 설정 푸시 + 마커 + 재연결 로그, '연결 성공' 문구는 없다."""
    shots = _shots(monkeypatch)
    FakeInstrument.scenario["fail_reads"] = True
    plc._poll_status()
    FakeInstrument.scenario["fail_reads"] = False
    n_msgs = len(plc._msgs)
    shots[-1][1]()
    inst = plc.instrument
    assert any(cl[0] == "write_register" and cl[1] == PLCM.PLC_SESSION_MARK_REG for cl in inst.calls)
    assert any(cl[0] == "write_register" and cl[1] != PLCM.PLC_SESSION_MARK_REG for cl in inst.calls)
    new = plc._msgs[n_msgs:]
    assert any("재연결(" in m for _, m in new) and not any("연결 성공" in m for _, m in new)


def test_T18_pending_heater_off_applied_on_link_up(plc, monkeypatch):
    shots = _shots(monkeypatch)
    FakeInstrument.scenario["fail_reads"] = True
    plc._poll_status()
    assert plc._link_down is True
    plc.set_heater_run(True)                     # ON 은 보관하지 않는다
    assert plc._pending_heater_off is False
    plc.set_heater_run(False)                    # OFF 는 보류
    assert plc._pending_heater_off is True
    assert any("히터 OFF 를 보류" in m for _, m in plc._msgs)
    FakeInstrument.scenario["fail_reads"] = False
    shots[-1][1]()                               # 링크 업 → 1회 적용
    inst = plc.instrument
    assert _run_writes(inst) == [("write_bit", CFG.HEATER_COIL_RUN, 0)]
    assert plc._pending_heater_off is False
    assert any("보류된 히터 OFF 적용" in m for _, m in plc._msgs)
    plc._poll_status()                           # 이후 다시 쓰지 않는다
    assert len(_run_writes(inst)) == 1


def test_T18_pending_heater_off_on_write_failure_then_mb_recovery(plc):
    """링크는 살아 있는데 쓰기가 실패한 OFF 도 보류 → 다음 _mb 복구 분기에서 적용."""
    inst = plc.instrument
    FakeInstrument.scenario["write_bit_fail_n"] = 3
    plc.set_heater_run(False)
    assert plc._pending_heater_off is True and plc._comm_fail_streak == 1
    plc._poll_status()                           # 읽기 성공 → 복구 분기 → 보류 OFF 적용
    assert plc._pending_heater_off is False
    assert len(_run_writes(inst)) == 4           # 실패 3회(기록됨) + 복구 시 1회
    assert any("보류된 히터 OFF 적용" in m for _, m in plc._msgs)


def test_T19_open_failure_reason_logged_with_cadence(plc, monkeypatch):
    shots = _shots(monkeypatch)
    FakeInstrument.scenario["fail_reads"] = True
    plc._poll_status()
    FakeInstrument.ctor_fail = True
    events = []
    monkeypatch.setattr(plc, "_emit_event", lambda k, d="", lost=None: events.append((k, d)))
    logs = {"v": True}
    monkeypatch.setattr(plc._policy, "should_log", lambda: logs["v"])
    n = len(plc._msgs)
    shots[-1][1]()                               # should_log True → 상태 메시지
    logs["v"] = False
    shots[-1][1]()                               # should_log False → 메시지 없음, 이벤트는 매번
    warn = [m for l, m in plc._msgs[n:] if l == "PLC(경고)" and "포트 열기 실패" in m]
    assert len(warn) == 1 and "could not open port" in warn[0]
    assert [k for k, _ in events].count("열기실패") == 2


def _silent(plc, monkeypatch):
    """T22 준비: 폴링 실패로 링크 다운 → 이후 시나리오는 '포트 열림 + 전부 무응답'."""
    shots = _shots(monkeypatch)
    steps = []
    orig = plc._policy_failure_step
    monkeypatch.setattr(plc, "_policy_failure_step", lambda r: (steps.append(r), orig(r)))
    alerts = []
    plc.comm_long_outage.connect(lambda d, lost: alerts.append((d, lost)))
    return shots, steps, alerts


def test_T22_silent_plc_port_opens_but_no_link_up(plc, monkeypatch):
    shots, steps, alerts = _silent(plc, monkeypatch)
    FakeInstrument.scenario.update(fail_reads=True, fail_writes=True)
    plc._poll_status()
    n_msg = len(plc._msgs)
    t0 = time.monotonic()
    shots[-1][1]()                                       # 재연결 1회
    took = time.monotonic() - t0
    assert took < 2.5, f"_reconnect_attempt {took:.2f}s"
    assert plc._link_down is True
    new = [m for _, m in plc._msgs[n_msg:]]
    assert not any("재연결(" in m or "연결 성공" in m for m in new)
    assert any("무응답" in m for m in new)
    inst = FakeInstrument.scenario["instances"][-1]
    assert not any(c[0] == "write_register" for c in inst.calls)     # 설정 푸시 0회
    assert steps == ["통신 실패", "무응답"]
    delays = [shots[-1][0]]
    shots[-1][1](); delays.append(shots[-1][0])
    shots[-1][1](); delays.append(shots[-1][0])
    assert delays == [2000, 4000, 8000]
    # 600초 뒤 장기 두절 알림 정확히 1회, 이후 60000 고정
    base = plc._policy._now
    monkeypatch.setattr(plc._policy, "_now", lambda: base() + 600.0)
    shots[-1][1]()
    assert [d for d, _ in alerts] == ["PLC"] and shots[-1][0] == 60000
    shots[-1][1]()
    assert len(alerts) == 1 and shots[-1][0] == 60000


def test_T23_long_outage_alert_single_path_on_open_failure(plc, monkeypatch):
    shots, steps, alerts = _silent(plc, monkeypatch)
    FakeInstrument.scenario["fail_reads"] = True
    plc._poll_status()
    FakeInstrument.ctor_fail = True
    shots[-1][1](); shots[-1][1]()
    assert steps == ["통신 실패", "열기 실패", "열기 실패"]
    base = plc._policy._now
    monkeypatch.setattr(plc._policy, "_now", lambda: base() + 600.0)
    shots[-1][1](); shots[-1][1]()
    assert [d for d, _ in alerts] == ["PLC"] and shots[-1][0] == 60000


# ───────────────────────── TC2 추종: 레지스터/코일/OT2/슬롯 ─────────────────────────
def _last_heater_st(plc):
    got = []
    plc.update_heater_status.connect(lambda st: got.append(dict(st)))
    plc._poll_status()
    return got[-1]


def test_T31_poll_reads_27_regs_12_coils_and_pv2_minus1_is_none(plc):
    inst = plc.instrument
    FakeInstrument.scenario.update(pv_raw=2750, pv2_raw=-1, sv2_max_raw=11000, ot2_raw=11500, pv_sel_eff_bit=1)
    st = _last_heater_st(plc)
    assert ("read_registers", CFG.HEATER_REG_BLOCK_START, 27) in inst.calls
    assert ("read_bits", CFG.HEATER_COIL_BASE, 12) in inst.calls
    assert st["pv2"] is None and st["pv_sel_eff"] is True and st["pv_sel"] is False
    assert st["sv2_max"] == pytest.approx(1100.0) and st["ot2_limit"] == pytest.approx(1150.0)
    FakeInstrument.scenario["pv2_raw"] = 10523
    st = _last_heater_st(plc)
    assert st["pv2"] == pytest.approx(1052.3)


def test_T32_ot2_pushed_and_read_back(plc):
    inst = plc.instrument
    raw = round(PLCM.HEATER_OT2_LIMIT_C * 10)
    assert ("write_register", CFG.HEATER_REG_OT2_LIMIT, raw) in inst.calls
    rb = [c for c in inst.calls if c[0] == "read_registers" and c[1] <= CFG.HEATER_REG_SV_LIMIT
          and c[1] + c[2] - 1 >= CFG.HEATER_REG_OT2_LIMIT]
    assert rb, "되읽기 범위가 D00034 를 포함해야 한다"
    assert any("OT2 1150.0°C" in m for _, m in plc._msgs)


def test_T33_sv2_and_pv_sel_slots(plc):
    inst = plc.instrument
    plc.set_heater_sv2(1052.34)
    assert inst.calls[-1] == ("write_register", CFG.HEATER_REG_SV2, 10523)
    assert any("TC2 목표 D00035 ← 1052.3°C" in m for _, m in plc._msgs)
    plc.set_heater_sv2(99999.0)                       # 클램프
    assert inst.calls[-1] == ("write_register", CFG.HEATER_REG_SV2, 32767)
    plc.set_heater_pv_sel(True)
    assert inst.calls[-1] == ("write_bit", CFG.HEATER_COIL_PV_SEL, 1)
    plc.set_heater_pv_sel(False)
    assert inst.calls[-1] == ("write_bit", CFG.HEATER_COIL_PV_SEL, 0)
    assert any("M0004A ← ON" in m for _, m in plc._msgs) and any("M0004A ← OFF" in m for _, m in plc._msgs)
    # 링크 다운이면 트랜잭션 0회
    plc._link_down = True
    n = len(inst.calls)
    plc.set_heater_sv2(500.0); plc.set_heater_pv_sel(True)
    assert len(inst.calls) == n


# ───────────────────────── plc_link 전이 ─────────────────────────
def test_T38_plc_link_emits_once_per_transition(plc, monkeypatch):
    links = []
    plc.plc_link.connect(lambda up: links.append(up))
    shots = _shots(monkeypatch)
    FakeInstrument.scenario["fail_reads"] = True
    plc._poll_status()                                   # 다운 1회
    plc._poll_status(); plc._poll_status()               # 연속 실패에도 추가 emit 없음
    FakeInstrument.ctor_fail = True
    shots[-1][1](); shots[-1][1]()                        # 열기 실패 반복
    assert links == [False]
    FakeInstrument.ctor_fail = False
    FakeInstrument.scenario.update(fail_reads=True, fail_writes=True)
    shots[-1][1]()                                        # 포트 열림 + 프로브 실패 → 여전히 다운
    assert links == [False]
    FakeInstrument.scenario.update(fail_reads=False, fail_writes=False)
    shots[-1][1]()                                        # 프로브 성공 → 업 1회
    assert links == [False, True]
    plc._poll_status()
    assert links == [False, True]


def test_T38_start_polling_first_link_up(qapp, monkeypatch):
    FakeInstrument.scenario = {}; FakeInstrument.ctor_fail = False
    monkeypatch.setattr(PLCM.minimalmodbus, "Instrument", FakeInstrument)
    c = PLCM.PLCController()
    links = []
    c.plc_link.connect(lambda up: links.append(up))
    c.start_polling(); c.polling_timer.stop(); c._heater_wd_timer.stop()
    assert links == [True]


def test_T39_first_poll_after_link_up_republishes_unchanged_buttons(plc, monkeypatch):
    shots = _shots(monkeypatch)
    pub = []
    plc.update_button_display.connect(lambda n, v: pub.append((n, v)))
    plc._poll_status()                                   # 정상 폴링 — 모든 버튼 발행(첫 폴링)
    n_all = len(pub); assert n_all >= len(CFG.PLC_COIL_MAP)
    pub.clear()
    plc._poll_status()                                   # 값이 안 바뀜 → 발행 0
    assert pub == []
    FakeInstrument.scenario["fail_reads"] = True
    plc._poll_status()                                   # 다운
    FakeInstrument.scenario["fail_reads"] = False
    shots[-1][1]()                                       # 링크 업 → 캐시 비움
    pub.clear()
    plc._poll_status()
    assert len(pub) == n_all                             # 값이 그대로여도 전부 다시 발행
    assert ("Door_Button", False) in pub
