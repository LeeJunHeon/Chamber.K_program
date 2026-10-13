# -*- coding: utf-8 -*-
"""공정 흐름 — 이온 게이지 베이스 압력 대기(IG_FILAMENT_ON / IG_BASE_WAIT / IG_FILAMENT_OFF).
가짜 스냅샷·가짜 시계·수동 틱으로 장비 없이 판정 규칙을 확인한다."""
import itertools
import time
from unittest.mock import MagicMock

import pytest

import controller.process_controller as PC
from controller.process_controller import ActionType, SputterProcessController
from device.IonGauge import IgSnapshot, COMM_TEXT, parse_status
from test_ion_gauge import e_reply, E_ITEMS

_seq = itertools.count(1)


def pair(v):
    """Torr → 934 '가수 공백 지수'(가수 2자리)."""
    if not v:
        return "0       -4"
    m, e = f"{v:.1e}".split("e")
    return f"{int(round(float(m) * 10))}      {int(e) - 1}"


def snap(ion=4.0e-6, fil=1, emis=1, settle_ago=30.0, comm="ok", fresh=True, code=0):
    items = list(E_ITEMS)
    items[1], items[2], items[4], items[23] = str(fil), str(emis), str(code), pair(ion)
    r = parse_status(e_reply(items), t=time.time())
    return IgSnapshot(comm=comm, comm_text=COMM_TEXT[comm], detail="", reading=r, fresh=fresh,
                      last_ok_wall=time.time(), t=float(next(_seq)),
                      emission_since=(time.time() - settle_ago) if (fil and emis) else None)


@pytest.fixture
def pc(qapp, monkeypatch):
    c = SputterProcessController(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
    c._running = True
    c._ig_tick_ms = 10_000_000                      # 수동 틱
    rec = {"on": [], "off": [], "msg": [], "crit": [], "stage": [], "reached": [], "next": [], "stop": []}
    c.request_ig_filament_on.connect(lambda: rec["on"].append(1))
    c.request_ig_filament_off.connect(lambda: rec["off"].append(1))
    c.status_message.connect(lambda lv, m: rec["msg"].append((lv, m)))
    c.critical_error.connect(lambda m: rec["crit"].append(m))
    c.stage_monitor.connect(lambda m: rec["stage"].append(m))
    c.ig_base_reached.connect(lambda v: rec["reached"].append(v))
    monkeypatch.setattr(c, "_next_step", lambda: rec["next"].append(1))
    monkeypatch.setattr(c, "request_stop", lambda: rec["stop"].append(1))
    clock = [1000.0]
    c._now = lambda: clock[0]
    box = {"snap": None}
    c.set_ig_snapshot_provider(lambda: box["snap"])
    c.box, c.clock, c.rec = box, clock, rec
    yield c
    c._ig_cancel(send_off=False)


def tick(c, s=None, dt=1.0):
    c.clock[0] += dt
    if s is not None:
        c.box["snap"] = s
    c._on_ig_tick()


def msgs(c):
    return [m for _, m in c.rec["msg"]]


# ───────────── 스텝 위치 ─────────────
BASE_P = dict(use_ar_gas=True, use_o2_gas=False, ar_flow=20.0, o2_flow=0.0, sp1_set=5.0,
              process_time=1.0, shutter_delay=0.0, dc_power=0.0, rf_power=0.0)


def _steps(c, **over):
    p = dict(BASE_P, **over)
    return [(s.action, s.message) for s in c._build_steps(p)]


def test_steps_without_heater_follow_gas_pre(pc):
    st = _steps(pc, base_pressure=1e-5)
    names = [m for _, m in st]
    i = names.index("이온 게이지 필라멘트 ON")
    assert names[i - 1] == "MFC Valve Open"
    assert [a for a, _ in st[i:i + 3]] == [ActionType.IG_FILAMENT_ON, ActionType.IG_BASE_WAIT, ActionType.IG_FILAMENT_OFF]
    assert names[i + 1] == "베이스 압력 대기 (목표 1.0E-05 Torr)"
    assert names[i + 3] == "Ch1 ZEROING"                    # build_gas_intro_steps 바로 앞


def test_steps_with_heater_follow_heater_wait(pc):
    st = _steps(pc, base_pressure=2e-6, use_heater=True, heater_temp=300.0, heater_ramp=12.0)
    acts = [a for a, _ in st]
    i = acts.index(ActionType.IG_FILAMENT_ON)
    assert acts[i - 1] == ActionType.HEATER_WAIT
    assert acts[i:i + 3] == [ActionType.IG_FILAMENT_ON, ActionType.IG_BASE_WAIT, ActionType.IG_FILAMENT_OFF]
    assert st[i + 3][1] == "Ch1 ZEROING"


def test_missing_base_pressure_uses_default(pc):
    st = _steps(pc)                                          # 열 없는 예전 레시피
    assert "베이스 압력 대기 (목표 1.0E-05 Torr)" in [m for _, m in st]


def test_no_steps_when_ig_off_or_zero(pc):
    pc.set_ig_snapshot_provider(None)
    assert not any(a.name.startswith("IG_") for a, _ in _steps(pc, base_pressure=1e-5))
    assert ("경고", "베이스 압력 대기 생략 — 이온 게이지 꺼짐(IG_ENABLED=false 또는 IG_PORT 없음)") in pc.rec["msg"]
    pc.set_ig_snapshot_provider(lambda: None)
    assert not any(a.name.startswith("IG_") for a, _ in _steps(pc, base_pressure=0))
    assert ("경고", "베이스 압력 대기 생략 — base_pressure 0") in pc.rec["msg"]


# ───────────── 필라멘트 ON / OFF 스텝 ─────────────
def test_filament_already_on_is_not_sent(pc):
    pc.box["snap"] = snap()
    pc._ig_filament_on_step()
    assert pc.rec["on"] == [] and pc.rec["next"] == [1]
    assert "[IG] 필라멘트 이미 켜짐·방출 정상 — 켜기 생략" in msgs(pc)


def test_filament_on_success_and_failure(pc):
    pc.box["snap"] = snap(fil=0, emis=0)
    pc._ig_filament_on_step()
    assert pc.rec["on"] == [1] and pc._ig_fil_on_by_us
    pc._on_ig_filament_result("on", True, "필라멘트 켜짐·방출 정상")
    assert pc.rec["next"] == [1]
    pc._ig_filament_on_step()
    pc._on_ig_filament_result("on", False, "20초 안에 필라멘트·방출 확인 안 됨 — 오류 코드 7: 필라멘트 전류 없음(필라멘트·케이블 단선 가능)")
    assert pc.rec["stop"] == [1] and "오류 코드 7" in pc.rec["crit"][-1] and "단선" in pc.rec["crit"][-1]


def test_filament_result_guard_when_no_result(pc, monkeypatch):
    monkeypatch.setattr(PC, "IG_RESULT_MARGIN_SEC", 0.0)
    pc.box["snap"] = snap(fil=0, emis=0)
    pc._ig_filament_on_step()
    pc._on_ig_guard_timeout()
    assert pc.rec["stop"] == [1] and "필라멘트 결과가 오지 않음" in pc.rec["crit"][-1]


def test_filament_off_failure_only_warns(pc):
    pc._ig_filament_off_step()
    assert pc.rec["off"] == [1] and not pc._ig_fil_on_by_us
    pc._on_ig_filament_result("off", False, "20초 안에 필라멘트 꺼짐 확인 안 됨")
    assert pc.rec["next"] == [1] and pc.rec["stop"] == []
    assert any(lv == "경고" and "필라멘트 끄기 실패(공정 계속" in m for lv, m in pc.rec["msg"])


# ───────────── 베이스 대기 판정 ─────────────
def test_measured_ranging_values_before_settle_are_not_used(pc):
    """실측 순서(0E-4 → 8E-4 → 9.9E-4 → 9.9E-5 → 9.9E-6 → 4.0E-6) — 목표가 1E-3 이어도 안정화 전 값으로는 통과하지 않는다."""
    pc._ig_base_wait_start(1e-3)
    assert msgs(pc)[-1] == "[IG] 베이스 대기 시작 목표 1.0E-03 Torr"
    for k, v in enumerate([0, 8e-4, 9.9e-4, 9.9e-5, 9.9e-6, 4.0e-6, 4.0e-6, 4.0e-6, 4.0e-6], start=1):
        tick(pc, snap(ion=v, settle_ago=float(k)))           # 방출 뒤 1~9초
    assert pc.rec["reached"] == [] and pc._ig_wait["count"] == 0
    for _ in range(4):
        tick(pc, snap(ion=4.0e-6, settle_ago=11.0))
    assert pc.rec["reached"] == []
    tick(pc, snap(ion=4.0e-6, settle_ago=12.0))
    assert pc.rec["reached"] == [pytest.approx(4.0e-6)] and pc.rec["next"] == [1]
    assert pc.ig_base_pressure_used == pytest.approx(4.0e-6)
    assert msgs(pc)[-1] == "[IG] 베이스 도달 4.0E-06 Torr (대기 00:14)"


def test_five_in_a_row_reset_by_high_or_unusable(pc):
    pc._ig_base_wait_start(1e-5)
    for _ in range(4):
        tick(pc, snap(ion=5e-6))
    tick(pc, snap(ion=2e-5))                                 # 목표 초과 → 0
    for _ in range(4):
        tick(pc, snap(ion=5e-6))
    tick(pc, snap(ion=5e-6, fresh=False, comm="timeout"))    # 판정에 쓸 수 없는 값 → 0
    for _ in range(4):
        tick(pc, snap(ion=5e-6))
    assert pc.rec["reached"] == []
    tick(pc, snap(ion=5e-6))
    assert pc.rec["reached"] == [pytest.approx(5e-6)]


def test_same_snapshot_is_not_counted_twice(pc):
    pc._ig_base_wait_start(1e-5)
    s = snap(ion=5e-6)
    for _ in range(8):
        tick(pc, s)
    assert pc._ig_wait["count"] == 1 and pc.rec["reached"] == []


def test_stale_or_ion_off_not_used(pc):
    pc._ig_base_wait_start(1e-5)
    old = snap(ion=5e-6)
    old.last_ok_wall = time.time() - 3.0                     # 2초보다 오래된 값
    tick(pc, old)
    tick(pc, snap(ion=0))                                    # 이온 0(꺼짐) → 압력 없음
    assert pc._ig_wait["count"] == 0


def test_timeout_aborts_with_target_last_and_elapsed(pc, monkeypatch):
    monkeypatch.setattr(PC, "IG_BASE_WAIT_TIMEOUT_SEC", 600)
    pc._ig_base_wait_start(1e-5)
    tick(pc, snap(ion=3.2e-5))
    tick(pc, snap(ion=3.3e-5), dt=600.0)
    assert pc.rec["stop"] == [1]
    reason = pc.rec["crit"][-1]
    assert "베이스 압력 대기 시간 초과" in reason and "목표 1.0E-05 Torr" in reason
    assert "마지막 3.3E-05 Torr" in reason and "경과 10:01" in reason
    assert any(m.startswith("[IG] 베이스 대기 실패 — 베이스 압력 대기 시간 초과") for m in msgs(pc))


def test_comm_lost_over_60s_aborts(pc):
    pc._ig_base_wait_start(1e-5)
    lost = snap(ion=5e-6, fresh=False, comm="timeout")
    tick(pc, lost, dt=30.0)
    tick(pc, snap(ion=5e-6, fresh=False, comm="starting"), dt=30.0)   # 시동 중은 끊김이 아니다
    tick(pc, lost, dt=59.0)
    assert pc.rec["stop"] == []
    tick(pc, lost, dt=2.0)
    assert pc.rec["stop"] == [1] and "이온 게이지 통신 60초 넘게 끊김" in pc.rec["crit"][-1]


def test_filament_off_during_wait_relights_twice_then_aborts(pc):
    pc._ig_base_wait_start(1e-5)
    for n in (1, 2):
        tick(pc, snap(fil=0, emis=0, code=3))
        assert pc.rec["on"] == [1] * n and pc._ig_wait["paused"]
        assert any(f"재점등 {n}/2" in m for m in msgs(pc))
        tick(pc, snap(ion=5e-6))                             # 결과 대기 중엔 판정하지 않음
        assert pc._ig_wait["count"] == 0
        pc._on_ig_filament_result("on", True, "필라멘트 켜짐·방출 정상")
        assert not pc._ig_wait["paused"]
    tick(pc, snap(fil=0, emis=0, code=3))
    assert pc.rec["on"] == [1, 1] and pc.rec["stop"] == [1]
    assert "재점등 2회 뒤에도 유지 안 됨" in pc.rec["crit"][-1] and "오류 코드 3" in pc.rec["crit"][-1]


def test_relight_result_failure_retries_then_aborts(pc):
    pc._ig_base_wait_start(1e-5)
    tick(pc, snap(fil=0, emis=0))
    pc._on_ig_filament_result("on", False, "오류 코드 7: 필라멘트 전류 없음(필라멘트·케이블 단선 가능)")
    assert pc.rec["on"] == [1, 1] and pc.rec["stop"] == []
    pc._on_ig_filament_result("on", False, "오류 코드 7: 필라멘트 전류 없음(필라멘트·케이블 단선 가능)")
    assert pc.rec["stop"] == [1] and "필라멘트 재점등 2회 실패" in pc.rec["crit"][-1]


def test_progress_every_30s(pc):
    pc._ig_base_wait_start(1e-5)
    tick(pc, snap(ion=3.2e-5), dt=29.0)
    assert pc.rec["stage"] == []
    tick(pc, snap(ion=3.2e-5), dt=1.0)
    want = "베이스 대기 3.2E-05 → 목표 1.0E-05 (00:30 / 10:00)"
    assert pc.rec["stage"] == [want] and want in msgs(pc)


# ───────────── STOP·중단 ─────────────
def test_stop_sends_b_once_if_we_turned_it_on(pc):
    pc.box["snap"] = snap(fil=0, emis=0)
    pc._ig_filament_on_step()
    pc._on_ig_filament_result("on", True, "ok")
    pc._ig_base_wait_start(1e-5)
    assert pc._ig_timer.isActive()
    pc._running = False                                      # 이미 정지 상태 경로(빠름) — IG 정리는 그 앞에서 한다
    pc._stop_impl()
    pc._stop_impl()
    assert pc.rec["off"] == [1] and not pc._ig_timer.isActive() and pc._ig_wait == {}
    assert "[IG] 공정 종료 — 필라멘트 OFF 1회 요청(결과를 기다리지 않음)" in msgs(pc)


def test_stop_without_our_filament_sends_nothing(pc):
    pc.box["snap"] = snap()
    pc._ig_filament_on_step()                                # 이미 켜져 있어 보내지 않음
    pc._running = False
    pc._stop_impl()
    assert pc.rec["off"] == []


def test_abort_path_reaches_stop(pc):
    pc._ig_base_wait_start(1e-5)
    pc._ig_base_fail("시험 사유")
    assert pc.rec["stop"] == [1] and pc.rec["crit"][-1].endswith("시험 사유") and pc._ig_wait == {}


def test_filament_switching_is_not_comm_loss(pc):
    pc._ig_base_wait_start(1e-5)
    tick(pc, snap(ion=5e-6, fresh=False, comm="stopping"), dt=50.0)
    tick(pc, snap(ion=5e-6, fresh=False, comm="starting"), dt=50.0)
    assert pc.rec["stop"] == []
    assert PC.IG_OFF_RESULT_WAIT_SEC == 30.0                # OFF 결과 대기 = 드라이버 20초 + 여유
