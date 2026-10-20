"""5-3 전원 — v1 과 줄 단위로 비교한 검토에서 나온 차이의 재현 시험(고치기 전 실패 → 고친 뒤 통과).

- DC OFF 미확인 재시도 10 s → 2배 → 최대 COMM_OUTAGE_LOG_SEC(v1 DCpower.py:269-270·366-379)
- DC·RF Pulse 통신 두절 중단 뒤 링크 복구 → 공정이 없으면 OFF 다시(v1 main.py:1491-1497·1511-1517)
- DC 시작: 120 ms 간격, OUTP? 실패해도 제어 루프 시작(v1 DCpower.py:678-687·829-833·438-440)
- RF 이상 중단(S32·S33A·S33B)은 램프다운 뒤 OFF(v1 RFpower.py:100-102·124-131·158-167·210-214)
- 판정 구간: S30·S34C·S32 는 그 장비를 켠 뒤만, 반사파 대기 중 S33A·S33B 멈춤(v1 DCpower.py:242-243, RFpulse.py:984, RFpower.py:79-104)
- RF Pulse: 단계 재시도·CSR 5 백오프, RF ON 뒤 800 ms, OFF 의 CSR 1 → Host 뒤 다시, S34A 경계 ≥(v1 RFpulse.py:595-606·745-751·956·1245)
"""
from v2.chk.power_control import DcControl
from v2.chk.safety import View
from v2.tests.test_dc import Rig as DcRig
from v2.tests.test_rfpulse import Rig as RfpRig
from v2.tests.test_safety_engine import CFG, H


# ───────── DC ─────────
def _groups(times, gap=3.0):
    out = []
    for t in times:
        if out and t - out[-1][-1] < gap:
            out[-1].append(t)
        else:
            out.append([t])
    return out


def test_dc_off_unconfirmed_retries_with_backoff():
    r = DcRig()
    r.seq_wait(lambda on_done: r.d.start_output(600.0, 0.1, on_done=on_done))
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 30.0)
    assert r.seq_wait(r.d.output_off_confirmed) == "DC_OFF_UNCONFIRMED"
    r.c.wait_until(t + 90.0)
    offs = [tt for tt, d in r.sim.received if tt >= t and d.strip() == b"OUTP OFF"]
    g = _groups(offs)
    assert len(g) >= 3, g                                    # 처음 + 10 s 뒤 + 20 s 뒤(2배)
    assert 9.0 <= g[1][0] - g[0][-1] <= 12.5 and 19.0 <= g[2][0] - g[1][-1] <= 22.5, g
    ok = [e for e in r.events if e.kind == "off_confirmed"]
    assert ok and "미확인" in ok[-1].detail and r.d.off_retry_s is None and not r.sim.out
    r.done()


def test_dc_start_continues_when_outp_query_fails():
    r = DcRig()
    msgs = []
    dcc = DcControl(r.cfg, r.d, lambda lv, tx: msgs.append(tx))
    box = []
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t + 0.3, t + 8.0)        # OUTP ON 뒤 OUTP? 가 답하지 않음
    r.d.start_output(600.0, 0.1, on_done=lambda n, x, res: (box.append(x), r.c.notify()), on_on=dcc._output_on)
    r.c.wait_until(None, lambda: bool(box))
    assert box[0] is None and dcc.started_ok is True        # v1: 확인 없이 제어 루프 시작
    seen = [(tt, d.strip().decode()) for tt, d in r.sim.received if tt >= t]
    w = [x for x in seen if not x[1].endswith("?")][:4]
    assert [c for _, c in w][:2] == ["*RST", "*CLS"]
    assert all(b[0] - a[0] >= 0.119 for a, b in zip(w, w[1:])), w     # v1 _send 의 120 ms
    assert any(e.kind == "out_check" for e in r.events)
    r.done()


def test_dc_comm_loss_abort_then_link_back_turns_dc_off_again():
    h = H(state="MAIN", uses_dc=True, dc_on=True)
    for k in range(1, 12):
        h.feed("tick", k, {}, q={"dc": "fail"}, ages={"dc": float(k)})
    assert h.e.run_reason == "DC_COMM_LOST" and h.p.dc_comm_aborted
    h.settle()
    h.p.state = "IDLE"
    h.calls.clear()
    h.s.recover_after_comm_loss(h.view(30, {}))              # 링크 복구(dc ok) — 공정 없음
    assert [k for k, _ in h.calls] == ["DC_SAFE_OFF"] and not h.p.dc_comm_aborted
    h2 = H(state="MAIN", rfp_comm_aborted=True)
    h2.s.recover_after_comm_loss(h2.view(1, {}))             # 공정 중 — 생략하고 표시만 지움(v1)
    assert h2.calls == [] and not h2.p.rfp_comm_aborted


def test_comm_loss_and_reflected_only_after_output_on():
    for rid, block, proc in (("S30", "dc", dict(uses_dc=True)), ("S34C", "rfp", dict(uses_rfp=True))):
        h = H(state="POWER_WAIT", **proc)                    # 아직 출력 명령 전
        fired = [x for k in range(1, 15) for x in h.feed("tick", k, {}, q={block: "fail"}, ages={block: float(k)})]
        assert rid not in fired, rid
    h = H(state="GAS_STABILIZE", uses_rf=True)               # RF 를 켜기 전 반사파
    fired = [x for k in range(0, 70) for x in h.feed("tick", k, {"rf.ref_w": 5.0})]
    assert "S32" not in fired


def test_rf_no_output_and_deviation_paused_during_reflected_wait():
    h = H(state="POWER_WAIT", uses_rf=True, rf_on=True, rf_ref_waiting=True)
    fired = [x for k in range(0, 20) for x in h.feed("tick", k, {"rf.dac": 500, "rf.fwd_w": 0.0})]
    assert "S33A" not in fired
    h = H(state="MAIN", uses_rf=True, rf_on=True, rf_holding=True, rf_set_w=100.0, rf_ref_waiting=True)
    fired = [x for k in range(0, 20) for x in h.feed("tick", k, {"rf.fwd_w": 50.0})]
    assert "S33B" not in fired


def test_rf_abort_ramps_down_before_off():
    h = H(state="MAIN", uses_rf=True, rf_on=True)
    for k in range(0, 70):
        h.feed("tick", k, {"rf.ref_w": 5.0})
    h.settle()
    kinds = [k for k, _ in h.calls]
    assert h.e.run_reason == "RF_REFLECTED_HIGH"
    assert "RF_RAMP_DOWN" in kinds and kinds.index("RF_RAMP_DOWN") < kinds.index("RF_SAFE_OFF")   # 즉시 0 아님


# ───────── RF Pulse ─────────
def test_rfp_start_retries_after_no_response_and_csr5_backoff():
    r = RfpRig()
    r.sim.csr_once[8] = 5                                    # 셋포인트 CSR 5(User Port RF signal off) 한 번
    t = r.c.monotonic()
    assert r.start(200.0, freq_hz=10000, duty=40) is None and r.sim.out
    sp = [tt for tt, d in r.sim.received if tt >= t and len(d) > 1 and d[1] == 8]
    assert len(sp) == 2 and sp[1] - sp[0] >= 1.2             # v1 max(간격 × 1.5, 1200 ms)
    on = [tt for tt, d in r.sim.received if tt >= t and len(d) > 1 and d[1] == 2][-1]
    rb = [tt for tt, d in r.sim.received if tt >= t and len(d) > 1 and d[1] == 193][0]
    assert rb - on >= 0.8                                    # v1 RF ON 뒤 800 ms
    r.done()
    r = RfpRig()
    t = r.c.monotonic()
    r.sim.add_fault("no_response", t, t + 0.3)               # Host 첫 시도 무응답 — 다시
    assert r.start(200.0) is None and r.sim.out
    r.done()


def test_rfp_off_csr1_sets_host_then_off_again():
    r = RfpRig()
    assert r.start(200.0) is None and r.sim.out
    t = r.c.monotonic()
    r.sim.csr_once[1] = 1
    box = []
    r.d.rf_off_confirmed(on_done=lambda res: (box.append(res.status), r.c.notify()))
    r.c.wait_until(None, lambda: bool(box))
    cmds = [d[1] for tt, d in r.sim.received if tt >= t and len(d) > 1 and d[1] < 128]
    assert box == ["ok"] and cmds[:3] == [1, 14, 1] and not r.sim.out
    r.done()


def test_rfp_forward_deviation_boundary_is_inclusive():
    h = H(state="MAIN", uses_rfp=True, rfp_on=True, rfp_set_w=100.0)
    fired = [x for k in range(0, 20) for x in h.feed("tick", k, {"rfp.out_on": True, "rfp.fwd_w": 95.0})]
    assert "S34A" in fired                                   # |95 − 100| = 5 ≥ 5 %(v1 RFpulse.py:1245)
