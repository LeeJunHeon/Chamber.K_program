"""장비 사실 표 시험 — 빠진 칸 없음, 근거 표기, 주소·예산 일관성."""
import re

from v2.chk import devices as D


def test_devices_complete():
    keys = [d.key for d in D.DEVICES]
    assert len(keys) == len(set(keys)) == 6
    for d in D.DEVICES:
        assert d.manual in D.MANUALS
        for f in (d.model, d.serial, d.framing, d.timing_manual, d.errors, d.safe_off, d.comm_loss, d.interlock):
            assert f.strip(), (d.key, f)
        assert d.commands and d.bench
        for c in d.commands:
            assert c.cmd and c.meaning and c.source
        for x in d.diffs:
            assert x.item and x.v1 and x.manual and x.action and x.source


def test_manual_page_refs_present():
    """매뉴얼 근거가 있는 명령에는 [장비 p.N] 표기가 있다."""
    pat = re.compile(r"\[(IG|MFC|DC|RF|CESAR) p\.\d+")
    for d in (D.MFC, D.DC, D.RFPULSE, D.IG):
        assert any(pat.search(c.source) for c in d.commands), d.key


def test_plc_addrs_unique():
    coils = [a.addr for a in D.PLC_ADDRS if a.kind == "coil"]
    regs = [a.addr for a in D.PLC_ADDRS if a.kind == "reg"]
    names = [a.name for a in D.PLC_ADDRS]
    assert len(coils) == len(set(coils))
    assert len(regs) == len(set(regs))
    assert len(names) == len(set(names))
    assert all(a.access in ("R", "W", "RW") for a in D.PLC_ADDRS)


def test_plc_block_reads_cover_addrs():
    """TICK_PLAN 의 PLC 블록(FC1 0..95, 160..182, FC3 0..60)이 읽어야 할 주소를 모두 덮는다."""
    readable = [a for a in D.PLC_ADDRS if "R" in a.access]
    for a in readable:
        if a.kind == "coil":
            assert 0 <= a.addr <= 95 or 160 <= a.addr <= 182 or a.addr == 320, a
        else:
            assert 0 <= a.addr <= 60, a


def test_session_marker_default_free():
    assert 60 not in D.PLC_D_USED


def test_tick_plan_covers_devices():
    assert {t.device for t in D.TICK_PLAN} == {d.key for d in D.DEVICES}
    for t in D.TICK_PLAN:
        assert t.fits in ("yes", "bench", "no")
        assert t.line_ms < 900


def test_pc_dead_plan():
    ids = [m.id for m in D.PC_DEAD_PLAN]
    assert len(ids) == len(set(ids))
    assert {"PD1", "PD2", "PD3"} <= set(ids)
    assert any(m.needs_wiring for m in D.PC_DEAD_PLAN)


def test_pc_dead_plan_hold_decision():
    """사용자 결정(2026-10-15): PD1~PD6·PD9 보류, PD7·PD8 은 계획대로(prod 에서만 쓰고 되읽기)."""
    st = {m.id: m.status for m in D.PC_DEAD_PLAN}
    assert {k for k, v in st.items() if v == "hold"} == {"PD1", "PD2", "PD3", "PD4", "PD5", "PD6", "PD9"}
    assert st["PD7"] == st["PD8"] == "planned"
    assert "10 s 이상" in {m.id: m for m in D.PC_DEAD_PLAN}["PD1"].note
    assert D.PLC_CONNECTED_POWER == ("rf",)


def test_rf_scale_confirmed():
    from v2.chk import params as P
    p = P.by_key()
    assert p["RF_FWD_FULL_SCALE_W"].default == 594.5 and p["RF_REF_FULL_SCALE_W"].default == 200.0
    diff = [x for x in D.RF.diffs if x.item == "반사파 스케일"][0]
    assert "확인 완료" in diff.manual and "변경 금지" not in diff.action
    assert not any("전면 미터" in b for b in D.RF.bench)


def test_assumptions_in_bench_lists():
    """가정값으로 진행하는 데이터는 그 장비 bench 확인 목록에 빠짐없이 있다."""
    by = {d.key: d for d in D.DEVICES}
    for a in D.ASSUMPTIONS:
        assert any(a.bench_item in b for b in by[a.device].bench), (a.item, a.bench_item)
