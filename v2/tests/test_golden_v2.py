"""골든 합격 세트(01~05, 08, 17, 18, 23, 24, 26) — v1 기록과 v2 시험대 결과를 단위별로 비교(golden_v2).

의도된 차이(INTENDED_DIFFS)는 근거와 함께 기대값을 바꾼다. 그 밖의 차이는 실패.
"""
import pytest

from v2.tests import golden_v2 as G

NAMES = sorted(n for n in G.SCENARIOS if n[:2] in G.SET)


def test_every_v1_golden_in_set_has_a_v2_script():
    files = sorted(p.stem for p in G.GOLDEN_DIR.glob("*.json") if p.stem[:2] in G.SET)
    assert files == NAMES


def test_intended_diffs_have_decisions_and_are_used():
    for d in G.INTENDED_DIFFS:
        assert d.decision and d.why, d.id
        if d.golden:
            assert any(d.applies(n) for n in NAMES), d.id
        else:                                   # 근거 기록용 — 어떤 시나리오의 기대값도 바꾸지 않는다
            assert not any(d.applies(n) for n in NAMES), d.id
    ids = {d.id for d in G.INTENDED_DIFFS}
    assert not {"ALL_STOP_KEEP_PUMPS", "REMOTE_PLC_LOCK"} & ids                 # 사용자 결정 2026-10-20 — v1 과 같게


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.slow
def test_golden(name, tmp_path):
    r = G.run_scenario(name, tmp_path)
    assert r.status in ("pass", "intended"), "\n".join([f"{name}: {r.status} {r.intended}"] + r.diff)
