"""pytest 공통 — 표식 slow: 골든 하니스·전체 공정 sim·bench 도구 시험대처럼 오래 걸리는 시험(사용자 결정 2026-10-20 ⑦).

전체 시험: python -m pytest v2\tests   (빠른 시험만: -m "not slow")

시험마다 만든 가상 시계를 시험이 끝나면 닫는다(_close_virtual_clocks) — 실패한 시험이 worker·정지 순서 스레드를 멈추지
못하고 끝나도 그 스레드가 다음 시험 중에 ClockDeadlock 경고(PytestUnhandledThreadExceptionWarning)를 남기지 않게.
"""
import pytest

from v2.common import clock as _clock


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: 오래 걸리는 시험(골든 하니스·전체 공정 sim 등) — 빠른 시험에서 뺀다")


@pytest.fixture(autouse=True)
def _close_virtual_clocks(monkeypatch):
    made = []
    init = _clock.VirtualClock.__init__

    def tracked(self, *a, **k):
        init(self, *a, **k)
        made.append(self)
    monkeypatch.setattr(_clock.VirtualClock, "__init__", tracked)
    yield
    for c in made:
        c.close()
