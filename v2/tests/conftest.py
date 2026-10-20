"""pytest 공통 — 표식 slow: 골든 하니스·전체 공정 sim·bench 도구 시험대처럼 오래 걸리는 시험(사용자 결정 2026-10-20 ⑦).

커밋마다 빠른 시험: python -m pytest v2\tests -m "not slow"   /   마지막 커밋 전 전체: python -m pytest v2\tests
"""


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: 오래 걸리는 시험(골든 하니스·전체 공정 sim 등) — 빠른 시험에서 뺀다")
