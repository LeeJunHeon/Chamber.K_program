"""Chamber 제어 프로그램 v2.

v2\\common : 장비 공통(챔버1·2 와 함께 쓸 부분) — 파라미터 체계, 스냅샷, 명령, 장비↔웹 메시지
v2\\chk    : 챔버K 전용 — 장비 사실, 파라미터 표, 스냅샷 필드, 명령 허용 표, 레시피, 공정 상태, 안전 규칙
v2\\tests  : python -m pytest v2\\tests (표준 라이브러리 + pytest 만)

v1 모듈(lib·core·device·controller·integrations …)을 import 하지 않는다 — v2\\tests\\test_isolation.py 가 지킨다.
"""
