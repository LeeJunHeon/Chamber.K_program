"""auto 읽기 계획 판정(5-20 ①) — MFC·RF Pulse 공통. 한 번 느렸다고 영영 번갈아 읽지 않게, 다시 빨라지면 돌아온다.

계획 두 가지: full(매초 셋 다 읽음) · alternate(매초 하나 + 나머지 둘을 번갈아 — 그 둘은 2초마다).
  <장비>_READ_PLAN = full·alternate 이면 그 계획에 고정(판정 안 함). auto 이면 full 로 시작해 아래 판정으로 바꾼다.
매초 읽기(full) 중: 최근 <장비>_READ_PLAN_SLOW_M 번 읽기 중 <장비>_READ_PLAN_SLOW_N 번 이상이 '느린 읽기'면 번갈아로.
번갈아 읽기 중: <장비>_READ_PLAN_RETURN_SEC 동안 센 읽기가 모두 '빠른 읽기'면 매초 읽기로.
  '빠른 읽기'(셋 다 읽었다면 걸렸을 시간이 마감의 FAST_PCT % 안)는 '느린 읽기'(마감의 SLOW_PCT % 넘음)보다 엄격하다
  (히스테리시스 — 기준 근처에서 오가지 않게).
바꾼 뒤 <장비>_READ_PLAN_HOLD_SEC(하한 60 s) 안에는 다시 바꾸지 않는다 — 1분에 한 번보다 자주 바뀌지 않는다.
판정에 넣지 않는 읽기(None): 장비가 느린 것이 아닌 읽기 — 기록용 질의(MFC R6) 무응답으로 길어진 읽기, 앞 작업에 밀려
  요청 마감이 먼저 끝난 읽기, 질의를 하나만 보낸 읽기(시간을 셋으로 미룰 수 없음).
바꿀 때마다 드라이버가 read_plan 이벤트를 낸다(두 방향 모두). 지금 계획은 스냅샷 <dev>.read_plan(full·alternate).
"""
from __future__ import annotations

from collections import deque
from typing import Any, Dict, Optional

FULL, ALTERNATE = "full", "alternate"      # 화면 이름은 snapshot_fields.READ_PLAN_LABELS


class ReadPlan:
    def __init__(self, cfg: Dict[str, Any], prefix: str) -> None:
        self.cfg, self.prefix = cfg, prefix               # 같은 설정 사전(설치 설정 — 재시작 때 바뀜)
        self.mode = cfg[f"{prefix}_READ_PLAN"]
        self.alternate = self.mode == ALTERNATE
        self.recent: "deque[bool]" = deque()               # 매초 읽기 중 최근 판정(느림 True)
        self.fast_since: Optional[float] = None            # 번갈아 읽기 중 '빠른 읽기'가 이어진 시작 시각
        self.changed_at = -1e18                            # 마지막으로 바꾼 시각
        self.switches = 0
        self.switch_slow = self.switch_of = 0
        self.last_ms: Optional[float] = None               # 마지막으로 센 읽기 시간(매초 읽기 = 잰 값, 번갈아 = 셋 다 읽었을 때 예상)

    def _c(self, name: str) -> float:
        return float(self.cfg[f"{self.prefix}_READ_PLAN_{name}"])

    @property
    def plan(self) -> str:
        return ALTERNATE if self.alternate else FULL

    def slow_ms(self, deadline_ms: float) -> float:
        return deadline_ms * self._c("SLOW_PCT") / 100.0

    def fast_ms(self, deadline_ms: float) -> float:
        return deadline_ms * self._c("FAST_PCT") / 100.0

    def note(self, now: float, ms: Optional[float], deadline_ms: float, slow: bool = False) -> Optional[str]:
        """읽기 하나를 센다. ms = 매초 읽기면 잰 시간, 번갈아 읽기면 셋 다 읽었을 때 예상 시간(None 이면 세지 않음).
        slow = 시간과 상관없이 느린 읽기(예: 질의가 응답 제한을 다 씀). 계획을 바꿨으면 새 계획 이름, 아니면 None."""
        if self.mode != "auto" or (ms is None and not slow):
            return None
        if ms is not None:
            self.last_ms = ms
        if not self.alternate:
            m = max(1, int(self._c("SLOW_M")))
            self.recent.append(slow or (ms is not None and ms > self.slow_ms(deadline_ms)))
            while len(self.recent) > m:
                self.recent.popleft()
            if sum(self.recent) >= int(self._c("SLOW_N")) and now - self.changed_at >= self._c("HOLD_SEC"):
                return self._switch(now, True)
            return None
        if slow or ms is None or ms > self.fast_ms(deadline_ms):
            self.fast_since = None
            return None
        if self.fast_since is None:
            self.fast_since = now
        if now - self.fast_since >= self._c("RETURN_SEC") and now - self.changed_at >= self._c("HOLD_SEC"):
            return self._switch(now, False)
        return None

    def slow_count(self) -> int:
        return sum(self.recent)

    def _switch(self, now: float, alternate: bool) -> str:
        self.alternate = alternate
        self.changed_at = now
        self.switches += 1
        self.switch_slow, self.switch_of = sum(self.recent), len(self.recent)     # 바꿀 때 센 느린 읽기 수(이벤트 문구용)
        self.recent.clear()
        self.fast_since = None
        return self.plan
