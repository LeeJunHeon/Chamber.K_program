# controller/heater_ramp.py
"""파이썬이 만드는 히터 램프 — 목표 직전 감속 접근(approach).

[왜 파이썬이 램프를 만드나]
  래더 램프 생성기(H6a)는 D00023 카운트를 _T1S 마다 더하는 구조라 최소 단위가
  1카운트/초 = 6°C/min 이다. 그보다 느린 속도를 낼 수 없고, 2단 감속
  (D00027/D00028)은 '구간에 들어오면 속도를 6°C/min 으로 바꾼다'는 계단식이라
  전환 지점에서 속도가 뚝 떨어질 뿐 도착 속도는 여전히 6°C/min 이다.
  그 속도로 도착하면 히터에 잉여 전력이 남아 그대로 넘어간다
  (2026-09-10 11:33 수동 100°C: 6°C/min 도착 → 19A 잔류 → +5°C 오버슈트).

  그래서 래더는 '항상 파이썬보다 빠른' 속도로 열어 두고, 실제 램프는 파이썬이
  D00012(SV)를 1초마다 조금씩 올려서 만든다. 래더는 그 SV 를 따라가기만 한다.

[앞섬 한계(LEAD)와 홀드백]
  파이썬 SV 를 무한정 앞세우면 래더 홀드백(D00021)의 의미가 사라진다.
  홀드백은 'PV 가 램프 목표보다 이만큼 뒤처지면 램프를 세운다'는 장치인데,
  SV 가 저 멀리 앞서 있으면 래더는 계속 따라가려 하고 적분이 감긴다.
  그래서 새 SV 는 항상 sv_ramp(D00019) + HEATER_APPROACH_LEAD_C 이하로 묶는다.
  PV 가 처져 래더가 멈추면 sv_ramp 도 멈추고, 따라서 파이썬 SV 도 그 자리에
  같이 멈춘다 — 홀드백이 그대로 살아 있다.

[왜 시간이 아니라 거리 기준인가]
  속도를 '남은 거리'의 함수로 정하기 때문에(lib/heater_profile.approach_rate),
  중간에 HOLD 로 멈췄다가 재개해도 보정할 것이 없다. 예전 느린 램프는
  '시작 시각 + 경과 시간 × 속도' 로 SV 를 계산해서, HOLD 뒤 재개하면 시계를
  손으로 밀어 줘야 했고 한 번 빠뜨리면 SV 가 껑충 뛰었다.
  여기서는 어디서 멈췄든 '지금 남은 거리'만 보면 된다.
"""

from __future__ import annotations

import math
import time

from PyQt6.QtCore import QObject, QTimer, pyqtSignal as Signal

from lib.config import (
    HEATER_RAMP_RATE_C_PER_MIN,
    HEATER_APPROACH_ZONE_C,
    HEATER_APPROACH_MIN_RATE_C_PER_MIN,
    HEATER_APPROACH_LEAD_C,
)
from lib.heater_profile import approach_rate, ramp_minutes


class RampProfiler(QObject):
    """SV(D00012)를 1초마다 밀어 올려 감속 접근 램프를 만든다.

    PLC 레지스터를 직접 쓰지 않는다 — request_* 시그널로만 내보낸다.
    호출부가 PLC 스레드의 슬롯에 큐 연결로 이어 준다.
    """

    request_target = Signal(float)     # → PLC.set_heater_target (°C)
    request_ramp   = Signal(int)       # → PLC.set_heater_ramp_rate (counts/s)
    status_message = Signal(str, str)
    finished       = Signal()

    def __init__(self, plc_controller, parent=None, autotick: bool = False):
        super().__init__(parent)
        self._plc = plc_controller
        self._active = False
        self._target = None
        self._rate = 0.0
        self._sv_cmd = None          # 마지막으로 내보낸 SV
        self._t_last = 0.0
        self._label = ""

        # 수동/공정 경로는 자기 틱이 없으므로 여기서 돌린다.
        # 레시피 러너는 자기 1초 틱에서 tick() 을 직접 부른다(시계가 하나여야 한다).
        self._timer = None
        if autotick:
            self._timer = QTimer(self)
            self._timer.setInterval(1000)
            self._timer.timeout.connect(self.tick)

    # ---------- 조회 ----------
    def is_active(self) -> bool:
        return bool(self._active)

    def target(self):
        return self._target

    def rate(self) -> float:
        return float(self._rate)

    # ---------- 제어 ----------
    def start(self, target: float, rate: float, label: str = "") -> bool:
        """감속 접근 램프를 시작한다. 못 쓰는 상황이면 False(호출부가 폴백)."""
        try:
            st = self._plc.get_heater_status() or {}
        except Exception:
            st = {}
        svr = st.get('sv_ramp')
        pv = st.get('pv')

        # 운전 중이면 지금 램프 목표에서 이어 간다. 꺼져 있으면 PV 에서 출발한다.
        if st.get('run') and svr is not None:
            base = svr
        elif pv is not None:
            base = pv
        else:
            base = svr
        if base is None:
            self.status_message.emit(
                "히터(경고)", "[히터] 램프: 현재 온도를 읽을 수 없어 감속 접근을 쓸 수 없습니다")
            return False

        base = float(base)
        target = float(target)
        rate = float(rate)
        if target <= base + 0.05:
            return False              # 하강/동일은 호출부가 직접 목표를 쓴다
        if rate <= 0:
            return False

        # 래더는 파이썬 푸시보다 항상 빠르게 열어 둔다. 래더가 더 느리면
        #  SV 를 올려 놔도 D00019 가 못 따라와 램프가 파이썬 의도보다 느려진다.
        counts = max(2, int(math.ceil(rate / 6.0)) + 1)
        self.request_ramp.emit(counts)

        # ★ RUN 을 켜기 전에 D00012 를 출발점으로 내려 둔다.
        #   예전 목표가 남아 있으면 래더가 첫 1초를 자기 속도로 달려 버린다.
        self._sv_cmd = base
        self.request_target.emit(round(base, 1))

        self._target = target
        self._rate = rate
        self._label = label or ""
        self._t_last = time.monotonic()
        self._active = True
        if self._timer is not None:
            self._timer.start()

        _lb = (" " + self._label) if self._label else ""
        self.status_message.emit(
            "히터",
            f"[히터] 램프{_lb}: {base:.1f}→{target:.1f}°C · {rate:g}°C/min"
            f" · 접근 {float(HEATER_APPROACH_ZONE_C):g}°C→"
            f"{float(HEATER_APPROACH_MIN_RATE_C_PER_MIN):g}°C/min"
            f" · 예상 {ramp_minutes(target - base, rate):.1f}분")
        return True

    def tick(self, dt_sec=None):
        """1초마다: 남은 거리로 속도를 정하고 그만큼 SV 를 올린다."""
        if not self._active:
            return
        try:
            st = self._plc.get_heater_status() or {}
        except Exception:
            return
        svr = st.get('sv_ramp')
        if svr is None:
            return                      # 읽을 수 없으면 이번 틱은 건너뛴다
        svr = float(svr)

        now = time.monotonic()
        dt = (now - self._t_last) if dt_sec is None else float(dt_sec)
        dt = min(max(dt, 0.0), 5.0)     # 절전/정지 뒤 큰 점프를 막는다
        self._t_last = now

        target = float(self._target)
        dist = max(0.0, target - svr)
        r_now = approach_rate(dist, self._rate)

        # 래더가 앞서 있으면(SV 를 이미 따라잡았으면) 거기서 이어 간다.
        #  단 sv_ramp 는 0.1°C 단위로 반올림되어 올라오므로, 그 눈금만큼은
        #  '앞선 것'으로 치지 않는다. 그러지 않으면 매 틱 _sv_cmd 가 반올림된
        #  0.1 격자로 끌어올려져, 느린 접근 구간에서도 램프가 0.1°C/초(=6°C/min)
        #  로 달린다(감속이 통째로 사라진다).
        base = max(float(self._sv_cmd), svr - 0.05)
        new = min(target,
                  base + r_now * dt / 60.0,
                  svr + float(HEATER_APPROACH_LEAD_C))   # 홀드백을 살리는 앞섬 한계
        new = max(new, float(self._sv_cmd))              # 뒤로 가지 않는다

        if round(new, 1) != round(float(self._sv_cmd), 1):
            self.request_target.emit(round(new, 1))
        self._sv_cmd = new

        # SV 도 목표고 래더 램프도 목표에 닿았으면 끝이다.
        if self._sv_cmd >= target - 1e-6 and svr >= target - 0.15:
            self._active = False
            if self._timer is not None:
                self._timer.stop()
            self.status_message.emit("히터", f"[히터] 램프 완료 {target:.1f}°C")
            self.finished.emit()

    def hold(self):
        """레시피 HOLD: 래더를 지금 자리에 세운다.

        SV 를 현재 램프 목표(D00019)에 맞춰 두면 래더가 더 올라가지 않는다.
        거리 기준이라 재개할 때 시계를 보정할 필요가 없다.
        """
        if not self._active:
            return
        try:
            svr = (self._plc.get_heater_status() or {}).get('sv_ramp')
        except Exception:
            svr = None
        if svr is not None:
            self.request_target.emit(round(float(svr), 1))
            self._sv_cmd = float(svr)
        if self._timer is not None:
            self._timer.stop()

    def resume(self):
        """HOLD 를 풀고 이어 간다. 멈춰 있던 시간은 계산에 쓰지 않는다."""
        if not self._active:
            return
        self._t_last = time.monotonic()
        if self._timer is not None:
            self._timer.start()

    def stop(self, restore_rate: bool = True):
        """램프를 그만둔다. SV 는 건드리지 않는다(호출부가 정한다)."""
        was = self._active
        self._active = False
        self._target = None
        if self._timer is not None:
            self._timer.stop()
        if restore_rate and was:
            # 래더 속도를 기본값으로 되돌린다. start() 가 올려 둔 채로 두면
            #  다음 수동 조작이 예상보다 빠른 램프로 달린다.
            self.request_ramp.emit(
                max(1, round(float(HEATER_RAMP_RATE_C_PER_MIN) / 6.0)))
