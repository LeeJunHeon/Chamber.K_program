# controller/heater_cascade.py
"""히터 2단(캐스케이드) 제어 — 안쪽 루프는 PLC PID(TC2), 바깥 루프는 파이썬(TC1).

[왜]
  PLC PID(래더 92·96 스텝이 Kp 3.0 / Ti 300 s 를 매 스캔 강제)를 TC1(시료 쪽 TC)으로 직접 돌리면
  히터 → TC2 약 40 s, TC2 → TC1 약 65 s, 합계 약 105 s 의 지연이 루프 안에 들어간다.
  600°C 에서는 맞던 Kp 가 300°C(복사 손실이 작아 민감, 출력이 0% 바닥 근처)에서는 과해
  2026-10-27 00:48·15:00 두 런 모두 TC1 288↔311°C(주기 약 260 s)로 진동하다 HEATER_WAIT 90분 타임아웃.
  메인 셔터를 열면 TC1 이 약 100°C 낮게 읽혀(센서 위치 효과) 증착 중에는 TC1 로 제어할 수도 없다.
  TC2 루프는 유지 모드 실측에서 표준편차 0.15~0.2°C 로 안정하다.

[구조]
  안쪽(빠름, PLC) : 히터 RUN 동안 처음부터 끝까지 PID PV = TC2, SV = SV2(D00035). 래더의 기존 TC2 경로
                   (M0004A ∧ TC2 정상 ∧ RUN ∧ D00035>0 → M0004B, D00019 ← min(D00035, D00036))를 그대로 쓴다.
                   M0004B 동안 래더 램프·홀드백·상한 클램프는 꺼지므로 SV2 를 천천히 올리는 것은 여기 책임이다.
  바깥(느림, 여기): r1 = TC1 기준값(감속 접근 램프 + TC1 홀드백), SV2 = map(r1) + ff(r1) + b_int.
                   ff(T) = 학습 보정 곡선 — 점 {(T_amb, 0)} ∪ {(tc1_i, tc2_i − map(tc1_i))} 사이 선형 보간, T ≤ T_amb 은 0,
                   가장 높은 학습점 위는 그 보정값 그대로(T_amb = 기본 매핑 첫 점의 TC1). 학습값은 그 목표 온도에서
                   필요한 보정이라 상온부터 통째로 더하지 않는다(2026-10-29 #8-4: 학습 +13.1 을 TC1 29°C 에서 더해 출발).
                   b_int = 적분 보정 — 매 운전 0 에서 시작.
                   e = r1(t − LAG) − TC1_ema, d(b_int)/dt = slope(map, r1) · e / TI (적분만, 변화율·크기 제한).
                   bias(화면·ERP·챗·CSV) = ff(r1) + b_int = 기본 매핑에서 벗어난 전체 보정(SV2 = map + bias).
                   비례항이 없어 바깥 루프는 TI(600 s) 시간 규모로만 움직인다 — TC1 의 105 s 지연이 루프를
                   흔들 수 없다. 비교 기준을 LAG 전의 r1 로 두는 것은 램프 중 TC1 이 열 지연만큼 뒤처지는 것을
                   오차로 보고 bias 를 감아 올렸다가 도착 뒤 넘치는 것을 막기 위해서다.
                   같은 이유로 세 가지를 더 둔다(시뮬레이터에서 각각 없을 때 도착 뒤 +5~14°C 를 확인했다):
                     · 비교 기준 r1(t−LAG) 에도 TC1 과 같은 EMA 를 건다(EMA 지연 30 s 가 오차로 들어가지 않게).
                     · 홀드백은 r1(t−LAG) 기준이다. 지금의 r1 기준이면 열 지연만으로 상시 걸린다.
                     · 안쪽 루프가 SV2 를 못 따라가는 동안(TC2 가 SV2 보다 slope×HOLDBACK 이상 처짐)은 같은 방향으로
                       적분하지 않는다(캐스케이드 anti-windup). 출력 상한/하한, SV2 상한/하한도 마찬가지.
  메인 셔터가 열려 있으면 b_int 적분을 멈추고 SV2 를 고정한다(상승 목표도 셔터가 닫힐 때까지 보류, 하강은 즉시).
  매 주기 D00012 ← r1 을 써 둔다 — TC2 를 잃어 래더가 TC1 제어로 넘어가도(M0004B 하강 엣지에서 D00019 ← TC1,
  이후 D00022 = min(D00012, D00013) 을 향해 래더 램프) 같은 r1 을 따라가게 하기 위해서다.

[PLC 쓰기] HeaterHold·RampProfiler 와 같이 직접 쓰지 않고 request_* 시그널로만 요청한다.
  해제 순서는 반드시 M0004A OFF 먼저, 그다음 D00035 = 0.

[tick] main.update_heater_display 가 PLC 폴링(200 ms)마다 tick(st) 를 부른다. 핸드셰이크 확인은 매 폴링,
  r1·bias·SV2 계산과 쓰기는 1 s 주기다. 시계는 이 하나뿐이다 — 히터 레시피 러너는 start/hold/resume 만 부르고
  tick 은 부르지 않는다(둘이 부르면 r1 이 두 배로 달린다).
"""
from __future__ import annotations

import datetime
import json
import math
import os
import time
from collections import deque
from typing import Callable, List, Optional

from PyQt6.QtCore import QObject, pyqtSignal as Signal

from lib.heater_profile import approach_rate, ramp_minutes

EMA_TAU_SEC         = 30.0    # TC1 EMA 시정수
CALC_PERIOD_SEC     = 1.0     # r1·bias·SV2 계산/쓰기 주기
ENGAGE_TIMEOUT_SEC  = 5.0     # engaging 각 단계(D00035 되읽기 / M0004B 확인) 대기 상한 — heater_hold 와 같다
ALARM_HOLD_SEC      = 60.0    # |bias| ≥ ALARM 이 이만큼 지속되면 TC1 탈락 의심
CAP_WARN_SEC        = 60.0    # SV2 가 상한에 걸린 채 TC1 이 목표 아래에 이만큼 머물면 안내 1회
LEARN_BIAS_RATE_MAX = 0.2     # 학습 조건: 유지 창 동안 bias 변화율 [°C/min] 이하
REENGAGE_OK_SEC     = 30.0    # TC2 폴백 뒤 TC2 가 이만큼 연속 정상이면 다시 TC2 제어를 시도
REENGAGE_RETRY_SEC  = 60.0    # 재시도 간격
REENGAGE_MAX        = 3       # 한 운전에서 재시도 상한(래더가 계속 거부하면 M0004A 를 흔들지 않는다)
TEXT_LOG_SEC        = 10.0    # "TC1 기준 r1 → SV2" 줄 간격(같은 값이면 남기지 않는다)
STATUS_LOG_SEC      = 60.0    # 상태 한 줄 간격
SV2_FLOOR_C         = 1.0     # SV2 하한 — 래더는 D00035 > 0 일 때만 TC2 제어를 유지한다
MV_SAT_REL          = 0.98    # 안쪽 루프 출력이 상한의 98% 이상이면 "더 올릴 수 없다"(anti-windup)
MV_FLOOR_MARGIN     = 5       # 출력이 MV_MIN + 이 값 이하면 "더 내릴 수 없다"(anti-windup)

# 상태 → 화면/로그 문구
STATE_TEXT = {
    'idle': "꺼짐", 'armed': "RUN 대기", 'engaging_sv2': "TC2 제어 진입(SV2 확인)",
    'engaging_sel': "TC2 제어 진입(M0004B 확인)", 'running': "TC2 제어", 'frozen': "TC2 제어 · 셔터 열림(고정)",
    'tc1': "TC1 제어(폴백)", 'dac': "DAC 상한 고정(폴백)",
}


def default_learned_path() -> str:
    """학습 매핑 파일 — config_user.json 과 같은 폴더(git 에 올리지 않는다)."""
    try:
        from lib import config_loader
        base = os.path.dirname(os.path.abspath(config_loader._JSON_PATH))
    except Exception:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "heater_tc2_map_learned.json")


class Tc2Map:
    """TC1 → TC2 기본 매핑. 점 사이 선형 보간, 범위 밖은 양 끝 구간 기울기로 외삽."""

    def __init__(self, points):
        pts = sorted((float(a), float(b)) for a, b in points)
        if len(pts) < 2:
            raise ValueError("매핑 점이 2개 이상이어야 합니다")
        self.points = pts

    def _seg(self, t1: float):
        p = self.points
        for i in range(len(p) - 1):
            if t1 <= p[i + 1][0]:
                return p[i], p[i + 1]
        return p[-2], p[-1]

    def slope(self, t1: float) -> float:
        (x0, y0), (x1, y1) = self._seg(float(t1))
        return (y1 - y0) / (x1 - x0)

    def value(self, t1: float) -> float:
        (x0, y0), _ = self._seg(float(t1))
        return y0 + self.slope(t1) * (float(t1) - x0)

    def text(self) -> str:
        return " ".join(f"{a:g}→{b:g}" for a, b in self.points)


class FfCurve:
    """학습 보정 곡선 ff(T). 점 = (T_amb, 0) + 학습점 (tc1, tc2 − map(tc1)). T ≤ T_amb 은 0, 점 사이 선형,
    가장 높은 점 위는 그 값 그대로(기울기 외삽 안 함). 학습점은 절대값(TC1, TC2)으로 저장돼 있어 매핑이 바뀌어도
    보정값은 지금 매핑 기준으로 다시 계산된다."""

    def __init__(self, t_amb: float, points=()):
        self.t_amb = float(t_amb)
        pts = sorted((float(a), float(b)) for a, b in points if float(a) > self.t_amb)
        self.points = [(self.t_amb, 0.0)] + pts

    def value(self, t) -> float:
        if t is None:
            return 0.0
        t = float(t)
        p = self.points
        if t <= p[0][0]:
            return 0.0
        for (x0, y0), (x1, y1) in zip(p, p[1:]):
            if t <= x1:
                return y0 + (y1 - y0) * (t - x0) / (x1 - x0)
        return p[-1][1]


class HeaterCascade(QObject):
    request_target   = Signal(float)   # D00012 ← r1 (TC1 기준값 — 폴백 경로와 화면 의미를 위해 늘 써 둔다)
    request_sv2      = Signal(float)   # D00035 ← SV2
    request_pv_sel   = Signal(bool)    # M0004A
    request_ramp     = Signal(int)     # D00020 — TC1 폴백 시 래더 램프가 r1 보다 빨라야 한다(RampProfiler 와 같은 규칙)
    request_mv_limit = Signal(int)     # D00018 — 셔터 열림 중 TC2 상실 시 DAC 상한 고정 / 해제 시 원복
    message          = Signal(str, str)
    # 챗 카드가 필요한 사건: "tc1_detached"(TC1 탈락 의심) / "tc2_lost"(TC1 제어 폴백) /
    #  "demoted"(셔터 열림 중 TC2 상실 → DAC 고정) / "demote_failed" / "sv2_capped"(SV2 상한으로 도달 불가)
    alert            = Signal(str, dict)

    def __init__(self, *, map_points, ti_sec: float, lag_sec: float, bias_rate_max: float,
                 bias_limit_c: float, bias_alarm_c: float, alarm_action: str,
                 holdback_c: float, tc2_margin_c: float, mv_limit: int, mv_min: int,
                 soak_tol_c: float, soak_sec: float,
                 learned_path: Optional[str] = None,
                 clock: Callable[[], float] = time.monotonic,
                 wallclock: Callable[[], datetime.datetime] = datetime.datetime.now, parent=None):
        super().__init__(parent)
        self.map = Tc2Map(map_points)
        self.ti = float(ti_sec); self.lag = float(lag_sec)
        self.bias_rate_max = float(bias_rate_max); self.bias_limit = float(bias_limit_c)
        self.bias_alarm = float(bias_alarm_c); self.alarm_action = str(alarm_action)
        self.holdback = float(holdback_c); self.tc2_margin = float(tc2_margin_c)
        self.mv_limit = int(mv_limit); self.mv_min = int(mv_min)
        self.soak_tol = float(soak_tol_c); self.soak_sec = float(soak_sec)
        self.learned_path = learned_path if learned_path is not None else default_learned_path()
        self._now = clock
        self._wall = wallclock
        self._learn_warned = False
        self._reset()

    # ───────────────────────── 상태 ─────────────────────────
    def _reset(self) -> None:
        self._state = 'idle'
        self._final = None; self._rate = 0.0; self._label = ""
        self._r1 = None; self._held = False
        self._b_int = 0.0; self._bias_src = ""
        self._ff = FfCurve(self.map.points[0][0])
        self._learn_skip_logged = False
        self._ema = None
        self._ring: deque = deque()                # (t, r1)
        self._t_calc = 0.0
        self._sv2_cmd = None; self._r1_cmd = None
        self._sv2_map = None
        self._engage_t0 = 0.0
        self._seen_run = False
        self._shutter = False
        self._resume_at = 0.0                      # 셔터가 닫힌 뒤 이 시각부터 적분 재개
        self._alarm_t0 = 0.0; self._alarm_sent = False
        self._cap_t0 = 0.0; self._cap_warned = False
        self._learned_targets: set = set()
        self._soak_t0 = 0.0; self._soak_bias0 = 0.0
        self._tc2_ok_since = 0.0; self._reengage_n = 0; self._engage_fail_t = 0.0
        self._dac_value = None
        self._last_mv = None
        self._text_log_t = 0.0; self._text_log_key = None
        self._status_log_t = 0.0
        self._done_logged = False
        self._last_st: dict = {}
        self._integrating = False
        self._r1_lag = None
        self._ref_ema = None

    @property
    def state(self) -> str:
        return self._state

    def is_active(self) -> bool:
        return self._state != 'idle'

    def target(self):
        """최종 목표(TC1) — 활성이 아니면 None."""
        return self._final if self.is_active() else None

    def rate(self) -> float:
        return float(self._rate)

    @property
    def r1(self):
        return self._r1

    @property
    def bias(self) -> float:
        """기본 매핑에서 벗어난 전체 보정 = ff(r1) + b_int (SV2 = map(r1) + bias)."""
        return self.bias_ff + float(self._b_int)

    @property
    def bias_ff(self) -> float:
        """학습 보정 ff(r1) — r1 이 아직 없으면 0."""
        return float(self._ff.value(self._r1))

    @property
    def bias_int(self) -> float:
        """적분 보정 b_int(매 운전 0 에서 시작)."""
        return float(self._b_int)

    @property
    def bias_source(self) -> str:
        return self._bias_src

    @property
    def sv2(self):
        """마지막으로 쓴 SV2. TC2 제어가 아니면 None."""
        return self._sv2_cmd if self.engaged() else None

    def engaged(self) -> bool:
        """PLC PID 가 TC2 를 보고 있다(M0004B 확인 뒤)."""
        return self._state in ('running', 'frozen')

    def is_holding(self) -> bool:
        """HeaterHold 와 같은 질의(화면·ERP·포화 감시) — TC2 제어 중이면 True."""
        return self.engaged()

    @property
    def kind(self):
        if self.engaged():
            return 'cascade'
        if self._state == 'dac':
            return 'dac'
        return None

    @property
    def dac_value(self):
        return self._dac_value if self._state == 'dac' else None

    def is_held(self) -> bool:
        return bool(self._held)

    def r1_done(self, target=None) -> bool:
        """r1 이 최종 목표에 닿았다(target 을 주면 그 목표가 지금 최종 목표인지도 본다)."""
        if not self.is_active() or self._final is None or self._r1 is None:
            return False
        if target is not None and abs(float(target) - float(self._final)) > 0.05:
            return False
        return abs(float(self._r1) - float(self._final)) <= 0.05

    def sat_hold(self):
        """heater_saturation.tick 의 (hold_active, hold_kind). DAC 고정 중이면 D00018 은 여기 소유다.
        TC2 제어 중에는 포화 감시가 TC1 직접 제어 때와 똑같이 돈다(MV 기준이라 모드와 무관하게 유효)."""
        if self._state == 'dac':
            return True, 'dac'
        return False, None

    def status_text(self) -> str:
        """화면 상태 문구 꼬리 — "TC2 제어 · bias +12.3"."""
        base = STATE_TEXT.get(self._state, self._state)
        if self.engaged():
            return f"{base} · bias {self.bias:+.1f}"
        if self._state == 'dac' and self._dac_value is not None:
            return f"{base} {self._dac_value}"
        return base

    def snapshot(self) -> dict:
        """process_controller(다른 스레드)가 읽는 값 — main 이 스냅샷에 싣는다."""
        return {"active": self.is_active(), "state": self._state, "engaged": self.engaged(),
                "final": self._final, "r1": self._r1, "r1_done": self.r1_done(),
                "sv2": self._sv2_cmd, "bias": self.bias, "bias_ff": self.bias_ff, "bias_int": self._b_int,
                "bias_src": self._bias_src,
                "shutter": self._shutter, "held": self._held}

    def log_fields(self) -> dict:
        """히터 CSV 의 캐스케이드 열."""
        def f(v):
            return "" if v is None else f"{float(v):.1f}"
        return {"r1_c": f(self._r1), "r1_lag_c": f(self._r1_lag), "tc1_ema_c": f(self._ema),
                "sv2_map_c": f(self._sv2_map), "bias_c": (f"{self.bias:.2f}" if self.is_active() else ""),
                "casc_state": self._state, "shutter_open": (1 if self._shutter else 0),
                "bias_ff_c": (f"{self.bias_ff:.2f}" if self.is_active() else ""),
                "bias_int_c": (f"{self._b_int:.2f}" if self.is_active() else "")}

    def settings_text(self) -> str:
        return (f"매핑 {self.map.text()} · TI {self.ti:g}s · LAG {self.lag:g}s · bias 변화율 ≤{self.bias_rate_max:g}°C/min"
                f" · |bias| ≤{self.bias_limit:g} · 경보 {self.bias_alarm:g}°C({self.alarm_action})")

    def _msg(self, level: str, text: str) -> None:
        self.message.emit(level, f"[캐스케이드] {text}")

    # ───────────────────────── 제어 ─────────────────────────
    def start(self, target: float, rate: float, label: str = "") -> bool:
        """목표(TC1)·속도를 정한다. 꺼져 있으면 새로 시작(RUN 이 서면 TC2 제어에 들어간다), 켜져 있으면 목표만 바꾼다."""
        target = float(target)
        rate = float(rate) if rate and float(rate) > 0 else 0.0
        st = self._last_st or {}
        if self.is_active():
            old = self._final
            self._final = target
            if rate > 0:
                self._rate = rate
            self._label = label or self._label
            if self._r1 is not None and target < self._r1:
                self._r1 = target                       # 하강은 바로 내린다(히터는 식는 속도를 못 만든다)
            self._done_logged = False
            self._cap_warned = False; self._cap_t0 = 0.0
            self._soak_t0 = 0.0
            self._emit_ramp_counts()
            self._msg("히터", f"목표 변경 {old if old is None else f'{old:.1f}'} → {target:.1f}°C"
                             f" · {self._rate:g}°C/min (r1 {self._fmt(self._r1)})")
            return True
        self._reset()
        self._state = 'armed'
        self._final = target
        self._rate = rate
        self._label = label or ""
        self._ff, self._bias_src = self._build_ff(target)
        self._b_int = 0.0
        self._emit_ramp_counts()
        pv = st.get('pv')
        if pv is not None:
            # RUN 을 켜기 전에 D00012 를 출발점으로 내려 둔다(TC1 폴백 경로에서 래더가 옛 목표로 달리지 않게)
            self._r1_cmd = round(float(pv), 1)
            self.request_target.emit(self._r1_cmd)
        _lb = (" " + self._label) if self._label else ""
        est = ""
        if pv is not None and rate > 0 and target > float(pv):
            est = f" · 예상 {ramp_minutes(target - float(pv), rate):.1f}분"
        self._msg("히터", f"시작{_lb}: 목표 TC1 {target:.1f}°C · {rate:g}°C/min{est} · {self._bias_src}"
                         f" · {self.settings_text()}")
        return True

    def _emit_ramp_counts(self) -> None:
        if self._rate > 0:
            self.request_ramp.emit(max(2, int(math.ceil(self._rate / 6.0)) + 1))

    def hold(self) -> None:
        """레시피 HOLD — r1 을 그 자리에 세운다(멈춘 시간은 계산에 쓰지 않는다: 거리 기준)."""
        if self.is_active() and not self._held:
            self._held = True
            self._msg("히터", f"r1 일시정지 {self._fmt(self._r1)}°C")

    def resume(self) -> None:
        if self.is_active() and self._held:
            self._held = False
            self._t_calc = self._now()                 # 멈춰 있던 시간만큼 한 번에 올라가지 않게
            self._msg("히터", f"r1 재개 {self._fmt(self._r1)}°C")

    def stop(self, why: str = "정지") -> None:
        """끈다. TC2 제어 중이면 M0004A OFF → D00035=0 순서로 되돌리고, DAC 고정 중이면 상한을 원복한다."""
        st_ = self._state
        if st_ == 'idle':
            return
        if st_ in ('engaging_sv2', 'engaging_sel', 'running', 'frozen'):
            self.request_pv_sel.emit(False)
            self.request_sv2.emit(0.0)
        if st_ == 'dac':
            self.request_mv_limit.emit(int(self.mv_limit))
        self._msg("히터", f"종료 ({why}) — r1 {self._fmt(self._r1)} / bias {self.bias:+.1f}"
                         f" (ff {self.bias_ff:+.1f} / int {self._b_int:+.1f})"
                         + (" · TC1 제어 복귀" if st_ in ('running', 'frozen') else ""))
        self._reset()

    # ───────────────────────── 학습 보정(ff) / 학습 ─────────────────────────
    def _load_learned(self) -> List[dict]:
        try:
            with open(self.learned_path, encoding="utf-8") as f:
                data = json.load(f)
            pts = data.get("points") if isinstance(data, dict) else None
            out = []
            for p in pts or []:
                out.append({"tc1": float(p["tc1"]), "tc2": float(p["tc2"]), "t": str(p.get("t") or "")})
            return out
        except FileNotFoundError:
            return []
        except Exception as e:
            self._msg("히터(경고)", f"학습 매핑 파일을 읽을 수 없음 — 무시 ({e!r})")
            return []

    def _build_ff(self, final: float):
        """학습 보정 곡선을 만든다. |tc2 − map(tc1)| ≥ 경보 폭인 학습점은 쓰지 않는다(TC1 이 떨어진 런 등 — 경고 1줄).
        돌려주는 문구는 시작 로그·화면 설정 줄·챗 카드에 그대로 쓴다."""
        pts = self._load_learned()
        t_amb = self.map.points[0][0]
        use, txt = [], []
        for p in sorted(pts, key=lambda q: q["tc1"]):
            corr = p["tc2"] - self.map.value(p["tc1"])
            tag = f"TC1 {p['tc1']:g} → TC2 {p['tc2']:.1f} ({p['t'] or '시각 없음'})"
            if p["tc1"] <= t_amb:
                continue
            if abs(corr) >= self.bias_alarm:
                self._msg("히터(경고)", f"학습점 무시(보정 과대) — {tag}: 보정 {corr:+.1f} ≥ ±{self.bias_alarm:g}°C")
                continue
            use.append((p["tc1"], corr))
            txt.append(tag)
        ff = FfCurve(t_amb, use)
        if not use:
            return ff, "학습점 없음 → ff 0 · 적분 보정 0 에서 시작"
        return ff, (f"학습 보정: [{', '.join(txt)}] · 목표 {final:g} 에서 ff {ff.value(final):+.1f} · 상온 0"
                    f" · 적분 보정 0 에서 시작")

    def _learn(self, final: float, sv2: float) -> None:
        pts = [p for p in self._load_learned() if int(round(p["tc1"] / 10.0)) != int(round(final / 10.0))]
        pts.append({"tc1": round(float(final), 1), "tc2": round(float(sv2), 1),
                    "t": self._wall().strftime("%Y-%m-%d %H:%M:%S")})
        pts.sort(key=lambda p: p["tc1"])
        try:
            tmp = self.learned_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"points": pts}, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.learned_path)
            self._msg("히터", f"매핑 학습 — TC1 {final:.1f} → TC2 {sv2:.1f} (bias {self.bias:+.1f} ="
                             f" ff {self.bias_ff:+.1f} + int {self._b_int:+.1f}) 저장")
        except Exception as e:
            if not self._learn_warned:
                self._learn_warned = True
                self._msg("히터(경고)", f"학습 매핑 저장 실패 ({e!r}) — 제어에는 영향 없음")

    # ───────────────────────── tick ─────────────────────────
    def tick(self, st: dict) -> None:
        try:
            self._tick(st or {})
        except Exception as e:
            self._msg("경고", f"처리 예외: {e!r}")

    def _tick(self, st: dict) -> None:
        now = self._now()
        prev_st = self._last_st
        self._last_st = st
        if prev_st.get('mv') is not None:
            self._last_mv = prev_st.get('mv')         # TC2 상실 순간의 "직전 MV"
        if self._state == 'idle':
            return
        run = bool(st.get('run'))
        self._shutter = bool(st.get('shutter_open'))
        if st.get('fault'):
            self.stop("히터 이상"); return
        if not run:
            if self._seen_run:
                self.stop("운전 OFF")
            return
        self._seen_run = True
        pv = st.get('pv')

        if self._state == 'armed':
            if self._r1 is None:
                if pv is None:
                    return
                self._r1 = float(pv)
                self._ema = float(pv)
                self._t_calc = now
                self._ring.append((now, self._r1))
            self._begin_engage(st, now, first=True)
            return

        if self._state in ('engaging_sv2', 'engaging_sel'):
            self._tick_engaging(st, now)
            return

        if self._state in ('running', 'frozen'):
            if st.get('pv2') is None:
                self._tc2_lost(st, now, "TC2 값 없음(D00011=-1)"); return
            if not bool(st.get('pv_sel_eff')):
                self._tc2_lost(st, now, "래더가 TC2 제어를 해제(M0004B OFF)"); return
            if self._shutter and self._state == 'running':
                self._state = 'frozen'
                self._msg("히터", f"메인 셔터 열림 — bias {self.bias:+.1f} (ff {self.bias_ff:+.1f} / int {self._b_int:+.1f})"
                                 f" · SV2 {self._fmt(self._sv2_cmd)} 고정")
            elif not self._shutter and self._state == 'frozen':
                self._state = 'running'
                self._resume_at = now + self.lag + 2.0 * EMA_TAU_SEC
                self._msg("히터", f"메인 셔터 닫힘 — TC2 제어 재개 (bias 적분은 "
                                 f"{self.lag + 2.0 * EMA_TAU_SEC:.0f}초 뒤 재개: TC1 회복 대기)")
        elif self._state == 'dac':
            if not self._shutter:
                self._leave_dac(st, now)
                if self._state != 'dac':
                    return
        elif self._state == 'tc1':
            if self._shutter and pv is not None:
                # TC1 제어 중 셔터가 열리면 TC1 이 낮게 읽혀 PID 가 출력을 상한까지 민다(2026-09-15) — 직전 MV 로 묶는다
                self._enter_dac(st, "TC2 없이 메인 셔터 열림")
                return
            self._maybe_reengage(st, now)
            if self._state != 'tc1':
                return

        if (now - self._t_calc) >= CALC_PERIOD_SEC:
            dt = min(max(now - self._t_calc, 0.0), 5.0)
            self._t_calc = now
            self._calc(st, now, dt)

    # ───────────────────────── 진입 ─────────────────────────
    def _ladder_supports(self, st: dict) -> Optional[str]:
        """TC2 제어를 못 쓰는 사유(없으면 None)."""
        try:
            sv2_max = float(st.get('sv2_max') or 0.0); ot2 = float(st.get('ot2_limit') or 0.0)
        except Exception:
            sv2_max, ot2 = 0.0, 0.0
        if sv2_max <= 0 or ot2 <= 0:
            return "래더가 TC2 제어를 지원하지 않음(D00036=0)"
        if st.get('pv2') is None:
            return "TC2 값 없음(D00011=-1)"
        return None

    def _sv2_cap(self, st: dict) -> float:
        try:
            sv2_max = float(st.get('sv2_max') or 0.0)
        except Exception:
            sv2_max = 0.0
        try:
            ot2 = float(st.get('ot2_limit') or 0.0)
        except Exception:
            ot2 = 0.0
        cap = ot2 - self.tc2_margin
        if sv2_max > 0:
            cap = min(cap, sv2_max)
        return cap

    def _sv2_for(self, st: dict):
        m = self.map.value(self._r1)
        self._sv2_map = m
        cap = self._sv2_cap(st)
        raw = m + self._ff.value(self._r1) + self._b_int
        return max(SV2_FLOOR_C, min(cap, raw)), raw, cap

    def _begin_engage(self, st: dict, now: float, first: bool = False) -> None:
        why = self._ladder_supports(st)
        if why:
            self._state = 'tc1'
            self._engage_fail_t = now
            self._msg("히터(경고)", f"TC2 제어를 쓸 수 없음 — {why}. TC1 제어(래더 기본 경로)로 진행, D00012 ← r1")
            if not first:
                return
            self.alert.emit("tc2_lost", {"why": why, "shutter": self._shutter})
            return
        sv2, _, _ = self._sv2_for(st)
        sv2 = round(sv2, 1)
        self._sv2_cmd = sv2
        self._state = 'engaging_sv2'
        self._engage_t0 = now
        self.request_sv2.emit(float(sv2))

    def _tick_engaging(self, st: dict, now: float) -> None:
        why = self._ladder_supports(st)
        if why:
            self._engage_failed(f"{why}")
            return
        if self._state == 'engaging_sv2':
            try:
                back = float(st.get('sv2'))
            except Exception:
                back = None
            if back is not None and abs(back - float(self._sv2_cmd)) <= 0.05:
                self._state = 'engaging_sel'; self._engage_t0 = now
                self.request_pv_sel.emit(True)
                return
        elif self._state == 'engaging_sel':
            if bool(st.get('pv_sel_eff')):
                restore_dac = self._dac_value is not None
                self._state = 'frozen' if self._shutter else 'running'
                self._t_calc = now
                if restore_dac:
                    self.request_mv_limit.emit(int(self.mv_limit))
                    self._dac_value = None
                self._msg("히터", f"TC2 제어 시작 — TC1 {self._fmt(st.get('pv'))} / TC2 {self._fmt(st.get('pv2'))}"
                                 f" · r1 {self._fmt(self._r1)} → SV2 {self._sv2_cmd:.1f}"
                                 f" ({self._parts()})"
                                 + (f" · DAC 상한 {int(self.mv_limit)} 원복" if restore_dac else ""))
                return
        if (now - self._engage_t0) > ENGAGE_TIMEOUT_SEC:
            step = "D00035 되읽기" if self._state == 'engaging_sv2' else "M0004B 확인"
            self._engage_failed(f"{step} {ENGAGE_TIMEOUT_SEC:.0f}초 초과")

    def _engage_failed(self, why: str) -> None:
        self.request_pv_sel.emit(False)
        self.request_sv2.emit(0.0)
        self._state = 'tc1'
        self._engage_fail_t = self._now()
        self._tc2_ok_since = 0.0
        self._msg("히터(경고)", f"TC2 제어 진입 실패 — {why}. TC1 제어(래더 기본 경로)로 진행, D00012 ← r1")
        if self._reengage_n == 0:
            self.alert.emit("tc2_lost", {"why": f"TC2 제어 진입 실패 — {why}", "shutter": self._shutter})

    def _maybe_reengage(self, st: dict, now: float) -> None:
        if self._shutter or self._ladder_supports(st):
            self._tc2_ok_since = 0.0
            return
        if self._reengage_n >= REENGAGE_MAX:
            return
        if not self._tc2_ok_since:
            self._tc2_ok_since = now
            return
        if (now - self._tc2_ok_since) < REENGAGE_OK_SEC or (now - self._engage_fail_t) < REENGAGE_RETRY_SEC:
            return
        self._reengage_n += 1
        self._msg("히터", f"TC2 정상 {REENGAGE_OK_SEC:.0f}초 — TC2 제어 재시도 ({self._reengage_n}/{REENGAGE_MAX})")
        self._begin_engage(st, now)

    # ───────────────────────── TC2 상실 ─────────────────────────
    def _tc2_lost(self, st: dict, now: float, why: str) -> None:
        """TC2 를 못 쓰게 됐다. 셔터가 닫혀 있으면 TC1 제어로 넘기고(히터는 계속), 열려 있으면 TC1 제어 복귀 대신
        DAC 상한을 직전 MV 로 고정한다(HeaterHold._demote_to_dac 과 같은 이유 — 2026-09-22)."""
        self.request_pv_sel.emit(False)
        self.request_sv2.emit(0.0)
        self._engage_fail_t = now
        self._tc2_ok_since = 0.0
        if self._shutter:
            self._enter_dac(st, why)
            return
        self._state = 'tc1'
        self._msg("히터(경고)", f"{why} — TC1 제어로 전환(히터 계속 운전, D00012 ← r1 {self._fmt(self._r1)}). "
                               f"OT2 과온 보호는 래더가 계속 본다 — TC2 배선을 확인하십시오")
        self.alert.emit("tc2_lost", {"why": why, "shutter": False})

    def _enter_dac(self, st: dict, why: str) -> None:
        mv = self._last_mv if self._last_mv is not None else st.get('mv')
        if mv is None:
            self._state = 'tc1'
            self._msg("히터(경고)", f"{why} — DAC 고정 실패(MV 없음) → TC1 제어. 출력 상한이 {int(self.mv_limit)} 그대로입니다 — 확인 필요")
            self.alert.emit("demote_failed", {"why": why, "reason": "MV 없음", "from": "cascade"})
            return
        value = max(int(self.mv_min) + 20, min(int(self.mv_limit), int(round(float(mv)))))
        self._dac_value = value
        self._state = 'dac'
        self.request_mv_limit.emit(value)
        self._msg("히터(경고)", f"{why} — 메인 셔터가 열려 있어 TC1 제어 대신 DAC 상한 {value} 고정 (강등 직전 MV). "
                               f"셔터가 닫히면 해제합니다. TC2 배선을 확인하십시오")
        self.alert.emit("demoted", {"why": why, "value": value, "from": "cascade", "source": "강등 직전 MV"})

    def _leave_dac(self, st: dict, now: float) -> None:
        """셔터가 닫혔다 — TC2 가 살아 있으면 다시 TC2 제어(성공하면 상한 원복), 아니면 상한 원복 후 TC1 제어."""
        if self._ladder_supports(st) is None:
            self._msg("히터", "메인 셔터 닫힘 — TC2 정상, TC2 제어 재진입 시도")
            self._begin_engage(st, now)
            return
        self.request_mv_limit.emit(int(self.mv_limit))
        self._msg("히터", f"메인 셔터 닫힘 — DAC 상한 {int(self.mv_limit)} 원복, TC1 제어(TC2 없음)")
        self._dac_value = None
        self._state = 'tc1'

    # ───────────────────────── 1초 계산 ─────────────────────────
    def _r1_at(self, t: float):
        val = None
        for (tt, r) in self._ring:
            if tt <= t:
                val = r
            else:
                break
        if val is None and self._ring:
            val = self._ring[0][1]
        return val

    def _calc(self, st: dict, now: float, dt: float) -> None:
        pv = st.get('pv')
        if pv is not None:
            self._ema = float(pv) if self._ema is None else self._ema + (float(pv) - self._ema) * dt / (EMA_TAU_SEC + dt)

        # ── r1 (TC1 기준값) ──
        frozen = (self._state == 'frozen')
        if self._final is not None and self._r1 is not None:
            if self._final < self._r1:
                self._r1 = float(self._final)
            elif not self._held and not frozen and self._rate > 0:
                # 홀드백(TC1 기준): TC1 이 '열 지연을 감안한 기준값' r1(t−LAG) 보다 HOLDBACK 이상 처지면 r1 을 세운다.
                #  지금의 r1 과 비교하면 열 지연만으로(6°C/min × 약 105 s ≈ 10.5°C) 홀드백이 상시로 걸려 램프가
                #  홀드백 속도로 끌려가고, 그 처짐이 그대로 e 로 들어가 bias 가 감긴 뒤 도착 후 넘친다(시뮬레이터 확인).
                ref = self._r1_lag if self._r1_lag is not None else self._r1
                behind = (pv is not None and (float(ref) - float(pv)) > self.holdback)
                if not behind:
                    dist = float(self._final) - self._r1
                    step = approach_rate(dist, self._rate) * dt / 60.0
                    self._r1 = min(float(self._final), self._r1 + step)
        self._ring.append((now, self._r1))
        while self._ring and self._ring[0][0] < now - self.lag - 5.0:
            self._ring.popleft()
        self._r1_lag = self._r1_at(now - self.lag)
        # 비교 기준에도 TC1 과 같은 EMA 를 건다 — EMA 는 잡음을 거르는 장치라 양쪽에 똑같이 걸어야
        #  램프 중 EMA 지연(시정수만큼, 6°C/min 이면 3°C)이 오차로 들어가 bias 가 감기지 않는다
        if self._r1_lag is not None:
            self._ref_ema = (float(self._r1_lag) if self._ref_ema is None
                             else self._ref_ema + (float(self._r1_lag) - self._ref_ema) * dt / (EMA_TAU_SEC + dt))

        if self._r1 is not None and self._r1_cmd != round(self._r1, 1):
            self._r1_cmd = round(self._r1, 1)
            self.request_target.emit(self._r1_cmd)
        if self.r1_done() and not self._done_logged:
            self._done_logged = True
            self._msg("히터", f"r1 목표 도달 {self._final:.1f}°C (TC1 {self._fmt(pv)})")

        if not self.engaged():
            self._integrating = False
            return

        # ── bias 적분 ──
        sv2_now, raw, cap = self._sv2_for(st)
        e = None
        if self._ref_ema is not None and self._ema is not None:
            e = float(self._ref_ema) - float(self._ema)
        stop_why = None
        if frozen:
            stop_why = "셔터"
        elif pv is None or e is None:
            stop_why = "TC1 이상"
        elif now < self._resume_at:
            stop_why = "셔터 닫힘 직후"
        elif (raw >= cap and e > 0) or (raw <= SV2_FLOOR_C and e < 0):
            stop_why = "SV2 한계"
        elif self._inner_lagging(st, e):
            stop_why = "안쪽 루프 추종 중"
        else:
            mv = st.get('mv')
            try:
                lim = float(st.get('mv_limit') or self.mv_limit)
            except Exception:
                lim = float(self.mv_limit)
            if mv is not None:
                if e > 0 and float(mv) >= MV_SAT_REL * lim:
                    stop_why = "출력 상한"
                elif e < 0 and float(mv) <= self.mv_min + MV_FLOOR_MARGIN:
                    stop_why = "출력 하한"
        self._integrating = stop_why is None
        if not frozen:
            if stop_why is None:
                d = self.map.slope(self._r1) * e / self.ti * dt
                lim_d = self.bias_rate_max / 60.0 * dt
                d = max(-lim_d, min(lim_d, d))
                self._b_int = max(-self.bias_limit, min(self.bias_limit, self._b_int + d))
            sv2_now, raw, cap = self._sv2_for(st)
            sv2_now = round(sv2_now, 1)
            if self._sv2_cmd is None or sv2_now != round(float(self._sv2_cmd), 1):
                self._sv2_cmd = sv2_now
                self.request_sv2.emit(float(sv2_now))
            self._watch(st, now, pv, raw, cap)
        else:
            # 셔터 열림: SV2 고정. 단 목표를 내렸으면(r1 하강) 내려간 쪽만 받는다 — 낮추는 것은 언제나 안전하다
            sv2_now, _, _ = self._sv2_for(st)
            sv2_now = round(sv2_now, 1)
            if self._sv2_cmd is not None and sv2_now < round(float(self._sv2_cmd), 1) - 0.05:
                self._sv2_cmd = sv2_now
                self.request_sv2.emit(float(sv2_now))
                self._msg("히터", f"셔터 열림 중 목표 하강 — SV2 {sv2_now:.1f} 로 내림 ({self._parts()}, int 고정)")
        self._logs(st, now)

    def _inner_lagging(self, st: dict, e: float) -> bool:
        """캐스케이드 anti-windup — 안쪽 루프(TC2)가 SV2 를 못 따라가고 있는 동안에는 같은 방향으로 bias 를 감지 않는다.
        TC2 가 SV2 보다 (TC1 홀드백을 TC2 눈금으로 바꾼 폭) 이상 처져 있으면 TC1 이 늦는 원인은 매핑이 아니라 안쪽 루프다 —
        여기서 SV2 를 더 올려도 빨라지지 않고 bias 만 감겨 도착 뒤 넘친다(냉간 시동: 위상제어 출력의 바닥 구간에서
        TC2 가 몇 분간 SV2 를 못 따라가는 동안 bias +30 → 100°C 목표 +10°C 오버슈트, 시뮬레이터 확인)."""
        pv2 = st.get('pv2')
        if pv2 is None or self._sv2_cmd is None or e is None:
            return False
        gap2 = float(self._sv2_cmd) - float(pv2)
        tol2 = self.map.slope(self._r1) * self.holdback
        return (e > 0 and gap2 > tol2) or (e < 0 and gap2 < -tol2)

    def _watch(self, st: dict, now: float, pv, raw: float, cap: float) -> None:
        """TC1 탈락 경보 / SV2 상한 안내 / 매핑 학습 — running(셔터 닫힘) 에서만."""
        # TC1 탈락 의심
        if abs(self.bias) >= self.bias_alarm:                 # 기본 매핑 대비 전체 벗어남(ff + b_int)
            if not self._alarm_t0:
                self._alarm_t0 = now
            elif (now - self._alarm_t0) >= ALARM_HOLD_SEC and not self._alarm_sent:
                self._alarm_sent = True
                exp = self.map.value(self._r1)
                text = (f"TC1 이 시료대에서 떨어졌거나 TC1/TC2 관계가 크게 바뀜 — TC1 {self._fmt(pv)}, "
                        f"TC2 {self._fmt(st.get('pv2'))}, 예상 TC2 {exp:.1f} (bias {self.bias:+.1f}, "
                        f"{ALARM_HOLD_SEC:.0f}초 이상 |bias| ≥ {self.bias_alarm:g})")
                self._msg("히터(경고)", text)
                self.alert.emit("tc1_detached", {"text": text, "pv": pv, "pv2": st.get('pv2'), "expected": exp,
                                                  "bias": self.bias, "action": self.alarm_action})
        else:
            self._alarm_t0 = 0.0
        # SV2 상한 때문에 목표 도달 불가
        if (raw >= cap and self.r1_done() and self._ema is not None
                and self._ema < float(self._final) - self.soak_tol):
            if not self._cap_t0:
                self._cap_t0 = now
            elif (now - self._cap_t0) >= CAP_WARN_SEC and not self._cap_warned:
                self._cap_warned = True
                text = (f"TC2 상한 때문에 목표 도달 불가 — SV2 {cap:.1f}(= min(D00036, OT2−{self.tc2_margin:g})) 인데 "
                        f"TC1 {self._fmt(pv)} < 목표 {float(self._final):.1f}. 공정은 승온 타임아웃이 판단합니다")
                self._msg("히터(경고)", text)
                self.alert.emit("sv2_capped", {"text": text, "cap": cap, "pv": pv})
        else:
            self._cap_t0 = 0.0
        # 매핑 학습 — 최종 목표 ±tol 을 soak_sec 이상, bias 변화율 ≤ 0.2°C/min
        fin = self._final
        if (fin is not None and self.r1_done() and pv is not None and abs(float(pv) - float(fin)) <= self.soak_tol
                and round(float(fin), 1) not in self._learned_targets):
            if not self._soak_t0:
                self._soak_t0 = now; self._soak_bias0 = self.bias
            elif (now - self._soak_t0) >= self.soak_sec:
                rate = abs(self.bias - self._soak_bias0) / ((now - self._soak_t0) / 60.0)
                skip = self._learn_block_reason()
                if skip:
                    if not self._learn_skip_logged:
                        self._learn_skip_logged = True
                        self._msg("히터(경고)", f"매핑 학습 안 함 — {skip}")
                elif rate <= LEARN_BIAS_RATE_MAX:
                    self._learned_targets.add(round(float(fin), 1))
                    self._learn(float(fin), float(self._sv2_cmd))
                else:
                    self._soak_t0 = now; self._soak_bias0 = self.bias     # 아직 bias 가 움직인다 — 창을 다시 잰다
        else:
            self._soak_t0 = 0.0

    def _logs(self, st: dict, now: float) -> None:
        key = (self._r1_cmd, None if self._sv2_cmd is None else round(float(self._sv2_cmd), 1))
        if (now - self._text_log_t) >= TEXT_LOG_SEC and key != self._text_log_key:
            self._text_log_t = now; self._text_log_key = key
            self._msg("히터", f"TC1 기준 r1 {self._fmt(self._r1)} → SV2 {self._fmt(self._sv2_cmd)}"
                             f" ({self._parts()})")
        if (now - self._status_log_t) >= STATUS_LOG_SEC:
            self._status_log_t = now
            self._msg("히터", f"TC1 {self._fmt(st.get('pv'))} / r1 {self._fmt(self._r1)} / TC2 {self._fmt(st.get('pv2'))}"
                             f" / SV2 {self._fmt(self._sv2_cmd)} / {self._parts()} / {STATE_TEXT.get(self._state)}"
                             + ("" if self._integrating or self._state != 'running' else " (적분 정지)"))

    def _learn_block_reason(self) -> Optional[str]:
        """매핑 학습을 막는 사유(없으면 None) — TC1 이 떨어진 런의 TC2 가 다음 런의 출발값이 되지 않게."""
        if self._alarm_sent:
            return "이번 운전에 TC1 탈락 경보가 났음"
        if abs(self.bias) >= self.bias_alarm:
            return f"보정 과대 |bias| {abs(self.bias):.1f} ≥ {self.bias_alarm:g}°C (ff {self.bias_ff:+.1f} / int {self._b_int:+.1f})"
        return None

    def _parts(self) -> str:
        """SV2 구성 — "map 569.0 + ff +3.4 + int -1.2"."""
        return f"map {self._fmt(self._sv2_map)} + ff {self.bias_ff:+.1f} + int {self._b_int:+.1f}"

    @staticmethod
    def _fmt(v) -> str:
        return "--.-" if v is None else f"{float(v):.1f}"
