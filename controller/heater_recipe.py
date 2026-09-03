# controller/heater_recipe.py
"""히터 전용 레시피 러너.

스퍼터 공정(controller/process_controller.py)과 완전히 독립이다.
process_controller를 import하지 않으며, ActionType도 쓰지 않는다.

[구조] RKC PZ400 프로그램 컨트롤러와 비슷한 '세그먼트 + 반복' 구조다.
  한 스텝(세그먼트) = 도달 목표값 + 소요 시간(또는 속도) + 유지 시간.
  전체 패턴을 repeat 회 반복할 수 있고, 실행 중 HOLD/STEP 조작이 가능하다.

[스레드]
  이 러너는 GUI 스레드에서 동작한다. 따라서 QEventLoop로 블로킹하면
  UI가 그대로 멈춘다. process_controller는 별도 워커 스레드에서 돌기 때문에
  QEventLoop를 쓸 수 있지만, 여기서는 쓰지 않는다.
  대신 '상태머신 + 1초 QTimer + PLC 상태 시그널 구독' 으로 구현한다.

[PLC 접근]
  레지스터를 직접 쓰지 않는다. 반드시 시그널(request_*)로 PLC 스레드의
  슬롯을 호출한다(QueuedConnection).
"""

from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import List, Dict, Optional

from PyQt6.QtCore import QObject, QTimer, pyqtSignal as Signal

from lib.recipe_io import load_table

from lib.config import (
    HEATER_MAX_TEMP,
    HEATER_RAMP_RATE_C_PER_MIN,
    HEATER_SOAK_TOLERANCE,
    HEATER_WAIT_TIMEOUT_SEC,
    HEATER_RECIPE_MAX_STEPS,
    HEATER_RECIPE_HOLD_AT_END,
)

# 마지막 스텝의 목표가 이 온도 이하이면 '냉각 스텝'으로 본다.
# 히터는 식는 속도를 제어할 수 없으므로 도달 대기를 걸면 타임아웃만 난다.
COOLDOWN_TARGET_C = 30.0

# TC 이상(pv=None)이 이 시간 이상 계속되면 중단한다.
TC_BAD_LIMIT_SEC = 60.0

# 운전 중인데 DAC 출력이 0인 상태(PID 정지)가 이 시간 이상 계속되면 중단한다.
OUT_DEAD_LIMIT_SEC = 10.0

# 가열 예정시간에 더할 여유. 도달 판정에 램프 완료 조건이 들어가면서
# HEATER_WAIT_TIMEOUT_SEC 이 스텝별 가열시간의 하드캡이 되어버렸다.
WAIT_TIMEOUT_MARGIN_SEC = 1800.0     # 30분
WAIT_TIMEOUT_MAX_SEC    = 43200.0    # 12시간 — 계산이 이상해져도 이 위로는 안 간다

# 래더 램프 목표(D00019)가 최종 목표에 닿았다고 볼 허용치.
# D00019 는 0.1°C 단위 정수라 그만큼의 여유를 둔다.
RAMP_DONE_TOL_C = 0.15

# 외부(수동/원격)에서 히터가 꺼진 상태가 이 시간 이상 계속되면 중단한다.
# 히터 ON 요청 직후 PLC 되읽기가 반영되기까지의 유예를 겸한다.
RUN_OFF_LIMIT_SEC = 5.0

# 래더 램프 생성기(H6a)는 D00023 카운트를 _T1S 마다 더하는 구조라
# 최소 1카운트/초 = 6°C/min 이다. 이보다 느린 속도는 래더가 표현하지
# 못하므로 파이썬이 SV(D00012)를 직접 밀어 올린다.
PLC_MIN_RAMP_C_PER_MIN = 6.0
SLOW_RAMP_TICK_SEC     = 3.0   # 느린 램프에서 SV 를 갱신하는 주기

# 반복 횟수 허용 범위
REPEAT_MIN, REPEAT_MAX = 1, 99

# 상태
IDLE, RAMPING, SOAKING, DONE, ABORTED = "IDLE", "RAMPING", "SOAKING", "DONE", "ABORTED"


def _ramp_raw(c_per_min: float) -> int:
    """°C/min → D00020 raw (카운트/초). 1카운트/초 = 6°C/min."""
    return max(1, int(round(float(c_per_min) / 6.0)))


def _fmt_hms(sec: float) -> str:
    """초 → M:SS (1시간 넘으면 H:MM:SS)."""
    s = max(0, int(sec))
    h, rem = divmod(s, 3600)
    m, ss = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{ss:02d}"
    return f"{m}:{ss:02d}"


class HeaterRecipeStep:
    __slots__ = ("index", "target_c", "ramp_c_per_min", "soak_min",
                 "is_cooldown", "ramp_min", "_resolved_rate")

    def __init__(self, index: int, target_c: float,
                 ramp_c_per_min: float, soak_min: float,
                 ramp_min: Optional[float] = None):
        self.index = index
        self.target_c = target_c
        self.ramp_c_per_min = ramp_c_per_min
        self.soak_min = soak_min
        self.is_cooldown = False
        # 사용자가 '소요 시간(분)'으로 적은 경우. 안 적었으면 None.
        self.ramp_min = ramp_min
        # 실제 적용될 °C/min. 출발 온도를 알아야 하므로 _advance()에서 채운다.
        self._resolved_rate = 0.0

    def describe(self) -> str:
        if self.is_cooldown:
            return f"{self.target_c:g}°C 냉각 · {self.soak_min:g}분 대기"
        if self.ramp_min:
            hours = self.ramp_min / 60.0
            span = (f"{hours:.1f}시간 가열" if hours >= 1.0
                    else f"{self.ramp_min:g}분 가열")
            return f"{self.target_c:g}°C · {span} · 유지 {self.soak_min:g}분"
        return (f"{self.target_c:g}°C · 램프 {self.ramp_c_per_min:g}°C/min"
                f" · 유지 {self.soak_min:g}분")


class HeaterRecipeRunner(QObject):
    status_message = Signal(str, str)      # (level, text)
    step_changed   = Signal(int, int, str)  # (현재 스텝, 총 스텝, 설명)
    finished       = Signal(bool, str)      # (성공 여부, 사유)

    request_target = Signal(float)          # → PLC.set_heater_target
    request_run    = Signal(bool)           # → PLC.set_heater_run
    request_ramp   = Signal(int)            # → PLC.set_heater_ramp_rate (counts/s)

    def __init__(self, plc_controller, parent=None):
        super().__init__(parent)
        self._plc = plc_controller

        self._steps: List[HeaterRecipeStep] = []
        self._path: Optional[Path] = None

        self._state = IDLE
        self._idx = -1

        self._step_started = 0.0     # RAMPING 시작 시각 (레시피 시계)
        self._step_timeout_sec = float(HEATER_WAIT_TIMEOUT_SEC)  # 이번 스텝의 가열 타임아웃
        self._tc_bad_since = 0.0
        self._out_dead_since = 0.0   # 출력 0 지속 시작 시각. 0이면 정상
        self._run_off_since = 0.0    # 외부 OFF 지속 시작 시각. 0이면 정상
        self._soak_deadline = 0.0
        self._soak_last_report = 0.0

        # --- 반복(패턴) ---
        self._repeat = 1
        self._cycle = 0              # 0-based 현재 사이클

        # --- 느린 램프(6°C/min 미만) ---
        self._slow_ramp = False
        self._slow_from = 0.0
        self._slow_to = 0.0
        self._slow_rate = 0.0        # °C/min
        self._slow_t0 = 0.0
        self._slow_last_push = 0.0

        # --- HOLD ---
        self._held = False
        self._held_at = 0.0          # 지금 정지 중이면 그 시작 시각(실시간), 아니면 0
        self._paused_total = 0.0     # 누적 일시정지 시간(초)
        self._held_target = None     # RAMPING 중 HOLD 시 보관한 원래 목표
        self._held_target_saved = None   # resume 이 _clear_hold 전에 옮겨 담는 값

        # 사용자/시스템이 의도적으로 멈춘 것인가(stop) vs 설비 이상인가(_abort).
        # 호출부가 이걸로 '공정도 같이 중단할지'를 판단한다.
        self._stopped_by_user: bool = False

        # --- 진행 표시 ---
        self._run_started = 0.0
        self._total_est_sec = 0.0
        self._percent_max = 0.0      # 진행률은 뒤로 가지 않는다(단조 증가)
        self._start_pv = 0.0

        # 1초 틱 — 상태 시그널이 끊겨도 타임아웃은 돌아야 한다
        self._tick = QTimer(self)
        self._tick.setInterval(1000)
        self._tick.timeout.connect(self._on_tick)

        try:
            self._plc.update_heater_status.connect(self._on_heater_status)
        except Exception:
            # PLC가 없는 환경(단위 테스트)에서도 load()는 되어야 한다
            pass

    # ==================== 시계 ====================
    def _now(self) -> float:
        """레시피 진행 시계. 일시정지 중에는 흐르지 않는다.

        진행 타이머(_run_started / _step_started / _slow_t0 / _soak_deadline ...)는
        전부 이 시계를 기준으로 둔다. 예전에는 타이머마다 정지 시간을 손으로
        밀어 줬는데, 타이머를 추가할 때마다 하나씩 빠뜨렸다.
        안전 타이머(_tc_bad_since / _out_dead_since / _run_off_since)는 정지 중에도
        흘러야 하므로 time.monotonic() 을 그대로 쓴다 — 여기로 바꾸지 말 것.
        """
        base = self._held_at if self._held_at else time.monotonic()
        return base - self._paused_total

    # ==================== 조회 ====================
    def steps(self) -> List[HeaterRecipeStep]:
        return list(self._steps)

    def is_running(self) -> bool:
        """HOLD 중에도 '실행 중'이다."""
        return self._state in (RAMPING, SOAKING)

    def is_held(self) -> bool:
        return bool(self._held)

    def was_user_stopped(self) -> bool:
        """마지막 종료가 stop()(의도적 중단)이었는가. _abort(설비 이상)면 False."""
        return bool(self._stopped_by_user)

    def current_step_no(self) -> int:
        """1-based. 실행 중이 아니면 0."""
        return self._idx + 1 if self.is_running() else 0

    def total_steps(self) -> int:
        return len(self._steps)

    def cycle_no(self) -> int:
        """1-based 현재 반복 회차."""
        return self._cycle + 1

    def repeat_count(self) -> int:
        return self._repeat

    def recipe_name(self) -> str:
        """불러온 레시피 파일 이름(로그 머리말용). 없으면 빈 문자열."""
        try:
            return self._path.name if self._path else ""
        except Exception:
            return ""

    def _current_step(self) -> Optional[HeaterRecipeStep]:
        """현재 스텝. 인덱스가 범위를 벗어나면 None(호출부가 안전 중단한다).

        _steps 가 실행 중에 교체되면 인덱스가 범위를 벗어날 수 있다.
        그 상태로 인덱싱하면 PLC 폴링(200ms)마다 예외가 터져 로그와
        챗 알림이 폭주한다. 실제로 그 사고가 있었다.
        """
        if 0 <= self._idx < len(self._steps):
            return self._steps[self._idx]
        return None

    def _plc_pv(self) -> Optional[float]:
        """직전 PLC 상태의 현재 온도. 못 읽으면 None."""
        try:
            pv = (self._plc.get_heater_status() or {}).get('pv')
            return None if pv is None else float(pv)
        except Exception:
            return None

    # ---------- 예상 소요 시간 ----------
    def _step_minutes(self, s: HeaterRecipeStep, from_temp: float) -> float:
        """한 스텝의 예상 소요 시간(분) = 가열 + 유지."""
        if s.is_cooldown:
            return float(s.soak_min)
        if s.ramp_min:
            ramp = float(s.ramp_min)
        else:
            rate = s.ramp_c_per_min or float(HEATER_RAMP_RATE_C_PER_MIN)
            ramp = abs(s.target_c - from_temp) / rate if rate > 0 else 0.0
        return ramp + float(s.soak_min)

    def _cycle_minutes(self, from_temp: float) -> float:
        """한 사이클(모든 스텝)의 예상 시간(분). 첫 스텝의 출발 온도를 받는다."""
        total = 0.0
        prev = float(from_temp)
        for s in self._steps:
            total += self._step_minutes(s, prev)
            prev = s.target_c
        return total

    def _estimate_total_sec(self, start_pv: float) -> float:
        """전체 예상 시간(초).

        2회차 이후의 첫 스텝은 시작 PV 가 아니라 직전 회차 마지막 스텝의 목표에서
        출발한다. 그걸 무시하면 반복 레시피의 예상 시간이 크게 부풀려진다
        (예: 60°C 2스텝 × 2회에서 2회차 스텝1 은 가열이 사실상 0인데
         시작 PV 43°C 에서 올리는 시간으로 잡혔다).
        """
        if not self._steps:
            return 0.0
        first = self._cycle_minutes(float(start_pv))
        rest = self._cycle_minutes(float(self._steps[-1].target_c))
        return (first + rest * max(0, self._repeat - 1)) * 60.0

    def _phase(self) -> str:
        """지금이 가열인지 대기인지 유지인지.

        ramp : 램프(sv_ramp)가 아직 목표에 못 닿았다
        wait : 램프는 끝났는데 PV 가 아직 허용오차 밖이다(드물다)
        soak : 목표에서 머무는 중
        """
        try:
            if self._state == SOAKING:
                return "soak"
            if self._state != RAMPING:
                return ""
            s = self._current_step()
            if s is None:
                return ""
            try:
                svr = (self._plc.get_heater_status() or {}).get('sv_ramp')
            except Exception:
                svr = None
            if svr is None:
                return "ramp"        # 못 읽으면 기존 동작에 가깝게
            return ("wait"
                    if abs(float(svr) - float(s.target_c)) <= RAMP_DONE_TOL_C
                    else "ramp")
        except Exception:
            return ""

    def _step_remain_sec(self, phase: str, soak_remain: float) -> float:
        """현재 스텝의 남은 시간(초). 계산 불가면 -1.

        가열 구간은 PV 가 아니라 sv_ramp 기준으로 잰다. PV 기준 abs() 를 쓰면
        오버슈트에서 거리가 다시 벌어져 남은 시간이 거꾸로 차오른다
        (실측: PV 45.0 → 46.9 일 때 0 → 38초로 역주행).
        """
        try:
            if phase == "soak":
                return max(0.0, float(soak_remain))

            # 램프는 끝났는데 PV 가 밴드 밖이면 언제 들어올지 알 수 없다.
            #  영영 못 들어오면 _on_tick 의 가열 타임아웃(_step_started 기준)이 잡는다.
            if phase == "wait":
                return -1.0

            if phase == "ramp":
                s = self._current_step()
                if s is None:
                    return -1.0
                rate = float(getattr(s, '_resolved_rate', 0.0) or 0.0)
                if rate <= 0:
                    return -1.0
                try:
                    svr = (self._plc.get_heater_status() or {}).get('sv_ramp')
                except Exception:
                    svr = None
                if svr is None:
                    return -1.0
                # 부호 있는 거리. abs() 를 쓰면 오버슈트에서 거리가 다시
                # 벌어져 남은 시간이 거꾸로 차오른다. 넘어섰으면 0 이다.
                #  (냉각 스텝은 _advance 가 바로 _enter_soak 하므로 여기는 항상 가열)
                gap = float(s.target_c) - float(svr)
                return max(0.0, gap / rate * 60.0)
            return -1.0
        except Exception:
            return -1.0

    def _remaining_sec(self, step_remain: float, phase: str) -> float:
        """현재 시점 기준 남은 시간(초).

        스텝 남은 시간(온도 기준)과 전체 남은 시간을 같은 근거로 맞춘다.
        시계 기준(totalEst − elapsed)과 섞으면 가열이 늦을 때 전체가 스텝보다
        작아지는 역전이 난다(실기 280s < 247+60s).
          (a) 현재 스텝 남은 시간 + 남은 유지
          (b) 같은 사이클의 이후 스텝들
          (c) 남은 반복 회차 × 1사이클
        """
        try:
            cur = self._current_step()
            # wait 은 stepRemain 이 -1(계산 불가)이지만, 남은 '유지'는 알 수 있다.
            #  이때만 0 에서 시작해 쌓는다. 그 외 -1 은 호출부가 폴백한다.
            if step_remain < 0:
                if phase != "wait":
                    return -1.0
                total = 0.0
            else:
                total = float(step_remain)

            # (a) 아직 안 지난 단계를 더한다.
            #     ramp / wait : 유지가 통째로 남았다
            #     soak        : 더할 것 없다
            if cur is not None and not cur.is_cooldown and phase in ("ramp", "wait"):
                total += float(cur.soak_min) * 60.0

            # (b) 같은 사이클의 이후 스텝 (직전 스텝 목표에서 출발한다고 본다)
            prev = float(cur.target_c) if cur is not None else 0.0
            for i, s_ in enumerate(self._steps):
                if i > self._idx:
                    total += self._step_minutes(s_, prev) * 60.0
                prev = s_.target_c

            # (c) 남은 반복 회차 — 다음 회차의 첫 스텝은 이번 회차 마지막 스텝의
            #     목표에서 출발한다(시작 PV 가 아니다)
            left_cycles = max(0, self._repeat - (self._cycle + 1))
            if left_cycles and self._steps:
                total += (self._cycle_minutes(float(self._steps[-1].target_c))
                          * 60.0 * left_cycles)
            return max(0.0, total)
        except Exception:
            return -1.0

    def progress(self) -> dict:
        """외부(ERP 리포터)에 넘길 진행 상태 요약."""
        steps = [
            {
                "no": i + 1,
                "target": s.target_c,
                "ramp": s.ramp_c_per_min,
                "rampMin": s.ramp_min,
                "soak": s.soak_min,
                "cooldown": bool(s.is_cooldown),
            }
            for i, s in enumerate(self._steps)
        ]
        remain = 0.0
        if self._state == SOAKING and self._soak_deadline > 0:
            # 레시피 시계 기준이라 정지 중에는 저절로 고정된다
            remain = max(0.0, self._soak_deadline - self._now())

        # 현재 단계(가열 / 대기 / 유지)와 그 단계의 남은 시간.
        #  계산할 수 없으면 -1 (호출부가 '계산 불가'로 구분해 --:-- 를 띄운다).
        phase = self._phase()
        step_remain = self._step_remain_sec(phase, remain)

        # 레시피 시계라 정지 중에는 elapsed 가 늘지 않는다(별도 보정 불필요)
        elapsed = (self._now() - self._run_started) if self._run_started else 0.0
        elapsed = max(0.0, elapsed)
        total_est = float(self._total_est_sec)

        # 남은 시간 — 스텝 남은 시간과 같은 근거로 계산한다.
        remain_sec = self._remaining_sec(step_remain, phase)
        if remain_sec < 0:
            remain_sec = max(0.0, total_est - elapsed)   # 폴백(기존 방식)

        # 진행률도 남은 시간 기준으로. 가열이 늦어지면 값이 줄 수 있으므로
        # 최댓값을 기억해 단조 증가시킨다(바가 뒤로 가면 안 된다).
        percent = 0.0
        denom = elapsed + remain_sec
        if denom > 0:
            percent = max(0.0, min(100.0, elapsed / denom * 100.0))
        elif total_est > 0:
            percent = max(0.0, min(100.0, elapsed / total_est * 100.0))
        try:
            if not self._held:
                self._percent_max = max(float(self._percent_max), percent)
            percent = min(100.0, float(self._percent_max))
        except Exception:
            pass

        return {
            "running": self.is_running(),
            "state": self._state,          # IDLE / RAMPING / SOAKING / DONE / ABORTED
            "stepNo": self.current_step_no(),
            "total": self.total_steps(),
            "soakRemainSec": int(remain),
            "stepRemainSec": int(step_remain),
            "phase": phase,          # ramp / wait / soak / "" 
            "remainSec": int(remain_sec),
            "steps": steps,
            # --- 진행 표시용 추가 키 ---
            "cycle": self.cycle_no(),
            "repeat": self._repeat,
            "held": bool(self._held),
            "elapsedSec": int(elapsed),
            "totalEstSec": int(total_est),
            "percent": round(percent, 1),
        }

    # ==================== 로딩 ====================
    def load(self, path) -> bool:
        """레시피 CSV를 읽는다. 실패하면 사유를 emit하고 False.

        실행 중에는 아무것도 바꾸지 않고 거부한다. start()에도 같은 가드가
        있지만, 원격 RECIPE_HEATER_RUN 은 load() → start() 순서라
        load() 가 먼저 성공하면 돌아가는 상태 기계 밑에서 _steps 가
        교체되어 버린다(2026-09-01 IndexError 폭주 사고).
        """
        if self.is_running():
            self.status_message.emit(
                "히터(경고)",
                "레시피 실행 중에는 새 레시피를 불러올 수 없습니다. 먼저 중단하세요.")
            return False

        p = Path(path)
        try:
            # CSV/TSV/XLSX 를 같은 list[dict] 로 받는다 (lib/recipe_io.py)
            rows = load_table(p, preferred_sheets=("HeaterRecipe", "heater", "히터"))
        except Exception as e:
            self.status_message.emit("히터(오류)", f"레시피 파일을 읽을 수 없습니다: {e}")
            return False

        if not rows:
            self.status_message.emit("히터(오류)", "레시피가 비어 있습니다.")
            return False

        steps: List[HeaterRecipeStep] = []
        repeat = 1
        repeat_seen = False

        for lineno, raw in enumerate(rows, start=2):
            # 컬럼명은 대소문자/공백 무시하고 매칭
            row = {str(k).strip().lower(): (v or "").strip()
                   for k, v in raw.items() if k is not None}

            # 빈 줄 / '#' 주석 행은 건너뛴다
            if not any(row.values()):
                continue
            first = (row.get('step') or "")
            if first.startswith('#'):
                continue

            try:
                target = float(row.get('target_c') or "")
            except ValueError:
                self.status_message.emit(
                    "히터(오류)", f"{lineno}행: target_c 가 숫자가 아닙니다.")
                return False
            if not (0.0 <= target <= HEATER_MAX_TEMP):
                self.status_message.emit(
                    "히터(오류)",
                    f"{lineno}행: target_c {target:g}°C 는 허용 범위(0~{HEATER_MAX_TEMP:.0f}°C)를 벗어납니다.")
                return False

            ramp_txt = row.get('ramp_c_per_min') or ""
            rmin_txt = row.get('ramp_min') or ""

            # 속도와 시간을 함께 적으면 어느 쪽을 따를지 알 수 없다
            if ramp_txt and rmin_txt:
                self.status_message.emit(
                    "히터(오류)",
                    f"{lineno}행: ramp_c_per_min 과 ramp_min 은 함께 쓸 수 없습니다")
                return False

            ramp_min: Optional[float] = None
            if rmin_txt:
                # 시간으로 적은 경우: 출발 온도를 알아야 속도가 나오므로
                # 여기서는 값만 보관하고 _advance()에서 확정한다.
                try:
                    ramp_min = float(rmin_txt)
                except ValueError:
                    self.status_message.emit(
                        "히터(오류)", f"{lineno}행: ramp_min 이 숫자가 아닙니다.")
                    return False
                if ramp_min <= 0:
                    self.status_message.emit(
                        "히터(오류)", f"{lineno}행: ramp_min 은 0보다 커야 합니다.")
                    return False
                ramp = 0.0
            else:
                if ramp_txt:
                    try:
                        ramp = float(ramp_txt)
                    except ValueError:
                        self.status_message.emit(
                            "히터(오류)", f"{lineno}행: ramp_c_per_min 이 숫자가 아닙니다.")
                        return False
                else:
                    ramp = float(HEATER_RAMP_RATE_C_PER_MIN)
                if ramp < 6.0:
                    # 1카운트/초 미만은 PLC가 표현하지 못한다.
                    # (더 느리게 올리고 싶으면 ramp_min 컬럼을 쓴다)
                    self.status_message.emit(
                        "히터", f"{lineno}행: 램프 {ramp:g} → 6 °C/min 으로 보정 (최소 1카운트/초)")
                    ramp = 6.0

            soak_txt = row.get('soak_min') or "0"
            try:
                soak = float(soak_txt)
            except ValueError:
                self.status_message.emit(
                    "히터(오류)", f"{lineno}행: soak_min 이 숫자가 아닙니다.")
                return False
            if soak < 0:
                self.status_message.emit(
                    "히터(오류)", f"{lineno}행: soak_min 은 음수일 수 없습니다.")
                return False

            # repeat 은 전체 패턴에 걸리는 값이라 첫 데이터 행만 읽는다
            if not repeat_seen:
                repeat_seen = True
                rep_txt = row.get('repeat') or ""
                if rep_txt:
                    try:
                        repeat = int(float(rep_txt))
                    except ValueError:
                        self.status_message.emit(
                            "히터(오류)", f"{lineno}행: repeat 이 숫자가 아닙니다.")
                        return False
                    if not (REPEAT_MIN <= repeat <= REPEAT_MAX):
                        self.status_message.emit(
                            "히터(오류)",
                            f"{lineno}행: repeat {repeat} 은 허용 범위"
                            f"({REPEAT_MIN}~{REPEAT_MAX})를 벗어납니다.")
                        return False

            steps.append(HeaterRecipeStep(
                len(steps) + 1, target, ramp, soak, ramp_min))

        if not steps:
            self.status_message.emit("히터(오류)", "실행할 스텝이 없습니다.")
            return False
        if len(steps) > HEATER_RECIPE_MAX_STEPS:
            self.status_message.emit(
                "히터(오류)",
                f"스텝이 {len(steps)}개입니다. 최대 {HEATER_RECIPE_MAX_STEPS}개까지만 허용됩니다.")
            return False

        # 마지막 스텝이 상온 부근이면 냉각 스텝으로 본다
        if steps[-1].target_c <= COOLDOWN_TARGET_C:
            steps[-1].is_cooldown = True

        self._steps = steps
        self._path = p
        self._repeat = repeat
        rep_txt = f" × {repeat}회 반복" if repeat > 1 else ""
        self.status_message.emit(
            "히터", f"레시피 로드: {p.name} ({len(steps)}스텝{rep_txt})")
        return True

    # ==================== 실행 ====================
    def start(self) -> bool:
        if self.is_running():
            self.status_message.emit("히터(경고)", "레시피가 이미 실행 중입니다.")
            return False
        if not self._steps:
            self.status_message.emit("히터(오류)", "먼저 레시피를 불러오세요.")
            return False

        # 인터락 확인 — 미충족이면 시작하지 않는다
        try:
            st = self._plc.get_heater_status() or {}
        except Exception:
            st = {}
        if not st.get('itl'):
            self.status_message.emit(
                "히터(오류)",
                "히터 인터락이 미충족 상태입니다. 레시피를 시작할 수 없습니다.")
            return False
        if st.get('fault'):
            self.status_message.emit(
                "히터(오류)", "히터 이상 상태입니다. 리셋 후 다시 시도하세요.")
            return False

        self._idx = -1
        self._cycle = 0
        self._state = IDLE
        self._stopped_by_user = False   # 지난 실행의 플래그를 물고 가지 않는다
        self._clear_hold()
        self._slow_ramp = False

        pv = st.get('pv')
        self._start_pv = float(pv) if pv is not None else 0.0
        self._paused_total = 0.0
        self._held_at = 0.0
        self._run_started = self._now()
        self._total_est_sec = self._estimate_total_sec(self._start_pv)
        self._percent_max = 0.0

        self._tick.start()
        rep_txt = f" × {self._repeat}회" if self._repeat > 1 else ""
        self.status_message.emit(
            "히터",
            f"레시피 시작: {len(self._steps)}스텝{rep_txt}"
            f" · 예상 {_fmt_hms(self._total_est_sec)}")
        self._advance()
        return True

    def stop(self, reason: str = "중단"):
        """어떤 경로로 들어와도 히터를 끄고 램프 속도를 원복한다.

        stop() 은 사람이/시스템이 의도적으로 부르는 경로다(원격 중단·비상 정지·
        사용자 중단·프로그램 종료). 설비 이상인 _abort() 와 구분해야, 공정 중에
        레시피만 멈췄을 때 스퍼터 공정까지 '설비 이상 실패'로 죽지 않는다.
        """
        was_running = self.is_running()
        self._stopped_by_user = True
        self._state = ABORTED
        self._out_dead_since = 0.0
        self._run_off_since = 0.0
        self._slow_ramp = False
        self._clear_hold()
        self._tick.stop()
        self._safe_shutdown()
        if was_running:
            self.status_message.emit("히터", f"레시피 중단: {reason}")
            self.finished.emit(False, reason)

    # ==================== HOLD / STEP ====================
    def _clear_hold(self):
        """HOLD 상태를 푼다. 멈춰 있던 시간을 시계에 누적한다.

        resume / skip / abort / stop 어느 경로로 들어와도 여기를 지나므로,
        타이머를 하나씩 밀어 줄 필요가 없다(_now() 가 알아서 멈춰 있었다).
        """
        if self._held and self._held_at:
            self._paused_total += max(0.0, time.monotonic() - self._held_at)
        self._held = False
        self._held_at = 0.0
        self._held_target = None

    def hold(self) -> bool:
        """현재 스텝을 그 자리에 멈춘다. 이미 HOLD면 아무것도 하지 않는다."""
        if not self.is_running():
            return False
        if self._held:
            return True

        self._held = True
        self._held_at = time.monotonic()
        pv = self._plc_pv()
        pv_txt = f"{pv:.1f}" if pv is not None else "--.-"

        if self._state == RAMPING:
            s = self._current_step()
            # 원래 목표를 보관하고, 현재 램프 목표(D00019)에 SV를 고정한다.
            self._held_target = float(s.target_c) if s is not None else None
            try:
                hold_at = (self._plc.get_heater_status() or {}).get('sv_ramp')
            except Exception:
                hold_at = None
            if hold_at is None or float(hold_at) <= 0:
                hold_at = pv if pv is not None else self._held_target
            if hold_at is not None:
                self.request_target.emit(float(hold_at))
        # SOAKING 은 따로 할 일이 없다 — _soak_deadline 이 레시피 시계 기준이라
        # 정지 중에는 (deadline − _now()) 가 저절로 고정된다.

        self.status_message.emit(
            "히터", f"스텝 {self._idx + 1} 일시정지 (현재 {pv_txt}°C)")
        self._emit_step_changed()
        return True

    def resume(self) -> bool:
        """HOLD를 풀고 멈춘 지점부터 이어간다."""
        if not self._held:
            return False
        self._held_target_saved = self._held_target   # _clear_hold 가 지우기 전에

        # 시계 보정은 _clear_hold() 가 한다. 타이머를 손으로 밀지 않는다.
        _slow = self._slow_ramp
        self._clear_hold()

        if self._state == RAMPING:
            if _slow:
                # 느린 램프에 최종 목표를 보내면 1초 뒤 램프가 중간값으로 되돌린다.
                #  (실기 로그 17:20:26~27) 대신 지금 있어야 할 값을 즉시 다시 민다.
                self._slow_last_push = 0.0
                self._push_slow_ramp(self._now())
            elif self._held_target_saved is not None:
                self.request_target.emit(float(self._held_target_saved))
        self.status_message.emit("히터", f"스텝 {self._idx + 1} 재개")
        self._emit_step_changed()
        return True

    def skip_step(self) -> bool:
        """현재 스텝을 버리고 다음 스텝으로 넘어간다."""
        if not self.is_running():
            return False
        no = self._idx + 1
        if self._held:
            self._clear_hold()
        self.status_message.emit("히터", f"스텝 {no} 건너뜀 (사용자)")
        self._advance()
        return True

    # ==================== 내부 ====================
    def _safe_shutdown(self, keep_running: bool = False):
        """히터 정지 + 램프 속도 기본값 원복. 예외를 밖으로 내보내지 않는다."""
        try:
            if not keep_running:
                self.request_run.emit(False)
        except Exception:
            pass
        try:
            self.request_ramp.emit(_ramp_raw(HEATER_RAMP_RATE_C_PER_MIN))
        except Exception:
            pass

    def _abort(self, reason: str):
        self._state = ABORTED
        self._out_dead_since = 0.0
        self._run_off_since = 0.0
        self._slow_ramp = False
        self._clear_hold()
        self._tick.stop()
        self._safe_shutdown()
        self.status_message.emit("히터(오류)", f"레시피 중단: {reason}")
        self.finished.emit(False, reason)

    def _step_desc(self, s: HeaterRecipeStep) -> str:
        """반복 중이면 사이클 정보를 앞에 붙인다."""
        if self._repeat > 1:
            return f"(반복 {self._cycle + 1}/{self._repeat}) · {s.describe()}"
        return s.describe()

    def _emit_step_changed(self):
        s = self._current_step()
        if s is None:
            return
        desc = self._step_desc(s)
        if self._held:
            desc = f"[일시정지] {desc}"
        self.step_changed.emit(self._idx + 1, len(self._steps), desc)

    def _calc_step_timeout(self, s: HeaterRecipeStep, from_temp: float) -> float:
        """이번 스텝의 가열 타임아웃(초).

        기본값보다 예정 가열시간이 길면 그쪽에 여유를 더해 쓴다. 산정 근거가
        없으면(냉각/속도 0 등) 기본값 그대로.
        """
        base = float(HEATER_WAIT_TIMEOUT_SEC)
        try:
            if s.is_cooldown:
                return base
            if s.ramp_min:
                need = float(s.ramp_min) * 60.0
            else:
                rate = float(s._resolved_rate or 0.0)
                if rate <= 0:
                    return base
                need = abs(float(s.target_c) - from_temp) / rate * 60.0
            return min(max(base, need + WAIT_TIMEOUT_MARGIN_SEC), WAIT_TIMEOUT_MAX_SEC)
        except Exception:
            return base

    def _advance(self):
        """다음 스텝으로 진입한다. 남은 스텝이 없으면 반복하거나 완료 처리."""
        self._slow_ramp = False
        self._clear_hold()
        self._idx += 1

        if self._idx >= len(self._steps):
            # 스텝이 하나도 없으면 반복 루프가 무한히 돌 수 있다
            if not self._steps:
                self._complete()
                return
            if self._cycle + 1 < self._repeat:
                # 다음 사이클 — 히터를 끄지 않고 이어서 돈다
                self._cycle += 1
                self._idx = -1
                self.status_message.emit(
                    "히터", f"반복 {self._cycle + 1}/{self._repeat} 시작")
                self._advance()
                return
            self._complete()
            return

        s = self._steps[self._idx]
        self._step_started = self._now()
        self._tc_bad_since = 0.0
        self._out_dead_since = 0.0
        self._run_off_since = 0.0

        # --- 출발 온도: 현재 PV → 직전 스텝 목표 → 0 ---
        from_temp = self._plc_pv()
        if from_temp is None:
            from_temp = (self._steps[self._idx - 1].target_c
                         if self._idx > 0 else 0.0)

        # --- 실제 적용 속도 확정 ---
        if s.ramp_min:
            span = abs(s.target_c - float(from_temp))
            s._resolved_rate = span / float(s.ramp_min) if s.ramp_min > 0 else 0.0
            self.status_message.emit(
                "히터",
                f"스텝 {self._idx + 1}: {s.target_c:g}°C 까지 {s.ramp_min:g}분"
                f" → {s._resolved_rate:.1f}°C/min 으로 가열")
        else:
            s._resolved_rate = float(s.ramp_c_per_min)

        # 이번 스텝의 가열 타임아웃을 산정한다. 기본값(HEATER_WAIT_TIMEOUT_SEC)이
        # 예정 가열시간보다 짧으면 램프가 끝나기 전에 확정적으로 중단되기 때문이다.
        self._step_timeout_sec = self._calc_step_timeout(s, float(from_temp))
        self.status_message.emit(
            "히터",
            f"스텝 {self._idx + 1}: 가열 타임아웃 {self._step_timeout_sec / 60:.0f}분")

        rate = s._resolved_rate
        if rate >= PLC_MIN_RAMP_C_PER_MIN or s.is_cooldown or rate <= 0:
            # 래더 램프 생성기가 표현할 수 있는 속도 — 지금까지와 동일
            self.request_ramp.emit(_ramp_raw(rate or HEATER_RAMP_RATE_C_PER_MIN))
            self.request_target.emit(float(s.target_c))
        else:
            # --- 느린 램프 모드 ---
            #  래더는 최소 속도(6°C/min)로 두고 파이썬이 SV 를 조금씩 밀어 올린다.
            #  파이썬이 미는 SV 가 래더 램프보다 항상 느리므로 둘이 싸우지 않는다.
            #  래더 홀드백(D00021)도 그대로 살아 있어 PV 가 뒤처지면 알아서 멈춘다.
            self._slow_ramp = True
            self._slow_from = float(from_temp)
            self._slow_to = float(s.target_c)
            self._slow_rate = rate
            self._slow_t0 = self._now()
            self._slow_last_push = 0.0
            self.request_ramp.emit(_ramp_raw(PLC_MIN_RAMP_C_PER_MIN))
            self.request_target.emit(float(from_temp))

        self.request_run.emit(True)

        self._emit_step_changed()
        self.status_message.emit(
            "히터", f"스텝 {self._idx + 1}/{len(self._steps)}: {self._step_desc(s)}")

        if s.is_cooldown:
            # 냉각은 도달 판정 없이 유지 시간만 기다린다
            self._enter_soak(s)
        else:
            self._state = RAMPING

    def _enter_soak(self, s: HeaterRecipeStep):
        self._state = SOAKING
        self._slow_ramp = False
        # 느린 램프는 파이썬이 SV를 밀어 올리는 방식이라, 여기서 밀어올림이 멈추면
        # SV가 중간값에 그대로 남는다(실제로 45°C 스텝이 43.8°C로 유지된 적이 있다).
        # 래더 램프 경로에서는 이미 같은 값이 들어가 있어 무해하다.
        try:
            self.request_target.emit(float(s.target_c))
        except Exception:
            pass
        self._soak_deadline = self._now() + s.soak_min * 60.0
        self._soak_last_report = 0.0
        if s.soak_min <= 0:
            self.status_message.emit(
                "히터", f"스텝 {self._idx + 1}: 도달 확인 (유지 없음)")
        else:
            self.status_message.emit(
                "히터", f"스텝 {self._idx + 1}: {s.target_c:g}°C 유지 {s.soak_min:g}분 시작")

    def _complete(self):
        self._state = DONE
        self._out_dead_since = 0.0
        self._run_off_since = 0.0
        self._slow_ramp = False
        self._clear_hold()
        self._tick.stop()
        last = self._steps[-1] if self._steps else None
        hold = bool(HEATER_RECIPE_HOLD_AT_END) and (last is not None and not last.is_cooldown)
        if hold:
            # 히터를 켠 채 끝낸다면 SV가 마지막 목표여야 한다. 느린 램프 도중에
            # skip_step 으로 빠져나오면 중간값이 남아 그 온도로 유지된다.
            try:
                self.request_target.emit(float(last.target_c))
            except Exception:
                pass
        self._safe_shutdown(keep_running=hold)
        msg = "레시피 완료" + (" (히터 계속 운전)" if hold else "")
        self.status_message.emit("히터", msg)
        self.finished.emit(True, msg)

    # ---------- 시그널/타이머 핸들러 ----------
    def _on_heater_status(self, st: dict):
        if self._state not in (RAMPING, SOAKING):
            return

        # --- 이상 검출은 HOLD 중에도 그대로 동작해야 한다 ---
        if st.get('fault'):
            if   st.get('ot'):     cause = "과온 트립"
            elif st.get('tc_err'): cause = "센서 이상"
            elif st.get('wd_err'): cause = "통신 두절"
            else:                  cause = "이상 발생"
            self._abort(f"히터 이상: {cause}")
            return

        # 외부(수동/원격)에서 히터가 꺼진 경우. 레시피는 도달할 수 없는
        # 온도를 타임아웃(기본 90분)까지 기다리게 되므로 즉시 중단한다.
        if not st.get('run'):
            if self._run_off_since == 0.0:
                self._run_off_since = time.monotonic()
            elif time.monotonic() - self._run_off_since >= RUN_OFF_LIMIT_SEC:
                self._abort("히터 운전이 외부에서 꺼졌습니다")
            return
        self._run_off_since = 0.0

        # 래더는 정상인데 PID가 멈춰 출력이 0으로 죽은 경우.
        # 도달할 수 없는 온도를 타임아웃까지 기다리지 않고 중단한다.
        if st.get('output_dead'):
            if self._out_dead_since == 0.0:
                self._out_dead_since = time.monotonic()
            elif time.monotonic() - self._out_dead_since >= OUT_DEAD_LIMIT_SEC:
                self._abort("히터 출력이 정지했습니다 (PID 이상)")
            return
        self._out_dead_since = 0.0

        # TC 값을 잃은 경우. 이것도 안전 판정이라 HOLD 중에도 실시간으로 센다
        #  (멈춰 둔 사이에 TC 가 빠져도 설비는 가열 중이다)
        if st.get('pv') is None:
            if self._tc_bad_since == 0.0:
                self._tc_bad_since = time.monotonic()
            elif time.monotonic() - self._tc_bad_since >= TC_BAD_LIMIT_SEC:
                self._abort("온도값을 읽을 수 없습니다 (TC 이상 60초 지속)")
            return
        self._tc_bad_since = 0.0

        # --- 여기부터는 진행 판정. HOLD 중에는 하지 않는다 ---
        if self._held:
            return

        if self._state != RAMPING:
            return

        s = self._current_step()
        if s is None:
            self._abort("내부 상태 불일치 (스텝 인덱스 범위 초과)")
            return
        pv = st.get('pv')      # 위에서 None 을 걱랬냈다

        # 램프가 아직 최종 목표에 닿지 않았으면 도달로 보지 않는다.
        #  (허용오차만으로 판정하면 유지 구간이 목표보다 낮은 온도에서 시작된다.
        #   실기에서 목표 45°C 가 43.8°C 에서 유지 시작된 적이 있다)
        _ramp_done = True
        try:
            _svr = st.get('sv_ramp')
            if _svr is not None:
                _ramp_done = abs(float(_svr) - float(s.target_c)) <= RAMP_DONE_TOL_C
        except Exception:
            _ramp_done = True      # 값을 못 읽으면 기존 동작(온도만으로 판정)

        # 램프가 끝났고 온도가 허용오차 안이면 바로 유지로 넘어간다.
        #  예전의 '도달 확인 60초'는 온도만으로 판정하던 시절의 안전장치였다.
        #  램프 완료 조건이 들어간 지금은 레시피에 없는 1분을 만들 뿐이다.
        if _ramp_done and abs(float(pv) - s.target_c) <= HEATER_SOAK_TOLERANCE:
            self.status_message.emit(
                "히터", f"스텝 {self._idx + 1}: {s.target_c:g}°C 도달 (현재 {pv:.1f}°C)")
            self._enter_soak(s)

    def _push_slow_ramp(self, now: float):
        """느린 램프: 경과 시간만큼 SV를 밀어 올린다(냉각이면 내린다)."""
        if (self._slow_last_push != 0.0
                and now - self._slow_last_push < SLOW_RAMP_TICK_SEC):
            return
        self._slow_last_push = now
        elapsed_min = (now - self._slow_t0) / 60.0
        sign = 1.0 if self._slow_to >= self._slow_from else -1.0
        target = self._slow_from + sign * self._slow_rate * elapsed_min
        # 최종 목표를 넘지 않도록 클램프
        target = min(target, self._slow_to) if sign > 0 else max(target, self._slow_to)
        self.request_target.emit(float(target))

    def _on_tick(self):
        now = self._now()

        # HOLD 중에는 타임아웃/카운트다운/램프 밀어올리기를 모두 멈춘다.
        # (이상 검출은 _on_heater_status 에서 계속 돈다)
        if self._held:
            return

        if self._state == RAMPING:
            if self._slow_ramp:
                self._push_slow_ramp(now)
            _to = float(getattr(self, "_step_timeout_sec", HEATER_WAIT_TIMEOUT_SEC))
            if now - self._step_started > _to:
                self._abort(
                    f"스텝 {self._idx + 1} 가열 시간 초과 ({_to:.0f}초)")
        elif self._state == SOAKING:
            remain = self._soak_deadline - now
            if remain <= 0:
                self._advance()
            elif (self._soak_last_report == 0.0
                  or now - self._soak_last_report >= 10.0):
                self._soak_last_report = now
                self.status_message.emit(
                    "히터",
                    f"스텝 {self._idx + 1}/{len(self._steps)} 유지 {_fmt_hms(remain)} 남음")
