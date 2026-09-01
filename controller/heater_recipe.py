# controller/heater_recipe.py
"""히터 전용 레시피 러너.

스퍼터 공정(controller/process_controller.py)과 완전히 독립이다.
process_controller를 import하지 않으며, ActionType도 쓰지 않는다.

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
    HEATER_SOAK_TIME_SEC,
    HEATER_WAIT_TIMEOUT_SEC,
    HEATER_RECIPE_MAX_STEPS,
    HEATER_RECIPE_HOLD_AT_END,
)

# 마지막 스텝의 목표가 이 온도 이하이면 '냉각 스텝'으로 본다.
# 히터는 식는 속도를 제어할 수 없으므로 도달 대기를 걸면 타임아웃만 난다.
COOLDOWN_TARGET_C = 30.0

# TC 이상(pv=None)이 이 시간 이상 계속되면 중단한다.
TC_BAD_LIMIT_SEC = 60.0

# 상태
IDLE, RAMPING, SOAKING, DONE, ABORTED = "IDLE", "RAMPING", "SOAKING", "DONE", "ABORTED"


def _ramp_raw(c_per_min: float) -> int:
    """°C/min → D00020 raw (카운트/초). 1카운트/초 = 6°C/min."""
    return max(1, int(round(float(c_per_min) / 6.0)))


class HeaterRecipeStep:
    __slots__ = ("index", "target_c", "ramp_c_per_min", "soak_min", "is_cooldown")

    def __init__(self, index: int, target_c: float,
                 ramp_c_per_min: float, soak_min: float):
        self.index = index
        self.target_c = target_c
        self.ramp_c_per_min = ramp_c_per_min
        self.soak_min = soak_min
        self.is_cooldown = False

    def describe(self) -> str:
        if self.is_cooldown:
            return f"{self.target_c:g}°C 냉각 · {self.soak_min:g}분 대기"
        return (f"{self.target_c:g}°C · 램프 {self.ramp_c_per_min:g}°C/min"
                f" · 소크 {self.soak_min:g}분")


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

        self._step_started = 0.0     # RAMPING 시작 시각 (monotonic)
        self._in_band_since = 0.0    # 허용 편차 안에 들어온 시각. 0이면 밖
        self._tc_bad_since = 0.0
        self._soak_deadline = 0.0
        self._soak_last_report = 0.0

        # 1초 틱 — 상태 시그널이 끊겨도 타임아웃은 돌아야 한다
        self._tick = QTimer(self)
        self._tick.setInterval(1000)
        self._tick.timeout.connect(self._on_tick)

        try:
            self._plc.update_heater_status.connect(self._on_heater_status)
        except Exception:
            # PLC가 없는 환경(단위 테스트)에서도 load()는 되어야 한다
            pass

    # ==================== 조회 ====================
    def steps(self) -> List[HeaterRecipeStep]:
        return list(self._steps)

    def is_running(self) -> bool:
        return self._state in (RAMPING, SOAKING)

    def current_step_no(self) -> int:
        """1-based. 실행 중이 아니면 0."""
        return self._idx + 1 if self.is_running() else 0

    def total_steps(self) -> int:
        return len(self._steps)

    # ==================== 로딩 ====================
    def load(self, path) -> bool:
        """레시피 CSV를 읽는다. 실패하면 사유를 emit하고 False."""
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
                # 1카운트/초 미만은 PLC가 표현하지 못한다
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

            steps.append(HeaterRecipeStep(len(steps) + 1, target, ramp, soak))

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
        self.status_message.emit(
            "히터", f"레시피 로드: {p.name} ({len(steps)}스텝)")
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
        self._state = IDLE
        self._tick.start()
        self.status_message.emit(
            "히터", f"레시피 시작: {len(self._steps)}스텝")
        self._advance()
        return True

    def stop(self, reason: str = "중단"):
        """어떤 경로로 들어와도 히터를 끄고 램프 속도를 원복한다."""
        was_running = self.is_running()
        self._state = ABORTED
        self._tick.stop()
        self._safe_shutdown()
        if was_running:
            self.status_message.emit("히터", f"레시피 중단: {reason}")
            self.finished.emit(False, reason)

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
        self._tick.stop()
        self._safe_shutdown()
        self.status_message.emit("히터(오류)", f"레시피 중단: {reason}")
        self.finished.emit(False, reason)

    def _advance(self):
        """다음 스텝으로 진입한다. 남은 스텝이 없으면 완료 처리."""
        self._idx += 1
        if self._idx >= len(self._steps):
            self._complete()
            return

        s = self._steps[self._idx]
        self._step_started = time.monotonic()
        self._in_band_since = 0.0
        self._tc_bad_since = 0.0

        self.request_ramp.emit(_ramp_raw(s.ramp_c_per_min))
        self.request_target.emit(float(s.target_c))
        self.request_run.emit(True)

        self.step_changed.emit(self._idx + 1, len(self._steps), s.describe())
        self.status_message.emit(
            "히터", f"스텝 {self._idx + 1}/{len(self._steps)}: {s.describe()}")

        if s.is_cooldown:
            # 냉각은 도달 판정 없이 소크 시간만 기다린다
            self._enter_soak(s)
        else:
            self._state = RAMPING

    def _enter_soak(self, s: HeaterRecipeStep):
        self._state = SOAKING
        self._soak_deadline = time.monotonic() + s.soak_min * 60.0
        self._soak_last_report = 0.0
        if s.soak_min <= 0:
            self.status_message.emit(
                "히터", f"스텝 {self._idx + 1}: 도달 확인 (소크 없음)")
        else:
            self.status_message.emit(
                "히터", f"스텝 {self._idx + 1}: 소크 {s.soak_min:g}분 시작")

    def _complete(self):
        self._state = DONE
        self._tick.stop()
        last = self._steps[-1] if self._steps else None
        hold = bool(HEATER_RECIPE_HOLD_AT_END) and (last is not None and not last.is_cooldown)
        self._safe_shutdown(keep_running=hold)
        msg = "레시피 완료" + (" (마지막 목표 유지)" if hold else "")
        self.status_message.emit("히터", msg)
        self.finished.emit(True, msg)

    # ---------- 시그널/타이머 핸들러 ----------
    def _on_heater_status(self, st: dict):
        if self._state not in (RAMPING, SOAKING):
            return

        # 이상은 상태와 무관하게 최우선
        if st.get('fault'):
            if   st.get('ot'):     cause = "과온 트립"
            elif st.get('tc_err'): cause = "센서 이상"
            elif st.get('wd_err'): cause = "통신 두절"
            else:                  cause = "이상 발생"
            self._abort(f"히터 이상: {cause}")
            return

        if self._state != RAMPING:
            return

        s = self._steps[self._idx]
        now = time.monotonic()
        pv = st.get('pv')

        if pv is None:
            # TC 이상 — 도달 판정 보류. 오래 가면 중단
            if self._tc_bad_since == 0.0:
                self._tc_bad_since = now
            elif now - self._tc_bad_since >= TC_BAD_LIMIT_SEC:
                self._abort("온도값을 읽을 수 없습니다 (TC 이상 60초 지속)")
            return
        self._tc_bad_since = 0.0

        if abs(float(pv) - s.target_c) <= HEATER_SOAK_TOLERANCE:
            if self._in_band_since == 0.0:
                self._in_band_since = now
            elif now - self._in_band_since >= HEATER_SOAK_TIME_SEC:
                self.status_message.emit(
                    "히터", f"스텝 {self._idx + 1}: {s.target_c:g}°C 도달 (현재 {pv:.1f}°C)")
                self._enter_soak(s)
        else:
            self._in_band_since = 0.0

    def _on_tick(self):
        now = time.monotonic()
        if self._state == RAMPING:
            if now - self._step_started > HEATER_WAIT_TIMEOUT_SEC:
                self._abort(
                    f"스텝 {self._idx + 1} 승온 시간 초과 ({HEATER_WAIT_TIMEOUT_SEC:.0f}초)")
        elif self._state == SOAKING:
            remain = self._soak_deadline - now
            if remain <= 0:
                self._advance()
            elif (self._soak_last_report == 0.0
                  or now - self._soak_last_report >= 10.0):
                self._soak_last_report = now
                self.status_message.emit(
                    "히터",
                    f"스텝 {self._idx + 1}/{len(self._steps)} 소크 남은 시간 {int(remain)}초")
