"""전원 제어 루프 — v1 동작 그대로(결정 5-2-1).

DcControl: v1 device/DCpower.py 정전류 제어(_tick_ramping_up·_tick_maintaining·_apply_current_step) + lib/dc_control.py.
  MEAS:ALL? 응답 직후(DC worker) ΔI 를 계산하고 CURR 는 측정 시작 + CURR_WRITE_DELAY_S 에 쓴다(v1: 같은 1초 틱 안에서 읽고 씀).
  측정이 없거나 형식이 틀리면 그 틱은 건너뛴다(v1 과 같게 0 W 로 위장하지 않음).
  옮기지 않은 것(SafetyEngine 이 시간 기반으로 판정): 램프업 무출력(S31A)·저전류(S31B)·파워 이탈(S31C) 중단.
  남긴 것: 전류·전압 상한에 걸린 채 DC_LIMIT_STALL_SEC 이면 유지 단계로 넘김(원래 시간 기반, 4-3).
RfControl: v1 device/RFpower.py — 1초마다 RF_RAMP_STEP_W 씩 스텝 목표를 올리고(DAC = offset + (W/RF_MAX)×4000×param),
  못 미치면 DAC +1, 넘치면 −1. 유지 중 ±RF_TOLERANCE_W 밖이면 ±1 보정. 반사파 ≥ RF_REFP_ABORT_THRESHOLD 면 램프를 멈추고 기다린다
  (중단은 S32 가 RF_REFP_WAIT_SEC 로). 램프업 무출력(S33A)·유지 이탈(S33B) 중단은 SafetyEngine.
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Optional

from v2.chk import devices as D
from v2.chk.drivers.dc import CURR_WRITE_DELAY_S


def power_step_current(diff_w: float, now_v: float, gain: float, up_max: float, down_max: float) -> float:
    """lib/dc_control.power_step_current — ΔI = gain·ΔP/V, 1초당 상승·하강 상한."""
    up_max, down_max = abs(float(up_max)), abs(float(down_max))
    if now_v <= 1.0:
        return min(up_max, 0.005) if diff_w > 0 else 0.0
    di = float(gain) * float(diff_w) / float(now_v)
    return max(-down_max, min(up_max, di))


class DcControl:
    IDLE, RAMPING_UP, MAINTAINING = "IDLE", "RAMPING_UP", "MAINTAINING"

    def __init__(self, cfg: Dict[str, Any], dc, emit: Callable[[str, str], None]) -> None:
        self.cfg, self.dc, self._emit = cfg, dc, emit
        self._lock = threading.Lock()
        self.state = self.IDLE
        self.target = 0.0
        self.current = 0.0
        self.stall_since: Optional[float] = None
        self.reached = False
        self.started_ok: Optional[bool] = None
        dc.meas_listeners.append(self._on_meas)

    @property
    def voltage_guard(self) -> float:
        """전압 상한 근처(DC_MAX_VOLTAGE − 20 V) — 쓰는 때 읽는다(웹 설정 적용, 5-6 ④)."""
        return float(self.cfg["DC_MAX_VOLTAGE"]) - 20.0

    def start(self, target_w: float, tick=None) -> None:
        c = self.cfg
        with self._lock:
            self.target = max(0.0, min(float(c["DC_MAX_POWER"]), float(target_w)))
            self.current = max(0.0, min(float(c["DC_INITIAL_CURRENT"]), float(c["DC_MAX_CURRENT"])))
            self.stall_since, self.reached, self.started_ok = None, False, None
            self.state = self.RAMPING_UP
        self.dc.set_w = self.target
        self.dc.start_output(float(c["DC_MAX_VOLTAGE"]), self.current, on_done=self._started, tick=tick,
                             on_on=self._output_on)
        self._emit("info", f"DC 프로세스 시작 (목표 {self.target:.1f} W)")

    def retarget(self, target_w: float) -> None:
        """켜진 채 목표만 바꾼다(파워 켜기 블록을 다시 만났을 때) — 지금 루프가 새 목표를 따라간다. 다시 도달하면 reached."""
        with self._lock:
            self.target = max(0.0, min(float(self.cfg["DC_MAX_POWER"]), float(target_w)))
            self.reached, self.stall_since = False, None
        self.dc.set_w = self.target
        self._emit("info", f"DC 목표 변경 → {self.target:.1f} W")

    def _output_on(self) -> None:
        self.started_ok = True                          # v1: OUTP ON 을 보내면 확인 없이 제어 루프 시작(DCpower.py:438-440)

    def _started(self, name, reason, results) -> None:
        if reason:
            if self.started_ok is not True:
                self.started_ok = False
            self._emit("error", f"DC 시작 실패: {reason}")

    def stop(self) -> None:
        with self._lock:
            self.state = self.IDLE
        self.dc.set_w = None

    @property
    def holding(self) -> bool:
        return self.state == self.MAINTAINING

    def _clamp_i(self, a: float) -> float:
        return max(0.0, min(float(self.cfg["DC_MAX_CURRENT"]), a))

    def _on_meas(self, m: Dict[str, Any], t0: float) -> None:
        with self._lock:
            if self.state == self.IDLE or self.started_ok is not True:
                return
            v, i, p = m.get("dc.v"), m.get("dc.i"), m.get("dc.p")
            if v is None or i is None or p is None:
                return
            c = self.cfg
            diff = self.target - p
            tol = float(c["DC_TOLERANCE_W"])
            if self.state == self.RAMPING_UP:
                if abs(diff) <= tol:
                    self._enter_maintaining(f"{self.target:.1f}W 도달. 파워 유지 시작")
                    return
                at_i_cap = self.current >= float(c["DC_MAX_CURRENT"]) - 1e-6
                at_v_cap = v >= self.voltage_guard
                if diff > 0 and (at_i_cap or at_v_cap):
                    if self.stall_since is None:
                        self.stall_since = t0
                    if t0 - self.stall_since >= float(c["DC_LIMIT_STALL_SEC"]) - 1e-9:
                        self._enter_maintaining(f"{'전류' if at_i_cap else '전압'} 상한에 걸린 채 {c['DC_LIMIT_STALL_SEC']}s "
                                                f"목표 미달(P={p:.1f}W) — 유지 단계로 진행")
                        return
                    if at_v_cap:
                        return                          # CV: 전류 설정을 쌓지 않는다
                else:
                    self.stall_since = None
                step = power_step_current(diff, v, c["DC_CONTROL_GAIN"], c["DC_RAMP_STEP_A"], c["DC_MAINTAIN_STEP_DOWN_A"])
                self._apply(step, t0)
            else:
                if abs(diff) <= tol:
                    self.reached = True
                    return
                if diff > 0 and v >= self.voltage_guard:
                    return
                small = max(tol, float(c["DC_SMALL_ERROR_RATIO"]) * self.target)
                gain = c["DC_SMALL_ERROR_GAIN"] if abs(diff) <= small else c["DC_CONTROL_GAIN"]
                step = power_step_current(diff, v, gain, c["DC_MAINTAIN_STEP_UP_A"], c["DC_MAINTAIN_STEP_DOWN_A"])
                self._apply(step, t0)

    def _apply(self, step: float, t0: float) -> None:
        new = self._clamp_i(self.current + step)
        if abs(new - self.current) < 1e-9:
            return                                      # 상·하한에 붙었으면 다시 보내지 않는다(CAP/HOLD)
        self.current = new
        self.dc.set_current(new, not_before=t0 + CURR_WRITE_DELAY_S)

    def _enter_maintaining(self, msg: str) -> None:
        self.state, self.stall_since, self.reached = self.MAINTAINING, None, True
        self._emit("info", msg)


class RfControl:
    IDLE, RAMPING_UP, MAINTAINING = "IDLE", "RAMPING_UP", "MAINTAINING"

    def __init__(self, cfg: Dict[str, Any], plc, emit: Callable[[str, str], None]) -> None:
        self.cfg, self.plc, self._emit = cfg, plc, emit
        self.state = self.IDLE
        self.target = 0.0
        self.offset, self.param = 0.0, 1.0
        self.step_w = 0.0
        self.pwm = 0
        self.ref_waiting = False
        self.reached = False
        self.down = False                 # 목표를 낮춘 재설정(retarget) 중 — 스텝을 내린 뒤 ±1 보정

    def power_to_dac(self, w: float) -> int:
        base = (w / float(self.cfg["RF_MAX_POWER"])) * D.RF_DAC_FULL_SCALE
        return max(0, min(int(round(self.offset + base * self.param)), D.RF_DAC_FULL_SCALE))

    def start(self, target_w: float, offset: float, param: float) -> None:
        self.target = min(float(target_w), float(self.cfg["RF_MAX_POWER"]))
        self.offset, self.param = float(offset), float(param)
        self.step_w, self.pwm, self.ref_waiting, self.reached = 0.0, 0, False, False
        self.down = False
        self.state = self.RAMPING_UP
        self.plc._rf_set_w = self.target
        self._emit("info", f"RF 시작 (목표 {self.target:.1f} W, offset {self.offset:g} / param {self.param:g})")

    def retarget(self, target_w: float) -> None:
        """켜진 채 목표만 바꾼다 — 지금 스텝 목표에서 RF_RAMP_STEP_W(1 W/s)씩 새 목표로(올리든 내리든), 도달하면 유지."""
        self.target = min(float(target_w), float(self.cfg["RF_MAX_POWER"]))
        self.reached = False
        self.down = self.target < self.step_w - 1e-9
        if self.state != self.IDLE:
            self.state = self.RAMPING_UP
        self.plc._rf_set_w = self.target
        self._emit("info", f"RF 목표 변경 → {self.target:.1f} W (램프 {self.cfg['RF_RAMP_STEP_W']:g} W/s)")

    def stop(self) -> None:
        self.state = self.IDLE
        self.plc._rf_set_w = 0.0

    @property
    def holding(self) -> bool:
        return self.state == self.MAINTAINING

    def tick(self, fwd: Optional[float], ref: Optional[float]) -> None:
        """1초마다(v1 control_timer 1000 ms)."""
        if self.state == self.IDLE or fwd is None:
            return
        c = self.cfg
        if ref is not None and ref >= float(c["RF_REFP_ABORT_THRESHOLD"]):
            if not self.ref_waiting:
                self.ref_waiting = True
                self._emit("warn", f"반사파 {ref:.1f}W ≥ {c['RF_REFP_ABORT_THRESHOLD']:g}W — 대기(최대 {c['RF_REFP_WAIT_SEC']} s, S32)")
            return
        if self.ref_waiting:
            self.ref_waiting = False
            self._emit("info", f"반사파 안정 ({ref}W) — 재개")
        tol = float(c["RF_TOLERANCE_W"])
        full = D.RF_DAC_FULL_SCALE
        if self.state == self.RAMPING_UP:
            if abs(fwd - self.target) <= tol:
                self.state, self.reached = self.MAINTAINING, True
                self._emit("info", f"{self.target:g}W 도달. 파워 유지 시작")
                return
            if self.down:                                        # 목표를 낮춘 경우 — 같은 속도로 내린 뒤 유지처럼 ±1
                if self.step_w > self.target + 1e-9:
                    self.step_w = max(self.step_w - float(c["RF_RAMP_STEP_W"]), self.target)
                    self.pwm = self.power_to_dac(self.step_w)
                else:
                    self.pwm = max(0, min(full, self.pwm + (1 if fwd < self.target else -1)))
                self.plc.rf_write_dac(self.pwm)
                return
            if fwd >= self.step_w:
                self.step_w = min(self.step_w + float(c["RF_RAMP_STEP_W"]), self.target)
                self.pwm = self.power_to_dac(self.step_w)
            else:
                self.pwm = min(self.pwm + 1, full)
            if fwd > self.step_w + tol:
                self.pwm = max(0, self.pwm - 1)
            self.plc.rf_write_dac(self.pwm)
        else:
            err = self.target - fwd
            if abs(err) > tol:
                self.pwm = max(0, min(full, self.pwm + (1 if err > 0 else -1)))
                self.plc.rf_write_dac(self.pwm)
