# RFpower.py
import time
from PySide6.QtCore import QObject, QThread, Signal, Slot, QTimer
from lib.config import (
    RF_MAX_POWER, RF_RAMP_STEP, RF_MAX_ERROR_COUNT, 
    RF_TOLERANCE_POWER, RF_FORWARD_SCALING_MAX_WATT, 
    RF_REFLECTED_SCALING_MAX_WATT, RF_DAC_FULL_SCALE
)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from device.PLC import PLCController

class RFPowerController(QObject):
    update_rf_status_display = Signal(float, float)
    status_message = Signal(str, str)
    target_reached = Signal()
    ramp_down_finished = Signal()
    
    def __init__(self, plc=None, parent=None):
        super().__init__(parent)
        self.plc: 'PLCController' = plc
        if self.plc is None:
            raise ValueError("RFPowerController: PLCController(호환) 인스턴스가 필요합니다.")
        self.target_power = 0
        self._is_running = False
        self._is_ramping_down = False
        self.current_pwm_value = 0
        self.pwm_offset_cal = 0.0
        self.pwm_param_cal = 1.0
        
        self.current_power_step = 0.0

        self.state = "IDLE"
        # [추가] 반사파 대기 상태 관리를 위한 변수
        self.previous_state = "IDLE"
        self.ref_p_wait_start_time = None
        self.control_timer = QTimer(self)
        self.control_timer.setInterval(1000) # 1초마다 tick
        self.control_timer.timeout.connect(self._on_timer_tick)

    @Slot(dict)
    def start_process(self, power_params):
        if self._is_running:
            self.status_message.emit("RFpower", "경고: RF 파워가 이미 동작 중입니다.")
            return
        
        requested_target  = power_params.get('target', 0)
        self.target_power = min(requested_target, RF_MAX_POWER)
        self.pwm_offset_cal = power_params.get('offset', 0)
        self.pwm_param_cal = power_params.get('param', 1.0)
        
        self.current_power_step = 0.0
        self.current_pwm_value = 0

        self._is_running = True
        self.state = "RAMPING_UP"
        self.control_timer.start()
    
    @Slot()
    def _on_timer_tick(self):
        if not self._is_running:
            self.control_timer.stop()
            return

        for_p, ref_p = self.read_rf_power()
        if for_p is None:
            self.status_message.emit("RFpower(경고)", "파워 읽기 실패")
            return

        self.update_rf_status_display.emit(for_p, ref_p)

                # --- [수정된 반사파 대기 로직] ---
        if ref_p is not None and ref_p > 3.0:
            if self.state != "REF_P_WAITING":
                # 대기 상태가 아니었다면, 현재 상태를 저장하고 대기 시작
                self.previous_state = self.state
                self.state = "REF_P_WAITING"
                self.ref_p_wait_start_time = time.time()
                self.status_message.emit("RFpower(대기)", f"반사파({ref_p:.1f}W) 안정화 대기를 시작합니다. (최대 30초)")
            
            # 대기 시간 초과 확인
            if time.time() - self.ref_p_wait_start_time > 30:
                self.status_message.emit("재시작", "반사파 안정화 시간(30초) 초과. 즉시 중단합니다.")
                self.stop_process()

            return # 대기 중이므로 아래 로직은 실행하지 않고 다음 틱까지 대기

        # 반사파가 3.0W 이하로 안정된 경우
        if self.state == "REF_P_WAITING":
            self.status_message.emit("RFpower(정보)", f"반사파 안정화 완료 ({ref_p:.1f}W). 공정을 재개합니다.")
            self.state = self.previous_state # 이전 상태(RAMPING_UP 또는 MAINTAINING)로 복귀
            self.ref_p_wait_start_time = None

        # --- 상태 머신 로직 ---
        if self.state == "RAMPING_UP":
            # 1. 최종 목표 도달 시 상태 변경
            if abs(for_p - self.target_power) <= RF_TOLERANCE_POWER:
                self.status_message.emit("RFpower", f"{self.target_power}W 도달. 파워 유지 시작")
                self.state = "MAINTAINING"
                self.target_reached.emit()
                return

            # 2. 현재 스텝 목표치에 도달했는지 확인
            if for_p >= self.current_power_step:
                # [Feed-forward] 현재 스텝에 도달했으므로, 다음 스텝 목표를 설정하고 PWM 예측
                self.current_power_step = min(self.current_power_step + RF_RAMP_STEP, self.target_power)
                base_pwm = self.pwm_offset_cal + (self.current_power_step * self.pwm_param_cal)
                self.current_pwm_value = max(0, min(int(base_pwm), 255))
            else:
                # [Feedback] 아직 현재 스텝에 도달 못함 (언더슈트) -> PWM 미세 조정
                self.current_pwm_value = min(self.current_pwm_value + 1, 255)
            
            # [Feedback] 계산된 PWM이 오버슈트를 유발했다면 미세 조정
            if for_p > self.current_power_step + RF_TOLERANCE_POWER:
                 self.current_pwm_value = max(0, self.current_pwm_value - 1)

            self._send_pwm_via_plc(self.current_pwm_value)
            self.status_message.emit("RFpower", f"Ramp-Up... 스텝 목표:{self.current_power_step:.1f}W, 현재:{for_p:.1f}W (PWM:{self.current_pwm_value})")

        elif self.state == "MAINTAINING":
            error = self.target_power - for_p
            if abs(error) > RF_TOLERANCE_POWER:
                adjustment = 1 if error > 0 else -1
                self.current_pwm_value = max(0, min(255, self.current_pwm_value + adjustment))
                self._send_pwm_via_plc(self.current_pwm_value)
                self.status_message.emit("RFpower", f"파워 유지 보정... (PWM:{self.current_pwm_value})")

    def _send_pwm_via_plc(self, dac_0_4000: int):
        dac = max(0, min(int(dac_0_4000), RF_DAC_FULL_SCALE))
        self.plc.send_rfpower_command(dac)

    @Slot()
    def stop_process(self):
        if self._is_ramping_down or not self._is_running: return
        self.status_message.emit("RFpower", "정지 신호 수신됨.")
        self._is_running = False
        self.state = "IDLE"
        self.control_timer.stop()
        self.ramp_down()

    def ramp_down(self):
        if self._is_ramping_down: return
        self._is_ramping_down = True
        self.status_message.emit("RFpower", "RF 파워 ramp-down 시작")
        
        self.ramp_down_timer = QTimer(self)
        def ramp_down_step():
            if self.current_pwm_value > 0:
                self.current_pwm_value = max(0, self.current_pwm_value - RF_RAMP_STEP)
                self._send_pwm_via_plc(self.current_pwm_value)
            else:
                self.ramp_down_timer.stop()
                self._send_pwm_via_plc(0)
                self.update_rf_status_display.emit(0.0, 0.0)
                self.status_message.emit("RFpower", "RF 파워 ramp-down 완료")
                self.ramp_down_finished.emit()
                self._is_ramping_down = False

        self.ramp_down_timer.timeout.connect(ramp_down_step)
        self.ramp_down_timer.start(50)

    def read_rf_power(self):
        """PLC로부터 (forward_watt, reflected_watt) 튜플을 받는다."""
        fwd, ref = self.plc.read_rf_feedback()
        return fwd, ref
    
    @Slot()
    def close_connection(self):
        self.stop_process()
