# RFpower.py
import time
from PyQt6.QtCore import QObject, QThread, pyqtSignal as Signal, pyqtSlot as Slot, QTimer
from lib.config import (
    RF_MAX_POWER, RF_RAMP_STEP, RF_MAX_ERROR_COUNT, 
    RF_TOLERANCE_POWER, RF_FORWARD_SCALING_MAX_WATT, 
    RF_REFLECTED_SCALING_MAX_WATT, RF_DAC_FULL_SCALE,
    RF_RAMP_DOWN_STEP, RF_POWER_ERROR_RATIO, RF_POWER_ERROR_MAX_COUNT,
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
        self.power_error_count = 0 # ★ RF 파워 편차(±5%) 모니터링용 카운터
        self.previous_state = "IDLE" # [추가] 반사파 대기 상태 관리를 위한 변수
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

        # ★ 새 공정 시작 시 편차 카운터/반사파 대기 상태 초기화
        self.power_error_count = 0
        self.ref_p_wait_start_time = None

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
        if ref_p is not None and ref_p > 30.0:
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

        # 반사파가 30.0W 이하로 안정된 경우
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
                # 다음 스텝으로 1W 올라가며, 그 스텝에 해당하는 DAC 예측값을 바로 사용
                self.current_power_step = min(self.current_power_step + RF_RAMP_STEP, self.target_power)
                self.current_pwm_value = self._power_to_dac(self.current_power_step)
            else:
                # 아직 스텝 목표 전력에 못 미치면 DAC를 소폭 올려 미세 보정
                self.current_pwm_value = min(self.current_pwm_value + 1, RF_DAC_FULL_SCALE)
            
            # [Feedback] 계산된 PWM이 오버슈트를 유발했다면 미세 조정
            if for_p > self.current_power_step + RF_TOLERANCE_POWER:
                 self.current_pwm_value = max(0, self.current_pwm_value - 1)

            self._send_pwm_via_plc(self.current_pwm_value)
            self.status_message.emit("RFpower", f"Ramp-Up... 스텝 목표:{self.current_power_step:.1f}W, 현재:{for_p:.1f}W (PWM:{self.current_pwm_value})")

        elif self.state == "MAINTAINING":
            error = self.target_power - for_p

            # ★ Forward Power가 목표 대비 ±5% 이상 연속 3회 벗어나면 공정 중단
            threshold_w = max(RF_TOLERANCE_POWER, self.target_power * RF_POWER_ERROR_RATIO)
            if abs(error) > threshold_w:
                self.power_error_count += 1
                if self.power_error_count >= RF_POWER_ERROR_MAX_COUNT:
                    self.status_message.emit(
                        "재시작",
                        (
                            f"RF Forward Power가 목표 {self.target_power:.1f}W에서 "
                            f"±{RF_POWER_ERROR_RATIO*100:.1f}% 이상 "
                            f"연속 {RF_POWER_ERROR_MAX_COUNT}회 벗어났습니다. 공정을 중단합니다."
                        ),
                    )
                    self.stop_process()
                    return
            else:
                # 허용 범위 안으로 돌아오면 카운터 리셋
                self.power_error_count = 0

            if abs(error) > RF_TOLERANCE_POWER:
                adjustment = 1 if error > 0 else -1
                self.current_pwm_value = max(0, min(RF_DAC_FULL_SCALE, self.current_pwm_value + adjustment))
                self._send_pwm_via_plc(self.current_pwm_value)
                self.status_message.emit("RFpower", f"파워 유지 보정... (PWM:{self.current_pwm_value})")

    def _send_pwm_via_plc(self, dac_0_4000: int):
        dac = max(0, min(int(dac_0_4000), RF_DAC_FULL_SCALE))
        self.plc.send_rfpower_command(dac)

    @Slot()
    def stop_process(self):
        # 이미 램프다운 중이면 두 번 하지 않음
        if self._is_ramping_down:
            return

        # 🔵 아직 RF가 한 번도 켜진 적 없는 상태에서 stop이 들어온 경우
        if not self._is_running:
            # ProcessController 쪽에서는 ramp_down_finished를 기다리고 있기 때문에
            # 여기서 바로 "아무것도 할 것 없음"을 알려주고 종료 신호를 보낸다.
            self.status_message.emit("RFpower", "이미 정지 상태 → 램프다운 없이 종료")
            # UI도 안전하게 0으로 정리
            self.update_rf_status_display.emit(0.0, 0.0)
            # ★ 중요: ramp_down_finished를 emit 해서 wait 루프를 깨운다
            self.ramp_down_finished.emit()
            return

        # 🔵 실제로 동작 중일 때는 기존 로직대로 램프다운
        # ★ 여기서부터는 실제 동작 중이므로 편차 카운터/반사파 대기 상태 초기화
        self.power_error_count = 0
        self.ref_p_wait_start_time = None
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
                self.current_pwm_value = max(0, self.current_pwm_value - RF_RAMP_DOWN_STEP)
                self._send_pwm_via_plc(self.current_pwm_value)
            else:
                self.ramp_down_timer.stop()
                self._send_pwm_via_plc(0)
                self.update_rf_status_display.emit(0.0, 0.0)
                self.status_message.emit("RFpower", "RF 파워 ramp-down 완료")
                self.ramp_down_finished.emit()
                self._is_ramping_down = False

        self.ramp_down_timer.timeout.connect(ramp_down_step)
        self.ramp_down_timer.start(500)

    def read_rf_power(self):
        """PLC로부터 (forward_watt, reflected_watt) 튜플을 받는다."""
        fwd, ref = self.plc.read_rf_feedback()
        return fwd, ref
    
    def _power_to_dac(self, power_w: float) -> int:
        """
        원하는 전력[W]를 DAC 카운트[0..RF_DAC_FULL_SCALE]로 변환.
        기본 스케일 (power / RF_MAX_POWER) * FULL_SCALE 에
        게인(param)과 오프셋(offset)을 적용.
        """
        base = (power_w / RF_MAX_POWER) * RF_DAC_FULL_SCALE
        counts = self.pwm_offset_cal + (base * self.pwm_param_cal)
        return max(0, min(int(round(counts)), RF_DAC_FULL_SCALE))
    
    @Slot()
    def close_connection(self):
        self.stop_process()
