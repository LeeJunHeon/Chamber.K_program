# RFpower.py
import time
from PyQt6.QtCore import QObject, QThread, pyqtSignal as Signal, pyqtSlot as Slot, QTimer
from lib.config import (
    RF_MAX_POWER, RF_RAMP_STEP, RF_MAX_ERROR_COUNT, 
    RF_TOLERANCE_POWER, RF_FORWARD_SCALING_MAX_WATT, 
    RF_REFLECTED_SCALING_MAX_WATT
)
class RFPowerController(QObject):
    update_rf_status_display = Signal(float, float)
    status_message = Signal(str, str)
    target_reached = Signal()
    ramp_down_finished = Signal()

    def __init__(self, faduino, parent=None):
        super().__init__(parent)
        self.faduino = faduino
        self.target_power = 0
        self._is_running = False
        self._is_ramping_down = False
        self.current_pwm_value = 0
        self.pwm_offset_cal = 0.0
        self.pwm_param_cal = 1.0
        self.current_power_step = 0.0

        self.state = "IDLE"
        self.previous_state = "IDLE"
        self.ref_p_wait_start_time = None

        # 제어 타이머(유지)
        self.control_timer = QTimer(self)
        self.control_timer.setInterval(1000)
        self.control_timer.timeout.connect(self._on_timer_tick)

        # ★ ramp-down 타이머를 __init__에서 1회 생성
        self.ramp_down_timer = QTimer(self)
        self.ramp_down_timer.setInterval(50)
        self.ramp_down_timer.timeout.connect(self._ramp_down_step)

        # ★ RF 파워 샘플을 비동기 신호로 받음 (Main에서 신호 연결 필요)
        self._last_for = None
        self._last_ref = None

    # 외부에서 faduino.rf_power_response(float,float)와 연결
    @Slot(float, float)
    def on_rf_power_sample(self, forward_w, reflected_w):
        self._last_for = forward_w
        self._last_ref = reflected_w

    @Slot(dict)
    def start_process(self, power_params):
        if self._is_running:
            self.status_message.emit("RFpower", "경고: RF 파워가 이미 동작 중입니다.")
            return
        self.target_power   = min(power_params.get('target', 0), RF_MAX_POWER)
        self.pwm_offset_cal = power_params.get('offset', 0.0)
        self.pwm_param_cal  = power_params.get('param', 1.0)

        self.current_power_step = 0.0
        self.current_pwm_value  = 0
        self._is_running = True
        self.state = "RAMPING_UP"
        self.control_timer.start()

    @Slot()
    def _on_timer_tick(self):
        if not self._is_running:
            self.control_timer.stop()
            return

        # ★ 매 틱마다 Faduino에게 RF 읽기 요청 (비동기)
        if hasattr(self.faduino, "request_rf_read"):
            self.faduino.request_rf_read()

        for_p = self._last_for
        ref_p = self._last_ref
        if for_p is None:
            # 아직 샘플이 안들어왔으면 다음 틱에서
            return

        self.update_rf_status_display.emit(for_p, ref_p if ref_p is not None else 0.0)

        # --- 반사파 대기 로직(그대로) ---
        if ref_p is not None and ref_p > 3.0:
            if self.state != "REF_P_WAITING":
                self.previous_state = self.state
                self.state = "REF_P_WAITING"
                self.ref_p_wait_start_time = time.time()
                self.status_message.emit("RFpower(대기)", f"반사파({ref_p:.1f}W) 안정화 대기 시작(최대 30초)")
            if time.time() - self.ref_p_wait_start_time > 30:
                self.status_message.emit("재시작", "반사파 안정화 시간 초과. 중단합니다.")
                self.stop_process()
            return
        if self.state == "REF_P_WAITING":
            self.status_message.emit("RFpower(정보)", f"반사파 안정화 완료 ({ref_p:.1f}W). 재개")
            self.state = self.previous_state
            self.ref_p_wait_start_time = None

        # --- 상태 머신(기존 로직) ---
        if self.state == "RAMPING_UP":
            if abs(for_p - self.target_power) <= RF_TOLERANCE_POWER:
                self.status_message.emit("RFpower", f"{self.target_power}W 도달. 유지 모드로 전환")
                self.state = "MAINTAINING"
                self.target_reached.emit()
                return
            if for_p >= self.current_power_step:
                self.current_power_step = min(self.current_power_step + RF_RAMP_STEP, self.target_power)
                base_pwm = self.pwm_offset_cal + (self.current_power_step * self.pwm_param_cal)
                self.current_pwm_value = max(0, min(int(base_pwm), 255))
            else:
                self.current_pwm_value = min(self.current_pwm_value + 1, 255)
            if for_p > self.current_power_step + RF_TOLERANCE_POWER:
                self.current_pwm_value = max(0, self.current_pwm_value - 1)
            self.faduino.send_rfpower_command(self.current_pwm_value)
        elif self.state == "MAINTAINING":
            err = self.target_power - for_p
            if abs(err) > RF_TOLERANCE_POWER:
                self.current_pwm_value = max(0, min(255, self.current_pwm_value + (1 if err > 0 else -1)))
                self.faduino.send_rfpower_command(self.current_pwm_value)

    @Slot()
    def stop_process(self):
        if self._is_ramping_down or not self._is_running:
            return
        self.status_message.emit("RFpower", "정지 신호 수신됨.")
        self._is_running = False
        self.state = "IDLE"
        self.control_timer.stop()
        self.ramp_down()

    def ramp_down(self):
        if self._is_ramping_down:
            return
        self._is_ramping_down = True
        self.status_message.emit("RFpower", "RF 파워 ramp-down 시작")
        # ★ 여기서 새 타이머를 만들지 말고, 이미 있는 타이머 start 만
        if not self.ramp_down_timer.isActive():
            self.ramp_down_timer.start()

    def _ramp_down_step(self):
        if self.current_pwm_value > 0:
            self.current_pwm_value = max(0, self.current_pwm_value - RF_RAMP_STEP)
            self.faduino.send_rfpower_command(self.current_pwm_value)
            return
        # 종료
        self.ramp_down_timer.stop()
        self.faduino.send_rfpower_command(0)
        self.update_rf_status_display.emit(0.0, 0.0)
        self.status_message.emit("RFpower", "RF 파워 ramp-down 완료")
        self._is_ramping_down = False
        self.ramp_down_finished.emit()

    @Slot()
    def close_connection(self):
        self.stop_process()
