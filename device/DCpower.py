import serial
from PySide6.QtCore import QObject, QThread, Signal, Slot, QTimer
from lib.config import (
    DC_PORT, DC_BAUDRATE,
    DC_INITIAL_VOLTAGE, DC_INITIAL_CURRENT, DC_MAX_VOLTAGE,
    DC_MAX_CURRENT, DC_MAX_POWER, DC_TOLERANCE_WATT, DC_MAX_ERROR_COUNT, 
)

class DCPowerController(QObject):
    update_dc_status_display = Signal(float, float, float)
    status_message = Signal(str, str)
    target_reached = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.target_power = 0
        self._is_running = False
        self.serial_dcpower = None
        self.current_voltage = DC_INITIAL_VOLTAGE
        self.current_current = DC_INITIAL_CURRENT
        self.error_count = 0

        self.state = "IDLE"

        self.control_timer = QTimer(self)
        self.control_timer.setInterval(1000)
        self.control_timer.timeout.connect(self._on_timer_tick)

        self.voltage_guard = DC_MAX_VOLTAGE - 20.0

    def connect_dcpower_device(self):
        try:
            self.serial_dcpower = serial.Serial(port=DC_PORT, baudrate=DC_BAUDRATE, timeout=1)
            self.status_message.emit("DCpower", "연결 성공, 버퍼 초기화를 시작합니다.")
            QThread.msleep(100)
            if self.serial_dcpower.in_waiting > 0: self.serial_dcpower.read_all()
            self.serial_dcpower.reset_input_buffer()
            self.serial_dcpower.reset_output_buffer()
            self.status_message.emit("DCpower", "입출력 버퍼를 리셋했습니다.")
            return True
        except Exception as e:
            self.status_message.emit("DCpower", f"연결 실패: {e}")
            return False

    @Slot(float)
    def start_process(self, target_power):
        if self._is_running:
            self.status_message.emit("DCpower", "경고: DC 파워가 이미 동작 중입니다.")
            return
        
        self.target_power = min(target_power, DC_MAX_POWER)
        self.status_message.emit("DCpower", f"프로세스 시작 (목표: {self.target_power}W)")

        if self.initialize_power_supply(DC_MAX_VOLTAGE, DC_INITIAL_CURRENT):
            self._is_running = True
            self.state = "RAMPING_UP"
            self.control_timer.start()

    @Slot()
    def _on_timer_tick(self):
        if not self._is_running:
            self.control_timer.stop()
            return

        now_power, now_voltage, now_current = self.read_dc_power()
        if not self._check_measurement(now_voltage, now_current):
            return

        if self.state == "RAMPING_UP":
            power_difference = self.target_power - now_power
            if abs(power_difference) <= DC_TOLERANCE_WATT:
                self.state = "MAINTAINING"
                self.status_message.emit("DCpower", f"{self.target_power}W 도달. 파워 유지 시작")
                self.target_reached.emit()
                return

            # [수정] 비례 제어 로직 도입
            # 목표와의 차이가 클수록 많이, 가까울수록 적게 전류를 조정하여 오버슈트 방지
            if now_voltage and now_voltage > 1.0:
                # 물리적으로 필요한 전류 변화량 계산 (P=VI -> dI = dP/V)
                delta_i = power_difference / now_voltage 
                # 한 번에 너무 큰 폭으로 변하지 않도록 조정폭을 제한 (최대 0.01A)
                step_i = max(-0.010, min(0.010, delta_i))
            else:
                # 전압이 매우 낮을 때는 고정값으로 안전하게 증가
                step_i = 0.005 

            self.current_current = max(DC_INITIAL_CURRENT, min(DC_MAX_CURRENT, self.current_current + step_i))
            self.send_command(f'CURR {self.current_current:.4f}')
            self.status_message.emit("DCpower", f"Ramping Up... C:{self.current_current:.4f}")

        elif self.state == "MAINTAINING":
            power_difference = self.target_power - now_power
            if abs(power_difference) <= DC_TOLERANCE_WATT:
                return

            if now_voltage >= self.voltage_guard and power_difference > 0:
                adjustment = -0.002
                self.status_message.emit("DCpower(경고)", f"전압 한계 근접({now_voltage:.1f}V). 파워 유지를 위해 전류 강제 감소.")
            else:
                adjustment = 0.001 if power_difference > 0 else -0.001

            self.current_current = max(DC_INITIAL_CURRENT, min(DC_MAX_CURRENT, self.current_current + adjustment))
            self.send_command(f'CURR {self.current_current:.4f}')
            self.status_message.emit("DCpower", f"파워 유지 보정... CURR {self.current_current:.4f}")

    def send_command(self, command):
        if not (self.serial_dcpower and self.serial_dcpower.is_open):
            self.status_message.emit("DCpower(에러)", "시리얼 포트가 열려있지 않습니다.")
            self.stop_process()
            return False
        for attempt in range(DC_MAX_ERROR_COUNT):
            try:
                if attempt > 0: self.status_message.emit("DCpower", f"'{command}' 전송 재시도... ({attempt + 1}/{DC_MAX_ERROR_COUNT})")
                else: self.status_message.emit("DCpower > 전송", command)
                self.serial_dcpower.write((command + '\n').encode())
                QThread.msleep(150)
                return True
            except Exception as e:
                self.status_message.emit("DCpower(경고)", f"시리얼 전송 예외: {e} (시도 {attempt + 1})")
                QThread.msleep(200)
                
        self.status_message.emit("DCpower(에러)", f"'{command}' 전송 최종 실패.")
        self.stop_process()
        return False

    def read_measurement(self):
        if not self.serial_dcpower: return None
        try:
            self.status_message.emit("DCpower < 수신 대기", "...")
            response = self.serial_dcpower.readline().decode().strip()
            if not response:
                self.status_message.emit("DCpower", "수신 실패")
                return None
            self.status_message.emit("DCpower", f"{response} 수신 성공")
            return float(response)
        except Exception:
            return None

    def initialize_power_supply(self, voltage, current):
        if not self.send_command('*RST'): return False
        if not self.send_command('*CLS'): return False
        if not self.send_command(f'VOLT {voltage:.2f}'): return False
        if not self.send_command(f'CURR {current:.4f}'): return False
        if not self.send_command('OUTP ON'): return False
        return True

    def _check_measurement(self, now_voltage, now_current):
        if now_voltage is None or now_current is None:
            self.error_count += 1
            self.status_message.emit("DCpower", f"측정값 오류({self.error_count}/{DC_MAX_ERROR_COUNT})")
            if self.error_count >= DC_MAX_ERROR_COUNT:
                self.status_message.emit("DCpower(에러)", "연속 측정 실패로 공정을 중단합니다.")
                self.stop_process()
            return False
        self.error_count = 0
        return True

    def read_dc_power(self):
        self.send_command('MEAS:VOLT?')
        now_voltage = self.read_measurement()
        self.send_command('MEAS:CURR?')
        now_current = self.read_measurement()
        now_power = None
        if now_voltage is not None and now_current is not None:
            now_power = now_voltage * now_current
        self.update_dc_status_display.emit(now_power, now_voltage, now_current)
        return now_power, now_voltage, now_current

    @Slot()
    def stop_process(self):
        if self._is_running:
            self.status_message.emit("DCpower", "정지 신호 수신됨.")
            self._is_running = False
            self.state = "IDLE"
            self.control_timer.stop()
            if self.serial_dcpower and self.serial_dcpower.is_open:
                self.send_command('OUTP OFF')
                self.status_message.emit("DCpower", "출력 OFF. 대기 상태로 전환합니다.")

    @Slot()
    def close_connection(self):
        self.stop_process()
        QThread.msleep(200)
        if self.serial_dcpower and self.serial_dcpower.is_open:
            self.serial_dcpower.close()
            self.serial_dcpower = None
            self.status_message.emit("DCpower", "시리얼 연결을 종료했습니다.")