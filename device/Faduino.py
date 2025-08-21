# Faduino.py

"""
Faduino(파두이노) 릴레이/센서/버튼 연동 클래스
- [수정] STATE_READ 명령을 사용하여 주기적으로 상태를 읽고, UI 클릭 시에만 상태를 제어하도록 변경
- 주기적 폴링/응답/센서 처리 등 포함
"""

from PySide6.QtCore import QObject, QThread, Signal, Slot, QMutex, QTimer
import serial
import time
from lib.config import (
    FADUINO_PORT, FADUINO_BAUD,
    FADUINO_PORT_INDEX, FADUINO_SENSOR_MAP, BUTTON_TO_PORT_MAP,
)

class FaduinoController(QObject):
    status_message = Signal(str, str)
    update_button_display = Signal(str, bool)
    update_sensor_display = Signal(str, bool)
    rf_power_response = Signal(float, float)

    def __init__(self):
        super().__init__()
        self.serial_faduino = None
        self.serial_lock = QMutex()
        self._is_running = False
        # 파이썬이 기억하는 하드웨어의 마지막 상태
        self.port_states = [False] * 14
        self.port_states[FADUINO_PORT_INDEX['Door_Down']] = True # 초기 상태: Door_Down = ON

        self.polling_timer = QTimer(self)
        self.polling_timer.setInterval(200)
        self.polling_timer.timeout.connect(self._poll_status)

        self.rf_controller = None # [추가] RF 컨트롤러 참조를 저장할 변수

        # 동기식 읽기(RF파워) 작업 중 다른 통신을 막기 위한 플래그
        self._is_sync_busy = False

    @Slot()
    def _poll_status(self):
        """[새 함수] QTimer에 의해 주기적으로 호출되어 상태를 확인합니다."""
        if self._is_running and not self._is_sync_busy:
            try:
                self.send_state_read_command()
                self.read_from_faduino()
            except Exception as e:
                self.status_message.emit("Faduino", f"폴링 오류: {e}")

    @Slot()
    def start_polling(self):
        """
        [수정] 스레드 시작 시, STATE_READ 명령으로 주기적인 상태 동기화를 수행
        """
        self._is_running = True
        try:
            self.serial_faduino = serial.Serial(FADUINO_PORT, FADUINO_BAUD, timeout=1)
            self.status_message.emit("Faduino", "연결 성공, 리셋 대기...")
            QThread.msleep(2000)
            if self.serial_faduino.in_waiting > 0:
                self.serial_faduino.read_all()
            self.serial_faduino.reset_input_buffer()
            self.serial_faduino.reset_output_buffer()
            self.status_message.emit("Faduino", "입출력 버퍼를 리셋했습니다.")
        except Exception as e:
            self.status_message.emit("Faduino", f"연결 실패: {e}")
            self.serial_faduino = None
            return

        self.status_message.emit("정보", "Faduino와 주기적 상태 동기화를 시작합니다.")

        self.polling_timer.start()

    def send_state_read_command(self):
        """[새 기능] 아두이노에 'STATE_READ' 명령을 보내 현재 상태를 요청하는 함수"""
        if not self.serial_faduino:
            return

        cmd = "STATE_READ\n"
        self.serial_lock.lock()
        try:
            self.serial_faduino.write(cmd.encode('ascii'))
        except Exception as e:
            self.status_message.emit("오류", f"Faduino 상태 요청 실패: {e}")
        finally:
            self.serial_lock.unlock()

    def send_full_state_command(self):
        """내부 상태(port_states)를 기반으로 전체 비트열 제어 명령을 전송하는 함수"""
        if not self.serial_faduino:
            return

        bit_str = ''.join(['1' if state else '0' for state in self.port_states])
        cmd = f"{bit_str},0\n" # 이 명령의 PWM 값은 항상 0

        self.serial_lock.lock()
        try:
            self.serial_faduino.write(cmd.encode('ascii'))
        except Exception as e:
            self.status_message.emit("오류", f"Faduino 상태 제어 실패: {e}")
        finally:
            self.serial_lock.unlock()

    # [추가] RF 컨트롤러의 인스턴스를 받아 저장하는 함수
    def set_rf_controller(self, rf_controller):
        self.rf_controller = rf_controller

    @Slot(str, bool)
    def update_port_state(self, name, state):
        """
        요청에 기반한 제어 명령만 생성하여 전송합니다.
        """

        self._is_sync_busy = True
        self.serial_lock.lock()

        try:
            if name == 'Door_Button':
                self.port_states[FADUINO_PORT_INDEX['Door_Up']] = state
                self.port_states[FADUINO_PORT_INDEX['Door_Down']] = not state
            elif name in BUTTON_TO_PORT_MAP:
                internal_name = BUTTON_TO_PORT_MAP[name]
                if internal_name in FADUINO_PORT_INDEX:
                    idx = FADUINO_PORT_INDEX[internal_name]
                    self.port_states[idx] = state
            else:
                self._is_sync_busy = False
                return # 모르는 이름이면 무시

            bit_str = ''.join(['1' if s else '0' for s in self.port_states])

            # [수정] PWM 값을 0으로 하드코딩하는 대신, RF 컨트롤러의 현재 값을 가져옴
            current_pwm = 0
            if self.rf_controller:
                current_pwm = self.rf_controller.current_pwm_value

            cmd = f"{bit_str},{current_pwm}\n" 

            # 3. 생성된 명령을 전송합니다. 
            if self.serial_faduino and self.serial_faduino.is_open:
                self.serial_faduino.reset_input_buffer()
                self.serial_faduino.write(cmd.encode('ascii'))
                self.status_message.emit("Faduino > 제어", f"포트 '{name}' 상태를 {'ON' if state else 'OFF'}으로 변경 시도")
                QThread.msleep(100)

            # 4. [추가] UI도 즉시 반영 (폴링 응답 전이라도)
            self.update_button_display.emit(name, state)

            # Ar, O2 같은 가스 포트는 인디케이터 이름이 동일하게 매핑돼 있으면 즉시 업데이트
            if name in BUTTON_TO_PORT_MAP:
                internal_name = BUTTON_TO_PORT_MAP[name] + "_Indicator"
                if internal_name in FADUINO_SENSOR_MAP:
                    self.update_sensor_display.emit(internal_name, state)

        except Exception as e:
            self.status_message.emit("오류", f"Faduino 상태 제어 실패: {e}")
        finally:
            self.serial_lock.unlock()
            self._is_sync_busy = False


    def update_buttons_from_state(self, state_str):
        """아두이노 응답으로 내부 상태와 UI 버튼을 동기화하는 함수"""
        if len(state_str) != 14: return

        # UI 버튼 이름과 포트 이름을 매핑하여 처리
        for btn_name, port_name in BUTTON_TO_PORT_MAP.items():
            idx = FADUINO_PORT_INDEX[port_name]
            new_state = (state_str[idx] == '1')
            if self.port_states[idx] != new_state:
                self.port_states[idx] = new_state
                self.update_button_display.emit(btn_name, new_state)

        # Door 버튼도 동일하게 처리
        door_up_idx = FADUINO_PORT_INDEX['Door_Up']
        door_down_idx = FADUINO_PORT_INDEX['Door_Down']
        new_door_state = (state_str[door_up_idx] == '1')

        if self.port_states[door_up_idx] != new_door_state:
            self.port_states[door_up_idx] = new_door_state
            self.port_states[door_down_idx] = not new_door_state
            self.update_button_display.emit('Door_Button', new_door_state)

    def read_from_faduino(self):
        """아두이노로부터의 모든 응답을 읽고 파싱하는 중앙 처리 함수"""
        if not self.serial_faduino or not self.serial_faduino.is_open: return

        line = ""
        self.serial_lock.lock()
        try:
            line = self.serial_faduino.readline().decode('ascii', errors='ignore').strip()
        except Exception as e:
            self.status_message.emit("오류", f"Faduino 수신 중 예외 발생: {e}")
        finally:
            self.serial_lock.unlock()

        if not line: return

        # RF 파워 응답을 우선적으로 처리
        if line.startswith("OK:RF_READ,"):
            parts = line.split(',')
            if len(parts) == 3:
                try:
                    forward_raw = float(parts[1])
                    reflected_raw = float(parts[2])
                    self.rf_power_response.emit(forward_raw, reflected_raw)
                except (ValueError, IndexError):
                    self.status_message.emit("Faduino(경고)", "잘못된 RF 파워 응답 수신")
            return

        # 일반 상태 응답 처리 (STATE_READ 또는 제어 명령의 결과)
        if ',' in line:
            state_part, sensor_part = line.split(',', 1)
            try:
                sensor_val = int(sensor_part, 16)
                for name, bit in FADUINO_SENSOR_MAP.items():
                    is_on = (sensor_val & (1 << bit)) != 0
                    self.update_sensor_display.emit(name, is_on)
            except (ValueError, IndexError) as e:
                self.status_message.emit("Faduino(오류)", f"센서 파싱 실패: {line}, {e}")
                pass

            # "22222"가 아니어도 항상 하드웨어 상태로 UI를 동기화
            if len(state_part) == 14:
                self.update_buttons_from_state(state_part)

    def send_rfpower_command(self, pwm_value):
        """RFpower 제어 시, 현재 상태 비트열과 PWM 값을 붙여 전송"""
        if not self.serial_faduino:
            return
            
        bit_str = ''.join(['1' if state else '0' for state in self.port_states])
        cmd = f"{bit_str},{pwm_value}\n"

        self.serial_lock.lock()
        try:
            self.serial_faduino.write(cmd.encode('ascii'))
            self.status_message.emit("Faduino > 전송", f"RF파워 송신: {cmd.strip()}")
        except Exception as e:
            self.status_message.emit("오류", f"RF파워 송신 실패: {e}")
        finally:
            self.serial_lock.unlock()

    def execute_sync_read_command(self, command_to_send, expected_prefix):
        """특정 명령을 보내고, 맞는 응답이 올 때까지 동기적으로 대기 (RF 파워 읽기 전용)"""
        if not self.serial_faduino:
            return None

        self._is_sync_busy = True # 동기 작업 시작을 알림
        try:
            self.serial_lock.lock()
            self.serial_faduino.reset_input_buffer()
            self.serial_faduino.write(command_to_send.encode('ascii'))

            start_time = time.time()
            while time.time() - start_time < 1.5: # 1.5초 타임아웃
                if self.serial_faduino.in_waiting > 0:
                    line = self.serial_faduino.readline().decode('ascii', errors='ignore').strip()
                    if line.startswith(expected_prefix):
                        return line
                QThread.msleep(10)
            return None
        except Exception as e:
            self.status_message.emit("오류", f"동기 읽기 실패: {e}")
            return None
        finally:
            self._is_sync_busy = False # 동기 작업 종료
            self.serial_lock.unlock()

    @Slot()
    def on_emergency_stop(self):
        """비상 정지 시 모든 포트를 끄는 제어 명령을 전송"""
        self.status_message.emit("Faduino", "ALL STOP")
        self._is_sync_busy = True

        try:
            self.port_states = [False] * 14
            self.port_states[FADUINO_PORT_INDEX['Door_Down']] = True
            self.send_full_state_command() # 모든 포트를 끄는 명령 전송

            # UI의 모든 버튼을 끄도록 신호 발생
            for btn_name in BUTTON_TO_PORT_MAP.keys():
                self.update_button_display.emit(btn_name, False)
            self.update_button_display.emit('Door_Button', False)
            self.status_message.emit("Faduino(비상)", "EMERGENCY STOP: 모든 포트 OFF")
        except Exception as e:
            self.status_message.emit("Faduino(오류)", f"EMERGENCY STOP 실패: {e}")
        finally:
            self._is_sync_busy = False  # 다시 polling 허용  


    @Slot()
    def cleanup(self):
        """프로그램 종료 전 안전하게 리소스 해제"""
        self.polling_timer.stop()
        self._is_running = False
        QThread.msleep(250) # 스레드 루프가 완전히 종료될 시간을 줌
        if self.serial_faduino and self.serial_faduino.is_open:
            self.serial_faduino.close()
            self.serial_faduino = None
            self.status_message.emit("Faduino", "시리얼 포트를 안전하게 닫았습니다.")