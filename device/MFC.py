# MFC.py
import serial
import inspect
from PySide6.QtCore import QObject, QThread, Signal, Slot
from lib.config import MFC_PORT, MFC_BAUDRATE, MFC_COMMANDS, FLOW_ERROR_TOLERANCE, FLOW_ERROR_MAX_COUNT

class MFCController(QObject):
    # --- 다른 컨트롤러/UI와 통신하기 위한 시그널 ---
    status_message = Signal(str, str)     # UI 로그창에 (레벨, 메시지) 형태로 로그를 보냄
    update_flow = Signal(str)             # 현재 유량 값을 UI에 보냄
    update_pressure = Signal(str)         # 현재 압력 값을 UI에 보냄
    command_requested = Signal(str, dict) # ProcessController에서 이 신호를 통해 명령을 요청함 (자기 자신에게 보내는 용도)
    command_failed = Signal(str)          # 명령 처리/검증 최종 실패 시 ProcessController에 알림
    command_confirmed = Signal(str)       # 명령 처리/검증 성공 시 ProcessController에 알림

    def __init__(self, parent=None):
        super().__init__(parent)
        self.serial_mfc = None
        self._is_running = False        # 스레드 실행 여부
        self._poll_interval_ms = 1000   # Polling 주기 (1초)
        self._polling_enabled = False   # Polling 기능 활성화 여부
        self.last_setpoints = {1: 0.0, 2: 0.0} # 유량 모니터링을 위한 마지막 설정값 저장
        self.flow_error_counters = {1: 0, 2: 0} # 유량 오차 연속 발생 횟수 카운터

    def connect_mfc_device(self):
        """MFC 장치와 시리얼 연결 및 버퍼 초기화"""
        #self.status_message.emit("DEBUG", "[connect_mfc_device] 진입")
        try:
            self.serial_mfc = serial.Serial(MFC_PORT, MFC_BAUDRATE, timeout=1)
            self.status_message.emit("MFC", "연결 성공, 버퍼 초기화를 시작합니다.")
            QThread.msleep(100)
            if self.serial_mfc.in_waiting > 0: self.serial_mfc.read_all()
            self.serial_mfc.reset_input_buffer()
            self.serial_mfc.reset_output_buffer()
            self.status_message.emit("MFC", "입출력 버퍼를 리셋했습니다.")
            return True
        except Exception as e:
            self.status_message.emit("MFC", f"연결 실패: {e}")
            return False

    @Slot()
    def start_polling(self):
        """[슬롯] 메인 공정 중 주기적으로 유량/압력을 읽어 UI에 업데이트하고, 유량 안정성을 감시."""
        #self.status_message.emit("DEBUG", "[start_polling] 진입")
        self._is_running = True
        while self._is_running:
            if self._polling_enabled:
                #self.status_message.emit("DEBUG", "[start_polling] Polling 동작")
                # 각 채널 유량 및 압력 읽기
                Ar_flow_str = self._execute_read_command('READ_FLOW', {'channel': 1})
                O2_flow_str = self._execute_read_command('READ_FLOW', {'channel': 2})
                pressure = self._execute_read_command('READ_PRESSURE')
                
                # UI 업데이트 신호 전송
                if Ar_flow_str is not None: self.update_flow.emit(f"Ar: {Ar_flow_str}")
                if O2_flow_str is not None: self.update_flow.emit(f"O2: {O2_flow_str}")
                if pressure is not None: self.update_pressure.emit(pressure)

                # 설정값과 실제 유량이 크게 차이나는지 감시
                self._monitor_flow(1, Ar_flow_str)
                self._monitor_flow(2, O2_flow_str)
            QThread.msleep(self._poll_interval_ms)

    def _send_and_log(self, cmd_str: str):
        """내부용: 명령어를 시리얼 포트로 전송하고 로그를 남김."""
        #self.status_message.emit("DEBUG", f"[_send_and_log] 송신: {cmd_str}")
        if not self.serial_mfc: 
            self.status_message.emit("ERROR", "[_send_and_log] 시리얼 없음!")
            return False
        if not cmd_str.endswith('\r'): cmd_str += '\r'
        try:
            self.serial_mfc.write(cmd_str.encode('ascii'))
            self.status_message.emit("MFC > 전송", f"{cmd_str.strip()}")
            QThread.msleep(500) # 장비가 명령을 처리할 시간 확보
            return True
        except Exception as e:
            self.status_message.emit("MFC(send)", f"명령 전송 실패: {e}"); 
            return False

    def _execute_read_command(self, read_cmd_key: str, params: dict = None) -> str | None:
        #self.status_message.emit("DEBUG", f"[_execute_read_command] {read_cmd_key} {params}")
        """내부용: 읽기 명령을 실행하고 응답 문자열을 반환."""
        if not self.serial_mfc: return None
        # 설정 파일(config)에서 명령어를 가져옴
        read_cmd_lambda = MFC_COMMANDS.get(read_cmd_key)
        if not read_cmd_lambda: 
            self.status_message.emit("ERROR", f"[_execute_read_command] 명령어 없음: {read_cmd_key}")
            return None
        # 파라미터가 있으면 채워서 명령어 문자열 생성
        read_cmd_str = read_cmd_lambda(**params) if params else read_cmd_lambda
        if self._send_and_log(read_cmd_str):
            try:
                resp = self.serial_mfc.readline().decode('ascii').strip()
                self.status_message.emit("MFC < 응답", f"{read_cmd_key} => '{resp}'")
                return resp if resp else None # 응답이 있으면 반환, 없으면 None
            except Exception: 
                self.status_message.emit("ERROR", "[_execute_read_command] 응답 파싱 실패")
                return None
        return None

    @Slot(str, dict)
    def handle_command(self, cmd: str, params: dict):
        """[슬롯][핵심] ProcessController로부터 모든 명령을 받아 처리하는 '실행자' 함수."""
        #self.status_message.emit("DEBUG", f"[handle_command] 명령: {cmd}, params: {params}")
        if not self.serial_mfc:
            self.status_message.emit("ERROR", "[handle_command] 시리얼 연결 없음")
            self.command_failed.emit("MFC 시리얼 연결 없음")
            return

        # 1. Polling 제어 같은 내부 명령 처리
        if cmd == "set_polling":
            self._polling_enabled = params.get('enable', False)
            status = "활성화" if self._polling_enabled else "비활성화"
            self.status_message.emit("MFC", f"주기적 읽기(Polling) {status}")
            # polling 비활성화 시 오차 카운터 초기화
            if not self._polling_enabled: self.flow_error_counters = {1: 0, 2: 0}
            return

        # 2. 하드웨어 제어 명령 처리
        try:
            # 명령어 검증/재시도 함수 호출
            if self._verify_command(cmd, params):
                #self.status_message.emit("DEBUG", f"[handle_command] {cmd} 명령 검증 성공")
                # 검증 성공 시 '성공' 신호 전송
                self.command_confirmed.emit(cmd)
            else:
                # 최종 실패 시 '실패' 신호 전송
                error_msg = f"MFC 명령({cmd})이 재시도 후에도 최종 실패했습니다."
                self.status_message.emit("ERROR", f"[handle_command] {error_msg}")
                self.command_failed.emit(error_msg)
        except Exception as e:
            error_msg = f"명령 처리 중 예외 발생: {e}"
            self.status_message.emit("ERROR", f"[handle_command] {error_msg}")
            self.command_failed.emit(error_msg)

    def _verify_command(self, cmd: str, params: dict) -> bool:
        """[핵심] 명령어 전송 후, 응답을 검증하고 실패 시 재시도하는 '검증관' 함수."""
        #self.status_message.emit("DEBUG", f"[_verify_command] 명령: {cmd}, params: {params}")
        MAX_RETRIES = 3 # 최대 3번까지 재시도
        command_lambda = MFC_COMMANDS.get(cmd)

        for attempt in range(MAX_RETRIES):
            # 1. 매 시도마다 명령어를 다시 생성하고 전송
            #self.status_message.emit("DEBUG", f"[_verify_command] {cmd} 시도 {attempt + 1}/{MAX_RETRIES}")
            command_str = command_lambda(**params) if params and callable(command_lambda) else command_lambda
            self._send_and_log(command_str)

            try:
                # 2. 명령어 종류별로 검증 로직을 다르게 수행
                if cmd == "FLOW_SET":
                    channel, sent_value = params['channel'], float(params['value'])
                    # 나중에 유량 모니터링을 위해 설정값을 저장
                    if channel: self.last_setpoints[channel] = sent_value
                    # 확인을 위해 '설정값 읽기' 명령을 보냄
                    read_str = self._execute_read_command('READ_FLOW_SET', {'channel': channel})
                    # 응답이 있고, 보낸 값과 거의 일치하면 성공
                    if read_str and '+' in read_str and abs(float(read_str.split('+')[1]) - sent_value) < 0.1:
                        self.status_message.emit("MFC < 확인", f"Ch{channel} 목표값 {sent_value:.1f} sccm 설정 완료."); 
                        return True

                elif cmd in ["FLOW_ON", "FLOW_OFF"]:
                    channel, state = params['channel'], (cmd == "FLOW_ON")
                    read_str = self._execute_read_command('READ_MFC_ON_OFF_STATUS')
                    if read_str and read_str.startswith("L") and len(read_str) == 6:
                        status_char = read_str[1 + channel]
                        # 응답의 해당 채널 비트가 원하는 상태(1 또는 0)와 일치하는지 먼저 확인
                        # --- FLOW_ON일 경우의 검증 로직 ---
                        if (state and status_char == '1'):
                            self.status_message.emit("MFC < 확인", f"Ch{channel} Flow ON 상태 확인. 유량 안정화 대기 시작...")
                            
                            # FLOW_SET 단계에서 설정했던 목표 유량 값을 가져옴
                            target_flow = self.last_setpoints.get(channel, 0.0)
                            tolerance = target_flow * FLOW_ERROR_TOLERANCE

                            # [핵심] 유량이 안정화될 때까지 최대 30초간 여기서 대기
                            stabilized = False
                            for _ in range(30): # 1초 간격으로 최대 30회 확인
                                # [핵심 수정] 외부에서 정지 신호가 오면 즉시 대기 종료
                                # if not self._is_running:
                                #     self.status_message.emit("MFC(경고)", "유량 안정화 중단 신호 수신됨.")
                                #     break

                                flow_str = self._execute_read_command('READ_FLOW', {'channel': channel})
                                if flow_str and '+' in flow_str:
                                    try:
                                        actual_flow = float(flow_str.split('+')[1])
                                        self.status_message.emit("MFC", f"유량 확인 중... (목표: {target_flow:.1f}, 현재: {actual_flow:.1f})")
                                        # 실제 유량이 목표치의 허용 오차 범위 내에 들어오면 성공
                                        if abs(actual_flow - target_flow) <= tolerance:
                                            self.status_message.emit("MFC < 확인", f"Ch{channel} 유량 안정화 완료.")
                                            stabilized = True
                                            break # 안정화 루프 탈출
                                    except (ValueError, IndexError):
                                        pass # 파싱 실패 시 다음 시도
                                QThread.msleep(1000) # 1초 대기

                            # 안정화에 성공했다면 최종 성공(True)을 반환
                            if stabilized:
                                return True

                        elif (not state and status_char == '0'):
                            # FLOW_OFF의 경우, 기존과 같이 상태 확인만으로 성공 처리합니다.
                            self.status_message.emit("MFC < 확인", f"Ch{channel} Flow OFF 확인.")
                            return True

                elif cmd == "VALVE_CLOSE":
                    QThread.msleep(3000) # 밸브가 움직일 시간을 줌
                    read_str = self._execute_read_command('READ_VALVE_POSITION')
                    if read_str and '+' in read_str and float(read_str.split('+')[1]) < 1.0:
                         self.status_message.emit("MFC < 확인", "밸브 닫힘 확인."); 
                         return True

                elif cmd == "VALVE_OPEN":
                    QThread.msleep(3000) # 밸브가 움직일 시간을 줌
                    read_str = self._execute_read_command('READ_VALVE_POSITION')
                    if read_str and '+' in read_str and float(read_str.split('+')[1]) > 99.0:
                         self.status_message.emit("MFC < 확인", "밸브 열림 확인."); 
                         return True

                elif cmd == "SP1_SET":
                    sent_value = float(params['value'])
                    read_str = self._execute_read_command('READ_SP1_VALUE')
                    if read_str and '+' in read_str and abs(float(read_str.split('+')[1]) - sent_value) < 0.1:
                        self.status_message.emit("MFC < 확인", f"SP1 목표값 {sent_value:.2f} 설정 완료."); 
                        return True

                elif cmd in ["SP1_ON", "SP4_ON"]:
                    read_str = self._execute_read_command('READ_SYSTEM_STATUS')
                    expected_char = '1' if cmd == "SP1_ON" else '4'
                    if read_str and read_str.startswith("M") and read_str[1] == expected_char:
                        self.status_message.emit("MFC < 확인", f"{cmd} 활성화 확인."); 
                        return True

                elif cmd in ["MFC_ZEROING", "PS_ZEROING"]:
                    # 이 명령들은 별도 확인 응답이 없으므로, 전송 자체를 성공으로 간주
                    self.status_message.emit("MFC < 확인", "별도 확인 응답 없음. 성공으로 간주."); 
                    return True

                # 검증 실패 시 로그 출력 후 재시도
                self.status_message.emit("MFC(경고)", f"명령({cmd}) 검증 실패 (시도 {attempt + 1}/{MAX_RETRIES})")
                QThread.msleep(500) # 재시도 전 잠시 대기

            except (ValueError, IndexError, TypeError) as e:
                self.status_message.emit("오류", f"검증 중 응답 파싱 오류: {e} (시도 {attempt + 1}/{MAX_RETRIES})")

        return False # 모든 재시도가 실패하면 최종적으로 False 반환

    def _monitor_flow(self, channel, actual_flow_str):
        """설정된 유량과 실제 유량을 비교하여 오차가 크면 경고 메시지를 보냄."""
        target_flow = self.last_setpoints.get(channel, 0.0)
        # 목표 유량이 0에 가까우면 모니터링 안 함
        if target_flow < 0.1: self.flow_error_counters[channel] = 0; return

        if actual_flow_str and '+' in actual_flow_str:
            try:
                actual_flow = float(actual_flow_str.split('+')[1])
                tolerance = target_flow * FLOW_ERROR_TOLERANCE # 허용 오차 범위 계산
                if abs(actual_flow - target_flow) > tolerance:
                    self.flow_error_counters[channel] += 1 # 오차 발생 시 카운터 증가
                    # 오차가 일정 횟수 이상 연속 발생하면 경고
                    if self.flow_error_counters[channel] >= FLOW_ERROR_MAX_COUNT:
                        self.status_message.emit("MFC(경고)", f"Ch{channel} 유량 불안정! (목표: {target_flow:.1f}, 현재: {actual_flow:.1f})")
                        self.flow_error_counters[channel] = 0 # 카운터 초기화
                else:
                    self.flow_error_counters[channel] = 0 # 오차 없으면 카운터 초기화
            except (ValueError, IndexError): pass

    def stop(self):
        """스레드 종료 플래그 설정"""
        self._is_running = False

    @Slot()
    def cleanup(self):
        """[슬롯] 프로그램 종료 시 안전하게 리소스를 해제"""
        self._is_running = False
        if self.serial_mfc and self.serial_mfc.is_open:
            self.serial_mfc.close()
            self.serial_mfc = None
            self.status_message.emit("MFC", "시리얼 포트를 안전하게 닫았습니다.")
            