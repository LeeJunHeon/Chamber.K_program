# device/process_controller.py

# QEventLoop: 특정 신호가 올 때까지 코드 실행을 잠시 멈추고 기다리게 해주는 도구
from PySide6.QtCore import QObject, Signal, Slot, QThread, QEventLoop, QTimer

class SputterProcessController(QObject):
    # --- UI 및 다른 컨트롤러와 통신하기 위한 시그널들 ---
    status_message = Signal(str, str)       # UI 로그창에 (레벨, 메시지) 형태로 로그를 보냄
    update_plc_port = Signal(str, bool) # plc의 포트 상태 변경을 요청
    start_dc_power = Signal(float)          # DC 파워 컨트롤러에 목표 파워를 전달하며 시작 요청
    stop_dc_power = Signal()                # DC 파워 컨트롤러에 정지 요청
    stop_rf_power = Signal()                # RF 파워 컨트롤러에 정지 요청
    shutter_delay_tick = Signal(int)        # Shutter delay 타이머의 남은 시간을 UI에 보냄
    process_time_tick = Signal(int)         # 메인 공정 타이머의 남은 시간을 UI에 보냄
    stage_monitor = Signal(str)             # 현재 진행 중인 공정 단계를 UI에 표시
    finished = Signal()                     # 모든 공정이 성공적으로 완료되었음을 알림
    start_requested = Signal(dict)          # UI에서 '시작' 버튼을 누르면 공정 파라미터와 함께 이 신호 발생
    connection_failed = Signal(str)         # 장비 연결 실패 시 에러 메시지를 보냄
    critical_error = Signal(str)            # 치명적인 오류 발생 시 에러 메시지를 보냄
    start_rf_power = Signal(dict)           # RF power 파라미터를 넘기는 시그널

    def __init__(self, mfc_controller, dc_controller, rf_controller, plc_controller):
        super().__init__()
        # 각 장비의 컨트롤러 인스턴스를 내부 변수로 저장
        self.mfc = mfc_controller
        self.dc = dc_controller
        self.rf = rf_controller
        self.plc = plc_controller

        # --- 내부 상태 변수 초기화 ---
        self._is_running = False            # 현재 공정이 실행 중인지 여부
        self.gas_valve_button = None        # 선택된 가스에 따라 제어할 plc 버튼 이름 (예: 'Ar_Button')
        self.is_dc_on = False               # 이번 공정에서 DC 파워를 사용하는지 여부
        self.is_rf_on = False               # 이번 공정에서 RF 파워를 사용하는지 여부
        self.process_channel = 1            # MFC 제어 채널 (1: Ar, 2: O2)
        self._steps = []                    # 공정 단계들을 저장할 리스트
        self._current_step_idx = 0          # 현재 실행 중인 단계의 인덱스
        self._step_in_progress = False      # 한 단계가 완료되기 전에 중복 실행되는 것을 방지하기 위한 플래그
        self.params = None                  # UI로부터 받은 공정 파라미터 딕셔너리

        # [추가] 비동기 타이머 설정
        self._timer = QTimer(self)
        self._timer.setInterval(1000)  # 1초 간격
        self._timer.timeout.connect(self._on_timer_tick)
        
        self._time_left = 0
        self._current_timer_purpose = None # 'shutter' 또는 'process' 등 타이머 용도 구분

    @Slot(dict)
    def start_process_flow(self, params):
        """[슬롯] UI에서 시작 신호(start_requested)를 받으면 가장 먼저 실행되는 함수."""
        self.params = params
        self._is_running = True

        # --- 1. 사용할 장비 연결 확인 및 시도 ---
        if params.get('dc_power', 0) > 0:
            # DC 파워를 사용하는데 연결이 안 되어있으면 연결 시도
            if not self.dc.serial_dcpower and not self.dc.connect_dcpower_device():
                self.connection_failed.emit("DC Power 장치에 연결할 수 없습니다.")
                return
            
        # MFC는 항상 사용하므로 연결 확인 및 시도
        if not self.mfc.serial_mfc and not self.mfc.connect_mfc_device():
            self.connection_failed.emit("MFC 장치에 연결할 수 없습니다.")
            return

        # --- 공정 파라미터로부터 내부 상태 변수 설정 ---
        selected_gas = params.get('selected_gas', 'Ar')
        self.process_channel = 1 if selected_gas == 'Ar' else 2
        self.gas_valve_button = 'Ar_Button' if selected_gas == 'Ar' else 'O2_Button'
        self.is_dc_on = params.get('dc_power', 0) > 0
        self.is_rf_on = params.get('rf_power', 0) > 0

        # --- 2. 공정 단계 목록 생성 ---
        self._prepare_steps(params)
        self._current_step_idx = 0
        self._step_in_progress = False

        self.status_message.emit("정보", "Sputtering 공정을 시작합니다.")
        # 첫 번째 단계부터 실행 시작
        self._run_next_step()

    def _prepare_steps(self, params):
        """공정 파라미터에 맞춰 실행할 단계(step) 리스트를 미리 생성."""
        ch = self.process_channel
        flow = float(params.get("mfc_flow", 0))
        sp1 = float(params.get("sp1_set", 0))
        sp1 = sp1 / 10 # 단위 보정

        # 리스트 형태로 각 단계를 정의. (장치, 명령, 파라미터, UI에 표시될 설명)
        self._steps = [
            ("MFC", "FLOW_OFF", {"channel": ch}, f"Ch{ch} flow off, valve open, zeroing ..."),
            ("MFC", "VALVE_OPEN", {}, "Valve open"),
            ("MFC", "MFC_ZEROING", {"channel": ch}, "MFC ZEROING"),
            ("MFC", "PS_ZEROING", {}, "PS ZEROING"),
            ("PLC", self.gas_valve_button, True, f"{self.params.get('selected_gas')} Valve를 엽니다."),
            ("MFC", "FLOW_SET", {"channel": ch, "value": flow}, f"Ch{ch} 유량 {flow} sccm 설정"),
            ("MFC", "FLOW_ON", {"channel": ch}, f"Ch{ch} flow ON"),
            ("MFC", "SP4_ON", {}, "SP4 ON (압력제어 준비)"),
            ("MFC", "SP1_SET", {"value": sp1}, f"SP1(압력 목표) 값 {sp1} 설정"),
            ("DELAY", 60_000, None, "SP4 안정화 1분 대기"),
        ]

        # [수정] G1 체크박스가 선택된 경우에만 S1 Shutter 열기 단계를 추가
        if params.get('use_g1', False):
            self._steps.append(("PLC", "S1_button", True, "Gun Shutter 1(S1)을 엽니다."))

        # [수정] G2 체크박스가 선택된 경우에만 S2 Shutter 열기 단계를 추가
        if params.get('use_g2', False):
            self._steps.append(("PLC", "S2_button", True, "Gun Shutter 2(S2)를 엽니다."))

        # [수정] 나머지 단계들을 self._steps 리스트에 추가
        self._steps.extend([
            ("POWER_WAIT", None, None, "파워 안정화 대기"),
            ("MFC", "SP1_ON", {}, "SP1 ON (압력 제어 시작)"),
            ("SHUTTER_DELAY", None, None, "Shutter delay 대기"),
            ("PLC", "MS_button", True, "Main Shutter(M.S.)를 엽니다."),
            ("PROCESS_TIME", None, None, "메인 공정 대기"),
            ("PLC", "MS_button", False, "Main Shutter(M.S.)를 닫습니다."),
            ("PLC", self.gas_valve_button, False, f"{self.params.get('selected_gas')} Valve를 닫습니다."),
        ])

    def _run_next_step(self):
        """[핵심] 공정 리스트에서 다음 단계를 가져와 실행하는 '감독관' 함수."""
        print(f"[DEBUG] _run_next_step 진입, step={self._current_step_idx}")
        if not self._is_running:
            self.status_message.emit("오류", "공정이 중단되어 다음 step을 실행하지 않습니다.")
            return

        if self._step_in_progress:
            self.status_message.emit("경고", "이전 단계가 아직 진행 중입니다. 중복 실행 방지.")
            return

        # 모든 단계가 끝났으면 공정 완료 처리
        if self._current_step_idx >= len(self._steps):
            self.status_message.emit("성공", "모든 공정 시퀀스가 완료되었습니다. 장비 종료를 시작합니다.")
            # [핵심 수정] 장비들을 안전하게 종료하기 위해 stop_process 함수를 호출합니다.
            self.stop_process()
            return

        # 현재 실행할 단계(step) 정보를 가져옴
        step = self._steps[self._current_step_idx]
        device, cmd, params, desc = step

        self.stage_monitor.emit(f"[{self._current_step_idx+1}/{len(self._steps)}] {desc}")
        self.status_message.emit("공정", desc)
        self._step_in_progress = True # 현재 단계가 진행 중임을 표시

        # 장치 종류에 따라 다른 동작 수행
        if device == "MFC":
            # MFC에 명령 요청 신호를 보내고 '대기'.
            # 실제 다음 단계 진행은 _on_mfc_confirmed 슬롯이 신호를 받아 처리함.
            self.status_message.emit("정보", f"[DEBUG] MFC 명령 emit: {cmd}, {params}")
            self.mfc.command_requested.emit(cmd, params)
        elif device == "PLC":
            # [수정] 검증 로직을 제거하고, 명령을 보낸 후 0.5초 대기하고 바로 다음으로 넘어감
            self.update_plc_port.emit(cmd, params)
            QThread.msleep(1000) # 명령이 처리될 최소한의 시간을 줌

            # 즉시 다음 단계로 진행
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
        elif device == "DELAY":
            # SP4 1분 대기
            delay_ms = cmd
            QThread.msleep(delay_ms)

            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
        elif device == "POWER_WAIT":
            self._power_stabilize() # 파워 안정화 대기 함수 호출
        elif device == "SHUTTER_DELAY":
            self._shutter_delay()   # Shutter delay 대기 함수 호출
        elif device == "PROCESS_TIME":
            self._process_time()    # 메인 공정 시간 대기 함수 호출
        else:
            # 정의되지 않은 장치일 경우, 오류 메시지 후 다음 단계로 넘어감
            self.status_message.emit("오류", f"정의되지 않은 step: {device}")
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()

    @Slot(str)
    def _on_mfc_confirmed(self, cmd):
        """[슬롯] MFC로부터 '명령 성공' 신호를 받았을 때 실행됨."""
        if not self._is_running: return

        # 현재 대기중인 step이 맞는지 확인 (다른 명령의 완료 신호와 겹치는 것 방지)
        step = self._steps[self._current_step_idx]
        if step[0] == "MFC" and step[1] == cmd:
            self.status_message.emit("MFC", f"'{cmd}' 명령 성공. 다음 단계로 진행합니다.")
            self._current_step_idx += 1    # 다음 단계로 인덱스 이동
            self._step_in_progress = False # 현재 단계 완료됨을 표시
            self._run_next_step()          # 다음 단계 실행
        else:
            self.status_message.emit("경고", f"예상치 못한 MFC 확인 신호 수신: {cmd}")

    @Slot(str)
    def _on_mfc_failed(self, error_msg):
        """[슬롯] MFC로부터 '명령 실패' 신호를 받았을 때 실행됨."""
        if not self._is_running: return
        step = self._steps[self._current_step_idx]
        self.status_message.emit("MFC(실패)", f"'{step[1]}' 명령 실패: {error_msg}")
        self.status_message.emit("치명적 오류", "MFC 통신 오류로 공정을 중단합니다.")
        self._is_running = False
        self.stop_process() # 즉시 공정 중단 함수 호출

    def _power_stabilize(self):
        """DC/RF Power가 목표치에 도달하고 안정화될 때까지 대기."""
        dc_power = self.params.get('dc_power', 0)
        rf_power = self.params.get('rf_power', 0)
        rf_offset = self.params.get('rf_offset', 0)
        rf_param = self.params.get('rf_param', 1.0)

        is_dc_on = dc_power > 0
        is_rf_on = rf_power > 0

        # DC/RF 파워가 목표치에 도달하면 각 컨트롤러가 'target_reached' 신호를 보냄.
        # QEventLoop를 사용하여 이 신호가 올 때까지 이 함수의 실행을 멈추고 대기함.
        loops = []
        if is_dc_on:
            dc_loop = QEventLoop()
            self.dc.target_reached.connect(dc_loop.quit) # target_reached 신호가 오면 루프 종료
            self.start_dc_power.emit(dc_power)           # DC 파워 시작 요청
            loops.append(dc_loop)
        if is_rf_on:
            rf_loop = QEventLoop()
            self.rf.target_reached.connect(rf_loop.quit) # target_reached 신호가 오면 루프 종료
            # ▼▼▼ RF 파워 시작 요청 시 딕셔너리 형태로 파라미터를 전달 ▼▼▼
            rf_params = {
                'target': rf_power,
                'offset': rf_offset,
                'param': rf_param,
            }

            self.start_rf_power.emit(rf_params)           # RF 파워 시작 요청
            loops.append(rf_loop)

        # 두 파워가 모두 목표에 도달할 때까지 대기
        self.status_message.emit("정보", "파워 목표치 도달 대기중...")
        for loop in loops:
            loop.exec()
        self.status_message.emit("정보", "파워 안정화 완료.")

        # 대기 완료 후 다음 단계로 진행
        self._current_step_idx += 1
        self._step_in_progress = False
        self._run_next_step()

    def _shutter_delay(self):
        """[수정] Shutter delay 시간만큼 비동기적으로 대기."""
        # [핵심 수정] UI에서 '분' 단위로 입력받은 값에 60을 곱해 '초' 단위로 변환합니다.
        delay_minutes = self.params.get('shutter_delay', 0)
        delay_seconds = int(delay_minutes * 60)

        if delay_seconds <= 0:
            # 딜레이가 없으면 바로 다음 단계로
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
            return
        
        self.status_message.emit("정보", f"Shutter Delay {delay_seconds}초 대기")
        self._time_left = delay_seconds
        self._current_timer_purpose = 'shutter'
        self.shutter_delay_tick.emit(self._time_left)
        self._timer.start() # 타이머 시작! (루프와 msleep 없음)

    def _process_time(self):
        """[수정] 메인 공정 시간만큼 비동기적으로 대기."""
        # [핵심 수정] UI에서 '분' 단위로 입력받은 값에 60을 곱해 '초' 단위로 변환합니다.
        process_minutes = self.params.get('process_time', 0)
        process_seconds = int(process_minutes * 60)

        if process_seconds <= 0:
            # 공정 시간이 없으면 바로 다음 단계로
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
            return
            
        self.status_message.emit("정보", f"메인 공정 {process_seconds}초 시작")
        self.mfc.command_requested.emit("set_polling", {'enable': True})
        
        self._time_left = process_seconds
        self._current_timer_purpose = 'process'
        self.process_time_tick.emit(self._time_left)
        self._timer.start() # 타이머 시작!

    def _on_device_failure(self, error_msg):
        """(사용되지 않음) 일반적인 장치 오류 처리용 예비 함수"""
        self.status_message.emit("치명적 오류", error_msg)
        self.stop_process()

    def _on_timer_tick(self):
        """[추가] 타이머가 1초마다 호출하는 슬롯."""
        if not self._is_running:
            self._timer.stop()
            # process_time 중에 중단되었다면 polling 비활성화
            if self._current_timer_purpose == 'process':
                self.mfc.command_requested.emit("set_polling", {'enable': False})
            return

        self._time_left -= 1

        # 현재 타이머의 용도에 따라 다른 UI 업데이트 신호를 보냄
        if self._current_timer_purpose == 'shutter':
            self.shutter_delay_tick.emit(self._time_left)
        elif self._current_timer_purpose == 'process':
            self.process_time_tick.emit(self._time_left)

        # 시간이 다 되면 타이머를 멈추고 다음 단계로 진행
        if self._time_left <= 0:
            self._timer.stop()
            
            # process_time이 끝났다면 polling 비활성화
            if self._current_timer_purpose == 'process':
                self.mfc.command_requested.emit("set_polling", {'enable': False})
                
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()

    @Slot()
    def stop_process(self):
        """[수정됨] 공정을 중단하고 모든 장비를 안전하게 종료 (동기 대기 방식 적용)"""
        if not self._is_running:
            return
        
        self.status_message.emit("정보", "종료 시퀀스를 실행합니다.")
        self._is_running = False # 모든 루프(딜레이, 공정시간 등)를 중단시키는 플래그

        self.stage_monitor.emit("M.S. close...")
        self.update_plc_port.emit('MS_button', False)

        # 1. 파워 끄기
        if self.is_dc_on and self.dc:
            self.status_message.emit("DCpower", "DC 파워를 끕니다.")
            self.stop_dc_power.emit()
        if self.is_rf_on and self.rf:
            self.status_message.emit("RFpower", "RF 파워를 끕니다 (Ramp-down).")
            
            # [핵심 수정] RF 파워가 완전히 꺼질 때까지 여기서 대기하는 로직 추가
            loop = QEventLoop()
            self.rf.ramp_down_finished.connect(loop.quit)
            self.stop_rf_power.emit() # ramp_down 시작 신호
            loop.exec() # ramp_down_finished 신호가 올 때까지 대기
            self.rf.ramp_down_finished.disconnect(loop.quit) # 연결 해제
            self.status_message.emit("정보", "RF 파워 OFF 완료.")

        # 2. MFC 가스 흐름 중지 및 밸브 제어 (명령 완료 대기 로직)
        self.stage_monitor.emit("MFC flow, valve off...")
        if self.mfc:
            loop = QEventLoop()
            # 성공/실패 시 모두 루프가 종료되도록 연결
            self.mfc.command_confirmed.connect(loop.quit)
            self.mfc.command_failed.connect(loop.quit)

            # FLOW_OFF 명령 후 대기
            self.mfc.command_requested.emit("FLOW_OFF", {'channel': self.process_channel})
            loop.exec() # command_confirmed 또는 command_failed 신호가 올 때까지 대기

            # VALVE_OPEN 명령 후 대기
            self.mfc.command_requested.emit("VALVE_OPEN", {})
            loop.exec()

            # Polling 비활성화 (이 명령은 검증이 필요 없으므로 대기하지 않음)
            self.mfc.command_requested.emit("set_polling", {'enable': False})

            # 루프 연결 해제 (메모리 누수 방지)
            self.mfc.command_confirmed.disconnect(loop.quit)
            self.mfc.command_failed.disconnect(loop.quit)

        # 3. plc 포트 제어 (셔터 및 가스 밸브 닫기)
        self.stage_monitor.emit("Gun shutter close...")
        self.update_plc_port.emit('S1_button', False)
        self.update_plc_port.emit('S2_button', False)
        QThread.msleep(1000)

        self.stage_monitor.emit("Gas valve close...")
        if self.gas_valve_button:
            gas_name = "Ar" if "Ar" in self.gas_valve_button else "O2"
            self.status_message.emit("plc", f"{gas_name} Valve를 닫습니다.")
            self.update_plc_port.emit(self.gas_valve_button, False)
        QThread.msleep(1000)

        self.status_message.emit("정보", "종료 시퀀스가 안전하게 완료되었습니다.")
        self.finished.emit() # UI에 최종 종료 신호 전송