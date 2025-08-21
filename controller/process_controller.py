# device/process_controller.py  (CH-K 수정본, PySide6)

from PySide6.QtCore import QObject, Signal, Slot, QThread, QEventLoop, Qt, QTimer
import math

class SputterProcessController(QObject):
    # --- UI 및 다른 컨트롤러와 통신하기 위한 시그널들 ---
    status_message = Signal(str, str)        # UI 로그창에 (레벨, 메시지)
    update_faduino_port = Signal(str, bool)  # Faduino 포트 상태 변경
    start_dc_power = Signal(float)           # DC 파워 시작 요청 (목표치)
    stop_dc_power = Signal()                 # DC 파워 정지 요청
    start_rf_power = Signal(dict)            # RF 파워 시작 요청 ({'target','offset','param'})
    stop_rf_power = Signal()                 # RF 파워 정지 요청

    shutter_delay_tick = Signal(int)         # Shutter delay 남은 초
    process_time_tick = Signal(int)          # 메인 공정 남은 초
    stage_monitor = Signal(str)              # 현재 단계 텍스트
    finished = Signal()                      # 전체 공정 종료
    start_requested = Signal(dict)           # UI → 공정 시작 파라미터
    connection_failed = Signal(str)          # 장비 연결 실패
    critical_error = Signal(str)             # 치명적 오류
    command_requested = Signal(str, dict)    # Process → MFC 명령 라우팅

    def __init__(self, mfc_controller, dc_controller, rf_controller, faduino_controller):
        super().__init__()
        self.mfc = mfc_controller
        self.dc = dc_controller
        self.rf = rf_controller
        self.faduino = faduino_controller

        # 내부 상태
        self._is_running = False
        self.gas_valve_button = None
        self.is_dc_on = False
        self.is_rf_on = False
        self.process_channel = 1
        self._steps = []
        self._current_step_idx = 0
        self._step_in_progress = False
        self.params = None

        # 타이머(스레드-로컬 생성)
        self._timer = None
        self._time_left = 0
        self._current_timer_purpose = None  # 'shutter' | 'process' | 'delay'

    # --- 타이머 셋업 (ProcessThread에서만 호출) ---
    @Slot()
    def _setup_timers(self):
        if self._timer is None:
            self._timer = QTimer(self)
            self._timer.setInterval(1000)
            self._timer.setTimerType(Qt.PreciseTimer)
            self._timer.timeout.connect(self._on_timer_tick)

    # --- 공정 시작 ---
    @Slot(dict)
    def start_process_flow(self, params):
        """UI의 start_requested 신호를 받아 공정 시작."""
        self.params = params
        self._is_running = True

        # 1) 장비 연결 확인/시도
        if params.get('dc_power', 0) > 0:
            if not self.dc.serial_dcpower and not self.dc.connect_dcpower_device():
                self.connection_failed.emit("DC Power 장치에 연결할 수 없습니다.")
                return

        # MFC는 항상 필요
        if not getattr(self.mfc, "serial_mfc", None) and not self.mfc.connect_mfc_device():
            self.connection_failed.emit("MFC 장치에 연결할 수 없습니다.")
            return

        # 2) 내부 상태
        selected_gas = params.get('selected_gas', 'Ar')
        self.process_channel = 1 if selected_gas == 'Ar' else 2
        self.gas_valve_button = 'Ar_Button' if selected_gas == 'Ar' else 'O2_Button'
        self.is_dc_on = params.get('dc_power', 0) > 0
        self.is_rf_on = params.get('rf_power', 0) > 0

        # 3) 단계 생성 및 시작
        self._prepare_steps(params)
        self._current_step_idx = 0
        self._step_in_progress = False

        self.status_message.emit("정보", "Sputtering 공정을 시작합니다.")
        self._run_next_step()

    def _prepare_steps(self, params):
        ch = self.process_channel
        flow = float(params.get("mfc_flow", 0))
        sp1 = float(params.get("sp1_set", 0)) / 10.0  # 단위 보정

        steps = [
            ("MFC", "FLOW_OFF", {"channel": ch}, f"Ch{ch} flow off, valve open, zeroing ..."),
            ("MFC", "VALVE_OPEN", {}, "Valve open"),
            ("MFC", "MFC_ZEROING", {"channel": ch}, "MFC ZEROING"),
            ("MFC", "PS_ZEROING", {}, "PS ZEROING"),
            ("FADUINO", self.gas_valve_button, True, f"{self.params.get('selected_gas')} Valve를 엽니다."),
            ("MFC", "FLOW_SET", {"channel": ch, "value": flow}, f"Ch{ch} 유량 {flow} sccm 설정"),
            ("MFC", "FLOW_ON", {"channel": ch}, f"Ch{ch} flow ON"),
            ("MFC", "SP4_ON", {}, "SP4 ON (압력제어 준비)"),
            ("MFC", "SP1_SET", {"value": sp1}, f"SP1(압력 목표) 값 {sp1} 설정"),
            ("DELAY", 60, None, "SP4 안정화 1분 대기"),  # 초 단위
        ]

        if params.get('use_g1', False):
            steps.append(("FADUINO", "S1_button", True, "Gun Shutter 1(S1)을 엽니다."))
        if params.get('use_g2', False):
            steps.append(("FADUINO", "S2_button", True, "Gun Shutter 2(S2)를 엽니다."))

        steps.extend([
            ("POWER_WAIT", None, None, "파워 안정화 대기"),
            ("MFC", "SP1_ON", {}, "SP1 ON (압력 제어 시작)"),
            ("SHUTTER_DELAY", None, None, "Shutter delay 대기"),
            ("FADUINO", "MS_button", True, "Main Shutter(M.S.)를 엽니다."),
            ("PROCESS_TIME", None, None, "메인 공정 대기"),
            ("FADUINO", "MS_button", False, "Main Shutter(M.S.)를 닫습니다."),
            ("FADUINO", self.gas_valve_button, False, f"{self.params.get('selected_gas')} Valve를 닫습니다."),
        ])
        self._steps = steps

    def _run_next_step(self):
        if not self._is_running:
            self.status_message.emit("오류", "공정이 중단되어 다음 step을 실행하지 않습니다.")
            return
        if self._step_in_progress:
            self.status_message.emit("경고", "이전 단계가 아직 진행 중입니다. 중복 실행 방지.")
            return
        if self._current_step_idx >= len(self._steps):
            self.status_message.emit("성공", "모든 공정 시퀀스가 완료되었습니다. 장비 종료를 시작합니다.")
            self.stop_process()
            return

        device, cmd, params, desc = self._steps[self._current_step_idx]
        self.stage_monitor.emit(f"[{self._current_step_idx+1}/{len(self._steps)}] {desc}")
        self.status_message.emit("공정", desc)
        self._step_in_progress = True

        if device == "MFC":
            self.status_message.emit("정보", f"[DEBUG] MFC 명령 emit: {cmd}, {params}")
            self.command_requested.emit(cmd, params)

        elif device == "FADUINO":
            self.update_faduino_port.emit(cmd, params)
            QThread.msleep(1000)
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()

        elif device == "DELAY":
            # ★ 버그수정: 지연 후 즉시 진행하지 말고 타이머로 끝날 때 넘어감
            seconds = int(cmd) if cmd else 0
            self._delay_seconds(seconds)

        elif device == "POWER_WAIT":
            self._power_stabilize()

        elif device == "SHUTTER_DELAY":
            self._shutter_delay()

        elif device == "PROCESS_TIME":
            self._process_time()

        else:
            self.status_message.emit("오류", f"정의되지 않은 step: {device}")
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()

    # --- MFC 콜백 ---
    @Slot(str)
    def _on_mfc_confirmed(self, cmd):
        if not self._is_running:
            return
        step = self._steps[self._current_step_idx]
        if step[0] == "MFC" and step[1] == cmd:
            self.status_message.emit("MFC", f"'{cmd}' 명령 성공. 다음 단계로 진행합니다.")
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
        else:
            self.status_message.emit("경고", f"예상치 못한 MFC 확인 신호 수신: {cmd}")

    @Slot(str)
    def _on_mfc_failed(self, error_msg):
        if not self._is_running:
            return
        step = self._steps[self._current_step_idx]
        self.status_message.emit("MFC(실패)", f"'{step[1]}' 명령 실패: {error_msg}")
        self.status_message.emit("치명적 오류", "MFC 통신 오류로 공정을 중단합니다.")
        self._is_running = False
        self.stop_process()

    # --- 파워 안정화 ---
    def _power_stabilize(self):
        dc_power = self.params.get('dc_power', 0.0)
        rf_power = self.params.get('rf_power', 0.0)
        rf_offset = self.params.get('rf_offset', 0.0)
        rf_param = self.params.get('rf_param', 1.0)

        is_dc_on = dc_power > 0
        is_rf_on = rf_power > 0

        loops = []
        if is_dc_on:
            dc_loop = QEventLoop()
            self.dc.target_reached.connect(dc_loop.quit)
            self.start_dc_power.emit(dc_power)
            loops.append(dc_loop)

        if is_rf_on:
            rf_loop = QEventLoop()
            self.rf.target_reached.connect(rf_loop.quit)
            rf_params = {'target': rf_power, 'offset': rf_offset, 'param': rf_param}
            self.start_rf_power.emit(rf_params)
            loops.append(rf_loop)

        self.status_message.emit("정보", "파워 목표치 도달 대기중...")
        for loop in loops:
            loop.exec()
        self.status_message.emit("정보", "파워 안정화 완료.")

        self._current_step_idx += 1
        self._step_in_progress = False
        self._run_next_step()

    # --- 타이머 기반 대기들 ---
    def _shutter_delay(self):
        delay_seconds = math.ceil(self.params.get('shutter_delay', 0) * 60)
        if delay_seconds <= 0:
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
            return
        self.status_message.emit("정보", f"Shutter Delay {delay_seconds}초 대기")
        self._time_left = delay_seconds
        self._current_timer_purpose = 'shutter'
        self.shutter_delay_tick.emit(self._time_left)
        self._timer.start()  # 1초 주기 틱

    def _process_time(self):
        process_seconds = math.ceil(self.params.get('process_time', 0) * 60)
        if process_seconds <= 0:
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
            return
        self.status_message.emit("정보", f"메인 공정 {process_seconds}초 시작")
        self.command_requested.emit("set_polling", {'enable': True})
        self._time_left = process_seconds
        self._current_timer_purpose = 'process'
        self.process_time_tick.emit(self._time_left)
        self._timer.start()

    def _delay_seconds(self, seconds: int):
        if seconds <= 0:
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()
            return
        self._time_left = int(seconds)
        self._current_timer_purpose = 'delay'
        self._timer.start()

    def _on_timer_tick(self):
        if not self._is_running:
            self._timer.stop()
            if self._current_timer_purpose == 'process':
                self.command_requested.emit("set_polling", {'enable': False})
            return

        self._time_left -= 1
        if self._current_timer_purpose == 'shutter':
            self.shutter_delay_tick.emit(self._time_left)
        elif self._current_timer_purpose == 'process':
            self.process_time_tick.emit(self._time_left)

        if self._time_left <= 0:
            self._timer.stop()
            if self._current_timer_purpose == 'process':
                self.command_requested.emit("set_polling", {'enable': False})
            self._current_step_idx += 1
            self._step_in_progress = False
            self._run_next_step()

    # --- 종료/정리 ---
    @Slot()
    def teardown(self):
        if self._timer and self._timer.isActive():
            self._timer.stop()

    @Slot()
    def stop_process(self):
        """공정을 중단하고 모든 장비를 안전하게 종료."""
        if not self._is_running:
            return

        self.status_message.emit("정보", "종료 시퀀스를 실행합니다.")
        self._is_running = False
        self._timer.stop() if self._timer else None

        self.stage_monitor.emit("M.S. close...")
        self.update_faduino_port.emit('MS_button', False)

        # 1) 파워 끄기
        if self.is_dc_on and self.dc:
            self.status_message.emit("DCpower", "DC 파워를 끕니다.")
            self.stop_dc_power.emit()

        if self.is_rf_on and self.rf:
            self.status_message.emit("RFpower", "RF 파워를 끕니다 (Ramp-down).")
            loop = QEventLoop()
            self.rf.ramp_down_finished.connect(loop.quit)
            self.stop_rf_power.emit()
            loop.exec()
            self.rf.ramp_down_finished.disconnect(loop.quit)
            self.status_message.emit("정보", "RF 파워 OFF 완료.")

        # 2) MFC 플로우/밸브/폴링
        self.stage_monitor.emit("MFC flow, valve off...")
        if self.mfc:
            loop = QEventLoop()
            # ★ mfc 신호에 연결해야 함
            self.mfc.command_confirmed.connect(loop.quit)
            self.mfc.command_failed.connect(loop.quit)

            self.command_requested.emit("FLOW_OFF", {'channel': self.process_channel})
            loop.exec()

            self.command_requested.emit("VALVE_OPEN", {})
            loop.exec()

            self.command_requested.emit("set_polling", {'enable': False})

            self.mfc.command_confirmed.disconnect(loop.quit)
            self.mfc.command_failed.disconnect(loop.quit)

        # 3) Faduino 셔터/가스 닫기
        self.stage_monitor.emit("Gun shutter close...")
        self.update_faduino_port.emit('S1_button', False)
        self.update_faduino_port.emit('S2_button', False)
        QThread.msleep(1000)

        self.stage_monitor.emit("Gas valve close...")
        if self.gas_valve_button:
            gas_name = "Ar" if "Ar" in self.gas_valve_button else "O2"
            self.status_message.emit("FADUINO", f"{gas_name} Valve를 닫습니다.")
            self.update_faduino_port.emit(self.gas_valve_button, False)
        QThread.msleep(1000)

        self.status_message.emit("정보", "종료 시퀀스가 안전하게 완료되었습니다.")
        self.finished.emit()
