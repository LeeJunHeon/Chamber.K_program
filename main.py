import sys
from functools import partial
from PyQt6.QtCore import QThread, pyqtSlot as Slot, pyqtSignal as Signal, QEventLoop
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox

from UI import Ui_Dialog
from lib.config import PLC_COIL_MAP
from lib.logger import set_monitor_widget, log_message_to_monitor, set_process_log_file
from controller.process_controller import SputterProcessController
from device.PLC import PLCController
from device.MFC import MFCController
from device.DCpower import DCPowerController
from device.RFpower import RFPowerController

class MainDialog(QDialog):
    shutdown_requested = Signal()
    request_process_stop = Signal()

    """메인 UI 및 전체 공정/장치 연결 클래스"""
    def __init__(self):
        super().__init__()
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)
        set_monitor_widget(self.ui.error_monitor)

        # --- [최종] 모든 컨트롤러를 Worker-Object 패턴으로 생성 ---

        # 1. PLC 컨트롤러 설정
        self.plc_thread = QThread()
        self.plc_thread.setObjectName("PLCThread")
        self.plc_controller = PLCController()
        self.plc_controller.moveToThread(self.plc_thread)

        # 2. MFC 컨트롤러 설정
        self.mfc_thread = QThread()
        self.mfc_thread.setObjectName("MFCThread")
        self.mfc_controller = MFCController()
        self.mfc_controller.moveToThread(self.mfc_thread)
        # MFC는 Start 버튼 클릭 시 process_controller에서 연결을 시도함

        # 3. DC Power 컨트롤러 설정
        self.dcpower_thread = QThread()
        self.dcpower_thread.setObjectName("DCPowerThread")
        self.dcpower_controller = DCPowerController()
        self.dcpower_controller.moveToThread(self.dcpower_thread)
        # DC Power도 Start 버튼 클릭 시 process_controller에서 연결을 시도함

        # 4. RF Power 컨트롤러 설정
        self.rfpower_thread = QThread()
        self.rfpower_thread.setObjectName("RFPowerThread")
        self.rfpower_controller = RFPowerController(plc=self.plc_controller)
        self.rfpower_controller.moveToThread(self.rfpower_thread)
        # [추가] PLC가 RF 컨트롤러의 현재 PWM 값을 알 수 있도록 참조를 전달
        self.plc_controller.set_rf_controller(self.rfpower_controller) 
        
        # 5. Process 컨트롤러 설정
        self.process_thread = QThread()
        self.process_thread.setObjectName("ProcessThread")
        self.process_controller = SputterProcessController(
            mfc_controller=self.mfc_controller, 
            dc_controller=self.dcpower_controller, 
            rf_controller=self.rfpower_controller,
            plc_controller=self.plc_controller
        )
        self.process_controller.moveToThread(self.process_thread)

        self._connect_signals()

        # --- 모든 스레드 시작 ---
        self.plc_thread.start()
        self.mfc_thread.start()
        self.dcpower_thread.start()
        self.rfpower_thread.start()
        self.process_thread.start()

        self.process_running = False
        self.ui.Sputter_Stop_Button.setEnabled(False)

    def _connect_signals(self):
        """[최종 수정] 모든 시그널-슬롯 연결을 논리적으로 정리하고 중복을 제거합니다."""
        
        # --- 1. 프로그램 및 스레드 생명주기 관련 연결 ---
        self.shutdown_requested.connect(self.plc_controller.cleanup)
        self.shutdown_requested.connect(self.mfc_controller.cleanup)
        self.shutdown_requested.connect(self.dcpower_controller.close_connection)
        self.shutdown_requested.connect(self.rfpower_controller.close_connection)
        
        self.plc_thread.started.connect(self.plc_controller.start_polling)
        #self.mfc_thread.started.connect(self.mfc_controller.start_polling)

        # --- 2. UI 이벤트 -> 컨트롤러 동작 연결 ---
        self.ui.Sputter_Start_Button.clicked.connect(self._handle_start_process)
        #self.ui.Sputter_Stop_Button.clicked.connect(self._handle_sputter_stop)
        self.ui.ALL_STOP_button.clicked.connect(self.plc_controller.on_emergency_stop)
        # UI에서 'Sputter 중지' 버튼을 누르면, 새로 만든 신호를 통해 비동기적으로 중단 요청
        self.ui.Sputter_Stop_Button.clicked.connect(self.request_process_stop)

        # PLC 버튼 연결
        for btn_name in PLC_COIL_MAP.keys():
            button = getattr(self.ui, btn_name, None)
            if button:
                button.toggled.connect(partial(self.plc_controller.update_port_state, btn_name))
        self.ui.Door_Button.toggled.connect(self._on_ui_door_toggled)

        # --- 3. 컨트롤러 간 상호작용 연결 ---
        # ProcessController가 시작 신호를 스스로 받도록 연결
        self.process_controller.start_requested.connect(self.process_controller.start_process_flow)

        # ProcessController -> 각 장치 컨트롤러로 명령 전달
        self.process_controller.update_plc_port.connect(self.plc_controller.update_port_state)
        self.process_controller.start_dc_power.connect(self.dcpower_controller.start_process)
        self.process_controller.stop_dc_power.connect(self.dcpower_controller.stop_process)
        self.process_controller.start_rf_power.connect(self.rfpower_controller.start_process)
        self.process_controller.stop_rf_power.connect(self.rfpower_controller.stop_process)
        
        # ProcessController -> MFC (명령 라우팅)
        self.process_controller.command_requested.connect(self.mfc_controller.handle_command)
        # MFC -> Process (결과 보고)
        self.mfc_controller.command_confirmed.connect(self.process_controller._on_mfc_confirmed)
        self.mfc_controller.command_failed.connect(
            lambda cmd, why: self.process_controller._on_mfc_failed(f"{cmd}: {why}")
        )
        
        # 새로 만든 신호를 Process Controller의 stop_process 슬롯에 연결
        # 이렇게 하면 stop_process는 Process 스레드에서 안전하게 실행됩니다.
        self.request_process_stop.connect(self.process_controller.stop_process) # <<< ▼ 이 줄을 추가하세요 ▼

        # --- 4. 컨트롤러 -> UI 상태 업데이트 연결 ---
        self.process_controller.finished.connect(self._handle_process_finished)
        self.process_controller.connection_failed.connect(self._handle_connection_failure)
        self.process_controller.critical_error.connect(self._handle_critical_error)
        self.process_controller.stage_monitor.connect(self.update_stage_monitor)
        self.process_controller.shutter_delay_tick.connect(self.update_shutter_delay_timer)
        self.process_controller.process_time_tick.connect(self.update_process_time_timer)
        
        self.plc_controller.update_button_display.connect(self.update_ui_button_display)
        self.plc_controller.update_sensor_display.connect(self.set_indicator)
        self.mfc_controller.update_flow.connect(self.update_mfc_flow_display)
        self.mfc_controller.update_pressure.connect(self.update_mfc_pressure_display)
        self.dcpower_controller.update_dc_status_display.connect(self.update_dc_status_display)
        self.rfpower_controller.update_rf_status_display.connect(self.update_rf_status_display)

        # --- 5. 모든 로그 메시지를 UI 모니터에 연결 ---
        self.plc_controller.status_message.connect(self.on_status_message)
        self.mfc_controller.status_message.connect(self.on_status_message)
        self.dcpower_controller.status_message.connect(self.on_status_message)
        self.rfpower_controller.status_message.connect(self.on_status_message)
        self.process_controller.status_message.connect(self.on_status_message)

    @Slot()
    def _handle_start_process(self):
        if self.process_running:
            QMessageBox.warning(self, "경고", "이미 공정이 진행 중입니다.")
            return
    
        try:
            # --- 가스 선택: Ar / O2 를 각각 체크박스로 처리 ---
            use_ar = self.ui.Ar_gas_radio.isChecked()
            use_o2 = self.ui.O2_gas_radio.isChecked()

            if not (use_ar or use_o2):
                raise ValueError("Ar 또는 O2 가스를 하나 이상 선택해야 합니다.")

            ar_flow = 0.0
            o2_flow = 0.0

            if use_ar:
                ar_text = self.ui.Ar_flow_edit.toPlainText().strip()
                if not ar_text:
                    raise ValueError("Ar 가스 유량을 입력해야 합니다.")
                ar_flow = float(ar_text)

            if use_o2:
                o2_text = self.ui.O2_flow_edit.toPlainText().strip()
                if not o2_text:
                    raise ValueError("O2 가스 유량을 입력해야 합니다.")
                o2_flow = float(o2_text)

            # 기존 RF offset/param 체크 로직 그대로 유지
            offset_text = self.ui.offset_edit.toPlainText().strip()
            param_text = self.ui.param_edit.toPlainText().strip()

            if self.ui.rf_power_checkbox.isChecked():
                if not offset_text:
                    raise ValueError("RF 파워의 Offset 값을 입력해야 합니다.")
                if not param_text:
                    raise ValueError("RF 파워의 Param 값을 입력해야 합니다.")

            # --- selected_gas / mfc_flow는 기존 코드 호환용으로 유지 ---
            if use_ar and not use_o2:
                selected_gas = "Ar"
                mfc_flow = ar_flow
            elif use_o2 and not use_ar:
                selected_gas = "O2"
                mfc_flow = o2_flow
            else:
                # 둘 다 쓰는 경우: 기본은 Ar 기준
                selected_gas = "Ar"
                mfc_flow = ar_flow

            params = {
                # ▼ 새 다중 가스 파라미터
                "use_ar_gas": use_ar,
                "use_o2_gas": use_o2,
                "ar_flow": ar_flow,
                "o2_flow": o2_flow,

                # ▼ 기존 단일 가스 방식(백워드 호환용)
                "selected_gas": selected_gas,
                "mfc_flow": float(mfc_flow),

                # ▼ 나머지 기존 파라미터들 그대로 유지
                "sp1_set": float(self.ui.working_pressure_edit.toPlainText().strip()),
                "dc_power": float(self.ui.DC_power_edit.toPlainText().strip() or 0) if self.ui.dc_power_checkbox.isChecked() else 0,
                "rf_power": float(self.ui.RF_power_edit.toPlainText().strip() or 0) if self.ui.rf_power_checkbox.isChecked() else 0,
                "shutter_delay": float(self.ui.Shutter_delay_edit.toPlainText().strip()),
                "process_time": float(self.ui.process_time_edit.toPlainText().strip()),
                "rf_offset": float(offset_text or 6.79),
                "rf_param": float(param_text or 1.0395),
                "use_g1": self.ui.G1_checkbox.isChecked(),
                "use_g2": self.ui.G2_checkbox.isChecked(),
            }
            params['main_shutter'] = (params['process_time'] > 0)

            if not (params['dc_power'] > 0 or params['rf_power'] > 0):
                raise ValueError("RF 또는 DC 파워 중 하나 이상을 입력해야 합니다.")
            if params['shutter_delay'] <= 0:
                raise ValueError("Shutter Delay는 0보다 커야 합니다.")

        except (ValueError, TypeError) as e:
            QMessageBox.warning(self, "입력 오류", f"공정 파라미터가 잘못되었습니다:\n{e}")
            return
        
        # ★★★ 여기서 이번 공정용 로그 파일을 NAS에 생성 (CHK_YYYYmmdd_HHMMSS.txt) ★★★
        set_process_log_file(prefix="CHK")
        log_message_to_monitor("정보", "=== CHK 공정 시작 ===")

        self.process_controller.start_requested.emit(params)
        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)

    @Slot(str)
    def _handle_connection_failure(self, error_message):
        QMessageBox.critical(self, "연결 실패", error_message)
        self._handle_process_finished()

    def on_status_message(self, level, message):
        log_message_to_monitor(level, message)
        if level == "재시작":
            log_message_to_monitor("재시작", "모든 공정을 중지합니다. 다시 시작하십시오.")
            if self.process_controller and self.process_running:
                self.request_process_stop.emit()   

    @Slot(str, bool)
    def set_indicator(self, name, state: bool):
        frame_name = f"{name}_Indicator"
        frame = getattr(self.ui, frame_name, None)
        if frame is not None:
            color = "#38d62f" if state else "#d6252f"
            frame.setStyleSheet(f"background: {color}; border-radius: 25px; border: 2px solid #333;")
        else:
            print(f"[set_indicator] '{frame_name}' 인디케이터가 UI에 없습니다.")

    @Slot(str, bool)
    def update_ui_button_display(self, button_name, state):
        # Doorup/Doordn은 UI의 Door_Button으로 합쳐서 표시
        if button_name in ("Doorup_button", "Doordn_button"):
            door_btn = getattr(self.ui, "Door_Button", None)
            if door_btn and state:  # True일 때만 반영(불필요한 토글 방지)
                door_btn.blockSignals(True)
                door_btn.setChecked(True if button_name == "Doorup_button" else False)
                door_btn.blockSignals(False)
            return
    
        button = getattr(self.ui, button_name, None)
        if button:
            button.blockSignals(True)
            button.setChecked(state)
            button.blockSignals(False)
        else:
            print(f"[update_ui_button_display] '{button_name}' 버튼이 UI에 없습니다.")

    # def _handle_sputter_stop(self):
    #     self.on_status_message("경고", "STOP 버튼 클릭됨")
    #     if self.process_controller and self.process_running:
    #         self.process_controller.stop_process()
        
    def _handle_process_finished(self):
        self.on_status_message("정보", "프로세스 종료중...")
        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        
        # ▼ 추가: 파워/리플렉트 표시값 초기화
        self.ui.Power_edit.setPlainText("0.0")
        self.ui.Voltage_edit.setPlainText("0.0")
        self.ui.Current_edit.setPlainText("0.0")
        self.ui.for_p_edit.setPlainText("0.0")
        self.ui.ref_p_edit.setPlainText("0.0")

        # ▼ 추가: 가스 유량 표시값 초기화
        self.ui.Ar_flow_edit.setPlainText("0.0")
        self.ui.O2_flow_edit.setPlainText("0.0")

        # [추가] 공정 종료 시 UI의 타이머 값을 기본값 "0"으로 초기화
        self.ui.Shutter_delay_edit.setPlainText("5")
        self.ui.process_time_edit.setPlainText("10")

    @Slot(str)
    def _handle_critical_error(self, error_message):
        QMessageBox.critical(self, "치명적 오류", f"장비와의 통신에 문제가 발생하여 공정을 중단했습니다.\n\n사유: {error_message}")
        self._handle_process_finished()

    def update_stage_monitor(self, stage_text):
        self.ui.stage_monitor.setPlainText(stage_text)

    def update_shutter_delay_timer(self, seconds_left):
        m, s = divmod(seconds_left, 60)
        text = f"{m:02d}:{s:02d}"
        self.ui.Shutter_delay_edit.setPlainText(text)

    def update_process_time_timer(self, seconds_left):
        m, s = divmod(seconds_left, 60)
        text = f"{m:02d}:{s:02d}"
        self.ui.process_time_edit.setPlainText(text)

    @Slot(str, float)
    def update_mfc_flow_display(self, gas, value):
        edit = self.ui.Ar_flow_edit if gas == "Ar" else self.ui.O2_flow_edit
        edit.setPlainText(f"{value:.2f}")

    def update_mfc_pressure_display(self, pressure):
        self.ui.working_pressure_edit.setPlainText("ERROR" if pressure is None else str(pressure))

    def update_dc_status_display(self, power, voltage, current):
        self.ui.Power_edit.setPlainText("ERROR" if power is None else f"{power:.3f}")
        self.ui.Voltage_edit.setPlainText("ERROR" if voltage is None else f"{voltage:.3f}")
        self.ui.Current_edit.setPlainText("ERROR" if current is None else f"{current:.3f}")

    def update_rf_status_display(self, for_power, ref_power):
        self.ui.for_p_edit.setPlainText("ERROR" if for_power is None else f"{for_power:.2f}")
        self.ui.ref_p_edit.setPlainText("ERROR" if ref_power is None else f"{ref_power:.2f}")

    @Slot(bool)
    def _on_ui_door_toggled(self, checked: bool):
        """
        UI의 Door_Button 한 개 토글을 PLC의 Up/Down 두 코일로 분리 전달.
        - True  -> Doorup_button (문 열기)
        - False -> Doordn_button (문 닫기)
        """
        if checked:
            self.plc_controller.update_port_state('Doorup_button', True)
            # 필요 시 반대 코일을 내려주고 싶으면 아래 줄 주석 해제
            # self.plc_controller.update_port_state('Doordn_button', False)
        else:
            self.plc_controller.update_port_state('Doordn_button', True)
            # self.plc_controller.update_port_state('Doorup_button', False)

    def closeEvent(self, event):
        """[수정됨] 안전한 스레드 종료 로직"""
        reply = QMessageBox.question(
            self, '종료 확인', '정말로 프로그램을 종료하시겠습니까?', 
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, 
            QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            log_message_to_monitor("정보", "프로그램 종료를 시작합니다...")

            # 1. 진행 중인 공정이 있다면 먼저 중지 요청
            if self.process_running:
                # process_controller의 stop_process가 동기적으로 완료될 때까지 대기
                loop = QEventLoop()
                self.process_controller.finished.connect(loop.quit)
                self.process_controller.stop_process()
                loop.exec() # stop_process가 끝나고 finished 신호를 보낼 때까지 대기
                self.process_controller.finished.disconnect(loop.quit)
                log_message_to_monitor("정보", "진행 중인 공정이 중지되었습니다.")

            # 2. 모든 백그라운드 스레드에 리소스 정리 및 종료 요청 (교착 상태 방지)
            self.shutdown_requested.emit()

            # 3. 모든 스레드의 이벤트 루프에 공식적인 종료 신호 전송
            threads = [self.process_thread, self.plc_thread, self.mfc_thread, self.dcpower_thread, self.rfpower_thread]
            for thread in threads:
                thread.quit()

            # 4. 모든 스레드가 실제로 종료될 때까지 대기
            for thread in threads:
                thread_name = thread.objectName()
                log_message_to_monitor("정보", f"{thread_name} 스레드 종료 대기 중...")
                if not thread.wait(3000): # 3초 타임아웃
                     log_message_to_monitor("경고", f"{thread_name} 스레드가 시간 내에 종료되지 않았습니다.")

            log_message_to_monitor("정보", "모든 스레드가 종료되었습니다. 프로그램을 닫습니다.")
            event.accept()
        else:
            event.ignore()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    dlg = MainDialog()
    dlg.show()
    sys.exit(app.exec())