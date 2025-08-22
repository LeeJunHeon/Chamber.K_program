# === CHAMBER-K main.py (수정본) ===
import sys
from functools import partial
from PySide6.QtCore import QThread, Slot, Signal, QEventLoop
from PySide6.QtWidgets import QApplication, QDialog, QMessageBox

from UI import Ui_Dialog
from lib.config import BUTTON_TO_PORT_MAP
from lib.logger import set_monitor_widget, log_message_to_monitor
from controller.process_controller import SputterProcessController
from device.Faduino import FaduinoController
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

        # --- 컨트롤러 및 스레드 생성 (워커-오브젝트 패턴) ---
        # 1) Faduino
        self.faduino_thread = QThread(self)
        self.faduino_thread.setObjectName("FaduinoThread")
        self.faduino_controller = FaduinoController()
        self.faduino_controller.moveToThread(self.faduino_thread)

        # 2) MFC
        self.mfc_thread = QThread(self)
        self.mfc_thread.setObjectName("MFCThread")
        self.mfc_controller = MFCController()
        self.mfc_controller.moveToThread(self.mfc_thread)

        # 3) DC Power
        self.dcpower_thread = QThread(self)
        self.dcpower_thread.setObjectName("DCPowerThread")
        self.dcpower_controller = DCPowerController()
        self.dcpower_controller.moveToThread(self.dcpower_thread)

        # 4) RF Power (필요 시 Faduino와 상호작용)
        self.rfpower_thread = QThread(self)
        self.rfpower_thread.setObjectName("RFPowerThread")
        self.rfpower_controller = RFPowerController(faduino=self.faduino_controller)
        self.rfpower_controller.moveToThread(self.rfpower_thread)

        # 5) Process(감독자)
        self.process_thread = QThread(self)
        self.process_thread.setObjectName("ProcessThread")
        self.process_controller = SputterProcessController(
            mfc_controller=self.mfc_controller,
            dc_controller=self.dcpower_controller,
            rf_controller=self.rfpower_controller,
            faduino_controller=self.faduino_controller,
        )
        self.process_controller.moveToThread(self.process_thread)

        # 신호 배선
        self._connect_signals()

        # --- 모든 스레드 시작 ---
        self.faduino_thread.start()
        self.mfc_thread.start()
        self.dcpower_thread.start()
        self.rfpower_thread.start()
        self.process_thread.start()

        self.process_running = False
        self.ui.Sputter_Stop_Button.setEnabled(False)

    def _connect_signals(self):
        """스레드-로컬 타이머/정리, UI 이벤트, 장치 명령 라우팅, 상태 표시를 연결"""

        # 1) 프로그램 생명주기 / 스레드-로컬 타이머 보장
        #    Faduino: 반드시 그 스레드에서 타이머 생성 후 폴링 시작
        if hasattr(self.faduino_controller, "_setup_timers"):
            self.faduino_thread.started.connect(self.faduino_controller._setup_timers)

        #    Process 타이머도 자신 스레드에서 생성
        if hasattr(self.process_controller, "_setup_timers"):
            self.process_thread.started.connect(self.process_controller._setup_timers)

        if hasattr(self.mfc_controller, "_setup_timers"):
            self.mfc_thread.started.connect(self.mfc_controller._setup_timers)

        #    종료 시 각 컨트롤러 정리를 '자기 스레드 슬롯'에서 수행
        self.shutdown_requested.connect(self.faduino_controller.cleanup)
        self.shutdown_requested.connect(self.mfc_controller.cleanup)
        self.shutdown_requested.connect(self.dcpower_controller.close_connection)
        self.shutdown_requested.connect(self.rfpower_controller.close_connection)
        if hasattr(self.process_controller, "teardown"):
            self.shutdown_requested.connect(self.process_controller.teardown)

        # 2) UI 이벤트 → 컨트롤러
        self.ui.Sputter_Start_Button.clicked.connect(self._handle_start_process)
        self.ui.Sputter_Stop_Button.clicked.connect(self.request_process_stop)
        self.ui.ALL_STOP_button.clicked.connect(self._handle_all_stop)

        # Faduino 포트 토글(버튼 이름→포트 이름 매핑)
        for btn_name in BUTTON_TO_PORT_MAP.keys():
            btn = getattr(self.ui, btn_name, None)
            if btn:
                btn.toggled.connect(partial(self.faduino_controller.update_port_state, btn_name))
        # Door는 특수 토글
        self.ui.Door_Button.toggled.connect(
            partial(self.faduino_controller.update_port_state, 'Door_Button')
        )
        self.faduino_controller.rf_power_response.connect(self.rfpower_controller.on_rf_power_sample)
    
        # 3) 감독자(Process) ↔ 장치 라우팅
        self.process_controller.start_requested.connect(self.process_controller.start_process_flow)

        #    Process → Devices
        self.process_controller.update_faduino_port.connect(self.faduino_controller.update_port_state)
        self.process_controller.start_dc_power.connect(self.dcpower_controller.start_process)
        self.process_controller.stop_dc_power.connect(self.dcpower_controller.stop_process)
        self.process_controller.start_rf_power.connect(self.rfpower_controller.start_process)
        self.process_controller.stop_rf_power.connect(self.rfpower_controller.stop_process)

        #    (핵심) Process → MFC: 명령 단일 라우트
        if hasattr(self.process_controller, "command_requested"):
            self.process_controller.command_requested.connect(self.mfc_controller.handle_command)

        #    MFC → Process: 결과 보고
        self.mfc_controller.command_confirmed.connect(self.process_controller._on_mfc_confirmed)
        self.mfc_controller.command_failed.connect(self.process_controller._on_mfc_failed)

        #    Stop 버튼은 항상 "요청 신호"로 (다른 스레드 직접호출 금지)
        self.request_process_stop.connect(self.process_controller.stop_process)

        # 4) 장치/공정 → UI 표시
        self.process_controller.finished.connect(self._handle_process_finished)
        self.process_controller.connection_failed.connect(self._handle_connection_failure)
        self.process_controller.critical_error.connect(self._handle_critical_error)
        self.process_controller.stage_monitor.connect(self.update_stage_monitor)
        self.process_controller.shutter_delay_tick.connect(self.update_shutter_delay_timer)
        self.process_controller.process_time_tick.connect(self.update_process_time_timer)

        self.faduino_controller.update_button_display.connect(self.update_ui_button_display)
        self.faduino_controller.update_sensor_display.connect(self.set_indicator)

        self.mfc_controller.update_flow.connect(self.update_mfc_flow_display)
        self.mfc_controller.update_pressure.connect(self.update_mfc_pressure_display)

        self.dcpower_controller.update_dc_status_display.connect(self.update_dc_status_display)
        self.rfpower_controller.update_rf_status_display.connect(self.update_rf_status_display)

        # 5) 로그 일원화
        self.faduino_controller.status_message.connect(self.on_status_message)
        self.mfc_controller.status_message.connect(self.on_status_message)
        self.dcpower_controller.status_message.connect(self.on_status_message)
        self.rfpower_controller.status_message.connect(self.on_status_message)
        self.process_controller.status_message.connect(self.on_status_message)

    # ---------- UI 이벤트 핸들러 ----------
    @Slot()
    def _handle_start_process(self):
        if self.process_running:
            QMessageBox.warning(self, "경고", "이미 공정이 진행 중입니다.")
            return

        try:
            if self.ui.Ar_gas_radio.isChecked():
                selected_gas = "Ar"; flow_text = self.ui.Ar_flow_edit.toPlainText().strip()
            elif self.ui.O2_gas_radio.isChecked():
                selected_gas = "O2"; flow_text = self.ui.O2_flow_edit.toPlainText().strip()
            else:
                raise ValueError("Ar 또는 O2 가스를 선택해야 합니다.")

            offset_text = self.ui.offset_edit.toPlainText().strip()
            param_text  = self.ui.param_edit.toPlainText().strip()

            if self.ui.rf_power_checkbox.isChecked():
                if not offset_text:  raise ValueError("RF 파워의 Offset 값을 입력해야 합니다.")
                if not param_text:   raise ValueError("RF 파워의 Param 값을 입력해야 합니다.")

            params = {
                "selected_gas": selected_gas,
                "mfc_flow": float(flow_text),
                "sp1_set": float(self.ui.working_pressure_edit.toPlainText().strip()),
                "dc_power": float(self.ui.DC_power_edit.toPlainText().strip() or 0) if self.ui.dc_power_checkbox.isChecked() else 0,
                "rf_power": float(self.ui.RF_power_edit.toPlainText().strip() or 0) if self.ui.rf_power_checkbox.isChecked() else 0,
                "shutter_delay": float(self.ui.Shutter_delay_edit.toPlainText().strip()),
                "process_time": float(self.ui.process_time_edit.toPlainText().strip()),
                "rf_offset": float(offset_text or 0),
                "rf_param": float(param_text or 1.0),
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

        self.process_controller.start_requested.emit(params)
        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)

    @Slot()
    def _handle_all_stop(self):
        # 1) 공정 중지 요청(프로세스 스레드에서 안전하게 처리)
        self.request_process_stop.emit()
        # 2) 장비 즉시 정지(Faduino 비상정지)
        self.faduino_controller.on_emergency_stop()

    # ---------- 상태/표시 ----------
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
            frame.setStyleSheet(
                f"background: {color}; border-radius: 25px; border: 2px solid #333;"
            )

    @Slot(str, bool)
    def update_ui_button_display(self, button_name, state):
        btn = getattr(self.ui, button_name, None)
        if btn:
            btn.blockSignals(True)
            btn.setChecked(state)
            btn.blockSignals(False)

    def _handle_process_finished(self):
        self.on_status_message("정보", "프로세스 종료중...")
        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        # 기본값 복원
        self.ui.Shutter_delay_edit.setPlainText("5")
        self.ui.process_time_edit.setPlainText("10")

    @Slot(str)
    def _handle_critical_error(self, error_message):
        QMessageBox.critical(self, "치명적 오류",
                             f"장비와의 통신에 문제가 발생하여 공정을 중단했습니다.\n\n사유: {error_message}")
        self._handle_process_finished()

    # ---------- 타이머 표시 ----------
    def update_stage_monitor(self, stage_text):
        self.ui.stage_monitor.setPlainText(stage_text)

    def update_shutter_delay_timer(self, seconds_left):
        m, s = divmod(int(seconds_left), 60)
        self.ui.Shutter_delay_edit.setPlainText(f"{m:02d}:{s:02d}")

    def update_process_time_timer(self, seconds_left):
        m, s = divmod(int(seconds_left), 60)
        self.ui.process_time_edit.setPlainText(f"{m:02d}:{s:02d}")

    # ---------- MFC 표시(문자열 1개 Signal 보호 파싱) ----------
    def update_mfc_flow_display(self, flow: str):
        # flow 예: "Ar: 1.00" | "O2: 0.5" | "1.00"
        flow = (flow or "").strip()
        gas = None
        value_str = None

        if ":" in flow:
            parts = flow.split(":", 1)
            gas = (parts[0] or "").strip().upper()
            value_str = (parts[1] or "").strip()
        else:
            gas = "AR" if self.ui.Ar_gas_radio.isChecked() else ("O2" if self.ui.O2_gas_radio.isChecked() else "AR")
            value_str = flow

        target_edit = self.ui.Ar_flow_edit if gas == "AR" else self.ui.O2_flow_edit
        try:
            target_edit.setPlainText(f"{float(value_str):.2f}")
        except Exception:
            target_edit.setPlainText("Parsing Error")

    def update_mfc_pressure_display(self, pressure):
        s = "" if pressure is None else str(pressure).strip()
        self.ui.working_pressure_edit.setPlainText("ERROR" if not s else s)

    # ---------- Power 표시 ----------
    def update_dc_status_display(self, power, voltage, current):
        self.ui.Power_edit.setPlainText("ERROR" if power is None else f"{power:.2f}")
        self.ui.Voltage_edit.setPlainText("ERROR" if voltage is None else f"{voltage:.2f}")
        self.ui.Current_edit.setPlainText("ERROR" if current is None else f"{current:.2f}")

    def update_rf_status_display(self, for_power, ref_power):
        self.ui.for_p_edit.setPlainText("ERROR" if for_power is None else f"{for_power:.2f}")
        self.ui.ref_p_edit.setPlainText("ERROR" if ref_power is None else f"{ref_power:.2f}")

    # ---------- 종료 ----------
    def closeEvent(self, event):
        reply = QMessageBox.question(
            self, '종료 확인', '정말로 프로그램을 종료하시겠습니까?',
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            event.ignore()
            return

        log_message_to_monitor("정보", "프로그램 종료를 시작합니다...")

        # 1) 공정 중이면 먼저 중지 요청 → finished 대기
        if self.process_running:
            loop = QEventLoop()
            self.process_controller.finished.connect(loop.quit)
            self.request_process_stop.emit()   # 반드시 신호로
            loop.exec()
            self.process_controller.finished.disconnect(loop.quit)
            log_message_to_monitor("정보", "진행 중인 공정이 중지되었습니다.")

        # 2) 각 컨트롤러 정리(자기 스레드 슬롯에서 실행)
        self.shutdown_requested.emit()

        # 3) 스레드 종료
        threads = [self.process_thread, self.faduino_thread, self.mfc_thread,
                   self.dcpower_thread, self.rfpower_thread]
        for th in threads:
            th.quit()
        for th in threads:
            name = th.objectName()
            log_message_to_monitor("정보", f"{name} 스레드 종료 대기 중...")
            if not th.wait(3000):
                log_message_to_monitor("경고", f"{name} 스레드가 시간 내에 종료되지 않았습니다.")

        log_message_to_monitor("정보", "모든 스레드가 종료되었습니다. 프로그램을 닫습니다.")
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    dlg = MainDialog()
    dlg.show()
    sys.exit(app.exec())
