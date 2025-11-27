import sys
from functools import partial
from PyQt6.QtCore import QThread, pyqtSlot as Slot, pyqtSignal as Signal, QEventLoop
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox, QFileDialog
from pathlib import Path
import csv
import datetime

from UI import Ui_Dialog
from lib.config import PLC_COIL_MAP
from lib.logger import set_monitor_widget, log_message_to_monitor, set_process_log_file, append_chk_csv_row
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

        # ★ 이번 공정 이름(단일/CSV 공정 공통)
        self.current_process_name: str = ""

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

        # --- CSV 기반 Process List 상태 ---
        self.csv_file_path: str | None = None         # 선택한 CSV 파일 전체 경로
        self.csv_rows: list[dict] = []                # CSV 한 줄 = dict
        self.csv_index: int = -1                      # 현재 실행 중인 줄 index
        self.csv_mode: bool = False                  # True면 '리스트 공정 모드'

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
        
        self.ui.select_csv_button.clicked.connect(self._on_select_csv_clicked)

    @Slot()
    def _handle_start_process(self):
        if self.process_running:
            QMessageBox.warning(self, "경고", "이미 공정이 진행 중입니다.")
            return
        
        # === 1) CSV 모드인지 먼저 확인 ===
        if self.csv_file_path:
            # CSV 로딩 & 리스트 공정 모드 진입
            if not self._load_csv_process_list():
                return  # 로딩 실패
            self.csv_mode = True
            self._start_next_csv_step()
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
        
        # ★ 단일 공정(수동 Start)일 때의 공정 이름
        self.current_process_name = "Single CHK"
        
        # ★★★ 여기서 이번 공정용 로그 파일을 NAS에 생성 (CHK_YYYYmmdd_HHMMSS.txt) ★★★
        set_process_log_file(prefix="CHK")
        log_message_to_monitor("정보", "=== CHK 공정 시작 ===")

        self.process_controller.start_requested.emit(params)
        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)

    @Slot()
    def _on_select_csv_clicked(self):
        """Process List용 CSV 파일 선택."""
        path, _ = QFileDialog.getOpenFileName(
            self,
            "공정 리스트 CSV 선택",
            "",
            "CSV Files (*.csv);;All Files (*)"
        )
        if not path:
            return

        # 실제 존재하는지 한번 더 확인(선택 취소 대비)
        p = Path(path)
        if not p.exists():
            QMessageBox.warning(self, "파일 오류", "선택한 CSV 파일을 찾을 수 없습니다.")
            return

        self.csv_file_path = str(p)
        # 라벨에 파일명 표시
        #self.ui.process_list_label.setText(f"Process List: {p.name}")
        log_message_to_monitor("정보", f"CSV 공정 리스트 파일 선택: {p}")

    def _load_csv_process_list(self) -> bool:
        """
        self.csv_file_path 에 지정된 CSV를 읽어서
        self.csv_rows 에 List[dict] 형태로 저장.
        성공하면 True, 실패하면 False.
        """
        if not self.csv_file_path:
            QMessageBox.warning(self, "CSV 없음", "먼저 CSV 파일을 선택해 주세요.")
            return False

        try:
            with open(self.csv_file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = [row for row in reader if any(v.strip() for v in row.values())]
        except Exception as ex:
            QMessageBox.critical(self, "CSV 읽기 오류", f"CSV 파일을 읽는 중 오류가 발생했습니다.\n\n{ex}")
            return False

        if not rows:
            QMessageBox.warning(self, "CSV 비어있음", "CSV 파일에 유효한 공정 행이 없습니다.")
            return False

        self.csv_rows = rows
        self.csv_index = -1
        return True

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

    # ==================== ChK CSV 로그용 헬퍼 ====================
    def _build_chk_csv_row(self) -> dict:
        """
        현재 UI에 표시되어 있는 값들을 이용해 ChK_log.csv에 기록할 row 생성.
        - 평균값은 아직 계산 안 하고, 공정 종료 시점 값 그대로 사용.
        """
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        process_name = self.current_process_name or ""

        # 작은 헬퍼: QPlainTextEdit에서 텍스트 안전하게 꺼내기
        def _get_text(widget_name: str) -> str:
            w = getattr(self.ui, widget_name, None)
            if w is None:
                return ""
            try:
                return w.toPlainText().strip()
            except Exception:
                return ""

        shutter_delay = _get_text("Shutter_delay_edit")
        process_time  = _get_text("process_time_edit")

        # Main Shutter: process_time > 0 이면 ON 으로 간주
        try:
            pt_val = float(process_time or 0)
        except Exception:
            pt_val = 0.0
        main_shutter = "ON" if pt_val > 0 else ""

        # 타겟 사용 여부 (체크박스)
        try:
            g1_on = self.ui.G1_checkbox.isChecked()
        except Exception:
            g1_on = False
        try:
            g2_on = self.ui.G2_checkbox.isChecked()
        except Exception:
            g2_on = False

        g1_target = "ON" if g1_on else ""
        g2_target = "ON" if g2_on else ""

        # 가스/압력/파워 값들
        ar_flow   = _get_text("Ar_flow_edit")
        o2_flow   = _get_text("O2_flow_edit")
        work_p    = _get_text("working_pressure_edit")

        rf_for_p  = _get_text("for_p_edit")
        rf_ref_p  = _get_text("ref_p_edit")

        dc_p      = _get_text("Power_edit")
        dc_v      = _get_text("Voltage_edit")
        dc_i      = _get_text("Current_edit")

        row = {
            "Timestamp":      now,
            "Process Name":   process_name,
            "Main Shutter":   main_shutter,
            "Shutter Delay":  shutter_delay,
            "G1 Target":      g1_target,
            "G2 Target":      g2_target,
            "Ar flow":        ar_flow,
            "O2 flow":        o2_flow,
            "Working Pressure": work_p,
            "Process Time":   process_time,
            "RF: For.P":      rf_for_p,
            "RF: Ref. P":     rf_ref_p,
            "DC: V":          dc_v,
            "DC: I":          dc_i,
            "DC: P":          dc_p,
        }
        return row
        
    def _handle_process_finished(self):
        self.on_status_message("정보", "프로세스 종료중...")

        # ★ 공정이 끝날 때마다 ChK_log.csv 에 한 줄 추가
        try:
            row = self._build_chk_csv_row()
            ok = append_chk_csv_row(row)
            if ok:
                log_message_to_monitor("정보", "ChK CSV 로그 저장 완료")
            else:
                log_message_to_monitor("경고", "ChK CSV 로그 저장 실패")
        except Exception as e:
            log_message_to_monitor("경고", f"ChK CSV 로그 처리 중 예외 발생: {e!r}")

        # 1) CSV 리스트 공정 모드인 경우 → 다음 행 실행
        if self.csv_mode and self.csv_rows:
            # 현재 단계가 실패로 끝났는지 여부에 따라
            # 전체 중단할지, 다음 단계로 갈지 분기하고 싶다면
            # 여기서 추가 로직을 넣으면 됩니다.
            self.process_running = False
            self._start_next_csv_step()
            return

        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        self.update_stage_monitor("공정 종료")
        
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

    # ============= csv 공정 =============
    def _build_params_from_csv_row(self, row: dict) -> dict:
        """
        sputter_process_recipe.csv 한 줄(row)을 기반으로
        process_controller 에 전달할 params dict 생성.

        CSV 헤더(예시):
        #,Process_name,main_shutter,base_pressure,working_pressure,
        process_time,shutter_delay,Ar,O2,Ar_flow,O2_flow,
        use_dc_power,dc_power,use_rf_power,rf_power,
        gun1,gun2,G1 Target,G2 Target
        """
        # 1) UI 현재 상태를 기본값으로 먼저 읽음 (컬럼이 비어있을 때 폴백용)
        use_ar_gas = self.ui.Ar_gas_radio.isChecked()
        use_o2_gas = self.ui.O2_gas_radio.isChecked()

        def _float_from(text, default=0.0):
            try:
                return float(str(text).strip())
            except Exception:
                return default

        def _bool_from(v, default=False):
            if v is None:
                return default
            s = str(v).strip().lower()
            if s == "":
                return default
            # 1 / y / yes / true / on 등을 모두 True로 처리
            return s in ("1", "y", "yes", "true", "t", "on")

        # UI 기본값 (레시피에서 비워두면 이 값 사용)
        ar_flow_ui   = _float_from(self.ui.Ar_flow_edit.toPlainText(), 0.0)
        o2_flow_ui   = _float_from(self.ui.O2_flow_edit.toPlainText(), 0.0)
        work_p_ui    = _float_from(self.ui.working_pressure_edit.toPlainText(), 0.0)
        rf_power_ui  = _float_from(self.ui.RF_power_edit.toPlainText(), 0.0)
        dc_power_ui  = _float_from(self.ui.DC_power_edit.toPlainText(), 0.0)
        sh_delay_ui  = _float_from(self.ui.Shutter_delay_edit.toPlainText(), 0.0)
        proc_time_ui = _float_from(self.ui.process_time_edit.toPlainText(), 0.0)
        use_g1_ui    = self.ui.G1_checkbox.isChecked()
        use_g2_ui    = self.ui.G2_checkbox.isChecked()

        # 2) CSV 컬럼으로 덮어쓰기
        # 가스 선택
        use_ar_gas = _bool_from(row.get("Ar"), use_ar_gas)
        use_o2_gas = _bool_from(row.get("O2"), use_o2_gas)

        # 유량
        ar_flow = _float_from(row.get("Ar_flow", ar_flow_ui), ar_flow_ui)
        o2_flow = _float_from(row.get("O2_flow", o2_flow_ui), o2_flow_ui)

        # 압력 / 시간
        work_p    = _float_from(row.get("working_pressure", work_p_ui), work_p_ui)
        proc_time = _float_from(row.get("process_time",    proc_time_ui), proc_time_ui)
        sh_delay  = _float_from(row.get("shutter_delay",   sh_delay_ui),  sh_delay_ui)

        # 파워 (use_*가 0/false 이면 강제로 0W 처리)
        use_dc_csv = row.get("use_dc_power")
        use_rf_csv = row.get("use_rf_power")

        use_dc = _bool_from(use_dc_csv, None)
        use_rf = _bool_from(use_rf_csv, None)

        dc_power_val = _float_from(row.get("dc_power", dc_power_ui), dc_power_ui)
        rf_power_val = _float_from(row.get("rf_power", rf_power_ui), rf_power_ui)

        if use_dc is False:
            dc_power = 0.0
        else:
            dc_power = dc_power_val

        if use_rf is False:
            rf_power = 0.0
        else:
            rf_power = rf_power_val

        # Gun 사용 여부 (G1/G2 → gun1/gun2)
        use_g1 = _bool_from(row.get("gun1"), use_g1_ui)
        use_g2 = _bool_from(row.get("gun2"), use_g2_ui)

        process_name = (row.get("Process_name") or "").strip()

        # 메인 셔터: CSV에 값이 없으면 "process_time > 0" 기준
        main_shutter = _bool_from(row.get("main_shutter"), (proc_time > 0))

        # 3) selected_gas / mfc_flow 생성 (기존 코드와 동일한 방식)
        if use_ar_gas and not use_o2_gas:
            selected_gas = "Ar"
            mfc_flow = ar_flow
        elif use_o2_gas and not use_ar_gas:
            selected_gas = "O2"
            mfc_flow = o2_flow
        else:
            # 둘 다 쓰면 Ar 기준
            selected_gas = "Ar"
            mfc_flow = ar_flow

        # RF 보정값은 CSV에 없으므로 UI 값을 그대로 사용
        offset_text = self.ui.offset_edit.toPlainText().strip()
        param_text  = self.ui.param_edit.toPlainText().strip()
        rf_offset = _float_from(offset_text or 6.79, 6.79)
        rf_param  = _float_from(param_text  or 1.0395, 1.0395)

        # 4) 최종 params (단일 공정 모드와 최대한 동일한 구조)
        params = {
            "use_ar_gas": use_ar_gas,
            "use_o2_gas": use_o2_gas,
            "ar_flow": ar_flow,
            "o2_flow": o2_flow,

            "selected_gas": selected_gas,
            "mfc_flow": mfc_flow,

            # ★ 중요: process_controller 는 sp1_set 키를 사용함
            "sp1_set": work_p,

            "dc_power": dc_power,
            "rf_power": rf_power,
            "shutter_delay": sh_delay,
            "process_time": proc_time,

            "rf_offset": rf_offset,
            "rf_param": rf_param,

            "use_g1": use_g1,
            "use_g2": use_g2,

            "main_shutter": main_shutter,
            "process_name": process_name,
        }

        return params
    
    def _start_next_csv_step(self):
        """csv_rows[csv_index+1] 공정을 하나 실행하거나, 모두 끝났으면 CSV 모드 종료."""
        self.csv_index += 1

        # 모든 행을 다 돌았으면 종료
        if self.csv_index >= len(self.csv_rows):
            QMessageBox.information(self, "CSV 공정 완료", "CSV에 있는 모든 공정을 완료했습니다.")
            self.csv_mode = False
            self.csv_rows = []
            self.csv_index = -1
            self.process_running = False
            self.ui.Sputter_Start_Button.setEnabled(True)
            self.ui.Sputter_Stop_Button.setEnabled(False)
            return

        row = self.csv_rows[self.csv_index]
        params = self._build_params_from_csv_row(row)

        # ★★★ 이 CSV STEP 전용 로그 파일 생성 ★★★
        set_process_log_file(prefix="CHK")
        step_no = self.csv_index + 1
        total   = len(self.csv_rows)
        log_message_to_monitor("정보", f"=== CHK CSV STEP {step_no}/{total} 시작 ===")

        # 로그/스테이지 표시
        name = params.get("process_name") or f"STEP {self.csv_index + 1}/{len(self.csv_rows)}"

        # ★ CSV STEP 공정 이름 저장 (ChK CSV용)
        self.current_process_name = name

        log_message_to_monitor("Process", f"CSV 공정 리스트 {self.csv_index + 1}/{len(self.csv_rows)} 실행: {name}")
        self.update_stage_monitor(name)

        # 실제 공정 시작
        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)
        self.process_controller.start_process_flow(params)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    dlg = MainDialog()
    dlg.show()
    sys.exit(app.exec())