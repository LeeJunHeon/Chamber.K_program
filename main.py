import sys
import threading
import traceback
from functools import partial
from PyQt6.QtCore import QThread, pyqtSlot as Slot, pyqtSignal as Signal, QEventLoop, QTimer, Qt, QElapsedTimer
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox, QFileDialog
from pathlib import Path

import re
import csv
import datetime

from UI import Ui_Dialog
from lib.config import PLC_COIL_MAP
from lib.logger import (
    set_monitor_widget, 
    log_message_to_monitor, 
    set_process_log_file,
    log_message_to_file, 
    append_chk_csv_row,
)
from controller.process_controller import SputterProcessController
from controller.chat_notifier import ChatNotifier
from device.PLC import PLCController
from device.MFC import MFCController
from device.DCpower import DCPowerController
from device.RFpower import RFPowerController

class MainDialog(QDialog):
    shutdown_requested = Signal()
    request_process_stop = Signal()
    clear_plc_fault = Signal()  # 새 공정 시작 시 PLC 통신 실패 래치 해제

    """메인 UI 및 전체 공정/장치 연결 클래스"""
    def __init__(self):
        super().__init__()
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)
        set_monitor_widget(self.ui.error_monitor)

        # === Google Chat Notifier (CH.K) ===
        try:
            from lib import config_local as cfgl
            url = (getattr(cfgl, 'CHAT_WEBHOOK_URL', '') or '').strip()
        except Exception:
            url = ''

        self.chat_chk = ChatNotifier(url) if url else None
        if self.chat_chk:
            try:
                self.chat_chk.setObjectName('ChatNotifier_CHK')
            except Exception:
                pass
            self.chat_chk.start()

        # 공정 알림 상태
        self._chat_user_stopped: bool = False
        self._chat_errors: list[str] = []
        self._chat_fail_notified: bool = False   # ✅ 실패 원인 일반채팅 중복 방지
        self._chat_fail_reason: str = ""         # ✅ 이번 공정에서 “가장 먼저 잡힌” 실패 원인 1개

        # === Google Chat Notifier (CH.K) ===

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
        self.csv_cancelled: bool = False              # ✅ STOP 시 리스트 전체 취소 플래그

        # --- CSV Delay(공정 사이 대기) 상태 ---
        self._csv_delay_timer: QTimer | None = None
        self._csv_delay_clock: QElapsedTimer | None = None   # ✅ 추가
        self._csv_delay_active: bool = False
        self._csv_delay_total_sec: int = 0
        self._csv_delay_remaining_sec: int = 0
        self._csv_delay_name: str = ""

        # --- ChK CSV용 평균값 누적 변수 초기화 ---
        self._reset_chk_stats()
        self._chk_process_ok: bool = False  # 이번 공정이 정상 종료되었는지 여부

        # CSV 상태 아래에 추가
        self._last_params: dict | None = None

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

        # --- 2. UI 이벤트 -> 컨트롤러 동작 연결 ---
        self.ui.Sputter_Start_Button.clicked.connect(self._handle_start_process)
        self.ui.ALL_STOP_button.clicked.connect(self.plc_controller.on_emergency_stop)
        # ✅ STOP 버튼 전용 핸들러에서 CSV 전체 취소 여부를 먼저 표시
        self.ui.Sputter_Stop_Button.clicked.connect(self._on_sputter_stop_clicked)

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
            self.process_controller._on_mfc_failed,
            type=Qt.ConnectionType.QueuedConnection
        )
        
        # 새로 만든 신호를 Process Controller의 stop_process 슬롯에 연결
        # 이렇게 하면 stop_process는 Process 스레드에서 안전하게 실행됩니다.
        self.request_process_stop.connect(self.process_controller.stop_process) # <<< ▼ 이 줄을 추가하세요 ▼
        self.clear_plc_fault.connect(self.plc_controller.clear_fault_latch)

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
        # ✅ DC Output OFF 검증 3회 실패 시: 즉시 구글챗 알림
        self.dcpower_controller.output_off_failed.connect(self._on_dcpower_output_off_failed)

        self.rfpower_controller.status_message.connect(self.on_status_message)
        # ✅ RF ramp-down 검증 실패 시: 즉시 구글챗 알림
        self.rfpower_controller.ramp_down_failed.connect(self._on_rfpower_rampdown_failed)

        self.process_controller.status_message.connect(self.on_status_message)
        
        self.ui.select_csv_button.clicked.connect(self._on_select_csv_clicked)

    # ==================== Google Chat 알림 헬퍼 (CH.K) ====================
    def _chat_reset_run_state(self):
        self._chat_user_stopped = False
        self._chat_errors = []
        self._chat_fail_notified = False
        self._chat_fail_reason = ""

    def _chat_build_params(self, params: dict, process_name: str) -> dict:
        """
        CH1/CH2 템플릿과 동일한 구조로 ChatNotifier에 전달할 params를 만든다.
        - CHK 구분: ch='CHK' 로 강제
        - 공정명: process_note / Process_name 모두 채움
        - 건/타겟 키 호환: g1_target_name -> G1_target_name 로 매핑
        - 0W 표시 방지: dc_power/rf_power가 0이면 chat용 params에서 제거
        """
        p = dict(params or {})

        # ✅ CHK로 확실히 구분되게 (카드 subtitle에 "CHK · ..."로 표시됨)
        p["ch"] = "CHK"
        p.setdefault("prefix", "CHK Sputter")

        # ✅ 공정명(카드 표시용)
        p["process_note"] = process_name
        p.setdefault("Process_name", process_name)

        # ✅ 건/타겟 키 매핑(현재 CHK UI 키는 g1_target_name 형태)
        for i in (1, 2, 3):
            low = f"g{i}_target_name"
            hi  = f"G{i}_target_name"
            if p.get(low) and not p.get(hi):
                p[hi] = p.get(low)

        # ✅ 0W 표기 방지(ChatNotifier는 값이 있으면 0도 표시할 수 있음)
        try:
            dc = float(p.get("dc_power", 0) or 0)
        except Exception:
            dc = 0
        try:
            rf = float(p.get("rf_power", 0) or 0)
        except Exception:
            rf = 0

        p["use_dc_power"] = dc > 0
        p["use_rf_power"] = rf > 0
        if dc <= 0:
            p.pop("dc_power", None)
        if rf <= 0:
            p.pop("rf_power", None)

        return p

    def _chat_add_error(self, reason: str):
        r = (reason or "").strip()
        if not r:
            return

        # ✅ 중복 제거
        if r not in self._chat_errors:
            self._chat_errors.append(r)

        # ✅ 최초 실패 원인 1개 보관
        if not self._chat_fail_reason:
            self._chat_fail_reason = r

    def _chat_notify_failed_now(self, reason: str, *, send_text: bool = False):
        """
        ✅ CH1&2 방식에 맞추기 위해 기본값(send_text=False)은 '저장만' 한다.
        - 카드(종료) 이후에 실패 원인을 일반채팅으로 1회 추가 전송하는 건 _chat_notify_finished에서 수행
        - 다만 필요하면 send_text=True로 즉시 전송도 가능
        """
        if not self.chat_chk:
            return

        r = (reason or "").strip()
        if r:
            self._chat_add_error(r)

        # 즉시 전송을 원할 때만 (기본은 False)
        if not send_text:
            return

        # STOP 중이거나 이미 보냈으면 중복 방지
        if self._chat_user_stopped or self._chat_fail_notified:
            return

        name = self.current_process_name or "CHK"
        self.chat_chk.notify_text(f"❌ CHK 공정 실패 이유: {name} | {r or '오류'}")
        self.chat_chk.flush()
        self._chat_fail_notified = True

    def _chat_notify_started(self, params: dict, process_name: str):
        if not self.chat_chk:
            return
        chat_params = self._chat_build_params(params, process_name)
        self.chat_chk.notify_process_started(chat_params)
        self.chat_chk.flush()

    def _chat_notify_finished(self, ok: bool):
        if not self.chat_chk:
            return

        name = self.current_process_name or "CHK"

        detail = {
            "process_name": name,
            "stopped": bool(self._chat_user_stopped),
            "aborting": False,
            "errors": list(self._chat_errors) if not ok else [],
        }

        # ✅ 종료 카드 먼저 전송
        try:
            self.chat_chk.notify_process_finished_detail(bool(ok), detail)
            self.chat_chk.flush()
        except Exception:
            pass

        # ✅ CH1&2처럼: "실패" && "STOP 아님"이면 카드 외에 일반채팅으로 원인 1줄 추가
        if ok:
            return
        if detail.get("stopped", False):
            return
        if self._chat_fail_notified:
            return

        reason = (self._chat_fail_reason or "").strip()
        if not reason and detail.get("errors"):
            try:
                reason = str(detail["errors"][0]).strip()
            except Exception:
                reason = ""

        if reason:
            try:
                self.chat_chk.notify_text(f"❌ CHK 공정 실패 이유: {name} | {reason}")
                self.chat_chk.flush()
                self._chat_fail_notified = True
            except Exception:
                pass
    # ==================== Google Chat 알림 헬퍼 (CH.K) ====================

    @Slot()
    def _handle_start_process(self):
        if self.process_running:
            QMessageBox.warning(self, "경고", "이미 공정이 진행 중입니다.")
            return
        
        self.clear_plc_fault.emit()
        
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

            # --- G1/G2 사용 여부 + 타겟 이름 ---
            use_g1_flag = self.ui.G1_checkbox.isChecked()
            use_g2_flag = self.ui.G2_checkbox.isChecked()
            g1_target_name = self.ui.G1_edit.toPlainText().strip()
            g2_target_name = self.ui.G2_edit.toPlainText().strip()

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

                # ▼ G1/G2 사용 여부 + 타겟 이름
                "use_g1": use_g1_flag,
                "use_g2": use_g2_flag,
                "g1_target_name": g1_target_name,
                "g2_target_name": g2_target_name,
            }

            if not (params['dc_power'] > 0 or params['rf_power'] > 0):
                raise ValueError("RF 또는 DC 파워 중 하나 이상을 입력해야 합니다.")
            if params['shutter_delay'] <= 0:
                raise ValueError("Shutter Delay는 0보다 커야 합니다.")

        except (ValueError, TypeError) as e:
            QMessageBox.warning(self, "입력 오류", f"공정 파라미터가 잘못되었습니다:\n{e}")
            return
        
        # ★ 단일 공정(수동 Start)일 때의 공정 이름
        self.current_process_name = "Single CHK"

        # ★ 수동 공정도 CSV 공정과 동일한 로그 포맷을 위해
        #    이번 공정 파라미터를 저장 + 평균값 누적 초기화
        self._last_params = dict(params)
        self._reset_chk_stats()
        self._chk_process_ok = True   # 이번 공정은 정상 종료로 가정하고 시작
        
        # ★★★ 여기서 이번 공정용 로그 파일을 NAS에 생성 (CHK_YYYYmmdd_HHMMSS.txt) ★★★
        set_process_log_file(prefix="CHK")
        log_message_to_monitor("정보", "=== CHK 공정 시작 ===")

        self._chat_reset_run_state()
        self._chat_notify_started(params, self.current_process_name)

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

        p = Path(path)
        if not p.exists():
            QMessageBox.warning(self, "파일 오류", "선택한 CSV 파일을 찾을 수 없습니다.")
            return

        self.csv_file_path = str(p)
        # 라벨에 파일명 표시 (원하면 이 줄은 빼도 됨)
        # self.ui.process_list_label.setText(f"Process List: {p.name}")
        log_message_to_monitor("정보", f"CSV 공정 리스트 파일 선택: {p}")

        # 🔹 CSV를 미리 읽어서 1번째 스텝을 UI에 반영
        if self._load_csv_process_list() and self.csv_rows:
            first_row = self.csv_rows[0]
            first_name = (first_row.get("Process_name") or "").strip()
            delay_sec = self._parse_csv_delay_seconds(first_name)

            if delay_sec is not None:
                self.update_stage_monitor(f"CSV 공정: 1/{len(self.csv_rows)} - {first_name} (대기 스텝)")
            else:
                try:
                    params = self._build_params_from_csv_row(first_row)
                except Exception as e:
                    QMessageBox.warning(self, "CSV 레시피 오류", f"첫 번째 공정 파라미터가 잘못되었습니다:\n{e}")
                    self.update_stage_monitor(f"CSV 공정: 1/{len(self.csv_rows)} - (오류)")
                    return
                self._apply_params_to_ui(params)
                name = params.get("process_name") or "STEP 1"
                self.update_stage_monitor(f"CSV 공정: 1/{len(self.csv_rows)} - {name}")

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
                def _has_content(row: dict) -> bool:
                    for k, v in (row or {}).items():
                        if (k or "").strip() == "#":
                            continue
                        if v is None:
                            continue
                        if str(v).strip() != "":
                            return True
                    return False

                rows = [row for row in reader if _has_content(row)]

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

        # ✅ 실패 원인 저장만 (종료 카드+실패 원인 일반챗은 _handle_process_finished에서)
        self._chat_notify_failed_now(error_message, send_text=False)

        self._chk_process_ok = False  # 연결 실패도 실패 처리
        self._handle_process_finished()

    def on_status_message(self, level, message):
        log_message_to_monitor(level, message)

        if level == "재시작":
            self._chk_process_ok = False

            reason = (message or "").strip() or "PLC 통신 이상(재시작)"

            # ✅ 어떤 장비 문제로 재시작인지 기록(Stop 시퀀스 분기용)
            src = "UNKNOWN"
            r = reason.upper()
            if r.startswith("PLC"):
                src = "PLC"
            elif r.startswith("MFC"):
                src = "MFC"
            elif "DC" in r or "DCPOWER" in r:
                src = "POWER"
            elif "RF" in r or "RFPOWER" in r:
                src = "POWER"

            if self.process_controller:
                # process_controller 쪽에서 이 값을 보고 Stop 순서/스킵을 결정하도록
                self.process_controller.abort_source = src
                self.process_controller.abort_reason = reason

            # ✅ PLC 재연결 5회 실패는 즉시 일반채팅도 전송(요구사항)
            send_now = ("재연결" in reason) and ("5회" in reason or "5" in reason) and ("실패" in reason)
            self._chat_notify_failed_now(reason, send_text=send_now)

            # ✅ CSV 리스트가 진행중이면, 다음 스텝이 이어지지 않도록 취소 플래그부터 세팅
            if getattr(self, "csv_mode", False) and getattr(self, "csv_rows", None):
                self.csv_cancelled = True

                # (1) 딜레이 중이거나, 스텝 사이(=process_running False)면: 바로 리스트 취소
                if getattr(self, "_csv_delay_active", False) or (not self.process_running):
                    self._cancel_csv_list_now("CSV 공정 취소", reason=reason)
                    return

            # (2) 실제 공정 스텝이 돌고 있으면: stop_process로 안전 종료
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
            log_message_to_monitor("WARN", f"[set_indicator] '{frame_name}' 인디케이터가 UI에 없습니다.")

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
            log_message_to_monitor("WARN", f"[update_ui_button_display] '{button_name}' 버튼이 UI에 없습니다.")

    @Slot()
    def _on_sputter_stop_clicked(self):
        """STOP 버튼 공통 처리: 현재 STEP 중단 + CSV 모드면 전체 리스트 취소."""
        # STOP으로 중단된 공정은 정상 종료로 보지 않는다.
        self._chk_process_ok = False
        self._chat_user_stopped = True
        self._chat_add_error("사용자 STOP")
        self.on_status_message("경고", "STOP 버튼 클릭됨")

        # ✅ CSV 리스트 공정 중이면, 이후 STEP들을 모두 취소하도록 플래그 설정
        if self.csv_mode:
            self.csv_cancelled = True
            log_message_to_monitor("정보", "사용자 STOP → CSV 리스트 전체 취소 플래그 설정")

        # ✅ CSV Delay(대기) 중이면 즉시 타이머 끊고 리스트 정리
        if getattr(self, "_csv_delay_active", False):
            log_message_to_monitor("정보", "CSV Delay 중 STOP → 딜레이 즉시 중단 및 리스트 공정 취소")
            self._cancel_csv_list_now("CSV 공정 취소됨")   # ✅ 여기서 구글챗도 같이 보내게 됨(아래 _cancel_csv_list_now 수정)
            return

        # ✅ [추가] CSV 리스트인데 '스텝 사이'(process_running=False)라면 즉시 리스트 취소
        if self.csv_mode and (not self.process_running):
            log_message_to_monitor("정보", "CSV STEP 사이 STOP → 리스트 공정 즉시 취소")
            self._cancel_csv_list_now("CSV 공정 취소됨")
            return

        # 실제 공정이 돌고 있으면 프로세스 스레드 쪽에 중단 요청
        if self.process_controller and self.process_running:
            self.request_process_stop.emit()

    # ==================== ChK CSV 로그용 헬퍼 ====================
    def _build_chk_csv_row(self) -> dict:
        """
        현재 공정(수동 Start / CSV STEP 공통)에 대해,
        입력 파라미터(self._last_params) + 진행 동안 측정된 평균값으로
        ChK_log.csv에 기록할 row 생성.
        """
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 수동/CSV 공통으로 저장해 둔 params 사용
        params = getattr(self, "_last_params", {}) or {}
        process_name = self.current_process_name or params.get("process_name", "")

        def _fmt_float(v) -> str:
            if v is None:
                return ""
            try:
                return f"{float(v):.3f}"
            except Exception:
                return ""

        def _avg(sum_, cnt, fallback=None) -> str:
            if cnt and cnt > 0:
                return _fmt_float(sum_ / cnt)
            return _fmt_float(fallback)

        # --- Shutter Delay / Process Time : 레시피/입력 분(min) 값 ---
        shutter_delay = _fmt_float(params.get("shutter_delay"))
        process_time  = _fmt_float(params.get("process_time"))

        # --- Main Shutter : 레시피/입력 기준 T/F ---
        # ms_bool = bool(params.get("main_shutter"))
        # main_shutter = "T" if ms_bool else "F"
        # 챔버K는 별도로 main shutter를 지정하지 않음

        # --- G1/G2 타겟 이름 (포맷 없이 그대로) ---
        g1_target = (params.get("g1_target_name") or "").strip()
        g2_target = (params.get("g2_target_name") or "").strip()

        # --- 평균값 (없으면 레시피/입력값으로 폴백) ---
        ar_flow   = _avg(self._sum_ar,  self._cnt_ar,  params.get("ar_flow"))
        o2_flow   = _avg(self._sum_o2,  self._cnt_o2,  params.get("o2_flow"))
        work_p    = _avg(self._sum_wp,  self._cnt_wp,  params.get("sp1_set"))

        rf_for_p  = _avg(self._sum_rf_for, self._cnt_rf, params.get("rf_power"))
        rf_ref_p  = _avg(self._sum_rf_ref, self._cnt_rf, 0.0)

        dc_p      = _avg(self._sum_dc_p, self._cnt_dc, params.get("dc_power"))
        dc_v      = _avg(self._sum_dc_v, self._cnt_dc, None)
        dc_i      = _avg(self._sum_dc_i, self._cnt_dc, None)

        row = {
            "Timestamp":        now,
            "Process Name":     process_name,
            "Shutter Delay":    shutter_delay,     # ← 입력 분 값
            "G1 Target":        g1_target,         # ← 입력/CSV 문자열 그대로
            "G2 Target":        g2_target,         # ← 입력/CSV 문자열 그대로
            "Ar flow":          ar_flow,           # ← 전체 공정 평균
            "O2 flow":          o2_flow,           # ← 전체 공정 평균
            "Working Pressure": work_p,            # ← 전체 공정 평균
            "Process Time":     process_time,      # ← 입력 분 값
            "RF: For.P":        rf_for_p,          # ← 전체 공정 평균
            "RF: Ref. P":       rf_ref_p,          # ← 전체 공정 평균
            "DC: V":            dc_v,              # ← 전체 공정 평균
            "DC: I":            dc_i,              # ← 전체 공정 평균
            "DC: P":            dc_p,              # ← 전체 공정 평균
        }

        return row
    
    def _reset_chk_stats(self):
        """ChK CSV 평균 계산용 누적값 + 샘플링 상태 초기화."""
        # 가스 유량(Ar/O2)
        self._sum_ar = 0.0
        self._sum_o2 = 0.0
        self._cnt_ar = 0
        self._cnt_o2 = 0

        # Working Pressure
        self._sum_wp = 0.0
        self._cnt_wp = 0

        # RF 파워(for/ref)
        self._sum_rf_for = 0.0
        self._sum_rf_ref = 0.0
        self._cnt_rf = 0

        # DC 파워(V/I/P)
        self._sum_dc_p = 0.0
        self._sum_dc_v = 0.0
        self._sum_dc_i = 0.0
        self._cnt_dc  = 0

        # Shutter delay → Process time 구간만 누적하기 위한 플래그
        self._chk_sampling_enabled: bool = False
        self._chk_sampling_started: bool = False

    def _reset_process_ui_fields(self):
        """공정 종료/중단 후 Sputter 관련 UI를 '초기 상태'로 리셋."""
        # --- Gas 선택 (UI.py 기본값: Ar 체크, O2 해제) ---
        self.ui.Ar_gas_radio.setChecked(False)
        self.ui.O2_gas_radio.setChecked(False)

        # --- Flow (UI.py 기본값: Ar=5, O2 공백) ---
        self.ui.Ar_flow_edit.setPlainText("5")
        self.ui.O2_flow_edit.setPlainText("")

        # --- Working pressure (UI.py 기본값: 2) ---
        self.ui.working_pressure_edit.setPlainText("2")

        # --- Power setpoint + 체크박스 (UI.py 기본값) ---
        self.ui.rf_power_checkbox.setChecked(False)
        self.ui.dc_power_checkbox.setChecked(False)
        self.ui.RF_power_edit.setPlainText("200")
        self.ui.DC_power_edit.setPlainText("200")

        # --- Shutter delay / Process time (UI.py 기본값) ---
        self.ui.Shutter_delay_edit.setPlainText("5")
        self.ui.process_time_edit.setPlainText("10")

        # --- G1/G2 타겟 및 사용 여부 (기본은 사용 안 함 + 공백) ---
        self.ui.G1_checkbox.setChecked(False)
        self.ui.G2_checkbox.setChecked(False)
        self.ui.G1_edit.clear()
        self.ui.G2_edit.clear()

        # --- RF 보정값 (UI.py 기본값) ---
        self.ui.offset_edit.setPlainText("6.79")
        self.ui.param_edit.setPlainText("1.0395")

        # --- 측정값(파워/전압/전류/for/ref)은 0으로 초기화 ---
        self.ui.Power_edit.setPlainText("0.0")
        self.ui.Voltage_edit.setPlainText("0.0")
        self.ui.Current_edit.setPlainText("0.0")
        self.ui.for_p_edit.setPlainText("0.0")
        self.ui.ref_p_edit.setPlainText("0.0")

    # ============= CSV Delay (공정 사이 대기) =============
    _CSV_DELAY_RE = re.compile(r"^\s*delay\s+(\d+(?:\.\d+)?)\s*([smhd])\s*$", re.IGNORECASE)

    def _parse_csv_delay_seconds(self, process_name: str) -> int | None:
        """Process_name이 'delay 60m' 같은 형태면 대기 시간(초)을 반환, 아니면 None."""
        if not process_name:
            return None
        m = self._CSV_DELAY_RE.match(process_name)
        if not m:
            return None

        try:
            num = float(m.group(1))
        except Exception:
            return None

        unit = (m.group(2) or "m").lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
        if mult is None:
            return None

        sec = int(num * mult)
        return max(sec, 0)

    def _fmt_hms(self, seconds: int) -> str:
        seconds = max(int(seconds or 0), 0)
        h, r = divmod(seconds, 3600)
        m, s = divmod(r, 60)
        return f"{h:d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"

    def _stop_csv_delay_timer(self) -> None:
        t = getattr(self, "_csv_delay_timer", None)
        if t is not None:
            try:
                t.stop()
                t.deleteLater()
            except Exception:
                pass
        self._csv_delay_timer = None
        self._csv_delay_clock = None   # ✅ 추가

    def _cancel_csv_list_now(
        self,
        stage_text: str = "CSV 공정 취소됨",
        *,
        notify_chat: bool = True,
        reason: str | None = None,
    ) -> None:
        """CSV 리스트 공정을 즉시 정리(딜레이/스텝 사이/즉시 취소 등에서 공통 사용)."""

        # ✅ (추가) finished 시그널을 안 거치는 케이스에서도 구글챗 종료/실패 알림 보장
        if notify_chat and getattr(self, "chat_chk", None):
            try:
                # reason이 있으면 그걸 '실패 원인'으로 저장
                if reason:
                    self._chat_notify_failed_now(reason, send_text=False)
                else:
                    # STOP이면 stage_text를 굳이 error로 넣지 않게(카드가 깔끔)
                    if not getattr(self, "_chat_user_stopped", False):
                        self._chat_add_error(stage_text)

                # 카드에 찍힐 공정명 보정
                if not (getattr(self, "current_process_name", "") or "").strip():
                    self.current_process_name = stage_text

                # 종료 카드 + (실패면) 일반챗 1줄(단, STOP이면 일반챗 추가 전송 안 함)
                self._chat_notify_finished(False)
            except Exception:
                pass

        # === 기존 정리 로직 그대로 ===
        self._stop_csv_delay_timer()
        self._csv_delay_active = False
        self._csv_delay_total_sec = 0
        self._csv_delay_remaining_sec = 0
        self._csv_delay_name = ""
        self._csv_delay_clock = None

        self.csv_cancelled = False
        self.csv_mode = False
        self.csv_rows = []
        self.csv_index = -1
        self.csv_file_path = None
        self.current_process_name = ""
        self._last_params = None

        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        self.update_stage_monitor(stage_text)
        self._reset_process_ui_fields()

    def _start_csv_delay_step(self, delay_sec: int, raw_name: str) -> None:
        """CSV 리스트 중 'delay Xm' 스텝 실행: UI는 멈추지 않고(타이머로) 카운트다운."""
        self._stop_csv_delay_timer()

        self._csv_delay_active = True
        self._csv_delay_total_sec = max(int(delay_sec), 0)
        self._csv_delay_remaining_sec = self._csv_delay_total_sec
        self._csv_delay_name = raw_name

        # ✅ 시작시간(모노토닉) 기록
        self._csv_delay_clock = QElapsedTimer()
        self._csv_delay_clock.start()

        step_no = self.csv_index + 1
        total = len(self.csv_rows)

        # 공정처럼 보이게 UI 버튼 상태 유지
        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)

        log_message_to_monitor(
            "Process",
            f"CSV DELAY STEP {step_no}/{total} 시작: {raw_name} (총 {self._fmt_hms(self._csv_delay_total_sec)})",
        )

        # 즉시 1회 표시 (UI에 알아보기 쉽게)
        self.update_stage_monitor(
            f"CSV {step_no}/{total} - {raw_name} (남은 {self._fmt_hms(self._csv_delay_remaining_sec)})"
        )
        
        # ✅ 구글챗(딜레이 시작 1회)
        try:
            if self.chat_chk:
                name = self.current_process_name or f"CSV {step_no}/{total} - {raw_name}"
                self.chat_chk.notify_text(
                    f"⏳ CHK 딜레이 시작: {name} | 총 {self._fmt_hms(self._csv_delay_total_sec)}"
                )
                self.chat_chk.flush()
        except Exception:
            pass

        # 1초마다 카운트다운
        self._csv_delay_timer = QTimer(self)
        self._csv_delay_timer.setInterval(1000)
        self._csv_delay_timer.setTimerType(Qt.TimerType.PreciseTimer)  # ✅ 권장
        self._csv_delay_timer.timeout.connect(self._on_csv_delay_tick)
        self._csv_delay_timer.start()

    @Slot()
    def _on_csv_delay_tick(self) -> None:
        # 외부에서 취소/종료된 경우
        if (not self.csv_mode) or self.csv_cancelled or (not self._csv_delay_active):
            self._stop_csv_delay_timer()
            return

        # ✅ elapsed 기반으로 남은 시간 재계산 (UI 렉이 있어도 누적오차 없음)
        if self._csv_delay_clock is not None:
            elapsed_sec = int(self._csv_delay_clock.elapsed() // 1000)
            self._csv_delay_remaining_sec = max(self._csv_delay_total_sec - elapsed_sec, 0)
        else:
            # 혹시 모를 fallback
            self._csv_delay_remaining_sec = max(self._csv_delay_remaining_sec - 1, 0)

        step_no = self.csv_index + 1
        total = len(self.csv_rows)

        if self._csv_delay_remaining_sec <= 0:
            self._stop_csv_delay_timer()
            self._csv_delay_active = False
            self.process_running = False
            log_message_to_monitor("정보", f"CSV DELAY 완료: {self._csv_delay_name}")

            # ✅ 구글챗(딜레이 완료 1회)
            try:
                if self.chat_chk:
                    name = self.current_process_name or f"CSV {step_no}/{total} - {self._csv_delay_name}"
                    self.chat_chk.notify_text(f"✅ CHK 딜레이 완료: {name}")
                    self.chat_chk.flush()
            except Exception:
                pass

            self._start_next_csv_step()
            return

        self.update_stage_monitor(
            f"CSV {step_no}/{total} - {self._csv_delay_name} (남은 {self._fmt_hms(self._csv_delay_remaining_sec)})"
        )

    # ==================== ChK CSV 로그용 헬퍼 ====================
    def _handle_process_finished(self):
        self.on_status_message("정보", "프로세스 종료중.")

        # ★ 이번 STEP이 정상 종료됐는지 여부를 먼저 보관
        last_step_ok = getattr(self, "_chk_process_ok", False)
        self._chat_notify_finished(last_step_ok)

        # ★ 정상적으로 완료된 공정만 ChK_log.csv 에 한 줄 추가
        try:
            if last_step_ok:
                row = self._build_chk_csv_row()
                ok = append_chk_csv_row(row)
                if ok:
                    log_message_to_monitor("정보", "ChK CSV 로그 저장 완료")
                else:
                    log_message_to_monitor("경고", "ChK CSV 로그 저장 실패")
            else:
                log_message_to_monitor("정보", "이번 공정은 비정상 종료 → ChK CSV에 기록하지 않음")
        except Exception as e:
            log_message_to_monitor("경고", f"ChK CSV 로그 처리 중 예외 발생: {e!r}")
        finally:
            # 다음 공정을 위해 평균 누적값/상태 초기화
            self._reset_chk_stats()
            self._chk_process_ok = False

        # ✅ 1) CSV 리스트 공정 모드인 경우
        if self.csv_mode and self.csv_rows:
            self.process_running = False

            # ✅ (1) 사용자가 STOP을 눌러 전체 리스트 취소한 경우
            if getattr(self, "csv_cancelled", False):
                # 플래그 리셋
                self.csv_cancelled = False

                # CSV 상태 전체 초기화
                self.csv_mode = False
                self.csv_rows = []
                self.csv_index = -1
                self.csv_file_path = None          # ★ CSV 파일 선택도 해제
                self.current_process_name = ""     # (선택) 이름 흔적 제거
                self._last_params = None           # (선택) 파라미터 흔적 제거

                # ▶ 공정 상태 및 UI 초기화
                self.ui.Sputter_Start_Button.setEnabled(True)
                self.ui.Sputter_Stop_Button.setEnabled(False)
                self.update_stage_monitor("CSV 공정 취소됨")
                self._reset_process_ui_fields()
                return

            # ✅ (2) STOP이 아닌 경우: 성공/실패에 따라 분기
            if last_step_ok:
                # 이전 STEP이 정상 종료된 경우에만 다음 STEP으로 진행
                self._start_next_csv_step()
                return
            else:
                # ❌ 실패/에러로 끝난 경우 → 이후 STEP들은 실행하지 않고 CSV 리스트 공정 종료
                log_message_to_monitor(
                    "경고",
                    "CSV 공정 중 실패 발생 → 다음 공정을 실행하지 않고 리스트 공정을 종료합니다.",
                )

                # CSV 상태 전체 초기화
                self.csv_mode = False
                self.csv_rows = []
                self.csv_index = -1
                self.csv_file_path = None
                self.current_process_name = ""
                self._last_params = None

                # 공정 상태 및 UI 초기화
                self.process_running = False
                self.ui.Sputter_Start_Button.setEnabled(True)
                self.ui.Sputter_Stop_Button.setEnabled(False)
                self.update_stage_monitor("CSV 공정 실패로 중단됨")
                self._reset_process_ui_fields()
                return

        # 🔻 여기 이하(단일 공정 종료 처리)는 그대로 유지
        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        self.update_stage_monitor("공정 종료")
        
        # ▶ 공통 UI 초기화
        self._reset_process_ui_fields()

    @Slot(str)
    def _handle_critical_error(self, error_message):
        QMessageBox.critical(self, "공정 중단", f"공정이 중단되었습니다.\n\n사유: {error_message}")
        self._chk_process_ok = False

        # ✅ 저장만 (stop 시퀀스 끝나고 finished에서 카드+일반챗 1줄)
        self._chat_notify_failed_now(error_message, send_text=False)

        # ✅ 여기서 _handle_process_finished()를 직접 호출하지 마세요.
        # stop 시퀀스가 끝나면 ProcessController.finished가 1번만 호출해줍니다.

    def update_stage_monitor(self, stage_text):
        self.ui.stage_monitor.setPlainText(stage_text)

    def update_shutter_delay_timer(self, seconds_left):
        # Shutter delay 카운트다운이 처음 시작되는 시점부터 샘플링 시작
        if not getattr(self, "_chk_sampling_started", False):
            self._chk_sampling_started = True
            self._chk_sampling_enabled = True

        m, s = divmod(seconds_left, 60)
        text = f"{m:02d}:{s:02d}"
        self.ui.Shutter_delay_edit.setPlainText(text)

    def update_process_time_timer(self, seconds_left):
        m, s = divmod(seconds_left, 60)
        text = f"{m:02d}:{s:02d}"
        self.ui.process_time_edit.setPlainText(text)

        # process time이 끝났으면 샘플링 종료
        if seconds_left <= 0:
            self._chk_sampling_enabled = False

    def update_mfc_flow_display(self, gas, value):
        if not getattr(self, "process_running", False):
            return
        
        edit = self.ui.Ar_flow_edit if gas == "Ar" else self.ui.O2_flow_edit
        edit.setPlainText(f"{value:.2f}")

        # --- ChK CSV 평균 계산: 샘플링 구간에서만 누적 ---
        if not getattr(self, "_chk_sampling_enabled", False):
            return

        try:
            v = float(value)
        except Exception:
            return

        if gas == "Ar":
            self._sum_ar += v
            self._cnt_ar += 1
        elif gas == "O2":
            self._sum_o2 += v
            self._cnt_o2 += 1

    def update_mfc_pressure_display(self, pressure):
        if not getattr(self, "process_running", False):
            return

        self.ui.working_pressure_edit.setPlainText("ERROR" if pressure is None else str(pressure))

        if not getattr(self, "_chk_sampling_enabled", False):
            return

        if pressure is not None:
            try:
                v = float(pressure)
            except Exception:
                return
            self._sum_wp += v
            self._cnt_wp += 1

    def update_dc_status_display(self, power, voltage, current):
        """DC 파워 측정값 (P, V, I)을 UI에 표시"""
        if not getattr(self, "process_running", False):
            return

        # 전압 / 전류
        self.ui.Voltage_edit.setPlainText(f"{voltage:.2f}")
        self.ui.Current_edit.setPlainText(f"{current:.3f}")
        # 파워는 장비에서 계산된 값 사용
        self.ui.Power_edit.setPlainText(f"{power:.2f}")

        # ✅ (추가) 읽을 때마다 1초 1줄 로그 저장
        try:
            log_message_to_file("DC", f"MEAS P={power:.2f}W, V={voltage:.2f}V, I={current:.3f}A")
        except Exception:
            pass

        # --- ChK CSV 평균 계산: 샘플링 구간에서만 누적 ---
        if getattr(self, "_chk_sampling_enabled", False):
            self._sum_dc_p += power
            self._sum_dc_v += voltage
            self._sum_dc_i += current
            self._cnt_dc += 1

    def update_rf_status_display(self, forward_power, reflected_power):
        if not getattr(self, "process_running", False):
            return

        self.ui.for_p_edit.setPlainText(f"{forward_power:.2f}")
        self.ui.ref_p_edit.setPlainText(f"{reflected_power:.2f}")

        # ✅ (추가) 읽을 때마다 1초 1줄 로그 저장
        try:
            log_message_to_file("RF", f"MEAS For={forward_power:.2f}W, Ref={reflected_power:.2f}W")
        except Exception:
            pass

        # --- ChK CSV 평균 계산: 샘플링 구간에서만 누적 ---
        if getattr(self, "_chk_sampling_enabled", False):
            self._sum_rf_for += forward_power
            self._sum_rf_ref += reflected_power
            self._cnt_rf += 1

    @Slot(bool)
    def _on_ui_door_toggled(self, checked: bool):
        """
        UI의 Door_Button 한 개 토글을 PLC의 Up/Down 두 코일로 분리 전달.
        - True  -> Doorup_button (문 열기)
        - False -> Doordn_button (문 닫기)
        """
        if checked:
            self.plc_controller.update_port_state('Doordn_button', False)
            self.plc_controller.update_port_state('Doorup_button', True)
        else:
            self.plc_controller.update_port_state('Doorup_button', False)
            self.plc_controller.update_port_state('Doordn_button', True)
            
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

            try:
                if self.chat_chk:
                    self.chat_chk.shutdown()
            except Exception:
                pass

            log_message_to_monitor("정보", "모든 스레드가 종료되었습니다. 프로그램을 닫습니다.")
            event.accept()
        else:
            event.ignore()

    # ============= csv 공정 =============
    def _build_params_from_csv_row(self, row: dict) -> dict:
        """
        ✅ 변경(중요)
        - CSV 값이 비어있을 때(UI 값으로) 폴백하지 않음
        - 필요한 값이 비어있거나 형식이 잘못되면 ValueError로 즉시 중단
        - use_* 가 False(또는 비어있음)인 기능은 안전하게 OFF(0) 처리
        """

        def _s(key: str):
            v = (row or {}).get(key)
            if v is None:
                return None
            s = str(v).strip()
            return s if s != "" else None

        def _b(key: str, default: bool = False) -> bool:
            s = _s(key)
            if s is None:
                return default
            return s.lower() in ("1", "y", "yes", "true", "t", "on")

        def _f(key: str, *, required: bool = False, default: float | None = None) -> float | None:
            s = _s(key)
            if s is None:
                if required:
                    raise ValueError(f"CSV '{key}' 값이 비어 있습니다.")
                return default
            try:
                return float(s)
            except Exception:
                raise ValueError(f"CSV '{key}' 값이 숫자가 아닙니다: {s!r}")

        process_name = (_s("Process_name") or "").strip()

        # ---- 가스(필수) ----
        use_ar_gas = _b("Ar", False)
        use_o2_gas = _b("O2", False)
        if not (use_ar_gas or use_o2_gas):
            raise ValueError(f"[{process_name or 'STEP'}] Ar/O2 중 하나 이상을 1(ON)로 지정해야 합니다.")

        ar_flow = 0.0
        o2_flow = 0.0
        if use_ar_gas:
            ar_flow = _f("Ar_flow", required=True)  # type: ignore
            if ar_flow is None or ar_flow <= 0:
                raise ValueError(f"[{process_name or 'STEP'}] Ar=ON 인데 Ar_flow가 비어있거나 0 이하입니다.")
        if use_o2_gas:
            o2_flow = _f("O2_flow", required=True)  # type: ignore
            if o2_flow is None or o2_flow <= 0:
                raise ValueError(f"[{process_name or 'STEP'}] O2=ON 인데 O2_flow가 비어있거나 0 이하입니다.")

        # ---- 압력/시간(필수) ----
        work_p = _f("working_pressure", required=True)  # type: ignore
        proc_time = _f("process_time", required=True)   # type: ignore
        sh_delay = _f("shutter_delay", required=False, default=0.0)

        if work_p is None or work_p <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] working_pressure가 비어있거나 0 이하입니다.")
        if proc_time is None or proc_time <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] process_time이 비어있거나 0 이하입니다.")
        if sh_delay is None:
            sh_delay = 0.0
        if sh_delay < 0:
            raise ValueError(f"[{process_name or 'STEP'}] shutter_delay는 0 이상이어야 합니다.")

        # ---- 파워(선택) : use_*가 비어있으면 OFF로 간주 ----
        use_dc = _b("use_dc_power", False)
        use_rf = _b("use_rf_power", False)

        dc_power = 0.0
        rf_power = 0.0

        if use_dc:
            dc_power_val = _f("dc_power", required=True)  # type: ignore
            if dc_power_val is None or dc_power_val <= 0:
                raise ValueError(f"[{process_name or 'STEP'}] use_dc_power=ON 인데 dc_power가 비어있거나 0 이하입니다.")
            dc_power = float(dc_power_val)

        if use_rf:
            rf_power_val = _f("rf_power", required=True)  # type: ignore
            if rf_power_val is None or rf_power_val <= 0:
                raise ValueError(f"[{process_name or 'STEP'}] use_rf_power=ON 인데 rf_power가 비어있거나 0 이하입니다.")
            rf_power = float(rf_power_val)

        # ---- Gun(선택) ----
        use_g1 = _b("gun1", False)
        use_g2 = _b("gun2", False)

        g1_target_name = (_s("G1 Target") or "").strip()
        g2_target_name = (_s("G2 Target") or "").strip()

        # selected_gas / mfc_flow (백워드 호환용)
        if use_ar_gas and not use_o2_gas:
            selected_gas = "Ar"
            mfc_flow = ar_flow
        elif use_o2_gas and not use_ar_gas:
            selected_gas = "O2"
            mfc_flow = o2_flow
        else:
            selected_gas = "Ar"
            mfc_flow = ar_flow

        # RF 보정값은 CSV에 없으므로 UI 값을 그대로 사용(단, RF를 쓰는 경우 값이 비면 오류)
        offset_text = self.ui.offset_edit.toPlainText().strip()
        param_text  = self.ui.param_edit.toPlainText().strip()

        def _float_ui(text, default):
            try:
                return float(str(text).strip())
            except Exception:
                return default

        if use_rf and (not offset_text or not param_text):
            raise ValueError(f"[{process_name or 'STEP'}] RF 사용인데 Offset/Param 값이 UI에 비어있습니다.")

        rf_offset = _float_ui(offset_text or 6.79, 6.79)
        rf_param  = _float_ui(param_text  or 1.0395, 1.0395)

        return {
            "use_ar_gas": use_ar_gas,
            "use_o2_gas": use_o2_gas,
            "ar_flow": float(ar_flow),
            "o2_flow": float(o2_flow),

            "selected_gas": selected_gas,
            "mfc_flow": float(mfc_flow),

            "sp1_set": float(work_p),

            "dc_power": float(dc_power),
            "rf_power": float(rf_power),
            "shutter_delay": float(sh_delay),
            "process_time": float(proc_time),

            "rf_offset": float(rf_offset),
            "rf_param": float(rf_param),

            "use_g1": bool(use_g1),
            "use_g2": bool(use_g2),

            "g1_target_name": g1_target_name,
            "g2_target_name": g2_target_name,

            "process_name": process_name,
        }
    
    def _apply_params_to_ui(self, params: dict) -> None:
        """
        CSV 한 단계(params)를 현재 UI 위젯에 반영.
        - CH1/CH2처럼, 실행 중인 스텝의 설정이 UI에도 보이도록 한다.
        """
        # 가스 선택
        use_ar = bool(params.get("use_ar_gas"))
        use_o2 = bool(params.get("use_o2_gas"))

        try:
            self.ui.Ar_gas_radio.blockSignals(True)
            self.ui.O2_gas_radio.blockSignals(True)
            self.ui.Ar_gas_radio.setChecked(use_ar)
            self.ui.O2_gas_radio.setChecked(use_o2)
        finally:
            self.ui.Ar_gas_radio.blockSignals(False)
            self.ui.O2_gas_radio.blockSignals(False)

        # 유량
        ar_flow = params.get("ar_flow")
        o2_flow = params.get("o2_flow")
        if ar_flow is not None:
            self.ui.Ar_flow_edit.setPlainText(f"{float(ar_flow):.2f}")
        if o2_flow is not None:
            self.ui.O2_flow_edit.setPlainText(f"{float(o2_flow):.2f}")

        # 작업 압력(sp1_set)
        sp1_set = params.get("sp1_set")
        if sp1_set is not None:
            self.ui.working_pressure_edit.setPlainText(str(sp1_set))

        # DC 파워
        dc_power = float(params.get("dc_power") or 0.0)
        try:
            self.ui.dc_power_checkbox.blockSignals(True)
            self.ui.dc_power_checkbox.setChecked(dc_power > 0)
        finally:
            self.ui.dc_power_checkbox.blockSignals(False)
        self.ui.DC_power_edit.setPlainText(f"{dc_power:.1f}" if dc_power > 0 else "0")

        # RF 파워
        rf_power = float(params.get("rf_power") or 0.0)
        try:
            self.ui.rf_power_checkbox.blockSignals(True)
            self.ui.rf_power_checkbox.setChecked(rf_power > 0)
        finally:
            self.ui.rf_power_checkbox.blockSignals(False)
        self.ui.RF_power_edit.setPlainText(f"{rf_power:.1f}" if rf_power > 0 else "0")

        # Shutter delay / process time (분 단위)
        sh_delay = params.get("shutter_delay")
        proc_time = params.get("process_time")
        if sh_delay is not None:
            self.ui.Shutter_delay_edit.setPlainText(str(sh_delay))
        if proc_time is not None:
            self.ui.process_time_edit.setPlainText(str(proc_time))

        # RF 보정값
        rf_offset = params.get("rf_offset")
        rf_param  = params.get("rf_param")
        if rf_offset is not None:
            self.ui.offset_edit.setPlainText(str(rf_offset))
        if rf_param is not None:
            self.ui.param_edit.setPlainText(str(rf_param))

        # Gun 선택
        use_g1 = params.get("use_g1")
        use_g2 = params.get("use_g2")
        if use_g1 is not None:
            self.ui.G1_checkbox.setChecked(bool(use_g1))
        if use_g2 is not None:
            self.ui.G2_checkbox.setChecked(bool(use_g2))

        # G1/G2 타겟 이름 UI 반영
        g1_name = params.get("g1_target_name")
        g2_name = params.get("g2_target_name")
        if g1_name is not None:
            self.ui.G1_edit.setPlainText(str(g1_name))
        if g2_name is not None:
            self.ui.G2_edit.setPlainText(str(g2_name))
    
    def _start_next_csv_step(self):
        """csv_rows[csv_index+1] 공정을 하나 실행하거나, 모두 끝났으면 CSV 모드 종료."""
        # ✅ STOP 등으로 CSV 리스트 전체가 취소된 뒤에
        #    _start_next_csv_step 이 호출되면 아무 것도 하지 않고 무시
        if not self.csv_mode or not self.csv_rows or self.csv_cancelled:
            log_message_to_monitor(
                "정보",
                "_start_next_csv_step 호출됐지만 CSV 모드가 아니거나 취소 플래그가 켜져 있어서 무시합니다.",
            )
            return

        self.csv_index += 1

        # 모든 행을 다 돌았으면 종료
        if self.csv_index >= len(self.csv_rows):
            QMessageBox.information(self, "CSV 공정 완료", "CSV에 있는 모든 공정을 완료했습니다.")
            self.csv_mode = False
            self.csv_rows = []
            self.csv_index = -1
            self.process_running = False

            # ✅ 이번 CSV 회차 공정 이름/파라미터 흔적 제거
            self.current_process_name = ""
            self._last_params = None
            self.csv_file_path = None      # ★ CSV 파일 선택도 해제

            # ✅ UI도 대기 상태로 정리
            self.ui.Sputter_Start_Button.setEnabled(True)
            self.ui.Sputter_Stop_Button.setEnabled(False)
            self.update_stage_monitor("CSV 공정 완료")

            # ▶ 공통 UI 초기화
            self._reset_process_ui_fields()
            return

        row = self.csv_rows[self.csv_index]

        # ✅ 여기서 step_no/total 먼저 고정(딜레이 포함 모든 분기에서 동일하게 쓰기)
        step_no = self.csv_index + 1
        total   = len(self.csv_rows)

        raw_name = (row.get("Process_name") or "").strip()
        delay_sec = self._parse_csv_delay_seconds(raw_name)
        if delay_sec is not None:
            if delay_sec <= 0:
                log_message_to_monitor("정보", f"CSV DELAY 스킵: {raw_name} (0초)")
                self._start_next_csv_step()
                return

            # ✅ 딜레이도 "현재 공정명"을 CSV 1/3 형태로 잡아두면 STOP/실패 텍스트가 안 헷갈림
            self.current_process_name = f"CSV {step_no}/{total} - {raw_name}"

            self._start_csv_delay_step(delay_sec, raw_name)
            return

        # (안전) '#'(번호)만 채워진 빈 행은 그냥 스킵
        def _is_blank_row(r: dict) -> bool:
            for k, v in (r or {}).items():
                if (k or "").strip() == "#":
                    continue
                if v is None:
                    continue
                if str(v).strip() != "":
                    return False
            return True

        if _is_blank_row(row):
            log_message_to_monitor("정보", f"CSV 빈 행 스킵: index={self.csv_index + 1}")
            self._start_next_csv_step()
            return

        try:
            params = self._build_params_from_csv_row(row)
        except Exception as e:
            QMessageBox.critical(
                self, "CSV 레시피 오류",
                f"CSV {self.csv_index + 1}번째 행 파라미터가 잘못되었습니다:\n{e}"
            )
            log_message_to_monitor("ERROR", f"CSV 레시피 오류로 리스트 공정을 중단합니다: {e}")

            # ✅ 구글챗에도 실패 알림(종료 카드 + 일반챗 1줄) 보장
            self._chk_process_ok = False
            try:
                # 이 케이스는 '공정 started'를 안 보냈을 수도 있으니, 상태를 새로 잡아줌
                self._chat_reset_run_state()

                # 위에서 step_no/total을 이미 만든 상태(네 코드 기준)라서 그대로 사용
                reason = f"CSV 레시피 오류: {e}"
                self.current_process_name = f"CSV {step_no}/{total} - (레시피 오류)"
                self._chat_notify_failed_now(reason, send_text=False)
                self._chat_notify_finished(False)
            except Exception:
                pass

            # ✅ 여기서는 중복 전송 방지 위해 notify_chat=False
            self._cancel_csv_list_now("CSV 레시피 오류로 중단", notify_chat=False)
            return

        # ★ 이번 CSV STEP도 수동 공정과 동일한 로그 포맷을 위해
        #    파라미터 저장 + 평균값 누적 초기화
        self._last_params = dict(params)
        self._reset_chk_stats()
        self._chk_process_ok = True   # 이 STEP이 정상 종료했을 때만 CSV에 기록

        # ✅ 이 단계의 파라미터를 UI에 반영 (CH1/CH2처럼 보이게)
        self._apply_params_to_ui(params)

        # ★★★ 이 CSV STEP 전용 로그 파일 생성 ★★★
        set_process_log_file(prefix="CHK")

        # ✅ (위에서 step_no/total을 이미 만들었다면 여기 재정의 필요 없음)
        # step_no = self.csv_index + 1
        # total   = len(self.csv_rows)

        log_message_to_monitor("정보", f"=== CHK CSV STEP {step_no}/{total} 시작 ===")

        # 로그/스테이지 표시
        base_name = (
            params.get("process_name")
            or params.get("Process_name")
            or f"STEP {step_no}/{total}"
        )

        # ✅ 이 STEP의 표시명(=CSV 1/3 포함)으로 통일
        display = f"CSV {step_no}/{total} - {base_name}"

        # ✅ 공정 종료 카드/실패 텍스트가 이 값을 보게 됨 → CSV 1/3이 항상 유지됨
        self.current_process_name = display

        self._chat_reset_run_state()
        self._chat_notify_started(params, display)

        log_message_to_monitor("Process", f"CSV 공정 리스트 {step_no}/{total} 실행: {display}")
        self.update_stage_monitor(display)

        # 실제 공정 시작
        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)

        self.clear_plc_fault.emit()              # ✅ 추가: 스텝 시작마다 PLC 실패 래치 초기화
        self.process_controller.start_requested.emit(params)

    @Slot(str)
    def _on_dcpower_output_off_failed(self, detail: str):
        # 1) 모니터 표시
        log_message_to_monitor("ERROR", f"[DC] {detail}")

        # 2) 이번 공정은 실패로 기록
        self._chk_process_ok = False
        self._chat_add_error(detail)

        # 3) ✅ 즉시 구글챗 알림 (3회 실패는 “즉시 알림”이 요구사항)
        try:
            if self.chat_chk:
                name = self.current_process_name or "CHK"
                self.chat_chk.notify_text(
                    f"⚠️ CHK DC Power OUTPUT OFF 실패(3회 재시도) | {name} | {detail}"
                )
                self.chat_chk.flush()
                self._chat_fail_notified = True
                if not self._chat_fail_reason:
                    self._chat_fail_reason = detail
        except Exception:
            pass

    @Slot(str)
    def _on_rfpower_rampdown_failed(self, detail: str):
        # 1) 모니터 표시
        log_message_to_monitor("ERROR", f"[RF] {detail}")

        # 2) 이번 공정은 실패로 기록
        self._chk_process_ok = False
        self._chat_add_error(detail)

        # 3) ✅ 즉시 구글챗 알림
        try:
            if self.chat_chk:
                name = self.current_process_name or "CHK"
                self.chat_chk.notify_text(
                    f"⚠️ CHK RF Power RAMP-DOWN 검증 실패(최대 3회 재시도) | {name} | {detail}"
                )
                self.chat_chk.flush()

                # 중복 전송 방지 플래그
                self._chat_fail_notified = True
                if not self._chat_fail_reason:
                    self._chat_fail_reason = detail
        except Exception:
            pass

if __name__ == "__main__":
    app = QApplication(sys.argv)

    def _send_fatal_to_chat(msg: str):
        try:
            dlg = getattr(app, "_main_dlg", None)
            if dlg and getattr(dlg, "chat_chk", None):
                dlg.chat_chk.notify_text(msg)
                dlg.chat_chk.flush()
        except Exception:
            pass

    def _excepthook(exctype, value, tb):
        try:
            tb_text = "".join(traceback.format_exception(exctype, value, tb))
            log_message_to_monitor("ERROR", f"[UNHANDLED] {tb_text}")
        except Exception:
            pass
        _send_fatal_to_chat(f"❌ CHK 프로그램 예외(메인): {value!r}")

    sys.excepthook = _excepthook

    def _thread_excepthook(args):
        try:
            tb_text = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
            log_message_to_monitor("ERROR", f"[UNHANDLED:{args.thread.name}] {tb_text}")
        except Exception:
            pass
        _send_fatal_to_chat(f"❌ CHK 프로그램 예외({args.thread.name}): {args.exc_value!r}")

    threading.excepthook = _thread_excepthook

    dlg = MainDialog()
    app._main_dlg = dlg
    dlg.show()
    sys.exit(app.exec())
