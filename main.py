import sys
import os as _os
import threading
import traceback
from functools import partial
from PyQt6.QtCore import (
    QThread, pyqtSlot as Slot, pyqtSignal as Signal,
    QEventLoop, QTimer, Qt, QElapsedTimer, QMetaObject
)
from PyQt6.QtWidgets import QApplication, QDialog, QMessageBox, QFileDialog
from pathlib import Path

import re
import csv
import time
import datetime

from UI import Ui_Dialog
from lib.config import PLC_COIL_MAP, PLC_SENSOR_BITS, PLC_MONITOR_BITS
from lib.logger import (
    set_monitor_widget,
    log_message_to_monitor,
    set_process_log_file,
    clear_process_log_file,
    set_heater_log_file,
    clear_heater_log_file,
    log_message_to_file,
    append_chk_csv_row,
    append_comm_event,
    write_plc_blackbox,
)
from reporter import ErpReporter
from controller.process_controller import SputterProcessController
from controller.chat_notifier import ChatNotifier
from device.PLC import PLCController
from device.MFC import MFCController
from device.DCpower import DCPowerController
from device.RFpower import RFPowerController
from device.RFpulse import RFPulseController
from lib.config import (PLC_COIL_MAP, DC_POWER_DELAY_SEC,
                        HEATER_ENABLED, HEATER_MAX_TEMP,
                        HEATER_MV_LIMIT, HEATER_MV_MIN, HEATER_LOG_ENABLED,
                        HEATER_HOLD_MODE, HEATER_HOLD_MV_ENTER_TOL_C, HEATER_HOLD_MV_ENTER_SEC,
                        HEATER_HOLD_MV_ARRIVE_TOL_C, HEATER_HOLD_MV_DRIFT_PV_C, HEATER_HOLD_MV_DRIFT_MV,
                        HEATER_HOLD_TC2_MARGIN_C, HEATER_HOLD_TC2_DRIFT_C, HEATER_MV_SAT_SEC,
                        COMM_PROBE_MS, HEATER_STALE_SEC, HEATER_STALE_FG,
                        HEATER_LOG_PERIOD_MS, HEATER_RECIPE_DIR,
                        HEATER_RAMP_RATE_C_PER_MIN, HEATER_SOAK_TOLERANCE,
                        HEATER_GAS_HOLD_RELEASE_C,
                        HEATER_APPROACH_ZONE_C,
                        HEATER_APPROACH_MIN_RATE_C_PER_MIN,
                        RFPULSE_MAX_POWER, RFPULSE_PULSE_FREQ_MAX_HZ, RFPULSE_PULSE_FREQ_MIN_HZ,
                        rfpulse_pulse_edge_violation,
                        RFPULSE_DUTY_MIN, RFPULSE_DUTY_MAX,
                        heater_est_current)
from lib.recipe_io import load_table
from controller.heater_recipe import HeaterRecipeRunner
from controller.heater_hold import HeaterHold
from controller.heater_saturation import HeaterSaturationGuard
from controller.heater_ramp import RampProfiler
from controller.heater_atmosphere import HeaterAtmosphere
from lib.heater_logger import HeaterCsvLogger

# 히터 레시피 단계 표기. 화면/툴팁이 같은 말을 쓰도록 여기 한 곳에서만 정한다.
HEATER_PHASE_TEXT = {"ramp": "RAMP", "soak": "SOAK", "cool": "COOL"}


def _fmt_hms_sec(sec: float) -> str:
    """초 → M:SS (1시간 넘으면 H:MM:SS). 히터 레시피 진행 표시용."""
    v = max(0, int(sec))
    h, rem = divmod(v, 3600)
    m, ss = divmod(rem, 60)
    return f"{h}:{m:02d}:{ss:02d}" if h else f"{m}:{ss:02d}"


class MainDialog(QDialog):
    shutdown_requested = Signal()
    request_process_stop = Signal()
    request_process_start = Signal(dict)
    request_plc_port_update = Signal(str, bool)
    request_plc_emergency_stop = Signal()
    request_heater_target = Signal(float)   # ★
    request_heater_run    = Signal(bool)    # ★
    request_heater_mv_limit = Signal(int)   # 목표 도달 후 DAC 상한 고정/원복 (→ PLC 스레드, Queued)
    request_heater_sv2    = Signal(float)   # TC2 추종: D00035 TC2 목표 (→ PLC 스레드, Queued)
    request_heater_pv_sel = Signal(bool)    # TC2 추종: M0004A TC2 제어 선택 (→ PLC 스레드, Queued)
    request_heater_reset  = Signal()        # ★
    clear_plc_fault = Signal()  # 새 공정 시작 시 PLC 통신 실패 래치 해제

    """메인 UI 및 전체 공정/장치 연결 클래스"""
    def __init__(self):
        super().__init__()
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)
        set_monitor_widget(self.ui.error_monitor)

        # DC Power 안정화 대기(선택): 기본 OFF
        self.ui.dc_delay_checkbox.setChecked(False)
        self.ui.dc_delay_checkbox.setToolTip(
            f"체크 시 SP1 도달 후 Shutter Delay 시작 전에 "
            f"DC Power 안정화 대기 {int(DC_POWER_DELAY_SEC)}초를 진행합니다.\n"
            f"(대기 시간은 config_user.json의 DC_POWER_DELAY_SEC로 조정)"
        )

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
        self._chat_emergency_stopped: bool = False
        # 이번 공정의 종료 처리(카드 + CSV 판정)를 이미 했는가. 시작한 적이 없으면 True.
        self._finish_handled: bool = True
        self._chat_errors: list[str] = []
        self._chat_fail_notified: bool = False   # ✅ 실패 원인 일반채팅 중복 방지
        # 설비 이상 상세 카드는 공정 1회당 1장만. 같은 이상이 PLC fault 와
        # 레시피 중단 두 경로로 올라와 카드가 2장 뜨던 것을 막는다.
        self._chat_fault_detail_sent: bool = False
        self._chat_fail_reason: str = ""         # ✅ 이번 공정에서 “가장 먼저 잡힌” 실패 원인 1개

        # === ERP Reporter (CH.K) ===
        try:
            from lib import config_local as _cfgl
            _erp_url = (getattr(_cfgl, "ERP_INGEST_URL", "") or "").strip()
            _erp_token = (getattr(_cfgl, "ERP_INGEST_TOKEN", "") or "").strip()
        except Exception:
            _erp_url, _erp_token = "", ""
        self.erp = ErpReporter(_erp_url, _erp_token, equipment="CHK")
        self.erp.start()
        self._erp_run_ended: bool = True   # 아직 시작된 공정 없음
        # MFC 실측값(유량/압력). MFC 폴링은 공정 중에만 돌므로 공정 밖에서는
        # 비어 있어야 한다 — 죽은 값을 '실측'으로 보고하면 안 된다.
        self._erp_meas: dict = {}
        try:
            from lib.logger import set_reporter
            set_reporter(self.erp)
        except Exception:
            pass
        # === ERP Reporter (CH.K) ===

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
        
        # 4-1. RF Pulse 컨트롤러 설정 (CESAR 1310, AE Bus RS-232)
        #   PLC DAC 로 도는 위 RF Power 와 완전히 별개 장비다. 시리얼 직결.
        self.rfpulse_thread = QThread()
        self.rfpulse_thread.setObjectName("RFPulseThread")
        self.rfpulse_controller = RFPulseController()
        self.rfpulse_controller.moveToThread(self.rfpulse_thread)

        # 5. Process 컨트롤러 설정
        self.process_thread = QThread()
        self.process_thread.setObjectName("ProcessThread")
        self.process_controller = SputterProcessController(
            mfc_controller=self.mfc_controller,
            dc_controller=self.dcpower_controller,
            rf_controller=self.rfpower_controller,
            plc_controller=self.plc_controller,
            rfpulse_controller=self.rfpulse_controller
        )
        self.process_controller.moveToThread(self.process_thread)

        # --- 히터 레시피 러너 / CSV 로거 (스퍼터 공정과 무관한 독립 경로) ---
        self.heater_recipe = HeaterRecipeRunner(self.plc_controller, self)
        self._heater_logger = HeaterCsvLogger()
        self._heater_log_last_ms = 0.0
        # RUN 상승/하강 엣지는 update_heater_display 에서 한 번만 계산한다(_heater_run_edge).
        #  로그/챗/가스유지 소비자는 자기 prev 플래그를 갖지 않는다.
        self._heater_run_prev_view = False
        # 이상 상승 엣지에서 RUN OFF 를 보냈는가(에피소드당 1회, fault 해제 시 리셋)
        self._heater_fault_off_sent = False
        # 히터 패널 stale 표시 — 마지막 폴링 시각 / 현재 stale 여부(바뀔 때만 스타일 재적용)
        self._heater_status_t = 0.0
        self._heater_stale = False
        self._heater_style_orig: dict = {}      # stale 진입 시 원본 styleSheet 보관 → 해제 시 그대로 복원
        # 목표 도달 후 유지 모드 상태기(dac / tc2) — 결정은 전부 controller/heater_hold 가 한다
        self.heater_hold = HeaterHold(
            HEATER_HOLD_MODE, mv_limit=HEATER_MV_LIMIT, mv_min=HEATER_MV_MIN,
            enter_tol_c=HEATER_HOLD_MV_ENTER_TOL_C, enter_sec=HEATER_HOLD_MV_ENTER_SEC,
            arrive_tol_c=HEATER_HOLD_MV_ARRIVE_TOL_C, drift_pv_c=HEATER_HOLD_MV_DRIFT_PV_C,
            drift_mv=HEATER_HOLD_MV_DRIFT_MV, tc2_margin_c=HEATER_HOLD_TC2_MARGIN_C,
            tc2_drift_c=HEATER_HOLD_TC2_DRIFT_C, parent=self)
        self.heater_hold.request_mv_limit.connect(self.request_heater_mv_limit)
        self.heater_hold.request_sv2.connect(self.request_heater_sv2)
        self.heater_hold.request_pv_sel.connect(self.request_heater_pv_sel)
        self.heater_hold.message.connect(log_message_to_monitor)
        self.heater_hold.engaged.connect(self._on_heater_hold_engaged)
        self.heater_hold.give_up.connect(lambda why: self._refresh_hold_snapshot())
        self.heater_hold.alert.connect(self._on_heater_hold_alert)
        # process_controller 는 main 속성을 직접 읽지 않는다 — 스냅샷 dict 를 돌려주는 콜러블을 주입한다
        self._hold_snapshot: dict = {}
        self._refresh_hold_snapshot()
        self.process_controller.set_hold_state_provider(lambda: dict(self._hold_snapshot))
        self.process_controller.request_hold_force.connect(self._on_hold_force)
        self.process_controller.heater_hold_failed.connect(self._on_heater_hold_failed)
        # DAC 포화 감시 — 유지 모드와 독립된 안전망(HEATER_HOLD_MODE 가 off 여도 돈다)
        self.heater_sat = HeaterSaturationGuard(mv_limit=HEATER_MV_LIMIT, mv_min=HEATER_MV_MIN,
                                                sat_sec=HEATER_MV_SAT_SEC, parent=self)
        self.heater_sat.request_mv_limit.connect(self.request_heater_mv_limit)
        self.heater_sat.message.connect(log_message_to_monitor)
        self.heater_sat.saturated.connect(self._on_heater_saturated)
        # 히터 시작 카드의 운전 시간 계산용(상승 엣지 시각)
        self._heater_chat_t0 = 0.0
        # 가스·압력 준비가 끝나면 무엇을 이어서 할지. ("manual_on", 목표온도) 또는
        #  ("recipe_start", None). 준비 중이 아니면 None.
        self._heater_pending = None
        # 가스 준비 진행도("3/9"). 상태 문구에 붙인다.
        self._atm_progress = ""
        # 히터가 꺼진 뒤에도 식을 때까지 가스를 물고 있는 중인가
        self._atm_hold = False
        # 이번 공정이 히터를 '소유'하는가 (use_heater 且 heater_temp>0).
        #  히터를 제어하는 주체는 한 번에 하나여야 한다 — 공정이 소유하면
        #  히터 레시피를 못 띄우고, 소유하지 않으면 둘이 함께 돌 수 있다.
        self._process_heater_claimed = False
        # 공정 레시피가 HEATER_RAMP 로 지정한 램프 속도(°C/min).
        #  HEATER_SET 이 왔을 때 감속 접근 램프에 그대로 넘긴다.
        self._process_heater_rate_c = 0.0
        # 레시피 진행 표시는 1초 주기. PLC 폴링(200ms)에 얹지 않는다.
        self._heater_ui_timer = QTimer(self)
        self._heater_ui_timer.setInterval(1000)
        self._heater_ui_timer.timeout.connect(self._refresh_heater_progress)
        self._heater_ui_timer.start()

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

        # 종료/파일선택 다이얼로그 상태
        self._is_closing: bool = False
        self._csv_dialog_open: bool = False

        # --- 모든 스레드 시작 ---
        self.plc_thread.start()
        self.mfc_thread.start()
        self.dcpower_thread.start()
        self.rfpower_thread.start()
        self.rfpulse_thread.start()
        self.process_thread.start()

        # RF Pulse 포트는 스레드가 뜬 뒤 그 스레드 안에서 연다(타이머/시리얼 소유권).
        #  하드웨어가 없어도 예외를 올리지 않고 경고만 남긴다.
        QMetaObject.invokeMethod(
            self.rfpulse_controller, "connect_device", Qt.ConnectionType.QueuedConnection)

        self.process_running = False
        self.ui.Sputter_Stop_Button.setEnabled(False)

        # === ERP 원격 명령 실행 (메인 스레드 전용) ===
        # 안전: 아래 화이트리스트에 없는 명령은 실행하지 않는다.
        #       PLC 버튼은 setChecked로 처리해 로컬 UI 상태와 항상 일치시킨다.
        _PLC_BTNS = {
            "Rotary_button", "RV_button", "FV_button", "MV_button", "Vent_button",
            "Turbo_button", "Ar_Button", "O2_Button", "MS_button",
            "S1_button", "S2_button", "BuzzStop_Button",
            "Door_Button",  # 도어는 상승/하강이 이 버튼 하나로 통합되어 있다
            "ION_button",   # 이오나이저 Remote On
        }

        def _erp_exec_one(c: dict):
            name = str(c.get("command", ""))
            args = c.get("args") or {}
            if name in _PLC_BTNS:
                btn = getattr(self.ui, name, None)
                if btn is None:
                    raise RuntimeError(f"버튼 없음: {name}")
                btn.setChecked(bool(args.get("on")))
            elif name == "PROCESS_START":
                def _set_text(widget_name: str, value):
                    w = getattr(self.ui, widget_name, None)
                    if w is None or value is None:
                        return
                    s = str(value)
                    if hasattr(w, "setPlainText"):
                        w.setPlainText(s)
                    elif hasattr(w, "setText"):
                        w.setText(s)

                def _set_check(widget_name: str, value):
                    w = getattr(self.ui, widget_name, None)
                    if w is not None and value is not None:
                        w.setChecked(bool(value))

                if args:
                    _set_check("G1_checkbox", args.get("useG1"))
                    _set_text("G1_edit", args.get("g1"))
                    _set_check("G2_checkbox", args.get("useG2"))
                    _set_text("G2_edit", args.get("g2"))
                    _set_check("Ar_gas_radio", args.get("useAr"))
                    _set_text("Ar_flow_edit", args.get("arFlow"))
                    _set_check("O2_gas_radio", args.get("useO2"))
                    _set_text("O2_flow_edit", args.get("o2Flow"))
                    _set_text("working_pressure_edit", args.get("workingPressure"))
                    _set_check("rf_power_checkbox", args.get("useRf"))
                    _set_text("RF_power_edit", args.get("rfPower"))
                    # RF Pulse — 전용 칸을 쓴다. 웹 폼이 안 보내면 키가 없어 미사용.
                    #  RF power 와 독립이므로 offset/param 을 건드리지 않는다.
                    _set_check("rf_pulse_checkbox", args.get("useRfPulse"))
                    _set_text("rfp_power_edit", args.get("rfPulsePower"))
                    _set_text("rfp_freq_edit", args.get("rfPulseFreq"))
                    _set_text("rfp_duty_edit", args.get("rfPulseDuty"))
                    _set_check("dc_power_checkbox", args.get("useDc"))
                    _set_text("DC_power_edit", args.get("dcPower"))
                    _set_check("dc_delay_checkbox", args.get("dcDelay"))
                    _set_text("Shutter_delay_edit", args.get("shutterDelay"))
                    _set_text("process_time_edit", args.get("processTime"))
                    _set_text("offset_edit", args.get("offset"))
                    _set_text("param_edit", args.get("param"))

                self._handle_start_process()
            elif name == "PROCESS_STOP":
                self._on_sputter_stop_clicked()
            elif name == "ALL_STOP":
                self._on_all_stop_clicked()
            elif name == "HEATER_SV":
                if HEATER_ENABLED and self.heater_recipe.is_running():
                    raise RuntimeError("히터 레시피 실행 중입니다. 레시피를 먼저 중단하세요.")
                val = args.get("value")
                if val is None:
                    raise RuntimeError("목표 온도 없음")
                self.ui.heater_sv_edit.setPlainText(str(val)) \
                    if hasattr(self.ui.heater_sv_edit, "setPlainText") \
                    else self.ui.heater_sv_edit.setText(str(val))
                self._on_heater_apply_clicked()
            elif name == "HEATER_ONOFF":
                if HEATER_ENABLED and self.heater_recipe.is_running():
                    raise RuntimeError("히터 레시피 실행 중입니다. 레시피를 먼저 중단하세요.")
                want = bool(args.get("on"))
                # 이미 원하는 상태인지는 버튼이 아니라 PLC(마지막 폴링)로 판단한다.
                # 가스 준비 중(_heater_pending)도 'ON 진행 중' 으로 본다: ON 은 거부, OFF 는 취소 경로.
                run = bool((self.plc_controller.get_heater_status() or {}).get("run"))
                pending = self._heater_pending is not None
                if want and pending:
                    raise RuntimeError("히터가 이미 준비 중입니다(가스·압력 대기)")
                if (run or pending) == want:
                    raise RuntimeError(f"히터가 이미 {'ON' if want else 'OFF'} 상태입니다")
                if want:
                    # 목표 온도가 함께 왔으면 먼저 반영한다(빈 SV로 인한 팝업 방지)
                    val = args.get("value")
                    if val is not None and str(val).strip() != "":
                        w = self.ui.heater_sv_edit
                        if hasattr(w, "setPlainText"):
                            w.setPlainText(str(val))
                        else:
                            w.setText(str(val))
                    # 가스·압력 입력이 함께 왔으면 히터 패널에 반영한다.
                    # 키가 없으면 장비 패널의 현재 설정을 그대로 쓴다.
                    def _set_chk(wn, v):
                        w_ = getattr(self.ui, wn, None)
                        if w_ is not None and v is not None:
                            w_.setChecked(bool(v))

                    def _set_txt(wn, v):
                        w_ = getattr(self.ui, wn, None)
                        if w_ is not None and v is not None:
                            w_.setText(str(v))

                    if "useAr" in args:
                        _set_chk("heater_ar_check", args.get("useAr"))
                        _set_txt("heater_ar_flow_edit", args.get("arFlow"))
                    if "useO2" in args:
                        _set_chk("heater_o2_check", args.get("useO2"))
                        _set_txt("heater_o2_flow_edit", args.get("o2Flow"))
                    if "wp" in args:
                        _set_txt("heater_wp_edit", args.get("wp"))
                    try:
                        self._sync_heater_gas_inputs()
                    except Exception:
                        pass
                    # 사전 검증 — 실패하면 팝업 대신 예외로 웹에 사유를 보고한다
                    sv_txt = ""
                    try:
                        sv_w = self.ui.heater_sv_edit
                        sv_txt = (sv_w.toPlainText() if hasattr(sv_w, "toPlainText")
                                  else sv_w.text()).strip()
                    except Exception:
                        pass
                    if sv_txt == "":
                        raise RuntimeError("히터 목표 온도가 설정되지 않았습니다")
                    try:
                        float(sv_txt)
                    except ValueError:
                        raise RuntimeError(f"히터 목표 온도가 숫자가 아닙니다: {sv_txt}")
                    st = self.plc_controller.get_heater_status() or {}
                    if not st.get("itl"):
                        raise RuntimeError("히터 인터락 미충족 (TC/DAC 모듈 상태 확인 필요)")

                # 버튼을 눌러 흉내내지 않고(P3) 핸들러를 직접 부른다 — 표시는 폴링이 맞춘다
                self._on_heater_onoff_toggled(want)

            elif name == "RECIPE_PROCESS_RUN":
                # 웹에서 만든 공정 레시피를 CSV로 저장하고 기존 CSV 실행 경로를 그대로 사용한다
                import csv as _csv, tempfile, os as _os
                rows = args.get("rows") or []
                if not rows:
                    raise RuntimeError("레시피 행이 없습니다")
                cols = ["Process_name", "Ar", "Ar_flow", "O2", "O2_flow",
                        "working_pressure", "process_time", "shutter_delay",
                        "use_rf_power", "rf_power", "use_dc_power", "dc_power",
                        "use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty",
                        "use_dc_delay", "use_heater", "heater_temp", "heater_ramp",
                        "gun1", "gun2", "G1 Target", "G2 Target"]
                d = _os.path.join(tempfile.gettempdir(), "vanam_recipe")
                _os.makedirs(d, exist_ok=True)
                path = _os.path.join(d, "process_web.csv")
                with open(path, "w", encoding="utf-8-sig", newline="") as f:
                    w = _csv.DictWriter(f, fieldnames=cols)
                    w.writeheader()
                    for r in rows:
                        w.writerow({c: r.get(c, "") for c in cols})
                self._start_csv_process_from_path(path)

            elif name == "RECIPE_HEATER_RUN":
                # UI 경로(_on_heater_recipe_clicked)와 같은 가드.
                #  HeaterRecipeRunner.start() 는 공정이 도는지 알지 못하므로
                #  여기서 막지 않으면 공정과 레시피가 같은 D00012/M00040 을
                #  서로 덮어쓴다.
                _proc_active = (self.process_running or self.csv_mode
                                or getattr(self, "_csv_delay_active", False))
                # CSV 리스트 공정은 뒤 STEP 에서 히터를 켤 수 있으므로 목록 전체를 본다
                _list_owns = (bool(getattr(self, "csv_file_path", ""))
                              and self._csv_list_uses_heater())
                if _proc_active and (self._process_heater_claimed or _list_owns):
                    _tail = (" (공정 목록의 뒤 STEP 에 히터 목표값이 있습니다)"
                             if (_list_owns and not self._process_heater_claimed) else "")
                    raise RuntimeError("현재 공정이 히터를 제어하고 있습니다. "
                                       "공정 레시피에 히터 목표값이 없을 때만 함께 실행할 수 있습니다."
                                       + _tail)
                import csv as _csv, tempfile, os as _os
                rows = args.get("rows") or []
                if not rows:
                    raise RuntimeError("레시피 행이 없습니다")
                cols = ["step", "target_c", "ramp_c_per_min", "ramp_min",
                        "soak_min", "repeat",
                        "use_ar", "ar_flow", "use_o2", "o2_flow", "wp_mtorr"]
                d = _os.path.join(tempfile.gettempdir(), "vanam_recipe")
                _os.makedirs(d, exist_ok=True)
                path = _os.path.join(d, "heater_web.csv")
                with open(path, "w", encoding="utf-8-sig", newline="") as f:
                    w = _csv.DictWriter(f, fieldnames=cols)
                    w.writeheader()
                    for r in rows:
                        w.writerow({c: r.get(c, "") for c in cols})
                if not self.heater_recipe.load(path):
                    raise RuntimeError("히터 레시피 검증 실패 (로그 확인)")
                if not self.heater_recipe.start():
                    raise RuntimeError("히터 레시피 시작 실패")

            elif name == "RECIPE_PROCESS_START":
                # 적재된 CSV 레시피로 공정을 시작한다(장비 앞 Start 버튼과 동일 경로)
                if not getattr(self, "csv_rows", None):
                    raise RuntimeError("적재된 레시피가 없습니다. 먼저 레시피를 적재하세요.")
                self._handle_start_process()

            elif name == "HEATER_RESET":
                # PLC 래치된 히터 이상(M00043) 해제. 확인은 웹이 이미 받았다.
                st = self.plc_controller.get_heater_status() or {}
                if not st.get("fault"):
                    raise RuntimeError("히터 이상 상태가 아닙니다")
                log_message_to_monitor("히터", "[원격] 히터 이상 리셋 요청")
                self.request_heater_reset.emit()

            elif name == "HEATER_GAS_RELEASE":
                # 냉각 대기 중 유지되는 가스·압력을 지금 해제한다.
                if not (HEATER_ENABLED and self.heater_atmosphere.is_active()):
                    raise RuntimeError("유지 중인 가스·압력이 없습니다")
                self._atm_hold = False
                log_message_to_monitor("히터", "[원격] 가스·압력 해제")
                self.heater_atmosphere.release("원격 해제")

            elif name == "RECIPE_HEATER_STOP":
                self.heater_recipe.stop("원격 중단")

            elif name == "HEATER_RECIPE_HOLD":
                # args: on=true → 일시정지, on=false → 재개
                if not self.heater_recipe.is_running():
                    raise RuntimeError("실행 중인 히터 레시피가 없습니다")
                if bool(args.get("on", True)):
                    if not self.heater_recipe.hold():
                        raise RuntimeError("일시정지에 실패했습니다")
                else:
                    if not self.heater_recipe.resume():
                        raise RuntimeError("일시정지 상태가 아닙니다")

            elif name == "HEATER_RECIPE_STEP":
                if not self.heater_recipe.is_running():
                    raise RuntimeError("실행 중인 히터 레시피가 없습니다")
                if not self.heater_recipe.skip_step():
                    raise RuntimeError("스텝 건너뛰기에 실패했습니다")

            else:
                raise RuntimeError(f"허용되지 않은 명령: {name}")

        def _erp_drain_commands():
            try:
                # ERP 는 장비당 한 인스턴스만 받는다. 다른 챔버K 가 이미 붙어 있으면
                #  리포터가 409 를 받고 보고를 잠시 멈춘 뒤 60초마다 hello 로 재연결을
                #  시도한다 — 사용자에게 멈춤/재개를 각 1회만 알린다.
                if getattr(self.erp, "rejected", False) and not getattr(self, "_erp_rejected_shown", False):
                    self._erp_rejected_shown = True
                    log_message_to_monitor(
                        "WARN",
                        "[ERP] 다른 챔버K 프로그램이 ERP 에 연결되어 있어 보고를 잠시 멈춥니다. "
                        "60초마다 재연결을 시도합니다.")
                elif not getattr(self.erp, "rejected", False) and getattr(self, "_erp_rejected_shown", False):
                    self._erp_rejected_shown = False
                    log_message_to_monitor("정보", "[ERP] 보고를 재개했습니다.")

                cmds = self.erp.pop_commands()
                if not cmds:
                    return
                # 장비에 모달 대화상자가 떠 있으면 조작이 막히므로 원인을 보고하고 중단한다
                if QApplication.activeModalWidget() is not None:
                    for c in cmds:
                        self.erp.cmd_result(
                            c.get("id"), False,
                            "장비에 확인 대화상자가 열려 있습니다. 현장에서 닫아주세요.")
                    log_message_to_monitor(
                        "WARN", "[원격] 대화상자가 열려 있어 명령을 거부했습니다")
                    return
                for c in cmds:
                    cid = c.get("id")
                    try:
                        _erp_exec_one(c)
                        log_message_to_monitor(
                            "정보", f"[원격] {c.get('command')} 실행")
                        self.erp.cmd_result(cid, True)
                    except Exception as ex:
                        log_message_to_monitor(
                            "ERROR", f"[원격] {c.get('command')} 실패: {ex}")
                        self.erp.cmd_result(cid, False, str(ex))
            except Exception:
                pass

        self._erp_cmd_timer = QTimer(self)
        self._erp_cmd_timer.timeout.connect(_erp_drain_commands)
        self._erp_cmd_timer.start(500)

        # === ERP 상태 스냅샷 (1초) ===
        def _erp_snapshot():
            try:
                def _w(name: str) -> str:
                    """UI 위젯 텍스트를 이름으로 안전하게 읽는다.
                    위젯이 없거나(주석 처리 등) 타입이 달라도 예외를 내지 않는다."""
                    w = getattr(self.ui, name, None)
                    if w is None:
                        return ""
                    for meth in ("toPlainText", "text"):
                        f = getattr(w, meth, None)
                        if callable(f):
                            try:
                                return str(f()).strip()
                            except Exception:
                                pass
                    return ""

                def _checked(name: str) -> bool:
                    w = getattr(self.ui, name, None)
                    try:
                        return bool(w.isChecked()) if w is not None else False
                    except Exception:
                        return False

                running = bool(getattr(self, "process_running", False))

                _stage_all = _w("stage_monitor")
                stage = _stage_all.splitlines()[-1] if _stage_all else ""

                # MFC 실측값 (update_mfc_*_display 가 채운다).
                #  MFC 폴링은 공정 중에만 돌기 때문에, 공정 밖에서는 마지막에
                #  읽은 죽은 값이 남는다. 그래서 공정 중일 때만 실측으로 내보낸다.
                #  (setpoint 는 UI 설정값이라 공정과 무관하게 항상 유효하다)
                _meas = (getattr(self, "_erp_meas", {}) or {}) if running else {}
                _flow = _meas.get("flow") or {}
                _press = _meas.get("pressure")

                # 공정 중이 아니면 계측값(PV)은 신뢰할 수 없다.
                # 위젯에는 이전 공정의 마지막 값이 남아 있으므로 대기 중에는 비운다.
                def _pv(name: str):
                    return _w(name) if running else ""

                # 계측 그룹 — value=계측값(PV), setpoint=설정값(SV)
                groups = [
                    {"label": "전원", "items": [
                        {"label": "DC Power", "value": _pv("Power_edit"),
                         "setpoint": _w("DC_power_edit"), "unit": "W"},
                        {"label": "Voltage", "value": _pv("Voltage_edit"), "unit": "V"},
                        {"label": "Current", "value": _pv("Current_edit"), "unit": "A"},
                    ]},
                    {"label": "RF", "items": [
                        {"label": "for.P", "value": _pv("for_p_edit"),
                         "setpoint": _w("RF_power_edit"), "unit": "W"},
                        {"label": "ref.P", "value": _pv("ref_p_edit"), "unit": "W"},
                        {"label": "offset / param",
                         "value": f'{_w("offset_edit")} / {_w("param_edit")}'},
                    ]},
                    # RF Pulse 는 별개 장비라 그룹을 따로 둔다(RF 와 나란히 비교)
                    {"label": "RF Pulse", "items": [
                        {"label": "for.P", "value": _pv("rfp_for_p_edit"),
                         "setpoint": _w("rfp_power_edit"), "unit": "W"},
                        {"label": "ref.P", "value": _pv("rfp_ref_p_edit"), "unit": "W"},
                        {"label": "Freq", "value": _w("rfp_freq_edit"), "unit": "kHz"},
                        {"label": "Duty", "value": _w("rfp_duty_edit"), "unit": "%"},
                    ]},
                    {"label": "가스", "items": [
                        {"label": "Ar", "value": _flow.get("Ar"),
                         "setpoint": _w("Ar_flow_edit"), "unit": "sccm"},
                        {"label": "O\u2082", "value": _flow.get("O2"),
                         "setpoint": _w("O2_flow_edit"), "unit": "sccm"},
                    ]},
                    {"label": "압력 · 시간", "items": [
                        {"label": "챔버 압력", "value": _press, "unit": "mTorr"},
                        {"label": "Working P", "setpoint": _w("working_pressure_edit"),
                         "unit": "mTorr"},
                        {"label": "공정 시간", "setpoint": _w("process_time_edit"), "unit": "분"},
                        {"label": "셔터 딜레이", "setpoint": _w("Shutter_delay_edit"),
                         "unit": "분"},
                    ]},
                ]

                # 타겟(체크된 건만)
                tg = []
                if _checked("G1_checkbox"):
                    tg.append({"label": "G1", "value": _w("G1_edit") or "미입력"})
                if _checked("G2_checkbox"):
                    tg.append({"label": "G2", "value": _w("G2_edit") or "미입력"})
                if tg:
                    groups.append({"label": "타겟", "items": tg})

                state = {
                    "status": "running" if running else "idle",
                    "stage": stage,
                    "groups": groups,
                    "heater": {
                        "pv": _w("heater_pv_edit"),
                        "sv": _w("heater_sv_edit"),
                        "status": _w("heater_status_label"),
                        "output": self._heater_output_text(),
                        "atmosphere": self._heater_atmosphere_snapshot(),
                        "on": _checked("heater_onoff_button"),
                        "curSv": (getattr(self, "_erp_heater", {}) or {}).get("cur_sv"),
                        "pidErr": (getattr(self, "_erp_heater", {}) or {}).get("pid_err"),
                        "otLimit": (getattr(self, "_erp_heater", {}) or {}).get("ot_limit"),
                        "run": bool((getattr(self, "_erp_heater", {}) or {}).get("run")),
                        "fault": bool((getattr(self, "_erp_heater", {}) or {}).get("fault")),
                        "tcErr": bool((getattr(self, "_erp_heater", {}) or {}).get("tc_err")),
                        "wdErr": bool((getattr(self, "_erp_heater", {}) or {}).get("wd_err")),
                        "ot": bool((getattr(self, "_erp_heater", {}) or {}).get("ot")),
                        "recipeRunning": bool(
                            getattr(getattr(self, "heater_recipe", None), "is_running", lambda: False)()
                        ),
                    },
                    "heaterRecipe": (
                        self.heater_recipe.progress()
                        if hasattr(getattr(self, "heater_recipe", None), "progress")
                        else None
                    ),
                    "csvRecipe": (
                        {
                            "name": _os.path.basename(str(getattr(self, "csv_file_path", "") or "")) or None,
                            "stepNo": int(getattr(self, "csv_index", -1)) + 1,
                            "total": len(getattr(self, "csv_rows", []) or []),
                            "active": bool(getattr(self, "csv_mode", False)),
                            "steps": [
                                str((r or {}).get("Process_name") or f"STEP{i+1}")
                                for i, r in enumerate(getattr(self, "csv_rows", []) or [])
                            ],
                            # 웹 상세 표시용 스텝 파라미터(문자열 그대로, 검증은 장비가 한다)
                            "rows": [
                                {k: str((r or {}).get(k, "") or "") for k in (
                                    "Process_name", "Ar", "Ar_flow", "O2", "O2_flow",
                                    "working_pressure", "process_time", "shutter_delay",
                                    "use_rf_power", "rf_power", "use_dc_power", "dc_power",
                                    "use_rf_pulse", "rf_pulse_power",
                                    "rf_pulse_freq", "rf_pulse_duty",
                                    "use_heater", "heater_temp", "heater_ramp",
                                    "gun1", "gun2", "G1 Target", "G2 Target",
                                )}
                                for r in (getattr(self, "csv_rows", []) or [])
                            ],
                        }
                        if (getattr(self, "csv_rows", None) or None)
                        else None
                    ),
                    "ion": {
                        "run": bool(getattr(self, "_erp_indicators", {}).get("ION_RUN")),
                        "lamp": bool(getattr(self, "_erp_indicators", {}).get("ION_LAMP")),
                        "overtime": bool(getattr(self, "_erp_indicators", {}).get("ION_OT")),
                    },
                    "indicators": dict(getattr(self, "_erp_indicators", {})),
                    "mvInterlock": (getattr(self, "_plc_bits", {}) or {}).get("MV_INTERLOCK"),
                    "valves": dict(getattr(self, "_erp_valves", {})),
                    # 링크 다운 중 indicators/valves 는 마지막 값 그대로다(거짓 OFF 보고 금지) — 이 플래그로 구분
                    "plc_link": bool(getattr(self, "_plc_link_up", False)),
                }

                # 공정 진행 정보 — 계산은 장비가 하고 웹은 표시만 한다.
                #  remainSec: 메인 공정 잔여 초 (process_time_tick 원본). -1 = 아직 메인 공정 전
                #  totalSec : 메인 공정 총 초
                #  phase    : main = 메인 공정 진행 중, pre = 준비 단계(승온·안정화·셔터딜레이)
                if running:
                    _remain = int(getattr(self, "_erp_main_remain_sec", -1))
                    _total = int(getattr(self, "_erp_main_total_sec", 0))
                    state["process"] = {
                        "name": getattr(self, "current_process_name", "") or "",
                        "remainSec": _remain,
                        "totalSec": _total,
                        "phase": "main" if _remain >= 0 else "pre",
                    }

                self.erp.update_state(state)
            except Exception as e:
                # 조용한 실패 방지: 원인을 웹 이벤트로 1회만 보고한다.
                try:
                    if not getattr(self, "_erp_snap_err", False):
                        self._erp_snap_err = True
                        self.erp.event("error", f"스냅샷 수집 실패: {type(e).__name__}: {e}")
                except Exception:
                    pass


        self._erp_snap_timer = QTimer(self)
        self._erp_snap_timer.timeout.connect(_erp_snapshot)
        self._erp_snap_timer.start(1000)

    def _invoke_worker_blocking(self, worker, method_name: str) -> None:
        """
        worker가 속한 스레드에서 method_name 슬롯을 동기 실행한다.
        main.py에서 worker QObject를 직접 건드리지 않기 위한 헬퍼.
        """
        t0 = time.monotonic()
        try:
            if worker.thread() is QThread.currentThread():
                getattr(worker, method_name)()
                return

            ok = QMetaObject.invokeMethod(
                worker,
                method_name,
                Qt.ConnectionType.BlockingQueuedConnection
            )
            if not ok:
                raise RuntimeError(f"invokeMethod 실패: {method_name}")
        except Exception as e:
            log_message_to_monitor("경고", f"{type(worker).__name__}.{method_name} 실행 실패: {e}")
        finally:
            # 워커 슬롯은 2초 안에 돌아와야 한다(P4). 넘으면 다음 사고의 증거로 남긴다.
            took = time.monotonic() - t0
            if took > 2.5:
                log_message_to_monitor("경고", f"{type(worker).__name__}.{method_name} {took:.1f}초 소요")

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
        self.ui.ALL_STOP_button.clicked.connect(self._on_all_stop_clicked)
        # ✅ STOP 버튼 전용 핸들러에서 CSV 전체 취소 여부를 먼저 표시
        self.ui.Sputter_Stop_Button.clicked.connect(self._on_sputter_stop_clicked)

        # PLC 버튼 연결
        for btn_name in PLC_COIL_MAP.keys():
            button = getattr(self.ui, btn_name, None)
            if button:
                button.toggled.connect(partial(self.request_plc_port_update.emit, btn_name))
        self.ui.Door_Button.toggled.connect(self._on_ui_door_toggled)

        # --- 3. 컨트롤러 간 상호작용 연결 ---
        # MainDialog -> ProcessController 시작 요청
        self.request_process_start.connect(self.process_controller.start_process_flow)

        # ProcessController -> 각 장치 컨트롤러로 명령 전달
        self.process_controller.update_plc_port.connect(self.plc_controller.update_port_state)
        self.process_controller.start_dc_power.connect(self.dcpower_controller.start_process)
        self.process_controller.stop_dc_power.connect(self.dcpower_controller.stop_process)
        self.process_controller.start_rf_power.connect(self.rfpower_controller.start_process)
        self.process_controller.stop_rf_power.connect(self.rfpower_controller.stop_process)

        # --- RF Pulse (CESAR) ---
        self.process_controller.start_rf_pulse.connect(self.rfpulse_controller.start_process)
        self.process_controller.stop_rf_pulse.connect(self.rfpulse_controller.stop_process)
        self.rfpulse_controller.update_rfpulse_status_display.connect(
            self.update_rfpulse_status_display)
        self.rfpulse_controller.status_message.connect(self.on_status_message)
        self.rfpulse_controller.pulse_config_readback.connect(
            self._on_rfpulse_config_readback)
        self.rfpulse_controller.pulse_config_warning.connect(self._on_rfpulse_config_warning)

        # RF power / RF Pulse / DC power 는 서로 독립이고 동시에 쓸 수 있다.
        #  체크박스는 자기 입력칸의 활성/비활성만 담당한다.
        self.ui.rf_pulse_checkbox.toggled.connect(self._on_rf_pulse_checkbox_toggled)
        self._sync_rfpulse_inputs()     # 기동 시 초기 상태(미체크 = 회색 잠금)
        
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
        self.request_process_stop.connect(self.process_controller.stop_process)
        self.request_plc_port_update.connect(self.plc_controller.update_port_state)
        self.request_plc_emergency_stop.connect(self.plc_controller.on_emergency_stop)
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
        self.plc_controller.plc_link.connect(self._on_plc_link)
        self.plc_controller.plc_bit_changed.connect(self._on_plc_bit_changed)
        self._plc_bits: dict = {}                 # 이름별 마지막 값(로그·ERP·안전 판정용)
        self._erp_main_remain_sec = -1            # ERP: 메인 공정 잔여 초(-1 = 미진입), 총 초
        self._erp_main_total_sec = 0
        self._mv_itl_timer = QTimer(self)         # MV_INTERLOCK OFF 1초 지속 판정
        self._mv_itl_timer.setSingleShot(True)
        self._mv_itl_timer.setInterval(int(self.MV_INTERLOCK_ABORT_MS))
        self._mv_itl_timer.timeout.connect(self._on_mv_interlock_timeout)
        self._mv_itl_off_t = 0.0
        # 첫 plc_link(True) 전까지는 PLC 조작 위젯을 다운 상태로 둔다
        self._plc_link_up = True
        self._on_plc_link(False, initial=True)
        self.mfc_controller.update_flow.connect(self.update_mfc_flow_display)
        self.mfc_controller.update_pressure.connect(self.update_mfc_pressure_display)
        self.dcpower_controller.update_dc_status_display.connect(self.update_dc_status_display)
        self.rfpower_controller.update_rf_status_display.connect(self.update_rf_status_display)
        self.mfc_controller.flow_alert.connect(self._on_mfc_flow_alert)        
        self.mfc_controller.pressure_alert.connect(self._on_mfc_pressure_alert) 

        # --- 5. 모든 로그 메시지를 UI 모니터에 연결 ---
        self.plc_controller.status_message.connect(self.on_status_message)
        self.plc_controller.plc_disconnected.connect(self._on_plc_disconnected)
        self.plc_controller.plc_reconnected.connect(self._on_plc_reconnected)
        # ── 2026-09-16 PLC CPU 정지 사고 대응: 이벤트 기록·블랙박스·재기동 감지·복구 후 안전 상태 ──
        #    (PLC 스레드는 파일을 쓰지 않는다 — 여기서 main 스레드가 lib.logger 로 기록한다)
        self.plc_controller.plc_event.connect(self._on_plc_event)
        self.plc_controller.plc_blackbox.connect(self._on_plc_blackbox)
        self.plc_controller.plc_restarted.connect(self._on_plc_restarted)
        self.plc_controller.plc_recovered.connect(self._on_plc_recovered)
        # 시리얼 장비 공통 통신 두절 정책 — DC / RF 펄스 / MFC 이벤트 기록 + 복구 후 안전 상태
        self.dcpower_controller.comm_event.connect(lambda r: self._on_comm_event("DC", r))
        self.dcpower_controller.dc_recovered.connect(self._on_dc_recovered)
        self.rfpulse_controller.comm_event.connect(lambda r: self._on_comm_event("RFPulse", r))
        self.rfpulse_controller.rfpulse_recovered.connect(self._on_rfpulse_recovered)
        self.mfc_controller.mfc_comm_event.connect(lambda r: self._on_comm_event("MFC", r))
        # 장기두절(10분) 알림 — 네 장치가 같은 정책(lib/comm_policy)으로 두절당 1회만 낸다
        for _dev in (self.plc_controller, self.mfc_controller, self.dcpower_controller, self.rfpulse_controller):
            _dev.comm_long_outage.connect(self._on_comm_long_outage)
        self.mfc_controller.status_message.connect(self.on_status_message)
        self.dcpower_controller.status_message.connect(self.on_status_message)
        self.rfpower_controller.status_message.connect(self.on_status_message)
        self.process_controller.status_message.connect(self.on_status_message)
        
        self.ui.select_csv_button.clicked.connect(self._on_select_csv_clicked)

        # --- 6. 히터 ---
        if HEATER_ENABLED:
            # (1) UI -> PLC : 사용자가 직접 조작하는 경로 (수동 제어)
            #     PLC 내장 PID가 실제 온도 제어를 담당하므로
            #     파이썬은 '목표 온도'와 '운전 요구'만 전달한다.
            self.request_heater_target.connect(self.plc_controller.set_heater_target)
            self.request_heater_run.connect(self.plc_controller.set_heater_run)
            self.request_heater_mv_limit.connect(
                self.plc_controller.set_heater_mv_limit, Qt.ConnectionType.QueuedConnection)
            self.request_heater_sv2.connect(
                self.plc_controller.set_heater_sv2, Qt.ConnectionType.QueuedConnection)
            self.request_heater_pv_sel.connect(
                self.plc_controller.set_heater_pv_sel, Qt.ConnectionType.QueuedConnection)
            self.request_heater_reset.connect(self.plc_controller.reset_heater_fault)

            # (2) PLC -> UI : 200ms 폴링으로 올라오는 히터 상태를 화면에 반영
            self.plc_controller.update_heater_status.connect(self.update_heater_display)
            self.plc_controller.heater_residual.connect(self._on_heater_residual)
            self.plc_controller.heater_fault.connect(self._on_heater_fault)

            # (3) UI 위젯 -> 핸들러
            self.ui.heater_apply_button.clicked.connect(self._on_heater_apply_clicked)
            self.ui.heater_onoff_button.toggled.connect(self._on_heater_onoff_toggled)

            # (3-1) 감속 접근 램프 — 수동/공정 경로가 쓴다.
            #   레시피 러너는 자기 안에 별도 프로파일러를 들고 자기 틱으로 돌린다.
            self.heater_ramp = RampProfiler(self.plc_controller, self, autotick=True)
            self.heater_ramp.request_target.connect(
                self.plc_controller.set_heater_target)
            self.heater_ramp.request_ramp.connect(
                self.plc_controller.set_heater_ramp_rate)
            self.heater_ramp.status_message.connect(self.on_status_message)
            self.heater_ramp.finished.connect(
                lambda: log_message_to_monitor(
                    "히터", "[히터] 목표 도달 — 감속 접근 완료"))

            # (4) 히터 레시피 러너 (GUI 스레드 → PLC 스레드는 큐 연결)
            self.heater_recipe.status_message.connect(self.on_status_message)
            self.heater_recipe.request_target.connect(self.plc_controller.set_heater_target)
            self.heater_recipe.request_run.connect(self.plc_controller.set_heater_run)
            self.heater_recipe.request_ramp.connect(self.plc_controller.set_heater_ramp_rate)

            # 공정 레시피(HEATER_RAMP 액션) → PLC 램프 속도
            self.process_controller.set_heater_ramp.connect(
                self.plc_controller.set_heater_ramp_rate)
            self.heater_recipe.step_changed.connect(self._on_heater_recipe_step)
            self.heater_recipe.finished.connect(self._on_heater_recipe_finished)
            self.ui.heater_recipe_button.clicked.connect(self._on_heater_recipe_clicked)
            self.ui.heater_reset_button.clicked.connect(self._on_heater_reset_clicked)
            self.ui.heater_gas_release_button.clicked.connect(
                self._on_heater_gas_release_clicked)
            self.ui.heater_hold_button.clicked.connect(self._on_heater_hold_clicked)
            self.ui.heater_skip_button.clicked.connect(self._on_heater_skip_clicked)
            self.ui.heater_stop_button.clicked.connect(self._on_heater_stop_clicked)

            # --- 히터 전용 가스·압력 ---
            #  공정 컨트롤러와 같은 MFC 를 쓴다. 동시에 못 쓰도록 서로 시작을 막는다.
            self.heater_atmosphere = HeaterAtmosphere(self.mfc_controller, self)
            self.heater_atmosphere.command_requested.connect(
                self.mfc_controller.handle_command)
            self.heater_atmosphere.update_plc_port.connect(
                self.plc_controller.update_port_state)
            self.mfc_controller.command_confirmed.connect(
                self.heater_atmosphere.on_mfc_confirmed)
            self.mfc_controller.command_failed.connect(
                self.heater_atmosphere.on_mfc_failed)
            self.heater_atmosphere.status_message.connect(self.on_status_message)
            self.heater_atmosphere.state_changed.connect(
                self._on_heater_atmosphere_state)
            self.heater_atmosphere.ready.connect(self._on_heater_atmosphere_ready)
            self.heater_atmosphere.failed.connect(self._on_heater_atmosphere_failed)
            self.heater_atmosphere.released.connect(self._on_heater_atmosphere_released)
            self.ui.heater_ar_check.toggled.connect(
                lambda _c: self._sync_heater_gas_inputs())
            self.ui.heater_o2_check.toggled.connect(
                lambda _c: self._sync_heater_gas_inputs())

            # (4) ★ ProcessController -> PLC : 레시피(CSV/단일 공정)에서
            #     HEATER_SET 스텝이 실행될 때 목표 온도와 운전을 PLC로 보낸다.
            #     이 연결이 없으면 레시피의 히터 스텝이 아무 동작도 하지 않는다.
            #   목표는 감속 접근 램프를 거친다(못 쓰면 그 안에서 직접 폴백한다).
            self.process_controller.set_heater_target.connect(
                self._on_process_heater_target)
            self.process_controller.set_heater_ramp_c.connect(
                self._on_process_heater_ramp_c)
            #   RUN 도 목표와 같이 main 큐를 거쳐 PLC 로 간다(_on_process_heater_run 안에서 emit)
            #   → 발행 순서 = 도착 순서(목표 → RUN)가 보장된다. PLC 직결은 두지 않는다.
            self.process_controller.set_heater_run.connect(self._on_process_heater_run)
            self.process_controller.heater_reached.connect(self._on_process_heater_reached)
        else:
            # 히터 비활성(config_user.json의 HEATER_ENABLED=false) 시
            # 조작 위젯을 잠가 오조작을 막는다. 표시용 위젯은 그대로 둔다.
            for w in ("heater_apply_button", "heater_onoff_button", "heater_sv_edit",
                      "heater_reset_button", "heater_gas_release_button",
                      "heater_ar_check",
                      "heater_ar_flow_edit", "heater_o2_check", "heater_o2_flow_edit",
                      "heater_wp_edit"):
                getattr(self.ui, w).setEnabled(False)

    # ==================== Google Chat 알림 헬퍼 (CH.K) ====================
    def _chat_reset_run_state(self):
        self._chat_user_stopped = False
        self._chat_emergency_stopped = False   # ALL STOP(비상 정지)으로 끝남 — 사용자 STOP 과 구분
        self._fault_abort_active = False       # _abort_process_by_fault 가 이미 시작됐다(중복 판정 방지)
        self._chat_hold_fail_key = None        # 유지 모드 실패 카드 중복 방지(공정당 같은 사유 1장)
        self._hold_force_result = None         # 마지막 폴백(force_*_hold) 결과 — 스냅샷으로 컨트롤러에 전달
        self._chat_hold_alert_sent = set()     # 유지 모드 알림 카드(강등/여유 없음) 공정당 종류별 1장
        self._finish_handled = False           # 새 공정(수동 / CSV STEP 마다) — 종료 처리 아직 안 함
        self._chat_errors = []
        self._chat_fail_notified = False
        self._chat_fault_detail_sent = False
        self._chat_fail_reason = ""
        self._erp_run_ended = False   # ERP: 이번 공정 run_end 미전송 상태로 초기화
        self._erp_meas = {}           # 이전 공정의 MFC 실측값을 물고 가지 않는다
        self._erp_main_remain_sec = -1    # -1 = 메인 공정 미진입
        self._erp_main_total_sec = 0

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

        try:
            rfp = float(p.get("rf_pulse_power", 0) or 0)
        except Exception:
            rfp = 0

        # 셋은 서로 독립이다 — 하나가 켜졌다고 다른 쪽을 끄지 않는다.
        p["use_dc_power"] = dc > 0
        p["use_rf_power"] = rf > 0
        p["use_rf_pulse"] = rfp > 0
        if dc <= 0:
            p.pop("dc_power", None)
        if rf <= 0:
            p.pop("rf_power", None)
        if rfp <= 0:
            # 펄스 미사용이면 관련 키를 통째로 뺀다(0W/None 표기 방지)
            p.pop("rf_pulse_power", None)
            p.pop("rf_pulse_freq", None)
            p.pop("rf_pulse_duty", None)
            p.pop("use_rf_pulse", None)
        else:
            if p.get("rf_pulse_freq") in (None, ""):
                p.pop("rf_pulse_freq", None)
            if p.get("rf_pulse_duty") in (None, ""):
                p.pop("rf_pulse_duty", None)

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
        # === ERP: 공정 종료 1회 보고 (chat_chk 유무와 무관하게 수행) ===
        try:
            if not getattr(self, "_erp_run_ended", True):
                self._erp_run_ended = True
                if bool(self._chat_user_stopped):
                    self.erp.run_end("aborted", "사용자 정지")
                elif ok:
                    self.erp.run_end("done")
                else:
                    _reason = (self._chat_fail_reason or "").strip()
                    if not _reason and self._chat_errors:
                        _reason = str(self._chat_errors[0])
                    self.erp.run_end("error", _reason)
        except Exception:
            pass

        try:
            self._erp_meas = {}   # 종료 후 대기 중에 이전 공정 값이 남지 않도록 비운다
            self._erp_main_remain_sec = -1    # -1 = 메인 공정 미진입
            self._erp_main_total_sec = 0
        except Exception:
            pass

        if not self.chat_chk:
            return

        name = self.current_process_name or "CHK"

        detail = {
            "process_name": name,
            "stopped": bool(self._chat_user_stopped),
            "aborting": bool(getattr(self, "_chat_emergency_stopped", False)),
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

    # ==================== PLC 연결 끊김/복구 알림 (CH.K) ====================
    @Slot(int)
    def _on_plc_disconnected(self, elapsed_s: int):
        """PLC가 60초 이상 끊겨 있을 때 1회 알림 (CH1&2와 동일 형식)."""
        if not self.chat_chk:
            return
        try:
            from lib.config import PLC_PORT
        except Exception:
            PLC_PORT = "PLC"
        try:
            self.chat_chk.notify_plc_link(
                ok=False,
                detail=f"[CHK] PLC 연결 끊김 {int(elapsed_s)}초 경과, 재연결 실패 ({PLC_PORT})",
            )
        except Exception:
            pass

    @Slot()
    def _on_plc_reconnected(self):
        """끊김 알림 후 재연결되면 1회 알림 (CH1&2와 동일 형식)."""
        if not self.chat_chk:
            return
        try:
            from lib.config import PLC_PORT
        except Exception:
            PLC_PORT = "PLC"
        try:
            self.chat_chk.notify_plc_link(
                ok=True,
                detail=f"[CHK] PLC 재연결 성공 ({PLC_PORT})",
            )
        except Exception:
            pass
    # ==================== PLC 이벤트 기록 / 블랙박스 / 재기동 / 복구 후 안전 상태 (2026-09-16 사고 대응) ====================
    def _plc_current_process_name(self) -> str:
        try:
            return (self.current_process_name or "").strip()
        except Exception:
            return ""

    @Slot(dict)
    def _on_plc_event(self, row: dict):
        """PLC 컨트롤러가 보낸 이벤트 1행을 COMM_events.csv(장비=PLC)에 남긴다(공정명은 여기서 채운다)."""
        self._on_comm_event("PLC", row)

    def _on_comm_event(self, device: str, row: dict):
        """시리얼 장비 공통 통신 이벤트 → COMM_events.csv (장비 스레드는 파일을 쓰지 않는다)."""
        try:
            r = dict(row or {})
            r["공정명"] = self._plc_current_process_name()
            append_comm_event(device, r)
        except Exception:
            pass

    @Slot(str, float)
    def _on_comm_long_outage(self, device: str, lost: float):
        """장치 통신 장기 두절(정책이 두절당 1회 보장) — 로그 + 이벤트 + 챗 텍스트 1줄."""
        mins = int(float(lost) // 60)
        msg = (f"{device} 통신 장기 두절 {mins}분 — {int(COMM_PROBE_MS) // 1000}초 간격으로만 재시도 중. "
               f"케이블/허브 확인 필요")
        log_message_to_monitor("경고", f"[{device}] {msg}")
        self._on_comm_event(device, {"종류": "장기두절", "상세": msg, "단절초": f"{float(lost):.1f}", "연속실패": ""})
        try:
            if self.chat_chk:
                self.chat_chk.notify_text(f"⚠️ CHK {msg}")
                self.chat_chk.flush()
        except Exception as e:
            log_message_to_monitor("경고", f"장기두절 챗 알림 실패({device}): {e!r}")

    def _process_active(self) -> bool:
        return (bool(getattr(self, "process_running", False))
                or bool(getattr(self, "csv_mode", False))
                or bool(getattr(self, "_csv_delay_active", False)))

    @Slot(float)
    def _on_dc_recovered(self, lost: float):
        """DC 파워 통신 복구. 이번 단절로 공정이 중단됐고 공정이 돌지 않으면 OUTP OFF 1회(확인형)."""
        dc = getattr(self, "dcpower_controller", None)
        if dc is None or not bool(getattr(dc, "_outage_abort", False)):
            return
        if self._process_active():
            log_message_to_monitor("정보", f"DC 파워 통신 복구({lost:.0f}초) — 공정 진행 중이라 안전 상태 재적용 생략")
            dc._outage_abort = False
            return
        QMetaObject.invokeMethod(dc, "safe_off", Qt.ConnectionType.QueuedConnection)
        msg = f"DC 파워 통신 복구({lost:.0f}초) → 안전 상태 재적용: OUTP OFF"
        log_message_to_monitor("정보", msg)
        try:
            if self.chat_chk:
                self.chat_chk.notify_text(f"🔁 CHK {msg}")
                self.chat_chk.flush()
        except Exception:
            pass

    @Slot(float)
    def _on_rfpulse_recovered(self, lost: float):
        """RF 펄스 통신 복구. 이번 단절로 공정이 중단됐고 공정이 돌지 않으면 확인형 RF OFF 1회."""
        rfp = getattr(self, "rfpulse_controller", None)
        if rfp is None or not bool(getattr(rfp, "_outage_abort", False)):
            return
        if self._process_active():
            log_message_to_monitor("정보", f"RF Pulse 통신 복구({lost:.0f}초) — 공정 진행 중이라 안전 상태 재적용 생략")
            rfp._outage_abort = False
            return
        QMetaObject.invokeMethod(rfp, "safe_off", Qt.ConnectionType.QueuedConnection)
        msg = f"RF Pulse 통신 복구({lost:.0f}초) → 안전 상태 재적용: RF OFF(확인형)"
        log_message_to_monitor("정보", msg)
        try:
            if self.chat_chk:
                self.chat_chk.notify_text(f"🔁 CHK {msg}")
                self.chat_chk.flush()
        except Exception:
            pass

    @Slot(list)
    def _on_plc_blackbox(self, rows: list):
        try:
            path = write_plc_blackbox(list(rows or []))
            if path is not None:
                log_message_to_monitor("정보", f"PLC 블랙박스 저장: {path} ({len(rows or [])}행)")
        except Exception:
            pass

    @Slot(str)
    def _on_plc_restarted(self, why: str):
        """마커 불일치 = PLC 재기동/메모리 초기화. 히터 설정은 PLC 컨트롤러가 이미 재적용했다."""
        try:
            if self.chat_chk:
                self.chat_chk.notify_text(f"⚠️ CHK PLC 재기동/메모리 초기화 감지 — 히터 설정 재적용 | {why}")
                self.chat_chk.flush()
        except Exception:
            pass
        _active = (bool(getattr(self, "process_running", False))
                   or bool(getattr(self, "csv_mode", False))
                   or bool(getattr(self, "_csv_delay_active", False)))
        if _active:
            # 펌프·밸브 출력이 초기화됐으므로 공정은 "재시작" 경로로 중단한다
            self.on_status_message("재시작", "PLC 재기동 감지 — 펌프·밸브 출력이 초기화되어 공정을 중단합니다")

    @Slot(float)
    def _on_plc_recovered(self, lost: float):
        """PLC 통신 복구. 이번 단절로 공정이 중단됐고(_outage_abort) 지금 공정이 돌지 않으면
        안전 상태를 한 번 다시 보낸다 — 단절 중엔 종료 시퀀스의 PLC 쓰기가 전부 실패했기 때문."""
        plc = getattr(self, "plc_controller", None)
        if plc is None or not bool(getattr(plc, "_outage_abort", False)):
            return          # 짧은 단절(중단 아님) — 아무것도 하지 않는다
        _active = (bool(getattr(self, "process_running", False))
                   or bool(getattr(self, "csv_mode", False))
                   or bool(getattr(self, "_csv_delay_active", False)))
        if _active:
            log_message_to_monitor("정보", f"PLC 통신 복구({lost:.0f}초) — 공정 진행 중이라 안전 상태 재적용 생략")
            plc._outage_abort = False
            return
        try:
            plc.send_rfpower_command(0)                       # RF DAC 0 (RFpower 가 쓰는 기존 경로)
        except Exception:
            pass
        try:
            self.request_heater_run.emit(False)               # 히터 OFF (기존 시그널)
        except Exception:
            pass
        try:
            self.request_plc_port_update.emit("Ar_Button", False)   # PLC_COIL_MAP 키(M00007)
            self.request_plc_port_update.emit("O2_Button", False)   # PLC_COIL_MAP 키(M00008)
        except Exception:
            pass
        msg = f"PLC 통신 복구({lost:.0f}초) → 안전 상태 재적용: RF DAC 0 · 히터 OFF · Ar/O2 CLOSE"
        log_message_to_monitor("정보", msg)
        try:
            append_comm_event("PLC", {**plc.make_event_row("안전상태재적용", msg, lost=lost),
                                      "공정명": self._plc_current_process_name()})
        except Exception:
            pass
        try:
            if self.chat_chk:
                self.chat_chk.notify_text(f"🔁 CHK {msg}")
                self.chat_chk.flush()
        except Exception:
            pass
        plc._outage_abort = False

    # ==================== PLC 연결 끊김/복구 알림 (CH.K) ====================

    # ==================== 히터 ====================
    def _read_heater_sv_input(self) -> float | None:
        # QLineEdit → toPlainText()가 아니라 text()
        txt = self.ui.heater_sv_edit.text().strip()
        if not txt:
            QMessageBox.warning(self, "입력 오류", "히터 목표 온도를 입력하세요.")
            return None
        try:
            v = float(txt)
        except ValueError:
            QMessageBox.warning(self, "입력 오류", f"숫자가 아닙니다: {txt}")
            return None
        # PLC가 실제로 보고한 소프트 상한(D00013)이 있으면 그쪽도 함께 본다.
        # 래더/모니터가 상한을 낮춰 둔 경우 UI가 먼저 막아 주도록.
        try:
            plc_limit = float((self.plc_controller.get_heater_status() or {}).get('sv_limit') or 0.0)
        except Exception:
            plc_limit = 0.0
        limit = min(HEATER_MAX_TEMP, plc_limit) if plc_limit > 0 else HEATER_MAX_TEMP
        if v < 0 or v > limit:
            QMessageBox.warning(self, "입력 오류",
                                f"목표 온도는 0 ~ {limit:.0f}°C 범위여야 합니다.")
            return None
        return v

    # ---------- 감속 접근 램프 경로 ----------
    def _on_process_heater_ramp_c(self, rate_c: float):
        """공정 레시피 HEATER_RAMP — 이번 공정의 램프 속도를 기억해 둔다."""
        try:
            self._process_heater_rate_c = float(rate_c or 0.0)
        except Exception:
            self._process_heater_rate_c = 0.0

    def _on_process_heater_target(self, t: float):
        """공정 레시피 HEATER_SET — 감속 접근으로 올린다.

        램프를 못 쓰는 상황(현재 온도 미상·하강)이면 예전처럼 목표만 쓴다.
        """
        rate = self._process_heater_rate_c or float(HEATER_RAMP_RATE_C_PER_MIN)
        if not self.heater_ramp.start(float(t), rate, "공정"):
            self.request_heater_target.emit(float(t))

    def _on_process_heater_run(self, on: bool):
        """공정의 히터 RUN. 끌 때는 램프를 먼저 끊고, RUN 은 main 큐를 거쳐 PLC 로 보낸다
        (목표 온도와 같은 경로 → 도착 순서가 발행 순서와 같다)."""
        if not on:
            self.heater_ramp.stop(restore_rate=False)
        self.request_heater_run.emit(bool(on))

    def _heater_manual_go(self, v: float):
        """수동 ON — 감속 접근으로 올리고 히터를 켠다."""
        if not self.heater_ramp.start(v, float(HEATER_RAMP_RATE_C_PER_MIN), "수동"):
            self.request_heater_target.emit(v)
        self.request_heater_run.emit(True)

    @Slot()
    def _on_heater_apply_clicked(self):
        v = self._read_heater_sv_input()
        if v is None:
            return
        st = self.plc_controller.get_heater_status() or {}
        if st.get('run'):
            # 운전 중 목표 변경 — 지금 램프를 끊고 새 목표로 다시 접근한다.
            self.heater_ramp.stop(restore_rate=False)
            if not self.heater_ramp.start(v, HEATER_RAMP_RATE_C_PER_MIN, "수동"):
                self.request_heater_target.emit(v)
        else:
            # 꺼져 있으면 목표만 적어 둔다. 켤 때 _heater_manual_go 가 램프를 건다.
            self.heater_ramp.stop()
            self.request_heater_target.emit(v)

    @Slot(bool)
    def _on_heater_onoff_toggled(self, checked: bool):
        # 레시피가 관리 중인 히터를 수동으로 건드리면 상태 기계와 충돌한다.
        # (레시피 실행 중 수동 OFF → 도달 못 할 온도를 타임아웃까지 대기)
        if HEATER_ENABLED and self.heater_recipe.is_running():
            QMessageBox.warning(
                self, "조작 불가",
                "히터 레시피 실행 중에는 수동 조작을 할 수 없습니다.\n"
                "레시피를 먼저 중단하세요.")
            # 눌리기 전 상태로 되돌린다 — 헬퍼는 blockSignals 로 재진입을 막는다
            self._set_heater_button_view(not checked, "OFF" if not checked else "ON")
            return

        if checked:
            st = self.plc_controller.get_heater_status() or {}
            if not st.get('itl'):
                QMessageBox.warning(self, "히터 시작 불가",
                    "히터 인터락이 미충족 상태입니다.\nTC/DAC 모듈 상태를 확인하세요.")
                # ★ 맨 setChecked(False) 는 toggled(False) 로 이 핸들러에 재진입해
                #   PLC 에 RUN=0 을 쓰는 부수효과가 있었다(09-17). 헬퍼는 시그널을 막는다.
                self._revert_heater_onoff()
                return
            v = self._read_heater_sv_input()
            if v is None:
                self._revert_heater_onoff()
                return

            # 가스·압력을 쓰기로 했으면 먼저 준비한다. 히터는 준비가 끝난 뒤
            #  _on_heater_atmosphere_ready 에서 켠다.
            if self._heater_gas_wanted() and not self.heater_atmosphere.is_ready():
                if not self._heater_gas_start_guard():
                    self._revert_heater_onoff()
                    return
                self._heater_pending = ("manual_on", v)
                self._show_heater_pending_button()
                self._sync_heater_gas_inputs()
                return
            self._heater_manual_go(v)          # 버튼 표시는 폴링(update_heater_display)이 맞춘다
        else:
            # 준비 중이면 [취소] 다. 히터는 아직 안 켜졌으니 OFF 를 보내지 않고
            #  잡아 둔 가스만 되돌린다. 이상으로 update_heater_display 가
            #  setChecked(False) 를 걸어도 이 경로로 자연히 해제된다.
            if self._heater_pending is not None:
                pending, self._heater_pending = self._heater_pending, None
                try:
                    self.heater_atmosphere.release("사용자 취소")
                except Exception:
                    pass
                self._revert_heater_onoff()
                self._sync_heater_recipe_buttons()
                log_message_to_monitor("히터", "[히터] 가스·압력 준비 취소 — 해제합니다")
                return
            self.heater_ramp.stop()
            self.request_heater_run.emit(False)   # 버튼 표시는 폴링이 맞춘다

    # ==================== 히터 CSV 로깅 / 레시피 ====================
    # ---------- 설비 이상으로 인한 공정 중단 ----------
    def _heater_fault_detail_text(self) -> str:
        """이상 시점의 히터/레시피 상태를 한 덩어리 텍스트로 만든다.

        구글챗 본문에 붙여 원인 파악에 쓴다. 어떤 접근이 실패해도
        빈 문자열을 돌려주고 예외를 밖으로 내보내지 않는다.
        """
        try:
            try:
                st = dict(self.plc_controller.get_heater_status() or {})
            except Exception as e:
                log_message_to_monitor("경고", f"히터 상태 조회 실패: {e!r}")
                st = {}

            # f-string 표현식 안에 역슬래시를 넣지 않기 위한 상수 (Py3.11 호환)
            _DEG = "\u00b0C"

            def _n(key, fmt="{:.1f}", unit=""):
                v = st.get(key)
                if v is None:
                    return "-"
                try:
                    return fmt.format(float(v)) + unit
                except Exception:
                    return str(v)

            def _b(key):
                return int(bool(st.get(key)))

            mv = st.get('mv')
            try:
                amp = heater_est_current(mv) if mv is not None else None
            except Exception:
                amp = None
            mv_txt = "-" if mv is None else (
                f"{int(mv)} ({int(st.get('mv_pct') or 0)}%"
                + (f", \u2248{amp:.1f}A)" if amp is not None else ")"))

            lines = [
                f"히터 : PV {_n('pv', unit=_DEG)} / SV {_n('cur_sv', unit=_DEG)} / MV {mv_txt}",
                (f"상태 : RUN {_b('run')} \u00b7 ITL {_b('itl')} \u00b7 FAULT {_b('fault')}"
                 f" \u00b7 OT {_b('ot')} \u00b7 TC {_b('tc_err')} \u00b7 WD {_b('wd_err')}"
                 f" \u00b7 PIDerr {st.get('pid_err', '-')}"),
            ]

            # 레시피 줄 — 실행 중이면 지금 값, 끝났으면 중단 시점 스냅샷.
            #  이상으로 공정이 죽는 순간 러너는 이미 ABORTED 라 running=False 다.
            #  그때가 이 정보가 가장 필요한 때이므로 스냅샷으로 채운다.
            try:
                pg = self.heater_recipe.progress()
                _tail = ""
                if not pg.get("running"):
                    pg = self.heater_recipe.last_snapshot() or {}
                    _tail = " (중단 시점)"
                if pg:
                    seg = f"STEP {pg.get('stepNo', 0)}/{pg.get('total', 0)}"
                    _el = int(pg.get("elapsedSec") or 0)
                    lines.append(
                        f"\ub808\uc2dc\ud53c: {seg} \u00b7 \uc804\uccb4 {pg.get('percent', 0):.0f}%"
                        f" \u00b7 \uacbd\uacfc {_fmt_hms_sec(_el)}{_tail}")
            except Exception:
                pass

            lines.append(
                f"설정 : DAC상한 {_n('mv_limit', '{:.0f}')}"
                f" \u00b7 램프 {_n('ramp_rate', '{:.0f}', _DEG + '/min')}"
                f" \u00b7 홀드백 {_n('holdback', unit=_DEG)}"
                f" \u00b7 접근 {HEATER_APPROACH_ZONE_C:g}\u00b0C\u2192"
                f"{HEATER_APPROACH_MIN_RATE_C_PER_MIN:g}\u00b0C/min"
                f" \u00b7 OT {_n('ot_limit', unit=_DEG)}")
            return "\n".join(lines)
        except Exception:
            return ""

    def _chat_send_fault_detail(self, reason: str, detail: str = ""):
        """중단 사유와 설비 상태를 구글챗으로 1회 보낸다(기존 카드와 별도)."""
        try:
            if not self.chat_chk:
                return
            name = self.current_process_name or "CHK"
            if self.csv_mode and self.csv_rows:
                name += f"  (CSV {self.csv_index + 1}/{len(self.csv_rows)})"
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            body = (f"\u274c CHK 공정 중단 \u2014 {reason}\n"
                    f"공정 : {name}\n"
                    f"시각 : {now}")
            if detail:
                body += "\n" + detail
            self.chat_chk.notify_text(body)
            self.chat_chk.flush()
        except Exception:
            pass

    def _abort_process_by_fault(self, reason: str, detail: str = ""):
        """설비 이상으로 공정을 즉시 중단한다(사용자 STOP 과 구분).

        온도가 무너진 시점에 그 시료는 이미 버린 것이라, 계속 돌리면
        타겟과 시간만 더 쓴다. 즉시 종료가 맞다.
        _chat_user_stopped 는 건드리지 않는다 — 그 플래그는 사용자 STOP
        전용이고, 설비 이상은 '실패'로 기록되어야 한다.
        """
        try:
            if not (self.process_running or self.csv_mode
                    or getattr(self, "_csv_delay_active", False)):
                return
        except Exception:
            return

        self._fault_abort_active = True
        try:
            self._chk_process_ok = False
        except Exception:
            pass
        try:
            log_message_to_monitor("경고", f"[공정 중단] {reason}")
            if detail:
                for _l in str(detail).splitlines():
                    log_message_to_monitor("경고", f"  {_l}")
        except Exception:
            pass
        try:
            self._chat_add_error(reason)
            self._chat_notify_failed_now(reason, send_text=False)
        except Exception:
            pass
        # 상세 카드는 공정 1회당 1장. 첫 사유가 대표 사유다.
        #  (중단 동작 자체는 아래에서 가드 없이 계속 수행된다)
        try:
            if not getattr(self, "_chat_fault_detail_sent", False):
                self._chat_fault_detail_sent = True     # 발송 전에 세운다
                self._chat_send_fault_detail(reason, detail)
            else:
                log_message_to_monitor(
                    "경고", f"[공정 중단] 추가 사유(챗 중복 발송 안 함): {reason}")
        except Exception:
            pass

        # CSV 리스트면 뒤 STEP 을 모두 취소한다
        try:
            if self.csv_mode:
                self.csv_cancelled = True
                log_message_to_monitor("정보", "설비 이상 → CSV 리스트 전체 취소 플래그 설정")
            if getattr(self, "_csv_delay_active", False):
                log_message_to_monitor("정보", "CSV Delay 중 설비 이상 → 딜레이 중단 및 리스트 취소")
                self._cancel_csv_list_now("CSV 공정 중단됨(설비 이상)")
                return
            if self.csv_mode and (not self.process_running):
                self._cancel_csv_list_now("CSV 공정 중단됨(설비 이상)")
                return
        except Exception:
            pass

        try:
            if self.process_controller and self.process_running:
                self.request_process_stop.emit()
        except Exception:
            pass

    def _csv_list_uses_heater(self) -> bool:
        """적재된 CSV 공정 목록의 어느 행이든 히터를 쓰면 True.

        한 행만 보면 안 된다 — 3번째 행에서 히터를 켜는 레시피가 있을 수 있다.
        판정 규칙은 _build_params_from_csv_row 와 같게 맞춘다. 여기서는 예외를
        던지지 않는다(실제 검증은 그쪽이 한다).
        """
        try:
            rows = getattr(self, "csv_rows", None) or []
        except Exception:
            return False
        for row in rows:
            try:
                use = ""
                temp = ""
                for k, v in (row or {}).items():
                    key = str(k or "").strip().lower()
                    if key == "use_heater":
                        use = str(v or "").strip().lower()
                    elif key == "heater_temp":
                        temp = str(v or "").strip()
                if use not in ("1", "y", "yes", "true", "t", "on"):
                    continue
                if float(temp) > 0:
                    return True
            except Exception:
                continue        # 값이 비었거나 숫자가 아니면 '히터 미사용'으로 본다
        return False

    @Slot()
    def _on_all_stop_clicked(self):
        """ALL STOP — 히터 레시피도 함께 멈춘다.

        레시피는 공정과 무관하게 돌 수 있으므로, 비상정지 뒤에도 레시피가
        살아 있으면 다음 스텝에서 히터를 다시 켜 버린다.
        (공정 STOP(정상 종료)에서는 레시피를 건드리지 않는다)
        """
        # ALL STOP 은 어떤 예외에도 멈추지 않아야 한다 — 단계마다 따로 감싼다.
        try:
            if HEATER_ENABLED and self.heater_recipe.is_running():
                self.heater_recipe.stop("비상 정지")
        except Exception:
            pass
        # 공정 상태머신도 세운다 — PLC 비상정지만으로는 컨트롤러가 다음 스텝을 계속 밟는다.
        #  ★ 비상정지로 끝난 공정은 '실패'로 기록되어야 한다. 그냥 request_process_stop 만
        #    보내면 _chk_process_ok 가 True 로 남아 구글챗에 "정상 종료" 카드가 나가고
        #    ChK_log.csv 에도 정상 공정으로 기록됐다. 사용자 STOP(_chat_user_stopped)과는
        #    구분해야 하므로 그 플래그는 건드리지 않고 별도 플래그로 "긴급 중단" 카드를 낸다.
        #  ★ 플래그는 request_plc_emergency_stop 보다 먼저 선다 — 비상정지로 코일이 전부 OFF 되면
        #    다음 폴링이 MV_button False 를 내는데, 그때 _mv_safety_armed() 가 이미 False 여야 이중 중단이 없다.
        _proc_active = bool(getattr(self, "process_running", False))
        _csv_active = bool(getattr(self, "csv_mode", False) and getattr(self, "csv_rows", None))
        _csv_delay = bool(getattr(self, "_csv_delay_active", False))
        _handled_by_cancel = False
        if _proc_active or _csv_active or _csv_delay:
            try:
                self._chk_process_ok = False
                self._chat_emergency_stopped = True
            except Exception:
                pass
        try:
            self.request_plc_emergency_stop.emit()
        except Exception:
            pass
        if _proc_active or _csv_active or _csv_delay:
            try:
                self._chat_notify_failed_now("ALL STOP(비상 정지)으로 중단", send_text=False)
            except Exception:
                pass
            # CSV 리스트면 뒤 STEP 을 모두 막는다("재시작" 경로와 같은 처리)
            try:
                if _csv_active:
                    self.csv_cancelled = True
                    # 딜레이 스텝은 process_running=True 로 두지만 컨트롤러는 안 돈다.
                    #  여기서 리스트를 정리하면 종료 카드가 이미 나가므로, 뒤의
                    #  request_process_stop 은 보내지 않는다("재시작" 경로의 return 과 같다).
                    #  보내면 _stop_impl 의 '이미 정지' 분기가 finished 를 한 번 더 내서
                    #  카드가 2장 나간다.
                    if _csv_delay or not _proc_active:
                        self._cancel_csv_list_now("CSV 공정 취소", reason="ALL STOP(비상 정지)으로 중단")
                        _handled_by_cancel = True
            except Exception:
                pass
        # 실제 공정 스텝이 돌고 있을 때만 컨트롤러를 세운다. 안 돌고 있는데 보내면
        #  컨트롤러가 '이미 정지' 로 finished 를 내고 엉뚱한 종료 카드가 나간다.
        if _proc_active and not _handled_by_cancel:
            try:
                self.request_process_stop.emit()
            except Exception:
                pass
        # CESAR(RF Pulse)는 PLC 를 거치지 않는 직결 시리얼이라 비상정지로 안 꺼진다.
        #  공정이 안 돌고 있어도 펄스가 켜져 있을 수 있으니 드라이버를 직접 한 번 더 끈다.
        #  ※ DC 도 직결이라 동일한 보완이 필요하다(다음 작업에서 다룬다).
        try:
            QMetaObject.invokeMethod(
                self.rfpulse_controller, "stop_process", Qt.ConnectionType.QueuedConnection)
        except Exception:
            pass

    def _log_heater_header(self, params: dict):
        """공정 로그 머리말에 히터 설정 한 줄. 히터를 안 쓰면 '미사용'."""
        try:
            if not (HEATER_ENABLED and bool(params.get("use_heater", False))):
                log_message_to_monitor("정보", "[히터] 미사용")
                return
            t = float(params.get("heater_temp", 0.0) or 0.0)
            r = float(params.get("heater_ramp", 0.0) or 0.0) or float(HEATER_RAMP_RATE_C_PER_MIN)
            amp = heater_est_current(HEATER_MV_LIMIT)
            log_message_to_monitor(
                "정보",
                f"[히터] 목표 {t:.1f}°C · 램프 {r:.0f}°C/min · "
                f"DAC 상한 {int(HEATER_MV_LIMIT)} (추정 ≈{amp:.1f}A)")
        except Exception:
            pass

    def _heater_output_pct(self, st: dict) -> float:
        """화면용 DAC 출력 비율 = mv / 운전상한(D00018) × 100.

        st['mv_pct'] 는 절대 최대치 기준이라 578/1200 이 20% 로 보여 혼란스러웠다.
        표시만 바꾸고 mv_pct 자체는 건드리지 않는다(히터 CSV 컬럼이 쓴다).
        """
        try:
            mv = float(st.get('mv', 0) or 0)
            lim = float(st.get('mv_limit', 0) or 0) or float(HEATER_MV_LIMIT)
            if lim <= 0:
                return 0.0
            return max(0.0, min(100.0, mv / lim * 100.0))
        except Exception:
            return 0.0

    def _heater_atmosphere_snapshot(self) -> dict:
        """ERP 스냅샷용 가스·압력 상태. 비활성이면 state 만 담는다."""
        try:
            a = self.heater_atmosphere
            if not a.is_active():
                return {"state": "IDLE"}
            p = a.params()
            return {
                "state": a.state(),
                "sp1": p.get("sp1"),
                "arFlow": p.get("ar_flow") if p.get("use_ar") else None,
                "o2Flow": p.get("o2_flow") if p.get("use_o2") else None,
            }
        except Exception:
            return {"state": "IDLE"}

    def _heater_output_text(self, st: dict | None = None) -> str:
        """DAC 출력 한 줄. 출력 바의 텍스트와 ERP 스냅샷이 같은 문구를 쓴다.

        퍼센트는 넣지 않는다. DAC 400 이 출력 0점(VARITAP 0.8V)이라
        mv/mv_limit 로는 401 이 33% 로 보이는데 실제 전류는 0.0A 다.
        분수(600/1200)와 추정 전류만으로 충분하다. 막대 채움에는 비율을 쓴다.
        """
        try:
            if st is None:
                st = dict(self.plc_controller.get_heater_status() or {})
            mv = int(st.get('mv', 0) or 0)
            lim = int(st.get('mv_limit', HEATER_MV_LIMIT) or HEATER_MV_LIMIT)
            if not st.get('run'):
                return f"정지 (DAC {mv})"
            # 운전 중인데 PID 가 출력을 0점까지 내린 정상 상황.
            #  이걸 '정지'로 쓰면 바로 위 '운전 중' 라벨과 화면이 자기모순이다
            #  (SOAK 오버슈트 구간에서 30초 넘게 그렇게 떠 있었다).
            if mv <= HEATER_MV_MIN:
                return f"출력 0% (DAC {mv}/{lim})"
            return (f"DAC {mv}/{lim}"
                    f" · ≈{float(st.get('est_current', 0.0) or 0.0):.1f}A")
        except Exception:
            return ""

    def _heater_log_prefix(self) -> str:
        """히터 CSV 파일명 접두사. 공정 중이면 공정명을 붙인다."""
        try:
            if self.process_running or self.csv_mode:
                name = (self.current_process_name or "").strip()
                if name:
                    safe = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", name).strip("_")
                    if safe:
                        return f"HEATER_{safe}"
        except Exception:
            pass
        return "HEATER"

    # ==================== 유지 모드 ↔ 공정 / DAC 포화 ====================
    def _refresh_hold_snapshot(self) -> None:
        """process_controller(다른 스레드)가 읽는 유지 모드 스냅샷 — main 스레드에서만 갱신한다."""
        h = self.heater_hold
        self._hold_snapshot = {"mode": HEATER_HOLD_MODE, "state": h.state, "kind": h.kind,
                               "holding": h.is_holding(), "sv2": h.sv2, "value": h.value, "gave_up": h.gave_up,
                               "relaxed": h.engaged_relaxed,
                               "force_result": getattr(self, "_hold_force_result", None)}

    @Slot(str)
    def _on_heater_hold_engaged(self, kind: str) -> None:
        self._refresh_hold_snapshot()
        h = self.heater_hold
        desc = f"SV2 {float(h.sv2):.1f}°C" if (kind == 'tc2' and h.sv2 is not None) else f"DAC 상한 {h.value}"
        log_message_to_monitor("히터", f"[유지 모드] 진입 — {kind} ({desc}){' [완화 경로]' if h.engaged_relaxed else ''}")

    @Slot(str, object)
    def _on_hold_force(self, mode: str, mv) -> None:
        """process_controller 의 폴백 요청: "tc2" → force_tc2_hold(완화 캡처), "dac" → force_dac_hold(마지막 창/현재 MV).
        결과는 스냅샷 force_result 로 돌려준다(컨트롤러는 다른 스레드)."""
        h = self.heater_hold
        ok = h.force_tc2_hold() if mode == "tc2" else h.force_dac_hold(mv)
        why = "" if ok else h.last_force_reason
        if not ok:
            log_message_to_monitor("히터(경고)", f"[유지 모드] 폴백 {mode} 불가 — {why}")
        self._hold_force_result = {"mode": mode, "ok": bool(ok), "reason": why}
        self._refresh_hold_snapshot()

    @Slot(dict)
    def _on_heater_hold_failed(self, info: dict) -> None:
        """유지 모드 진입 실패(공정 소유 히터) — 경고 로그 + 챗 카드 1장(공정당 같은 사유 반복 없음)."""
        reason = str(info.get("reason") or "")
        action = str(info.get("action") or "")
        log_message_to_monitor("히터(경고)", f"[유지 모드] 진입 실패({action}) — {reason}")
        key = (action, reason[:40])
        if getattr(self, "_chat_hold_fail_key", None) == key:
            return
        self._chat_hold_fail_key = key
        if not self.chat_chk:
            return
        try:
            st = self.plc_controller.get_heater_status() or {}
            act_txt = {"tc2_relaxed": "완화 조건으로 TC2 추종 진입 후 진행",
                       "dac": "DAC 상한 강제 고정 후 진행(TC2 추종 불가)", "abort": "공정 중단",
                       "proceed": "경고만 내고 진행"}.get(action, action)
            fields = {"사유": reason, "조치": act_txt,
                      "경과": f"{int(info.get('elapsed') or 0)}초", "상태": str(info.get("state")),
                      "TC1": self._chat_temp(st.get('pv')), "TC2": self._chat_temp(st.get('pv2')),
                      "MV": str(st.get('mv'))}
            if action == "tc2_relaxed":
                fields["SV2"] = self._chat_temp(info.get("sv2") if info.get("sv2") is not None else self.heater_hold.sv2)
                fields["근거"] = f"{info.get('why') or '정착 창 미확보'} — 최근 {HEATER_HOLD_MV_ENTER_SEC:.0f}초 TC2 평균"
            if action == "dac":
                fields["TC2 추종 불가 사유"] = str(info.get("tc2_why") or "-")
                if self.heater_hold.value is not None:
                    fields["DAC 상한"] = str(self.heater_hold.value)
            if action in ("abort", "proceed"):
                fields["완화 tc2"] = str(info.get("tc2_why") or "-"); fields["dac"] = str(info.get("dac_why") or "-")
            self.chat_chk.notify_heater_alert("히터 유지 모드 진입 실패", self._heater_run_context(), fields, ok=False)
        except Exception as e:
            log_message_to_monitor("경고", f"유지 모드 실패 카드 전송 실패: {e!r}")

    @Slot(str, dict)
    def _on_heater_hold_alert(self, kind: str, d: dict) -> None:
        """HeaterHold 의 알림 사건 → 챗 카드(공정당 같은 종류 1장): tc2→dac 강등 / 강등 실패 / 상한 여유 없음."""
        self._refresh_hold_snapshot()
        sent = getattr(self, "_chat_hold_alert_sent", None)
        if sent is None:
            sent = self._chat_hold_alert_sent = set()
        if kind in sent or not self.chat_chk:
            return
        sent.add(kind)
        try:
            st = self.plc_controller.get_heater_status() or {}
            common = {"TC1": self._chat_temp(st.get('pv')), "TC2": self._chat_temp(st.get('pv2')), "MV": str(st.get('mv'))}
            if kind == "demoted":
                title = "히터 TC2 추종 → DAC 고정 강등"
                fields = {"사유": str(d.get("why")), "조치": f"D00018 ← {d.get('value')} (마지막 측정 창/현재 MV)",
                          "주의": "OT2 과온 보호도 함께 사라졌습니다. TC2 배선을 확인하십시오", **common}
            elif kind == "demote_failed":
                title = "히터 TC2 추종 해제 — DAC 고정 실패"
                fields = {"사유": str(d.get("why")), "DAC 고정 불가": str(d.get("reason")),
                          "상태": f"TC1 제어 복귀, 출력 상한 {HEATER_MV_LIMIT} 그대로 — 확인 필요", **common}
            else:
                title = "히터 출력 여유 없음 — TC2 추종으로 고정"
                fields = {"평균 MV": f"{d.get('mv')} / 상한 {d.get('limit')}", "안내": str(d.get("why")), **common}
            self.chat_chk.notify_heater_alert(title, self._heater_run_context(), fields, ok=False)
        except Exception as e:
            log_message_to_monitor("경고", f"유지 모드 알림 카드 전송 실패: {e!r}")

    @Slot(dict)
    def _on_heater_saturated(self, d: dict) -> None:
        """DAC 포화 판정(HeaterSaturationGuard 가 에피소드당 1회) — 챗 카드 1장."""
        if not self.chat_chk:
            return
        try:
            fields = {"TC1": self._chat_temp(d.get("pv")), "TC2": self._chat_temp(d.get("pv2")),
                      "MV": str(d.get("mv")), "경과": f"{float(d.get('sec') or 0):.0f}초",
                      "클램프": f"D00018 ← {d.get('clamp')} ({d.get('src')})"}
            self.chat_chk.notify_heater_alert("히터 DAC 출력 포화", self._heater_run_context(), fields, ok=False)
        except Exception as e:
            log_message_to_monitor("경고", f"DAC 포화 카드 전송 실패: {e!r}")

    # ==================== 히터 시작/종료/도달 구글챗 카드 ====================
    @staticmethod
    def _chat_temp(v) -> str:
        """카드 온도 문구: None → "--.-", 아니면 "600.0°C"."""
        return "--.-" if v is None else f"{float(v):.1f}°C"

    @Slot(dict)
    def _on_process_heater_reached(self, d: dict) -> None:
        """공정 소유 히터의 승온 대기 통과 → "히터 도달" 카드 1장(수동 히터·히터 레시피는 이 경로가 없다)."""
        if not self.chat_chk:
            return
        try:
            took = int(d.get("took_sec") or 0)
            fields = {"목표": self._chat_temp(d.get("target")),
                      "도달 TC1": self._chat_temp(d.get("pv")), "도달 TC2": self._chat_temp(d.get("pv2")),
                      "승온 소요": f"{took // 60}분 {took % 60}초",
                      "다음 단계": (d.get("next") or "-")}
            self.chat_chk.notify_heater_reached(self._heater_run_context(), fields)
        except Exception as e:
            log_message_to_monitor("경고", f"히터 도달 카드 실패: {e!r}")

    def _heater_run_context(self) -> str:
        """_heater_log_note / _heater_log_prefix 와 같은 판정 — 레시피 > 공정 > 수동."""
        try:
            if HEATER_ENABLED and self.heater_recipe.is_running():
                return f"레시피 {self.heater_recipe.current_step_no()}/{self.heater_recipe.total_steps()}"
        except Exception:
            pass
        try:
            if self.process_running or self.csv_mode:
                return f'공정 "{(self.current_process_name or "").strip()}"'
        except Exception:
            pass
        return "수동"

    def _heater_final_target(self, st: dict):
        """최종 목표 온도. st['sv'] 는 램프 출발점(= 시작 순간 PV)일 수 있어 그대로 쓰면 안 된다.
        heater_ramp.target() → 레시피 현재 스텝 target_c → st['sv'] 순."""
        try:
            t = self.heater_ramp.target()
            if t is not None:
                return float(t)
        except Exception:
            pass
        try:
            if HEATER_ENABLED and self.heater_recipe.is_running():
                steps = self.heater_recipe.steps(); k = self.heater_recipe.current_step_no() - 1
                if 0 <= k < len(steps):
                    return float(steps[k].target_c)
        except Exception:
            pass
        return st.get('sv')

    def _heater_ramp_rate_text(self) -> str:
        try:
            if HEATER_ENABLED and self.heater_recipe.is_running():
                steps = self.heater_recipe.steps(); k = self.heater_recipe.current_step_no() - 1
                if 0 <= k < len(steps) and steps[k].ramp_c_per_min:
                    return f"{float(steps[k].ramp_c_per_min):g}°C/min"
        except Exception:
            pass
        return f"{float(HEATER_RAMP_RATE_C_PER_MIN):g}°C/min"

    @staticmethod
    def _fmt_duration(sec: float) -> str:
        m = int(max(0.0, sec) // 60)
        return f"{m // 60}시간 {m % 60}분" if m >= 60 else f"{m}분"

    def _heater_chat_tick(self, st: dict, edge) -> None:
        """RUN 엣지(update_heater_display 가 계산)에서 히터 시작/종료 카드를 1장씩 보낸다.
        어떤 예외도 밖으로 내보내지 않는다. flush() 는 부르지 않는다(urgent 카드)."""
        if edge not in ('rise', 'fall'):
            return
        run = (edge == 'rise')
        try:
            pv_txt = self._chat_temp(st.get('pv')); pv2_txt = self._chat_temp(st.get('pv2'))
            ctx = self._heater_run_context()
            if run:
                self._heater_chat_t0 = time.monotonic()
                tgt = self._heater_final_target(st)
                fields = {"목표": self._chat_temp(tgt),
                          "램프": self._heater_ramp_rate_text(),
                          "현재 TC1": pv_txt, "현재 TC2": pv2_txt}
            else:
                if st.get('ot'):        why = "이상 — 과온"
                elif st.get('tc_err'):  why = "이상 — 온도센서"
                elif st.get('wd_err'):  why = "이상 — 통신 워치독"
                elif st.get('fault'):   why = "이상"
                elif self.process_running or self.csv_mode: why = "공정 종료"
                elif ctx.startswith("레시피"):               why = "레시피 종료"
                else:                                        why = "정지"
                fields = {"마지막 TC1": pv_txt, "마지막 TC2": pv2_txt,
                          "운전 시간": self._fmt_duration(time.monotonic() - self._heater_chat_t0) if self._heater_chat_t0 else "-",
                          "사유": why}
            if self.chat_chk:
                # 종료 카드 아이콘: 이상(fault/ot/tc_err/wd_err)이면 ❌, 아니면 ✅. 시작 카드는 ℹ️ 그대로
                ok = not (st.get('fault') or st.get('ot') or st.get('tc_err') or st.get('wd_err'))
                self.chat_chk.notify_heater_run(run, ctx, fields, ok=bool(ok))
        except Exception as e:
            log_message_to_monitor("경고", f"히터 카드 전송 실패: {e!r}")

    def _heater_log_tick(self, st: dict, edge) -> None:
        """히터 운전 구간 동안만 CSV를 남긴다. 화면 갱신 로직과는 독립.
        RUN 엣지는 update_heater_display 가 계산해 넘긴다."""
        if not HEATER_LOG_ENABLED:
            return
        run = bool(st.get('run'))
        try:
            now = time.monotonic() * 1000.0

            if edge == 'rise':
                path = self._heater_logger.start(self._heater_log_prefix())
                if path is not None:
                    # CSV 와 같은 이름/타임스탬프의 .txt 를 히터 전용 텍스트 로그로 쓴다
                    #  (HEATER_20260903_144337.csv / .txt 로 짝을 이룬다)
                    try:
                        set_heater_log_file(Path(path).with_suffix(".txt"))
                    except Exception:
                        pass
                    log_message_to_monitor("정보", f"히터 로그 시작: {path}")
                    # 파일이 열리기 전에 나간 레시피 시작 로그는 이 파일에 없다.
                    #  (더 일찍 열면 시작 직후 실패 시 파일이 닫히지 않는다)
                    #  그래서 여는 순간 무엇을 돌리는지 머리말로 남긴다.
                    self._write_heater_log_header()
                self._heater_log_last_ms = 0.0

            if run and (self._heater_log_last_ms == 0.0
                        or now - self._heater_log_last_ms >= HEATER_LOG_PERIOD_MS):
                self._heater_log_last_ms = now
                self._heater_logger.write_row(st, self._heater_log_note(), self.heater_hold.log_tuple(), self.heater_hold.kind)

            if edge == 'fall':
                # 정지 직후 마지막 한 행을 남기고 파일을 닫는다
                self._heater_logger.write_row(st, self._heater_log_note(), self.heater_hold.log_tuple(), self.heater_hold.kind)
                self._heater_logger.stop()
                try:
                    clear_heater_log_file()
                except Exception:
                    pass
        except Exception as e:
            log_message_to_monitor("경고", f"히터 CSV 로그 처리 실패: {e!r}")

    def _write_heater_log_header(self):
        """히터 로그 파일이 열리는 순간, 무엇을 돌리는지 요약을 남긴다.

        level="히터" 로 남겨야 히터 파일로 라우팅된다(공정 파일에도 함께 들어간다).
        어떤 정보를 못 읽어도 예외를 밖으로 내보내지 않는다.
        """
        def _emit(msg: str):
            try:
                log_message_to_monitor("히터", msg)
            except Exception:
                pass

        _emit("=== 로그 시작 ===")

        # --- 레시피 정보 (없으면 수동 운전) ---
        try:
            rc = self.heater_recipe
            # 머리말은 '지금 무엇이 돌고 있는가'만 적는다.
            #  그래서 로드 여부(total_steps)가 아니라 is_running() 을 본다.
            #  예전에 둘을 같이 보다가 수동 ON 인데 지난 레시피가
            #  머리말에 찍혔다(2026-09-04 10:34:42 히터 로그.
            #  동작은 정상, 표시만 틀렸다).
            if rc.is_running():
                name = rc.recipe_name() or "(이름 없음)"
                est = ""
                try:
                    total = int(rc.progress().get("totalEstSec") or 0)
                    if total > 0:
                        est = f" · 예상 {_fmt_hms_sec(total)}"
                except Exception:
                    pass
                _emit(f"레시피: {name} · {rc.total_steps()}스텝{est}")
                for st_ in rc.steps():
                    _emit(f"  {st_.index}. {st_.describe()}")
                # 로그 파일은 PLC 가 run=True 를 읽은 뒤에야 열린다. 그래서
                #  스텝1 의 시작 로그(속도 확정·타임아웃)가 파일에 안 남았다.
                #  지금 돌고 있는 스텝을 한 줄로 다시 적어 둔다.
                try:
                    _no = rc.current_step_no()
                    _steps = rc.steps()
                    if 1 <= _no <= len(_steps):
                        _cur = _steps[_no - 1]
                        _bits = [f"{_no}/{len(_steps)}", _cur.describe()]
                        # 속도 지정 스텝은 describe() 에 이미 °C/min 이 들어 있다.
                        #  시간 지정(ramp_min) 스텝일 때만 확정된 속도를 덧붙인다.
                        _rate = rc.resolved_rate()
                        if _rate and getattr(_cur, "ramp_min", None):
                            _bits.append(f"→ {_rate:.1f}°C/min")
                        _to = rc.step_timeout_sec()
                        if _to:
                            _bits.append(f"타임아웃 {_to / 60:.0f}분")
                        _emit("현재 스텝: " + " · ".join(_bits))
                except Exception:
                    pass

            else:
                _emit("수동 운전 (레시피 없음)")
        except Exception:
            pass

        # --- 히터 전용 가스·압력 ---
        try:
            if self.heater_atmosphere.is_active():
                p = self.heater_atmosphere.params()
                _ar = f"{p.get('ar_flow', 0.0):.1f} sccm" if p.get("use_ar") else "—"
                _o2 = f"{p.get('o2_flow', 0.0):.1f} sccm" if p.get("use_o2") else "—"
                _stt = {"PREPARING": "준비중", "READY": "준비됨",
                        "RELEASING": "해제중"}.get(self.heater_atmosphere.state(), "-")
                _emit(f"분위기: Ar {_ar} · O2 {_o2} · "
                      f"WP {p.get('sp1', 0.0):.2f} mTorr · {_stt}")
        except Exception:
            pass

        # --- 함께 돌고 있는 공정 ---
        try:
            if self.process_running or self.csv_mode:
                nm = (self.current_process_name or "").strip()
                if nm:
                    _emit(f"공정: {nm}")
        except Exception:
            pass

        # --- PLC 에 들어 있는 설정값 ---
        try:
            st = dict(self.plc_controller.get_heater_status() or {})
            if st:
                def _n(k, fmt="{:.1f}", unit=""):
                    v = st.get(k)
                    if v is None:
                        return "-"
                    try:
                        return fmt.format(float(v)) + unit
                    except Exception:
                        return str(v)
                _emit(f"설정: DAC상한 {_n('mv_limit', '{:.0f}')}"
                      f" · 램프 {_n('ramp_rate', '{:.0f}', '°C/min')}"
                      f" · 홀드백 {_n('holdback', unit='°C')}"
                      f" · 접근 {HEATER_APPROACH_ZONE_C:g}°C→"
                      f"{HEATER_APPROACH_MIN_RATE_C_PER_MIN:g}°C/min"
                      f" · OT {_n('ot_limit', unit='°C')}")
        except Exception:
            pass

    def _heater_log_note(self) -> str:
        """공정 중이면 현재 스텝 설명, 히터 레시피 중이면 'step k/N'."""
        try:
            if self.heater_recipe.is_running():
                return (f"step {self.heater_recipe.current_step_no()}"
                        f"/{self.heater_recipe.total_steps()}")
        except Exception:
            pass
        try:
            if self.process_running or self.csv_mode:
                return (self.ui.stage_monitor.toPlainText() or "").strip().replace("\n", " ")
        except Exception:
            pass
        return ""

    @Slot()
    def _on_heater_recipe_clicked(self):
        # [레시피]는 불러오기 전용이다. 중단은 [정지] 버튼이 맡는다.
        # (실행 중에는 _sync_heater_recipe_buttons 가 이 버튼을 비활성화한다)
        if self.heater_recipe.is_running():
            return

        # 공정이 히터를 소유할 때만 막는다. 공정 레시피에 히터값이 없으면
        # 히터 레시피를 함께 돌릴 수 있다(제어 주체가 하나면 충돌하지 않는다).
        _proc_active = (self.process_running or self.csv_mode
                        or getattr(self, "_csv_delay_active", False))
        # CSV 리스트 공정은 뒤 STEP 에서 히터를 켤 수 있으므로 목록 전체를 본다
        _list_owns = (bool(getattr(self, "csv_file_path", ""))
                      and self._csv_list_uses_heater())
        if _proc_active and (self._process_heater_claimed or _list_owns):
            _tail = ("\n(공정 목록의 뒤 STEP 에 히터 목표값이 있습니다)"
                     if (_list_owns and not self._process_heater_claimed) else "")
            QMessageBox.warning(self, "실행 불가",
                                "현재 공정이 히터를 제어하고 있습니다.\n"
                                "공정 레시피에 히터 목표값이 없을 때만 히터 레시피를 함께 돌릴 수 있습니다."
                                + _tail)
            return

        start_dir = HEATER_RECIPE_DIR or str(Path.cwd())
        path, _ = QFileDialog.getOpenFileName(
            self, "히터 레시피 파일 선택", start_dir,
            "레시피 파일 (*.xlsx *.xlsm *.csv *.tsv);;Excel (*.xlsx *.xlsm);;CSV (*.csv *.tsv);;All Files (*)")
        if not path:
            return
        if not self.heater_recipe.load(path):
            QMessageBox.warning(self, "레시피 오류",
                                "레시피를 불러오지 못했습니다. 로그를 확인하세요.")
            return

        # 레시피에 가스·압력이 적혀 있으면 패널에 옮겨 담는다. 이후 흐름은
        #  패널 값을 보는 기존 경로(_heater_gas_wanted → HeaterAtmosphere)가 맡는다.
        gas = self.heater_recipe.recipe_gas()
        atm_txt = self.heater_recipe.describe_gas()
        if gas is None:
            pass                                   # 옛 레시피 — 패널 그대로
        elif self.heater_atmosphere.is_active():
            # 이미 가스가 잡혀 있다. 그 위에 다른 조건을 덮어쓰지 않는다.
            log_message_to_monitor(
                "히터(경고)",
                "[히터] 가스가 이미 잡혀 있어 레시피의 가스 설정을 적용하지 않습니다"
                " — 현재 분위기로 진행")
            atm_txt = "현재 잡혀 있는 가스 유지"
        else:
            self._apply_recipe_gas_to_panel(gas)

        steps = self.heater_recipe.steps()
        body = "\n".join(f"{i}. {s.describe()}" for i, s in enumerate(steps, 1))
        reply = QMessageBox.question(
            self, "히터 레시피 실행",
            f"{Path(path).name}\n\n{body}\n\n분위기: {atm_txt}\n\n이대로 실행할까요?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._rebuild_heater_step_list()

        # 가스·압력을 쓰기로 했으면 먼저 준비한다. 레시피는 준비가 끝난 뒤 시작한다.
        if self._heater_gas_wanted() and not self.heater_atmosphere.is_ready():
            if not self._heater_gas_start_guard():
                return
            self._heater_pending = ("recipe_start", None)
            self._show_heater_pending_button()
            self._sync_heater_recipe_buttons()
            return
        self.heater_ramp.stop()          # 레시피 러너가 자기 램프를 쥔다
        if self.heater_recipe.start():
            self._refresh_heater_progress()   # 버튼 상태는 여기서 함께 맞춰진다

    # ---------- PZ400 스타일 표시부 ----------
    def _heater_gas_hold_tick(self, st: dict, edge) -> None:
        """히터 RUN 하강 에지(update_heater_display 가 계산)에서 가스 유지/해제를 정하고,
        유지 중이면 PV 를 보며 해제 시점을 잡는다.

        뜨거운 상태에서 가스를 끊으면 진공만 남은 챔버에서 시료가 식는다.
        식을 때까지는 분위기를 유지한다. PV 를 못 읽는 동안에는 자동으로
        끊지 않는다 — 수동 [가스 해제] 가 있다.
        """
        try:
            atm = self.heater_atmosphere
            run = bool(st.get('run'))
            pv_ok = bool(st.get('ok')) and st.get('pv') is not None
            pv = float(st['pv']) if pv_ok else None
            thr = float(HEATER_GAS_HOLD_RELEASE_C)

            # 공정 중에는 MFC 가 공정 것이다 — 가스 유지/해제를 하지 않는다(로그 1줄만)
            if self._process_active():
                if edge == 'fall':
                    log_message_to_monitor("히터", "[히터] 히터 OFF — 공정 중이라 가스·압력 유지/해제는 하지 않습니다")
                return

            if edge == 'fall' and atm.is_active():
                if pv_ok and pv <= thr:
                    atm.release("히터 OFF")
                else:
                    self._atm_hold = True
                    pv_txt = f"{pv:.1f}°C" if pv_ok else "온도 미확인"
                    log_message_to_monitor(
                        "히터",
                        f"[히터] 히터 OFF — 가스·압력은 PV {thr:g}°C 이하가 될 때까지"
                        f" 유지합니다 (현재 {pv_txt})")
                    if not (self.process_running or self.csv_mode
                            or getattr(self, "_csv_delay_active", False)):
                        self.update_stage_monitor(
                            f"[가스 유지] 냉각 대기 — PV ≤ {thr:g}°C 에서 해제")

            if run:
                self._atm_hold = False

            if self._atm_hold:
                if atm.state() != "READY":
                    self._atm_hold = False          # 다른 경로로 이미 풀렸다
                elif pv_ok and pv <= thr:
                    self._atm_hold = False
                    log_message_to_monitor(
                        "히터", f"[히터] PV {pv:.1f}°C ≤ {thr:g}°C — 가스·압력 해제")
                    atm.release(f"냉각 완료 {pv:.1f}°C")
        except Exception as e:
            log_message_to_monitor("경고", f"히터 가스 유지 판정 실패: {e!r}")

    @Slot()
    def _on_heater_gas_release_clicked(self):
        """냉각 대기 중인 가스를 사용자가 지금 끊는다."""
        if not (HEATER_ENABLED and self.heater_atmosphere.is_active()):
            return
        pv_txt = (self.ui.heater_pv_edit.text() or "").strip()
        reply = QMessageBox.question(
            self, "가스·압력 해제",
            "냉각 대기 중인 가스·압력을 지금 해제할까요?"
            + (f"\n(현재 {pv_txt}°C)" if pv_txt else "")
            + "\nAr/O2 유량을 끊고 밸브를 닫습니다.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._atm_hold = False
        log_message_to_monitor("히터", "[히터] 사용자가 가스·압력을 해제했습니다")
        self.heater_atmosphere.release("사용자 해제")

    def _heater_gas_guard_for_process(self) -> bool:
        """MFC 는 한 주체만 — 히터 분위기(HeaterAtmosphere)가 활성이면 공정 시작(수동·CSV)을 거부한다.
        _heater_gas_start_guard(공정 중이면 히터 가스 준비 거부)의 반대편 규칙. True 면 진행."""
        if not (HEATER_ENABLED and getattr(self, "heater_atmosphere", None) is not None):
            return True
        if not self.heater_atmosphere.is_active():
            return True
        QMessageBox.warning(
            self, "공정 시작 불가",
            "히터 가스·압력이 잡혀 있습니다. [가스 해제] 후 시작하세요.")
        return False

    def _heater_status_text(self, st: dict) -> tuple[str, str]:
        """상태 라벨의 (문구, 색). 우선순위: 이상 > 인터락 > 가스 > 운전 > 정지.

        빨간색 3종은 PLC 래더에서 SET 코일로 래치되므로 원인이 사라져도
        [적용]/재시작만으로는 안 풀린다. 가스 준비/해제는 히터가 아직 안
        돌지만 무언가 진행 중인 구간이라 '정지'로 보이면 안 된다.
        """
        atm = getattr(self, "heater_atmosphere", None)
        state = atm.state() if (HEATER_ENABLED and atm is not None) else "IDLE"
        if st.get('fault'):
            if   st.get('ot'):     return "과온 트립", "#c62828"
            elif st.get('tc_err'): return "센서 이상", "#c62828"
            elif st.get('wd_err'): return "워치독 트립", "#c62828"
            else:                  return "이상 발생", "#c62828"
        if not st.get('itl'):
            return "인터락", "#ef6c00"          # 하드웨어 조건 미충족
        if state == "PREPARING":
            return f"가스 준비중 {self._atm_progress}".strip(), "#1565c0"
        if state == "RELEASING":
            return "가스 해제중", "#616161"
        if st.get('run'):
            if self.heater_hold.is_holding():
                return ("운전 중 · TC2 추종" if self.heater_hold.kind == 'tc2' else "운전 중 · 출력 고정"), "#2e7d32"
            return "운전 중", "#2e7d32"
        if self._atm_hold and state == "READY":
            return "가스 유지 · 냉각 중", "#1565c0"
        return "정지", "#616161"

    def _update_heater_pv2_label(self, st: dict) -> None:
        """TC2 한 줄: "TC2 1052.3 °C" / 없으면 "TC2 --.-" / TC2 추종 중 "TC2 1052.3 → 1052.3"(운전색)."""
        ui = self.ui
        pv2 = st.get('pv2')
        tc2_hold = self.heater_hold.is_holding() and self.heater_hold.kind == 'tc2'
        if pv2 is None:
            txt = "TC2 --.-"
        elif tc2_hold and self.heater_hold.sv2 is not None:
            txt = f"TC2 {float(pv2):.1f} → {float(self.heater_hold.sv2):.1f}"
        else:
            txt = f"TC2 {float(pv2):.1f} °C"
        ui.heater_pv2_label.setText(txt)
        col = "#2e7d32" if tc2_hold else "#6b7280"
        if getattr(self, "_heater_pv2_col", None) != col and not self._heater_stale:
            self._heater_pv2_col = col
            ui.heater_pv2_label.setStyleSheet(
                f"QLabel {{border: none; background: transparent; color: {col}; font-size: 8pt;}}")

    def _update_heater_lcd(self, st: dict):
        """LCD 영역(PV/SV/편차/출력/배지/테두리)을 갱신한다.

        PLC 폴링(200ms)마다 불린다. 스타일시트 재적용은 비싸므로
        바뀔 때만 넣는다.
        """
        ui = self.ui
        try:
            pv = st.get('pv')
            cur_sv = st.get('cur_sv')
            running = bool(st.get('run'))

            # SV 표시값.
            #  운전 중  : cur_sv(D00016) — 램프 중간 목표가 보여야 PZ400 과 같다
            #  정지 중  : sv(D00012) — 래더가 cur_sv 를 운전 중에만 갱신해서
            #             정지 상태에서는 마지막 운전 때의 중간 목표가 남는다.
            #             그대로 두면 꺼져 있는데 600°C 를 향하는 것처럼 보인다.
            #  TC2 추종 중: cur_sv(D00016)는 TC2 목표를 비추므로 sv(D00012 = TC1 목표)를 보이고
            #             편차도 TC1 − 최종 목표로 잰다(TC1 600 옆에 SV 1052 가 보이면 안 된다)
            tc2_hold = self.heater_hold.is_holding() and self.heater_hold.kind == 'tc2'
            if tc2_hold:
                sv_show = st.get('sv')
                dev_ref = self._heater_final_target(st)
                dev_ref = float(dev_ref) if dev_ref is not None else sv_show
            else:
                sv_show = cur_sv if running else st.get('sv')
                dev_ref = cur_sv
            if sv_show is None:
                ui.heater_sv_big.setText("---")
            else:
                ui.heater_sv_big.setText(f"{float(sv_show):.1f}")

            # 편차 — 정지 중에는 의미가 없으므로 비운다
            if running and pv is not None and dev_ref is not None:
                dev = float(pv) - float(dev_ref)
                ui.heater_dev_label.setText(f"\u0394{dev:+.1f}")
                col = "#2e7d32" if abs(dev) <= HEATER_SOAK_TOLERANCE else "#6b7280"
                if getattr(self, "_heater_dev_col", None) != col:
                    self._heater_dev_col = col
                    ui.heater_dev_label.setStyleSheet(
                        f"QLabel {{border: none; background: transparent; "
                        f"color: {col}; font-size: 9pt;}}")
            else:
                ui.heater_dev_label.setText("")

            # 운전 배지 (우선순위: FAULT > ITL > HOLD > RUN > STOP)
            held = False
            try:
                held = bool(self.heater_recipe.progress().get('held'))
            except Exception:
                held = False
            # 색은 상태에만 쓴다. FAULT 만 진한 단색이라 이상이 나면 눈에 띈다.
            if st.get('fault'):
                badge, bd, bg, fg = "FAULT", "#c62828", "#c62828", "#ffffff"
            elif not st.get('itl'):
                badge, bd, bg, fg = "ITL", "#ffcc80", "#fff3e0", "#b84c00"
            elif held:
                badge, bd, bg, fg = "HOLD", "#ffe082", "#fff8e1", "#b84c00"
            elif st.get('run'):
                badge, bd, bg, fg = "RUN", "#a5d6a7", "#e8f5e9", "#2e7d32"
            else:
                badge, bd, bg, fg = "STOP", "#c8cdd3", "#eef1f4", "#5f6b76"
            if getattr(self, "_heater_badge", None) != badge:
                self._heater_badge = badge
                ui.heater_run_badge.setText(badge)
                ui.heater_run_badge.setStyleSheet(
                    f"QLabel {{border: 1px solid {bd}; background: {bg}; color: {fg}; "
                    f"font-size: 8pt; font-weight: bold; border-radius: 3px;}}")

            # LCD 테두리 — 이상이면 붉고 두껍게
            border = "2px solid #c62828" if st.get('fault') else "1px solid #dfe3e8"
            if getattr(self, "_heater_lcd_border", None) != border:
                self._heater_lcd_border = border
                ui.heater_lcd.setStyleSheet(
                    f"QFrame#heater_lcd {{background: #f7f8fa; "
                    f"border: {border}; border-radius: 6px;}}")
        except Exception:
            pass

    def _rebuild_heater_step_list(self):
        """레시피 스텝 목록을 다시 채운다. 레시피가 없으면 비운다."""
        try:
            lst = self.ui.heater_step_list
            lst.clear()
            for s in self.heater_recipe.steps():
                # 목록 폭(184px)에 맞춘 짧은 문구. describe() 는 다른 곳에서도
                # 쓰므로 건드리지 않고 여기서만 줄인다. 원문은 툴팁으로 붙인다.
                # (RAMP 시간과 SOAK 시간이 둘 다 '분'이라 ↗ 로 구분한다)
                if s.is_cooldown:
                    txt = f"{s.index}. {s.target_c:g}°C 냉각 · {s.soak_min:g}분"
                elif s.ramp_min:
                    txt = (f"{s.index}. {s.target_c:g}°C · {s.ramp_min:g}분↗"
                           f" · {s.soak_min:g}분")
                else:
                    txt = (f"{s.index}. {s.target_c:g}°C · {s.ramp_c_per_min:g}°C/min"
                           f" · {s.soak_min:g}분")
                lst.addItem(txt)
                lst.item(lst.count() - 1).setToolTip(f"{s.index}. {s.describe()}")
            self._highlight_heater_step()
        except Exception:
            pass

    def _highlight_heater_step(self):
        """현재 실행 중인 스텝만 강조한다. 실행 중이 아니면 전부 해제."""
        try:
            from PyQt6.QtGui import QBrush, QColor, QFont
            lst = self.ui.heater_step_list
            cur = self.heater_recipe.current_step_no() if self.heater_recipe.is_running() else 0
            for i in range(lst.count()):
                it = lst.item(i)
                on = (i + 1 == cur)
                f = it.font()
                f.setBold(on)
                it.setFont(f)
                it.setBackground(QBrush(QColor("#e3f0fb")) if on
                                 else QBrush(QColor("#fafafa")))
            if cur:
                lst.scrollToItem(lst.item(cur - 1))
        except Exception:
            pass

    def _refresh_heater_progress(self):
        """레시피 진행 표시(1초 주기). PLC 폴링에 얹지 않는다."""
        ui = self.ui
        try:
            pg = self.heater_recipe.progress()
            running = bool(pg.get("running"))

            if running:
                # 지금이 RAMP 인지 SOAK 인지 — 화면에 없던 정보다.
                #  라벨 폭 120px. 한 자리 스텝 수에서는 "STEP 9/9 SOAK" 가
                #  여유롭게 들어간다. 스텝이 두 자리가 되면 넘치므로
                #  그때만 "S" 로 줄인다.
                #  (폰트 계산을 매초 하지 않고 이 규칙으로 가른다)
                _tot = int(pg.get("total", 0) or 0)
                _head = "S" if _tot >= 10 else "STEP "
                seg = f"{_head}{pg.get('stepNo', 0)}/{_tot}"
                _ph = HEATER_PHASE_TEXT.get(
                    str(pg.get("phase") or ""), "")
                if _ph:
                    seg += f" {_ph}"
                ui.heater_seg_label.setText(seg)
                # RAMP 구간은 추정치라 계산 불가(-1)면 --:-- 로 둔다
                step_remain = int(pg.get("stepRemainSec", 0) or 0)
                ui.heater_time_label.setText(
                    "--:--" if step_remain < 0 else _fmt_hms_sec(step_remain))
                pct = float(pg.get("percent") or 0.0)
                ui.heater_prog_bar.setValue(int(pct))
                ui.heater_prog_bar.setFormat(f"전체 {pct:.0f}%")
                # 남은 시간은 스텝 남은 시간과 같은 근거로 계산된 remainSec 를 쓴다
                remain = int(pg.get("remainSec", -1) or 0)
                ui.heater_prog_label.setText(
                    f"남음 {_fmt_hms_sec(remain)}" if remain >= 0 else "")
            else:
                ui.heater_seg_label.setText("레시피 없음")
                ui.heater_time_label.setText("--:--:--")
                ui.heater_prog_bar.setValue(0)
                ui.heater_prog_bar.setFormat("레시피 없음")
                ui.heater_prog_label.setText("")

            self._highlight_heater_step()
            self._sync_heater_recipe_buttons()
        except Exception:
            pass
        self._heater_stale_tick()

    def _heater_stale_tick(self) -> None:
        """1초 주기. 마지막 폴링 뒤 HEATER_STALE_SEC 이 지나면 'PLC 응답 없음 · n초 전 값' 로 바꾼다.
        값은 지우지 않는다(마지막 값임을 알 수 있게). 전환은 플래그가 바뀔 때만 스타일을 재적용하고,
        stale 중에는 라벨의 초 수와 버튼 잠금만 매초 갱신한다."""
        if not HEATER_ENABLED or self._heater_status_t <= 0.0:
            return
        age = time.monotonic() - self._heater_status_t
        if age <= float(HEATER_STALE_SEC):
            return
        n = int(age)
        if not self._heater_stale:
            self._heater_set_stale(True, n)
        else:
            self.ui.heater_status_label.setText(f"PLC 응답 없음 · {n}초 전 값")
            for _w in ("heater_onoff_button", "heater_apply_button", "heater_reset_button"):
                getattr(self.ui, _w).setEnabled(False)      # _sync_heater_recipe_buttons 가 매초 되살리므로 다시 잠근다

    def _heater_set_stale(self, stale: bool, n: int) -> None:
        """stale 전환(스타일 재적용은 여기서만). True: 값 글자색만 회색 + 빨간 라벨 + 버튼 잠금.
        False: 진입 때 보관한 원본 styleSheet 를 그대로 복원(UI.py 의 스타일을 여기서 다시 적지 않는다)."""
        self._heater_stale = stale
        ui = self.ui
        try:
            if stale:
                ui.heater_status_label.setText(f"PLC 응답 없음 · {n}초 전 값")
                ui.heater_status_label.setStyleSheet("border: none; color:#c62828; font-weight:bold;")
                for _w in ("heater_pv_edit", "heater_sv_big", "heater_pv2_label"):
                    wdg = getattr(ui, _w)
                    orig = wdg.styleSheet()
                    if _w in self._heater_style_orig:      # 이미 stale(링크 다운 → 5초 tick 재호출) — 원본을 덮지 않는다
                        continue
                    self._heater_style_orig[_w] = orig
                    if re.search(r"(?<![-\w])color\s*:", orig):          # background-color 는 제외
                        new = re.sub(r"(?<![-\w])color\s*:\s*[^;}]+", f"color: {HEATER_STALE_FG}", orig)
                    elif orig.rstrip().endswith("}"):
                        new = orig.rstrip()[:-1] + f" color: {HEATER_STALE_FG};}}"
                    else:
                        new = f"{orig}; color: {HEATER_STALE_FG};"
                    wdg.setStyleSheet(new)
                for _w in ("heater_onoff_button", "heater_apply_button", "heater_reset_button"):
                    getattr(ui, _w).setEnabled(False)
            else:
                for _w, orig in self._heater_style_orig.items():
                    getattr(ui, _w).setStyleSheet(orig)
                self._heater_style_orig.clear()
                self._heater_pv2_col = None            # 다음 폴링이 TC2 색을 다시 정한다
                self._sync_heater_recipe_buttons()    # ON/적용 활성은 레시피 상태에 따라 원래 규칙으로
        except Exception as e:
            log_message_to_monitor("경고", f"히터 stale 표시 전환 실패: {e!r}")

    def _sync_heater_recipe_buttons(self):
        """레시피 조작 버튼 상태를 한 곳에서 맞춘다.

        [레시피]는 '불러오기 전용'이라 실행 중에는 비활성으로만 표현한다.
        버튼 하나가 상황에 따라 다른 일을 하지 않게 하는 것이 목적이다.
        """
        try:
            running = self.heater_recipe.is_running()
            held = self.heater_recipe.is_held()
            self.ui.heater_hold_button.setEnabled(running)
            self.ui.heater_skip_button.setEnabled(running)
            self.ui.heater_stop_button.setEnabled(running)
            self.ui.heater_hold_button.setText("재개" if (running and held) else "일시정지")
            self.ui.heater_recipe_button.setEnabled(
                (not running) and self._heater_pending is None)
            self.ui.heater_recipe_button.setText("레시피")

            # 레시피가 목표를 관리하는 동안에는 수동 입력을 막고, 목표칸에
            # 현재 스텝 목표를 보여준다. 끝나면 마지막 목표를 남긴 채 다시 연다.
            #  (HEATER_ENABLED=false 면 _connect_signals 가 이미 잠가 뒀다)
            if running:
                try:
                    steps = self.heater_recipe.steps()
                    no = self.heater_recipe.current_step_no()
                    if 1 <= no <= len(steps):
                        self.ui.heater_sv_edit.setText(f"{steps[no - 1].target_c:g}")
                except Exception:
                    pass
            if HEATER_ENABLED:
                for _w in ("heater_sv_edit", "heater_apply_button", "heater_onoff_button"):
                    getattr(self.ui, _w).setEnabled(not running)
            self._sync_heater_gas_inputs()
        except Exception:
            pass

    # ---------- 히터 전용 가스·압력 ----------
    def _read_heater_gas_inputs(self) -> dict:
        """패널의 가스·압력 입력을 읽는다. 숫자가 아니면 0.0."""
        def _f(name):
            try:
                return float((getattr(self.ui, name).text() or "").strip())
            except (TypeError, ValueError):
                return 0.0
        return {
            "use_ar": self.ui.heater_ar_check.isChecked(),
            "use_o2": self.ui.heater_o2_check.isChecked(),
            "ar_flow": _f("heater_ar_flow_edit"),
            "o2_flow": _f("heater_o2_flow_edit"),
            "sp1": _f("heater_wp_edit"),
        }

    def _apply_recipe_gas_to_panel(self, gas: dict):
        """레시피의 가스·압력을 패널 입력칸에 옮겨 담는다."""
        try:
            ui = self.ui
            use_ar = bool(gas.get("use_ar"))
            use_o2 = bool(gas.get("use_o2"))
            uses_gas = use_ar or use_o2

            ui.heater_ar_check.setChecked(use_ar)
            ui.heater_o2_check.setChecked(use_o2)

            if uses_gas:
                # 쓰지 않는 가스의 유량칸은 비워 둔다(옛 값이 남으면 헷갈린다)
                ui.heater_ar_flow_edit.setText(
                    f"{float(gas.get('ar_flow', 0.0)):g}" if use_ar else "")
                ui.heater_o2_flow_edit.setText(
                    f"{float(gas.get('o2_flow', 0.0)):g}" if use_o2 else "")
                ui.heater_wp_edit.setText(f"{float(gas.get('wp_mtorr', 0.0)):g}")
            # 가스 없음을 명시한 경우 유량/압력 텍스트는 그대로 둔다
            #  (다음 수동 운전에 다시 쓸 수 있다)
        except Exception:
            pass
        try:
            log_message_to_monitor(
                "히터",
                f"[히터] 레시피 가스 설정 적용: {self.heater_recipe.describe_gas()}")
        except Exception:
            pass
        self._sync_heater_gas_inputs()

    def _heater_gas_wanted(self) -> bool:
        """가스·압력 단계를 실제로 밟아야 하는가.

        Ar/O2 를 하나라도 골랐으면 밟는다. 하나도 안 골랐으면 가스를 안 쓰는
        것이고, 가스를 안 쓰면 압력도 안 쓴다 — 막을 일이 아니라 건너뛸 일이다.
        """
        return (self.ui.heater_ar_check.isChecked()
                or self.ui.heater_o2_check.isChecked())

    def _sync_heater_gas_inputs(self):
        """가스 입력칸의 활성 상태를 한 곳에서 정한다.

        운전 중·레시피 중·분위기가 놀지 않는 중·준비 대기 중에는 잠근다.
        유량칸은 그 가스를 골랐을 때만, 압력칸은 가스를 하나라도 골랐을 때만.
        """
        if not HEATER_ENABLED:
            return
        try:
            run = bool((self.plc_controller.get_heater_status() or {}).get('run'))
            base = ((not run) and (not self.heater_recipe.is_running())
                    and self.heater_atmosphere.state() == "IDLE"
                    and self._heater_pending is None)
            ui = self.ui
            ar, o2 = ui.heater_ar_check.isChecked(), ui.heater_o2_check.isChecked()
            ui.heater_ar_check.setEnabled(base)
            ui.heater_o2_check.setEnabled(base)
            ui.heater_ar_flow_edit.setEnabled(base and ar)
            ui.heater_o2_flow_edit.setEnabled(base and o2)
            ui.heater_wp_edit.setEnabled(base and (ar or o2))   # 가스를 안 쓰면 압력도 안 쓴다
        except Exception:
            pass

    def _set_heater_button_view(self, checked: bool, text: str) -> None:
        """ON 버튼의 checked/text 를 바꾸는 유일한 경로. 버튼은 RUN 의 표시이지 저장소가 아니다.
        blockSignals 로 감싸 toggled 재진입(→ PLC 쓰기 부수효과)을 원천 차단한다."""
        btn = self.ui.heater_onoff_button
        try:
            btn.blockSignals(True)
            self.ui.heater_onoff_button.setChecked(bool(checked))   # 저장소 전체에서 유일한 setChecked
            self.ui.heater_onoff_button.setText(text)
        except Exception as e:
            log_message_to_monitor("경고", f"히터 버튼 표시 갱신 실패: {e!r}")
        finally:
            btn.blockSignals(False)

    def _show_heater_pending_button(self):
        """준비 중에는 ON 버튼이 [취소] 가 된다 — 유일한 중단 수단이다."""
        self._set_heater_button_view(True, "취소")
        self.ui.heater_onoff_button.setEnabled(True)

    def _heater_gas_start_guard(self) -> bool:
        """가스·압력 준비를 시작해도 되는지 확인하고 시작한다.

        MFC 는 공정과 공유하는 자원이라 소유자가 하나여야 한다.
        """
        if (self.process_running or self.csv_mode
                or getattr(self, "_csv_delay_active", False)):
            QMessageBox.warning(
                self, "가스 사용 불가",
                "공정이 MFC를 사용 중입니다.\n공정이 끝난 뒤에 사용하세요.")
            return False
        if not self._check_main_valve_open():
            return False
        kw = self._read_heater_gas_inputs()
        if not self.heater_atmosphere.start(**kw):
            QMessageBox.warning(
                self, "가스 준비 불가",
                "가스·압력을 시작하지 못했습니다.\n"
                "Ar/O2 선택과 유량·압력 값을 확인하세요. 자세한 사유는 로그에 있습니다.")
            return False
        return True

    def _revert_heater_onoff(self):
        """ON 버튼을 눌리기 전 상태(unchecked/"ON")로 되돌린다."""
        self._set_heater_button_view(False, "ON")
        self.ui.heater_onoff_button.setEnabled(True)

    @Slot(str, str)
    def _on_heater_atmosphere_state(self, state: str, detail: str):
        txt = {"IDLE": "대기", "PREPARING": "준비중", "READY": "준비됨",
               "RELEASING": "해제중"}.get(state, state)
        head = (detail.split(" ", 1)[0] if detail else "")
        if state == "PREPARING":
            if "/" in head:
                self._atm_progress = head
                txt = f"준비중 {head}"
        elif state == "IDLE":
            self._atm_progress = ""
        elif detail.startswith("오류"):
            txt = "오류"
        # 공정이 stage monitor 를 쓰는 중에는 덮어쓰지 않는다
        if not (self.process_running or self.csv_mode
                or getattr(self, "_csv_delay_active", False)):
            try:
                # 해제가 끝나 IDLE 로 돌아오면 단계 표시를 비운다 —
                #  가스 제어를 쓰기 전과 같은 모습이어야 한다.
                if state == "IDLE":
                    self.update_stage_monitor("")
                else:
                    self.update_stage_monitor(
                        f"[가스 {detail}]" if detail else f"[가스] {txt}")
            except Exception:
                pass
        self._sync_heater_gas_inputs()

    @Slot()
    def _on_heater_atmosphere_ready(self):
        pending, self._heater_pending = self._heater_pending, None
        try:
            p = self.heater_atmosphere.params()
            log_message_to_monitor(
                "히터",
                f"[히터] 가스·압력 준비됨 — "
                f"Ar {p.get('ar_flow', 0.0):g} sccm · O2 {p.get('o2_flow', 0.0):g} sccm"
                f" · WP {p.get('sp1', 0.0):g} mTorr")
        except Exception:
            pass

        if not pending:
            return                      # 사용자가 아직 ON 을 안 눌렀다
        kind, val = pending
        if kind == "manual_on":
            self._heater_manual_go(val)
            self._set_heater_button_view(True, "OFF")    # 폴링이 곧 RUN 으로 확정한다
            self.ui.heater_onoff_button.setEnabled(True)
            self._sync_heater_gas_inputs()
        elif kind == "recipe_start":
            # 준비 중 [취소] 로 쓰던 버튼을 원래대로 돌린다.
            #  실행이 시작되면 _refresh_heater_progress 가 다시 잠근다.
            self._revert_heater_onoff()
            self.heater_ramp.stop()      # 레시피 러너가 자기 램프를 쥔다
            if self.heater_recipe.start():
                self._refresh_heater_progress()
            else:
                log_message_to_monitor("경고", "[히터] 레시피 시작에 실패했습니다.")
                self.heater_atmosphere.release("레시피 시작 실패")

    @Slot(str)
    def _on_heater_atmosphere_failed(self, reason: str):
        pending, self._heater_pending = self._heater_pending, None
        if pending:
            # 레시피는 로드된 채로 둔다 — 다시 [레시피]로 시작할 수 있다
            self._revert_heater_onoff()
            self._sync_heater_recipe_buttons()
        # 이미 운전 중이라면 히터는 건드리지 않는다. 가스 이탈은 경고 정책이다.
        try:
            log_message_to_monitor("경고", f"[히터] 가스·압력 준비 실패: {reason}")
        except Exception:
            pass
        try:
            if not self._is_closing:
                QMessageBox.warning(self, "가스·압력 준비 실패", reason)
        except Exception:
            pass
        try:
            if self.chat_chk:
                self.chat_chk.notify_text(f"⚠️ CHK 히터 가스·압력 준비 실패: {reason}")
                self.chat_chk.flush()
        except Exception:
            pass
        self._sync_heater_gas_inputs()

    @Slot()
    def _on_heater_atmosphere_released(self):
        self._atm_progress = ""
        self._atm_hold = False
        self._sync_heater_gas_inputs()

    @Slot()
    def _on_heater_reset_clicked(self):
        """PLC 에 래치된 히터 이상을 지운다(M00043).

        래치는 프로그램을 껐다 켜도 안 지워진다. 예전에는 XG5000 을 띄워야만
        풀 수 있었다(2026-09-04 TC 배선 후 TC=1/ITL=0 이 걸렸다).
        안전 래치를 지우는 동작이라 무조건 확인을 받는다.
        RUN 이 켜진 채 ITL 이 복구되면 래더가 같은 스캔에서 PID 를 재개해 편차만큼 즉시
        풀파워가 된다 — RUN 이 켜져 있으면 리셋을 막는다.
        """
        if bool((self.plc_controller.get_heater_status() or {}).get('run')):
            QMessageBox.warning(self, "리셋 불가",
                                "히터 RUN 이 켜져 있습니다.\n먼저 히터를 OFF 한 뒤 리셋하세요.")
            return
        reply = QMessageBox.question(
            self, "히터 이상 리셋",
            "히터 이상을 리셋합니다.\n"
            "원인(배선 · TC 모듈 · 과온)을 먼저 확인하셨습니까?\n\n"
            "원인이 남아 있으면 리셋 직후 다시 이상이 걸립니다.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        # 누가 언제 눌렀는지 남긴다 — 래치를 지우는 조작이다
        try:
            log_message_to_monitor("히터", "사용자가 히터 이상 리셋을 요청했습니다")
        except Exception:
            pass
        self.request_heater_reset.emit()

    @Slot()
    def _on_heater_hold_clicked(self):
        """[일시정지] 토글. HOLD 중이면 재개한다."""
        if not self.heater_recipe.is_running():
            return
        if self.heater_recipe.is_held():
            self.heater_recipe.resume()
        else:
            self.heater_recipe.hold()
        self._sync_heater_recipe_buttons()

    @Slot()
    def _on_heater_stop_clicked(self):
        """[정지] 레시피를 중단한다. 공정이 돌고 있어도 공정은 건드리지 않는다."""
        if not self.heater_recipe.is_running():
            return
        _in_process = (self.process_running or self.csv_mode
                       or getattr(self, "_csv_delay_active", False))
        msg = ("히터 레시피를 중단하고 히터를 끕니다.\n"
               "공정은 계속 진행됩니다.\n"
               "계속할까요?") if _in_process else (
              "히터 레시피를 중단하고 히터를 끕니다.\n"
              "계속할까요?")
        reply = QMessageBox.question(
            self, "레시피 중단", msg,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        self.heater_recipe.stop("사용자 중단")
        self._sync_heater_recipe_buttons()

    @Slot()
    def _on_heater_skip_clicked(self):
        if not self.heater_recipe.is_running():
            return
        reply = QMessageBox.question(
            self, "스텝 건너뛰기",
            "현재 스텝을 건너뛰고 다음으로 진행합니다. 계속할까요?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return
        self.heater_recipe.skip_step()
        self._sync_heater_recipe_buttons()

    @Slot(int, int, str)
    def _on_heater_recipe_step(self, cur: int, total: int, desc: str):
        # 버튼 텍스트는 건드리지 않는다. 스텝 진행은 LCD(STEP n/m)에 나온다.
        self._sync_heater_recipe_buttons()
        # 공정 중에는 stage monitor 를 공정이 쓴다. 히터 진행은 히터 패널에
        # 자체 표시(STEP/남은시간/진행률/스텝목록)가 있으므로 덮어쓰지 않는다.
        if not (self.process_running or self.csv_mode
                or getattr(self, "_csv_delay_active", False)):
            self.update_stage_monitor(self._heater_recipe_stage_text(cur, total, desc))

    def _heater_recipe_stage_text(self, cur: int, total: int, desc: str) -> str:
        """진행률과 남은 시간까지 한 줄로 만든다.
        예: 히터 레시피 2/3 · 610°C 유지 · 전체 43% · 남음 2:15:40
        """
        base = f"히터 레시피 {cur}/{total} - {desc}"
        try:
            pg = self.heater_recipe.progress()
            total_est = int(pg.get("totalEstSec") or 0)
            elapsed = int(pg.get("elapsedSec") or 0)
            if total_est > 0:
                # 히터 패널과 같은 근거(스텝 남은시간 기준)를 쓴다. 옛 시계
                #  기준을 쓰면 같은 화면에 '남음'이 두 개 다르게 뜬다.
                _r = pg.get("remainSec")
                try:
                    remain = int(_r) if (_r is not None and int(_r) >= 0) else -1
                except Exception:
                    remain = -1
                if remain < 0:
                    remain = max(0, total_est - elapsed)
                base += (f" · 전체 {pg.get('percent', 0):.0f}%"
                         f" · 남음 {_fmt_hms_sec(remain)}")
        except Exception:
            pass
        return base

    @Slot(bool, str)
    def _on_heater_recipe_finished(self, ok: bool, reason: str):
        # ★ 중단 여부를 맨 앞에서 확정한다. 아래 표시 갱신이 무엇을 하든
        #   공정 중단은 반드시 실행되어야 한다.
        try:
            in_process = bool(self.process_running or self.csv_mode
                              or getattr(self, "_csv_delay_active", False))
        except Exception:
            in_process = False
        # stop() 으로 끝난 경우는 의도적 중단이다. 설비 이상(_abort)과 달리
        # 공정까지 '설비 이상 실패'로 죽이면 안 된다.
        _user_stop = False
        try:
            _user_stop = bool(self.heater_recipe.was_user_stopped())
        except Exception:
            _user_stop = False
        _need_abort = bool(in_process and not ok and not _user_stop)

        # 끝난 레시피는 로드 상태와 화면 목록을 함께 비운다. 남겨 두면
        #  수동 운전 중에도 지난 스텝 목록이 떠 있어 뭔가 돌고 있는 것처럼 보인다.
        #  (in_process 로 일찍 return 하는 경로들보다 앞에서 부른다)
        try:
            self.heater_recipe.clear()
            self._rebuild_heater_step_list()
        except Exception:
            pass

        try:
            self._sync_heater_recipe_buttons()
            self._refresh_heater_progress()
            if not in_process:
                self.update_stage_monitor(
                    f"히터 레시피 {'완료' if ok else '중단'}: {reason}")
        except Exception:
            pass

        if in_process:
            # 공정 중에는 모달을 띄우지 않는다 — 작업자가 공정 화면을 못 본다.
            if ok:
                try:
                    log_message_to_monitor("정보", f"[히터] 레시피 완료: {reason}")
                except Exception:
                    pass
                return
            if _user_stop:
                # 사람이 의도적으로 멈춘 것이다. 공정은 그대로 둔다.
                #  실패가 아니므로 _chat_add_error / _chat_notify_failed_now 는 부르지 않는다
                #  (종료 카드의 실패 원인 목록에 들어가면 안 된다).
                try:
                    log_message_to_monitor(
                        "경고",
                        f"[히터] 사용자가 레시피를 중단했습니다: {reason}"
                        f" → 공정은 계속됩니다. 히터는 꺼집니다.")
                except Exception:
                    pass
                try:
                    if self.chat_chk:
                        self.chat_chk.notify_text(
                            f"\u23f9 CHK 히터 레시피 중단(사용자): {reason} — 공정은 계속 진행됩니다.")
                        self.chat_chk.flush()
                except Exception:
                    pass
                return

            # 히터가 무너진 시점에 시료는 이미 버린 것이다. 공정을 즉시 끝낸다.
            try:
                log_message_to_monitor(
                    "경고", f"[히터] 레시피가 중단되었습니다: {reason} → 공정을 중단합니다.")
            except Exception:
                pass
            # detail 은 먼저 만들어 둔다. 인자 자리에서 바로 부르면 그 계산이
            # 예외를 낼 때 _abort_process_by_fault 가 호출조차 되지 않는다.
            _detail = ""
            try:
                _detail = self._heater_fault_detail_text()
            except Exception:
                _detail = ""
            if _need_abort:
                try:
                    self._abort_process_by_fault(
                        f"히터 레시피 중단 — {reason}", _detail)
                except Exception:
                    pass
            return

        if self._is_closing:
            return

        if ok:
            QMessageBox.information(self, "히터 레시피", reason)
        else:
            QMessageBox.warning(self, "히터 레시피", f"중단되었습니다.\n\n{reason}")

    @Slot(dict)
    def update_heater_display(self, st: dict):
        """PLC 폴링(200ms)으로 올라온 히터 상태를 화면에 반영한다.

        st 딕셔너리는 device/PLC.py 의 _poll_heater() 가 만든다.
          ok      : 온도값 유효 여부 (False = 열전대 단선 / TC 모듈 이상)
          pv      : 현재 온도 [°C]
          cur_sv  : SV Ramp가 적용된 '현재 중간 목표' [°C]
          mv      : DAC 카운트 원본
          mv_limit: 살아있는 DAC 상한 (D00018)
          mv_pct  : 출력 백분율 (HEATER_MV_MIN=0%, mv_limit=100%)
          est_current : DAC 카운트로 추정한 전류 [A]
          run/itl/fault/ot/tc_err/wd_err : 상태 비트
        """
        # 폴링이 왔다 — stale 시계를 되감고, stale 표시 중이었으면 원래대로 복원
        self._heater_status_t = time.monotonic()
        if self._heater_stale:
            self._heater_set_stale(False, 0)

        # ERP 리포터용 히터 상세 상태 기록
        try:
            self._erp_heater = dict(st or {})
            self._erp_heater['hold_kind'] = self.heater_hold.kind if self.heater_hold.is_holding() else None
        except Exception:
            pass

        # --- 현재 온도 TC1 (QLineEdit이므로 setText 사용) ---
        if st.get('ok') and st.get('pv') is not None:
            self.ui.heater_pv_edit.setText(f"{st['pv']:.1f}")
        else:
            # 단선/모듈이상 시 PLC가 hFFFF를 쓰고 파이썬은 -1로 읽는다
            self.ui.heater_pv_edit.setText("")      # 빈 칸 → placeholder "--.-" 노출
        # --- TC2 한 줄 ---
        self._update_heater_pv2_label(st)

        # --- 상태 문구 ---
        s, c = self._heater_status_text(st)
        self.ui.heater_status_label.setText(s)
        self.ui.heater_status_label.setStyleSheet(
            f"border: none; color:{c}; font-weight:bold;")

        # --- 출력 표시 : DAC 원본값 + 출력% + 추정 전류 (바 안에 텍스트로) ---
        #     DAC 원본을 함께 보여야 PLC 모니터(D00041)와 대조할 수 있다.
        try:
            _mv = int(st.get('mv', 0) or 0)
            _on = bool(st.get('run')) and _mv > HEATER_MV_MIN
            self.ui.heater_out_bar.setValue(
                int(self._heater_output_pct(st)) if _on else 0)
            self.ui.heater_out_bar.setFormat(self._heater_output_text(st))
        except Exception:
            pass

        # --- RUN 엣지는 여기서 한 번만 계산한다(소비자: 챗·로그·가스유지) ---
        run = bool(st.get('run'))
        edge = self._heater_run_edge(run)

        # --- 버튼 = RUN 의 표시 (준비 중 [취소] 일 때는 손대지 않는다) ---
        if self._heater_pending is None:
            self._set_heater_button_view(run, "OFF" if run else "ON")

        # --- 이상 상승 엣지 → RUN OFF 명시 전송(에피소드당 1회) ---
        self._heater_fault_off_tick(st)

        # --- DAC 포화 감시(유지 모드가 출력을 묶고 있지 않을 때만) → 유지 모드(dac / tc2) ---
        self.heater_sat.tick(st, self.heater_hold.is_holding(), self.heater_hold.kind)
        self.heater_hold.tick(st, self._heater_final_target(st))
        self._refresh_hold_snapshot()

        # --- 히터 시작/종료 구글챗 카드 (RUN 엣지) ---
        self._heater_chat_tick(st, edge)

        # --- CSV 로깅 (운전 중에만, HEATER_LOG_PERIOD_MS 주기) ---
        #     폴링은 200ms이므로 반드시 시각 비교로 솎아낸다.
        self._heater_log_tick(st, edge)

        # --- PZ400 스타일 LCD 표시 ---
        self._update_heater_lcd(st)

        # --- 히터 OFF → 가스 유지/해제 ---
        self._heater_gas_hold_tick(st, edge)

        # --- 가스·압력 위젯 활성 ---
        self._sync_heater_gas_inputs()

        # --- 이상 리셋 버튼: 이상일 때만, 레시피가 안 돌 때만 ---
        #     눌러야 할 이유가 없을 때 눌리면 안 된다.
        try:
            self.ui.heater_reset_button.setVisible(bool(st.get('fault')))
            self.ui.heater_reset_button.setEnabled(
                bool(st.get('fault')) and not self.heater_recipe.is_running())
            # 같은 자리를 쓰는 [가스 해제]. 이상이 우선이라 fault 면 내린다.
            self.ui.heater_gas_release_button.setVisible(
                self._atm_hold and not bool(st.get('fault')))
        except Exception:
            pass

        # --- NAS CSV 로그용 평균 누적 (운전 중 + 온도 유효할 때만) ---
        if st.get('ok') and st.get('pv') is not None and st.get('run'):
            self._chk_heater_sum += float(st['pv'])
            self._chk_heater_cnt += 1

    def _heater_run_edge(self, run: bool):
        """RUN 의 상승/하강 엣지 — 'rise' / 'fall' / None. 한 폴링에 한 번만 계산한다."""
        prev = self._heater_run_prev_view
        self._heater_run_prev_view = run
        if run and not prev:
            return 'rise'
        if prev and not run:
            return 'fall'
        return None

    def _heater_fault_off_tick(self, st: dict) -> None:
        """이상 상승 엣지에서 RUN 이면 RUN OFF 를 명시적으로 보낸다(에피소드당 1회).
        래더 H9 가 출력은 끊지만 RUN 비트는 파이썬만 끈다 — 시그널 재진입 부수효과가 아니라
        여기서 명시적으로 쓴다."""
        fault = bool(st.get('fault'))
        if not fault:
            self._heater_fault_off_sent = False
            return
        if self._heater_fault_off_sent:
            return
        self._heater_fault_off_sent = True
        if bool(st.get('run')):
            self.request_heater_run.emit(False)
            log_message_to_monitor("히터", "[히터] 이상 발생 — RUN OFF 전송")

    @Slot(dict)
    def _on_heater_residual(self, d: dict):
        """시작 시 PLC 에 남아 있던 RUN 을 PLC 스레드가 정리했다 — 기록 + 챗 텍스트 1줄."""
        pv = d.get('pv'); sv_old = d.get('sv_old')
        pv_txt = "--.-" if pv is None else f"{float(pv):.1f}"
        try:
            log_message_to_monitor(
                "히터", f"[히터] 이전 세션의 히터 RUN 잔존 (PV {pv_txt} / 이전 SV {float(sv_old or 0):.1f})"
                        f" → OFF, SV 를 현재 온도로 초기화")
        except Exception as e:
            print(f"[main] 잔존 RUN 로그 실패: {e!r}")
        try:
            if self.chat_chk:
                self.chat_chk.notify_text("⚠️ CHK 이전 세션 히터 RUN 잔존 감지 → OFF")
                self.chat_chk.flush()
        except Exception as e:
            log_message_to_monitor("경고", f"잔존 RUN 챗 알림 실패: {e!r}")

    @Slot(str)
    def _on_heater_fault(self, reason: str):
        """히터 이상. 공정 중단은 공정이 히터를 소유(_process_heater_claimed, CSV 행)하거나 히터 레시피가 돌 때만 —
        수동 히터(히터 패널)가 이상으로 꺼져도 공정은 계속된다(수동 모드는 히터와 공정이 분리돼 있다)."""
        # 이상 발생 시점의 상태값을 함께 남긴다.
        #  - log_message_to_monitor 는 내부에서 파일 로그(NAS)까지 수행하므로
        #    프로그램을 재시작해도 기록이 남는다. (화면 모니터는 재시작 시 지워짐)
        #  - 나중에 재발했을 때 과온/센서/워치독 중 무엇이었는지 구분하려면
        #    비트 상태가 반드시 필요하다.
        #
        # ★ 중단 여부는 맨 앞에서 확정한다. 로그·챗 같은 부수 작업이 무엇을 하든
        #   공정 중단은 반드시 실행되어야 하기 때문이다(로그를 못 남겼다는 이유로
        #   히터 이상이 난 공정이 그대로 도는 일이 없어야 한다).
        _need_abort = False
        try:
            _need_abort = bool(self.process_running
                               and (self._process_heater_claimed
                                    or self.heater_recipe.is_running()))
        except Exception:
            _need_abort = False

        st = {}
        try:
            st = self.plc_controller.get_heater_status()   # 마지막 폴링 캐시(추가 통신 없음)
        except Exception:
            pass

        detail = (
            f"[히터] {reason} | "
            f"PV={st.get('pv')} "
            f"ITL={int(bool(st.get('itl')))} "
            f"OT={int(bool(st.get('ot')))} "
            f"TC={int(bool(st.get('tc_err')))} "
            f"WD={int(bool(st.get('wd_err')))} "
            f"MV={st.get('mv')} "
            f"PIDerr={st.get('pid_err')}"
        )
        try:
            log_message_to_monitor("경고", detail)
        except Exception:
            pass

        if self.chat_chk:
            try:
                self.chat_chk.notify_error_with_src("HEATER", reason)
            except Exception:
                pass
        try:
            if self.process_running:
                self._chat_add_error(f"HEATER: {reason}")
        except Exception:
            pass

        # 히터를 실제로 쓰고 있을 때만 공정을 중단한다.
        #  아무도 안 쓰는 상태에서 예전 래치가 올라온 경우까지 공정을 죽이면 안 된다.
        #  (판정은 이 함수 맨 앞에서 이미 끝났다)
        # detail 은 먼저 만들어 둔다. 인자 자리에서 바로 부르면 그 계산이
        # 예외를 낼 때 _abort_process_by_fault 가 호출조차 되지 않는다.
        _detail = ""
        try:
            _detail = self._heater_fault_detail_text()
        except Exception:
            _detail = ""
        if _need_abort:
            try:
                self._abort_process_by_fault(f"히터 이상 — {reason}", _detail)
            except Exception:
                pass
    # ==================== 히터 ====================

    def _check_main_valve_open(self) -> bool:
        """메인밸브(MV)와 MV_INTERLOCK을 읽어 둘 다 ON인지 확인.
        둘 다 ON(메인밸브 실제 개방)일 때만 True. 그 외에는 경고 후 False."""
        mv, itl = self.plc_controller.read_main_valve_state()
        if mv is None or itl is None:
            QMessageBox.warning(
                self, "공정 시작 불가",
                "메인밸브 상태를 읽을 수 없습니다.\nPLC 연결을 확인하세요."
            )
            return False
        if not (mv and itl):
            QMessageBox.warning(
                self, "공정 시작 불가",
                "메인밸브가 열려 있지 않아 공정을 시작할 수 없습니다.\n"
                f"(MV={'ON' if mv else 'OFF'}, "
                f"MV_INTERLOCK={'ON' if itl else 'OFF'})\n"
                "메인밸브를 먼저 개방한 뒤 다시 시작하세요."
            )
            return False
        return True

    @Slot()
    def _handle_start_process(self):
        # 히터 레시피가 돌아도, 이 공정이 히터를 건드리지 않으면 시작을 허용한다.
        if HEATER_ENABLED and self.heater_recipe.is_running():
            if self.csv_file_path and self._csv_list_uses_heater():
                QMessageBox.warning(self, "시작 불가",
                                    "히터 레시피가 실행 중입니다.\n"
                                    "이 공정 레시피에는 히터 목표값이 들어 있어 함께 실행할 수 없습니다.\n"
                                    "히터 레시피를 중단하거나, 레시피에서 히터값을 빼세요.")
                return
            # 수동 모드는 아래에서 use_heater 를 강제로 끈다
        if self.process_running:
            QMessageBox.warning(self, "경고", "이미 공정이 진행 중입니다.")
            return
        
        # 히터 전용 가스·압력이 MFC 를 쥐고 있으면 공정이 그 위에 겹칠 수 없다
        if not self._heater_gas_guard_for_process():
            return

        # ★ 메인밸브 개방 확인: MV & MV_INTERLOCK 둘 다 ON일 때만 공정 시작 허용
        if not self._check_main_valve_open():
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

            # --- RF Pulse: 전용 칸에서 읽는다. RF power 와 독립이다 ---
            #   파워는 필수, freq/duty 는 빈 칸 허용(= 장비 현재값 유지)
            use_rf_pulse = self.ui.rf_pulse_checkbox.isChecked()
            rf_pulse_power = 0.0
            rf_pulse_freq = None
            rf_pulse_duty = None
            if use_rf_pulse:
                _rp = self.ui.rfp_power_edit.toPlainText().strip()
                if not _rp or float(_rp) <= 0:
                    # 흔한 실수: RF power 칸(RF_power_edit)에 펄스 파워를 넣는다.
                    #  그 칸은 아날로그 RF 전용이라 펄스 파워는 비어 있다.
                    _rf_big = self.ui.RF_power_edit.toPlainText().strip()
                    if (not _rp) and _rf_big and not self.ui.rf_power_checkbox.isChecked():
                        raise ValueError(
                            "RF Pulse 파워는 RF Pulse 체크박스 바로 아래 칸에 입력하세요. "
                            "왼쪽 칸은 RF power(아날로그) 전용입니다.")
                    raise ValueError("RF Pulse 파워를 입력해야 합니다.")
                rf_pulse_power = float(_rp)
                _fq = self.ui.rfp_freq_edit.toPlainText().strip()
                _dt = self.ui.rfp_duty_edit.toPlainText().strip()
                if _fq:
                    rf_pulse_freq = float(_fq)          # kHz
                if _dt:
                    rf_pulse_duty = int(float(_dt))     # %
                self._check_rfpulse_pulse_range(rf_pulse_freq, rf_pulse_duty, "수동 시작")

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

                # ▼ RF Pulse (CESAR). freq 는 kHz, duty 는 %. None = 장비 현재값 유지
                "use_rf_pulse": bool(use_rf_pulse),
                "rf_pulse_power": float(rf_pulse_power),
                "rf_pulse_freq": rf_pulse_freq,
                "rf_pulse_duty": rf_pulse_duty,
                "use_g1": self.ui.G1_checkbox.isChecked(),
                "use_g2": self.ui.G2_checkbox.isChecked(),

                # ▼ G1/G2 사용 여부 + 타겟 이름
                "use_g1": use_g1_flag,
                "use_g2": use_g2_flag,

                # ▼ DC Power 안정화 대기 사용 여부 (기본 OFF)
                "use_dc_delay": self.ui.dc_delay_checkbox.isChecked(),

                # ▼ 히터: 수동 공정은 히터를 소유하지 않는다(2026-09-17). 히터 패널의 ON/OFF·목표·유지 모드는
                #    히터 패널 몫이고, 공정은 램프/설정/대기 스텝을 만들지 않으며 종료·중단·STOP 때 히터를 끄지 않는다.
                #    (600°C ON 상태에서 공정을 시작하면 공정이 히터를 잡아 목표 재설정·110분 대기·종료 시 OFF 까지 했다)
                #    히터를 공정이 소유하는 것은 CSV 행의 use_heater/heater_temp 뿐이다.
                "use_heater": False,
                "heater_temp": 0.0,

                "g1_target_name": g1_target_name,
                "g2_target_name": g2_target_name,
            }

            if params['rf_pulse_power'] > float(RFPULSE_MAX_POWER):
                raise ValueError(
                    f"RF Pulse 파워 {params['rf_pulse_power']:g}W 가 장비 상한 "
                    f"{float(RFPULSE_MAX_POWER):g}W(RFPULSE_MAX_POWER)를 넘습니다.")

            if not (params['dc_power'] > 0 or params['rf_power'] > 0
                    or params['rf_pulse_power'] > 0):
                raise ValueError("RF / RF Pulse / DC 파워 중 하나 이상을 입력해야 합니다.")
            
            # Shutter Delay / Process Time 정책:
            #   - 둘 다 0 이상
            #   - 둘 중 하나는 반드시 > 0
            #   - process_time == 0 이면 Main Shutter는 열지 않고 shutter delay만 진행 후 종료
            if params['shutter_delay'] < 0:
                raise ValueError("Shutter Delay는 0 이상이어야 합니다.")
            if params['process_time'] < 0:
                raise ValueError("Process Time은 0 이상이어야 합니다.")
            if params['shutter_delay'] <= 0 and params['process_time'] <= 0:
                raise ValueError("Shutter Delay와 Process Time 중 하나는 0보다 커야 합니다.")

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

        # 이번 공정이 히터를 소유하는가 (히터 레시피와의 충돌 판정 기준)
        self._process_heater_claimed = bool(
            params.get("use_heater") and float(params.get("heater_temp") or 0) > 0)
        if HEATER_ENABLED and self.heater_recipe.is_running() \
                and not self._process_heater_claimed:
            log_message_to_monitor(
                "정보",
                "[히터] 히터 레시피가 제어 중입니다. 이번 공정은 히터를 제어하지 않습니다.")

        # 이번 공정의 히터 설정을 로그 머리말에 남긴다
        self._log_heater_header(params)

        self._chat_reset_run_state()
        self._chat_notify_started(params, self.current_process_name)

        try:
            self.erp.run_start(
                self.current_process_name or params.get("process_note", "") or "CHK 공정",
                params,
            )
        except Exception:
            pass

        self.request_process_start.emit(params)

        self.process_running = True
        self.ui.Sputter_Start_Button.setEnabled(False)
        self.ui.Sputter_Stop_Button.setEnabled(True)
        self.ui.select_csv_button.setEnabled(False)

    def _start_csv_process_from_path(self, path: str):
        """파일 대화상자 없이 지정된 CSV/엑셀 레시피를 적재한다(원격 실행용).

        _on_select_csv_clicked 가 경로를 얻은 뒤 수행하는 처리와 동일하다.
        (UI 경로와 동작을 하나로 유지하기 위해 본문을 이쪽으로 옮겼다)
        """
        if self._is_closing:
            return

        # UI 경로는 대화상자 앞에서 이미 막지만, 원격 호출은 여기서 막아야 한다
        if self.process_running or self.csv_mode or self._csv_delay_active:
            QMessageBox.warning(
                self,
                "변경 불가",
                "공정 진행 중에는 CSV 파일을 변경할 수 없습니다."
            )
            return

        if not path:
            return

        p = Path(path)
        if not p.exists():
            QMessageBox.warning(self, "파일 오류", "선택한 CSV 파일을 찾을 수 없습니다.")
            return

        self.csv_file_path = str(p)
        log_message_to_monitor("정보", f"CSV 공정 리스트 파일 선택: {p}")

        if not self._load_csv_process_list():
            return

        if self._is_closing or not self.csv_rows:
            return

        first_row = self.csv_rows[0]
        first_name = (first_row.get("Process_name") or "").strip()
        delay_sec = self._parse_csv_delay_seconds(first_name)

        if delay_sec is not None:
            self.update_stage_monitor(f"CSV 공정: 1/{len(self.csv_rows)} - {first_name} (대기 스텝)")
            return

        try:
            params = self._build_params_from_csv_row(first_row)
        except Exception as e:
            QMessageBox.warning(self, "CSV 레시피 오류", f"첫 번째 공정 파라미터가 잘못되었습니다:\n{e}")
            self.update_stage_monitor(f"CSV 공정: 1/{len(self.csv_rows)} - (오류)")
            return

        if self._is_closing:
            return

        self._apply_params_to_ui(params)
        name = params.get("process_name") or "STEP 1"
        self.update_stage_monitor(f"CSV 공정: 1/{len(self.csv_rows)} - {name}")

    @Slot()
    def _on_select_csv_clicked(self):
        """Process List용 CSV 파일 선택."""
        if self._is_closing:
            return

        if self.process_running or self.csv_mode or self._csv_delay_active:
            QMessageBox.warning(
                self,
                "변경 불가",
                "공정 진행 중에는 CSV 파일을 변경할 수 없습니다."
            )
            return

        if self._csv_dialog_open:
            return

        self._csv_dialog_open = True
        try:
            path, _ = QFileDialog.getOpenFileName(
                self,
                "공정 리스트 파일 선택",
                "",
                "레시피 파일 (*.xlsx *.xlsm *.csv *.tsv);;Excel (*.xlsx *.xlsm);;CSV (*.csv *.tsv);;All Files (*)"
            )
        finally:
            self._csv_dialog_open = False

        if not path:
            return

        self._start_csv_process_from_path(path)

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
            # 입력 소스만 바뀐다 — CSV/TSV/XLSX 를 같은 list[dict] 로 받는다.
            reader = load_table(
                self.csv_file_path,
                preferred_sheets=("Recipe", "recipe", "공정", "Sheet1"))

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

            # ✅ 실제 원인(message)을 그대로 남김 (CH1/CH2처럼)
            reason = (message or "").strip() or "PLC 통신 이상(재시작)"
            self._chat_notify_failed_now(reason, send_text=False)   # 저장만(카드+일반챗은 finished에서)

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
    def set_indicator(self, name, state):
        """센서 램프. state: True(녹) / False(적) / None(회색 — PLC 링크 다운으로 알 수 없음).
        ERP 기록은 True/False 일 때만(링크 다운 중엔 마지막 값을 그대로 둔다 — 거짓 OFF 보고 금지)."""
        if state is not None:
            try:
                if not hasattr(self, "_erp_indicators"):
                    self._erp_indicators = {}
                self._erp_indicators[str(name)] = bool(state)
            except Exception:
                pass

        frame_name = f"{name}_Indicator"
        frame = getattr(self.ui, frame_name, None)
        if frame is not None:
            color = "#9e9e9e" if state is None else ("#38d62f" if state else "#d6252f")
            frame.setStyleSheet(f"background: {color}; border-radius: 25px; border: 2px solid #333;")
        else:
            log_message_to_monitor("WARN", f"[set_indicator] '{frame_name}' 인디케이터가 UI에 없습니다.")

    # ==================== PLC 링크 ↔ 화면 ====================
    PLC_LINK_DOWN_TITLE = " — PLC 연결 끊김"

    def _plc_link_widgets(self):
        """링크 상태로 잠그는 화면 조작 위젯: PLC_COIL_MAP 의 모든 버튼 + Door_Button."""
        names = list(PLC_COIL_MAP.keys()) + ["Door_Button"]
        return [w for w in (getattr(self.ui, n, None) for n in names) if w is not None]

    MV_INTERLOCK_ABORT_MS = 1000

    @Slot(str, bool, object)
    def _on_plc_bit_changed(self, name: str, state: bool, prev):
        """PLC 코일/DI 전이 1곳: (1) 값이 바뀌면 로그 1줄(첫 값 prev=None 은 로그 없음)
        (2) 공정 중 MV 닫힘 안전 판정 — 판정도 여기서만 한다(중단 자체는 _abort_process_by_fault 하나).
        2026-09-17 CeO2 #1-2: RF 펄스 ON 직후 M00032 → M00003 이 내려갔는데 파이썬은 압력 대기에 갇혀 있었다."""
        state = bool(state)
        self._plc_bits[name] = state
        if prev is not None and bool(prev) != state:
            log_message_to_monitor("PLC", f"{name} {'ON' if prev else 'OFF'}→{'ON' if state else 'OFF'}")
        if name == "MV_INTERLOCK" and state and self._mv_itl_timer.isActive():
            self._mv_itl_timer.stop()
            ms = int((time.monotonic() - self._mv_itl_off_t) * 1000)
            log_message_to_monitor("PLC", f"PLC MV 인터락 순간 해제 후 복귀 ({ms} ms)")
            return
        if state or not self._mv_safety_armed():
            return
        # prev=None 인 첫 발행이라도 False 면 판정한다(링크 복구 뒤 MV 가 닫혀 있으면 중단해야 한다)
        if name == "MV_button":
            self._abort_process_by_fault("메인밸브 닫힘 (M00003 OFF)", detail=self._mv_detail())
        elif name == "MV_INTERLOCK" and not self._mv_itl_timer.isActive():
            self._mv_itl_off_t = time.monotonic()
            self._mv_itl_timer.start()

    def _mv_safety_armed(self) -> bool:
        """공정이 돌고 있고 STOP/ALL STOP/설비 이상 중단 시퀀스가 아직 시작되지 않았을 때만 판정한다."""
        return (self._process_active()
                and not (self._chat_user_stopped or self._chat_emergency_stopped
                         or getattr(self, "_fault_abort_active", False)))

    def _mv_detail(self) -> str:
        b = self._plc_bits
        def v(n):
            x = b.get(n); return "?" if x is None else ("ON" if x else "OFF")
        return (f"MV={v('MV_button')} MV_INTERLOCK={v('MV_INTERLOCK')} "
                f"Air={v('Air')} Gauge1={v('G1')} Gauge2={v('G2')}")

    @Slot()
    def _on_mv_interlock_timeout(self):
        if self._plc_bits.get("MV_INTERLOCK") is False and self._mv_safety_armed():
            self._abort_process_by_fault(
                f"메인밸브 인터락 해제 (M00032 OFF, {self.MV_INTERLOCK_ABORT_MS / 1000:g}초 지속)", detail=self._mv_detail())

    @Slot(bool)
    def _on_plc_link(self, up: bool, initial: bool = False):
        """PLC 링크 전이 1곳. down: 버튼 unchecked+잠금, 램프 회색, 히터 stale 즉시, 제목 접미사.
        up: 잠금 해제·제목 복원 — 값은 첫 폴링(캐시 비움)이 다시 채우고 히터 stale 은 update_heater_display 가 푼다.
        프로그램 경로의 PLC 쓰기(공정·히터 OFF 보류·ERP)는 건드리지 않는다 — 잠금은 화면 조작만이다."""
        up = bool(up)
        if getattr(self, "_plc_link_up", None) == up:
            return
        self._plc_link_up = up
        title = self.windowTitle()
        if title.endswith(self.PLC_LINK_DOWN_TITLE):
            title = title[:-len(self.PLC_LINK_DOWN_TITLE)]
        if up:
            for w in self._plc_link_widgets():
                w.setEnabled(True)
            self.setWindowTitle(title)
            log_message_to_monitor("정보", "PLC 링크 업 — 표시 재동기화")
            return
        for w in self._plc_link_widgets():
            w.blockSignals(True)
            try:
                w.setChecked(False)
            finally:
                w.blockSignals(False)
            w.setEnabled(False)
        for name in PLC_SENSOR_BITS:
            self.set_indicator(name, None)
        if HEATER_ENABLED:
            self._heater_set_stale(True, 0)
        self.setWindowTitle(title + self.PLC_LINK_DOWN_TITLE)
        if not initial:      # 생성자의 초기 down 은 전이가 아니라 초기 상태 — 경고를 찍지 않는다
            log_message_to_monitor("경고", "PLC 링크 다운 — 수동 조작 잠금, 표시 초기화")

    @Slot(str, bool)
    def update_ui_button_display(self, button_name, state):
        # ERP 리포터용 상태 기록 (표시 로직에는 영향 없음)
        try:
            if not hasattr(self, "_erp_valves"):
                self._erp_valves = {}
            label = str(button_name).replace("_button", "").replace("_Button", "")
            self._erp_valves[label] = bool(state)
        except Exception:
            pass

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

        # RF power(PLC DAC) 전용. 펄스는 아래 전용 컬럼으로 나간다.
        rf_for_p  = _avg(self._sum_rf_for, self._cnt_rf, params.get("rf_power"))
        rf_ref_p  = _avg(self._sum_rf_ref, self._cnt_rf, 0.0)

        # --- RF Pulse 설정값 (평균 아님) ---
        if params.get("use_rf_pulse") and float(params.get("rf_pulse_power") or 0) > 0:
            _f_in = params.get("rf_pulse_freq")
            _d_in = params.get("rf_pulse_duty")
            _f_rb = getattr(self, "_chk_rfpulse_freq_khz", None)
            _d_rb = getattr(self, "_chk_rfpulse_duty", None)
            _f = _f_in if _f_in not in (None, "") else _f_rb
            _d = _d_in if _d_in not in (None, "") else _d_rb
            rfp_freq = "" if _f in (None, "") else f"{float(_f):g}"
            rfp_duty = "" if _d in (None, "") else f"{int(_d)}"
        else:
            rfp_freq = ""
            rfp_duty = ""

        # 펄스 계측 평균 — 전용 누적기. 미사용이면 0.000 이 아니라 빈 문자열이다.
        if self._cnt_rfp:
            rfp_for_p = _fmt_float(self._sum_rfp_for / self._cnt_rfp)
            rfp_ref_p = _fmt_float(self._sum_rfp_ref / self._cnt_rfp)
        else:
            rfp_for_p = ""
            rfp_ref_p = ""

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
            "Heater Temp": (f"{self._chk_heater_sum / self._chk_heater_cnt:.1f}"
                if self._chk_heater_cnt else ""),     # ★
            "RF: For.P":        rf_for_p,          # ← 전체 공정 평균
            "RF: Ref. P":       rf_ref_p,          # ← 전체 공정 평균
            "DC: V":            dc_v,              # ← 전체 공정 평균
            "DC: I":            dc_i,              # ← 전체 공정 평균
            "DC: P":            dc_p,              # ← 전체 공정 평균
            # RF Pulse 설정값 — 계측값이 아니므로 평균 내지 않는다.
            #  입력값이 있으면 입력값, 비워 뒀으면 장비에서 되읽은 값.
            "RF Pulse: Freq[kHz]": rfp_freq,
            "RF Pulse: Duty[%]":   rfp_duty,
            "RF Pulse: For.P":     rfp_for_p,
            "RF Pulse: Ref. P":    rfp_ref_p,
        }

        return row
    
    def _reset_chk_stats(self):
        """ChK CSV 평균 계산용 누적값 + 샘플링 상태 초기화."""
        self._chk_heater_sum = 0.0      # ★
        self._chk_heater_cnt = 0        # ★

        # RF Pulse 설정 리드백 (평균이 아니라 '이번 공정에 실제로 걸린 값')
        self._chk_rfpulse_freq_khz = None
        self._chk_rfpulse_duty = None

        # RF Pulse 계측 전용 누적기 (RF power 와 분리 — 동시 사용이 가능하다)
        self._sum_rfp_for = 0.0
        self._sum_rfp_ref = 0.0
        self._cnt_rfp = 0

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
        self.ui.rf_pulse_checkbox.setChecked(False)
        self.ui.dc_power_checkbox.setChecked(False)
        self.ui.dc_delay_checkbox.setChecked(False)
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

        # --- RF Pulse 전용 칸 ---
        self.ui.rfp_power_edit.setPlainText("")
        self.ui.rfp_freq_edit.setPlainText("")
        self.ui.rfp_duty_edit.setPlainText("")
        self.ui.rfp_for_p_edit.setPlainText("0.0")
        self.ui.rfp_ref_p_edit.setPlainText("0.0")
        try:
            self._sync_rfpulse_inputs()
        except Exception:
            pass

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
                # 이 공정의 종료 처리는 여기서 끝났다 — 뒤늦게 finished 가 와도 카드를 또 내지 않는다
                self._finish_handled = True
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
        self.ui.select_csv_button.setEnabled(True)
        self.update_stage_monitor(stage_text)
        self._reset_process_ui_fields()
        # 반드시 맨 끝 — 중단 사유 로그는 해당 공정 파일에 남아야 한다
        clear_process_log_file()

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
        # ★ 중복/헛호출 가드. 컨트롤러 _stop_impl 의 '이미 정지' 분기는 request_stop 을
        #   부르는 어느 경로에서든 finished 를 낼 수 있어, 호출부 하나를 막아도 다른
        #   경로에서 종료 카드 2장 / 엉뚱한 카드가 재발한다. 여기서 뿌리를 막는다.
        #   플래그는 _chat_reset_run_state(수동 시작 / CSV STEP 시작마다)에서 False 가 된다 —
        #   스텝 시작에서 리셋되지 않으면 2번째 STEP 부터 종료 처리가 통째로 사라지므로
        #   리셋 위치를 옮기지 말 것.
        if getattr(self, "_finish_handled", False):
            _alive = (bool(getattr(self, "process_running", False))
                      or bool(getattr(self, "csv_mode", False))
                      or bool(getattr(self, "_csv_delay_active", False)))
            log_message_to_monitor(
                "정보",
                "finished 무시 — 이번 공정의 종료 처리는 이미 끝났음"
                + ("" if _alive else " (시작된 공정 없음)"))
            return
        self._finish_handled = True

        self.on_status_message("정보", "프로세스 종료중.")
        # 히터 소유권 해제 — 다음 공정/레시피 판정에 남지 않게 한다
        self._process_heater_claimed = False
        # MFC 폴링이 멈추므로 실측값도 함께 버린다(죽은 값 보고 방지)
        self._erp_meas = {}

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
                self.ui.select_csv_button.setEnabled(True)
                self.update_stage_monitor("CSV 공정 취소됨")
                self._reset_process_ui_fields()
                clear_process_log_file()   # 리스트 종료 — 이후 로그는 log.txt 로
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
                self.ui.select_csv_button.setEnabled(True)
                self.update_stage_monitor("CSV 공정 실패로 중단됨")
                self._reset_process_ui_fields()
                clear_process_log_file()   # 리스트 종료 — 이후 로그는 log.txt 로
                return

        # 🔻 여기 이하(단일 공정 종료 처리)는 그대로 유지
        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        self.ui.select_csv_button.setEnabled(True)
        self.update_stage_monitor("공정 종료")
        
        # ▶ 공통 UI 초기화
        self._reset_process_ui_fields()

        # 공정 로그 파일 해제 — 반드시 종료 처리의 맨 마지막.
        #  해제하지 않으면 공정이 끝난 뒤의 모든 로그(히터만 돌려도)가 이미 끝난
        #  공정의 .txt 에 계속 덧붙는다.
        #  CSV 리스트가 이어지는 중(_start_next_csv_step)에는 해제하지 않는다 —
        #  다음 STEP 이 set_process_log_file 로 새 파일을 만든다.
        clear_process_log_file()

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

        # process_time == 0 인 공정에서는 main shutter / 메인 공정 DELAY 스텝이
        # 시퀀스에 없으므로 update_process_time_timer 가 호출되지 않는다.
        # → shutter delay 가 0초가 되는 시점에 여기서 샘플링을 종료해야
        #   이후 셧다운 시퀀스의 RF/DC 램프다운 측정값이 평균에 섞이지 않는다.
        if seconds_left <= 0:
            try:
                pt = float((self._last_params or {}).get("process_time", 0) or 0)
            except Exception:
                pt = 0.0
            if pt <= 0:
                self._chk_sampling_enabled = False

        m, s = divmod(seconds_left, 60)
        text = f"{m:02d}:{s:02d}"
        self.ui.Shutter_delay_edit.setPlainText(text)

    def update_process_time_timer(self, seconds_left):
        m, s = divmod(seconds_left, 60)
        text = f"{m:02d}:{s:02d}"
        self.ui.process_time_edit.setPlainText(text)

        # ERP 전송용 원본 값. UI 칸은 "MM:SS" 로 덮이므로 숫자를 따로 들고 있어야 한다.
        try:
            self._erp_main_remain_sec = int(seconds_left)
            if getattr(self, "_erp_main_total_sec", 0) <= 0:
                # 메인 공정 첫 tick 을 총 시간으로 본다(이후 갱신하지 않는다)
                self._erp_main_total_sec = int(seconds_left)
        except Exception:
            pass

        # process time이 끝났으면 샘플링 종료
        if seconds_left <= 0:
            self._chk_sampling_enabled = False

    def update_mfc_flow_display(self, gas, value):
        # ERP 리포터용 실측값 기록 (표시 로직에는 영향 없음)
        #  이 함수는 가스 하나씩 불리므로 가스 이름을 키로 누적해 둔다.
        try:
            if not hasattr(self, "_erp_meas"):
                self._erp_meas = {}
            if not isinstance(self._erp_meas.get("flow"), dict):
                self._erp_meas["flow"] = {}
            self._erp_meas["flow"][str(gas)] = (
                None if value is None else float(value))
        except Exception:
            pass

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
        # ERP 리포터용 실측값 기록
        try:
            if not hasattr(self, "_erp_meas"):
                self._erp_meas = {}
            #  update_pressure 는 Signal(str) 이라 문자열로 온다. 유량(float)과
            #  같은 타입으로 맞춰 내보낸다(시그널 정의는 건드리지 않는다).
            try:
                self._erp_meas["pressure"] = (
                    None if pressure is None else float(pressure))
            except (TypeError, ValueError):
                self._erp_meas["pressure"] = None
        except Exception:
            pass

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

    @Slot(str)
    def _heater_atm_active(self) -> bool:
        """히터 분위기 제어가 MFC 를 쥐고 있는가. 히터 비활성이면 항상 False."""
        try:
            return bool(HEATER_ENABLED and self.heater_atmosphere.is_active())
        except Exception:
            return False

    def _on_mfc_flow_alert(self, msg: str):
        log_message_to_monitor("MFC(경고)", msg)
        if self.chat_chk and (self.process_running or self._heater_atm_active()):
            name = self.current_process_name or (
                "히터 분위기" if self._heater_atm_active() else "CHK")
            self.chat_chk.notify_text(f"⚠️ CHK 가스 유량 이상: {name} | {msg}")
            self.chat_chk.flush()

    @Slot(str)
    def _on_rfpulse_config_warning(self, msg: str):
        """장비 펄스 설정이 16 µs 규칙 위반(리드백) — 공정은 계속, 공정 중이면 챗 텍스트 1줄(_on_mfc_flow_alert 관례)."""
        if self.chat_chk and self.process_running:
            name = self.current_process_name or "CHK"
            self.chat_chk.notify_text(f"⚠️ CHK RF 펄스 설정 경고: {name} | {msg}")
            self.chat_chk.flush()

    @Slot(str)
    def _on_mfc_pressure_alert(self, msg: str):
        log_message_to_monitor("MFC(경고)", msg)
        if self.chat_chk and (self.process_running or self._heater_atm_active()):
            name = self.current_process_name or (
                "히터 분위기" if self._heater_atm_active() else "CHK")
            self.chat_chk.notify_text(f"⚠️ CHK 압력 이상: {name} | {msg}")
            self.chat_chk.flush()

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

    # ---------- RF Pulse 입력칸 활성/비활성 ----------
    @Slot(bool)
    def _on_rf_pulse_checkbox_toggled(self, checked: bool):
        """RF Pulse 를 안 쓰면 freq/duty 입력칸을 회색으로 잠근다.

        파워 칸(rfp_power_edit)은 RF/DC 파워 칸처럼 항상 활성이다 — 체크 안 하면 값이
        무시될 뿐이다(use_rf_pulse 일 때만 읽는다). freq/duty 만 체크에 따라 잠근다.
        RF power / DC power 와는 완전히 독립이다 — 서로 끄지 않는다.
        표시칸(rfp_for_p/rfp_ref_p)은 항상 ReadOnly 이므로 잠그지 않는다.
        """
        self._sync_rfpulse_inputs(checked)

    def _sync_rfpulse_inputs(self, enabled: bool = None):
        if enabled is None:
            try:
                enabled = self.ui.rf_pulse_checkbox.isChecked()
            except Exception:
                return
        # 파워 칸은 항상 활성(RF/DC 칸과 동일). freq/duty 만 체크에 따라 잠금.
        for name in ("rfp_freq_edit", "rfp_duty_edit"):
            w = getattr(self.ui, name, None)
            if w is None:
                continue
            try:
                w.setEnabled(bool(enabled))
                # 히터 패널과 같은 방식 — 안 쓰는 칸은 회색
                w.setStyleSheet("" if enabled else "background-color: #F0F0F0; color: #909090;")
            except Exception:
                pass

    @Slot(object)
    def _on_rfpulse_config_readback(self, rb):
        """장비에서 되읽은 실제 펄스 설정. CSV/로그에 남길 값으로 보관한다.

        freq/duty 를 비워 두면(=장비 현재값 유지) 입력값이 없어 기록할 게 없다.
        그때 이 값으로 채운다. rb 는 {'freq_khz': float|None, 'duty': int|None} —
        한쪽만 실패하면 그쪽만 None 이고, _build_chk_csv_row 에서 빈 문자열이 된다.
        """
        try:
            rb = dict(rb or {})
        except Exception:
            return
        _f = rb.get('freq_khz')
        _d = rb.get('duty')
        self._chk_rfpulse_freq_khz = None if _f is None else float(_f)
        self._chk_rfpulse_duty = None if _d is None else int(_d)

    @Slot(float, float)
    def update_rfpulse_status_display(self, forward_power, reflected_power):
        """RF Pulse 계측값 — 전용 칸과 전용 누적기를 쓴다.

        RF power(PLC ADC)와 완전히 분리한다. 두 장비를 동시에 돌릴 수 있으므로
        칸도 누적기도 섞으면 안 된다.
        체크박스를 보지 않는다 — 드라이버는 펄스 공정이 돌 때만 폴링하므로
        신호가 오는 것 자체가 '사용 중'이라는 뜻이다.
        """
        if not getattr(self, "process_running", False):
            return

        self.ui.rfp_for_p_edit.setPlainText(f"{forward_power:.2f}")
        self.ui.rfp_ref_p_edit.setPlainText(f"{reflected_power:.2f}")

        try:
            log_message_to_file(
                "RFPulse", f"MEAS For={forward_power:.2f}W, Ref={reflected_power:.2f}W")
        except Exception:
            pass

        if getattr(self, "_chk_sampling_enabled", False):
            self._sum_rfp_for += forward_power
            self._sum_rfp_ref += reflected_power
            self._cnt_rfp += 1

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
            self.request_plc_port_update.emit('Doordn_button', False)
            self.request_plc_port_update.emit('Doorup_button', True)
        else:
            self.request_plc_port_update.emit('Doorup_button', False)
            self.request_plc_port_update.emit('Doordn_button', True)
            
    def closeEvent(self, event):
        if self._csv_dialog_open:
            QMessageBox.warning(
                self,
                "종료 불가",
                "파일 선택 창이 열려 있는 동안에는 프로그램을 종료할 수 없습니다.\n먼저 파일 선택 창을 닫아주세요."
            )
            event.ignore()
            return

        if self.process_running or self.csv_mode or self._csv_delay_active:
            QMessageBox.warning(
                self,
                "종료 불가",
                "공정 진행 중에는 프로그램을 종료할 수 없습니다.\n먼저 STOP으로 공정을 종료한 뒤 다시 닫아주세요."
            )
            event.ignore()
            return

        # 가스를 문 채로 나가면 밸브가 열린 상태로 남는다. 먼저 풀고 나가게 한다.
        if (HEATER_ENABLED and getattr(self, "heater_atmosphere", None) is not None
                and self.heater_atmosphere.is_active()):
            QMessageBox.warning(
                self, "종료 불가",
                "히터 가스·압력이 잡혀 있거나 해제 중입니다.\n"
                "[가스 해제](냉각 대기 중) 또는 [취소](준비 중)로 해제가 끝난 뒤 다시 닫아주세요.")
            event.ignore()
            return

        # 히터가 살아 있으면 '무엇이 꺼지는지'를 종료 확인창에 명시한다.
        #   종료 자체는 막지 않는다 — 파이썬이 죽으면 래더 워치독(약 10초)이
        #   어차피 트립시키므로, 정상 경로로 끄고 나가는 편이 항상 더 안전하다.
        heater_warn = ""
        if HEATER_ENABLED:
            try:
                pv_txt = (self.ui.heater_pv_edit.text() or "").strip()
                pv_txt = f" (현재 {pv_txt}°C)" if pv_txt else ""
                if self.heater_recipe.is_running():
                    heater_warn = (
                        f"\n\n[주의] 히터 레시피 실행 중 "
                        f"{self.heater_recipe.current_step_no()}"
                        f"/{self.heater_recipe.total_steps()} 스텝{pv_txt}\n"
                        "종료하면 레시피가 중단되고 히터가 꺼집니다."
                    )
                elif bool((self.plc_controller.get_heater_status() or {}).get('run')):
                    heater_warn = (
                        f"\n\n[주의] 히터 운전 중{pv_txt}\n"
                        "종료하면 히터가 꺼집니다."
                    )
            except Exception:
                heater_warn = ""

        reply = QMessageBox.question(
            self,
            '종료 확인',
            '정말로 프로그램을 종료하시겠습니까?' + heater_warn,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if reply != QMessageBox.StandardButton.Yes:
            event.ignore()
            return

        self._is_closing = True
        self.ui.select_csv_button.setEnabled(False)
        self._stop_csv_delay_timer()

        # ★ 히터 정지 (포트를 닫기 전에 정상 경로로 먼저 끈다)
        #   - PLC.cleanup()에도 동일한 안전장치가 있지만,
        #     그쪽은 포트를 닫는 중이라 실패할 수 있어 여기서 한 번 더 끈다.
        #   - HEATER_RUN이 꺼지면 PLC 래더 H9가 DAC 출력을 0으로 강제한다.
        if HEATER_ENABLED:
            try:
                self.heater_ramp.stop(restore_rate=False)
            except Exception:
                pass
            try:
                # 가스를 넣어 두었으면 먼저 되돌린다(기다리지는 않는다 — best effort)
                if self.heater_atmosphere.is_active():
                    self.heater_atmosphere.release("프로그램 종료")
            except Exception:
                pass
            try:
                # 레시피가 돌고 있으면 먼저 멈춘다(내부에서 히터 OFF까지 처리한다)
                self.heater_recipe.stop("프로그램 종료")
            except Exception:
                pass
            try:
                self._heater_ui_timer.stop()
            except Exception:
                pass
            try:
                self._heater_logger.stop()
                clear_heater_log_file()
            except Exception:
                pass
            try:
                self.request_heater_run.emit(False)   # 표시는 폴링/PLC cleanup 이 맞춘다
            except Exception:
                pass

        log_message_to_monitor("정보", "프로그램 종료를 시작합니다...")

        # worker 정리를 각 worker 자신의 스레드에서 먼저 수행
        self._invoke_worker_blocking(self.process_controller, "teardown")
        self._invoke_worker_blocking(self.plc_controller, "cleanup")
        self._invoke_worker_blocking(self.mfc_controller, "cleanup")
        self._invoke_worker_blocking(self.dcpower_controller, "close_connection")
        self._invoke_worker_blocking(self.rfpower_controller, "close_connection")
        self._invoke_worker_blocking(self.rfpulse_controller, "cleanup")

        threads = [
            self.process_thread,
            self.plc_thread,
            self.mfc_thread,
            self.dcpower_thread,
            self.rfpower_thread,
            self.rfpulse_thread,
        ]

        for thread in threads:
            thread.quit()

        for thread in threads:
            thread_name = thread.objectName()
            log_message_to_monitor("정보", f"{thread_name} 스레드 종료 대기 중...")
            if not thread.wait(3000):
                log_message_to_monitor("경고", f"{thread_name} 스레드가 시간 내에 종료되지 않았습니다.")

        try:
            if self.chat_chk:
                self.chat_chk.shutdown()
        except Exception:
            pass

        log_message_to_monitor("정보", "모든 스레드가 종료되었습니다. 프로그램을 닫습니다.")
        event.accept()

    # ============= csv 공정 =============
    @staticmethod
    def _check_rfpulse_pulse_range(freq_khz, duty, where: str) -> None:
        """펄스 주파수/듀티가 장비 범위 안인지 시작 전에 확인한다. None 은 통과(장비 현재값 유지).

        CESAR 1310 매뉴얼: 펄스 주파수 1 Hz~30 kHz, 듀티 1~99 %, 그리고 최소 ON/OFF 시간 16 µs(명령 96).
        16 µs 규칙은 둘 다 주어졌을 때만 여기서 잰다(rfpulse_pulse_edge_violation). 한쪽이 None(장비값 유지)이면
        START 뒤 리드백에서 같은 함수로 경고한다 — 장비는 위반 설정도 CSR 0 으로 받아들이므로(2026-09-17 20 kHz·80 %)
        CSR 51 에 기대지 않는다.
        """
        if freq_khz is not None:
            _hz = float(freq_khz) * 1000.0
            _lo = float(RFPULSE_PULSE_FREQ_MIN_HZ)
            _hi = float(RFPULSE_PULSE_FREQ_MAX_HZ)
            if not (_lo <= _hz <= _hi):
                raise ValueError(
                    f"[{where}] RF Pulse 주파수 {float(freq_khz):g}kHz 가 장비 범위 "
                    f"{_lo / 1000.0:g}~{_hi / 1000.0:g}kHz(CESAR 1310, RFPULSE_PULSE_FREQ_MAX_HZ)를 벗어납니다.")
        if duty is not None:
            _d = int(duty)
            if not (int(RFPULSE_DUTY_MIN) <= _d <= int(RFPULSE_DUTY_MAX)):
                raise ValueError(
                    f"[{where}] RF Pulse 듀티 {_d}% 가 범위 "
                    f"{int(RFPULSE_DUTY_MIN)}~{int(RFPULSE_DUTY_MAX)}% 를 벗어납니다.")
        if freq_khz is not None and duty is not None:
            _why = rfpulse_pulse_edge_violation(int(round(float(freq_khz) * 1000.0)), int(duty))
            if _why:
                raise ValueError(f"[{where}] RF Pulse 설정 {_why} (CESAR 1310 최소 ON/OFF 시간, RFPULSE_PULSE_MIN_EDGE_US).")

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

        if proc_time is None:
            proc_time = 0.0
        if sh_delay is None:
            sh_delay = 0.0

        if proc_time < 0:
            raise ValueError(f"[{process_name or 'STEP'}] process_time은 0 이상이어야 합니다.")
        if sh_delay < 0:
            raise ValueError(f"[{process_name or 'STEP'}] shutter_delay는 0 이상이어야 합니다.")
        if proc_time <= 0 and sh_delay <= 0:
            raise ValueError(
                f"[{process_name or 'STEP'}] shutter_delay와 process_time 중 "
                f"하나는 0보다 커야 합니다."
            )
        
        # ---- 파워(선택) : use_*가 비어있으면 OFF로 간주 ----
        use_dc = _b("use_dc_power", False)
        use_rf = _b("use_rf_power", False)
        # ---- RF Pulse(선택) : 컬럼이 아예 없는 기존 레시피는 자동 OFF (하위 호환) ----
        #  RF power / RF Pulse / DC power 는 서로 독립이다 — 동시에 켤 수 있다.
        use_rf_pulse = _b("use_rf_pulse", False)

        # ---- 히터(선택) : 컬럼이 없으면 자동 OFF → 기존 CSV 하위 호환 ----
        use_heater  = _b("use_heater", False)
        heater_temp = _f("heater_temp", required=False, default=0.0) or 0.0
        if use_heater and heater_temp <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] use_heater=ON 인데 heater_temp가 0 이하입니다.")
        if use_heater and heater_temp > HEATER_MAX_TEMP:
            raise ValueError(f"[{process_name or 'STEP'}] heater_temp가 상한({HEATER_MAX_TEMP:.0f}°C)을 넘습니다.")

        # RAMP 속도(°C/min). 컬럼이 없거나 0이면 config 기본값을 쓴다.
        heater_ramp = _f("heater_ramp", required=False, default=0.0) or 0.0
        if use_heater:
            if heater_ramp <= 0:
                heater_ramp = float(HEATER_RAMP_RATE_C_PER_MIN)
            elif heater_ramp < 6.0:
                # PLC 램프는 1카운트/초 = 6°C/min 단위라 그 아래는 표현되지 않는다
                self.on_status_message(
                    "히터(경고)",
                    f"[{process_name or 'STEP'}] heater_ramp {heater_ramp:g} → 6 °C/min 으로 보정")
                heater_ramp = 6.0
            elif heater_ramp > 60.0:
                raise ValueError(
                    f"[{process_name or 'STEP'}] heater_ramp가 상한(60°C/min)을 넘습니다.")

        # DC Power 안정화 대기(선택) : 컬럼이 없거나 비어 있으면 OFF
        use_dc_delay = _b("use_dc_delay", False)

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

        # RF Pulse 값 — 파워는 사용 시 필수, freq/duty 는 비어 있으면 None(장비 현재값 유지)
        rf_pulse_power = 0.0
        rf_pulse_freq = None
        rf_pulse_duty = None
        if use_rf_pulse:
            _v = _f("rf_pulse_power", required=False, default=0.0) or 0.0
            if _v <= 0:
                raise ValueError(
                    f"[{process_name or 'STEP'}] use_rf_pulse=ON 인데 "
                    f"rf_pulse_power가 비어있거나 0 이하입니다.")
            if _v > float(RFPULSE_MAX_POWER):
                raise ValueError(
                    f"[{process_name or 'STEP'}] rf_pulse_power {float(_v):g}W 가 "
                    f"장비 상한 {float(RFPULSE_MAX_POWER):g}W(RFPULSE_MAX_POWER)를 넘습니다.")
            rf_pulse_power = float(_v)
            _fq = _f("rf_pulse_freq", required=False, default=None)
            _dt = _f("rf_pulse_duty", required=False, default=None)
            rf_pulse_freq = float(_fq) if _fq not in (None, "") else None
            rf_pulse_duty = int(float(_dt)) if _dt not in (None, "") else None
            self._check_rfpulse_pulse_range(rf_pulse_freq, rf_pulse_duty, process_name or "STEP")

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

            # ▼ RF Pulse — 컬럼이 없으면 전부 미사용으로 떨어진다
            "use_rf_pulse": bool(use_rf_pulse),
            "rf_pulse_power": float(rf_pulse_power),
            "rf_pulse_freq": rf_pulse_freq,
            "rf_pulse_duty": rf_pulse_duty,

            "use_g1": bool(use_g1),
            "use_g2": bool(use_g2),

            "use_dc_delay": bool(use_dc_delay and use_dc),
            "use_heater":   bool(use_heater and HEATER_ENABLED),   # ★
            "heater_temp":  float(heater_temp),                    # ★
            "heater_ramp":  float(heater_ramp),                    # ★ °C/min

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

        # DC Power 안정화 대기 사용 여부
        try:
            self.ui.dc_delay_checkbox.blockSignals(True)
            self.ui.dc_delay_checkbox.setChecked(bool(params.get("use_dc_delay", False)))
        finally:
            self.ui.dc_delay_checkbox.blockSignals(False)

        # RF 파워
        rf_power = float(params.get("rf_power") or 0.0)
        rf_pulse_power = float(params.get("rf_pulse_power") or 0.0)
        use_rf_pulse = bool(params.get("use_rf_pulse")) and rf_pulse_power > 0
        # 두 체크박스는 서로 독립이다 — 한쪽이 다른 쪽을 끄지 않는다.
        try:
            self.ui.rf_power_checkbox.blockSignals(True)
            self.ui.rf_pulse_checkbox.blockSignals(True)
            self.ui.rf_power_checkbox.setChecked(rf_power > 0)
            self.ui.rf_pulse_checkbox.setChecked(use_rf_pulse)
        finally:
            self.ui.rf_power_checkbox.blockSignals(False)
            self.ui.rf_pulse_checkbox.blockSignals(False)
        self.ui.RF_power_edit.setPlainText(f"{rf_power:.1f}" if rf_power > 0 else "0")

        # RF Pulse 전용 칸
        self.ui.rfp_power_edit.setPlainText(
            f"{rf_pulse_power:.1f}" if rf_pulse_power > 0 else "")
        _fq = params.get("rf_pulse_freq")
        _dt = params.get("rf_pulse_duty")
        self.ui.rfp_freq_edit.setPlainText("" if _fq in (None, "") else str(_fq))
        self.ui.rfp_duty_edit.setPlainText("" if _dt in (None, "") else str(_dt))
        try:
            self._sync_rfpulse_inputs()
        except Exception:
            pass

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

        # 히터: CSV 레시피의 목표 온도를 화면에도 반영
        if HEATER_ENABLED:
            ht = float(params.get("heater_temp", 0.0) or 0.0)
            if ht > 0:
                self.ui.heater_sv_edit.setText(f"{ht:g}")   # QLineEdit
    
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
            self.ui.select_csv_button.setEnabled(True)
            self.update_stage_monitor("CSV 공정 완료")

            # ▶ 공통 UI 초기화
            self._reset_process_ui_fields()
            # 리스트 정상 완료 — 이후 로그가 마지막 STEP 파일에 덧붙지 않게 해제
            clear_process_log_file()
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

        # 이번 STEP 이 히터를 소유하는가 (히터 레시피와의 충돌 판정 기준)
        self._process_heater_claimed = bool(
            params.get("use_heater") and float(params.get("heater_temp") or 0) > 0)
        if HEATER_ENABLED and self.heater_recipe.is_running() \
                and not self._process_heater_claimed:
            log_message_to_monitor(
                "정보",
                "[히터] 히터 레시피가 제어 중입니다. 이번 공정은 히터를 제어하지 않습니다.")

        # 이번 공정의 히터 설정을 로그 머리말에 남긴다
        self._log_heater_header(params)

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
        self.ui.select_csv_button.setEnabled(False)

        self.clear_plc_fault.emit()              # ✅ 추가: 스텝 시작마다 PLC 실패 래치 초기화

        try:
            self.erp.run_start(
                self.current_process_name or params.get("process_note", "") or "CHK 공정",
                params,
            )
        except Exception:
            pass

        self.request_process_start.emit(params)

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

    # 같은 예외가 폭주할 때 로그와 챗 알림이 수백 건 나가는 것을 막는다.
    # 실제 사고(2026-09-01): PLC 폴링 슬롯에서 200ms마다 IndexError가 터져
    # 로그 100여 줄 + 구글챗 40건이 몇 초 만에 나갔다.
    _EXC_SUPPRESS_SEC = 30.0
    _exc_seen: dict = {}          # key -> [마지막 통과 시각, 그 뒤 억제된 건수]

    def _exc_key(exctype, tb) -> str:
        """예외 종류 + 마지막 프레임 위치. 같은 자리에서 반복되면 같은 키."""
        try:
            last = traceback.extract_tb(tb)[-1]
            return f"{exctype.__name__}:{last.filename}:{last.lineno}"
        except Exception:
            return getattr(exctype, "__name__", "?")

    def _exc_gate(key: str):
        """(내보낼지, 직전까지 억제된 건수)"""
        now = time.monotonic()
        rec = _exc_seen.get(key)
        if rec is None or now - rec[0] >= _EXC_SUPPRESS_SEC:
            skipped = rec[1] if rec else 0
            _exc_seen[key] = [now, 0]
            return True, skipped
        rec[1] += 1
        return False, 0

    def _report_exc(prefix: str, exctype, value, tb):
        try:
            ok, skipped = _exc_gate(_exc_key(exctype, tb))
        except Exception:
            ok, skipped = True, 0
        if not ok:
            return
        tail = f"  (같은 예외 {skipped}건 생략)" if skipped else ""
        try:
            tb_text = "".join(traceback.format_exception(exctype, value, tb))
            log_message_to_monitor("ERROR", f"[UNHANDLED{prefix}] {tb_text}{tail}")
        except Exception:
            pass
        who = prefix.lstrip(":") or "메인"
        _send_fatal_to_chat(f"❌ CHK 프로그램 예외({who}): {value!r}{tail}")

    def _excepthook(exctype, value, tb):
        _report_exc("", exctype, value, tb)

    sys.excepthook = _excepthook

    def _thread_excepthook(args):
        _report_exc(f":{args.thread.name}", args.exc_type, args.exc_value, args.exc_traceback)

    threading.excepthook = _thread_excepthook

    dlg = MainDialog()
    app._main_dlg = dlg
    dlg.show()
    sys.exit(app.exec())
