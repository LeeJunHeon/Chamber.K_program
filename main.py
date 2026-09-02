import sys
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
from lib.config import PLC_COIL_MAP
from lib.logger import (
    set_monitor_widget,
    log_message_to_monitor,
    set_process_log_file,
    log_message_to_file,
    append_chk_csv_row,
)
from reporter import ErpReporter
from controller.process_controller import SputterProcessController
from controller.chat_notifier import ChatNotifier
from device.PLC import PLCController
from device.MFC import MFCController
from device.DCpower import DCPowerController
from device.RFpower import RFPowerController
from lib.config import (PLC_COIL_MAP, DC_POWER_DELAY_SEC,
                        HEATER_ENABLED, HEATER_MAX_TEMP,
                        HEATER_MV_LIMIT, HEATER_LOG_ENABLED,
                        HEATER_LOG_PERIOD_MS, HEATER_RECIPE_DIR,
                        HEATER_RAMP_RATE_C_PER_MIN, HEATER_SOAK_TOLERANCE,
                        heater_est_current)
from lib.recipe_io import load_table
from controller.heater_recipe import HeaterRecipeRunner
from lib.heater_logger import HeaterCsvLogger

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
        self._chat_errors: list[str] = []
        self._chat_fail_notified: bool = False   # ✅ 실패 원인 일반채팅 중복 방지
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

        # --- 히터 레시피 러너 / CSV 로거 (스퍼터 공정과 무관한 독립 경로) ---
        self.heater_recipe = HeaterRecipeRunner(self.plc_controller, self)
        self._heater_logger = HeaterCsvLogger()
        self._heater_log_last_ms = 0.0
        self._heater_run_prev = False
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
        self.process_thread.start()

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
                self.request_plc_emergency_stop.emit()
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
                btn = self.ui.heater_onoff_button
                if want:
                    # 목표 온도가 함께 왔으면 먼저 반영한다(빈 SV로 인한 팝업 방지)
                    val = args.get("value")
                    if val is not None and str(val).strip() != "":
                        w = self.ui.heater_sv_edit
                        if hasattr(w, "setPlainText"):
                            w.setPlainText(str(val))
                        else:
                            w.setText(str(val))
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
                    st = getattr(self.plc_controller, "_heater_last", None) or {}
                    if not st.get("itl"):
                        raise RuntimeError("히터 인터락 미충족 (TC/DAC 모듈 상태 확인 필요)")

                if btn.isChecked() == want:
                    # 이미 원하는 상태 — setChecked 가 무시되므로 명시적으로 알린다
                    raise RuntimeError(f"히터가 이미 {'ON' if want else 'OFF'} 상태입니다")
                btn.setChecked(want)
                if btn.isChecked() != want:
                    raise RuntimeError("히터 상태 변경이 장비에서 거부되었습니다")

            elif name == "RECIPE_PROCESS_RUN":
                # 웹에서 만든 공정 레시피를 CSV로 저장하고 기존 CSV 실행 경로를 그대로 사용한다
                import csv as _csv, tempfile, os as _os
                rows = args.get("rows") or []
                if not rows:
                    raise RuntimeError("레시피 행이 없습니다")
                cols = ["Process_name", "Ar", "Ar_flow", "O2", "O2_flow",
                        "working_pressure", "process_time", "shutter_delay",
                        "use_rf_power", "rf_power", "use_dc_power", "dc_power",
                        "use_dc_delay", "use_heater", "heater_temp", "gun1", "gun2"]
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
                if (self.process_running or self.csv_mode
                        or getattr(self, "_csv_delay_active", False)):
                    raise RuntimeError("스퍼터 공정이 진행 중입니다. 공정 종료 후 실행하세요.")
                import csv as _csv, tempfile, os as _os
                rows = args.get("rows") or []
                if not rows:
                    raise RuntimeError("레시피 행이 없습니다")
                cols = ["step", "target_c", "ramp_c_per_min", "ramp_min",
                        "soak_min", "repeat"]
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

                # MFC 실측값 (update_mfc_*_display 가 채운다)
                _meas = getattr(self, "_erp_meas", {}) or {}
                _flow = _meas.get("flow") or {}
                _press = _meas.get("pressure")

                # 계측 그룹 — value=계측값(PV), setpoint=설정값(SV)
                groups = [
                    {"label": "전원", "items": [
                        {"label": "DC Power", "value": _w("Power_edit"),
                         "setpoint": _w("DC_power_edit"), "unit": "W"},
                        {"label": "Voltage", "value": _w("Voltage_edit"), "unit": "V"},
                        {"label": "Current", "value": _w("Current_edit"), "unit": "A"},
                    ]},
                    {"label": "RF", "items": [
                        {"label": "for.P", "value": _w("for_p_edit"),
                         "setpoint": _w("RF_power_edit"), "unit": "W"},
                        {"label": "ref.P", "value": _w("ref_p_edit"), "unit": "W"},
                        {"label": "offset / param",
                         "value": f'{_w("offset_edit")} / {_w("param_edit")}'},
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
                        "output": _w("heater_mv_label"),
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
                            "stepNo": int(getattr(self, "csv_index", -1)) + 1,
                            "total": len(getattr(self, "csv_rows", []) or []),
                            "active": bool(getattr(self, "csv_mode", False)),
                            "steps": [
                                str((r or {}).get("Process_name") or f"STEP{i+1}")
                                for i, r in enumerate(getattr(self, "csv_rows", []) or [])
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
                    "valves": dict(getattr(self, "_erp_valves", {})),
                }

                # 진행률 계산용 총 공정 시간(초)
                if running:
                    try:
                        total_min = float(_w("process_time_edit") or 0)
                        state["process"] = {
                            "name": getattr(self, "current_process_name", "") or "",
                            "totalSec": int(total_min * 60),
                        }
                    except Exception:
                        pass

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
        self.ui.ALL_STOP_button.clicked.connect(self.request_plc_emergency_stop.emit)
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
            self.request_heater_reset.connect(self.plc_controller.reset_heater_fault)

            # (2) PLC -> UI : 200ms 폴링으로 올라오는 히터 상태를 화면에 반영
            self.plc_controller.update_heater_status.connect(self.update_heater_display)
            self.plc_controller.heater_fault.connect(self._on_heater_fault)

            # (3) UI 위젯 -> 핸들러
            self.ui.heater_apply_button.clicked.connect(self._on_heater_apply_clicked)
            self.ui.heater_onoff_button.toggled.connect(self._on_heater_onoff_toggled)

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
            self.ui.heater_hold_button.clicked.connect(self._on_heater_hold_clicked)
            self.ui.heater_skip_button.clicked.connect(self._on_heater_skip_clicked)

            # (4) ★ ProcessController -> PLC : 레시피(CSV/단일 공정)에서
            #     HEATER_SET 스텝이 실행될 때 목표 온도와 운전을 PLC로 보낸다.
            #     이 연결이 없으면 레시피의 히터 스텝이 아무 동작도 하지 않는다.
            self.process_controller.set_heater_target.connect(self.plc_controller.set_heater_target)
            self.process_controller.set_heater_run.connect(self.plc_controller.set_heater_run)
        else:
            # 히터 비활성(config_user.json의 HEATER_ENABLED=false) 시
            # 조작 위젯을 잠가 오조작을 막는다. 표시용 위젯은 그대로 둔다.
            for w in ("heater_apply_button", "heater_onoff_button", "heater_sv_edit"):
                getattr(self.ui, w).setEnabled(False)

    # ==================== Google Chat 알림 헬퍼 (CH.K) ====================
    def _chat_reset_run_state(self):
        self._chat_user_stopped = False
        self._chat_errors = []
        self._chat_fail_notified = False
        self._chat_fail_reason = ""
        self._erp_run_ended = False   # ERP: 이번 공정 run_end 미전송 상태로 초기화

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
            plc_limit = float((self.plc_controller._heater_last or {}).get('sv_limit') or 0.0)
        except Exception:
            plc_limit = 0.0
        limit = min(HEATER_MAX_TEMP, plc_limit) if plc_limit > 0 else HEATER_MAX_TEMP
        if v < 0 or v > limit:
            QMessageBox.warning(self, "입력 오류",
                                f"목표 온도는 0 ~ {limit:.0f}°C 범위여야 합니다.")
            return None
        return v

    @Slot()
    def _on_heater_apply_clicked(self):
        v = self._read_heater_sv_input()
        if v is None:
            return
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
            btn = self.ui.heater_onoff_button
            btn.blockSignals(True)          # setChecked 재진입 방지
            btn.setChecked(not checked)     # 눌리기 전 상태로 되돌린다
            btn.setText("OFF" if btn.isChecked() else "ON")
            btn.blockSignals(False)
            return

        if checked:
            st = self.plc_controller._heater_last or {}
            if not st.get('itl'):
                QMessageBox.warning(self, "히터 시작 불가",
                    "히터 인터락이 미충족 상태입니다.\nTC/DAC 모듈 상태를 확인하세요.")
                self.ui.heater_onoff_button.setChecked(False)
                return
            v = self._read_heater_sv_input()
            if v is None:
                self.ui.heater_onoff_button.setChecked(False)
                return
            self.request_heater_target.emit(v)
            self.request_heater_run.emit(True)
            self.ui.heater_onoff_button.setText("OFF")
        else:
            self.request_heater_run.emit(False)
            self.ui.heater_onoff_button.setText("ON")

    # ==================== 히터 CSV 로깅 / 레시피 ====================
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

    def _heater_log_tick(self, st: dict):
        """히터 운전 구간 동안만 CSV를 남긴다. 화면 갱신 로직과는 독립."""
        if not HEATER_LOG_ENABLED:
            return
        run = bool(st.get('run'))
        try:
            now = time.monotonic() * 1000.0

            if run and not self._heater_run_prev:
                path = self._heater_logger.start(self._heater_log_prefix())
                if path is not None:
                    log_message_to_monitor("정보", f"히터 로그 시작: {path}")
                self._heater_log_last_ms = 0.0

            if run and (self._heater_log_last_ms == 0.0
                        or now - self._heater_log_last_ms >= HEATER_LOG_PERIOD_MS):
                self._heater_log_last_ms = now
                self._heater_logger.write_row(st, self._heater_log_note())

            if (not run) and self._heater_run_prev:
                # 정지 직후 마지막 한 행을 남기고 파일을 닫는다
                self._heater_logger.write_row(st, self._heater_log_note())
                self._heater_logger.stop()
        except Exception:
            pass
        finally:
            self._heater_run_prev = run

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
        # 실행 중이면 중단 확인
        if self.heater_recipe.is_running():
            reply = QMessageBox.question(
                self, "레시피 중단",
                "히터 레시피를 중단하고 히터를 끕니다. 계속할까요?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                self.heater_recipe.stop("사용자 중단")
            return

        # 스퍼터 공정과 동시 실행은 허용하지 않는다
        if self.process_running or self.csv_mode or getattr(self, "_csv_delay_active", False):
            QMessageBox.warning(self, "실행 불가",
                                "스퍼터 공정이 진행 중입니다. 공정 종료 후 실행하세요.")
            return

        start_dir = HEATER_RECIPE_DIR or str(Path.cwd())
        path, _ = QFileDialog.getOpenFileName(
            self, "히터 레시피 파일 선택", start_dir,
            "CSV Files (*.csv);;All Files (*)")
        if not path:
            return
        if not self.heater_recipe.load(path):
            QMessageBox.warning(self, "레시피 오류",
                                "레시피를 불러오지 못했습니다. 로그를 확인하세요.")
            return

        steps = self.heater_recipe.steps()
        body = "\n".join(f"{i}. {s.describe()}" for i, s in enumerate(steps, 1))
        reply = QMessageBox.question(
            self, "히터 레시피 실행",
            f"{Path(path).name}\n\n{body}\n\n이대로 실행할까요?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._rebuild_heater_step_list()
        if self.heater_recipe.start():
            self.ui.heater_recipe_button.setText("중단")
            self._refresh_heater_progress()

    # ---------- PZ400 스타일 표시부 ----------
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
            sv_show = cur_sv if running else st.get('sv')
            if sv_show is None:
                ui.heater_sv_big.setText("---")
            else:
                ui.heater_sv_big.setText(f"{float(sv_show):.1f}")

            # 편차 — 정지 중에는 의미가 없으므로 비운다
            if running and pv is not None and cur_sv is not None:
                dev = float(pv) - float(cur_sv)
                ui.heater_dev_label.setText(f"\u0394{dev:+.1f}")
                col = "#2e7d32" if abs(dev) <= HEATER_SOAK_TOLERANCE else "#6b7280"
                if getattr(self, "_heater_dev_col", None) != col:
                    self._heater_dev_col = col
                    ui.heater_dev_label.setStyleSheet(
                        f"QLabel {{border: none; background: transparent; "
                        f"color: {col}; font-size: 9pt;}}")
            else:
                ui.heater_dev_label.setText("")

            # 출력 바 — 운전 중이 아니면 0
            ui.heater_out_bar.setValue(
                int(st.get('mv_pct') or 0) if running else 0)

            # 목표 입력칸이 비어 있으면 PLC 에 들어 있는 목표로 한 번만 채운다.
            #  사용자가 입력한 뒤에는 절대 덮어쓰지 않는다(_heater_sv_seeded).
            if not getattr(self, "_heater_sv_seeded", False):
                try:
                    sv0 = float(st.get('sv') or 0.0)
                    if sv0 > 0 and not ui.heater_sv_edit.text().strip():
                        ui.heater_sv_edit.setText(f"{sv0:g}")
                        self._heater_sv_seeded = True
                except Exception:
                    pass

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
                # (승온 시간과 유지 시간이 둘 다 '분'이라 ↗ 로 구분한다)
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
                seg = f"STEP {pg.get('stepNo', 0)}/{pg.get('total', 0)}"
                if int(pg.get("repeat", 1) or 1) > 1:
                    seg += f" · ×{pg.get('cycle', 1)}/{pg.get('repeat', 1)}"
                ui.heater_seg_label.setText(seg)
                # 승온 구간은 추정치라 계산 불가(-1)면 --:-- 로 둔다
                step_remain = int(pg.get("stepRemainSec", 0) or 0)
                ui.heater_time_label.setText(
                    "--:--" if step_remain < 0 else _fmt_hms_sec(step_remain))
                pct = float(pg.get("percent") or 0.0)
                ui.heater_prog_bar.setValue(int(pct))
                total_est = int(pg.get("totalEstSec") or 0)
                remain = max(0, total_est - int(pg.get("elapsedSec") or 0))
                ui.heater_prog_label.setText(
                    f"전체 {pct:.0f}% · 남음 {_fmt_hms_sec(remain)}"
                    if total_est > 0 else "")
            else:
                ui.heater_seg_label.setText("레시피 없음")
                ui.heater_time_label.setText("--:--:--")
                ui.heater_prog_bar.setValue(0)
                ui.heater_prog_label.setText("")

            self._highlight_heater_step()
            self._sync_heater_recipe_buttons()
        except Exception:
            pass

    def _sync_heater_recipe_buttons(self):
        """레시피가 돌 때만 [일시정지]/[건너뛰기]를 쓸 수 있게 한다."""
        try:
            running = self.heater_recipe.is_running()
            held = self.heater_recipe.is_held()
            self.ui.heater_hold_button.setEnabled(running)
            self.ui.heater_skip_button.setEnabled(running)
            self.ui.heater_hold_button.setText("재개" if (running and held) else "일시정지")
        except Exception:
            pass

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
        self.ui.heater_recipe_button.setText(f"{cur}/{total}")
        self._sync_heater_recipe_buttons()
        self.update_stage_monitor(self._heater_recipe_stage_text(cur, total, desc))

    def _heater_recipe_stage_text(self, cur: int, total: int, desc: str) -> str:
        """진행률과 남은 시간까지 한 줄로 만든다.
        예: 히터 레시피 2/3 (반복 1/2) · 610°C 유지 · 전체 43% · 남음 2:15:40
        """
        base = f"히터 레시피 {cur}/{total} - {desc}"
        try:
            pg = self.heater_recipe.progress()
            total_est = int(pg.get("totalEstSec") or 0)
            elapsed = int(pg.get("elapsedSec") or 0)
            if total_est > 0:
                remain = max(0, total_est - elapsed)
                base += (f" · 전체 {pg.get('percent', 0):.0f}%"
                         f" · 남음 {_fmt_hms_sec(remain)}")
        except Exception:
            pass
        return base

    @Slot(bool, str)
    def _on_heater_recipe_finished(self, ok: bool, reason: str):
        self.ui.heater_recipe_button.setText("레시피")
        self._sync_heater_recipe_buttons()
        # 목록은 남겨 두고 강조만 해제한다(무엇을 돌렸는지 확인용)
        self._refresh_heater_progress()
        self.update_stage_monitor(f"히터 레시피 {'완료' if ok else '중단'}: {reason}")
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
        # ERP 리포터용 히터 상세 상태 기록
        try:
            self._erp_heater = dict(st or {})
        except Exception:
            pass

        # --- 현재 온도 (QLineEdit이므로 setText 사용) ---
        if st.get('ok') and st.get('pv') is not None:
            self.ui.heater_pv_edit.setText(f"{st['pv']:.1f}")
        else:
            # 단선/모듈이상 시 PLC가 hFFFF를 쓰고 파이썬은 -1로 읽는다
            self.ui.heater_pv_edit.setText("")      # 빈 칸 → placeholder "--.-" 노출

        # --- 상태 문구 (우선순위: 이상 > 인터락 > 운전 > 정지) ---
        #     빨간색 3종은 PLC 래더에서 SET 코일로 래치되므로
        #     원인이 사라져도 [적용]/재시작만으로는 안 풀린다.
        if st.get('fault'):
            if   st.get('ot'):     s, c = "과온 트립", "#c62828"
            elif st.get('tc_err'): s, c = "센서 이상", "#c62828"
            elif st.get('wd_err'): s, c = "통신 두절", "#c62828"
            else:                  s, c = "이상 발생", "#c62828"
        elif not st.get('itl'):
            s, c = "인터락", "#ef6c00"          # 하드웨어 조건 미충족
        elif st.get('run'):
            s, c = "운전 중", "#2e7d32"
        else:
            s, c = "정지", "#616161"
        self.ui.heater_status_label.setText(s)
        self.ui.heater_status_label.setStyleSheet(
            f"border: none; color:{c}; font-weight:bold;")

        # --- 출력 표시 : DAC 원본값 + 백분율 + 램프 목표 ---
        #     DAC 원본을 함께 보여야 PLC 모니터(D00041)와 대조할 수 있다.
        if st.get('run'):
            self.ui.heater_mv_label.setText(
                f"DAC {int(st.get('mv', 0))}/{int(st.get('mv_limit', HEATER_MV_LIMIT))}"
                f" ({st.get('mv_pct', 0):.0f}%) · ≈{st.get('est_current', 0.0):.1f}A"
            )
        else:
            self.ui.heater_mv_label.setText(f"출력 : 정지 (DAC {int(st.get('mv', 0))})")

        # 라벨 폭(200px)에 다 못 넣는 램프/한계값은 툴팁으로 뺀다.
        self.ui.heater_mv_label.setToolTip(
            f"램프 목표 {st.get('sv_ramp', 0.0):.1f}°C"
            f" · 램프 {st.get('ramp_rate', 0):.0f}°C/min"
            f" · 홀드백 {st.get('holdback', 0.0):.1f}°C"
            f" · OT {st.get('ot_limit', 0.0):.1f}°C"
        )

        # --- CSV 로깅 (운전 중에만, HEATER_LOG_PERIOD_MS 주기) ---
        #     폴링은 200ms이므로 반드시 시각 비교로 솎아낸다.
        self._heater_log_tick(st)

        # --- PZ400 스타일 LCD 표시 ---
        self._update_heater_lcd(st)

        # --- 이상 발생 시 ON 버튼 자동 해제 ---
        #     PLC 래더는 HEATER_RUN을 절대 건드리지 않으므로,
        #     이 처리가 없으면 '화면은 ON인데 히터는 정지' 상태가 된다.
        if st.get('fault') and self.ui.heater_onoff_button.isChecked():
            self.ui.heater_onoff_button.setChecked(False)

        # --- NAS CSV 로그용 평균 누적 (운전 중 + 온도 유효할 때만) ---
        if st.get('ok') and st.get('pv') is not None and st.get('run'):
            self._chk_heater_sum += float(st['pv'])
            self._chk_heater_cnt += 1

        # 이상 시 ON 버튼 자동 해제 (래더는 HEATER_RUN을 건드리지 않음)
        if st.get('fault') and self.ui.heater_onoff_button.isChecked():
            self.ui.heater_onoff_button.setChecked(False)

        # 통계 누적 (NAS CSV용)
        if st.get('ok') and st.get('pv') is not None and st.get('run'):
            self._chk_heater_sum += float(st['pv'])
            self._chk_heater_cnt += 1

    @Slot(str)
    def _on_heater_fault(self, reason: str):
        # 이상 발생 시점의 상태값을 함께 남긴다.
        #  - log_message_to_monitor 는 내부에서 파일 로그(NAS)까지 수행하므로
        #    프로그램을 재시작해도 기록이 남는다. (화면 모니터는 재시작 시 지워짐)
        #  - 나중에 재발했을 때 과온/센서/워치독 중 무엇이었는지 구분하려면
        #    비트 상태가 반드시 필요하다.
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
        log_message_to_monitor("경고", detail)

        if self.chat_chk:
            try:
                self.chat_chk.notify_error_with_src("HEATER", reason)
            except Exception:
                pass
        if self.process_running:
            self._chat_add_error(f"HEATER: {reason}")
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
        if HEATER_ENABLED and self.heater_recipe.is_running():
            QMessageBox.warning(self, "경고",
                                "히터 레시피가 실행 중입니다. 레시피를 먼저 중단하세요.")
            return
        if self.process_running:
            QMessageBox.warning(self, "경고", "이미 공정이 진행 중입니다.")
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
                "use_g1": self.ui.G1_checkbox.isChecked(),
                "use_g2": self.ui.G2_checkbox.isChecked(),

                # ▼ G1/G2 사용 여부 + 타겟 이름
                "use_g1": use_g1_flag,
                "use_g2": use_g2_flag,

                # ▼ DC Power 안정화 대기 사용 여부 (기본 OFF)
                "use_dc_delay": self.ui.dc_delay_checkbox.isChecked(),

                # ▼ 히터: 수동 UI 값 그대로. 0이면 히터 대기 스텝 생략
                "use_heater": HEATER_ENABLED and bool(self.ui.heater_onoff_button.isChecked()),
                # QLineEdit → text(). 비어 있으면 0 (= 히터 미사용)
                "heater_temp": float(self.ui.heater_sv_edit.text().strip() or 0),

                "g1_target_name": g1_target_name,
                "g2_target_name": g2_target_name,
            }

            if not (params['dc_power'] > 0 or params['rf_power'] > 0):
                raise ValueError("RF 또는 DC 파워 중 하나 이상을 입력해야 합니다.")
            
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
                "CSV Files (*.csv);;All Files (*)"
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
    def set_indicator(self, name, state: bool):
        # ERP 리포터용 상태 기록 (표시 로직에는 영향 없음)
        try:
            if not hasattr(self, "_erp_indicators"):
                self._erp_indicators = {}
            self._erp_indicators[str(name)] = bool(state)
        except Exception:
            pass

        frame_name = f"{name}_Indicator"
        frame = getattr(self.ui, frame_name, None)
        if frame is not None:
            color = "#38d62f" if state else "#d6252f"
            frame.setStyleSheet(f"background: {color}; border-radius: 25px; border: 2px solid #333;")
        else:
            log_message_to_monitor("WARN", f"[set_indicator] '{frame_name}' 인디케이터가 UI에 없습니다.")

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
            "Heater Temp": (f"{self._chk_heater_sum / self._chk_heater_cnt:.1f}"
                if self._chk_heater_cnt else ""),     # ★
            "RF: For.P":        rf_for_p,          # ← 전체 공정 평균
            "RF: Ref. P":       rf_ref_p,          # ← 전체 공정 평균
            "DC: V":            dc_v,              # ← 전체 공정 평균
            "DC: I":            dc_i,              # ← 전체 공정 평균
            "DC: P":            dc_p,              # ← 전체 공정 평균
        }

        return row
    
    def _reset_chk_stats(self):
        """ChK CSV 평균 계산용 누적값 + 샘플링 상태 초기화."""
        self._chk_heater_sum = 0.0      # ★
        self._chk_heater_cnt = 0        # ★

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
        self.ui.select_csv_button.setEnabled(True)
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
                self.ui.select_csv_button.setEnabled(True)
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
                self.ui.select_csv_button.setEnabled(True)
                self.update_stage_monitor("CSV 공정 실패로 중단됨")
                self._reset_process_ui_fields()
                return

        # 🔻 여기 이하(단일 공정 종료 처리)는 그대로 유지
        self.process_running = False
        self.ui.Sputter_Start_Button.setEnabled(True)
        self.ui.Sputter_Stop_Button.setEnabled(False)
        self.ui.select_csv_button.setEnabled(True)
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
            self._erp_meas["pressure"] = pressure
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
    def _on_mfc_flow_alert(self, msg: str):
        log_message_to_monitor("MFC(경고)", msg)
        if self.chat_chk and self.process_running:
            name = self.current_process_name or "CHK"
            self.chat_chk.notify_text(f"⚠️ CHK 가스 유량 이상: {name} | {msg}")
            self.chat_chk.flush()

    @Slot(str)
    def _on_mfc_pressure_alert(self, msg: str):
        log_message_to_monitor("MFC(경고)", msg)
        if self.chat_chk and self.process_running:
            name = self.current_process_name or "CHK"
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
                elif self.ui.heater_onoff_button.isChecked():
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
            except Exception:
                pass
            try:
                self.request_heater_run.emit(False)
                self.ui.heater_onoff_button.setChecked(False)
            except Exception:
                pass

        log_message_to_monitor("정보", "프로그램 종료를 시작합니다...")

        # worker 정리를 각 worker 자신의 스레드에서 먼저 수행
        self._invoke_worker_blocking(self.process_controller, "teardown")
        self._invoke_worker_blocking(self.plc_controller, "cleanup")
        self._invoke_worker_blocking(self.mfc_controller, "cleanup")
        self._invoke_worker_blocking(self.dcpower_controller, "close_connection")
        self._invoke_worker_blocking(self.rfpower_controller, "close_connection")

        threads = [
            self.process_thread,
            self.plc_thread,
            self.mfc_thread,
            self.dcpower_thread,
            self.rfpower_thread,
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

        # ---- 히터(선택) : 컬럼이 없으면 자동 OFF → 기존 CSV 하위 호환 ----
        use_heater  = _b("use_heater", False)
        heater_temp = _f("heater_temp", required=False, default=0.0) or 0.0
        if use_heater and heater_temp <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] use_heater=ON 인데 heater_temp가 0 이하입니다.")
        if use_heater and heater_temp > HEATER_MAX_TEMP:
            raise ValueError(f"[{process_name or 'STEP'}] heater_temp가 상한({HEATER_MAX_TEMP:.0f}°C)을 넘습니다.")

        # 승온 속도(°C/min). 컬럼이 없거나 0이면 config 기본값을 쓴다.
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
