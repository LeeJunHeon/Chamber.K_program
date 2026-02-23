# device/process_controller.py  (Chamber-K, PLC version — chamber2 스타일, IG/OES/RGA 제거)

from __future__ import annotations
from typing import Optional, List, Tuple, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
from enum import Enum

import math
from PyQt6.QtCore import (
    QObject, QTimer, QEventLoop, QMetaObject, QThread,
    pyqtSignal as Signal, pyqtSlot as Slot, Qt, QElapsedTimer
)

from lib.config import PLC_RECONNECT_MAX_TOTAL_SEC

# ===================== 액션 / 스텝 정의 =====================

if TYPE_CHECKING:
    # 여기 이름은 실제 파일/클래스명과 정확히 일치해야 함
    from device.MFC import MFCController
    from device.DCpower import DCPowerController      # 파일: DCpower.py, 클래스: DCPowerController
    from device.RFpower import RFPowerController      # 파일: RFpower.py, 클래스: RFPowerController
    from device.PLC import PLCController

class ActionType(str, Enum):
    MFC_CMD       = "MFC_CMD"        # params=('CMD', {args})
    PLC_CMD       = "PLC_CMD"        # params=('ButtonName', True/False)
    DELAY         = "DELAY"          # duration_sec:int , timer_purpose: 'shutter'|'process'|None
    POWER_WAIT    = "POWER_WAIT"     # DC/RF 목표 도달 대기
    DC_POWER_SET  = "DC_POWER_SET"   # value: float
    DC_POWER_STOP = "DC_POWER_STOP"
    RF_POWER_SET  = "RF_POWER_SET"   # value: float (offset/param은 start에서 params로 받음)
    RF_POWER_STOP = "RF_POWER_STOP"

@dataclass
class ProcessStep:
    action: ActionType
    message: str
    # 공용 옵션
    params: Optional[Tuple] = None       # see ActionType별 주석
    value: Optional[float] = None        # DC/RF target 등
    duration_sec: Optional[int] = None   # DELAY 용
    timer_purpose: Optional[str] = None  # 'shutter' | 'process' | None
    polling: bool = False                # 이 스텝에서만 MFC 폴링 ON

# ===================== 유틸 : 스레드 안전 호출 =====================

def _invoke_connect(obj, method_name: str) -> bool:
    """obj.method_name()을 obj의 스레드에서 동기 호출하고, 가능하면 실제 반환값(bool)을 돌려준다."""
    try:
        if obj.thread() is QThread.currentThread():
            ret = getattr(obj, method_name)()
            return bool(True if ret is None else ret)   # 반환값 없으면 True로 간주
        ok = QMetaObject.invokeMethod(obj, method_name, Qt.ConnectionType.BlockingQueuedConnection)
        return bool(ok)
    except Exception:
        return False

# ===================== 메인 컨트롤러 =====================

class SputterProcessController(QObject):
    # --- 로그/상태(UI) ---
    status_message        = Signal(str, str)   # (level, text)
    stage_monitor         = Signal(str)
    shutter_delay_tick    = Signal(int)        # sec
    process_time_tick     = Signal(int)        # sec
    finished              = Signal()

    # --- 수명/오류/시작 ---
    start_requested       = Signal(dict)       # Main이 이 신호를 emit 하도록 구성됨
    connection_failed     = Signal(str)
    critical_error        = Signal(str)

    # --- 장치 제어 신호 (Main 쪽 연결과 호환) ---
    update_plc_port       = Signal(str, bool)  # (btn_name, state)
    start_dc_power        = Signal(float)      # target W
    stop_dc_power         = Signal()
    start_rf_power        = Signal(dict)       # {'target':W, 'offset':x, 'param':y}
    stop_rf_power         = Signal()

    # --- MFC 라우팅 (Process -> MFC) ---
    command_requested     = Signal(str, dict)  # (cmd, params)

    def __init__(self, mfc_controller, dc_controller, rf_controller, plc_controller):
        super().__init__()
        self.mfc: MFCController = mfc_controller
        self.dc:  DCPowerController = dc_controller
        self.rf:  RFPowerController = rf_controller
        self.plc: PLCController = plc_controller

        # 내부 상태
        self._steps: List[ProcessStep] = []
        self._idx: int = -1
        self._running: bool = False
        self.params: Dict[str, Any] = {}

        # 타이머(실시간 기반 하트비트 + 모노토닉 시계)
        self._timer: Optional[QTimer] = None
        self._timer_purpose: Optional[str] = None  # 'shutter'|'process'|None
        self._delay_clock: Optional[QElapsedTimer] = None  # 실시간 측정용 (모노토닉)
        self._delay_total_sec: int = 0                      # 전체 대기 초
        self._last_emitted_sec: int = -1                    # UI 갱신 중복 방지

        # 파워 대기
        self._power_loops: List[Tuple[str, QEventLoop]] = []

        # 채널/버튼
        self._process_channel: int = 1
        self._gas_valve_button: str = "Ar_Button"

        # 다중 가스용: 사용 중인 채널/밸브 목록
        self._active_channels: list[int] = [1]      # [1]=Ar, [2]=O2
        self._gas_valve_buttons: list[str] = ["Ar_Button"]

        # 사용 여부
        self.is_dc_on = False
        self.is_rf_on = False

        self._stop_pending = False
        self._active_loops: list[tuple[str, QEventLoop]] = []

        # 램프다운 대기 루프 핸들
        self._rfdown_wait: Optional[QEventLoop] = None

        # --- PLC 상태 캐시(버튼 출력 코일 상태) ---
        self._plc_button_states: Dict[str, bool] = {}

        # --- PLC_CMD 대기용 핸들 ---
        self._plc_wait_loop: Optional[QEventLoop] = None
        self._plc_wait_target: Optional[Tuple[str, bool]] = None

        # --- PLC 시그널 연결 (상태 변화/치명 오류 감지) ---
        try:
            self.plc.update_button_display.connect(
                self._on_plc_button_update,
                type=Qt.ConnectionType.QueuedConnection
            )
        except TypeError:
            self.plc.update_button_display.connect(self._on_plc_button_update)

        try:
            self.plc.status_message.connect(
                self._on_plc_status_message,
                type=Qt.ConnectionType.QueuedConnection
            )
        except TypeError:
            self.plc.status_message.connect(self._on_plc_status_message)

        # --- DC/RF 시그널 연결 (치명 오류 감지) ---
        try:
            self.dc.status_message.connect(self._on_power_status_message,
                                        type=Qt.ConnectionType.QueuedConnection)
        except TypeError:
            self.dc.status_message.connect(self._on_power_status_message)

        try:
            self.rf.status_message.connect(self._on_power_status_message,
                                        type=Qt.ConnectionType.QueuedConnection)
        except TypeError:
            self.rf.status_message.connect(self._on_power_status_message)

    @Slot(str, str)
    def _on_power_status_message(self, level: str, text: str):
        lvl = str(level or "").strip()
        msg = str(text or "").strip()
        if lvl == "재시작":
            self.status_message.emit("오류", f"Power 치명: {msg}")
            self._abort_with_error(f"Power 재연결 실패/치명 오류로 공정 중단: {msg}")

    # ---------- 타이머 준비 ----------
    @Slot()
    def _setup_timers(self):
        if self._timer is None:
            self._timer = QTimer(self)
            # 하트비트: 250 ms. 실제 남은 시간 계산은 QElapsedTimer로 수행.
            self._timer.setInterval(250)
            self._timer.setTimerType(Qt.TimerType.PreciseTimer)
            self._timer.timeout.connect(self._on_tick)
        self.status_message.emit("정보", "ProcessController 타이머 준비 완료")

    def _invoke_self(self, name: str):
        if self.thread() is QThread.currentThread():
            getattr(self, name)()
        else:
            QMetaObject.invokeMethod(self, name, Qt.ConnectionType.BlockingQueuedConnection)

    # ==================== PLC 상태/재연결 대응 ====================

    @Slot(str, bool)
    def _on_plc_button_update(self, btn_name: str, state: bool):
        """PLC가 관측한 버튼(출력 코일) 상태 캐시."""
        btn = str(btn_name)
        st = bool(state)
        self._plc_button_states[btn] = st

        # PLC_CMD 대기 중이면 목표 달성 시 즉시 깨움
        if self._plc_wait_target and self._plc_wait_loop:
            t_btn, t_state = self._plc_wait_target
            if btn == t_btn and st == t_state:
                try:
                    self._plc_wait_loop.quit()
                except Exception:
                    pass

    @Slot(str, str)
    def _on_plc_status_message(self, level: str, text: str):
        """PLC 쪽 치명 이벤트를 공정 중단으로 연결."""
        lvl = str(level or "").strip()
        msg = str(text or "").strip()

        if lvl == "재시작":
            self.status_message.emit("오류", f"PLC 치명: {msg}")
            self._abort_with_error(f"PLC 재연결 실패로 공정 중단: {msg}")

    def _wait_plc_state(self, btn: str, desired: bool, timeout_ms: int) -> bool:
        """
        PLC 버튼(출력 코일) 상태가 desired가 될 때까지 대기.
        - PLC가 끊겨 있으면(큐잉/재연결 중) 상태 업데이트가 올 때까지 기다림
        - timeout_ms 초과 시 False
        """
        btn = str(btn)
        desired = bool(desired)

        if self._plc_button_states.get(btn) == desired:
            return True

        if not self._running or self._stop_pending:
            return False

        loop = QEventLoop()
        self._plc_wait_loop = loop
        self._plc_wait_target = (btn, desired)

        t = QTimer()
        t.setSingleShot(True)

        state = {"timeout": False}

        def _on_timeout():
            state["timeout"] = True
            try:
                loop.quit()
            except Exception:
                pass

        t.timeout.connect(_on_timeout)
        t.start(int(timeout_ms))

        loop.exec()

        # cleanup
        try:
            t.stop()
        except Exception:
            pass
        try:
            t.timeout.disconnect(_on_timeout)
        except Exception:
            pass

        self._plc_wait_loop = None
        self._plc_wait_target = None

        if not self._running or self._stop_pending:
            return False

        if state["timeout"]:
            return False

        return self._plc_button_states.get(btn) == desired

    # ==================== 공정 시작 ====================

    @Slot(dict)
    def start_process_flow(self, params: Dict[str, Any]):
        try:
            self.params = params
            self._running = True

            # 타이머는 자신의 스레드에서 생성
            self._invoke_self("_setup_timers")

            # ✅ PLC: 이전 런의 래치/큐 정리 + 폴링/재연결 루프 시작
            _invoke_connect(self.plc, "clear_fault_latch")
            _invoke_connect(self.plc, "start_polling")

            # 장치 연결 확인
            _invoke_connect(self.dc, "connect_dcpower_device")

            if float(params.get('dc_power', 0) or 0) > 0:
                if not self._is_connected(self.dc):
                    # ✅ 즉시 종료하지 말고, 이후 POWER_WAIT/재시작 시그널/타임아웃에서 정리
                    self.status_message.emit("경고", "DC Power 미연결 상태로 시작(재연결 시도/큐 처리로 복구 기대)")

            _invoke_connect(self.mfc, "connect_mfc_device")
            if not self._is_connected(self.mfc):
                self.connection_failed.emit("MFC 장치에 연결할 수 없습니다.")
                self._running = False
                return

            # 채널/버튼/파워 사용여부
            use_ar = bool(params.get('use_ar_gas', False))
            use_o2 = bool(params.get('use_o2_gas', False))

            active_channels: list[int] = []
            gas_buttons: list[str] = []

            if use_ar:
                active_channels.append(1)
                gas_buttons.append("Ar_Button")

            if use_o2:
                active_channels.append(2)
                gas_buttons.append("O2_Button")

            # 아무 가스도 체크 안 된 경우 → 옛날 single-gas 파라미터로 fallback
            if not active_channels:
                gas = params.get('selected_gas', 'Ar')
                ch = 1 if gas == "Ar" else 2
                active_channels = [ch]
                gas_buttons = ['Ar_Button' if ch == 1 else 'O2_Button']

            # 내부 상태 저장
            self._active_channels = active_channels
            self._gas_valve_buttons = gas_buttons

            # 기존 코드와의 호환(여러 곳에서 여전히 첫 채널만 쓰고 있음)
            self._process_channel = active_channels[0]
            self._gas_valve_button = gas_buttons[0]

            # 🔹 MFC 쪽에도 이번 공정에서 실제 사용하는 채널 정보 전달
            try:
                self.command_requested.emit("set_active_channels", {"channels": active_channels})
            except Exception:
                pass

            self.is_dc_on = float(params.get('dc_power', 0) or 0) > 0
            self.is_rf_on = float(params.get('rf_power', 0) or 0) > 0

            # 스텝 구성
            self._steps = self._build_steps(params)
            self._idx = -1

            self.status_message.emit("정보", "Sputtering 공정을 시작합니다.")
            self._next_step()
        except Exception as e:
            self._running = False
            self._abort_with_error(f"start_process_flow 예외: {e}")

    # ==================== 스텝 구성 ====================
    def _build_steps(self, p: Dict[str, Any]) -> List[ProcessStep]:
        """
        params에서 선택된 가스(Ar/O2)에 따라
        - 여러 채널의 Flow OFF / ZEROING / Flow Set / Flow ON
        - 여러 가스 밸브 Open/Close
        를 처리한다.
        """
        channels = getattr(self, "_active_channels", None)
        if not channels:
            ch = 1 if p.get('selected_gas', 'Ar') == 'Ar' else 2
            channels = [ch]

        flows: Dict[int, float] = {}
        default_flow = float(p.get('mfc_flow', 0.0))

        if 1 in channels:
            flows[1] = float(p.get('ar_flow', default_flow))
        if 2 in channels:
            flows[2] = float(p.get('o2_flow', default_flow))

        sp1_ui = float(p.get('sp1_set', 0.0))

        steps: List[ProcessStep] = []

        steps.append(ProcessStep(ActionType.DC_POWER_STOP, "PRE: DC Power OFF"))
        steps.append(ProcessStep(ActionType.RF_POWER_STOP, "PRE: RF Power OFF"))
        steps.append(ProcessStep(ActionType.DELAY, "PRE: Power OFF settle", duration_sec=1))

        for ch in channels:
            steps.append(
                ProcessStep(
                    ActionType.MFC_CMD,
                    f"Ch{ch} Flow OFF",
                    params=('FLOW_OFF', {'channel': ch}),
                )
            )

        steps.append(
            ProcessStep(
                ActionType.MFC_CMD,
                "MFC Valve Open",
                params=('VALVE_OPEN', {}),
            )
        )

        for ch in channels:
            steps.append(
                ProcessStep(
                    ActionType.MFC_CMD,
                    f"Ch{ch} ZEROING",
                    params=('MFC_ZEROING', {'channel': ch}),
                )
            )

        steps.append(
            ProcessStep(
                ActionType.MFC_CMD,
                "PS ZEROING",
                params=('PS_ZEROING', {}),
            )
        )

        gas_buttons = getattr(self, "_gas_valve_buttons", [self._gas_valve_button])
        for btn in gas_buttons:
            gas_name = "Ar" if "Ar" in btn else "O2"
            steps.append(
                ProcessStep(
                    ActionType.PLC_CMD,
                    f"{gas_name} Valve Open",
                    params=(btn, True),
                )
            )

        for ch in channels:
            flow = float(flows.get(ch, 0.0))
            if flow > 0.0:
                steps.append(
                    ProcessStep(
                        ActionType.MFC_CMD,
                        f"Ch{ch} {flow:.2f}sccm 설정",
                        params=('FLOW_SET', {'channel': ch, 'value': flow}),
                    )
                )
                steps.append(
                    ProcessStep(
                        ActionType.MFC_CMD,
                        f"Ch{ch} Flow ON",
                        params=('FLOW_ON', {'channel': ch}),
                    )
                )

        steps.append(
            ProcessStep(
                ActionType.MFC_CMD,
                "SP4 ON",
                params=('SP4_ON', {}),
            )
        )

        steps.append(
            ProcessStep(
                ActionType.DELAY,
                "압력 안정화 대기(60초)",
                duration_sec=60,
            )
        )

        if p.get('use_g1', False):
            steps.append(
                ProcessStep(
                    ActionType.PLC_CMD,
                    "Gun Shutter 1 Open",
                    params=('S1_button', True),
                )
            )
        if p.get('use_g2', False):
            steps.append(
                ProcessStep(
                    ActionType.PLC_CMD,
                    "Gun Shutter 2 Open",
                    params=('S2_button', True),
                )
            )

        steps.append(
            ProcessStep(
                ActionType.POWER_WAIT,
                "파워 목표치 도달 대기",
            )
        )

        steps.append(
            ProcessStep(
                ActionType.MFC_CMD,
                f"SP1={sp1_ui:.2f} 설정",
                params=('SP1_SET', {'value': sp1_ui}),
            )
        )

        if sp1_ui < 5.0:
            steps.append(
                ProcessStep(
                    ActionType.MFC_CMD,
                    "SP2=5.00 설정",
                    params=('SP2_SET', {'value': 5.0}),
                )
            )
            steps.append(
                ProcessStep(
                    ActionType.MFC_CMD,
                    "SP2 ON",
                    params=('SP2_ON', {}),
                )
            )
            steps.append(
                ProcessStep(
                    ActionType.DELAY,
                    "압력 안정화 대기 (SP2, 60초)",
                    duration_sec=60,
                )
            )

        steps.append(
            ProcessStep(
                ActionType.MFC_CMD,
                "SP1 ON",
                params=('SP1_ON', {}),
            )
        )

        sd_sec = max(0, int(math.ceil(float(p.get('shutter_delay', 0.0)) * 60)))
        if sd_sec > 0:
            steps.append(
                ProcessStep(
                    ActionType.DELAY,
                    f"Shutter Delay {sd_sec}s",
                    duration_sec=sd_sec,
                    timer_purpose='shutter',
                    polling=True,
                )
            )

        if float(p.get('process_time', 0.0)) > 0:
            steps.append(
                ProcessStep(
                    ActionType.PLC_CMD,
                    "Main Shutter Open",
                    params=('MS_button', True),
                )
            )

        pt_sec = max(0, int(math.ceil(float(p.get('process_time', 0.0)) * 60)))
        if pt_sec > 0:
            steps.append(
                ProcessStep(
                    ActionType.DELAY,
                    f"메인 공정 {pt_sec}s 진행",
                    duration_sec=pt_sec,
                    timer_purpose='process',
                    polling=True,
                )
            )

        return steps

    # ==================== google chat 관련 추가 ====================
    def _step_tag(self, step: Optional[ProcessStep] = None) -> str:
        total = len(self._steps) if self._steps else 0
        idx = self._idx
        if step is None and 0 <= idx < total:
            step = self._steps[idx]
        act = step.action.value if step else "UNKNOWN"
        msg = step.message if step else ""
        if idx >= 0 and total > 0:
            base = f"Step {idx+1}/{total} {act}"
        else:
            base = f"Step ?/{total} {act}"
        return f"{base}: {msg}" if msg else base

    def _abort_with_error(self, reason: str):
        r = (reason or "").strip() or "오류"
        try:
            self.critical_error.emit(r)
        except Exception:
            pass
        try:
            self.request_stop()
        except Exception:
            try:
                self._running = False
                self.finished.emit()
            except Exception:
                pass
    # ==================== google chat 관련 추가 ====================

    # ==================== 스텝 실행 ====================
    def _next_step(self):
        try:
            if not self._running:
                self.status_message.emit("오류", "중단 상태 — 다음 스텝 실행 안 함")
                return

            self._idx += 1
            if self._idx >= len(self._steps):
                self.status_message.emit("성공", "모든 스텝 완료 — 안전 종료로 이동")
                self.stop_process()
                return

            step = self._steps[self._idx]
            self.stage_monitor.emit(f"[{self._idx+1}/{len(self._steps)}] {step.message}")
            self.status_message.emit("공정", step.message)

            self.command_requested.emit("set_polling", {'enable': bool(step.polling)})

            if step.action == ActionType.MFC_CMD:
                cmd, args = step.params
                self.command_requested.emit(cmd, dict(args))

            elif step.action == ActionType.PLC_CMD:
                btn, st = step.params
                btn = str(btn)
                st = bool(st)

                self.update_plc_port.emit(btn, st)

                timeout_ms = int((float(PLC_RECONNECT_MAX_TOTAL_SEC) + 5.0) * 1000)
                ok = self._wait_plc_state(btn, st, timeout_ms=timeout_ms)
                if not ok:
                    self._abort_with_error(f"{self._step_tag(step)} | PLC_CMD 적용 타임아웃: {btn}={st}")
                    return

                QTimer.singleShot(800, self._next_step)

            elif step.action == ActionType.DC_POWER_SET:
                self.start_dc_power.emit(float(step.value or 0.0))

            elif step.action == ActionType.DC_POWER_STOP:
                self.stop_dc_power.emit()
                QTimer.singleShot(100, self._next_step)

            elif step.action == ActionType.RF_POWER_SET:
                payload = {
                    'target': float(step.value or 0.0),
                    'offset': float(self.params.get('rf_offset', 0.0) or 0.0),
                    'param':  float(self.params.get('rf_param', 1.0) or 1.0),
                }
                self.start_rf_power.emit(payload)

            elif step.action == ActionType.RF_POWER_STOP:
                self.stop_rf_power.emit()
                QTimer.singleShot(100, self._next_step)

            elif step.action == ActionType.POWER_WAIT:
                self._power_wait()

            elif step.action == ActionType.DELAY:
                self._start_delay(int(step.duration_sec or 0), step.timer_purpose)

            else:
                self.status_message.emit("오류", f"알 수 없는 액션: {step.action}")
                self._next_step()
        except Exception as e:
            self._abort_with_error(f"{self._step_tag()} | _next_step 예외: {e}")

    # ==================== 장치 콜백(MFC) ====================

    @Slot(str)
    def _on_mfc_confirmed(self, cmd: str):
        if not self._running or self._idx >= len(self._steps):
            return
        step = self._steps[self._idx]
        if step.action == ActionType.MFC_CMD:
            expected = step.params[0] if step.params else None
            if cmd == expected:
                self.status_message.emit("MFC", f"'{cmd}' 확인 → 다음 단계")
                self._next_step()
            else:
                self.status_message.emit("경고", f"MFC 확인 무시: '{cmd}', 기대 '{expected}'")

    @Slot(str, str)
    def _on_mfc_failed(self, cmd: str, why: str):
        if not self._running:
            return

        step = self._steps[self._idx] if 0 <= self._idx < len(self._steps) else None
        tag = self._step_tag(step)

        if cmd == "FLOW_MON":
            self.status_message.emit("MFC(실패)", f"{tag} | [FLOW_MON] {why}")
            self._abort_with_error(f"{tag} | 가스 유량 이탈로 공정 중단: {why}")
            return

        bad = (step.params[0] if (step and step.params) else "?")
        self.status_message.emit("MFC(실패)", f"{tag} | '{bad}' 실패: {why}")
        self._abort_with_error(f"{tag} | MFC '{bad}' 실패: {why}")

    # ==================== 파워 안정화 ====================

    def _power_wait(self):
        dc_power = float(self.params.get('dc_power', 0.0) or 0.0)
        rf_power = float(self.params.get('rf_power', 0.0) or 0.0)
        rf_offset = float(self.params.get('rf_offset', 0.0) or 0.0)
        rf_param  = float(self.params.get('rf_param', 1.0) or 1.0)

        need_dc = dc_power > 0.0
        need_rf = rf_power > 0.0

        if not (need_dc or need_rf):
            self._next_step()
            return

        reached = {"dc": (not need_dc), "rf": (not need_rf)}
        state = {"timeout": False}

        loop = QEventLoop()
        self._active_loops = [("power", loop)]
        self.status_message.emit("정보", "파워 목표치 도달 대기중...")

        def _try_quit():
            if reached["dc"] and reached["rf"]:
                try: loop.quit()
                except Exception: pass

        def _on_dc():
            reached["dc"] = True
            _try_quit()

        def _on_rf():
            reached["rf"] = True
            _try_quit()

        if need_dc:
            self.dc.target_reached.connect(_on_dc)
            self.start_dc_power.emit(dc_power)

        if need_rf:
            self.rf.target_reached.connect(_on_rf)
            self.start_rf_power.emit({'target': rf_power, 'offset': rf_offset, 'param': rf_param})

        # ✅ 타임아웃: PLC 재연결 최대시간 + 여유(예: +10s) 기반
        timeout_ms = int((float(PLC_RECONNECT_MAX_TOTAL_SEC) + 10.0) * 1000)
        t = QTimer()
        t.setSingleShot(True)

        def _on_timeout():
            state["timeout"] = True
            try: loop.quit()
            except Exception: pass

        t.timeout.connect(_on_timeout)
        t.start(timeout_ms)

        loop.exec()

        # cleanup
        try:
            t.stop()
            t.timeout.disconnect(_on_timeout)
        except Exception:
            pass

        if need_dc:
            try: self.dc.target_reached.disconnect(_on_dc)
            except Exception: pass
        if need_rf:
            try: self.rf.target_reached.disconnect(_on_rf)
            except Exception: pass

        self._active_loops = []

        if not self._running or self._stop_pending:
            return

        if state["timeout"]:
            missing = []
            if need_dc and not reached["dc"]: missing.append("DC")
            if need_rf and not reached["rf"]: missing.append("RF")
            self._abort_with_error(f"{self._step_tag()} | POWER_WAIT 타임아웃({timeout_ms}ms): 미도달={','.join(missing)}")
            return

        self.status_message.emit("정보", "파워 안정화 완료.")
        self._next_step()

    # ==================== 딜레이/타이머 ====================

    def _emit_delay_ui(self, remaining_sec: int):
        if remaining_sec == self._last_emitted_sec:
            return
        self._last_emitted_sec = remaining_sec
        if self._timer_purpose == 'shutter':
            self.shutter_delay_tick.emit(remaining_sec)
        elif self._timer_purpose == 'process':
            self.process_time_tick.emit(remaining_sec)

    def _start_delay(self, seconds: int, purpose: Optional[str]):
        if seconds <= 0:
            self._next_step()
            return

        self._delay_total_sec = int(seconds)
        self._timer_purpose = purpose

        self._delay_clock = QElapsedTimer()
        self._delay_clock.start()

        self._last_emitted_sec = -1
        self._emit_delay_ui(self._delay_total_sec)

        if self._timer:
            self._timer.start()
        else:
            self.status_message.emit("오류", "타이머 초기화 누락")
            self._next_step()

    def _on_tick(self):
        try:
            if not self._running:
                if self._timer and self._timer.isActive():
                    self._timer.stop()
                self.command_requested.emit("set_polling", {'enable': False})
                return

            if not self._delay_clock:
                return

            elapsed_ms = self._delay_clock.elapsed()
            elapsed_sec = int(elapsed_ms // 1000)
            remaining = max(0, self._delay_total_sec - elapsed_sec)

            self._emit_delay_ui(remaining)

            if remaining <= 0:
                if self._timer and self._timer.isActive():
                    self._timer.stop()
                self.command_requested.emit("set_polling", {'enable': False})

                self._delay_clock = None
                self._delay_total_sec = 0
                self._timer_purpose = None
                self._last_emitted_sec = -1

                self._next_step()
        except Exception as e:
            self._abort_with_error(f"{self._step_tag()} | timer(_on_tick) 예외: {e}")

    # ==================== 종료/정리 ====================

    @Slot()
    def _on_rf_rampdown_finished(self):
        self.status_message.emit("RFpower", "램프다운 완료 신호 수신")
        if self._rfdown_wait is not None and self._rfdown_wait.isRunning():
            self._rfdown_wait.quit()

    @Slot()
    def teardown(self):
        if self._timer and self._timer.isActive():
            self._timer.stop()
        self._delay_clock = None
        self._delay_total_sec = 0
        self._timer_purpose = None
        self._last_emitted_sec = -1

    @Slot()
    def request_stop(self):
        if self._stop_pending:
            return
        self._stop_pending = True
        if self.thread() is QThread.currentThread():
            self._stop_impl()
        else:
            QMetaObject.invokeMethod(self, "_stop_impl",
                                    Qt.ConnectionType.QueuedConnection)

    @Slot()
    def stop_process(self):
        self.request_stop()

    @Slot()
    def _stop_impl(self):
        try:
            if self._timer and self._timer.isActive():
                self._timer.stop()
            self._delay_clock = None
            self._delay_total_sec = 0
            self._timer_purpose = None
            self._last_emitted_sec = -1

            # ✅ PLC_CMD 대기 중이면 즉시 깨움
            if self._plc_wait_loop is not None:
                try:
                    self._plc_wait_loop.quit()
                except Exception:
                    pass
                self._plc_wait_loop = None
                self._plc_wait_target = None

            if self._active_loops:
                for _name, lp in self._active_loops:
                    try: lp.quit()
                    except: pass
                self._active_loops.clear()

            if not self._running:
                self.finished.emit()
                self._stop_pending = False
                return

            self.status_message.emit("정보", "종료 시퀀스를 실행합니다.")
            self._running = False

            self.command_requested.emit("set_polling", {'enable': False})

            self.stage_monitor.emit("M.S. close...")
            self.update_plc_port.emit('MS_button', False)

            if self.is_dc_on:
                self.status_message.emit("DCpower", "DC 파워 OFF")
                self.stop_dc_power.emit()
            if self.is_rf_on:
                self.status_message.emit("RFpower", "RF 파워 OFF (ramp-down)")

                self._rfdown_wait = QEventLoop()

                try:
                    self.rf.ramp_down_finished.connect(
                        self._on_rf_rampdown_finished,
                        type=Qt.ConnectionType.QueuedConnection
                    )
                except TypeError:
                    self.rf.ramp_down_finished.connect(self._on_rf_rampdown_finished)

                QTimer.singleShot(120_000, self._on_rf_rampdown_finished)

                self.stop_rf_power.emit()
                self._rfdown_wait.exec()

                try:
                    self.rf.ramp_down_finished.disconnect(self._on_rf_rampdown_finished)
                except Exception:
                    pass
                self._rfdown_wait = None

            channels = getattr(self, "_active_channels", None)
            if not channels:
                channels = [self._process_channel]

            def _wait_mfc_cmd(cmd: str, params: dict, timeout_ms: int) -> bool:
                if not self._is_connected(self.mfc):
                    self.status_message.emit("경고", f"[STOP] MFC 미연결 → {cmd} 생략(PASS)")
                    return False

                loop = QEventLoop()
                state = {"done": False, "ok": False, "why": ""}

                def _on_ok(c, *args):
                    if c == cmd:
                        state["done"] = True
                        state["ok"] = True
                        try: loop.quit()
                        except: pass

                def _on_fail(c, reason="", *args):
                    if c == cmd:
                        state["done"] = True
                        state["ok"] = False
                        state["why"] = str(reason or "")
                        try: loop.quit()
                        except: pass

                t = QTimer()
                t.setSingleShot(True)

                def _on_timeout():
                    state["done"] = True
                    state["ok"] = False
                    state["why"] = f"timeout {timeout_ms}ms"
                    try: loop.quit()
                    except: pass

                self.mfc.command_confirmed.connect(_on_ok)
                self.mfc.command_failed.connect(_on_fail)
                t.timeout.connect(_on_timeout)

                t.start(int(timeout_ms))
                self.command_requested.emit(cmd, dict(params))
                loop.exec()

                try: t.stop()
                except Exception: pass
                try: self.mfc.command_confirmed.disconnect(_on_ok)
                except Exception: pass
                try: self.mfc.command_failed.disconnect(_on_fail)
                except Exception: pass
                try: t.timeout.disconnect(_on_timeout)
                except Exception: pass

                if not state["ok"]:
                    why = state["why"] or "unknown"
                    self.status_message.emit("경고", f"[STOP] MFC {cmd} 실패/미응답({why}) → PASS")
                    return False

                return True

            for ch in channels:
                _wait_mfc_cmd("FLOW_OFF", {"channel": ch}, timeout_ms=7_000)

            _wait_mfc_cmd("VALVE_OPEN", {}, timeout_ms=15_000)

            self.update_plc_port.emit('S1_button', False)
            self.update_plc_port.emit('S2_button', False)

            gas_buttons = getattr(self, "_gas_valve_buttons", [self._gas_valve_button])
            for btn in gas_buttons:
                gas_name = "Ar" if "Ar" in btn else "O2"
                self.status_message.emit("PLC", f"{gas_name} Valve Close")
                self.update_plc_port.emit(btn, False)

            QTimer.singleShot(800, self._finish_stop)
            self._stop_pending = False
        except Exception as e:
            try:
                self.critical_error.emit(f"종료 시퀀스 예외: {e}")
            except Exception:
                pass
            try:
                self._running = False
            except Exception:
                pass
            try:
                self.finished.emit()
            except Exception:
                pass
        finally:
            self._stop_pending = False

    @Slot()
    def _finish_stop(self):
        self.status_message.emit("정보", "종료 완료")
        self.finished.emit()

    def _is_connected(self, obj) -> bool:
        """컨트롤러의 연결상태를 최대한 보수적으로 판정."""
        try:
            fn = getattr(obj, "is_connected", None)
            if callable(fn):
                return bool(fn())
        except Exception:
            pass

        for name in ("serial", "serial_mfc", "serial_dcpower"):
            s = getattr(obj, name, None)
            if s is not None and hasattr(s, "isOpen"):
                try:
                    if s.isOpen():
                        return True
                except Exception:
                    pass

        # 3) minimalmodbus Instrument 판정(PLC)
        try:
            inst = getattr(obj, "instrument", None)
            if inst is not None:
                ser = getattr(inst, "serial", None)
                if ser is not None:
                    if hasattr(ser, "is_open"):
                        return bool(getattr(ser, "is_open"))
                    if hasattr(ser, "isOpen"):
                        return bool(ser.isOpen())
        except Exception:
            pass

        return False
