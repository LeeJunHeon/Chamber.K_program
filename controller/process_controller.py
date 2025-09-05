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

        # 사용 여부
        self.is_dc_on = False
        self.is_rf_on = False

        self._stop_pending = False
        self._active_loops: list[tuple[str, QEventLoop]] = []

        # 램프다운 대기 루프 핸들
        self._rfdown_wait: Optional[QEventLoop] = None

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

    # ==================== 공정 시작 ====================

    @Slot(dict)
    def start_process_flow(self, params: Dict[str, Any]):
        self.params = params
        self._running = True

        # 타이머는 자신의 스레드에서 생성
        self._invoke_self("_setup_timers")

        # 장치 연결 확인
        if float(params.get('dc_power', 0) or 0) > 0:
            _invoke_connect(self.dc, "connect_dcpower_device")
            if not self._is_connected(self.dc):
                self.connection_failed.emit("DC Power 장치에 연결할 수 없습니다.")
                self._running = False
                return

        _invoke_connect(self.mfc, "connect_mfc_device")
        if not self._is_connected(self.mfc):
            self.connection_failed.emit("MFC 장치에 연결할 수 없습니다.")
            self._running = False
            return

        # 채널/버튼/파워 사용여부
        gas = params.get('selected_gas', 'Ar')
        self._process_channel = 1 if gas == "Ar" else 2
        self._gas_valve_button = 'Ar_Button' if gas == 'Ar' else 'O2_Button'
        self.is_dc_on = float(params.get('dc_power', 0) or 0) > 0
        self.is_rf_on = float(params.get('rf_power', 0) or 0) > 0

        # 스텝 구성
        self._steps = self._build_steps(params)
        self._idx = -1

        self.status_message.emit("정보", "Sputtering 공정을 시작합니다.")
        self._next_step()

    # ==================== 스텝 구성 ====================

    def _build_steps(self, p: Dict[str, Any]) -> List[ProcessStep]:
        ch   = 1 if p.get('selected_gas', 'Ar') == 'Ar' else 2
        flow = float(p.get('mfc_flow', 0.0))
        # (중요) SP1_SET 값은 UI 그대로 보냄 — MFC가 1/10 스케일 변환
        sp1_ui = float(p.get('sp1_set', 0.0))

        steps: List[ProcessStep] = [
            # 초기화: Flow Off, Valve Open, Zeroing
            ProcessStep(ActionType.MFC_CMD, f"Ch{ch} Flow OFF", params=('FLOW_OFF', {'channel': ch})),
            ProcessStep(ActionType.MFC_CMD, "MFC Valve Open", params=('VALVE_OPEN', {})),
            ProcessStep(ActionType.MFC_CMD, f"Ch{ch} ZEROING", params=('MFC_ZEROING', {'channel': ch})),
            ProcessStep(ActionType.MFC_CMD, "PS ZEROING",     params=('PS_ZEROING', {})),

            # 가스 밸브(PLC)
            ProcessStep(ActionType.PLC_CMD, f"{p.get('selected_gas')} Valve Open",
                        params=(self._gas_valve_button, True)),

            # 유량 설정 & ON
            ProcessStep(ActionType.MFC_CMD, f"Ch{ch} {flow:.2f}sccm 설정",
                        params=('FLOW_SET', {'channel': ch, 'value': flow})),
            ProcessStep(ActionType.MFC_CMD, f"Ch{ch} Flow ON",
                        params=('FLOW_ON', {'channel': ch})),

            # 압력 제어 준비 및 목표 설정(SP1=UI값)
            ProcessStep(ActionType.MFC_CMD, "SP4 ON", params=('SP4_ON', {})),
            ProcessStep(ActionType.MFC_CMD, f"SP1={sp1_ui:.2f} 설정", params=('SP1_SET', {'value': sp1_ui})),
            ProcessStep(ActionType.DELAY, "압력 안정화 대기(60초)", duration_sec=60),
        ]

        # 선택: 건 셔터
        if p.get('use_g1', False):
            steps.append(ProcessStep(ActionType.PLC_CMD, "Gun Shutter 1 Open", params=('S1_button', True)))
        if p.get('use_g2', False):
            steps.append(ProcessStep(ActionType.PLC_CMD, "Gun Shutter 2 Open", params=('S2_button', True)))

        # 파워 안정화
        steps.append(ProcessStep(ActionType.POWER_WAIT, "파워 목표치 도달 대기"))

        # 압력 제어 시작
        steps.append(ProcessStep(ActionType.MFC_CMD, "SP1 ON", params=('SP1_ON', {})))

        # Shutter Delay
        sd_sec = max(0, int(math.ceil(float(p.get('shutter_delay', 0.0)) * 60)))
        if sd_sec > 0:
            steps.append(ProcessStep(ActionType.DELAY, f"Shutter Delay {sd_sec}s",
                                     duration_sec=sd_sec, timer_purpose='shutter'))

        # Main Shutter
        if float(p.get('process_time', 0.0)) > 0:
            steps.append(ProcessStep(ActionType.PLC_CMD, "Main Shutter Open", params=('MS_button', True)))

        # 메인 공정(이 구간만 폴링 ON)
        pt_sec = max(0, int(math.ceil(float(p.get('process_time', 0.0)) * 60)))
        if pt_sec > 0:
            steps.append(ProcessStep(ActionType.DELAY, f"메인 공정 {pt_sec}s 진행",
                                     duration_sec=pt_sec, timer_purpose='process', polling=True))

        # 종료: Main Shutter 닫기, 가스 닫기
        steps.extend([
            ProcessStep(ActionType.PLC_CMD, "Main Shutter Close", params=('MS_button', False)),
            ProcessStep(ActionType.PLC_CMD, f"{p.get('selected_gas')} Valve Close",
                        params=(self._gas_valve_button, False)),
        ])

        return steps

    # ==================== 스텝 실행 ====================

    def _next_step(self):
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

        # 스텝 진입 시 폴링 설정
        self.command_requested.emit("set_polling", {'enable': bool(step.polling)})

        # 액션 분기
        if step.action == ActionType.MFC_CMD:
            cmd, args = step.params
            self.command_requested.emit(cmd, dict(args))

        elif step.action == ActionType.PLC_CMD:
            btn, st = step.params
            self.update_plc_port.emit(str(btn), bool(st))
            QTimer.singleShot(800, self._next_step)  # 릴레이/실린더 여유

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
        # 다른 액션 중일 때 들어온 확인은 무시

    @Slot(str)
    def _on_mfc_failed(self, why: str):
        if not self._running:
            return
        step = self._steps[self._idx] if 0 <= self._idx < len(self._steps) else None
        bad = (step.params[0] if (step and step.params) else "?")
        self.status_message.emit("MFC(실패)", f"'{bad}' 실패: {why}")
        self.critical_error.emit(f"MFC 통신 오류: {why}")
        self.stop_process()

    # ==================== 파워 안정화 ====================

    def _power_wait(self):
        dc_power = float(self.params.get('dc_power', 0.0) or 0.0)
        rf_power = float(self.params.get('rf_power', 0.0) or 0.0)
        rf_offset = float(self.params.get('rf_offset', 0.0) or 0.0)
        rf_param  = float(self.params.get('rf_param', 1.0) or 1.0)

        loops: List[Tuple[str, QEventLoop]] = []

        if dc_power > 0.0:
            dc_loop = QEventLoop()
            self.dc.target_reached.connect(dc_loop.quit)
            loops.append(("dc", dc_loop))
            self.start_dc_power.emit(dc_power)

        if rf_power > 0.0:
            rf_loop = QEventLoop()
            self.rf.target_reached.connect(rf_loop.quit)
            loops.append(("rf", rf_loop))
            self.start_rf_power.emit({'target': rf_power, 'offset': rf_offset, 'param': rf_param})

        if not loops:
            self._next_step()
            return

        self._active_loops = loops  # ★ STOP에서 끊어낼 수 있도록 보관
        self.status_message.emit("정보", "파워 목표치 도달 대기중...")

        for _name, lp in loops:
            # STOP 눌렀으면 더 기다리지 않음
            if not self._running or self._stop_pending:
                break
            lp.exec()

        # disconnect
        for name, lp in loops:
            try:
                if name == "dc": self.dc.target_reached.disconnect(lp.quit)
                else:            self.rf.target_reached.disconnect(lp.quit)
            except:
                pass
        self._active_loops = []

        if not self._running:   # STOP 중이면 여기서 종료
            return

        self.status_message.emit("정보", "파워 안정화 완료.")
        self._next_step()

    # ==================== 딜레이/타이머 ====================

    def _emit_delay_ui(self, remaining_sec: int):
        """남은 초가 바뀌었을 때만 해당 UI 시그널을 보낸다."""
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

        # 모노토닉 시계 시작
        self._delay_clock = QElapsedTimer()
        self._delay_clock.start()

        # 시작 시점에 남은 시간(=총 시간) 1회 즉시 반영
        self._last_emitted_sec = -1
        self._emit_delay_ui(self._delay_total_sec)

        if self._timer:
            # 타이머는 단순 하트비트 역할만 수행
            self._timer.start()
        else:
            self.status_message.emit("오류", "타이머 초기화 누락")
            self._next_step()

    def _on_tick(self):
        # 프로세스 중이 아니면 타이머 정지 및 폴링 OFF
        if not self._running:
            if self._timer and self._timer.isActive():
                self._timer.stop()
            self.command_requested.emit("set_polling", {'enable': False})
            return

        # 딜레이 구간이 아닐 수 있음
        if not self._delay_clock:
            return

        # QElapsedTimer 기반으로 경과/잔여 시간 계산
        elapsed_ms = self._delay_clock.elapsed()  # ms
        elapsed_sec = int(elapsed_ms // 1000)
        remaining = max(0, self._delay_total_sec - elapsed_sec)

        # 초 단위로 값이 변했을 때만 UI 갱신
        self._emit_delay_ui(remaining)

        # 종료 처리
        if remaining <= 0:
            # 하트비트 정지 및 폴링 OFF
            if self._timer and self._timer.isActive():
                self._timer.stop()
            self.command_requested.emit("set_polling", {'enable': False})

            # 상태 초기화
            self._delay_clock = None
            self._delay_total_sec = 0
            self._timer_purpose = None
            self._last_emitted_sec = -1

            # 다음 스텝으로
            self._next_step()

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
        # 타이머/딜레이 상태 초기화
        self._delay_clock = None
        self._delay_total_sec = 0
        self._timer_purpose = None
        self._last_emitted_sec = -1

    @Slot()
    def request_stop(self):
        """UI/다른 스레드에서 눌러도 항상 '내 스레드'에서 안전 종료."""
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
        """실제 안전 종료(컨트롤러 스레드 안에서만 실행)."""
        # 진행 중인 딜레이 즉시 중단
        if self._timer and self._timer.isActive():
            self._timer.stop()
        self._delay_clock = None
        self._delay_total_sec = 0
        self._timer_purpose = None
        self._last_emitted_sec = -1

        # 파워 대기 루프가 돌고 있으면 즉시 깨움
        if self._active_loops:
            for _name, lp in self._active_loops:
                try: lp.quit()
                except: pass
            self._active_loops.clear()

        # 여기서부터는 기존 stop_process 본문 그대로
        if not self._running:
            self.finished.emit()
            self._stop_pending = False
            return

        self.status_message.emit("정보", "종료 시퀀스를 실행합니다.")
        self._running = False

        # 폴링 OFF
        self.command_requested.emit("set_polling", {'enable': False})

        # Main Shutter 닫기
        self.stage_monitor.emit("M.S. close...")
        self.update_plc_port.emit('MS_button', False)

        # 파워 끄기
        if self.is_dc_on:
            self.status_message.emit("DCpower", "DC 파워 OFF")
            self.stop_dc_power.emit()
        if self.is_rf_on:
            self.status_message.emit("RFpower", "RF 파워 OFF (ramp-down)")

            # 이벤트 루프 준비
            self._rfdown_wait = QEventLoop()

            # ★ RF → Process 스레드로 안전하게 큐드 연결
            try:
                self.rf.ramp_down_finished.connect(
                    self._on_rf_rampdown_finished,
                    type=Qt.ConnectionType.QueuedConnection
                )
            except TypeError:
                # 일부 환경에서 type= 키워드가 안 먹으면 기본 Auto로도 무방
                self.rf.ramp_down_finished.connect(self._on_rf_rampdown_finished)

            # 타임아웃(예: 120초) — 신호 미수신 시 빠져나오기
            QTimer.singleShot(120_000, self._on_rf_rampdown_finished)

            # 램프다운 시작
            self.stop_rf_power.emit()

            # 완료까지 대기
            self._rfdown_wait.exec()

            # 뒷정리
            try:
                self.rf.ramp_down_finished.disconnect(self._on_rf_rampdown_finished)
            except Exception:
                pass
            self._rfdown_wait = None

        # MFC 종료 루틴 (FLOW_OFF, VALVE_OPEN) — 기존 코드 그대로
        ch = self._process_channel
        loop = QEventLoop()
        def _quit(*_):
            try: loop.quit()
            except: pass
        self.mfc.command_confirmed.connect(_quit)
        self.mfc.command_failed.connect(_quit)
        self.command_requested.emit("FLOW_OFF", {'channel': ch})
        loop.exec()
        self.command_requested.emit("VALVE_OPEN", {})
        loop.exec()
        try:
            self.mfc.command_confirmed.disconnect(_quit)
            self.mfc.command_failed.disconnect(_quit)
        except:
            pass

        # 건 셔터/가스 닫기
        self.update_plc_port.emit('S1_button', False)
        self.update_plc_port.emit('S2_button', False)
        gas_name = "Ar" if ch == 1 else "O2"
        self.status_message.emit("PLC", f"{gas_name} Valve Close")
        self.update_plc_port.emit(self._gas_valve_button, False)

        QTimer.singleShot(800, self._finish_stop)
        self._stop_pending = False

    @Slot()
    def _finish_stop(self):
        self.status_message.emit("정보", "종료 완료")
        self.finished.emit()

    def _is_connected(self, obj) -> bool:
        """컨트롤러의 연결상태를 최대한 보수적으로 판정."""
        # 1) 우선 is_connected()가 있으면 그것을 신뢰
        try:
            fn = getattr(obj, "is_connected", None)
            if callable(fn):
                return bool(fn())
        except Exception:
            pass
        # 2) 공통 속성으로 대체 판정
        for name in ("serial", "serial_mfc", "serial_dcpower"):
            s = getattr(obj, name, None)
            if s is not None and hasattr(s, "isOpen"):
                try:
                    if s.isOpen():
                        return True
                except Exception:
                    pass
        return False
