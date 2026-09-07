# controller/heater_atmosphere.py
"""히터 전용 가스 도입 · 압력 제어.

스퍼터 공정 없이 히터만 돌릴 때, 가스를 먼저 넣고 압력을 잡은 뒤 히터를 켠다.
스텝 목록은 controller/process_controller.py 의 빌더 함수를 그대로 쓴다 —
공정과 히터가 서로 다른 시퀀스로 갈라지지 않게 하려는 것이다.

[공정과 다른 점]
  SP4_ON / 60초 안정화 / SP3 / SP2 단계가 없다. 스퍼터는 플라즈마를 자연압에서
  점화한 뒤 단계적으로 내리지만, 여기는 파워가 없으므로 곧바로 SP1 로 간다.

[스레드]
  heater_recipe.py 와 같다. GUI 스레드에서 QTimer 로만 돈다. QEventLoop 로
  블로킹하면 UI 가 멈춘다. 장치 레지스터를 직접 만지지 않고 시그널만 낸다.

[MFC 소유권]
  공정 컨트롤러와 같은 MFC 를 쓴다. 동시에 쓰면 서로의 command_confirmed 를
  가로채므로, main.py 가 '공정 중에는 시작 불가 / 분위기 활성 중에는 공정 시작
  불가'로 막는다. 여기서도 IDLE 일 때 들어오는 시그널은 전부 무시한다.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from PyQt6.QtCore import QObject, QTimer, pyqtSignal as Signal, pyqtSlot as Slot

from controller.process_controller import (
    ActionType,
    ProcessStep,
    build_gas_pre_steps,
    build_gas_intro_steps,
    build_pressure_stage_steps,
    build_gas_release_steps,
    _invoke_connect,
)
from lib.config import MFC_DELAY_MS_VALVE

# 상태
IDLE, PREPARING, READY, RELEASING = "IDLE", "PREPARING", "READY", "RELEASING"

# MFC 응답 대기 기본 타임아웃
MFC_TIMEOUT_SEC = 60.0
# 밸브는 실제로 움직이는 데 오래 걸린다(MFC_DELAY_MS_VALVE 뒤에 위치 확인).
MFC_VALVE_TIMEOUT_SEC = max(30.0, MFC_DELAY_MS_VALVE / 1000.0 * 3 + 10.0)
# WAIT_PRESSURE 는 스텝이 들고 있는 timeout 에 이만큼 여유를 더해 감시한다.
WAIT_PRESSURE_MARGIN_SEC = 30.0
# PLC 릴레이/실린더 여유 (공정 컨트롤러와 같은 값)
PLC_SETTLE_MS = 800
# 해제 시퀀스는 오래 붙잡지 않는다. 응답이 없어도 다음으로 넘어간다.
RELEASE_STEP_TIMEOUT_SEC = 3.0

# 최종 압력(SP1) 도달 대기 한도
SP1_TIMEOUT_SEC = 300.0


class HeaterAtmosphere(QObject):
    """히터 전용 가스·압력 상태기계."""

    command_requested = Signal(str, dict)    # → MFC.handle_command
    update_plc_port   = Signal(str, bool)    # → PLC.update_port_state
    status_message    = Signal(str, str)     # (level, text) — level 은 "히터" 계열
    state_changed     = Signal(str, str)     # (state, detail)
    ready             = Signal()
    failed            = Signal(str)
    released          = Signal()

    def __init__(self, mfc_controller, parent=None):
        super().__init__(parent)
        self._mfc = mfc_controller

        self._state = IDLE
        self._steps: List[ProcessStep] = []
        self._idx = -1

        self._channels: List[int] = []
        self._gas_buttons: List[str] = []
        self._params: Dict[str, float] = {}

        # 지금 응답을 기다리는 MFC 명령. None 이면 기다리는 것이 없다.
        self._expect: Optional[str] = None

        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._on_timeout)

        # DELAY 스텝의 남은 시간 표시용
        self._delay_left = 0
        self._delay_tick = QTimer(self)
        self._delay_tick.setInterval(1000)
        self._delay_tick.timeout.connect(self._on_delay_tick)

    # ==================== 조회 ====================
    def state(self) -> str:
        return self._state

    def is_active(self) -> bool:
        """무언가 붙잡고 있는 상태인가(공정 시작을 막아야 하는 상태)."""
        return self._state in (PREPARING, READY, RELEASING)

    def is_ready(self) -> bool:
        return self._state == READY

    def params(self) -> dict:
        return dict(self._params)

    def channels(self) -> List[int]:
        return list(self._channels)

    # ==================== 시작 ====================
    def start(self, use_ar: bool, use_o2: bool,
              ar_flow: float, o2_flow: float, sp1: float) -> bool:
        """가스 도입 + 압력 제어를 시작한다. 받아들이면 True."""
        if self._state != IDLE:
            self.status_message.emit(
                "히터(경고)", f"[가스] 이미 동작 중입니다 ({self._state})")
            return False

        # --- 입력 검증 ---
        if not (use_ar or use_o2):
            self.status_message.emit("히터(오류)", "[가스] 가스를 하나 이상 선택하세요.")
            return False
        try:
            ar_flow = float(ar_flow or 0.0)
            o2_flow = float(o2_flow or 0.0)
            sp1 = float(sp1 or 0.0)
        except (TypeError, ValueError):
            self.status_message.emit("히터(오류)", "[가스] 유량/압력이 숫자가 아닙니다.")
            return False
        if use_ar and ar_flow <= 0:
            self.status_message.emit("히터(오류)", "[가스] Ar 유량은 0보다 커야 합니다.")
            return False
        if use_o2 and o2_flow <= 0:
            self.status_message.emit("히터(오류)", "[가스] O2 유량은 0보다 커야 합니다.")
            return False
        if sp1 <= 0:
            self.status_message.emit("히터(오류)", "[가스] 목표 압력은 0보다 커야 합니다.")
            return False

        # --- MFC 연결 ---
        if not self._ensure_mfc():
            self.status_message.emit("히터(오류)", "[가스] MFC 연결에 실패했습니다.")
            return False

        # --- 채널/밸브: start_process_flow 와 같은 규칙 ---
        channels: List[int] = []
        gas_buttons: List[str] = []
        flows: Dict[int, float] = {}
        if use_ar:
            channels.append(1)
            gas_buttons.append("Ar_Button")
            flows[1] = ar_flow
        if use_o2:
            channels.append(2)
            gas_buttons.append("O2_Button")
            flows[2] = o2_flow

        self._channels = channels
        self._gas_buttons = gas_buttons
        self._params = {
            "use_ar": bool(use_ar), "use_o2": bool(use_o2),
            "ar_flow": ar_flow if use_ar else 0.0,
            "o2_flow": o2_flow if use_o2 else 0.0,
            "sp1": sp1,
        }

        self.command_requested.emit("set_active_channels", {"channels": list(channels)})

        self._steps = (
            build_gas_pre_steps(channels)
            + build_gas_intro_steps(channels, flows, gas_buttons)
            + build_pressure_stage_steps(
                1, sp1, f"히터 분위기(SP1={sp1:.2f})",
                timeout_sec=SP1_TIMEOUT_SEC, fail_on_timeout=True)
        )
        self._idx = -1
        self._state = PREPARING

        gas_txt = " · ".join(
            ([f"Ar {ar_flow:g} sccm"] if use_ar else [])
            + ([f"O2 {o2_flow:g} sccm"] if use_o2 else []))
        self.status_message.emit(
            "히터", f"[가스] 도입 시작: {gas_txt} · 목표 {sp1:.2f} mTorr "
                    f"({len(self._steps)}단계)")
        self._emit_state()
        self._next_step()
        return True

    def _ensure_mfc(self) -> bool:
        """MFC 가 연결돼 있지 않으면 연결을 시도한다(공정 컨트롤러와 같은 방식)."""
        try:
            if self._is_connected(self._mfc):
                return True
            _invoke_connect(self._mfc, "connect_mfc_device")
            return bool(self._is_connected(self._mfc))
        except Exception:
            return False

    @staticmethod
    def _is_connected(obj) -> bool:
        """SputterProcessController._is_connected 와 같은 판정."""
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
        return False

    # ==================== 해제 ====================
    def release(self, reason: str = "해제") -> None:
        """가스를 끊고 밸브를 닫는다. 멱등 — 이미 해제 중이면 무시."""
        if self._state in (IDLE, RELEASING):
            return

        # 진행 중이던 스텝의 타이머와 기대 명령을 버린다
        self._cancel_pending()

        self._state = RELEASING
        self.status_message.emit("히터", f"[가스] 해제: {reason}")

        # 감시부터 끈다 — 유량이 0으로 떨어지는 과정에서 경고가 뜨지 않게
        try:
            self.command_requested.emit("set_monitoring", {"enable": False})
            self.command_requested.emit("set_polling", {"enable": False})
        except Exception:
            pass

        self._steps = build_gas_release_steps(self._channels, self._gas_buttons)
        self._idx = -1
        self._emit_state()
        self._next_step()

    def _cancel_pending(self):
        try:
            self._timer.stop()
        except Exception:
            pass
        try:
            self._delay_tick.stop()
        except Exception:
            pass
        self._expect = None

    # ==================== 실행기 ====================
    def _emit_state(self, detail: str = ""):
        try:
            if not detail:
                n = len(self._steps)
                cur = min(max(self._idx + 1, 0), n)
                msg = self._steps[self._idx].message if 0 <= self._idx < n else ""
                detail = f"{cur}/{n} {msg}".strip()
            self.state_changed.emit(self._state, detail)
        except Exception:
            pass

    def _next_step(self):
        self._expect = None
        self._idx += 1

        if self._idx >= len(self._steps):
            if self._state == RELEASING:
                self._finish_release()
            else:
                self._enter_ready()
            return

        st = self._steps[self._idx]
        self._emit_state()
        self.status_message.emit(
            "히터", f"[가스] {self._idx + 1}/{len(self._steps)} {st.message}")

        act = st.action
        if act == ActionType.MFC_CMD:
            self._run_mfc(st)
        elif act == ActionType.PLC_CMD:
            self._run_plc(st)
        elif act == ActionType.DELAY:
            self._run_delay(st)
        else:
            self._fail(f"지원하지 않는 동작입니다: {getattr(act, 'value', act)}")

    def _run_mfc(self, st: ProcessStep):
        try:
            cmd, args = st.params
            args = dict(args or {})
        except Exception:
            self._fail(f"스텝 인자가 잘못되었습니다: {st.message}")
            return

        self._expect = cmd
        self._timer.start(int(self._mfc_timeout(cmd, args) * 1000))
        try:
            self.command_requested.emit(cmd, args)
        except Exception as e:
            self._fail(f"명령 전송 실패({cmd}): {e}")

    def _mfc_timeout(self, cmd: str, args: dict) -> float:
        if self._state == RELEASING:
            return RELEASE_STEP_TIMEOUT_SEC
        if cmd == "WAIT_PRESSURE":
            try:
                return float(args.get("timeout_sec") or 0.0) + WAIT_PRESSURE_MARGIN_SEC
            except Exception:
                return SP1_TIMEOUT_SEC + WAIT_PRESSURE_MARGIN_SEC
        if cmd == "VALVE_OPEN":
            return MFC_VALVE_TIMEOUT_SEC
        return MFC_TIMEOUT_SEC

    def _run_plc(self, st: ProcessStep):
        try:
            btn, state = st.params
        except Exception:
            self._fail(f"스텝 인자가 잘못되었습니다: {st.message}")
            return
        try:
            self.update_plc_port.emit(str(btn), bool(state))
        except Exception as e:
            self._fail(f"PLC 명령 실패({btn}): {e}")
            return
        QTimer.singleShot(PLC_SETTLE_MS, self._next_step)   # 릴레이/실린더 여유

    def _run_delay(self, st: ProcessStep):
        """지금 시퀀스에는 DELAY 가 없다. 빌더 호환용으로만 둔다."""
        try:
            self._delay_left = int(getattr(st, "duration_sec", 0) or 0)
        except Exception:
            self._delay_left = 0
        if self._delay_left <= 0:
            QTimer.singleShot(0, self._next_step)
            return
        self._delay_tick.start()
        self._timer.start(self._delay_left * 1000)

    def _on_delay_tick(self):
        self._delay_left = max(0, self._delay_left - 1)
        self._emit_state(f"대기 {self._delay_left}초")

    def _on_timeout(self):
        self._delay_tick.stop()
        if self._state == RELEASING:
            # 해제는 응답이 없어도 계속 간다 — 밸브를 반드시 닫아야 한다
            self.status_message.emit(
                "히터(경고)", f"[가스] 해제 단계 응답 없음 — 계속 진행: {self._cur_msg()}")
            self._next_step()
            return
        if self._expect is None and self._steps and self._idx < len(self._steps) \
                and self._steps[self._idx].action == ActionType.DELAY:
            self._next_step()
            return
        self._fail(f"응답 시간 초과: {self._cur_msg()}")

    def _cur_msg(self) -> str:
        try:
            return self._steps[self._idx].message
        except Exception:
            return "-"

    # ==================== 완료/실패 ====================
    def _enter_ready(self):
        self._cancel_pending()
        self._state = READY
        # 준비가 끝난 뒤에야 감시를 켠다
        try:
            self.command_requested.emit("set_polling", {"enable": True})
            self.command_requested.emit("set_monitoring", {"enable": True})
        except Exception:
            pass
        self.status_message.emit(
            "히터", f"[가스] 준비 완료 — {self._params.get('sp1', 0.0):.2f} mTorr 유지")
        self._emit_state("준비됨")
        self.ready.emit()

    def _finish_release(self):
        self._cancel_pending()
        self._state = IDLE
        self._steps = []
        self._idx = -1
        self.status_message.emit("히터", "[가스] 해제 완료")
        self._emit_state("대기")
        self.released.emit()

    def _fail(self, why: str):
        self._cancel_pending()
        self.status_message.emit("히터(오류)", f"[가스] 실패: {why}")
        self._emit_state(f"오류: {why}")
        self.failed.emit(why)
        # 넣던 가스는 반드시 되돌린다
        self.release(why)

    # ==================== MFC 응답 ====================
    @Slot(str)
    def on_mfc_confirmed(self, cmd: str):
        # IDLE 이면 공정 컨트롤러의 응답이다. 절대 건드리지 않는다.
        if self._state == IDLE:
            return
        if self._expect is None:
            return
        if cmd != self._expect:
            self.status_message.emit(
                "히터(경고)", f"[가스] 기다리던 응답이 아닙니다: {cmd} (대기 {self._expect})")
            return
        self._timer.stop()
        self._expect = None
        self._next_step()

    @Slot(str, str)
    def on_mfc_failed(self, cmd: str, reason: str):
        if self._state == IDLE:
            return
        if self._expect is None or cmd != self._expect:
            self.status_message.emit(
                "히터(경고)", f"[가스] 다른 명령의 실패 보고: {cmd} ({reason})")
            return
        self._timer.stop()
        self._expect = None
        if self._state == RELEASING:
            # 해제 중 실패는 멈출 이유가 못 된다. 남은 밸브를 마저 닫는다.
            self.status_message.emit(
                "히터(경고)", f"[가스] 해제 단계 실패 — 계속 진행: {cmd} ({reason})")
            self._next_step()
            return
        self._fail(f"{cmd} 실패 ({reason})")
