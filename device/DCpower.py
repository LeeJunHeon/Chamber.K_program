# DCPowerController_qtserial.py — PyQt6 QSerialPort 동기형(이벤트루프 대기) 구현
from __future__ import annotations
import re
from typing import Optional, Tuple

from PyQt6.QtCore import QObject, QTimer, QThread, QEventLoop, pyqtSignal as Signal, pyqtSlot as Slot, Qt
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo, QSerialPort as QS
from PyQt6.QtCore import QIODeviceBase

from lib.config import (
    DC_PORT, DC_BAUDRATE,
    DC_INITIAL_VOLTAGE, DC_INITIAL_CURRENT, DC_MAX_VOLTAGE,
    DC_MAX_CURRENT, DC_MAX_POWER, DC_TOLERANCE_WATT, DC_MAX_ERROR_COUNT,
    DC_MIN_CURRENT_ABORT, DC_FAIL_ISET_THRESHOLD, DC_FAIL_POWER_THRESHOLD,
    DC_FAIL_MAX_TICKS,
)

class DCPowerController(QObject):
    update_dc_status_display = Signal(float, float, float)  # (P, V, I)
    status_message = Signal(str, str)
    target_reached = Signal()
    
    # ✅ OUTP OFF 검증 3회 실패 → 메인에서 구글챗 보낼 수 있게
    output_off_failed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.target_power: float = 0.0
        self._is_running: bool = False
        self.state: str = "IDLE"
        self.error_count: int = 0
        self.power_error_count: int = 0 # ★ DC 파워 편차(±5%) 모니터링용 카운터

        self.current_voltage: float = DC_INITIAL_VOLTAGE
        self.current_current: float = DC_INITIAL_CURRENT
        self.voltage_guard: float = DC_MAX_VOLTAGE - 20.0

        # QtSerialPort
        self.serial: Optional[QSerialPort] = None
        self._rx = bytearray()

        # 1s 제어 루프 (기존 로직 유지)
        self.control_timer = QTimer(self)
        self.control_timer.setInterval(1000)
        self.control_timer.setTimerType(Qt.TimerType.PreciseTimer)
        self.control_timer.timeout.connect(self._on_timer_tick)

        # === 오버슈트 시 빠른 하강용 파라미터 ===
        self.fast_overshoot_watt   = 5.0   # 목표보다 +5W 이상이면 "빠른 하강" 트리거
        self.fast_overshoot_ratio  = 0.12  # 또는 목표의 +12% 초과 시 트리거
        self.step_up               = 0.001 # 부족 시(+) 1초당 +3mA
        self.step_down             = 0.001 # 과다 시(-) 1초당 -6mA
        self.step_down_fast        = 0.005 # "빠른 하강"시 1초당 -40mA (공격적)

        self._fail_no_output_ticks = 0

        # ===== 재연결(최대 5회) =====
        self._want_connected: bool = False
        self._reconnect_attempts: int = 0
        self._max_reconnect_attempts: int = 5
        self._last_disconnect_error: str = ""
        self._fatal_latched: bool = False

        # 끊김 중 동작 복구용 플래그
        self._resume_after_reconnect: bool = False     # 끊기기 전 공정 중이었으면 True
        self._pending_start_power: Optional[float] = None  # 시작 요청이 있는데 아직 연결 안 됨
        self._pending_outp_off: bool = False           # stop 중 끊긴 경우 OFF를 재시도

        # 재연결 타이머 (single-shot)
        self._reconnect_timer = QTimer(self)
        self._reconnect_timer.setSingleShot(True)
        self._reconnect_timer.timeout.connect(self._try_reconnect)

        # QSerialPort error signal 중복 연결 방지
        self._serial_signals_connected: bool = False

    # ---------------- 연결 ----------------
    @Slot()
    def connect_dcpower_device(self) -> bool:
        # 포트 존재 확인
        ports = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if DC_PORT not in ports:
            self.status_message.emit("DCpower", f"{DC_PORT} 포트를 찾을 수 없습니다. 사용 가능: {sorted(ports)}")
            return False

        if self.serial is None:
            self.serial = QSerialPort(self)
            self.serial.setBaudRate(DC_BAUDRATE)
            self.serial.setDataBits(QSerialPort.DataBits.Data8)
            self.serial.setParity(QSerialPort.Parity.NoParity)
            self.serial.setStopBits(QSerialPort.StopBits.OneStop)
            self.serial.setFlowControl(QSerialPort.FlowControl.NoFlowControl)

            # ✅ 시리얼 에러 감지(USB 끊김 등) → 재연결 트리거
            if not self._serial_signals_connected:
                try:
                    self.serial.errorOccurred.connect(self._on_serial_error)
                except Exception:
                    pass
                self._serial_signals_connected = True

        self.serial.setPortName(DC_PORT)
        if not self.serial.open(QIODeviceBase.OpenModeFlag.ReadWrite):
            self.status_message.emit("DCpower", f"연결 실패: {self.serial.errorString()}")
            return False

        # DTR/RTS 및 버퍼 초기화
        self.serial.setDataTerminalReady(True)
        self.serial.setRequestToSend(False)
        self.serial.clear(QS.Direction.AllDirections)
        self._rx.clear()

        # 연결 성공
        self._want_connected = True
        self._fatal_latched = False

        self.status_message.emit("DCpower", f"{DC_PORT} 연결 성공(QSerialPort, LF 종단)")
        return True
    
    # ==================== 재연결 유틸 ====================
    def _backoff_delay_ms(self, attempt_no: int) -> int:
        # 1->500ms, 2->1000ms, 3->2000ms, 4->4000ms, 5->8000ms(캡)
        return min(8000, 500 * (2 ** max(attempt_no - 1, 0)))

    def _close_serial(self):
        try:
            if self.serial and self.serial.isOpen():
                self.serial.close()
        except Exception:
            pass

    def _schedule_reconnect(self):
        if not self._want_connected or self._fatal_latched:
            return
        if self._reconnect_timer.isActive():
            return
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            self._fail_fatal()
            return

        next_attempt = self._reconnect_attempts + 1
        delay_ms = self._backoff_delay_ms(next_attempt)
        self.status_message.emit("DCpower", f"재연결 예약: {next_attempt}/{self._max_reconnect_attempts} (in {delay_ms}ms)")
        self._reconnect_timer.start(delay_ms)

    def _mark_disconnected(self, where: str, ex: Exception | str):
        # 이미 fatal이면 더 진행 X
        if self._fatal_latched:
            return

        self._last_disconnect_error = f"{where}: {ex!r}" if not isinstance(ex, str) else f"{where}: {ex}"

        # 공정 제어 루프 정지(끊긴 상태에서 CURR 계속 보내면 의미 없음)
        try:
            if self.control_timer.isActive():
                self.control_timer.stop()
        except Exception:
            pass

        # 재연결이 필요함
        self._want_connected = True

        # 끊기기 직전에 공정 중이었다면, 붙으면 복구(OUTP ON + setpoint)해줄 것
        self._resume_after_reconnect = bool(self._is_running)

        # 포트 닫기
        self._close_serial()

        self.status_message.emit("DCpower(경고)", f"연결 끊김 감지({where}) → 재연결 시도")
        self._schedule_reconnect()

    def _restore_after_reconnect(self) -> bool:
        """
        재연결 성공 후 복구:
        - stop 요청 중 끊겼으면 OUTP OFF 우선
        - 공정 중 끊겼으면 OUTP ON + APPLy(현재 전압/전류) 재보장 후 타이머 재개
        """
        if not (self.serial and self.serial.isOpen()):
            return False

        # 1) stop 중 끊겼던 경우: OFF를 먼저 시도
        if self._pending_outp_off:
            self.status_message.emit("DCpower", "재연결 후 OUTP OFF 재시도")
            ok = self._ensure_output_off(max_retries=3)
            self._pending_outp_off = False
            if not ok:
                detail = "DC OUTPUT OFF 실패: 재연결 후에도 OUTP?=0 확인 불가"
                self.status_message.emit("DCpower(에러)", detail)
                self.output_off_failed.emit(detail)  # 기존 즉시 알림 루트 활용
            return True  # stop 상태면 여기서 끝

        # 2) 공정 중이었다면 상태 복구
        if self._resume_after_reconnect:
            try:
                # ✅ 1) 재연결 직후 장비 에러 상태를 먼저 로깅(LOCAL/REMOTE/트립 등 디버깅 도움)
                self._log_syst_err("after_reconnect")

                # Reset(*RST)은 위험할 수 있어 복구에서는 *CLS만 사용 (성공 여부 체크)
                if not self._send("*CLS"):
                    return False
                
                # 전압/전류 재보장
                v = self._clamp_v(DC_MAX_VOLTAGE)
                i = self._clamp_i(self.current_current)
                if not self._send(f"APPLy {v:.2f},{i:.4f}"):
                    self._log_syst_err("apply_failed")
                    return False
                
                # ✅ 2) OUTP ON 전에 OUTP?로 상태 확인 → 이미 ON이면 생략
                outp_on = self._is_output_on()
                if outp_on is None:
                    # OUTP? 자체가 실패 = 통신 불안정 가능 → 재연결 루프가 다시 돌게 됨(_query가 mark_disconnected)
                    return False
                
                if outp_on:
                    self.status_message.emit("DCpower", "OUTP?=1(이미 ON) → OUTP ON 생략")
                else:
                    if not self._send("OUTP ON"):
                        self._log_syst_err("outp_on_failed")
                        return False

                self.status_message.emit("DCpower", f"재연결 복구 완료: OUTP ON, APPLy {v:.2f},{i:.4f}")
                # 제어 루프 재개
                self._is_running = True
                self.control_timer.start()
                return True
            
            except Exception as e:
                self.status_message.emit("DCpower(경고)", f"재연결 후 복구 실패: {e}")
                return False

        return True

    def _fail_fatal(self):
        if self._fatal_latched:
            return
        self._fatal_latched = True

        # 모두 정지
        try:
            self.control_timer.stop()
        except Exception:
            pass
        try:
            self._reconnect_timer.stop()
        except Exception:
            pass

        self._is_running = False
        self.state = "IDLE"
        self._want_connected = False
        self._close_serial()

        msg = f"DC 재연결 {self._max_reconnect_attempts}회 실패: {self._last_disconnect_error} → 공정 전체 종료"
        self.status_message.emit("재시작", msg)

    @Slot()
    def _try_reconnect(self):
        if not self._want_connected or self._fatal_latched:
            return

        if self._reconnect_attempts >= self._max_reconnect_attempts:
            self._fail_fatal()
            return

        self._reconnect_attempts += 1
        attempt = self._reconnect_attempts

        self.status_message.emit("DCpower", f"재연결 시도 {attempt}/{self._max_reconnect_attempts}...")
        try:
            ok = self.connect_dcpower_device()
            if not ok:
                raise IOError("connect_dcpower_device failed")

            # 성공
            self.status_message.emit("DCpower", f"재연결 성공 (attempt {attempt})")
            self._reconnect_attempts = 0
            self._last_disconnect_error = ""

            # 재연결 후 복구(OFF pending / 공정 resume)
            if not self._restore_after_reconnect():
                raise IOError("restore_after_reconnect failed")

            # 시작 대기 중이었다면(연결이 늦어서 start 못한 케이스) 시작 수행
            if self._pending_start_power is not None and (not self._is_running):
                p = float(self._pending_start_power)
                self._pending_start_power = None
                self.status_message.emit("DCpower", f"재연결 후 start 재개: target={p:.1f}W")
                self.start_process(p)

        except Exception as e:
            self._last_disconnect_error = f"reconnect_attempt_{attempt}: {e!r}"
            self.status_message.emit("DCpower(경고)", f"재연결 실패 {attempt}/{self._max_reconnect_attempts}: {e}")

            if self._reconnect_attempts >= self._max_reconnect_attempts:
                self._fail_fatal()
            else:
                self._schedule_reconnect()

    @Slot(int)
    def _on_serial_error(self, err: int):
        # QSerialPort 에러 이벤트(USB 끊김 등)에서 재연결 트리거
        # 0(=NoError)은 무시
        if err == 0:
            return
        self._mark_disconnected("QSerialPort.errorOccurred", f"err={err}")

    # ---------------- 공정 시작 ----------------
    @Slot(float)
    def start_process(self, target_power: float):
        if self._is_running:
            self.status_message.emit("DCpower", "경고: DC 파워가 이미 동작 중입니다.")
            return

        if self.serial is None or not self.serial.isOpen():
            # ✅ 연결이 안 되면 start 요청을 보류하고 재연결 루프를 돌린다
            if not self.connect_dcpower_device():
                self._pending_start_power = float(target_power)
                self._want_connected = True
                self._fatal_latched = False
                self._reconnect_attempts = 0
                self.status_message.emit("DCpower(경고)", "DC 포트 연결 실패 → 재연결 시도 후 start 재개 예정")
                self._schedule_reconnect()
                return

        self.target_power = max(0.0, min(DC_MAX_POWER, float(target_power)))
        self.power_error_count = 0 # ★ 새 공정 시작 시 편차 카운터 초기화
        self.status_message.emit("DCpower", f"프로세스 시작 (목표: {self.target_power:.1f} W)")

        self._fail_no_output_ticks = 0

        # 초기화: 전압·전류 동시 설정(APPLy) + 출력 ON
        #  - APPLy는 전압·전류를 동시에 설정할 때만 사용
        if not self._initialize_power_supply(self._clamp_v(DC_MAX_VOLTAGE), self._clamp_i(DC_INITIAL_CURRENT)):
            self.status_message.emit("DCpower(에러)", "초기화 실패")
            return

        self._is_running = True
        self.state = "RAMPING_UP"
        self.control_timer.start()

    # ---------------- 제어 루프 ----------------
    @Slot()
    def _on_timer_tick(self):
        if not self._is_running:
            self.control_timer.stop()
            return

        now_power, now_v, now_i = self.read_dc_power()  # MEAS:ALL?
        if not self._check_measurement(now_v, now_i):
            return

        diff = self.target_power - now_power         # +: 더 올려야 함,  -: 과다(오버슈트)
        # 오버슈트(과다) 판정 기준: 'W 기준' 또는 '% 기준' 중 큰 쪽
        overshoot_w = -diff
        overshoot_trig = (overshoot_w > 0) and (
            overshoot_w >= max(self.fast_overshoot_watt, self.fast_overshoot_ratio * max(self.target_power, 1.0))
        )

        if self.state == "RAMPING_UP":
            if abs(diff) <= DC_TOLERANCE_WATT:
                self.state = "MAINTAINING"
                self.status_message.emit("DCpower", f"{self.target_power:.1f}W 도달. 파워 유지 시작")
                self.target_reached.emit()
                return
            
            # --- 램프업 무응답 보호: 설정전류는 올렸는데 파워가 계속 '거의 0'이면 실패 ---
            if diff > 0 and self.current_current >= DC_FAIL_ISET_THRESHOLD and now_power <= DC_FAIL_POWER_THRESHOLD:
                self._fail_no_output_ticks += 1
                if self._fail_no_output_ticks >= DC_FAIL_MAX_TICKS:
                    self.status_message.emit(
                        "재시작",
                        (f"DC 램프업 실패: Iset≥{DC_FAIL_ISET_THRESHOLD}A인데 P≤{DC_FAIL_POWER_THRESHOLD}W가 "
                        f"{DC_FAIL_MAX_TICKS}s 지속. 장비 OFF/인터락/부하/케이블 확인 필요. "
                        f"(P={now_power:.2f}W, V={now_v:.2f}V, I={now_i:.4f}A, Iset={self.current_current:.4f}A)")
                    )
                    self.stop_process()
                    return
            else:
                self._fail_no_output_ticks = 0

            if now_v > 1.0:
                # 기본 권장 변화량(dI ≈ dP/V)
                di_prop = diff / now_v
            else:
                di_prop = 0.005

            if overshoot_trig:
                # ★ 오버슈트일 때: 빠르게 내리기
                drop_i = overshoot_w / max(now_v, 1.0)       # 내려야 하는 대략적 dI
                step_i = -min(self.step_down_fast, max(self.step_down, drop_i))
                why = "FAST"
            else:
                # 일반 램핑: 너무 큰 스텝 방지 (대칭이지만 음수일 때는 더 크게 클램프해도 됨)
                lo, hi = -0.010, 0.010
                step_i = max(lo, min(hi, di_prop))
                why = "NORM"

            self.current_current = self._clamp_i(self.current_current + step_i)
            self._send_noresp(f"CURR {self.current_current:.4f}")
            self.status_message.emit("DCpower", f"Ramping({why}) P={now_power:.2f}W → diff={diff:+.2f}W, dI={step_i:+.4f}A, I={self.current_current:.4f}A")

        elif self.state == "MAINTAINING":
            # ★ 유지 구간에서 전류(I)가 너무 낮으면 공정 중단
            if now_i <= DC_MIN_CURRENT_ABORT:
                self.status_message.emit(
                    "재시작",
                    f"DC 전류(I={now_i:.4f}A)가 최소 허용값 "
                    f"{DC_MIN_CURRENT_ABORT:.3f}A 이하입니다. 공정을 중단합니다."
                )
                self.stop_process()
                return
    
            # 유지 구간에서는 setpoint 편차로 공정을 중단하지 않고,
            # 목표 파워와의 차이가 DC_TOLERANCE_WATT 이하이면 그대로 유지.
            if abs(diff) <= DC_TOLERANCE_WATT:
                return

            if overshoot_trig:
                # 유지구간에서도 오버슈트면 강하게 끌어내림
                drop_i = overshoot_w / max(now_v, 1.0)
                step_i = -min(self.step_down_fast, max(self.step_down, drop_i))
                why = "FAST"
            else:
                # 전압 가드 근처면 상승은 억제
                if now_v >= self.voltage_guard and diff > 0:
                    step_i = -self.step_down
                    why = "V-GUARD"
                else:
                    # 부족(+): 조금씩 올림 / 과다(-): 조금 더 내림
                    step_i = self.step_up if diff > 0 else -self.step_down
                    why = "NORM"

            self.current_current = self._clamp_i(self.current_current + step_i)
            self._send_noresp(f"CURR {self.current_current:.4f}")
            self.status_message.emit(
                "DCpower",
                f"Maintain({why}) P={now_power:.2f}W → diff={diff:+.2f}W, "
                f"dI={step_i:+.4f}A, I={self.current_current:.4f}A"
            )

        # ==================== setpoint를 5% 이상 이탈시 종료하는 로직 ====================
        # elif self.state == "MAINTAINING":
        #     # ★ 유지 구간에서 목표 파워 대비 편차 감시
        #     #    - 기준: max(DC_TOLERANCE_WATT, target * 5%)
        #     threshold_w = max(DC_TOLERANCE_WATT, self.target_power * DC_POWER_ERROR_RATIO)
        #     if abs(diff) > threshold_w:
        #         self.power_error_count += 1
        #         if self.power_error_count >= DC_POWER_ERROR_MAX_COUNT:
        #             # 메인에서 전체 공정을 중단하도록 "재시작" 레벨로 알림
        #             self.status_message.emit(
        #                 "재시작",
        #                 (
        #                     f"DC 파워가 목표 {self.target_power:.1f}W에서 "
        #                     f"±{DC_POWER_ERROR_RATIO*100:.1f}% 이상 "
        #                     f"연속 {DC_POWER_ERROR_MAX_COUNT}회 벗어났습니다. 공정을 중단합니다."
        #                 ),
        #             )
        #             self.stop_process()
        #             return
        #     else:
        #         # 허용 범위 안으로 돌아오면 카운터 리셋
        #         self.power_error_count = 0

        #     if abs(diff) <= DC_TOLERANCE_WATT:
        #         return

        #     if overshoot_trig:
        #         # ★ 유지구간에서도 오버슈트면 강하게 끌어내림
        #         drop_i = overshoot_w / max(now_v, 1.0)
        #         step_i = -min(self.step_down_fast, max(self.step_down, drop_i))
        #         why = "FAST"
        #     else:
        #         # 전압 가드 근처면 상승은 억제
        #         if now_v >= self.voltage_guard and diff > 0:
        #             step_i = -self.step_down
        #             why = "V-GUARD"
        #         else:
        #             # 부족(+): 조금씩 올림 / 과다(-): 조금 더 내림
        #             step_i = self.step_up if diff > 0 else -self.step_down
        #             why = "NORM"

        #     self.current_current = self._clamp_i(self.current_current + step_i)
        #     self._send_noresp(f"CURR {self.current_current:.4f}")
        #     self.status_message.emit("DCpower", f"Maintain({why}) P={now_power:.2f}W → diff={diff:+.2f}W, dI={step_i:+.4f}A, I={self.current_current:.4f}A")

    # ---------------- 초기화/종료 ----------------
    def _initialize_power_supply(self, voltage: float, current: float) -> bool:
        if not self._send("*RST"): return False
        if not self._send("*CLS"): return False
        # 전압·전류 동시에 설정(APPLy v,i), 메뉴얼 7-3절
        if not self._send(f"APPLy {voltage:.2f},{current:.4f}"): return False
        if not self._send("OUTP ON"): return False
        self.current_voltage = voltage
        self.current_current = current
        return True
    
    def _parse_outp_state(self, resp: Optional[str]) -> Optional[int]:
        """
        OUTP? 응답 파싱: 0(OFF) / 1(ON)
        - 메뉴얼: "0" 또는 "1" (공백 포함 가능)
        """
        if resp is None:
            return None
        s = str(resp).strip()
        if not s:
            return None
        m = re.search(r"([01])", s)
        return int(m.group(1)) if m else None

    def _ensure_output_off(self, max_retries: int = 3) -> bool:
        """
        OUTP OFF 전송 후 OUTP?로 실제 OFF(0) 확인
        - 최대 max_retries회 재시도
        """
        if not (self.serial and self.serial.isOpen()):
            self.status_message.emit("DCpower(에러)", "OUTP OFF 검증 실패: 시리얼 포트가 열려있지 않음")
            return False

        last_resp: Optional[str] = None

        for attempt in range(1, max_retries + 1):
            self.status_message.emit("DCpower", f"OUTP OFF 검증 시도 {attempt}/{max_retries}")

            # 1) OFF 명령 (응답 없음)
            self._send_noresp("OUTP OFF")
            QThread.msleep(200)

            # 2) 상태 조회로 검증
            last_resp = self._query("OUTP?", timeout_ms=900)
            state = self._parse_outp_state(last_resp)

            if state == 0:
                self.status_message.emit("DCpower", f"OUTP?=0 확인(OFF) (attempt {attempt})")
                return True

            self.status_message.emit("DCpower(경고)", f"OUTP OFF 확인 실패: OUTP?={last_resp!r} → 재시도")
            QThread.msleep(250)

        self.status_message.emit("DCpower(에러)", f"OUTP OFF 최종 실패(3회): 마지막 OUTP?={last_resp!r}")
        return False
    
    def _log_syst_err(self, where: str) -> None:
        """
        재연결 직후/출력ON 실패 시점 등에 SYST:ERR?를 조회해 로그로 남긴다.
        예: 0,"No error" 또는 에러 코드/메시지
        """
        resp = self._query("SYST:ERR?", timeout_ms=900)
        if resp is None:
            self.status_message.emit("DCpower(경고)", f"[{where}] SYST:ERR? 조회 실패")
            return
        self.status_message.emit("DCpower", f"[{where}] SYST:ERR? -> {resp}")

    def _is_output_on(self) -> Optional[bool]:
        """
        OUTP? 상태 확인:
        - True  : ON
        - False : OFF
        - None  : 응답 실패(끊김/타임아웃 등)
        """
        resp = self._query("OUTP?", timeout_ms=900)
        state = self._parse_outp_state(resp)  # 0/1/None
        if state is None:
            return None
        return (state == 1)

    @Slot()
    def stop_process(self):
        # ✅ 이미 stop 상태여도 "출력 OFF 보장"을 위해 return하지 않음
        was_running = self._is_running
        self._is_running = False
        self.state = "IDLE"
        self.control_timer.stop()
        self.power_error_count = 0

        if not (self.serial and self.serial.isOpen()):
            detail = "DC OUTPUT OFF 검증 불가: 포트 닫힘(USB 끊김 등) → 재연결 후 OFF 재시도 예정"
            self.status_message.emit("DCpower(에러)", detail)

            # ✅ OFF를 나중에라도 시도하기 위한 플래그
            self._pending_outp_off = True
            self._want_connected = True
            self._fatal_latched = False
            self._schedule_reconnect()

            # ✅ 즉시 알림은 기존 루트 유지
            self.output_off_failed.emit(detail)
            return

        ok = self._ensure_output_off(max_retries=3)

        if ok:
            self.status_message.emit("DCpower", "출력 OFF 확인 완료, 대기 상태로 전환")
        else:
            detail = "DC OUTPUT OFF 실패: 3회 재시도 후에도 OUTP?=0 확인 불가"
            self.status_message.emit("DCpower(에러)", detail)
            # ✅ 메인에서 구글챗 보내도록 신호 발생
            self.output_off_failed.emit(detail)

    @Slot()
    def close_connection(self):
        self._want_connected = False
        try:
            self._reconnect_timer.stop()
        except Exception:
            pass

        self.stop_process()
        QThread.msleep(150)
        if self.serial and self.serial.isOpen():
            self.serial.close()
            self.serial = None
            self.status_message.emit("DCpower", "시리얼 연결 종료")

    # ---------------- 측정 (MEAS:ALL?) ----------------
    def read_dc_power(self):
        # ① 한번에 읽기 (타임아웃 여유)
        resp = self._query("MEAS:ALL?", timeout_ms=1500)
        v, i = self._parse_meas_all(resp)

        # ② 실패 시 개별 쿼리 폴백
        if v is None or i is None:
            v = self._to_float(self._query("MEAS:VOLT?", timeout_ms=1200))
            i = self._to_float(self._query("MEAS:CURR?", timeout_ms=1200))

        p = (v * i) if (v is not None and i is not None) else None
        self.update_dc_status_display.emit(p or 0.0, v or 0.0, i or 0.0)
        return (p or 0.0, v or 0.0, i or 0.0)

    # ---------------- 전송/수신 (QtSerialPort 동기 래핑) ----------------
    def _send(self, command: str, timeout_ms: int = 500) -> bool:
        """응답 없는 일반 명령 (간단 확인 위해, 에러시 재시도)"""
        for attempt in range(DC_MAX_ERROR_COUNT):
            try:
                if attempt > 0:
                    self.status_message.emit("DCpower", f"'{command}' 재시도...({attempt+1}/{DC_MAX_ERROR_COUNT})")
                else:
                    self.status_message.emit("DCpower > 전송", command)

                if not self._write_line(command):
                    raise IOError("write failed")

                QThread.msleep(120)
                return True

            except Exception as e:
                self.status_message.emit("DCpower(경고)", f"전송 예외: {e} (시도 {attempt+1})")
                # ✅ 'write failed'류는 끊김으로 간주 → 재연결 트리거 후 즉시 종료
                if "write failed" in str(e).lower():
                    self._mark_disconnected("_send/write", e)
                    return False
                QThread.msleep(150)
        return False

    def _send_noresp(self, command: str) -> None:
        try:
            self.status_message.emit("DCpower > 전송", command)
            ok = self._write_line(command)
            if not ok:
                self.status_message.emit("DCpower(경고)", f"전송 실패(무응답): write failed ({command})")
                self._mark_disconnected("send_noresp/write", "write failed")
                return
        except Exception as e:
            self.status_message.emit("DCpower(경고)", f"전송 오류(무응답): {e}")
            self._mark_disconnected("send_noresp/except", e)

    def _query(self, command: str, timeout_ms: int = 500) -> Optional[str]:
        # 잔여 입력을 readAll()로 비움 (clear(Input) 대신)
        if self.serial and self.serial.bytesAvailable() > 0:
            try: self.serial.readAll()
            except Exception: pass

        self.status_message.emit("DCpower > 전송", command)
        if not self._write_line(command):
            self.status_message.emit("DCpower", "전송 실패")
            self._mark_disconnected("_query/write", "write failed")
            return None

        line = self._readline_blocking(timeout_ms)
        if line is None:
            self.status_message.emit("DCpower", "수신 타임아웃")
            self._mark_disconnected("_query/timeout", f"timeout {timeout_ms}ms ({command})")
            return None

        else:
            self.status_message.emit("DCpower < 응답", line)
        return line

    def _write_line(self, s: str) -> bool:
        if not (self.serial and self.serial.isOpen()):
            return False
        data = (s.rstrip("\n") + "\n").encode("ascii")  # '\r\n' 사용
        n = int(self.serial.write(data))
        if n <= 0:
            return False
        self.serial.flush()
        self.serial.waitForBytesWritten(200)  # 실제 송신 보장
        return True

    def _readline_blocking(self, timeout_ms: int = 500) -> Optional[str]:
        """readyRead를 기다려 '\n' 또는 '\r'까지 한 줄을 동기적으로 읽는다."""
        if not (self.serial and self.serial.isOpen()):
            return None

        buf = bytearray()
        line_value: Optional[str] = None
        loop = QEventLoop()
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.setInterval(timeout_ms)

        def finish():
            if loop.isRunning():
                loop.quit()

        def on_timeout():
            nonlocal line_value
            line_value = None
            finish()

        def on_ready():
            nonlocal line_value, buf
            ba = self.serial.readAll()
            if not ba.isEmpty():
                buf.extend(bytes(ba))
            # CR/LF 탐색
            i_cr = buf.find(b'\r')
            i_lf = buf.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                return
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line_bytes = bytes(buf[:idx])
            # CRLF/LFCR 처리
            drop = idx + 1
            if drop < len(buf):
                ch = buf[idx]
                nxt = buf[idx + 1] if (idx + 1) < len(buf) else None
                if nxt is not None and ((ch == 13 and nxt == 10) or (ch == 10 and nxt == 13)):
                    drop += 1
            del buf[:drop]
            try:
                line_value = line_bytes.decode("ascii", errors="ignore").strip()
            except Exception:
                line_value = ""
            finish()

        timer.timeout.connect(on_timeout)
        self.serial.readyRead.connect(on_ready)
        timer.start()
        loop.exec()

        try:
            self.serial.readyRead.disconnect(on_ready)
        except Exception:
            pass
        timer.stop()
        return line_value

    # ---------------- 파싱/검증 ----------------
    def _parse_meas_all(self, s: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
        if not s:
            return (None, None)
        s = s.strip()
        # 기본 포맷: "voltage,current"
        if "," in s:
            left, right = s.split(",", 1)
            return (self._to_float(left), self._to_float(right))
        # 폴백: 문자열 내 숫자 2개 추출
        nums = re.findall(r'[-+]?\d+(?:\.\d+)?', s)
        if len(nums) >= 2:
            try:
                return (float(nums[0]), float(nums[1]))
            except Exception:
                pass
        return (None, None)

    def _check_measurement(self, v: Optional[float], i: Optional[float]) -> bool:
        if v is None or i is None:
            self.error_count += 1
            self.status_message.emit("DCpower", f"측정값 오류({self.error_count}/{DC_MAX_ERROR_COUNT})")
            if self.error_count >= DC_MAX_ERROR_COUNT:
                self.status_message.emit("DCpower(에러)", "연속 측정 실패로 공정을 중단합니다.")
                self.stop_process()
            return False
        self.error_count = 0
        return True

    def _to_float(self, s: Optional[str]) -> Optional[float]:
        try:
            if s is None: return None
            s = s.strip()
            if not s: return None
            return float(s)
        except Exception:
            return None

    def _clamp_v(self, v: float) -> float:
        return max(0.0, min(float(v), DC_MAX_VOLTAGE))

    def _clamp_i(self, c: float) -> float:
        return max(0.0, min(float(c), DC_MAX_CURRENT))

    # ---------------- 수신 버퍼 핸들러(잔여 데이터 관리용) ----------------
    def _on_ready_read(self):
        # 동기 _readline_blocking 에서 직접 readAll()을 하므로,
        # 여기서는 잔여쓰레기 누적만 제한(필요 시 로그 추가)
        ba = self.serial.readAll()
        if not ba.isEmpty():
            self._rx.extend(bytes(ba))
            # 누적 제한
            if len(self._rx) > 4096:
                del self._rx[:-4096]

    def is_connected(self) -> bool:
        return bool(self.serial and self.serial.isOpen())