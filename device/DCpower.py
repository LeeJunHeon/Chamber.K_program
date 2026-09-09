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
    DC_FAIL_MAX_TICKS, DC_POWER_ERROR_RATIO, DC_POWER_ERROR_MAX_COUNT,
    DC_MIN_CURRENT_ABORT_COUNT,
    DC_CONTROL_GAIN, DC_RAMP_STEP_A, DC_MAINTAIN_STEP_UP_A, DC_MAINTAIN_STEP_DOWN_A,
    DC_LIMIT_STALL_SEC, DC_SMALL_ERROR_RATIO, DC_SMALL_ERROR_GAIN,
)
from lib.dc_control import power_step_current, is_at_current_cap

class DCPowerController(QObject):
    update_dc_status_display = Signal(float, float, float)  # (P, V, I)
    status_message = Signal(str, str)
    target_reached = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.target_power: float = 0.0
        self._is_running: bool = False
        self.state: str = "IDLE"
        self.error_count: int = 0
        self.power_error_count: int = 0 # DC 파워 편차(±10%) 모니터링용 카운터

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

        # 제어식은 lib/dc_control.py (ΔI = ΔP / V). 스텝 상한·이득은 lib/config.py 의 DC_* 상수.
        #   기존 고정 스텝(±0.001 A/s, 과다 12% 이상일 때만 0.005 A/s)은 압력 단계 전환 때
        #   V 가 20~30% 뛰는 것을 못 따라가 ±10% 이탈 감시에 걸렸다 (2026-09-09 로그).

        self._fail_no_output_ticks = 0
        self._min_current_abort_count = 0   # ← 추가: 저전류 연속 카운터
        self._limit_stall_ticks = 0         # 램프업 중 전류/전압 상한에 걸린 채 목표 미달인 연속 초

        # ▼ NEW: shutter delay 시작 시점에 True로 전환 → 이때부터 ±% 이탈 abort 활성화
        self._power_monitor_armed: bool = False

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

        self.serial.setPortName(DC_PORT)
        if not self.serial.open(QIODeviceBase.OpenModeFlag.ReadWrite):
            self.status_message.emit("DCpower", f"연결 실패: {self.serial.errorString()}")
            return False

        # DTR/RTS 및 버퍼 초기화
        self.serial.setDataTerminalReady(True)
        self.serial.setRequestToSend(False)
        self.serial.clear(QS.Direction.AllDirections)
        self._rx.clear()

        self.status_message.emit("DCpower", f"{DC_PORT} 연결 성공(QSerialPort, LF 종단)")
        return True
    
    @Slot()
    def arm_power_monitor(self):
        """Shutter Delay 시작 시점에 호출되어 setpoint 이탈 감시(±%×N abort)를 활성화한다.
        이전 SP step-down + DC Power Delay 구간에서는 비활성 상태이며,
        여기서 활성화 + 카운터 리셋한다."""
        if not self._is_running:
            return
        if self._power_monitor_armed:
            return  # 이미 armed
        self._power_monitor_armed = True
        self.power_error_count = 0
        self.status_message.emit(
            "DCpower",
            f"setpoint 이탈 감시 활성 (±{DC_POWER_ERROR_RATIO*100:.1f}%, "
            f"{DC_POWER_ERROR_MAX_COUNT}회 연속 시 중단)"
        )

    # ---------------- 공정 시작 ----------------
    @Slot(float)
    def start_process(self, target_power: float):
        if self._is_running:
            self.status_message.emit("DCpower", "경고: DC 파워가 이미 동작 중입니다.")
            return

        if self.serial is None or not self.serial.isOpen():
            if not self.connect_dcpower_device():
                return

        self.target_power = max(0.0, min(DC_MAX_POWER, float(target_power)))
        self.status_message.emit("DCpower", f"프로세스 시작 (목표: {self.target_power:.1f} W)")

        self.power_error_count = 0 # ★ 새 공정 시작 시 편차 카운터 초기화
        self._fail_no_output_ticks = 0
        self._min_current_abort_count = 0
        self._limit_stall_ticks = 0
        self._power_monitor_armed = False   # ▼ NEW: Shutter Delay 전까지 감시 비활성

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

        if self.state == "RAMPING_UP":
            self._tick_ramping_up(now_power, now_v, now_i, diff)
        elif self.state == "MAINTAINING":
            self._tick_maintaining(now_power, now_v, now_i, diff)

    def _tick_ramping_up(self, now_power: float, now_v: float, now_i: float, diff: float) -> None:
        """램프업 1초 처리: 목표까지 전류를 1초당 DC_RAMP_STEP_A 이내로 올린다.

        전류 상한(DC_MAX_CURRENT) 또는 전압 상한(voltage_guard)에 걸리면 기다려도 파워는
        안 오른다 — CC 모드에서 V 는 플라즈마(압력)가 정하기 때문이다. 그 상태가
        DC_LIMIT_STALL_SEC 동안 이어지면 목표 미달인 채로 유지 단계로 넘겨 압력 step-down 을
        진행시킨다 (압력이 내려가면 V 가 올라 파워가 따라 올라온다). 그래도 목표에 못 미치면
        Shutter Delay 에서 ±% 이탈 감시가 잡는다. 2026-09-09 로그에서는 1.0 A 상한에서
        248~249 W 로 2.5분을 허비했고, 목표가 조금만 더 높았으면 영원히 기다릴 뻔했다.
        """
        if abs(diff) <= DC_TOLERANCE_WATT:
            self._enter_maintaining(f"{self.target_power:.1f}W 도달. 파워 유지 시작")
            return

        if self._ramp_no_output_abort(now_power, now_v, now_i, diff):
            return

        at_i_cap = is_at_current_cap(self.current_current, DC_MAX_CURRENT)
        at_v_cap = now_v >= self.voltage_guard
        if diff > 0 and (at_i_cap or at_v_cap):
            self._limit_stall_ticks += 1
            if self._limit_stall_ticks >= DC_LIMIT_STALL_SEC:
                limit = (f"전류 상한 {DC_MAX_CURRENT:.2f}A" if at_i_cap
                         else f"전압 상한 {self.voltage_guard:.0f}V")
                short_pct = diff / max(self.target_power, 1.0) * 100.0
                self._enter_maintaining(
                    f"{limit}에 걸린 채 {DC_LIMIT_STALL_SEC}s 동안 목표 미달 "
                    f"(P={now_power:.1f}W, 목표 {self.target_power:.1f}W 대비 -{short_pct:.1f}%). "
                    f"이 압력에서는 더 못 올리므로 유지 단계로 진행 — 압력이 내려가면 V 가 올라 목표에 접근함"
                )
                return
            if at_v_cap:
                # CV 모드: 전류 설정을 올려도 파워는 안 오르고 설정값만 쌓인다(와인드업). 그대로 둔다.
                self.status_message.emit(
                    "DCpower",
                    f"Ramping(V-LIMIT) V={now_v:.1f}V ≥ {self.voltage_guard:.0f}V → "
                    f"전류 {self.current_current:.4f}A 유지 (P={now_power:.2f}W, diff={diff:+.2f}W)"
                )
                return
        else:
            self._limit_stall_ticks = 0

        step_i = power_step_current(diff, now_v, DC_CONTROL_GAIN, DC_RAMP_STEP_A, DC_MAINTAIN_STEP_DOWN_A)
        self._apply_current_step("Ramping", step_i, now_power, now_v, diff, DC_CONTROL_GAIN)

    def _tick_maintaining(self, now_power: float, now_v: float, now_i: float, diff: float) -> None:
        """유지 1초 처리: 측정 전압 기준으로 필요한 전류를 바로 계산해 목표 파워를 따라간다.

        압력 단계 전환(SP4→SP3→SP2→SP1)마다 V 가 5~30% 뛰는데, ΔI = ΔP/V 로 계산하면
        그 비율만큼 전류가 즉시 따라 내려가므로 목표 파워·압력이 달라도 파라미터를 다시 맞출
        필요가 없다. 하강 스텝 상한(DC_MAINTAIN_STEP_DOWN_A)은 측정 글리치 한 번에 전류가
        크게 튀는 것을 막는 안전장치다.

        작은 오차(목표의 DC_SMALL_ERROR_RATIO 이내)는 플라즈마 노이즈일 수 있어 이득을
        낮춘다. 매초 전량 보정하면 1초 주기로 상승/하강을 번갈아 하며 스스로 흔든다
        (2026-09-09 17:00 로그: 자기상관 -0.59, 900초 중 473초 보정).
        큰 오차 — 압력 단계 전환 같은 진짜 변화 — 는 기존대로 한 번에 따라간다.
        """
        if self._min_current_abort(now_i):
            return
        if self._power_deviation_abort(now_power, diff):
            return

        # 목표 파워와의 차이가 DC_TOLERANCE_WATT 이하이면 그대로 유지.
        if abs(diff) <= DC_TOLERANCE_WATT:
            return

        if diff > 0 and now_v >= self.voltage_guard:
            # 서플라이가 전압 상한(CV)에 걸린 상태: 전류 설정을 올려도 파워는 안 오르고(설정값만
            # 쌓여 압력 회복 시 오버슈트), 내리면 파워가 더 떨어진다(2026-05-15 사고). 그대로 둔다.
            self.status_message.emit(
                "DCpower",
                f"Maintain(V-LIMIT) V={now_v:.1f}V ≥ {self.voltage_guard:.0f}V → "
                f"전류 {self.current_current:.4f}A 유지 (P={now_power:.2f}W, diff={diff:+.2f}W)"
            )
            return

        # 데드밴드 밖이지만 작은 오차면 절반만 따라간다. 경계는 최소 DC_TOLERANCE_WATT.
        small_w = max(DC_TOLERANCE_WATT, DC_SMALL_ERROR_RATIO * self.target_power)
        gain = DC_SMALL_ERROR_GAIN if abs(diff) <= small_w else DC_CONTROL_GAIN

        step_i = power_step_current(diff, now_v, gain, DC_MAINTAIN_STEP_UP_A, DC_MAINTAIN_STEP_DOWN_A)
        self._apply_current_step("Maintain", step_i, now_power, now_v, diff, gain)

    def _apply_current_step(self, phase: str, step_i: float, now_power: float, now_v: float,
                            diff: float, gain: float = DC_CONTROL_GAIN) -> None:
        """전류 설정을 step_i 만큼 바꿔 서플라이에 보내고 로그를 남긴다.

        gain 은 step_i 를 만들 때 실제로 쓴 이득이다. LIM/PROP 판정이 같은 이득으로
        계산한 값과 비교해야 하고, 1.0 이 아니면 로그에 남겨 왜 조금만 움직였는지
        알 수 있게 한다(예: "Maintain(PROP g0.5)").

        로그 태그:
          PROP = 계산값 그대로 적용, LIM = 1초당 스텝 상한에 잘림,
          CAP  = 전류 상/하한에 붙어 설정이 안 움직임(전송 없음),
          HOLD = 계산된 스텝 자체가 0(전송 없음).

        상/하한에 붙었으면 CURR 를 다시 보내지 않는다. 예전에는 같은 값을 매초
        재전송하면서 "dI=+0.0000A" 를 찍어, 로그만 보면 제어가 도는 것처럼 보였다
        (2026-09-09 16:55:04~16:55:16, 1.0 A 상한에서 10초).
        """
        raw_i = gain * diff / max(now_v, 1.0)
        gtag = "" if abs(gain - DC_CONTROL_GAIN) < 1e-9 else f" g{gain:g}"
        new_i = self._clamp_i(self.current_current + step_i)
        applied = new_i - self.current_current

        if abs(applied) < 1e-9:
            # 설정이 안 바뀌면 보낼 것도 없다. 왜 안 움직이는지만 남긴다.
            if abs(step_i) > 1e-9:
                edge = "전류 상한, 더 못 올림" if step_i > 0 else "전류 하한, 더 못 내림"
                why = "CAP"
            else:
                edge = "계산된 변화량 0"
                why = "HOLD"
            self.status_message.emit(
                "DCpower",
                f"{phase}({why}{gtag}) P={now_power:.2f}W → diff={diff:+.2f}W, "
                f"I={self.current_current:.4f}A — {edge}"
            )
            return

        why = "LIM" if abs(step_i) + 1e-9 < abs(raw_i) else "PROP"
        self.current_current = new_i
        self._send_noresp(f"CURR {self.current_current:.4f}")
        self.status_message.emit(
            "DCpower",
            f"{phase}({why}{gtag}) P={now_power:.2f}W → diff={diff:+.2f}W, "
            f"dI={applied:+.4f}A, I={self.current_current:.4f}A"
        )

    def _enter_maintaining(self, message: str) -> None:
        """RAMPING_UP → MAINTAINING 전환. 공정 컨트롤러(POWER_WAIT)에 target_reached 를 알린다."""
        self.state = "MAINTAINING"
        self._limit_stall_ticks = 0
        self.status_message.emit("DCpower", message)
        self.target_reached.emit()

    def _ramp_no_output_abort(self, now_power: float, now_v: float, now_i: float, diff: float) -> bool:
        """램프업 무응답 보호: 설정전류는 올렸는데 파워가 계속 '거의 0'이면 공정을 중단한다.

        Returns:
            True 면 중단했으므로 호출자는 더 진행하지 말 것.
        """
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
                return True
        else:
            self._fail_no_output_ticks = 0
        return False

    def _min_current_abort(self, now_i: float) -> bool:
        """유지 중 저전류(타겟/케이블/접촉 이상) 연속 감지 시 공정을 중단한다.

        Returns:
            True 면 중단했으므로 호출자는 더 진행하지 말 것.
        """
        if now_i <= DC_MIN_CURRENT_ABORT:
            self._min_current_abort_count += 1
            if self._min_current_abort_count >= DC_MIN_CURRENT_ABORT_COUNT:
                self.status_message.emit(
                    "재시작",
                    f"DC 전류(I={now_i:.4f}A)가 최소 허용값 "
                    f"{DC_MIN_CURRENT_ABORT:.3f}A 이하가 "
                    f"{DC_MIN_CURRENT_ABORT_COUNT}회 연속 감지되었습니다. 공정을 중단합니다."
                )
                self.stop_process()
                return True
        else:
            self._min_current_abort_count = 0   # 복귀 시 리셋
        return False

    def _power_deviation_abort(self, now_power: float, diff: float) -> bool:
        """setpoint 이탈 감시: ±DC_POWER_ERROR_RATIO 를 DC_POWER_ERROR_MAX_COUNT 회 연속 벗어나면 중단.

        Shutter Delay 시작 시점(arm_power_monitor)부터만 활성. 그 전(압력 step-down 구간)에는
        카운터를 누적하지 않는다.

        Returns:
            True 면 중단했으므로 호출자는 더 진행하지 말 것.
        """
        if not self._power_monitor_armed:
            self.power_error_count = 0
            return False

        threshold_w = max(DC_TOLERANCE_WATT, self.target_power * DC_POWER_ERROR_RATIO)
        if abs(diff) <= threshold_w:
            self.power_error_count = 0
            return False

        self.power_error_count += 1
        if self.power_error_count >= DC_POWER_ERROR_MAX_COUNT:
            self.status_message.emit(
                "재시작",
                f"DC 파워가 목표 {self.target_power:.1f}W에서 "
                f"±{DC_POWER_ERROR_RATIO*100:.1f}% 이상 "
                f"연속 {DC_POWER_ERROR_MAX_COUNT}회 벗어났습니다. 공정을 중단합니다. "
                f"(현재: {now_power:.2f}W)"
            )
            self.stop_process()
            return True
        return False

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
    
    def _stop_control_timer(self):
        try:
            if self.control_timer.isActive():
                self.control_timer.stop()
        except Exception:
            pass

    @Slot()
    def stop_process(self):
        was_running = self._is_running

        self._is_running = False
        self.state = "IDLE"
        self.error_count = 0
        self.power_error_count = 0
        self._fail_no_output_ticks = 0
        self._min_current_abort_count = 0
        self._limit_stall_ticks = 0
        self._power_monitor_armed = False   # ▼ NEW

        self._stop_control_timer()

        try:
            if self.serial and self.serial.isOpen():
                self._send_noresp("OUTP OFF")
                self.serial.waitForBytesWritten(200)
        except Exception:
            pass

        self.update_dc_status_display.emit(0.0, 0.0, 0.0)

        if was_running:
            self.status_message.emit("DCpower", "출력 OFF, 대기 상태로 전환")

    @Slot()
    def cleanup(self):
        """
        프로그램 종료용 즉시 정리.
        제어 타이머를 멈추고 출력 OFF 후 시리얼 포트를 닫는다.
        """
        self._is_running = False
        self.state = "IDLE"
        self.error_count = 0
        self.power_error_count = 0
        self._fail_no_output_ticks = 0
        self._min_current_abort_count = 0
        self._limit_stall_ticks = 0
        self._power_monitor_armed = False   # ▼ NEW

        self._stop_control_timer()

        try:
            if self.serial and self.serial.isOpen():
                self._send_noresp("OUTP OFF")
                self.serial.waitForBytesWritten(200)
        except Exception:
            pass

        try:
            if self.serial and self.serial.isOpen():
                self.serial.clear(QS.Direction.AllDirections)
                self.serial.close()
        except Exception:
            pass
        finally:
            self.serial = None
            self._rx.clear()

        self.update_dc_status_display.emit(0.0, 0.0, 0.0)
        self.status_message.emit("DCpower", "시리얼 연결 종료")

    @Slot()
    def close_connection(self):
        self.cleanup()

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
                # 약간의 여유
                QThread.msleep(120)
                return True
            except Exception as e:
                self.status_message.emit("DCpower(경고)", f"전송 예외: {e} (시도 {attempt+1})")
                QThread.msleep(150)
        return False

    def _send_noresp(self, command: str) -> None:
        """응답 불필요한 빠른 명령(전류 미세조정 등)"""
        try:
            self.status_message.emit("DCpower > 전송", command)
            self._write_line(command)
        except Exception as e:
            self.status_message.emit("DCpower(경고)", f"전송 오류(무응답): {e}")

    def _query(self, command: str, timeout_ms: int = 500) -> Optional[str]:
        # 잔여 입력을 readAll()로 비움 (clear(Input) 대신)
        if self.serial and self.serial.bytesAvailable() > 0:
            try: self.serial.readAll()
            except Exception: pass

        self.status_message.emit("DCpower > 전송", command)
        if not self._write_line(command):
            self.status_message.emit("DCpower", "전송 실패")
            return None

        line = self._readline_blocking(timeout_ms)
        if line is None:
            self.status_message.emit("DCpower", "수신 타임아웃")
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

        try:
            if timer.isActive():
                timer.stop()
        except Exception:
            pass

        timer.deleteLater()
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