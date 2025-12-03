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
    DC_POWER_ERROR_RATIO, DC_POWER_ERROR_MAX_COUNT,
)

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
        self.power_error_count = 0 # ★ 새 공정 시작 시 편차 카운터 초기화
        self.status_message.emit("DCpower", f"프로세스 시작 (목표: {self.target_power:.1f} W)")

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

    @Slot()
    def stop_process(self):
        if not self._is_running:
            return
        self._is_running = False
        self.state = "IDLE"
        self.control_timer.stop()
        self.power_error_count = 0 # ★ 편차 카운터도 초기화
        self._send_noresp("OUTP OFF")
        self.status_message.emit("DCpower", "출력 OFF, 대기 상태로 전환")

    @Slot()
    def close_connection(self):
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