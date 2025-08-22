# DCPowerController.py — PyQt6 QSerialPort + Command Queue(비동기) [스레드-로컬 타이머 적용]

from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Callable, Optional, Deque

from PyQt6.QtCore import QObject, QTimer, QIODeviceBase, pyqtSignal as Signal, pyqtSlot as Slot, Qt
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo

from lib.config import (
    DC_PORT, DC_BAUDRATE,
    DC_INITIAL_VOLTAGE, DC_INITIAL_CURRENT, DC_MAX_VOLTAGE,
    DC_MAX_CURRENT, DC_MAX_POWER, DC_TOLERANCE_WATT, DC_MAX_ERROR_COUNT,
)

# ---- 큐에서 쓰는 커맨드 객체 ---------------------------------------------------
@dataclass
class Command:
    cmd_str: str
    callback: Callable[[Optional[str]], None]          # 응답 1줄 또는 None(타임아웃/무응답)
    timeout_ms: int = 500
    gap_ms: int = 1000
    tag: str = ""
    retries_left: int = 2
    allow_no_reply: bool = False                       # SCPI 설정명령(응답 없음)일 때 True

# ---- 컨트롤러 -----------------------------------------------------------------
class DCPowerController(QObject):
    update_dc_status_display = Signal(float, float, float)  # (P, V, I)
    status_message = Signal(str, str)
    target_reached = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        # 목표/상태
        self.target_power: float = 0.0
        self._is_running = False
        self.state = "IDLE"
        self.error_count = 0
        self.voltage_guard = DC_MAX_VOLTAGE - 20.0

        # 최근 측정값(폴링이 업데이트)
        self.now_voltage: Optional[float] = None
        self.now_current: Optional[float] = None
        self.now_power:   Optional[float] = None

        # 제어변수
        self.current_current = DC_INITIAL_CURRENT

        # --- QSerialPort 설정
        self.serial = QSerialPort(self)
        self.serial.setBaudRate(DC_BAUDRATE)
        self.serial.setDataBits(QSerialPort.DataBits.Data8)
        self.serial.setParity(QSerialPort.Parity.NoParity)
        self.serial.setStopBits(QSerialPort.StopBits.OneStop)
        self.serial.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
        self.serial.readyRead.connect(self._on_ready_read)
        self.serial.errorOccurred.connect(self._on_serial_error)

        # 수신 버퍼(라인 파서)
        self._rx = bytearray()
        self._RX_MAX = 8 * 1024
        self._LINE_MAX = 512

        # 명령 큐
        self._q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # --- 타이머는 스레드-로컬에서 생성 (여기서는 None으로만) ---
        self._cmd_timer: Optional[QTimer] = None
        self._gap_timer: Optional[QTimer] = None
        self.polling_timer: Optional[QTimer] = None
        self.control_timer: Optional[QTimer] = None

    # ---------------- 타이머 생성 (반드시 DCPowerThread에서 호출) ----------------
    @Slot()
    def _setup_timers(self):
        if self._cmd_timer is None:
            self._cmd_timer = QTimer(self); self._cmd_timer.setSingleShot(True)
            self._cmd_timer.timeout.connect(self._on_cmd_timeout)
        if self._gap_timer is None:
            self._gap_timer = QTimer(self); self._gap_timer.setSingleShot(True)
            self._gap_timer.timeout.connect(self._dequeue_and_send)
        if self.polling_timer is None:
            self.polling_timer = QTimer(self)
            self.polling_timer.setInterval(3000)  # 3s
            self.polling_timer.setTimerType(Qt.TimerType.PreciseTimer)
            self.polling_timer.timeout.connect(self._enqueue_poll_cycle)
        if self.control_timer is None:
            self.control_timer = QTimer(self)
            self.control_timer.setInterval(1000)  # 1s
            self.control_timer.setTimerType(Qt.TimerType.PreciseTimer)
            self.control_timer.timeout.connect(self._on_control_tick)

    # ---------------- 연결/해제 ----------------
    def connect_dcpower_device(self) -> bool:
        # 포트 존재 확인
        ports = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if DC_PORT not in ports:
            self.status_message.emit("DCpower", f"{DC_PORT} 포트를 찾을 수 없습니다. 사용 가능: {sorted(ports)}")
            return False
        self.serial.setPortName(DC_PORT)
        if not self.serial.open(QIODeviceBase.OpenModeFlag.ReadWrite):
            self.status_message.emit("DCpower", f"연결 실패: {self.serial.errorString()}")
            return False

        # DTR/RTS, 버퍼 초기화
        self.serial.setDataTerminalReady(True)
        self.serial.setRequestToSend(False)
        self.serial.clear(QSerialPort.Direction.AllDirections)
        self._rx.clear()

        self.status_message.emit("DCpower", f"{DC_PORT} 연결 성공(QSerialPort)")
        return True

    @Slot()
    def cleanup(self):
        self.stop_process()
        if self.polling_timer: self.polling_timer.stop()
        if self._cmd_timer:    self._cmd_timer.stop()
        if self._gap_timer:    self._gap_timer.stop()
        if self.control_timer: self.control_timer.stop()
        # 큐/인플라이트 정리
        if self._inflight:
            cb = self._inflight.callback
            self._inflight = None
            try: cb(None)
            except Exception: pass
        while self._q:
            cmd = self._q.popleft()
            try: cmd.callback(None)
            except Exception: pass
        if self.serial.isOpen():
            self.serial.close()
        self.status_message.emit("DCpower", "시리얼 연결 종료")

    # ---------------- 공정 시작/제어 ----------------
    @Slot(float)
    def start_process(self, target_power: float):
        if not self.serial.isOpen():
            if not self.connect_dcpower_device():
                return
        if self._is_running:
            self.status_message.emit("DCpower", "경고: 이미 동작 중입니다.")
            return

        self.target_power = min(max(0.0, float(target_power)), DC_MAX_POWER)
        self.status_message.emit("DCpower", f"프로세스 시작 (목표 {self.target_power:.1f} W)")

        # 초기화 시퀀스 → 완료 후 런칭
        self._initialize_power_supply_async(
            voltage=DC_MAX_VOLTAGE,
            current=DC_INITIAL_CURRENT,
            on_done=self._after_init_start
        )

    def _after_init_start(self):
        self._is_running = True
        self.state = "RAMPING_UP"
        # 주기 폴링/제어 시작
        if self.polling_timer and not self.polling_timer.isActive():
            self.polling_timer.start()
        if self.control_timer and not self.control_timer.isActive():
            self.control_timer.start()

    @Slot()
    def stop_process(self):
        if not self._is_running:
            return
        self._is_running = False
        self.state = "IDLE"
        if self.control_timer: self.control_timer.stop()
        # 출력 OFF(응답 없이)
        self._enqueue_cmd("OUTP OFF", lambda _l: None, allow_no_reply=True, tag="[OUTP OFF]")
        self.status_message.emit("DCpower", "정지: 출력 OFF")

    # ---------------- 제어 타이머(상태 머신) ----------------
    def _on_control_tick(self):
        if not self._is_running:
            return

        # 폴링이 채워준 최신 측정값 사용
        now_v = self.now_voltage
        now_i = self.now_current
        now_p = self.now_power

        if now_v is None or now_i is None or now_p is None:
            # 아직 측정값 없으면 다음 틱에서
            return

        # 표시 갱신(폴링에서도 하긴 하지만, 여기서도 보증)
        self.update_dc_status_display.emit(now_p, now_v, now_i)

        if self.state == "RAMPING_UP":
            diff = self.target_power - now_p
            if abs(diff) <= DC_TOLERANCE_WATT:
                self.state = "MAINTAINING"
                self.target_reached.emit()
                self.status_message.emit("DCpower", f"{self.target_power:.1f} W 도달. 유지 모드로 전환")
                return

            # 비례 제어: dI ≈ dP / V, 스텝 제한
            if now_v > 1.0:
                delta_i = diff / now_v
                step_i = max(-0.010, min(0.010, delta_i))
            else:
                step_i = 0.005

            self._set_current_urgent(self.current_current + step_i)

        elif self.state == "MAINTAINING":
            diff = self.target_power - now_p
            if abs(diff) <= DC_TOLERANCE_WATT:
                return

            if now_v >= self.voltage_guard and diff > 0:
                # 전압 한계 접근: 강제 감쇠
                self.status_message.emit("DCpower(경고)", f"전압 상한 근접({now_v:.1f} V) → 전류 감소")
                adjust = -0.002
            else:
                adjust = 0.001 if diff > 0 else -0.001

            self._set_current_urgent(self.current_current + adjust)

    # ---------------- 폴링(큐가 비었을 때만) ----------------
    def _enqueue_poll_cycle(self):
        if self._inflight is not None or self._q:
            return  # UI/제어 우선

        # V → I 순으로 읽고 갱신
        def on_v(line: Optional[str]):
            v = self._parse_float(line)
            self.now_voltage = v
            # 전류 읽기
            def on_i(line2: Optional[str]):
                i = self._parse_float(line2)
                self.now_current = i
                p = (v * i) if (v is not None and i is not None) else None
                self.now_power = p
                self.update_dc_status_display.emit(p if p is not None else 0.0,
                                                   v if v is not None else 0.0,
                                                   i if i is not None else 0.0)
            self._enqueue_cmd("MEAS:CURR?", on_i, timeout_ms=500, tag="[MEAS:CURR?]")

        self._enqueue_cmd("MEAS:VOLT?", on_v, timeout_ms=500, tag="[MEAS:VOLT?]")

    # ---------------- 초기화 시퀀스(비동기 체인) ----------------
    def _initialize_power_supply_async(self, voltage: float, current: float, on_done: Callable[[], None]):
        seq = [
            ("*RST",      True),
            ("*CLS",      True),
            (f"VOLT {max(0.0, min(voltage, DC_MAX_VOLTAGE)):.2f}", True),
            (f"CURR {max(0.0, min(current, DC_MAX_CURRENT)):.4f}", True),
            ("OUTP ON",   True),
        ]
        def run_step(idx: int = 0):
            if idx >= len(seq):
                on_done(); return
            cmd, no_reply = seq[idx]
            self._enqueue_cmd(cmd, lambda _l, i=idx: run_step(i+1),
                              allow_no_reply=no_reply, timeout_ms=400, tag=f"[INIT {idx+1}/{len(seq)}]")
        run_step(0)

    # ---------------- 전류 세팅(긴급/앞삽입) ----------------
    def _set_current_urgent(self, value: float):
        self.current_current = max(0.0, min(float(value), DC_MAX_CURRENT))
        cmd = f"CURR {self.current_current:.4f}"
        # 응답 필요 없음(많이 보내므로 가볍게)
        self._enqueue_cmd_front(cmd, lambda _l: None, allow_no_reply=True, timeout_ms=200, tag="[CURR*]")

    # ---------------- 공용 유틸 ----------------
    def _parse_float(self, s: Optional[str]) -> Optional[float]:
        try:
            if s is None: return None
            s = s.strip()
            if not s: return None
            return float(s)
        except Exception:
            return None

    # ---------------- 큐 코어 ----------------
    def _enqueue_cmd(self, cmd_str: str, on_reply: Callable[[Optional[str]], None],
                     timeout_ms: int = 500, gap_ms: int = 20,
                     tag: str = "", retries_left: int = DC_MAX_ERROR_COUNT,
                     allow_no_reply: bool = False):
        if not cmd_str.endswith("\n"):
            cmd_str += "\n"
        self._q.append(Command(cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))
        if self._inflight is None and self._gap_timer and not self._gap_timer.isActive():
            self._gap_timer.start(0)

    def _enqueue_cmd_front(self, cmd_str: str, on_reply: Callable[[Optional[str]], None],
                           timeout_ms: int = 500, gap_ms: int = 20,
                           tag: str = "", retries_left: int = DC_MAX_ERROR_COUNT,
                           allow_no_reply: bool = False):
        if not cmd_str.endswith("\n"):
            cmd_str += "\n"
        self._q.appendleft(Command(cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))
        if self._inflight is None and self._gap_timer:
            self._gap_timer.stop()
            self._gap_timer.start(0)

    def _dequeue_and_send(self):
        if self._inflight is not None or not self._q or not self.serial.isOpen() or (self._gap_timer and self._gap_timer.isActive()):
            return
        cmd = self._q.popleft()
        self._inflight = cmd
        self._rx.clear()

        try:
            n = self.serial.write(cmd.cmd_str.encode("ascii"))
            if int(n) <= 0:
                raise IOError("write failed")
            self.serial.flush()

            # ★ 무응답 허용 명령은 전송 직후 즉시 완료 처리 (타임아웃 대기 X)
            if cmd.allow_no_reply:
                self._finish_command(None)
                return

            if self._cmd_timer:
                self._cmd_timer.stop()
                self._cmd_timer.start(cmd.timeout_ms)

            # 로그
            if cmd.tag:
                self.status_message.emit("DCpower > 전송", f"{cmd.tag} {cmd.cmd_str.strip()}")
            else:
                self.status_message.emit("DCpower > 전송", cmd.cmd_str.strip())

        except Exception as e:
            # 전송 실패 → 재시도
            self.status_message.emit("DCpower", f"전송 오류: {e}")
            if self._cmd_timer: self._cmd_timer.stop()
            failed = self._inflight
            self._inflight = None
            if failed and failed.retries_left > 0:
                failed.retries_left -= 1
                self._q.appendleft(failed)  # 바로 다시 시도
            if self._gap_timer: self._gap_timer.start(50)  # 잠깐 쉬고 다시
            return

    def _on_cmd_timeout(self):
        cmd = self._inflight
        if cmd is None:
            return
        # 무응답 허용이면 성공으로 넘김
        if cmd.allow_no_reply:
            cb = cmd.callback
            self._inflight = None
            try: cb(None)
            finally:
                if self._gap_timer: self._gap_timer.start(cmd.gap_ms)
            return

        # 재시도 또는 실패 통지
        if cmd.retries_left > 0:
            cmd.retries_left -= 1
            self._inflight = None
            self._q.appendleft(cmd)
            if self._gap_timer: self._gap_timer.start(50)
        else:
            self._finish_command(None)

    def _finish_command(self, line: Optional[str]):
        cmd = self._inflight
        if cmd is None:
            return
        if self._cmd_timer: self._cmd_timer.stop()
        self._inflight = None

        # 콜백
        try:
            cmd.callback(line)
        except Exception:
            pass

        # 다음 명령
        if self._gap_timer: self._gap_timer.start(cmd.gap_ms)

    # ---------------- 시리얼 이벤트 ----------------
    def _on_ready_read(self):
        ba = self.serial.readAll()
        if ba.isEmpty():
            return
        self._rx.extend(bytes(ba))
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]

        # 라인 단위 파싱
        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line = self._rx[:idx]
            drop = idx + 1
            # CRLF/LFCR 동시 처리
            if drop < len(self._rx):
                ch = self._rx[idx]
                nxt = self._rx[idx + 1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]

            if len(line) > self._LINE_MAX:
                line = line[:self._LINE_MAX]
            try:
                s = line.decode("ascii", errors="ignore").strip()
            except Exception:
                s = ""

            if s == "":
                continue

            # 첫 유효 1줄 응답만 소비
            self._finish_command(s)
            break

        # 남은 CR/LF 정리
        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    def _on_serial_error(self, err):
        # 포트가 닫히면 큐를 실패로 털지 않고, 재전송으로 복구 시도
        if err == QSerialPort.SerialPortError.NoError:
            return
        self.status_message.emit("DCpower", f"시리얼 오류: {self.serial.errorString()}")

        # 진행 중 명령 재시도 준비
        inflight = self._inflight
        if inflight is not None:
            if self._cmd_timer: self._cmd_timer.stop()
            self._inflight = None
            if inflight.retries_left > 0:
                inflight.retries_left -= 1
                self._q.appendleft(inflight)
            else:
                try: inflight.callback(None)
                except Exception: pass

        if self.serial.isOpen():
            try: self.serial.close()
            except Exception: pass

        # 재연결 시도 후 즉시 재개
        if self.connect_dcpower_device():
            if self._gap_timer: self._gap_timer.start(0)
