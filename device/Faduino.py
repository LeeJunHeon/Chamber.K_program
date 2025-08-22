# Faduino_K_QSerialQueued.py
# 챔버K용 Faduino 컨트롤러 — QSerialPort + 명령 큐 + 타임아웃/재시도/워치독
from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import Deque, Callable, Optional

from PyQt6.QtCore import QObject, QTimer, QIODevice, pyqtSignal as Signal, pyqtSlot as Slot, Qt, QThread, QMetaObject
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo

# === 프로젝트 설정 ===
from lib.config import (
    FADUINO_PORT, FADUINO_BAUD,
    FADUINO_PORT_INDEX, FADUINO_SENSOR_MAP, BUTTON_TO_PORT_MAP,
)

# ---- 타이밍/백오프 ----
POLL_INTERVAL_MS = 500
CMD_TIMEOUT_MS   = 1000
GAP_MS           = 40
RECON_BACKOFF_START_MS = 500
RECON_BACKOFF_MAX_MS   = 4000

STATE_BITS_LEN = 14
PWM_FULL_SCALE = 255  # 0~255

@dataclass
class Command:
    cmd_str: str
    on_reply: Callable[[Optional[str]], None]
    timeout_ms: int = CMD_TIMEOUT_MS
    gap_ms: int = GAP_MS
    tag: str = ""
    retries_left: int = 3
    allow_no_reply: bool = False


class FaduinoController(QObject):
    # === 시그널 ===
    status_message = Signal(str, str)
    update_button_display = Signal(str, bool)
    update_sensor_display = Signal(str, bool)
    rf_power_response = Signal(float, float)  # OK:RF_READ,<for>,<ref>

    def __init__(self, parent=None):
        super().__init__(parent)

        # 내부 릴레이 상태(문 Down=초기 ON 유지)
        self.port_states = [False] * STATE_BITS_LEN
        if 'Door_Down' in FADUINO_PORT_INDEX:
            self.port_states[FADUINO_PORT_INDEX['Door_Down']] = True

        # 현재 PWM(0~255)
        self.current_pwm_value = 0

        # ── 스레드 진입 전에는 핸들만 마련 ──
        self.serial: QSerialPort | None = None
        self._cmd_timer: QTimer | None = None
        self._gap_timer: QTimer | None = None
        self.polling_timer: QTimer | None = None
        self._watchdog: QTimer | None = None
        self._reconnect_pending = False
        self._want_connected = False
        self._inflight = None

        # --- 수신 버퍼/파서 ---
        self._rx = bytearray()
        self._RX_MAX = 16 * 1024
        self._LINE_MAX = 512

        # --- 큐/상태 ---
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # --- 타이머(스레드-로컬 생성; 여기서는 None으로만 두기) ---
        self._cmd_timer: Optional[QTimer] = None
        self._gap_timer: Optional[QTimer] = None
        self.polling_timer: Optional[QTimer] = None
        self._watchdog: Optional[QTimer] = None
        self._reconnect_timer: Optional[QTimer] = None

        self._want_connected = False
        self._reconnect_backoff_ms = RECON_BACKOFF_START_MS
        self._reconnect_pending = False

    # ========== 타이머 생성(반드시 FaduinoThread에서 호출) ==========
    @Slot()
    def _setup_timers(self):
            
        # --- QSerialPort ---
        if self.serial is None:
            self.serial = QSerialPort(self)
            self.serial.setBaudRate(FADUINO_BAUD)
            self.serial.setDataBits(QSerialPort.DataBits.Data8)
            self.serial.setParity(QSerialPort.Parity.NoParity)
            self.serial.setStopBits(QSerialPort.StopBits.OneStop)
            self.serial.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
            self.serial.readyRead.connect(self._on_ready_read)
            self.serial.errorOccurred.connect(self._on_serial_error)

        if self._cmd_timer is None:
            self._cmd_timer = QTimer(self)
            self._cmd_timer.setSingleShot(True)
            self._cmd_timer.timeout.connect(self._on_cmd_timeout)

        if self._gap_timer is None:
            self._gap_timer = QTimer(self)
            self._gap_timer.setSingleShot(True)
            self._gap_timer.timeout.connect(self._dequeue_and_send)

        if self.polling_timer is None:
            self.polling_timer = QTimer(self)
            self.polling_timer.setInterval(POLL_INTERVAL_MS)
            self.polling_timer.setTimerType(Qt.TimerType.PreciseTimer)
            self.polling_timer.timeout.connect(self._enqueue_state_read)

        if self._watchdog is None:
            self._watchdog = QTimer(self)
            self._watchdog.setInterval(1000)
            self._watchdog.setTimerType(Qt.TimerType.PreciseTimer)
            self._watchdog.timeout.connect(self._watch_connection)

        if self._reconnect_timer is None:
            self._reconnect_timer = QTimer(self)
            self._reconnect_timer.setSingleShot(True)
            self._reconnect_timer.timeout.connect(self._try_reconnect)

        # ▼▼▼ 타이머 시작 로직을 이쪽으로 이동 ▼▼▼
        if not self._watchdog.isActive():
            self._watchdog.start()
        if not self.polling_timer.isActive():
            self.polling_timer.start()

    # ========== 연결/해제 ==========
    @Slot()
    def start_polling(self):
        """포트 열고 폴링 시작 (스레드-로컬 타이머 보장)"""
        # 가드: 혹시 _setup_timers가 먼저 안 불렸다면 여기서 보강
        if self._cmd_timer is None:
            self._setup_timers()

        self._want_connected = True
        self._open_port()
        # if not self._watchdog.isActive():
        #     self._watchdog.start()
        # if not self.polling_timer.isActive():
        #     self.polling_timer.start()
        self.status_message.emit("Faduino", "주기적 상태 동기화 시작")

    def _open_port(self):
        if self.serial is None:
            self._setup_timers()
        if self.serial.isOpen():
            return
        
        names = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if FADUINO_PORT not in names:
            self.status_message.emit("Faduino", f"{FADUINO_PORT} 미존재. 사용 가능: {sorted(names)}")
            return
        self.serial.setPortName(FADUINO_PORT)
        if not self.serial.open(QIODevice.ReadWrite):
            self.status_message.emit("Faduino", f"포트 열기 실패: {self.serial.errorString()}")
            return

        self.serial.setDataTerminalReady(True)
        self.serial.setRequestToSend(False)
        self.serial.clear(QSerialPort.AllDirections)
        self._rx.clear()
        self._reconnect_backoff_ms = RECON_BACKOFF_START_MS
        self._reconnect_pending = False
        self.status_message.emit("Faduino", f"{FADUINO_PORT} 연결 성공(QSerialPort)")

    def _watch_connection(self):
        if not self._want_connected or self.serial.isOpen() or self._reconnect_pending:
            return
        self._reconnect_pending = True
        backoff = self._reconnect_backoff_ms
        self.status_message.emit("Faduino", f"재연결 대기… {backoff} ms")
        self._reconnect_timer.start(backoff)

    def _try_reconnect(self):
        self._reconnect_pending = False
        if not self._want_connected or self.serial.isOpen():
            return
        self._open_port()
        if self.serial.isOpen():
            self.status_message.emit("Faduino", "재연결 성공")
            if self._cmd_q and self._inflight is None:
                self._gap_timer.start(0)
        else:
            self._reconnect_backoff_ms = min(self._reconnect_backoff_ms * 2, RECON_BACKOFF_MAX_MS)

    @Slot()
    def cleanup(self):
        """안전 종료 (자기 스레드에서 실행)"""
        self._want_connected = False
        if self.polling_timer:  self.polling_timer.stop()
        if self._cmd_timer:     self._cmd_timer.stop()
        if self._gap_timer:     self._gap_timer.stop()
        if self._watchdog:      self._watchdog.stop()
        if self._reconnect_timer: self._reconnect_timer.stop()
        self._rx.clear()
        self._inflight = None
        self._cmd_q.clear()
        if self.serial and self.serial.isOpen():
            self.serial.close()
        self.status_message.emit("Faduino", "연결 종료")

    # ========== 수신/파싱 ==========
    def _on_serial_error(self, err):
        if err == QSerialPort.NoError:
            return
        self.status_message.emit("Faduino", f"시리얼 오류: {self.serial.errorString()}")
        # inflight 복구(재시도)
        if self._inflight:
            cmd = self._inflight
            self._inflight = None
            if self._cmd_timer: self._cmd_timer.stop()
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
        if self.serial.isOpen():
            self.serial.close()
        if self._gap_timer:  self._gap_timer.stop()  # 잠시 멈췄다가 워치독이 재연결

    def _on_ready_read(self):
        ba = self.serial.readAll()
        if ba.isEmpty():
            return
        self._rx.extend(bytes(ba))
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]

        # 라인 파싱
        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line_bytes = self._rx[:idx]
            drop = idx + 1
            if drop < len(self._rx):
                ch = self._rx[idx]; nxt = self._rx[idx + 1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]
            if not line_bytes:
                continue
            if len(line_bytes) > self._LINE_MAX:
                line_bytes = line_bytes[:self._LINE_MAX]
            try:
                line = line_bytes.decode('ascii', errors='ignore').strip()
            except Exception:
                line = ""
            if not line:
                continue

            # 에코 스킵
            if self._inflight and line == self._inflight.cmd_str.strip():
                continue

            # 비동기 RF 응답
            if line.startswith("OK:RF_READ,"):
                parts = line.split(',')
                if len(parts) == 3:
                    try:
                        fwd = float(parts[1]); ref = float(parts[2])
                        self.rf_power_response.emit(fwd, ref)
                    except Exception:
                        pass
                continue

            # 인플라이트 응답 처리
            self._finish_command(line)
            break  # 한 번에 한 명령만 완료

        # 수신 꼬리 정리
        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    # ========== 큐/전송/타임아웃 ==========
    def enqueue(self, cmd: Command):
        if not cmd.cmd_str.endswith('\n') and not cmd.cmd_str.endswith('\r'):
            cmd.cmd_str += '\n'  # 챔버K는 \n 기반 (필요시 \r로 변경)
        self._cmd_q.append(cmd)
        if self._inflight is None and self._gap_timer and not self._gap_timer.isActive():
            self._gap_timer.start(0)

    def _dequeue_and_send(self):
        if self._inflight is not None or not self._cmd_q:
            return
        if not self.serial.isOpen():
            return
        if self._gap_timer and self._gap_timer.isActive():
            return

        cmd = self._cmd_q.popleft()
        self._inflight = cmd

        # 전송
        payload = cmd.cmd_str.encode('ascii')
        n = int(self.serial.write(payload))
        if n <= 0:
            # 전송 실패 → 재시도 예약
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            if self.serial.isOpen():
                self.serial.close()
            if self._reconnect_timer:
                self._reconnect_timer.start(0)
            return

        # ★ 무응답 허용 명령은 즉시 완료 처리(타임아웃 기다리지 않음)
        if cmd.allow_no_reply:
            self._finish_command(None)
            return

        self.serial.flush()
        if self._cmd_timer:
            self._cmd_timer.stop()
            self._cmd_timer.start(cmd.timeout_ms)

    def _on_cmd_timeout(self):
        self._finish_command(None)

    def _finish_command(self, line: Optional[str]):
        if self._inflight is None:
            return
        cmd = self._inflight
        self._inflight = None
        if self._cmd_timer:
            self._cmd_timer.stop()

        if line is None:
            # 타임아웃/무응답
            if cmd.allow_no_reply:
                try:
                    cmd.on_reply(None)
                finally:
                    if self._gap_timer: self._gap_timer.start(cmd.gap_ms)
                return

            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
                if self.serial.isOpen():
                    self.serial.close()
                if self._reconnect_timer:
                    self._reconnect_timer.start(0)   # ← 타이핑 오류 수정됨
                return
            else:
                try:
                    cmd.on_reply(None)
                finally:
                    if self._gap_timer: self._gap_timer.start(cmd.gap_ms)
                return

        # 정상 응답
        try:
            cmd.on_reply(line.strip())
        finally:
            if self._gap_timer: self._gap_timer.start(cmd.gap_ms)

    # ========== 폴링/상태 동기화 ==========
    def _has_pending(self, tag: str) -> bool:
        if self._inflight and self._inflight.tag == tag:
            return True
        return any(c.tag == tag for c in self._cmd_q)

    def _enqueue_state_read(self):
        """STATE_READ → '<14bits>,<hex>' 또는 'OK:STATE,<bits>,<hex>' 응답 허용"""
        # 이미 대기/진행 중인 폴링이 있으면 스킵(큐 잠식 방지)
        if self._has_pending("[POLL]"):
            return

        def on_reply(line: Optional[str]):
            if not line:
                return
            s = line.strip()
            if s.startswith("OK:STATE,"):
                s = s[len("OK:STATE,"):]
            if ',' not in s:
                return

            state_part, sensor_hex = s.split(',', 1)

            # 센서 갱신
            try:
                sensor_val = int(sensor_hex.strip(), 16)
                for name, bit in FADUINO_SENSOR_MAP.items():
                    self.update_sensor_display.emit(name, (sensor_val & (1 << bit)) != 0)
            except Exception:
                pass

            # 버튼 동기화
            if len(state_part) == STATE_BITS_LEN:
                self._update_buttons_from_state(state_part)

        # 폴링은 무응답 허용 + 짧은 타임아웃
        self.enqueue(Command(
            "STATE_READ",
            on_reply,
            timeout_ms=200,
            gap_ms=GAP_MS,
            tag="[POLL]",
            allow_no_reply=True
        ))

    def _update_buttons_from_state(self, state_str: str):
        # 일반 버튼
        for btn_name, port_name in BUTTON_TO_PORT_MAP.items():
            if port_name not in FADUINO_PORT_INDEX:
                continue
            idx = FADUINO_PORT_INDEX[port_name]
            new_state = (state_str[idx] == '1')
            if self.port_states[idx] != new_state:
                self.port_states[idx] = new_state
                self.update_button_display.emit(btn_name, new_state)

        # Door 버튼(Up 기준)
        if 'Door_Up' in FADUINO_PORT_INDEX and 'Door_Down' in FADUINO_PORT_INDEX:
            up = FADUINO_PORT_INDEX['Door_Up']
            down = FADUINO_PORT_INDEX['Door_Down']
            new_door = (state_str[up] == '1')
            if self.port_states[up] != new_door:
                self.port_states[up] = new_door
                self.port_states[down] = (state_str[down] == '1')
                self.update_button_display.emit('Door_Button', new_door)

    # ========== 공개 API ==========
    @Slot(str, bool)
    def update_port_state(self, name: str, state: bool):
        """UI 클릭 → 내부 상태 갱신 → 전체비트+PWM 전송"""
        if name == 'Door_Button':
            if 'Door_Up' in FADUINO_PORT_INDEX and 'Door_Down' in FADUINO_PORT_INDEX:
                self.port_states[FADUINO_PORT_INDEX['Door_Up']]   = state
                self.port_states[FADUINO_PORT_INDEX['Door_Down']] = not state
        elif name in BUTTON_TO_PORT_MAP:
            port = BUTTON_TO_PORT_MAP[name]
            if port in FADUINO_PORT_INDEX:
                self.port_states[FADUINO_PORT_INDEX[port]] = state
        else:
            return

        # UI 즉시 반영
        self.update_button_display.emit(name, state)
        # 현재 PWM과 함께 전체 상태 전송
        self._send_full_state_with_pwm(self.current_pwm_value)

    @Slot(int)
    def send_rfpower_command(self, pwm_0_255: int):
        """RF 파워(PWM 0~255) 갱신 — 전체 비트 + PWM 같이 보냄"""
        self.current_pwm_value = self._clamp_pwm(pwm_0_255)
        self._send_full_state_with_pwm(self.current_pwm_value)

    def _send_full_state_with_pwm(self, pwm_0_255: int):
        bit_str = ''.join('1' if s else '0' for s in self.port_states)
        cmd_line = f"{bit_str},{self._clamp_pwm(pwm_0_255)}"
        # 에코만 오거나 무응답일 수 있으므로 allow_no_reply 권장
        self.enqueue(Command(
            cmd_line,
            lambda _l: None,
            timeout_ms=CMD_TIMEOUT_MS,
            gap_ms=GAP_MS,
            tag="[STATE+PWM]",
            allow_no_reply=True
        ))

    @Slot()
    def request_rf_read(self):
        self.enqueue(Command("RF_READ,0", lambda _l: None, allow_no_reply=True, gap_ms=GAP_MS, tag="[RF_READ]"))

    @Slot()
    def on_emergency_stop(self):
        """모든 포트 OFF, Door_Down만 ON"""
        self.port_states = [False] * STATE_BITS_LEN
        if 'Door_Down' in FADUINO_PORT_INDEX:
            self.port_states[FADUINO_PORT_INDEX['Door_Down']] = True
        # UI 동기화
        for btn in BUTTON_TO_PORT_MAP.keys():
            self.update_button_display.emit(btn, False)
        self.update_button_display.emit('Door_Button', False)
        # 하드웨어 반영
        self._send_full_state_with_pwm(0)
        self.status_message.emit("Faduino(비상)", "EMERGENCY STOP: 모든 포트 OFF")

    # ========== 유틸 ==========
    def _clamp_pwm(self, v: int) -> int:
        try:
            v = int(v)
        except Exception:
            v = 0
        if v < 0: v = 0
        if v > PWM_FULL_SCALE: v = PWM_FULL_SCALE
        return v
