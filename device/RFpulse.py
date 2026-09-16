# device/RFpulse.py
# -*- coding: utf-8 -*-
"""RF Pulse (CESAR 1310, AE Bus RS-232, 9600 8O1) 컨트롤러 — Chamber-K.

[시리얼 규격 — 매뉴얼 그대로]
  CESAR 매뉴얼 RS-232: "Odd parity, one start bit, eight data bits, one stop bit".
  즉 9600 8O1 이다. 8N1 로 열면 패킷 바이트가 정확해도 수신기 쪽에서 패리티 오류로
  깨져 SET_ACTIVE_CTRL 부터 NAK 만 돌아온다(2026-09-15 22:21 실기 로그).
  패리티는 config RFPULSE_PARITY 로 두되 데이터 8비트 / 스톱 1비트는 고정이다.

원본은 챔버1,2 프로그램(Chamber.Total_program)의 device/rf_pulse.py 다.
AE Bus 프로토콜 로직(프레임 빌더·커맨드 번호·CSR·STATUS 비트·시퀀스 순서·
타임아웃·감시 임계값)은 상수값까지 그대로 옮겼고, I/O 층만 asyncio →
QSerialPort + QTimer 로 교체했다. CHK 안에는 asyncio 이벤트 루프를 만들지
않는다(파이썬 스레드 + Qt 시그널로만 돈다).

[원본과 다른 점 — 의도적]
  - 전송 계층: TCP(이더넷 컨버터) → 시리얼 직결(QSerialPort). CHK 는 COM 포트로
    직결한다. TCP 경로는 만들지 않는다.
  - await 체인 → '스텝 리스트 + 콜백' 상태 머신. QEventLoop 를 중첩하지 않는다.
  - 이벤트 큐(RFPulseEvent) → Qt 시그널. 중단 사유는 status_message("재시작", ...)
    로 올려 main 의 on_status_message 가 기존 중단 경로를 태운다.

[이 장비는 PLC DAC RF Power 와 별개다]
  PLC DAC(D00040)나 DAC enable 코일은 이 파일에서 절대 건드리지 않는다.
  화면의 for.P/ref.P 칸만 공유한다.

[AE Bus 프레이밍]
  라인 프로토콜이 아니다. 길이 기반으로 직접 잘라야 한다.
    0x06 = ACK, 0x15 = NAK (단일 바이트)
    그 외: header(1) → len=header&0x07 → len==7 이면 다음 1바이트가 실제 길이
           → cmd(1) → data(len) → XOR 체크섬(1)
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Optional, Tuple

from PyQt6.QtCore import QObject, QTimer, QIODeviceBase, pyqtSignal as Signal, pyqtSlot as Slot
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo

from lib.config import (
    RFPULSE_PORT, RFPULSE_BAUD, RFPULSE_ADDR, RFPULSE_PARITY, RFPULSE_MAX_POWER,
    RFPULSE_ACK_TIMEOUT_MS, RFPULSE_QUERY_TIMEOUT_MS, RFPULSE_CMD_GAP_MS,
    RFPULSE_RAW_LOG, RFPULSE_VERIFY_PULSE_CONFIG,
    RFPULSE_POLL_INTERVAL_MS, RFPULSE_POLL_QUERY_TIMEOUT_MS,
    RFPULSE_POLL_START_DELAY_AFTER_RF_ON_MS,
    RFPULSE_WATCHDOG_INTERVAL_MS,
    RFPULSE_RECONNECT_BACKOFF_START_MS, RFPULSE_RECONNECT_BACKOFF_MAX_MS,
    RFPULSE_FORP_TOLERANCE_PERCENT, RFPULSE_FORP_CONSECUTIVE_LIMIT,
    RFPULSE_REFP_LIMIT_WATTS, RFPULSE_REFP_CONSECUTIVE_LIMIT,
)

# ===== AE Bus command numbers (원본 그대로) =====
CMD_RF_OFF              = 1
CMD_RF_ON               = 2
CMD_SET_CTRL_MODE       = 3
CMD_SET_SETPOINT        = 8
CMD_SET_ACTIVE_CTRL     = 14

# Reads (report)
CMD_REPORT_STATUS       = 162
CMD_REPORT_SETPOINT     = 164
CMD_REPORT_FORWARD      = 165
CMD_REPORT_REFLECTED    = 166
CMD_REPORT_DELIVERED    = 167

# Pulsing
CMD_SET_PULSING = 27  # AE Bus: set pulsing configuration (data byte: 0~5)

PULSING_TX = {
    0: 0x00,  # Pulsing off
    1: 0x01,  # Internal pulsing
    2: 0x02,  # External pulsing
    3: 0x03,  # External pulsing inverted
    4: 0x04,  # Gated internal pulsing
    5: 0x05,  # Gated internal pulsing inverted
}

CMD_SET_PULSE_FREQ      = 93     # 3 bytes (Hz, LSB first)
CMD_SET_PULSE_DUTY      = 96     # 2 bytes (percent, LSB first)

# Pulsing 리드백(선택)
CMD_REPORT_PULSING      = 177
CMD_REPORT_PULSE_FREQ   = 193
CMD_REPORT_PULSE_DUTY   = 196

CSR_CODES = {
    0:  "Command accepted",
    1:  "Wrong control mode (not HOST)",
    2:  "RF output is ON (cannot change this setting now)",
    4:  "Data out of range / bad value",
    5:  "User Port RF signal OFF / interlock (model dependent)",
    7:  "Active faults exist",
    9:  "Data byte count incorrect",
    19: "Recipe mode active",
    50: "Frequency out of range",
    51: "Duty out of range",
    99: "Command not implemented",
}

MODE_SET  = {"fwd": 6, "load": 7, "ext": 8}
MODE_NAME = {6: "FWD", 7: "LOAD", 8: "EXT"}

CMD_NAMES = {
    CMD_RF_OFF: "RF_OFF",
    CMD_RF_ON: "RF_ON",
    CMD_SET_CTRL_MODE: "SET_CTRL_MODE",
    CMD_SET_SETPOINT: "SET_SETPOINT",
    CMD_SET_ACTIVE_CTRL: "SET_ACTIVE_CTRL",
    CMD_REPORT_STATUS: "REPORT_STATUS",
    CMD_REPORT_SETPOINT: "REPORT_SETPOINT",
    CMD_REPORT_FORWARD: "REPORT_FORWARD",
    CMD_REPORT_REFLECTED: "REPORT_REFLECTED",
    CMD_REPORT_DELIVERED: "REPORT_DELIVERED",
    CMD_SET_PULSING: "SET_PULSING",
    CMD_SET_PULSE_FREQ: "SET_PULSE_FREQ",
    CMD_SET_PULSE_DUTY: "SET_PULSE_DUTY",
    CMD_REPORT_PULSING: "REPORT_PULSING",
    CMD_REPORT_PULSE_FREQ: "REPORT_PULSE_FREQ",
    CMD_REPORT_PULSE_DUTY: "REPORT_PULSE_DUTY",
}


# ---- REPORT_STATUS(0xA2) 파싱 ----
@dataclass
class RfStatus:
    rf_output_on: bool          # Byte1 bit5
    rf_on_requested: bool       # Byte1 bit6
    setpoint_mismatch: bool     # Byte1 bit7 (True면 아직 목표 미도달)
    interlock_open: bool        # Byte2 bit7
    overtemp: bool              # Byte2 bit3
    current_limit: bool         # Byte4 bit0
    extended_fault: bool        # Byte4 bit5
    cex_lock: bool              # Byte4 bit7
    raw: bytes = b""


def _u16le(buf: bytes, i: int = 0) -> int:
    return buf[i] | (buf[i + 1] << 8)


# ===== 프레임 빌더 (원본 그대로) =====
def _hex(b: bytes) -> str:
    """로그용 16진 표기: b'	' → '09 0E 02 05'."""
    return ' '.join(f'{x:02X}' for x in (b or b''))


def _build_packet(addr: int, cmd: int, data: bytes = b"") -> bytes:
    if not (0 <= addr <= 31):
        raise ValueError("addr 0..31")
    L = len(data)
    if L <= 6:
        header = ((addr & 0x1F) << 3) | L
        body = bytes([header, cmd]) + data
    else:
        header = ((addr & 0x1F) << 3) | 0x07
        body = bytes([header, cmd, L & 0xFF]) + data
    cs = 0
    for b in body:
        cs ^= b
    return body + bytes([cs & 0xFF])


# ===== 큐 명령 구조 =====
@dataclass
class RfCommand:
    kind: str                 # "exec"=쓰기(CSR 필요), "query"=읽기(데이터 프레임)
    cmd: int
    data: bytes
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool
    allow_when_closing: bool
    callback: Callable[[Optional[bytes]], None]
    # exec 실패 시 상위에 CSR 을 알려 주기 위한 자리
    last_csr: Optional[int] = field(default=None)


# config RFPULSE_PARITY → QSerialPort. 데이터 8비트 / 스톱 1비트는 매뉴얼대로 고정.
_PARITY_MAP = {
    'none': QSerialPort.Parity.NoParity,
    'odd':  QSerialPort.Parity.OddParity,
    'even': QSerialPort.Parity.EvenParity,
}
_PARITY_NAME = {v: k for k, v in _PARITY_MAP.items()}


class RFPulseController(QObject):
    """CESAR 1310 RF Pulse 제너레이터 — AE Bus over RS-232."""

    update_rfpulse_status_display = Signal(float, float)   # forward, reflected
    status_message                = Signal(str, str)
    target_reached                = Signal()               # RF ON 완료
    power_off_finished            = Signal()               # RF OFF 완료
    # START 완료 직후 장비에서 되읽은 실제 펄스 설정 (freq kHz, duty %)
    #  freq/duty 를 비워 두면(=장비 현재값 유지) 기록할 값이 없어서 되읽는다.
    # {'freq_khz': float|None, 'duty': int|None} — 한쪽만 실패해도 있는 쪽은 올린다.
    #  (예전 Signal(float, int) 은 둘 다 있어야 emit 해서 freq 실패에 duty 까지 버려졌다)
    pulse_config_readback         = Signal(object)

    _RX_MAX = 4096         # 수신 버퍼 상한(바이트)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.addr = int(RFPULSE_ADDR) if RFPULSE_ADDR is not None else 1

        self.serial_rfp: Optional[QSerialPort] = None
        self._rx = bytearray()
        self._overflow_count = 0
        self._cs_bad_count = 0          # 체크섬 불일치 누적(로그 솎아내기용)

        # 명령 큐 / 인플라이트
        self._cmd_q: Deque[RfCommand] = deque()
        self._inflight: Optional[RfCommand] = None
        self._cmd_timer: Optional[QTimer] = None
        self._gap_timer: Optional[QTimer] = None
        self._send_spin = False

        # 연결/재연결
        self._want_connected = False
        self._closing = False
        self._reconnect_pending = False
        self._reconnect_backoff_ms = RFPULSE_RECONNECT_BACKOFF_START_MS
        self._watchdog: Optional[QTimer] = None

        # 폴링
        self._poll_timer: Optional[QTimer] = None
        self._poll_busy = False
        self._last_forward_w: Optional[float] = None
        self._last_reflected_w: Optional[float] = None
        self._last_status: Optional[RfStatus] = None

        # START 시퀀스 상태 머신
        self._seq: list = []
        self._seq_idx = -1
        self._seq_running = False

        # 감시
        self._stop_requested = False
        self._target_setpoint_w: float = 0.0
        self._req_freq_hz: Optional[int] = None    # 이번 공정이 요청한 펄스 주파수(None=유지)
        self._req_duty: Optional[int] = None       # 이번 공정이 요청한 듀티(None=유지)
        self._forp_out_of_range_count: int = 0
        self._refp_over_limit_count: int = 0

    # ==================== 타이머/시리얼 지연 생성 ====================
    def _ensure_serial_created(self):
        if self.serial_rfp is not None:
            return
        self.serial_rfp = QSerialPort(self)
        self.serial_rfp.setBaudRate(int(RFPULSE_BAUD))
        self.serial_rfp.setDataBits(QSerialPort.DataBits.Data8)
        # ★ CESAR 는 홀수 패리티(8O1). NoParity 로 열면 NAK 만 돌아온다 — config 참조.
        self.serial_rfp.setParity(_PARITY_MAP.get(RFPULSE_PARITY, QSerialPort.Parity.OddParity))
        self.serial_rfp.setStopBits(QSerialPort.StopBits.OneStop)
        self.serial_rfp.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
        self.serial_rfp.readyRead.connect(self._on_ready_read)
        self.serial_rfp.errorOccurred.connect(self._on_serial_error)

    def _ensure_timers_created(self):
        if self._cmd_timer is None:
            self._cmd_timer = QTimer(self)
            self._cmd_timer.setSingleShot(True)
            self._cmd_timer.timeout.connect(self._on_cmd_timeout)

        if self._gap_timer is None:
            self._gap_timer = QTimer(self)
            self._gap_timer.setSingleShot(True)
            self._gap_timer.timeout.connect(self._dequeue_and_send)

        if self._poll_timer is None:
            self._poll_timer = QTimer(self)
            self._poll_timer.setInterval(RFPULSE_POLL_INTERVAL_MS)
            self._poll_timer.timeout.connect(self._poll_cycle)

        if self._watchdog is None:
            self._watchdog = QTimer(self)
            self._watchdog.setInterval(RFPULSE_WATCHDOG_INTERVAL_MS)
            self._watchdog.timeout.connect(self._watch_connection)

    # ==================== 연결/해제 ====================
    @Slot()
    def connect_device(self) -> bool:
        """포트를 연다. 실패해도 예외를 올리지 않는다(하드웨어가 없어도 프로그램은 떠야 한다)."""
        self._ensure_serial_created()
        self._ensure_timers_created()

        self._closing = False
        self._want_connected = True
        ok = self._open_port()
        if self._watchdog:
            self._watchdog.start()
        return ok

    def _open_port(self) -> bool:
        if self.serial_rfp and self.serial_rfp.isOpen():
            return True

        ports = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if RFPULSE_PORT not in ports:
            self.status_message.emit(
                "RFPulse", f"{RFPULSE_PORT} 존재하지 않음. 사용 가능 포트: {sorted(ports)}")
            return False

        self.serial_rfp.setPortName(RFPULSE_PORT)
        if not self.serial_rfp.open(QIODeviceBase.OpenModeFlag.ReadWrite):
            self.status_message.emit(
                "RFPulse", f"{RFPULSE_PORT} 연결 실패: {self.serial_rfp.errorString()}")
            return False

        self.serial_rfp.setDataTerminalReady(True)
        self.serial_rfp.setRequestToSend(False)
        self.serial_rfp.clear(QSerialPort.Direction.AllDirections)
        self._rx.clear()

        # 실제 적용된 포트 설정을 남긴다 — 다음에 NAK 이 나면 로그만 보고 잡을 수 있게.
        self.status_message.emit(
            "RFPulse",
            f"{RFPULSE_PORT} 열림: {int(self.serial_rfp.baudRate())} bps, "
            f"data {int(self.serial_rfp.dataBits().value)}, "
            f"parity {_PARITY_NAME.get(self.serial_rfp.parity(), str(self.serial_rfp.parity()))}, "
            f"stop {int(self.serial_rfp.stopBits().value)} "
            f"(CESAR 규격 9600 8O1)")

        self._reconnect_backoff_ms = RFPULSE_RECONNECT_BACKOFF_START_MS
        self._reconnect_pending = False

        self.status_message.emit(
            "RFPulse", f"{RFPULSE_PORT} 연결 성공 (PyQt6 QSerialPort, {RFPULSE_BAUD}bps)")
        return True

    def _watch_connection(self):
        if (not self._want_connected) or (self.serial_rfp and self.serial_rfp.isOpen()):
            return
        if self._reconnect_pending:
            return
        self._reconnect_pending = True
        self.status_message.emit(
            "RFPulse", f"재연결 시도... ({self._reconnect_backoff_ms} ms)")
        QTimer.singleShot(self._reconnect_backoff_ms, self._try_reconnect)

    def _try_reconnect(self):
        self._reconnect_pending = False
        if (not self._want_connected) or (self.serial_rfp and self.serial_rfp.isOpen()):
            return
        if self._open_port():
            self.status_message.emit("RFPulse", "재연결 성공. 대기 중 명령 재개.")
            QTimer.singleShot(0, self._dequeue_and_send)
            self._reconnect_backoff_ms = RFPULSE_RECONNECT_BACKOFF_START_MS
        else:
            self._reconnect_backoff_ms = min(
                self._reconnect_backoff_ms * 2, RFPULSE_RECONNECT_BACKOFF_MAX_MS)

    @Slot()
    def close_connection(self):
        """포트만 닫는다. 컨트롤러는 재사용 가능한 상태로 남는다."""
        self._want_connected = False
        if self.serial_rfp and self.serial_rfp.isOpen():
            try:
                self.serial_rfp.close()
                self.status_message.emit("RFPulse", "시리얼 포트를 안전하게 닫았습니다.")
            except Exception:
                pass
        self._rx.clear()

    @Slot()
    def cleanup(self):
        self._closing = True
        self._want_connected = False
        self.set_process_status(False)
        self._seq_running = False
        self._seq = []
        self._seq_idx = -1

        self._purge_pending("shutdown")

        for tname in ("_cmd_timer", "_gap_timer", "_poll_timer", "_watchdog"):
            t = getattr(self, tname, None)
            if t:
                try:
                    t.stop()
                except Exception:
                    pass
                t.deleteLater()
                setattr(self, tname, None)

        if self.serial_rfp:
            try:
                if self.serial_rfp.isOpen():
                    self.serial_rfp.close()
                    self.status_message.emit("RFPulse", "시리얼 포트를 안전하게 닫았습니다.")
            finally:
                self.serial_rfp.deleteLater()
                self.serial_rfp = None

        self._rx.clear()
        self._reconnect_pending = False

    # ==================== 시리얼 이벤트 ====================
    def _on_serial_error(self, err):
        if err == QSerialPort.SerialPortError.NoError:
            return
        serr = self.serial_rfp.errorString() if self.serial_rfp else ""
        self.status_message.emit("RFPulse", f"시리얼 오류: {serr} (err={err})")

        if self._inflight is not None:
            cmd = self._inflight
            if self._cmd_timer:
                self._cmd_timer.stop()
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

        if self.serial_rfp and self.serial_rfp.isOpen():
            self.serial_rfp.close()
        if self._gap_timer:
            self._gap_timer.stop()
        self._rx.clear()

        QTimer.singleShot(0, self._watch_connection)

    def _on_ready_read(self):
        if not (self.serial_rfp and self.serial_rfp.isOpen()):
            return
        ba = self.serial_rfp.readAll()
        if ba.isEmpty():
            return

        self._rx.extend(bytes(ba))
        if len(self._rx) > self._RX_MAX:
            # AE Bus 는 길이 기반이라 중간부터 다시 맞추기 어렵다. 통째로 버린다.
            self._rx.clear()
            self._overflow_count += 1
            if self._overflow_count % 5 == 1:
                self.status_message.emit(
                    "RFPulse", f"수신 버퍼 과다(RX>{self._RX_MAX}B) — 버퍼를 비웠습니다.")
            return

        for kind, payload in self._drain_tokens():
            self._handle_token(kind, payload)

    def _drain_tokens(self):
        """버퍼에서 뽑을 수 있는 토큰을 모두 꺼낸다. ('ACK'|'NAK'|'FRAME', payload)."""
        out = []
        while True:
            if not self._rx:
                break

            # 1) ACK/NAK 단일 토큰
            if self._rx[0] == 0x06:
                del self._rx[:1]
                out.append(("ACK", None))
                continue
            if self._rx[0] == 0x15:
                del self._rx[:1]
                out.append(("NAK", None))
                continue

            # 2) 프레임 헤더 점검
            if len(self._rx) < 2:
                break
            hdr = self._rx[0]
            length_bits = hdr & 0x07

            if length_bits == 7:
                if len(self._rx) < 3:
                    break
                data_len = self._rx[2]
                total = 1 + 1 + 1 + data_len + 1
            else:
                data_len = length_bits
                total = 1 + 1 + data_len + 1
            if len(self._rx) < total:
                break
            pkt = bytes(self._rx[:total])
            del self._rx[:total]

            # 3) XOR 체크섬 검증
            cs = 0
            for x in pkt[:-1]:
                cs ^= x
            if (cs ^ pkt[-1]) != 0:
                # ★ total 바이트를 통째로 버리면 노이즈 1바이트 때문에 뒤에 붙은
                #   정상 프레임까지 먹는다. 선두 1바이트만 버리고 다시 맞춰 본다.
                #   (원본은 통째로 버린다 — 의도적으로 달라진 부분)
                self._rx[:0] = pkt          # 되돌려 놓고
                del self._rx[:1]            # 선두 1바이트만 버린다
                self._cs_bad_count += 1
                if self._cs_bad_count % 20 == 1:
                    self.status_message.emit(
                        "RFPulse",
                        f"[RX] 체크섬 불일치 — 1바이트 버리고 재동기화 "
                        f"(누적 {self._cs_bad_count}회) raw="
                        + " ".join(f"{x:02X}" for x in pkt[:8]))
                continue

            out.append(("FRAME", pkt))
        return out

    # ==================== 토큰 → 인플라이트 판정 ====================
    def _rx_log(self, text: str):
        """수신 프레임 로그(챔버2 원본 [RFP][RAW][RX] 수준). RFPULSE_RAW_LOG 로 on/off."""
        if RFPULSE_RAW_LOG:
            self.status_message.emit("RFPulse < 수신", text)

    def _handle_token(self, kind: str, payload: Optional[bytes]):
        cmd = self._inflight
        if cmd is None:
            # 지연 도착/예상 밖 토큰은 버린다 — 로그만 남긴다
            if kind == "ACK":
                self._rx_log("ACK(06) (대기 명령 없음)")
            elif kind == "NAK":
                self._rx_log("NAK(15) (대기 명령 없음)")
            else:
                self._rx_log(f"(대기 명령 없음) raw={_hex(payload or b'')}")
            return

        _who = f"{cmd.tag} {self._cmd_label(cmd.cmd)}".strip()
        if kind == "ACK":
            # ACK 은 참고용 신호일 뿐, 성공 판정에 쓰지 않는다(원본과 동일).
            self._rx_log("ACK(06)")
            return
        if kind == "NAK":
            self._rx_log(f"NAK(15) ← {_who}")
            self._fail_inflight("NAK")
            return
        if kind != "FRAME" or not payload:
            return
        if not self._frame_match(payload, cmd.cmd):
            self._rx_log(f"FRAME raw={_hex(payload)} (다른 명령의 프레임 — 무시) ← {_who}")
            return                      # 다른 명령의 프레임 — 무시

        data = self._extract_data(payload)

        if cmd.kind == "exec":
            if not data or len(data) < 1:
                self._rx_log(f"FRAME raw={_hex(payload)} CSR 없음 ← {_who}")
                self._fail_inflight("CSR 없음")
                return
            csr = data[0]
            cmd.last_csr = csr
            self._rx_log(
                f"FRAME raw={_hex(payload)} CSR={csr}({CSR_CODES.get(csr, 'Unknown')}) ← {_who}")
            if csr != 0:
                self.status_message.emit(
                    "RFPulse",
                    f"CSR {csr} ({CSR_CODES.get(csr, 'Unknown')}) for {self._cmd_label(cmd.cmd)}")
                if self._try_csr_recovery(cmd, csr):
                    return
                self._fail_inflight(f"csr={csr}", csr=csr)
                return
            self._finish_inflight(data)
        else:
            self._rx_log(f"FRAME raw={_hex(payload)} data={_hex(data) or '(없음)'} ← {_who}")
            self._finish_inflight(data)

    def _try_csr_recovery(self, cmd: RfCommand, csr: int) -> bool:
        """원본의 CSR 기반 자동 복구. 처리했으면 True."""
        if self._closing or cmd.retries_left <= 0:
            return False

        # CSR=1: HOST 가 아니어서 거부 → HOST 재설정 후 재시도
        if csr == 1 and cmd.cmd != CMD_SET_ACTIVE_CTRL:
            cmd.retries_left -= 1
            if self._cmd_timer:
                self._cmd_timer.stop()
            self._inflight = None
            self._cmd_q.appendleft(cmd)
            self._cmd_q.appendleft(self._make_cmd(
                "exec", CMD_SET_ACTIVE_CTRL, b"\x02", tag="[AUTO HOST]",
                timeout_ms=RFPULSE_ACK_TIMEOUT_MS,
                gap_ms=max(200, RFPULSE_CMD_GAP_MS), retries=1))
            self._schedule_next(max(200, RFPULSE_CMD_GAP_MS))
            return True

        # CSR=2: RF output ON → START 시퀀스에서만 RF_OFF 후 재시도
        if csr == 2 and (cmd.tag or "").startswith("[START") and cmd.cmd != CMD_RF_OFF:
            cmd.retries_left -= 1
            if self._cmd_timer:
                self._cmd_timer.stop()
            self._inflight = None
            self._cmd_q.appendleft(cmd)
            self._cmd_q.appendleft(self._make_cmd(
                "exec", CMD_RF_OFF, b"", tag="[AUTO RF_OFF]",
                timeout_ms=RFPULSE_ACK_TIMEOUT_MS,
                gap_ms=max(200, RFPULSE_CMD_GAP_MS), retries=1))
            self._schedule_next(max(200, RFPULSE_CMD_GAP_MS))
            return True

        return False

    # ==================== 명령 큐 ====================
    def _make_cmd(self, kind: str, cmd: int, data: bytes, *, tag: str = "",
                  timeout_ms: Optional[int] = None, gap_ms: Optional[int] = None,
                  retries: int = 3, allow_no_reply: bool = False,
                  allow_when_closing: bool = False,
                  callback: Optional[Callable[[Optional[bytes]], None]] = None) -> RfCommand:
        if timeout_ms is None:
            timeout_ms = RFPULSE_ACK_TIMEOUT_MS if kind == "exec" else RFPULSE_QUERY_TIMEOUT_MS
        return RfCommand(
            kind=kind, cmd=cmd, data=data, timeout_ms=int(timeout_ms),
            gap_ms=int(RFPULSE_CMD_GAP_MS if gap_ms is None else gap_ms),
            tag=tag, retries_left=int(retries), allow_no_reply=bool(allow_no_reply),
            allow_when_closing=bool(allow_when_closing),
            callback=callback or (lambda _b: None))

    def _enqueue(self, c: RfCommand):
        if self._closing and not c.allow_when_closing:
            self._safe_callback(c.callback, None)
            return
        self._cmd_q.append(c)
        QTimer.singleShot(0, self._dequeue_and_send)

    def _enqueue_exec(self, cmd: int, data: bytes = b"", **kw):
        self._enqueue(self._make_cmd("exec", cmd, data, **kw))

    def _enqueue_query(self, cmd: int, data: bytes = b"", **kw):
        self._enqueue(self._make_cmd("query", cmd, data, **kw))

    def _schedule_next(self, gap_ms: int):
        if self._gap_timer:
            self._gap_timer.start(max(0, int(gap_ms)))
        else:
            QTimer.singleShot(max(0, int(gap_ms)), self._dequeue_and_send)

    def _dequeue_and_send(self):
        if self._inflight is not None or not self._cmd_q:
            return
        if not (self.serial_rfp and self.serial_rfp.isOpen()):
            return
        if self._gap_timer and self._gap_timer.isActive():
            return
        if self._send_spin:
            return
        self._send_spin = True

        try:
            cmd = self._cmd_q.popleft()
            self._inflight = cmd
            # exec 직전에는 남아 있는 토큰을 버린다(원본과 같은 취지).
            if cmd.kind == "exec":
                self._rx.clear()

            pkt = _build_packet(self.addr, cmd.cmd, cmd.data)
            n = int(self.serial_rfp.write(pkt))
            if n <= 0:
                raise IOError(f"serial write returned {n}")
            if n != len(pkt):
                m = int(self.serial_rfp.write(pkt[n:]))
                if m <= 0 or n + m != len(pkt):
                    raise IOError(f"partial write: {n + max(0, m)}/{len(pkt)}")
            self.serial_rfp.flush()

            _data_s = _hex(cmd.data)
            if RFPULSE_RAW_LOG:
                # 챔버2 원본 [RFP][RAW][TX] 수준 — 실제 프레임 바이트열까지 남긴다
                _tx = f"{cmd.tag} {self._cmd_label(cmd.cmd)} data={_data_s or '(없음)'} raw={_hex(pkt)}"
            else:
                _tx = f"{cmd.tag} {self._cmd_label(cmd.cmd)} data={_data_s}".strip()
            self.status_message.emit("RFPulse > 전송", _tx)

            if cmd.allow_no_reply and cmd.kind == "exec":
                # 응답을 기다리지 않는다(종료 중 RF OFF 등)
                QTimer.singleShot(0, lambda: self._finish_inflight(b""))
            elif self._cmd_timer:
                self._cmd_timer.stop()
                self._cmd_timer.start(cmd.timeout_ms)

        except Exception as e:
            self.status_message.emit("RFPulse", f"[ERROR] 전송 실패: {e}")
            failed = self._inflight
            self._inflight = None
            if self._cmd_timer:
                self._cmd_timer.stop()
            if failed is not None:
                if failed.retries_left > 0:
                    failed.retries_left -= 1
                    self._cmd_q.appendleft(failed)
                else:
                    self._safe_callback(failed.callback, None)
            if not (self.serial_rfp and self.serial_rfp.isOpen()):
                QTimer.singleShot(0, self._try_reconnect)
            else:
                self._schedule_next(RFPULSE_CMD_GAP_MS)
        finally:
            self._send_spin = False

    def _on_cmd_timeout(self):
        if self._inflight is None:
            return
        self._fail_inflight("timeout")

    def _finish_inflight(self, data: Optional[bytes]):
        cmd = self._inflight
        if cmd is None:
            return
        if self._cmd_timer:
            self._cmd_timer.stop()
        self._inflight = None
        self._safe_callback(cmd.callback, data if data is not None else b"")
        self._schedule_next(cmd.gap_ms)

    def _fail_inflight(self, reason: str, csr: Optional[int] = None):
        cmd = self._inflight
        if cmd is None:
            return
        if self._cmd_timer:
            self._cmd_timer.stop()
        self._inflight = None

        if cmd.retries_left > 0 and not self._closing:
            cmd.retries_left -= 1
            self._cmd_q.appendleft(cmd)
            backoff_ms = max(150, cmd.gap_ms)
            if csr == 5:
                backoff_ms = max(int(cmd.gap_ms * 1.5), 1200)
            self._schedule_next(backoff_ms)
            return

        self.status_message.emit(
            "RFPulse", f"[FAIL] {cmd.tag} {self._cmd_label(cmd.cmd)} ({reason})")
        self._safe_callback(cmd.callback, None)
        self._schedule_next(cmd.gap_ms)

    @staticmethod
    def _is_rf_off_cmd(c: "RfCommand") -> bool:
        """종료용 RF OFF(allow_when_closing) — 폐기하면 안 되는 명령."""
        return c is not None and c.cmd == CMD_RF_OFF and bool(c.allow_when_closing)

    def _purge_pending(self, reason: str = "") -> int:
        """대기·인플라이트 명령을 버린다. 단 종료용 RF OFF 는 남긴다.

        ★ 감시 트립/리드백 불일치로 드라이버가 스스로 stop_process() 한 직후 상위 종료
          시퀀스가 stop_rf_pulse 로 한 번 더 부르면, 두 번째 호출의 purge 가 gap 타이머
          (1.5초) 때문에 아직 큐에 있던 첫 RF OFF 를 꺼내 콜백 _done(None) 을 불렀다 →
          전송되지 않은 RF OFF 에 "OFF 완료"/power_off_finished 가 나갔다. RF OFF 는
          실제로 전송된 뒤에만 완료가 나가야 한다(두 번 전송은 무해 — CESAR 는 CSR=0 을
          두 번 돌려줄 뿐이다).
        """
        purged = 0
        if self._inflight is not None and not self._is_rf_off_cmd(self._inflight):
            cmd = self._inflight
            self._inflight = None
            purged += 1
            self._safe_callback(cmd.callback, None)
        kept = []
        while self._cmd_q:
            c = self._cmd_q.popleft()
            if self._is_rf_off_cmd(c):
                kept.append(c)              # 순서 유지한 채 남긴다
                continue
            purged += 1
            self._safe_callback(c.callback, None)
        self._cmd_q.extend(kept)
        if reason and purged:
            self.status_message.emit("RFPulse", f"대기 중 명령 {purged}개 폐기 ({reason})")
        return purged

    # ==================== START 시퀀스 (상태 머신) ====================
    @Slot(dict)
    def start_process(self, params: dict):
        """{'target': W, 'freq_hz': int|None, 'duty': int|None}

        HOST → RF_OFF(사전 정리) → 200ms → MODE(FWD) → SETP →
        [FREQ] → [DUTY] → PULSING(1) → RF_ON → 800ms → 폴링 시작 → target_reached
        """
        params = params or {}
        target_w = float(params.get('target') or 0.0)
        freq_hz = params.get('freq_hz')
        duty = params.get('duty')

        self._stop_requested = False
        # ★ purge 보다 먼저 이전 시퀀스를 내린다. 순서가 바뀌면 _purge_pending 이
        #   이전 시퀀스의 콜백을 None 으로 불러 '재시작'(START 실패)이 허위로 뜬다.
        self._seq_running = False
        self._seq = []
        self._seq_idx = -1
        self.set_process_status(False)

        # 감시용 setpoint/카운터 초기화 + 리드백 검증용 요청값 보관
        self._target_setpoint_w = target_w
        self._req_freq_hz = int(freq_hz) if freq_hz is not None else None
        self._req_duty = int(duty) if duty is not None else None
        self._forp_out_of_range_count = 0
        self._refp_over_limit_count = 0

        # 최후 방어선 — UI/레시피에서 이미 막지만 원격 경로가 뚫릴 수 있다.
        if target_w > float(RFPULSE_MAX_POWER):
            self.status_message.emit(
                "재시작",
                f"RF Pulse 목표 {target_w:g}W 가 장비 상한 "
                f"{float(RFPULSE_MAX_POWER):g}W(RFPULSE_MAX_POWER)를 넘습니다 — 시작 불가")
            return

        if not (self.serial_rfp and self.serial_rfp.isOpen()):
            # 포트가 없으면 시퀀스를 시작조차 하지 않는다. 공정은 중단된다.
            self.status_message.emit(
                "재시작", f"RF Pulse 포트({RFPULSE_PORT})가 열려 있지 않습니다 — RF Pulse 시작 불가")
            return

        sp = int(round(target_w))
        steps: list = [
            ("exec", CMD_SET_ACTIVE_CTRL, b"\x02", "[START HOST]",
             RFPULSE_ACK_TIMEOUT_MS, "HOST 실패"),
            ("exec", CMD_RF_OFF, b"", "[START PRE RF OFF]",
             max(RFPULSE_ACK_TIMEOUT_MS, 2500), "RF OFF(사전) 실패"),
            ("delay", 200),
            ("exec", CMD_SET_CTRL_MODE, bytes([MODE_SET["fwd"]]), "[START MODE FWD]",
             RFPULSE_ACK_TIMEOUT_MS, "MODE=FWD 실패"),
            ("exec", CMD_SET_SETPOINT, bytes([sp & 0xFF, (sp >> 8) & 0xFF]),
             f"[START SETP {sp}W]", RFPULSE_ACK_TIMEOUT_MS, "SETP 실패"),
        ]

        # freq/duty 는 빈 칸(None)이면 명령 자체를 보내지 않는다 = 장비 현재값 유지.
        #  값은 클램프하지 않는다 — 범위를 벗어나면 장비가 CSR 50/51 로 알려 준다.
        if freq_hz is not None:
            hz = int(freq_hz)
            steps.append(("exec", CMD_SET_PULSE_FREQ,
                          bytes([hz & 0xFF, (hz >> 8) & 0xFF, (hz >> 16) & 0xFF]),
                          "[START FREQ]", RFPULSE_ACK_TIMEOUT_MS, "PULSE FREQ 실패"))
        if duty is not None:
            v = int(duty) & 0xFFFF
            steps.append(("exec", CMD_SET_PULSE_DUTY,
                          bytes([v & 0xFF, (v >> 8) & 0xFF]),
                          "[START DUTY]", RFPULSE_ACK_TIMEOUT_MS, "PULSE DUTY 실패"))

        steps.append(("exec", CMD_SET_PULSING, bytes([PULSING_TX[1]]),
                      "[START PULSING 1]", RFPULSE_ACK_TIMEOUT_MS, "PULSING 설정 실패"))
        steps.append(("exec", CMD_RF_ON, b"", "[START RF ON]",
                      max(RFPULSE_ACK_TIMEOUT_MS, 2500), "RF ON 실패"))
        steps.append(("delay", RFPULSE_POLL_START_DELAY_AFTER_RF_ON_MS))
        steps.append(("done",))

        f_txt = f"{freq_hz}Hz" if freq_hz is not None else "유지"
        d_txt = f"{duty}%" if duty is not None else "유지"
        self.status_message.emit(
            "RFPulse", f"RF Pulse 시작: {sp}W · freq={f_txt} · duty={d_txt}")

        self._seq = steps
        self._seq_idx = -1
        self._seq_running = True
        self._seq_next()

    def _seq_next(self):
        if not self._seq_running:
            return
        self._seq_idx += 1
        if self._seq_idx >= len(self._seq):
            self._seq_running = False
            return
        st = self._seq[self._seq_idx]
        kind = st[0]

        if kind == "delay":
            QTimer.singleShot(int(st[1]), self._seq_next)
            return

        if kind == "done":
            self._seq_running = False
            self.set_process_status(True)
            self.status_message.emit("RFPulse", "RF Pulse ON 완료 — 폴링 시작")
            self.target_reached.emit()
            self._readback_pulse_config()
            return

        # ("exec", cmd, data, tag, timeout, fail_why)
        _, cmd, data, tag, timeout_ms, why = st

        def _cb(res, _why=why):
            if not self._seq_running:
                return
            if res is None:
                self._seq_running = False
                self._seq = []
                self.set_process_status(False)
                self.status_message.emit(
                    "재시작", f"RF Pulse START 시퀀스 실패: {_why} "
                             f"(목표 {self._target_setpoint_w:.0f}W)")
                return
            self._seq_next()

        self._enqueue_exec(cmd, data, tag=tag, timeout_ms=timeout_ms, callback=_cb)

    def _readback_pulse_config(self):
        """START 직후 1회, 장비에 실제로 걸린 펄스 설정을 되읽는다.

        - 공정이 freq/duty 를 지정했으면 검증이다: 리드백이 요청값과 다르거나 실패하면
          "재시작" 으로 공정을 중단하고 스스로 RF OFF 한다(_check_power_monitors 트립과
          같은 관례). RFPULSE_VERIFY_PULSE_CONFIG=False 면 경고만.
        - 비워 둔(장비 현재값 유지) 항목은 기록용이다 — 실패해도 경고만 남기고 계속한다.
        """
        state = {'freq_khz': None, 'duty': None}
        req_f, req_d = self._req_freq_hz, self._req_duty

        def _verify():
            if self._stop_requested:           # 외부 stop 중이면 검증하지 않는다
                return
            bad = []
            if req_f is not None:
                if state['freq_khz'] is None:
                    bad.append("주파수 리드백 실패")
                elif abs(round(state['freq_khz'] * 1000.0) - req_f) > 1:
                    bad.append("주파수")
            if req_d is not None:
                if state['duty'] is None:
                    bad.append("듀티 리드백 실패")
                elif int(state['duty']) != req_d:
                    bad.append("듀티")
            if not bad:
                return
            _rq = (
                (f"{req_f / 1000.0:g}kHz" if req_f is not None else "유지")
                + "·"
                + (f"{req_d}%" if req_d is not None else "유지")
            )
            _rb = (
                ("?" if state['freq_khz'] is None else f"{state['freq_khz']:g}kHz")
                + "·"
                + ("?" if state['duty'] is None else f"{state['duty']}%")
            )
            _msg = f"RF Pulse 설정 불일치({', '.join(bad)}): 요청 {_rq} / 장비 {_rb}"
            if RFPULSE_VERIFY_PULSE_CONFIG:
                self.status_message.emit("재시작", _msg + " — 공정 중단")
                self.stop_process()
            else:
                self.status_message.emit(
                    "RFPulse", _msg + " — 경고만(RFPULSE_VERIFY_PULSE_CONFIG=false)")

        def _emit_final():
            # duty 콜백이 끝난 시점에 한 번만. 없는 쪽은 None 으로 올리고,
            #  둘 다 실패면 올리지 않는다(기록할 것이 없다). 그 뒤에 검증한다.
            if state['freq_khz'] is not None or state['duty'] is not None:
                _f = "?" if state['freq_khz'] is None else f"{state['freq_khz']:g}"
                _d = "?" if state['duty'] is None else str(state['duty'])
                self.status_message.emit("RFPulse", f"펄스 설정 리드백: {_f} kHz · {_d}%")
                self.pulse_config_readback.emit(dict(state))
            _verify()

        def on_duty(res):
            if res is None or len(res) < 2:
                self.status_message.emit(
                    "RFPulse", "펄스 duty 리드백 실패" + ("(무시)" if req_d is None else ""))
            else:
                state['duty'] = int(_u16le(res, 0))
            _emit_final()

        def on_freq(res):
            if res is None or len(res) < 3:
                self.status_message.emit(
                    "RFPulse", "펄스 주파수 리드백 실패" + ("(무시)" if req_f is None else ""))
            else:
                # 193 은 3바이트 LE(Hz). 화면/CSV 단위는 kHz 다.
                hz = res[0] | (res[1] << 8) | (res[2] << 16)
                state['freq_khz'] = hz / 1000.0
            if self._stop_requested:
                # 정지 중이면 DUTY 쿼리를 넣지 않는다 — 다음 전송이 RF OFF 여야 한다.
                _emit_final()
                return
            self._enqueue_query(CMD_REPORT_PULSE_DUTY, b"", tag="[READBACK DUTY]",
                                retries=1, callback=on_duty)

        self._enqueue_query(CMD_REPORT_PULSE_FREQ, b"", tag="[READBACK FREQ]",
                            retries=1, callback=on_freq)

    # ==================== STOP ====================
    @Slot()
    def stop_process(self):
        """폴링 off → RF OFF 1회 → power_off_finished.

        응답이 없어도, 포트를 닫는 중이어도 콜백은 반드시 호출한다.
        종료 시퀀스가 여기서 멈추면 안 된다.
        """
        self._stop_requested = True
        self._seq_running = False
        self._seq = []
        self._seq_idx = -1
        self.set_process_status(False)

        def _done(_res):
            self.status_message.emit("RFPulse", "RF Pulse OFF 완료")
            self.power_off_finished.emit()

        if not (self.serial_rfp and self.serial_rfp.isOpen()):
            # 포트가 없으면 보낼 수 없다. 그래도 상위가 기다리지 않도록 즉시 알린다.
            self.status_message.emit(
                "RFPulse", "포트가 닫혀 있어 RF OFF 를 보내지 못했습니다(완료로 처리).")
            QTimer.singleShot(0, lambda: self.power_off_finished.emit())
            return

        self._enqueue(self._make_cmd(
            "exec", CMD_RF_OFF, b"", tag="[RF OFF]",
            timeout_ms=max(RFPULSE_ACK_TIMEOUT_MS, 2500),
            allow_no_reply=True, allow_when_closing=True, retries=0,
            callback=_done))

    # ==================== 폴링 ====================
    @Slot(bool)
    def set_process_status(self, should_poll: bool):
        if should_poll:
            if self._poll_timer and not self._poll_timer.isActive():
                self._poll_busy = False
                self._poll_timer.start()
            return
        # ★ 폴링을 끌 때는 대기 중인 폴링 쿼리까지 반드시 버린다.
        #   안 버리면 STOP 의 RF OFF 가 남은 쿼리(각 최대 9초 + gap 1.5초) 뒤에
        #   줄을 서서 최악 ~30초 걸리고, _stop_impl 의 30초 타임아웃에 걸려
        #   RF 가 켜진 채 종료 시퀀스가 진행된다.
        if self._poll_timer:
            self._poll_timer.stop()
        self._purge_pending("polling off")
        self._poll_busy = False

    def _poll_cycle(self):
        """1주기: REPORT_STATUS → REPORT_FORWARD → REPORT_REFLECTED. 중첩 금지."""
        if self._poll_busy:
            return
        if not (self.serial_rfp and self.serial_rfp.isOpen()):
            return
        self._poll_busy = True

        def after_ref(res_r):
            try:
                if res_r is not None:
                    self._last_reflected_w = float(
                        _u16le(res_r, 0) if len(res_r) >= 2 else 0.0)
                self._poll_finish()
            finally:
                self._poll_busy = False

        def after_fwd(res_f):
            if res_f is not None:
                self._last_forward_w = float(
                    _u16le(res_f, 0) if len(res_f) >= 2 else 0.0)
            self._enqueue_query(CMD_REPORT_REFLECTED, b"", tag="[POLL REF]",
                                timeout_ms=RFPULSE_POLL_QUERY_TIMEOUT_MS,
                                callback=after_ref)

        def after_status(res_s):
            st = self._parse_status_0xA2(res_s)
            if st:
                self._last_status = st
                self.status_message.emit(
                    "RFPulse", f"STATUS {self._status_summary_str(st)}")
                self._validate_status(st)
            self._enqueue_query(CMD_REPORT_FORWARD, b"", tag="[POLL FWD]",
                                timeout_ms=RFPULSE_POLL_QUERY_TIMEOUT_MS,
                                callback=after_fwd)

        self._enqueue_query(CMD_REPORT_STATUS, b"", tag="[POLL WAKE]",
                            timeout_ms=RFPULSE_POLL_QUERY_TIMEOUT_MS,
                            callback=after_status)

    def _poll_finish(self):
        self._check_power_monitors()
        if (self._last_forward_w is not None) and (self._last_reflected_w is not None):
            self.update_rfpulse_status_display.emit(
                float(self._last_forward_w), float(self._last_reflected_w))

    # ==================== FORP / REFP 감시 ====================
    def _check_power_monitors(self):
        """FORP 5% 이탈 3회 연속 / REFP 20W 이상 3회 연속이면 공정 중단.

        최근 STATUS 의 rf_output_on 이 True 일 때만 본다. 외부 stop 중에는 보지 않는다.
        """
        if not (self._target_setpoint_w > 0.0):
            return
        if self._last_forward_w is None or self._last_reflected_w is None:
            return
        if self._stop_requested:
            return

        status = self._last_status
        rf_on = bool(status.rf_output_on) if status is not None else True
        if not rf_on:
            return

        # 1) FORP: setpoint 대비 허용 오차 이탈
        tol = self._target_setpoint_w * (RFPULSE_FORP_TOLERANCE_PERCENT / 100.0)
        diff = abs(self._last_forward_w - self._target_setpoint_w)
        if diff >= tol:
            self._forp_out_of_range_count += 1
        else:
            self._forp_out_of_range_count = 0

        if self._forp_out_of_range_count >= RFPULSE_FORP_CONSECUTIVE_LIMIT:
            n = self._forp_out_of_range_count
            self._forp_out_of_range_count = 0      # 중복 트리거 방지
            self.status_message.emit(
                "재시작",
                f"RF Pulse FORP setpoint 이탈: 측정 {self._last_forward_w:.1f}W, "
                f"목표 {self._target_setpoint_w:.1f}W, "
                f"허용오차 ±{RFPULSE_FORP_TOLERANCE_PERCENT:.1f}% "
                f"(±{tol:.1f}W), 편차 {diff:.1f}W — {n}회 연속")
            # RFpower.py 와 같은 관례 — 알리기만 하지 않고 드라이버가 스스로 끈다.
            #  상위의 "재시작" 처리가 늦거나 없어도 RF 는 꺼져야 한다.
            #  (이때 나가는 power_off_finished 는 아직 아무도 기다리지 않으므로 그냥
            #   버려지고, 뒤이어 오는 _stop_impl 의 stop_process 가 RF OFF 를 한 번 더
            #   보내 자기 완료 신호를 받는다. 중복 stop 은 무해하다)
            self.stop_process()
            return

        # 2) REFP: 임계값 이상
        if self._last_reflected_w >= RFPULSE_REFP_LIMIT_WATTS:
            self._refp_over_limit_count += 1
        else:
            self._refp_over_limit_count = 0

        if self._refp_over_limit_count >= RFPULSE_REFP_CONSECUTIVE_LIMIT:
            n = self._refp_over_limit_count
            self._refp_over_limit_count = 0
            self.status_message.emit(
                "재시작",
                f"RF Pulse REFP 과다 반사: 측정 {self._last_reflected_w:.1f}W, "
                f"한계 {RFPULSE_REFP_LIMIT_WATTS:.1f}W — {n}회 연속")
            self.stop_process()

    # ==================== 파싱/검증/유틸 ====================
    def _frame_match(self, payload: bytes, expected_cmd: int) -> bool:
        if not payload or len(payload) < 3:
            return False
        hdr = payload[0]
        cmd_b = payload[1]
        rx_addr = (hdr >> 3) & 0x1F
        return (rx_addr == self.addr) and (cmd_b == expected_cmd)

    def _extract_data(self, payload: bytes) -> bytes:
        hdr = payload[0]
        length_bits = hdr & 0x07
        idx = 2
        if length_bits == 7:
            dlen = payload[idx]
            idx += 1
        else:
            dlen = length_bits
        return bytes(payload[idx:idx + dlen])

    def _parse_status_0xA2(self, data: Optional[bytes]) -> Optional[RfStatus]:
        if not data or len(data) < 4:
            if data is not None:
                self.status_message.emit("RFPulse", "STATUS payload too short")
            return None
        b1, b2, b3, b4 = data[0], data[1], data[2], data[3]
        return RfStatus(
            rf_output_on      = bool(b1 & (1 << 5)),
            rf_on_requested   = bool(b1 & (1 << 6)),
            setpoint_mismatch = bool(b1 & (1 << 7)),
            interlock_open    = bool(b2 & (1 << 7)),
            overtemp          = bool(b2 & (1 << 3)),
            current_limit     = bool(b4 & (1 << 0)),
            extended_fault    = bool(b4 & (1 << 5)),
            cex_lock          = bool(b4 & (1 << 7)),
            raw=bytes(data[:4]))

    def _status_summary_str(self, st: RfStatus) -> str:
        return (f"on={int(st.rf_output_on)} req={int(st.rf_on_requested)} "
                f"sp_miss={int(st.setpoint_mismatch)} ilock={int(st.interlock_open)} "
                f"ot={int(st.overtemp)} limI={int(st.current_limit)} "
                f"xflt={int(st.extended_fault)} cex={int(st.cex_lock)}")

    def _validate_status(self, st: RfStatus) -> None:
        # 원본과 같이 경고만 로깅한다(강한 게이팅은 하지 않는다).
        if st.interlock_open:
            self.status_message.emit("RFPulse", "STATUS: Interlock OPEN detected")
        if st.overtemp:
            self.status_message.emit("RFPulse", "STATUS: Over-Temperature detected")
        if st.extended_fault:
            self.status_message.emit("RFPulse", "STATUS: Extended fault present")
        if st.rf_on_requested and not st.rf_output_on:
            self.status_message.emit("RFPulse", "STATUS: RF requested but output not ON yet")

    def _cmd_label(self, cmd: int) -> str:
        name = CMD_NAMES.get(cmd)
        return f"{name}(0x{cmd:02X})" if name else f"0x{cmd:02X}"

    def _safe_callback(self, cb, arg):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception as e:
            self.status_message.emit("RFPulse", f"콜백 오류: {e}")

    def is_connected(self) -> bool:
        return bool(self.serial_rfp and self.serial_rfp.isOpen())
