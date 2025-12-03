# -*- coding: utf-8 -*-
"""
MFC (Chamber-K) — PyQt6 QSerialPort 비동기 컨트롤러
- 챔버2 MFC 구조 통일
- 가스: Ch1=Ar, Ch2=O2
- 유량: 스케일 변환 없음(장비 값 그대로 사용)
- 압력: UI↔HW 1/10 스케일 변환 (보낼 때 ×, 표시할 때 ÷)
- 그래프/CSV용 시그널 제거, UI용 문자열만 emit
- ProcessController의 'set_polling' 명령 지원
"""

from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from typing import Deque, Callable, Optional
import re
import traceback

from PyQt6.QtCore import QObject, QTimer, QIODeviceBase, pyqtSignal as Signal, pyqtSlot as Slot
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo

@dataclass
class Command:
    cmd_str: str
    callback: Callable[[Optional[str]], None]
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool

# 필요한 설정만 가져옵니다(스케일 관련 상수는 사용하지 않음).
from lib.config import (
    MFC_PORT, MFC_BAUD, MFC_COMMANDS,
    FLOW_ERROR_TOLERANCE, FLOW_ERROR_MAX_COUNT,
    MFC_POLLING_INTERVAL_MS, MFC_STABILIZATION_INTERVAL_MS,
    MFC_WATCHDOG_INTERVAL_MS, MFC_RECONNECT_BACKOFF_START_MS,
    MFC_RECONNECT_BACKOFF_MAX_MS, MFC_TIMEOUT,
    MFC_GAP_MS, MFC_DELAY_MS, MFC_DELAY_MS_VALVE,
    MFC_PRESSURE_SCALE, MFC_PRESSURE_DECIMALS, MFC_SP1_VERIFY_TOL
)

class MFCController(QObject):
    # --- 시그널 ---
    status_message     = Signal(str, str)
    update_flow        = Signal(str, float) # 예: ("Ar", 12.34)
    update_pressure    = Signal(str)        # 예: "760.00"
    command_failed     = Signal(str, str)   # (cmd, reason)
    command_confirmed  = Signal(str)

    # (호환성) 혹시 메인에서 self-loop 연결을 했다면 깨지지 않게 유지
    command_requested  = Signal(str, dict)

    def __init__(self, parent=None):
        super().__init__(parent)

        # 스레드 내부에서 지연 생성
        self.serial_mfc: Optional[QSerialPort] = None
        self._cmd_timer: Optional[QTimer] = None
        self._gap_timer: Optional[QTimer] = None
        self.polling_timer: Optional[QTimer] = None
        self.stabilization_timer: Optional[QTimer] = None
        self._watchdog: Optional[QTimer] = None

        # RX 버퍼
        self._rx = bytearray()
        self._RX_MAX = 16 * 1024
        self._LINE_MAX = 512
        self._overflow_count = 0

        # 명령 큐
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None
        self._send_spin = False

        # 연결 상태
        self._want_connected = False
        self._reconnect_backoff_ms = MFC_RECONNECT_BACKOFF_START_MS
        self._reconnect_pending = False

        # 안정화/모니터링
        self.gas_map = {1: "Ar", 2: "O2"}       # ← 2채널만
        self.last_setpoints = {1: 0.0, 2: 0.0}  # 장비 단위(=UI 단위와 동일)
        self.flow_error_counters = {1: 0, 2: 0}

        # 압력 모니터링용 (SP1 기준, UI 단위)
        self.last_pressure_setpoint: float = 0.0
        self.pressure_error_count: int = 0

        # 🔹 Shutter Delay / Process Time 구간에서만 에러 체크할지 여부
        self._flow_monitoring_enabled: bool = False
        self._pressure_monitoring_enabled: bool = False

        # 🔹 ProcessController에서 알려주는 "이번 공정 활성 채널"
        #    기본값은 두 채널 모두 활성
        self._active_channels: list[int] = [1, 2]

        self._stabilizing_channel: Optional[int] = None
        self._stabilizing_target: float = 0.0
        self.stabilization_attempts = 0

        # (호환성) self-loop로 들어와도 처리되게 자기 자신에 연결
        self.command_requested.connect(self.handle_command)

    # ---------- 타이머/시리얼 지연 생성 ----------
    def _ensure_serial_created(self):
        if self.serial_mfc is not None:
            return
        self.serial_mfc = QSerialPort(self)
        self.serial_mfc.setBaudRate(MFC_BAUD)
        self.serial_mfc.setDataBits(QSerialPort.DataBits.Data8)
        self.serial_mfc.setParity(QSerialPort.Parity.NoParity)
        self.serial_mfc.setStopBits(QSerialPort.StopBits.OneStop)
        self.serial_mfc.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
        self.serial_mfc.readyRead.connect(self._on_ready_read)
        self.serial_mfc.errorOccurred.connect(self._on_serial_error)

    def _ensure_timers_created(self):
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
            self.polling_timer.setInterval(MFC_POLLING_INTERVAL_MS)
            self.polling_timer.timeout.connect(self._enqueue_poll_cycle)

        if self.stabilization_timer is None:
            self.stabilization_timer = QTimer(self)
            self.stabilization_timer.setInterval(MFC_STABILIZATION_INTERVAL_MS)
            self.stabilization_timer.timeout.connect(self._check_flow_stabilization)

        if self._watchdog is None:
            self._watchdog = QTimer(self)
            self._watchdog.setInterval(MFC_WATCHDOG_INTERVAL_MS)
            self._watchdog.timeout.connect(self._watch_connection)

    # ---------- 연결/해제 ----------
    @Slot()
    def connect_mfc_device(self) -> bool:
        self._ensure_serial_created()
        self._ensure_timers_created()

        self._want_connected = True
        ok = self._open_port()
        if self._watchdog:
            self._watchdog.start()
        return ok

    def _open_port(self) -> bool:
        if self.serial_mfc and self.serial_mfc.isOpen():
            return True

        ports = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if MFC_PORT not in ports:
            self.status_message.emit("MFC", f"{MFC_PORT} 존재하지 않음. 사용 가능 포트: {sorted(ports)}")
            return False

        self.serial_mfc.setPortName(MFC_PORT)
        if not self.serial_mfc.open(QIODeviceBase.OpenModeFlag.ReadWrite):
            self.status_message.emit("MFC", f"{MFC_PORT} 연결 실패: {self.serial_mfc.errorString()}")
            return False

        self.serial_mfc.setDataTerminalReady(True)
        self.serial_mfc.setRequestToSend(False)
        self.serial_mfc.clear(QSerialPort.Direction.AllDirections)
        self._rx.clear()

        self._reconnect_backoff_ms = MFC_RECONNECT_BACKOFF_START_MS
        self._reconnect_pending = False

        self.status_message.emit("MFC", f"{MFC_PORT} 연결 성공 (PyQt6 QSerialPort)")
        return True

    def _watch_connection(self):
        if (not self._want_connected) or (self.serial_mfc and self.serial_mfc.isOpen()):
            return
        if self._reconnect_pending:
            return
        self._reconnect_pending = True
        self.status_message.emit("MFC", f"재연결 시도... ({self._reconnect_backoff_ms} ms)")
        QTimer.singleShot(self._reconnect_backoff_ms, self._try_reconnect)

    def _try_reconnect(self):
        self._reconnect_pending = False
        if (not self._want_connected) or (self.serial_mfc and self.serial_mfc.isOpen()):
            return
        if self._open_port():
            self.status_message.emit("MFC", "재연결 성공. 대기 중 명령 재개.")
            QTimer.singleShot(0, self._dequeue_and_send)
            self._reconnect_backoff_ms = MFC_RECONNECT_BACKOFF_START_MS
        else:
            self._reconnect_backoff_ms = min(self._reconnect_backoff_ms * 2, MFC_RECONNECT_BACKOFF_MAX_MS)

    @Slot()
    def cleanup(self):
        self._want_connected = False

        # 진행/대기 콜백 정리
        if self._inflight is not None:
            self._safe_callback(self._inflight.callback, None)
            self._inflight = None
        while self._cmd_q:
            pending = self._cmd_q.popleft()
            self._safe_callback(pending.callback, None)

        # 타이머 정리
        for tname in ("_cmd_timer", "_gap_timer", "polling_timer", "stabilization_timer", "_watchdog"):
            t = getattr(self, tname)
            if t:
                try: t.stop()
                except: pass
                t.deleteLater()
                setattr(self, tname, None)

        # 포트 정리
        if self.serial_mfc:
            try:
                if self.serial_mfc.isOpen():
                    self.serial_mfc.close()
                    self.status_message.emit("MFC", "시리얼 포트를 안전하게 닫았습니다.")
            finally:
                self.serial_mfc.deleteLater()
                self.serial_mfc = None

        self._rx.clear()
        self._reconnect_pending = False

    # ---------- 시리얼 이벤트 ----------
    def _on_serial_error(self, err: QSerialPort.SerialPortError):
        if err == QSerialPort.SerialPortError.NoError:
            return

        serr = self.serial_mfc.errorString() if self.serial_mfc else ""
        self.status_message.emit("MFC", f"시리얼 오류: {serr} (err={err})")

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

        if self.serial_mfc and self.serial_mfc.isOpen():
            self.serial_mfc.close()
        if self._gap_timer:
            self._gap_timer.stop()
        self._rx.clear()

        QTimer.singleShot(0, self._watch_connection)

    def _on_ready_read(self):
        if not (self.serial_mfc and self.serial_mfc.isOpen()):
            return

        ba = self.serial_mfc.readAll()
        if ba.isEmpty():
            return

        self._rx.extend(bytes(ba))
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]
            self._overflow_count += 1
            if self._overflow_count % 5 == 1:
                self.status_message.emit("MFC", f"수신 버퍼 과다(RX>{self._RX_MAX}); 최근 {self._RX_MAX}B만 보존.")

        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line_bytes = self._rx[:idx]
            drop = idx + 1
            if drop < len(self._rx):
                ch = self._rx[idx]
                nxt = self._rx[idx + 1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]

            if len(line_bytes) > self._LINE_MAX:
                over = len(line_bytes) - self._LINE_MAX
                self.status_message.emit("MFC", f"[WARN] RX line too long (+{over}B), truncating")
                line_bytes = line_bytes[:self._LINE_MAX]

            try:
                line = line_bytes.decode('ascii', errors='ignore').strip()
            except Exception:
                line = None

            if not line:
                continue

            # 에코 라인은 스킵
            if self._inflight:
                sent = (self._inflight.cmd_str or "").strip()
                if line == sent:
                    continue

            self.status_message.emit("MFC", f"[RECV] {repr(line)}")
            self._finish_command(line)
            break

        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    # ---------- 명령 큐 ----------
    def enqueue(self, cmd_str: str, on_reply: Callable[[Optional[str]], None],
                timeout_ms: int = MFC_TIMEOUT, gap_ms: int = MFC_GAP_MS,
                tag: str = "", retries_left: int = 5,
                allow_no_reply: bool = False):
        if not cmd_str.endswith('\r'):
            cmd_str += '\r'
        self._cmd_q.append(Command(cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))
        if (self._inflight is None) and (not (self._gap_timer and self._gap_timer.isActive())):
            QTimer.singleShot(0, self._dequeue_and_send)

    def _dequeue_and_send(self):
        if self._inflight is not None or not self._cmd_q:
            return
        if not (self.serial_mfc and self.serial_mfc.isOpen()):
            return
        if self._gap_timer and self._gap_timer.isActive():
            return

        if self._send_spin:
            self.status_message.emit("MFC", "[GUARD] _dequeue_and_send re-enter blocked")
            return
        self._send_spin = True

        try:
            cmd = self._cmd_q.popleft()
            self._inflight = cmd
            self._rx.clear()

            self.status_message.emit("MFC > 전송", f"{cmd.tag or ''} {cmd.cmd_str.strip()}".strip())

            payload = cmd.cmd_str.encode('ascii')
            n = int(self.serial_mfc.write(payload))
            if n <= 0:
                raise IOError(f"serial write returned {n}")

            total = n
            if total != len(payload):
                remain = payload[total:]
                m = int(self.serial_mfc.write(remain))
                if m > 0:
                    total += m
                if total != len(payload):
                    raise IOError(f"partial write: queued {total}/{len(payload)} bytes")

            self.serial_mfc.flush()

            if self._cmd_timer:
                self._cmd_timer.stop()
                if cmd.allow_no_reply:
                    QTimer.singleShot(0, lambda: self._finish_command(None))
                else:
                    self._cmd_timer.start(cmd.timeout_ms)

        except Exception as e:
            self.status_message.emit("MFC", f"[ERROR] Send failed: {e}")
            failed = self._inflight
            self._inflight = None
            if self._cmd_timer:
                self._cmd_timer.stop()
            if failed:
                self._cmd_q.appendleft(failed)
            try:
                if not (self.serial_mfc and self.serial_mfc.isOpen()):
                    QTimer.singleShot(0, self._try_reconnect)
                else:
                    gap_ms = failed.gap_ms if failed else 100
                    if self._gap_timer:
                        self._gap_timer.start(gap_ms)
                    QTimer.singleShot(gap_ms + 1, self._dequeue_and_send)
            except Exception as ee:
                self.status_message.emit("MFC", f"[WARN] reconnect/retry schedule failed: {ee}")
        finally:
            self._send_spin = False

    def _on_cmd_timeout(self):
        if not self._inflight:
            return
        cmd = self._inflight
        self.status_message.emit("MFC", "[TIMEOUT] command response timed out" if not cmd.allow_no_reply
                                 else "[NOTE] no-reply command; proceeding after write")
        self._finish_command(None)

    def _finish_command(self, line: Optional[str]):
        if self._inflight is None:
            return
        cmd = self._inflight
        if self._cmd_timer:
            self._cmd_timer.stop()
        self._inflight = None

        if line is None:
            if cmd.allow_no_reply:
                self._safe_callback(cmd.callback, None)
                if self._gap_timer:
                    self._gap_timer.start(cmd.gap_ms)
                return
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
                if self.serial_mfc and self.serial_mfc.isOpen():
                    self.serial_mfc.close()
                self._try_reconnect()
                return
            self._safe_callback(cmd.callback, None)
            if self._gap_timer:
                self._gap_timer.start(cmd.gap_ms)
            return

        self._safe_callback(cmd.callback, (line.strip() if isinstance(line, str) else line))
        if self._gap_timer:
            self._gap_timer.start(cmd.gap_ms)

    # ---------- 폴링 ----------
    def _is_poll_read_cmd(self, cmd_str: str, tag: str = "") -> bool:
        if (tag or "").startswith("[POLL "):
            return True
        s = (cmd_str or "").lstrip().upper()
        return s.startswith("R60") or s.startswith("R5")

    def _purge_poll_reads_only(self, cancel_inflight: bool = True, reason: str = "") -> int:
        purged = 0
        if cancel_inflight and self._inflight and self._is_poll_read_cmd(self._inflight.cmd_str, self._inflight.tag):
            if self._cmd_timer:
                self._cmd_timer.stop()
            cmd = self._inflight
            self._inflight = None
            purged += 1
            self.status_message.emit("MFC", f"[QUIESCE] 폴링 읽기 인플라이트 취소: {cmd.tag or cmd.cmd_str.strip()} ({reason})")
            self._safe_callback(cmd.callback, None)

        kept = deque()
        while self._cmd_q:
            c = self._cmd_q.popleft()
            if self._is_poll_read_cmd(c.cmd_str, c.tag):
                purged += 1
                continue
            kept.append(c)
        self._cmd_q = kept
        if purged:
            self.status_message.emit("MFC", f"[QUIESCE] 폴링 읽기 명령 {purged}건 제거 ({reason})")
        return purged

    @Slot(bool)
    def set_process_status(self, should_poll: bool):
        if not self.polling_timer:
            return

        # 🔸 Shutter Delay / Process Time 구간에서만 모니터링 켜기
        self._flow_monitoring_enabled = bool(should_poll)
        self._pressure_monitoring_enabled = bool(should_poll)

        if should_poll:
            if not self.polling_timer.isActive():
                self.status_message.emit("MFC", "주기적 읽기(Polling) 시작")
                self.polling_timer.start()
        else:
            if self.polling_timer.isActive():
                self.polling_timer.stop()
                self.status_message.emit("MFC", "주기적 읽기(Polling) 중지")
            self._purge_poll_reads_only(cancel_inflight=True, reason="polling off/shutter closed")

            # 🔸 모니터링 구간이 끝날 때는 에러 카운터도 리셋
            self.flow_error_counters = {1: 0, 2: 0}
            self.pressure_error_count = 0

    def _enqueue_poll_cycle(self):
        self._read_flow_all_async(tag="[POLL R60]")

        def on_p(line: Optional[str]):
            val_hw = self._parse_pressure_value(line)
            if val_hw is None:
                return
            ui_val = self._to_ui_pressure(val_hw)
            fmt = "{:." + str(int(MFC_PRESSURE_DECIMALS)) + "f}"
            self.update_pressure.emit(fmt.format(ui_val))
            # SP1 설정값 대비 압력 모니터링
            try:
                self._monitor_pressure(ui_val)
            except Exception:
                # 모니터링 중 예외로 폴링이 죽지 않도록 방어
                traceback.print_exc()

        self.enqueue(
            MFC_COMMANDS['READ_PRESSURE'],
            on_p, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag='[POLL PRESS]'
        )

    # ---------- 외부 명령(프로세스 컨트롤러) ----------
    @Slot(str, dict)
    def handle_command(self, cmd: str, params: dict):
        # 🔹 활성 채널 설정 (ProcessController → MFC)
        if cmd == "set_active_channels":
            chans = params.get("channels") or []
            try:
                # 1 또는 2만 허용, 중복 제거 후 정렬
                lst = sorted({int(c) for c in chans if int(c) in (1, 2)})
            except Exception:
                lst = [1, 2]

            if not lst:
                lst = [1, 2]

            self._active_channels = lst

            # 이번 공정에서 사용하지 않는 채널은 세트포인트/에러 카운터를 초기화
            for ch in (1, 2):
                if ch not in lst:
                    self.last_setpoints[ch] = 0.0
                    self.flow_error_counters[ch] = 0

            self.status_message.emit("MFC", f"활성 채널 설정: {self._active_channels}")
            return
        
        # 폴링 ON/OFF (챔버K 프로세스 컨트롤러 호환용)
        if cmd == "set_polling":
            enable = bool(params.get("enable", False))
            self.set_process_status(enable)
            if not enable:
                # 공정 딜레이(셔터/메인 공정) 종료 시 카운터 초기화
                self.flow_error_counters = {1: 0, 2: 0}
                self.pressure_error_count = 0
            return

        # FLOW_SET (스케일 변환 없음)
        if cmd == "FLOW_SET":
            ch = int(params.get("channel", 1))
            target = float(params.get("value", 0.0))  # 장비=UI 동일 단위
            def after_set(_line):
                self._verify_flow_set_async(ch, target)
            self.enqueue(MFC_COMMANDS["FLOW_SET"](channel=ch, value=target),
                         after_set, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                         tag=f"[SET ch{ch}]", allow_no_reply=True)
            return

        # FLOW_ON / FLOW_OFF
        if cmd in ("FLOW_ON", "FLOW_OFF"):
            ch = int(params.get("channel", 1))
            self._apply_flow_onoff_with_L0(ch, cmd == "FLOW_ON")
            return

        # 밸브
        if cmd in ("VALVE_OPEN", "VALVE_CLOSE"):
            vcmd = MFC_COMMANDS[cmd]
            def after_valve(_):
                self.status_message.emit("MFC", f"밸브 이동 대기 ({int(MFC_DELAY_MS_VALVE/1000)}초)...")
                QTimer.singleShot(MFC_DELAY_MS_VALVE, lambda: self._check_valve_position_async(cmd))
            self.enqueue(vcmd, after_valve, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                         tag=f"[{cmd}]", allow_no_reply=True)
            return

        # SP1/4 
        if cmd == "SP1_SET":
            ui_val = float(params.get("value", 0.0))
            hw_val = self._to_hw_pressure(ui_val)
            
            # 압력 모니터링용 SP1 설정값(UI 단위) 저장
            self.last_pressure_setpoint = float(ui_val)
            self.pressure_error_count = 0

            # ✨ 추가: 장비 송신 값(하드웨어 스케일)을 지정 소수 자리로 문자열 포맷
            dec = int(MFC_PRESSURE_DECIMALS)
            val_str = f"{hw_val:.{dec}f}"

            def after_sent(line, ui_val=ui_val):
                if line:
                    self.status_message.emit("MFC < 확인", f"SP1 <- {ui_val:.2f}")
                # ✅ 검증도 HW값 기준으로 비교
                self._verify_simple_async("SP1_SET", {"value": hw_val, "_dec": dec})

            # ✨ 변경: value=hw_val  →  value=val_str
            self.enqueue(MFC_COMMANDS["SP1_SET"](value=val_str),
                        after_sent, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                        tag=f"[SP1_SET {ui_val:.2f}]", allow_no_reply=True)
            return
        
        if cmd == "SP2_SET":
            ui_val = float(params.get("value", 0.0))
            hw_val = self._to_hw_pressure(ui_val)

            dec = int(MFC_PRESSURE_DECIMALS)
            val_str = f"{hw_val:.{dec}f}"

            def after_sent(_line, ui_val=ui_val):
                if _line:
                    self.status_message.emit("MFC < 확인", f"SP2 <- {ui_val:.2f}")
                # 여기서는 간단히 확인만 하고 넘어가고, 필요하면 별도 verify 추가 가능
                self.command_confirmed.emit("SP2_SET")

            self.enqueue(
                MFC_COMMANDS["SP2_SET"](value=val_str),
                after_sent,
                timeout_ms=MFC_TIMEOUT,
                gap_ms=MFC_GAP_MS,
                tag=f"[SP2_SET {ui_val:.2f}]",
                allow_no_reply=True,
            )
            return

        if cmd in ("SP1_ON", "SP2_ON","SP4_ON"):
            scmd = MFC_COMMANDS[cmd]
            def after_simple(_line, _cmd=cmd):
                self._verify_simple_async(_cmd, {})
            self.enqueue(scmd, after_simple, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                         tag=f"[{cmd}]", allow_no_reply=True)
            return

        if cmd == "PS_ZEROING":
            def after_ps(_): self.command_confirmed.emit("PS_ZEROING")
            self.enqueue(MFC_COMMANDS["PS_ZEROING"], after_ps,
                         timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                         tag="[PS_ZEROING]", allow_no_reply=True)
            return

        if cmd == "MFC_ZEROING":
            ch = int(params.get("channel", 1))
            def after_mfc(_): self.command_confirmed.emit("MFC_ZEROING")
            self.enqueue(MFC_COMMANDS["MFC_ZEROING"](channel=ch), after_mfc,
                         timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                         tag=f"[MFC_ZEROING ch{ch}]", allow_no_reply=True)
            return

        # READ
        if cmd == "READ_FLOW":
            ch = int(params.get('channel', 1))
            def on_done(vals):
                if vals and 1 <= ch <= len(vals):
                    v = float(vals[ch-1])
                    gas = "Ar" if ch == 1 else "O2"
                    self.update_flow.emit(gas, v)
                else:
                    self.command_failed.emit("READ_FLOW", "R60 파싱 실패/채널 누락")
            self._read_flow_all_async(on_done=on_done, tag=f"[READ R60 ch{ch}]")
            return

        if cmd == "READ_PRESSURE":
            def on_p(line: Optional[str]):
                val_hw = self._parse_pressure_value(line)
                if val_hw is None:
                    self.command_failed.emit("READ_PRESSURE", "응답 없음/파싱 실패")
                    return
                ui_val = self._to_ui_pressure(val_hw)
                fmt = "{:." + str(int(MFC_PRESSURE_DECIMALS)) + "f}"
                self.update_pressure.emit(fmt.format(ui_val))
            self.enqueue(
                MFC_COMMANDS['READ_PRESSURE'],
                on_p, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag='[READ_PRESSURE]'
            )
            return

        self.command_failed.emit(cmd, "알 수 없는 명령")

    def _verify_simple_async(self, cmd: str, params: dict,
                            attempt: int = 1, max_attempts: int = 5, delay_ms: int = MFC_DELAY_MS):
        if cmd == "SP1_SET":
            val_hw = float(params['value'])
            ui_val = float(params.get("_ui_value", self._to_ui_pressure(val_hw)))
            rd = MFC_COMMANDS['READ_SP1_VALUE']

            def on_reply(line: Optional[str], attempt=attempt):
                cur_hw = self._parse_pressure_value(line or "")
                tol = max(float(MFC_SP1_VERIFY_TOL), 1e-9)
                ok = (cur_hw is not None) and (abs(cur_hw - val_hw) <= tol)
                if ok:
                    fmt_hw = "{:." + str(int(MFC_PRESSURE_DECIMALS)) + "f}"
                    self.status_message.emit(
                        "MFC < 확인",
                        f"SP1 설정 완료: UI {ui_val:.2f} (장비 {fmt_hw.format(val_hw)})"
                    )
                    self.command_confirmed.emit("SP1_SET")
                else:
                    if attempt < max_attempts:
                        self.status_message.emit(
                            "WARN",
                            f"[SP1_SET 검증 재시도] 응답={repr(line)} (시도 {attempt}/{max_attempts})"
                        )
                        QTimer.singleShot(
                            delay_ms,
                            lambda: self._verify_simple_async(
                                "SP1_SET",
                                {"value": val_hw, "_ui_value": ui_val},
                                attempt + 1, max_attempts, delay_ms
                            )
                        )
                    else:
                        self.command_failed.emit("SP1_SET", "SP1 설정 확인 실패")

            self.enqueue(rd, on_reply, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag='[VERIFY SP1_SET]')
            return

        if cmd in ("SP1_ON", "SP2_ON", "SP4_ON"):
            rd = MFC_COMMANDS['READ_SYSTEM_STATUS']
            # 각 SP에 해당하는 기대 문자 매핑
            sp_map = {
                "SP1_ON": "1",
                "SP2_ON": "2",
                "SP4_ON": "4",
            }

            def on_reply(line: Optional[str], attempt=attempt, cmd=cmd):
                s = (line or "").strip()
                expect = sp_map.get(cmd, "")
                ok = bool(
                    s and s.startswith("M") and len(s) > 1 and s[1] == expect
                )
                if ok:
                    self.status_message.emit("MFC < 확인", f"{cmd} 활성화 확인")
                    self.command_confirmed.emit(cmd)
                else:
                    if attempt < max_attempts:
                        self.status_message.emit(
                            "WARN",
                            f"[{cmd} 검증 재시도] 응답={repr(s)} (시도 {attempt}/{max_attempts})"
                        )
                        QTimer.singleShot(
                            delay_ms,
                            lambda: self._verify_simple_async(
                                cmd, params, attempt + 1, max_attempts, delay_ms
                            )
                        )
                    else:
                        self.command_failed.emit(cmd, f"{cmd} 상태 확인 실패")

            self.enqueue(rd, on_reply,
                         timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                         tag=f'[VERIFY {cmd}]')
            return

    # ---------- 파서/유틸 ----------
    def _q_prefixes_for(self, cmd_key: str, ch: int) -> tuple[str, ...]:
        if cmd_key == 'READ_FLOW_SET':  # R65~R68
            return (f"Q{4 + int(ch)}",)
        if cmd_key == 'READ_FLOW_ALL':  # R60
            return ("Q0",)
        return tuple()

    def _parse_q_value_with_prefixes(self, line: str | None, expected_prefixes: tuple[str, ...]) -> float | None:
        s = (line or "").strip()
        for p in expected_prefixes:
            if s.startswith(p):
                m = re.match(r'^' + re.escape(p) + r'\s*([+\-]?\d+(?:\.\d+)?)$', s)
                if m:
                    try: return float(m.group(1))
                    except: return None
                return None
        return None

    def _parse_r60_values(self, line: str | None) -> list[float] | None:
        s = (line or "").strip()
        if not s.startswith("Q0"):
            return None
        nums = re.findall(r'([+\-]?\d+(?:\.\d+)?)', s[2:])
        try: return [float(x) for x in nums]
        except: return None

    # ---------- 비동기 검증/읽기 ----------
    def _verify_flow_set_async(self, ch: int, target_value: float,
                               attempt: int = 1, max_attempts: int = 5, delay_ms: int = MFC_DELAY_MS):
        cmd = MFC_COMMANDS['READ_FLOW_SET'](channel=ch)
        expected = self._q_prefixes_for('READ_FLOW_SET', ch)

        def on_reply(line: Optional[str], ch=ch, target_value=target_value, attempt=attempt):
            s = (line or "").strip()
            val = self._parse_q_value_with_prefixes(s, expected)
            ok = (val is not None) and (abs(val - target_value) < 0.1)

            if ok:
                self.last_setpoints[ch] = target_value
                self.status_message.emit("MFC < 확인", f"Ch{ch} 목표 {target_value:.2f} 설정 완료.")
                self.command_confirmed.emit("FLOW_SET")
            else:
                if attempt < max_attempts:
                    # 재전송 → 재확인
                    self.enqueue(MFC_COMMANDS['FLOW_SET'](channel=ch, value=target_value),
                                 lambda _l: None, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                                 tag=f"[RE-SET ch{ch}]", allow_no_reply=True)
                    self.status_message.emit("MFC", f"[FLOW_SET 검증 재시도] ch{ch} 응답={repr(s)} (시도 {attempt}/{max_attempts})")
                    QTimer.singleShot(delay_ms, lambda: self._verify_flow_set_async(ch, target_value, attempt+1, max_attempts, delay_ms))
                else:
                    self.command_failed.emit("FLOW_SET", f"Ch{ch} FLOW_SET 확인 실패")

        self.enqueue(cmd, on_reply, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag=f"[VERIFY SET ch{ch}]")

    def _apply_flow_onoff_with_L0(self, ch: int, turn_on: bool):
        def after_r69(line: Optional[str], ch=ch, turn_on=turn_on):
            now_bits = self._parse_r69_bits((line or "").strip()) or "0000"
            bits = list(now_bits.ljust(4, '0'))
            if 1 <= ch <= len(bits):
                bits[ch-1] = '1' if turn_on else '0'
            target = ''.join(bits[:4])

            # OFF이면 안정화 취소 & 카운터 리셋
            if (not turn_on) and (self._stabilizing_channel == ch):
                if self.stabilization_timer and self.stabilization_timer.isActive():
                    self.stabilization_timer.stop()
                self._stabilizing_channel = None
                self._stabilizing_target = 0.0
                self.last_setpoints[ch] = 0.0
                self.flow_error_counters[ch] = 0
                self.status_message.emit("MFC", f"FLOW_OFF 요청: ch{ch} 안정화 취소")

            self._set_onoff_mask_and_verify(
                bits_target=target,
                start_stab_for_ch=(ch if turn_on else None),
                confirm_cmd=("FLOW_ON" if turn_on else "FLOW_OFF"),
            )

        self.enqueue(
            MFC_COMMANDS['READ_MFC_ON_OFF_STATUS'],
            after_r69, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag="[READ R69]"
        )

    def _set_onoff_mask_and_verify(self, bits_target: str,
                                   attempt: int = 1, max_attempts: int = 2, delay_ms: int = MFC_DELAY_MS,
                                   start_stab_for_ch: int | None = None, confirm_cmd: str | None = None):
        # 1) L0 적용
        self.enqueue(MFC_COMMANDS['SET_ONOFF_MASK'](bits_target),
                     lambda _l: None, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                     tag=f"[L0 {bits_target}]", allow_no_reply=True)

        # 2) 검증 & (필요 시) 안정화
        def make_verify(cur_attempt: int):
            def _verify(line: Optional[str]):
                now = self._parse_r69_bits((line or "").strip())
                if now == bits_target:
                    self.status_message.emit("MFC < 확인", f"L0 적용 확인: {bits_target}")

                    started_stab = False
                    if start_stab_for_ch:
                        ch = start_stab_for_ch
                        if 1 <= ch <= len(bits_target) and bits_target[ch-1] == '1':
                            if self.stabilization_timer and self.stabilization_timer.isActive():
                                self.stabilization_timer.stop()
                            self._stabilizing_channel = ch
                            self._stabilizing_target  = float(self.last_setpoints.get(ch, 0.0))
                            if (self._stabilizing_target > 0) and self.stabilization_timer:
                                self.stabilization_attempts = 0
                                self.stabilization_timer.start()
                                started_stab = True
                    if (not started_stab) and confirm_cmd:
                        self.command_confirmed.emit(confirm_cmd)
                    return

                if cur_attempt < max_attempts:
                    # 재적용 후 재확인
                    self.enqueue(MFC_COMMANDS['SET_ONOFF_MASK'](bits_target),
                                 lambda _l: None, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                                 tag=f"[RE-L0 {bits_target}]", allow_no_reply=True)
                    QTimer.singleShot(delay_ms, lambda: self.enqueue(
                        MFC_COMMANDS['READ_MFC_ON_OFF_STATUS'], make_verify(cur_attempt+1),
                        timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag="[VERIFY R69]"
                    ))
                else:
                    self.command_failed.emit("FLOW_ONOFF_BATCH", f"L0 적용 불일치(now={now}, want={bits_target})")
            return _verify

        self.enqueue(MFC_COMMANDS['READ_MFC_ON_OFF_STATUS'], make_verify(attempt),
                     timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag="[VERIFY R69]")

    def _check_valve_position_async(self, origin_cmd: str,
                                    attempt: int = 1, max_attempts: int = 5,
                                    delay_ms: int = MFC_DELAY_MS,
                                    resend_on_attempts: tuple = (2, 4),
                                    resend_wait_ms: int = MFC_DELAY_MS_VALVE):
        cmd = MFC_COMMANDS['READ_VALVE_POSITION']

        def on_reply(line: Optional[str], attempt=attempt):
            s = (line or "").strip()
            ok = False
            try:
                m = re.match(r'^(?:V\s*)?\+?([+\-]?\d+(?:\.\d+)?)$', s)
                pos = float(m.group(1)) if m else None
                ok = (pos is not None) and (
                    (origin_cmd == "VALVE_CLOSE" and pos < 1.0) or
                    (origin_cmd == "VALVE_OPEN"  and pos > 99.0)
                )
            except Exception:
                ok = False

            if ok:
                self.status_message.emit("MFC < 확인", f"{origin_cmd} 완료.")
                self.command_confirmed.emit(origin_cmd)
            else:
                if attempt < max_attempts:
                    if attempt in resend_on_attempts:
                        self.enqueue(MFC_COMMANDS[origin_cmd], lambda _l: None,
                                     timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                                     tag=f"[RE-{origin_cmd}]", allow_no_reply=True)
                        wait = max(delay_ms, resend_wait_ms)
                    else:
                        wait = delay_ms
                    self.status_message.emit("MFC", f"[{origin_cmd} 재확인] 응답={repr(s)} (시도 {attempt}/{max_attempts})")
                    QTimer.singleShot(wait, lambda: self._check_valve_position_async(
                        origin_cmd, attempt+1, max_attempts, delay_ms, resend_on_attempts, resend_wait_ms))
                else:
                    self.command_failed.emit(origin_cmd, "밸브 위치 확인 실패")

        self.enqueue(cmd, on_reply, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS,
                     tag=f"[VERIFY VALVE {origin_cmd}]")

    def _read_flow_all_async(self, on_done=None, tag: str = "[POLL R60]", attempt: int = 1):
        def on_reply(line: Optional[str], attempt=attempt):
            s = (line or "").strip()
            vals = self._parse_r60_values(s)
            if not vals:
                if attempt < 2:
                    QTimer.singleShot(MFC_DELAY_MS, lambda: self._read_flow_all_async(on_done, tag, attempt+1))
                else:
                    if on_done: self._safe_callback(on_done, None)
                return

            for ch, name in ((1, "Ar"), (2, "O2")):
                idx = ch - 1
                if idx < len(vals):
                    v = float(vals[idx])  # 스케일 변환 없음
                    self.update_flow.emit(name, v)
                    self._monitor_flow(ch, v)  # 모니터링은 장비단위 v로

            if on_done:
                self._safe_callback(on_done, vals)

        self.enqueue(MFC_COMMANDS['READ_FLOW_ALL'], on_reply,
                     timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag=tag)

    def _read_pressure_async(self, tag: str = ""):
        cmd = MFC_COMMANDS['READ_PRESSURE']
        def on_reply(line: Optional[str]):
            val_hw = self._parse_pressure_value((line or "").strip())
            if val_hw is None:
                self.command_failed.emit("READ_PRESSURE", "응답 없음/파싱 실패")
                return
            ui_val = self._to_ui_pressure(val_hw)
            fmt = "{:." + str(int(MFC_PRESSURE_DECIMALS)) + "f}"
            self.update_pressure.emit(fmt.format(ui_val))
        self.enqueue(cmd, on_reply, timeout_ms=MFC_TIMEOUT, gap_ms=MFC_GAP_MS, tag=tag or "[READ_PRESSURE]")    

    # ---------- 안정화/모니터링 ----------
    def _check_flow_stabilization(self):
        ch = self._stabilizing_channel
        target = float(self._stabilizing_target)
        if ch is None or target <= 0:
            if self.stabilization_timer:
                self.stabilization_timer.stop()
            self._stabilizing_channel = None
            self._stabilizing_target = 0.0
            self.command_failed.emit("FLOW_ON", "안정화 대상 없음")
            return

        tol = target * FLOW_ERROR_TOLERANCE

        def _finish(actual: float | None):
            self.stabilization_attempts += 1
            self.status_message.emit("MFC", f"유량 확인... (목표 {target:.2f}, 현재 {(-1 if actual is None else actual):.2f})")

            if (actual is not None) and (abs(actual - target) <= tol):
                if self.stabilization_timer:
                    self.stabilization_timer.stop()
                self.command_confirmed.emit("FLOW_ON")
                self._stabilizing_channel = None
                self._stabilizing_target = 0.0
                return

            if self.stabilization_attempts >= 30:
                if self.stabilization_timer:
                    self.stabilization_timer.stop()
                self.command_failed.emit("FLOW_ON", "유량 안정화 시간 초과")
                self._stabilizing_channel = None
                self._stabilizing_target = 0.0

        def _after_all(vals):
            if vals and (ch - 1) < len(vals):
                _finish(vals[ch - 1])
            else:
                _finish(None)

        self._read_flow_all_async(on_done=_after_all, tag=f"[STAB R60 ch{ch}]")

    def _monitor_flow(self, channel: int, actual_flow: float):
        # 🔸 Shutter Delay / Process Time 구간이 아니면 에러 체크 안 함
        if not getattr(self, "_flow_monitoring_enabled", False):
            # 가스 ON/안정화 구간에서 쌓인 카운터는 의미 없으니 리셋
            if channel in self.flow_error_counters:
                self.flow_error_counters[channel] = 0
            return

        # 0) 이번 공정에서 사용하지 않는 채널이면 모니터링하지 않음
        actives = getattr(self, "_active_channels", None)
        if actives and (channel not in actives):
            # 혹시 이전 공정 세트포인트가 남아 있으면 정리
            self.last_setpoints[channel] = 0.0
            self.flow_error_counters[channel] = 0
            return

        target_flow = self.last_setpoints.get(channel, 0.0)
        if target_flow < 0.1:
            # 거의 0에 가까운 세트포인트는 모니터링 대상 아님
            self.flow_error_counters[channel] = 0
            return

        tol = abs(target_flow) * FLOW_ERROR_TOLERANCE
        if abs(actual_flow - target_flow) > tol:
            # 허용 범위 밖 → 연속 카운트 증가
            self.flow_error_counters[channel] += 1
            if self.flow_error_counters[channel] >= FLOW_ERROR_MAX_COUNT:
                msg = (
                    f"Ch{channel} 유량이 설정값에서 5% 이상 벗어났습니다. "
                    f"(목표: {target_flow:.2f}, 현재: {actual_flow:.2f})"
                )
                # UI 로그 + 프로세스 중단 신호
                self.status_message.emit("MFC(오류)", msg)
                self.command_failed.emit("FLOW_MON", msg)
                # 다시 처음부터 연속 카운트
                self.flow_error_counters[channel] = 0
        else:
            # 허용 범위 안으로 돌아오면 연속 카운트 리셋
            if self.flow_error_counters[channel]:
                self.flow_error_counters[channel] = 0

    def _monitor_pressure(self, actual_pressure_ui: float) -> None:
        # 🔸 Shutter Delay / Process Time 구간이 아니면 압력 에러 체크 안 함
        if not getattr(self, "_pressure_monitoring_enabled", False):
            self.pressure_error_count = 0
            return

        target = float(getattr(self, "last_pressure_setpoint", 0.0) or 0.0)
        if target <= 0.0:
            # 설정값이 없으면 카운터 리셋
            self.pressure_error_count = 0
            return

        tol = abs(target) * FLOW_ERROR_TOLERANCE
        if abs(actual_pressure_ui - target) > tol:
            # 허용 범위를 벗어남 → 연속 카운트 증가
            self.pressure_error_count += 1
            if self.pressure_error_count >= FLOW_ERROR_MAX_COUNT:
                msg = (
                    "압력이 설정값에서 5% 이상 벗어났습니다. "
                    f"(목표: {target:.3f}, 현재: {actual_pressure_ui:.3f})"
                )
                self.status_message.emit("MFC(오류)", msg)
                self.command_failed.emit("PRESS_MON", msg)
                self.pressure_error_count = 0
        else:
            # 허용 범위 안 → 연속 카운트 리셋
            if self.pressure_error_count:
                self.pressure_error_count = 0

    # ---------- 보조 ----------
    def _parse_r69_bits(self, resp: str) -> str:
        s = (resp or "").strip()
        if s.startswith("L0"): payload = s[2:]
        elif s.startswith("L"): payload = s[1:]
        else: payload = s
        bits = "".join(ch for ch in payload if ch in "01")
        return bits[:4]

    def _parse_pressure_value(self, line: str | None) -> float | None:
        s = (line or "").strip()
        if not s:
            return None
        # 1) '+' 뒤의 숫자 우선
        m = re.search(r'\+\s*([+\-]?\d+(?:\.\d+)?)', s)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None
        # 2) 폴백: 문자열 내 마지막 숫자
        nums = re.findall(r'([+\-]?\d+(?:\.\d+)?)', s)
        if not nums:
            return None
        try:
            return float(nums[-1])
        except Exception:
            return None
        
    def _to_hw_pressure(self, ui_val: float) -> float:  
        return float(ui_val) * float(MFC_PRESSURE_SCALE)
    
    def _to_ui_pressure(self, hw_val: float) -> float:
        return float(hw_val) / float(MFC_PRESSURE_SCALE)


    def _safe_callback(self, callback: Callable, *args):
        try:
            callback(*args)
        except Exception as e:
            self.status_message.emit("MFC", traceback.format_exc())
            self.status_message.emit("MFC", f"콜백 오류: {e}")
