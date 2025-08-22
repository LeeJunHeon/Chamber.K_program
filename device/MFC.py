# MFC.py — PyQt6 QSerialPort + 비동기 명령 큐 (스레드-로컬 타이머 적용)
from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Optional, Callable, Deque, Dict

from PyQt6.QtCore import QObject, QTimer, QIODeviceBase, pyqtSignal as Signal, pyqtSlot as Slot, Qt, QThread, QMetaObject
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo

from lib.config import (
    MFC_PORT, MFC_BAUDRATE, MFC_COMMANDS,
    FLOW_ERROR_TOLERANCE, FLOW_ERROR_MAX_COUNT,
)

# ---- 내부 명령 큐 엔트리 ----
@dataclass
class Command:
    cmd_str: str
    callback: Callable[[Optional[str]], None]
    timeout_ms: int = 1500
    gap_ms: int = 1000
    tag: str = ""
    retries_left: int = 3
    allow_no_reply: bool = False

class MFCController(QObject):
    # --- 시그널 ---
    status_message = Signal(str, str)
    update_flow = Signal(str)             # "Ar: +12.3" / "O2: +1.2"
    update_pressure = Signal(str)         # 장비 원문 문자열
    command_requested = Signal(str, dict) # (호환 유지용)
    command_failed = Signal(str)
    command_confirmed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        # 시리얼
        self.serial_mfc = QSerialPort(self)
        self.serial_mfc.setBaudRate(MFC_BAUDRATE)
        self.serial_mfc.setDataBits(QSerialPort.DataBits.Data8)
        self.serial_mfc.setParity(QSerialPort.Parity.NoParity)
        self.serial_mfc.setStopBits(QSerialPort.StopBits.OneStop)
        self.serial_mfc.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
        self.serial_mfc.readyRead.connect(self._on_ready_read)
        self.serial_mfc.errorOccurred.connect(self._on_serial_error)

        # RX 라인 파서 버퍼
        self._rx = bytearray()
        self._RX_MAX = 16 * 1024
        self._LINE_MAX = 512

        # 명령 큐/상태
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # --- 타이머는 스레드-로컬에서 생성 (여기서는 None) ---
        self._cmd_timer: Optional[QTimer] = None
        self._gap_timer: Optional[QTimer] = None
        self.polling_timer: Optional[QTimer] = None

        # 폴링 상태
        self._is_running = False
        self._polling_enabled = False
        self._poll_interval_ms = 1000  # 기본 1초

        # 상태 추적
        self.last_setpoints: Dict[int, float] = {1: 0.0, 2: 0.0}
        self.flow_error_counters: Dict[int, int] = {1: 0, 2: 0}
        self._stabilize_attempts_left: Dict[int, int] = {1: 0, 2: 0}

        self._reconnect_timer: Optional[QTimer] = None
        self._recon_backoff_ms = 500
        self._recon_backoff_max = 4000

    # ================== 스레드-로컬 타이머 생성 ==================
    @Slot()
    def _setup_timers(self):
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
            self.polling_timer.setInterval(self._poll_interval_ms)
            self.polling_timer.setTimerType(Qt.TimerType.PreciseTimer)
            self.polling_timer.timeout.connect(self._enqueue_poll_cycle)

        if self._reconnect_timer is None:
            self._reconnect_timer = QTimer(self)
            self._reconnect_timer.setSingleShot(True)
            self._reconnect_timer.timeout.connect(self._try_reconnect)

    def _ensure_timers(self):
        """컨트롤러가 속한 스레드에서 타이머들을 반드시 생성한다."""
        if self._cmd_timer and self._gap_timer and self.polling_timer:
            return
        if self.thread() is not QThread.currentThread():
            QMetaObject.invokeMethod(self, "_setup_timers", Qt.ConnectionType.BlockingQueuedConnection)
        else:
            self._setup_timers()

    # ================== 연결/해제 ==================
    def connect_mfc_device(self) -> bool:
        if self.serial_mfc.isOpen():
            return True
        ports = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if MFC_PORT not in ports:
            self.status_message.emit("MFC", f"{MFC_PORT} 없음. 사용 가능 포트: {sorted(ports)}")
            return False
        self.serial_mfc.setPortName(MFC_PORT)
        if not self.serial_mfc.open(QIODeviceBase.OpenModeFlag.ReadWrite):
            self.status_message.emit("MFC", f"연결 실패: {self.serial_mfc.errorString()}")
            return False

        self.serial_mfc.clear(QSerialPort.Direction.AllDirections)
        self._rx.clear()
        self._ensure_timers()  
        self.status_message.emit("MFC", "연결 성공, 입출력 버퍼 초기화 완료.")
        return True
    
    def _force_reconnect(self, reason: str, requeue_cmd: Optional[Command] = None):
        try:
            self.status_message.emit("MFC", f"재연결 트리거: {reason}")
        except Exception:
            pass

        if requeue_cmd is not None:
            requeue_cmd.retries_left = 3     # ★ 복구 후 다시 3회 기회 부여
            self._cmd_q.appendleft(requeue_cmd)

        try:
            if self.serial_mfc.isOpen():
                self.serial_mfc.close()
        except Exception:
            pass

        if self._reconnect_timer:
            self._reconnect_timer.start(0)

    def _try_reconnect(self):
        ok = self.connect_mfc_device()
        if ok:
            self._recon_backoff_ms = 500
            if self._gap_timer and not self._gap_timer.isActive():
                self._gap_timer.start(0)
            self.status_message.emit("MFC", "재연결 성공")
        else:
            self._recon_backoff_ms = min(self._recon_backoff_ms * 2, self._recon_backoff_max)
            if self._reconnect_timer:
                self._reconnect_timer.start(self._recon_backoff_ms)
            self.status_message.emit("MFC", f"재연결 대기… {self._recon_backoff_ms} ms")


    # ================== 폴링 제어 ==================
    @Slot()
    def start_polling(self):
        """(선택) 외부에서 폴링 기동"""
        self._ensure_timers()  
        self._is_running = True
        self._polling_enabled = True
        if self.polling_timer and not self.polling_timer.isActive():
            self.polling_timer.start()
        self.status_message.emit("MFC", "주기적 읽기(Polling) 활성화")

    def stop(self):
        self._is_running = False
        self._polling_enabled = False
        if self.polling_timer and self.polling_timer.isActive():
            self.polling_timer.stop()

    @Slot()
    def cleanup(self):
        """종료 시 안전 정리."""
        self.stop()
        if self._cmd_timer: self._cmd_timer.stop()
        if self._gap_timer: self._gap_timer.stop()
        # 진행 중 콜백 취소 통지
        if self._inflight:
            cb = self._inflight.callback
            self._inflight = None
            try: cb(None)
            except Exception: pass
        while self._cmd_q:
            cmd = self._cmd_q.popleft()
            try: cmd.callback(None)
            except Exception: pass
        if self.serial_mfc.isOpen():
            self.serial_mfc.close()
            self.status_message.emit("MFC", "시리얼 포트를 안전하게 닫았습니다.")

    # ================== QSerialPort 이벤트 ==================
    def _on_serial_error(self, err):
        if err == QSerialPort.SerialPortError.NoError:
            return
        self.status_message.emit("MFC(에러)", f"시리얼 오류: {self.serial_mfc.errorString()}")

        if self._inflight:
            cmd = self._inflight
            self._inflight = None
            if self._cmd_timer: self._cmd_timer.stop()
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._force_reconnect("시리얼 오류(재시도 소진)", requeue_cmd=cmd)   # ★
                return

        try:
            if self.serial_mfc.isOpen():
                self.serial_mfc.close()
        except Exception:
            pass
        if self._reconnect_timer:
            self._reconnect_timer.start(0)

    def _on_ready_read(self):
        ba = self.serial_mfc.readAll()
        if ba.isEmpty():
            return
        self._rx.extend(bytes(ba))
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]

        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line = self._rx[:idx]
            drop = idx + 1
            if drop < len(self._rx):
                ch = self._rx[idx]
                nxt = self._rx[idx + 1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]

            if len(line) > self._LINE_MAX:
                line = line[:self._LINE_MAX]
            try:
                s = line.decode('ascii', errors='ignore').strip()
            except Exception:
                s = ""
            if not s:
                continue
            if self._inflight and s == (self._inflight.cmd_str.strip()):
                continue  # 에코 스킵
            self._finish_command(s)
            break

        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    # ================== 명령 큐 ==================
    def enqueue(self, cmd_str: str, callback: Callable[[Optional[str]], None],
                *, timeout_ms: int = 1500, gap_ms: int = 1000,
                tag: str = "", retries_left: int = 3,
                allow_no_reply: bool = False):
        self._ensure_timers()  
        if not cmd_str.endswith('\r'):
            cmd_str += '\r'
        self._cmd_q.append(Command(cmd_str, callback, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))
        if self._inflight is None:
            if self._gap_timer and not self._gap_timer.isActive():
                self._gap_timer.start(0)
            else:
                # 초기 레이스(타이머 아직 없음) 대비
                QTimer.singleShot(0, self._dequeue_and_send)

    def _dequeue_and_send(self):
        if self._inflight is not None or not self._cmd_q:
            return
        if not self.serial_mfc.isOpen():
            return

        cmd = self._cmd_q.popleft()
        self._inflight = cmd
        self._rx.clear()

        n = self.serial_mfc.write(cmd.cmd_str.encode('ascii'))
        if int(n) <= 0:
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
                if self._gap_timer: self._gap_timer.start(max(50, cmd.gap_ms))
            else:
                self._force_reconnect("전송 실패(재시도 소진)", requeue_cmd=cmd)   # ★ 추가
            return


        self.serial_mfc.flush()

        # 무응답 허용 명령은 즉시 성공 처리(타임아웃 대기 X)
        if cmd.allow_no_reply:
            self._finish_command(None)
            return

        if self._cmd_timer:
            self._cmd_timer.stop()
            self._cmd_timer.start(cmd.timeout_ms)

        self.status_message.emit("MFC > 전송", f"{cmd.tag or ''} {cmd.cmd_str.strip()}".strip())

    def _on_cmd_timeout(self):
        self._finish_command(None)

    def _finish_command(self, line: Optional[str]):
        if self._inflight is None:
            return

        cmd = self._inflight
        self._inflight = None

        if self._cmd_timer:
            self._cmd_timer.stop()

        # --- 응답 없음(타임아웃/무응답) ---
        if line is None:
            # 0) 진짜 '확인 응답이 없는' 명령(ZEROING 류)은 성공 처리
            if cmd.allow_no_reply:
                try:
                    cmd.callback(None)
                finally:
                    if self._gap_timer:
                        self._gap_timer.start(cmd.gap_ms)
                return

            # 1) 재시도 남음 → 같은 명령 재전송(큐 맨 앞) + 최소 간격 보장
            try:
                self.status_message.emit("MFC-DBG", f"[TIMEOUT] {cmd.tag or cmd.cmd_str.strip()}")
            except Exception:
                pass

            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
                try:
                    self.status_message.emit("MFC-DBG",
                        f"[RETRY] {cmd.tag or cmd.cmd_str.strip()} (남은 {cmd.retries_left})")
                except Exception:
                    pass
                if self._gap_timer:
                    self._gap_timer.start(max(50, cmd.gap_ms))  # 과도 재전송 방지
                return

            # 2) 재시도 소진 → 재연결 승급(같은 명령은 복구 후 다시 3회 시도할 수 있게 리큐)
            self._force_reconnect("응답 없음(재시도 초과)", requeue_cmd=cmd)
            return

        # --- 정상 응답 ---
        try:
            cmd.callback(line.strip())
        finally:
            if self._gap_timer:
                self._gap_timer.start(cmd.gap_ms)

    # ================== 폴링 사이클 ==================
    def _enqueue_poll_cycle(self):
        if not self._polling_enabled:
            return
        
        # 이미 진행/대기 중이면 이번 사이클은 스킵 (누적 방지)
        if (self._has_pending('[POLL FLOW1]') or
            self._has_pending('[POLL FLOW2]') or
            self._has_pending('[POLL PRESS]')):
            return

        # 1) CH1 유량
        def on_flow1(resp: Optional[str]):
            if resp:
                self.update_flow.emit(f"Ar: {resp}")
                self._monitor_flow(1, resp)
        self.enqueue(self._build_cmd('READ_FLOW', {'channel': 1}),
                     on_flow1, tag='[POLL FLOW1]', gap_ms=200)

        # 2) CH2 유량
        def on_flow2(resp: Optional[str]):
            if resp:
                self.update_flow.emit(f"O2: {resp}")
                self._monitor_flow(2, resp)
        self.enqueue(self._build_cmd('READ_FLOW', {'channel': 2}),
                     on_flow2, tag='[POLL FLOW2]', gap_ms=200)

        # 3) 압력
        def on_press(resp: Optional[str]):
            if resp:
                self.update_pressure.emit(resp)
        self.enqueue(self._build_cmd('READ_PRESSURE', None),
                     on_press, tag='[POLL PRESS]', gap_ms=200)
        
    def _has_pending(self, tag: str) -> bool:
        if self._inflight and self._inflight.tag == tag:
            return True
        return any(c.tag == tag for c in self._cmd_q)


    # ================== 외부 명령 처리 ==================
    @Slot(str, dict)
    def handle_command(self, cmd: str, params: dict):
        if not self.serial_mfc.isOpen():
            self.command_failed.emit("MFC 시리얼 연결 없음")
            self.status_message.emit("MFC(에러)", "시리얼 연결 없음")
            return

        # 내부 제어
        if cmd == "set_polling":
            enable = params.get("enable", False)
            self._polling_enabled = enable
            # Process에서 set_polling만 쓰더라도 동작하도록 보장
            self._is_running = enable
            if enable and self.polling_timer and not self.polling_timer.isActive():
                self.polling_timer.start()
            elif not enable and self.polling_timer and self.polling_timer.isActive():
                self.polling_timer.stop()
            self.status_message.emit("MFC", f"주기적 읽기(Polling) {'활성화' if enable else '비활성화'}")
            if not enable:
                self.flow_error_counters = {1: 0, 2: 0}
            return

        # 실제 하드웨어 제어
        self._verify_and_execute(cmd, params)

    # --- 검증형 실행(비동기) ---
    def _verify_and_execute(self, cmd: str, params: dict):
        make = lambda k, p=None: self._build_cmd(k, p)

        # 1) FLOW_SET
        if cmd == "FLOW_SET":
            channel = int(params['channel'])
            sent_value = float(params['value'])
            self.last_setpoints[channel] = sent_value

            def on_set(_ack: Optional[str]):
                def on_read_set(resp: Optional[str]):
                    ok = False
                    try:
                        if resp and '+' in resp:
                            ok = abs(float(resp.split('+')[1]) - sent_value) < 0.1
                    except Exception:
                        ok = False
                    if ok:
                        self.command_confirmed.emit(cmd)
                        self.status_message.emit("MFC < 확인", f"Ch{channel} 목표 {sent_value:.1f} sccm 설정 완료")
                    else:
                        self.command_failed.emit(cmd)
                        self.status_message.emit("MFC(경고)", f"Ch{channel} FLOW_SET 검증 실패")
                self.enqueue(make('READ_FLOW_SET', {'channel': channel}),
                             on_read_set, tag='[READ_FLOW_SET]')
            self.enqueue(make('FLOW_SET', params), on_set, tag='[FLOW_SET]')
            return

        # 2) FLOW_ON / FLOW_OFF
        if cmd in ("FLOW_ON", "FLOW_OFF"):
            channel = int(params['channel'])
            want_on = (cmd == "FLOW_ON")

            def on_set(_ack: Optional[str]):
                def on_status(resp: Optional[str]):
                    ok = False
                    try:
                        if resp and resp.startswith('L') and len(resp) >= (1 + channel + 1):
                            status_char = resp[1 + channel]
                            ok = (status_char == ('1' if want_on else '0'))
                    except Exception:
                        ok = False

                    if not ok:
                        self.command_failed.emit(cmd)
                        self.status_message.emit("MFC(경고)", f"Ch{channel} Flow {'ON' if want_on else 'OFF'} 상태 확인 실패")
                        return

                    if want_on:
                        target = self.last_setpoints.get(channel, 0.0)
                        tol = target * FLOW_ERROR_TOLERANCE
                        self._stabilize_attempts_left[channel] = 30

                        def one_check(_r=None):
                            left = self._stabilize_attempts_left[channel]
                            if left <= 0:
                                self.command_failed.emit(cmd)
                                self.status_message.emit("MFC(경고)", f"Ch{channel} 유량 안정화 실패(타임아웃)")
                                return

                            def on_flow(resp2: Optional[str]):
                                ok2 = False
                                try:
                                    if resp2 and '+' in resp2:
                                        actual = float(resp2.split('+')[1])
                                        ok2 = abs(actual - target) <= tol
                                        self.status_message.emit(
                                            "MFC",
                                            f"유량 확인... (Ch{channel} 목표:{target:.1f}, 현재:{actual:.1f}, 남은:{left-1}s)"
                                        )
                                except Exception:
                                    ok2 = False

                                if ok2:
                                    self.command_confirmed.emit(cmd)
                                    self.status_message.emit("MFC < 확인", f"Ch{channel} 유량 안정화 완료")
                                else:
                                    self._stabilize_attempts_left[channel] = left - 1
                                    # MFC 스레드에서 실행되므로 static singleShot 안전
                                    QTimer.singleShot(1000, lambda: self.enqueue(
                                        make('READ_FLOW', {'channel': channel}),
                                        on_flow, tag='[STABILIZE FLOW]'
                                    ))

                            self.enqueue(make('READ_FLOW', {'channel': channel}),
                                         on_flow, tag='[STABILIZE FLOW]')
                        one_check()
                    else:
                        self.command_confirmed.emit(cmd)
                        self.status_message.emit("MFC < 확인", f"Ch{channel} Flow OFF 확인")

                self.enqueue(make('READ_MFC_ON_OFF_STATUS', None),
                             on_status, tag='[READ_MFC_ON_OFF_STATUS]')
            self.enqueue(make(cmd, params), on_set, tag=f'[{cmd}]')
            return

        # 3) VALVE_OPEN / VALVE_CLOSE
        if cmd in ("VALVE_OPEN", "VALVE_CLOSE"):
            want_open = (cmd == "VALVE_OPEN")

            def on_cmd(_ack: Optional[str]):
                def delayed_read():
                    def on_read(resp: Optional[str]):
                        ok = False
                        try:
                            if resp and '+' in resp:
                                pos = float(resp.split('+')[1])
                                ok = (pos > 99.0) if want_open else (pos < 1.0)
                        except Exception:
                            ok = False
                        if ok:
                            self.command_confirmed.emit(cmd)
                            self.status_message.emit("MFC < 확인", f"밸브 {'열림' if want_open else '닫힘'} 확인")
                        else:
                            self.command_failed.emit(cmd)
                            self.status_message.emit("MFC(경고)", f"밸브 {'열림' if want_open else '닫힘'} 확인 실패")
                    self.enqueue(self._build_cmd('READ_VALVE_POSITION', None),
                                 on_read, tag='[READ_VALVE_POSITION]')
                # 이 호출도 MFC 스레드에서 실행 → 안전
                QTimer.singleShot(3000, delayed_read)

            self.enqueue(self._build_cmd(cmd, params), on_cmd, tag=f'[{cmd}]')
            return

        # 4) SP1_SET / SP1_ON / SP4_ON / ZEROING 류
        if cmd == "SP1_SET":
            sent_value = float(params['value'])

            def on_set(_ack: Optional[str]):
                def on_read(resp: Optional[str]):
                    ok = False
                    try:
                        if resp and '+' in resp:
                            ok = abs(float(resp.split('+')[1]) - sent_value) < 0.1
                    except Exception:
                        ok = False
                    if ok:
                        self.command_confirmed.emit(cmd)
                        self.status_message.emit("MFC < 확인", f"SP1 {sent_value:.2f} 설정 완료")
                    else:
                        self.command_failed.emit(cmd)
                        self.status_message.emit("MFC(경고)", "SP1_SET 검증 실패")
                self.enqueue(self._build_cmd('READ_SP1_VALUE', None),
                             on_read, tag='[READ_SP1_VALUE]')
            self.enqueue(self._build_cmd('SP1_SET', params), on_set, tag='[SP1_SET]')
            return

        if cmd in ("SP1_ON", "SP4_ON"):
            expected_char = '1' if cmd == "SP1_ON" else '4'
            def on_set(_ack: Optional[str]):
                def on_read(resp: Optional[str]):
                    ok = bool(resp and resp.startswith("M") and resp[1] == expected_char)
                    if ok:
                        self.command_confirmed.emit(cmd)
                        self.status_message.emit("MFC < 확인", f"{cmd} 활성화 확인")
                    else:
                        self.command_failed.emit(cmd)
                        self.status_message.emit("MFC(경고)", f"{cmd} 확인 실패")
                self.enqueue(self._build_cmd('READ_SYSTEM_STATUS', None),
                             on_read, tag='[READ_SYSTEM_STATUS]')
            self.enqueue(self._build_cmd(cmd, None), on_set, tag=f'[{cmd}]')
            return

        if cmd in ("MFC_ZEROING", "PS_ZEROING"):
            # 확인 응답 없음 → 전송 성공으로 간주
            def on_sent(_):
                self.command_confirmed.emit(cmd)
                self.status_message.emit("MFC < 확인", f"{cmd}: 확인 응답 없음, 성공 간주")
            self.enqueue(self._build_cmd(cmd, None), on_sent, allow_no_reply=True, tag=f'[{cmd}]')
            return

        self.command_failed.emit(cmd)
        self.status_message.emit("MFC(경고)", f"알 수 없는 명령: {cmd}")

    # ================== 유틸 ==================
    def _build_cmd(self, key: str, params: Optional[dict]) -> str:
        maker = MFC_COMMANDS.get(key)
        if maker is None:
            return key
        return maker(**params) if (params and callable(maker)) else (maker if isinstance(maker, str) else key)

    def _monitor_flow(self, channel: int, actual_flow_str: Optional[str]):
        target_flow = self.last_setpoints.get(channel, 0.0)
        if target_flow < 0.1:
            self.flow_error_counters[channel] = 0
            return

        if actual_flow_str and '+' in actual_flow_str:
            try:
                actual_flow = float(actual_flow_str.split('+')[1])
                tolerance = target_flow * FLOW_ERROR_TOLERANCE
                if abs(actual_flow - target_flow) > tolerance:
                    self.flow_error_counters[channel] += 1
                    if self.flow_error_counters[channel] >= FLOW_ERROR_MAX_COUNT:
                        self.status_message.emit(
                            "MFC(경고)",
                            f"Ch{channel} 유량 불안정! (목표: {target_flow:.1f}, 현재: {actual_flow:.1f})"
                        )
                        self.flow_error_counters[channel] = 0
                else:
                    self.flow_error_counters[channel] = 0
            except Exception:
                pass
