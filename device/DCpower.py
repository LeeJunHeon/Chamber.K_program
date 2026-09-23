# DCPowerController_qtserial.py — PyQt6 QSerialPort 동기형(이벤트루프 대기) 구현
from __future__ import annotations
import re
from typing import Optional, Tuple

from PyQt6.QtCore import QObject, QTimer, QThread, QEventLoop, pyqtSignal as Signal, pyqtSlot as Slot, Qt
import time
import datetime
from PyQt6.QtSerialPort import QSerialPort, QSerialPortInfo, QSerialPort as QS
from PyQt6.QtCore import QIODeviceBase

from lib.comm_policy import ReconnectPolicy
from lib.config import (
    DC_PORT, DC_BAUDRATE,
    DC_INITIAL_VOLTAGE, DC_INITIAL_CURRENT, DC_MAX_VOLTAGE,
    DC_MAX_CURRENT, DC_MAX_POWER, DC_TOLERANCE_WATT, DC_MAX_ERROR_COUNT,
    DC_MIN_CURRENT_ABORT, DC_FAIL_ISET_THRESHOLD, DC_FAIL_POWER_THRESHOLD,
    DC_FAIL_MAX_TICKS, DC_POWER_ERROR_RATIO, DC_POWER_ERROR_MAX_COUNT,
    DC_MIN_CURRENT_ABORT_COUNT,
    DC_CONTROL_GAIN, DC_RAMP_STEP_A, DC_MAINTAIN_STEP_UP_A, DC_MAINTAIN_STEP_DOWN_A,
    DC_LIMIT_STALL_SEC, DC_SMALL_ERROR_RATIO, DC_SMALL_ERROR_GAIN,
    DC_COMM_LOSS_ABORT_SEC,
    COMM_RECONNECT_START_MS, COMM_RECONNECT_MAX_MS, COMM_LONG_OUTAGE_SEC, COMM_PROBE_MS, COMM_OUTAGE_LOG_SEC,
)

# 연속 무응답 이 횟수면 포트를 닫고 백오프 재연결로 넘어간다(USB 어댑터 재열거 대응).
#  재연결은 무응답이 이어지는 동안 DC_COMM_REOPEN_SEC 마다 한 번씩만.
DC_COMM_FAIL_RECONNECT_STREAK = 3
DC_COMM_REOPEN_SEC = 3.0
# OFF 미확인 에피소드의 재시도 간격 — 실패할 때마다 2배, COMM_OUTAGE_LOG_SEC 에서 캡.
#  "_is_running=False 동안 출력 OFF" 는 언제나 맞는 상태라 공정 활성 여부와 무관하게 재시도해도 안전하다.
DC_OFF_RETRY_START_SEC = 10.0
from lib.dc_control import power_step_current, is_at_current_cap

class DCPowerController(QObject):
    update_dc_status_display = Signal(float, float, float)  # (P, V, I)
    status_message = Signal(str, str)
    target_reached = Signal()
    # ── 시리얼 장비 공통 통신 두절 정책 ──
    comm_event   = Signal(dict)    # COMM_events.csv 1행 (파일 I/O 는 main 스레드의 lib.logger)
    dc_recovered = Signal(float)   # 단절 뒤 첫 성공 (단절 초) — main 이 안전 상태 재적용 여부 판단
    comm_long_outage = Signal(str, float)   # 장기두절 진입(장치명, 단절 초) — 두절당 1회
    off_unconfirmed = Signal(str)           # OFF 미확인 에피소드 시작(where) — 에피소드당 1회
    off_confirmed   = Signal(str, float)    # 그 에피소드가 확인으로 끝남(where, 미확인 지속 초) — 1회

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

        # ── 통신 두절 정책 상태 ──
        self._comm_last_ok: float = time.monotonic()   # 마지막으로 응답을 받은 시각
        self._comm_fail_streak: int = 0                 # 연속 무응답(쿼리 실패) 수
        self._outage_abort: bool = False                # 이번 단절로 공정을 중단했는가
        self._policy = ReconnectPolicy(COMM_RECONNECT_START_MS, COMM_RECONNECT_MAX_MS, COMM_LONG_OUTAGE_SEC, COMM_PROBE_MS, COMM_OUTAGE_LOG_SEC)   # 재연결 스케줄(공통)
        self._reconnect_pending: bool = False
        self._want_connected: bool = False
        self._comm_last_fail_log_t: float = 0.0
        self._comm_last_reopen_t: float = -1e9
        # ── 출력 OFF 추적 ──
        #  "_is_running=False 동안 출력은 OFF" 가 불변식이다. OFF 를 확인하지 못했으면 기억하고 재시도한다.
        self._output_maybe_on: bool = False        # OUTP ON 을 보낸 뒤 OFF 가 확인되기 전까지 True
        self._off_unconfirmed: bool = False        # OFF 를 시도했지만 확인 못 한 에피소드(출력을 켠 적 있을 때만)
        self._off_unconfirmed_since: float = 0.0
        self._off_retry_next: float = 0.0
        self._off_retry_sec: float = DC_OFF_RETRY_START_SEC
        self._off_last_log_t: float = 0.0          # 에피소드 중 경고·이벤트 솎음(COMM_OUTAGE_LOG_SEC)
        self._io_depth: int = 0                    # _readline_blocking 중첩 깊이(재진입 판정)
        self._verify_pending: bool = False         # _verify_output_off 재예약이 걸려 있는가
        self._emg_restore = None                   # ALL STOP 이 임시로 포트를 연 경우 되돌릴 상태

        # 두절 중단 뒤에는 제어 타이머가 멈춰 쿼리가 없다 → 2초마다 가벼운 프로브로 복구를 감지한다
        self._outage_probe = QTimer(self)
        self._outage_probe.setInterval(2000)
        self._outage_probe.timeout.connect(self._outage_probe_tick)

    # ---------------- 연결 ----------------
    @Slot()
    def connect_dcpower_device(self) -> bool:
        # 포트 존재 확인
        ports = {p.portName() for p in QSerialPortInfo.availablePorts()}
        if DC_PORT not in ports:
            self.status_message.emit("DCpower", f"{DC_PORT} 포트를 찾을 수 없습니다. 사용 가능: {sorted(ports)}")
            return False

        if self.serial is not None and self.serial.isOpen():
            return True          # 이미 열려 있다 — 다시 열면 "Device is already open" 경고만 남는다
        if self.serial is None:
            self.serial = QSerialPort(self)
            self.serial.setBaudRate(DC_BAUDRATE)
            self.serial.setDataBits(QSerialPort.DataBits.Data8)
            self.serial.setParity(QSerialPort.Parity.NoParity)
            self.serial.setStopBits(QSerialPort.StopBits.OneStop)
            self.serial.setFlowControl(QSerialPort.FlowControl.NoFlowControl)
            self.serial.errorOccurred.connect(self._on_serial_error)
        self._want_connected = True

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
        self._reconnect_pending = False      # 정책의 성공(on_success)은 포트 열림이 아니라 장비 응답(_comm_ok)
        if self._comm_fail_streak == 0:
            # 첫 연결에서만 예산 시계를 맞춘다 — 단절 중 포트 재오픈은 장비 응답이 아니다
            self._comm_last_ok = time.monotonic()
        return True

    # ---------------- 통신 두절 정책 (시리얼 장비 공통) ----------------
    def _emit_event(self, kind: str, detail: str, lost=None) -> None:
        try:
            self.comm_event.emit({
                "시각": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "종류": kind, "상세": detail,
                "단절초": ("" if lost is None else f"{float(lost):.1f}"),
                "연속실패": self._comm_fail_streak,
            })
        except Exception:
            pass

    def _comm_ok(self) -> None:
        """응답을 받았다. 단절 뒤 첫 성공이면 복구 이벤트 + dc_recovered."""
        now = time.monotonic()
        if self._comm_fail_streak > 0:
            lost = now - self._comm_last_ok
            n = self._comm_fail_streak
            self._comm_fail_streak = 0
            self.status_message.emit("DCpower", f"DC 파워 통신 복구 (단절 {lost:.1f}초, 실패 {n}회)")
            self._emit_event("복구", f"실패 {n}회", lost=lost)
            self.dc_recovered.emit(float(lost))
        if self._policy.in_outage():
            self._policy.on_success()      # 장비가 실제로 응답했다 — 여기서만 리셋(포트 열림은 성공이 아니다)
        self._comm_last_ok = now

    def _comm_fail(self, why: str) -> None:
        """응답이 없었다. 연속 N회면 포트를 닫고 백오프 재연결로 넘어간다."""
        self._comm_fail_streak += 1
        now = time.monotonic()
        lost = now - self._comm_last_ok
        if (now - self._comm_last_fail_log_t) >= 1.0:
            self._comm_last_fail_log_t = now
            self._emit_event("실패", why, lost=lost)
        if (self._comm_fail_streak >= DC_COMM_FAIL_RECONNECT_STREAK and self._want_connected
                and (now - self._comm_last_reopen_t) >= DC_COMM_REOPEN_SEC):
            self._comm_last_reopen_t = now
            self.status_message.emit(
                "DCpower(경고)",
                f"연속 {self._comm_fail_streak}회 무응답(단절 {lost:.1f}s) — 포트를 닫고 재연결합니다")
            self._close_port_for_reconnect()
            self._schedule_reconnect()

    def _close_port_for_reconnect(self) -> None:
        try:
            if self.serial and self.serial.isOpen():
                self.serial.close()
        except Exception:
            pass
        self._rx.clear()

    def _schedule_reconnect(self) -> None:
        if self._reconnect_pending or not self._want_connected:
            return
        self._reconnect_pending = True
        ms = self._policy.on_failure()
        if self._policy.take_long_outage_alert():
            lost = self._policy.outage_sec()
            self.status_message.emit("DCpower(경고)", f"DC 파워 통신 장기 두절 {lost:.0f}초 — {ms} ms 간격으로만 재시도")
            self._emit_event("장기두절", f"{lost:.0f}초", lost=lost)
            self.comm_long_outage.emit("DC", float(lost))
        if self._policy.should_log():
            self.status_message.emit("DCpower", f"재연결 시도... ({ms} ms)")
            self._emit_event("재연결시도", f"{ms} ms 뒤")
        QTimer.singleShot(ms, self._try_reconnect)

    def _try_reconnect(self) -> None:
        self._reconnect_pending = False
        if not self._want_connected or (self.serial and self.serial.isOpen()):
            return
        if self.connect_dcpower_device():
            self.status_message.emit("DCpower", "재연결 성공")
            self._emit_event("재연결", DC_PORT)
        else:
            self._schedule_reconnect()

    def _on_serial_error(self, err) -> None:
        if err == QSerialPort.SerialPortError.NoError:
            return
        serr = self.serial.errorString() if self.serial else ""
        self.status_message.emit("DCpower(경고)", f"시리얼 오류: {serr} (err={err})")
        self._emit_event("시리얼오류", f"{serr} (err={err})")
        if err == QSerialPort.SerialPortError.ResourceError or not (self.serial and self.serial.isOpen()):
            self._close_port_for_reconnect()
            self._schedule_reconnect()

    def _check_comm_budget(self) -> None:
        """공정 중 DC_COMM_LOSS_ABORT_SEC 이상 응답이 없으면 1회만 "재시작"."""
        if self._outage_abort or not self._is_running:
            return
        lost = time.monotonic() - self._comm_last_ok
        if lost >= float(DC_COMM_LOSS_ABORT_SEC):
            self._outage_abort = True
            self.status_message.emit(
                "재시작",
                f"DC 파워 통신 두절 {lost:.1f}초 (연속 실패 {self._comm_fail_streak}회) → 공정을 중단합니다. "
                f"DC 출력은 PC 가 끌 수 없으므로 장비 전면에서 OFF 를 확인하세요.")
            self._emit_event("두절중단", f"연속 실패 {self._comm_fail_streak}회", lost=lost)
            if not self._outage_probe.isActive():
                self._outage_probe.start()

    def _outage_probe_tick(self) -> None:
        """두절 중단(_outage_abort) 또는 OFF 미확인(_off_unconfirmed) 동안 돈다.
        전자는 복구 감지(main 이 safe_off 를 부른다), 후자는 드라이버가 직접 OFF 를 재시도한다."""
        if not (self._outage_abort or self._off_unconfirmed):
            self._outage_probe.stop()
            return
        if self._is_running or self._io_depth > 0:
            return          # 공정이 다시 돌면 제어 루프의 쿼리가 대신한다 / 읽기 중이면 다음 틱에
        if not (self.serial and self.serial.isOpen()):
            self._schedule_reconnect()
            return
        if self._outage_abort:
            self._query("MEAS:VOLT?", timeout_ms=800)
            return
        if time.monotonic() >= self._off_retry_next:
            self._output_off_confirmed("OFF 재시도")

    # ---------------- 출력 OFF (쓰기 / 확인 분리) ----------------
    def _send_output_off(self, where: str) -> bool:
        """"OUTP OFF" 쓰기만 한다(읽기 없음 — 중첩 중에도 안전)."""
        try:
            if not (self.serial and self.serial.isOpen()):
                return False
            self.status_message.emit("DCpower > 전송", "OUTP OFF")
            ok = self._write_line("OUTP OFF")
            self.serial.waitForBytesWritten(200)
            return bool(ok)
        except Exception:
            return False

    def _verify_output_off(self, where: str):
        """OUTP? 로 확인한다. True=확인 / False=미확인 / None=확인 보류(읽기 중이라 뒤로 미룸).
        매뉴얼 7-8절: OUTP? 는 "0"=출력 차단, "1"=출력 허용."""
        if self._io_depth > 0:
            # 중첩 읽기 중에 또 _query 를 하면 두 on_ready 가 같은 readyRead 를 나눠 갖는다 — 읽기가 끝난 뒤로 미룬다
            if not self._verify_pending:
                self._verify_pending = True
                QTimer.singleShot(50, lambda: self._verify_retry(where))
            return None
        if self._is_running:
            return None          # 그 사이 새 공정이 시작됐다 — 출력의 주인이 바뀌었으니 건드리지 않는다
        ans = self._query("OUTP?", timeout_ms=800)
        if ans is not None and ans.strip() in ("0", "OFF"):
            self._mark_off_confirmed(where)
            return True
        # 무응답·"1"·기타 → 한 번 더 쓰고 다시 확인
        time.sleep(0.3)
        if self._is_running:
            return None
        self._send_output_off(where)
        ans2 = self._query("OUTP?", timeout_ms=800)
        if ans2 is not None and ans2.strip() in ("0", "OFF"):
            self._mark_off_confirmed(where)
            return True
        self._mark_off_unconfirmed(where, f"OUTP? 응답 {ans!r}/{ans2!r}")
        return False

    def _verify_retry(self, where: str) -> None:
        """중첩 때문에 미뤘던 확인을 다시 시도한다(읽기가 끝날 때까지 50ms 간격)."""
        self._verify_pending = False
        if self._is_running:
            return
        r = self._verify_output_off(where)
        if r is not None:
            self._emg_restore_if_needed()

    def _output_off_confirmed(self, where: str):
        """OUTP OFF 쓰기 + 확인. True=확인 / False=미확인 / None=확인 보류(읽기 중)."""
        if not self._send_output_off(where):
            self._mark_off_unconfirmed(where, "전송 실패")
            return False
        return self._verify_output_off(where)

    def _mark_off_confirmed(self, where: str) -> None:
        """OFF 확인 — 에피소드가 열려 있었으면 닫고 1회 알린다."""
        self._output_maybe_on = False
        if self._off_unconfirmed:
            sec = time.monotonic() - self._off_unconfirmed_since
            self._off_unconfirmed = False
            self._off_retry_sec = DC_OFF_RETRY_START_SEC
            self._off_retry_next = 0.0
            self._off_last_log_t = 0.0
            self.status_message.emit("DCpower", f"DC 출력 OFF 확인(OUTP?=0, {where})")
            self._emit_event("OFF확인", f"{where}: 미확인 {sec:.0f}초 뒤")
            self.off_confirmed.emit(where, float(sec))
        else:
            self.status_message.emit("DCpower", f"DC 출력 OFF 확인(OUTP?=0, {where})")
        if not self._outage_abort and self._outage_probe.isActive():
            self._outage_probe.stop()

    def _mark_off_unconfirmed(self, where: str, detail: str) -> None:
        """OFF 미확인. 출력을 켠 적이 없으면 에피소드가 아니라 정보 1줄로 끝낸다
        (그날 첫 공정의 "PRE: DC Power OFF" 가 포트 미연결로 내던 거짓 경고가 여기에 해당한다)."""
        if not self._output_maybe_on:
            self.status_message.emit(
                "DCpower", f"DC OFF 생략/미확인 ({where}: {detail}) — 이번 실행에서 DC 출력을 켠 적 없음")
            return
        now = time.monotonic()
        first = not self._off_unconfirmed
        if first:
            self._off_unconfirmed = True
            self._off_unconfirmed_since = now
            self._off_retry_sec = DC_OFF_RETRY_START_SEC
            self._off_retry_next = now + self._off_retry_sec
        else:
            self._off_retry_sec = min(float(COMM_OUTAGE_LOG_SEC), self._off_retry_sec * 2.0)
            self._off_retry_next = now + self._off_retry_sec
        if first or (now - self._off_last_log_t) >= float(COMM_OUTAGE_LOG_SEC):
            self._off_last_log_t = now
            self.status_message.emit(
                "DCpower(경고)",
                f"DC 출력 OFF 미확인 ({where}: {detail}) — 장비 전면에서 확인 필요, 통신되면 자동 재시도")
            self._emit_event("OFF미확인", f"{where}: {detail}")
        if first:
            self.off_unconfirmed.emit(where)
        if not self._outage_probe.isActive():
            self._outage_probe.start()

    @Slot()
    def safe_off(self):
        """복구 후 안전 상태 재적용 — 공정이 돌지 않을 때 main 이 부른다. OUTP OFF 1회(확인형)."""
        r = self._output_off_confirmed("복구 후 안전 상태")
        self._emit_event("안전상태재적용",
                         "OUTP OFF " + ("확인" if r is True else ("확인 보류" if r is None else "미확인")))
        self._outage_abort = False
        if r is True:
            self._outage_probe.stop()      # 미확인·보류면 프로브가 계속 돌며 재시도한다

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
        if self._io_depth > 0:
            # 읽기(중첩 이벤트루프) 도중에 *RST/APPLy/OUTP ON 이 끼어들면 응답이 뒤섞인다 — 뒤로 미룬다
            QTimer.singleShot(50, lambda: self.start_process(target_power))
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
        if self._io_depth > 0:
            return          # 앞 틱의 읽기가 아직 안 끝났다 — 겹친 틱은 건너뛴다(응답 뒤섞임 방지)

        now_power, now_v, now_i = self.read_dc_power()  # MEAS:ALL?
        if not self._is_running:
            return          # 읽는 동안(중첩 이벤트루프) 정지가 끼어들었다 — 제어·CURR 전송 금지
        if now_power is None or now_v is None or now_i is None:
            # ★ 측정 실패를 0 W 로 위장하지 않는다 — 이 틱은 건너뛴다(파워 이탈·램프업 무응답·
            #   저전류 카운터를 올리지 않음). 화면은 마지막 값 유지. 중단 여부는 통신 예산이 정한다.
            self._check_comm_budget()
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
        self._output_maybe_on = True        # 부분 전송도 출력이 켜졌을 수 있다 — 보내기 전에 세운다
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

    def _reset_run_state(self) -> bool:
        """공정 실행 상태 초기화(stop_process / emergency_off 공용). 돌고 있었는지 돌려준다."""
        was_running = self._is_running
        self._is_running = False
        self.state = "IDLE"
        self.error_count = 0
        self.power_error_count = 0
        self._fail_no_output_ticks = 0
        self._min_current_abort_count = 0
        self._limit_stall_ticks = 0
        self._power_monitor_armed = False
        self._stop_control_timer()
        return was_running

    @Slot()
    def stop_process(self):
        was_running = self._reset_run_state()

        self._output_off_confirmed("stop_process")

        self.update_dc_status_display.emit(0.0, 0.0, 0.0)

        if was_running:
            self.status_message.emit("DCpower", "출력 OFF, 대기 상태로 전환")

    @Slot()
    def emergency_off(self):
        """ALL STOP — 공정 상태와 무관하게 DC 출력을 직접 끈다(확인형).
        DC 서플라이는 PLC 를 거치지 않는 직결 시리얼이라 PLC 비상정지로는 꺼지지 않는다."""
        self._reset_run_state()
        opened_here = False
        want_before = self._want_connected
        streak_before = self._comm_fail_streak
        if not (self.serial and self.serial.isOpen()) and self._io_depth == 0:
            opened_here = bool(self.connect_dcpower_device())
        self._emg_restore = ({"want": want_before, "streak": streak_before}
                             if (opened_here and not self._output_maybe_on) else None)
        if self.serial and self.serial.isOpen():
            self.status_message.emit("DCpower", "ALL STOP — DC 출력 OFF 요청")
            r = self._output_off_confirmed("ALL STOP")
        else:
            self._mark_off_unconfirmed("ALL STOP", "포트 연결 불가")
            r = False
        self.update_dc_status_display.emit(0.0, 0.0, 0.0)
        if r is not None:
            self._emg_restore_if_needed()      # 보류(None)면 _verify_retry 가 끝낸 뒤 되돌린다

    def _emg_restore_if_needed(self) -> None:
        """ALL STOP 이 임시로 연 포트를 원래대로 — 대기 중 재연결 루프·장기두절 알림이 생기면 안 된다."""
        st = self._emg_restore
        if not st:
            return
        self._emg_restore = None
        try:
            self._close_port_for_reconnect()
        except Exception:
            pass
        self._want_connected = bool(st["want"])
        self._comm_fail_streak = int(st["streak"])

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
        self._want_connected = False

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

        if v is None or i is None:
            # ★ 실패를 0 W 로 위장하지 않는다 — 화면은 마지막 값 유지, 호출자는 틱을 건너뛴다
            return (None, None, None)
        p = v * i
        if self._is_running:
            self.update_dc_status_display.emit(p, v, i)   # 정지 중이면 0 표시를 덮지 않는다
        return (p, v, i)

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
                self._comm_last_ok = time.monotonic()
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
            self._comm_fail(f"{command} 전송 실패")
            return None

        line = self._readline_blocking(timeout_ms)
        if line is None:
            self.status_message.emit("DCpower", "수신 타임아웃")
            self._comm_fail(f"{command} 수신 타임아웃")
        else:
            self.status_message.emit("DCpower < 응답", line)
            self._comm_ok()
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
        """readyRead를 기다려 '\n' 또는 '\r'까지 한 줄을 동기적으로 읽는다.
        중첩 QEventLoop 라 이 안에서 다른 슬롯·타이머가 돈다 — _io_depth 로 재진입을 막는다."""
        if not (self.serial and self.serial.isOpen()):
            return None
        self._io_depth += 1
        try:
            return self._readline_blocking_impl(timeout_ms)
        finally:
            self._io_depth -= 1

    def _readline_blocking_impl(self, timeout_ms: int = 500) -> Optional[str]:

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