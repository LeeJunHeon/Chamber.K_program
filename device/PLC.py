# device/PLC.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Tuple, Callable, Deque, Optional
from PyQt6.QtCore import QObject, QThread, pyqtSignal as Signal, pyqtSlot as Slot, QTimer, QMutex
import minimalmodbus
import time
from collections import deque

from lib.config import (
    PLC_PORT, PLC_SLAVE_ID, PLC_BAUD,
    PLC_TIMEOUT, PLC_COIL_MAP,
    PLC_SENSOR_BITS,

    # ✅ 재연결/재시도 정책
    PLC_POLLING_INTERVAL_MS,
    PLC_RECONNECT_BACKOFF_START_MS, PLC_RECONNECT_BACKOFF_MAX_MS,
    PLC_RECONNECT_MAX_TOTAL_SEC,
    PLC_CMD_QUEUE_MAX, PLC_CMD_DEFAULT_RETRIES,
    PLC_CMD_ACK_TIMEOUT_SEC,

    RF_ADC_FORWARD_ADDR,
    RF_ADC_REFLECT_ADDR, RF_ADC_MAX_COUNT,
    RF_DAC_ADDR_CH0, COIL_ENABLE_DAC_CH0,
    RF_FORWARD_SCALING_MAX_WATT,
    RF_REFLECTED_SCALING_MAX_WATT
)


# 주의: 아래 표에서 대괄호 […]가 실제 Modbus 주소(0-based, HEX 표시)입니다.
# 예) S1: [31] -> 0x31(=49), S2: [32] -> 0x32(=50)
# 이 주소들은 lib.config.PLC_COIL_MAP에서 바로 int로 지정합니다.

# ===============================
# 주소 맵 (신규 하드웨어 맵 반영)
# ===============================
# Air       P00000    M00100 [DI0]
# Gauge1    P00001    M00101 [DI1]
# Gauge2    P00002    M00102 [DI2]
# ATM       P00003    M00103 [DI3]
# Water     P00004    M00104 [DI4]

# Rotary[0]  M00000    P00040 [P020]
# RV[1]      M00001    P00041 [P021]
# FV[2]      M00002    P00042 [P022]
# MV[3]      M00003    P00043 [23]
# Vent[4]    M00004    P00161 [24]
# Turbo[5]   M00005    P00045 [25]
# Doorup[6]  M00006    P00162 [26]
# Doordn[7]  M00020    P00163 [27]   <-- 비연속 주소(20)
# Ar[8]      M00007    P00047 [2A]
# O2[9]      M00008    P00164 [2B]
# Buzz[10]   M00021    P00165 [2F]   <-- 비연속 주소(21)
# MS[11]     M00009    P00049 [30]
# S1[12]     M00010    P0004A [31]
# S2[13]     M00011    P0004B [32]

# ADC        RDY    U01.00.F
# ADC Ch0    act    U01.01.0
# ADC Ch0    data   U01.02    D00000
# ADC Ch1    act    U01.01.1
# ADC Ch1    data   U01.03    D00001
# ADC Ch2    act    U01.01.2
# ADC Ch2    data   U01.04    D00002
# ADC Ch3    act    U01.01.3
# ADC Ch3    data   U01.05    D00003
# ERR               U01.00.0
# ERR_CLR           U01.11.0

# DAC           RDY                U02.00.F
# DAC    Ch0    outen    M00200    U02.02.0
# DAC    Ch0    act                U02.01.0
# DAC    Ch0    data     D00040    U02.03
# DAC    Ch0    ERR                U02.00.0
# DAC    Ch1    outen    M00201    U02.02.1
# DAC    Ch1    act                U02.01.1
# DAC    Ch1    data     D00041    U02.04
# DAC    Ch1    ERR                U02.00.1
# DAC    Ch2    outen    M00202    U02.02.2
# DAC    Ch2    act                U02.01.2
# DAC    Ch2    data     D00042    U02.05
# DAC    Ch2    ERR                U02.00.2
# DAC    Ch3    outen    M00203    U02.02.3
# DAC    Ch3    act                U02.01.3
# DAC    Ch3    data     D00043    U02.06
# DAC    Ch3    ERR                U02.00.3


class PLCController(QObject):
    status_message = Signal(str, str)
    update_button_display = Signal(str, bool)
    update_sensor_display = Signal(str, bool)

    def __init__(self):
        super().__init__()
        self.instrument: minimalmodbus.Instrument | None = None

        self.polling_timer = QTimer(self)
        self.polling_timer.setInterval(int(PLC_POLLING_INTERVAL_MS))  # ✅ config값(현재 1초=1000ms)
        self.polling_timer.timeout.connect(self._poll_status)

        # ✅ 재연결 타이머(지수 백오프)
        self._reconnect_timer = QTimer(self)
        self._reconnect_timer.setSingleShot(True)
        self._reconnect_timer.timeout.connect(self._try_reconnect)

        self._reconnect_backoff_ms = int(PLC_RECONNECT_BACKOFF_START_MS)
        self._reconnect_started_at: Optional[float] = None
        self._comm_error_latched = False
        self._fatal_disconnect_emitted = False
        self._last_comm_error = ""

        # ✅ 끊김 동안 명령 큐잉 (재연결 후 순서대로 재전송)
        self._cmd_queue: Deque[Tuple[str, Callable[[minimalmodbus.Instrument], None], int]] = deque()

        self._is_running = False
        self._mutex = QMutex()
        self._busy = False

        self.rf_controller = None
        self._last_button_states: Dict[str, bool] = {}

        self._coil_read_fail_latched = False

    def set_rf_controller(self, rf_controller):
        self.rf_controller = rf_controller

    @Slot()
    def clear_fault_latch(self):
        """새 공정 시작 전에 PLC 통신 오류/재연결 상태를 초기화."""
        self._coil_read_fail_latched = False
        self._comm_error_latched = False
        self._fatal_disconnect_emitted = False
        self._reconnect_started_at = None
        self._reconnect_backoff_ms = int(PLC_RECONNECT_BACKOFF_START_MS)
        self._last_comm_error = ""
        try:
            self._cmd_queue.clear()
        except Exception:
            pass

    # ============== 연결/해제 =================
    @Slot()
    def start_polling(self):
        if self._is_running:
            return

        # ✅ 시작 시점에도 백오프로 재연결을 계속 시도
        self._is_running = True
        try:
            self._connect_once()
            self.polling_timer.start()
            self.status_message.emit("PLC", f"연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID}")
        except Exception as e:
            self._last_comm_error = f"초기 연결 실패: {e}"
            self.status_message.emit("PLC(경고)", f"연결 실패: {e} → 재연결을 시도합니다.")
            self._begin_reconnect()

    @Slot()
    def cleanup(self):
        try:
            self.polling_timer.stop()
        except Exception:
            pass
        try:
            self._reconnect_timer.stop()
        except Exception:
            pass

        self._is_running = False
        QThread.msleep(200)

        try:
            self._cmd_queue.clear()
        except Exception:
            pass

        self._close_instrument()
        self.status_message.emit("PLC", "포트를 안전하게 닫았습니다.")

    # ============== 내부 유틸 =================
    # ---- 재연결/큐 핵심 로직(필수) ----
    def _connect_once(self) -> None:
        """PLC Instrument를 1회 생성/설정합니다."""
        inst = minimalmodbus.Instrument(PLC_PORT, PLC_SLAVE_ID, mode=minimalmodbus.MODE_RTU)
        inst.serial.baudrate = PLC_BAUD
        inst.serial.bytesize = 8
        inst.serial.parity = 'N'
        inst.serial.stopbits = 1
        inst.serial.timeout = PLC_TIMEOUT

        inst.close_port_after_each_call = False
        inst.clear_buffers_before_each_transaction = True
        inst.handle_local_echo = False

        self.instrument = inst

    def _close_instrument(self) -> None:
        try:
            if self.instrument and self.instrument.serial and self.instrument.serial.is_open:
                self.instrument.serial.close()
        except Exception:
            pass
        self.instrument = None

    def _begin_reconnect(self) -> None:
        """지수 백오프 재연결 루프 시작/유지."""
        if not self._is_running or self._fatal_disconnect_emitted:
            return

        if self._reconnect_started_at is None:
            self._reconnect_started_at = time.monotonic()
            self._reconnect_backoff_ms = int(PLC_RECONNECT_BACKOFF_START_MS)

        if not self._reconnect_timer.isActive():
            self._reconnect_timer.start(int(self._reconnect_backoff_ms))

    def _handle_comm_error(self, context: str, ex: Exception) -> None:
        """통신 오류 공통 처리: 폴링 중지 + 포트 닫기 + 재연결 시작(즉시 공정 종료 X)."""
        self._last_comm_error = f"{context}: {ex}"

        if not self._comm_error_latched:
            self._comm_error_latched = True
            self.status_message.emit("PLC(경고)", f"{context}: {ex} → 재연결 시도")

        try:
            self.polling_timer.stop()
        except Exception:
            pass

        self._close_instrument()
        self._begin_reconnect()

    @Slot()
    def _try_reconnect(self) -> None:
        """재연결 타이머 콜백: 연결 시도 → 성공 시 폴링 재개 + 큐 flush."""
        if not self._is_running or self._fatal_disconnect_emitted:
            return

        # 다른 작업 중이면 잠깐 뒤로
        if self._busy:
            self._reconnect_timer.start(200)
            return

        self._busy = True
        self._mutex.lock()
        try:
            try:
                self._connect_once()
            except Exception as e:
                self._last_comm_error = f"재연결 실패: {e}"

                # 총 재연결 시간 초과 시 공정 종료 트리거(재시작) 1회
                if self._reconnect_started_at is not None:
                    elapsed = time.monotonic() - self._reconnect_started_at
                    if elapsed >= float(PLC_RECONNECT_MAX_TOTAL_SEC):
                        self._fatal_disconnect_emitted = True
                        try:
                            self._cmd_queue.clear()
                        except Exception:
                            pass
                        self.status_message.emit(
                            "재시작",
                            f"PLC 재연결 실패: {elapsed:.1f}s 경과 / 마지막 오류=({self._last_comm_error}) → 공정을 중단합니다."
                        )
                        return

                # 아직 허용시간 이내면 백오프 증가 후 재시도
                self.status_message.emit("PLC(경고)", f"재연결 실패: {e} (다음 시도까지 {self._reconnect_backoff_ms}ms)")
                self._reconnect_backoff_ms = min(int(self._reconnect_backoff_ms) * 2, int(PLC_RECONNECT_BACKOFF_MAX_MS))
                self._reconnect_timer.start(int(self._reconnect_backoff_ms))
                return

            # ✅ 재연결 성공
            self._comm_error_latched = False
            self._reconnect_started_at = None
            self._reconnect_backoff_ms = int(PLC_RECONNECT_BACKOFF_START_MS)
            self.status_message.emit("PLC", f"재연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID}")

            try:
                self.polling_timer.start()
            except Exception:
                pass

            self._flush_cmd_queue()

        finally:
            self._busy = False
            self._mutex.unlock()

    def _enqueue_cmd(
        self,
        desc: str,
        fn: Callable[[minimalmodbus.Instrument], None],
        *,
        retries: Optional[int] = None,
        front: bool = False
    ) -> None:
        """명령 큐에 적재. front=True면 실패한 명령을 우선 재시도."""
        if retries is None:
            retries = int(PLC_CMD_DEFAULT_RETRIES)

        max_q = int(PLC_CMD_QUEUE_MAX)
        while max_q > 0 and len(self._cmd_queue) >= max_q:
            try:
                dropped = self._cmd_queue.popleft()
                self.status_message.emit("PLC(경고)", f"명령 큐가 가득 참 → 오래된 명령 폐기: {dropped[0]}")
            except Exception:
                break

        item = (desc, fn, int(retries))
        if front:
            self._cmd_queue.appendleft(item)
        else:
            self._cmd_queue.append(item)

        if self.instrument is None:
            self._begin_reconnect()

    def _flush_cmd_queue(self) -> None:
        """재연결 직후 큐 명령들을 순서대로 재전송."""
        if self.instrument is None:
            return

        while self._cmd_queue and self.instrument is not None and not self._fatal_disconnect_emitted:
            desc, fn, retries_left = self._cmd_queue[0]
            try:
                fn(self.instrument)
                self._cmd_queue.popleft()
                self.status_message.emit("PLC", f"큐 명령 재전송 OK: {desc}")
            except Exception as e:
                retries_left -= 1
                if retries_left > 0:
                    self._cmd_queue[0] = (desc, fn, retries_left)
                else:
                    try:
                        self._cmd_queue.popleft()
                    except Exception:
                        pass
                    self.status_message.emit("PLC(오류)", f"큐 명령 재전송 실패(포기): {desc} / {e}")

                self._handle_comm_error(f"큐 명령 재전송 실패({desc})", e)
                break

    def _apply_port_state(self, inst: minimalmodbus.Instrument, btn_name: str, state: bool) -> None:
        """update_port_state의 실제 쓰기 로직(큐 재전송에서도 재사용)."""
        if btn_name == "Door_Button":
            up_addr = PLC_COIL_MAP.get("Doorup_button")
            dn_addr = PLC_COIL_MAP.get("Doordn_button")
            if up_addr is None or dn_addr is None:
                raise RuntimeError("Door up/down 주소가 설정되지 않았습니다.")

            inst.write_bit(up_addr, int(state), functioncode=5)
            inst.write_bit(dn_addr, int(not state), functioncode=5)

            self.update_button_display.emit("Door_Button", state)
            self._last_button_states["Door_Button"] = state
            self.update_button_display.emit("Doorup_button", state)
            self.update_button_display.emit("Doordn_button", not state)
            self._last_button_states["Doorup_button"] = state
            self._last_button_states["Doordn_button"] = (not state)
            return

        if btn_name in ("Doorup_button", "Doordn_button"):
            up_addr = PLC_COIL_MAP.get("Doorup_button")
            dn_addr = PLC_COIL_MAP.get("Doordn_button")
            if up_addr is None or dn_addr is None:
                raise RuntimeError("Door up/down 주소가 설정되지 않았습니다.")

            if btn_name == "Doorup_button":
                inst.write_bit(up_addr, int(state), functioncode=5)
                if state:
                    inst.write_bit(dn_addr, 0, functioncode=5)
                self.update_button_display.emit("Door_Button", state)
                self._last_button_states["Door_Button"] = state
                self.update_button_display.emit("Doordn_button", False if state else self._last_button_states.get("Doordn_button", False))
            else:
                inst.write_bit(dn_addr, int(state), functioncode=5)
                if state:
                    inst.write_bit(up_addr, 0, functioncode=5)
                self.update_button_display.emit("Door_Button", not state if state else self._last_button_states.get("Door_Button", False))
                self._last_button_states["Door_Button"] = (not state) if state else self._last_button_states.get("Door_Button", False)
                self.update_button_display.emit("Doorup_button", False if state else self._last_button_states.get("Doorup_button", False))

            self.update_button_display.emit(btn_name, state)
            self._last_button_states[btn_name] = state
            return

        addr = PLC_COIL_MAP.get(btn_name)
        if addr is None:
            raise RuntimeError(f"알 수 없는 버튼: {btn_name}")

        inst.write_bit(addr, int(state), functioncode=5)
        self.update_button_display.emit(btn_name, state)
        self._last_button_states[btn_name] = state

    def _apply_rf_dac(self, inst: minimalmodbus.Instrument, pwm_value: int) -> None:
        """send_rfpower_command의 실제 쓰기 로직(큐 재전송에서도 재사용)."""
        if COIL_ENABLE_DAC_CH0 is not None:
            inst.write_bit(COIL_ENABLE_DAC_CH0, 1, functioncode=5)
        inst.write_register(RF_DAC_ADDR_CH0, int(pwm_value), functioncode=6)

    # ============== 내부 유틸(기존) =================
    def _read_coils_grouped(self, addrs: List[int]) -> Dict[int, bool]:
        """
        비연속 주소를 연속 구간으로 묶어 배치 읽기 후 {addr: bool} 딕셔너리로 반환.
        """
        assert self.instrument is not None
        if not addrs:
            return {}

        sorted_addrs = sorted(set(addrs))
        ranges: List[Tuple[int, int]] = []
        start = prev = sorted_addrs[0]
        for a in sorted_addrs[1:]:
            if a == prev + 1:
                prev = a
            else:
                ranges.append((start, prev))
                start = prev = a
        ranges.append((start, prev))

        result: Dict[int, bool] = {}
        for (s, e) in ranges:
            count = (e - s) + 1
            try:
                bits = self.instrument.read_bits(s, count, functioncode=1)  # Coils
                for i, b in enumerate(bits):
                    result[s + i] = bool(b)
            except Exception as ex:
                # ✅ 즉시 재시작 트리거 금지 → 재연결 루프
                self._coil_read_fail_latched = True
                self._handle_comm_error(f"PLC Coils 읽기 실패 [{s}..{e}]", ex)
                break

        return result

    def _safe_read_discrete_inputs(self, start_addr: int, count: int) -> List[bool]:
        assert self.instrument is not None
        return self.instrument.read_bits(start_addr, count, functioncode=1)

    # ============== 폴링 ======================
    @Slot()
    def _poll_status(self):
        if not self._is_running or self._busy or self.instrument is None:
            return

        self._busy = True
        self._mutex.lock()
        try:
            # 1) 코일 상태(버튼) 동기화 — 비연속 주소를 그룹 폴링
            coil_addr_list = list(PLC_COIL_MAP.values())
            addr_to_state = self._read_coils_grouped(coil_addr_list)

            for btn_name, addr in PLC_COIL_MAP.items():
                val = bool(addr_to_state.get(addr, False))
                if self._last_button_states.get(btn_name) != val:
                    self._last_button_states[btn_name] = val
                    self.update_button_display.emit(btn_name, val)

            up_addr = PLC_COIL_MAP.get("Doorup_button")
            if up_addr is not None:
                door_state = bool(addr_to_state.get(up_addr, False))
                if self._last_button_states.get("Door_Button") != door_state:
                    self._last_button_states["Door_Button"] = door_state
                    self.update_button_display.emit("Door_Button", door_state)

            # 2) 센서(코일) 읽기
            if PLC_SENSOR_BITS:
                try:
                    coil_addrs = list(PLC_SENSOR_BITS.values())
                    addr_to_state = self._read_coils_grouped(coil_addrs)
                    for name, addr in PLC_SENSOR_BITS.items():
                        self.update_sensor_display.emit(name, bool(addr_to_state.get(addr, False)))
                except Exception as ex:
                    self._handle_comm_error("센서(코일) 읽기 실패", ex)

        except Exception as e:
            self._handle_comm_error("폴링 실패", e)
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== 쓰기(버튼 클릭 반영) =========
    @Slot(str, bool)
    def update_port_state(self, btn_name: str, state: bool):
        desc = f"COIL {btn_name}={int(bool(state))}"

        if self.instrument is None:
            self.status_message.emit("PLC(경고)", f"포트 닫힘 → 명령 큐잉: {desc}")
            self._enqueue_cmd(desc, lambda inst: self._apply_port_state(inst, btn_name, state))
            return

        self._busy = True
        self._mutex.lock()
        try:
            inst = self.instrument
            if inst is None:
                self.status_message.emit("PLC(경고)", f"포트 닫힘 → 명령 큐잉: {desc}")
                self._enqueue_cmd(desc, lambda inst2: self._apply_port_state(inst2, btn_name, state))
                return

            self._apply_port_state(inst, btn_name, state)

        except Exception as e:
            # ✅ 실패한 명령은 앞쪽에 넣어 재연결 후 최우선 재시도
            self._enqueue_cmd(desc, lambda inst: self._apply_port_state(inst, btn_name, state), front=True)
            self._handle_comm_error(f"코일 쓰기 실패({btn_name})", e)
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== RF (옵션) ==================
    def send_rfpower_command(self, pwm_value: int):
        desc = f"RF/DAC={int(pwm_value)} @D{RF_DAC_ADDR_CH0}"

        if self.instrument is None:
            self.status_message.emit("PLC(경고)", f"포트 닫힘 → 명령 큐잉: {desc}")
            self._enqueue_cmd(desc, lambda inst: self._apply_rf_dac(inst, pwm_value))
            return

        self._busy = True
        self._mutex.lock()
        try:
            inst = self.instrument
            if inst is None:
                self.status_message.emit("PLC(경고)", f"포트 닫힘 → 명령 큐잉: {desc}")
                self._enqueue_cmd(desc, lambda inst2: self._apply_rf_dac(inst2, pwm_value))
                return

            self._apply_rf_dac(inst, pwm_value)

            # Echo 확인(선택)
            try:
                old_to = getattr(inst.serial, "timeout", None)
                inst.serial.timeout = float(PLC_CMD_ACK_TIMEOUT_SEC)
                echo = inst.read_register(RF_DAC_ADDR_CH0, 0, functioncode=3, signed=False)
                self.status_message.emit("PLC > 확인", f"DAC echo={echo}")
                if old_to is not None:
                    inst.serial.timeout = old_to
            except Exception:
                try:
                    if old_to is not None:
                        inst.serial.timeout = old_to
                except Exception:
                    pass

            self.status_message.emit("PLC > 전송", desc)

        except Exception as e:
            self._enqueue_cmd(desc, lambda inst: self._apply_rf_dac(inst, pwm_value), front=True)
            self._handle_comm_error("RF 파워 송신 실패", e)
        finally:
            self._busy = False
            self._mutex.unlock()

    def read_rf_feedback(self) -> tuple[float, float] | tuple[None, None]:
        if self.instrument is None:
            self._begin_reconnect()
            return None, None

        self._busy = True
        self._mutex.lock()
        try:
            inst = self.instrument
            if inst is None:
                self._begin_reconnect()
                return None, None

            f_raw = inst.read_register(RF_ADC_FORWARD_ADDR, 0, functioncode=3, signed=False)
            r_raw = inst.read_register(RF_ADC_REFLECT_ADDR, 0, functioncode=3, signed=False)
            forward_watt = (f_raw / RF_ADC_MAX_COUNT) * RF_FORWARD_SCALING_MAX_WATT
            reflected_watt = (r_raw / RF_ADC_MAX_COUNT) * RF_REFLECTED_SCALING_MAX_WATT
            return forward_watt, reflected_watt

        except Exception as e:
            self._handle_comm_error("RF 피드백 읽기 실패", e)
            return None, None
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== 비상 정지 ==================
    @Slot()
    def on_emergency_stop(self):
        if self.instrument is None:
            return
        self._busy = True
        self._mutex.lock()
        try:
            addrs = sorted(set(PLC_COIL_MAP.values()))
            if addrs:
                ranges: List[Tuple[int, int]] = []
                start = prev = addrs[0]
                for a in addrs[1:]:
                    if a == prev + 1:
                        prev = a
                    else:
                        ranges.append((start, prev))
                        start = prev = a
                ranges.append((start, prev))

                for (s, e) in ranges:
                    try:
                        if s == e:
                            self.instrument.write_bit(s, 0, functioncode=5)
                        else:
                            count = (e - s) + 1
                            self.instrument.write_bits(s, [0] * count)  # FC=15
                    except Exception:
                        for a in range(s, e + 1):
                            try:
                                self.instrument.write_bit(a, 0, functioncode=5)
                            except Exception:
                                pass

            for btn_name in PLC_COIL_MAP.keys():
                self.update_button_display.emit(btn_name, False)

            self.update_button_display.emit("Door_Button", False)
            self._last_button_states["Door_Button"] = False

            if COIL_ENABLE_DAC_CH0 is not None:
                try:
                    self.instrument.write_bit(COIL_ENABLE_DAC_CH0, 0, functioncode=5)
                except Exception:
                    pass

            self.status_message.emit("PLC(비상)", "EMERGENCY STOP: 모든 코일 OFF, DAC 비활성")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"EMERGENCY STOP 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()
