# device/PLC.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, List, Tuple
from PyQt6.QtCore import QObject, QThread, pyqtSignal as Signal, pyqtSlot as Slot, QTimer, QMutex
import minimalmodbus

from lib.config import (
    PLC_PORT, PLC_SLAVE_ID, PLC_BAUD,
    PLC_TIMEOUT, PLC_COIL_MAP,
    PLC_SENSOR_BITS, RF_ADC_FORWARD_ADDR,
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
        self.polling_timer.setInterval(200)
        self.polling_timer.timeout.connect(self._poll_status)

        self._is_running = False
        self._mutex = QMutex()
        self._busy = False

        self.rf_controller = None
        self._last_button_states: Dict[str, bool] = {}

        self._coil_read_fail_latched = False

        # ===== 재연결(최대 5회) + 끊김 중 명령 저장 =====
        self._want_connected = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._last_disconnect_error: str = ""
        self._fatal_latched = False

        # 끊긴 동안의 "쓰기" 요청만 저장(마지막 값 기준)
        self._pending_bit_writes: Dict[int, int] = {}
        self._pending_reg_writes: Dict[int, int] = {}

        # 재연결 타이머 (single-shot)
        self._reconnect_timer = QTimer(self)
        self._reconnect_timer.setSingleShot(True)
        self._reconnect_timer.timeout.connect(self._try_reconnect)

    # 상위와 동일 API 유지
    def set_rf_controller(self, rf_controller):
        self.rf_controller = rf_controller

    @Slot()
    def clear_fault_latch(self):
        """새 공정 시작 전에 PLC 통신 실패 상태를 해제하고, 필요 시 재연결을 다시 시작."""
        self._coil_read_fail_latched = False

        # ✅ fatal(재연결 5회 실패)까지 걸렸던 경우, 다음 공정에서는 다시 붙을 기회를 줌
        if self._fatal_latched:
            self._fatal_latched = False
            self._want_connected = True
            self._reconnect_attempts = 0
            self._last_disconnect_error = ""
            self.status_message.emit("PLC", "PLC 통신 실패(fatal) 래치 해제 → 재연결 재시도")

        # ✅ 현재 끊긴 상태면 즉시 재연결 시도
        if self.instrument is None and self._want_connected and (not self._reconnect_timer.isActive()):
            self._reconnect_timer.start(0)

    # ============== 연결/해제 =================
    @Slot()
    def start_polling(self):
        if self._is_running:
            return

        # ✅ 재연결 루프 활성화
        self._is_running = True
        self._want_connected = True
        self._fatal_latched = False

        # 새 시작이므로 카운터/에러 리셋
        self._reconnect_attempts = 0
        self._last_disconnect_error = ""
        self._coil_read_fail_latched = False

        # 끊김 중 저장된 쓰기 명령도 새 시작이면 비움(원하면 유지해도 됨)
        self._pending_bit_writes.clear()
        self._pending_reg_writes.clear()

        # 타이머 정리
        try:
            self.polling_timer.stop()
        except Exception:
            pass
        try:
            self._reconnect_timer.stop()
        except Exception:
            pass

        # 즉시 1회 연결 시도(실패하면 _try_reconnect 내부에서 backoff로 5회까지)
        self.status_message.emit("PLC", f"연결 시도 시작: {PLC_PORT}, ID={PLC_SLAVE_ID} (max {self._max_reconnect_attempts} tries)")
        self._reconnect_timer.start(0)

    @Slot()
    def cleanup(self):
        self._want_connected = False
        self._is_running = False

        try:
            self.polling_timer.stop()
        except Exception:
            pass
        try:
            self._reconnect_timer.stop()
        except Exception:
            pass

        QThread.msleep(200)
        self._close_instrument()

        self._pending_bit_writes.clear()
        self._pending_reg_writes.clear()

        self.status_message.emit("PLC", "포트를 안전하게 닫았습니다.")

    # ================== 재연결 유틸 ==================
    def _is_port_open(self) -> bool:
        try:
            return bool(self.instrument and self.instrument.serial and self.instrument.serial.is_open)
        except Exception:
            return False

    def _close_instrument(self) -> None:
        try:
            if self.instrument and self.instrument.serial and self.instrument.serial.is_open:
                self.instrument.serial.close()
        except Exception:
            pass
        self.instrument = None

    def _open_instrument(self) -> None:
        """Instrument 생성 + serial 세팅 (열기까지 시도). 실패하면 예외 발생."""
        inst = minimalmodbus.Instrument(PLC_PORT, PLC_SLAVE_ID, mode=minimalmodbus.MODE_RTU)
        inst.serial.baudrate = PLC_BAUD
        inst.serial.bytesize = 8
        inst.serial.parity   = 'N'
        inst.serial.stopbits = 1
        inst.serial.timeout  = PLC_TIMEOUT

        inst.close_port_after_each_call = False
        inst.clear_buffers_before_each_transaction = True
        inst.handle_local_echo = False

        # 일부 환경에선 명시적으로 open이 필요할 수 있음
        if inst.serial and not inst.serial.is_open:
            try:
                inst.serial.open()
            except Exception:
                # minimalmodbus가 이후 트랜잭션에서 열 수도 있으니, 여기서 무조건 실패 처리하진 않음
                pass

        self.instrument = inst

    def _backoff_delay_ms(self, attempt_no: int) -> int:
        """
        attempt_no: 1..N
        1->500ms, 2->1000ms, 3->2000ms, 4->4000ms, 5->8000ms (캡)
        """
        return min(8000, 500 * (2 ** max(attempt_no - 1, 0)))

    def _schedule_reconnect(self) -> None:
        if not self._want_connected:
            return
        if self._fatal_latched:
            return
        if self._reconnect_timer.isActive():
            return

        # 이미 실패 누적이 max 이상이면 즉시 치명 처리
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            self._fail_fatal()
            return

        # 다음 시도는 attempt+1번째
        next_attempt = self._reconnect_attempts + 1
        delay_ms = self._backoff_delay_ms(next_attempt)
        self.status_message.emit("PLC", f"재연결 예약: {next_attempt}/{self._max_reconnect_attempts} (in {delay_ms}ms)")
        self._reconnect_timer.start(delay_ms)

    def _mark_disconnected(self, where: str, ex: Exception) -> None:
        """통신 예외 발생 시: 포트 닫고 재연결 스케줄"""
        self._last_disconnect_error = f"{where}: {ex!r}"
        try:
            if self.polling_timer.isActive():
                self.polling_timer.stop()
        except Exception:
            pass

        self._close_instrument()
        self.status_message.emit("PLC(경고)", f"연결 끊김 감지({where}) → 재연결 시도")
        self._schedule_reconnect()

    def _flush_pending_writes(self) -> None:
        """재연결 성공 후, 끊긴 동안 저장된 쓰기 요청(마지막 값)을 적용."""
        if not self.instrument:
            return
        try:
            for addr, val in list(self._pending_bit_writes.items()):
                self.instrument.write_bit(addr, int(val), functioncode=5)
            self._pending_bit_writes.clear()

            for addr, val in list(self._pending_reg_writes.items()):
                self.instrument.write_register(addr, int(val), functioncode=6)
            self._pending_reg_writes.clear()

            self.status_message.emit("PLC", "재연결 후 저장된 명령 flush 완료")
        except Exception as e:
            self._mark_disconnected("flush_pending", e)

    @Slot()
    def _try_reconnect(self) -> None:
        if not self._want_connected or self._fatal_latched:
            return

        # 이미 max 도달이면 치명 처리
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            self._fail_fatal()
            return

        self._reconnect_attempts += 1
        attempt = self._reconnect_attempts

        # 혹시 이전 instrument가 남아 있으면 정리
        self._close_instrument()

        try:
            self._open_instrument()
            # 여기서 _is_port_open()이 False여도, 실제 트랜잭션에서 열릴 수 있음.
            # 최소한 instrument 생성 성공이면 "연결 시도 성공"으로 간주하고 폴링을 재개.
            self.status_message.emit("PLC", f"재연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID} (attempt {attempt})")

            # 성공했으니 카운터 리셋
            self._reconnect_attempts = 0
            self._coil_read_fail_latched = False

            # 폴링 재개
            self._is_running = True
            self.polling_timer.start()

            # 끊김 중 저장된 쓰기만 적용
            self._flush_pending_writes()
            return

        except Exception as e:
            self._last_disconnect_error = f"reconnect_attempt_{attempt}: {e!r}"
            self.status_message.emit("PLC(경고)", f"재연결 실패 {attempt}/{self._max_reconnect_attempts}: {e}")

            if self._reconnect_attempts >= self._max_reconnect_attempts:
                self._fail_fatal()
            else:
                self._schedule_reconnect()

    def _fail_fatal(self) -> None:
        """5회 재연결 실패 → 공정 전체 종료 트리거"""
        if self._fatal_latched:
            return
        self._fatal_latched = True
        self._want_connected = False

        try:
            self.polling_timer.stop()
        except Exception:
            pass
        try:
            self._reconnect_timer.stop()
        except Exception:
            pass

        self._close_instrument()

        msg = f"PLC 재연결 {self._max_reconnect_attempts}회 실패: {self._last_disconnect_error} → 공정 전체 종료"
        # main.py가 이미 '재시작'을 공정 중단 트리거로 사용 중이므로 그대로 활용
        self.status_message.emit("재시작", msg)
    # ================== 재연결 유틸 ==================

    # ============== 내부 유틸 =================
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
                # ✅ 읽기 실패 = 통신 끊김으로 간주 → 재연결(최대 5회) 시도
                self._mark_disconnected(f"read_bits[{s}..{e}]", ex)
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
                # addr_to_state 는 이미 _read_coils_grouped() 결과 딕셔너리
                door_state = bool(addr_to_state.get(up_addr, False))
                if self._last_button_states.get("Door_Button") != door_state:
                    self._last_button_states["Door_Button"] = door_state
                    self.update_button_display.emit("Door_Button", door_state)

            # 2) 센서(코일) 읽기 — FC=1, 절대 코일 주소
            if PLC_SENSOR_BITS:
                try:
                    coil_addrs = list(PLC_SENSOR_BITS.values())               # [256,257,258,259,260]
                    addr_to_state = self._read_coils_grouped(coil_addrs)      # FC=1로 그룹 폴링 (이미 구현됨)
                    for name, addr in PLC_SENSOR_BITS.items():
                        self.update_sensor_display.emit(name, bool(addr_to_state.get(addr, False)))
                except Exception as ex:
                    self.status_message.emit("PLC(경고)", f"센서(코일) 읽기 실패: {ex}")

        except Exception as e:
            self.status_message.emit("PLC(경고)", f"폴링 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== 쓰기(버튼 클릭 반영) =========
    @Slot(str, bool)
    def update_port_state(self, btn_name: str, state: bool):
        if self.instrument is None:
            # ✅ 끊김 중이면: "쓰기 명령만 저장"(마지막 값) + 재연결 시도
            if btn_name == "Door_Button":
                up_addr = PLC_COIL_MAP.get("Doorup_button")
                dn_addr = PLC_COIL_MAP.get("Doordn_button")
                if up_addr is not None and dn_addr is not None:
                    self._pending_bit_writes[up_addr] = int(state)
                    self._pending_bit_writes[dn_addr] = int(not state)
                self.status_message.emit("PLC(경고)", "PLC 끊김: Door 명령 저장(대기) 후 재연결 시도")
                self._schedule_reconnect()
                return

            if btn_name in ("Doorup_button", "Doordn_button"):
                up_addr = PLC_COIL_MAP.get("Doorup_button")
                dn_addr = PLC_COIL_MAP.get("Doordn_button")
                if up_addr is not None and dn_addr is not None:
                    if btn_name == "Doorup_button":
                        self._pending_bit_writes[up_addr] = int(state)
                        if state:
                            self._pending_bit_writes[dn_addr] = 0
                    else:
                        self._pending_bit_writes[dn_addr] = int(state)
                        if state:
                            self._pending_bit_writes[up_addr] = 0
                self.status_message.emit("PLC(경고)", "PLC 끊김: Door up/down 명령 저장(대기) 후 재연결 시도")
                self._schedule_reconnect()
                return

            addr = PLC_COIL_MAP.get(btn_name)
            if addr is not None:
                self._pending_bit_writes[addr] = int(state)
                self.status_message.emit("PLC(경고)", f"PLC 끊김: {btn_name} 명령 저장(대기) 후 재연결 시도")
                self._schedule_reconnect()
            else:
                self.status_message.emit("PLC(오류)", f"알 수 없는 버튼: {btn_name}")
            return

        self._busy = True
        self._mutex.lock()
        try:
            # 3-1) 단일 문 버튼: Door_Button (Faduino와 동일한 의미)
            #     True -> Doorup ON, Doordn OFF
            #     False -> Doorup OFF, Doordn ON
            if btn_name == "Door_Button":
                up_addr = PLC_COIL_MAP.get("Doorup_button")
                dn_addr = PLC_COIL_MAP.get("Doordn_button")
                if up_addr is None or dn_addr is None:
                    self.status_message.emit("PLC(오류)", "Door up/down 주소가 설정되지 않았습니다.")
                    return

                self.instrument.write_bit(up_addr, int(state), functioncode=5)
                self.instrument.write_bit(dn_addr, int(not state), functioncode=5)

                # UI 동기화 (Door_Button은 Up 상태를 표시)
                self.update_button_display.emit("Door_Button", state)
                self._last_button_states["Door_Button"] = state
                # 개별 버튼도 존재하면 동기화
                self.update_button_display.emit("Doorup_button", state)
                self.update_button_display.emit("Doordn_button", not state)
                self._last_button_states["Doorup_button"] = state
                self._last_button_states["Doordn_button"] = (not state)
                return

            # 3-2) 문 개별 버튼이 직접 들어오는 경우(상호배타 보장)
            if btn_name in ("Doorup_button", "Doordn_button"):
                up_addr = PLC_COIL_MAP.get("Doorup_button")
                dn_addr = PLC_COIL_MAP.get("Doordn_button")
                if up_addr is None or dn_addr is None:
                    self.status_message.emit("PLC(오류)", "Door up/down 주소가 설정되지 않았습니다.")
                    return

                if btn_name == "Doorup_button":
                    # Up = state, Down은 동시에 켜지지 않도록
                    self.instrument.write_bit(up_addr, int(state), functioncode=5)
                    if state:
                        self.instrument.write_bit(dn_addr, 0, functioncode=5)
                    # Door_Button은 Up 기준 표시
                    self.update_button_display.emit("Door_Button", state)
                    self._last_button_states["Door_Button"] = state
                    self.update_button_display.emit("Doordn_button", False if state else self._last_button_states.get("Doordn_button", False))
                else:
                    # Down = state, Up은 동시에 켜지지 않도록
                    self.instrument.write_bit(dn_addr, int(state), functioncode=5)
                    if state:
                        self.instrument.write_bit(up_addr, 0, functioncode=5)
                    # Door_Button은 Up 기준 표시 → Down이 True면 Door_Button은 False
                    self.update_button_display.emit("Door_Button", not state if state else self._last_button_states.get("Door_Button", False))
                    self._last_button_states["Door_Button"] = (not state) if state else self._last_button_states.get("Door_Button", False)
                    self.update_button_display.emit("Doorup_button", False if state else self._last_button_states.get("Doorup_button", False))

                # 개별 버튼 토글 반영
                self.update_button_display.emit(btn_name, state)
                self._last_button_states[btn_name] = state
                return

            # 3-3) 일반 코일 (그 외 버튼들)
            addr = PLC_COIL_MAP.get(btn_name)
            if addr is None:
                self.status_message.emit("PLC(오류)", f"알 수 없는 버튼: {btn_name}")
                return
            self.instrument.write_bit(addr, int(state), functioncode=5)
            self.update_button_display.emit(btn_name, state)
            self._last_button_states[btn_name] = state

        except Exception as e:
            self.status_message.emit("PLC(오류)", f"코일 쓰기 실패({btn_name}): {e}")
            self._mark_disconnected(f"write_bit[{btn_name}]", e)
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== RF (옵션) ==================
    def send_rfpower_command(self, pwm_value: int):
        if self.instrument is None:
            # ✅ 끊김 중이면 마지막 set 값만 저장
            if COIL_ENABLE_DAC_CH0 is not None:
                self._pending_bit_writes[COIL_ENABLE_DAC_CH0] = 1
            self._pending_reg_writes[RF_DAC_ADDR_CH0] = int(pwm_value)
            self.status_message.emit("PLC(경고)", "PLC 끊김: RF/DAC 명령 저장(대기) 후 재연결 시도")
            self._schedule_reconnect()
            return
        self._busy = True
        self._mutex.lock()
        try:
            if COIL_ENABLE_DAC_CH0 is not None:
                self.instrument.write_bit(COIL_ENABLE_DAC_CH0, 1, functioncode=5)
            self.instrument.write_register(RF_DAC_ADDR_CH0, int(pwm_value), functioncode=6)
            try:
                echo = self.instrument.read_register(RF_DAC_ADDR_CH0, 0, functioncode=3, signed=False)
                self.status_message.emit("PLC > 확인", f"DAC echo={echo}")
            except Exception as _:
                pass
            self.status_message.emit("PLC > 전송", f"RF/DAC={int(pwm_value)} @D{RF_DAC_ADDR_CH0}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"RF 파워 송신 실패: {e}")
            self._mark_disconnected("write_register[RF_DAC]", e)
        finally:
            self._busy = False
            self._mutex.unlock()

    def read_rf_feedback(self) -> tuple[float, float] | tuple[None, None]:
        if self.instrument is None:
            return None, None
        self._busy = True
        self._mutex.lock()
        try:
            f_raw = self.instrument.read_register(RF_ADC_FORWARD_ADDR, 0, functioncode=3, signed=False)
            r_raw = self.instrument.read_register(RF_ADC_REFLECT_ADDR, 0, functioncode=3, signed=False)
            forward_watt   = (f_raw / RF_ADC_MAX_COUNT) * RF_FORWARD_SCALING_MAX_WATT
            reflected_watt = (r_raw / RF_ADC_MAX_COUNT) * RF_REFLECTED_SCALING_MAX_WATT
            return forward_watt, reflected_watt
        except Exception as e:
            self.status_message.emit("PLC(경고)", f"RF 피드백 읽기 실패: {e}")
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
            # 연속 블록이면 FC=15로 한 번에, 아니면 개별로
            if addrs:
                # 구간화
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
                        # 범용 폴백
                        for a in range(s, e + 1):
                            try:
                                self.instrument.write_bit(a, 0, functioncode=5)
                            except Exception:
                                pass

            # UI 즉시 반영
            for btn_name in PLC_COIL_MAP.keys():
                self.update_button_display.emit(btn_name, False)

            self.update_button_display.emit("Door_Button", False)
            self._last_button_states["Door_Button"] = False

            # DAC OFF
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
