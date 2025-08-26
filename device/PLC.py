# device/PLC.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, List, Tuple
from PySide6.QtCore import QObject, QThread, Signal, Slot, QTimer, QMutex
import minimalmodbus

from lib.config import (
    RF_FORWARD_SCALING_MAX_WATT,
    RF_REFLECTED_SCALING_MAX_WATT,
    PLC_PORT,
    PLC_SLAVE_ID,
    PLC_BAUD,
    PLC_TIMEOUT,
    PLC_COIL_MAP,
    SENSOR_DI_BASE,
    PLC_SENSOR_BITS
)

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

# === RF 피드백(ADC) === (모듈 사양에 맞게 조정)
RF_ADC_FORWARD_ADDR = 0
RF_ADC_REFLECT_ADDR = 1
RF_ADC_MAX_COUNT    = 4000

RF_DAC_ADDR_CH0      = 40     # D00040 → 0-based 40
COIL_ENABLE_DAC_CH0  = 200    # M00200 → 0-based 200

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

    # 상위와 동일 API 유지
    def set_rf_controller(self, rf_controller):
        self.rf_controller = rf_controller

    # ============== 연결/해제 =================
    @Slot()
    def start_polling(self):
        if self._is_running:
            return
        try:
            self.instrument = minimalmodbus.Instrument(
                PLC_PORT, PLC_SLAVE_ID, mode=minimalmodbus.MODE_RTU
            )
            self.instrument.serial.baudrate = PLC_BAUD
            self.instrument.serial.bytesize = 8
            self.instrument.serial.parity   = 'N'
            self.instrument.serial.stopbits = 1
            self.instrument.serial.timeout  = PLC_TIMEOUT
            self._is_running = True
            self.polling_timer.start()
            self.status_message.emit("PLC", f"연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"연결 실패: {e}")

    @Slot()
    def cleanup(self):
        self.polling_timer.stop()
        self._is_running = False
        QThread.msleep(200)
        try:
            if self.instrument and self.instrument.serial and self.instrument.serial.is_open:
                self.instrument.serial.close()
        except Exception:
            pass
        self.instrument = None
        self.status_message.emit("PLC", "포트를 안전하게 닫았습니다.")

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
                self.status_message.emit("PLC(경고)", f"Coils 읽기 실패 [{s}..{e}]: {ex}")
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

            # 2) 센서 DI 읽기
            if PLC_SENSOR_BITS:
                di_count = max(PLC_SENSOR_BITS.values()) + 1
                try:
                    bits = self._safe_read_discrete_inputs(SENSOR_DI_BASE, di_count)
                    for name, bit in PLC_SENSOR_BITS.items():
                        if bit < len(bits):
                            self.update_sensor_display.emit(name, bool(bits[bit]))
                        else:
                            self.status_message.emit("PLC(경고)", f"DI 인덱스 초과: {name}->{bit} (len={len(bits)})")
                except Exception as ex:
                    self.status_message.emit("PLC(경고)", f"DI 읽기 실패: {ex}")
            # debug
            try:
                fc2_base0   = self.instrument.read_bits(0,   5, functioncode=2)
                fc2_base100 = self.instrument.read_bits(100, 5, functioncode=2)
                fc1_base100 = self.instrument.read_bits(100, 5, functioncode=1)
                self.status_message.emit(
                    "PLC-DBG",
                    f"DI probe: fc2@0={fc2_base0} fc2@100={fc2_base100} fc1@100={fc1_base100}"
                )
            except Exception as ex:
                self.status_message.emit("PLC-DBG", f"DI probe failed: {ex}")
            # debug

        except Exception as e:
            self.status_message.emit("PLC(경고)", f"폴링 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== 쓰기(버튼 클릭 반영) =========
    @Slot(str, bool)
    def update_port_state(self, btn_name: str, state: bool):
        self.status_message.emit("PLC-DBG", f"update_port_state({btn_name}={state})")

        if self.instrument is None:
            self.status_message.emit("PLC(오류)", "포트가 열려 있지 않습니다.")
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
        finally:
            self._busy = False
            self._mutex.unlock()

    # ============== RF (옵션) ==================
    def send_rfpower_command(self, pwm_value: int):
        if self.instrument is None:
            self.status_message.emit("PLC(오류)", "포트가 열려 있지 않습니다.")
            return
        self._busy = True
        self._mutex.lock()
        try:
            if COIL_ENABLE_DAC_CH0 is not None:
                self.instrument.write_bit(COIL_ENABLE_DAC_CH0, 1, functioncode=5)
            self.instrument.write_register(RF_DAC_ADDR_CH0, int(pwm_value), functioncode=6)
            self.status_message.emit("PLC > 전송", f"RF/DAC={int(pwm_value)} @D{RF_DAC_ADDR_CH0}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"RF 파워 송신 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    def read_rf_feedback(self) -> tuple[float, float] | tuple[None, None]:
        if self.instrument is None:
            return None, None
        try:
            f_raw = self.instrument.read_register(RF_ADC_FORWARD_ADDR, 0, functioncode=4, signed=False)
            r_raw = self.instrument.read_register(RF_ADC_REFLECT_ADDR, 0, functioncode=4, signed=False)
            forward_watt   = (f_raw / RF_ADC_MAX_COUNT) * RF_FORWARD_SCALING_MAX_WATT
            reflected_watt = (r_raw / RF_ADC_MAX_COUNT) * RF_REFLECTED_SCALING_MAX_WATT
            return forward_watt, reflected_watt
        except Exception as e:
            self.status_message.emit("PLC(경고)", f"RF 피드백 읽기 실패: {e}")
            return None, None

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
