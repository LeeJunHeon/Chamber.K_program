# device/PLC.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Tuple
from PyQt6.QtCore import QObject, QThread, pyqtSignal as Signal, pyqtSlot as Slot, QTimer, QMutex

import time
import minimalmodbus
import serial.tools.list_ports

from lib.config import (
    PLC_PORT, PLC_SLAVE_ID, PLC_BAUD, PLC_TIMEOUT, PLC_COIL_MAP, PLC_SENSOR_BITS, RF_ADC_FORWARD_ADDR, RF_ADC_REFLECT_ADDR, RF_ADC_MAX_COUNT,
    RF_DAC_ADDR_CH0, COIL_ENABLE_DAC_CH0, RF_FORWARD_SCALING_MAX_WATT, RF_REFLECTED_SCALING_MAX_WATT, PLC_POLL_SINGLE_FLIGHT, PLC_POLL_TIMEOUT_S,
    PLC_WATCHDOG_INTERVAL_MS, PLC_RECONNECT_BACKOFF_START_MS, PLC_RECONNECT_BACKOFF_MAX_MS, PLC_LINK_LOST_FAIL_MS,
    PLC_RECONNECT_BACKOFF_PORT_MISSING_START_MS, PLC_RECONNECT_BACKOFF_PORT_MISSING_MAX_MS, PLC_FAIL_PROCESS_ON_LINK_LOST,
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

        # ✅ 공정 중(critical)일 때만 링크 끊김 FAIL 트리거
        self._process_critical = False

        # ✅ 재연결/백오프/끊김 누적
        self._reconnect_backoff_ms = int(PLC_RECONNECT_BACKOFF_START_MS)
        self._reconnect_pending = False
        self._last_port_missing = False
        self._link_lost_since_ms = None
        self._link_fail_latched = False

        # ✅ 재연결 타이머(singleShot)
        self._reconnect_timer = QTimer(self)
        self._reconnect_timer.setSingleShot(True)
        self._reconnect_timer.timeout.connect(self._try_reconnect)

    # 상위와 동일 API 유지
    def set_rf_controller(self, rf_controller):
        self.rf_controller = rf_controller

    @Slot()
    def clear_fault_latch(self):
        """새 공정 시작 전에 PLC 코일 읽기 실패 래치를 해제."""
        self._coil_read_fail_latched = False

    @Slot(bool)
    def set_process_critical(self, on: bool):
        self._process_critical = bool(on)
        if on:
            # 공정 구간 진입 시 끊김 타이머 초기화
            self._link_lost_since_ms = None
            self._link_fail_latched = False
            self._coil_read_fail_latched = False

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
            self.instrument.serial.timeout  = float(PLC_POLL_TIMEOUT_S)

            self.instrument.close_port_after_each_call = False
            self.instrument.clear_buffers_before_each_transaction = True
            self.instrument.handle_local_echo = False

            self._is_running = True
            self.polling_timer.start()
            self.status_message.emit("PLC", f"연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"연결 실패: {e}")

    @Slot()
    def cleanup(self):
        self.polling_timer.stop()
        if self._reconnect_timer.isActive():
            self._reconnect_timer.stop()

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
                # ✅ 즉시 재시작 X → 재연결로 전환 (공정 중 지속 끊김이면 _note_link_lost...가 재시작 올림)
                if not self._coil_read_fail_latched:
                    self._coil_read_fail_latched = True
                    self.status_message.emit("PLC(경고)", f"PLC Coils 읽기 실패 [{s}..{e}]: {ex}")
                else:
                    self.status_message.emit("PLC(경고)", f"Coils 읽기 실패(반복) [{s}..{e}]: {ex}")

                self._on_comm_error(f"PLC Coils read [{s}..{e}]", ex)

        return result

    def _safe_read_discrete_inputs(self, start_addr: int, count: int) -> List[bool]:
        assert self.instrument is not None
        return self.instrument.read_bits(start_addr, count, functioncode=1)
    
    def _now_ms(self) -> int:
        return int(time.monotonic() * 1000)

    def _available_ports(self) -> set[str]:
        # COM 고정이지만 "포트가 OS에서 사라짐" 감지용
        return {p.device.upper().replace("\\\\.\\", "") for p in serial.tools.list_ports.comports()}

    def _note_link_lost_and_maybe_fail(self, reason: str):
        if self._link_lost_since_ms is None:
            self._link_lost_since_ms = self._now_ms()
            self._link_fail_latched = False

        if (not self._link_fail_latched) and self._process_critical and PLC_FAIL_PROCESS_ON_LINK_LOST:
            elapsed = self._now_ms() - self._link_lost_since_ms
            if elapsed >= int(PLC_LINK_LOST_FAIL_MS):
                self._link_fail_latched = True
                self.status_message.emit("재시작", f"PLC 링크 끊김 {elapsed}ms 지속({reason}) → 공정을 중단합니다.")

    def _schedule_reconnect(self, reason: str):
        if self._reconnect_timer.isActive():
            return

        ports = self._available_ports()
        port_missing = (PLC_PORT.upper() not in ports)
        self._last_port_missing = port_missing

        if port_missing:
            backoff = min(
                max(self._reconnect_backoff_ms, int(PLC_RECONNECT_BACKOFF_PORT_MISSING_START_MS)),
                int(PLC_RECONNECT_BACKOFF_PORT_MISSING_MAX_MS),
            )
        else:
            backoff = min(
                max(self._reconnect_backoff_ms, int(PLC_RECONNECT_BACKOFF_START_MS)),
                int(PLC_RECONNECT_BACKOFF_MAX_MS),
            )

        self.status_message.emit("PLC(경고)", f"{reason} → 재연결 대기 {backoff}ms (ports={sorted(ports)})")
        self._reconnect_timer.start(backoff)

        # 다음 backoff 증가
        self._reconnect_backoff_ms = min(
            backoff * 2,
            int(PLC_RECONNECT_BACKOFF_PORT_MISSING_MAX_MS) if port_missing else int(PLC_RECONNECT_BACKOFF_MAX_MS)
        )

    def _try_reconnect(self):
        # 포트가 없으면 다시 스케줄
        ports = self._available_ports()
        if PLC_PORT.upper() not in ports:
            self._note_link_lost_and_maybe_fail("port missing")
            self._schedule_reconnect("PLC 포트 미검출")
            return

        try:
            inst = minimalmodbus.Instrument(PLC_PORT, PLC_SLAVE_ID, mode=minimalmodbus.MODE_RTU)
            inst.serial.baudrate = PLC_BAUD
            inst.serial.bytesize = 8
            inst.serial.parity   = 'N'
            inst.serial.stopbits = 1
            inst.serial.timeout  = float(PLC_POLL_TIMEOUT_S)  # ✅ 폴링은 짧게

            inst.close_port_after_each_call = False
            inst.clear_buffers_before_each_transaction = True
            inst.handle_local_echo = False

            self.instrument = inst
            self._reconnect_backoff_ms = int(PLC_RECONNECT_BACKOFF_START_MS)
            self._link_lost_since_ms = None
            self._link_fail_latched = False
            self._coil_read_fail_latched = False
            self.status_message.emit("PLC", f"재연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID}")
        except Exception as e:
            self.instrument = None
            self._note_link_lost_and_maybe_fail(f"reconnect failed: {e}")
            self._schedule_reconnect(f"PLC 재연결 실패: {e}")

    def _on_comm_error(self, where: str, ex: Exception):
        # 현재 instrument를 폐기하고 재연결 루프로 넘김
        try:
            if self.instrument and self.instrument.serial and self.instrument.serial.is_open:
                self.instrument.serial.close()
        except Exception:
            pass
        self.instrument = None
        self._note_link_lost_and_maybe_fail(where)
        self._schedule_reconnect(f"{where}: {ex}")

    # ============== 폴링 ======================
    @Slot()
    def _poll_status(self):
        if not self._is_running:
            return
        if PLC_POLL_SINGLE_FLIGHT and self._busy:
            return

        # ✅ 연결이 없으면 재연결만 스케줄하고 이번 폴은 종료
        if self.instrument is None:
            self._note_link_lost_and_maybe_fail("poll: instrument None")
            self._schedule_reconnect("PLC 연결 끊김")
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
            self.status_message.emit("PLC(오류)", "포트가 열려 있지 않습니다.")
            self._note_link_lost_and_maybe_fail("write: instrument None")
            self._schedule_reconnect("PLC 연결 끊김(쓰기)")
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
            try:
                echo = self.instrument.read_register(RF_DAC_ADDR_CH0, 0, functioncode=3, signed=False)
                self.status_message.emit("PLC > 확인", f"DAC echo={echo}")
            except Exception as _:
                pass
            self.status_message.emit("PLC > 전송", f"RF/DAC={int(pwm_value)} @D{RF_DAC_ADDR_CH0}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"RF 파워 송신 실패: {e}")
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
