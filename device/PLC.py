# device/PLC.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, List, Tuple
from PyQt6.QtCore import QObject, QThread, pyqtSignal as Signal, pyqtSlot as Slot, QTimer, QMutex
import minimalmodbus
import time

from lib.config import (
    PLC_PORT, PLC_SLAVE_ID, PLC_BAUD,
    PLC_TIMEOUT, PLC_COIL_MAP,
    PLC_SENSOR_BITS, RF_ADC_FORWARD_ADDR,
    RF_ADC_REFLECT_ADDR, RF_ADC_MAX_COUNT,
    RF_DAC_ADDR_CH0, COIL_ENABLE_DAC_CH0,
    RF_FORWARD_SCALING_MAX_WATT,
    RF_REFLECTED_SCALING_MAX_WATT,
    PLC_MV_COIL, PLC_MV_INTERLOCK_COIL,
    HEATER_ENABLED, HEATER_TEMP_SCALE, HEATER_WD_PERIOD_MS,
    HEATER_MV_MIN, HEATER_MV_ABS_MAX, HEATER_MV_LIMIT,
    HEATER_CURRENT_SLOPE, HEATER_CURRENT_ZERO,
    HEATER_REG_BLOCK_START, HEATER_REG_BLOCK_COUNT,
    HEATER_REG_PV, HEATER_REG_SV, HEATER_REG_SV_LIMIT,
    HEATER_REG_WD, HEATER_REG_CUR_SV, HEATER_REG_PID_ERR, HEATER_REG_MV,
    HEATER_REG_MV_LIMIT, HEATER_REG_SV_RAMP, HEATER_REG_RAMP_RATE,
    HEATER_REG_HOLDBACK, HEATER_REG_OT_LIMIT, HEATER_REG_SLOW_ZONE,
    HEATER_REG_SLOW_RATE,
    HEATER_PUSH_CONFIG, HEATER_SV_LIMIT_C, HEATER_RAMP_RATE_C_PER_MIN,
    HEATER_HOLDBACK_C, HEATER_OT_LIMIT_C, HEATER_SLOW_ZONE_C,
    HEATER_SLOW_RATE_C_PER_MIN,
    HEATER_COIL_BASE, HEATER_COIL_COUNT,
    HEATER_COIL_RUN, HEATER_COIL_ITL, HEATER_COIL_FAULT, HEATER_COIL_RST,
    HEATER_COIL_OT, HEATER_COIL_TC_ERR, HEATER_COIL_WD_ERR,
    HEATER_COIL_AT_REQ, HEATER_COIL_AT_DONE, HEATER_COIL_PID_RUN,
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
    plc_disconnected = Signal(int)   # ★ () → (int)로 변경: 경과 초 전달
    plc_reconnected = Signal()       # ★ 추가
    update_heater_status = Signal(dict)    # ★ 히터 상태 일괄 전달
    heater_fault        = Signal(str)      # ★ 히터 이상 (원인 문자열)

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
        
        # ★ 추가: PLC 끊김 60초 감지 (알림 1회용)
        self._disconnect_since = None      # 폴링 실패가 처음 감지된 시각(monotonic)
        self._disconnect_notified = False  # 끊김 알림을 이미 보냈는지
        self._DISCONNECT_GRACE_S = 60.0    # 끊김 확정까지 유예 시간(초)

        # ★ 히터: 워치독(하트비트) — 폴링과 독립 타이머
        self._heater_wd = 0
        self._heater_wd_timer = QTimer(self)
        self._heater_wd_timer.setInterval(int(HEATER_WD_PERIOD_MS))
        self._heater_wd_timer.timeout.connect(self._kick_heater_watchdog)

        # ★ 히터: 상태 캐시 / 이상 중복 알림 방지
        self._heater_last: Dict[str, object] = {}
        self._heater_fault_notified = False
        # 첫 폴링 여부.
        #  PLC의 이상 비트는 SET 코일(래치)이라 프로그램을 껐다 켜도 남아 있다.
        #  시작 직후 읽은 이상은 '지금 발생'이 아니라 '이전부터 있던 것'이므로
        #  문구를 구분해야 "재시작했더니 알림이 왔다"는 혼선을 막을 수 있다.
        self._heater_first_poll = True

    # 상위와 동일 API 유지
    def set_rf_controller(self, rf_controller):
        self.rf_controller = rf_controller

    @Slot()
    def clear_fault_latch(self):
        """새 공정 시작 전에 PLC 코일 읽기 실패 래치를 해제."""
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
            self.instrument.serial.timeout  = PLC_TIMEOUT

            self.instrument.close_port_after_each_call = False
            self.instrument.clear_buffers_before_each_transaction = True
            self.instrument.handle_local_echo = False

            self._is_running = True
            self.polling_timer.start()
            if HEATER_ENABLED:
                self._heater_wd_timer.start()      # ★ 하트비트 시작

            # 접속할 때마다 JSON의 한계값을 PLC에 복구한다.
            # 여기서 무슨 일이 나도 '연결 실패'로 떨어뜨리지 않는다.
            try:
                self._push_heater_config()
            except Exception as ex:
                self.status_message.emit("히터(경고)", f"설정 적용 중 예외: {ex}")

            self.status_message.emit("PLC", f"연결 성공: {PLC_PORT}, ID={PLC_SLAVE_ID}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"연결 실패: {e}")

    @Slot()
    def cleanup(self):
        self.polling_timer.stop()
        self._heater_wd_timer.stop()               # ★
        try:
            if self.instrument:                    # ★ 종료 전 히터 정지
                self.instrument.write_bit(HEATER_COIL_RUN, 0, functioncode=5)
        except Exception:
            pass
        
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
                # 코일 읽기 실패는 PLC 통신 이상으로 간주 → 공정 중단(재시작) 트리거
                if not getattr(self, "_coil_read_fail_latched", False):
                    self._coil_read_fail_latched = True
                    self.status_message.emit("재시작", f"PLC Coils 읽기 실패 [{s}..{e}]: {ex} → 공정을 중단합니다.")
                else:
                    # 이미 중단 트리거가 걸린 상태면 경고만 남김(로그 스팸 방지)
                    self.status_message.emit("PLC(경고)", f"Coils 읽기 실패(반복) [{s}..{e}]: {ex}")

        return result

    def _safe_read_discrete_inputs(self, start_addr: int, count: int) -> List[bool]:
        assert self.instrument is not None
        return self.instrument.read_bits(start_addr, count, functioncode=1)

    # ============== 폴링 ======================
    @Slot()
    def _poll_status(self):
        if not self._is_running or self._busy:
            return
        if self.instrument is None:
            self._check_disconnect_timeout()   # ★ instrument 없을 때도 끊김 감지
            return
        self._busy = True
        self._mutex.lock()
        try:
            # 1) 코일 상태(버튼) 동기화 — 비연속 주소를 그룹 폴링
            coil_addr_list = list(PLC_COIL_MAP.values())
            addr_to_state = self._read_coils_grouped(coil_addr_list)

            # ★ 코일 읽기가 전부 실패(빈 dict)면 통신 두절로 간주
            if coil_addr_list and not addr_to_state:
                raise RuntimeError("PLC 코일 읽기 전부 실패")

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

            # 3) ★ 히터 상태 읽기
            if HEATER_ENABLED:
                self._poll_heater()

            # ★ 폴링 성공 → 끊김 알림을 보낸 적 있으면 재연결 알림 1회
            if self._disconnect_notified:
                self.plc_reconnected.emit()
            self._disconnect_since = None
            self._disconnect_notified = False

        except Exception as e:
            self.status_message.emit("PLC(경고)", f"폴링 실패: {e}")
            # ★ 추가: 폴링 실패 → 60초 끊김 감지
            self._check_disconnect_timeout()
        finally:
            self._busy = False
            self._mutex.unlock()

    def _check_disconnect_timeout(self):
        """폴링 실패가 60초 이상 지속되면 Google Chat 알림을 1회 방출."""
        now = time.monotonic()
        if self._disconnect_since is None:
            # 실패가 처음 감지된 순간 → 시각 기록
            self._disconnect_since = now
            return
        if (not self._disconnect_notified
                and (now - self._disconnect_since) >= self._DISCONNECT_GRACE_S):
            self._disconnect_notified = True
            self.plc_disconnected.emit(int(now - self._disconnect_since))

    # ============== 쓰기(버튼 클릭 반영) =========
    @Slot(str, bool)
    def update_port_state(self, btn_name: str, state: bool):
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

    # ============== 히터 (PLC 내장 PID) ==============
    def _push_heater_config(self):
        """config_user.json의 히터 한계값을 PLC D레지스터에 기록한다.

        PLC를 재기동하면 D영역이 초기화되므로, 접속할 때마다 다시 써서
        JSON을 단일 출처로 유지한다. 실패해도 연결 자체는 유지한다.
        """
        if not HEATER_ENABLED or not HEATER_PUSH_CONFIG:
            return
        if self.instrument is None:
            return

        def _clamp(v: int) -> int:
            return max(0, min(32767, int(v)))

        def _rate(c_per_min: float) -> int:
            # 1카운트/초 = 6°C/min
            return max(1, int(round(float(c_per_min) / 6.0)))

        # (레지스터, raw, 표시이름, 표시값)
        items = [
            (HEATER_REG_SV_LIMIT,  _clamp(round(HEATER_SV_LIMIT_C * 10)),
             "SV상한",   f"{HEATER_SV_LIMIT_C:.1f}°C"),
            (HEATER_REG_MV_LIMIT,  _clamp(HEATER_MV_LIMIT),
             "DAC상한",  f"{int(HEATER_MV_LIMIT)}"),
            (HEATER_REG_RAMP_RATE, _clamp(_rate(HEATER_RAMP_RATE_C_PER_MIN)),
             "램프",     f"{HEATER_RAMP_RATE_C_PER_MIN:.0f}°C/min"),
            (HEATER_REG_HOLDBACK,  _clamp(round(HEATER_HOLDBACK_C * 10)),
             "홀드백",   f"{HEATER_HOLDBACK_C:.1f}°C"),
            (HEATER_REG_OT_LIMIT,  _clamp(round(HEATER_OT_LIMIT_C * 10)),
             "OT",       f"{HEATER_OT_LIMIT_C:.1f}°C"),
            (HEATER_REG_SLOW_ZONE, _clamp(round(HEATER_SLOW_ZONE_C * 10)),
             "감속구간", f"{HEATER_SLOW_ZONE_C:.1f}°C"),
            (HEATER_REG_SLOW_RATE, _clamp(_rate(HEATER_SLOW_RATE_C_PER_MIN)),
             "감속램프", f"{HEATER_SLOW_RATE_C_PER_MIN:.0f}°C/min"),
        ]

        self._busy = True
        self._mutex.lock()
        try:
            failed = []
            for addr, raw, name, _disp in items:
                try:
                    self.instrument.write_register(addr, raw, functioncode=6)
                except Exception as ex:
                    failed.append(f"{name}(D{addr:05d}: {ex})")

            if failed:
                self.status_message.emit(
                    "히터(경고)", "설정 쓰기 실패: " + ", ".join(failed))

            # 되읽어 검증 — 래더가 값을 강제하고 있으면 여기서 드러난다
            try:
                lo = min(a for a, *_ in items)
                hi = max(a for a, *_ in items)
                back = self.instrument.read_registers(lo, hi - lo + 1, functioncode=3)
                mismatch = [f"{name} 요청 {raw} → 실제 {back[addr - lo]}"
                            for addr, raw, name, _d in items
                            if back[addr - lo] != raw]
                if mismatch:
                    self.status_message.emit(
                        "히터(경고)",
                        "설정값이 되읽기와 다릅니다 (래더가 강제 중일 수 있음): "
                        + ", ".join(mismatch))
            except Exception as ex:
                self.status_message.emit("히터(경고)", f"설정 되읽기 실패: {ex}")

            amp = max(0.0, HEATER_CURRENT_SLOPE * (HEATER_MV_LIMIT - HEATER_CURRENT_ZERO))
            self.status_message.emit(
                "히터",
                f"설정 적용: DAC상한 {int(HEATER_MV_LIMIT)}(약 {amp:.1f}A)"
                f" · 램프 {HEATER_RAMP_RATE_C_PER_MIN:.0f}°C/min"
                f" · 홀드백 {HEATER_HOLDBACK_C:.1f}°C"
                f" · OT {HEATER_OT_LIMIT_C:.1f}°C"
                f" · SV상한 {HEATER_SV_LIMIT_C:.1f}°C")
        finally:
            self._busy = False
            self._mutex.unlock()

    def _poll_heater(self):
        """D00010~D00028 + D00041 + 코일 64~73 을 읽어 update_heater_status로 발행.
        주의: 호출자(_poll_status)가 이미 _mutex를 잡고 있으므로 여기서 잠그지 않는다."""
        try:
            blk  = self.instrument.read_registers(
                HEATER_REG_BLOCK_START, HEATER_REG_BLOCK_COUNT, functioncode=3)
            mv   = self.instrument.read_register(HEATER_REG_MV, 0, functioncode=3, signed=False)
            bits = self.instrument.read_bits(HEATER_COIL_BASE, HEATER_COIL_COUNT, functioncode=1)
        except Exception as ex:
            self.status_message.emit("PLC(경고)", f"히터 읽기 실패: {ex}")
            return

        def _reg(addr: int) -> int:
            return blk[addr - HEATER_REG_BLOCK_START]

        def _signed(v: int) -> int:
            return v - 65536 if v >= 32768 else v

        raw_pv = _signed(_reg(HEATER_REG_PV))
        tc_bad = (raw_pv == -1)                       # hFFFF = 단선/모듈이상

        def _bit(coil: int) -> bool:
            return bool(bits[coil - HEATER_COIL_BASE])

        # 출력%는 설정 파일의 절대 최대치(HEATER_MV_ABS_MAX)를 100%로 계산한다.
        # 운전 상한(D00018)은 운전 중 바뀔 수 있어 분모로 쓰면 기준이 흔들린다.
        # (상한을 낮췄는데 같은 출력에서 %가 올라가는 모순 방지)
        mv_limit = _reg(HEATER_REG_MV_LIMIT)
        span = HEATER_MV_ABS_MAX - HEATER_MV_MIN
        if mv < HEATER_MV_MIN or span <= 0:
            _mv_pct = 0.0
        else:
            _mv_pct = max(0.0, min(100.0, (mv - HEATER_MV_MIN) / span * 100.0))

        # 현재 운전 상한이 절대 최대치의 몇 %인지 (같은 기준의 참고값)
        _limit_pct = (
            max(0.0, min(100.0, (mv_limit - HEATER_MV_MIN) / span * 100.0))
            if span > 0 else 0.0
        )

        st = {
            'ok':        not tc_bad,
            'pv':        None if tc_bad else raw_pv * HEATER_TEMP_SCALE,
            'sv':        _reg(HEATER_REG_SV)       * HEATER_TEMP_SCALE,
            'sv_limit':  _reg(HEATER_REG_SV_LIMIT) * HEATER_TEMP_SCALE,
            'cur_sv':    _reg(HEATER_REG_CUR_SV)   * HEATER_TEMP_SCALE,
            'pid_err':   _reg(HEATER_REG_PID_ERR),
            'mv':        mv,
            'mv_pct':    _mv_pct,
            'mv_limit':  mv_limit,
            'limit_pct': _limit_pct,
            'sv_ramp':   _reg(HEATER_REG_SV_RAMP)   * HEATER_TEMP_SCALE,
            'ramp_rate': _reg(HEATER_REG_RAMP_RATE) * 6,          # °C/min 환산
            'holdback':  _reg(HEATER_REG_HOLDBACK)  * HEATER_TEMP_SCALE,
            'ot_limit':  _reg(HEATER_REG_OT_LIMIT)  * HEATER_TEMP_SCALE,
            'est_current': 0.0 if mv < HEATER_MV_MIN
                           else max(0.0, HEATER_CURRENT_SLOPE * (mv - HEATER_CURRENT_ZERO)),
            'run':       _bit(HEATER_COIL_RUN),
            'itl':       _bit(HEATER_COIL_ITL),
            'fault':     _bit(HEATER_COIL_FAULT),
            'ot':        _bit(HEATER_COIL_OT),
            'tc_err':    _bit(HEATER_COIL_TC_ERR),
            'wd_err':    _bit(HEATER_COIL_WD_ERR),
            'at_done':   _bit(HEATER_COIL_AT_DONE),
            'pid_run':   _bit(HEATER_COIL_PID_RUN),
        }
        self._heater_last = st
        self.update_heater_status.emit(st)

        # 이상 발생 시 1회만 알림 (원인 우선순위: 과온 > 센서 > 워치독)
        if st['fault']:
            if not self._heater_fault_notified:
                self._heater_fault_notified = True
                if st['ot']:
                    why = "히터 과온 트립"
                elif st['tc_err']:
                    why = "히터 온도센서 이상(단선/모듈)"
                elif st['wd_err']:
                    why = "히터 통신 워치독 타임아웃"
                else:
                    why = f"히터 이상 (PID err={st['pid_err']})"

                # 첫 폴링에서 잡힌 이상은 PLC에 이미 래치되어 있던 것이다.
                if self._heater_first_poll:
                    why = f"{why} — 기존 래치 상태 (프로그램 시작 시 확인됨)"

                self.heater_fault.emit(why)
        else:
            # 해제 시각도 남겨야 지속 시간을 알 수 있다.
            # 순간적 접촉 불량인지 지속 고장인지 구분하는 근거가 된다.
            if self._heater_fault_notified:
                self.status_message.emit("히터", "이상 해제됨 (정상 복귀)")
            self._heater_fault_notified = False

        # 이상 유무와 무관하게 첫 폴링은 한 번뿐이므로 여기서 해제한다.
        self._heater_first_poll = False

    @Slot()
    def _kick_heater_watchdog(self):
        """PLC에 '파이썬 살아있음'을 알리는 하트비트. 값이 바뀌기만 하면 됨."""
        if self.instrument is None or self._busy:
            return
        self._busy = True
        self._mutex.lock()
        try:
            self._heater_wd = (self._heater_wd + 1) & 0x7FFF
            self.instrument.write_register(HEATER_REG_WD, self._heater_wd, functioncode=6)
        except Exception:
            pass          # 실패해도 조용히 — 다음 주기에 재시도, PLC가 알아서 트립
        finally:
            self._busy = False
            self._mutex.unlock()

    @Slot(float)
    def set_heater_target(self, temp_c: float):
        """목표 온도 쓰기 (PLC가 HEATER_SV_LIMIT으로 한 번 더 클램프함)."""
        if self.instrument is None:
            self.status_message.emit("PLC(오류)", "포트가 열려 있지 않습니다.")
            return
        self._busy = True
        self._mutex.lock()
        try:
            raw = int(round(float(temp_c) / HEATER_TEMP_SCALE))
            raw = max(0, min(32767, raw))
            self.instrument.write_register(HEATER_REG_SV, raw, functioncode=6)
            self.status_message.emit("히터", f"목표 온도 {temp_c:.1f}°C 설정 (raw={raw})")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"히터 목표 온도 쓰기 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    @Slot(int)
    def set_heater_ramp_rate(self, counts_per_sec: int):
        """레시피 스텝마다 램프 속도(D00020)를 바꾼다. 1 카운트/초 = 6°C/min."""
        if self.instrument is None:
            return
        self._busy = True
        self._mutex.lock()
        try:
            v = max(1, min(100, int(counts_per_sec)))
            self.instrument.write_register(HEATER_REG_RAMP_RATE, v, functioncode=6)
            self.status_message.emit("히터", f"램프 속도 {v * 6}°C/min 설정 (raw={v})")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"히터 램프 속도 쓰기 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    @Slot(bool)
    def set_heater_run(self, on: bool):
        if self.instrument is None:
            return
        self._busy = True
        self._mutex.lock()
        try:
            self.instrument.write_bit(HEATER_COIL_RUN, int(bool(on)), functioncode=5)
            self.status_message.emit("히터", f"운전 {'ON' if on else 'OFF'}")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"히터 운전 쓰기 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    @Slot()
    def reset_heater_fault(self):
        """이상 리셋. PLC 래더가 자기 리셋하므로 0으로 되돌릴 필요 없음."""
        if self.instrument is None:
            return
        self._busy = True
        self._mutex.lock()
        try:
            self.instrument.write_bit(HEATER_COIL_RST, 1, functioncode=5)
            self.status_message.emit("히터", "이상 리셋 요청")
        except Exception as e:
            self.status_message.emit("PLC(오류)", f"히터 리셋 실패: {e}")
        finally:
            self._busy = False
            self._mutex.unlock()

    def get_heater_status(self) -> Dict[str, object]:
        """마지막 폴링 결과 캐시 (동기 조회용)."""
        return dict(self._heater_last)

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

    # ============== 메인밸브 상태(공정 시작 인터록) ==============
    def read_main_valve_state(self) -> tuple[bool, bool] | tuple[None, None]:
        """메인밸브 상태를 동기 읽기로 반환: (MV, MV_INTERLOCK).
        둘 다 True여야 메인밸브가 실제로 열린 상태(공정 시작 허용).
        포트 미연결/읽기 실패 시 (None, None)."""
        if self.instrument is None:
            return None, None
        self._busy = True
        self._mutex.lock()
        try:
            mv  = bool(self.instrument.read_bit(PLC_MV_COIL,          functioncode=1))
            itl = bool(self.instrument.read_bit(PLC_MV_INTERLOCK_COIL, functioncode=1))
            return mv, itl
        except Exception as e:
            self.status_message.emit("PLC(경고)", f"메인밸브 상태 읽기 실패: {e}")
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

            # ★ 히터 정지
            if HEATER_ENABLED:
                try:
                    self.instrument.write_bit(HEATER_COIL_RUN, 0, functioncode=5)
                except Exception:
                    pass

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
