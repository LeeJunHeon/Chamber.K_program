"""챔버K PLC 시뮬레이터 — Modbus RTU 슬레이브(바이트 단위) + 래더 동작 모델.

통신: 요청 CRC 가 틀리거나 국번이 다르면 응답하지 않는다(실제 슬레이브처럼). 함수 1·3·5·6·15·16,
  범위 밖 주소 → 예외 2, 모르는 함수 → 예외 1. 응답 지연 = 회선 시간(요청+응답 바이트 × 10 / 보레이트)
  + 스캔 지연(scan_delay_s) + USB 지연(usb_latency_s). 고장 주입은 SimDevice 의 fault(no_response·delay·
  garbage·truncate·bad_crc·disconnect).
래더 모델(0.1 s 단계로 시각을 따라간다 — 요청이 올 때와 advance_to 때):
  - 히터: 1차 열 모델 dT/dt = (HEAT_GAIN·p − (T − T_AMB)) / TAU, p = (MV − 400)/1200.
    TC2 = T + TC2_RISE·p(소자 쪽이 더 뜨겁다). 램프: 중간 목표 sv_ramp 가 SV 쪽으로 D20(카운트/초 = 0.1 °C/s)씩,
    남은 거리가 감속 구간(D27) 안이면 D28, PV 가 sv_ramp 보다 홀드백(D21) 넘게 뒤지면 멈춤.
    PI 제어로 MV(D41) = 400 + Kp·e + Ki·∫e, 400~D18. RUN·ITL·¬FAULT 일 때만, 아니면 D41 = 0.
    TC2 제어(M0004A 요구 + RUN + D36>0 + TC2 정상 → M0004B): 목표 SV2(D35), PV = TC2.
  - 트립: TC1 ≥ D26 또는 TC2 ≥ D34 → OT, TC 이상 주입 → TC_ERR(D10 = -1), 워치독 D14 가 10 s 안 바뀌면 → WD_ERR.
    셋 다 FAULT 래치 + RUN OFF. 리셋 M00043(래더가 스스로 0 으로) — 원인이 없어졌을 때만 해제.
  - 래더 강제: D27 = 100(10.0 °C), D18 ≤ 1600, SV ≤ D13, D36 = D34 − 500.
  - MV 인터록 M00032 = MV(M00003) ∧ 진공(¬ATM) ∧ ¬주입 고장. 도어 올림·내림은 소프트웨어 상호배타라 모델 안 함.
  - 진공 센서: ATM = 압력 > 700 Torr. Air·G1·G2·Water 는 설정값.
  - RF: DAC enable(M00200) ∧ D40>0 → 순방향 = D40/4000 × RF_GEN_FULL_W(1차 지연 0.5 s), 반사 = 순방향 × 반사 비율
    (+ 주입 W). ADC: 순방향 4000 = 594.5 W, 반사 4000 = 200 W(드라이버와 같은 스케일).
  - 압력: 1차 모델 — Vent 면 760 Torr 쪽(τ 20 s), MV·Turbo 면 기본 + 유량×K(τ 5 s), 러핑이면 0.05 Torr 쪽.
    MFC 시뮬레이터가 붙기 전에는 gas_sccm 으로 유량을, pressure_override 로 압력을 직접 넣는다.
  - 재기동(restart): 메모리 전체 초기화(D60 마커 포함) — 래더 초기값만 남는다.
"""
from __future__ import annotations

import math
import struct
from typing import Dict, List, Optional, Tuple

from v2.common.modbus_rtu import (
    READ_COILS, READ_REGS, WRITE_COIL, WRITE_COILS, WRITE_REG, WRITE_REGS, crc_ok, with_crc,
)
from v2.common.simdev import SimDevice
from v2.chk import devices as D

A = {a.name: a.addr for a in D.PLC_ADDRS}

# 모델 가정값(보고에 적는다)
T_AMB = 25.0            # °C
HEAT_GAIN = 1000.0      # p=1 에서 정상상태 상승 °C
TAU_HEAT = 600.0        # 열 시정수 s
TC2_RISE = 150.0        # p=1 에서 TC2 − TC1 °C
KP, KI = 8.0, 0.4       # 카운트/°C, 카운트/(°C·s)
WD_TON_S = float(D.HEATER_WD_TON_SEC)
RF_GEN_FULL_W = 600.0   # DAC 4000 = 600 W(YSR-06MF 정격)
RF_TAU_S = 0.5
RF_REF_RATIO = 0.01     # 정상 반사 1 %
COIL_N, REG_N = 1024, 256
LADDER_INIT = {13: 5000, 18: 1200, 20: 2, 21: 20, 26: 5500, 27: 100, 28: 1, 34: 11500}


class PlcSim(SimDevice):
    def __init__(self, slave: int = 1, baud: int = 115200, scan_delay_s: float = 0.005,
                 usb_latency_s: float = 0.002, start: float = 0.0) -> None:
        super().__init__("plc", latency_s=0.0)
        self.slave, self.baud = slave, baud
        self.scan_delay_s, self.usb_latency_s = scan_delay_s, usb_latency_s
        self.t = start
        self.frames: List[Tuple[float, bytes]] = []     # 받은 요청(시험: 쓰기가 나갔는지)
        # 주입·설정
        self.sensors = {"air": True, "g1": True, "g2": True, "water": True}
        self.itl_fault = False
        self.tc_fail = False
        self.tc2_fail = False
        self.ref_inject_w = 0.0
        self.gas_sccm = 0.0
        self.pressure_override: Optional[float] = None
        self.forced_regs: Dict[int, int] = {}          # 래더가 강제하는 값 흉내(되읽기 불일치 시험)
        self._init_memory()

    # ───────── 메모리 ─────────
    def _init_memory(self) -> None:
        self.coils = [False] * COIL_N
        self.regs = [0] * REG_N
        for k, v in LADDER_INIT.items():
            self.regs[k] = v
        self.T = T_AMB
        self.sv_ramp = T_AMB
        self.integ = 0.0
        self.rf_fwd_w = 0.0
        self.pressure = 5e-7          # 고진공에서 시작
        self.wd_last_val = self.regs[A["heater_wd"]]
        self.wd_last_change = self.t
        self.coils[A["mv"]] = True    # 진공 운전 중 상태로 시작
        self.coils[A["turbo"]] = True
        self.coils[A["rotary"]] = True
        self.coils[A["fv"]] = True
        self._ladder_outputs()

    def restart(self) -> None:
        """PLC 재기동 — D 영역·M 영역 초기화(래더 초기값만)."""
        self._init_memory()

    # ───────── 시각 진행 ─────────
    def advance_to(self, now: float) -> None:
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            self._step(dt)
            self.t += dt

    def _step(self, dt: float) -> None:
        r, c = self.regs, self.coils
        # 래더 강제값
        r[A["heater_slow_zone"]] = 100
        for k, v in self.forced_regs.items():
            r[k] = v
        r[A["heater_mv_limit"]] = min(r[A["heater_mv_limit"]], D.HEATER_DAC_PHYS_MAX)
        r[A["heater_sv2_max"]] = max(0, r[A["heater_ot2_limit"]] - 500)
        r[A["heater_sv"]] = min(r[A["heater_sv"]], r[A["heater_sv_limit"]])
        # 워치독(D14 변화 감시)
        if r[A["heater_wd"]] != self.wd_last_val:
            self.wd_last_val, self.wd_last_change = r[A["heater_wd"]], self.t
        wd_timeout = (self.t - self.wd_last_change) >= WD_TON_S
        # 리셋(래더가 스스로 0 으로)
        if c[A["heater_rst"]]:
            c[A["heater_rst"]] = False
            if not self.tc_fail and self.T < r[A["heater_ot_limit"]] / 10 and not wd_timeout:
                for k in ("heater_ot", "heater_tc_err", "heater_wd_err", "heater_fault"):
                    c[A[k]] = False
        # 트립
        tc2 = self.T + TC2_RISE * self._p()
        if self.T >= r[A["heater_ot_limit"]] / 10 or (not self.tc2_fail and tc2 >= r[A["heater_ot2_limit"]] / 10):
            c[A["heater_ot"]] = True
        if self.tc_fail:
            c[A["heater_tc_err"]] = True
        if c[A["heater_run"]] and wd_timeout:
            c[A["heater_wd_err"]] = True
        if c[A["heater_ot"]] or c[A["heater_tc_err"]] or c[A["heater_wd_err"]]:
            c[A["heater_fault"]] = True
        if c[A["heater_fault"]]:
            c[A["heater_run"]] = False
        c[A["heater_itl"]] = self.sensors["water"]
        run = c[A["heater_run"]] and c[A["heater_itl"]] and not c[A["heater_fault"]]
        if not c[A["heater_run"]]:
            c[A["heater_pv_sel"]] = False                 # ¬RUN 이면 래더가 리셋
        c[A["heater_pv_sel_eff"]] = bool(c[A["heater_pv_sel"]] and run and r[A["heater_sv2_max"]] > 0
                                         and not self.tc2_fail)
        # 램프·PID
        sv = r[A["heater_sv"]] / 10
        if run:
            dist = sv - self.sv_ramp
            zone = r[A["heater_slow_zone"]] / 10
            rate = (r[A["heater_slow_rate"]] if abs(dist) <= zone else r[A["heater_ramp_rate"]]) * 0.1   # °C/s
            held = (self.sv_ramp - self.T) > r[A["heater_holdback"]] / 10 and dist > 0
            if not held:
                step = max(-rate * dt, min(rate * dt, dist))
                self.sv_ramp += step
            if c[A["heater_pv_sel_eff"]]:
                target, pv = r[A["heater_sv2"]] / 10, tc2
            else:
                target, pv = self.sv_ramp, self.T
            e = target - pv
            self.integ = max(-2000.0, min(2000.0, self.integ + e * dt))
            mv = 400 + KP * e + KI * self.integ
            mv = int(max(400, min(r[A["heater_mv_limit"]], mv)))
            r[A["heater_mv"]] = mv
            c[A["heater_pid_run"]] = True
        else:
            self.sv_ramp = self.T
            self.integ = 0.0
            r[A["heater_mv"]] = 0
            c[A["heater_pid_run"]] = False
        p = self._p()
        self.T += (HEAT_GAIN * p - (self.T - T_AMB)) / TAU_HEAT * dt
        # RF
        target_w = (r[A["rf_dac"]] / D.RF_DAC_FULL_SCALE * RF_GEN_FULL_W) if c[A["rf_dac_enable"]] else 0.0
        self.rf_fwd_w += (target_w - self.rf_fwd_w) * min(1.0, dt / RF_TAU_S)
        # 압력
        if self.pressure_override is not None:
            self.pressure = self.pressure_override
        else:
            if c[A["vent"]]:
                tgt, tau = 760.0, 20.0
            elif c[A["mv"]] and c[A["turbo"]]:
                tgt, tau = 5e-7 + self.gas_sccm * 1e-4, 5.0
            elif c[A["rotary"]] and (c[A["rv"]] or c[A["fv"]]):
                tgt, tau = 0.05, 10.0
            else:
                tgt, tau = self.pressure, 1.0
            self.pressure += (tgt - self.pressure) * min(1.0, dt / tau)
        self._ladder_outputs()

    def _p(self) -> float:
        mv = self.regs[A["heater_mv"]]
        return 0.0 if mv < 400 else min(1.0, (mv - 400) / 1200.0)

    def _ladder_outputs(self) -> None:
        r, c = self.regs, self.coils
        tc2 = self.T + TC2_RISE * self._p()
        r[A["heater_pv"]] = 0xFFFF if self.tc_fail else int(round(self.T * 10)) & 0xFFFF
        r[A["heater_pv2"]] = 0xFFFF if self.tc2_fail else int(round(tc2 * 10)) & 0xFFFF
        r[A["heater_sv_ramp"]] = int(round(self.sv_ramp * 10))
        r[A["heater_cur_sv"]] = r[A["heater_sv_ramp"]]
        r[A["heater_pv_ctrl"]] = r[A["heater_pv2"]] if c[A["heater_pv_sel_eff"]] else r[A["heater_pv"]]
        fwd = self.rf_fwd_w
        ref = fwd * RF_REF_RATIO + (self.ref_inject_w if fwd > 1.0 else 0.0)
        r[A["rf_fwd_adc"]] = max(0, min(4000, int(round(fwd / 594.5 * 4000))))
        r[A["rf_ref_adc"]] = max(0, min(4000, int(round(ref / 200.0 * 4000))))
        c[A["atm"]] = self.pressure > 700.0
        for k, v in self.sensors.items():
            c[A[k]] = v
        c[A["mv_interlock"]] = bool(c[A["mv"]] and not c[A["atm"]] and not self.itl_fault)
        c[A["ion_run"]] = c[A["ion_remote_on"]]
        c[A["ion_lamp"]] = c[A["ion_remote_on"]]

    # ───────── Modbus 슬레이브 ─────────
    def respond(self, data: bytes, now: float):
        self.advance_to(now)
        self.frames.append((now, data))
        out = super().respond(data, now)
        if not out:
            return out
        (t, frame), = out
        if self._active("truncate", now):
            frame = frame[: max(1, len(frame) // 2)]
        if self._active("bad_crc", now):
            frame = frame[:-1] + bytes([frame[-1] ^ 0xFF])
        line = (len(data) + len(frame)) * 10.0 / self.baud
        return [(t + line + self.scan_delay_s + self.usb_latency_s, frame)]

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        if len(data) < 4 or not crc_ok(data) or data[0] != self.slave:
            return None
        fc = data[1]
        try:
            if fc in (READ_COILS, READ_REGS):
                addr, n = struct.unpack(">HH", data[2:6])
                if fc == READ_COILS:
                    if addr + n > COIL_N:
                        return self._exc(fc, 2)
                    bits = self.coils[addr:addr + n]
                    body = bytearray((n + 7) // 8)
                    for i, b in enumerate(bits):
                        if b:
                            body[i // 8] |= 1 << (i % 8)
                    return with_crc(bytes([self.slave, fc, len(body)]) + bytes(body))
                if addr + n > REG_N:
                    return self._exc(fc, 2)
                vals = self.regs[addr:addr + n]
                return with_crc(bytes([self.slave, fc, 2 * n]) + b"".join(struct.pack(">H", v & 0xFFFF) for v in vals))
            if fc == WRITE_COIL:
                addr, v = struct.unpack(">HH", data[2:6])
                if addr >= COIL_N or v not in (0, 0xFF00):
                    return self._exc(fc, 2 if addr >= COIL_N else 3)
                self.coils[addr] = v == 0xFF00
                self._step(0.0)
                return with_crc(data[:6])
            if fc == WRITE_REG:
                addr, v = struct.unpack(">HH", data[2:6])
                if addr >= REG_N:
                    return self._exc(fc, 2)
                self.regs[addr] = v
                self._step(0.0)
                return with_crc(data[:6])
            if fc == WRITE_COILS:
                addr, n, nb = struct.unpack(">HHB", data[2:7])
                if addr + n > COIL_N:
                    return self._exc(fc, 2)
                bits = data[7:7 + nb]
                for i in range(n):
                    self.coils[addr + i] = bool(bits[i // 8] >> (i % 8) & 1)
                self._step(0.0)
                return with_crc(data[:6])
            if fc == WRITE_REGS:
                addr, n, nb = struct.unpack(">HHB", data[2:7])
                if addr + n > REG_N:
                    return self._exc(fc, 2)
                for i in range(n):
                    self.regs[addr + i] = struct.unpack(">H", data[7 + 2 * i:9 + 2 * i])[0]
                self._step(0.0)
                return with_crc(data[:6])
        except struct.error:
            return self._exc(fc, 3)
        return self._exc(fc, 1)

    def _exc(self, fc: int, code: int) -> bytes:
        return with_crc(bytes([self.slave, fc | 0x80, code]))

    # ───────── 시험 도우미 ─────────
    def write_funcs_seen(self) -> List[int]:
        return [f[1] for _, f in self.frames if len(f) > 1 and f[1] in (5, 6, 15, 16)]

    def coil(self, name: str) -> bool:
        return self.coils[A[name]]

    def reg(self, name: str) -> int:
        return self.regs[A[name]]
