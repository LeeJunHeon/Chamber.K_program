"""시뮬레이터 공용 챔버 모델 — PLC·IG·DC(이후 MFC·RF Pulse) 시뮬레이터가 압력·밸브·가스·플라즈마 상태를 함께 쓴다.

시각은 advance_to(now) 로 0.1 s 단계씩 따라간다(여러 시뮬레이터가 불러도 한 번만 진행).
밸브 상태는 PLC 시뮬레이터가 set_valves 로 넣는다. 가스 유량은 MFC 시뮬레이터가 붙기 전까지 gas_sccm 에 직접.

압력(Torr) — 1차 모델(가정값):
  Vent 열림            → 760 쪽으로, τ 20 s
  MV·Turbo            → BASE_TORR + 총 유량 × TORR_PER_SCCM / 유효 배기 쪽으로, τ 5 s   (20 sccm·완전 열림 ≈ 2 mTorr)
                         유효 배기 = THROTTLE_MIN + (1 − THROTTLE_MIN) × throttle(압력 제어 밸브 열림 0~1, MFC 시뮬레이터가 넣음)
  러핑(Rotary·RV|FV)   → 0.05 쪽으로, τ 10 s
  그 밖               → 그대로
  pressure_override 가 있으면 그 값 고정.
DC 플라즈마 — 압력으로 전압이 정해지는 단순 V-I(정전류 전원):
  출력 꺼짐 → V 0, I 0
  점화 조건(가스 있음·압력 IGNITE_MIN~IGNITE_MAX mTorr·¬점화 실패 주입) 아니면 → 무부하: V = 전압 설정, I 0
  점화 → 유지 전압 Vp = 350 + 150/√P(mTorr) (≤ 900), V = Vp + 50·I
         V 가 전압 설정보다 높으면 전압 한계(CV): V = 설정, I = (설정 − Vp)/50 (0 이하면 소멸)
  점화 조건이 깨지면(가스 없음·압력 범위 밖·점화 실패 주입) 소멸 → 무부하.
  고장 주입: ignite_fail(점화 실패), low_current(전류 10 %), v_limit(유지 전압 +300 V → 전압 한계).
"""
from __future__ import annotations

import math
from typing import Dict, Optional

BASE_TORR = 5e-7
TORR_PER_SCCM = 1e-4
ROUGH_TORR = 0.05
THROTTLE_MIN = 0.02      # 밸브를 다 닫아도 남는 배기(누설·바이패스)
ATM_TORR = 760.0
IGNITE_MIN_MTORR = 0.5
IGNITE_MAX_MTORR = 100.0
VP_MAX = 900.0
R_PLASMA = 50.0          # V/A


class Chamber:
    def __init__(self, start: float = 0.0) -> None:
        self.t = start
        self.pressure = BASE_TORR
        self.pressure_override: Optional[float] = None
        self.valves: Dict[str, bool] = {"mv": True, "turbo": True, "rotary": True, "rv": False, "fv": True,
                                        "vent": False}
        self.gas_sccm: Dict[str, float] = {"ar": 0.0, "o2": 0.0}
        self.throttle = 1.0                     # 압력 제어 밸브 열림(0~1) — MFC(GPC3000) 시뮬레이터가 넣는다
        self.ignite_fail = False
        self.low_current = False
        self.v_limit = False
        self.dc_ignited = False

    # ───────── 상태 넣기 ─────────
    def set_valves(self, **v: bool) -> None:
        self.valves.update(v)

    def total_sccm(self) -> float:
        return sum(self.gas_sccm.values())

    @property
    def pressure_mtorr(self) -> float:
        return self.pressure * 1000.0

    # ───────── 시각 ─────────
    def advance_to(self, now: float) -> None:
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            self._step(dt)
            self.t += dt

    def _step(self, dt: float) -> None:
        if self.pressure_override is not None:
            self.pressure = self.pressure_override
            return
        v = self.valves
        if v["vent"]:
            tgt, tau = ATM_TORR, 20.0
        elif v["mv"] and v["turbo"]:
            eff = THROTTLE_MIN + (1.0 - THROTTLE_MIN) * min(1.0, max(0.0, self.throttle))
            tgt, tau = BASE_TORR + self.total_sccm() * TORR_PER_SCCM / eff, 5.0
        elif v["rotary"] and (v["rv"] or v["fv"]):
            tgt, tau = ROUGH_TORR, 10.0
        else:
            return
        self.pressure += (tgt - self.pressure) * min(1.0, dt / tau)

    # ───────── DC 플라즈마 ─────────
    def can_ignite(self) -> bool:
        p = self.pressure_mtorr
        return (not self.ignite_fail and self.total_sccm() > 0.0
                and IGNITE_MIN_MTORR <= p <= IGNITE_MAX_MTORR)

    def sustain_voltage(self) -> float:
        vp = 350.0 + 150.0 / math.sqrt(max(self.pressure_mtorr, 1e-3))
        if self.v_limit:
            vp += 300.0
        return min(vp, VP_MAX)

    def dc_operating_point(self, out_on: bool, v_set: float, i_set: float) -> tuple:
        """(V, I) — 정전류 전원 + 플라즈마 부하."""
        if not out_on or i_set <= 0:
            self.dc_ignited = False
            return 0.0, 0.0
        if not self.dc_ignited:
            if not self.can_ignite() or v_set < self.sustain_voltage():
                return float(v_set), 0.0                         # 무부하(점화 안 됨)
            self.dc_ignited = True
        if not self.can_ignite():                                # 가스가 없거나 압력이 범위를 벗어나면 소멸
            self.dc_ignited = False
            return float(v_set), 0.0
        vp = self.sustain_voltage()
        i = float(i_set) * (0.1 if self.low_current else 1.0)
        v = vp + R_PLASMA * i
        if v > v_set:                                            # 전압 한계(CV)
            i = (v_set - vp) / R_PLASMA
            if i <= 0:
                self.dc_ignited = False
                return float(v_set), 0.0
            v = float(v_set)
        return v, i
