"""MFC 컨트롤러 시뮬레이터 — ATOVAC GPC3000(RS-232 9600 8N1, ASCII 명령 + CR, 값은 %FS).

질의(응답 한 줄 + CR LF): R60 → "Q0+aa.a+bb.b+cc.c+dd.d"(채널 유량 %FS), R61~R64 → "Q1+…", R65~R68 → "Q5+…"(설정 유량),
  R69 → "L0 bbbb"(ON/OFF), R5 → "P+x.xx"(압력 %FS), R1~R4 → "S1+x.xx"(설정점), R6 → "V+vvv.v"(밸브 위치 %),
  R7 → "MXYZ"(X 선택 설정점, Y 0 제어·2 열림·4 닫힘, Z 압력 >10 %FS 면 1), R70~R73 → "W1+fff.f"(채널 FS sccm),
  R74~R77 → "W5+1"(단위 1 = SCCM).
설정(응답 없음): Q1~Q4 v, L0 bbbb, L1~L4 0|1, L5~L8 1(영점), S1~S5 v, D1~D5(설정점 선택 → 압력 제어), O·C·H(밸브), Z1.
명령 간격: MIN_GAP_S 안에 들어온 명령은 놓친다(무응답) — 실제 최소 간격은 bench 에서 잰다.
유량: 켜진 채널은 설정 %FS × FS sccm 쪽으로 1차 지연(τ 1 s) → 공용 챔버 gas_sccm(ch1 = ar, ch2 = o2).
압력 제어(가정 모델): 설정점이 선택되면 제어 모드 — 목표 압력을 내는 밸브 열림을 계산해 그쪽으로 1차 지연(τ 2 s, O·C 는 τ 0.5 s) →
  공용 챔버 throttle. O = 완전 열림, C = 닫힘, H = 그 자리. 압력 %FS = 압력(Torr) / PRESSURE_FS_TORR × 100.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

from v2.common.simdev import SimDevice
from v2.chk.sim.chamber import BASE_TORR, THROTTLE_MIN, TORR_PER_SCCM, Chamber

QUERY_LATENCY_S = 0.03
BAUD = 9600
MIN_GAP_S = 0.05
FLOW_TAU_S = 1.0
VALVE_TAU_S = 2.0
VALVE_OPEN_TAU_S = 0.5      # O·C(완전 열기·닫기)는 구동기가 끝까지 — 가정(v1 은 5 s 뒤 > 99 % 를 기대)
PRESSURE_FS_TORR = 1.0


class MfcSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, fs_sccm=(100.0, 100.0, 100.0, 100.0)) -> None:
        super().__init__("mfc", latency_s=0.0)
        self.chamber = chamber or Chamber()
        self.fs = list(fs_sccm)
        self.set_pct = [0.0] * 4
        # 매뉴얼 표 11 [MFC p.80-88] 의 나머지 질의 응답(가정 — 실제 값은 bench 로 읽는다). R33 E03 = 1, R34 F0 = Torr → FS 1 Torr
        self.info = {0: "S0+0.00", 10: "S5+0.00", 11: "P1+0.00", 12: "P2+100.00", 13: "P3+0.00", 14: "P4+100.00",
                     23: "J1", 25: "T01", 26: "T11", 27: "T21", 28: "T31", 29: "T41", 30: "T51", 32: "N0", 33: "E03",
                     34: "F0", 37: "M100", 38: "H3 1.00", 51: "V1"}
        self.info.update({40 + i: f"X{i}+0.0" for i in range(1, 6)})
        self.info.update({45 + i: f"M{i}+100" for i in range(1, 6)})
        self.on = [False] * 4
        self.flow_pct = [0.0] * 4
        self.sp = [0.0] * 5                     # 설정점 %FS
        self.sp_sel = 0
        self.valve_mode = "open"                # open / closed / hold / control
        self.valve = 1.0
        self.t = 0.0
        self.last_cmd_t = -1e9
        self.received: List[tuple] = []
        self.dropped = 0
        self.ignore_writes: set = set()         # 시험: 이 접두어의 쓰기를 무시(되읽기 불일치)

    # ───────── 시각 ─────────
    def _advance(self, now: float) -> None:
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            for i in range(4):
                tgt = self.set_pct[i] if self.on[i] else 0.0
                self.flow_pct[i] += (tgt - self.flow_pct[i]) * min(1.0, dt / FLOW_TAU_S)
            self.chamber.gas_sccm["ar"] = self.flow_pct[0] / 100.0 * self.fs[0]
            self.chamber.gas_sccm["o2"] = self.flow_pct[1] / 100.0 * self.fs[1]
            if self.valve_mode == "open":
                vt = 1.0
            elif self.valve_mode == "closed":
                vt = 0.0
            elif self.valve_mode == "control" and self.sp_sel:
                target_torr = self.sp[self.sp_sel - 1] / 100.0 * PRESSURE_FS_TORR
                flow = self.chamber.total_sccm()
                if target_torr <= BASE_TORR * 1.01 or flow <= 0:
                    vt = 1.0
                else:
                    eff = flow * TORR_PER_SCCM / (target_torr - BASE_TORR)
                    vt = (eff - THROTTLE_MIN) / (1.0 - THROTTLE_MIN)
                vt = min(1.0, max(0.0, vt))
            else:
                vt = self.valve
            tau = VALVE_OPEN_TAU_S if self.valve_mode in ("open", "closed") else VALVE_TAU_S
            self.valve += (vt - self.valve) * min(1.0, dt / tau)
            self.chamber.throttle = self.valve
            self.chamber.advance_to(self.t + dt)
            self.t += dt
        self.chamber.advance_to(now)

    def pressure_pct(self) -> float:
        return self.chamber.pressure / PRESSURE_FS_TORR * 100.0

    # ───────── 통신 ─────────
    def respond(self, data: bytes, now: float):
        self._advance(now)
        self.received.append((now, data))
        if now - self.last_cmd_t < MIN_GAP_S:
            self.dropped += 1
            return []
        self.last_cmd_t = now
        if self._active("no_response", now):
            return []
        out = self.handle(data, now)
        if out is None:
            return []
        if self._active("garbage", now):
            out = b"#" * max(1, len(out) - 2) + out[-2:]
        lat = QUERY_LATENCY_S + (len(data) + len(out)) * 10.0 / BAUD + sum(f.extra_s for f in self._active("delay", now))
        return [(now + lat, out)]

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        cmd = data.decode("ascii", errors="replace").strip()
        if cmd.startswith("R"):
            ans = self._query(cmd)
            return None if ans is None else (ans + "\r\n").encode()
        if any(cmd.startswith(p) for p in self.ignore_writes):
            return None
        self._set(cmd)
        return None

    def _query(self, cmd: str) -> Optional[str]:
        try:
            n = int(cmd[1:])
        except ValueError:
            return None
        if n == 60:
            return "Q0" + "".join(f"{v:+.1f}" for v in self.flow_pct)
        if 61 <= n <= 64:
            return f"Q{n - 60}{self.flow_pct[n - 61]:+.1f}"
        if 65 <= n <= 68:
            return f"Q{n - 60}{self.set_pct[n - 65]:+.1f}"
        if n == 69:
            return "L0 " + "".join("1" if x else "0" for x in self.on)
        if n == 5:
            return f"P{self.pressure_pct():+.2f}"
        if 1 <= n <= 4:
            return f"S{n}{self.sp[n - 1]:+.2f}"
        if n == 6:
            return f"V{self.valve * 100:+.1f}"
        if n == 7:
            y = {"control": 0, "open": 2, "closed": 4, "hold": 0}[self.valve_mode]
            return f"M{self.sp_sel}{y}{1 if self.pressure_pct() > 10 else 0}"
        if 70 <= n <= 73:
            return f"W{n - 69}{self.fs[n - 70]:+.1f}"
        if 74 <= n <= 77:
            return f"W{n - 69}+1"
        return self.info.get(n)

    def _set(self, cmd: str) -> None:
        m = re.fullmatch(r"Q([1-4])\s+([+\-]?\d+(?:\.\d+)?)", cmd)
        if m:
            self.set_pct[int(m.group(1)) - 1] = float(m.group(2))
            return
        m = re.fullmatch(r"L0\s*([01]{4})", cmd)
        if m:
            self.on = [c == "1" for c in m.group(1)]
            return
        m = re.fullmatch(r"L([1-4])\s+([01])", cmd)
        if m:
            self.on[int(m.group(1)) - 1] = m.group(2) == "1"
            return
        if re.fullmatch(r"L[5-8]\s+1", cmd) or cmd == "Z1":
            return                                              # 영점(모델 없음)
        m = re.fullmatch(r"S([1-5])\s+([+\-]?\d+(?:\.\d+)?)", cmd)
        if m:
            self.sp[int(m.group(1)) - 1] = float(m.group(2))
            return
        m = re.fullmatch(r"D([1-5])", cmd)
        if m:
            self.sp_sel = int(m.group(1))
            self.valve_mode = "control"
            return
        if cmd == "O":
            self.valve_mode, self.sp_sel = "open", 0
        elif cmd == "C":
            self.valve_mode, self.sp_sel = "closed", 0
        elif cmd == "H":
            self.valve_mode = "hold"

    def set_commands_seen(self) -> List[str]:
        return [d.decode("ascii", errors="replace").strip() for _, d in self.received
                if not d.decode("ascii", errors="replace").strip().startswith("R")]
