"""DC 전원 시뮬레이터 — ODA EX 시리즈 SCPI(RS-232 9600 8N1, 종단 LF).

명령(devices.DC): *IDN? *RST *CLS, APPL[y] v,i, CURR x / CURR?, VOLT x / VOLT?, OUTP ON|OFF / OUTP?,
  MEAS:ALL?("V,I") MEAS:VOLT? MEAS:CURR?, SYST:ERR?, VOLT:OVP x / VOLT:OVP?, CURR:OCP x / CURR:OCP?,
  FACT:LAST-STA DIS|SAF|FUL / FACT:LAST-STA?(응답은 DISABLE·SAFETY·FULLY [DC p.94]).
설정 명령에는 응답이 없다. v1 실측(2026-09-28): 직전 명령을 처리하는 동안 들어온 질의에는 답하지 않는다 —
  설정 명령 뒤 BUSY_S(0.5 s) 안의 질의는 무시(무응답). 모르는 명령은 오류 큐에 -113(Undefined header).
출력: 공용 챔버의 플라즈마 V-I(dc_operating_point). OVP 를 넘으면 출력 차단 + 오류 큐.
LAST-STA 는 장비에 영구 저장되는 값 — writes_last_sta 로 몇 번 써졌는지 센다.
"""
from __future__ import annotations

from typing import List, Optional

from v2.common.simdev import SimDevice
from v2.chk.sim.chamber import Chamber

BUSY_S = 0.5
QUERY_LATENCY_S = 0.04
BAUD = 9600


class DcSim(SimDevice):
    def __init__(self, chamber: Optional[Chamber] = None, idn: str = "ODA,EX-600-1,SIM0001,1.00",
                 last_sta: str = "DIS") -> None:
        super().__init__("dc", latency_s=0.0)
        self.chamber = chamber or Chamber()
        self.idn = idn
        self.out = False
        self.v_set, self.i_set = 0.0, 0.0
        self.ovp, self.ocp = 660.0, 1.1
        self.last_sta = last_sta
        self.writes_last_sta = 0
        self.errors: List[str] = []
        self.busy_until = -1.0
        self.received: List[tuple] = []
        self.ignored_queries = 0

    def respond(self, data: bytes, now: float):
        self.chamber.advance_to(now)
        self.received.append((now, data))
        if self._active("no_response", now):
            return []
        out = self.handle(data, now)
        if out is None:
            return []
        line = (len(data) + len(out)) * 10.0 / BAUD
        lat = QUERY_LATENCY_S + line + sum(f.extra_s for f in self._active("delay", now))
        return [(now + lat, out)]

    def measure(self) -> tuple:
        v, i = self.chamber.dc_operating_point(self.out, self.v_set, self.i_set)
        if self.out and v > self.ovp:
            self.out = False
            self.errors.append('-200,"Execution error: OVP trip"')
            return 0.0, 0.0
        return v, i

    def handle(self, data: bytes, now: float) -> Optional[bytes]:
        cmd = data.decode("ascii", errors="replace").strip()
        u = cmd.upper()
        if u.endswith("?"):
            if now < self.busy_until:
                self.ignored_queries += 1
                return None                                   # 직전 명령 처리 중 — 답하지 않음
            ans = self._query(u)
            if ans is None:
                self.errors.append('-113,"Undefined header"')
                return None
            return (ans + "\n").encode()
        self.busy_until = now + BUSY_S
        if not self._set(u):
            self.errors.append('-113,"Undefined header"')
        return None

    def _query(self, u: str) -> Optional[str]:
        if u == "*IDN?":
            return self.idn
        if u == "MEAS:ALL?":
            v, i = self.measure()
            return f"{v:.4f},{i:.4f}"
        if u == "MEAS:VOLT?":
            return f"{self.measure()[0]:.4f}"
        if u == "MEAS:CURR?":
            return f"{self.measure()[1]:.4f}"
        if u == "OUTP?":
            return "1" if self.out else "0"
        if u == "CURR?":
            return f"{self.i_set:.4f}"
        if u == "VOLT?":
            return f"{self.v_set:.4f}"
        if u == "VOLT:OVP?":
            return f"{self.ovp:.4f}"
        if u == "CURR:OCP?":
            return f"{self.ocp:.4f}"
        if u == "FACT:LAST-STA?":
            return {"DIS": "DISABLE", "SAF": "SAFETY", "FUL": "FULLY"}.get(self.last_sta, self.last_sta)
        if u == "SYST:ERR?":
            return self.errors.pop(0) if self.errors else '0,"No error"'
        return None

    def _set(self, u: str) -> bool:
        try:
            if u == "*RST":
                self.out, self.v_set, self.i_set = False, 0.0, self.ocp
                self.chamber.dc_ignited = False
                return True
            if u == "*CLS":
                self.errors.clear()
                return True
            if u.startswith("APPL"):
                v, i = u.split(None, 1)[1].split(",")
                self.v_set, self.i_set = float(v), float(i)
                return True
            if u.startswith("CURR:OCP "):
                self.ocp = float(u.split()[1])
                return True
            if u.startswith("VOLT:OVP "):
                self.ovp = float(u.split()[1])
                return True
            if u.startswith("CURR "):
                self.i_set = min(float(u.split()[1]), self.ocp)
                return True
            if u.startswith("VOLT "):
                self.v_set = float(u.split()[1])
                return True
            if u in ("OUTP ON", "OUTP 1"):
                self.out = True
                return True
            if u in ("OUTP OFF", "OUTP 0"):
                self.out = False
                self.chamber.dc_ignited = False          # 출력이 꺼지면 플라즈마도 꺼진다
                return True
            if u.startswith("FACT:LAST-STA "):
                self.last_sta = u.split()[1]
                self.writes_last_sta += 1
                return True
        except (ValueError, IndexError):
            self.errors.append('-121,"Invalid data"')
            return True
        return False

    def commands_seen(self) -> List[str]:
        return [d.decode("ascii", errors="replace").strip() for _, d in self.received]

    def set_commands_seen(self) -> List[str]:
        return [c for c in self.commands_seen() if not c.endswith("?")]
