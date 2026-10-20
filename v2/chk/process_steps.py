"""공정 스텝 목록과 실행기 — v1 controller/process_controller.py 의 스텝 빌더(110-277, _build_steps 486-682)를 다시 쓴 것.

스텝 목록은 v1 과 같은 순서·같은 동작·같은 인자로 만든다(골든 시험이 v1 기록과 비교한다). 바뀐 곳(의도된 차이):
- WAIT_PRESSURE: v1 "stable_count 3회 연속"(1 s 간격 + 명령 간격 ≈ 6 s) → "stable_sec PRESSURE_WAIT_STABLE_SEC 동안"(횟수→시간, 4-3).
- 숫자 상수는 파라미터로: SP3_MTORR·SP2_MTORR·PRESSURE_WAIT_TOL_RATIO·PRESSURE_STAGE_TIMEOUT_SEC·SP1_TIMEOUT_SEC·
  GAS_STABILIZE_SEC·PRE_POWER_OFF_SETTLE_SEC·DC_POWER_DELAY_SEC(기본값 = v1 값).
각 스텝에는 process_states 의 상태 이름(state)을 붙인다 — 상태 전이·상태 한도는 이것으로 센다.

실행기(StepRunner): 매 tick 스냅샷 하나로 지금 스텝을 진행한다(중첩 이벤트 루프 없음, 대기는 실제 시각).
- MFC_CMD: 드라이버 명령을 한 번 내고, 완료 콜백(되읽기 확인)이 오면 다음 tick 에 다음 스텝. 실패면 MFC_CMD_FAILED.
  FLOW_ON 은 R69 확인 뒤 유량 안정화까지(v1 device/MFC.py:1128-1139·1236-1270): 매 tick 스냅샷 유량이
  |실제 − 설정| ≤ 설정 × FLOW_ERROR_TOLERANCE 면 통과, MFC_FLOW_STABLE_TIMEOUT_SEC(30 s)를 넘으면 FLOW_STABILIZE_TIMEOUT.
  설정은 드라이버가 FLOW_SET 되읽기로 확인한 값(v1 last_setpoints) — 0 이면 안정화 없이 통과(v1).
  WAIT_PRESSURE 는 명령이 아니라 스냅샷 판정: |P − 목표| ≤ 목표×tol 이 stable_sec 이어지면 통과(judge.Persist, ok·late 값만),
  timeout_sec 를 넘으면 fail_on_timeout 이면 실패(SP1_TIMEOUT), 아니면 경고하고 진행.
- PLC_CMD: 버튼 쓰기 뒤 0.8 s(v1 릴레이·실린더 여유) 지나면 다음.
- DELAY: duration_sec 가 실제로 지나면 다음.
- 그 밖의 동작은 실행기를 쓰는 쪽(handlers)이 처리한다.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from v2.common.judge import Persist
from v2.chk.recipe import pulse_freq_hz

PLC_SETTLE_S = 0.8                 # v1 QTimer.singleShot(800, _next_step)
POWER_STOP_SETTLE_S = 0.1          # v1 DC/RF/RF Pulse STOP 뒤 100 ms
HEATER_RAMP_SETTLE_S = 0.2         # v1 HEATER_RAMP 뒤 200 ms
HEATER_SET_SETTLE_S = 0.5          # v1 HEATER_SET 뒤 500 ms
MFC_CMD_GUARD_S = 60.0             # 완료 콜백이 끝내 안 오면(드라이버 이상) 실패로 — v1 heater_atmosphere MFC_TIMEOUT_SEC

# v1 버튼 이름 → v2 PLC 주소 이름
PLC_BUTTON_ADDR = {"Ar_Button": "ar_valve", "O2_Button": "o2_valve", "S1_button": "s1", "S2_button": "s2",
                   "MS_button": "ms"}


@dataclass
class ProcessStep:
    action: str
    message: str
    params: Any = None
    value: Optional[float] = None
    duration_sec: Optional[int] = None
    timer_purpose: Optional[str] = None
    polling: bool = False
    state: str = ""

    def unit(self) -> dict:
        """골든 비교 단위(문구·polling 빼고)."""
        p = self.params
        if isinstance(p, tuple):
            p = list(p)
        return {"action": self.action, "params": p, "value": self.value, "duration_sec": self.duration_sec,
                "timer_purpose": self.timer_purpose}


S = ProcessStep


# ───────────── 빌더(v1 과 같은 순서) ─────────────
def build_gas_pre_steps(channels: List[int], state: str = "GAS_PRE") -> List[ProcessStep]:
    steps = [S("MFC_CMD", f"Ch{ch} Flow OFF", ("FLOW_OFF", {"channel": ch}), state=state) for ch in channels]
    steps.append(S("MFC_CMD", "MFC Valve Open", ("VALVE_OPEN", {}), state=state))
    return steps


def build_gas_intro_steps(channels: List[int], flows: Dict[int, float], gas_buttons: List[str],
                          state: str = "GAS_INTRO") -> List[ProcessStep]:
    steps = [S("MFC_CMD", f"Ch{ch} ZEROING", ("MFC_ZEROING", {"channel": ch}), state=state) for ch in channels]
    steps.append(S("MFC_CMD", "PS ZEROING", ("PS_ZEROING", {}), state=state))
    for btn in gas_buttons:
        gas = "Ar" if "Ar" in btn else "O2"
        steps.append(S("PLC_CMD", f"{gas} Valve Open", (btn, True), state=state))
    for ch in channels:
        flow = float(flows.get(ch, 0.0))
        if flow > 0.0:
            steps.append(S("MFC_CMD", f"Ch{ch} {flow:.2f}sccm 설정", ("FLOW_SET", {"channel": ch, "value": flow}), state=state))
            steps.append(S("MFC_CMD", f"Ch{ch} Flow ON", ("FLOW_ON", {"channel": ch}), state=state))
    return steps


def build_pressure_stage_steps(sp_index: int, sp_value: float, label: str, cfg: Dict[str, Any], timeout_sec: float,
                               fail_on_timeout: bool = False, state: str = "") -> List[ProcessStep]:
    tol = float(cfg["PRESSURE_WAIT_TOL_RATIO"])
    stable = float(cfg["PRESSURE_WAIT_STABLE_SEC"])
    set_cmd, on_cmd = f"SP{sp_index}_SET", f"SP{sp_index}_ON"
    return [
        S("MFC_CMD", f"{label}: {set_cmd}={sp_value:.2f} 설정", (set_cmd, {"value": sp_value}), state=state),
        S("MFC_CMD", f"{label}: {on_cmd}", (on_cmd, {}), state=state),
        S("MFC_CMD", f"{label}: 압력 도달 대기 (target={sp_value:.2f}, ±{tol * 100:.0f}%, timeout={int(timeout_sec)}s"
          + (", FAIL ON TIMEOUT" if fail_on_timeout else "") + ")",
          ("WAIT_PRESSURE", {"target": sp_value, "tolerance_ratio": tol, "stable_sec": stable,
                             "timeout_sec": float(timeout_sec), "fail_on_timeout": fail_on_timeout}), state=state),
    ]


def build_gas_release_steps(channels: List[int], gas_buttons: List[str], state: str = "") -> List[ProcessStep]:
    steps = [S("MFC_CMD", f"Ch{ch} Flow OFF", ("FLOW_OFF", {"channel": ch}), state=state) for ch in channels]
    steps.append(S("MFC_CMD", "MFC Valve Open", ("VALVE_OPEN", {}), state=state))
    for btn in gas_buttons:
        gas = "Ar" if "Ar" in btn else "O2"
        steps.append(S("PLC_CMD", f"{gas} Valve Close", (btn, False), state=state))
    return steps


def channels_of(p: Dict[str, Any]):
    chans, btns = [], []
    if p.get("use_ar_gas"):
        chans.append(1)
        btns.append("Ar_Button")
    if p.get("use_o2_gas"):
        chans.append(2)
        btns.append("O2_Button")
    if not chans:                                  # v1 selected_gas 폴백
        ch = 1 if p.get("selected_gas", "Ar") == "Ar" else 2
        chans, btns = [ch], ["Ar_Button" if ch == 1 else "O2_Button"]
    return chans, btns


def heater_used(p: Dict[str, Any]) -> bool:
    return bool(p.get("use_heater")) and float(p.get("heater_temp") or 0.0) > 0.0


def build_process_steps(p: Dict[str, Any], cfg: Dict[str, Any], *, ig_enabled: bool, rfpulse_present: bool = True
                        ) -> List[ProcessStep]:
    """v1 _build_steps(486-682) — 같은 순서·조건."""
    channels, buttons = channels_of(p)
    default_flow = float(p.get("mfc_flow", 0.0) or 0.0)
    flows = {}
    if 1 in channels:
        flows[1] = float(p.get("ar_flow", default_flow) or 0.0)
    if 2 in channels:
        flows[2] = float(p.get("o2_flow", default_flow) or 0.0)
    sp1 = float(p.get("sp1_set", 0.0) or 0.0)
    steps: List[ProcessStep] = [S("DC_POWER_STOP", "PRE: DC Power OFF", state="PRE_CLEAN"),
                                S("RF_POWER_STOP", "PRE: RF Power OFF", state="PRE_CLEAN")]
    if rfpulse_present:
        steps.append(S("RF_PULSE_STOP", "PRE: RF Pulse OFF", state="PRE_CLEAN"))
    steps.append(S("DELAY", "PRE: Power OFF settle", duration_sec=int(round(cfg["PRE_POWER_OFF_SETTLE_SEC"])),
                   state="PRE_CLEAN"))
    steps += build_gas_pre_steps(channels)
    if heater_used(p):
        temp = float(p["heater_temp"])
        ramp = float(p.get("heater_ramp") or 0.0) or float(cfg["HEATER_RAMP_RATE_C_PER_MIN"])
        steps.append(S("HEATER_RAMP", f"히터 램프 속도 {ramp:.0f}°C/min 설정", value=ramp, state="HEATER_RAMP"))
        steps.append(S("HEATER_SET", f"히터 목표 {temp:.1f}°C 설정 — 승온 시작", value=temp, state="HEATER_RAMP"))
        steps.append(S("HEATER_WAIT", f"히터 온도 도달 대기 (±{cfg['HEATER_SOAK_TOLERANCE']:.1f}°C, "
                       f"{int(cfg['HEATER_SOAK_TIME_SEC'])}s 유지)", value=temp, state="HEATER_RAMP"))
    bp = base_pressure_of(p)
    if bp > 0 and ig_enabled:
        steps += [S("IG_FILAMENT_ON", "이온 게이지 필라멘트 ON", state="BASE_WAIT"),
                  S("IG_BASE_WAIT", f"베이스 압력 대기 (목표 {bp:.1E} Torr)", value=bp, state="BASE_WAIT"),
                  S("IG_FILAMENT_OFF", "이온 게이지 필라멘트 OFF", state="BASE_WAIT")]
    steps += build_gas_intro_steps(channels, flows, buttons)
    stab = int(round(cfg["GAS_STABILIZE_SEC"]))
    steps.append(S("MFC_CMD", "SP4 ON", ("SP4_ON", {}), state="GAS_STABILIZE"))
    steps.append(S("DELAY", f"압력 안정화 대기({stab}초)", duration_sec=stab, state="GAS_STABILIZE"))
    if p.get("use_g1"):
        steps.append(S("PLC_CMD", "Gun Shutter 1 Open", ("S1_button", True), state="GUN_SHUTTER"))
    if p.get("use_g2"):
        steps.append(S("PLC_CMD", "Gun Shutter 2 Open", ("S2_button", True), state="GUN_SHUTTER"))
    steps.append(S("POWER_WAIT", "파워 목표치 도달 대기", state="POWER_WAIT"))
    sp3, sp2 = float(cfg["SP3_MTORR"]), float(cfg["SP2_MTORR"])
    stage_to = float(cfg["PRESSURE_STAGE_TIMEOUT_SEC"])
    if sp1 < sp3:
        steps += build_pressure_stage_steps(3, sp3, "Stage1(SP3)", cfg, stage_to, state="PRESSURE_SP3")
    if sp1 < sp2:
        steps += build_pressure_stage_steps(2, sp2, "Stage2(SP2)", cfg, stage_to, state="PRESSURE_SP2")
    steps += build_pressure_stage_steps(1, sp1, f"Stage3(SP1={sp1:.2f})", cfg, float(cfg["SP1_TIMEOUT_SEC"]),
                                        fail_on_timeout=True, state="PRESSURE_SP1")
    dc_power = float(p.get("dc_power", 0.0) or 0.0)
    dcd = int(cfg["DC_POWER_DELAY_SEC"])
    if p.get("use_dc_delay") and dc_power > 0.0 and dcd > 0:
        steps.append(S("DELAY", f"DC Power 안정화 대기 {dcd}s", duration_sec=dcd, state="DC_DELAY"))
    sd = max(0, int(math.ceil(float(p.get("shutter_delay", 0.0) or 0.0) * 60)))
    if sd > 0:
        steps.append(S("DELAY", f"Shutter Delay {sd}s", duration_sec=sd, timer_purpose="shutter", polling=True,
                       state="SHUTTER_DELAY"))
    if float(p.get("process_time", 0.0) or 0.0) > 0:
        steps.append(S("PLC_CMD", "Main Shutter Open", ("MS_button", True), state="MAIN"))
    pt = max(0, int(math.ceil(float(p.get("process_time", 0.0) or 0.0) * 60)))
    if pt > 0:
        steps.append(S("DELAY", f"메인 공정 {pt}s 진행", duration_sec=pt, timer_purpose="process", polling=True,
                       state="MAIN"))
    return steps


def base_pressure_of(p: Dict[str, Any]) -> float:
    v = p.get("base_pressure")
    if v is None or (isinstance(v, str) and not v.strip()):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def row_to_params(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """v2 레시피 행(recipe.check_process_row 가 정리한 것) → 공정 params(v1 build_csv_params 와 같은 키).
    RF 보정값은 설정(RF_CAL_OFFSET·RF_CAL_GAIN)에서 — v1 은 노트북 입력칸(의도된 차이 RF_CAL_FROM_CONFIG)."""
    ar, o2 = row.get("ar") is True, row.get("o2") is True
    use_dc, use_rf, use_rfp = row.get("use_dc") is True, row.get("use_rf") is True, row.get("use_rf_pulse") is True
    use_h = row.get("use_heater") is True
    num = lambda k: float(row.get(k) or 0.0)
    return {
        "process_name": row.get("process_name") or "",
        "use_ar_gas": ar, "use_o2_gas": o2,
        "ar_flow": num("ar_flow") if ar else 0.0, "o2_flow": num("o2_flow") if o2 else 0.0,
        "selected_gas": "Ar" if ar or not o2 else "O2",
        "mfc_flow": num("ar_flow") if ar else num("o2_flow"),
        "base_pressure": num("base_pressure"),
        "sp1_set": num("working_pressure"),
        "dc_power": num("dc_power") if use_dc else 0.0,
        "rf_power": num("rf_power") if use_rf else 0.0,
        "shutter_delay": num("shutter_delay"), "process_time": num("process_time"),
        "rf_offset": float(cfg["RF_CAL_OFFSET"]), "rf_param": float(cfg["RF_CAL_GAIN"]),
        "use_rf_pulse": use_rfp, "rf_pulse_power": num("rf_pulse_power") if use_rfp else 0.0,
        "rf_pulse_freq_hz": pulse_freq_hz(row.get("rf_pulse_freq")) if use_rfp else None,        # kHz → Hz(v1)
        "rf_pulse_duty": int(row["rf_pulse_duty"]) if use_rfp and row.get("rf_pulse_duty") not in (None, "") else None,
        "use_g1": row.get("gun1") is True, "use_g2": row.get("gun2") is True,
        "use_dc_delay": row.get("use_dc_delay") is True and use_dc,
        "use_heater": use_h, "heater_temp": num("heater_temp") if use_h else 0.0,
        "heater_ramp": num("heater_ramp") if use_h else 0.0,
        "g1_target_name": row.get("g1_target") or "", "g2_target_name": row.get("g2_target") or "",
    }


# ───────────── 실행기 ─────────────
@dataclass
class StepEvent:
    kind: str              # step / done / failed / warn
    detail: str = ""
    code: str = ""
    index: int = -1


@dataclass
class _Cur:
    t0: float
    issued: bool = False
    done: bool = False
    err: Optional[str] = None
    persist: Optional[Persist] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class StepRunner:
    """스텝 목록을 tick 마다 진행한다. handlers[action](runner, step, cur, view, now) → "wait" | "next" | ("fail", 코드)."""

    def __init__(self, clock, steps: List[ProcessStep], mfc, plc, handlers: Optional[Dict[str, Callable]] = None,
                 on_event: Optional[Callable[[StepEvent], None]] = None, name: str = "process",
                 continue_on_fail: bool = False, cfg: Optional[Dict[str, Any]] = None) -> None:
        self.clock, self.steps, self.mfc, self.plc = clock, list(steps), mfc, plc
        self.cfg = cfg                                  # FLOW_ON 유량 안정화 판정(없으면 R69 확인만)
        self.continue_on_fail = continue_on_fail       # 해제 시퀀스: 실패해도 남은 밸브를 마저 닫는다(v1 heater_atmosphere)
        self.handlers = dict(handlers or {})
        self.on_event = on_event
        self.name = name
        self.idx = -1
        self.cur: Optional[_Cur] = None
        self.finished = False
        self.failed: Optional[str] = None
        self.cancelled = False

    @property
    def step(self) -> Optional[ProcessStep]:
        return self.steps[self.idx] if 0 <= self.idx < len(self.steps) else None

    def _emit(self, e: StepEvent) -> None:
        if self.on_event:
            self.on_event(e)

    def cancel(self) -> None:
        self.cancelled = True

    def _advance(self, now: float) -> None:
        self.idx += 1
        if self.idx >= len(self.steps):
            self.finished = True
            self.cur = None
            self._emit(StepEvent("done", self.name))
            return
        self.cur = _Cur(t0=now)
        s = self.steps[self.idx]
        self._emit(StepEvent("step", f"[{self.idx + 1}/{len(self.steps)}] {s.message}", index=self.idx))

    def tick(self, view, now: float) -> None:
        """한 tick — 끝난 스텝은 넘기고, 같은 tick 안에 바로 끝나는 스텝(내보내기만 하는 명령)은 이어서 진행."""
        if self.finished or self.failed or self.cancelled:
            return
        if self.idx < 0:
            self._advance(now)
        guard = 0
        while not (self.finished or self.failed or self.cancelled) and guard < len(self.steps) + 2:
            guard += 1
            s, cur = self.steps[self.idx], self.cur
            r = self._run(s, cur, view, now)
            if r == "wait":
                return
            if isinstance(r, tuple) and r[0] == "fail" and self.continue_on_fail:
                self._emit(StepEvent("warn", f"[{self.idx + 1}/{len(self.steps)}] {s.message}: {r[1]} — 계속 진행", r[1], self.idx))
            elif isinstance(r, tuple) and r[0] == "fail":
                self.failed = r[1]
                self._emit(StepEvent("failed", f"[{self.idx + 1}/{len(self.steps)}] {s.message}: {r[1]}", r[1], self.idx))
                return
            self._advance(now)

    def _done_cb(self, cur: _Cur):
        def cb(*a):
            # Sequence on_done(name, reason, results) 또는 Request on_done(Result)
            if len(a) == 3:
                err = a[1]
            elif a:
                res = a[0]
                err = None if getattr(res, "status", "ok") == "ok" else (getattr(res, "reason", None) or res.status)
            else:
                err = None
            cur.err, cur.done = err, True
        return cb

    def _run(self, s: ProcessStep, cur: _Cur, view, now: float):
        h = self.handlers.get(s.action)
        if h is not None:
            return h(self, s, cur, view, now)
        if s.action == "MFC_CMD":
            return self._mfc(s, cur, view, now)
        if s.action == "PLC_CMD":
            if not cur.issued:
                cur.issued = True
                btn, on = s.params
                self.plc.set_button(PLC_BUTTON_ADDR.get(btn, btn), bool(on), on_done=self._done_cb(cur))
            if cur.done and cur.err:
                return ("fail", "PLC_CMD_FAILED")
            return "next" if now - cur.t0 >= PLC_SETTLE_S - 1e-9 else "wait"
        if s.action == "DELAY":
            return "next" if now - cur.t0 >= float(s.duration_sec or 0) - 1e-9 else "wait"
        return ("fail", "INTERNAL_ERROR")

    def _mfc(self, s: ProcessStep, cur: _Cur, view, now: float):
        cmd, a = s.params
        if cmd == "WAIT_PRESSURE":
            return self._wait_pressure(a, cur, view, now)
        if not cur.issued:
            cur.issued = True
            cb, m = self._done_cb(cur), self.mfc
            if cmd in ("FLOW_ON", "FLOW_OFF"):
                m.set_channel(int(a["channel"]), cmd == "FLOW_ON", on_done=cb)
            elif cmd == "VALVE_OPEN":
                m.valve(True, on_done=cb)
            elif cmd == "VALVE_CLOSE":
                m.valve(False, on_done=cb)
            elif cmd == "MFC_ZEROING":
                m.zero(f"L{4 + int(a['channel'])} 1", on_done=cb)
            elif cmd == "PS_ZEROING":
                m.zero("Z1", on_done=cb)
            elif cmd == "FLOW_SET":
                m.set_flow(int(a["channel"]), float(a["value"]), on_done=cb)
            elif cmd in ("SP1_SET", "SP2_SET", "SP3_SET", "SP4_SET"):
                m.set_sp(int(cmd[2]), float(a["value"]), on_done=cb)
            elif cmd in ("SP1_ON", "SP2_ON", "SP3_ON", "SP4_ON"):
                m.select_sp(int(cmd[2]), on_done=cb)
            else:
                return ("fail", "MFC_CMD_FAILED")
        if cur.done:
            if cur.err:
                return ("fail", "MFC_CMD_FAILED")
            if cmd == "FLOW_ON":
                return self._flow_stable(int(a["channel"]), cur, view, now)
            return "next"
        if now - cur.t0 > MFC_CMD_GUARD_S:
            return ("fail", "MFC_CMD_FAILED")
        return "wait"

    def _flow_stable(self, ch: int, cur: _Cur, view, now: float):
        """FLOW ON 유량 안정화(v1 _check_flow_stabilization) — 한 번이라도 허용 범위 안이면 통과, 한도를 넘으면 실패."""
        if self.cfg is None:
            return "next"
        target = getattr(self.mfc, "ar_set" if ch == 1 else "o2_set", None) if ch in (1, 2) else None
        if not target or float(target) <= 0:
            return "next"                                   # v1: 안정화 대상(설정) 없으면 FLOW_ON 바로 확인
        t0 = cur.extra.setdefault("stab_t0", now)
        x = view.get(f"mfc.{'ar' if ch == 1 else 'o2'}_sccm") if view is not None else None
        if x is not None and abs(float(x) - float(target)) <= float(target) * float(self.cfg["FLOW_ERROR_TOLERANCE"]):
            return "next"
        if now - t0 >= float(self.cfg["MFC_FLOW_STABLE_TIMEOUT_SEC"]) - 1e-9:
            self._emit(StepEvent("warn", f"Ch{ch} 유량 안정화 시간 초과(목표 {float(target):.2f}, 현재 {x})",
                                 "FLOW_STABILIZE_TIMEOUT", self.idx))
            return ("fail", "FLOW_STABILIZE_TIMEOUT")
        return "wait"

    def _wait_pressure(self, a: Dict[str, Any], cur: _Cur, view, now: float):
        target = float(a["target"])
        if target <= 0:
            return "next"
        if cur.persist is None:
            cur.persist = Persist(float(a["stable_sec"]))
        p = view.get("mfc.pressure_mtorr") if view is not None else None
        t = view.t_of("mfc") if view is not None else now
        cond = None if p is None else abs(float(p) - target) <= target * float(a["tolerance_ratio"])
        if cur.persist.update(t, cond):
            return "next"
        if now - cur.t0 >= float(a["timeout_sec"]) - 1e-9:
            if a.get("fail_on_timeout"):
                return ("fail", "SP1_TIMEOUT")
            self._emit(StepEvent("warn", f"압력 {target:.2f} mTorr 도달 시간 초과({a['timeout_sec']:.0f} s) — 다음 단계 진행",
                                 "PRESSURE_STAGE_TIMEOUT", self.idx))
            return "next"
        return "wait"


def view_time(row: Dict[str, Any]) -> float:
    return datetime.fromisoformat(row["ts"]).timestamp()
