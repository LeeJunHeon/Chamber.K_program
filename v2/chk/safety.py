"""챔버K 안전 규칙 실행 — safety_rules.py 표의 조건을 코드로, 조치를 드라이버 호출로 잇는다.

연결
- plc250 규칙: PlcDriver.fast_listeners → on_poll(PLC 250 ms 폴링 직후, PLC worker 스레드). 조치는 바로 safety 우선순위.
- tick 규칙  : TickLoop sink → on_row(1초 행이 닫힌 뒤, core 스레드).
- 이벤트 규칙: on_driver_event(드라이버 이벤트) · event(이름) — plc_link_up · plc_first_poll · plc_restarted ·
  heater_run_falling · heater_gas_failed · rfp_start_failed · shutdown.
- 명령·설정 거부 규칙: check_start · check_heater_start · check_shutdown(여기) + 이미 있는 곳(IMPLEMENTED_IN).

공정 쪽(process)은 state·active·on_abort(reason)·on_stop_done(kind)·uses_* 를 가진 객체 — 다음 단계 ProcessEngine 이
같은 모양으로 대신한다. 지금은 SimpleProcess(시험·시험대용).

pending_confirm 규칙은 지금 권장값으로 실행(S42 공정 중 잠금 버튼, ALL_STOP_KEEP_PUMPS=true). on_hold(S46)는 실행하지 않는다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from v2.common.safety_engine import LiveRule, SafetyEngine, SafetyEvent, Step
from v2.common.worker import Status
from v2.chk import process_states as PS
from v2.chk import safety_rules as SR
from v2.chk.safety_rules import Act

FRESH = ("ok", "late")
ACT = frozenset(PS.ACTIVE_STATES)
# plc250 조치 지연 목표(사용자 결정 5-2): 조건 발생 → 조치 쓰기 도착 ≤ PLC 폴링 주기(PLC_FAST_POLL_MS) + 35 ms.
# 최악 = 조건이 폴링이 그 값을 읽은 직후에 생긴 경우: 폴링 주기 + 3블록 읽기(약 15 ms) + 쓰기 왕복(약 13 ms) ≈ 285 ms.
PLC250_LATENCY_EXTRA_S = 0.035
DEVICE_BLOCKS = ("plc", "heater", "rf", "mfc", "dc", "rfp", "ig")

# 표의 규칙마다 어디서 실행되는지 — engine(여기 LiveRule) / 그 밖(이미 있는 코드) / next:(다음 단계) / on_hold
IMPLEMENTED_IN: Dict[str, str] = {
    "S1": "check_start", "S2": "engine", "S3": "engine", "S4": "SafetyEngine skip_when_stopping(S2·S3)",
    "S5": "check_start·check_heater_start", "S6": "engine", "S7": "engine",
    "S8": "drivers/plc.py blackbox(링버퍼)", "S9": "engine(event plc_restarted) + drivers/plc.py 마커 2회 판정",
    "S10": "engine(event plc_link_up)", "S11": "locks", "S12": "drivers/plc.py heater_run(보류)",
    "S13": "drivers/plc.py _init_writes", "S14": "drivers/plc.py on_tick 하트비트", "S15": "engine(event plc_first_poll)",
    "S16": "engine", "S17": "engine", "S18A": "engine", "S18B": "engine", "S18C": "engine",
    "S18D": "next:HeaterEngine(레시피 스텝)", "S18E": "next:ProcessEngine(state:HEATER_RAMP 한도)",
    "S19": "check_start·check_heater_start", "S20": "commands.check_command(Pre HEATER_RUN_OFF)",
    "S21": "commands.check_command(param_max HEATER_MAX_TEMP)", "S22": "next:HeaterEngine(유지 모드)",
    "S23": "next:ProcessEngine(state:HEATER_HOLD_WAIT)", "S24": "next:HeaterEngine(포화 — 앞창·폴백)",
    "S25": "next:HeaterEngine(감속 접근)", "S26": "engine(event heater_run_falling)",
    "S27": "engine(event heater_gas_failed)", "S28": "locks", "S29": "drivers/dc.py output_off_confirmed",
    "S30": "engine", "S31A": "engine", "S31B": "engine", "S31C": "engine", "S32": "engine", "S33A": "engine",
    "S33B": "engine", "S34A": "engine", "S34B": "engine", "S34C": "engine", "S34D": "engine(event rfp_start_failed)",
    "S35": "recipe.check_process_row + drivers/rfpulse.py 되읽기", "S36": "next:ProcessEngine(state:POWER_WAIT)",
    "S37": "next:ProcessEngine(state:PRESSURE_SP1)", "S38A": "engine", "S38B": "engine",
    "S39A": "next:ProcessEngine(state:BASE_WAIT)", "S39B": "engine", "S39C": "next:ProcessEngine(state:BASE_WAIT)",
    "S40": "check_shutdown", "S41": "engine(event shutdown)", "S42": "commands.check_command",
    "S43": "transport.PortGuard·worker.Backoff", "S44": "SafetyEngine 발동·해제 1회씩(웹 서버가 챗으로)", "S45": "params.CROSS_RULES X22",
    "S46": "on_hold", "S47": "recipe.check_process_row", "S48": "config_store.apply",
    "S49": "engine", "S50": "drivers/dc.py 연결 초기화(DC_SET_PROTECTION)",
}

# 표와 평가 위치가 다른 규칙(이유)
WHERE_DIFFERS: Dict[str, str] = {
    "S9": "표 plc250 — 마커 2회 연속 판정은 PLC 드라이버가 폴링마다(250 ms) 하고, 엔진은 그 이벤트(plc_restarted)로 조치",
}


# ───────────── 값 보기 ─────────────
class View:
    """tick 행 또는 PLC 폴링 값. get(col) 은 그 블록 품질이 ok·late 일 때만 값, 아니면 None(모름)."""

    def __init__(self, values: Dict[str, Any], t: float, quality: Dict[str, str], ages: Dict[str, Optional[float]]):
        self.values, self.t, self.quality, self.ages = values, t, quality, ages

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "View":
        t = datetime.fromisoformat(row["ts"]).timestamp()
        bs = {k.split(".")[0] for k in row if k.endswith(".q") and k.count(".") == 1}
        return cls(row, t, {b: row[f"{b}.q"] for b in bs}, {b: row.get(f"{b}.age_s") for b in bs})

    @classmethod
    def from_poll(cls, poll) -> "View":
        bs = ("plc", "heater", "rf")
        return cls(poll.values, poll.t_wall, {b: "ok" for b in bs}, {b: 0.0 for b in bs})

    def q(self, block: str) -> str:
        return self.quality.get(block, "off")

    def age(self, block: str) -> Optional[float]:
        return self.ages.get(block)

    def get(self, col: str) -> Any:
        if self.q(col.split(".")[0]) not in FRESH:
            return None
        return self.values.get(col)

    def t_of(self, block: str) -> float:
        return self.t - (self.ages.get(block) or 0.0)


# ───────────── 공정 쪽(다음 단계 ProcessEngine 이 대신) ─────────────
@dataclass
class SimpleProcess:
    state: str = "IDLE"
    uses_dc: bool = False
    uses_rf: bool = False
    uses_rfp: bool = False
    uses_heater: bool = False
    heater_recipe_running: bool = False
    dc_holding: bool = False
    rf_holding: bool = False
    dc_set_w: Optional[float] = None
    rf_set_w: Optional[float] = None
    rfp_set_w: Optional[float] = None
    flow_set: Dict[str, Optional[float]] = field(default_factory=lambda: {"ar": None, "o2": None})
    sp1_mtorr: Optional[float] = None
    filament_by_process: bool = False
    comm_aborted: bool = False
    heater_gas_state: str = "IDLE"
    log: List[tuple] = field(default_factory=list)

    @property
    def active(self) -> bool:
        return self.state in ACT

    def on_stop(self) -> None:
        self.log.append(("stop",))
        if self.state in ACT:
            self.state = "STOPPING"

    def on_abort(self, reason: str) -> None:
        self.log.append(("abort", reason))
        if self.state in ACT:
            self.state = "STOPPING"

    def on_stop_done(self, kind: str) -> None:
        self.log.append(("stop_done", kind))
        if self.state == "STOPPING":
            self.state = "DONE" if kind == "stop" else "ABORTED"

    def heater_recipe_abort(self, reason: str) -> None:
        self.log.append(("heater_recipe_abort", reason))
        self.heater_recipe_running = False

    def heater_gas_hold(self) -> None:
        self.log.append(("heater_gas_hold",))


# ───────────── 규칙 ─────────────
def _abort(code):
    return Act("PROCESS_ABORT", {"reason": code})


def _note(level, code, **kw):
    return Act("NOTIFY", dict({"level": level, "code": code}, **kw))


def _fixed(*acts):
    return lambda v, ctx: list(acts)


def _p(key, scale=1.0):
    return lambda cfg: float(cfg[key]) * scale


def _age_ge(cfg, block, key, skip_suppressed=False):
    """두절 판정 — 나이 ≥ 한도. 값을 한 번도 못 받았으면(나이 없음) 모름."""
    def cond(v, ctx):
        if skip_suppressed and v.q(block) == "suppressed":
            return None
        a = v.age(block)
        return None if a is None else a >= cfg[key]
    return cond


def _all(*vals):
    return None if any(x is None for x in vals) else True


class _Dead:
    """히터 출력 죽음(S17·S18A) — RUN 참·fault 거짓·MV < 최소, RUN 상승 뒤 유예 지난 뒤부터."""

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.run_since: Optional[float] = None

    def __call__(self, v: View, ctx) -> Optional[bool]:
        run, fault, mv = v.get("heater.run"), v.get("heater.fault"), v.get("heater.mv")
        if run is None:
            return None
        if not run:
            self.run_since = None
            return False
        t = v.t_of("heater")
        if self.run_since is None:
            self.run_since = t
        if fault or mv is None:
            return False if fault else None
        if t - self.run_since < self.cfg["HEATER_OUT_DEAD_GRACE_SEC"]:
            return False
        return mv < self.cfg["HEATER_MV_MIN"]


def _dev(set_v, actual, tol_abs, ratio):
    if set_v is None or actual is None:
        return None
    return abs(actual - set_v) > max(tol_abs, set_v * ratio)


def _heater_fault_acts(v: View, ctx) -> List[Act]:
    cause = "OT" if v.get("heater.ot") else "TC" if v.get("heater.tc_err") else "WD" if v.get("heater.wd_err") else "?"
    acts = [Act("HEATER_FORCE_OFF")]
    if ctx.active and ctx.uses_heater:
        acts.append(_abort("HEATER_FAULT"))
    if ctx.heater_recipe_running:
        acts.append(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_FAULT"}))
    acts.append(_note("error", "HEATER_FAULT", detail=f"원인 {cause}"))
    return acts


def _s10_acts(v, ctx):
    ctx.comm_aborted = False
    return [Act("RF_SAFE_OFF"), Act("HEATER_FORCE_OFF"), Act("GAS_VALVES_CLOSE"), _note("warn", "PLC_RECOVERED_SAFE")]


def _s6_acts(v, ctx):
    ctx.comm_aborted = True
    return [_abort("PLC_COMM_LOST"), Act("BLACKBOX_DUMP", {"tag": "plc_comm_lost"})]


def _flow_dev(cfg, v: View, ctx) -> Optional[bool]:
    res = []
    for ch in ("ar", "o2"):
        sp = ctx.flow_set.get(ch)
        if not sp:
            continue
        x = v.get(f"mfc.{ch}_sccm")
        if x is None:
            return None
        res.append(abs(x - sp) > sp * cfg["FLOW_ERROR_TOLERANCE"])
    return any(res) if res else False


def _pressure_dev(cfg, v: View, ctx) -> Optional[bool]:
    sp = ctx.sp1_mtorr
    x = v.get("mfc.pressure_mtorr")
    if not sp or x is None:
        return None if sp else False
    return abs(x - sp) > sp * cfg["MFC_PRESSURE_WARN_RATIO"]


def build_rules(cfg: Dict[str, Any]) -> List[LiveRule]:
    L, C = LiveRule, cfg
    g = lambda col: (lambda v, ctx: v.get(col))
    neg = lambda col: (lambda v, ctx: None if v.get(col) is None else not v.get(col))
    rules = [
        # plc250
        L("S2", "abort", "plc250", neg("plc.mv"), _fixed(_abort("MV_CLOSED")), states=ACT, skip_when_stopping=True),
        L("S3", "abort", "plc250", neg("plc.mv_interlock"), _fixed(_abort("MV_INTERLOCK_LOST")),
          _p("MV_INTERLOCK_ABORT_MS", 0.001), states=ACT, skip_when_stopping=True, blip_code="MV_INTERLOCK_BLIP"),
        L("S16", "off", "plc250", g("heater.fault"), _heater_fault_acts),
        L("S17", "notify", "plc250", _Dead(C), _fixed(_note("warn", "HEATER_OUTPUT_DEAD_WARN")), _p("HEATER_OUT_DEAD_SEC")),
        # tick
        L("S6", "abort", "tick", _age_ge(C, "plc", "PLC_COMM_LOSS_ABORT_SEC"), _s6_acts, states=ACT),
        L("S7", "notify", "tick", _age_ge(C, "plc", "PLC_DISCONNECT_NOTIFY_SEC"),
          _fixed(_note("error", "PLC_DISCONNECTED"), Act("BLACKBOX_DUMP", {"tag": "plc_disconnected"})),
          on_release=_fixed(_note("info", "PLC_RECONNECTED"))),
        L("S18A", "off", "tick", _Dead(C), _fixed(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_OUTPUT_DEAD"})),
          _p("HEATER_OUT_DEAD_LIMIT_SEC"), applies=lambda c: c.heater_recipe_running, block="heater"),
        L("S18B", "off", "tick", neg("heater.run"), _fixed(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_RUN_OFF_EXTERNAL"})),
          _p("HEATER_RUN_OFF_LIMIT_SEC"), applies=lambda c: c.heater_recipe_running, block="heater"),
        L("S18C", "off", "tick",
          lambda v, c: None if v.q("heater") not in FRESH else bool(v.get("heater.tc_err") or v.get("heater.pv") is None),
          _fixed(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_TC_BAD"})), _p("HEATER_TC_BAD_LIMIT_SEC"),
          applies=lambda c: c.heater_recipe_running, block="heater"),
        L("S30", "abort", "tick", _age_ge(C, "dc", "DC_COMM_LOSS_ABORT_SEC"),
          _fixed(Act("DC_SAFE_OFF"), _abort("DC_COMM_LOST")), states=ACT, applies=lambda c: c.uses_dc),
        L("S31A", "abort", "tick",
          lambda v, c: None if _all(v.get("dc.i_set"), v.get("dc.p")) is None else
          (v.get("dc.i_set") >= C["DC_FAIL_ISET_A"] and v.get("dc.p") <= C["DC_FAIL_POWER_W"]),
          _fixed(_abort("DC_NO_OUTPUT")), _p("DC_FAIL_SEC"), states=frozenset({"POWER_WAIT"}),
          applies=lambda c: c.uses_dc, block="dc"),
        L("S31B", "abort", "tick",
          lambda v, c: None if v.get("dc.i") is None else v.get("dc.i") <= C["DC_MIN_CURRENT_ABORT"],
          _fixed(_abort("DC_LOW_CURRENT")), _p("DC_MIN_CURRENT_ABORT_SEC"), states=ACT,
          applies=lambda c: c.uses_dc and c.dc_holding, block="dc"),
        L("S31C", "abort", "tick",
          lambda v, c: _dev(c.dc_set_w if c.dc_set_w is not None else v.get("dc.set_w"), v.get("dc.p"),
                            C["DC_TOLERANCE_W"], C["DC_POWER_ERROR_RATIO"]),
          _fixed(_abort("DC_POWER_DEVIATION")), _p("DC_POWER_ERROR_SEC"), states=frozenset({"SHUTTER_DELAY", "MAIN"}),
          applies=lambda c: c.uses_dc, block="dc"),
        L("S32", "abort", "tick",
          lambda v, c: None if v.get("rf.ref_w") is None else v.get("rf.ref_w") >= C["RF_REFP_ABORT_THRESHOLD"],
          _fixed(Act("RF_SAFE_OFF"), _abort("RF_REFLECTED_HIGH")), _p("RF_REFP_WAIT_SEC"), states=ACT,
          applies=lambda c: c.uses_rf, block="rf"),
        L("S33A", "abort", "tick",
          lambda v, c: None if _all(v.get("rf.dac"), v.get("rf.fwd_w")) is None else
          (v.get("rf.dac") >= C["RF_FAIL_DAC_COUNTS"] and v.get("rf.fwd_w") <= C["RF_FAIL_FWD_W"]),
          _fixed(Act("RF_SAFE_OFF"), _abort("RF_NO_OUTPUT")), _p("RF_FAIL_SEC"), states=frozenset({"POWER_WAIT"}),
          applies=lambda c: c.uses_rf, block="rf"),
        L("S33B", "abort", "tick",
          lambda v, c: _dev(c.rf_set_w if c.rf_set_w is not None else v.get("rf.set_w"), v.get("rf.fwd_w"),
                            C["RF_TOLERANCE_W"], C["RF_POWER_ERROR_RATIO"]),
          _fixed(Act("RF_SAFE_OFF"), _abort("RF_POWER_DEVIATION")), _p("RF_POWER_ERROR_SEC"), states=ACT,
          applies=lambda c: c.uses_rf and c.rf_holding, block="rf"),
        L("S34A", "abort", "tick",
          lambda v, c: None if v.get("rfp.out_on") is None else (bool(v.get("rfp.out_on")) and _dev(
              c.rfp_set_w if c.rfp_set_w is not None else v.get("rfp.set_w"), v.get("rfp.fwd_w"), 0.0,
              C["RFPULSE_FORP_TOLERANCE_PERCENT"] / 100.0) is True),
          _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_FORWARD_DEVIATION")), _p("RFPULSE_FORP_SEC"), states=ACT,
          applies=lambda c: c.uses_rfp, block="rfp"),
        L("S34B", "abort", "tick",
          lambda v, c: None if v.get("rfp.ref_w") is None else v.get("rfp.ref_w") >= C["RFPULSE_REFP_LIMIT_WATTS"],
          _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_REFLECTED_HIGH")), _p("RFPULSE_REFP_SEC"), states=ACT,
          applies=lambda c: c.uses_rfp, block="rfp"),
        L("S34C", "abort", "tick", _age_ge(C, "rfp", "RFPULSE_COMM_LOSS_ABORT_SEC"),
          _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_COMM_LOST")), states=ACT, applies=lambda c: c.uses_rfp),
        L("S38A", "notify", "tick", lambda v, c: _flow_dev(C, v, c), _fixed(_note("warn", "FLOW_DEVIATION")), _p("FLOW_ERROR_SEC"),
          states=frozenset({"MAIN"}), block="mfc"),
        L("S38B", "notify", "tick", lambda v, c: _pressure_dev(C, v, c), _fixed(_note("warn", "PRESSURE_DEVIATION")), _p("MFC_PRESSURE_WARN_SEC"),
          states=frozenset({"MAIN"}), block="mfc"),
        L("S39B", "abort", "tick", _age_ge(C, "ig", "IG_COMM_LOSS_ABORT_SEC", skip_suppressed=True),
          _fixed(_abort("IG_COMM_LOST")), states=frozenset({"BASE_WAIT"})),
        L("S49", "abort", "tick", g("rfp.itl_open"), _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_FAULT")), states=ACT,
          applies=lambda c: c.uses_rfp, block="rfp"),
        # 이벤트
        L("S9", "abort", "plc_restarted", lambda v, c: True,
          lambda v, c: ([_abort("PLC_RESTARTED")] if c.active else []) + [_note("error", "PLC_RESTARTED")]),
        L("S10", "off", "plc_link_up", lambda v, c: True, _s10_acts,
          states=frozenset({"IDLE", "LOADED", "ABORTED"}), applies=lambda c: c.comm_aborted),
        L("S15", "off", "plc_first_poll", lambda v, c: True,
          _fixed(Act("HEATER_RESIDUAL_CLEAR"), _note("warn", "HEATER_RESIDUAL_RUN"))),
        L("S26", "maintain", "heater_run_falling",
          lambda v, c: None if v.get("heater.pv") is None else v.get("heater.pv") > C["HEATER_GAS_HOLD_RELEASE_C"],
          _fixed(Act("HEATER_GAS_HOLD")), states=frozenset({"IDLE", "LOADED"})),
        L("S27", "off", "heater_gas_failed", lambda v, c: True,
          _fixed(Act("GAS_FLOW_OFF"), Act("GAS_EXHAUST"), Act("GAS_VALVES_CLOSE"), _note("error", "HEATER_GAS_FAILED"))),
        L("S34D", "abort", "rfp_start_failed", lambda v, c: True, _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_START_FAILED")),
          states=frozenset({"POWER_WAIT"})),
        L("S41", "maintain", "shutdown", lambda v, c: True, _fixed(Act("HEATER_FORCE_OFF"))),
    ]
    return rules


EVENT_WHERE = ("plc_restarted", "plc_link_up", "plc_first_poll", "heater_run_falling", "heater_gas_failed",
               "rfp_start_failed", "shutdown")


# ───────────── 조치 처리기 ─────────────
def _req(done):
    return lambda r: done(None if r.status == Status.OK else (r.reason or r.status))


def _seq(done):
    return lambda name, reason, results: done(reason)


class ChkSafety:
    """엔진 + 조치 처리기 + 정지 순서 + 연결 고리."""

    def __init__(self, clock, cfg: Dict[str, Any], process, plc, mfc, dc, rfp, ig,
                 on_event: Optional[Callable[[SafetyEvent], None]] = None, heater=None) -> None:
        self.clock, self.cfg, self.process = clock, cfg, process
        self.heater = heater                      # HeaterEngine(있으면 히터 레시피 중단·가스 유지를 여기로)
        self.plc, self.mfc, self.dc, self.rfp, self.ig = plc, mfc, dc, rfp, ig
        self.engine = SafetyEngine(clock, build_rules(cfg), cfg, self._handlers(), process, on_event)
        self.engine.stop_steps = self.stop_steps
        self.engine.all_stop_steps = self.all_stop_steps
        self.locks: set = set()
        self._prev_run: Optional[bool] = None
        self.notifications: List[dict] = []
        self.poll_eval_s: List[float] = []        # plc250 평가 시각(시험: 폴링 → 조치 지연)

    # ── 연결 고리 ──
    def on_poll(self, poll) -> None:
        v = View.from_poll(poll)
        self.engine.evaluate("plc250", v.t, v, self.process)
        self.poll_eval_s.append(self.clock.monotonic())
        run = poll.values.get("heater.run")
        if self._prev_run and run is False:
            self.event("heater_run_falling", v)
        self._prev_run = run

    def on_row(self, row: Dict[str, Any]) -> None:
        v = View.from_row(row)
        self.engine.evaluate("tick", v.t, v, self.process)
        stale = self.cfg["HEATER_STALE_SEC"]
        self.locks = {b for b in DEVICE_BLOCKS
                      if v.q(b) not in ("ok", "late", "suppressed", "off") and (v.age(b) is None or v.age(b) >= stale)}

    def on_driver_event(self, e) -> None:
        name = {"plc_restarted": "plc_restarted", "heater_residual_run": "plc_first_poll",
                "link_up": "plc_link_up"}.get(getattr(e, "kind", ""))
        if name and type(e).__name__ == "PlcEvent":
            last = self.plc.last
            self.event(name, View.from_poll(last) if last is not None else View({}, self.clock.wall(), {}, {}))

    def on_rfp_start_done(self, name, reason, results) -> None:
        if reason and reason != "BENCH_READ_ONLY":
            self.event("rfp_start_failed")

    def event(self, name: str, view: Optional[View] = None) -> List[str]:
        v = view or View({}, self.clock.wall(), {}, {})
        fired = self.engine.evaluate(name, v.t, v, self.process)
        for r in self.engine.rules:
            if r.where == name:
                self.engine.reset_rule(r.id)
        return fired

    # ── 사용자 명령 ──
    def stop(self) -> bool:
        return self.engine.stop()

    def all_stop(self) -> None:
        self.engine.all_stop()

    # ── 거부 검사 ──
    def check_start(self, view: View, recipe_uses_heater: bool) -> Optional[str]:
        """공정 시작 guard(S1·S5·S19) — 사유 코드 또는 None. 레시피 검사(S35·S47)는 recipe.check_process_row."""
        if self.process.heater_gas_state != "IDLE":
            return "HEATER_GAS_ACTIVE"                                   # S5
        if view.q("plc") not in FRESH or (view.age("plc") or 0.0) >= self.cfg["HEATER_STALE_SEC"]:
            return "MV_STATE_UNKNOWN"                                    # S1(읽기 실패도 거부)
        if not view.get("plc.mv"):
            return "MV_NOT_OPEN"
        if not view.get("plc.mv_interlock"):
            return "MV_INTERLOCK_OFF"
        if recipe_uses_heater:
            return self._heater_guard(view)                              # S19
        return None

    def check_heater_start(self, view: View) -> Optional[str]:
        if self.process.active:
            return "PROCESS_RUNNING"                                     # S5(히터 가스)
        return self._heater_guard(view)

    def _heater_guard(self, view: View) -> Optional[str]:
        if view.q("heater") not in FRESH:
            return "HEATER_ITL_OFF"
        if view.get("heater.fault"):
            return "HEATER_FAULT_ACTIVE"
        if not view.get("heater.itl"):
            return "HEATER_ITL_OFF"
        return None

    def check_shutdown(self) -> Optional[str]:
        if self.process.active or self.process.state == "STOPPING":
            return "PROCESS_RUNNING"                                     # S40
        if self.process.heater_gas_state != "IDLE":
            return "HEATER_GAS_ACTIVE"
        return None

    # ── 정지 순서 ──
    def _to(self, key: str) -> float:
        return float(self.cfg[key or "STOP_STEP_WAIT_DEFAULT_SEC"])

    def _steps(self, table, when: Dict[str, Callable[[], bool]], args: Dict[str, dict]) -> List[Step]:
        out = []
        for s in table:
            a = s.act
            if a.kind in args:
                a = Act(a.kind, dict(a.args, **args[a.kind]))
            out.append(Step(s.no, a, self._to(s.timeout_param), s.must, when.get(f"{s.no}", lambda: True), s.desc))
        return out

    def stop_steps(self) -> List[Step]:
        p = self.process
        rf_on = lambda: p.uses_rf or bool(self.plc.last and (self.plc.last.values.get("rf.dac") or 0) > 0)
        return self._steps(SR.STOP_SEQUENCE, {"1": lambda: p.filament_by_process, "3": lambda: p.uses_heater,
                                              "6": rf_on}, {})

    def all_stop_steps(self) -> List[Step]:
        p = self.process
        fil = lambda: bool(self.ig.last and self.ig.last.filament) or p.filament_by_process
        return self._steps(SR.ALL_STOP_SEQUENCE,
                           {"1": lambda: p.heater_recipe_running, "2": lambda: p.active, "9": fil},
                           {"PLC_OUTPUTS_OFF": {"keep_pumps": bool(self.cfg["ALL_STOP_KEEP_PUMPS"])}})

    # ── 처리기 ──
    def _handlers(self) -> Dict[str, Callable]:
        plc, mfc, dc, rfp, ig, p = self.plc, self.mfc, self.dc, self.rfp, self.ig, self.process

        def notify(a, done):
            self.notifications.append(a)
            done(None)

        def abort_in_seq(a, done):                    # ALL STOP 2단계 — 공정 표시만(정지 순서는 ALL STOP 이 이미 실행 중)
            if p.active:
                p.on_abort(a.get("reason", "ALL_STOP"))
            done(None)

        def heater_off(a, done):
            plc.heater_run(False, on_done=_req(done))

        def residual(a, done):
            plc.heater_run(False)
            pv = plc.last.values.get("heater.pv") if plc.last else None
            if pv is not None:
                plc.heater_set_sv(pv, on_done=_req(done))
            else:
                done(None)

        def ramp_down(a, done):
            step = max(1, int(self.cfg["RF_RAMP_DOWN_STEP_COUNTS"]))
            cur = int((plc.last.values.get("rf.dac") if plc.last else 0) or 0)
            deadline = self.clock.monotonic() + max(0.0, self.cfg["STOP_RF_RAMPDOWN_TIMEOUT_SEC"] - 1.0)
            while cur > 0 and self.clock.monotonic() < deadline:
                cur = max(0, cur - step)
                plc.rf_set_dac(cur)
                self.clock.wait_until(self.clock.monotonic() + 0.5)
            plc.rf_set_dac(0, on_done=_req(done))          # 한도를 넘었으면 즉시 0

        def coils_off(names):
            return lambda a, done: plc.coils_off(names(a), on_done=_req(done))

        return {
            "PROCESS_ABORT": abort_in_seq,
            "NOTIFY": notify,
            "HEATER_FORCE_OFF": heater_off,
            "HEATER_RESIDUAL_CLEAR": residual,
            "HEATER_RECIPE_ABORT": lambda a, done: ((self.heater.recipe_abort if self.heater else p.heater_recipe_abort)(
                a.get("reason", "")), done(None)),
            "HEATER_GAS_HOLD": lambda a, done: ((self.heater.gas_hold if self.heater else p.heater_gas_hold)(), done(None)),
            "DC_SAFE_OFF": lambda a, done: dc.output_off_confirmed(on_done=_seq(done)),
            "RFP_SAFE_OFF": lambda a, done: rfp.rf_off_confirmed(on_done=_req(done)),
            "RF_SAFE_OFF": lambda a, done: plc.rf_off(on_done=_req(done)),
            "RF_RAMP_DOWN": ramp_down,
            "GAS_FLOW_OFF": lambda a, done: mfc.all_flow_off(on_done=_seq(done)),
            "GAS_EXHAUST": lambda a, done: mfc.valve(True, on_done=_seq(done)),
            "GAS_VALVES_CLOSE": coils_off(lambda a: ["ar_valve", "o2_valve"]),
            "SHUTTERS_CLOSE": coils_off(lambda a: a.get("which", "ms").split(",")),
            "PLC_OUTPUTS_OFF": lambda a, done: plc.outputs_off(bool(a.get("keep_pumps", True)), on_done=_req(done)),
            "IG_FILAMENT_OFF": lambda a, done: ig.filament_off(on_done=_req(done)),
            "BLACKBOX_DUMP": lambda a, done: (plc.dump_blackbox(a.get("tag", "safety")), done(None)),
        }
