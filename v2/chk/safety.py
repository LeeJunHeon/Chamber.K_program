"""챔버K 안전 규칙 실행 — safety_rules.py 표의 조건을 코드로, 조치를 드라이버 호출로 잇는다.

연결
- plc250 규칙: PlcDriver.fast_listeners → on_poll(PLC 250 ms 폴링 직후, PLC worker 스레드). 조치는 바로 safety 우선순위.
- tick 규칙  : TickLoop sink → on_row(1초 행이 닫힌 뒤, core 스레드).
- 이벤트 규칙: on_driver_event(드라이버 이벤트) · event(이름) — plc_link_up · plc_first_poll · plc_restarted ·
  heater_run_falling · heater_gas_failed · rfp_start_failed · shutdown.
- 명령·설정 거부 규칙: check_start · check_heater_start · check_shutdown(여기) + 이미 있는 곳(IMPLEMENTED_IN).

공정 쪽(process)은 state·active·on_abort(reason)·on_stop_done(kind)·uses_* 를 가진 객체 — 다음 단계 ProcessEngine 이
같은 모양으로 대신한다. 지금은 SimpleProcess(시험·시험대용).

ALL STOP 은 v1 처럼 전부 끈다(사용자 결정 2026-10-20 — 펌프 유지 옵션 없음). on_hold(S46)는 실행하지 않는다.

적용 구간(블록 공정, 사용자 결정 2026-10-20): 공정 상태 이름 대신 장비·블록 상태로 —
무출력 S31A·S33A = 그 장비 램프 중(dc_ramping·rf_ramping), 두절·반사파 S30·S34C·S32 = 그 장비 출력 명령 뒤(dc_on·rfp_on·rf_on),
이탈 감시 S31C·S33B·S34A·S38A·S38B = 지금 블록의 감시(watching), S39B = 베이스 압력 대기 블록, S34D = RF Pulse 를 쓰는 행(시작 실패 이벤트는 START 에서만 온다).
공정 중 수정(PROCESS_ADJUST) 뒤에는 새 목표에 다시 도달할 때까지 S31C(dc_settling)·S34A(rfp_settling)를 쉬고,
RF 는 램프로 돌아가 유지 중에만 보는 S33B 가 저절로 쉰다. 기본 템플릿에서는 지금(5-3)과 같은 판정이다(SimpleProcess 는 상태 이름으로 같은 값을 낸다 — blocks.watch_for_state).
STOP·이상 중단 정지 순서는 고정(SR.STOP_SEQUENCE 전체, v1 처럼 — 사용자 결정 2026-10-21). 정상 끝 정지(end_row)만
end_stop 블록의 skip 으로 단계를 뺄 수 있다(파워 끄기 단계 SR.END_STOP_MUST_KEEP 는 못 뺀다). ALL STOP 도 고정.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from v2.common.safety_engine import LiveRule, SafetyEngine, SafetyEvent, Step
from v2.common.worker import Status
from v2.chk import blocks as BK
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
    "S18D": "HeaterEngine(레시피 스텝) — 비정상 끝은 ChkSafety.heater_recipe_failed 로 공정도 중단", "S18E": "next:ProcessEngine(state:HEATER_RAMP 한도)",
    "S19": "check_start·check_heater_start", "S20": "commands.check_command(Pre HEATER_RUN_OFF)",
    "S21": "commands.check_command(param_max HEATER_MAX_TEMP)", "S22": "next:HeaterEngine(유지 모드)",
    "S23": "next:ProcessEngine(state:HEATER_HOLD_WAIT)", "S24": "next:HeaterEngine(포화 — 앞창·폴백)",
    "S25": "next:HeaterEngine(감속 접근)", "S26": "engine(event heater_run_falling)",
    "S27": "engine(event heater_gas_failed)", "S28": "locks", "S29": "drivers/dc.py output_off_confirmed",
    "S30": "engine + ChkSafety.recover_after_comm_loss(복구 때 DC_SAFE_OFF)", "S31A": "engine", "S31B": "engine", "S31C": "engine", "S32": "engine", "S33A": "engine",
    "S33B": "engine", "S34A": "engine", "S34B": "engine", "S34C": "engine + ChkSafety.recover_after_comm_loss(복구 때 RFP_SAFE_OFF)", "S34D": "engine(event rfp_start_failed)",
    "S35": "recipe.check_process_row + drivers/rfpulse.py 되읽기", "S36": "next:ProcessEngine(state:POWER_WAIT)",
    "S37": "next:ProcessEngine(state:PRESSURE_SP1)", "S38A": "engine", "S38B": "engine",
    "S39A": "next:ProcessEngine(state:BASE_WAIT)", "S39B": "engine", "S39C": "next:ProcessEngine(state:BASE_WAIT)",
    "S40": "check_shutdown", "S41": "engine(event shutdown)", "S42": "commands.check_command",
    "S43": "transport.PortGuard·worker.Backoff", "S44": "SafetyEngine 발동·해제 1회씩(웹 서버가 챗으로)", "S45": "params.CROSS_RULES X22",
    "S46": "on_hold", "S47": "recipe.check_process_row", "S48": "config_store.apply",
    "S49": "engine", "S50": "drivers/dc.py 연결 초기화(DC_SET_PROTECTION)",
    "S51": "ChkSafety.first_read(장비별 첫 정상 행 — 프로그램이 켜진 뒤 한 번)",
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
    dc_on: bool = False                 # 이번 행에서 출력 명령을 냈다(정지 순서·두절 판정 구간)
    rf_on: bool = False
    rfp_on: bool = False
    rf_ref_waiting: bool = False
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
    dc_comm_aborted: bool = False
    rfp_comm_aborted: bool = False
    heater_gas_state: str = "IDLE"
    dc_ramp: Optional[bool] = None      # None 이면 상태 이름으로(POWER_WAIT 이고 유지 중이 아님)
    rf_ramp: Optional[bool] = None
    watch: Optional[Dict[str, bool]] = None   # None 이면 상태 이름으로(blocks.watch_for_state — 기본 템플릿과 같은 감시)
    log: List[tuple] = field(default_factory=list)

    @property
    def active(self) -> bool:
        return self.state in ACT

    @property
    def dc_ramping(self) -> bool:
        return self.dc_ramp if self.dc_ramp is not None else (self.state == "POWER_WAIT" and not self.dc_holding)

    @property
    def rf_ramping(self) -> bool:
        return self.rf_ramp if self.rf_ramp is not None else (self.state == "POWER_WAIT" and not self.rf_holding)

    @property
    def block_type(self) -> Optional[str]:
        return BK.STAGE_TO_TYPE.get(self.state) if self.state in ACT else None

    def watching(self, item: str) -> bool:
        w = self.watch if self.watch is not None else BK.watch_for_state(self.state)
        return bool(w.get(item, False))

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

    def __init__(self, cfg, grace: bool = True) -> None:
        self.cfg = cfg
        self.grace = grace                          # S18A(레시피)는 유예 없이(v1 PLC.py:1013-1015, heater_recipe.py:1161-1165)
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
        if self.grace and t - self.run_since < self.cfg["HEATER_OUT_DEAD_GRACE_SEC"]:
            return False
        return mv < self.cfg["HEATER_MV_MIN"]


def _dev(set_v, actual, tol_abs, ratio):
    if set_v is None or actual is None:
        return None
    return abs(actual - set_v) > max(tol_abs, set_v * ratio)


def _heater_fault_acts(v: View, ctx) -> List[Act]:
    cause = "OT" if v.get("heater.ot") else "TC" if v.get("heater.tc_err") else "WD" if v.get("heater.wd_err") else "?"
    acts = [Act("HEATER_FORCE_OFF")]
    if ctx.active and (ctx.uses_heater or ctx.heater_recipe_running):     # v1 main.py:3620-3622
        acts.append(_abort("HEATER_FAULT"))
    if ctx.heater_recipe_running:
        acts.append(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_FAULT"}))
    acts.append(_note("error", "HEATER_FAULT", detail=f"원인 {cause}"))
    return acts


def _s10_acts(v, ctx):
    ctx.comm_aborted = False
    return [Act("RF_SAFE_OFF"), Act("HEATER_FORCE_OFF"), Act("GAS_VALVES_CLOSE"), _note("warn", "PLC_RECOVERED_SAFE")]


def _s30_acts(v, ctx):
    ctx.dc_comm_aborted = True                     # 링크가 돌아오면 공정이 없을 때 DC_SAFE_OFF 다시(v1 main.py:1491-1497)
    return [Act("DC_SAFE_OFF"), _abort("DC_COMM_LOST")]


def _s34c_acts(v, ctx):
    ctx.rfp_comm_aborted = True                    # v1 main.py:1511-1517
    return [Act("RFP_SAFE_OFF"), _abort("RFP_COMM_LOST")]


def _dev_ge(set_v, actual, ratio):
    """RF Pulse 순방향 이탈 — v1 RFpulse.py:1245 diff >= tol(경계 포함)."""
    if set_v is None or actual is None:
        return None
    return abs(actual - set_v) >= set_v * ratio


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
        L("S18A", "off", "tick", _Dead(C, grace=False), _fixed(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_OUTPUT_DEAD"})),
          _p("HEATER_OUT_DEAD_LIMIT_SEC"), applies=lambda c: c.heater_recipe_running, block="heater"),
        L("S18B", "off", "tick", neg("heater.run"), _fixed(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_RUN_OFF_EXTERNAL"})),
          _p("HEATER_RUN_OFF_LIMIT_SEC"), applies=lambda c: c.heater_recipe_running, block="heater"),
        L("S18C", "off", "tick",
          lambda v, c: None if v.q("heater") not in FRESH else bool(v.get("heater.tc_err") or v.get("heater.pv") is None),
          _fixed(Act("HEATER_RECIPE_ABORT", {"reason": "HEATER_TC_BAD"})), _p("HEATER_TC_BAD_LIMIT_SEC"),
          applies=lambda c: c.heater_recipe_running, block="heater"),
        # 판정 구간(v1 DCpower.py:242-243·454-458): DC 를 켠 뒤(POWER_WAIT 에서 출력 명령을 낸 뒤)만
        L("S30", "abort", "tick", _age_ge(C, "dc", "DC_COMM_LOSS_ABORT_SEC"), _s30_acts, states=ACT,
          applies=lambda c: c.uses_dc and bool(getattr(c, "dc_on", False))),
        L("S31A", "abort", "tick",
          lambda v, c: None if _all(v.get("dc.i_set"), v.get("dc.p")) is None else
          (v.get("dc.i_set") >= C["DC_FAIL_ISET_A"] and v.get("dc.p") <= C["DC_FAIL_POWER_W"]),
          _fixed(_abort("DC_NO_OUTPUT")), _p("DC_FAIL_SEC"), states=ACT,
          applies=lambda c: c.uses_dc and c.dc_ramping, block="dc"),
        L("S31B", "abort", "tick",
          lambda v, c: None if v.get("dc.i") is None else v.get("dc.i") <= C["DC_MIN_CURRENT_ABORT"],
          _fixed(_abort("DC_LOW_CURRENT")), _p("DC_MIN_CURRENT_ABORT_SEC"), states=ACT,
          applies=lambda c: c.uses_dc and c.dc_holding, block="dc"),
        L("S31C", "abort", "tick",
          lambda v, c: _dev(c.dc_set_w if c.dc_set_w is not None else v.get("dc.set_w"), v.get("dc.p"),
                            C["DC_TOLERANCE_W"], C["DC_POWER_ERROR_RATIO"]),
          _fixed(_abort("DC_POWER_DEVIATION")), _p("DC_POWER_ERROR_SEC"), states=ACT,
          applies=lambda c: c.uses_dc and c.watching("dc") and not getattr(c, "dc_settling", False), block="dc"),
        # RF(아날로그) 이상 중단은 v1 처럼 정지 순서의 램프다운(8 카운트/0.5 s) 뒤 OFF — 즉시 0 은 ALL STOP 만
        # (v1 RFpower.py:100-102·124-131·158-167·210-214). 반사파는 RF 를 켠 뒤만(RFpower.py:79-81),
        # 반사파 대기 중에는 무출력·이탈 판정을 멈춘다(RFpower.py:91-104).
        L("S32", "abort", "tick",
          lambda v, c: None if v.get("rf.ref_w") is None else v.get("rf.ref_w") >= C["RF_REFP_ABORT_THRESHOLD"],
          _fixed(_abort("RF_REFLECTED_HIGH")), _p("RF_REFP_WAIT_SEC"), states=ACT,
          applies=lambda c: c.uses_rf and bool(getattr(c, "rf_on", False)), block="rf"),
        L("S33A", "abort", "tick",
          lambda v, c: None if _all(v.get("rf.dac"), v.get("rf.fwd_w")) is None else
          (v.get("rf.dac") >= C["RF_FAIL_DAC_COUNTS"] and v.get("rf.fwd_w") <= C["RF_FAIL_FWD_W"]),
          _fixed(_abort("RF_NO_OUTPUT")), _p("RF_FAIL_SEC"), states=ACT,
          applies=lambda c: c.uses_rf and c.rf_ramping and not getattr(c, "rf_ref_waiting", False), block="rf"),
        L("S33B", "abort", "tick",
          lambda v, c: _dev(c.rf_set_w if c.rf_set_w is not None else v.get("rf.set_w"), v.get("rf.fwd_w"),
                            C["RF_TOLERANCE_W"], C["RF_POWER_ERROR_RATIO"]),
          _fixed(_abort("RF_POWER_DEVIATION")), _p("RF_POWER_ERROR_SEC"), states=ACT,
          applies=lambda c: c.uses_rf and c.rf_holding and not getattr(c, "rf_ref_waiting", False) and c.watching("rf"),
          block="rf"),
        L("S34A", "abort", "tick",
          lambda v, c: None if v.get("rfp.out_on") is None else (bool(v.get("rfp.out_on")) and _dev_ge(
              c.rfp_set_w if c.rfp_set_w is not None else v.get("rfp.set_w"), v.get("rfp.fwd_w"),
              C["RFPULSE_FORP_TOLERANCE_PERCENT"] / 100.0) is True),
          _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_FORWARD_DEVIATION")), _p("RFPULSE_FORP_SEC"), states=ACT,
          applies=lambda c: c.uses_rfp and c.watching("rfp") and not getattr(c, "rfp_settling", False), block="rfp"),
        L("S34B", "abort", "tick",
          lambda v, c: None if v.get("rfp.ref_w") is None else v.get("rfp.ref_w") >= C["RFPULSE_REFP_LIMIT_WATTS"],
          _fixed(Act("RFP_SAFE_OFF"), _abort("RFP_REFLECTED_HIGH")), _p("RFPULSE_REFP_SEC"), states=ACT,
          applies=lambda c: c.uses_rfp, block="rfp"),
        # 판정 구간(v1 RFpulse.py:984·1150-1161): RF Pulse START 를 낸 뒤만
        L("S34C", "abort", "tick", _age_ge(C, "rfp", "RFPULSE_COMM_LOSS_ABORT_SEC"), _s34c_acts, states=ACT,
          applies=lambda c: c.uses_rfp and bool(getattr(c, "rfp_on", False))),
        L("S38A", "notify", "tick", lambda v, c: _flow_dev(C, v, c), _fixed(_note("warn", "FLOW_DEVIATION")), _p("FLOW_ERROR_SEC"),
          states=ACT, applies=lambda c: c.watching("flow"), block="mfc"),
        L("S38B", "notify", "tick", lambda v, c: _pressure_dev(C, v, c), _fixed(_note("warn", "PRESSURE_DEVIATION")), _p("MFC_PRESSURE_WARN_SEC"),
          states=ACT, applies=lambda c: c.watching("pressure"), block="mfc"),
        L("S39B", "abort", "tick", _age_ge(C, "ig", "IG_COMM_LOSS_ABORT_SEC", skip_suppressed=True),
          _fixed(_abort("IG_COMM_LOST")), states=ACT, applies=lambda c: c.block_type == "base_wait"),
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
          states=ACT, applies=lambda c: c.uses_rfp),
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
        self.first_done: set = set()              # S51 — 첫 정상 읽기를 본 장비(재연결 때는 다시 하지 않음)
        self.residual: List[tuple] = []           # S51 조치 기록 (장비, 코드, 조치)

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
        self.first_read(v)
        self.engine.evaluate("tick", v.t, v, self.process)
        self.recover_after_comm_loss(v)
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

    # ── S30·S34C 해제 동작 — 두절로 중단한 장비의 링크가 돌아오면 ──
    def recover_after_comm_loss(self, v: View) -> None:
        """DC·RF Pulse 통신 두절로 공정을 중단한 뒤 링크가 돌아오면: 공정이 없으면 OFF(확인형)를 한 번 더, 공정 중이면 생략
        (v1 main.py:1488-1517 _on_dc_recovered·_on_rfpulse_recovered)."""
        p = self.process
        for blk, flag, act, code in (("dc", "dc_comm_aborted", "DC_SAFE_OFF", "DC_RECOVERED_SAFE"),
                                     ("rfp", "rfp_comm_aborted", "RFP_SAFE_OFF", "RFP_RECOVERED_SAFE")):
            if not getattr(p, flag, False) or v.q(blk) not in FRESH:
                continue
            setattr(p, flag, False)
            rule = "S30" if blk == "dc" else "S34C"
            if p.active or p.state == "STOPPING":
                self.notifications.append({"level": "info", "code": code, "detail": "공정 진행 중이라 안전 상태 재적용 생략"})
                continue
            self.notifications.append({"level": "info", "code": code, "detail": f"통신 복구 → {act}"})
            self.engine._emit(SafetyEvent("released", rule, code, f"통신 복구 → {act}", "info", self.clock.wall()))
            self.engine._do(Act(act), rule, self.clock.wall())

    # ── S51 시작 때 남은 출력 정리 ──
    def _s51(self, level: str, code: str, detail: str, act: Optional[str] = None, dev: str = "") -> None:
        self.notifications.append({"level": level, "code": code, "detail": detail})
        self.engine._emit(SafetyEvent("fired", "S51", code, detail, level, self.clock.wall()))
        if act:
            self.residual.append((dev, code, act))
            self.engine._do(Act(act), "S51", self.clock.wall())

    def first_read(self, v: View) -> None:
        """장비별 첫 정상 행(프로그램이 켜진 뒤 한 번 — 재연결 때는 아님). bench 에서는 아무것도 하지 않는다."""
        if self.cfg.get("RUN_MODE") == "bench":
            return
        p = self.process
        busy = bool(p.active or getattr(p, "heater_recipe_running", False) or self.engine.stopping)
        tail = " — 공정·히터 레시피 진행 중이라 끄지 않음" if busy else ""
        if "plc" not in self.first_done and v.q("plc") in FRESH:
            self.first_done.add("plc")
            dac, en, fwd = v.get("rf.dac") or 0, bool(v.get("rf.dac_enable")), v.get("rf.fwd_w")
            if dac > 0:
                self._s51("warn", "RF_RESIDUAL_OUTPUT", f"D40 {dac} · 순방향 {fwd} W{tail}")
                if not busy:                                     # STOP 순서와 같은 램프다운(따로 스레드 — tick 을 막지 않음)
                    self.residual.append(("plc", "RF_RESIDUAL_OUTPUT", "RF_RAMP_DOWN+RF_SAFE_OFF"))
                    self.engine._start_seq("residual_rf", [
                        Step(1, Act("RF_RAMP_DOWN"), float(self.cfg["STOP_RF_RAMPDOWN_TIMEOUT_SEC"]), True, desc="RF 램프다운"),
                        Step(2, Act("RF_SAFE_OFF"), float(self.cfg["STOP_STEP_WAIT_DEFAULT_SEC"]), True, desc="D40 0 → enable OFF")])
            elif en:
                self._s51("info", "RF_ENABLE_RESIDUAL", f"M00200 켜짐 · D40 0 · 순방향 {fwd} W — v1 정상 종료 흔적{tail}",
                          None if busy else "RF_SAFE_OFF", "plc")
        if "dc" not in self.first_done and v.q("dc") in FRESH:
            self.first_done.add("dc")
            if getattr(self.dc, "out_at_connect", None):
                self._s51("warn", "DC_RESIDUAL_OUTPUT", f"연결 때 OUTP? 1{tail}", None if busy else "DC_SAFE_OFF", "dc")
        if self.cfg.get("RFPULSE_ENABLED", True) and "rfp" not in self.first_done and v.q("rfp") in FRESH:
            self.first_done.add("rfp")
            if getattr(self.rfp, "out_at_connect", None):
                host = getattr(self.rfp, "control_at_connect", None) == 2
                if host and not busy:
                    self._s51("warn", "RFP_RESIDUAL_OUTPUT", "연결 때 RF Pulse 출력 켜짐(Host) — 끔", "RFP_SAFE_OFF", "rfp")
                else:
                    self._s51("warn", "RFP_RESIDUAL_OUTPUT",
                              "연결 때 RF Pulse 출력 켜짐" + (tail or " — 앞면(Host 아님) 모드라 끄지 않음"))
        if "mfc" not in self.first_done and v.q("mfc") in FRESH:
            self.first_done.add("mfc")
            bits = getattr(self.mfc, "onoff_at_connect", None) or ""
            flow = max(v.get("mfc.ar_sccm") or 0.0, v.get("mfc.o2_sccm") or 0.0)
            if "1" in bits[:2] or flow > 0.1:
                self._s51("warn", "GAS_RESIDUAL_FLOW", f"R69 {bits or '?'} · 유량 {flow:g} sccm — 끄지 않음(확인)")

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
        why = self.check_mv(view)
        if why:
            return why
        if recipe_uses_heater:
            return self._heater_guard(view)                              # S19
        return None

    def check_mv(self, view: View) -> Optional[str]:
        """S1 메인 밸브 — MV·MV_INTERLOCK 이 모두 켜져 있고 PLC 값이 신선해야(v1 main.py:3675 _check_main_valve_open)."""
        if view.q("plc") not in FRESH or (view.age("plc") or 0.0) >= self.cfg["HEATER_STALE_SEC"]:
            return "MV_STATE_UNKNOWN"                                    # 읽기 실패도 거부
        if not view.get("plc.mv"):
            return "MV_NOT_OPEN"
        if not view.get("plc.mv_interlock"):
            return "MV_INTERLOCK_OFF"
        return None

    def plc_view(self) -> View:
        last = self.plc.last
        if last is None:
            return View({}, self.clock.wall(), {}, {})
        v = View.from_poll(last)
        v.ages = {b: max(0.0, self.clock.wall() - last.t_wall) for b in ("plc", "heater", "rf")}
        return v

    def heater_recipe_failed(self, code: str) -> None:
        """히터 레시피가 비정상으로 끝남(사용자 정지 아님) — 같이 돌던 공정도 중단(v1 main.py:3380·3443)."""
        if code and code not in ("USER_STOP",) and self.process.active:
            self.engine.abort(code, rule="heater_recipe")

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
            out.append(Step(s.no, a, self._to(s.timeout_param), s.must, when.get(f"{s.no}", lambda: True), s.desc,
                            s.nowait))
        return out

    def stop_steps(self, skip=()) -> List[Step]:
        """STOP·이상 중단 = 표 전체(고정). skip 은 정상 끝 정지(end_row)에서만 오고, 파워 끄기 단계는 빼지 않는다."""
        p = self.process
        rf_on = lambda: p.uses_rf or bool(self.plc.last and (self.plc.last.values.get("rf.dac") or 0) > 0)
        drop = set(skip or ()) - set(SR.END_STOP_MUST_KEEP)
        order = [s for s in SR.STOP_SEQUENCE if s.no not in drop]
        return self._steps(order, {"1": lambda: p.filament_by_process, "3": lambda: p.uses_heater,
                                   "4": lambda: bool(getattr(p, "dc_on", False)),
                                   "5": lambda: bool(getattr(p, "rfp_on", False)), "6": rf_on}, {})

    def all_stop_steps(self) -> List[Step]:
        p = self.process
        was_active = bool(p.active)                     # ALL STOP 을 받은 때 공정이 돌고 있었나(v1 공정 정지 순서)
        fil = lambda: bool(self.ig.last and self.ig.last.filament) or p.filament_by_process
        return self._steps(SR.ALL_STOP_SEQUENCE,
                           {"1": lambda: p.heater_recipe_running, "2": lambda: p.active or p.state == "STOPPING", "6": lambda: was_active, "7": fil},
                           {})

    # ── 처리기 ──
    def _handlers(self) -> Dict[str, Callable]:
        plc, mfc, dc, rfp, ig, p = self.plc, self.mfc, self.dc, self.rfp, self.ig, self.process

        def notify(a, done):
            self.notifications.append(a)
            done(None)

        def abort_in_seq(a, done):                    # ALL STOP 2단계 — 공정 표시만(정지 순서는 ALL STOP 이 이미 실행 중)
            if p.active:
                p.on_abort(a.get("reason", "ALL_STOP"))
            elif hasattr(p, "late_stop"):              # 행 끝 정지 순서 중 — 리스트를 끝낸다(v1 csv_cancelled)
                p.late_stop("error", a.get("reason", "ALL_STOP"))
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
            "RFP_SAFE_OFF": lambda a, done: (rfp.rf_off_confirmed(on_done=_req(done)) if self.cfg.get("RFPULSE_ENABLED", True)
                                             else done(None)),          # 설정에서 꺼진 장비 — 보낼 곳 없음
            "RF_SAFE_OFF": lambda a, done: plc.rf_off(on_done=_req(done)),
            "RF_RAMP_DOWN": ramp_down,
            "GAS_FLOW_OFF": lambda a, done: mfc.all_flow_off(on_done=_seq(done)),
            "GAS_EXHAUST": lambda a, done: mfc.valve(True, on_done=_seq(done)),
            "GAS_VALVES_CLOSE": coils_off(lambda a: ["ar_valve", "o2_valve"]),
            "SHUTTERS_CLOSE": coils_off(lambda a: a.get("which", "ms").split(",")),
            "PLC_OUTPUTS_OFF": lambda a, done: plc.outputs_off(on_done=_req(done)),
            "IG_FILAMENT_OFF": lambda a, done: (ig.filament_off(on_done=_req(done)) if self.cfg.get("IG_ENABLED", True)
                                                else done(None)),
            "BLACKBOX_DUMP": lambda a, done: (plc.dump_blackbox(a.get("tag", "safety")), done(None)),
        }
