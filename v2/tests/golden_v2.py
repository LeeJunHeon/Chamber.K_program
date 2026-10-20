"""골든 하니스 — v1 골든 기록(tests/golden/*.json)을 읽어(v1 모듈 import 없음) 같은 입력을 v2 시험대에서 돌리고 비교한다.

합격 세트(결정 5-2-5): 01~05, 08, 17, 18, 23, 24, 26. 비교 단위(같은 이름으로 v1·v2 둘 다 만든다):
  ("run_start", 이름, 주요 params)      v1 erp.run_start
  ("steps", [스텝 단위])                 v1 컨트롤러가 그 params 로 만든 스텝 목록(동작·인자·값·시간 — 문구 제외)
  ("run_end", 상태, 사유 코드)           v1 erp.run_end(문구 → 사유 코드)
  ("cmd", id, 성공, 사유 코드)           v1 erp.cmd_result
  ("stop_request",)                      v1 sig.request_process_stop(같은 것이 이어지면 하나로)
  ("all_stop",)                          v1 비상정지 묶음(PLC 비상정지·DC 직결 OFF·[공정 정지]·RF Pulse 정지)
  ("heater_recipe_stop",)                v1 heater.recipe_stop
  ("plc_button", 대상, 켬)               v1 sig.request_plc_port_update(원격 버튼)
  ("start_reject", 사유 코드)            v2 만(노트북 Start 거부) — v1 은 경고창
  ("check", 이름표, 공정 진행, 리스트 모드, 리스트 행, 딜레이 중)   v1 check(상태 전이)
비교하지 않는 것: 로그·챗·ERP 상태 문구·경고창 문구·입력칸(fields)·버튼 화면(buttons)·파일 목록(files) — v1 로그 문구는 비교하지 않는다.
INTENDED_DIFFS(의도된 차이)는 근거와 함께 기대값(v1)을 v2 결정대로 바꾼다. 그 밖의 차이는 실패.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import VirtualClock
from v2.common.commands import Source
from v2.chk.process_engine import ACT
from v2.chk.process_steps import ProcessStep
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig

GOLDEN_DIR = Path(__file__).resolve().parents[2] / "tests" / "golden"     # v1 기록(읽기만)
SET = ("01", "02", "03", "04", "05", "08", "17", "18", "23", "24", "26")

RUN_KEYS = ("use_ar_gas", "use_o2_gas", "ar_flow", "o2_flow", "sp1_set", "dc_power", "rf_power", "shutter_delay",
            "process_time", "use_rf_pulse", "use_g1", "use_g2", "use_dc_delay", "use_heater", "g1_target_name")
V1_BUTTON = {"MV_button": "mv", "Doorup_button": "door_up", "Doordn_button": "door_down", "ION_button": "ion",
             "Rotary_button": "rotary"}

# v1 사유 문구 → v2 사유 코드(문구 자체는 비교하지 않는다)
V1_REASON = (
    ("사용자 정지", "USER_STOP"),
    ("ALL STOP(비상 정지)", "ALL_STOP"),
    ("Ar 유량 이탈", "FLOW_DEVIATION"),
    ("PLC 재기동 감지", "PLC_RESTARTED"),
    ("히터 TC1 급락", "HEATER_FAULT"),
    ("DC Power 장치에 연결할 수 없습니다", "CONNECTION_FAILED"),
    ("허용되지 않은 명령", "UNKNOWN_COMMAND"),
    ("장비에 확인 대화상자가 열려 있습니다", "MODAL_OPEN"),
    ("적재된 레시피가 없습니다", "RECIPE_NOT_LOADED"),
    ("레시피 행이 없습니다", "RECIPE_INVALID"),
    ("시작 불가: 장비에 레시피가 적재돼", "RECIPE_LOADED"),
    ("입력 오류", "RECIPE_INVALID"),
    ("해제 불가: 공정 진행 중", "PROCESS_RUNNING"),
    ("PLC 통신이 끊겨", "LINK_DOWN"),
)


def v1_code(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    for k, c in V1_REASON:
        if text.startswith(k) or k in text:
            return c
    return "V1:" + text


def _num_eq(a, b) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < 1e-9
    return a == b


def run_params(p: Dict[str, Any]) -> tuple:
    return tuple((k, p.get(k)) for k in RUN_KEYS)


def step_unit(s: dict) -> dict:
    return {"action": s["action"], "params": s["params"], "value": s["value"], "duration_sec": s["duration_sec"],
            "timer_purpose": s["timer_purpose"]}


# ───────────── v1 → 단위 ─────────────
def load_v1(name: str) -> dict:
    with open(GOLDEN_DIR / f"{name}.json", encoding="utf-8") as f:
        return json.load(f)


def v1_units(name: str) -> List[tuple]:
    tl = load_v1(name)["timeline"]
    out: List[tuple] = []
    in_all_stop = False
    for e in tl:
        n, a = e[0], e[1]
        if n == "erp.run_start":
            out.append(("run_start", a[0], run_params(a[1])))
        elif n == "steps":
            out.append(("steps", [step_unit(s) for s in a[0]]))
        elif n == "erp.run_end":
            out.append(("run_end", a[0], v1_code(a[1]) if len(a) > 1 else None))
        elif n == "erp.cmd_result":
            out.append(("cmd", a[0], bool(a[1]), v1_code(a[2]) if len(a) > 2 else None))
        elif n == "sig.request_plc_emergency_stop":
            in_all_stop = True
            out.append(("all_stop",))
        elif n == "invoke":
            if a == ["RFPulseController", "stop_process"]:
                in_all_stop = False
        elif n == "sig.request_process_stop":
            if not in_all_stop:
                out.append(("stop_request",))
        elif n == "heater.recipe_stop":
            out.append(("heater_recipe_stop",))
        elif n == "sig.request_plc_port_update":
            out.append(("plc_button", V1_BUTTON.get(a[0], a[0]), bool(a[1])))
        elif n == "check":
            c = a[1]
            out.append(("check", a[0], c["process_running"], c["csv_mode"], c["csv_index"], c["csv_delay_active"]))
    return dedupe(out)


def dedupe(units: List[tuple]) -> List[tuple]:
    out = []
    for u in units:
        if u == ("stop_request",) and out and out[-1] == u:
            continue
        out.append(u)
    return out


# ───────────── 의도된 차이 ─────────────
@dataclass
class IntendedDiff:
    id: str
    decision: str
    why: str
    applies: Callable[[str], bool]
    fix: Callable[[str, List[tuple]], List[tuple]] = lambda name, units: units     # 기대값(v1)을 v2 결정대로
    golden: bool = True             # False: 골든 시나리오가 다루지 않는 v1 과의 차이(근거 기록용 — 비교 단위를 바꾸지 않음)


def _fix_count_to_time(name, units):
    out = []
    for u in units:
        if u[0] == "steps":
            steps = []
            for s in u[1]:
                s = dict(s)
                if s["action"] == "MFC_CMD" and s["params"][0] == "WAIT_PRESSURE":
                    a = dict(s["params"][1])
                    a.pop("stable_count", None)
                    a["stable_sec"] = 6.0          # v1 3회 × (폴 1 s + 명령 간격 ≈1 s) — PRESSURE_WAIT_STABLE_SEC
                    s["params"] = [s["params"][0], a]
                steps.append(s)
            u = ("steps", steps)
        out.append(u)
    return out


def _fix_connect_reject(name, units):
    out, skip = [], False
    for u in units:
        if u[0] == "run_start":
            out.append(("start_reject", "LINK_DOWN"))
            skip = True
            continue
        if skip and u[0] in ("steps", "run_end"):
            if u[0] == "run_end":
                skip = False
            continue
        out.append(u)
    return out


def _fix_no_modal(name, units):
    """확인창이 없으면 v1 도 두 명령을 처리했다: MV 켬(성공) · 인자 없는 PROCESS_START(입력 오류 → RECIPE_INVALID)."""
    out = []
    for u in units:
        if u[0] == "cmd" and u[3] == "MODAL_OPEN":
            if u[1] == 11:
                out += [("plc_button", "mv", True), ("cmd", 11, True, None)]
            else:
                out.append(("cmd", u[1], False, "RECIPE_INVALID"))
            continue
        out.append(u)
    return out


INTENDED_DIFFS: tuple = (
    IntendedDiff("COUNT_TO_TIME", "4-3 결정: 횟수 판정을 시간 판정으로",
                 "WAIT_PRESSURE 'stable_count 3회 연속' → 'stable_sec 6 s 동안'(v1 실제 소요 시간, PRESSURE_WAIT_STABLE_SEC)",
                 lambda n: True, _fix_count_to_time),
    IntendedDiff("CONNECT_FAIL_REJECT", "5-2 결정 4: 연결 안 된 장비를 쓰면 시작 거부",
                 "v1 은 시작한 뒤 연결 실패로 끝냈다(run_start·run_end error) → v2 는 시작하지 않고 LINK_DOWN",
                 lambda n: n == "05_connection_failed", _fix_connect_reject),
    IntendedDiff("NO_MODAL", "v2 방향 1·2: 웹만으로 제어, 노트북 화면은 비상용(확인창 없음)",
                 "v1 은 노트북에 확인창이 떠 있으면 원격 명령을 모두 거부 → v2 에는 확인창이 없어 그대로 처리(MV 켬 성공, "
                 "인자 없는 PROCESS_START 는 RECIPE_INVALID — v1 도 확인창이 없으면 입력 오류로 거부)",
                 lambda n: n == "24c_modal_open_rejects_all", _fix_no_modal),
    IntendedDiff("RF_CAL_FROM_CONFIG", "v2 설정: RF 보정값은 RF_CAL_OFFSET·RF_CAL_GAIN(설정)",
                 "v1 은 노트북 offset·param 입력칸이나 원격 인자로 덮어썼다 → v2 는 설정에서만(run_start 비교에서 rf_offset·rf_param 제외)",
                 lambda n: n == "26c_rf_calibration_keep_or_override"),
    IntendedDiff("NO_LOCAL_FILE_LOAD", "v2 방향 1: 레시피는 웹(RECIPE_LOAD 행 목록)으로만",
                 "v1 노트북 파일 적재·없는 파일·읽기 오류는 v2 에 없다 — 하니스는 같은 입력을 RECIPE_LOAD(행)·적재 실패로 넣는다",
                 lambda n: n in ("08a_csv_stop_during_delay", "08b_csv_stop_during_step", "17a_csv_step_all_stop",
                                 "17b_csv_delay_all_stop", "18b_csv_delay_plc_restarted", "23c_csv_step_fault",
                                 "23d_csv_delay_fault", "23k_csv_step_plc_restarted", "26a_loaded_recipe_remote_manual_fields_untouched",
                                 "26e_failed_load_releases_recipe", "26f_start_uses_rows_read_at_load", "26g_recipe_names",
                                 "26j_input_fields_reset_on_fail_and_clear")),
    IntendedDiff("NO_CHK_LOG", "사용자 결정: ChK_log.csv 는 읽는 곳이 없어 쓰지 않는다",
                 "v1 23h·23i(ChK_log 저장 실패·예외)는 v2 에서 해당 없음 — 비교 단위는 같다",
                 lambda n: n in ("23h_chk_csv_save_failed", "23i_chk_csv_exception")),
    IntendedDiff("INJECTED_FAULTS", "하니스 입력 대응",
                 "v1 컨트롤러 안의 critical_error(유량 이탈)는 v2 공정 엔진 안의 실패와 같은 경로(정지 요청 단위 없음)로, "
                 "창의 설비 이상 중단(히터 TC1 급락)은 안전 중단(정지 요청)으로 같은 사유 코드를 넣는다 — "
                 "v2 S38A 유량 이탈 자체는 경고만(공정 계속)",
                 lambda n: n[:3] in ("04a", "04b", "23a", "23b", "23c", "23d", "23e")),
    # ── 골든 시나리오 밖 — v1 과 다르지만 v2 방식 유지(사용자 결정 2026-10-20, 근거 기록) ──
    IntendedDiff("RF_ENABLE_OFF_WITH_RF", "사용자 결정 2026-10-20 ⑥: RF 를 끌 때 enable(M00200)도 같이 OFF",
                 "v1 은 RF 를 끌 때 D00040 만 0 으로 쓰고 M00200 은 비상 정지에서만 껐다(device/PLC.py send_rfpower_command·"
                 "on_emergency_stop) → v2 RF_SAFE_OFF 는 D00040=0 뒤 M00200=0(devices.RF diffs)",
                 lambda n: False, golden=False),
    IntendedDiff("RFP_INTERLOCK_ABORT", "사용자 결정 2026-10-20 ⑥: RF Pulse 인터록 열림 즉시 중단(S49)",
                 "v1 에는 CESAR 162 인터록 비트 판정이 없다 → v2 는 RF Pulse 사용 공정에서 rfp.itl_open 이면 즉시 중단(devices.RFPULSE diffs)",
                 lambda n: False, golden=False),
    IntendedDiff("HEATER_RECIPE_RAMP_MAX", "사용자 결정 2026-10-20 ⑥: 히터 레시피 램프 상한 60 °C/min",
                 "v1 히터 레시피(controller/heater_recipe.py)에는 램프 상한이 없다(공정 레시피만 core/params.py:364 60 °C/min) → "
                 "v2 는 히터 레시피에도 HEATER_RECIPE_RAMP_MAX_C_PER_MIN(60)",
                 lambda n: False, golden=False),
    IntendedDiff("PLC_HEARTBEAT_EVERY_TICK", "사용자 결정 2026-10-20 ⑥: PLC 하트비트(D00014) 매 tick",
                 "v1 은 HEATER_WD_PERIOD_MS(3 s)마다(device/PLC.py:1110-1163) → v2 는 1초 tick 마다(devices.PLC diffs)",
                 lambda n: False, golden=False),
)


def intended_for(name: str) -> List[IntendedDiff]:
    return [d for d in INTENDED_DIFFS if d.applies(name)]


def expected_units(name: str) -> List[tuple]:
    u = v1_units(name)
    for d in intended_for(name):
        u = d.fix(name, u)
    return u


# ───────────── v1 입력 → v2 입력 ─────────────
_TRUE = {"1", "true", "yes", "y", "o", "on", "t"}


def _b(v) -> bool:
    return str(v).strip().lower() in _TRUE


def _n(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return s                                    # 숫자가 아니면 그대로 — 레시피 검사가 거부한다


def csv_row(name="S1", **over) -> dict:
    """v1 tests/test_golden_process.csv_row 의 기본값(문자열 CSV 행)."""
    r = {"Process_name": name, "Ar": "1", "Ar_flow": "20", "O2": "0", "O2_flow": "0", "working_pressure": "5",
         "process_time": "1", "shutter_delay": "1", "use_rf_power": "0", "rf_power": "0", "use_dc_power": "1",
         "dc_power": "100", "use_rf_pulse": "0", "rf_pulse_power": "0", "rf_pulse_freq": "", "rf_pulse_duty": "",
         "use_dc_delay": "0", "use_heater": "0", "heater_temp": "0", "heater_ramp": "0", "gun1": "1", "gun2": "0",
         "G1 Target": "CeO2", "G2 Target": ""}
    r.update(over)
    return r


def delay_row(text) -> dict:
    return {"Process_name": text}


def v1_csv_to_row(r: dict) -> dict:
    """v1 CSV 행(문자열) → v2 레시피 행(값 타입 그대로) — 웹 가져오기가 할 일."""
    if set(r) == {"Process_name"} or (r.get("Process_name", "").lower().startswith("delay")):
        return {"process_name": r.get("Process_name")}
    out = {"process_name": r.get("Process_name") or None, "ar": _b(r.get("Ar")), "o2": _b(r.get("O2")),
           "working_pressure": _n(r.get("working_pressure")), "process_time": _n(r.get("process_time")),
           "shutter_delay": _n(r.get("shutter_delay")), "use_dc": _b(r.get("use_dc_power")),
           "use_rf": _b(r.get("use_rf_power")), "use_rf_pulse": _b(r.get("use_rf_pulse")),
           "use_dc_delay": _b(r.get("use_dc_delay")), "use_heater": _b(r.get("use_heater")),
           "gun1": _b(r.get("gun1")), "gun2": _b(r.get("gun2")),
           "g1_target": r.get("G1 Target") or "", "g2_target": r.get("G2 Target") or ""}
    if out["ar"]:
        out["ar_flow"] = _n(r.get("Ar_flow"))
    if out["o2"]:
        out["o2_flow"] = _n(r.get("O2_flow"))
    if out["use_dc"]:
        out["dc_power"] = _n(r.get("dc_power"))
    if out["use_rf"]:
        out["rf_power"] = _n(r.get("rf_power"))
    if out["use_heater"]:
        out["heater_temp"], out["heater_ramp"] = _n(r.get("heater_temp")), _n(r.get("heater_ramp"))
    return {k: v for k, v in out.items() if v is not None}


REMOTE_ARGS = {"useG1": True, "g1": "CeO2", "useG2": False, "useAr": True, "arFlow": 20, "useO2": False,
               "workingPressure": 5, "useRf": False, "useDc": True, "dcPower": 100, "shutterDelay": 1, "processTime": 1}


def remote_args_to_row(a: dict) -> dict:
    """v1 원격 PROCESS_START 인자(camelCase) → v2 행. 없는 키는 끔(v1 B1 26b). offset·param 은 받지 않는다(RF_CAL_FROM_CONFIG)."""
    b = lambda k: a.get(k) is True
    out = {"process_name": None, "ar": b("useAr"), "o2": b("useO2"), "working_pressure": _n(a.get("workingPressure")),
           "process_time": _n(a.get("processTime")), "shutter_delay": _n(a.get("shutterDelay")),
           "use_dc": b("useDc"), "use_rf": b("useRf"), "use_rf_pulse": b("useRfPulse"), "use_dc_delay": b("dcDelay"),
           "gun1": b("useG1"), "gun2": b("useG2"), "g1_target": a.get("g1") or "", "g2_target": a.get("g2") or ""}
    for flag, key, src in (("ar", "ar_flow", "arFlow"), ("o2", "o2_flow", "o2Flow"), ("use_dc", "dc_power", "dcPower"),
                           ("use_rf", "rf_power", "rfPower")):
        if out[flag]:
            out[key] = _n(a.get(src))
    return {k: v for k, v in out.items() if v is not None}


MANUAL = {"ar": True, "ar_flow": 20.0, "working_pressure": 5.0, "use_dc": True, "dc_power": 100.0, "shutter_delay": 1.0,
          "process_time": 1.0, "gun1": True, "g1_target": "CeO2"}


# ───────────── v2 하니스 ─────────────
class V2H:
    """시나리오 스크립트(생성기)를 tick 마다 진행하며 v2 단위를 모은다. 스크립트는 '기다릴 조건'을 yield 한다."""

    def __init__(self, tmp: Path, script: Callable[["V2H"], Any]) -> None:
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.rig = ChkRig(self.c, tmp, cfg={"RFPULSE_ENABLED": True})   # v1 기록은 RF Pulse 컨트롤러가 있던 상태
        rig = self.rig
        self.plc = rig.sims["plc"]
        for k in ("mv", "turbo", "rotary", "fv"):
            self.plc.coils[A[k]] = True
        self.units: List[tuple] = []
        self._lk = threading.Lock()
        self.seq = -1
        self._abort_rule = ""
        self._frozen = None
        self._freeze_next = False
        self.manual_row = dict(MANUAL)
        self._chain(rig.process, "on_event", self._on_proc)
        self._chain(rig.safety.engine, "on_event", self._on_safety)
        self._chain(rig.heater, "on_event", self._on_heater)
        self.gen = script(self)
        self.wait: Optional[Callable[[], bool]] = None
        self.done = False
        self.error: Optional[BaseException] = None
        rig.loop.tick_hooks.append(self._tick)

    @staticmethod
    def _chain(obj, attr, fn):
        old = getattr(obj, attr)

        def both(e):
            if old:
                old(e)
            fn(e)
        setattr(obj, attr, both)

    def _add(self, u: tuple) -> None:
        with self._lk:
            self.units.append(u)

    def _on_proc(self, e) -> None:
        if e.kind == "run_start":
            self._add(("run_start", e.data["name"], run_params(e.data["params"])))
        elif e.kind == "steps":
            self._add(("steps", [step_unit(s) for s in e.data["steps"]]))
        elif e.kind == "run_end":
            self._add(("run_end", e.data["status"], e.data["reason"]))

    def _on_safety(self, e) -> None:
        if e.kind == "abort":
            self._abort_rule = e.rule
        elif e.kind == "seq_start" and (e.rule == "stop" or (e.rule == "abort" and self._abort_rule != "process")):
            # 공정 정지 요청 = 공정 밖(사용자·안전 규칙)이 정지를 요청한 것. 공정 엔진 안의 실패(v1: 컨트롤러가 스스로
            # critical_error → request_stop)는 v1 기록에도 정지 요청이 없다
            self._add(("stop_request",))
        elif e.kind == "step" and e.rule == "all_stop" and e.code == "PLC_OUTPUTS_OFF":
            self._add(("all_stop",))
            # v2 ALL STOP 은 시뮬레이터에서 1초 안에 끝난다 — v1 'ALL STOP 직후' check 는 출력 OFF 단계 바로 뒤의 상태로 찍는다
            if self._freeze_next:
                with self._lk:
                    self._frozen, self._freeze_next = (len(self.units), self.p.ui_check()), False

    def _on_heater(self, e) -> None:
        if e.kind == "recipe_finished" and e.code == "USER_STOP":
            self._add(("heater_recipe_stop",))

    # ── 진행 ──
    def _tick(self, t) -> None:
        self.seq = t.seq
        if self.done:
            return
        try:
            while True:
                if self.wait is not None and not self.wait():
                    return
                self.wait = next(self.gen)
        except StopIteration:
            self.done = True
            self.rig.loop.stop()
        except BaseException as ex:          # 스크립트 오류도 결과로
            self.error, self.done = ex, True
            self.rig.loop.stop()

    def run(self, max_ticks: int = 8000) -> List[tuple]:
        assert self.rig.start()
        try:
            self.rig.loop.run(max_ticks)
        finally:
            self.rig.stop()
        if self.error:
            raise self.error
        if not self.done:
            raise AssertionError(f"시나리오가 {max_ticks} tick 안에 끝나지 않음 — 상태 {self.rig.process.state}")
        return dedupe(self.units)

    def ticks(self, n: int = 1):
        target = self.seq + n
        yield lambda: self.seq >= target

    @property
    def p(self):
        return self.rig.process

    def _n(self, kind):
        return sum(1 for u in self.units if u[0] == kind)

    # ── v1 하니스 동작에 대응하는 것 ──
    def check(self, label: str) -> None:
        with self._lk:
            frozen, self._frozen = self._frozen, None
        if frozen is not None:                          # ALL STOP 직후 — 출력 OFF 단계에서 찍어 둔 상태를 그 자리에
            i, c = frozen
            with self._lk:
                self.units.insert(i, ("check", label, c["process_running"], c["csv_mode"], c["csv_index"],
                                      c["csv_delay_active"]))
            return
        c = self.p.ui_check()
        self._add(("check", label, c["process_running"], c["csv_mode"], c["csv_index"], c["csv_delay_active"]))

    def start(self):
        """노트북 Start(v1) — 적재돼 있으면 RECIPE_START, 아니면 수동 PROCESS_START. 노트북 동작이라 cmd 단위 없음."""
        if self.p.rows:
            r = self.rig.core.command("RECIPE_START", {})
        else:
            r = self.rig.core.command("PROCESS_START", {"row": dict(self.manual_row)})
        if not r.ok:
            self._add(("start_reject", r.reason))
        yield from self.ticks(1)

    def local(self, kind: str, args: Optional[dict] = None):
        n_all = sum(1 for u in self.units if u == ("all_stop",))
        self._freeze_next = kind == "ALL_STOP"          # 바로 뒤 check 를 출력 OFF 단계의 상태로
        self.rig.core.command(kind, args or {}, source=Source.HMI if kind in ("PROCESS_STOP", "ALL_STOP") else Source.WEB)
        if kind == "ALL_STOP":
            yield lambda: sum(1 for u in self.units if u == ("all_stop",)) > n_all     # 출력 OFF 단계까지 — 바로 check
            return
        yield from self.ticks(2)

    def remote(self, v1_cmd: str, args: Optional[dict] = None, cid: Any = 1):
        """v1 원격 명령 → v2 명령(들). cmd 단위는 v1 처럼 명령 하나에 결과 하나."""
        args = dict(args or {})
        res = None
        n_all = sum(1 for u in self.units if u == ("all_stop",))
        for kind, a in self._map(v1_cmd, args):
            r = self.rig.core.command(kind, a)
            if r.ok and kind == "PLC_BUTTON":
                self._add(("plc_button", a["target"], a["on"]))
            if kind == "ALL_STOP" and r.ok:
                yield lambda: sum(1 for u in self.units if u == ("all_stop",)) > n_all
            res = r if res is None or res.ok else res
        self._add(("cmd", cid, res.ok, res.reason))
        yield from self.ticks(1)

    @staticmethod
    def _map(cmd: str, a: dict) -> List[tuple]:
        if cmd in ("MV_button", "ION_button", "Rotary_button"):
            return [("PLC_BUTTON", {"target": V1_BUTTON[cmd], "on": a.get("on") is True})]
        if cmd == "Door_Button":
            on = a.get("on") is True
            return ([("PLC_BUTTON", {"target": "door_down", "on": False}), ("PLC_BUTTON", {"target": "door_up", "on": True})]
                    if on else [("PLC_BUTTON", {"target": "door_up", "on": False}),
                                ("PLC_BUTTON", {"target": "door_down", "on": True})])
        if cmd == "PROCESS_START":
            return [("PROCESS_START", {"row": remote_args_to_row(a)})]
        if cmd == "RECIPE_PROCESS_RUN":
            return [("RECIPE_LOAD", {"name": a.get("name") or "", "rows": [v1_csv_to_row(r) for r in a.get("rows") or []]})]
        if cmd == "RECIPE_PROCESS_START":
            return [("RECIPE_START", {})]
        if cmd in ("PROCESS_STOP", "ALL_STOP", "RECIPE_CLEAR"):
            return [(cmd, {})]
        return [(cmd, a)]                       # 모르는 명령 → UNKNOWN_COMMAND

    def drain(self, cmds: List[dict]):
        for c in cmds:
            yield from self.remote(c["command"], c.get("args"), c["id"])

    def load_local(self, rows: List[dict], label: Optional[str] = "적재 후"):
        """v1 노트북 파일 적재 → v2 RECIPE_LOAD(행). 노트북 동작이라 cmd 단위 없음(NO_LOCAL_FILE_LOAD)."""
        self.rig.core.command("RECIPE_LOAD", {"name": "recipe.csv", "rows": [v1_csv_to_row(r) for r in rows]})
        yield from self.ticks(1)
        if label:
            self.check(label)

    def load_local_fail(self):
        """없는 파일·읽기 오류 → v2 적재 실패(행 없음). 적재 실패는 이전 적재도 해제(v1 B2)."""
        self.rig.core.command("RECIPE_LOAD", {"name": "missing.csv", "rows": []})
        yield from self.ticks(1)

    def ctrl_run(self):
        """v1 ctrl_run(컨트롤러가 메인 공정까지) — v2 는 실제로 MAIN 에 들어갈 때까지 진행하고 '공정 중' check."""
        yield lambda: self.p.state == "MAIN"
        self.check("공정 중")

    def ctrl_finish(self):
        """v1 ctrl_finish(컨트롤러 finished) — v2 는 이번 행이 끝나(run_end) 다음 상태로 갈 때까지."""
        n_start = self._n("run_start")
        yield lambda: (self._n("run_end") >= n_start and
                       (self.p.state in ("IDLE", "LOADED", "CSV_DELAY") or
                        (self.p.state in ACT and self._n("run_start") > n_start)))
        yield from self.ticks(1)

    def advance(self, sec: int):
        yield from self.ticks(sec)

    def abort(self, code: str, internal: bool = False):
        """v1 _abort_process_by_fault(창이 정지 요청) → 안전 중단. v1 critical_error(컨트롤러 안의 장비 오류) → internal —
        공정 엔진 안의 실패와 같은 경로(rule "process", 정지 요청 단위 없음). INJECTED_FAULTS."""
        self.rig.safety.engine.abort(code, rule="process" if internal else "harness")
        yield from self.ticks(2)

    def plc_restarted(self):
        self.rig.safety.event("plc_restarted")
        yield from self.ticks(2)

    def plc_link(self, up: bool):
        w = self.rig.workers["plc"]
        if up:
            self.plc.faults = [f for f in self.plc.faults if f.kind != "disconnect"]
            yield lambda: w.ready
        else:
            self.plc.add_fault("disconnect", self.c.monotonic(), 1e9)
            yield lambda: not w.ready
        yield from self.ticks(1)


# ───────────── 시나리오(v1 스크립트와 같은 입력) ─────────────
def _manual(h):
    yield from h.start()
    yield from h.ctrl_run()


def s01_manual_complete(h):
    h.check("시작 전")
    yield from h.start()
    h.check("시작 직후")
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    h.check("종료 후")


def s02_manual_user_stop(h):
    yield from _manual(h)
    yield from h.local("PROCESS_STOP")
    h.check("STOP 직후")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s03_manual_all_stop(h):
    yield from _manual(h)
    yield from h.local("ALL_STOP")
    h.check("ALL STOP 직후")
    yield from h.ctrl_finish()
    h.check("종료 후")


def _critical(h):
    yield from h.abort("FLOW_DEVIATION", internal=True)
    h.check("critical_error 직후")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s04a_critical_error_local(h):
    yield from _manual(h)
    yield from _critical(h)


def s04b_critical_error_erp(h):
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS))
    yield from h.ctrl_run()
    yield from _critical(h)


def s05_connection_failed(h):
    h.rig.sims["dc"].add_fault("disconnect", h.c.monotonic(), 1e9)
    yield lambda: not h.rig.workers["dc"].ready
    yield from h.start()
    h.check("연결 실패 후")


def s08a_csv_stop_during_delay(h):
    yield from h.load_local([csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    yield from h.advance(2)
    yield from h.local("PROCESS_STOP")
    h.check("딜레이 중 STOP 뒤")
    yield from h.ticks(1)                              # 늦은 finished — v2 에는 없음
    h.check("늦은 finished 뒤")


def s08b_csv_stop_during_step(h):
    yield from h.load_local([csv_row("S1"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.local("PROCESS_STOP")
    h.check("STEP 1 중 STOP 뒤")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s17a_csv_step_all_stop(h):
    yield from h.load_local([csv_row("S1"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.local("ALL_STOP")
    h.check("ALL STOP 직후")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s17b_csv_delay_all_stop(h):
    yield from h.load_local([csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    yield from h.advance(2)
    yield from h.local("ALL_STOP")
    h.check("딜레이 중 ALL STOP 뒤")
    yield from h.ticks(1)
    h.check("늦은 finished 뒤")


def s18a_manual_plc_restarted(h):
    yield from _manual(h)
    yield from h.plc_restarted()
    h.check("PLC 재기동 감지 뒤")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s18b_csv_delay_plc_restarted(h):
    yield from h.load_local([csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    yield from h.advance(2)
    yield from h.plc_restarted()
    h.check("딜레이 중 PLC 재기동 감지 뒤")


def s18c_idle_plc_restarted(h):
    h.check("대기")
    yield from h.plc_restarted()
    h.check("PLC 재기동 감지 뒤")


def s23a_manual_fault_abort(h):
    yield from _manual(h)
    yield from h.abort("HEATER_FAULT")
    h.check("설비 이상 중단 직후")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s23b_manual_fault_twice(h):
    yield from _manual(h)
    yield from h.abort("HEATER_FAULT")
    yield from h.abort("MV_CLOSED")
    h.check("설비 이상 두 번 뒤")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s23c_csv_step_fault(h):
    yield from h.load_local([csv_row("S1"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.abort("HEATER_FAULT")
    h.check("설비 이상 직후")
    yield from h.ctrl_finish()
    h.check("스텝 종료 뒤")


def s23d_csv_delay_fault(h):
    yield from h.load_local([csv_row("S1"), delay_row("delay 10s"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    yield from h.advance(2)
    yield from h.abort("HEATER_FAULT")
    h.check("딜레이 중 설비 이상 뒤")


def s23e_idle_fault(h):
    h.check("대기")
    yield from h.abort("HEATER_FAULT")
    h.check("설비 이상 뒤")


def s23f_idle_all_stop(h):
    h.check("대기")
    yield from h.local("ALL_STOP")
    h.check("ALL STOP 뒤")


def s23g_manual_all_stop_with_heater_recipe(h):
    r = h.rig.core.command("HEATER_RECIPE_RUN", {"name": "bg", "steps": [{"target_c": 40, "ramp_c_per_min": 6, "soak_min": 600}]})
    assert r.ok, r
    yield from h.ticks(2)
    yield from _manual(h)
    yield from h.local("ALL_STOP")
    h.check("ALL STOP 직후")
    yield from h.ctrl_finish()
    h.check("종료 후")


def s23h_chk_csv_save_failed(h):
    yield from _manual(h)
    yield from h.ctrl_finish()
    h.check("종료 후")


def s23i_chk_csv_exception(h):
    yield from _manual(h)
    yield from h.ctrl_finish()
    h.check("종료 후")


def s23j_idle_stop(h):
    h.check("대기")
    yield from h.local("PROCESS_STOP")
    h.check("STOP 뒤")


def s23k_csv_step_plc_restarted(h):
    yield from h.load_local([csv_row("S1"), csv_row("S2")])
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.plc_restarted()
    h.check("PLC 재기동 감지 뒤")
    yield from h.ctrl_finish()
    h.check("스텝 종료 뒤")


def s24a_remote_plc_buttons(h):
    yield from h.remote("MV_button", {"on": True}, 1)
    yield from h.remote("Door_Button", {"on": True}, 2)
    yield from h.remote("ION_button", {"on": True}, 3)
    yield from h.remote("MV_button", {"on": False}, 4)
    yield from h.remote("Door_Button", {}, 5)
    h.check("버튼 명령 뒤")


def s24b_unknown_command(h):
    yield from h.remote("FORMAT_C_DRIVE", {"x": 1})
    h.check("거부 뒤")


def s24c_modal_open_rejects_all(h):
    # v2 에는 노트북 확인창이 없다(NO_MODAL) — 같은 명령을 그대로 처리한다
    yield from h.drain([{"id": 11, "command": "MV_button", "args": {"on": True}},
                        {"id": 12, "command": "PROCESS_START", "args": {}}])
    h.check("거부 뒤")


def s24d_erp_rejected_then_resumed(h):
    yield from h.ticks(4)                               # ERP 거부·재개는 로그만 — 비교 단위 없음


def s24e_several_commands_in_order(h):
    yield from h.drain([{"id": 21, "command": "MV_button", "args": {"on": True}},
                        {"id": 22, "command": "PROCESS_STOP", "args": {}},
                        {"id": 23, "command": "NOPE", "args": {}},
                        {"id": 24, "command": "ALL_STOP", "args": {}},
                        {"id": 25, "command": "RECIPE_PROCESS_START", "args": {}}])
    h.check("여러 건 뒤")


def s24f_recipe_run_without_rows(h):
    yield from h.remote("RECIPE_PROCESS_RUN", {"rows": []}, 31)
    yield from h.remote("RECIPE_PROCESS_RUN", {}, 32)
    h.check("뒤")


def s24g_recipe_start_without_loaded(h):
    yield from h.remote("RECIPE_PROCESS_START", {}, 41)
    h.check("뒤")


def s26a_loaded_recipe_remote_manual_fields_untouched(h):
    yield from h.load_local([csv_row("R1", dc_power="100")])
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS, dcPower=150, workingPressure=3, offset=9, param=9))


def s26b_missing_keys_turn_off(h):
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS))
    h.check("시작 뒤")
    yield from h.ctrl_run()
    yield from h.ctrl_finish()


def s26c_rf_calibration_keep_or_override(h):
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS, useRf=True, rfPower=150), 1)
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS, useRf=True, rfPower=150, offset="6.5", param=" 1.2 "), 2)
    yield from h.ctrl_run()
    yield from h.ctrl_finish()


def s26d_input_error_fields_untouched(h):
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS, arFlow="", dcPower="abc"))
    h.check("거부 뒤")


def s26e_failed_load_releases_recipe(h):
    def good():
        yield from h.load_local([csv_row("G1"), csv_row("G2")], label=None)
        h.check("정상 적재")
    yield from good()
    yield from h.load_local_fail()
    h.check("없는 파일 뒤")
    yield from good()
    yield from h.load_local_fail()
    h.check("읽기 오류 뒤")
    yield from good()
    yield from h.load_local([{c: "" for c in csv_row("X")}], label=None)
    h.check("빈 레시피 뒤")
    yield from good()
    yield from h.load_local([csv_row("B1", working_pressure="abc")], label=None)
    h.check("첫 행 오류 뒤")


def s26f_start_uses_rows_read_at_load(h):
    yield from h.load_local([csv_row("L1", dc_power="100")], label="적재")
    yield from h.start()                                # 적재 뒤 파일을 고쳐도 적재한 행으로(v2 는 행을 받는다)
    h.check("Start 뒤")
    yield from h.ctrl_run()
    yield from h.ctrl_finish()
    h.check("리스트 완료")


def s26g_recipe_names(h):
    yield from h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 A", "rows": [csv_row("W1")]}, 1)
    h.check("이름 있음")
    yield from h.remote("PROCESS_START", dict(REMOTE_ARGS), 2)
    yield from h.remote("RECIPE_PROCESS_RUN", {"rows": [csv_row("W2")]}, 3)
    h.check("이름 없음 → 파일 이름")
    yield from h.load_local([csv_row("L1")], label="노트북 적재 → 파일 이름")
    yield from h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 B", "rows": [csv_row("W4")]}, 4)


def s26h_recipe_clear(h):
    yield from h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 C", "rows": [csv_row("W1"), csv_row("W2")]}, 1)
    h.check("적재")
    yield from h.remote("RECIPE_CLEAR", {}, 2)
    h.check("해제 뒤")
    yield from h.remote("RECIPE_CLEAR", {}, 3)
    h.check("다시 해제 뒤")
    yield from h.remote("RECIPE_PROCESS_RUN", {"name": "웹 레시피 D", "rows": [csv_row("W3")]}, 4)
    yield from h.remote("RECIPE_PROCESS_START", {}, 5)
    yield from h.remote("RECIPE_CLEAR", {}, 6)
    h.check("공정 중 해제 요청 뒤")
    yield from h.ctrl_run()
    yield from h.ctrl_finish()


def s26i_plc_buttons_need_link(h):
    yield from h.plc_link(False)
    yield from h.remote("MV_button", {"on": True}, 1)
    yield from h.plc_link(True)
    yield from h.remote("MV_button", {"on": True}, 2)
    yield from h.start()
    yield from h.remote("Rotary_button", {"on": True}, 3)
    yield from h.ctrl_run()
    yield from h.ctrl_finish()


def s26j_input_fields_reset_on_fail_and_clear(h):
    yield from h.load_local([csv_row("G1", dc_power="120")], label=None)
    yield from h.remote("RECIPE_CLEAR", {}, 1)
    yield from h.remote("RECIPE_CLEAR", {}, 2)
    yield from h.load_local_fail()
    yield from h.load_local([csv_row("G1", dc_power="120")], label=None)
    yield from h.start()
    yield from h.ctrl_run()
    yield from h.remote("RECIPE_CLEAR", {}, 3)
    yield from h.ctrl_finish()
    h.check("④ 리스트 완료")


SCENARIOS: Dict[str, Callable] = {name[1:]: fn for name, fn in sorted(globals().items())
                                  if name[:1] == "s" and name[1:3].isdigit() and callable(fn)}


@dataclass
class Result:
    name: str
    status: str                      # pass / intended / fail
    intended: List[str] = field(default_factory=list)
    diff: List[str] = field(default_factory=list)


def compare(name: str, actual: List[tuple]) -> Result:
    exp = expected_units(name)
    used = [d.id for d in intended_for(name) if d.id != "COUNT_TO_TIME" or any(u[0] == "steps" for u in exp)]
    diff = []
    for i in range(max(len(exp), len(actual))):
        e = exp[i] if i < len(exp) else None
        a = actual[i] if i < len(actual) else None
        if not _unit_eq(e, a):
            diff.append(f"[{i}] v1(기대) {_short(e)}  ≠  v2 {_short(a)}")
            if len(diff) >= 8:
                break
    if diff:
        return Result(name, "fail", used, diff)
    return Result(name, "intended" if used else "pass", used, [])


def _unit_eq(e, a) -> bool:
    if e is None or a is None or e[0] != a[0] or len(e) != len(a):
        return False
    if e[0] == "run_start":
        return e[1] == a[1] and all(_num_eq(x[1], y[1]) for x, y in zip(e[2], a[2]))
    if e[0] == "steps":
        return len(e[1]) == len(a[1]) and all(_step_eq(x, y) for x, y in zip(e[1], a[1]))
    return all(_num_eq(x, y) for x, y in zip(e, a))


def _step_eq(x: dict, y: dict) -> bool:
    return (x["action"] == y["action"] and _num_eq(x["value"], y["value"]) and x["duration_sec"] == y["duration_sec"]
            and x["timer_purpose"] == y["timer_purpose"] and _deep_eq(x["params"], y["params"]))


def _deep_eq(a, b) -> bool:
    if isinstance(a, dict) and isinstance(b, dict):
        return set(a) == set(b) and all(_deep_eq(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(_deep_eq(x, y) for x, y in zip(a, b))
    return _num_eq(a, b)


def _short(u) -> str:
    if u is None:
        return "(없음)"
    if u[0] == "steps":
        return f"steps({len(u[1])})"
    if u[0] == "run_start":
        return f"run_start({u[1]})"
    return str(u)


def run_scenario(name: str, tmp: Path) -> Result:
    h = V2H(tmp, SCENARIOS[name])
    actual = h.run()
    return compare(name, actual)
