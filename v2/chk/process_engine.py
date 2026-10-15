"""챔버K ProcessEngine — process_states.py 의 상태 기계를 실행한다(SimpleProcess 를 대신).

v1 근거(읽고 다시 씀): core/process_service.py(시작·CSV 리스트·딜레이·STOP·ALL STOP·설비 이상·종료),
controller/process_controller.py(스텝 순서·파워 대기·베이스 압력 대기·히터 도달 대기).
매 tick 스냅샷 하나(on_row)로 다음 동작을 정한다 — 중첩 이벤트 루프 없음, 대기는 실제 시각.

상태: 지금 스텝의 state(process_steps 가 붙인 이름). HEATER_WAIT 스텝은 HeaterEngine 단계로 HEATER_RAMP / HEATER_HOLD_WAIT.
상태 한도(결정 5-2-3, 상태 기계 안): HEATER_RAMP(S18E)·HEATER_HOLD_WAIT(S23)는 HeaterEngine.process_wait_tick,
POWER_WAIT(S36) 공식 한도, PRESSURE_SP1(S37) WAIT_PRESSURE 한도(fail_on_timeout), BASE_WAIT(S39A 시간 초과·S39C 필라멘트).
압력 안정 판정은 v1 기준 ±PRESSURE_WAIT_TOL_RATIO 를 PRESSURE_WAIT_STABLE_SEC 동안.

끝맺음: 행 정상 끝 → SafetyEngine.end_row(정지 순서) → run_end done → 다음 행(CSV_BETWEEN) 또는 DONE.
STOP → 정지 순서 → run_end aborted(USER_STOP). 중단(안전 규칙·장비 오류) → run_end error(첫 사유). ALL STOP → error(ALL_STOP).
딜레이 행·행 사이에는 장비가 없어 정지 순서 없이 리스트를 바로 정리한다(v1 cancel_csv_list_now). 리스트가 끝나거나 취소되면
적재도 해제한다(v1 st.clear_csv_list — process_states 의 DONE→LOADED 는 쓰지 않음).
시작 거부(결정 5-2-4): 레시피가 쓰는 장비가 설정에서 꺼져 있으면 DEVICE_DISABLED, 연결 안 됐으면 LINK_DOWN.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.judge import Persist
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk.power_control import DcControl, RfControl
from v2.chk.process_steps import (HEATER_RAMP_SETTLE_S, HEATER_SET_SETTLE_S, POWER_STOP_SETTLE_S, ProcessStep,
                                  StepEvent, StepRunner, base_pressure_of, build_process_steps, heater_used,
                                  row_to_params)

ACT = frozenset(PS.ACTIVE_STATES)
DC_EST_VOLTAGE_V = 300.0          # v1 process_controller.DC_EST_VOLTAGE_V
RFPULSE_EST_SEC = 20.0            # v1 RFPULSE_EST_SEC


def _row_error_code(errs: List[str]) -> str:
    """레시피 행 검사 오류 → 사유 코드. 꺼진 장비를 쓰면 시작 거부(결정 5-2-4): 이온 게이지는 IG_DISABLED(S47), 히터는 DEVICE_DISABLED."""
    if any("IG_ENABLED" in e for e in errs):
        return "IG_DISABLED"
    if any("HEATER_ENABLED" in e for e in errs):
        return "DEVICE_DISABLED"
    return "RECIPE_INVALID"


@dataclass
class ProcEvent:
    kind: str                 # run_start / run_end / state / steps / step / warn / list_done / list_cancel / reject
    detail: str = ""
    code: Optional[str] = None
    data: Optional[dict] = None


class _NextTick:
    def __init__(self, t_mono: float) -> None:
        self.t_mono = t_mono


class ProcessEngine:
    def __init__(self, clock, cfg: Dict[str, Any], plc, mfc, dc, rfp, ig, heater,
                 links: Dict[str, Callable[[], bool]], on_event: Optional[Callable[[ProcEvent], None]] = None) -> None:
        self.clock, self.cfg = clock, cfg
        self.plc, self.mfc, self.dc, self.rfp, self.ig, self.heater = plc, mfc, dc, rfp, ig, heater
        self.links = links
        self.on_event = on_event
        self.safety = None                      # ChkSafety — 연결은 만든 쪽이(서로 참조)
        self._lock = threading.RLock()
        self.state = "IDLE"
        self.events: List[ProcEvent] = []
        # 레시피
        self.recipe_name = ""
        self.rows: List[Dict[str, Any]] = []
        self.list_active = False
        self.csv_index = -1
        self.delay_t0 = 0.0
        self.delay_s = 0.0
        # 이번 행
        self.params: Dict[str, Any] = {}
        self.run_name = ""
        self.run_id = ""
        self.runner: Optional[StepRunner] = None
        self.row_is_process = False
        self.end_kind: Optional[str] = None     # done / aborted / error
        self.end_code: Optional[str] = None
        self.dcc = DcControl(cfg, dc, self._dev_msg)
        self.rfc = RfControl(cfg, plc, self._dev_msg)
        self.filament_by_process = False
        self.comm_aborted = False
        self._next_tick_mono = 0.0
        self._settle_pending = False
        self.power: Dict[str, Any] = {}
        self.base: Dict[str, Any] = {}

    # ── 안전 엔진이 보는 것 ──
    @property
    def active(self) -> bool:
        return self.state in ACT

    @property
    def needs_stop_sequence(self) -> bool:
        """딜레이 행·행 사이에는 켜진 장비가 없다 — 정지 순서 없이 정리(v1 cancel_csv_list_now)."""
        return self.state not in ("CSV_DELAY", "CSV_BETWEEN")

    @property
    def uses_dc(self) -> bool:
        return float(self.params.get("dc_power") or 0) > 0

    @property
    def uses_rf(self) -> bool:
        return float(self.params.get("rf_power") or 0) > 0

    @property
    def uses_rfp(self) -> bool:
        return bool(self.params.get("use_rf_pulse")) and float(self.params.get("rf_pulse_power") or 0) > 0

    @property
    def uses_heater(self) -> bool:
        return heater_used(self.params)

    @property
    def heater_recipe_running(self) -> bool:
        return self.heater.recipe_running

    @property
    def heater_gas_state(self) -> str:
        return self.heater.gas_state

    @property
    def dc_holding(self) -> bool:
        return self.dcc.holding

    @property
    def rf_holding(self) -> bool:
        return self.rfc.holding

    @property
    def dc_set_w(self):
        return self.dcc.target if self.dcc.state != DcControl.IDLE else None

    @property
    def rf_set_w(self):
        return self.rfc.target if self.rfc.state != RfControl.IDLE else None

    @property
    def rfp_set_w(self):
        return float(self.params.get("rf_pulse_power") or 0) if self.uses_rfp else None

    @property
    def flow_set(self) -> Dict[str, Optional[float]]:
        return {"ar": self.params.get("ar_flow") if self.params.get("use_ar_gas") else None,
                "o2": self.params.get("o2_flow") if self.params.get("use_o2_gas") else None}

    @property
    def sp1_mtorr(self):
        return self.params.get("sp1_set") or None

    # ── 기록 ──
    def _emit(self, e: ProcEvent) -> None:
        self.events.append(e)
        if self.on_event:
            try:
                self.on_event(e)
            except Exception:
                pass

    def _dev_msg(self, level: str, text: str) -> None:
        self._emit(ProcEvent("message", text, None, {"level": level}))

    def _set_state(self, s: str) -> None:
        if s != self.state:
            old, self.state = self.state, s
            self._emit(ProcEvent("state", f"{old}→{s}", data={"from": old, "to": s, "t": self.clock.monotonic()}))

    def snapshot_fields(self) -> Dict[str, Any]:
        return {"proc.state": self.state, "proc.run_id": self.run_id or None,
                "proc.row": (self.csv_index + 1) if self.list_active else None}

    def ui_check(self) -> Dict[str, Any]:
        """v1 골든 check 와 같은 뜻의 값(process_running·csv_mode·csv_index·csv_delay_active)."""
        return {"process_running": self.state in ACT or self.state == "STOPPING",
                "csv_mode": self.list_active, "csv_index": self.csv_index if self.list_active else -1,
                "csv_delay_active": self.state == "CSV_DELAY"}

    # ── 명령(core 가 부른다) — 돌려주는 값은 거부 사유 코드 또는 None ──
    def load(self, name: str, rows: List[Dict[str, Any]]) -> Optional[str]:
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            checked, err = self._check_rows(rows)
            if err:
                self._clear_recipe()                    # v1 B2: 적재 실패는 이전 적재도 해제
                return err
            self.recipe_name, self.rows = name or "", checked
            self.list_active, self.csv_index = False, -1
            self._set_state("LOADED")
            return None

    def _check_rows(self, rows) -> tuple:
        if not isinstance(rows, list) or not rows:
            return None, "RECIPE_INVALID"
        out = []
        for r in rows:
            if not isinstance(r, dict):
                return None, "RECIPE_INVALID"
            if not any(v not in (None, "", False) for v in r.values()):
                continue                                # 빈 행(v1 row_has_content)
            ok, errs, _ = RC.check_process_row(r, self.cfg)
            if errs:
                return None, _row_error_code(errs)
            out.append(ok)
        if not out:
            return None, "RECIPE_INVALID"
        return out, None

    def clear(self) -> Optional[str]:
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            self._clear_recipe()                        # 적재 없음이면 아무것도 안 하고 성공(v1 B5)
            return None

    def _clear_recipe(self) -> None:
        self.recipe_name, self.rows, self.list_active, self.csv_index = "", [], False, -1
        if self.state == "LOADED":
            self._set_state("IDLE")

    def start_manual(self, row: Dict[str, Any]) -> Optional[str]:
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            if self.rows:
                return "RECIPE_LOADED"
            r = {k: v for k, v in dict(row or {}).items() if k not in ("use_heater", "heater_temp", "heater_ramp")}
            ok, errs, _ = RC.check_process_row(r, self.cfg)
            if errs:
                return _row_error_code(errs)
            if ok.get("delay_s") is not None:
                return "RECIPE_INVALID"
            params = row_to_params(ok, self.cfg)
            why = self._start_guard(params)
            if why:
                return why
            self.list_active, self.csv_index = False, -1
            self._start_row(params, "Single CHK")
            return None

    def start_recipe(self) -> Optional[str]:
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            if not self.rows:
                return "RECIPE_NOT_LOADED"
            if self.heater.recipe_running and any(r.get("use_heater") is True for r in self.rows):
                return "HEATER_RECIPE_CONFLICT"
            for r in self.rows:
                if r.get("delay_s") is None:
                    why = self._device_guard(row_to_params(r, self.cfg))
                    if why:
                        return why
            why = self._plc_guard()
            if why:
                return why
            self.list_active, self.csv_index = True, -1
            self._next_row()
            return None

    def _plc_guard(self) -> Optional[str]:
        if self.safety is None:
            return None
        from v2.chk.safety import View
        last = self.plc.last
        v = View.from_poll(last) if last is not None else View({}, self.clock.wall(), {}, {})
        if last is not None:
            v.ages = {b: max(0.0, self.clock.wall() - last.t_wall) for b in ("plc", "heater", "rf")}
        return self.safety.check_start(v, False)

    def _device_guard(self, p: Dict[str, Any]) -> Optional[str]:
        """결정 5-2-4 — 꺼진 장비 DEVICE_DISABLED, 연결 안 된 장비 LINK_DOWN."""
        c = self.cfg
        if heater_used(p) and not c["HEATER_ENABLED"]:
            return "DEVICE_DISABLED"
        if base_pressure_of(p) > 0 and not c["IG_ENABLED"]:
            return "DEVICE_DISABLED"
        need = ["plc", "mfc"]
        if float(p.get("dc_power") or 0) > 0:
            need.append("dc")
        if p.get("use_rf_pulse") and float(p.get("rf_pulse_power") or 0) > 0:
            need.append("rfpulse")
        if base_pressure_of(p) > 0:
            need.append("ig")
        for k in need:
            if not self.links.get(k, lambda: False)():
                return "LINK_DOWN"
        return None

    def _start_guard(self, p: Dict[str, Any]) -> Optional[str]:
        return self._device_guard(p) or self._plc_guard()

    def stop(self) -> None:
        """사용자 STOP — 지금 행 중단 + 리스트 취소(v1 ProcessService.stop)."""
        with self._lock:
            if self.state in ("CSV_DELAY", "CSV_BETWEEN"):
                self._cancel_list("USER_STOP")
                return
            if self.state in ACT and self.safety is not None:
                self.safety.stop()

    # ── 안전 엔진이 부르는 것 ──
    def on_stop(self) -> None:
        with self._lock:
            self.end_kind, self.end_code = "aborted", "USER_STOP"
            self._halt()
            self._set_state("STOPPING")

    def on_abort(self, reason: str) -> None:
        with self._lock:
            if self.state in ("CSV_DELAY", "CSV_BETWEEN"):
                self._cancel_list(reason)
                return
            if self.end_kind is None or self.end_kind == "done":
                self.end_kind, self.end_code = "error", reason
            if reason in ("PLC_COMM_LOST",):
                self.comm_aborted = True
            self._halt()
            if self.state in ACT:
                self._set_state("STOPPING")

    def _halt(self) -> None:
        if self.runner is not None:
            self.runner.cancel()
        self.dcc.stop()
        self.rfc.stop()
        if self.uses_heater:
            self.heater.process_off()

    def on_stop_done(self, kind: str) -> None:
        with self._lock:
            if not self.row_is_process:
                return
            if kind == "row_end" and self.end_kind is None:
                self.end_kind = "done"
            kind_, code = self.end_kind or "error", self.end_code
            self._emit(ProcEvent("run_end", kind_, code, {"name": self.run_name, "status": kind_, "reason": code}))
            self.row_is_process = False
            self.filament_by_process = False
            self.params = {}
            if kind_ == "done" and self.list_active and self.csv_index + 1 < len(self.rows):
                self._set_state("CSV_BETWEEN")
                return
            ended_list = self.list_active
            self._set_state("DONE" if kind_ == "done" else "ABORTED")
            if ended_list:
                self._emit(ProcEvent("list_done" if kind_ == "done" else "list_cancel", self.recipe_name))
                self.recipe_name, self.rows, self.list_active, self.csv_index = "", [], False, -1
            self._settle_pending = True

    def _cancel_list(self, code: str) -> None:
        self._emit(ProcEvent("list_cancel", self.recipe_name, code))
        self.recipe_name, self.rows, self.list_active, self.csv_index = "", [], False, -1
        self._set_state("ABORTED")
        self._settle_pending = True

    # ── 행 ──
    def _next_row(self) -> None:
        while True:
            self.csv_index += 1
            if self.csv_index >= len(self.rows):
                self._emit(ProcEvent("list_done", self.recipe_name))
                self.recipe_name, self.rows, self.list_active, self.csv_index = "", [], False, -1
                self._set_state("DONE")
                self._settle_pending = True
                return
            r = self.rows[self.csv_index]
            if r.get("delay_s") is not None:
                if r["delay_s"] <= 0:
                    continue                              # 0초 딜레이는 건너뜀(v1)
                self.delay_s, self.delay_t0 = float(r["delay_s"]), self.clock.monotonic()
                self.row_is_process = False
                self._set_state("CSV_DELAY")
                return
            n, i = len(self.rows), self.csv_index + 1
            self._start_row(row_to_params(r, self.cfg), f"CSV {i}/{n} - {r.get('process_name') or f'STEP{i}'}")
            return

    def _start_row(self, params: Dict[str, Any], name: str) -> None:
        self.params, self.run_name, self.run_id = params, name, uuid.uuid4().hex[:12]
        self.end_kind = self.end_code = None
        self.row_is_process, self.comm_aborted, self.filament_by_process = True, False, False
        self.power, self.base = {}, {}
        steps = build_process_steps(params, self.cfg, ig_enabled=bool(self.cfg["IG_ENABLED"]))
        h = {"DC_POWER_STOP": self._h_dc_stop, "RF_POWER_STOP": self._h_rf_stop, "RF_PULSE_STOP": self._h_rfp_stop,
             "HEATER_RAMP": self._h_heater_ramp, "HEATER_SET": self._h_heater_set, "HEATER_WAIT": self._h_heater_wait,
             "IG_FILAMENT_ON": self._h_ig_on, "IG_BASE_WAIT": self._h_ig_wait, "IG_FILAMENT_OFF": self._h_ig_off,
             "POWER_WAIT": self._h_power_wait}
        self.runner = StepRunner(self.clock, steps, self.mfc, self.plc, h, self._step_event)
        if self.safety is not None:
            self.safety.engine.new_run()
        self._emit(ProcEvent("run_start", name, data={"name": name, "params": dict(params), "run_id": self.run_id}))
        self._emit(ProcEvent("steps", name, data={"steps": [s.unit() for s in steps]}))
        self._set_state(steps[0].state)

    def _step_event(self, e: StepEvent) -> None:
        if e.kind == "step":
            self._emit(ProcEvent("step", e.detail))
        elif e.kind == "warn":
            self._emit(ProcEvent("warn", e.detail, e.code))

    # ── tick ──
    def on_tick_start(self, tick) -> None:
        self._next_tick_mono = tick.t_mono + 1.0

    def on_row(self, view) -> None:
        with self._lock:
            now = self.clock.monotonic()
            if self._settle_pending and self.state in ("DONE", "ABORTED"):
                self._settle_pending = False
                self._set_state("LOADED" if self.rows else "IDLE")
                return
            if self.state == "CSV_DELAY":
                if now - self.delay_t0 >= self.delay_s - 1e-9:
                    self._next_row()
                return
            if self.state == "CSV_BETWEEN":
                self._next_row()
                return
            if self.state not in ACT or self.runner is None:
                return
            self.rfc.tick(view.get("rf.fwd_w"), view.get("rf.ref_w"))
            self.runner.tick(view, now)
            if self.runner.failed and self.state in ACT:
                if self.safety is not None:
                    self.safety.engine.abort(self.runner.failed, rule="process")
                return
            if self.runner.finished and self.state in ACT:
                if self.safety is not None and self.safety.engine.end_row():
                    self._halt()                          # 제어 루프를 멈추고 정지 순서로(상태 STOPPING — 진행 상태 규칙 끔)
                    self._set_state("STOPPING")
                return
            s = self.runner.step
            if s is not None and self.state in ACT:
                st = s.state
                if s.action == "HEATER_WAIT" and self.heater.process_phase == "HOLD_WAIT":
                    st = "HEATER_HOLD_WAIT"
                self._set_state(st)

    # ── 스텝 처리기(StepRunner handlers) ──
    def _h_dc_stop(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.dcc.stop()
            self.dc.output_off_confirmed()
        return "next" if now - cur.t0 >= POWER_STOP_SETTLE_S else "wait"

    def _h_rf_stop(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.rfc.stop()
            self.plc.rf_off()
        return "next" if now - cur.t0 >= POWER_STOP_SETTLE_S else "wait"

    def _h_rfp_stop(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.rfp.rf_off_confirmed()
        return "next" if now - cur.t0 >= POWER_STOP_SETTLE_S else "wait"

    def _h_heater_ramp(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.heater.process_ramp(float(s.value or self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]))
        return "next" if now - cur.t0 >= HEATER_RAMP_SETTLE_S else "wait"

    def _h_heater_set(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.heater.process_set(float(s.value or 0.0))
        return "next" if now - cur.t0 >= HEATER_SET_SETTLE_S else "wait"

    def _h_heater_wait(self, r, s, cur, view, now):
        res = self.heater.process_wait_tick(now)
        if res == "reached":
            return "next"
        return res if isinstance(res, tuple) else "wait"

    # 베이스 압력(v1 _ig_*)
    def _ig_result(self, since: float):
        lr = self.ig.last_result
        return lr if lr is not None and lr[3] >= since else None

    def _h_ig_on(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            if view.get("ig.filament") and view.get("ig.emission"):
                return "next"                             # 이미 켜짐·방출 정상
            self.filament_by_process = True
            self.ig.filament_on()
        res = self._ig_result(cur.t0)
        if res is not None and res[0] == "on":
            if res[1]:
                self.base["emission_since"] = None
                return "next"
            return ("fail", "IG_FILAMENT_FAILED")         # S39C
        if now - cur.t0 > float(self.cfg["IG_FILAMENT_ON_TIMEOUT_SEC"]) + 10.0:
            return ("fail", "IG_FILAMENT_FAILED")
        return "wait"

    def _h_ig_wait(self, r, s, cur, view, now):
        c, b = self.cfg, self.base
        target = float(s.value)
        if cur.persist is None:
            cur.persist = Persist(float(c["IG_BASE_STABLE_SEC"]))
            b.update(relights=0, relight_t0=None)
        if now - cur.t0 > float(c["IG_BASE_WAIT_TIMEOUT_SEC"]):
            return ("fail", "BASE_WAIT_TIMEOUT")         # S39A
        if b.get("relight_t0") is not None:              # 재점등 결과 대기
            res = self._ig_result(b["relight_t0"])
            if res is None:
                return "wait"
            if not res[1]:
                return ("fail", "IG_FILAMENT_FAILED") if b["relights"] >= int(c["IG_MAX_RELIGHTS"]) else self._relight(b, now)
            b["relight_t0"], b["emission_since"] = None, None
            cur.persist.reset()
        fil, emi, ion = view.get("ig.filament"), view.get("ig.emission"), view.get("ig.ion_torr")
        if fil is False:
            if b["relights"] >= int(c["IG_MAX_RELIGHTS"]):
                return ("fail", "IG_FILAMENT_FAILED")    # S39C
            return self._relight(b, now)
        t = view.t_of("ig")
        if fil and emi:
            if b.get("emission_since") is None:
                b["emission_since"] = t
        else:
            b["emission_since"] = None
        settled = b.get("emission_since") is not None and t - b["emission_since"] >= float(c["IG_SETTLE_SEC"])
        if ion is None or fil is None:
            cond = None                                  # 모름(stale·fail·suppressed) — 판정 유지
        else:
            cond = bool(settled and ion > 0 and ion <= target)
        if cur.persist.update(t, cond):
            self._emit(ProcEvent("base_reached", f"{ion:.1E} Torr", data={"ion": ion}))
            return "next"
        return "wait"

    def _relight(self, b, now):
        b["relights"] += 1
        b["relight_t0"] = now
        self._emit(ProcEvent("warn", f"베이스 대기 중 필라멘트 꺼짐 — 재점등 {b['relights']}/{self.cfg['IG_MAX_RELIGHTS']}"))
        self.ig.filament_on()
        return "wait"

    def _h_ig_off(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.filament_by_process = False
            self.ig.filament_off()
        res = self._ig_result(cur.t0)
        if res is not None and res[0] == "off":
            if not res[1]:
                self._emit(ProcEvent("warn", f"필라멘트 끄기 실패(공정 계속): {res[2]}"))
            return "next"
        if now - cur.t0 > float(self.cfg["IG_FILAMENT_OFF_TIMEOUT_SEC"]) + 10.0:
            self._emit(ProcEvent("warn", "필라멘트 끄기 결과 없음(공정 계속)"))
            return "next"
        return "wait"

    # 파워 대기(v1 _power_wait)
    def power_wait_timeout(self, dc_w: float, rf_w: float, rfp_w: float) -> float:
        c, est = self.cfg, []
        if dc_w > 0:
            est.append((dc_w / DC_EST_VOLTAGE_V) / max(float(c["DC_RAMP_STEP_A"]), 1e-6))
        if rf_w > 0:
            est.append(rf_w / max(float(c["RF_RAMP_STEP_W"]), 1e-6) + 3.0 * float(c["RF_REFP_WAIT_SEC"]))
        if rfp_w > 0:
            est.append(RFPULSE_EST_SEC)
        need = (max(est) * float(c["POWER_WAIT_EST_FACTOR"]) + float(c["POWER_WAIT_EST_MARGIN_SEC"])) if est else 0.0
        return min(max(float(c["POWER_WAIT_TIMEOUT_SEC"]), need), float(c["POWER_WAIT_TIMEOUT_MAX_SEC"]))

    def _h_power_wait(self, r, s, cur, view, now):
        p = self.params
        dc_w, rf_w = float(p.get("dc_power") or 0), float(p.get("rf_power") or 0)
        rfp_w = float(p.get("rf_pulse_power") or 0) if self.uses_rfp else 0.0
        pw = self.power
        if not cur.issued:
            cur.issued = True
            if not (dc_w or rf_w or rfp_w):
                return "next"
            pw.update(timeout=self.power_wait_timeout(dc_w, rf_w, rfp_w), rfp=None)
            nt = _NextTick(self._next_tick_mono)
            if dc_w > 0:
                self.dcc.start(dc_w, tick=nt)
            if rf_w > 0:
                self.rfc.start(rf_w, float(p.get("rf_offset") or 0.0), float(p.get("rf_param") or 1.0))
            if rfp_w > 0:
                def done(name, reason, results):
                    pw["rfp"] = reason is None
                    if self.safety is not None:
                        self.safety.on_rfp_start_done(name, reason, results)
                self.rfp.start(rfp_w, freq_hz=p.get("rf_pulse_freq_hz"), duty=p.get("rf_pulse_duty"), on_done=done, tick=nt)
        pend = []
        if dc_w > 0 and not self.dcc.reached:
            pend.append("dc")
        if rf_w > 0 and not self.rfc.reached:
            pend.append("rf")
        if rfp_w > 0 and pw.get("rfp") is not True:
            pend.append("rfpulse")
        if not pend:
            return "next"
        if now - cur.t0 > pw["timeout"]:
            self._emit(ProcEvent("warn", f"파워 목표 도달 대기 timeout({pw['timeout']:.0f}s) — 미도달: {', '.join(pend)}"))
            return ("fail", "POWER_WAIT_TIMEOUT")        # S36
        return "wait"
