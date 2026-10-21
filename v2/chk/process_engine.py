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
행 끝 정지 순서(STOPPING) 중에 온 STOP·중단·ALL STOP 은 late_stop 으로 사유를 남겨, 정지 순서가 끝나면 다음 행을 시작하지 않고
리스트를 끝낸다(v1 csv_cancelled — core/process_service.py:725-727·742-743·868·938-943·1026).
딜레이 행·행 사이에는 장비가 없어 정지 순서 없이 리스트를 바로 정리한다(v1 cancel_csv_list_now). 리스트가 끝나거나 취소되면
적재도 해제한다(v1 st.clear_csv_list — process_states 의 DONE→LOADED 는 쓰지 않음).
시작 거부(결정 5-2-4): 레시피가 쓰는 장비가 설정에서 꺼져 있으면 DEVICE_DISABLED, 연결 안 됐으면 LINK_DOWN.

블록 공정(blocks.py, 사용자 결정 2026-10-20): 한 행 = 블록 목록. v1 행은 기본 템플릿 블록으로 바꿔 받는다(같은 스텝).
- RECIPE_LOAD·PROCESS_START 는 v1 행과 {"blocks": [...]} 행을 둘 다 받는다(CSV 리스트·딜레이 행은 지금처럼).
- 블록 시작·끝·결과는 ProcEvent("block")(번호·종류·라벨·값). 화면·기록 상태는 지금 블록 번호/전체·종류·라벨·남은 시간
  (proc.block·proc.block_type·proc.block_label·proc.remain_s) — 상태 이름은 블록의 stage(기본 템플릿은 v1 이름).
- 정상 끝 정지 블록(END_STOP)이 정지 순서(SafetyEngine.end_row)를 낸다. 중간에 있으면 정지 순서 뒤 다음 블록으로.
  목록 끝이 정상 끝 정지가 아니면 blocks.expand 가 "자동 끝 정지"를 덧붙인다(AUTO_END_STOP) — 목록 끝은 늘 정상 끝 정지.
  STOP·이상 중단 정지 순서는 고정(표 전체), 정상 끝 정지만 skip(파워 끄기 단계는 못 뺀다 — 사용자 결정 2026-10-21).
- 사용 장비(uses_*)는 블록에서(blocks.uses_of). 안전 규칙의 적용 구간은 장비 상태(dc_ramping·rf_ramping)와 블록 감시(watching).

공정 중 값 수정(adjust, 5-4 ③ — PROCESS_ADJUST): "이번 공정의 그 값"을 바꾼다. 값 바꾸기는 여기 한 곳에서
검사(공정 중·범위·이번 공정에서 쓰는 항목) → 드라이버 쓰기(지금 쓰는 중이면 — 되읽기 확인 포함) → 판정 기준 갱신
(overrides·cur_vals → 아직 안 온 블록이 새 값, 이탈 판정 지속 시간 처음부터, 파워는 다시 도달할 때까지 이탈 판정 쉼) → 기록
(ProcEvent "adjust" PROCESS_ADJUST: 장비·항목·이전 → 새 값·출처·결과(rejected/scheduled/applied → confirmed/failed)·사유).
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.judge import Persist
from v2.chk import blocks as BK
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk.power_control import DcControl, RfControl
from v2.chk.process_steps import (HEATER_RAMP_SETTLE_S, HEATER_SET_SETTLE_S, POWER_STOP_SETTLE_S, ProcessStep,
                                  StepEvent, StepRunner, base_pressure_of, heater_used,
                                  row_to_params)

ACT = frozenset(PS.ACTIVE_STATES)
DC_EST_VOLTAGE_V = 300.0          # v1 process_controller.DC_EST_VOLTAGE_V
RFPULSE_EST_SEC = 20.0            # v1 RFPULSE_EST_SEC


def _row_error_code(errs: List[str]) -> str:
    """레시피 행 검사 오류 → 사유 코드. 꺼진 장비를 쓰면 시작 거부(결정 5-2-4): 이온 게이지는 IG_DISABLED(S47), 히터는 DEVICE_DISABLED."""
    if any("IG_ENABLED" in e for e in errs):
        return "IG_DISABLED"
    if any("HEATER_ENABLED" in e or "RFPULSE_ENABLED" in e for e in errs):
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
        self.dc_on = self.rf_on = self.rfp_on = False     # 이번 행에서 출력 명령을 냈다(v1 is_dc_on·is_rf_on·is_rfpulse_on)
        self.comm_aborted = False
        self.dc_comm_aborted = self.rfp_comm_aborted = False   # 두절로 중단 — 링크 복구 때 OFF 다시(S30·S34C 해제)
        self._next_tick_mono = 0.0
        self._settle_pending = False
        self.power: Dict[str, Any] = {}
        self.base: Dict[str, Any] = {}
        # 블록
        self.blocks: List[dict] = []
        self.insts: List[BK.BlockInst] = []
        self.uses: Dict[str, Any] = BK.uses_of([])
        self.block_idx = -1
        self.block_t0 = 0.0
        self.overrides: Dict[str, Any] = {}     # 공정 중 수정 — 항목 키 → 이번 공정의 새 값
        self.cur_vals: Dict[str, Any] = {}      # 스텝이 장비에 쓴 값(항목 키 → 값)
        self.flow_on = {1: False, 2: False}
        self.rfp_target: Optional[float] = None
        self.rfp_started = False                # RF Pulse START 가 끝났다(공정 중 수정은 그 뒤에만)
        self.settle: Dict[str, float] = {}      # 공정 중 수정 뒤 새 목표에 도달할 때까지 이탈 판정 쉼(장비 → 시작 시각)
        self._end_stop_resume = False

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
        return bool(self.uses["dc"])

    @property
    def uses_rf(self) -> bool:
        return bool(self.uses["rf"])

    @property
    def uses_rfp(self) -> bool:
        return bool(self.uses["rfp"])

    @property
    def uses_heater(self) -> bool:
        return bool(self.uses["heater"])

    @property
    def dc_ramping(self) -> bool:
        """무출력 판정(S31A) 구간 — DC 가 램프업 중(v1 _tick_ramping_up)."""
        return self.dcc.state == DcControl.RAMPING_UP

    @property
    def rf_ramping(self) -> bool:
        return self.rfc.state == RfControl.RAMPING_UP

    @property
    def block(self) -> Optional[BK.BlockInst]:
        return self.insts[self.block_idx] if 0 <= self.block_idx < len(self.insts) else None

    @property
    def block_type(self) -> Optional[str]:
        b = self.block
        return b.type if b is not None and self.state in ACT else None

    @property
    def dc_settling(self) -> bool:
        return "dc" in self.settle

    @property
    def rfp_settling(self) -> bool:
        return "rfp" in self.settle

    def watching(self, item: str) -> bool:
        """이탈 감시(S31C·S33B·S34A·S38A·S38B) — 지금 블록의 watch. 블록이 없으면 기본값."""
        b = self.block
        w = b.watch if b is not None else BK.WATCH_DEFAULT
        return bool(w.get(item, False))

    @property
    def rf_ref_waiting(self) -> bool:
        return self.rfc.ref_waiting

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
        return self.rfp_target if self.uses_rfp else None

    @property
    def flow_set(self) -> Dict[str, Optional[float]]:
        """유량 이탈(S38A) 기준 — 켜 둔 채널의 설정값(공정 중 수정 반영)."""
        return {"ar": self.cur_vals.get("mfc.ar_flow") if self.flow_on[1] else None,
                "o2": self.cur_vals.get("mfc.o2_flow") if self.flow_on[2] else None}

    @property
    def sp1_mtorr(self):
        return self.cur_vals.get("mfc.wp") or None

    # ── 기록 ──
    def _emit(self, e: ProcEvent) -> None:
        self.events.append(e)
        if self.on_event:
            try:
                self.on_event(e)
            except Exception:
                pass

    def note_plc_manual(self, target: str, on: bool, source: str) -> None:
        """공정 중 PLC 버튼 실행 기록(PLC_MANUAL_IN_PROCESS, info) — 대상·켬/끔·출처·시각(사용자 결정 2026-10-20)."""
        self._emit(ProcEvent("plc_manual", f"{target} {'ON' if on else 'OFF'} ({source})", "PLC_MANUAL_IN_PROCESS",
                             {"target": target, "on": bool(on), "source": source, "t": self.clock.wall(),
                              "run_id": self.run_id or None, "state": self.state, "level": "info"}))

    def _dev_msg(self, level: str, text: str) -> None:
        self._emit(ProcEvent("message", text, None, {"level": level}))

    def _set_state(self, s: str) -> None:
        if s != self.state:
            old, self.state = self.state, s
            self._emit(ProcEvent("state", f"{old}→{s}", data={"from": old, "to": s, "t": self.clock.monotonic()}))

    def snapshot_fields(self) -> Dict[str, Any]:
        b = self.block if self.row_is_process and (self.state in ACT or self.state == "STOPPING") else None
        return {"proc.state": self.state, "proc.run_id": self.run_id or None,
                "proc.row": (self.csv_index + 1) if self.list_active else None,
                "proc.block": f"{b.index + 1}/{len(self.insts)}" if b is not None else None,
                "proc.block_type": b.type if b is not None else None,
                "proc.block_label": (b.label or b.no) if b is not None else None,
                "proc.remain_s": self.remain_s()}

    def remain_s(self) -> Optional[int]:
        """지금 대기 스텝(DELAY·조건 대기·압력 대기)의 남은 초 — 화면용. 딜레이 행이면 그 남은 시간."""
        now = self.clock.monotonic()
        if self.state == "CSV_DELAY":
            return max(0, int(round(self.delay_s - (now - self.delay_t0))))
        r = self.runner
        if r is None or self.state not in ACT or r.step is None or r.cur is None:
            return None
        s, cur = r.step, r.cur
        if s.action == "DELAY":
            dur = float(cur.extra.get("dur", s.duration_sec) or 0)
        elif s.action == "COND_WAIT":
            dur = float(s.params["timeout_sec"])
        elif s.action == "MFC_CMD" and s.params[0] == "WAIT_PRESSURE":
            dur = float(s.params[1]["timeout_sec"])
        else:
            return None
        return max(0, int(round(dur - (now - cur.t0))))

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
            ok, err = self._check_row(r)
            if err:
                return None, err
            out.append(ok)
        if not out:
            return None, "RECIPE_INVALID"
        return out, None

    def _check_row(self, r: Dict[str, Any]) -> tuple:
        """행 하나 → (정리된 행, 사유). 블록 행 {"process_name"(선택), "blocks": [...]} 또는 v1 행·딜레이 행."""
        if "blocks" in r:
            if set(r) - {"process_name", "blocks"} or not isinstance(r.get("process_name", ""), (str, type(None))):
                return None, "RECIPE_INVALID"
            bl, errs, warns = BK.check_blocks(r["blocks"], self.cfg)
            if errs:
                self._emit(ProcEvent("reject", "; ".join(errs[:5]), _row_error_code(errs), {"errors": errs}))
                return None, _row_error_code(errs)
            return {"process_name": r.get("process_name") or "", "blocks": bl}, None
        ok, errs, _ = RC.check_process_row(r, self.cfg)
        if errs:
            return None, _row_error_code(errs)
        return ok, None

    def _row_uses(self, r: Dict[str, Any]) -> Dict[str, Any]:
        """적재된 행이 쓰는 장비. v1 행은 그 행의 기본 템플릿과 같은 판단(지금 _device_guard 와 같은 조건)."""
        if "blocks" in r:
            return BK.uses_of(r["blocks"])
        p = row_to_params(r, self.cfg)
        u = BK.uses_of([])
        u.update(dc=float(p.get("dc_power") or 0) > 0, rf=float(p.get("rf_power") or 0) > 0,
                 rfp=bool(p.get("use_rf_pulse")) and float(p.get("rf_pulse_power") or 0) > 0,
                 heater=heater_used(p), ig=base_pressure_of(p) > 0, ar=bool(p.get("use_ar_gas")), o2=bool(p.get("use_o2_gas")))
        return u

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
            row = dict(row or {})
            if "blocks" not in row:                     # v1 수동 공정은 히터를 쓰지 않는다(히터 열 무시)
                row = {k: v for k, v in row.items() if k not in ("use_heater", "heater_temp", "heater_ramp")}
            ok, why = self._check_row(row)
            if why:
                return why
            if ok.get("delay_s") is not None:
                return "RECIPE_INVALID"
            u = self._row_uses(ok)
            if u["heater"] and self.heater.recipe_running:
                return "HEATER_RECIPE_CONFLICT"
            why = self._device_guard(u) or self._plc_guard()
            if why:
                return why
            self.list_active, self.csv_index = False, -1
            self._start_row(ok, "Single CHK")
            return None

    def start_recipe(self) -> Optional[str]:
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            if not self.rows:
                return "RECIPE_NOT_LOADED"
            if self.heater.recipe_running and any(r.get("delay_s") is None and self._row_uses(r)["heater"]
                                                  for r in self.rows):
                return "HEATER_RECIPE_CONFLICT"
            for r in self.rows:
                if r.get("delay_s") is None:
                    why = self._device_guard(self._row_uses(r))
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
        return self.safety.check_start(self.safety.plc_view(), False)

    @property
    def list_uses_heater(self) -> bool:
        """적재된 행 중 하나라도 히터를 쓰는가(v1 _csv_list_uses_heater)."""
        return any(r.get("delay_s") is None and self._row_uses(r)["heater"] for r in self.rows)

    def _device_guard(self, u: Dict[str, Any]) -> Optional[str]:
        """결정 5-2-4 — 꺼진 장비 DEVICE_DISABLED, 연결 안 된 장비 LINK_DOWN. u = 행이 쓰는 장비(_row_uses)."""
        c = self.cfg
        if u["heater"] and not c["HEATER_ENABLED"]:
            return "DEVICE_DISABLED"
        if u["ig"] and not c["IG_ENABLED"]:
            return "DEVICE_DISABLED"
        if u["rfp"] and not c["RFPULSE_ENABLED"]:
            return "DEVICE_DISABLED"
        if getattr(self.mfc, "scale_mismatch", None):
            return "MFC_SCALE_MISMATCH"                          # 연결 때 R33·R34·R74·R75·R37 대조(값은 자동으로 바꾸지 않음)
        need = ["plc", "mfc"]
        if u["dc"]:
            need.append("dc")
        if u["rfp"]:
            need.append("rfpulse")
        if u["ig"]:
            need.append("ig")
        for k in need:
            if not self.links.get(k, lambda: False)():
                return "LINK_DOWN"
        return None

    def stop(self) -> None:
        """사용자 STOP — 지금 행 중단 + 리스트 취소(v1 ProcessService.stop)."""
        with self._lock:
            if self.state in ("CSV_DELAY", "CSV_BETWEEN"):
                self._cancel_list("USER_STOP")
                return
            if self.state == "STOPPING":
                self.late_stop("aborted", "USER_STOP")
                return
            if self.state in ACT and self.safety is not None:
                self.safety.stop()

    # ── 안전 엔진이 부르는 것 ──
    def on_stop(self) -> None:
        with self._lock:
            self.end_kind, self.end_code = "aborted", "USER_STOP"
            self._halt()
            self._set_state("STOPPING")

    def late_stop(self, kind: str, code: str) -> None:
        """정지 순서(행 끝 포함) 중에 온 STOP·중단·ALL STOP — 첫 사유를 남긴다. 정지 순서가 끝나면 이 행은 그 사유로 끝나고
        리스트는 다음 행 없이 끝난다(v1 step_ok=False·csv_cancelled)."""
        with self._lock:
            if self.state == "STOPPING" and self.row_is_process and self.end_kind in (None, "done"):
                self.end_kind, self.end_code = kind, code
                self._emit(ProcEvent("warn", f"정지 순서 중 {code} — 리스트를 끝낸다", code))

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

    def _halt(self, cancel_runner: bool = True) -> None:
        if self.runner is not None and cancel_runner:
            self.runner.cancel()
        self.dcc.stop()
        self.rfc.stop()
        if self.uses_heater:
            self.heater.process_off()

    def on_stop_done(self, kind: str) -> None:
        with self._lock:
            if not self.row_is_process:
                return
            r = self.runner
            if (kind == "row_end" and self.end_kind is None and self._end_stop_resume and r is not None
                    and not r.cancelled and r.cur is not None):
                # 목록 중간의 정상 끝 정지 블록 — 정지 순서 뒤 다음 블록으로
                self._end_stop_resume = False
                r.cur.done = True
                nxt = r.steps[r.idx + 1].state if r.idx + 1 < len(r.steps) else "PRE_CLEAN"
                self._set_state(nxt if nxt in ACT else "WAIT")
                return
            self._end_stop_resume = False
            if kind == "row_end" and self.end_kind is None:
                self.end_kind = "done"
            kind_, code = self.end_kind or "error", self.end_code
            self._block_end("ok" if kind_ == "done" else kind_, code)
            self._emit(ProcEvent("run_end", kind_, code, {"name": self.run_name, "status": kind_, "reason": code}))
            self.row_is_process = False
            self.filament_by_process = False
            self.params = {}
            self.flow_on, self.settle = {1: False, 2: False}, {}       # 행이 끝나면 판정 기준도 정리
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
            self._start_row(r, f"CSV {i}/{n} - {r.get('process_name') or f'STEP{i}'}")
            return

    def _start_row(self, row: Dict[str, Any], name: str) -> None:
        """행 시작 — 블록 행은 그 블록, v1 행은 기본 템플릿 블록(blocks.template_blocks)."""
        ig_en, rfp_en = bool(self.cfg["IG_ENABLED"]), bool(self.cfg["RFPULSE_ENABLED"])
        if "blocks" in row:
            blocks = row["blocks"]
            params = BK.params_summary(blocks, row.get("process_name") or "", self.cfg)
        else:
            params = row_to_params(row, self.cfg)
            blocks = BK.template_blocks(params, self.cfg, ig_enabled=ig_en, rfpulse_present=rfp_en)
        steps, insts = BK.expand(blocks, self.cfg, ig_enabled=ig_en, rfpulse_present=rfp_en)
        self.params, self.run_name, self.run_id = params, name, uuid.uuid4().hex[:12]
        self.blocks, self.insts, self.uses = blocks, insts, BK.uses_of(blocks)
        self.block_idx, self.overrides, self.cur_vals = -1, {}, {}
        self.flow_on, self.rfp_target, self._end_stop_resume = {1: False, 2: False}, None, False
        self.rfp_started, self.settle = False, {}
        self.end_kind = self.end_code = None
        self.row_is_process, self.comm_aborted, self.filament_by_process = True, False, False
        self.dc_on = self.rf_on = self.rfp_on = False
        self.power, self.base = {}, {}
        h = {"DC_POWER_STOP": self._h_dc_stop, "RF_POWER_STOP": self._h_rf_stop, "RF_PULSE_STOP": self._h_rfp_stop,
             "HEATER_RAMP": self._h_heater_ramp, "HEATER_SET": self._h_heater_set, "HEATER_WAIT": self._h_heater_wait,
             "IG_FILAMENT_ON": self._h_ig_on, "IG_BASE_WAIT": self._h_ig_wait, "IG_FILAMENT_OFF": self._h_ig_off,
             "POWER_WAIT": self._h_power_wait, "POWER_START": self._h_power_wait, "RF_RAMP_OFF": self._h_rf_ramp_off,
             "HEATER_OFF": self._h_heater_off, "END_STOP": self._h_end_stop}
        self.runner = StepRunner(self.clock, steps, self.mfc, self.plc, h, self._step_event, cfg=self.cfg,
                                 resolve=self._resolve, on_step_done=self._on_step_done)
        if self.safety is not None:
            self.safety.engine.new_run()
        self._emit(ProcEvent("run_start", name, data={"name": name, "params": dict(params), "run_id": self.run_id,
                                                       "blocks": [b.record() for b in insts]}))
        self._emit(ProcEvent("steps", name, data={"steps": [s.unit() for s in steps if not s.internal]}))
        if insts and insts[-1].auto:
            self._emit(ProcEvent("auto_end_stop", f"목록 끝이 정상 끝 정지가 아니어서 '{insts[-1].label}'(블록 {insts[-1].no}) 덧붙임",
                                 "AUTO_END_STOP", {"no": insts[-1].no, "level": "info"}))
        self._set_state(steps[0].state if steps[0].state in ACT else "WAIT")

    # ── 블록 기록 ──
    def _block_start(self, idx: int) -> None:
        if idx == self.block_idx:
            return
        self._block_end("ok")
        self.block_idx, self.block_t0 = idx, self.clock.monotonic()
        b = self.block
        if b is not None:
            self._emit(ProcEvent("block", f"{b.no} {b.type} {b.label}".strip(), data=dict(
                b.record(), phase="start", index=b.index + 1, total=len(self.insts), t=self.clock.wall(), run_id=self.run_id)))

    def _block_end(self, result: str, code: Optional[str] = None) -> None:
        b = self.block
        if b is None or b.ended:
            return
        b.ended = True
        self._emit(ProcEvent("block", f"{b.no} {b.type} → {result}", code, data=dict(
            b.record(), phase="end", result=result, reason=code, index=b.index + 1, total=len(self.insts),
            elapsed_s=round(self.clock.monotonic() - self.block_t0, 1), t=self.clock.wall(), run_id=self.run_id)))

    def _step_event(self, e: StepEvent) -> None:
        if e.kind in ("step", "istep") and self.runner is not None and 0 <= e.index < len(self.runner.steps):
            s = self.runner.steps[e.index]
            self._block_start(s.block)
            if (s.spec or {}).get("main") and "process.main_remaining" in self.overrides and self.runner.cur is not None:
                v = float(self.overrides.pop("process.main_remaining"))       # 메인 전에 수정한 길이 — 이 메인 블록이 쓴다
                self.runner.cur.extra["dur"] = v
                self._adjust_rec("process", "main_remaining", s.duration_sec, v, "scheduled", "applied", None)
        if e.kind == "step":
            self._emit(ProcEvent("step", e.detail))
        elif e.kind == "warn":
            self._emit(ProcEvent("warn", e.detail, e.code))
        elif e.kind == "failed":
            self._block_end("failed", e.code)

    # ── 값(공정 중 수정이 바꾼 값 → 그 항목을 쓰는 스텝) ──
    def _resolve(self, s: ProcessStep, v: Any) -> Any:
        if s.ref and s.ref in self.overrides and not s.ref.startswith("process."):
            v = self.overrides[s.ref]
        if s.ref:
            self.cur_vals[s.ref] = v
        return v

    def _on_step_done(self, s: ProcessStep) -> None:
        if s.action == "MFC_CMD" and s.params[0] in ("FLOW_ON", "FLOW_OFF"):
            self.flow_on[int(s.params[1]["channel"])] = s.params[0] == "FLOW_ON"

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
            self._settle_tick(view, now)
            self.runner.tick(view, now)
            if self.runner.failed and self.state in ACT:
                if self.safety is not None:
                    self.safety.engine.abort(self.runner.failed, rule="process")
                return
            if self.runner.finished and self.state in ACT:
                # 목록 끝은 늘 정상 끝 정지(자동 끝 정지 포함)라 여기는 정지 순서를 못 낸 경우뿐(안전 엔진 없음)
                self.end_kind = self.end_kind or "done"
                self.on_stop_done("row_end")
                return
            s = self.runner.step
            if s is not None and self.state in ACT:
                st = s.state
                if s.action == "HEATER_WAIT" and self.heater.process_phase == "HOLD_WAIT":
                    st = "HEATER_HOLD_WAIT"
                if st in ACT:
                    self._set_state(st)

    # ── 스텝 처리기(StepRunner handlers) ──
    def _h_dc_stop(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.dcc.stop()
            self.dc.output_off_confirmed()
            self.dc_on = False
        return "next" if now - cur.t0 >= POWER_STOP_SETTLE_S else "wait"

    def _h_rf_stop(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.rfc.stop()
            self.plc.rf_off()
            self.rf_on = False
        return "next" if now - cur.t0 >= POWER_STOP_SETTLE_S else "wait"

    def _h_rfp_stop(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.rfp.rf_off_confirmed()
            self.rfp_on, self.rfp_target = False, None
        return "next" if now - cur.t0 >= POWER_STOP_SETTLE_S else "wait"

    def _h_rf_ramp_off(self, r, s, cur, view, now):
        """파워 끄기 블록의 RF — 정지 순서와 같은 처리기(RF_RAMP_DOWN → RF_SAFE_OFF)를 따로 스레드에서(tick 을 막지 않음)."""
        if not cur.issued:
            cur.issued = True
            self.rfc.stop()
            hs = self.safety.engine.handlers if self.safety is not None else {}

            def done(err=None):
                cur.err, cur.done = err, True
                self.clock.notify()

            def run():
                if "RF_RAMP_DOWN" in hs:
                    hs["RF_RAMP_DOWN"]({}, lambda err=None: None)
                if "RF_SAFE_OFF" in hs:
                    hs["RF_SAFE_OFF"]({}, done)
                else:
                    self.plc.rf_off(on_done=lambda res: done(None))
            self.clock.start_thread(run, "rf-rampdown")
        if cur.done:
            self.rf_on = False
            if cur.err:
                self._emit(ProcEvent("warn", f"RF OFF 결과 오류: {cur.err}", "RF_OFF_FAILED"))
            return "next"
        if now - cur.t0 > float(self.cfg["STOP_RF_RAMPDOWN_TIMEOUT_SEC"]) + 10.0:
            return ("fail", "INTERNAL_ERROR")
        return "wait"

    def _h_heater_off(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.heater.process_off()
        return "next" if now - cur.t0 >= HEATER_RAMP_SETTLE_S else "wait"

    def _h_end_stop(self, r, s, cur, view, now):
        """정상 끝 정지 블록 — 지금 행 끝 정지 순서(SafetyEngine.end_row). 끝나면 on_stop_done 이 행을 끝내거나(마지막 블록)
        다음 블록으로 넘긴다(중간 블록)."""
        if not cur.issued:
            cur.issued = True
            skip = tuple((s.spec or {}).get("skip") or ())
            if self.safety is not None and self.safety.engine.end_row(skip=skip):
                self._end_stop_resume = r.idx + 1 < len(r.steps)
                self._halt(cancel_runner=False)            # 제어 루프를 멈추고 정지 순서로(상태 STOPPING — 진행 상태 규칙 끔)
                self._set_state("STOPPING")
            return "wait"
        return "next" if cur.done else "wait"

    def _h_heater_ramp(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.heater.process_ramp(float(self._resolve(s, s.value) or self.cfg["HEATER_RAMP_RATE_C_PER_MIN"]))
        return "next" if now - cur.t0 >= HEATER_RAMP_SETTLE_S else "wait"

    def _h_heater_set(self, r, s, cur, view, now):
        if not cur.issued:
            cur.issued = True
            self.heater.process_set(float(self._resolve(s, s.value) or 0.0))
        return "next" if now - cur.t0 >= HEATER_SET_SETTLE_S else "wait"

    def _h_heater_wait(self, r, s, cur, view, now):
        if not self.heater.proc.get("target"):
            self._emit(ProcEvent("warn", "히터 목표가 없음(승온 블록 없음) — 도달 대기를 건너뜀", "HEATER_NO_TARGET"))
            return "next"
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
        # IG_SETTLE 은 드라이버가 처음 방출을 확인한 시각(ig.emission_since)부터 — 없으면 이 대기에서 처음 본 시각
        es = getattr(self.ig, "emission_since", None) if (fil and emi) else None
        since = es if es is not None else b.get("emission_since")
        settled = since is not None and t - since >= float(c["IG_SETTLE_SEC"])
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
        self.filament_by_process = True                    # 재점등한 필라멘트도 공정 소유 — 정지 때 끈다(v1 pc:1092-1094·1254)
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

    # ── 공정 중 값 수정(PROCESS_ADJUST) ──
    def _adjust_rec(self, device: str, item: str, old: Any, new: Any, source: str, result: str, reason: Optional[str]) -> None:
        self._emit(ProcEvent("adjust", f"{device}.{item} {old} → {new} ({source}) {result}{' ' + reason if reason else ''}",
                             "PROCESS_ADJUST", {"device": device, "item": item, "old": old, "new": new, "source": source,
                                                "result": result, "reason": reason, "t": self.clock.wall(),
                                                "run_id": self.run_id or None, "state": self.state,
                                                "block": self.block.no if self.block is not None else None,
                                                "level": "warn" if result in ("rejected", "failed") else "info"}))

    def _uses_item(self, ai: BK.AdjustItem) -> bool:
        if ai.device in ("dc", "rf", "rfpulse"):
            return bool(self.uses[{"dc": "dc", "rf": "rf", "rfpulse": "rfp"}[ai.device]])
        return self.runner is not None and any(s.ref == ai.key for s in self.runner.steps)

    def _adjust_old(self, ai: BK.AdjustItem):
        if ai.key == "rfpulse.freq_hz":
            hz = self.overrides.get(ai.key, self.cur_vals.get(ai.key))
            return None if hz is None else hz / 1000.0
        if ai.key == "process.main_remaining":
            r = self.runner
            if r is not None and r.step is not None and (r.step.spec or {}).get("main") and r.cur is not None:
                return self.remain_s()
            return self.overrides.get(ai.key)
        return self.overrides.get(ai.key, self.cur_vals.get(ai.key))

    def adjust(self, device: str, item: str, value: Any, source: str = "web") -> Optional[str]:
        """PROCESS_ADJUST — 돌려주는 값은 거부 사유 코드 또는 None. 결과는 ProcEvent("adjust")."""
        with self._lock:
            ai = BK.adjust_item(str(device), str(item))
            if ai is None:
                self._adjust_rec(str(device), str(item), None, value, source, "rejected", "BAD_ARGS")
                return "BAD_ARGS"
            old = self._adjust_old(ai)
            why = self._adjust_check(ai, value)
            if why:
                self._adjust_rec(device, item, old, value, source, "rejected", why)
                return why
            result = self._adjust_apply(ai, value, source, old)
            self._adjust_rec(device, item, old, value, source, result, None)
            return None

    def _adjust_check(self, ai: BK.AdjustItem, value: Any) -> Optional[str]:
        if not self.row_is_process or self.state not in ACT or self.runner is None:
            return "PROCESS_NOT_RUNNING"                                  # 공정 밖·정지 순서 중·딜레이 행
        e = BK._check_param(ai.param, value, self.cfg, ())
        if e:
            return "BAD_ARGS" if ("숫자" in e or "정수" in e or "true/false" in e) else "ADJUST_OUT_OF_RANGE"
        r = self.runner
        if ai.key == "process.skip_wait":
            s = r.step
            if value is not True or s is None:
                return "BAD_ARGS" if value is not True else "ADJUST_NOT_APPLICABLE"
            if not (s.spec or {}).get("skippable"):
                return "ADJUST_NOT_APPLICABLE"                             # 대기·조건 대기 블록이 아님
            return None
        if not self._uses_item(ai):
            return "ADJUST_NOT_USED"                                       # 이번 공정에서 쓰지 않는 장비·항목
        if ai.device == "rfpulse":
            if self.rfp_on and not self.rfp_started:
                return "ADJUST_BUSY"                                       # START 중 — 끝난 뒤에
            hz = int(round(float(value) * 1000.0)) if ai.item == "freq" else self.overrides.get(
                "rfpulse.freq_hz", self.cur_vals.get("rfpulse.freq_hz"))
            du = int(value) if ai.item == "duty" else self.overrides.get("rfpulse.duty", self.cur_vals.get("rfpulse.duty"))
            if hz and du and RC.pulse_edge_violation(float(hz), float(du), self.cfg["RFPULSE_PULSE_MIN_EDGE_US"]):
                return "ADJUST_OUT_OF_RANGE"
        return None

    def _adjust_apply(self, ai: BK.AdjustItem, value: Any, source: str, old: Any) -> str:
        """판정 기준 갱신 + 지금 쓰는 중이면 드라이버 경로로. 'applied'(바로) 또는 'scheduled'(아직 안 온 블록이 새 값으로)."""
        key, now, r = ai.key, self.clock.monotonic(), self.runner
        eng = self.safety.engine if self.safety is not None else None
        reset = lambda: [eng.reset_rule(x) for x in ai.rules] if eng is not None else None

        def confirm(res_reason):
            ok = res_reason is None
            self._adjust_rec(ai.device, ai.item, old, value, source, "confirmed" if ok else "failed", res_reason)
            if (not ok and res_reason == "RFP_CONFIG_MISMATCH" and self.cfg["RFPULSE_VERIFY_PULSE_CONFIG"]
                    and eng is not None and self.active):
                eng.abort("RFP_CONFIG_MISMATCH", rule="adjust")             # START 되읽기 불일치와 같은 처리(S35)
        seq_cb = lambda name, reason, results: confirm(reason)
        if key == "process.skip_wait":
            r.cur.extra["skip"] = True
            return "applied"
        if key == "process.main_remaining":
            s = r.step
            if s is not None and (s.spec or {}).get("main") and r.cur is not None:
                r.cur.extra["dur"] = (now - r.cur.t0) + float(value)
                return "applied"
            self.overrides[key] = float(value)
            return "scheduled"
        v = int(round(float(value) * 1000.0)) if key == "rfpulse.freq_hz" else (
            int(value) if ai.param.type == "int" else (float(value) if ai.param.type == "float" else value))
        self.overrides[key] = v
        live = False
        if ai.device == "mfc":
            reset()
            if key in self.cur_vals:
                live = True
                self.cur_vals[key] = v
                if key == "mfc.wp":
                    self.mfc.set_sp(1, v, on_done=seq_cb)
                else:
                    self.mfc.set_flow(1 if key == "mfc.ar_flow" else 2, v, on_done=seq_cb)
        elif key == "dc.power":
            if self.dc_on and self.dcc.state != DcControl.IDLE:
                live = True
                self.cur_vals[key] = v
                self.dcc.retarget(v)
                self.settle["dc"] = now
            reset()
        elif key == "rf.power":
            if self.rf_on and self.rfc.state != RfControl.IDLE:
                live = True
                self.cur_vals[key] = v
                self.rfc.retarget(v)                                       # 유지 → 램프(S33B 는 유지 중에만)
            reset()
        elif ai.device == "rfpulse":
            if self.rfp_on and self.rfp_started:
                live = True
                self.cur_vals[key] = v
                kw = {"power": "power_w", "freq": "freq_hz", "duty": "duty"}[ai.item]
                if ai.item == "power":
                    self.rfp_target = v
                    self.settle["rfp"] = now
                self.rfp.adjust(**{kw: v}, on_done=seq_cb)
            reset()
        elif ai.device == "heater":
            live = bool(self.heater.process_adjust(**{"target" if ai.item == "target" else "rate": v}))
            if live:
                self.cur_vals[key] = v
        return "applied" if live else "scheduled"

    def _settle_tick(self, view, now: float) -> None:
        """공정 중 수정 뒤 이탈 판정 쉼 — 새 목표에 다시 도달하면(또는 ADJUST_SETTLE_MAX_SEC 가 지나면) 판정을 다시 켠다."""
        lim = float(self.cfg["ADJUST_SETTLE_MAX_SEC"])
        if "dc" in self.settle:
            if self.dcc.reached or self.dcc.state == DcControl.IDLE:
                self.settle.pop("dc")
            elif now - self.settle["dc"] >= lim:
                self.settle.pop("dc")
                self._emit(ProcEvent("warn", f"DC 새 목표 {self.dcc.target:.1f} W 에 {lim:.0f} s 안에 못 감 — 이탈 판정 다시 켬",
                                     "ADJUST_SETTLE_TIMEOUT"))
        if "rfp" in self.settle:
            fwd, tgt = view.get("rfp.fwd_w"), self.rfp_target
            tol = float(self.cfg["RFPULSE_FORP_TOLERANCE_PERCENT"]) / 100.0
            if not self.rfp_on or tgt is None or (fwd is not None and abs(float(fwd) - tgt) < tgt * tol):
                self.settle.pop("rfp")
            elif now - self.settle["rfp"] >= lim:
                self.settle.pop("rfp")
                self._emit(ProcEvent("warn", f"RF Pulse 새 목표 {tgt:.0f} W 에 {lim:.0f} s 안에 못 감 — 이탈 판정 다시 켬",
                                     "ADJUST_SETTLE_TIMEOUT"))

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
        """파워 켜기 블록(POWER_WAIT = 도달 대기, POWER_START = 켜기만). 값은 블록(spec) — 공정 중 수정한 값이 있으면 그 값.
        이미 켜진 장비는 다시 켜지 않고 목표만 바꾼다(DC 제어 목표·RF 1 W/s 램프)."""
        sp = s.spec or {}
        ov = self.overrides
        dc_w = float(ov.get("dc.power", sp.get("dc") or 0)) if sp.get("dc") else 0.0
        rf_w = float(ov.get("rf.power", sp.get("rf") or 0)) if sp.get("rf") else 0.0
        rfp_w = float(ov.get("rfpulse.power", sp.get("rfp") or 0)) if sp.get("rfp") else 0.0
        fhz = ov.get("rfpulse.freq_hz", sp.get("freq_hz")) if rfp_w else None
        duty = ov.get("rfpulse.duty", sp.get("duty")) if rfp_w else None
        pw = cur.extra
        if not cur.issued:
            cur.issued = True
            if not (dc_w or rf_w or rfp_w):
                return "next"
            pw.update(timeout=sp.get("timeout") or self.power_wait_timeout(dc_w, rf_w, rfp_w), rfp=None)
            self.power = pw
            nt = _NextTick(self._next_tick_mono)
            p = self.params
            if dc_w > 0:
                self.cur_vals["dc.power"] = dc_w
                if self.dc_on and self.dcc.state != DcControl.IDLE:
                    self.dcc.retarget(dc_w)
                else:
                    self.dc_on = True
                    self.dcc.start(dc_w, tick=nt)
            if rf_w > 0:
                self.cur_vals["rf.power"] = rf_w
                if self.rf_on and self.rfc.state != RfControl.IDLE:
                    self.rfc.retarget(rf_w)
                else:
                    self.rf_on = True
                    self.rfc.start(rf_w, float(p.get("rf_offset", self.cfg["RF_CAL_OFFSET"]) or 0.0),
                                   float(p.get("rf_param", self.cfg["RF_CAL_GAIN"]) or 1.0))
            if rfp_w > 0:
                self.rfp_on, self.rfp_target = True, rfp_w
                self.cur_vals.update({"rfpulse.power": rfp_w, "rfpulse.freq_hz": fhz, "rfpulse.duty": duty})
                def done(name, reason, results):
                    pw["rfp"] = reason is None
                    self.rfp_started = reason is None
                    if self.safety is not None:
                        self.safety.on_rfp_start_done(name, reason, results)
                self.rfp.start(rfp_w, freq_hz=fhz, duty=duty, on_done=done, tick=nt)
            if s.action == "POWER_START":
                return "next"
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
