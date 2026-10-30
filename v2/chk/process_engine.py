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
- 남긴 출력(left): 정상 끝 정지에서 뺀 단계 때문에 실제로 켜진 채 끝난 것(넘긴 히터·유량·차단밸브·셔터·MFC 밸브·필라멘트) —
  OUTPUTS_LEFT_ON(info). 히터 단계를 빼면 HeaterEngine 소유를 manual 로 넘긴다(HEATER_HANDOVER). 공정 밖 STOP 은 남긴 것 중
  지금도 켜진 것만 고정 정지 순서로 정리(OUTPUTS_CLEANUP), 없으면 아무것도 하지 않고 성공(v1). 공정 중 STOP·이상 중단·끝 정지의
  정지 순서도 남긴 것(아직 그대로인 것)을 함께 끈다. ALL STOP 은 기록도 지운다.
  정상 끝 정지(STOPPING) 중에 STOP·이상 중단이 오면 남김을 취소한다 — 그 정지 순서가 끝나면 뺀 단계를 표 순서로 이어서
  실행하고(stop_rest, 조건은 STOP 과 같게 — 히터는 process_off 로 소유 해제) run_end. HEATER_HANDOVER·OUTPUTS_LEFT_ON 은 없다.
- 사용 장비(uses_*)는 블록에서(blocks.uses_of). 안전 규칙의 적용 구간은 장비 상태(dc_ramping·rf_ramping)와 블록 감시(watching).

공정 중 값 수정(adjust, 5-4 ③ — PROCESS_ADJUST): "이번 공정의 그 값"을 바꾼다. 값 바꾸기는 여기 한 곳에서
검사(공정 중·범위·이번 공정에서 쓰는 항목) → 드라이버 쓰기(지금 쓰는 중이면 — 되읽기 확인 포함) → 판정 기준 갱신
(overrides·cur_vals → 아직 안 온 블록이 새 값, 이탈 판정 지속 시간 처음부터, 파워는 다시 도달할 때까지 이탈 판정 쉼) → 기록
(ProcEvent "adjust" PROCESS_ADJUST: 장비·항목·이전 → 새 값·출처·결과(rejected/scheduled/applied → confirmed/failed)·사유).
지금 값(adjust_state → state.adjust): 항목마다 이번 공정이 지금 쓰는 값·레시피 원래 값·마지막 수정(by·시각)·scope —
adjust 와 같은 기준(공정 중 = 진행 상태·공정 행, 지금 쓰는 중 = _adjust_apply 가 바로 쓰는 조건, 안 씀 = ADJUST_NOT_USED).
공정 중 건너뛰기·일시정지·순서 고치기(5-15 — PROCESS_SKIP·PROCESS_PAUSE·PROCESS_RESUME·PROCESS_EDIT): 지금 행만, 판단은 여기.
- 규칙은 블록 정의(blocks.BlockDef skip·pause, blocks.control) — 지금 블록 번호(no)·판 번호(revision)가 지금과 다르면 *_STALE.
  정지 순서 중에는 셋 다 거부(STATE_NOT_ALLOWED — 명령 표의 허용 상태). 딜레이 행은 대기와 같은 규칙(no = "delay", 판 0).
- 건너뛰기: 지금 블록의 남은 스텝을 버리고 다음 블록으로(블록 결과 skipped). 정리는 kind 대로 — main 은 Main 셔터 닫음,
  base 는 이 공정이 켠 필라멘트 끔, settle 은 '정착 확인 없이 진행' 경고, cmd·wait 는 보낸 명령·쓴 값 그대로. 멈춘 블록도 건너뛴다.
- 일시정지: 스텝 실행기를 tick 하지 않는다(판정·시간 한도가 멈춤 — 상태 이름·안전 규칙·STOP·ALL STOP·공정 중 수정은 그대로).
  main 은 Main 셔터를 닫고, 재개 때 다시 연다. 재개 때 지금 스텝의 시작 시각을 멈춘 만큼 미루고 안정 판정을 처음부터.
  PROCESS_PAUSE_MAX_SEC(0 = 한도 없음) — 5분 전 경고(PAUSE_LONG), 넘으면 정지 순서(PAUSE_TIMEOUT).
- 순서 고치기: 새 블록 전체 목록 — 시작한 블록(진행 중인 반복은 통째로)과 그 앞은 그대로여야 하고(EDIT_TOUCHES_STARTED),
  적재 때와 같은 검사(check_blocks·꺼진 장비·연결) 뒤 판 +1, 지금 블록은 끊지 않고 남은 부분을 새 목록으로 다시 펼친다.
  공정 중 수정 값은 남은 스텝이 쓰는 항목이면 지우고(새 목록 값), 지금 쓰는 값은 그대로.
- 기록: 블록마다 실행 결과(exec — 번호·종류·이름표·시작·끝·done/skipped/aborted·멈춘 초·실행 판), run_end summary 에 실행 순서·
  마지막 판·건너뛴/멈춘 횟수. 사건 skip·pause·resume·edit(PROCESS_SKIP·PROCESS_PAUSE·PROCESS_RESUME·PROCESS_EDIT).
"""
from __future__ import annotations

import collections
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.judge import Persist
from v2.chk import blocks as BK
from v2.chk.heater_cascade import SettleTracker, mmss
from v2.chk import process_states as PS
from v2.chk import recipe as RC
from v2.chk import safety_rules as SR
from v2.chk.power_control import DcControl, RfControl
from v2.chk.process_steps import (HEATER_RAMP_SETTLE_S, HEATER_SET_SETTLE_S, POWER_STOP_SETTLE_S, ProcessStep,
                                  StepEvent, StepRunner, base_pressure_of, heater_used,
                                  row_to_params)

ACT = frozenset(PS.ACTIVE_STATES)
# state.adjust scope — live 지금 블록이 쓰는 중(수정하면 바로) · future 앞으로 올 블록이 씀(그 블록이 새 값으로) ·
# done 쓰던 블록이 지났고 남은 블록도 안 씀(수정은 받지만 쓸 블록 없음) · unused 이번 공정에서 안 씀(ADJUST_NOT_USED)
ADJUST_SCOPES = ("live", "future", "done", "unused")
_POWER_SPEC = {"dc.power": ("dc", "dc"), "rf.power": ("rf", "rf"), "rfpulse.power": ("rfp", "rfp"),
               "rfpulse.freq_hz": ("rfp", "freq_hz"), "rfpulse.duty": ("rfp", "duty")}
DC_EST_VOLTAGE_V = 300.0          # v1 process_controller.DC_EST_VOLTAGE_V
RFPULSE_EST_SEC = 20.0            # v1 RFPULSE_EST_SEC
EVENTS_KEEP = 20000               # 최근 기록(길이 제한 링 버퍼 — 운전 중 끝없이 쌓이지 않게)


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
        self.events: "collections.deque[ProcEvent]" = collections.deque(maxlen=EVENTS_KEEP)
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
        self.adjusted: Dict[str, Any] = {}      # 받아들인 공정 중 수정 — 항목 키 → 마지막 {by, t, source}
        self.flow_on = {1: False, 2: False}
        self.rfp_target: Optional[float] = None
        self.rfp_started = False                # RF Pulse START 가 끝났다(공정 중 수정은 그 뒤에만)
        self.settle: Dict[str, float] = {}      # 공정 중 수정 뒤 새 목표에 도달할 때까지 이탈 판정 쉼(장비 → 시작 시각)
        self.left: Dict[str, Any] = {}          # 남긴 출력(행이 끝난 뒤 — 항목 → 기록 값)
        self.end_skip: tuple = ()               # 이번 정상 끝 정지에서 뺀 단계
        self.cleaning = False                   # 공정 밖 STOP 정리 중
        self.reject_detail = ""                 # 마지막 적재·시작·수정 거부의 이유 문구(첫 오류 — core 가 cmd_result.detail 로)
        self.last_view = None
        self._end_stop_resume = False
        # 셔터 전 히터 정착(HEATER_SETTLE, v1 3328872) — 승온 대기 통과 뒤부터 TC1 범위 안 연속 시간을 센다(행이 끝나면 지움)
        self.settle_track: Optional[SettleTracker] = None
        # 건너뛰기·일시정지·순서 고치기(5-15)
        self.revision = 0                       # 지금 행의 판 번호(행 시작 1, 고칠 때마다 +1)
        self.paused: Optional[Dict[str, Any]] = None
        self.exec_log: List[Dict[str, Any]] = []   # 블록 실행 결과(행마다)
        self.skips = self.pauses = 0
        self.paused_total = 0.0
        self.block_paused = 0.0                 # 지금 블록이 멈춘 초 합
        self.block_wall0 = 0.0
        self.block_rev = 0
        self.blocks_version = 0                 # 블록 목록·상태가 바뀔 때마다 +1(state.run.blocks 를 다시 싣는 기준)
        self._delay_rec: Optional[Dict[str, Any]] = None

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

    def note_plc_manual(self, target: str, on: bool, source: str, by: Optional[str] = None) -> None:
        """공정 중 PLC 버튼 실행 기록(PLC_MANUAL_IN_PROCESS, info) — 대상·켬/끔·출처·요청자·시각(사용자 결정 2026-10-20)."""
        self._emit(ProcEvent("plc_manual", f"{target} {'ON' if on else 'OFF'} ({source}{', ' + by if by else ''})",
                             "PLC_MANUAL_IN_PROCESS",
                             {"target": target, "on": bool(on), "source": source, "by": by, "t": self.clock.wall(),
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
                "proc.remain_s": self.remain_s(),
                "proc.paused": self.paused is not None if self.state in ACT else None,
                "proc.revision": self.revision if (self.row_is_process and self.state in ACT) else None}

    def remain_s(self) -> Optional[int]:
        """지금 대기 스텝(DELAY·조건 대기·압력 대기)의 남은 초 — 화면용. 딜레이 행이면 그 남은 시간."""
        now = self.paused["t0"] if self.paused is not None else self.clock.monotonic()   # 멈춘 동안은 멈춘 값
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
            self.reject_detail = ""
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

    def _reject(self, code: str, detail: str) -> tuple:
        self.reject_detail = detail[:300]
        return None, code

    def _check_rows(self, rows) -> tuple:
        if not isinstance(rows, list) or not rows:
            return self._reject("RECIPE_INVALID", "행 목록이 비어 있음")
        out = []
        for i, r in enumerate(rows, 1):
            if not isinstance(r, dict):
                return self._reject("RECIPE_INVALID", f"{i}행: 객체가 아님")
            if not any(v not in (None, "", False) for v in r.values()):
                continue                                # 빈 행(v1 row_has_content)
            ok, err = self._check_row(r)
            if err:
                self.reject_detail = f"{i}행: {self.reject_detail}"[:300] if self.reject_detail else ""
                return None, err
            out.append(ok)
        if not out:
            return self._reject("RECIPE_INVALID", "내용 있는 행이 없음")
        return out, None

    def _check_row(self, r: Dict[str, Any]) -> tuple:
        """행 하나 → (정리된 행, 사유). 블록 행 {"process_name"(선택), "blocks": [...]} 또는 v1 행·딜레이 행."""
        if "blocks" in r:
            if set(r) - {"process_name", "blocks"} or not isinstance(r.get("process_name", ""), (str, type(None))):
                return self._reject("RECIPE_INVALID", "블록 행은 process_name(문자열)·blocks 만")
            bl, errs, warns = BK.check_blocks(r["blocks"], self.cfg)
            if errs:
                self._emit(ProcEvent("reject", "; ".join(errs[:5]), _row_error_code(errs), {"errors": errs}))
                return self._reject(_row_error_code(errs), errs[0])
            return {"process_name": r.get("process_name") or "", "blocks": bl}, None
        ok, errs, _ = RC.check_process_row(r, self.cfg)
        if errs:
            return self._reject(_row_error_code(errs), errs[0])
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

    def start_manual(self, row: Dict[str, Any], name: str = "") -> Optional[str]:
        """수동 시작. run 이름 = name → 행의 process_name → "Single CHK"(v1 기본)."""
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            if self.cleaning:
                return "CLEANUP_RUNNING"
            if self.rows:
                return "RECIPE_LOADED"
            self.reject_detail = ""
            row = dict(row or {})
            if "blocks" not in row:                     # v1 수동 공정은 히터를 쓰지 않는다(히터 열 무시)
                row = {k: v for k, v in row.items() if k not in ("use_heater", "heater_temp", "heater_ramp")}
            ok, why = self._check_row(row)
            if why:
                return why
            if ok.get("delay_s") is not None:
                return self._reject("RECIPE_INVALID", "수동 시작은 딜레이 행을 받지 않음")[1]
            u = self._row_uses(ok)
            if u["heater"] and self.heater.recipe_running:
                return "HEATER_RECIPE_CONFLICT"
            why = self._device_guard(u) or self._plc_guard()
            if why:
                return why
            self.list_active, self.csv_index = False, -1
            self._start_row(ok, name.strip() or str(ok.get("process_name") or "").strip() or "Single CHK")
            return None

    def start_recipe(self) -> Optional[str]:
        with self._lock:
            if self.state in ACT or self.state == "STOPPING":
                return "PROCESS_RUNNING"
            if self.cleaning:
                return "CLEANUP_RUNNING"
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
        """사용자 STOP — 지금 행 중단 + 리스트 취소(v1 ProcessService.stop). 공정 밖이면 지난 공정이 남긴 출력만 정리
        (남은 게 없으면 아무것도 하지 않음 — v1)."""
        with self._lock:
            if self.state in ("CSV_DELAY", "CSV_BETWEEN"):
                self._cancel_list("USER_STOP")
                self._cleanup_left()
                return
            if self.state == "STOPPING":
                self.late_stop("aborted", "USER_STOP")
                return
            if self.state in ACT and self.safety is not None:
                self.safety.stop()
                return
            self._cleanup_left()

    # ── 남긴 출력 ──
    def left_on(self, item: str) -> bool:
        """남긴 출력이 지금도 켜져 있나(지난 공정이 남긴 것이고 그대로). 가스는 히터 가스가 MFC 를 잡으면 제외."""
        if item not in self.left:
            return False
        h = self.heater
        if item == "heater":
            return bool(getattr(h, "left_on", lambda: False)())
        if item in ("ar", "o2", "mfc_valve", "ar_valve", "o2_valve") and h.atm.is_active():
            return False
        if item == "filament":
            last = getattr(self.ig, "last", None)
            return bool(last is not None and getattr(last, "filament", False))
        if item in ("ms", "s1", "s2", "ar_valve", "o2_valve"):
            last = self.plc.last
            return bool(last is not None and last.values.get(f"plc.{item}"))
        if item in ("ar", "o2"):
            v = self.last_view
            x = v.get(f"mfc.{item}_sccm") if v is not None else None
            return x is None or float(x) > 0.1                 # 값을 모르면 켜진 것으로(끄는 쪽)
        return True                                             # mfc_valve — 배기를 하지 않았다(위치는 안 본다)

    def _record_left(self) -> None:
        """정상 끝 정지 뒤 — 뺀 단계 때문에 실제로 켜진 채 남은 것. 히터 단계를 뺐으면 소유를 넘긴다."""
        sk, left = set(self.end_skip), {}
        last = self.plc.last.values if self.plc.last is not None else {}
        ig = getattr(self.ig, "last", None)
        if 1 in sk and ig is not None and getattr(ig, "filament", False):
            left["filament"] = True
        if 2 in sk and last.get("plc.ms"):
            left["ms"] = True
        if 3 in sk:
            tgt = self.heater.handover() if self.heater.owner == "process" else None
            if tgt is not None:
                self._emit(ProcEvent("heater_handover", f"히터를 켠 채 넘김 — 목표 {tgt} °C(웹 히터 카드로 조작)",
                                     "HEATER_HANDOVER", {"target": tgt, "level": "info"}))
            if self.heater.left_on():
                left["heater"] = {"target": tgt if tgt is not None else self.heater.st.get("sv")}
        if 9 in sk:
            for ch, g in ((1, "ar"), (2, "o2")):
                if self.flow_on[ch]:
                    left[g] = self.cur_vals.get(f"mfc.{g}_flow")
        if 10 in sk and (self.uses["ar"] or self.uses["o2"]):
            left["mfc_valve"] = True
        if 11 in sk:
            left.update({k: True for k in ("s1", "s2") if last.get(f"plc.{k}")})
        if 12 in sk:
            left.update({k: True for k in ("ar_valve", "o2_valve") if last.get(f"plc.{k}")})
        self.left = left
        if left:
            self._emit(ProcEvent("warn", f"정상 끝 정지에서 뺀 단계로 남긴 출력: {', '.join(left)}", "OUTPUTS_LEFT_ON",
                                 {"outputs": list(left), "left": dict(left), "skip": sorted(sk), "level": "info"}))

    def _cleanup_left(self) -> bool:
        """공정 밖 STOP — 남긴 것 중 지금도 켜진 것만 고정 정지 순서로. 정리 중 STOP 은 무시. 정리했으면 True."""
        if self.cleaning:
            return False
        items = [k for k in self.left if self.left_on(k)]
        if not items:
            self.left = {}
            return False
        if self.safety is None:
            return False
        self.cleaning = True
        self._emit(ProcEvent("cleanup_start", f"지난 공정이 남긴 출력 정리: {', '.join(items)}", "OUTPUTS_CLEANUP",
                             {"items": items, "level": "info"}))
        self.safety.cleanup(items)
        return True

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

    def _halt(self, cancel_runner: bool = True, heater: bool = True) -> None:
        if self.runner is not None and cancel_runner:
            self.runner.cancel()
        self.dcc.stop()
        self.rfc.stop()
        self.settle_track = None
        if self.paused is not None:
            self._end_pause(self.clock.monotonic(), resumed=False)
        if heater and self.uses_heater:
            self.heater.process_off()

    def on_stop_done(self, kind: str) -> None:
        with self._lock:
            if kind == "cleanup":
                res = self.safety.engine.seq_results.get("cleanup", []) if self.safety is not None else []
                self._emit(ProcEvent("cleanup", "남긴 출력 정리 끝 — " + ", ".join(f"{n}:{st}" for n, _, st in res),
                                     "OUTPUTS_CLEANUP", {"steps": [{"no": n, "act": a, "status": st} for n, a, st in res],
                                                         "items": list(self.left), "level": "info"}))
                self.left, self.cleaning = {}, False
                return
            if kind == "all_stop":
                self.left = {}                                    # ALL STOP 은 전부 끈다 — 남긴 기록도 지움
                self.heater.drop_handover()
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
            drop = sorted(set(self.end_skip) - set(SR.END_STOP_MUST_KEEP))
            if (kind == "row_end" and self.end_kind in ("aborted", "error") and drop and self.end_code != "ALL_STOP"
                    and self.safety is not None):
                # 정상 끝 정지 중 STOP·이상 중단 — 남김 취소: 뺀 단계를 표 순서로 이어서 실행한 뒤 run_end(5-6 ①)
                self.end_skip = ()
                self._emit(ProcEvent("stop_rest", f"정지 순서 중 {self.end_code} — 뺀 단계 {drop} 이어서 실행",
                                     self.end_code, {"steps": drop, "level": "info"}))
                self.safety.stop_rest(drop)
                return
            if kind == "row_end" and self.end_kind is None:
                self.end_kind = "done"
            kind_, code = self.end_kind or "error", self.end_code
            if kind == "row_end" and kind_ == "done" and self.end_skip:
                self._record_left()                                # 뺀 단계로 남은 것(히터는 소유 넘김)
            else:
                self.left = {}                                     # 정지 순서 전체 — 남긴 것도 함께 꺼졌다
            self.end_skip = ()
            self._block_end("ok" if kind_ == "done" else kind_, code)
            self._emit(ProcEvent("run_end", kind_, code, {"name": self.run_name, "status": kind_, "reason": code,
                                                          "summary": self.run_summary()}))
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

    def program_exit(self) -> bool:
        """프로그램 강제 종료(콘솔 창 닫기 등, app.force_exit) — 공정 중이면 run_end error PROGRAM_EXIT 기록.
        app 은 이것을 먼저 부르고(다음 블록이 파워를 다시 켜지 않게) 그다음 파워 출력 끄기를 보낸다
        (결정 4 — PLC 릴레이는 그대로이므로 정지 순서는 내지 않는다)."""
        with self._lock:
            if not self.row_is_process or not (self.state in ACT or self.state == "STOPPING"):
                return False
            self.end_kind, self.end_code = "error", "PROGRAM_EXIT"
            if self.runner is not None:
                self.runner.cancel()
            self.dcc.stop()
            self.rfc.stop()
            self._block_end("error", "PROGRAM_EXIT")
            self._emit(ProcEvent("run_end", "error", "PROGRAM_EXIT",
                                 {"name": self.run_name, "status": "error", "reason": "PROGRAM_EXIT",
                                  "summary": self.run_summary()}))
            self.row_is_process = False
            if self.list_active:
                self._emit(ProcEvent("list_cancel", self.recipe_name, "PROGRAM_EXIT"))
                self.recipe_name, self.rows, self.list_active, self.csv_index = "", [], False, -1
            self._set_state("ABORTED")
            return True

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
                self._delay_rec, self.paused, self.revision = {}, None, 0
                self.blocks_version += 1
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
        self.block_idx, self.overrides, self.cur_vals, self.adjusted = -1, {}, {}, {}
        self.flow_on, self.rfp_target, self._end_stop_resume = {1: False, 2: False}, None, False
        self.rfp_started, self.settle, self.end_skip = False, {}, ()
        self.end_kind = self.end_code = None
        self.row_is_process, self.comm_aborted, self.filament_by_process = True, False, False
        self.dc_on = self.rf_on = self.rfp_on = False
        self.power, self.base = {}, {}
        self.settle_track = None
        self.revision, self.paused, self.exec_log = 1, None, []
        self.skips = self.pauses = 0
        self.paused_total = self.block_paused = 0.0
        self.blocks_version += 1
        h = {"DC_POWER_STOP": self._h_dc_stop, "RF_POWER_STOP": self._h_rf_stop, "RF_PULSE_STOP": self._h_rfp_stop,
             "HEATER_RAMP": self._h_heater_ramp, "HEATER_SET": self._h_heater_set, "HEATER_WAIT": self._h_heater_wait,
             "IG_FILAMENT_ON": self._h_ig_on, "IG_BASE_WAIT": self._h_ig_wait, "IG_FILAMENT_OFF": self._h_ig_off,
             "POWER_WAIT": self._h_power_wait, "POWER_START": self._h_power_wait, "RF_RAMP_OFF": self._h_rf_ramp_off,
             "HEATER_OFF": self._h_heater_off, "END_STOP": self._h_end_stop, "HEATER_SETTLE": self._h_heater_settle}
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
        self.block_wall0, self.block_paused, self.block_rev = self.clock.wall(), 0.0, self.revision
        self.blocks_version += 1
        b = self.block
        if b is not None:
            self._emit(ProcEvent("block", f"{b.no} {b.type} {b.label}".strip(), data=dict(
                b.record(), phase="start", index=b.index + 1, total=len(self.insts), t=self.clock.wall(), run_id=self.run_id)))

    def _block_end(self, result: str, code: Optional[str] = None) -> None:
        b = self.block
        if b is None or b.ended:
            return
        b.ended = True
        self.exec_log.append({"no": b.no, "type": b.type, "label": b.label, "start": self.block_wall0, "end": self.clock.wall(),
                              "result": {"ok": "done", "skipped": "skipped"}.get(result, "aborted"),
                              "pausedSec": round(self.block_paused, 1), "revision": self.block_rev})
        self.blocks_version += 1
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
            self.last_view = view
            if self._settle_pending and self.state in ("DONE", "ABORTED"):
                self._settle_pending = False
                self._set_state("LOADED" if self.rows else "IDLE")
                return
            if self.state == "CSV_DELAY":
                if self.paused is not None:
                    self._pause_tick(now)
                elif self._delay_rec is not None and self._delay_rec.get("skip"):
                    self._delay_rec = None
                    self._next_row()
                elif now - self.delay_t0 >= self.delay_s - 1e-9:
                    self._next_row()
                return
            if self.state == "CSV_BETWEEN":
                self._next_row()
                return
            if self.state not in ACT or self.runner is None:
                return
            self.rfc.tick(view.get("rf.fwd_w"), view.get("rf.ref_w"))
            self._settle_feed(view)
            self._settle_tick(view, now)
            if self.paused is not None:
                self._pause_tick(now)                   # 멈춘 동안은 스텝 실행기를 돌리지 않는다(판정·시간 한도 멈춤)
                return
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

    # ── 건너뛰기·일시정지·순서 고치기(5-15) ──
    def _top(self, inst: BK.BlockInst) -> int:
        """블록 번호("3"·"3.2#1") → 맨 위 목록 순번(0부터)."""
        return int(str(inst.no).split(".")[0].split("#")[0]) - 1

    def _ctl_check(self, no: Any, revision: Any, stale: str) -> Optional[str]:
        """공통 거부 — 공정 중·정지 순서 아님·지금 블록·판. 딜레이 행은 no "delay"·판 0."""
        if self.state not in ACT or self.state == "CSV_BETWEEN":
            return "PROCESS_NOT_RUNNING"
        if self.safety is not None and self.safety.engine.stopping is not None:
            return "STATE_NOT_ALLOWED"
        if self.state == "CSV_DELAY":
            if str(no) != BK.DELAY_NO or revision != 0:
                self.reject_detail = f"지금은 딜레이 행(번호 {BK.DELAY_NO}, 판 0)"
                return stale
            return None
        b = self.block
        if not self.row_is_process or self.runner is None or b is None:
            return "PROCESS_NOT_RUNNING"
        if revision != self.revision or str(no) != str(b.no):
            self.reject_detail = f"지금 블록 {b.no}, 판 {self.revision}"
            return stale
        return None

    def _ctl_event(self, kind: str, code: str, text: str, data: Dict[str, Any]) -> None:
        self.blocks_version += 1
        self._emit(ProcEvent(kind, text, code, dict(data, run_id=self.run_id or None, revision=self.revision,
                                                    t=self.clock.wall(), level="info")))

    def skip(self, no: Any, revision: Any, by: Optional[str] = None, source: str = "web") -> Optional[str]:
        """PROCESS_SKIP — 지금 블록을 끝내고 다음으로. 돌려주는 값은 거부 사유 코드 또는 None."""
        with self._lock:
            self.reject_detail = ""
            why = self._ctl_check(no, revision, "SKIP_STALE")
            if why:
                return why
            now = self.clock.monotonic()
            if self.state == "CSV_DELAY":
                was = self._end_pause(now, resumed=False) if self.paused is not None else 0.0
                self.skips += 1
                self._delay_rec = {"skip": True}
                self._ctl_event("skip", "PROCESS_SKIP", f"딜레이 행 건너뜀 ({by or source})",
                                {"by": by, "source": source, "no": BK.DELAY_NO, "type": "delay", "label": "",
                                 "kind": "wait", "cleaned": [], "pausedSec": round(was, 1)})
                return None
            b, r = self.block, self.runner
            ok, kind, text = BK.control(b.type, b.params, "skip")
            if not ok:
                self.reject_detail = text
                return "SKIP_NOT_ALLOWED"
            was = self._end_pause(now, resumed=False) if self.paused is not None else 0.0
            cleaned = self._skip_cleanup(b, kind)
            self.skips += 1
            self._ctl_event("skip", "PROCESS_SKIP", f"블록 {b.no} {b.type} 건너뜀 ({by or source})",
                            {"by": by, "source": source, "no": b.no, "type": b.type, "label": b.label, "kind": kind,
                             "cleaned": cleaned, "pausedSec": round(was, 1)})
            if kind == "settle":
                self._emit(ProcEvent("warn", "셔터 전 히터 정착 확인 없이 진행(사용자 건너뛰기)", "HEATER_SETTLE_NOT_CHECKED",
                                     {"by": by, "level": "warn"}))
            self._block_end("skipped")
            i = r.idx + 1
            while i < len(r.steps) and r.steps[i].block == b.index:
                i += 1
            r.skip_to(i, now)
            return None

    def _skip_cleanup(self, b: BK.BlockInst, kind: str) -> List[str]:
        r = self.runner
        s, cur = r.step, r.cur
        out: List[str] = []
        if kind == "main":
            first = next(i for i, x in enumerate(r.steps) if x.block == b.index)
            opened = r.idx > first or (cur is not None and cur.issued)
            if opened:
                self.plc.set_button("ms", False)
                out.append("Main 셔터 닫음")
        elif kind == "base":
            last = getattr(self.ig, "last", None)
            if self.filament_by_process and last is not None and getattr(last, "filament", False):
                self.ig.filament_off()
                out.append("이 공정이 켠 필라멘트 끔")
            self.filament_by_process = False
        elif kind == "settle":
            self.settle_track = None
        elif kind == "cmd" and s is not None and cur is not None and cur.issued:
            if s.action == "MFC_CMD" and s.params[0] in ("FLOW_ON", "FLOW_OFF"):
                self._on_step_done(s)                      # 보낸 FLOW ON 은 그대로 — 유량 이탈 판정 기준도
            out.append(f"보낸 명령 그대로({s.message})")
        return out

    def pause(self, no: Any, revision: Any, by: Optional[str] = None, source: str = "web") -> Optional[str]:
        """PROCESS_PAUSE — 지금 블록 멈춤."""
        with self._lock:
            self.reject_detail = ""
            why = self._ctl_check(no, revision, "PAUSE_STALE")
            if why:
                return why
            if self.paused is not None:
                return "PROCESS_PAUSED"
            now = self.clock.monotonic()
            if self.state == "CSV_DELAY":
                b_no, b_type, b_label, kind = BK.DELAY_NO, "delay", "", "timer"
            else:
                b = self.block
                ok, kind, text = BK.control(b.type, b.params, "pause")
                if not ok:
                    self.reject_detail = text
                    return "PAUSE_NOT_ALLOWED"
                s = self.runner.step
                if kind == "judge" and b.type == "power_on" and (s is None or s.action != "POWER_WAIT"):
                    self.reject_detail = "도달 대기가 없는 파워 켜기는 멈출 수 없음"
                    return "PAUSE_NOT_ALLOWED"
                b_no, b_type, b_label = b.no, b.type, b.label
            remain = self.remain_s()
            self.paused = {"t0": now, "at": self.clock.wall(), "no": b_no, "type": b_type, "label": b_label, "kind": kind,
                           "by": by, "source": source, "remainS": remain, "warned": False, "msClosed": False}
            if kind == "main":
                self.plc.set_button("ms", False)
                self.paused["msClosed"] = True
            self.pauses += 1
            self._ctl_event("pause", "PROCESS_PAUSE", f"블록 {b_no} 일시정지 ({by or source})",
                            {"by": by, "source": source, "no": b_no, "type": b_type, "label": b_label, "kind": kind,
                             "remainS": remain, "cleaned": ["Main 셔터 닫음"] if kind == "main" else []})
            return None

    def resume(self, revision: Any, by: Optional[str] = None, source: str = "web") -> Optional[str]:
        """PROCESS_RESUME — 멈춘 블록 이어서."""
        with self._lock:
            self.reject_detail = ""
            if self.paused is None:
                return "PROCESS_NOT_PAUSED"
            if revision != (0 if self.state == "CSV_DELAY" else self.revision):
                self.reject_detail = f"지금 판 {0 if self.state == 'CSV_DELAY' else self.revision}"
                return "PAUSE_STALE"
            p = dict(self.paused)
            dur = self._end_pause(self.clock.monotonic(), resumed=True)
            self._ctl_event("resume", "PROCESS_RESUME", f"블록 {p['no']} 재개 — {dur:.0f} s 멈춤 ({by or source})",
                            {"by": by, "source": source, "no": p["no"], "type": p["type"], "label": p["label"],
                             "kind": p["kind"], "pausedSec": round(dur, 1),
                             "cleaned": ["Main 셔터 다시 엶"] if p["msClosed"] else []})
            return None

    def _end_pause(self, now: float, resumed: bool) -> float:
        """멈춤 끝 — 멈춘 초를 기록하고, 지금 스텝의 시작 시각을 그만큼 미루고 안정 판정을 처음부터(재개)."""
        p, self.paused = self.paused, None
        if p is None:
            return 0.0
        dur = max(0.0, now - p["t0"])
        self.paused_total += dur
        self.block_paused += dur
        self.blocks_version += 1
        if self.state == "CSV_DELAY":
            self.delay_t0 += dur
            return dur
        r = self.runner
        cur, s = (r.cur, r.step) if r is not None else (None, None)
        if cur is not None:
            cur.t0 += dur
            if cur.persist is not None:
                cur.persist.reset()
            if "stab_t0" in cur.extra:
                cur.extra["stab_t0"] += dur
        if s is not None and s.action == "HEATER_WAIT":
            hp = self.heater.proc
            if hp.get("t0") is not None:
                hp["t0"] += dur
            if hp.get("phase") == "SOAK":
                hp["phase"] = "RAMP"
            if hp.get("hold_t0") is not None:
                hp["hold_t0"] += dur
        if s is not None and s.action == "HEATER_SETTLE" and self.settle_track is not None:
            self.settle_track.since = None
        if s is not None and s.action == "IG_BASE_WAIT" and self.base.get("relight_t0") is not None:
            self.base["relight_t0"] += dur
        if resumed and p.get("msClosed"):
            self.plc.set_button("ms", True)
        return dur

    def _pause_tick(self, now: float) -> None:
        """멈춘 동안 — PROCESS_PAUSE_MAX_SEC 5분 전 경고, 넘으면 정지 순서(PAUSE_TIMEOUT)."""
        p = self.paused
        mx = float(self.cfg["PROCESS_PAUSE_MAX_SEC"])
        if p is None or mx <= 0:
            return
        el = now - p["t0"]
        if not p["warned"] and el >= mx - 300.0:
            p["warned"] = True
            self._emit(ProcEvent("pause_long", f"일시정지 {el:.0f} s — {max(0.0, mx - el):.0f} s 뒤 정지 순서", "PAUSE_LONG",
                                 {"no": p["no"], "pausedSec": round(el, 1), "maxSec": mx, "level": "warn"}))
        if el >= mx:
            self._end_pause(now, resumed=False)
            if self.state == "CSV_DELAY":
                self._cancel_list("PAUSE_TIMEOUT")
            elif self.safety is not None:
                self.safety.engine.abort("PAUSE_TIMEOUT", rule="pause")

    def edit(self, revision: Any, blocks: Any, by: Optional[str] = None, source: str = "web") -> Optional[str]:
        """PROCESS_EDIT — 지금 행의 새 블록 전체 목록. 시작한 블록과 그 앞은 그대로, 나머지를 바꾼다."""
        with self._lock:
            self.reject_detail = ""
            if self.state not in ACT or self.state in ("CSV_BETWEEN", "CSV_DELAY") or not self.row_is_process \
                    or self.runner is None:
                return "PROCESS_NOT_RUNNING"
            if self.safety is not None and self.safety.engine.stopping is not None:
                return "STATE_NOT_ALLOWED"
            if revision != self.revision:
                self.reject_detail = f"지금 판 {self.revision}"
                return "EDIT_STALE"
            bl, errs, warns = BK.check_blocks(blocks, self.cfg)
            if errs:
                self.reject_detail = errs[0][:300]
                return _row_error_code(errs)
            b = self.block
            k = self._top(b) if b is not None else -1
            if bl[:k + 1] != self.blocks[:k + 1]:
                self.reject_detail = f"시작했거나 끝난 블록(맨 위 1~{k + 1}번)과 그 앞 순서는 그대로여야 함"
                return "EDIT_TOUCHES_STARTED"
            u = BK.uses_of(bl)
            why = self._device_guard(u)
            if why:
                return why
            if u["heater"] and not self.uses_heater and self.heater.recipe_running:
                return "HEATER_RECIPE_CONFLICT"
            ig_en, rfp_en = bool(self.cfg["IG_ENABLED"]), bool(self.cfg["RFPULSE_ENABLED"])
            steps, insts = BK.expand(bl, self.cfg, ig_enabled=ig_en, rfpulse_present=rfp_en)
            n = sum(1 for x in self.insts if self._top(x) <= k)
            r = self.runner
            cut_old = next((i for i, s in enumerate(r.steps) if s.block >= n), len(r.steps))
            cut_new = next((i for i, s in enumerate(steps) if s.block >= n), len(steps))
            tail = steps[cut_new:]
            # 남은 스텝이 쓰는 항목의 공정 중 수정 값은 지운다(아직 안 온 블록은 새 목록 값) — 지금 쓰는 값(cur_vals)은 그대로
            drop = {s.ref for s in tail if s.ref}
            if any(s.action in ("POWER_WAIT", "POWER_START") for s in tail):
                drop |= {"dc.power", "rf.power", "rfpulse.power", "rfpulse.freq_hz", "rfpulse.duty"}
            if any((s.spec or {}).get("main") for s in tail):
                drop.add("process.main_remaining")
            for key in drop:
                self.overrides.pop(key, None)
            old_n = len(self.blocks)
            first = next((i for i, (x, y) in enumerate(zip(self.blocks, bl)) if x != y), min(old_n, len(bl)))
            r.replace_tail(cut_old, tail)
            self.insts = self.insts[:n] + insts[n:]
            self.blocks, self.uses = bl, u
            self.revision += 1
            summary = f"맨 위 블록 {old_n} → {len(bl)}개, {first + 1}번부터 바뀜"
            self._ctl_event("edit", "PROCESS_EDIT", f"순서 고침 판 {self.revision} — {summary} ({by or source})",
                            {"by": by, "source": source, "summary": summary, "from": first + 1, "count": len(bl),
                             "blocks": bl, "warnings": list(warns), "insts": [x.record() for x in self.insts]})
            return None

    def run_summary(self) -> Dict[str, Any]:
        """run_end summary — 실행된 순서·마지막 판·건너뛴/멈춘 횟수(웹 DB 는 이 객체를 그대로 저장)."""
        return {"executed": list(self.exec_log), "revision": self.revision, "skips": self.skips, "pauses": self.pauses,
                "pausedSec": round(self.paused_total, 1)}

    def run_state(self, full: bool) -> Optional[Dict[str, Any]]:
        """state.run — 판·지금 블록·멈춤(늘), 블록 목록은 full 일 때만(바뀌었거나 주기적으로 — hub)."""
        if self.state == "CSV_DELAY":
            out = {"revision": 0, "current": BK.DELAY_NO, "paused": self._pause_public(), "blocksVersion": self.blocks_version,
                   "delay": {"sec": self.delay_s, "remainS": self.remain_s()}}
            if full:
                ok_s, _, ts = BK.control(BK.DELAY_NO, {}, "skip")
                ok_p, _, tp = BK.control(BK.DELAY_NO, {}, "pause")
                out["blocks"] = [{"no": BK.DELAY_NO, "type": "delay", "label": "", "status": "paused" if self.paused else "running",
                                  "start": None, "end": None, "durationS": self.delay_s, "skip": ok_s, "pause": ok_p,
                                  "skipText": ts, "pauseText": tp, "pausedSec": 0.0, "revision": 0}]
            return out
        if not self.row_is_process or self.state not in ACT and self.state != "STOPPING":
            return None
        b = self.block
        out = {"revision": self.revision, "current": b.no if b is not None else None, "paused": self._pause_public(),
               "blocksVersion": self.blocks_version}
        if full:
            done = {}
            for e in self.exec_log:
                done[e["no"]] = e
            stopping = self.state == "STOPPING"
            lst = []
            for x in self.insts:
                e = done.get(x.no)
                if e is not None:
                    st = e["result"]
                elif b is not None and x.index == b.index:
                    st = "paused" if self.paused is not None else "running"
                else:
                    st = "waiting"
                ok_s, _, ts = BK.control(x.type, x.params, "skip")
                ok_p, _, tp = BK.control(x.type, x.params, "pause")
                live = st in ("running", "paused") and not stopping
                dur = x.params.get("sec") if x.type in ("wait", "main") else None
                lst.append({"no": x.no, "type": x.type, "label": x.label, "status": st,
                            "start": e["start"] if e else (self.block_wall0 if live else None), "end": e["end"] if e else None,
                            "durationS": dur, "skip": ok_s and live, "pause": ok_p and live, "skipText": ts, "pauseText": tp,
                            "pausedSec": e["pausedSec"] if e else (round(self.block_paused, 1) if live else 0.0),
                            "revision": e["revision"] if e else self.revision, "auto": x.auto})
            out["blocks"] = lst
        return out

    def _pause_public(self) -> Optional[Dict[str, Any]]:
        p = self.paused
        if p is None:
            return None
        return {"at": p["at"], "no": p["no"], "type": p["type"], "label": p["label"], "kind": p["kind"], "by": p["by"],
                "source": p["source"], "remainS": p["remainS"], "pausedS": round(self.clock.monotonic() - p["t0"], 1)}

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
                self.end_skip = skip
                # 제어 루프를 멈추고 정지 순서로(상태 STOPPING — 진행 상태 규칙 끔). 히터는 3단계가 끈다(빼면 남긴다)
                self._halt(cancel_runner=False, heater=False)
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
            nxt = next((x for x in r.steps[r.idx + 1:] if x.action == "HEATER_SETTLE"), None)
            if nxt is not None:
                self._settle_track_start(float(self.heater.proc["target"]), nxt)
            return "next"
        return res if isinstance(res, tuple) else "wait"

    # ── 셔터 전 히터 정착(HEATER_SETTLE, v1 process_controller._heater_settle) ──
    def _settle_track_start(self, target: float, s: ProcessStep) -> None:
        sp = s.spec or {}
        self.settle_track = SettleTracker(target, float(sp.get("tol", self.cfg["HEATER_SHUTTER_SETTLE_TOL_C"])),
                                          float(sp.get("sec", self.cfg["HEATER_SHUTTER_SETTLE_SEC"])), self.clock.monotonic)

    def _settle_feed(self, view) -> None:
        """승온 대기 통과 뒤 매 tick — TC1 과 캐스케이드 최종 목표(공정 목표와 같아야 연속)."""
        tr = self.settle_track
        if tr is None:
            return
        c = self.heater.casc.snapshot()
        final_ok = c.get("final") is not None and abs(float(c["final"]) - tr.target) <= float(self.cfg["HEATER_RAMP_DONE_TOL_C"])
        tr.feed(view.get("heater.pv"), final_ok=final_ok)

    @staticmethod
    def _settle_bias_text(c: dict) -> str:
        return (f"bias {float(c.get('bias') or 0.0):+.1f} (ff {float(c.get('bias_ff') or 0.0):+.1f}"
                f" / int {float(c.get('bias_int') or 0.0):+.1f})")

    @staticmethod
    def _f1(v) -> str:
        return "--.-" if v is None else f"{float(v):.1f}"

    def _h_heater_settle(self, r, s, cur, view, now):
        """메인 셔터 열기 직전 — TC1 이 목표 ±tol 안에 sec 이상 연속이면 통과(승온 대기 통과 뒤부터 센다 — 이미 맞으면 바로).
        상한 초과는 fail_action(proceed = 경고 후 진행 / abort = HEATER_SETTLE_FAILED). 이 동안 DC ±이탈 감시(블록 watch dc)·
        STOP·ALL STOP 은 셔터 딜레이와 같다."""
        sp = s.spec or {}
        tol, need, mx = float(sp["tol"]), float(sp["sec"]), float(sp["max_sec"])
        if not cur.issued:
            cur.issued = True
            c = self.heater.casc.snapshot()
            if not self.heater.cascade or not c.get("active"):
                self.settle_track = None
                self._emit(ProcEvent("warn", f"셔터 전 히터 정착 확인 생략 — 캐스케이드가 활성이 아님"
                                             f" (상태 {c.get('state') or '-'}, 공정이 히터를 소유하지 않음)",
                                     "HEATER_SETTLE_SKIPPED", {"level": "warn"}))
                return "next"
            target = float(s.value) if s.value is not None else float(c.get("final") or 0.0)
            tr = self.settle_track
            if tr is None or abs(tr.target - target) > float(self.cfg["HEATER_RAMP_DONE_TOL_C"]):
                self._settle_track_start(target, s)          # 승온 대기를 거치지 않은 경우 — 지금부터 센다
                self._settle_feed(view)
            if self.settle_track.ok():
                return self._settle_pass(0.0)
            cur.extra["last_log"] = now
            self._dev_msg("info", f"셔터 전 히터 정착 대기 시작 — 목표 {target:.1f}°C ±{tol:g}, {need:.0f}초 연속 "
                                  f"(지금 {self.settle_track.held_sec():.0f}초), 상한 {mmss(mx)}")
            return "wait"
        tr = self.settle_track
        if tr is None:
            return "next"
        if view is not None and view.get("heater.fault"):
            self.settle_track = None
            self._emit(ProcEvent("warn", "히터 이상 발생(셔터 전 정착 대기 중)", "HEATER_FAULT"))
            return ("fail", "HEATER_FAULT")
        el = now - cur.t0
        if tr.ok():
            return self._settle_pass(el)
        if el - cur.extra.get("last_log", cur.t0) >= 30.0:
            cur.extra["last_log"] = now
            self._dev_msg("info", f"셔터 전 히터 정착 대기 — TC1 {self._f1(tr.last_pv)} / 목표 {tr.target:.1f}"
                                  f" (±{tol:g}, {need:.0f}초 연속 필요, 지금 {tr.held_sec():.0f}초) · "
                                  f"{self._settle_bias_text(self.heater.casc.snapshot())}"
                                  f" · 경과 {mmss(el)} / 상한 {mmss(mx)}")
        if el < mx - 1e-9:
            return "wait"
        action = str(sp.get("fail_action") or "proceed")
        reason = (f"셔터 전 정착 미달 — TC1 {self._f1(tr.last_pv)} / 목표 {tr.target:.1f} ±{tol:g}, "
                  f"{need:.0f}초 연속 못 채움(최장 지금 {tr.held_sec():.0f}초) · 상한 {mmss(mx)} 초과")
        self.settle_track = None
        c = self.heater.casc.snapshot()
        data = {"action": action, "reason": reason, "pv": tr.last_pv, "target": float(tr.target), "tol": tol,
                "need": need, "elapsed": int(round(el)), "sv2": c.get("sv2"), "bias": c.get("bias"),
                "bias_ff": c.get("bias_ff"), "bias_int": c.get("bias_int"), "level": "warn"}
        if action == "abort":
            self._emit(ProcEvent("warn", f"{reason} — 공정 중단(HEATER_SHUTTER_SETTLE_FAIL_ACTION=abort)",
                                 "HEATER_SETTLE_FAILED", data))
            return ("fail", "HEATER_SETTLE_FAILED")
        self._emit(ProcEvent("warn", f"{reason} — 경고 후 셔터를 열고 진행(HEATER_SHUTTER_SETTLE_FAIL_ACTION=proceed)",
                             "HEATER_SETTLE_TIMEOUT", data))
        return "next"

    def _settle_pass(self, waited: float):
        tr = self.settle_track
        pv = tr.last_pv if tr is not None else None
        dev = "-" if (pv is None or tr is None) else f"{pv - tr.target:+.1f}"
        self._dev_msg("info", f"셔터 전 히터 정착 확인 — TC1 {self._f1(pv)} (목표 {tr.target:.1f}, 편차 {dev})"
                              f" · 대기 {mmss(waited)}")
        self.settle_track = None
        return "next"

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
        ai = BK.adjust_item(device, item)
        if ai is not None and result in ("scheduled", "applied") and source != "scheduled":   # 받아들인 수정(예약 값 적용은 빼고)
            self.adjusted[ai.key] = {"by": getattr(self, "_adjust_by", None), "t": self.clock.wall(), "source": source}
        self._emit(ProcEvent("adjust", f"{device}.{item} {old} → {new} ({source}) {result}{' ' + reason if reason else ''}",
                             "PROCESS_ADJUST", {"device": device, "item": item, "old": old, "new": new, "source": source,
                                                "by": getattr(self, "_adjust_by", None),
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

    def adjust(self, device: str, item: str, value: Any, source: str = "web", by: Optional[str] = None) -> Optional[str]:
        """PROCESS_ADJUST — 돌려주는 값은 거부 사유 코드 또는 None. 결과는 ProcEvent("adjust")(by = 요청자)."""
        with self._lock:
            self._adjust_by = by
            self.reject_detail = ""
            ai = BK.adjust_item(str(device), str(item))
            if ai is None:
                self.reject_detail = f"{device}.{item} — 모르는 항목"
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

    def _adjust_live(self, ai: BK.AdjustItem) -> bool:
        """지금 블록이 쓰는 중 — _adjust_apply 가 드라이버에 바로 쓰는 조건과 같다(RF Pulse 는 START 중도 포함, busy)."""
        key, r = ai.key, self.runner
        s = r.step if r is not None else None
        if key == "process.skip_wait":
            return s is not None and bool((s.spec or {}).get("skippable"))
        if key == "process.main_remaining":
            return s is not None and bool((s.spec or {}).get("main")) and r.cur is not None
        if ai.device == "mfc":
            return key in self.cur_vals
        if key == "dc.power":
            return self.dc_on and self.dcc.state != DcControl.IDLE
        if key == "rf.power":
            return self.rf_on and self.rfc.state != RfControl.IDLE
        if ai.device == "rfpulse":
            return self.rfp_on
        if ai.device == "heater":
            return self.heater is not None and bool(self.heater.process_adjust_live(ai.item))
        return False

    @staticmethod
    def _step_recipe(ai: BK.AdjustItem, s: ProcessStep):
        """(이 스텝이 그 항목을 쓰는지, 레시피 값) — 값은 항목 단위(RF Pulse 주파수 kHz)."""
        key, sp = ai.key, s.spec or {}
        if key in _POWER_SPEC:
            dev, fld = _POWER_SPEC[key]
            if s.action not in ("POWER_WAIT", "POWER_START") or not sp.get(dev):
                return False, None
            v = sp.get(fld)
            return True, (v / 1000.0 if key == "rfpulse.freq_hz" and v is not None else v)
        if key == "process.skip_wait":
            return bool(sp.get("skippable")), None
        if s.ref != key:
            return False, None
        if key == "process.main_remaining":
            return True, s.duration_sec
        if s.action == "MFC_CMD":
            a = s.params[1]
            v = a.get("value", a.get("target"))
            return v is not None, v
        return s.value is not None, s.value

    def adjust_state(self) -> Optional[list]:
        """state.adjust — 공정 중(adjust 가 받는 상태)이면 ADJUST_ITEMS 마다 {device, item, value(이번 공정이 지금 쓰는 값),
        recipe(레시피 원래 값 — 쓰는 중·지난 블록이면 그 블록, 아니면 다음 블록), adjusted(마지막 수정 {by, t, source} 또는 null),
        scope(ADJUST_SCOPES), busy(RF Pulse START 중 — 수정은 ADJUST_BUSY)}. 공정 밖이면 None."""
        with self._lock:
            r = self.runner
            if not self.row_is_process or self.state not in ACT or r is None:
                return None
            out = []
            for ai in BK.ADJUST_ITEMS:
                past = nxt = None
                for i, s in enumerate(r.steps):
                    hit, v = self._step_recipe(ai, s)
                    if not hit:
                        continue
                    if i <= r.idx:
                        past = (v,)
                    elif nxt is None:
                        nxt = (v,)
                live = self._adjust_live(ai)
                if not (ai.key == "process.skip_wait" or self._uses_item(ai)):
                    scope = "unused"
                elif live:
                    scope = "live"
                elif nxt is not None:
                    scope = "future"
                else:
                    scope = "done" if past is not None else "unused"
                recipe = ((past if live or nxt is None else nxt) or (None,))[0]
                if ai.key == "process.skip_wait":
                    value = None
                elif ai.key == "process.main_remaining":
                    value = self.remain_s() if live else self.overrides.get(ai.key, recipe)
                else:
                    value = self._adjust_old(ai) if scope != "unused" else None
                    if value is None:
                        value = recipe
                out.append({"device": ai.device, "item": ai.item, "value": value, "recipe": recipe,
                            "adjusted": dict(self.adjusted[ai.key]) if ai.key in self.adjusted else None,
                            "scope": scope, "busy": ai.device == "rfpulse" and self.rfp_on and not self.rfp_started})
            return out

    def _adjust_check(self, ai: BK.AdjustItem, value: Any) -> Optional[str]:
        if not self.row_is_process or self.state not in ACT or self.runner is None:
            return "PROCESS_NOT_RUNNING"                                  # 공정 밖·정지 순서 중·딜레이 행
        e = BK._check_param(ai.param, value, self.cfg, ())
        if e:
            self.reject_detail = str(e)[:300]
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
