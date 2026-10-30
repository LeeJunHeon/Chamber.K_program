"""챔버K 명령 처리 — 웹·로컬 화면 명령을 검사(commands.check_command + 장비 값 조건)하고 엔진에 넘긴다.

결과는 (성공, 사유 코드, 이유 문구 detail). 다음 단계(로컬 HMI·웹 연동)가 이 함수 하나를 부른다. 골든 하니스도 이것으로 v1 원격 명령을 흉내 낸다.
- PROCESS_STOP: 공정이 없으면 지난 공정이 남긴 출력만 정리(OUTPUTS_CLEANUP), 남은 게 없으면 아무것도 하지 않고 성공(v1). RECIPE_CLEAR: 적재가 없으면 성공, 공정 중이면 PROCESS_RUNNING(v1 B5).
- PLC_BUTTON: PLC 링크가 없으면 LINK_DOWN(v1 B6). 공정 중에도 모두 허용(v1, 사용자 결정 2026-10-20) — 공정 중 실행은
  공정 기록에 PLC_MANUAL_IN_PROCESS(info: 대상·켬/끔·출처·시각).
- ALL_STOP: 공정 여부와 무관 — SafetyEngine.all_stop.
- CONFIG_APPLY·CONFIG_ROLLBACK(웹만): system.ConfigApply → ConfigStore.apply/rollback. 결과는 config_ack 메시지.
- 결과는 on_result(cid, kind, 결과, 출처, 요청자) — MessageHub 가 cmd_result 메시지로(어느 화면이든 누가 무엇을 했는지 보게).
- by(요청자): 웹은 명령의 requestedBy, 로컬 화면은 "local". cmd_result·config_ack·공정 기록(공정 중 수정·PLC 버튼·run_start)에 남는다.
- 여러 스레드(로컬 서버·웹 연결·시험)가 동시에 불러도 명령은 잠금 하나로 하나씩(검사와 실행 사이에 다른 명령이 끼지 않게).
  처리는 큐에 넣기·상태 바꾸기뿐이라 짧다. ALL_STOP 은 잠금을 기다리지 않는다(SafetyEngine.all_stop 이 자기 잠금으로 안전).
- 인자는 check_command(경로·상태) 다음에 commands.check_args(명령 표 ArgDef — 형식·범위)로 — 웹·로컬 화면이 같은 길.
  틀리면 BAD_ARGS + detail(예: "value 5000 — 범위 0 ~ 700 °C (HEATER_MAX_TEMP)"). 이유 문구가 없는 결과는 detail 이 빈 문자열.
  엔진이 문구와 함께 거부하면(행 검사 RECIPE_INVALID·IG_DISABLED·DEVICE_DISABLED 의 첫 오류, 히터 가스 준비 거부, 공정 중 수정 형식·범위,
  히터 레시피 검사) 그 첫 문구도 detail 로(300자) — 엔진의 reject_detail.
- bench(장비 쓰기 금지)에서는 장비에 쓰는 명령을 BENCH_READ_ONLY 로 거부한다 — worker 도 막지만 결과가 accepted 로 보이지 않게.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common.commands import Source
from v2.chk import commands as C
from v2.chk.heater_engine import parse_recipe_steps
from v2.chk import recipe as RC


NO_DEVICE_WRITE = tuple(c.kind for c in C.COMMANDS if c.bench_ok)   # bench 에서도 받는 명령(commands.CommandDef.bench_ok)


@dataclass
class CmdResult:
    ok: bool
    reason: Optional[str] = None
    detail: str = ""                # 사람이 읽는 거부 이유(없으면 "") — cmd_result.detail·로컬 /api/v1/command


class Rejected(Exception):
    """_run 안에서 사유 코드와 이유 문구를 함께 돌려줄 때."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason, self.detail = reason, detail


class ChkCore:
    def __init__(self, cfg: Dict[str, Any], process, heater, safety, plc, ig, links: Dict[str, Callable[[], bool]],
                 on_result: Optional[Callable[..., None]] = None, config=None, writes_allowed: bool = True) -> None:
        self.cfg, self.process, self.heater, self.safety, self.plc, self.ig = cfg, process, heater, safety, plc, ig
        self.links = links
        self.on_result = on_result
        self.config = config                    # system.ConfigApply(없으면 설정 명령은 UNKNOWN_COMMAND)
        self.writes_allowed = writes_allowed
        self._lock = threading.Lock()

    def _plc_up(self) -> bool:
        return self.links.get("plc", lambda: False)()

    @staticmethod
    def default_by(source: Source, args: dict) -> str:
        if source == Source.HMI:
            return "local"
        return str(args.get("requestedBy") or args.get("by") or getattr(source, "value", source))

    def command(self, kind: str, args: Optional[dict] = None, source: Source = Source.WEB, cid: Any = None,
                by: Optional[str] = None) -> CmdResult:
        args = dict(args or {})
        by = str(by) if by else self.default_by(source, args)
        detail = ""
        try:
            if kind == "ALL_STOP":
                r = self._run(kind, args, source, by)
            else:
                with self._lock:
                    r = self._run(kind, args, source, by)
        except Rejected as e:
            r, detail = e.reason, e.detail
        except Exception:
            r = "INTERNAL_ERROR"
        res = CmdResult(r is None, r, detail)
        if self.on_result:
            try:
                self.on_result(cid, kind, res, source, by)
            except Exception:
                pass
        return res

    def _run(self, kind: str, a: dict, source: Source, by: str = "") -> Optional[str]:
        p, h = self.process, self.heater
        state = p.state
        why = C.check_command(kind, source, state, a)
        if why:
            return why
        bad = C.check_args(C.by_kind()[kind], a, self.cfg, self._sv_limit() if kind in C.SV_LIMIT_KINDS else None)
        if bad:
            raise Rejected("BAD_ARGS", bad)
        if not self.writes_allowed and kind not in NO_DEVICE_WRITE:
            return "BENCH_READ_ONLY"
        if kind == "PROCESS_STOP":
            p.stop()
            return None
        if kind == "ALL_STOP":
            self.safety.all_stop()
            return None
        if kind in ("CONFIG_APPLY", "CONFIG_ROLLBACK"):
            if self.config is None:
                return "UNKNOWN_COMMAND"
            return self.config.apply(a, by) if kind == "CONFIG_APPLY" else self.config.rollback(a, by)
        if kind == "BUZZ_STOP":
            if not self._plc_up():
                return "LINK_DOWN"
            self.plc.set_button("buzz_stop", True)
            self._note_plc_manual("buzz_stop", True, source, by)
            return None
        if kind in ("PROCESS_START", "RECIPE_START"):
            p.started_by = by                     # run_start 에 남는다(hub)
        if kind == "PROCESS_START":
            if isinstance(a.get("blocks"), list) and "row" not in a:
                return self._why(p, lambda: p.start_manual({"process_name": str(a.get("name") or ""), "blocks": a["blocks"]},
                                                           str(a.get("name") or "")))
            if not isinstance(a.get("row"), dict):
                raise Rejected("BAD_ARGS", "row 또는 blocks 하나 필요")
            return self._why(p, lambda: p.start_manual(a["row"], str(a.get("name") or "")))
        if kind == "RECIPE_LOAD":
            if isinstance(a.get("blocks"), list) and "rows" not in a:      # 블록 목록 하나 = 블록 행 하나
                return self._why(p, lambda: p.load(str(a.get("name") or ""),
                                                   [{"process_name": str(a.get("name") or ""), "blocks": a["blocks"]}]))
            if not isinstance(a.get("rows"), list):
                raise Rejected("BAD_ARGS", "rows 또는 blocks 하나 필요")
            return self._why(p, lambda: p.load(str(a.get("name") or ""), a["rows"]))
        if kind == "RECIPE_CLEAR":
            return p.clear()
        if kind == "PROCESS_ADJUST":
            src = a.get("source") or ("local" if source == Source.HMI else "web")
            return self._why(p, lambda: p.adjust(str(a.get("device") or ""), str(a.get("item") or ""), a["value"], src, by=by))
        if kind in ("PROCESS_SKIP", "PROCESS_PAUSE", "PROCESS_RESUME", "PROCESS_EDIT"):
            src = "local" if source == Source.HMI else "web"
            if kind == "PROCESS_SKIP":
                return self._why(p, lambda: p.skip(a["no"], int(a["revision"]), by=by, source=src))
            if kind == "PROCESS_PAUSE":
                return self._why(p, lambda: p.pause(a["no"], int(a["revision"]), by=by, source=src))
            if kind == "PROCESS_RESUME":
                return self._why(p, lambda: p.resume(int(a["revision"]), by=by, source=src))
            return self._why(p, lambda: p.edit(int(a["revision"]), a["blocks"], by=by, source=src))
        if kind == "RECIPE_START":
            return p.start_recipe()
        if kind == "PLC_BUTTON":
            if not self._plc_up():
                return "LINK_DOWN"
            btn = {b.target: b for b in C.PLC_BUTTONS}[a["target"]]
            self.plc.set_button(btn.addr, bool(a["on"]))
            self._note_plc_manual(btn.target, bool(a["on"]), source, by)
            return None
        if kind in ("HEATER_ON", "HEATER_OFF", "HEATER_SV", "HEATER_RESET", "HEATER_STOP", "HEATER_GAS_RELEASE",
                    "HEATER_RECIPE_RUN", "HEATER_RECIPE_STOP", "HEATER_RECIPE_HOLD", "HEATER_RECIPE_SKIP"):
            return self._heater(kind, a)
        if kind == "IG_FILAMENT":
            last = self.plc.last
            atm = None if last is None else last.values.get("plc.atm")
            why = C.check_ig_filament(source, state, bool(a.get("on")), atm, self._plc_up() and last is not None)
            if why:
                return why
            (self.ig.filament_on if a.get("on") else self.ig.filament_off)()
            return None
        return "UNKNOWN_COMMAND"

    @staticmethod
    def _why(engine, call: Callable[[], Optional[str]]) -> Optional[str]:
        """엔진 호출 — 거부하면서 이유 문구(engine.reject_detail)를 남겼으면 detail 과 함께."""
        if hasattr(engine, "reject_detail"):
            engine.reject_detail = ""
        r = call()
        detail = str(getattr(engine, "reject_detail", "") or "") if r else ""
        if detail:
            raise Rejected(r, detail[:300])
        return r

    def _sv_limit(self) -> Optional[float]:
        """PLC 가 보고한 히터 목표 상한(heater.sv_limit, D00013 °C) — 모르면 None."""
        return (getattr(self.heater, "st", None) or {}).get("sv_limit")

    def _note_plc_manual(self, target: str, on: bool, source: Source, by: str = "") -> None:
        """공정 중(진행·정지 순서) 실행한 PLC 버튼 — 공정 기록에 이벤트(info)."""
        p = self.process
        if (p.active or p.state == "STOPPING") and hasattr(p, "note_plc_manual"):
            p.note_plc_manual(target, on, getattr(source, "value", str(source)), by=by or None)

    def _heater(self, kind: str, a: dict) -> Optional[str]:
        h, p, c = self.heater, self.process, self.cfg
        st = h.st
        if kind not in ("HEATER_STOP", "HEATER_RECIPE_STOP", "HEATER_GAS_RELEASE") and not c["HEATER_ENABLED"]:
            return "HEATER_DISABLED"
        if not self._plc_up() and kind not in ("HEATER_GAS_RELEASE",):
            return "LINK_DOWN"
        owned = p.uses_heater and (p.active or p.state == "STOPPING")
        if kind == "HEATER_STOP":
            h.recipe_stop("사용자 정지")
            h.manual_off()
            if owned:
                p.stop()
            return None
        if kind in ("HEATER_ON", "HEATER_RECIPE_RUN", "HEATER_SV", "HEATER_OFF") and owned:
            return "HEATER_OWNED_BY_PROCESS"
        if kind == "HEATER_RECIPE_RUN" and (p.active or p.state == "STOPPING") and getattr(p, "list_uses_heater", False):
            return "HEATER_OWNED_BY_PROCESS"           # 진행 중(딜레이·행 사이 포함) 리스트의 어느 행이 히터를 쓰면(v1 main.py:2618-2620)
        if kind in ("HEATER_ON", "HEATER_SV", "HEATER_OFF") and h.recipe_running:
            return "HEATER_RECIPE_RUNNING"
        if kind in ("HEATER_ON", "HEATER_RECIPE_RUN"):
            if st.get("fault"):
                return "HEATER_FAULT_ACTIVE"
            if not st.get("itl"):
                return "HEATER_ITL_OFF"
        if kind == "HEATER_ON":
            gas = {k: a.get(k) for k in ("use_ar", "ar_flow", "use_o2", "o2_flow", "wp_mtorr")}
            if (gas["use_ar"] or gas["use_o2"]) and (p.active or p.state == "STOPPING"):
                return "PROCESS_RUNNING"
            if (gas["use_ar"] or gas["use_o2"]) and h.atm.state != "READY":
                why = self.safety.check_mv(self.safety.plc_view())       # 히터 가스도 MV 열림·인터록(v1 main.py:3158·1692)
                if why:
                    return why
            return self._why(h, lambda: h.manual_on(float(a["value"]), gas if (gas["use_ar"] or gas["use_o2"]) else None))
        if kind == "HEATER_OFF":
            h.manual_off()
            return None
        if kind == "HEATER_SV":
            h.set_sv(float(a["value"]))
            return None
        if kind == "HEATER_RESET":
            if not st.get("fault"):
                return "HEATER_NOT_FAULT"
            if st.get("run"):
                return "HEATER_RUN_ON"
            self.plc.heater_reset()
            return None
        if kind == "HEATER_GAS_RELEASE":
            if not h.atm.is_active():
                return "HEATER_GAS_NOT_ACTIVE"
            h.gas_release()
            return None
        if kind == "HEATER_RECIPE_RUN":
            gas = a.get("gas") or None
            errs, _ = RC.check_heater_recipe(a.get("steps") or [], gas, c)
            if errs:
                raise Rejected("RECIPE_INVALID", errs[0][:300])
            if gas and (gas.get("use_ar") or gas.get("use_o2")) and (p.active or p.state == "STOPPING"):
                return "PROCESS_RUNNING"
            if gas and (gas.get("use_ar") or gas.get("use_o2")) and h.atm.state != "READY":
                why = self.safety.check_mv(self.safety.plc_view())       # v1 main.py:2665 _heater_gas_start_guard
                if why:
                    return why
            try:
                steps = parse_recipe_steps(a["steps"], c)
            except ValueError as e:
                raise Rejected("RECIPE_INVALID", str(e)[:300])
            return self._why(h, lambda: h.recipe_run(str(a.get("name") or ""), steps, gas))
        if kind == "HEATER_RECIPE_STOP":
            # 레시피가 없어도 v1 처럼 안전 종료(RUN=0·램프 복구·SV=PV). 공정이 히터를 쓰는 중이어도 v1 은 그대로 끄고
            # 공정은 중단하지 않는다(main.py:1952-1953 — 소유 검사 없음)
            h.recipe_stop("사용자 정지")
            return None
        if not h.recipe_running:
            return "HEATER_RECIPE_NOT_RUNNING"
        if kind == "HEATER_RECIPE_HOLD":
            if not h.recipe_hold(bool(a.get("on"))):
                return "HEATER_RECIPE_NOT_RUNNING" if a.get("on") else "HEATER_ALREADY"     # v1: 일시정지 상태가 아님
        elif kind == "HEATER_RECIPE_SKIP":
            h.recipe_skip()
        return None
