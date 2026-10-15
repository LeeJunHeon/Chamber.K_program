"""챔버K 명령 처리 — 웹·로컬 화면 명령을 검사(commands.check_command + 장비 값 조건)하고 엔진에 넘긴다.

결과는 (성공, 사유 코드). 다음 단계(로컬 HMI·웹 연동)가 이 함수 하나를 부른다. 골든 하니스도 이것으로 v1 원격 명령을 흉내 낸다.
- PROCESS_STOP: 공정이 없으면 아무것도 하지 않고 성공(v1). RECIPE_CLEAR: 적재가 없으면 성공, 공정 중이면 PROCESS_RUNNING(v1 B5).
- PLC_BUTTON: PLC 링크가 없으면 LINK_DOWN(v1 B6), 공정 중 잠금 버튼은 REMOTE_LOCKED_IN_PROCESS(v2, pending_confirm).
- ALL_STOP: 공정 여부와 무관 — SafetyEngine.all_stop.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from v2.common.commands import Source
from v2.chk import commands as C
from v2.chk.heater_engine import parse_recipe_steps
from v2.chk import recipe as RC


@dataclass
class CmdResult:
    ok: bool
    reason: Optional[str] = None


class ChkCore:
    def __init__(self, cfg: Dict[str, Any], process, heater, safety, plc, ig, links: Dict[str, Callable[[], bool]],
                 on_result: Optional[Callable[[Any, str, CmdResult], None]] = None) -> None:
        self.cfg, self.process, self.heater, self.safety, self.plc, self.ig = cfg, process, heater, safety, plc, ig
        self.links = links
        self.on_result = on_result

    def _plc_up(self) -> bool:
        return self.links.get("plc", lambda: False)()

    def command(self, kind: str, args: Optional[dict] = None, source: Source = Source.WEB, cid: Any = None) -> CmdResult:
        args = dict(args or {})
        r = self._run(kind, args, source)
        res = CmdResult(r is None, r)
        if self.on_result:
            self.on_result(cid, kind, res)
        return res

    def _run(self, kind: str, a: dict, source: Source) -> Optional[str]:
        p, h = self.process, self.heater
        state = p.state
        why = C.check_command(kind, source, state, a, in_process=p.active or state == "STOPPING")
        if why:
            return why
        if kind == "PROCESS_STOP":
            p.stop()
            return None
        if kind == "ALL_STOP":
            self.safety.all_stop()
            return None
        if kind == "BUZZ_STOP":
            if not self._plc_up():
                return "LINK_DOWN"
            self.plc.set_button("buzz_stop", True)
            return None
        if kind == "PROCESS_START":
            if not isinstance(a.get("row"), dict):
                return "BAD_ARGS"
            return p.start_manual(a["row"])
        if kind == "RECIPE_LOAD":
            if not isinstance(a.get("rows"), list):
                return "BAD_ARGS"
            return p.load(str(a.get("name") or ""), a["rows"])
        if kind == "RECIPE_CLEAR":
            return p.clear()
        if kind == "RECIPE_START":
            return p.start_recipe()
        if kind == "PLC_BUTTON":
            if not self._plc_up():
                return "LINK_DOWN"
            btn = {b.target: b for b in C.PLC_BUTTONS}[a["target"]]
            self.plc.set_button(btn.addr, bool(a["on"]))
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
            return h.manual_on(float(a["value"]), gas if (gas["use_ar"] or gas["use_o2"]) else None)
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
                return "RECIPE_INVALID"
            if gas and (gas.get("use_ar") or gas.get("use_o2")) and (p.active or p.state == "STOPPING"):
                return "PROCESS_RUNNING"
            try:
                steps = parse_recipe_steps(a["steps"], c)
            except ValueError:
                return "RECIPE_INVALID"
            return h.recipe_run(str(a.get("name") or ""), steps, gas)
        if not h.recipe_running:
            return "HEATER_RECIPE_NOT_RUNNING"
        if kind == "HEATER_RECIPE_STOP":
            h.recipe_stop("사용자 정지")
        elif kind == "HEATER_RECIPE_HOLD":
            (h.recipe.hold if a.get("on") else h.recipe.resume)()
        elif kind == "HEATER_RECIPE_SKIP":
            h.recipe.skip()
        return None
