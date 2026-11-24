"""평소 보기 줄 골든 만들기(웹 W-18) — 장비가 웹으로 내는 봉투를 여러 흐름에서 모아 웹 화면 줄 검사(ops2:chk-feed)에 쓴다.

python -m v2.tools.make_web_feed_golden   → v2/tests/golden/web_feed_cases.json 을 다시 쓴다(손으로 고치지 않음).
운전 모드(prod) 코드 경로 + 가짜 포트(시뮬레이터 — ChkRig, 가상 시계). 실제 포트·바깥 네트워크 없음(웹 주소는 .invalid).
v2/chk·v2/common 은 읽기만 한다. 흐름마다 MessageHub 가 낸 봉투(state·snapshot_summary 는 뺀다)의 {type, data} 를 순서대로.
  (a) RF 허용 신호가 남은 채 시작  (b) 정상 공정 끝까지  (c) 공정 중 STOP  (d) 공정 중 MV 인터락 꺼짐  (e) MV 닫힘에서 시작 거부
  (f) 수동 조작 가스 켜기·셔터 열기·끝내기  (g) Filament 켜기·끄기  (h) PLC 버튼  (i) 히터 켜기·끄기  (j) 히터 레시피 짧게 한 번
  (k) 닿지 않는 웹 주소로 웹 연결 끊김  (l) ALL STOP  (m) 프로그램 종료
봉투의 id·instance·ts 는 빼고(매번 다름) 흐름 안 순서만 남긴다. (k) 는 실제 시간으로 몇 초 돌아 문장 속 초가 달라질 수 있다.
"""
from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

from v2.common.clock import VirtualClock
from v2.chk import web_schema as W
from v2.chk.sim.plc_sim import A
from v2.chk.sim.rig import ChkRig

OUT = Path(__file__).resolve().parents[1] / "tests" / "golden" / "web_feed_cases.json"
SKIP_TYPES = {"state", "snapshot_summary"}
FAST = {"GAS_STABILIZE_SEC": 5.0, "IG_ENABLED": True}
ROW = {"process_name": "Ti 증착", "ar": True, "ar_flow": 20.0, "working_pressure": 5.0, "process_time": 0.2,
       "shutter_delay": 0.1, "use_dc": True, "dc_power": 100.0, "gun1": True}


class Run:
    """ChkRig(운전 모드) 한 번 — tick 번호·조건에 맞춰 명령을 보내고 허브 봉투를 모은다."""

    def __init__(self, tmp: Path, cfg: Dict[str, Any] = None, vacuum: bool = True, before: Callable = None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.rig = ChkRig(self.c, tmp, cfg=dict(FAST, **(cfg or {})), mode="prod")
        self.plc = self.rig.sims["plc"]
        for k in ("mv", "turbo", "rotary", "fv"):
            self.plc.coils[A[k]] = vacuum
        if before:
            before(self)
        self.envs: List[dict] = []
        pub = self.rig.hub.publish

        def keep(type_, data):
            env = pub(type_, data)
            if type_ not in SKIP_TYPES:
                self.envs.append({"type": type_, "data": json.loads(json.dumps(data, ensure_ascii=False, default=str))})
            return env
        self.rig.hub.publish = keep
        self.at: Dict[int, List[Callable]] = {}
        self.when_: List[tuple] = []
        self.rig.loop.tick_hooks.append(self._hook)

    def _hook(self, t):
        for fn in self.at.pop(t.seq, []):
            fn()
        for w in list(self.when_):
            if w[0]():
                self.when_.remove(w)
                w[1]()

    def on(self, seq: int, fn: Callable) -> None:
        self.at.setdefault(seq, []).append(fn)

    def when(self, pred: Callable, fn: Callable) -> None:
        self.when_.append((pred, fn))

    def cmd(self, kind: str, args: dict = None, source=None) -> Callable:
        from v2.common.commands import Source
        return lambda: self.rig.core.command(kind, args or {}, source=source or Source.WEB)

    def run(self, n: int, stop: bool = True) -> List[dict]:
        assert self.rig.start()
        try:
            self.rig.loop.run(n)
        finally:
            if stop:
                self.rig.stop()
        return self.envs


def cases(tmp: Path) -> List[dict]:
    from v2.common.commands import Source
    out = []

    def add(name, covers, envs):
        out.append({"name": name, "covers": covers, "envelopes": envs})

    # (a) RF 허용 신호(M00200)만 켜진 채 시작 — 시작 정리(S51 RF_ENABLE_RESIDUAL)
    r = Run(tmp / "a", before=lambda r: r.plc.coils.__setitem__(A["rf_dac_enable"], True))
    add("a_rf_enable_residual", "시작 때 남은 RF 허용 신호 정리", r.run(8))
    # (b) 정상 공정 끝까지
    r = Run(tmp / "b")
    r.on(3, r.cmd("PROCESS_START", {"row": ROW}))
    add("b_process_done", "정상 공정 시작·단계·상태·끝", r.run(170))
    # (c) 공정 중 STOP
    r = Run(tmp / "c")
    r.on(3, r.cmd("PROCESS_START", {"row": ROW}))
    r.on(25, r.cmd("PROCESS_STOP"))
    add("c_process_stop", "공정 중 STOP — 정지 순서·사용자 STOP 끝", r.run(80))
    # (d) 공정 중 MV 인터락 꺼짐(지속)
    r = Run(tmp / "d")
    r.on(3, r.cmd("PROCESS_START", {"row": ROW}))
    r.on(25, lambda: setattr(r.plc, "itl_fault", True))          # 래더가 MV 인터락을 끈다(시뮬레이터 고장 주입)
    add("d_mv_interlock_lost", "공정 중 MV 인터락 꺼짐 — 안전 규칙·오류 끝(MV_INTERLOCK_LOST)", r.run(90))
    # (e) MV 닫힘에서 시작 거부
    r = Run(tmp / "e", vacuum=False)
    r.on(3, r.cmd("PROCESS_START", {"row": ROW}))
    add("e_start_reject_mv_closed", "MV 닫힘 — 시작 거부(명령 결과)", r.run(10))
    # (f) 수동 조작 가스 켜기 · Gun1 셔터 열기 · 끝내기
    r = Run(tmp / "f")
    seq = [("MANUAL_BLOCK", {"block": {"type": "gas_intro", "params": {"ar": True, "ar_flow": 20.0}}}),
           ("MANUAL_BLOCK", {"block": {"type": "shutter", "params": {"action": "open", "targets": ["g1"]}}}),
           ("MANUAL_END", {})]

    def feed(t, q=list(seq)):
        if q and t.seq >= 3 and (t.seq == 3 or r.rig.process.state == "MANUAL_WAIT"):
            k, a = q.pop(0)
            r.rig.core.command(k, a, source=Source.HMI if k == "MANUAL_END" else Source.WEB)
    r.rig.loop.tick_hooks.append(feed)
    add("f_manual", "수동 조작(웹·노트북) — 조작·끝내기", r.run(120))
    # (g) Filament 켜기 · 끄기
    r = Run(tmp / "g")
    r.on(3, r.cmd("IG_FILAMENT", {"on": True}))
    r.on(25, r.cmd("IG_FILAMENT", {"on": False}))
    add("g_filament", "Ion Gauge Filament 켜기·끄기 결과", r.run(50))
    # (h) PLC 버튼 — Vent 열기·닫기, Ionizer 켜기(노트북)
    r = Run(tmp / "h")
    r.on(3, r.cmd("PLC_BUTTON", {"target": "rv", "on": True}))
    r.on(6, r.cmd("PLC_BUTTON", {"target": "rv", "on": False}))
    r.on(9, r.cmd("PLC_BUTTON", {"target": "ion", "on": True}, source=Source.HMI))
    add("h_plc_button", "PLC 버튼(웹·노트북)", r.run(14))
    # (i) 히터 켜기 · 끄기
    r = Run(tmp / "i")
    r.on(3, r.cmd("HEATER_ON", {"value": 60.0}))
    r.on(40, r.cmd("HEATER_OFF"))
    add("i_heater_on_off", "히터 켜기·끄기", r.run(50))
    # (j) 히터 레시피 짧게 한 번
    r = Run(tmp / "j")
    r.on(3, r.cmd("HEATER_RECIPE_RUN", {"name": "짧은 승온", "steps": [{"target_c": 28, "ramp_c_per_min": 6, "soak_min": 0.05}]}))
    add("j_heater_recipe", "히터 레시피 시작·단계·끝", r.run(160))
    # (l) ALL STOP — 공정 중
    r = Run(tmp / "l")
    r.on(3, r.cmd("PROCESS_START", {"row": ROW}))
    r.on(25, r.cmd("ALL_STOP"))
    add("l_all_stop", "공정 중 ALL STOP", r.run(70))
    # (m) 프로그램 종료 — 공정 중 창 닫기(App.force_exit)
    from v2.chk.app import App
    r = Run(tmp / "m")
    r.on(3, r.cmd("PROCESS_START", {"row": ROW}))
    app = App(system=r.rig, clock=r.c, print_fn=lambda *_: None)
    r.on(30, lambda: app.force_exit("CTRL_CLOSE"))
    r.on(38, lambda: r.rig.safety.event("shutdown"))          # 정상 종료 때와 같은 S41(종료 때 히터 RUN=0) 사건
    add("m_program_exit", "공정 중 프로그램 종료(PROGRAM_EXIT)·종료 때 S41", r.run(45))
    # (k) 닿지 않는 웹 주소 — 웹 연결 끊김(실제 시간 몇 초, 주소 .invalid)
    add("k_uplink_down", "웹 연결 끊김(닿지 않는 주소)", uplink_down(tmp / "k"))
    return out


def uplink_down(tmp: Path) -> List[dict]:
    from v2.chk import uplink as U
    rig = ChkRig(VirtualClock(start_wall=time.time()), tmp, cfg={"UPLINK_INTERVAL_MS": 300, "UPLINK_TIMEOUT_SEC": 1.0,
                                                                 "UPLINK_DOWN_SEC": 2}, mode="prod")
    envs: List[dict] = []
    pub = rig.hub.publish

    def keep(type_, data):
        env = pub(type_, data)
        if type_ not in SKIP_TYPES:
            envs.append({"type": type_, "data": json.loads(json.dumps(data, ensure_ascii=False, default=str))})
        return env
    rig.hub.publish = keep
    up = U.Uplink(rig, "http://localhost.invalid:9/ops2", "t", tmp / "data")
    rig.uplink = up
    up.start()
    try:
        t_end = time.monotonic() + 25
        while time.monotonic() < t_end and not any(e["data"].get("code") == "UPLINK_DOWN" for e in envs):
            time.sleep(0.2)
    finally:
        up.stop()
    return envs


def bundle_for_web() -> dict:
    b = W.bundle()
    for k in ("params", "crossRules", "dropped", "recipe", "adjust", "preconditions"):
        b.pop(k, None)
    return b


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        cs = cases(Path(d))
    doc = {"exportVersion": W.EXPORT_VERSION, "bundleHash": W.content_hash(), "bundle": bundle_for_web(), "cases": cs}
    OUT.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT.name}: {len(cs)} 흐름, 봉투 {sum(len(c['envelopes']) for c in cs)}개")
    for c in cs:
        codes = sorted({str(e["data"].get("code") or e["data"].get("reasonCode") or e["type"]) for e in c["envelopes"]})
        print(f"  {c['name']}: {len(c['envelopes'])} — {', '.join(codes)[:200]}")


if __name__ == "__main__":
    main()
