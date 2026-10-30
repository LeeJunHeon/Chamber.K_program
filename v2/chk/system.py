"""챔버K 조립(사용자 지시 5-6 ②) — 드라이버 5개(PLC·MFC·DC·RF Pulse·IG) + 묶기 + 기록 + 안전 + 히터 + 공정 + 명령 + 장비 메시지.

모드에 따라 전송만 다르다: sim = 시뮬레이터(SimTransport — sim.rig.make_sims), bench·prod = SerialTransport(serial_transports).
어느 모드든 포트 바로 위에 통신층 가드(cmd_guard.GuardedTransport — 그 모드 목록). 쓰기 허용은 모드 정책(mode.policy).
시험 하니스(sim.rig.ChkRig)와 실행 프로그램(app)이 이 조립 하나를 쓴다(배선이 두 벌이 되지 않게).

연결
- 안전 규칙(ChkSafety): plc250 = PLC 폴링 직후(fast_listeners), tick = 행이 닫힌 뒤(sink), 드라이버 이벤트 → 이벤트 규칙.
- 1초 행이 닫히면: 기록 → 안전 규칙(tick) → 히터(on_tick) → 공정(on_row) → 설정 대기 적용(idle) → 장비 메시지(state).
- 내부 이벤트(ProcEvent·SafetyEvent·HeaterEvent·드라이버)는 events(링 버퍼)·기록·MessageHub 로.
- 기록은 data_dir, NAS 사본은 모드 정책(nas_copy)이 켜고 NAS_COPY_DIR 이 있을 때만.
- 설정(ConfigApply): ConfigStore + 모든 구성 요소가 같이 보는 cfg 사전. live 키는 공정 잠금 아래 그 자리에서,
  idle 키는 idle 이 되면(tick 마다 확인), restart 키는 재시작 때. 결과는 config_ack 메시지.
"""
from __future__ import annotations

import collections
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from v2.common.commands import Source
from v2.common.config_store import ApplyResult, ConfigStore
from v2.common.mode import RunMode, policy
from v2.common.paramdef import schema_hash
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source as BusSource, TickLoop, iso_local
from v2.common.transport import SerialSettings, SerialTransport, Transport
from v2.common.worker import Worker
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.cmd_guard import GuardedTransport
from v2.chk.core import ChkCore
from v2.chk.drivers.dc import DcDriver
from v2.chk.drivers.ig import IgDriver
from v2.chk.drivers.mfc import MfcDriver
from v2.chk.drivers.plc import PlcDriver, PlcFastPoller
from v2.chk.drivers.rfpulse import RfPulseDriver
from v2.chk.heater_engine import HeaterEngine
from v2.chk.hub import MessageHub
from v2.chk.process_engine import ProcessEngine
from v2.chk.safety import ChkSafety, SimpleProcess, View

DEVICES = ("plc", "mfc", "dc", "rfpulse", "ig")
ENABLE_KEY = {"rfpulse": "RFPULSE_ENABLED", "ig": "IG_ENABLED"}
STREAK = {"plc": "PLC_FAIL_STREAK", "mfc": "MFC_FAIL_STREAK", "dc": "DC_FAIL_STREAK",
          "rfpulse": "RFPULSE_FAIL_STREAK", "ig": "IG_FAIL_STREAK"}
EVENTS_KEEP = 20000                # 최근 내부 이벤트(링 버퍼) — 시험 하니스 기본


@dataclass(frozen=True)
class Retention:
    """메모리에 두는 고정 크기 기록의 크기 — 한 곳(사용자 지시 5-7). 운전 기록은 기록 파일(Recorder)·MessageHub 가 남기므로
    이 기록들은 화면·시험용이다. 시험 하니스(기본)는 결과를 보려고 크게, 실행 프로그램(APP_RETENTION)은 작게 —
    크게 두면 공정을 계속 돌릴 때 다 차기까지(하루~이틀) 메모리가 늘어 보여 진짜 누수와 구분되지 않는다.
    크기가 설정·상수로 정해지는 다른 기록(실행 프로그램에서도 그대로):
      PLC 블랙박스 BLACKBOX_SECONDS × 4(250 ms 폴링) · 허브 소비자 큐 hub.SUB_QUEUE 1000 · plc250 평가 시각 2400 ·
      정지 순서 스레드 64 · 기록 오류 목록 recorder.ERRORS_KEEP 200 · 블랙박스 파일 이름 100 ·
      웹 명령 id uplink.CMD_KEEP 2000 · 웹 보관함 UPLINK_SPOOL_MAX_MB(디스크)."""
    events: int = EVENTS_KEEP          # ChkSystem.events·ProcessEngine.events·SafetyEngine.events·HeaterEngine.events
    notifications: int = 5000          # ChkSafety.notifications
    hub_recent: int = 500              # MessageHub.recent(브라우저·웹 연결이 다시 붙을 때 이어 받는 링 버퍼)
    tick_stats: int = 21600            # TickLoop.jitters·close_lags(6 시간분)
    sim_log: Optional[int] = None      # 시뮬레이터가 받은 요청 기록(None = simdev.SIM_LOG_KEEP 50000)


TEST_RETENTION = Retention()
APP_RETENTION = Retention(events=1000, notifications=1000, hub_recent=500, tick_stats=21600, sim_log=1000)


def serial_settings(cfg: Dict[str, Any], ports: Optional[Dict[str, str]] = None) -> Dict[str, SerialSettings]:
    """장비별 포트 설정 — 포트 이름은 ports(있으면) 아니면 설정의 *_PORT."""
    pt = dict({"plc": cfg["PLC_PORT"], "mfc": cfg["MFC_PORT"], "dc": cfg["DC_PORT"], "rfpulse": cfg["RFPULSE_PORT"],
               "ig": cfg["IG_PORT"]}, **(ports or {}))
    par = {"none": "N", "odd": "O", "even": "E"}[cfg["RFPULSE_PARITY"]]
    return {"plc": SerialSettings(pt["plc"], cfg["PLC_BAUD"]),
            "mfc": SerialSettings(pt["mfc"], cfg["MFC_BAUD"]),
            "dc": SerialSettings(pt["dc"], cfg["DC_BAUD"]),
            "rfpulse": SerialSettings(pt["rfpulse"], cfg["RFPULSE_BAUD"], parity=par),
            "ig": SerialSettings(pt["ig"], cfg["IG_BAUD"])}


def serial_transports(clock, cfg: Dict[str, Any], ports: Optional[Dict[str, str]] = None,
                      serial_factory: Optional[Callable] = None) -> Dict[str, Transport]:
    """bench·prod 의 포트 전송(가드는 ChkSystem 이 그 위에). 포트가 없거나 사용 중이면 worker 가 재연결로 계속 시도한다."""
    st = serial_settings(cfg, ports)
    return {k: SerialTransport(clock, st[k], open_timeout_s=cfg["PORT_OPEN_TIMEOUT_SEC"], serial_factory=serial_factory)
            for k in DEVICES}


class ConfigApply:
    """웹 설정 적용(CONFIG_APPLY·CONFIG_ROLLBACK) — ConfigStore 와 실행 cfg 사전을 잇는다(5-6 ④).

    busy = 공정 진행·정지 순서·CSV 리스트·공정 밖 정리·히터 레시피·히터 가스 중(S48 H 등급 거부·idle 키 대기).
    설치·비밀 키: 비밀값은 웹에서 받지 않고(CONFIG_SECRET_FROM_WEB), RUN_MODE·EQUIPMENT_ID 는 지금 값과 다르면 거부
    (CONFIG_LOCKED_KEY — 덮어쓰기에 없어도, 저장 값과 비교), local_override 가 정한 키는 웹 값보다 덮어쓰기가 이긴다(overriddenKeys).
    CONFIG_ROLLBACK version = 웹이 정한 새 버전 번호(없으면 지금 + 1).
    install 키는 restart 대기.
    """

    def __init__(self, store: ConfigStore, cfg: Dict[str, Any], busy: Callable[[], bool], lock,
                 on_ack: Callable[[dict], None]) -> None:
        self.store, self.cfg, self.busy, self.lock, self.on_ack = store, cfg, busy, lock, on_ack

    def _push(self, keys) -> None:
        with self.lock:                                   # 공정 잠금 아래 그 자리에서(모든 구성 요소가 같은 사전을 본다)
            for k in keys:
                self.cfg[k] = self.store.values[k]

    def _ack(self, res: ApplyResult, by: str) -> dict:
        d = {"version": res.version, "status": res.status, "reasonCode": res.reason, "errors": list(res.errors),
             "appliedKeys": list(res.applied_keys), "pendingKeys": list(res.pending_keys),
             "overriddenKeys": list(res.overridden_keys), "kind": res.kind, "by": by, "changes": list(res.changes),
             "values": res.values}
        self.on_ack(d)
        return d

    def apply(self, a: dict, by: str) -> Optional[str]:
        v, vals, sh = a.get("version"), a.get("values"), a.get("schemaHash")
        if not isinstance(v, int) or isinstance(v, bool) or not isinstance(vals, dict) or not isinstance(sh, str):
            return "BAD_ARGS"
        res = self.store.apply(v, vals, sh, self.busy(), by=by)
        if res.status != "rejected":
            self._push(res.applied_keys)
        self._ack(res, by)
        return res.reason

    def rollback(self, a: dict, by: str) -> Optional[str]:
        v = a.get("version")
        if v is not None and (not isinstance(v, int) or isinstance(v, bool)):
            return "BAD_ARGS"
        res = self.store.rollback(self.busy(), by=by, version=v)
        if res.status != "rejected":
            self._push(res.applied_keys)
        self._ack(res, by)
        return res.reason

    def tick(self) -> None:
        """idle 이 되면 대기 중 idle 키 적용(ConfigStore.on_idle) → config_ack(kind idle)."""
        if not self.store.pending_idle or self.busy():
            return
        keys = self.store.on_idle()
        if keys:
            self._push(keys)
            self.on_ack({"version": self.store.version, "status": "applied", "reasonCode": None, "errors": [],
                         "appliedKeys": keys, "pendingKeys": sorted(self.store.pending_restart), "overriddenKeys": [],
                         "kind": "idle", "by": "system", "changes": [], "values": None})


class ChkSystem:
    def __init__(self, clock, cfg: Dict[str, Any], data_dir: Path, transports: Dict[str, Transport], mode: str = "sim",
                 simple_process: bool = False, store: Optional[ConfigStore] = None, nas_dir: Optional[str] = None,
                 events_keep: int = EVENTS_KEEP, scenario_hook: Optional[Callable[[Any], None]] = None,
                 program: str = "chk-v2", version: str = "unknown", retention: Retention = TEST_RETENTION) -> None:
        self.clock = clock
        self.cfg = cfg                           # 모든 구성 요소가 이 사전을 같이 본다(복사하지 않음)
        self.mode = mode
        self.policy = policy(RunMode(mode))
        self.program, self.version = program, version
        c = self.cfg
        wr = self.policy.device_writes
        self.enabled = {k: bool(c[ENABLE_KEY[k]]) if k in ENABLE_KEY else True for k in DEVICES}
        self.retention = rt = retention
        if events_keep != EVENTS_KEEP:
            rt = Retention(events_keep, rt.notifications, rt.hub_recent, rt.tick_stats, rt.sim_log)
        self.events: "collections.deque[Any]" = collections.deque(maxlen=rt.events)
        self.hub = MessageHub(self, recent_keep=rt.hub_recent)
        # 가드는 포트(전송) 바로 위
        self.guards = {k: GuardedTransport(transports[k], k, mode, c, on_event=self._ev) for k in DEVICES}
        self.workers = {k: Worker(k, self.guards[k], writes_allowed=wr, fail_streak=c[STREAK[k]],
                                  connect_init_timeout_s=c["CONNECT_INIT_TIMEOUT_SEC"], on_event=self._ev)
                        for k in DEVICES}
        self.recorder: Optional[Recorder] = None
        w = self.workers
        self.plc = PlcDriver(clock, w["plc"], c, wr, on_event=self._ev,
                             blackbox_sink=lambda name, rows: self.recorder.put_blackbox(name, rows))
        self.mfc = MfcDriver(clock, w["mfc"], c, wr, on_event=self._ev)
        self.dc = DcDriver(clock, w["dc"], c, wr, on_event=self._ev)
        self.rfp = RfPulseDriver(clock, w["rfpulse"], c, wr, on_event=self._ev)
        self.ig = IgDriver(clock, w["ig"], c, wr, on_event=self._ev)
        self.bus = SnapshotBus(clock, SF.BLOCKS, [
            BusSource("plc", w["plc"], self.plc.source_read, ("plc", "heater", "rf"), c["PLC_READ_DEADLINE_MS"]),
            BusSource("mfc", w["mfc"], self.mfc.source_read, ("mfc",), c["MFC_READ_DEADLINE_MS"]),
            BusSource("dc", w["dc"], self.dc.source_read, ("dc",), c["DC_READ_DEADLINE_MS"]),
            BusSource("rfpulse", w["rfpulse"], self.rfp.source_read, ("rfp",), c["RFPULSE_READ_DEADLINE_MS"],
                      enabled=self.enabled["rfpulse"]),
            BusSource("ig", w["ig"], self.ig.source_read, ("ig",), c["IG_RESPONSE_LIMIT_MS"], enabled=self.enabled["ig"]),
        ], c["TICK_DEADLINE_MS"], SF.PROCESS_FIELDS)
        nas = nas_dir if (self.policy.nas_copy and nas_dir) else None
        self.recorder = Recorder(Path(data_dir), self.bus.columns, retention_days=c["SNAPSHOT_RETENTION_DAYS"],
                                 nas_dir=Path(nas) if nas else None, nas_period_s=float(c["NAS_SYNC_PERIOD_SEC"]))
        self.fast = PlcFastPoller(clock, self.plc, c["PLC_FAST_POLL_MS"])
        self.heater = HeaterEngine(clock, c, self.plc, self.mfc, on_event=self._hev,
                                   process_active=lambda: self.process.active,
                                   on_gas_failed=lambda why: self.safety.event("heater_gas_failed"),
                                   on_recipe_failed=lambda code: self.safety.heater_recipe_failed(code))
        self.links = {k: (lambda x=x: x.ready) for k, x in self.workers.items()}
        if simple_process:
            self.process = SimpleProcess()
        else:
            self.process = ProcessEngine(clock, c, self.plc, self.mfc, self.dc, self.rfp, self.ig, self.heater, self.links,
                                         on_event=self._pev)
        self.safety = ChkSafety(clock, c, self.process, self.plc, self.mfc, self.dc, self.rfp, self.ig, on_event=self._sev,
                                heater=self.heater, bench=self.policy.mode is RunMode.BENCH)
        if not simple_process:
            self.process.safety = self.safety
        self.store = store
        if store is not None and not store.values:
            store.load()
        self.config = (ConfigApply(store, c, self.busy, getattr(self.process, "_lock", None) or _NullLock(),
                                   self.hub.config_ack) if store is not None else None)
        self.core = ChkCore(c, self.process, self.heater, self.safety, self.plc, self.ig, self.links,
                            on_result=self.hub.cmd_result, config=self.config, writes_allowed=wr)
        self.uplink = None                       # app 이 웹 연결을 붙이면(uplink.Uplink) — state.uplink 로 보인다
        self.plc.fast_listeners.append(self.safety.on_poll)
        self.plc.fast_listeners.append(self.heater.on_poll)
        self._scenario_hook = scenario_hook
        self.loop = TickLoop(clock, self.bus, [self.recorder.put_row, self.safety.on_row, self._on_row],
                             tick_hooks=[self._tick], extra_fn=self._extra)
        self._apply_retention(rt)
        self.started = False

    def _apply_retention(self, rt: Retention) -> None:
        """구성 요소가 만든 기록의 크기를 rt 로(기록은 화면·시험용 — 제어는 읽지 않는다)."""
        def resize(obj, name, n):
            q = getattr(obj, name, None)
            if q is not None and q.maxlen != n:
                setattr(obj, name, collections.deque(q, maxlen=n))
        for obj in (self.process, self.safety.engine, self.heater):
            resize(obj, "events", rt.events)
        resize(self.safety, "notifications", rt.notifications)
        resize(self.loop, "jitters", rt.tick_stats)
        resize(self.loop, "close_lags", rt.tick_stats)

    def retention_report(self) -> Dict[str, int]:
        """실제로 쓰는 기록 크기(보고·시험용)."""
        p, s, h, lp = self.process, self.safety, self.heater, self.loop
        return {"system.events": self.events.maxlen, "process.events": getattr(getattr(p, "events", None), "maxlen", 0) or 0,
                "safety.events": s.engine.events.maxlen, "heater.events": h.events.maxlen,
                "safety.notifications": s.notifications.maxlen, "safety.poll_eval_s": s.poll_eval_s.maxlen,
                "hub.recent": self.hub.recent.maxlen, "tick.jitters": lp.jitters.maxlen, "tick.close_lags": lp.close_lags.maxlen,
                "plc.blackbox": self.plc.blackbox.maxlen, "recorder.errors": self.recorder.errors.maxlen}

    # ── 상태 ──
    def busy(self) -> bool:
        """설정 적용의 busy(S48·idle 대기) — 공정 진행·정지 순서·CSV 리스트·공정 밖 정리·히터 레시피·히터 가스."""
        p, h = self.process, self.heater
        return bool(p.active or p.state == "STOPPING" or getattr(p, "list_active", False) or getattr(p, "cleaning", False)
                    or h.recipe_running or h.atm.is_active() or self.safety.engine.stopping)

    @property
    def config_version(self) -> int:
        return self.store.version if self.store is not None else 0

    # ── 1초 행 ──
    def _on_row(self, row) -> None:
        v = View.from_row(row)
        self.heater.on_tick(v)
        if hasattr(self.process, "on_row"):
            self.process.on_row(v)
        if self.config is not None:
            try:
                self.config.tick()
            except Exception as e:
                self._dispatch_error("config", e)
        self.hub.on_row(row)

    def _extra(self, t) -> dict:
        out = {"heater.hold": self.heater.hold_label}
        if hasattr(self.process, "snapshot_fields"):
            out.update(self.process.snapshot_fields())
        return out

    def _tick(self, t) -> None:
        self.plc.on_tick(t)
        self.ig.on_tick(t)
        if hasattr(self.process, "on_tick_start"):
            self.process.on_tick_start(t)
        if self._scenario_hook is not None:
            self._scenario_hook(t)

    # ── 내부 이벤트 → 링 버퍼·기록·장비 메시지 ──
    def _dispatch_error(self, where: str, e: Exception) -> None:
        if self.recorder is not None:
            self.recorder.put_event(iso_local(self.clock.wall()), "error", "INTERNAL_ERROR", where, repr(e))

    def _hub(self, e) -> None:
        try:
            self.hub.on_event(e)
        except Exception as x:                    # 메시지 만들기 실패가 제어를 막지 않는다
            self._dispatch_error("hub", x)

    def _pev(self, e) -> None:
        self.events.append(e)
        if self.recorder is not None and e.kind in ("run_start", "run_end", "state", "warn", "plc_manual"):
            level = (e.data or {}).get("level") or ("warn" if e.code else "info")
            self.recorder.put_event(iso_local(self.clock.wall()), level, (e.code or e.kind).upper(), "process", e.detail)
        self._hub(e)

    def _hev(self, e) -> None:
        self.events.append(e)
        if self.recorder is not None and e.kind != "message":
            self.recorder.put_event(iso_local(self.clock.wall()), e.level, (e.code or e.kind).upper(), "heater", e.detail)
        self._hub(e)

    def _sev(self, e) -> None:
        self.events.append(e)
        if self.recorder is not None:
            self.recorder.put_event(iso_local(self.clock.wall()), e.level, (e.code or e.kind).upper(),
                                    f"safety:{e.rule}", e.detail)
        self._hub(e)

    def _ev(self, e) -> None:
        self.events.append(e)
        if getattr(self, "safety", None) is not None:
            self.safety.on_driver_event(e)
        if self.recorder is not None:
            kind = getattr(e, "kind", "?")
            reason = getattr(e, "reason", None)
            self.recorder.put_event(iso_local(self.clock.wall()), "warn" if reason else "info", (reason or kind).upper(),
                                    getattr(e, "device", type(e).__name__), getattr(e, "detail", ""))
        self._hub(e)

    # ── 생명주기 ──
    def start(self, ready_timeout_s: float = 15.0) -> bool:
        """worker 를 시작하고, 모두 연결·초기화가 끝날 때까지(최대 ready_timeout_s) 기다린 뒤 돌아온다 —
        첫 tick 의 읽기가 연결 초기화 뒤에 줄 서서 실패로 남지 않게. 모두 준비되면 True(아니어도 worker 는 계속 재연결)."""
        self.recorder.start()
        self.hub.hello()                             # 첫 메시지(장비 연결 이벤트보다 먼저)
        with self.clock.hold():
            for k, x in self.workers.items():
                if self.enabled[k]:                  # 설정에서 꺼진 장비는 포트를 열지 않는다
                    x.start()
            self.fast.start()
        self.started = True
        return self.clock.wait_until(self.clock.monotonic() + ready_timeout_s,
                                     lambda: all(x.ready for k, x in self.workers.items() if self.enabled[k]))

    def stop(self) -> None:
        self.loop.stop()
        self.fast.stop()
        for k, x in self.workers.items():
            if self.enabled[k]:
                x.stop()
        self.recorder.stop()
        self.hub.close()

    def command(self, kind: str, args: Optional[dict] = None, source: Source = Source.WEB, cid: Any = None,
                by: Optional[str] = None):
        return self.core.command(kind, args, source=source, cid=cid, by=by)


class _NullLock:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def param_schema_hash() -> str:
    return schema_hash(P.PARAMS, P.CROSS_RULES)


def new_store(directory: Path) -> ConfigStore:
    return ConfigStore(Path(directory), P.PARAMS, P.CROSS_RULES, param_schema_hash())
