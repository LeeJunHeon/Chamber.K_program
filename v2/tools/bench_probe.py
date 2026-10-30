"""bench 실측 도구 — 장비 노트북에서 v1 을 끈 상태로, 실제 장비를 읽기 전용으로 붙여 설정을 전부 읽고 1초 수집·응답 시간을 잰다.

    python -m v2.tools.bench_probe --minutes 10
    (포트 바꾸기: --plc COM9 --mfc COM10 --dc COM11 --ig COM12 --rfpulse COM13, 빼기: --skip ig, 기록 폴더: --out <폴더>)

- RUN_MODE=bench 고정: 장비에 아무것도 쓰지 않는다(하트비트·연결 시 설정·필라멘트 포함). worker 가 쓰기를 거부하고
  드라이버도 읽기 전용이며, 포트 바로 위의 통신층 가드(v2.chk.cmd_guard)가 매뉴얼 질의 목록 밖 바이트를 막는다(삼중 보호).
- *_ENABLED 가 false 인 장비(기본: RF Pulse — 미연결)와 --skip <장비> 는 포트를 열지 않고 요약에 '제외'.
- 시작 전 안전 확인 — 걸리면 아무것도 쓰지 않고 끝낸다:
  · 포트를 열 수 없으면(v1 이 켜져 있을 가능성) 다시 시도하지 않고 종료
  · PLC 를 읽을 수 없거나 첫 읽기에서 히터 RUN 이 켜져 있으면 종료 — bench 는 하트비트를 쓰지 않아 10 s 뒤 PLC 가 히터를 끈다
  · RF 실제 출력(D40 > 0 또는 순방향 ≥ 5 W), DC 출력 ON, RF Pulse 출력 ON, 가스 유량 > 0.1 sccm 이면 종료.
    RF DAC enable(M00200)만 켜져 있고 출력이 없으면 v1 정상 종료 흔적으로 보고 [참고] 를 남긴 뒤 계속
    (v1 send_rfpower_command 는 DAC 를 쓸 때마다 enable=1 을 먼저 쓰고 enable=0 은 비상정지에서만 쓴다)
- 설정 전부 읽기(안전 확인 뒤, 수집 전): 장비별 매뉴얼 질의를 한 번씩(앞 응답을 받은 뒤 드라이버 간격대로, 무응답이면 0.3 s 쉬고 다음)
  → 요약 표 + inventory.json. 해석·대조는 v2.chk.inventory(맞지 않으면 '!!!' 만 — 값은 바꾸지 않음).
- 기록: 바탕화면\\v2_bench_YYYYMMDD_HHMMSS — events.jsonl(처음부터), inventory.json, 스냅샷(SQLite·CSV), 통신 녹화 rec_*.jsonl, summary.txt.
  NAS·C:\\ChamberK_v2 에는 쓰지 않는다. Ctrl+C 로 언제든 멈출 수 있고, 멈춰도 요약은 남긴다.
종료 코드: 0 정상, 2 안전 확인에서 멈춤, 3 포트 열기 실패.
"""
from __future__ import annotations

import argparse
import collections
import datetime as _dt
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import RealClock
from v2.common.paramdef import defaults
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop, iso_local
from v2.common.transport import RecordingTransport, SerialSettings, SerialTransport, TransportError, save_exchanges
from v2.common.worker import Request, Status, Worker
from v2.chk import cmd_guard as G
from v2.chk import devices as D
from v2.chk import inventory as INV
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.dc import DcDriver
from v2.chk.drivers.ig import E_ITEM_COUNT, IgDriver, pressure_text
from v2.chk.drivers.mfc import MfcDriver
from v2.chk.drivers.plc import PlcDriver, PlcFastPoller
from v2.chk.drivers.rfpulse import RfPulseDriver
from v2.chk.system import DEVICES, ENABLE_KEY, serial_settings

TEXT_DEVICES = ("dc", "ig", "mfc")                    # 녹화를 글자로 보여 줄 장비(나머지는 hex)
RANGE_KEYS = ("rf.fwd_w", "rf.ref_w", "rf.dac", "dc.v", "dc.i", "mfc.ar_sccm", "mfc.o2_sccm", "mfc.pressure_mtorr",
              "ig.ion_torr", "ig.a_torr", "ig.b_torr")
NO_REPLY_PAUSE_S = 0.3
BANNER = """
==================================================================
  챔버K v2 bench — 읽기 전용(RUN_MODE=bench)
  장비에 아무것도 쓰지 않습니다(하트비트·설정·필라멘트 포함).
  v1 프로그램을 끈 상태에서 실행하세요. 멈추려면 Ctrl+C.
==================================================================""".strip("\n")


def _settings(cfg: Dict[str, Any], ports: Dict[str, str]) -> Dict[str, SerialSettings]:
    return serial_settings(cfg, ports)                  # 포트 설정은 조립(v2.chk.system)과 같은 것


def _pct(xs: List[float], p: float) -> float:
    s = sorted(xs)
    if not s:
        return float("nan")
    return s[min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))]


def _full(rx: str, raw: str) -> bool:
    return re.fullmatch(rx, raw.strip()) is not None


def _ev_json(e) -> dict:
    out = {"type": type(e).__name__}
    for k in ("device", "kind", "detail", "reason", "t"):
        v = getattr(e, k, None)
        if v is not None:
            out[k] = v if isinstance(v, (str, int, float, bool)) else repr(v)
    return out


class BenchProbe:
    def __init__(self, ports: Dict[str, str], out_root: Path, serial_factory: Optional[Callable] = None,
                 print_fn: Callable[[str], None] = print, cfg_over: Optional[Dict[str, Any]] = None,
                 skip: tuple = ()) -> None:
        self.clock = RealClock()
        self.cfg = dict(defaults(P.PARAMS), **(cfg_over or {}), RUN_MODE="bench")
        self.excluded: Dict[str, str] = {}
        for k in DEVICES:
            if k in ENABLE_KEY and not self.cfg[ENABLE_KEY[k]]:
                self.excluded[k] = f"제외(설정 꺼짐 — {ENABLE_KEY[k]}=false)"
            elif k in skip:
                self.excluded[k] = "제외(--skip)"
        self.active = [k for k in DEVICES if k not in self.excluded]
        self.ports = ports
        self.out = Path(out_root) / _dt.datetime.now().strftime("v2_bench_%Y%m%d_%H%M%S")
        self.out.mkdir(parents=True, exist_ok=True)
        self._events_f = open(self.out / "events.jsonl", "a", encoding="utf-8", newline="\n")
        self.p = print_fn
        self.events: List[Any] = []
        self.lines: List[str] = []
        self.t0 = self.clock.monotonic()
        c = self.cfg
        st = _settings(c, ports)
        # 녹화 → 가드 → 포트: 가드는 포트 바로 위(막힌 바이트는 포트에도 녹화에도 닿지 않는다 — 이벤트로 남음)
        self.guards = {k: G.GuardedTransport(SerialTransport(self.clock, st[k], open_timeout_s=c["PORT_OPEN_TIMEOUT_SEC"],
                                                             serial_factory=serial_factory), k, "bench", c, on_event=self._ev)
                       for k in DEVICES}
        self.rec = {k: RecordingTransport(self.guards[k]) for k in DEVICES}
        streak = {"plc": "PLC_FAIL_STREAK", "mfc": "MFC_FAIL_STREAK", "dc": "DC_FAIL_STREAK",
                  "rfpulse": "RFPULSE_FAIL_STREAK", "ig": "IG_FAIL_STREAK"}
        self.w = {k: Worker(k, self.rec[k], writes_allowed=False, fail_streak=c[streak[k]],
                            connect_init_timeout_s=c["CONNECT_INIT_TIMEOUT_SEC"], on_event=self._ev) for k in DEVICES}
        self.plc = PlcDriver(self.clock, self.w["plc"], c, False, on_event=self._ev)
        self.mfc = MfcDriver(self.clock, self.w["mfc"], c, False, on_event=self._ev)
        self.dc = DcDriver(self.clock, self.w["dc"], c, False, on_event=self._ev)
        self.rfp = RfPulseDriver(self.clock, self.w["rfpulse"], c, False, on_event=self._ev)
        self.ig = IgDriver(self.clock, self.w["ig"], c, False, on_event=self._ev)
        self.recorder: Optional[Recorder] = None
        self.loop: Optional[TickLoop] = None
        self.rows: List[Dict[str, Any]] = []
        self.started = False
        self.stop_mono: Optional[float] = None      # 멈추기 시작한 시각 — 그 뒤에 끊긴 요청은 '종료 취소'로 센다
        self.inv = INV.Inventory()
        self.first_poll = None
        self.notes: List[str] = []

    def _ev(self, e) -> None:
        self.events.append(e)
        try:
            j = _ev_json(e)
            j["t_s"] = round(self.clock.monotonic() - self.t0, 3)
            self._events_f.write(json.dumps(j, ensure_ascii=False) + "\n")
            self._events_f.flush()
        except Exception:
            pass
        if self.recorder is not None:
            self.recorder.put_event(iso_local(self.clock.wall()), "warn" if getattr(e, "reason", None) else "info",
                                    (getattr(e, "reason", None) or getattr(e, "kind", "?")).upper(),
                                    getattr(e, "device", type(e).__name__), getattr(e, "detail", ""))

    def say(self, s: str = "") -> None:
        self.lines.append(s)
        self.p(s)

    # ───────── 단계 ─────────
    def open_ports(self) -> bool:
        opened = []
        for k in self.active:
            try:
                self.rec[k].open()
                opened.append(k)
            except TransportError as e:
                self.say(f"[중지] {k} 포트 {self.ports[k]} 를 열 수 없음: {e}")
                self.say("        v1 프로그램이 켜져 있거나 포트 번호가 다를 수 있습니다. 다시 시도하지 않고 끝냅니다.")
                for o in opened:
                    self.rec[o].close()
                return False
        self.say("포트 열기: " + ", ".join(f"{k}={self.ports[k]}" for k in self.active)
                 + "".join(f" · {k} {why}" for k, why in self.excluded.items()))
        return True

    def start_workers(self) -> Dict[str, bool]:
        with self.clock.hold():
            for k in self.active:
                self.w[k].start()
        self.started = True
        self.clock.wait_until(self.clock.monotonic() + self.cfg["CONNECT_INIT_TIMEOUT_SEC"] + 3.0,
                              lambda: all(self.w[k].ready for k in self.active))
        return {k: (self.w[k].ready if k in self.active else False) for k in DEVICES}

    def _read_once(self, k: str, fn, wait_s: float = 4.0):
        """(값, 상태). 무응답·실패면 값 None."""
        box = []
        self.w[k].submit(Request("read", fn, deadline=self.clock.monotonic() + 2.0,
                                 on_done=lambda r: (box.append(r), self.clock.notify())))
        self.clock.wait_until(self.clock.monotonic() + wait_s, lambda: bool(box))
        if box and box[0].status == Status.OK:
            return box[0].value, Status.OK
        return None, (box[0].status if box else "timeout")

    def safety_check(self, ready: Dict[str, bool]) -> Optional[str]:
        """멈춰야 하면 이유, 아니면 None. 읽기만 한다. RF 값은 판정과 상관없이 출력."""
        plc, _ = self._read_once("plc", lambda s: self.plc.source_read(s, 0)) if ready.get("plc") else (None, None)
        if plc is None:
            return "PLC 를 읽을 수 없음 — 히터 RUN 상태를 확인할 수 없어 멈춥니다"
        self.first_poll = self.plc.last
        dac, fwd, ref, en = plc.get("rf.dac"), plc.get("rf.fwd_w"), plc.get("rf.ref_w"), plc.get("rf.dac_enable")
        self.say(f"RF(PLC 아날로그): D40 {dac} · 순방향 {fwd} W · 반사 {ref} W · enable(M00200) {int(bool(en))}")
        if plc["heater.run"]:
            return ("PLC 히터 RUN 이 켜져 있음 — bench 는 하트비트를 쓰지 않아 10 s 뒤 PLC 가 히터를 끕니다. "
                    "히터를 끈 뒤 다시 실행하세요")
        if (dac or 0) > 0 or (fwd or 0.0) >= 5.0:
            return f"RF 출력이 있음(D40 {dac} · 순방향 {fwd} W) — 공정 진행 중으로 보입니다"
        if en:
            self.notes.append(f"[참고] M00200 켜짐 · D40 {dac} · 순방향 {fwd} W — v1 정상 종료 흔적(출력 없음)")
            self.say(self.notes[-1])
        if ready.get("dc"):
            outp, _ = self._read_once("dc", lambda s: self.dc._query(s, "OUTP?"))
            if outp == "1":
                return "DC 출력(OUTP?)이 켜져 있음 — 공정 진행 중으로 보입니다"
        if ready.get("rfpulse"):
            r, _ = self._read_once("rfpulse", lambda s: self.rfp.source_read(s, 0))
            if r and r.get("rfp.out_on"):
                return "RF Pulse 출력이 켜져 있음 — 공정 진행 중으로 보입니다"
        if ready.get("mfc"):
            m, _ = self._read_once("mfc", lambda s: self.mfc.source_read(s, 0))
            if m and max(m.get("mfc.ar_sccm") or 0.0, m.get("mfc.o2_sccm") or 0.0) > 0.1:
                return f"가스 유량이 있음(Ar {m.get('mfc.ar_sccm')} · O2 {m.get('mfc.o2_sccm')} sccm) — 공정 진행 중으로 보입니다"
        return None

    # ───────── 설정 전부 읽기 ─────────
    def _q(self, k: str, fn):
        v, _ = self._read_once(k, fn)
        if v is None:
            self.clock.wait_until(self.clock.monotonic() + NO_REPLY_PAUSE_S)
        return v

    def inventory(self, ready: Dict[str, bool]) -> None:
        inv = self.inv
        if ready.get("dc"):
            for q in G.DC_BENCH:
                if q == "SYST:ERR?":
                    continue
                raw = self._q("dc", lambda s, q=q: self.dc._query(s, q))
                rx, mean = INV.DC_FORMAT[q]
                inv.items.append(INV.Item("dc", q, raw, raw is None or _full(rx, raw), mean))
            for n in range(10):                          # +0 이 나올 때까지(최대 10 — 읽은 오류는 지워진다 [p.96])
                raw = self._q("dc", lambda s: self.dc._query(s, "SYST:ERR?"))
                inv.items.append(INV.Item("dc", f"SYST:ERR? {n + 1}", raw,
                                          raw is None or _full(INV.DC_FORMAT["SYST:ERR?"][0], raw), INV.DC_FORMAT["SYST:ERR?"][1]))
                if raw is None or raw.startswith(("+0,", "0,")):
                    break
            INV.dc_interpret(inv)
        if ready.get("mfc"):
            for n in G.MFC_BENCH_R:
                raw = self._q("mfc", lambda s, n=n: self.mfc._query(s, f"R{n}"))
                inv.items.append(INV.Item("mfc", f"R{n}", raw, raw is None or INV.mfc_format_ok(n, raw),
                                          INV.MFC_FORMAT.get(n, ("", ""))[1]))
            INV.mfc_interpret(inv, self.cfg)
        if ready.get("ig"):
            lines = self._q("ig", lambda s: self.ig._query(s, "E"))
            if lines is None:
                inv.items.append(INV.Item("ig", "E", None))
            else:
                ok = len(lines) >= E_ITEM_COUNT
                for i in range(1, E_ITEM_COUNT + 1):
                    raw = lines[i - 1] if i - 1 < len(lines) else None
                    inv.items.append(INV.Item("ig", f"E{i}", raw, ok, INV.IG_ITEMS[i],
                                              INV.ig_meaning(i, raw) if raw is not None else ""))
            for ch in ("F", "G", "H"):
                got = self._q("ig", lambda s, ch=ch: self.ig._query(s, ch))
                raw = got[0] if got else None
                okf = raw is None or INV.ig_pair(raw, False) is not None or raw.strip() in ("0", "-900", "-999")
                inv.items.append(INV.Item("ig", ch, raw, okf, {"F": "이온 압력", "G": "게이지 A 압력", "H": "게이지 B 압력"}[ch],
                                          INV.ig_meaning({"F": 24, "G": 25, "H": 26}[ch], raw) if raw is not None else ""))
            INV.ig_interpret(inv)
        if self.first_poll is not None:
            fp = self.first_poll
            for a in D.PLC_ADDRS:
                raw = fp.coils.get(a.addr) if a.kind == "coil" else (fp.regs[a.addr] if a.addr < len(fp.regs) else None)
                if raw is None:
                    continue
                inv.items.append(INV.Item("plc", a.plc, str(int(raw)), True, f"{a.name} — {a.desc}",
                                          _plc_value(fp.values, a.name)))
        (self.out / "inventory.json").write_text(json.dumps(inv.as_json(), ensure_ascii=False, indent=1), encoding="utf-8")

    def print_inventory(self) -> None:
        self.say("---------------- 장비 설정(읽은 값) ----------------")
        for dev in ("dc", "mfc", "ig", "plc"):
            items = [i for i in self.inv.items if i.device == dev]
            if not items:
                continue
            self.say(f"[{dev}] {len(items)}개")
            for i in items:
                raw = "(무응답)" if i.raw is None else repr(i.raw)
                fmt = "" if i.ok_format else "  ← 형식 다름(원문 그대로)"
                extra = f" → {i.note}" if i.note else ""
                self.say(f"  {i.query:<18} {raw:<34} {i.meaning}{extra}{fmt}")
        self.say("---------------- 해석·대조 ----------------")
        for line in self.inv.checks:
            self.say(line)
        self.say("---------------- 명령으로 확인할 수 없는 것 ----------------")
        for line in INV.UNVERIFIABLE:
            self.say(f"[미확인] {line}")

    def run_ticks(self, seconds: int) -> None:
        c = self.cfg
        bus = SnapshotBus(self.clock, SF.BLOCKS, [
            Source("plc", self.w["plc"], self.plc.source_read, ("plc", "heater", "rf"), c["PLC_READ_DEADLINE_MS"]),
            Source("mfc", self.w["mfc"], self.mfc.source_read, ("mfc",), c["MFC_READ_DEADLINE_MS"], enabled="mfc" in self.active),
            Source("dc", self.w["dc"], self.dc.source_read, ("dc",), c["DC_READ_DEADLINE_MS"], enabled="dc" in self.active),
            Source("rfpulse", self.w["rfpulse"], self.rfp.source_read, ("rfp",), c["RFPULSE_READ_DEADLINE_MS"],
                   enabled="rfpulse" in self.active),
            Source("ig", self.w["ig"], self.ig.source_read, ("ig",), c["IG_RESPONSE_LIMIT_MS"], enabled="ig" in self.active),
        ], c["TICK_DEADLINE_MS"], SF.PROCESS_FIELDS)
        self.recorder = Recorder(self.out, bus.columns, retention_days=c["SNAPSHOT_RETENTION_DAYS"])
        self.recorder.start()
        fast = PlcFastPoller(self.clock, self.plc, c["PLC_FAST_POLL_MS"])
        fast.start()
        self.loop = TickLoop(self.clock, bus, [self.recorder.put_row, self.rows.append],
                             tick_hooks=[lambda t: (self.plc.on_tick(t), self.ig.on_tick(t))])
        self.say(f"수집 시작 — {seconds} s (Ctrl+C 로 멈춤)")
        try:
            self.loop.run(seconds)
        except KeyboardInterrupt:
            self.say("Ctrl+C — 멈춤. 요약을 남깁니다")
        finally:
            fast.stop()

    def stop(self) -> None:
        self.stop_mono = self.clock.monotonic()
        if self.started:
            for k in self.active:
                self.w[k].stop()
        for r in [self.rec[k] for k in self.active]:
            try:
                r.close()
            except Exception:
                pass
        if self.recorder is not None:
            self.recorder.stop()
            for day in sorted({r["ts"][:10].replace("-", "") for r in self.rows}):
                try:
                    self.recorder.export_csv(day)
                except Exception as e:
                    self.say(f"CSV 내보내기 실패 {day}: {e!r}")

    def _cut_at_stop(self, k: str) -> int:
        """응답이 없는 마지막 요청 중 멈추기 시작한 뒤에도 아직 응답 기다리는 중이던 것(종료 때 취소) 개수."""
        rec = self.rec[k]
        if self.stop_mono is None or rec._t0 is None:
            return 0
        limit = 2.0                                  # 질의 하나의 최대 기다림보다 넉넉히(IG 1.2 s)
        stop_rel = self.stop_mono - rec._t0
        n = 0
        for e in reversed(rec.exchanges):
            if e.rx or e.t_s < stop_rel - limit:
                break
            n += 1
        return n

    def _dump(self, k: str, n: int = 8) -> None:
        self.say(f"  녹화 앞부분 {k}(최대 {n}개): 보낸 시각 · 보낸 것 → 받은 것 · 지연")
        for e in self.rec[k].exchanges[:n]:
            if k in TEXT_DEVICES:
                tx, rx = repr(e.tx.decode("ascii", errors="replace")), repr(e.rx.decode("ascii", errors="replace"))
            else:
                tx, rx = e.tx.hex(" "), (e.rx.hex(" ") or "(없음)")
            self.say(f"    {e.t_s:8.3f}s  {tx}  →  {rx}  {e.delay_s * 1000:.1f} ms")

    # ───────── 요약 ─────────
    def summary(self, status: str) -> None:
        self.say("")
        self.say("================ 요약 ================")
        self.say(f"결과: {status}")
        self.say(f"기록 폴더: {self.out}")
        stats, problem = {}, []
        for k in DEVICES:
            if k in self.excluded:
                stats[k] = []
                self.say(f"응답 {k:8s}: {self.excluded[k]}")
                continue
            ex = self.rec[k].exchanges
            save_exchanges(self.out / f"rec_{k}.jsonl", ex)
            d = [e.delay_s * 1000 for e in ex if e.rx]
            cut = self._cut_at_stop(k)
            none = sum(1 for e in ex if not e.rx) - cut
            stats[k] = d
            if none > 0 or not self.w[k].ready:
                problem.append(k)
            if d:
                self.say(f"응답 {k:8s}: {len(d)}회  p50 {_pct(d, 50):.1f} ms  p99 {_pct(d, 99):.1f} ms  "
                         f"최대 {max(d):.1f} ms  무응답 {none}" + (f"  (종료 때 끊김 {cut} — 무응답 아님)" if cut else ""))
            else:
                self.say(f"응답 {k:8s}: 응답 없음(보낸 {len(ex)}회)")
        if self.loop is not None:
            self.say(f"tick: {self.loop.rows}행, 놓친 tick {self.loop.missed}, "
                     f"jitter 최대 {max(self.loop.jitters, default=0):.1f} ms")
            for b in SF.BLOCKS:
                cnt = collections.Counter(r[f"{b.name}.q"] for r in self.rows)
                self.say(f"품질 {b.name:7s}: " + ", ".join(f"{q}={n}" for q, n in sorted(cnt.items())))
            self.say("---------------- 대기 중 값 범위(최소 ~ 최대) ----------------")
            for key in RANGE_KEYS:
                vals = [r.get(key) for r in self.rows if r.get(key) is not None]
                if vals:
                    self.say(f"  {key:<20} {min(vals):g} ~ {max(vals):g}  ({len(vals)}개)")
        if self.inv.items:
            self.print_inventory()
        for n in self.notes:
            self.say(n)
        self.say("---------------- 연결 기록 ----------------")
        conn = [e for e in self.events if type(e).__name__ in ("WorkerEvent", "GuardEvent")]
        for e in conn[:40]:
            self.say(f"  {getattr(e, 'device', '?'):8s} {e.kind:<16} {getattr(e, 'detail', '')}")
        if len(conn) > 40:
            self.say(f"  … {len(conn) - 40}건 더(events.jsonl)")
        for k in problem:
            self._dump(k)
        self.say("---------------- 확인 항목 ----------------")
        fs_ok = self.mfc.fs_source == "R70/R71"
        self.say(f"MFC FS: Ar {self.mfc.fs[1]:g} · O2 {self.mfc.fs[2]:g} sccm ({'R70/R71 로 읽음' if fs_ok else '못 읽음 — 파라미터 값'})")
        if any(abs(v - 100.0) > 1e-9 for v in self.mfc.fs.values()):
            self.say("!!!!!!!! 경고: MFC 풀스케일이 100 이 아닙니다 — v1 레시피 유량이 %FS 였을 수 있습니다.")
            self.say("!!!!!!!! 실제 공정 전에 레시피 유량 단위를 확인하세요.")
        self.say(f"DC *IDN?: {self.dc.model}")
        self.say(f"DC FACT:LAST-STA?: {self.dc.last_state}")
        ev = lambda cls, kind: [e for e in self.events if type(e).__name__ == cls and e.kind == kind]
        for e in ev("DcEvent", "syst_err"):
            self.say(f"DC SYST:ERR?(연결 때): {e.detail}")
        for e in ev("RfpEvent", "identity") + ev("RfpEvent", "auto_off_read"):
            self.say(f"CESAR: {e.detail}")
        for e in ev("IgEvent", "identity_ok"):
            self.say(f"IG {e.detail}")
        r = self.ig.last
        if r is not None:
            self.say(f"IG E: 이온 {r.raw[0]!r} · A {r.raw[1]!r} · B {r.raw[2]!r} · 필라멘트 {int(r.filament)} · 오류 {r.err_code}")
            self.say(f"IG: 이온 {pressure_text(r.ion_torr, r.ion_state)} · 게이지 A {pressure_text(r.a_torr, r.a_state)}"
                     f" · 게이지 B {pressure_text(r.b_torr, r.b_state)}")
        plc_ok = sum(1 for x in self.rows if x["plc.q"] == "ok")
        self.say(f"PLC 블록 읽기(FC1 0..95·FC1 160..320·FC3 D0..D60): 정상 {plc_ok}/{len(self.rows)} tick")
        self.say("---------------- devices.ASSUMPTIONS ----------------")
        inv = self.inv
        chk = lambda word: next((c.split("] ", 1)[-1] for c in inv.checks if word in c and c.startswith("[확인]")), "")
        measured = {
            "MFC 유량 풀스케일": (bool(chk("MFC ch1 FS")) and bool(chk("MFC ch2 FS")), f"{chk('MFC ch1 FS')} / {chk('MFC ch2 FS')}"),
            "MFC 압력 레인지·작업 압력 상한": (bool(chk("MFC 압력 레인지")), chk("MFC 압력 레인지")),
            "DC 형명·정격": ("dc_ovl" in inv.values, f"*IDN? {self.dc.model} · OVL {inv.values.get('dc_ovl')} V · "
                                                  f"OCL {inv.values.get('dc_ocl')} A"),
            "DC Delimiter": (bool(stats["dc"]), "LF 로 응답 받음" if stats["dc"] else "응답 없음"),
            "DC LAST-STA 현재 값": (self.dc.last_state is not None, f"{self.dc.last_state}"),
            "CESAR 응답 지연": (bool(stats["rfpulse"]), f"p99 {_pct(stats['rfpulse'], 99):.0f} ms"),
            "PLC 트랜잭션 왕복": (bool(stats["plc"]), f"p50 {_pct(stats['plc'], 50):.0f} · p99 {_pct(stats['plc'], 99):.0f} ms"),
            "이온 게이지 E 응답 분포": (bool(stats["ig"]), f"p50 {_pct(stats['ig'], 50):.0f} · 최대 {max(stats['ig'], default=0):.0f} ms"),
        }
        for a in D.ASSUMPTIONS:
            m = measured.get(a.item)
            if m and m[0]:
                self.say(f"[확인] {a.item}: {m[1]}  (가정: {a.assumed})")
            else:
                self.say(f"[미확인] {a.item} — 가정: {a.assumed}")
        (self.out / "summary.txt").write_text("\n".join(self.lines) + "\n", encoding="utf-8")
        try:
            self._events_f.close()
        except Exception:
            pass


def _plc_value(values: Dict[str, Any], name: str) -> str:
    """PLC 주소 이름 → 스냅샷 열 값(환산값). 매핑이 없으면 빈 문자열."""
    cands = (f"plc.{name}", f"heater.{name[len('heater_'):]}" if name.startswith("heater_") else "",
             f"rf.{name[len('rf_'):]}" if name.startswith("rf_") else "")
    for k in cands:
        if k and k in values:
            return f"{values[k]}"
    return ""


def run(seconds: int, ports: Dict[str, str], out_root: Path, serial_factory=None, print_fn=print,
        cfg_over: Optional[Dict[str, Any]] = None, skip: tuple = ()) -> int:
    print_fn(BANNER)
    b = BenchProbe(ports, out_root, serial_factory, print_fn, cfg_over, skip)
    b.say("읽기 전용 가드 켜짐 — 장비별 허용 질의 수: " + " · ".join(
        f"{k} {'FC1·FC3' if k == 'plc' else G.allowed_query_count(k)}" for k in DEVICES))
    code, status = 0, "정상 종료"
    try:
        if not b.open_ports():
            code, status = 3, "포트 열기 실패 — 장비에 아무것도 쓰지 않고 종료"
        else:
            ready = b.start_workers()
            not_ready = [k for k in b.active if not ready[k]]
            if not_ready:
                b.say("연결 안 된 장비(응답 없음·신원 확인 실패): " + ", ".join(not_ready))
            why = b.safety_check(ready)
            if why:
                b.say(f"[중지] {why}")
                code, status = 2, "안전 확인에서 멈춤 — 장비에 아무것도 쓰지 않고 종료"
            else:
                b.say("안전 확인 통과 — 히터 RUN 꺼짐, RF·DC·가스 출력 없음")
                b.say("장비 설정 읽기 …")
                b.inventory(ready)
                b.run_ticks(seconds)
    except KeyboardInterrupt:
        status = "Ctrl+C 로 멈춤"
    finally:
        b.stop()
        b.summary(status)
    return code


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="챔버K v2 bench 실측(읽기 전용)")
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--seconds", type=int, default=None, help="--minutes 대신 초 단위")
    cfg = defaults(P.PARAMS)
    for k, key in (("plc", "PLC_PORT"), ("mfc", "MFC_PORT"), ("dc", "DC_PORT"), ("ig", "IG_PORT"), ("rfpulse", "RFPULSE_PORT")):
        ap.add_argument(f"--{k}", default=cfg[key])
    ap.add_argument("--out", default=str(Path.home() / "Desktop"), help="기록 폴더를 만들 곳(기본: 바탕화면)")
    ap.add_argument("--skip", action="append", default=[], choices=DEVICES, help="이 장비는 포트를 열지 않는다(여러 번 가능)")
    a = ap.parse_args(argv)
    seconds = a.seconds if a.seconds is not None else int(round(a.minutes * 60))
    ports = {"plc": a.plc, "mfc": a.mfc, "dc": a.dc, "ig": a.ig, "rfpulse": a.rfpulse}
    return run(seconds, ports, Path(a.out), skip=tuple(a.skip))


if __name__ == "__main__":
    sys.exit(main())
