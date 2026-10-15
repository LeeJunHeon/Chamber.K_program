"""bench 실측 도구 — 장비 노트북에서 v1 을 끈 상태로, 실제 장비 5대를 읽기 전용으로 붙여 1초 수집·응답 시간을 잰다.

    python -m v2.tools.bench_probe --minutes 10
    (포트 바꾸기: --plc COM9 --mfc COM10 --dc COM11 --ig COM12 --rfpulse COM13, 기록 폴더: --out <폴더>)

- RUN_MODE=bench 고정: 장비에 아무것도 쓰지 않는다(하트비트·연결 시 설정·필라멘트 포함). worker 가 쓰기를 거부하고
  드라이버도 읽기 전용이다(이중 보호).
- 시작 전 안전 확인 — 하나라도 걸리면 바로 끝낸다(장비에는 아무것도 쓰지 않음):
  · 포트를 열 수 없으면(v1 이 켜져 있을 가능성) 다시 시도하지 않고 종료
  · PLC 를 읽을 수 없거나, 첫 읽기에서 히터 RUN 이 켜져 있으면 종료 — bench 는 하트비트를 쓰지 않아 10 s 뒤 PLC 가 히터를 끈다
  · 공정 흔적(DC 출력 ON, RF Pulse 출력 ON, RF DAC enable ON, 가스 유량 > 0.1 sccm)이 보이면 종료
- 기록: 바탕화면\\v2_bench_YYYYMMDD_HHMMSS — 스냅샷(SQLite·CSV), 통신 녹화(장비별 *.jsonl), 이벤트, summary.txt.
  NAS·C:\\ChamberK_v2 에는 쓰지 않는다.
- Ctrl+C 로 언제든 멈출 수 있고, 멈춰도 요약은 남긴다.
종료 코드: 0 정상, 2 안전 확인에서 멈춤, 3 포트 열기 실패.
"""
from __future__ import annotations

import argparse
import collections
import datetime as _dt
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import RealClock
from v2.common.paramdef import defaults
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop, iso_local
from v2.common.transport import RecordingTransport, SerialSettings, SerialTransport, TransportError, save_exchanges
from v2.common.worker import Request, Status, Worker
from v2.chk import devices as D
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk.drivers.dc import DcDriver
from v2.chk.drivers.ig import IgDriver
from v2.chk.drivers.mfc import MfcDriver
from v2.chk.drivers.plc import PlcDriver, PlcFastPoller
from v2.chk.drivers.rfpulse import RfPulseDriver

DEVICES = ("plc", "mfc", "dc", "rfpulse", "ig")
BANNER = """
==================================================================
  챔버K v2 bench — 읽기 전용(RUN_MODE=bench)
  장비에 아무것도 쓰지 않습니다(하트비트·설정·필라멘트 포함).
  v1 프로그램을 끈 상태에서 실행하세요. 멈추려면 Ctrl+C.
==================================================================""".strip("\n")


def _settings(cfg: Dict[str, Any], ports: Dict[str, str]) -> Dict[str, SerialSettings]:
    par = {"none": "N", "odd": "O", "even": "E"}[cfg["RFPULSE_PARITY"]]
    return {"plc": SerialSettings(ports["plc"], cfg["PLC_BAUD"]),
            "mfc": SerialSettings(ports["mfc"], cfg["MFC_BAUD"]),
            "dc": SerialSettings(ports["dc"], cfg["DC_BAUD"]),
            "rfpulse": SerialSettings(ports["rfpulse"], cfg["RFPULSE_BAUD"], parity=par),
            "ig": SerialSettings(ports["ig"], cfg["IG_BAUD"])}


def _pct(xs: List[float], p: float) -> float:
    s = sorted(xs)
    if not s:
        return float("nan")
    return s[min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))]


class BenchProbe:
    def __init__(self, ports: Dict[str, str], out_root: Path, serial_factory: Optional[Callable] = None,
                 print_fn: Callable[[str], None] = print) -> None:
        self.clock = RealClock()
        self.cfg = dict(defaults(P.PARAMS), RUN_MODE="bench")
        self.ports = ports
        self.out = Path(out_root) / _dt.datetime.now().strftime("v2_bench_%Y%m%d_%H%M%S")
        self.out.mkdir(parents=True, exist_ok=True)
        self.p = print_fn
        self.events: List[Any] = []
        self.lines: List[str] = []
        c = self.cfg
        st = _settings(c, ports)
        self.rec = {k: RecordingTransport(SerialTransport(self.clock, st[k], open_timeout_s=c["PORT_OPEN_TIMEOUT_SEC"],
                                                          serial_factory=serial_factory)) for k in DEVICES}
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

    def _ev(self, e) -> None:
        self.events.append(e)
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
        for k in DEVICES:
            try:
                self.rec[k].open()
                opened.append(k)
            except TransportError as e:
                self.say(f"[중지] {k} 포트 {self.ports[k]} 를 열 수 없음: {e}")
                self.say("        v1 프로그램이 켜져 있거나 포트 번호가 다를 수 있습니다. 다시 시도하지 않고 끝냅니다.")
                for o in opened:
                    self.rec[o].close()
                return False
        self.say("포트 열기: " + ", ".join(f"{k}={self.ports[k]}" for k in DEVICES))
        return True

    def start_workers(self) -> Dict[str, bool]:
        with self.clock.hold():
            for x in self.w.values():
                x.start()
        self.started = True
        self.clock.wait_until(self.clock.monotonic() + self.cfg["CONNECT_INIT_TIMEOUT_SEC"] + 3.0,
                              lambda: all(x.ready for x in self.w.values()))
        return {k: x.ready for k, x in self.w.items()}

    def _read_once(self, k: str, fn) -> Optional[Dict[str, Any]]:
        box = []
        self.w[k].submit(Request("read", fn, deadline=self.clock.monotonic() + 2.0,
                                 on_done=lambda r: (box.append(r), self.clock.notify())))
        self.clock.wait_until(self.clock.monotonic() + 4.0, lambda: bool(box))
        if box and box[0].status == Status.OK:
            return box[0].value
        return None

    def safety_check(self, ready: Dict[str, bool]) -> Optional[str]:
        """멈춰야 하면 이유, 아니면 None. 읽기만 한다."""
        plc = self._read_once("plc", lambda s: self.plc.source_read(s, 0)) if ready["plc"] else None
        if plc is None:
            return "PLC 를 읽을 수 없음 — 히터 RUN 상태를 확인할 수 없어 멈춥니다"
        if plc["heater.run"]:
            return ("PLC 히터 RUN 이 켜져 있음 — bench 는 하트비트를 쓰지 않아 10 s 뒤 PLC 가 히터를 끕니다. "
                    "히터를 끈 뒤 다시 실행하세요")
        if plc["rf.dac_enable"]:
            return "RF DAC enable(M00200)이 켜져 있음 — 공정 진행 중으로 보입니다"
        if ready["dc"]:
            outp = self._read_once("dc", lambda s: self.dc._query(s, "OUTP?"))
            if outp == "1":
                return "DC 출력(OUTP?)이 켜져 있음 — 공정 진행 중으로 보입니다"
        if ready["rfpulse"]:
            r = self._read_once("rfpulse", lambda s: self.rfp.source_read(s, 0))
            if r and r.get("rfp.out_on"):
                return "RF Pulse 출력이 켜져 있음 — 공정 진행 중으로 보입니다"
        if ready["mfc"]:
            m = self._read_once("mfc", lambda s: self.mfc.source_read(s, 0))
            if m and max(m.get("mfc.ar_sccm") or 0.0, m.get("mfc.o2_sccm") or 0.0) > 0.1:
                return f"가스 유량이 있음(Ar {m.get('mfc.ar_sccm')} · O2 {m.get('mfc.o2_sccm')} sccm) — 공정 진행 중으로 보입니다"
        return None

    def run_ticks(self, seconds: int) -> None:
        c = self.cfg
        bus = SnapshotBus(self.clock, SF.BLOCKS, [
            Source("plc", self.w["plc"], self.plc.source_read, ("plc", "heater", "rf"), c["PLC_READ_DEADLINE_MS"]),
            Source("mfc", self.w["mfc"], self.mfc.source_read, ("mfc",), c["MFC_READ_DEADLINE_MS"]),
            Source("dc", self.w["dc"], self.dc.source_read, ("dc",), c["DC_READ_DEADLINE_MS"]),
            Source("rfpulse", self.w["rfpulse"], self.rfp.source_read, ("rfp",), c["RFPULSE_READ_DEADLINE_MS"]),
            Source("ig", self.w["ig"], self.ig.source_read, ("ig",), c["IG_RESPONSE_LIMIT_MS"]),
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
        if self.started:
            for x in self.w.values():
                x.stop()
        for r in self.rec.values():
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

    # ───────── 요약 ─────────
    def summary(self, status: str) -> None:
        self.say("")
        self.say("================ 요약 ================")
        self.say(f"결과: {status}")
        self.say(f"기록 폴더: {self.out}")
        stats = {}
        for k in DEVICES:
            ex = self.rec[k].exchanges
            save_exchanges(self.out / f"rec_{k}.jsonl", ex)
            d = [e.delay_s * 1000 for e in ex if e.rx]
            none = sum(1 for e in ex if not e.rx)
            stats[k] = d
            if d:
                self.say(f"응답 {k:8s}: {len(d)}회  p50 {_pct(d, 50):.1f} ms  p99 {_pct(d, 99):.1f} ms  "
                         f"최대 {max(d):.1f} ms  무응답 {none}")
            else:
                self.say(f"응답 {k:8s}: 응답 없음(보낸 {len(ex)}회)")
        if self.loop is not None:
            self.say(f"tick: {self.loop.rows}행, 놓친 tick {self.loop.missed}, "
                     f"jitter 최대 {max(self.loop.jitters, default=0):.1f} ms")
            for b in SF.BLOCKS:
                cnt = collections.Counter(r[f"{b.name}.q"] for r in self.rows)
                self.say(f"품질 {b.name:7s}: " + ", ".join(f"{q}={n}" for q, n in sorted(cnt.items())))
        self.say("---------------- 확인 항목 ----------------")
        ev = lambda cls, kind: [e for e in self.events if type(e).__name__ == cls and e.kind == kind]
        fs_ok = self.mfc.fs_source == "R70/R71"
        self.say(f"MFC FS: Ar {self.mfc.fs[1]:g} · O2 {self.mfc.fs[2]:g} sccm ({'R70/R71 로 읽음' if fs_ok else '못 읽음 — 파라미터 값'})")
        if any(abs(v - 100.0) > 1e-9 for v in self.mfc.fs.values()):
            self.say("!!!!!!!! 경고: MFC 풀스케일이 100 이 아닙니다 — v1 레시피 유량이 %FS 였을 수 있습니다.")
            self.say("!!!!!!!! 실제 공정 전에 레시피 유량 단위를 확인하세요.")
        self.say(f"DC *IDN?: {self.dc.model}")
        for e in ev("DcEvent", "syst_err"):
            self.say(f"DC SYST:ERR?: {e.detail}")
        for e in ev("RfpEvent", "identity") + ev("RfpEvent", "auto_off_read"):
            self.say(f"CESAR: {e.detail}")
        for e in ev("IgEvent", "identity_ok"):
            self.say(f"IG {e.detail}")
        r = self.ig.last
        if r is not None:
            self.say(f"IG E: 이온 {r.raw[0]!r} · A {r.raw[1]!r} · B {r.raw[2]!r} · 필라멘트 {int(r.filament)} · 오류 {r.err_code}")
        plc_ok = sum(1 for x in self.rows if x["plc.q"] == "ok")
        self.say(f"PLC 블록 읽기(FC1 0..95·FC1 160..320·FC3 D0..D60): 정상 {plc_ok}/{len(self.rows)} tick")
        self.say("---------------- devices.ASSUMPTIONS ----------------")
        measured = {
            "MFC 유량 풀스케일": (fs_ok, f"Ar {self.mfc.fs[1]:g} · O2 {self.mfc.fs[2]:g}"),
            "MFC 명령 간격": (bool(stats["mfc"]), f"응답 p99 {_pct(stats['mfc'], 99):.0f} ms (간격 자체는 쓰기 없이 확인 불가)"),
            "DC 형명·정격": (self.dc.model is not None, f"{self.dc.model}"),
            "DC Delimiter": (bool(stats["dc"]), "LF 로 응답 받음" if stats["dc"] else "응답 없음"),
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


def run(seconds: int, ports: Dict[str, str], out_root: Path, serial_factory=None, print_fn=print) -> int:
    print_fn(BANNER)
    b = BenchProbe(ports, out_root, serial_factory, print_fn)
    code, status = 0, "정상 종료"
    try:
        if not b.open_ports():
            code, status = 3, "포트 열기 실패 — 장비에 아무것도 쓰지 않고 종료"
        else:
            ready = b.start_workers()
            not_ready = [k for k, v in ready.items() if not v]
            if not_ready:
                b.say("연결 안 된 장비(응답 없음·신원 확인 실패): " + ", ".join(not_ready))
            why = b.safety_check(ready)
            if why:
                b.say(f"[중지] {why}")
                code, status = 2, "안전 확인에서 멈춤 — 장비에 아무것도 쓰지 않고 종료"
            else:
                b.say("안전 확인 통과 — 히터 RUN 꺼짐, 공정 흔적 없음")
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
    a = ap.parse_args(argv)
    seconds = a.seconds if a.seconds is not None else int(round(a.minutes * 60))
    ports = {"plc": a.plc, "mfc": a.mfc, "dc": a.dc, "ig": a.ig, "rfpulse": a.rfpulse}
    return run(seconds, ports, Path(a.out))


if __name__ == "__main__":
    sys.exit(main())
