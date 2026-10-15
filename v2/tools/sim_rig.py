"""가짜 장비 5대로 공통 틀 전체(시계·worker·묶기·기록)를 묶은 시험대 — 실제 장비 모델은 다음 단계.

장비(이름은 챔버K 와 비슷한 부하를 흉내 낼 뿐, 프로토콜은 v2.common.simdev 의 줄 프로토콜)
- fast : PLC 처럼 짧은 응답(30 ms), 한 tick 에 1회
- slow : 이온 게이지처럼 평소 280 ms, 열 번에 한 번 860 ms(레인지 찾기)
- alt  : MFC 처럼 두 명령(각 150 ms) — 유량은 짝수 tick 에만(번갈아 읽기)
- dc   : 40 ms
- rfp  : 세 질의(각 50 ms)
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from v2.common.recorder import Recorder
from v2.common.simdev import LineDevice, line_read_many
from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop, iso_local
from v2.common.transport import SimTransport
from v2.common.worker import Worker, WorkerEvent

F = FType.FLOAT


def _blk(name: str, fields: List[str], own: tuple = ()) -> DeviceBlock:
    return DeviceBlock(name, name, tuple(FieldDef(f, F, "", f, own_quality=f in own) for f in fields))


BLOCKS = (
    _blk("fast", ["v1", "v2", "v3"]),
    _blk("slow", ["ion", "ga", "gb"]),
    _blk("alt", ["flow1", "flow2", "press"], own=("flow1", "flow2")),
    _blk("dc", ["v", "i"]),
    _blk("rfp", ["fwd", "ref", "st"]),
)


def _slow_jitter(now: float) -> float:
    return 0.58 if int(now) % 10 == 7 else 0.0       # 280 → 860 ms


def make_devices() -> Dict[str, LineDevice]:
    return {
        "fast": LineDevice("fast", {"v1": 1, "v2": 2, "v3": 3}, 0.03),
        "slow": LineDevice("slow", {"ion": 4.0e-6, "ga": 1.2e-3, "gb": 9.5e-4}, 0.28, _slow_jitter),
        "alt": LineDevice("alt", {"flow1": 20.0, "flow2": 0.0, "press": 2.0}, 0.15),
        "dc": LineDevice("dc", {"v": 412.0, "i": 0.61}, 0.04),
        "rfp": LineDevice("rfp", {"fwd": 100, "ref": 2, "st": 32}, 0.05),
    }


def _reader(name: str):
    if name == "alt":
        def read_alt(sess, seq):
            out = line_read_many(sess, "alt", ["press"])
            if seq % 2 == 0:
                out.update(line_read_many(sess, "alt", ["flow1", "flow2"]))
            return out
        return read_alt
    if name == "rfp":
        def read_rfp(sess, seq):
            out = {}
            for k in ("st", "fwd", "ref"):
                out.update(line_read_many(sess, "rfp", [k]))
            return out
        return read_rfp
    fields = [f.name for b in BLOCKS if b.name == name for f in b.fields]
    return lambda sess, seq: line_read_many(sess, name, fields)


DEADLINES_MS = {"fast": 400, "slow": 900, "alt": 900, "dc": 500, "rfp": 900}


class Rig:
    def __init__(self, clock, out_dir: Path, devices: Optional[Dict[str, LineDevice]] = None,
                 nas_dir: Optional[Path] = None, period: float = 1.0, tick_deadline_ms: int = 900) -> None:
        self.clock = clock
        self.devices = devices or make_devices()
        self.events: List[WorkerEvent] = []
        self.workers = {n: Worker(n, SimTransport(clock, d), on_event=self._on_event) for n, d in self.devices.items()}
        sources = [Source(n, self.workers[n], _reader(n), (n,), DEADLINES_MS[n]) for n in self.devices]
        self.bus = SnapshotBus(clock, BLOCKS, sources, tick_deadline_ms)
        self.recorder = Recorder(out_dir, self.bus.columns, nas_dir=nas_dir)
        self.loop = TickLoop(clock, self.bus, [self.recorder.put_row], period=period)

    def _on_event(self, ev: WorkerEvent) -> None:
        self.events.append(ev)
        self.recorder.put_event(iso_local(self.clock.wall()), "warn" if ev.kind != "connected" else "info",
                                ev.kind.upper(), ev.device, ev.detail)

    def start(self) -> None:
        self.recorder.start()
        with self.clock.hold():
            for w in self.workers.values():
                w.start()

    def stop(self) -> None:
        for w in self.workers.values():
            w.stop()
        self.recorder.stop()
