"""웹 상시 연결(uplink, 사용자 지시 5-7 · 결정 8 2026-10-20) — 계약(경로·헤더·JSON·응답 코드·명령·보관함)은 v2.common.messages 독스트링.

스레드 셋(데몬, 실제 시간 — 시스템 시계는 envelope 시각·명령 만료 판정에만):
  uplink-in   MessageHub 소비자. state 는 최신 하나만(+ snapshot_summary 계산 — 공정 중 구간만), hello 는 건너뜀(연결마다 새로 만든다),
              나머지는 받자마자 보관함(Spool)에 — HIGH 면 uplink-out 을 깨운다(직전 POST 성공·200 ms 지남이면 바로 POST). 느린 소비자로 끊기면 마지막 seq 로 다시 붙어 링 버퍼에서 이어 받는다
              (링 버퍼보다 많이 놓쳤으면 UPLINK_GAP).
  uplink-out  POST {URL}/ingest — UPLINK_INTERVAL_MS 마다 [hello(연결 뒤 첫 POST)] + 보관함(오래된 것부터, 개수·바이트 상한) + 최신 state.
              acked id 만 보관함에서 지운다. 응답 commands 는 SSE 와 같게 처리. needWebSchema 면 webSchema 를 실은 hello 를 한 번 더.
  uplink-sse  GET {URL}/stream — 명령 받기(주 경로). 끊기면 다시 붙는다.
tick·worker·로컬 서버를 막지 않는다 — 허브는 put_nowait, 보관함·명령 기억은 상한, 네트워크는 자기 스레드·시간 제한 UPLINK_TIMEOUT_SEC.
끊겨도 공정·안전은 그대로(기록·화면에만 — UPLINK_DOWN·state.uplink).
UPLINK_URL·UPLINK_TOKEN 은 로그·메시지에 쓰지 않는다(주소는 호스트만).
"""
from __future__ import annotations

import collections
import datetime as _dt
import heapq
import http.client
import json
import math
import random
import ssl
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit

from v2.common.commands import Source
from v2.common.messages import dumps_json
from v2.common.snapshot_bus import iso_local
from v2.chk.core import CmdResult
from v2.chk.hub import CLOSED, SUB_QUEUE, utc_iso

UPLINK_BATCH_MAX = 500                 # 한 POST 에 보관함 메시지 최대 개수
UPLINK_BATCH_BYTES = 512 * 1024        # 한 POST 본문 최대 바이트(대략)
AUTH_RETRY_S = 60.0                    # 401
CONFLICT_RETRY_S = 10.0                # 409
BACKOFF_MAX_S = 30.0                   # 그 밖 오류: 1→2→4…→30 s
GROW_AFTER_413_S = 60.0                # 413 로 줄인 묶음 크기를 다시 늘리기까지
SSE_IDLE_S = 45.0                      # 웹 keep-alive 15 s — 세 번 못 받으면 끊고 다시
CMD_KEEP = 2000                        # 기억하는 명령 id(같은 id 는 한 번만 실행)
SPOOL_FULL_WARN_S = 60.0               # UPLINK_SPOOL_FULL 경고 간격(그동안 버린 수를 모아서)
SEG_BYTES = 256 * 1024                 # 보관함 조각 파일 크기
WAKE_GAP_S = 0.2                       # HIGH 메시지로 주기 밖 POST — 직전 POST 끝에서 이만큼은 띄운다
HIGH, LOW = 1, 0


def priority(env: dict) -> int:
    """보관함이 차면 LOW(info event·snapshot_summary)부터 버린다 — run_*·cmd_result·config_ack·warn/error event 는 마지막까지."""
    t = env.get("type")
    if t == "snapshot_summary":
        return LOW
    if t == "event" and (env.get("data") or {}).get("level") not in ("warn", "error"):
        return LOW
    return HIGH


def retry_delay(code: Optional[int], fails: int, rnd: Callable[[], float] = random.random) -> float:
    """다시 시도까지 초 — 401 60 s, 409 10 s, 그 밖 1→2→4…→30 s(±20 % 흔들림, 30 s 넘지 않음)."""
    if code == 401:
        return AUTH_RETRY_S
    if code == 409:
        return CONFLICT_RETRY_S
    base = min(BACKOFF_MAX_S, 2.0 ** max(0, fails - 1))
    return min(BACKOFF_MAX_S, base * (0.8 + 0.4 * rnd()))


def parse_iso(s: Any) -> Optional[float]:
    if not isinstance(s, str) or not s:
        return None
    try:
        d = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=_dt.timezone.utc)
    return d.timestamp()


# ───────────── 보관함 ─────────────
class Spool:
    """디스크 보관함 — 조각 파일(seg-N.jsonl)에 한 줄씩 덧붙이고, 지운(acked·버린) id 는 그 조각의 seg-N.ack 에 적는다
    (다시 켰을 때 지운 것이 되살아나지 않게). 조각의 메시지가 모두 지워지면 두 파일을 지운다(평소: 1 초 안에 acked →
    조각이 쌓이지 않음).
    디스크 크기가 상한의 두 배를 넘으면(조각마다 몇 개씩 남은 경우) 남은 것만 새 조각으로 다시 쓴다(순서 유지).
    다시 켜면 남은 조각을 읽어 이어 올린다(지난 실행 것 포함 — envelope 의 instance 로 구분)."""

    def __init__(self, directory: Path, max_bytes: int) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = int(max_bytes)
        self._lock = threading.Lock()
        self._n = 0
        self._q: Dict[int, "collections.OrderedDict[str, list]"] = {HIGH: collections.OrderedDict(),
                                                                    LOW: collections.OrderedDict()}
        self._seg_live: Dict[int, int] = {}
        self._seg_size: Dict[int, int] = {}
        self.live_bytes = 0
        self.dropped = 0                       # 상한으로 버린 수(누적)
        self.dropped_by_type: Dict[str, int] = collections.Counter()
        self._cur: Optional[int] = None
        self._fh = None
        self._next_seg = 1
        self._tombs: Dict[int, List[str]] = {}
        self.loaded = self._load()

    # ── 파일 ──
    def _path(self, seg: int) -> Path:
        return self.dir / f"seg-{seg:08d}.jsonl"

    def _ack_path(self, seg: int) -> Path:
        return self.dir / f"seg-{seg:08d}.ack"

    def _load(self) -> int:
        n = 0
        segs = sorted(int(p.stem[4:]) for p in self.dir.glob("seg-*.jsonl") if p.stem[4:].isdigit())
        for seg in segs:
            data = self._path(seg).read_bytes()
            ap = self._ack_path(seg)
            gone = set(ap.read_text(encoding="utf-8").split()) if ap.exists() else set()
            self._seg_size[seg] = len(data)
            self._seg_live[seg] = 0
            off = 0
            for line in data.split(b"\n"):
                ln = len(line) + 1
                if line.strip():
                    try:
                        env = json.loads(line)
                        eid = env["id"]
                    except (ValueError, KeyError, TypeError):
                        env = None                       # 끝이 잘린 줄(쓰는 중에 꺼짐) — 버림
                    if env is not None and eid not in gone and not self._has(eid):
                        self._add(eid, seg, off, len(line), priority(env), env.get("type", "?"))
                        n += 1
                off += ln
            if self._seg_live[seg] == 0:
                self._unlink(seg)
        for ap in self.dir.glob("seg-*.ack"):                     # 조각 없이 남은 ack 파일
            if not ap.with_suffix(".jsonl").exists():
                try:
                    ap.unlink()
                except OSError:
                    pass
        self._next_seg = (segs[-1] + 1) if segs else 1
        return n

    def _has(self, eid: str) -> bool:
        return eid in self._q[HIGH] or eid in self._q[LOW]

    def _add(self, eid: str, seg: int, off: int, length: int, prio: int, type_: str) -> None:
        self._n += 1
        self._q[prio][eid] = [self._n, seg, off, length, type_, prio]
        self._seg_live[seg] = self._seg_live.get(seg, 0) + 1
        self.live_bytes += length

    def _unlink(self, seg: int) -> None:
        if seg == self._cur:
            self._fh.close()
            self._fh, self._cur = None, None
        for f in (self._path(seg), self._ack_path(seg)):
            try:
                f.unlink()
            except OSError:
                pass
        self._seg_live.pop(seg, None)
        self._seg_size.pop(seg, None)
        self._tombs.pop(seg, None)

    def _remove(self, eid: str) -> bool:
        for q in self._q.values():
            e = q.pop(eid, None)
            if e is not None:
                seg = e[1]
                self.live_bytes -= e[3]
                self._seg_live[seg] -= 1
                if self._seg_live[seg] == 0:
                    self._unlink(seg)
                else:
                    self._tombs.setdefault(seg, []).append(eid)
                return True
        return False

    def _flush_tombs(self) -> None:
        for seg, ids in self._tombs.items():
            try:
                with open(self._ack_path(seg), "a", encoding="utf-8") as f:
                    f.write("\n".join(ids) + "\n")
            except OSError:
                pass                                       # 다시 켜면 다시 올라갈 뿐(웹은 id 로 한 번 저장)
        self._tombs = {}

    def _append(self, line: bytes) -> Tuple[int, int]:
        if self._cur is None or self._seg_size[self._cur] >= SEG_BYTES:
            if self._fh is not None:
                self._fh.close()
            self._cur, self._next_seg = self._next_seg, self._next_seg + 1
            self._fh = open(self._path(self._cur), "ab")
            self._seg_size[self._cur] = 0
            self._seg_live.setdefault(self._cur, 0)
        off = self._seg_size[self._cur]
        self._fh.write(line + b"\n")
        self._fh.flush()
        self._seg_size[self._cur] += len(line) + 1
        return self._cur, off

    def _read(self, entries: List[list]) -> List[bytes]:
        out: List[bytes] = []
        fhs: Dict[int, Any] = {}
        try:
            for e in entries:
                seg = e[1]
                if seg == self._cur:
                    self._fh.flush()
                f = fhs.get(seg)
                if f is None:
                    f = fhs[seg] = open(self._path(seg), "rb")
                f.seek(e[2])
                out.append(f.read(e[3]))
        finally:
            for f in fhs.values():
                f.close()
        return out

    def _ordered(self):
        return heapq.merge(((e[0], k, e) for k, e in self._q[HIGH].items()),
                           ((e[0], k, e) for k, e in self._q[LOW].items()))

    def _compact(self) -> None:
        items = [(k, e) for _, k, e in self._ordered()]
        lines = self._read([e for _, e in items])
        old = list(self._seg_size)
        if self._fh is not None:
            self._fh.close()
        self._fh, self._cur = None, None
        for q in self._q.values():
            q.clear()
        self._seg_live, self._seg_size, self.live_bytes = {}, {}, 0
        for (k, e), line in zip(items, lines):
            seg, off = self._append(line)
            self._add(k, seg, off, len(line), e[5], e[4])
        self._tombs = {}
        for seg in old:
            for f in (self._path(seg), self._ack_path(seg)):
                try:
                    f.unlink()
                except OSError:
                    pass

    # ── 쓰기·읽기 ──
    def put(self, env: dict) -> int:
        """넣기 — 상한을 넘으면 낮은 것부터(같으면 오래된 것부터) 버린다. 버린 수를 돌려준다."""
        line = dumps_json(env)
        prio = priority(env)
        dropped = 0
        with self._lock:
            if len(line) + 1 > self.max_bytes:
                self.dropped += 1
                self.dropped_by_type[env.get("type", "?")] += 1
                return 1
            while self.live_bytes + len(line) > self.max_bytes:
                q = self._q[LOW] if self._q[LOW] else self._q[HIGH]
                if not q:
                    break
                if q is self._q[HIGH] and prio == LOW:
                    self.dropped += 1                          # 넣으려는 것이 더 낮다 — 이것을 버린다
                    self.dropped_by_type[env.get("type", "?")] += 1
                    return dropped + 1
                eid, e = next(iter(q.items()))
                self._remove(eid)
                self.dropped += 1
                self.dropped_by_type[e[4]] += 1
                dropped += 1
            seg, off = self._append(line)
            self._add(env["id"], seg, off, len(line), prio, env.get("type", "?"))
            if sum(self._seg_size.values()) > 2 * self.max_bytes + SEG_BYTES:
                self._compact()
            self._flush_tombs()
        return dropped

    def batch(self, max_count: int, max_bytes: int) -> List[Tuple[str, bytes]]:
        """오래된 것부터 (id, 줄) — 개수·바이트 상한(첫 하나는 상한보다 커도 넣는다 — 413 이면 나눠서)."""
        with self._lock:
            picked, size = [], 0
            for _, k, e in self._ordered():
                if len(picked) >= max_count or (picked and size + e[3] > max_bytes):
                    break
                picked.append((k, e))
                size += e[3] + 1
            lines = self._read([e for _, e in picked])
        return [(k, ln) for (k, _), ln in zip(picked, lines)]

    def ack(self, ids) -> int:
        with self._lock:
            n = sum(1 for i in ids if isinstance(i, str) and self._remove(i))
            self._flush_tombs()
            return n

    def drop(self, eid: str) -> None:
        with self._lock:
            if self._remove(eid):
                self.dropped += 1
            self._flush_tombs()

    @property
    def count(self) -> int:
        return len(self._q[HIGH]) + len(self._q[LOW])

    def disk_bytes(self) -> int:
        with self._lock:
            return sum(self._seg_size.values())

    def files(self) -> int:
        return len(list(self.dir.glob("seg-*")))

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh, self._cur = None, None


# ───────────── snapshot_summary ─────────────
class Summary:
    """정각에 맞춘 구간(UPLINK_SUMMARY_PERIOD_SEC)마다 숫자 열의 last·mean·min·max, 장비별 ok 비율·최대 나이, 행 수.
    공정 중에만 — 구간 안 행에 proc.run_id 가 하나라도 있을 때만 낸다(runId = 구간의 마지막 run id, 사용자 결정 2026-10-22)."""

    SKIP = ("seq",)

    def __init__(self) -> None:
        self.start: Optional[float] = None
        self.period = 60
        self._reset()

    def _reset(self) -> None:
        self.rows = 0
        self.run_id: Optional[str] = None
        self.vals: Dict[str, list] = {}           # 열 → [last, sum, n, min, max]
        self.q: Dict[str, list] = {}              # 장비 → [ok, n, max_age]

    def add(self, t_wall: float, row: Dict[str, Any], period: int) -> Optional[dict]:
        start = math.floor(t_wall / period) * period
        out = None
        if self.start is not None and (start != self.start or period != self.period):
            out = self.finish()
        if self.start is None or out is not None:
            self.start, self.period = start, period
        self.rows += 1
        rid = row.get("proc.run_id")
        if rid:
            self.run_id = str(rid)
        for k, v in row.items():
            if k.endswith(".q"):
                dev = k[:-2]
                qq = self.q.setdefault(dev, [0, 0, None])
                qq[0] += v == "ok"
                qq[1] += 1
                age = row.get(f"{dev}.age_s")
                if isinstance(age, (int, float)) and not isinstance(age, bool):
                    qq[2] = age if qq[2] is None else max(qq[2], age)
                continue
            if k in self.SKIP or k.endswith(".age_s") or isinstance(v, bool) or not isinstance(v, (int, float)):
                continue
            if isinstance(v, float) and not math.isfinite(v):
                continue
            a = self.vals.get(k)
            if a is None:
                self.vals[k] = [v, v, 1, v, v]
            else:
                a[0], a[1], a[2] = v, a[1] + v, a[2] + 1
                a[3], a[4] = min(a[3], v), max(a[4], v)
        return out

    def finish(self) -> Optional[dict]:
        if self.start is None or not self.rows or self.run_id is None:
            self.start = None
            self._reset()
            return None
        r = lambda x: round(x, 6) if isinstance(x, float) else x
        d = {"runId": self.run_id, "from": utc_iso(self.start), "to": utc_iso(self.start + self.period), "rows": self.rows,
             "values": {k: {"last": r(a[0]), "mean": r(a[1] / a[2]), "min": r(a[3]), "max": r(a[4])}
                        for k, a in self.vals.items()},
             "quality": {k: {"okRatio": round(q[0] / q[1], 4) if q[1] else None, "maxAge": q[2]}
                         for k, q in self.q.items()}}
        self.start = None
        self._reset()
        return d


# ───────────── 연결 ─────────────
class HttpError(Exception):
    def __init__(self, status: Optional[int], what: str) -> None:
        super().__init__(what)
        self.status, self.what = status, what


class Uplink:
    def __init__(self, system, url: str, token: str, data_dir: Path) -> None:
        self.sys = system
        self.hub = system.hub
        self.cfg = system.cfg
        u = urlsplit(url)
        self.scheme, self.host, self.port = u.scheme, u.hostname or "", u.port
        self.base = u.path.rstrip("/")
        self._auth = f"Bearer {token}"
        self.spool = Spool(Path(data_dir) / "uplink", int(self.cfg["UPLINK_SPOOL_MAX_MB"]) * 1024 * 1024)
        self.summary = Summary()
        self.latest_state: Optional[dict] = None
        self._stop = threading.Event()
        self._wake = threading.Event()                      # HIGH 메시지 → 주기 밖 POST(stop 도 깨운다)
        self._last_post_end: Optional[float] = None
        self.early_posts = 0
        self._threads: List[threading.Thread] = []
        self._sse_sock = None
        self._ssl = ssl.create_default_context() if self.scheme == "https" else None
        # 보내기 상태
        self.need_hello = True
        self.want_schema = False
        self.batch_n = UPLINK_BATCH_MAX
        self._last_413 = -1e9
        self.fails = 0
        self.fail_since: Optional[float] = None
        self.down_reported = False
        self.last_ok_wall: Optional[float] = None
        self.last_error: Optional[str] = None
        self.sse_connected = False
        self.sse_fails = 0
        self._warned: set = set()
        self.posts = 0
        # 명령
        self._cmd_lock = threading.Lock()
        self.done_cmds: "collections.OrderedDict[str, dict]" = collections.OrderedDict()
        self.dup_cmds = 0
        # 소비자
        self.last_seq: Optional[int] = None
        self.resubscribes = 0
        self.sub_queue = SUB_QUEUE
        self._full_pending = 0
        self._full_warn_t: Optional[float] = None

    @property
    def where(self) -> str:
        """로그·화면에 쓰는 주소 — 호스트만(경로·토큰 없음)."""
        return self.host

    # ── 생명주기 ──
    def start(self) -> None:
        if self.spool.loaded:
            self._note("info", "UPLINK_SPOOL_LOADED", f"지난 실행이 남긴 웹 미전송 메시지 {self.spool.loaded}개 — 이어서 올림",
                       count=self.spool.loaded)
        sub = self.hub.subscribe(None, maxsize=self.sub_queue, name="uplink")   # 시작 전에 붙어 첫 메시지부터
        for target, name in ((lambda: self._consume(sub), "uplink-in"), (self._send_loop, "uplink-out"),
                             (self._sse_loop, "uplink-sse")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        sk = self._sse_sock
        if sk is not None:
            try:
                sk.shutdown(2)                                 # 읽기 중인 SSE 를 깨운다
            except OSError:
                pass
        t_end = time.monotonic() + timeout
        for t in self._threads:
            t.join(max(0.0, t_end - time.monotonic()))
        self.spool.close()

    def status(self) -> dict:
        return {"enabled": True, "host": self.where, "connected": self.sse_connected,
                "lastOkAt": utc_iso(self.last_ok_wall) if self.last_ok_wall is not None else None,
                "lastError": self.last_error, "queued": self.spool.count, "spoolBytes": self.spool.live_bytes,
                "dropped": self.spool.dropped}

    def _note(self, level: str, code: str, msg: str, **ctx) -> None:
        s = self.sys
        try:
            if s.recorder is not None:
                s.recorder.put_event(iso_local(s.clock.wall()), level, code, "uplink", msg)
            self.hub._event(level, code, None, msg, dict({"source": "uplink", "kind": code.lower()}, **ctx))
        except Exception:
            pass

    def _warn_once(self, key: str, code: str, msg: str, **ctx) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self._note("warn", code, msg, **ctx)

    # ── 소비자(uplink-in) ──
    def _consume(self, sub) -> None:
        while not self._stop.is_set():
            if sub is None:
                self.resubscribes += 1
                sub = self.hub.subscribe(self.last_seq, maxsize=self.sub_queue, name="uplink")
                if sub.gap:
                    self._note("warn", "UPLINK_GAP", f"웹으로 올릴 메시지를 놓침(seq {sub.gap[0]}~{sub.gap[1]} 중 링 버퍼에서 밀려난 것)",
                               fromSeq=sub.gap[0], toSeq=sub.gap[1])
            while True:
                env = sub.get(0.5)
                if env is None:
                    if self._stop.is_set():
                        return
                    self._maybe_full_warn()
                    continue
                if env is CLOSED:
                    break
                try:
                    self._take(env)
                except Exception as e:                          # 보관함 쓰기 실패 — 버리고 계속(공정에 영향 없음)
                    self._note("warn", "UPLINK_SPOOL_ERROR", f"보관함 쓰기 실패: {type(e).__name__}")
            if sub.reason == "shutdown":
                return
            sub = None

    def _take(self, env: dict) -> None:
        t = env["type"]
        if t == "state":
            self.latest_state = env
            row = (env.get("data") or {}).get("snapshot") or {}
            out = self.summary.add(parse_iso(env["ts"]) or self.sys.clock.wall(), row,
                                   int(self.cfg["UPLINK_SUMMARY_PERIOD_SEC"]))
            if out is not None:
                self.hub.publish("snapshot_summary", out)
            return
        if self.last_seq is not None and env["seq"] <= self.last_seq:
            return                                              # 다시 붙을 때 겹친 것
        self.last_seq = env["seq"]
        if t == "hello":
            return                                              # 연결마다 새로 만든다(uplink-out)
        n = self.spool.put(env)
        if priority(env) == HIGH:
            self._wake.set()
        if n:
            self._full_pending += n
            self._maybe_full_warn()

    def _maybe_full_warn(self) -> None:
        if not self._full_pending:
            return
        now = time.monotonic()
        if self._full_warn_t is not None and now - self._full_warn_t < SPOOL_FULL_WARN_S:
            return
        n, self._full_pending, self._full_warn_t = self._full_pending, 0, now
        self._note("warn", "UPLINK_SPOOL_FULL",
                   f"웹 미전송 보관함 가득(UPLINK_SPOOL_MAX_MB {self.cfg['UPLINK_SPOOL_MAX_MB']}) — {n}개 버림(낮은 것부터)",
                   dropped=n, total=self.spool.dropped)

    # ── HTTP ──
    def _conn(self, timeout: float) -> http.client.HTTPConnection:
        if self.scheme == "https":
            return http.client.HTTPSConnection(self.host, self.port, timeout=timeout, context=self._ssl)
        return http.client.HTTPConnection(self.host, self.port, timeout=timeout)

    def _headers(self, **extra) -> dict:
        return dict({"Authorization": self._auth, "User-Agent": "chk-v2"}, **extra)

    def post_ingest(self, body: bytes) -> dict:
        c = self._conn(float(self.cfg["UPLINK_TIMEOUT_SEC"]))
        try:
            c.request("POST", self.base + "/ingest", body=body,
                      headers=self._headers(**{"Content-Type": "application/json"}))
            r = c.getresponse()
            data = r.read(4_000_000)
            if r.status != 200:
                raise HttpError(r.status, f"HTTP {r.status}")
            try:
                out = json.loads(data.decode("utf-8") or "{}")
            except ValueError:
                raise HttpError(None, "bad_response")
            if not isinstance(out, dict):
                raise HttpError(None, "bad_response")
            return out
        except (OSError, http.client.HTTPException) as e:
            raise HttpError(None, "timeout" if isinstance(e, TimeoutError) else type(e).__name__)
        finally:
            c.close()

    # ── 보내기(uplink-out) ──
    def _hello(self) -> dict:
        d = self.hub.hello_data()
        if self.want_schema:
            from v2.chk import web_schema as WS
            st = self.sys.store
            pend = dict(st.pending_idle, **st.pending_restart) if st is not None else {}
            d["webSchema"] = WS.bundle(dict(self.cfg), pend, self.sys.config_version)
        return self.hub.envelope("hello", d)

    def _send_loop(self) -> None:
        next_at = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if now < next_at:
                self._check_down(now)
                wait = min(0.5, next_at - now)
                if self.fails or self._last_post_end is None:
                    self._stop.wait(wait)                       # 실패 뒤 대기(401·409·백오프)는 깨우지 않는다
                    continue
                if self._wake.is_set():                         # HIGH 메시지 — 직전 POST 성공, 200 ms 지나면 바로
                    early = self._last_post_end + WAKE_GAP_S
                    if now >= early:
                        next_at = now
                        self.early_posts += 1
                    else:
                        self._stop.wait(min(wait, early - now))
                    continue
                self._wake.wait(wait)
                continue
            self._wake.clear()                                  # 이 POST 에 실린다 — 이 뒤에 온 것만 다시 깨운다
            try:
                more = self.send_once()
                next_at = time.monotonic() + (0.05 if more else float(self.cfg["UPLINK_INTERVAL_MS"]) / 1000.0)
            except HttpError as e:
                next_at = time.monotonic() + self._failed(e)
            except Exception as e:                              # 예상 밖 — 끊김으로 다루고 계속
                next_at = time.monotonic() + self._failed(HttpError(None, type(e).__name__))

    def send_once(self) -> bool:
        """POST 한 번. 보관함에 더 보낼 것이 남았으면 True(바로 이어서)."""
        hello = self._hello() if self.need_hello else None
        with_schema = hello is not None and "webSchema" in hello["data"]
        state = self.latest_state
        head = [_dumps(hello)] if hello else []
        tail = [_dumps(state)] if state else []
        room = UPLINK_BATCH_BYTES - sum(len(x) for x in head + tail)
        items = self.spool.batch(self.batch_n, max(1, room))
        msgs = head + [ln for _, ln in items] + tail
        body = (b'{"equipment":' + _dumps(self.cfg["EQUIPMENT_ID"]) + b',"instance":' + _dumps(self.hub.instance)
                + b',"schemaHash":' + _dumps(self.hub.hello_data()["schemaHash"]) + b',"messages":['
                + b",".join(msgs) + b"]}")
        try:
            out = self.post_ingest(body)
        except HttpError as e:
            self._last_post_end = time.monotonic()
            if e.status == 413:
                return self._too_large(items, with_schema)
            raise
        self.posts += 1
        self._last_post_end = time.monotonic()
        acked = out.get("acked") or []
        if isinstance(acked, list):
            self.spool.ack(acked)
        if hello is not None:
            self.need_hello = False
            if with_schema:
                self.want_schema = False
        if out.get("needWebSchema") and not with_schema:
            self.want_schema = self.need_hello = True
        self._ok()
        for cmd in out.get("commands") or []:
            if isinstance(cmd, dict):
                self.handle_command(cmd, "post")
        if (len(items) == self.batch_n and self.batch_n < UPLINK_BATCH_MAX
                and time.monotonic() - self._last_413 > GROW_AFTER_413_S):   # 413 뒤 한동안은 줄인 크기로
            self.batch_n = min(UPLINK_BATCH_MAX, self.batch_n * 2)
        return bool(items) and self.spool.count > 0 and (len(items) >= self.batch_n or self.need_hello)

    def _too_large(self, items, with_schema: bool) -> bool:
        self._last_413 = time.monotonic()
        if len(items) > 1:
            self.batch_n = max(1, len(items) // 2)              # 반으로 나눠 바로 다시
            return True
        if len(items) == 1:
            eid = items[0][0]
            self.spool.drop(eid)
            self._note("warn", "UPLINK_TOO_LARGE", f"웹이 받지 않는 큰 메시지 하나를 버림({len(items[0][1])} B)", id=eid)
            return True
        if with_schema:
            self.want_schema = False
            self._note("warn", "UPLINK_TOO_LARGE", "webSchema 를 실은 hello 가 너무 큼 — 묶음 없이 보냄")
            return True
        raise HttpError(413, "HTTP 413")

    def _ok(self) -> None:
        self.last_ok_wall = self.sys.clock.wall()
        self.last_error = None
        if self.down_reported:
            self._note("info", "UPLINK_UP", f"웹 연결 다시 됨({self.where})")
        self.fails, self.fail_since, self.down_reported = 0, None, False
        self._warned = {k for k in self._warned if not k.startswith("post:")}

    def _failed(self, e: HttpError) -> float:
        now = time.monotonic()
        self.fails += 1
        self.last_error = e.what
        self.need_hello = True                                  # 다시 연결되면 맨 앞에 hello
        if self.fail_since is None:
            self.fail_since = now
        if e.status == 401:
            self._warn_once("post:401", "UPLINK_AUTH_FAILED", f"웹이 장비 토큰을 거부(401, {self.where}) — 60 s 마다 다시")
        elif e.status == 409:
            self._warn_once("post:409", "UPLINK_CONFLICT",
                            f"웹이 이 실행을 받지 않음(409 — 같은 장비 ID 의 다른 실행이 연결 중, {self.where}) — 10 s 마다 다시")
        self._check_down(now)
        return retry_delay(e.status, self.fails)

    def _check_down(self, now: float) -> None:
        if (self.fail_since is not None and not self.down_reported
                and now - self.fail_since >= float(self.cfg["UPLINK_DOWN_SEC"])):
            self.down_reported = True
            self._note("warn", "UPLINK_DOWN", f"웹 연결 끊김 {now - self.fail_since:.0f} s({self.where}, {self.last_error}) — "
                                              "공정·안전은 그대로, 메시지는 보관함에", lastError=self.last_error)

    # ── 명령(uplink-sse·POST 응답) ──
    def handle_command(self, cmd: dict, via: str = "sse") -> Optional[dict]:
        """같은 id 는 한 번만 실행 — 다시 오면 같은 cmd_result 를 다시 보낸다. 만료 → COMMAND_EXPIRED. 돌려주는 값 = cmd_result data."""
        cid = cmd.get("id")
        if not isinstance(cid, str) or not cid:
            self._note("warn", "UPLINK_BAD_COMMAND", f"id 없는 웹 명령을 버림({via})")
            return None
        with self._cmd_lock:
            prev = self.done_cmds.get(cid)
            if prev is not None:
                self.dup_cmds += 1
                self.hub.publish("cmd_result", dict(prev))
                return prev
            kind, args = cmd.get("kind"), cmd.get("args", {})
            by = str(cmd.get("requestedBy") or "web")
            exp = parse_iso(cmd.get("expiresAt"))
            if not isinstance(kind, str) or not isinstance(args, dict) or exp is None:
                res = CmdResult(False, "BAD_ARGS", "kind 는 문자열, args 는 객체, expiresAt 은 ISO 시각")
                self.hub.cmd_result(cid, str(kind), res, Source.WEB, by)
            elif self.sys.clock.wall() > exp + float(self.cfg["UPLINK_CLOCK_SKEW_SEC"]):
                res = CmdResult(False, "COMMAND_EXPIRED")
                self.hub.cmd_result(cid, kind, res, Source.WEB, by)
            else:
                res = self.sys.core.command(kind, args, source=Source.WEB, cid=cid, by=by)   # cmd_result 는 core 가
            data = self.hub.cmd_result_data(cid, str(kind), res, Source.WEB, by)
            self.done_cmds[cid] = data
            while len(self.done_cmds) > CMD_KEEP:
                self.done_cmds.popitem(last=False)
            return data

    def _sse_loop(self) -> None:
        while not self._stop.is_set():
            code = None
            try:
                code = self._sse_once()
            except Exception:                                   # 끊김·형식 오류 — 다시 붙는다
                pass
            finally:
                self.sse_connected = False
                self._sse_sock = None
            if self._stop.is_set():
                return
            self.sse_fails += 1
            if code == 401:
                self._warn_once("sse:401", "UPLINK_AUTH_FAILED", f"웹이 장비 토큰을 거부(401, 명령 받기, {self.where}) — 60 s 마다 다시")
            elif code == 409:
                self._warn_once("sse:409", "UPLINK_CONFLICT", f"웹이 이 실행을 받지 않음(409, 명령 받기, {self.where}) — 10 s 마다 다시")
            self._stop.wait(retry_delay(code, self.sse_fails))

    def _sse_once(self) -> Optional[int]:
        c = self._conn(float(self.cfg["UPLINK_TIMEOUT_SEC"]))
        q = urlencode({"equipment": self.cfg["EQUIPMENT_ID"], "instance": self.hub.instance})
        c.request("GET", f"{self.base}/stream?{q}", headers=self._headers(Accept="text/event-stream"))
        sock = self._sse_sock = c.sock                         # getresponse 뒤에는 c.sock 이 비기도 한다(Connection: close)
        if self._stop.is_set():
            c.close()
            return None
        r = c.getresponse()
        if r.status != 200:
            r.read(65536)
            c.close()
            return r.status
        sock.settimeout(SSE_IDLE_S)
        self.sse_connected = True
        self.sse_fails = 0
        self._warned = {k for k in self._warned if not k.startswith("sse:")}
        ev, data = None, []
        try:
            while not self._stop.is_set():
                raw = r.fp.readline(1_000_000)
                if not raw:
                    return None                                  # 웹이 끊음
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line == "":
                    if ev == "command" and data:
                        try:
                            cmd = json.loads("\n".join(data))
                        except ValueError:
                            cmd = None
                        if isinstance(cmd, dict):
                            self.handle_command(cmd, "sse")
                        else:
                            self._note("warn", "UPLINK_BAD_COMMAND", "웹 명령 JSON 을 읽을 수 없음(sse)")
                    ev, data = None, []
                elif line.startswith(":"):
                    continue
                elif line.startswith("event:"):
                    ev = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[6:] if line.startswith("data: ") else line[5:])
        finally:
            r.close()
            c.close()
        return None


def _dumps(obj: Any) -> bytes:
    return dumps_json(obj)
