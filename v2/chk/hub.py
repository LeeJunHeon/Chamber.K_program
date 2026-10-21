"""장비 메시지(MessageHub, 사용자 지시 5-6 ③) — 내부 이벤트를 messages.py 형식(envelope id·instance·seq·ts(UTC)·type·data)으로
바꾸는 곳 하나. instance = 이 실행의 id(프로그램을 켤 때마다 새로) — seq 는 실행 안에서만 이어진다.

type
- run_start · run_end · cmd_result · config_ack · hello 는 그 type 으로.
- state 는 tick 마다 하나(최신 1개만 보관 — 링 버퍼에 넣지 않음).
- 나머지(ProcEvent·SafetyEvent·HeaterEvent·드라이버 이벤트)는 event {level, code, ruleId, message, context}.
  ProcEvent 의 kind·data 는 context 에(context.kind·context.data), 출처는 context.source(process·safety·heater·장비 이름).

소비자(로컬 서버 — 이번, uplink — 5-7)는 subscribe() 로 받는다. 소비자마다 길이 제한 큐 — 넘치면 그 소비자만 끊는다
(tick·worker 를 막지 않는다: 넣기는 put_nowait 뿐). 연결 때 최신 state 하나 + 최근 메시지(링 버퍼, last_seq 뒤부터).
last_seq 가 없거나 지금 seq 보다 크면(지난 실행의 번호 — 프로그램을 다시 켠 뒤 브라우저가 옛 번호로 이어 붙음) 처음부터:
마지막 hello + 최신 state + 최근 메시지 전부. 링 버퍼에서 이미 밀려난 메시지가 last_seq 뒤에 있으면 sub.gap = (첫 seq, 끝 seq).
"""
from __future__ import annotations

import collections
import datetime as _dt
import queue
import re
import threading
import uuid
from typing import Any, Dict, List, Optional

from v2.common.commands import Source

RECENT_KEEP = 500                  # 최근 메시지(state 제외) 링 버퍼
SUB_QUEUE = 1000                   # 소비자 큐 길이 — 넘치면 그 소비자를 끊는다
CLOSED = object()                  # 큐 끝 표시
_RULE_ID = re.compile(r"S\d")


def utc_iso(t_wall: Optional[float] = None) -> str:
    d = (_dt.datetime.now(_dt.timezone.utc) if t_wall is None
         else _dt.datetime.fromtimestamp(t_wall, _dt.timezone.utc))
    return d.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Subscriber:
    def __init__(self, hub: "MessageHub", maxsize: int, name: str) -> None:
        self.hub, self.name = hub, name
        self.q: "queue.Queue" = queue.Queue(maxsize=maxsize)
        self.backlog: "collections.deque[dict]" = collections.deque()   # 연결 때 다시 보내는 것(링 버퍼 길이까지 — 큐와 따로)
        self.closed = False
        self.reason = ""
        self.gap: Optional[tuple] = None          # (놓친 첫 seq, 놓친 끝 seq) — 링 버퍼보다 많이 놓쳤을 때

    def put(self, env: dict) -> bool:
        if self.closed:
            return False
        try:
            self.q.put_nowait(env)
            return True
        except queue.Full:
            self.close("slow")                   # 느린 소비자 — 이 소비자만 끊는다
            return False

    def get(self, timeout: float):
        """다음 메시지(다시 보내기 먼저), 시간 안에 없으면 None, 끊겼으면 CLOSED."""
        if self.backlog:
            return self.backlog.popleft()
        if self.closed and self.q.empty():
            return CLOSED
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return CLOSED if self.closed else None

    def close(self, reason: str = "closed") -> None:
        if not self.closed:
            self.closed, self.reason = True, reason
            self.hub._drop(self)
            try:
                self.q.put_nowait(CLOSED)
            except queue.Full:
                pass


class MessageHub:
    def __init__(self, system, recent_keep: int = RECENT_KEEP) -> None:
        self.sys = system
        self._lock = threading.Lock()
        self.seq = 0
        self.instance = uuid.uuid4().hex
        self.recent: "collections.deque[dict]" = collections.deque(maxlen=recent_keep)
        self.last_state: Optional[dict] = None
        self.last_hello: Optional[dict] = None
        self.evicted_seq = 0                         # 링 버퍼에서 밀려난 마지막 seq
        self.subs: List[Subscriber] = []
        self.dropped_subs = 0
        self.alarm_codes: Dict[str, dict] = {}       # 규칙 id → 마지막 발동(code·since)
        self._web_hash: Optional[str] = None

    # ── 보내기 ──
    def _wall(self) -> float:
        return self.sys.clock.wall()

    def publish(self, type_: str, data: Dict[str, Any]) -> dict:
        with self._lock:
            self.seq += 1
            env = {"id": uuid.uuid4().hex, "instance": self.instance, "seq": self.seq, "ts": utc_iso(self._wall()),
                   "type": type_, "data": data}
            if type_ == "state":
                self.last_state = env
            else:
                if type_ == "hello":
                    self.last_hello = env
                if len(self.recent) == self.recent.maxlen:
                    self.evicted_seq = self.recent[0]["seq"]
                self.recent.append(env)
            subs = list(self.subs)
        for s in subs:
            s.put(env)
        return env

    def subscribe(self, last_seq: Optional[int] = None, maxsize: int = SUB_QUEUE, name: str = "") -> Subscriber:
        """새 소비자 — 최신 state 하나 + 최근 메시지(last_seq 가 있으면 그 뒤부터)를 먼저 넣는다.
        last_seq 가 지금 seq 보다 크면(지난 실행의 번호) 없는 것으로 — 마지막 hello 부터 처음부터 보낸다."""
        sub = Subscriber(self, maxsize, name)
        with self._lock:
            if last_seq is not None and last_seq > self.seq:
                last_seq = None
            first = []
            if last_seq is None and self.last_hello is not None:
                first.append(self.last_hello)                     # 링 버퍼에서 밀려났어도 hello 는 맨 앞에
            if self.last_state is not None:
                first.append(self.last_state)
            first += [e for e in self.recent if (last_seq is None or e["seq"] > last_seq) and e is not self.last_hello]
            if last_seq is not None and self.evicted_seq > last_seq:
                sub.gap = (last_seq + 1, self.evicted_seq)
            sub.backlog.extend(first)
            self.subs.append(sub)
        return sub

    def _drop(self, sub: Subscriber) -> None:
        with self._lock:
            if sub in self.subs:
                self.subs.remove(sub)
                if sub.reason == "slow":
                    self.dropped_subs += 1

    def close(self) -> None:
        for s in list(self.subs):
            s.close("shutdown")

    # ── hello ──
    @property
    def web_schema_hash(self) -> str:
        if self._web_hash is None:
            from v2.chk import web_schema as WS
            self._web_hash = WS.content_hash()
        return self._web_hash

    def hello_data(self) -> dict:
        from v2.chk import commands as C
        s = self.sys
        return {"equipment": s.cfg.get("EQUIPMENT_ID"), "instance": self.instance, "program": s.program,
                "version": s.version, "runMode": s.mode, "schemaHash": _param_hash(),
                "configVersion": s.config_version, "webSchemaHash": self.web_schema_hash,
                "snapshotColumns": list(s.bus.columns),
                "commandKinds": [c.kind for c in C.COMMANDS if Source.HMI in c.sources],
                "writesAllowed": bool(s.policy.device_writes)}

    def hello(self) -> dict:
        return self.publish("hello", self.hello_data())

    # ── 명령·설정 결과 ──
    @staticmethod
    def cmd_result_data(cid: Any, kind: str, res, source: Any = None, by: Optional[str] = None) -> dict:
        src = getattr(source, "value", source)
        return {"cmdId": None if cid is None else str(cid), "kind": kind, "status": "accepted" if res.ok else "rejected",
                "reasonCode": res.reason, "source": src, "by": by,
                "detail": f"{kind} ({src}{', ' + by if by else ''}) → {'ok' if res.ok else res.reason}"[:300]}

    def cmd_result(self, cid: Any, kind: str, res, source: Any = None, by: Optional[str] = None) -> dict:
        return self.publish("cmd_result", self.cmd_result_data(cid, kind, res, source, by))

    def config_ack(self, d: dict) -> None:
        self.publish("config_ack", dict(d))

    # ── 내부 이벤트 → 메시지 ──
    def on_event(self, e) -> None:
        name = type(e).__name__
        if name == "ProcEvent":
            self._proc(e)
        elif name == "SafetyEvent":
            if e.kind == "fired":
                self.alarm_codes[e.rule] = {"code": e.code, "since": utc_iso(e.t or self._wall())}
            rule = e.rule if _RULE_ID.match(e.rule or "") else None      # 정지 순서 이름(stop·row_end …)은 context.seq 로
            self._event(e.level or "info", e.code or e.kind.upper(), rule, e.detail,
                        {"source": "safety", "kind": e.kind, "seq": None if rule else (e.rule or None)})
        elif name == "HeaterEvent":
            self._event(e.level or "info", e.code or e.kind.upper(), None, e.detail,
                        {"source": "heater", "kind": e.kind, "data": e.data})
        else:
            reason = getattr(e, "reason", None)
            dev = getattr(e, "device", None) or name.replace("Event", "").lower()
            data = getattr(e, "data", None)
            self._event("warn" if reason else "info", (reason or getattr(e, "kind", "?")).upper(), None,
                        getattr(e, "detail", ""), {"source": dev, "kind": getattr(e, "kind", None),
                                                   "data": data if data else None})

    def _event(self, level: str, code: str, rule: Optional[str], message: str, context: dict) -> None:
        self.publish("event", {"level": level, "code": code, "ruleId": rule, "message": (message or "")[:500],
                               "context": context})

    def _proc(self, e) -> None:
        p, d = self.sys.process, dict(e.data or {})
        if e.kind == "run_start":
            self.publish("run_start", {"runId": d.get("run_id"), "recipeName": getattr(p, "recipe_name", "") or None,
                                       "by": getattr(p, "started_by", None),
                                       "name": d.get("name"), "stepNo": (p.csv_index + 1) if getattr(p, "list_active", False) else 1,
                                       "params": d.get("params"), "blocks": d.get("blocks"),
                                       "configVersion": self.sys.config_version})
            return
        if e.kind == "run_end":
            self.publish("run_end", {"runId": getattr(p, "run_id", None) or None, "result": d.get("status"),
                                     "reasonCode": d.get("reason"), "summary": {"name": d.get("name")}})
            return
        level = d.get("level") or ("warn" if e.code else "info")
        self._event(level, e.code or e.kind.upper(), None, e.detail, {"source": "process", "kind": e.kind, "data": d or None})

    # ── state(tick 마다) ──
    def on_row(self, row: Dict[str, Any]) -> None:
        self.publish("state", self.state_data(row))

    def state_data(self, row: Dict[str, Any]) -> dict:
        s, p, h = self.sys, self.sys.process, self.sys.heater
        f = p.snapshot_fields() if hasattr(p, "snapshot_fields") else {}
        blk = None
        if f.get("proc.block"):
            no, total = f["proc.block"].split("/")
            blk = {"no": int(no), "total": int(total), "type": f.get("proc.block_type"), "label": f.get("proc.block_label"),
                   "remainS": f.get("proc.remain_s")}
        rows = getattr(p, "rows", None) or []
        recipe = ({"name": p.recipe_name, "rows": len(rows), "row": f.get("proc.row"), "listActive": bool(p.list_active)}
                  if rows else None)
        rr = h.recipe
        hr = ({"name": rr.name, "state": rr.state, "step": rr.idx + 1, "steps": len(rr.steps)}
              if h.recipe_running else None)
        fired = [r for r, on in getattr(s.safety.engine, "_fired", {}).items() if on]
        alarms = [dict({"ruleId": r}, **self.alarm_codes.get(r, {})) for r in sorted(fired)]
        links = {b.name: {"q": row.get(f"{b.name}.q"), "age": row.get(f"{b.name}.age_s")} for b in s.bus.blocks}
        st = s.store
        return {"procState": p.state, "runId": f.get("proc.run_id"), "step": blk, "remainS": f.get("proc.remain_s"),
                "recipe": recipe, "heaterRecipe": hr,
                "heater": {"owner": h.owner, "handedOver": bool(h.handed_over), "gas": h.gas_state},
                "alarms": alarms, "links": links, "configVersion": s.config_version,
                "configPending": ({"idle": dict(st.pending_idle), "restart": dict(st.pending_restart)}
                                  if st is not None and (st.pending_idle or st.pending_restart) else None),
                "snapshot": row, "left": dict(getattr(p, "left", {}) or {}),
                "stopping": s.safety.engine.stopping}


def _param_hash() -> str:
    from v2.chk import params as P
    from v2.common.paramdef import schema_hash
    return schema_hash(P.PARAMS, P.CROSS_RULES)
