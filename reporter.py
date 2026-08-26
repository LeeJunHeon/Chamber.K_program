# reporter.py — ERP 이벤트 리포터
# 아웃바운드 HTTPS 배치 전송. 실패 시 로컬 스풀(jsonl)에 보관 후 재전송.
# 이 모듈이 어떤 예외를 내더라도 호출부(try/except)와 데몬 스레드 구조 덕에
# 본 프로그램 동작에는 영향이 없다.
import json
import queue
import uuid
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ErpReporter:
    def __init__(self, url: str, token: str, equipment: str = "CHK",
                 spool_path: str = "reporter_spool.jsonl"):
        self._url = (url or "").strip()
        self._token = (token or "").strip()
        self._equipment = equipment
        self._spool = Path(spool_path)
        self._q: "queue.Queue[dict]" = queue.Queue(maxsize=5000)
        self._state: dict = {}
        self._state_lock = threading.Lock()
        self._dirty = False
        self._last_state_ts = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="ErpReporter", daemon=True
        )

    @property
    def enabled(self) -> bool:
        return bool(self._url and self._token)

    def start(self):
        if self.enabled:
            self._enqueue({"type": "hello", "data": {"program": "Chamber.K"}})
            self._thread.start()

    # ── 외부에서 호출하는 API (전부 스레드 안전, 예외 삼킴) ──

    def update_state(self, partial: dict):
        try:
            with self._state_lock:
                self._state.update(partial)
                self._dirty = True
        except Exception:
            pass

    def event(self, level: str, message: str):
        self._enqueue({"type": "event", "level": level, "message": str(message)[:500]})

    def run_start(self, process_name: str, params: dict | None = None):
        safe = {}
        try:
            safe = {k: v for k, v in (params or {}).items()
                    if isinstance(v, (str, int, float, bool))}
        except Exception:
            pass
        self._enqueue({"type": "run_start", "processName": str(process_name or ""),
                       "params": safe})
        self.update_state({"status": "running"})

    def run_end(self, result: str, error_msg: str = ""):
        self._enqueue({"type": "run_end", "result": result,
                       "errorMsg": str(error_msg or "")[:500]})
        self.update_state({"status": "idle", "stage": ""})

    # ── 내부 ──

    def _enqueue(self, msg: dict):
        if not self.enabled:
            return
        msg.setdefault("ts", _now_iso())
        msg.setdefault("id", uuid.uuid4().hex)  # 재전송 시 서버가 중복을 걸러낼 키
        try:
            self._q.put_nowait(msg)
        except queue.Full:
            try:
                self._q.get_nowait()  # 가장 오래된 것 버리고
                self._q.put_nowait(msg)
            except Exception:
                pass

    def _snapshot_msg(self) -> dict:
        with self._state_lock:
            data = dict(self._state)
            self._dirty = False
        return {"type": "state", "ts": _now_iso(), "data": data}

    def _loop(self):
        while not self._stop.is_set():
            batch: list[dict] = []
            try:
                try:
                    batch.append(self._q.get(timeout=1.0))
                except queue.Empty:
                    pass
                while len(batch) < 50:
                    try:
                        batch.append(self._q.get_nowait())
                    except queue.Empty:
                        break

                now = time.monotonic()
                need_state = (self._dirty and now - self._last_state_ts >= 1.0) or \
                             (now - self._last_state_ts >= 10.0)
                if need_state:
                    batch.append(self._snapshot_msg())
                    self._last_state_ts = now

                if not batch:
                    continue

                if self._post(batch):
                    self._flush_spool()
                else:
                    self._to_spool(batch)
            except Exception:
                # 리포터 내부 오류는 어떤 경우에도 밖으로 내보내지 않는다
                try:
                    self._to_spool(batch)
                except Exception:
                    pass
                time.sleep(2.0)

    def _post(self, messages: list[dict]) -> bool:
        try:
            body = json.dumps(
                {"equipment": self._equipment, "messages": messages}
            ).encode("utf-8")
            req = urllib.request.Request(
                self._url, data=body, method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._token}",
                },
            )
            with urllib.request.urlopen(req, timeout=15) as res:
                return 200 <= res.status < 300
        except Exception:
            return False

    def _to_spool(self, messages: list[dict]):
        if not messages:
            return
        try:
            # 장기 단절 시 스풀이 무한히 커지지 않도록 상한(약 5MB)을 둔다.
            if self._spool.exists() and self._spool.stat().st_size > 5_000_000:
                lines = self._spool.read_text(encoding="utf-8").splitlines()
                self._spool.write_text(
                    "\n".join(lines[len(lines) // 2:]) + "\n", encoding="utf-8"
                )
        except Exception:
            pass
        try:
            with self._spool.open("a", encoding="utf-8") as f:
                for m in messages:
                    # 스냅샷(state)은 최신만 의미 있고, hello는 세션 시작 표시일 뿐이므로
                    # 둘 다 재전송 대상에서 제외한다.
                    if m.get("type") in ("state", "hello"):
                        continue
                    f.write(json.dumps(m, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _flush_spool(self):
        try:
            if not self._spool.exists():
                return
            lines = self._spool.read_text(encoding="utf-8").splitlines()
            if not lines:
                self._spool.unlink(missing_ok=True)
                return
            pending = [json.loads(x) for x in lines[:200] if x.strip()]
            if pending and self._post(pending):
                rest = lines[200:]
                if rest:
                    self._spool.write_text("\n".join(rest) + "\n", encoding="utf-8")
                else:
                    self._spool.unlink(missing_ok=True)
        except Exception:
            pass
