"""챔버K MFC 드라이버 — ATOVAC GPC3000(ASCII + CR, 값은 %FS). v1 device/MFC.py 를 읽고 다시 쓴 것(import 안 함).

읽기(스냅샷 묶기 소스): R5(압력) 매초 + R60(유량) — MFC_READ_PLAN: full=매초, alternate=격초(짝수 tick),
  auto=한 번 읽기가 마감의 60 %를 넘거나 실패하면 다음부터 격초. 유량 필드는 자기 품질(own_quality)이라 격초면 stale·나이 1.
  명령 사이는 MFC_CMD_GAP_MS, 질의 하나는 MFC_QUERY_TIMEOUT_MS.
단위: 유량 %FS ↔ sccm = %FS/100 × FS(연결 때 R70·R71 로 읽음, 못 읽으면 MFC_AR/O2_FS_SCCM). 읽기·설정 모두 환산.
  FS 가 100 이 아니면 이벤트 fs_not_100(v1 레시피 유량이 %FS 였을 수 있음 — 공정 전에 사용자 확인).
  압력 %FS ↔ mTorr = %FS/100 × MFC_PRESSURE_FS_TORR × 1000 (FS 1 Torr 면 v1 의 ×0.1 과 같다).
쓰기(응답 없음): 여러 tick 에 걸친 Sequence 로 쓰고 되읽어 확인 — Q→R65~, L0→R69, S1→R1, D→R7, O·C→R6(밸브 대기 뒤).
  되읽기가 MFC_VERIFY_TRIES 안에 맞지 않으면 VERIFY_MISMATCH. 영점(L5~L8 1, Z1)은 되읽기 없음.
bench: 질의(R…)만 — 설정 명령은 worker 거부 + 드라이버 이중 보호.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.transport import Cancelled
from v2.common.worker import SEQ_DONE, Sequence, Worker

CH_AR, CH_O2 = 1, 2
_NUM = r"([+\-]?\d+(?:\.\d+)?)"


class MfcError(Exception):
    pass


class MfcReadOnly(MfcError):
    reason = "BENCH_READ_ONLY"


class MfcVerifyError(MfcError):
    reason = "VERIFY_MISMATCH"


@dataclass(frozen=True)
class MfcEvent:
    kind: str     # fs / fs_not_100 / verify_ok / verify_mismatch / read_plan
    detail: str
    reason: Optional[str] = None


# ───────── 파서(v1 _parse_* 와 같은 규칙) ─────────
def parse_r60(line: str) -> Optional[List[float]]:
    s = (line or "").strip()
    if not s.startswith("Q0"):
        return None
    nums = re.findall(_NUM, s[2:])
    return [float(x) for x in nums] if nums else None


def parse_prefixed(line: str, prefix: str) -> Optional[float]:
    m = re.match("^" + re.escape(prefix) + r"\s*" + _NUM + "$", (line or "").strip())
    return float(m.group(1)) if m else None


def parse_pressure(line: str) -> Optional[float]:
    s = (line or "").strip()
    m = re.search(r"\+\s*" + _NUM, s)
    if m:
        return float(m.group(1))
    nums = re.findall(_NUM, s)
    return float(nums[-1]) if nums else None


def parse_r69(line: str) -> str:
    s = (line or "").strip()
    payload = s[2:] if s.startswith("L0") else (s[1:] if s.startswith("L") else s)
    return "".join(c for c in payload if c in "01")[:4]


class MfcDriver:
    def __init__(self, clock, worker: Worker, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[MfcEvent], None]] = None) -> None:
        self.clock, self.worker, self.cfg = clock, worker, dict(cfg)
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.gap_s = cfg["MFC_CMD_GAP_MS"] / 1000.0
        self.q_timeout_s = cfg["MFC_QUERY_TIMEOUT_MS"] / 1000.0
        self.fs = {CH_AR: cfg["MFC_AR_FS_SCCM"], CH_O2: cfg["MFC_O2_FS_SCCM"]}
        self.fs_source = "param"
        self.alternate = cfg["MFC_READ_PLAN"] == "alternate"
        self.ar_set: Optional[float] = None
        self.o2_set: Optional[float] = None
        self.sp_active: Optional[int] = None
        self.sp_mtorr: Optional[float] = None
        self._last_cmd = -1e9
        worker.on_connect = self._on_connect

    def _emit(self, kind: str, detail: str, reason: Optional[str] = None) -> None:
        if self.on_event:
            self.on_event(MfcEvent(kind, detail, reason))

    # ───────── 단위 ─────────
    def pct_to_sccm(self, ch: int, pct: float) -> float:
        return pct / 100.0 * self.fs[ch]

    def sccm_to_pct(self, ch: int, sccm: float) -> float:
        return sccm / self.fs[ch] * 100.0

    def pct_to_mtorr(self, pct: float) -> float:
        return pct / 100.0 * self.cfg["MFC_PRESSURE_FS_TORR"] * 1000.0

    def mtorr_to_pct(self, mtorr: float) -> float:
        return mtorr / 1000.0 / self.cfg["MFC_PRESSURE_FS_TORR"] * 100.0

    # ───────── 줄 입출력 ─────────
    def _wait_gap(self) -> None:
        ready = self._last_cmd + self.gap_s
        if ready > self.clock.monotonic():
            self.clock.sleep_until(ready)

    def _send(self, session, cmd: str) -> None:
        if not cmd.startswith("R") and not self.writes_allowed:
            raise MfcReadOnly(f"bench — 보내지 않음: {cmd}")
        self._wait_gap()
        session.transport.reset_input()
        session.write((cmd + "\r").encode("ascii"))
        self._last_cmd = self.clock.monotonic()

    def _query(self, session, cmd: str) -> str:
        if not cmd.startswith("R"):
            raise ValueError("질의는 R…")
        self._send(session, cmd)
        dl = min(session.deadline, self.clock.monotonic() + self.q_timeout_s)
        return session.read_until(b"\r", deadline=dl).decode("ascii", errors="replace").strip()

    # ───────── 연결 ─────────
    def _on_connect(self, session) -> None:
        self._last_cmd = -1e9
        for ch, key in ((CH_AR, "MFC_AR_FS_SCCM"), (CH_O2, "MFC_O2_FS_SCCM")):
            try:
                v = parse_prefixed(self._query(session, f"R{69 + ch}"), f"W{ch}")
            except Cancelled:
                raise
            except Exception:
                v = None
            if v and v > 0:
                self.fs[ch], self.fs_source = v, "R70/R71"
            else:
                self.fs[ch] = self.cfg[key]
        self._emit("fs", f"FS Ar {self.fs[CH_AR]:g} sccm · O2 {self.fs[CH_O2]:g} sccm ({self.fs_source})")
        odd = [f"ch{ch}={v:g}" for ch, v in self.fs.items() if abs(v - 100.0) > 1e-9]
        if odd:
            self._emit("fs_not_100", "MFC 풀스케일이 100 이 아님(" + ", ".join(odd) + ") — v1 레시피 유량이 %FS 였을 수 있다. "
                       "실제 공정 전에 사용자 확인 필요", "MFC_FS_NOT_100")

    # ───────── 읽기 ─────────
    def source_read(self, session, seq: int) -> Dict[str, Any]:
        t0 = self.clock.monotonic()
        out: Dict[str, Any] = {}
        p = parse_pressure(self._query(session, "R5"))
        if p is None:
            raise MfcError("R5 형식 다름")
        out["mfc.pressure_mtorr"] = round(self.pct_to_mtorr(p), 3)
        if not self.alternate or seq % 2 == 0:
            vals = parse_r60(self._query(session, "R60"))
            if not vals or len(vals) < 2:
                raise MfcError("R60 형식 다름")
            out["mfc.ar_sccm"] = round(self.pct_to_sccm(CH_AR, vals[0]), 3)
            out["mfc.o2_sccm"] = round(self.pct_to_sccm(CH_O2, vals[1]), 3)
            took = self.clock.monotonic() - t0
            if (self.cfg["MFC_READ_PLAN"] == "auto" and not self.alternate
                    and took > 0.6 * self.cfg["MFC_READ_DEADLINE_MS"] / 1000.0):
                self.alternate = True
                self._emit("read_plan", f"한 번 읽기 {took * 1000:.0f} ms — R60 격초로")
        out.update({"mfc.ar_set": self.ar_set, "mfc.o2_set": self.o2_set, "mfc.sp_active": self.sp_active,
                    "mfc.sp_mtorr": self.sp_mtorr})
        return out

    # ───────── 쓰기(Sequence: 쓰기 → 되읽기 × N) ─────────
    def _write_verify(self, name: str, cmd: str, query: str, check: Callable[[str], bool],
                      on_done=None, settle_s: Optional[float] = None, after_ok: Optional[Callable[[], None]] = None) -> None:
        tries = self.cfg["MFC_VERIFY_TRIES"]
        seen: List[str] = []

        def write(s):
            self._send(s, cmd)

        def verify(k):
            def fn(s):
                try:
                    ans = self._query(s, query)
                except Cancelled:
                    raise
                except Exception as e:
                    ans = f"무응답({type(e).__name__})"
                seen.append(ans)
                if check(ans):
                    if after_ok:
                        after_ok()
                    self._emit("verify_ok", f"{name}: {cmd} → {query}={ans}")
                    return SEQ_DONE
                if k >= tries:
                    raise MfcVerifyError(f"{name}: {cmd} 뒤 {query} = " + "/".join(seen))
            return fn

        def done(n, reason, results):
            if reason and reason != "BENCH_READ_ONLY":
                self._emit("verify_mismatch", f"{name}: {cmd} 되읽기 불일치 " + "/".join(seen), "VERIFY_MISMATCH")
            if on_done:
                on_done(n, "VERIFY_MISMATCH" if reason and reason != "BENCH_READ_ONLY" else reason, results)

        steps = [(cmd, write)] + [(f"{query} {k}", verify(k)) for k in range(1, tries + 1)]
        self.worker.submit_sequence(Sequence(f"mfc:{name}", steps, gap_s=settle_s if settle_s else self.gap_s,
                                             step_timeout_s=self.q_timeout_s + self.gap_s + 0.2, on_done=done,
                                             confirms_link=False))

    def set_flow(self, ch: int, sccm: float, on_done=None) -> None:
        pct = round(self.sccm_to_pct(ch, sccm), 1)

        def ok():
            if ch == CH_AR:
                self.ar_set = sccm
            elif ch == CH_O2:
                self.o2_set = sccm
        self._write_verify(f"flow{ch}", f"Q{ch} {pct:.1f}", f"R6{4 + ch}",
                           lambda a: (v := parse_prefixed(a, f"Q{4 + ch}")) is not None and abs(v - pct) <= 0.1,
                           on_done, after_ok=ok)

    def set_onoff(self, bits: str, on_done=None) -> None:
        if not re.fullmatch(r"[01]{4}", bits):
            raise ValueError(bits)
        self._write_verify("onoff", f"L0{bits}", "R69", lambda a: parse_r69(a) == bits, on_done)

    def all_flow_off(self, on_done=None) -> None:
        self.set_onoff("0000", on_done)

    def set_sp(self, n: int, mtorr: float, on_done=None) -> None:
        hw = round(self.mtorr_to_pct(mtorr), 2)
        tol = self.cfg["MFC_SP1_VERIFY_TOL"]

        def ok():
            if n == 1:
                self.sp_mtorr = mtorr
        self._write_verify(f"S{n}", f"S{n} {hw:.2f}", f"R{n}",
                           lambda a: (v := parse_pressure(a)) is not None and abs(v - hw) <= tol, on_done, after_ok=ok)

    def select_sp(self, n: int, on_done=None) -> None:
        def ok():
            self.sp_active = n
        self._write_verify(f"D{n}", f"D{n}", "R7", lambda a: a.startswith("M") and len(a) > 1 and a[1] == str(n),
                           on_done, after_ok=ok)

    def valve(self, open_: bool, on_done=None) -> None:
        cmd = "O" if open_ else "C"

        def ok():
            self.sp_active = 0
        self._write_verify(f"valve_{cmd}", cmd, "R6",
                           lambda a: (v := parse_pressure(a)) is not None and (v >= 99.0 if open_ else v <= 1.0),
                           on_done, settle_s=self.cfg["MFC_VALVE_SETTLE_SEC"], after_ok=ok)

    def zero(self, cmd: str, on_done=None) -> None:
        """영점(L5~L8 1, Z1) — 되읽기 없음."""
        if not re.fullmatch(r"L[5-8] 1|Z1", cmd):
            raise ValueError(cmd)
        self.worker.submit_sequence(Sequence(f"mfc:{cmd}", [(cmd, lambda s: self._send(s, cmd))],
                                             step_timeout_s=self.gap_s + 0.5, on_done=on_done, confirms_link=False))
