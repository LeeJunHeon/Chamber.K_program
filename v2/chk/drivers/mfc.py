"""챔버K MFC 드라이버 — ATOVAC GPC3000(ASCII + CR, 값은 %FS). v1 device/MFC.py 를 읽고 다시 쓴 것(import 안 함).

읽기(스냅샷 묶기 소스): R5(압력) 매초 + R60(유량) — MFC_READ_PLAN: full=매초, alternate=격초(짝수 tick),
  auto=한 번 읽기가 마감의 60 %를 넘거나 실패하면 다음부터 격초. 유량 필드는 자기 품질(own_quality)이라 격초면 stale·나이 1.
  명령 사이는 MFC_CMD_GAP_MS, 질의 하나는 MFC_QUERY_TIMEOUT_MS.
단위: 유량 %FS ↔ sccm = %FS/100 × FS(연결 때 R70·R71 로 읽음, 못 읽으면 MFC_AR/O2_FS_SCCM). 읽기·설정 모두 환산.
  FS 가 100 이 아니면 이벤트 fs_not_100(v1 레시피 유량이 %FS 였을 수 있음 — 공정 전에 사용자 확인).
  압력 %FS ↔ mTorr = %FS/100 × MFC_PRESSURE_FS_TORR × 1000 (FS 1 Torr 면 v1 의 ×0.1 과 같다).
쓰기(응답 없음): v1 device/MFC.py 와 같은 재시도로 쓰고 되읽어 확인(I/O 하나가 worker 요청 하나 — 그 사이 tick 읽기가 끼어든다).
  - Q(유량): 되읽기 R65~ 가 다르면 Q 를 다시 보내고 1 s 뒤 다시 확인, 5회(v1 MFC.py:1058-1082, 비교 |값−목표| < 0.1)
  - FLOW ON/OFF: R69 로 지금 마스크 → 그 채널만 바꿔 L0 → R69 확인, 다르면 L0 1회 다시(v1 1084-1170). R69 를 못 읽으면 실패
    (v1 의 "0000" 대체값은 다른 채널을 끌 수 있어 옮기지 않음 — v1 1086)
  - O·C(밸브): MFC_VALVE_SETTLE_SEC 뒤 R6 확인 5회, 2·4번째 뒤 다시 보내고 밸브 대기(v1 1172-1211, 닫힘 < 1·열림 > 99)
  - S1: R1 확인 1 s 간격 5회(v1 953-981). D1~D4: R7 확인 1 s 간격 5회(v1 983-1021)
  - S2·S3·S4: v1 은 되읽기 없이 확인(666-670·689-692) — v2 는 R2~R4 를 한 번 읽어 다르면 경고만(실패 아님)
  - 질의 하나는 무응답이면 5회 다시(v1 enqueue retries_left=5, 354·448-454). 링크가 없으면(재연결 중) MFC_CMD_GUARD_S 안에서
    기다렸다 다시(v1 365-366 — 포트가 닫혀 있으면 큐에서 기다림). 영점(L5~L8 1, Z1)은 되읽기 없음.
bench: 질의(R…)만 — 설정 명령은 worker 거부 + 드라이버 이중 보호.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from v2.common.transport import Cancelled
from v2.common.worker import Request, Result, Status, Worker
from v2.chk.process_steps import MFC_CMD_GUARD_S

CH_AR, CH_O2 = 1, 2
QUERY_RETRIES = 5          # v1 device/MFC.py:354 enqueue(retries_left=5) — 무응답 질의를 다시 보내는 횟수
VERIFY_RETRY_S = 1.0       # v1 MFC_DELAY_MS(lib/config.py:798) — 되읽기 다시 하기 전 대기
VERIFY_TRIES = 5           # v1 max_attempts=5 (FLOW_SET·SP1·SPn_ON·밸브)
L0_TRIES = 2               # v1 _set_onoff_mask_and_verify max_attempts=2
LINK_RETRY_S = 1.0         # 재연결을 기다리며 다시 내는 간격
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
        self.clock, self.worker, self.cfg = clock, worker, cfg            # 같은 설정 사전(웹 설정 적용이 그 자리에서 바꾼다)
        self.writes_allowed = writes_allowed
        self.on_event = on_event
        self.gap_s = cfg["MFC_CMD_GAP_MS"] / 1000.0
        self.q_timeout_s = cfg["MFC_QUERY_TIMEOUT_MS"] / 1000.0
        self.fs = {CH_AR: cfg["MFC_AR_FS_SCCM"], CH_O2: cfg["MFC_O2_FS_SCCM"]}
        self.fs_source = "param"
        self.scale_mismatch: Optional[str] = None      # 연결 때 R33·R34·R74·R75·R37 대조 — 다르면 공정 시작 거부
        self.scale_raw: Dict[int, Optional[str]] = {}
        self.onoff_at_connect: Optional[str] = None     # 연결 때 R69 'bbbb'
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
        self._check_scale(session)
        try:                                              # 연결 때 ON/OFF(S51 남은 가스 판단 — 읽기만)
            self.onoff_at_connect = parse_r69(self._query(session, "R69"))
        except Cancelled:
            raise
        except Exception:
            self.onoff_at_connect = None
        odd = [f"ch{ch}={v:g}" for ch, v in self.fs.items() if abs(v - 100.0) > 1e-9]
        if odd:
            self._emit("fs_not_100", "MFC 풀스케일이 100 이 아님(" + ", ".join(odd) + ") — v1 레시피 유량이 %FS 였을 수 있다. "
                       "실제 공정 전에 사용자 확인 필요", "MFC_FS_NOT_100")

    def _check_scale(self, session) -> None:
        """R33·R34(압력 레인지·단위 → FS)·R70·R71(유량 FS)·R74·R75(유량 단위)를 설정과 대조 — 확실히 다르면
        MFC_SCALE_MISMATCH(공정 시작 거부), 못 읽으면 경고만(설정값 사용). 값은 자동으로 바꾸지 않는다.
        R37(MXYZ, X=Remote)은 정보로만: 10/20 bench 에서 'M006'(X=0, 매뉴얼상 Local)인데 v1 은 실제로 원격 제어가 됐다."""
        from v2.chk import inventory as INV
        raw = {}
        for n in (33, 34, 74, 75, 37):
            try:
                raw[n] = self._query(session, f"R{n}")
            except Cancelled:
                raise
            except Exception:
                raw[n] = None
        bad, unknown = [], []
        sc = INV.mfc_scale(raw[33], raw[34])
        if sc is None:
            unknown.append(f"압력 레인지·단위(R33 {raw[33]!r}, R34 {raw[34]!r})")
        elif abs(sc[2] - float(self.cfg["MFC_PRESSURE_FS_TORR"])) > 1e-9:
            bad.append(f"압력 FS {sc[2]:g} Torr(레인지 {sc[0]:02d}·단위 {sc[1]}) ≠ MFC_PRESSURE_FS_TORR {self.cfg['MFC_PRESSURE_FS_TORR']}")
        for ch, n in ((1, 74), (2, 75)):
            u = INV.mfc_int(raw[n], f"W{ch + 4}")
            if u is None:
                unknown.append(f"ch{ch} 단위(R{n} {raw[n]!r})")
            elif u != 1:
                bad.append(f"ch{ch} 단위 {INV.MFC_FLOW_UNIT.get(u, u)} ≠ SCCM")
        if self.fs_source == "R70/R71":
            for ch, v in self.fs.items():
                if abs(v - 100.0) > 1e-9:
                    bad.append(f"ch{ch} 유량 FS {v:g} ≠ 100(v1 레시피 유량 = %FS)")
        st = (raw[37] or "").strip()
        self._emit("remote_state", f"R37 {st!r}" + (" — X=0(매뉴얼상 Local)이지만 v1 은 원격 제어가 됨(10/20 bench) — 정보만"
                                                    if len(st) == 4 and st[:2] == "M0" else " — 정보만"))
        self.scale_raw = raw
        self.scale_mismatch = "; ".join(bad) or None
        if bad:
            self._emit("scale", "MFC 설정이 v2 설정과 다름 — 공정 시작 거부: " + "; ".join(bad), "MFC_SCALE_MISMATCH")
        if unknown:
            self._emit("scale", "MFC 설정을 못 읽음(설정값 사용): " + "; ".join(unknown), "MFC_SCALE_UNREAD")
        if not bad and not unknown:
            self._emit("scale", f"MFC 압력 FS {sc[2]:g} Torr · 유량 FS 100·단위 SCCM 확인")

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

    # ───────── 쓰기(v1 재시도 — 한 I/O 씩 worker 요청) ─────────
    def _op(self, name: str, gen, on_done=None) -> None:
        """gen: 생성기 — yield ("w", 명령) / ("q", 질의) → 응답(재시도 뒤에도 무응답이면 None) / ("sleep", 초).
        정상 끝나면 성공, MfcVerifyError 면 VERIFY_MISMATCH. 링크가 없으면 MFC_CMD_GUARD_S 안에서 기다렸다 같은 I/O 를 다시."""
        if not self.writes_allowed:
            if on_done:
                on_done(f"mfc:{name}", "BENCH_READ_ONLY", [])
            return
        t0 = self.clock.monotonic()
        guard = MFC_CMD_GUARD_S - 2.0                 # 공정 스텝 한도(MFC_CMD_GUARD_S)보다 먼저 결과를 낸다
        st: Dict[str, Any] = {"nb": None, "results": [], "done": False}

        def finish(reason: Optional[str]) -> None:
            if st["done"]:
                return
            st["done"] = True
            if reason and reason not in ("BENCH_READ_ONLY", "CANCELLED"):
                self._emit("verify_mismatch" if reason == "VERIFY_MISMATCH" else "write_failed",
                           f"{name}: {st.get('why') or reason}", reason)
            if on_done:
                on_done(f"mfc:{name}", reason, st["results"])

        def advance(value=None) -> None:
            try:
                item = gen.send(value)
            except StopIteration:
                finish(None)
                return
            except MfcVerifyError as e:
                st["why"] = str(e)
                finish("VERIFY_MISMATCH")
                return
            except Exception as e:                    # 생성기 오류(형식 등)
                st["why"] = repr(e)
                finish("DEVICE_REJECTED")
                return
            if item[0] == "sleep":
                st["nb"] = self.clock.monotonic() + float(item[1])
                advance(None)
                return
            issue(item, 0)

        def issue(item, tries: int, not_before: Optional[float] = None) -> None:
            kind, cmd = item
            nb = not_before if not_before is not None else st["nb"]
            st["nb"] = None
            fn = (lambda s: self._send(s, cmd)) if kind == "w" else (lambda s: self._query(s, cmd))
            self.worker.submit(Request("write", fn, on_done=lambda r: result(item, tries, r), name=f"mfc:{name}:{cmd}",
                                       not_before=nb, exec_timeout_s=self.q_timeout_s + self.gap_s + 0.2,
                                       confirms_link=(kind == "q")))

        def result(item, tries: int, r: Result) -> None:
            st["results"].append(r)
            if st["done"]:
                return
            if r.status == Status.OK:
                advance(r.value if item[0] == "q" else None)
            elif r.status == Status.TIMEOUT and item[0] == "q":
                if tries < QUERY_RETRIES:
                    issue(item, tries + 1)            # v1: 무응답 질의는 다시(재연결 뒤)
                else:
                    advance(None)                     # v1: 재시도 소진 — 응답 없음(None)으로 판정
            elif r.status in (Status.LINK_DOWN, Status.EXPIRED) or r.reason == "LINK_DOWN":
                if self.clock.monotonic() - t0 < guard:
                    issue(item, tries, self.clock.monotonic() + LINK_RETRY_S)     # 재연결 중 — 기다렸다 다시
                else:
                    st["why"] = f"{item[1]}: 링크 없음 {guard:.0f} s"
                    finish("LINK_DOWN")
            elif r.status == Status.CANCELLED:
                finish("CANCELLED")                   # safety 요청이 큐를 비웠다
            elif r.status == Status.REJECTED:
                finish(r.reason or "BENCH_READ_ONLY")
            else:
                st["why"] = f"{item[1]}: {r.error or r.reason}"
                finish(r.reason or "DEVICE_REJECTED")
        advance(None)

    def _verify_loop(self, name: str, query: str, check: Callable[[Optional[str]], bool], tries: int,
                     resend: Optional[str] = None, resend_on=None, resend_wait: float = VERIFY_RETRY_S):
        """되읽기 × tries — 맞으면 끝. resend 가 있으면 resend_on 회차(None = 매번) 뒤 다시 보내고 resend_wait 대기."""
        seen: List[str] = []
        for k in range(1, tries + 1):
            ans = yield ("q", query)
            seen.append(repr(ans))
            if check(ans):
                self._emit("verify_ok", f"{name}: {query}={ans} ({k}회째)")
                return
            if k < tries:
                if resend and (resend_on is None or k in resend_on):
                    yield ("w", resend)
                    yield ("sleep", max(VERIFY_RETRY_S, resend_wait))
                else:
                    yield ("sleep", VERIFY_RETRY_S)
        raise MfcVerifyError(f"{name}: {query} = " + "/".join(seen))

    def set_flow(self, ch: int, sccm: float, on_done=None) -> None:
        """FLOW_SET — v1 _verify_flow_set_async(MFC.py:1058-1082): Q → R6(4+ch) 확인, 다르면 Q 다시 + 1 s, 5회."""
        cmd, pct = flow_cmd(ch, sccm, self.fs[ch])

        def gen():
            yield ("w", cmd)
            yield from self._verify_loop(
                f"flow{ch}", f"R6{4 + ch}",
                lambda a: (v := parse_prefixed(a or "", f"Q{4 + ch}")) is not None and abs(v - pct) < 0.1,
                VERIFY_TRIES, resend=cmd)
            if ch == CH_AR:
                self.ar_set = sccm
            elif ch == CH_O2:
                self.o2_set = sccm
        self._op(f"flow{ch}", gen(), on_done)

    def _l0_gen(self, name: str, bits: str):
        """L0 쓰기 → R69 확인, 다르면 L0 1회 다시(v1 _set_onoff_mask_and_verify, MFC.py:1131-1170)."""
        cmd = f"L0{bits}"
        yield ("w", cmd)
        yield from self._verify_loop(name, "R69", lambda a: parse_r69(a or "") == bits, L0_TRIES, resend=cmd)

    def set_onoff(self, bits: str, on_done=None) -> None:
        if not re.fullmatch(r"[01]{4}", bits):
            raise ValueError(bits)
        self._op("onoff", self._l0_gen("onoff", bits), on_done)

    def all_flow_off(self, on_done=None) -> None:
        self.set_onoff("0000", on_done)

    def set_channel(self, ch: int, on: bool, on_done=None) -> None:
        """v1 FLOW_ON/FLOW_OFF(_apply_flow_onoff_with_L0, MFC.py:1084-1129): R69 로 지금 마스크를 읽고 그 채널만 바꿔 L0 → R69 확인.
        R69 를 끝내 못 읽으면 실패 — v1 의 "0000" 대체값(1086)은 다른 채널을 끌 수 있어 쓰지 않는다."""
        if ch not in (1, 2, 3, 4):
            raise ValueError(ch)
        name = f"ch{ch}_{'on' if on else 'off'}"

        def gen():
            ans = yield ("q", "R69")
            bits = parse_r69(ans or "")
            if ans is None or len(bits) != 4:
                raise MfcVerifyError(f"{name}: R69 를 읽지 못함({ans!r}) — L0 를 보내지 않음")
            b = list(bits)
            b[ch - 1] = "1" if on else "0"
            yield from self._l0_gen(name, "".join(b))
        self._op(name, gen(), on_done)

    def set_sp(self, n: int, mtorr: float, on_done=None) -> None:
        """SP1: R1 확인 1 s 간격 5회(v1 _verify_simple_async). SP2~SP4: v1 은 되읽기 없이 확인 — R(n) 1회 읽어 다르면 경고만."""
        cmd, hw = sp_cmd(n, mtorr, float(self.cfg["MFC_PRESSURE_FS_TORR"]))
        tol = max(float(self.cfg["MFC_SP1_VERIFY_TOL"]), 1e-9)

        def ok(a):
            v = parse_pressure(a) if a is not None else None
            return v is not None and abs(v - hw) <= tol

        def gen():
            yield ("w", cmd)
            if n == 1:
                yield from self._verify_loop("S1", "R1", ok, VERIFY_TRIES)
                self.sp_mtorr = mtorr
            else:
                ans = yield ("q", f"R{n}")
                if ok(ans):
                    self._emit("verify_ok", f"S{n}: R{n}={ans}")
                else:
                    self._emit("sp_readback", f"S{n}: {cmd} 뒤 R{n} = {ans!r} — 경고만(v1 은 되읽기 없이 진행)",
                               "VERIFY_MISMATCH")
        self._op(f"S{n}", gen(), on_done)

    def select_sp(self, n: int, on_done=None) -> None:
        """D1~D4 → R7 확인 1 s 간격 5회(v1 _verify_simple_async SPn_ON, MFC.py:983-1021)."""
        def gen():
            yield ("w", f"D{n}")
            yield from self._verify_loop(
                f"D{n}", "R7", lambda a: bool(a) and a.startswith("M") and len(a) > 1 and a[1] == str(n), VERIFY_TRIES)
            self.sp_active = n
        self._op(f"D{n}", gen(), on_done)

    def valve(self, open_: bool, on_done=None) -> None:
        """O·C → 밸브 대기(MFC_VALVE_SETTLE_SEC) → R6 확인 5회, 2·4번째 뒤 다시 보내고 밸브 대기(v1 MFC.py:1172-1211)."""
        cmd = "O" if open_ else "C"
        settle = float(self.cfg["MFC_VALVE_SETTLE_SEC"])

        def pos_ok(a):
            m = re.match(r"^(?:V\s*)?\+?([+\-]?\d+(?:\.\d+)?)$", (a or "").strip())
            pos = float(m.group(1)) if m else None
            return pos is not None and (pos > 99.0 if open_ else pos < 1.0)

        def gen():
            yield ("w", cmd)
            yield ("sleep", settle)
            yield from self._verify_loop(f"valve_{cmd}", "R6", pos_ok, VERIFY_TRIES, resend=cmd, resend_on=(2, 4),
                                         resend_wait=settle)
            self.sp_active = 0
        self._op(f"valve_{cmd}", gen(), on_done)

    def valve_hold(self, on_done=None) -> None:
        """H — 밸브를 그 자리에(압력 제어 멈춤) [MFC p.74]. 응답 규정이 없고 v1 은 쓰지 않았다 — 되읽기 없음(블록 공정 '밸브 정지')."""
        def gen():
            yield ("w", "H")
            self.sp_active = 0
        self._op("valve_H", gen(), on_done)

    def zero(self, cmd: str, on_done=None) -> None:
        """영점(L5~L8 1, Z1) — 되읽기 없음."""
        if not re.fullmatch(r"L[5-8] 1|Z1", cmd):
            raise ValueError(cmd)

        def gen():
            yield ("w", cmd)
        self._op(cmd, gen(), on_done)


# ───────── 쓰기 명령 글자(v1 lib/config.py:811-834 MFC_COMMANDS 와 같은 바이트) ─────────
def flow_cmd(ch: int, sccm: float, fs: float) -> tuple:
    """Q{ch} 값 — (명령, 되읽기 비교 %FS). v1 은 설정값을 반올림 없이 그대로 보낸다(f"Q{channel} {value}", value 는 float,
    device/MFC.py:609-614) — 장비는 소수 둘째 자리까지 저장(10/20 bench R65 'Q5+014.00'). FS 가 100 이면 %FS = sccm 이라
    v1 과 같은 글자, FS 가 100 이 아니면 공정 시작이 거부되므로(MFC_SCALE_MISMATCH) 환산값은 소수 둘째 자리로만."""
    if abs(float(fs) - 100.0) < 1e-9:
        v = float(sccm)
    else:
        v = round(float(sccm) / float(fs) * 100.0, 2)
    return f"Q{ch} {v}", v


def sp_cmd(n: int, mtorr: float, fs_torr: float) -> tuple:
    """S{n} 값 — (명령, 되읽기 비교 %FS). v1: hw = UI × MFC_PRESSURE_SCALE(0.1), f"S{n} {hw:.2f}"(device/MFC.py:551·644·663·686).
    FS 1 Torr 면 100 / (1 × 1000) = 0.1 — 같은 곱셈이라 같은 바이트."""
    hw = float(mtorr) * (100.0 / (float(fs_torr) * 1000.0))
    return f"S{n} {hw:.2f}", hw
