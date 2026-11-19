"""실행 시작 보호(5-19 ②) — bench 실측 도구(v2.tools.bench_probe)와 실행 프로그램(python -m v2.chk)이 같이 쓴다.

v1 이 켜진 채 v2 를 띄우면 v1 이 잠깐 놓은 포트(DC 는 필요할 때만 연다)를 v2 가 잡아 v1 공정을 깨뜨릴 수 있다. 그래서
- 포트 열기(open_ports): 정한 순서로 한 번씩 연다. 하나라도 못 열면 그때까지 연 포트를 닫고 실패한 장비·오류를 돌려준다
  (다시 시도하지 않는다 — 부르는 쪽이 멈춘다). 모두 열리면 연 채로 둔다(worker 가 그대로 쓴다 — SerialTransport.open 은
  이미 열려 있으면 아무것도 하지 않는다).
- 포트 사용 중 구분(port_in_use): pyserial(serialwin32.Serial.open)은 CreateFile 실패를
  SerialException("could not open port 'COMn': " + repr(ctypes.WinError())) 로 올린다. 다른 프로그램이 쥔 COM 포트는
  ERROR_ACCESS_DENIED(5) → PermissionError(13, …, None, 5), 없는 포트는 ERROR_FILE_NOT_FOUND(2) → FileNotFoundError.
  PortGuard 가 원래 예외를 TransportError 의 __cause__ 로 남기므로 그 예외(또는 그 글)로 가른다. 실기 확인은 아직 없음.
- 첫 읽기 안전 확인(first_read_check): 읽기만 한다. PLC 를 읽을 수 없음 / 히터 RUN 켜짐 / RF 출력(D40 > 0 또는
  순방향 ≥ 5 W) / DC 출력 ON / RF Pulse 출력 ON / 가스 유량 > 0.1 sccm 이면 이유를 돌려준다(공정 진행 중으로 보임).
  RF DAC enable(M00200)만 켜져 있고 출력이 없으면 v1 정상 종료 흔적으로 보고 [참고] 를 남긴 뒤 통과.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from v2.common.transport import Transport, TransportError
from v2.common.worker import Request, Status

PORT_HINT = "v1 프로그램이 켜져 있거나 포트 번호가 다를 수 있습니다"
APP_OPEN_ORDER = ("plc", "mfc", "ig", "rfpulse", "dc")      # 실행 프로그램 — DC 는 v1 이 필요할 때만 열어 맨 뒤
DEVICE_NAMES = {"plc": "PLC", "mfc": "MFC", "ig": "이온 게이지", "rfpulse": "RF Pulse", "dc": "DC Power"}
RF_FWD_ACTIVE_W = 5.0                                        # 순방향 이 값 이상이면 RF 출력 있음
GAS_ACTIVE_SCCM = 0.1                                        # 유량 이 값 초과면 가스 흐름 있음
READ_DEADLINE_S = 2.0
READ_WAIT_S = 4.0


def open_ports(transports: Dict[str, Transport], order: Sequence[str]) -> Tuple[List[str], Optional[Tuple[str, TransportError]]]:
    """order 순서로 한 번씩 연다. 모두 열면 (연 장비, None). 하나라도 못 열면 그때까지 연 포트를 닫고 ([], (장비, 오류))."""
    opened: List[str] = []
    for k in order:
        try:
            transports[k].open()
            opened.append(k)
        except TransportError as e:
            for o in opened:
                transports[o].close()
            return [], (k, e)
    return opened, None


def port_in_use(err: BaseException) -> bool:
    """포트 열기 오류가 '다른 프로그램이 사용 중'(윈도우 접근 거부)인가 — 아니면(포트 없음·시간 초과 등) False."""
    seen = set()
    e: Optional[BaseException] = err
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        if isinstance(e, PermissionError) or getattr(e, "winerror", None) == 5:
            return True
        if "PermissionError(" in str(e):                    # pyserial: "could not open port 'COM9': PermissionError(13, …)"
            return True
        e = e.__cause__ or e.__context__
    return False


def read_once(clock, worker, fn: Callable, wait_s: float = READ_WAIT_S) -> Tuple[Any, Any]:
    """worker 로 읽기 하나 — (값, 상태). 무응답·실패면 값 None."""
    box: list = []
    worker.submit(Request("read", fn, deadline=clock.monotonic() + READ_DEADLINE_S,
                          on_done=lambda r: (box.append(r), clock.notify())))
    clock.wait_until(clock.monotonic() + wait_s, lambda: bool(box))
    if box and box[0].status == Status.OK:
        return box[0].value, Status.OK
    return None, (box[0].status if box else "timeout")


def first_read_check(read: Callable[[str, Callable], Tuple[Any, Any]], plc, dc, rfp, mfc, ready: Dict[str, bool],
                     say: Callable[[str], None], note: Callable[[str], None],
                     on_plc: Optional[Callable[[], None]] = None) -> Optional[str]:
    """멈춰야 하면 이유, 아니면 None. 읽기만 한다(read(장비, fn) → (값, 상태)). RF 값은 판정과 상관없이 say 로 남긴다."""
    v, _ = read("plc", lambda s: plc.source_read(s, 0)) if ready.get("plc") else (None, None)
    if v is None:
        return "PLC 를 읽을 수 없음 — 히터 RUN 상태를 확인할 수 없어 멈춥니다"
    if on_plc:
        on_plc()
    dac, fwd, ref, en = v.get("rf.dac"), v.get("rf.fwd_w"), v.get("rf.ref_w"), v.get("rf.dac_enable")
    say(f"RF(PLC 아날로그): D40 {dac} · 순방향 {fwd} W · 반사 {ref} W · enable(M00200) {int(bool(en))}")
    if v["heater.run"]:
        return ("PLC 히터 RUN 이 켜져 있음 — bench 는 하트비트를 쓰지 않아 10 s 뒤 PLC 가 히터를 끕니다. "
                "히터를 끈 뒤 다시 실행하세요")
    if (dac or 0) > 0 or (fwd or 0.0) >= RF_FWD_ACTIVE_W:
        return f"RF 출력이 있음(D40 {dac} · 순방향 {fwd} W) — 공정 진행 중으로 보입니다"
    if en:
        note(f"[참고] M00200 켜짐 · D40 {dac} · 순방향 {fwd} W — v1 정상 종료 흔적(출력 없음)")
    if ready.get("dc"):
        outp, _ = read("dc", lambda s: dc._query(s, "OUTP?"))
        if outp == "1":
            return "DC 출력(OUTP?)이 켜져 있음 — 공정 진행 중으로 보입니다"
    if ready.get("rfpulse"):
        r, _ = read("rfpulse", lambda s: rfp.source_read(s, 0))
        if r and r.get("rfp.out_on"):
            return "RF Pulse 출력이 켜져 있음 — 공정 진행 중으로 보입니다"
    if ready.get("mfc"):
        m, _ = read("mfc", lambda s: mfc.source_read(s, 0))
        if m and max(m.get("mfc.ar_sccm") or 0.0, m.get("mfc.o2_sccm") or 0.0) > GAS_ACTIVE_SCCM:
            return f"가스 유량이 있음(Ar {m.get('mfc.ar_sccm')} · O2 {m.get('mfc.o2_sccm')} sccm) — 공정 진행 중으로 보입니다"
    return None
