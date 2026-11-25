"""컨트롤러 값(W-22 E) — 장비 안에 저장된 설정값을 읽어 보여 주고, 관리자가 고른 몇 가지만 바꾼다. 순수 함수만(입출력은 드라이버).

MFC(GPC3000) — 읽기(질의만 — cmd_guard bench 허용 목록 안):
  SP1~5 값 R1~R4·R10, SP 종류 R26~R30(T: 0 위치·1 압력), P.Lead R41~R45(X, s), Gain R46~R50(M, %), 자동 제어 방식 R51
  (V: 0 Self-Tuning·1 PID), 밸브 종류 R23(J), 제어 방향 R32(N), 센서 레인지 R33(E 코드), 단위 R34(F 코드), 공정 한계 R11~R14
  (P1 한계 1 하한·P2 한계 1 상한·P3 한계 2 하한·P4 한계 2 상한, %FS), 유량 FS R70·R71(W1·W2), 유량 단위 R74·R75(W5·W6),
  상태 R37(MXYZ), 버전 R38(H…).
  읽는 때: 연결 뒤 한 번(연결 때 이미 읽은 R1~R4·R10·R33·R34·R37·R70·R71·R74·R75 는 그 응답을 쓴다), DEVICE_PARAMS_READ.
  1초 읽기에는 넣지 않는다 — 1초 읽기가 끝난 뒤 그 초에 남는 시간에만 질의 요청(읽기 우선순위 — 공정 명령이 먼저)을 넣고,
  공정·수동 조작 중이거나 설정 명령이 진행 중이면 끝날 때까지 미룬다(drivers/mfc.py params_*).
MFC — 바꾸기(DEVICE_PARAM_WRITE, 관리자, 대기·적재 상태만 — 범위는 아래 WRITE_ITEMS):
  SP1~5 값(S1~S5, mTorr ↔ %FS), Gain(M1~M5 정수 1~9999 %), P.Lead(X1~X5 0.01~2.00 s, 소수 둘째 자리), 자동 제어 방식(V0·V1),
  공정 한계(P1~P4 %FS 0~100). 쓰고 되읽어 비교(MFC_VERIFY_TRIES 회, 다르면 다시 쓰기) — 다르면 실패 이벤트. 장비 밖으로 나가는 글자는
  cmd_guard 의 좁은 허용 정규식(사용자 결정 2026-11-25)과 같아야 한다.
Ion Gauge(934) — E 응답(26항목)의 10·15~23 항목을 읽기만(바꾸지 않음): 10 필라멘트 상태(자동), 15 셋포인트 보호 해제,
  16 자동 필라멘트, 17 자동 필라멘트 셋포인트, 18·19 이온 SP1·SP2, 20 가스 계수, 21 감도, 22·23 게이지 A·B 셋포인트
  (셋포인트는 xy z = xy × 10^(z−2) Torr). 24~26(압력)은 1초 읽기 값 그대로.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional, Tuple

SP_Q = {1: 1, 2: 2, 3: 3, 4: 4, 5: 10}                    # SPn 값 질의 번호(SP5 = R10 — R5 는 압력)
MFC_QUERIES: tuple = (1, 2, 3, 4, 10, 26, 27, 28, 29, 30, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
                      23, 32, 33, 34, 11, 12, 13, 14, 70, 71, 74, 75, 37, 38)
CONNECT_REUSE = (1, 2, 3, 4, 10, 33, 34, 37, 70, 71, 74, 75)   # 연결 때 이미 읽는 질의 — 연결 뒤 읽기는 그 응답을 쓴다
SP_TYPE = {0: "위치", 1: "압력"}
AUTO_MODE = {0: "Self-Tuning", 1: "PID"}
LIMIT_LABEL = {1: "공정 한계 1 하한", 2: "공정 한계 1 상한", 3: "공정 한계 2 하한", 4: "공정 한계 2 상한"}
_NUM = r"([+\-]?\d+(?:\.\d+)?)"


def _pref(raw: Optional[str], prefix: str) -> Optional[float]:
    m = re.match("^" + re.escape(prefix) + r"\s*" + _NUM + "$", (raw or "").strip())
    return float(m.group(1)) if m else None


def _sp_pct(raw: Optional[str], n: int) -> Optional[float]:
    s = (raw or "").strip()
    if not s.upper().startswith(f"S{n}"):
        return None
    m = re.search(r"[+\-]?\s*\d+(?:\.\d+)?", s[2:])
    return float(m.group(0).replace(" ", "")) if m else None


def _int(v: Optional[float]) -> Optional[int]:
    return None if v is None else int(round(v))


# ───────── 바꿀 수 있는 것 ─────────
# item → (이름 틀, 번호 범위, 단위, 최소, 최대, 정수?) — SP 의 범위는 압력 FS(MFC_PRESSURE_FS_TORR × 1000 mTorr)
WRITE_ITEMS: Dict[str, dict] = {
    "sp": {"label": "SP{n} 설정값", "nos": (1, 2, 3, 4, 5), "unit": "mTorr", "min": 0.0, "max": None, "int": False},
    "gain": {"label": "SP{n} Gain", "nos": (1, 2, 3, 4, 5), "unit": "%", "min": 1, "max": 9999, "int": True},
    "lead": {"label": "SP{n} P.Lead", "nos": (1, 2, 3, 4, 5), "unit": "s", "min": 0.01, "max": 2.0, "int": False},
    "auto": {"label": "자동 제어 방식", "nos": (None,), "unit": "", "min": 0, "max": 1, "int": True},
    "limit": {"label": "{lim}", "nos": (1, 2, 3, 4), "unit": "%FS", "min": 0.0, "max": 100.0, "int": False},
}


def item_label(item: str, no: Optional[int]) -> str:
    spec = WRITE_ITEMS[item]
    return spec["label"].format(n=no, lim=LIMIT_LABEL.get(no or 0, ""))


def check_write(item: str, no: Any, value: Any, fs_torr: float) -> Tuple[Optional[str], Optional[int], Optional[float]]:
    """(오류 문구 또는 None, 번호, 값) — 형식·범위. 값은 정수 항목이면 정수로, P.Lead 는 소수 둘째 자리로."""
    spec = WRITE_ITEMS.get(str(item))
    if spec is None:
        return f"바꿀 수 없는 항목: {item!r}", None, None
    if spec["nos"] == (None,):
        no = None
    else:
        try:
            no = int(no)
        except (TypeError, ValueError):
            return f"번호가 필요합니다({'·'.join(str(n) for n in spec['nos'])})", None, None
        if no not in spec["nos"]:
            return f"번호 {no} — {'·'.join(str(n) for n in spec['nos'])} 중 하나여야 합니다", None, None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "값은 숫자여야 합니다", no, None
    v = float(value)
    if v != v:
        return "값은 숫자여야 합니다", no, None
    hi = float(fs_torr) * 1000.0 if item == "sp" else spec["max"]
    lo = spec["min"]
    if spec["int"]:
        if abs(v - round(v)) > 1e-9:
            return f"{item_label(item, no)} 은 정수여야 합니다", no, None
        v = int(round(v))
    elif item == "lead":
        if abs(v * 100 - round(v * 100)) > 1e-6:
            return "P.Lead 는 소수 둘째 자리까지입니다", no, None
        v = round(v, 2)
    if v < lo - 1e-9 or v > hi + 1e-9:
        u = spec["unit"]
        return f"{item_label(item, no)} {v:g} — 범위 {lo:g} ~ {hi:g}{(' ' + u) if u else ''}", no, None
    return None, no, v


def write_plan(item: str, no: Optional[int], value: float, fs_torr: float) -> Tuple[str, str, Callable[[Optional[str]], Optional[float]], Callable[[Optional[float]], bool]]:
    """(쓰기 글자, 되읽기 질의, 응답 → 값(화면 단위), 같은가) — 글자는 cmd_guard 허용 정규식과 같은 꼴."""
    if item == "sp":
        hw = float(value) * (100.0 / (float(fs_torr) * 1000.0))           # mTorr → %FS (drivers/mfc.sp_cmd 와 같은 식)
        cmd = f"S{no} {hw:.2f}"
        hw = float(cmd.split(" ", 1)[1])

        def parse(a, n=no):
            p = _sp_pct(a, n)
            return None if p is None else p / 100.0 * float(fs_torr) * 1000.0
        return cmd, f"R{SP_Q[no]}", parse, lambda got: got is not None and abs(got - hw / 100.0 * fs_torr * 1000.0) <= \
            0.01 / 100.0 * fs_torr * 1000.0 + 1e-9
    if item == "gain":
        return (f"M{no} {int(value)}", f"R{45 + no}", lambda a, n=no: _pref(a, f"M{n}"),
                lambda got: got is not None and int(round(got)) == int(value))
    if item == "lead":
        return (f"X{no} {float(value):.2f}", f"R{40 + no}", lambda a, n=no: _pref(a, f"X{n}"),
                lambda got: got is not None and abs(got - float(value)) < 0.005)
    if item == "auto":
        return (f"V{int(value)}", "R51", lambda a: _pref(a, "V"), lambda got: got is not None and int(round(got)) == int(value))
    if item == "limit":
        return (f"P{no} {float(value):.2f}", f"R{10 + no}", lambda a, n=no: _pref(a, f"P{n}"),
                lambda got: got is not None and abs(got - float(value)) <= 0.01 + 1e-9)
    raise ValueError(item)


def value_text(item: str, v: Optional[float]) -> str:
    if v is None:
        return "모름"
    if item == "auto":
        return AUTO_MODE.get(int(round(v)), f"{v:g}")
    u = WRITE_ITEMS[item]["unit"]
    s = f"{int(round(v))}" if WRITE_ITEMS[item]["int"] else (f"{v:.2f}" if item in ("lead", "limit", "sp") else f"{v:g}")
    return f"{s} {u}".strip()


# ───────── 읽은 응답 → 화면 값 ─────────
def mfc_view(raw: Dict[int, Optional[str]], fs_torr: float) -> dict:
    """질의 응답(R 번호 → 원문) → 화면 값. 못 읽었거나 형식이 다르면 그 값은 None(원문은 raw 에 그대로)."""
    from v2.chk import inventory as INV
    sp = []
    for n in (1, 2, 3, 4, 5):
        pct = _sp_pct(raw.get(SP_Q[n]), n)
        t = _int(_pref(raw.get(25 + n), f"T{n}"))
        sp.append({"no": n, "pct": pct, "mtorr": None if pct is None else round(pct / 100.0 * fs_torr * 1000.0, 3),
                   "type": t, "typeText": SP_TYPE.get(t) if t is not None else None,
                   "leadS": _pref(raw.get(40 + n), f"X{n}"), "gainPct": _int(_pref(raw.get(45 + n), f"M{n}"))})
    auto = _int(_pref(raw.get(51), "V"))
    rng, unit = _int(_pref(raw.get(33), "E")), _int(_pref(raw.get(34), "F"))
    sc = INV.mfc_scale(raw.get(33), raw.get(34))
    return {
        "sp": sp, "autoMode": auto, "autoModeText": AUTO_MODE.get(auto) if auto is not None else None,
        "valveType": _int(_pref(raw.get(23), "J")), "direction": _int(_pref(raw.get(32), "N")),
        "rangeCode": rng, "rangeText": (f"{INV.MFC_RANGE[rng]:g}" if rng in INV.MFC_RANGE else None),
        "unitCode": unit, "unitText": (INV.MFC_UNIT[unit][0] if unit in INV.MFC_UNIT else None),
        "fsTorr": None if sc is None else sc[2],
        "limits": [{"no": n, "label": LIMIT_LABEL[n], "pct": _pref(raw.get(10 + n), f"P{n}")} for n in (1, 2, 3, 4)],
        "flowFs": [_pref(raw.get(70), "W1"), _pref(raw.get(71), "W2")],
        "flowUnits": [INV.MFC_FLOW_UNIT.get(_int(_pref(raw.get(74), "W5")) or 0), INV.MFC_FLOW_UNIT.get(_int(_pref(raw.get(75), "W6")) or 0)],
        "status": (raw.get(37) or "").strip() or None, "version": (raw.get(38) or "").strip() or None,
        "raw": {f"R{n}": raw.get(n) for n in MFC_QUERIES},
    }


def mfc_value(view: Optional[dict], item: str, no: Optional[int]) -> Optional[float]:
    """화면 값에서 그 항목의 지금 값(바꾸기 전 값 — 이벤트의 old)."""
    if not view:
        return None
    if item in ("sp", "gain", "lead"):
        r = (view.get("sp") or [{}] * 5)[int(no) - 1]
        return r.get({"sp": "mtorr", "gain": "gainPct", "lead": "leadS"}[item])
    if item == "auto":
        return view.get("autoMode")
    if item == "limit":
        return (view.get("limits") or [{}] * 4)[int(no) - 1].get("pct")
    return None


IG_ITEMS = {10: "필라멘트 상태(자동)", 15: "셋포인트 보호 해제", 16: "자동 필라멘트", 17: "자동 필라멘트 셋포인트",
            18: "이온 셋포인트 1", 19: "이온 셋포인트 2", 20: "가스 계수", 21: "감도", 22: "게이지 A 셋포인트", 23: "게이지 B 셋포인트"}


def ig_view(r, read_at: Optional[str]) -> Optional[dict]:
    """IgReading(drivers/ig.py — E 응답) → 화면 값(읽기만)."""
    if r is None:
        return None
    return {"readAt": read_at, "autoFilamentState": r.auto_filament_state, "spProtectOff": r.sp_protect_off,
            "autoFilament": r.auto_filament, "autoFilamentSpTorr": r.sp_auto_filament, "ionSp1Torr": r.sp_ion1,
            "ionSp2Torr": r.sp_ion2, "gasFactor": r.gas_factor, "sensitivity": r.sensitivity,
            "gaugeASpTorr": r.sp_a, "gaugeBSpTorr": r.sp_b,
            "raw": {f"E{n}": r.items[n - 1] for n in sorted(IG_ITEMS) if r.items and len(r.items) >= n}}
