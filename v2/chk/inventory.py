"""장비 설정 읽기(인벤토리) — 질의 목록·응답 형식·해석·대조. bench 도구가 쓰고, 연결 검사(MFC 스케일 등)도 해석을 같이 쓴다.

값을 추측하지 않는다(사용자 지시 2026-10-16): 응답 형식이 매뉴얼 예와 다르면 원문 그대로 두고 '형식 다름'.
대조에서 맞지 않으면 '!!!' 경고만 — 값을 자동으로 바꾸지 않는다.
근거(인쇄 쪽): DC [DC p.86-98, p.15, p.53, p.78, p.83, p.94, p.104-105], MFC [MFC p.30-31, p.54, p.74, p.80-88],
IG [IG p.41, p.44, p.53-54].
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from v2.chk import cmd_guard as G

NUM = r"[+\-]?\d+(?:\.\d+)?"


@dataclass
class Item:
    device: str
    query: str
    raw: Optional[str]                 # None = 무응답
    ok_format: bool = True
    meaning: str = ""
    note: str = ""                     # 해석·대조 결과 문장('!!!' 로 시작하면 경고)


@dataclass
class Inventory:
    items: List[Item] = field(default_factory=list)
    checks: List[str] = field(default_factory=list)      # [확인]·[미확인]·!!! 줄
    values: Dict[str, Any] = field(default_factory=dict)

    def by(self, dev: str, q: str) -> Optional[Item]:
        for i in self.items:
            if i.device == dev and i.query == q:
                return i
        return None

    def raw(self, dev: str, q: str) -> Optional[str]:
        i = self.by(dev, q)
        return None if i is None else i.raw

    def as_json(self) -> dict:
        return {"items": [i.__dict__ for i in self.items], "checks": self.checks, "values": self.values}


# ───────────── DC(ODA EX) ─────────────
DC_FORMAT = {
    "*IDN?": (r"[^,]+,[^,]+,[^,]+", "제조사,모델,버전(시스템-앞면-SCPI) [p.97]"),
    "*SN?": (r"\S+", "일련번호 [p.97]"), "SYST:VERS?": (r"\d{4}\.\d+", "SCPI 버전 [p.96]"),
    "OUTP?": (r"[01]", "출력 0 OFF / 1 ON [p.53]"), "KEYL?": (r"\s*[01]\s*", "앞면 잠금 0/1 [p.97]"),
    "APPL?": (rf"{NUM},{NUM}", "전압,전류 설정 [p.88]"),
    "VOLT?": (NUM, "전압 설정 V [p.88]"), "CURR?": (NUM, "전류 설정 A [p.90]"),
    "VOLT:STEP?": (NUM, "전압 STEP V [p.88]"), "CURR:STEP?": (NUM, "전류 STEP A [p.90]"),
    "VOLT:OVP?": (NUM, "OVP 레벨 V [p.89]"), "CURR:OCP?": (NUM, "OCP 레벨 A [p.90]"),
    "VOLT:OVP:TRIP?": (r"[01]", "OVP 트립 0/1 [p.89]"), "CURR:OCP:TRIP?": (r"[01]", "OCP 트립 0/1 [p.90]"),
    "VOLT:UVL?": (NUM, "UVL V [p.89]"), "VOLT:OVL?": (NUM, "OVL V — 기본값 = 설정 가능한 최대 [p.15, p.90]"),
    "CURR:UCL?": (NUM, "UCL A [p.91]"), "CURR:OCL?": (NUM, "OCL A — 기본값 = 설정 가능한 최대 [p.15, p.91]"),
    "FACT:LAST-STA?": (r"DISABLE|SAFETY|FULLY", "전원 복귀 상태 [p.94]"),
    "FACT:AUTO-CUR?": (r"[01]", "앞면 자동 커서 [p.94]"), "FACT:AUTO-LOC?": (r"[01]", "자동 키 잠금 [p.94]"),
    "FACT:OVP?": (r"[01]", "OVP 사용 [p.95]"), "FACT:OCP?": (r"[01]", "OCP 사용 [p.95]"),
    "FACT:ADC?": (r"(5|20|50|100|300)Hz|1\.3KHz", "ADC 샘플링 [p.95]"),
    "MEAS:ALL?": (rf"{NUM},{NUM}", "측정 V,A [p.92]"), "MEAS:VOLT?": (NUM, "측정 V [p.92]"),
    "MEAS:CURR?": (NUM, "측정 A [p.92]"), "SYST:ERR?": (r"[+\-]?\d+,\".*\"", "오류 기록(최근 것부터, 읽으면 지워짐) [p.54, p.96]"),
}
# 사양표 [DC p.104-105] 는 PDF 에 그림으로만 있어 텍스트로 대조하지 못한다 — 사용자가 적은 한 줄만(2026-10-16).
DC_SPEC = {"EX 600-1": (630.0, 1.1)}


def dc_interpret(inv: Inventory) -> None:
    c, r = inv.checks, lambda q: inv.raw("dc", q)
    ls = r("FACT:LAST-STA?")
    if ls is None:
        c.append("[미확인] DC LAST-STA — 응답 없음")
    elif ls == "FULLY":
        c.append("!!! DC LAST-STA = FULLY — 전원 차단 전 출력까지 되살아남 [p.78]. 앞면 FACTORY 메뉴에서 DISABLE·SAFETY 로(사람이)")
    else:
        c.append(f"[확인] DC LAST-STA = {ls} — 부팅 때 출력 OFF [p.15, p.53, p.78]")
    ovl, ocl = _f(r("VOLT:OVL?")), _f(r("CURR:OCL?"))
    if ovl is None or ocl is None:
        c.append("[미확인] DC 정격 — VOLT:OVL?·CURR:OCL? 를 못 읽음")
    else:
        inv.values["dc_ovl"], inv.values["dc_ocl"] = ovl, ocl
        cand = [m for m, (v, a) in DC_SPEC.items() if abs(v - ovl) < 0.5 and abs(a - ocl) < 0.005]
        c.append(f"[확인] DC OVL {ovl:g} V · OCL {ocl:g} A — 기본값이면 설정 가능한 최대[p.15, p.98] → 정격 후보 "
                 + (", ".join(cand) if cand else "사양표(그림)와 대조 못 함 — 사람이 [p.104-105]") )
    for q, name in (("VOLT:OVP:TRIP?", "OVP"), ("CURR:OCP:TRIP?", "OCP")):
        if r(q) == "1":
            c.append(f"!!! DC {name} 트립 중 — 트립 중에는 출력이 나오지 않는다 [p.89-91]")
    c.append(f"[확인] DC OVP 사용 {r('FACT:OVP?')} 레벨 {r('VOLT:OVP?')} V · OCP 사용 {r('FACT:OCP?')} 레벨 {r('CURR:OCP?')} A"
             if r("FACT:OVP?") is not None else "[미확인] DC OVP·OCP")
    adc = r("FACT:ADC?")
    if adc is not None:
        c.append(("[확인] " if adc in ("20Hz", "50Hz") else "!!! ") + f"DC ADC {adc} — 공장 기본 20 Hz, PC 제어 권장 20~50 Hz [p.83]")


# ───────────── MFC(GPC3000) ─────────────
MFC_FORMAT = {0: ("S0", "아날로그 셋포인트 %FS"), 5: ("P", "압력 %FS"), 6: ("V", "밸브 열림 %"), 7: ("M", "상태 MXYZ"),
              10: ("S5", "셋포인트 5 %FS"), 23: ("J", "밸브 종류"), 32: ("N", "제어 방향"), 33: ("E", "압력 센서 레인지 코드"),
              34: ("F", "압력 단위"), 37: ("M", "시스템 상태 MXYZ(X 1=Remote)"), 38: ("H3", "소프트웨어 버전"),
              51: ("V", "자동 압력 제어 종류"), 60: ("Q0", "전 채널 유량 %FS"), 65: ("Q5", "ch1 설정 %FS"),
              66: ("Q6", "ch2 설정 %FS"), 69: ("L0", "ON/OFF"), 70: ("W1", "ch1 FS"), 71: ("W2", "ch2 FS"),
              74: ("W5", "ch1 단위(1 SCCM, 2 SLM)"), 75: ("W6", "ch2 단위")}
for _n in range(1, 5):
    MFC_FORMAT[_n] = (f"S{_n}", f"셋포인트 {_n} %FS")
for _n in range(11, 15):
    MFC_FORMAT[_n] = (f"P{_n - 10}", "공정 한계 %FS")
for _n in range(25, 31):
    MFC_FORMAT[_n] = (f"T{_n - 25}", "셋포인트 종류(0 위치, 1 압력)")
for _n in range(41, 46):
    MFC_FORMAT[_n] = (f"X{_n - 40}", "P.Lead s")
for _n in range(46, 51):
    MFC_FORMAT[_n] = (f"M{_n - 45}", "Gain %")
MFC_FORMAT[61], MFC_FORMAT[62] = ("Q", "ch1 유량 %FS"), ("Q", "ch2 유량 %FS")
MFC_RANGE = {0: 0.1, 1: 0.2, 2: 0.5, 3: 1, 4: 2, 5: 5, 6: 10, 7: 50, 8: 100, 9: 300, 10: 500, 11: 1000, 12: 1250, 13: 2500,
             14: 5000, 15: 10000, 16: 1.33, 17: 2.66, 18: 13.33, 19: 133.3, 20: 1333, 21: 6666, 22: 13332}   # [MFC p.74, p.83]
MFC_UNIT = {0: ("Torr", 1.0), 1: ("mTorr", 1e-3), 2: ("mbar", 0.750062), 3: ("µbar", 7.50062e-4), 4: ("kPa", 7.50062),
            5: ("Pa", 7.50062e-3), 6: ("cmH2O", 0.735559), 7: ("inH2O", 1.86832)}   # 단위 코드 [p.83], Torr 환산은 표준 상수
MFC_FLOW_UNIT = {1: "SCCM", 2: "SLM"}    # [p.88]


def mfc_format_ok(n: int, raw: str) -> bool:
    pre, _ = MFC_FORMAT.get(n, ("", ""))
    s = raw.strip()
    if n in (7, 37):
        return re.fullmatch(r"M\d{3}", s) is not None
    if n == 60:
        return re.fullmatch(r"Q0(\s*[+\-]\d+(\.\d+)?){2,4}", s) is not None
    if n == 69:
        return re.fullmatch(r"L0\s*[01]{4}", s) is not None
    if n == 38:
        return s.startswith("H3")
    if n in (61, 62):
        return re.fullmatch(rf"Q[0-2]\s*{NUM}", s) is not None      # 표는 Q1·Q2, 예는 Q0+100.0 [p.86]
    if n in (23, 32, 33, 34, 51, 74, 75) or 25 <= n <= 30:
        return re.fullmatch(rf"{re.escape(pre)}\s*[+]?\d+", s) is not None
    return re.fullmatch(rf"{re.escape(pre)}\s*{NUM}", s) is not None


def mfc_int(raw: Optional[str], pre: str) -> Optional[int]:
    if raw is None:
        return None
    m = re.fullmatch(rf"{re.escape(pre)}\s*\+?(\d+)", raw.strip())
    return int(m.group(1)) if m else None


def mfc_num(raw: Optional[str], pre: str) -> Optional[float]:
    if raw is None:
        return None
    m = re.fullmatch(rf"{re.escape(pre)}\s*({NUM})", raw.strip())
    return float(m.group(1)) if m else None


def mfc_scale(e_raw: Optional[str], f_raw: Optional[str]) -> Optional[tuple]:
    """(레인지 코드, 단위 코드, FS Torr) — R33·R34 를 못 읽거나 모르는 코드면 None."""
    code, unit = mfc_int(e_raw, "E"), mfc_int(f_raw, "F")
    if code not in MFC_RANGE or unit not in MFC_UNIT:
        return None
    return code, unit, MFC_RANGE[code] * MFC_UNIT[unit][1]


def mfc_interpret(inv: Inventory, cfg: Dict[str, Any]) -> None:
    c, r = inv.checks, lambda n: inv.raw("mfc", f"R{n}")
    sc = mfc_scale(r(33), r(34))
    if sc is None:
        c.append(f"[미확인] MFC 압력 레인지·단위 — R33 {r(33)!r} · R34 {r(34)!r}")
    else:
        code, unit, fs = sc
        inv.values["mfc_pressure_fs_torr"] = fs
        txt = f"MFC 압력 레인지 {code:02d}({MFC_RANGE[code]:g}) · 단위 {unit}({MFC_UNIT[unit][0]}) → FS {fs:g} Torr"
        if abs(fs - float(cfg["MFC_PRESSURE_FS_TORR"])) < 1e-9:
            c.append(f"[확인] {txt} = MFC_PRESSURE_FS_TORR — v1 0.1 배율(mTorr = %FS × 10)과 같다 [p.74 Note, p.30-31]")
        else:
            c.append(f"!!! {txt} ≠ MFC_PRESSURE_FS_TORR {cfg['MFC_PRESSURE_FS_TORR']} — R5 %FS 환산이 다르다(값은 자동으로 바꾸지 않음)")
    for ch, (nf, nu) in ((1, (70, 74)), (2, (71, 75))):
        fs, unit = mfc_num(r(nf), f"W{ch}"), mfc_int(r(nu), f"W{ch + 4}")
        name = MFC_FLOW_UNIT.get(unit, f"모름({r(nu)!r})")
        if fs is None or unit is None:
            c.append(f"[미확인] MFC ch{ch} FS·단위 — R{nf} {r(nf)!r} · R{nu} {r(nu)!r}")
        elif abs(fs - 100.0) < 1e-9 and unit == 1:
            c.append(f"[확인] MFC ch{ch} FS {fs:g} {name}")
        else:
            c.append(f"!!! MFC ch{ch} FS {fs:g} {name} — 100 SCCM 이 아님(레시피 유량 단위 확인)")
    st = r(37)
    if st is None or not re.fullmatch(r"M\d{3}", st.strip()):
        c.append(f"[미확인] MFC Remote — R37 {st!r}")
    elif st.strip()[1] == "1":
        c.append("[확인] MFC Remote(R37 X=1) — 원격 제어 가능 [p.84, p.54]")
    else:
        c.append("!!! MFC Local(R37 X=0) — 원격 제어가 되지 않는다 [p.84, p.54]")


# ───────────── IG(934) ─────────────
IG_ITEMS = {1: "디개스", 2: "필라멘트", 3: "방출 정상", 4: "릴레이 SP1/게이지 A", 5: "필라멘트 오류 코드", 6: "(사용 안 함)",
            7: "릴레이 SP2/게이지 B", 8: "릴레이 SP3/이온", 9: "릴레이 SP4/이온", 10: "필라멘트 상태(자동)", 11: "(사용 안 함)",
            12: "(사용 안 함)", 13: "(사용 안 함)", 14: "(사용 안 함)", 15: "셋포인트 보호 해제(비밀 스위치)",
            16: "자동 필라멘트", 17: "자동 필라멘트 셋포인트", 18: "이온 셋포인트 1", 19: "이온 셋포인트 2", 20: "가스 계수",
            21: "감도", 22: "게이지 A 셋포인트", 23: "게이지 B 셋포인트", 24: "이온 압력", 25: "게이지 A 압력", 26: "게이지 B 압력"}
IG_SETPOINT_ITEMS = (17, 18, 19, 22, 23)       # xy z = xy × 10^(z−2) Torr [IG p.53]
IG_PRESSURE_ITEMS = (24, 25, 26)               # xy z = xy × 10^z Torr


def ig_pair(raw: str, setpoint: bool) -> Optional[float]:
    m = re.fullmatch(r"\s*(-?\d+)\s+(-?\d+)\s*", raw or "")
    if not m:
        return None
    return int(m.group(1)) * 10.0 ** (int(m.group(2)) - (2 if setpoint else 0))


def ig_meaning(n: int, raw: str) -> str:
    if n in IG_SETPOINT_ITEMS:
        v = ig_pair(raw, True)
        return "형식 다름" if v is None else f"{v:.1E} Torr"
    if n in IG_PRESSURE_ITEMS:
        if raw.strip() == "0":
            return "꺼짐(0)"
        if raw.strip() in ("-900", "-999"):
            return {"-900": "게이지 없음", "-999": "영점 안 됨"}[raw.strip()]
        v = ig_pair(raw, False)
        return "형식 다름" if v is None else f"{v:.1E} Torr"
    if n == 20:
        return f"{int(raw) / 100:.2f}" if raw.strip().isdigit() else "형식 다름"
    if n == 21:
        return f"{int(raw) / 10:.1f}" if raw.strip().isdigit() else "형식 다름"
    return raw.strip()


def ig_interpret(inv: Inventory) -> None:
    c = inv.checks
    g, s = inv.raw("ig", "E20"), inv.raw("ig", "E21")
    if g is None or s is None:
        c.append("[미확인] IG 가스 계수·감도 — E 를 못 읽음")
        return
    gas, sens = ig_meaning(20, g), ig_meaning(21, s)
    ok = gas == "1.00" and sens == "10.0"
    c.append(("[확인] " if ok else "!!! ") + f"IG 가스 계수 {gas} · 감도 {sens}"
             + ("" if ok else " — 1.00·10.0 이 아니면 9.9E-3 Torr 위에서 필라멘트가 꺼진다(코드 12) [IG p.41, p.44]"))
    c.append(f"[확인] IG 자동 필라멘트 {inv.raw('ig', 'E16')} · 셋포인트 {ig_meaning(17, inv.raw('ig', 'E17') or '')}"
             f" · 이온 SP1 {ig_meaning(18, inv.raw('ig', 'E18') or '')} · SP2 {ig_meaning(19, inv.raw('ig', 'E19') or '')}")


def _f(s: Optional[str]) -> Optional[float]:
    try:
        return float(s) if s is not None else None
    except ValueError:
        return None


UNVERIFIABLE = (
    "DC Response 설정 — 앞면 I/O 6.Response(공장 기본 No [DC p.49]). 명령으로 읽을 수 없다. v1 에서 설정 명령이 무응답이었으니 No 로 보임",
    "DC 아날로그 옵션 — 뒷면 단자(Analog Remote Interface, 옵션) — 눈으로",
    "IG DIP S2-3 — 전원을 켤 때 S2 값이 표시된다 [IG p.36]",
    "MFC·DC 쓰기 뒤 최소 간격 — 쓰기 없이는 잴 수 없다(bench 는 쓰지 않음)",
    "PLC STOP 때 DAC 출력 — PLC 를 STOP 해 봐야 안다(PD9 보류)",
)
