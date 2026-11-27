"""챔버K 파라미터 정의 표.

출처
- v1 config_user.json 118개 항목(주석 제외, main 12ecc2d) — default 는 이 운영값이다.
- v1 lib/config.py 기본값 — json 과 다르면 v1_default 에 참고로 남긴다.
- v1 에서 json 으로 못 바꾸던 상수(코드에 박힌 안전 한도·시간) — v1_source 에 파일:줄.
- v2 에서 새로 생긴 값(장비 ID·실행 모드·1초 수집·웹 연동·기록) — v1_source="v2".

규칙(시험이 지킨다: v2\\tests\\test_params.py)
- H 등급은 admin 전용이고 공정 중 변경 금지(apply 가 live 가 아님).
- install/secret 은 재시작 필요(apply=restart).
- 모든 default 는 자기 범위와 키 사이 규칙(CROSS_RULES)을 통과한다.
- v1 json 키는 전부 PARAMS(key 또는 v1_key) 나 DROPPED 에 있다.
"""
from __future__ import annotations

from dataclasses import replace as _dc_replace
from typing import Any, Mapping, Optional

from v2.common.paramdef import (
    Apply, CrossRule, Dropped, ParamDef, PType, Role, Safety, Scope,
)
from v2.chk import devices as dev

F, I, B, S, E = PType.FLOAT, PType.INT, PType.BOOL, PType.STR, PType.ENUM
H, M, L = Safety.H, Safety.M, Safety.L
LIVE, IDLE, RESTART = Apply.LIVE, Apply.IDLE, Apply.RESTART

_JSON = "config_user.json"
_V2 = "v2"
_COM = r"COM[1-9][0-9]{0,2}"


def _p(key, group, label, unit, type_, default, lo=None, hi=None, *, safety=M, apply=IDLE,
       scope=Scope.PROCESS, role=None, desc="", enum=None, pattern=None, v1_key=None,
       v1_default=None, src=_JSON, pending=False, note="", v1_period=None, locked=False, choices=None,
       enum_labels=None) -> ParamDef:
    if choices is not None:                     # 허용 숫자 목록 — 범위(min·max)는 목록의 양 끝(목록을 모르는 화면도 범위는 맞게)
        lo, hi = min(choices), max(choices)
    if role is None:
        role = Role.ADMIN if (safety is H or scope is not Scope.PROCESS) else Role.OPERATOR
    return ParamDef(key=key, group=group, label=label, desc=desc or label, unit=unit, type=type_,
                    default=default, min=lo, max=hi, enum=enum, pattern=pattern, safety=safety,
                    apply=apply, scope=scope, role=role, v1_key=v1_key, v1_default=v1_default,
                    v1_source=src, pending_confirm=pending, note=note, v1_period_s=v1_period, web_locked=locked,
                    choices=tuple(choices) if choices is not None else None,
                    enum_labels=tuple(enum_labels) if enum_labels is not None else None)


def _inst(key, label, type_, default, *, desc="", pattern=None, lo=None, hi=None, enum=None,
          safety=M, v1_key=None, src=_JSON, unit="", v1_default=None, locked=False, choices=None,
          enum_labels=None, note="") -> ParamDef:
    return _p(key, "install", label, unit, type_, default, lo, hi, safety=safety, apply=RESTART,
              scope=Scope.INSTALL, desc=desc, pattern=pattern, enum=enum, v1_key=v1_key, src=src,
              v1_default=v1_default, locked=locked, choices=choices, enum_labels=enum_labels, note=note)


# 읽기 계획 판정 기준(5-20 ① — drivers/readplan.py). auto 일 때만 쓰고, 모두 PC 쪽 설정이다(장비 쪽에 맞출 값 없음).
def _read_plan_params(dev_key: str, name: str, what: str, slow_n: int, reads: str) -> tuple:
    k = f"{dev_key}_READ_PLAN"
    src = "v2 (5-20 ① — 11/23 bench: 한 번 느렸던 읽기로 끝까지 번갈아 읽음)"
    return (
        _inst(f"{k}_SLOW_N", f"{name} 번갈아로 바꾸는 느린 읽기 수", I, slow_n, lo=2, hi=60, unit="회", src=src,
              desc=f"매초 읽기 중 최근 읽기('{name} 느린 읽기를 세는 최근 읽기 수') 가운데 느린 읽기가 이 횟수 이상이면 "
                   "번갈아 읽기로 바꿉니다. 한 번 느렸다고 바꾸지 않도록 하한을 2 로 두었습니다.",
              note=f"세는 읽기 수 = {k}_SLOW_M."),
        _inst(f"{k}_SLOW_M", f"{name} 느린 읽기를 세는 최근 읽기 수", I, 10, lo=2, hi=120, unit="회", src=src,
              desc="느린 읽기를 셀 때 보는 최근 읽기 수이며, 1초에 한 번 읽으므로 대략 초와 같습니다. "
                   f"'{name} 번갈아로 바꾸는 느린 읽기 수' 이상이어야 합니다.",
              note=f"{k}_SLOW_N 이상."),
        _inst(f"{k}_SLOW_PCT", f"{name} 느린 읽기 기준", I, 60, lo=20, hi=100, unit="%", src=src,
              desc=f"{what}를 모두 읽은 한 번이 '{name} 읽기 마감'의 이 비율을 넘으면 느린 읽기로 봅니다. "
                   "기본값은 처음 쓰던 기준(마감의 60 %)과 같습니다.",
              note=f"셋 = {reads}. 기본은 v2 처음 기준(마감의 60 %)과 같다. 마감 = {dev_key}_READ_DEADLINE_MS."),
        _inst(f"{k}_FAST_PCT", f"{name} 매초 읽기로 돌아오는 기준", I, 45, lo=10, hi=95, unit="%", src=src,
              desc="번갈아 읽는 동안 셋을 다 읽었다면 걸렸을 시간(이번 읽기 + 명령 간격 + 가장 긴 응답)이 "
                   f"'{name} 읽기 마감'의 이 비율 안이면 빠른 읽기로 봅니다. 기준 근처에서 계획이 오가지 않도록 "
                   f"'{name} 느린 읽기 기준'보다 작아야 합니다.",
              note=f"{k}_SLOW_PCT 보다 작아야 한다."),
        _inst(f"{k}_RETURN_SEC", f"{name} 매초 읽기로 돌아오기까지", I, 60, lo=10, hi=3600, unit="s", src=src,
              desc="번갈아 읽는 동안 이 시간 내내 빠른 읽기만 이어지면 매초 읽기로 돌아옵니다. 느린 읽기가 하나라도 있으면 "
                   "처음부터 다시 셉니다."),
        _inst(f"{k}_HOLD_SEC", f"{name} 읽기 계획을 다시 바꾸기까지", I, 60, lo=60, hi=3600, unit="s", src=src,
              desc="읽기 계획을 바꾼 뒤 이 시간 안에는 다시 바꾸지 않습니다. 하한이 60 s 라서 1분에 한 번보다 자주 바뀌지 "
                   "않습니다."),
    )


# 통신 설정 설명(W-23 D): 이 값은 PC 쪽 설정일 뿐이고 장비 쪽은 사람이 따로 맞춘다 — 장비에서 바꾸는 곳은 각 desc 에 말로 적는다.
# SPn 값 출처 설명에 쓰는 압력 단계 화면 이름(SPn_MTORR 의 label 과 같아야 한다 — 시험이 본다)
_SP_LABEL = {2: "압력 2단계(SP2)", 3: "압력 1단계(SP3)", 4: "가스 안정화 SP(SP4)", 5: "압력 0단계(SP5)"}


PARAMS: tuple = (
    # ───────────── 설치값(install) — 웹 admin + 노트북 비상 덮어쓰기, 재시작 필요 ─────────────
    _inst("EQUIPMENT_ID", "장비 ID", S, "CHK2", pattern=r"[A-Z0-9][A-Z0-9-]{1,15}", safety=H, src=_V2, locked=True,
          desc="웹에서 이 장비를 구분하는 ID 입니다. 웹에서는 바꾸지 않고 노트북 설정 파일에서만 바꿉니다.",
          note="개발 중에는 v1(CHK)과 섞이지 않게 CHK2 를 쓴다."),
    _inst("RUN_MODE", "실행 모드", E, "sim", enum=("sim", "bench", "prod"), safety=H, src=_V2, locked=True,
          desc="프로그램을 어떻게 돌릴지 정하며, sim 은 가짜 장비로 외부 연결 없이, bench 는 실장비를 읽기만, prod 는 실제 "
               "운전입니다. 노트북 설정 파일이 없으면 sim 으로 돌고, prod 는 설정 파일에 명시했을 때만 씁니다."),
    _inst("PLC_PORT", "PLC 포트", S, "COM9", pattern=_COM, safety=H,
          desc="PLC 가 연결된 PC 의 COM 포트입니다. 번호는 장치 관리자에서 확인합니다."),
    _inst("PLC_BAUD", "PLC 통신 속도", I, 115200, choices=dev.PLC_BAUDS, unit="bps",
          src="lib/config.py:24 (고정값) · XGB 내장 Cnet 매뉴얼(사용자 확인 2026-11-13)",
          desc="PLC(XGB 내장 Cnet, Modbus RTU 서버)가 받는 통신 속도 가운데에서 고르며, 지금 장비는 115200 "
               "bps, 데이터 8·패리티 없음·정지 1 입니다. PC 쪽 설정이므로 장비 쪽도 XG5000 기본 파라미터 Cnet 에서 같은 "
               "값으로 맞춰야 합니다.",
          note="지금 장비 115200 8N1."),
    _inst("PLC_PARITY", "PLC 패리티", E, "none", enum=dev.PLC_PARITIES,
          src="v2 (5-17 — XGB 내장 Cnet 매뉴얼, 사용자 확인 2026-11-13. v1 은 8N1 고정)",
          desc="PLC 통신의 패리티이며 지금 장비는 none 이고, 데이터 비트는 Modbus RTU 라서 8 로 고정입니다. PC 쪽 "
               "설정이므로 장비 쪽도 XG5000 기본 파라미터 Cnet 에서 같은 값으로 맞춰야 합니다."),
    _inst("PLC_STOPBITS", "PLC 정지 비트", I, 1, choices=dev.PLC_STOPBITS, unit="bit",
          src="v2 (5-17 — XGB 내장 Cnet 매뉴얼, 사용자 확인 2026-11-13. v1 은 8N1 고정)",
          desc="PLC 통신의 정지 비트이며 지금 장비는 1 입니다. PC 쪽 설정이므로 장비 쪽도 XG5000 기본 파라미터 Cnet 에서 "
               "같은 값으로 맞춰야 합니다."),
    _inst("PLC_SLAVE_ID", "PLC Modbus 국번", I, 1, lo=1, hi=247, src="lib/config.py:25 (고정값)",
          desc="PLC 의 Modbus 국번이며 지금 장비는 1 입니다. PC 쪽 설정이므로 장비 쪽도 XG5000 기본 파라미터 Cnet "
               "에서 같은 값으로 맞춰야 합니다."),
    _inst("MFC_PORT", "MFC 포트", S, "COM10", pattern=_COM, desc="MFC 컨트롤러가 연결된 PC 의 COM 포트입니다."),
    _inst("MFC_BAUD", "MFC 통신 속도", I, 9600, choices=dev.MFC_BAUDS, unit="bps", safety=L,
          src="config_user.json · [MFC p.72, p.54] (9600 기본, 9600~115200 선택 — 장비 사실 표 근거 그대로)",
          desc="MFC(GPC3000)가 받는 통신 속도 가운데에서 고르며, 데이터 8·패리티 없음·정지 1 은 장비에 고정돼 있습니다. PC "
               "쪽 설정이므로 장비 쪽도 장비 메뉴에서 같은 값으로 맞춰야 합니다.",
          note="데이터·패리티·정지 비트는 장비 고정(8N1)."),
    _inst("DC_PORT", "DC Power 포트", S, "COM11", pattern=_COM, desc="DC Power 가 연결된 PC 의 COM 포트입니다."),
    _inst("DC_BAUD", "DC Power 통신 속도", I, 9600, choices=dev.DC_BAUDS, unit="bps", safety=L,
          v1_key="DC_BAUDRATE", src="config_user.json · [DC p.38-40] (사용자 확인 2026-11-13)",
          desc="DC Power(ODA EX)가 받는 통신 속도 가운데에서 고르며, 데이터 8·정지 1·패리티 없음은 장비에 고정돼 있습니다. "
               "PC 쪽 설정이므로 장비 쪽도 앞면 I/O 메뉴에서 같은 값으로 맞춰야 합니다.",
          note="데이터·정지 비트·패리티는 장비 고정(8N1)."),
    _inst("DC_DELIMITER", "DC Power 종결 문자", E, "LF", enum=dev.DC_DELIMITERS,
          src="v2 (5-17 — [DC p.49], 출고값 LF. v1 은 LF 고정 송신)",
          desc="DC Power 명령 끝에 붙이고 응답 끝으로 읽는 문자(LF·CR·CRLF)이며, 장비와 다르면 장비가 명령이 끝난 것으로 "
               "보지 않아 응답이 없습니다. PC 쪽 설정이므로 장비 쪽도 앞면 I/O 메뉴에서 같은 값으로 맞춰야 합니다."),
    _inst("RFPULSE_ENABLED", "RF Pulse 사용", B, False, v1_default=True, src=_V2,
          desc="RF Pulse 를 쓸지 정합니다. 꺼 두면 포트를 열지 않고 RF Pulse 값은 꺼짐 상태로 두며, RF Pulse 를 "
               "쓰는 레시피는 시작을 거부합니다.",
          note="꺼짐이면 rfp 블록 품질을 off 로 둔다. 시작 거부 사유 코드 DEVICE_DISABLED. 지금 연결하지 않음(사용자 "
               "2026-10-16)."),
    _inst("RFPULSE_PORT", "RF Pulse 포트", S, "COM13", pattern=_COM, desc="RF Pulse(CESAR)가 연결된 PC 의 COM 포트입니다."),
    _inst("RFPULSE_BAUD", "RF Pulse 통신 속도", I, 9600, choices=dev.CESAR_BAUDS, unit="bps", safety=L,
          src="config_user.json · [CESAR 4-32~4-37] (사용자 확인 2026-11-13)",
          desc="RF Pulse(CESAR)가 받는 통신 속도 가운데에서 고르며, 데이터 8·정지 1 은 장비에 고정돼 있습니다. PC 쪽 "
               "설정이므로 장비 쪽도 앞면에서 같은 값으로 맞춰야 합니다."),
    _inst("RFPULSE_ADDR", "RF Pulse AE Bus 주소", I, 1, lo=0, hi=31,
          desc="RF Pulse 의 AE Bus 주소(0~31)이며, 매뉴얼에 주소가 늘 1 이라고 나와 있어 기본 1 을 그대로 씁니다. "
               "PC 쪽 설정이므로 장비 쪽도 앞면에서 같은 값으로 맞춰야 합니다.",
          note="v1 은 주소가 32 이상이면 응답 비교가 늘 맞지 않았다. 매뉴얼 판 57023916-00B 에 'unit address "
               "is always 1'."),
    _inst("RFPULSE_PARITY", "RF Pulse 패리티", E, "odd", enum=("none", "odd", "even"),
          desc="RF Pulse(CESAR) RS-232 통신의 패리티이며, 규격과 장비가 홀수 패리티로 고정돼 있어 odd 로 둡니다. "
               "다르면 장비가 거부 응답만 돌려주며, PC 쪽 설정이므로 장비 쪽도 앞면에서 같은 값으로 맞춰야 합니다.",
          note="CESAR RS-232 규격은 8O1. 패리티가 다르면 NAK 만 돌아온다."),
    _inst("IG_ENABLED", "Ion Gauge 사용", B, True, v1_default=False,
          desc="Ion Gauge 를 쓸지 정합니다. 꺼 두면 레시피에 '베이스 압력' 값이 있는 공정은 시작을 거부합니다.",
          note="레시피 열 base_pressure. v1 은 경고만 하고 베이스 대기를 건너뛰었다."),
    _inst("IG_PORT", "Ion Gauge 포트", S, "COM12", pattern=r"(" + _COM + r")?", v1_default="",
          desc="Ion Gauge 가 연결된 PC 의 COM 포트(MOXA UPort 경유)입니다."),
    _inst("IG_BAUD", "Ion Gauge 통신 속도", I, 9600, choices=dev.IG_BAUDS, unit="bps", safety=L,
          src="lib/config.py:735 · [IG p.15, p.18] (DIP S1-1·S1-2, 출고 9600 — 사용자 확인 2026-11-13)",
          desc="Ion Gauge(Terranova 934)가 받는 통신 속도 가운데에서 고르며, 데이터 8·패리티 없음·정지 1 은 장비에 "
               "고정돼 있습니다. PC 쪽 설정이므로 장비 쪽도 내부 DIP 스위치에서 같은 값으로 맞춰야 합니다.",
          note="장비 고정 8N1. 장비에서 바꾸는 곳은 내부 DIP S1."),
    _inst("HEATER_ENABLED", "히터 기능 사용", B, True, safety=H,
          desc="히터 기능을 쓸지 정합니다. 꺼 두면 히터 명령과 레시피의 히터 행을 거부하며, 히터 워치독 하트비트는 이 설정과 관계없이 늘 "
               "보냅니다."),
    _inst("PLC_FAIL_STREAK", "PLC 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="v2 (3단계 worker 기본값)",
          desc="PLC 에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 간격을 점점 늘려 가며 다시 연결합니다.",
          note="재연결은 백오프."),
    _inst("MFC_FAIL_STREAK", "MFC 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="v2 (v1 은 무응답 1회에 닫음)",
        desc="MFC 읽기·명령에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 간격을 점점 늘려 가며 다시 연결합니다. 작을수록 "
             "잠깐의 무응답에도 다시 연결하고, 클수록 끊김을 늦게 알아챕니다.",
        note="재연결은 백오프."),
    _inst("DC_FAIL_STREAK", "DC 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="device/DCpower.py:28",
        desc="DC 질의에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 간격을 점점 늘려 가며 다시 연결합니다. 작을수록 잠깐의 "
             "무응답에도 다시 연결하고, 클수록 끊김을 늦게 알아챕니다.",
        note="재연결은 백오프."),
    _inst("RFPULSE_FAIL_STREAK", "RF Pulse 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="v2 (3단계 worker 기본값)",
        desc="RF Pulse 질의·명령에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 간격을 점점 늘려 가며 다시 연결합니다. "
             "작을수록 잠깐의 무응답에도 다시 연결하고, 클수록 끊김을 늦게 알아챕니다.",
        note="재연결은 백오프."),
    _inst("IG_FAIL_STREAK", "Ion Gauge 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="device/IonGauge.py (3회)",
        desc="Ion Gauge 질의에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 간격을 점점 늘려 가며 다시 연결합니다. "
             "Filament 켜기·끄기 직후 'Filament 전환 뒤 무응답 유예' 동안의 무응답은 세지 않습니다.",
        note="재연결은 백오프. 유예 = IG_FILAMENT_SWITCH_QUIET_SEC."),
    _inst("PLC_TXN_TIMEOUT_MS", "PLC 트랜잭션 응답 제한", I, 150, lo=30, hi=1000, unit="ms",
          src="v2 (4-1 drivers/plc.py TXN_TIMEOUT_S, v1 0.5 s)",
          desc="PLC 와 Modbus 트랜잭션 하나를 주고받을 때 응답을 기다리는 한도입니다. 실장비에서 잰 지연(회선·PLC 스캔·USB "
               "지연)에 맞춰 정합니다.",
          note="bench 실측(회선 + 스캔 + USB 지연)으로 맞춘다."),
    _inst("PLC_WRITE_DEADLINE_MS", "PLC 쓰기 요청 한도", I, 2000, lo=500, hi=10000, unit="ms",
          src="v2 (4-1 drivers/plc.py submit_write 의 고정값 2.0 s)",
          desc="PLC 쓰기 요청 하나의 한도로, 대기열에서 기다리는 시간·트랜잭션·재시도를 모두 합친 시간입니다. 이 시간을 넘으면 그 "
               "쓰기는 실패로 끝냅니다."),
    _inst("DC_WRITE_QUERY_GAP_MS", "DC 쓰기 뒤 질의 금지 시간", I, 500, lo=100, hi=3000, unit="ms",
          src="device/DCpower.py:36 (2026-09-28 실측)",
          desc="DC Power 에 쓰기 명령을 보낸 뒤 질의를 보내지 않고 기다리는 시간입니다. ODA EX 는 직전 명령을 처리하는 동안 "
               "들어온 질의에 답하지 않기 때문입니다."),
    _inst("DC_START_GAP_MS", "DC 출력 켜기 명령 간격", I, 120, lo=120, hi=2000, unit="ms",
          src="device/DCpower.py:829-833 (_send 의 msleep(120))",
          desc="DC 출력을 켜는 순서에서 명령과 명령 사이에 두는 간격입니다. 실측값인 120 ms 보다 짧게 둘 수 없습니다.",
          note="출력 켜기 순서: *RST → *CLS → APPLy → OUTP ON. 하한 120 ms 는 v1 실측값."),
    _inst("DC_STEP_TIMEOUT_MS", "DC 켜기·끄기 응답 제한", I, 1500, lo=1500, hi=10000, unit="ms",
          src="device/DCpower.py (OUTP? 1.5 s)", safety=H,
          desc="DC 출력 켜기와 OFF 확인 순서에서 단계 하나(출력 상태 질의 포함)를 기다리는 한도입니다. OFF 확인을 일찍 포기하지 "
               "않도록 1500 ms 보다 짧게 둘 수 없습니다.",
          note="출력 상태 질의 = OUTP?."),
    _inst("DC_CURR_WRITE_DELAY_MS", "DC 측정 뒤 전류 쓰기까지", I, 100, lo=100, hi=400, unit="ms",
          src="v2 (사용자 결정 2026-10-15, drivers/dc.py CURR_WRITE_DELAY_S)",
          desc="매초 측정 질의를 보낸 뒤 전류 쓰기(와 출력 켜기 시작)까지 기다리는 시간입니다. 이 값과 'DC 쓰기 뒤 질의 금지 "
               "시간'의 합이 1000 ms 를 넘으면 다음 초의 측정이 질의 금지 시간에 걸립니다.",
          note="측정 질의 = MEAS:ALL?, 전류 쓰기 = CURR. 교차 규칙 X42: DC_CURR_WRITE_DELAY_MS + "
               "DC_WRITE_QUERY_GAP_MS ≤ 1000 ms."),
    _inst("MFC_CMD_GAP_MS", "MFC 명령 간격", I, 150, lo=20, hi=2000, unit="ms",
          src="v2 (사용자 결정 2026-10-15, v1 1000 ms)", desc="MFC 에 보내는 명령(질의·설정) 사이의 최소 간격입니다. 실장비에서 잰 값에 맞춰 정합니다.",
                                                              note="bench 실측으로 맞춘다."),
    _inst("MFC_QUERY_TIMEOUT_MS", "MFC 질의 응답 제한", I, 300, lo=50, hi=2000, unit="ms",
          src="v2 (사용자 결정 2026-10-15, v1 1000 ms)", desc="MFC 질의 하나의 응답을 기다리는 한도입니다. 실장비에서 잰 응답 시간에 맞춰 정합니다.",
                                                              note="bench 실측으로 맞춘다."),
    _inst("MFC_QUERY_RETRIES", "MFC 무응답 질의 다시 보내기 횟수", I, 5, lo=0, hi=10, unit="회",
          src="device/MFC.py enqueue(retries_left=5)",
          desc="설정 명령을 되읽는 질의 하나에 응답이 없으면 이 횟수만큼 다시 보내고, 다 쓰면 '응답 없음'으로 판정합니다. 장비 쪽에 "
               "맞출 값이 없는 PC 쪽 설정입니다."),
    _inst("MFC_VERIFY_RETRY_MS", "MFC 되읽기 다시 하기 전 대기", I, 1000, lo=200, hi=5000, unit="ms",
          src="lib/config.py MFC_DELAY_MS(1000)",
          desc="설정 명령(유량·SP·SP 선택·밸브·ON/OFF)을 보낸 뒤 되읽은 값이 다르면 이 시간만큼 기다렸다가 다시 확인합니다. "
               "'MFC 명령 간격'(기본 150 ms)보다 길게 두려고 하한을 200 ms 로 두었으며, 장비 쪽에 맞출 값이 없는 PC 쪽 "
               "설정입니다.",
          note="하한 비교 대상 MFC_CMD_GAP_MS."),
    _inst("MFC_VERIFY_TRIES", "MFC 되읽기 확인 횟수", I, 5, lo=1, hi=10, unit="회",
          src="device/MFC.py max_attempts=5 (FLOW_SET·SPn_SET·SPn_ON·밸브)",
          desc="설정 명령을 보낸 뒤 되읽어 확인하는 최대 횟수로, 이 안에 되읽은 값이 맞지 않으면 그 명령은 실패합니다. 횟수 × 'MFC "
               "되읽기 다시 하기 전 대기'가 공정 단계 한도(60 s)를 넘지 않도록 상한을 10 으로 두었으며, 장비 쪽에 맞출 값이 없는 "
               "PC 쪽 설정입니다.",
          note="실패 사유 코드 VERIFY_MISMATCH. 대기 = MFC_VERIFY_RETRY_MS. 공정 스텝 한도 60 s."),
    _inst("MFC_SP_REFRESH_SEC", "MFC 장비 SP 값 다시 읽기 주기", I, 60, lo=0, hi=3600, unit="s", src="v2 (5-19 ③ — v1 없음)",
          desc="공정이 돌지 않는 동안 장비에 저장된 SP 값을 이 주기로 다시 읽으며, 한 초에 하나씩 읽고 설정 명령과 매초 읽기에 "
               "양보합니다. 0 이면 다시 읽지 않고 연결 직후와 SP 를 고를 때만 읽으며, 장비 쪽에 맞출 값이 없는 PC 쪽 설정입니다.",
          note="장비 SP 값 읽기 = R1~R4·R10. 설정 명령·읽기 마감에 양보."),
    _inst("MFC_LINK_RETRY_MS", "MFC 재연결 대기 중 다시 내는 간격", I, 1000, lo=200, hi=5000, unit="ms",
          src="v2 (4-2 drivers/mfc.py LINK_RETRY_S, v1 은 큐에서 기다림)",
          desc="MFC 포트가 끊긴 동안 공정 단계 한도 안에서 설정 명령을 이 간격으로 다시 냅니다. 장비 쪽에 맞출 값이 없는 PC 쪽 "
               "설정입니다."),
    _inst("MFC_AR_FS_SCCM", "MFC Ar 채널 풀스케일(못 읽을 때)", F, 100.0, lo=1.0, hi=100000.0, unit="sccm",
          src="v2 (사용자 결정 2026-10-15)", desc="MFC Ar 채널의 풀스케일로, 연결할 때 장비에서 읽은 풀스케일을 먼저 쓰고 읽지 못하면 이 값을 씁니다. 100 이면 "
                                                       "%FS 와 sccm 값이 같습니다.",
                                                  note="연결 때 R70 으로 FS 를 읽는다."),
    _inst("MFC_O2_FS_SCCM", "MFC O2 채널 풀스케일(못 읽을 때)", F, 100.0, lo=1.0, hi=100000.0, unit="sccm",
          src="v2 (사용자 결정 2026-10-15)", desc="MFC O2 채널의 풀스케일로, 연결할 때 장비에서 읽은 풀스케일을 먼저 쓰고 읽지 못하면 이 값을 씁니다.",
                                                  note="연결 때 R71 로 읽은 FS 를 우선한다."),
    _inst("MFC_PRESSURE_FS_TORR", "MFC 압력 센서 풀스케일", F, 1.0, lo=0.001, hi=1000.0, unit="Torr",
          src="lib/config.py:806 (MFC_PRESSURE_SCALE 0.1 = FS 1 Torr 가정)",
          desc="MFC 압력 값(%FS)을 mTorr 로 바꿀 때 쓰는 압력 센서의 풀스케일입니다. 실장비에서 장비의 레인지 코드로 확인해 "
               "맞춥니다.",
          note="bench 에서 레인지 코드(E)로 확인. 압력 mTorr = %FS/100 × MFC_PRESSURE_FS_TORR × "
               "1000."),
    _inst("MFC_READ_PLAN", "MFC 읽기 계획", E, "auto", enum=("auto", "full", "alternate"), src="v2 (사용자 결정 2026-10-15)",
          desc="MFC 의 압력·유량·밸브 위치를 매초 어떻게 읽을지 정합니다. full 은 셋을 매초 모두 읽고, alternate 는 "
               "압력은 매초·유량은 짝수 초·밸브 위치는 홀수 초에 읽으며, auto 는 느린 읽기가 이어질 때만 alternate 로 바꾸고 "
               "다시 빨라지면 full 로 돌아옵니다.",
          note="full = 매초 R5(압력)·R60(유량)·R6(밸브 위치), alternate = R5 매초·R60 짝수 초·R6 홀수 "
               "초. auto 판정에서 R6 무응답으로 길어진 읽기는 세지 않고, 읽기 실패로도 바꾸지 않는다. R6 는 기록용이라 설정 "
               "명령이 진행 중이거나 R6 를 기다리면 마감을 넘길 초에는 보내지 않고 양보한다(그 초의 밸브 값만 stale). 지금 계획은 "
               "스냅샷 mfc.read_plan, 판정 기준은 "
               "MFC_READ_PLAN_SLOW_N·SLOW_M·SLOW_PCT·FAST_PCT·RETURN_SEC·HOLD_SEC."),
    *_read_plan_params("MFC", "MFC", "압력·유량·밸브 위치", 5, "R5·R60·R6"),
    _inst("RFPULSE_CMD_GAP_MS", "RF Pulse 명령 간격", I, 50, lo=0, hi=2000, unit="ms",
          src="[CESAR p.174 / 6-4] (v1 1500 ms)", desc="RF Pulse 응답을 받은 뒤 다음 명령을 보내기까지 두는 간격입니다. 매뉴얼은 핸드셰이크를 쓰거나 50 ms 를 두도록 "
                                                       "합니다."),
    _inst("RFPULSE_QUERY_TIMEOUT_MS", "RF Pulse 질의 응답 제한", I, 250, lo=50, hi=3000, unit="ms",
          src="v2 (사용자 결정 2026-10-15, v1 4500~9000 ms)", desc="RF Pulse 질의 하나(상태·Forward·Reflected 각각)의 응답을 기다리는 한도입니다.",
                                                                   note="질의 = 162·165·166 각각."),
    _inst("RFPULSE_ACK_TIMEOUT_MS", "RF Pulse 설정 명령 응답 제한", I, 2000, lo=500, hi=10000, unit="ms",
          src="lib/config.py:649 (RFPULSE_ACK_TIMEOUT_MS 2000, 고정값)",
          desc="RF Pulse 설정 명령(모드·SP·주파수·듀티·Host 전환)에 대한 장비의 확인 응답을 기다리는 시간입니다.",
          note="확인 응답 = CSR. SP = 셋포인트."),
    _inst("RFPULSE_ONOFF_TIMEOUT_MS", "RF Pulse RF ON·OFF 응답 제한", I, 2500, lo=2500, hi=10000, unit="ms", safety=H,
          src="device/RFpulse.py:927-956 (max(RFPULSE_ACK_TIMEOUT_MS, 2500))",
          desc="RF ON·RF OFF 명령에 대한 장비의 확인 응답을 기다리는 시간입니다. RF OFF 를 일찍 포기하지 않도록 2500 "
               "ms 보다 짧게 둘 수 없습니다.",
          note="확인 응답 = CSR."),
    _inst("RFPULSE_ON_SETTLE_MS", "RF Pulse RF ON 뒤 되읽기까지", I, 800, lo=0, hi=5000, unit="ms",
          src="lib/config.py:657 (RFPULSE_POLL_START_DELAY_AFTER_RF_ON_MS 800, 고정값)",
          desc="RF ON 뒤 펄스 설정(펄스 주파수·듀티)을 되읽기 전에 기다리는 시간입니다.",
          note="되읽기 질의 193·196."),
    _inst("RFPULSE_CMD_RETRIES", "RF Pulse 시작 명령 재시도 횟수", I, 3, lo=1, hi=10, unit="회",
          src="device/RFpulse.py (_make_cmd(retries=3))",
          desc="RF Pulse 시작 순서에서 설정 명령 하나가 응답이 없거나 거부되거나 오류 상태로 돌아올 때 다시 보내는 횟수입니다.",
          note="다시 보내는 경우: 무응답·NAK·CSR ≠ 0."),
    _inst("RFPULSE_READ_PLAN", "RF Pulse 읽기 계획", E, "auto", enum=("auto", "full", "alternate"),
          src="v2 (사용자 결정 2026-10-15)",
          desc="RF Pulse 의 상태·Forward·Reflected 를 매초 어떻게 읽을지 정합니다. full 은 셋을 매초 모두 읽고, "
               "alternate 는 Reflected 는 매초·Forward 와 상태는 번갈아 읽으며, auto 는 느린 읽기가 이어질 때만 "
               "alternate 로 바꾸고 다시 빨라지면 full 로 돌아옵니다.",
          note="full = 매초 162(상태)·165(Forward)·166(Reflected), alternate = 166 매초·165 "
               "와 162 번갈아. 느린 읽기는 질의 하나가 응답 제한(RFPULSE_QUERY_TIMEOUT_MS)을 다 썼거나 한 번 "
               "읽기가 마감의 기준 비율(RFPULSE_READ_PLAN_SLOW_PCT)을 넘은 읽기이다. 지금 계획은 스냅샷 "
               "rfp.read_plan."),
    *_read_plan_params("RFPULSE", "RF Pulse", "상태·Forward·Reflected ", 3, "162·165·166"),
    _inst("IG_CHAR_GAP_MS", "Ion Gauge 글자 사이 간격", I, 50, lo=dev.IG_CHAR_GAP_MIN_MS, hi=1000, unit="ms",
          src="v2 (5-17 — [IG p.52-53] 글자 사이 50 ms 이상, 초당 약 2글자보다 빠르게 보내지 않는다)",
          desc="Ion Gauge 에 명령 글자를 하나 보낸 뒤 다음 글자를 보내기까지의 최소 간격입니다. 평소에는 상태 질의를 1초에 한 "
               "번만 보내므로 이 간격에 걸리지 않습니다.",
          note="명령 글자 = E·F·A·B. 평소에는 E 를 1초에 한 번만 보낸다."),
    _inst("IG_FILAMENT_E_BLOCK_MS", "Filament 명령 뒤 E 금지 시간", I, 1000, lo=500, hi=5000, unit="ms",
          src="device/IonGauge.py (e_block_sec 1.0)",
          desc="Filament 켜기·끄기 명령을 보낸 뒤 이 시간 동안은 상태 질의를 보내지 않습니다. 장비가 초당 약 2글자만 받으므로 "
               "500 ms 보다 짧게 둘 수 없습니다.",
          note="A(켜기)·B(끄기) 뒤 상태 질의 E 를 보내지 않는다."),
    _inst("PORT_WRITE_TIMEOUT_SEC", "포트 쓰기 제한 시간", F, 1.0, lo=0.2, hi=10.0, unit="s",
          src="v2 (3단계 transport.SerialSettings.write_timeout 1.0)",
          desc="시리얼 포트에 데이터를 쓰는 호출 하나의 한도로, USB-시리얼 장치가 사라졌을 때 쓰기가 막혀 있지 않게 합니다. 다섯 "
               "장비에 함께 씁니다."),
    _inst("CONNECT_RETRY_PAUSE_MS", "연결 초기화 다시 하기 전 쉼", I, 300, lo=100, hi=5000, unit="ms",
          src="v2 (3단계 worker.CONNECT_RETRY_PAUSE_S 0.3)",
          desc="연결 직후 첫 질의에 응답이 없으면 포트를 닫지 않고 이 시간만큼 쉰 뒤 입력을 비우고 초기화를 한 번 더 합니다."),
    _inst("PORT_OPEN_TIMEOUT_SEC", "포트 열기 제한 시간", F, 2.0, lo=0.5, hi=30.0, unit="s", src="lib/serial_guard.py (v1 2 s)",
          desc="USB-시리얼 장치가 사라졌을 때 포트 열기가 오래 막히지 않도록, 포트 열기를 별도 스레드에서 이 시간만큼만 기다립니다.",
          note="2026-09-16 사고: USB-시리얼이 사라진 뒤 close/open 이 30초씩 막혀 워커가 멈춤."),
    _inst("CONNECT_INIT_TIMEOUT_SEC", "연결 직후 초기화 제한 시간", F, 5.0, lo=1.0, hi=60.0, unit="s", src="v2 (3단계 worker 상수)",
          desc="연결 직후 장비 초기화(설정 쓰기·되읽기 등)에 주는 한도입니다."),
    _inst("LOCAL_HMI_PORT", "노트북 화면 포트", I, 8710, lo=1024, hi=65535, safety=L, src=_V2,
          desc="노트북 화면 서버가 쓰는 포트 번호입니다. 이 노트북 안(127.0.0.1)에서만 열립니다."),
    _inst("LOCAL_HMI_CONTROL", "노트북 조작", E, "unlock", enum=("view_stop", "unlock", "always"),
          enum_labels=(("view_stop", "보기와 정지만"), ("unlock", "잠금을 푼 뒤 조작"), ("always", "항상 조작")),
          src="v2 (5-20 ② — 11/23 bench 뒤 사용자 결정)",
          desc="노트북 화면에서 받을 명령의 범위를 정하며, '보기와 정지만'은 정지와 공정 중 값 바꾸기·건너뛰기·일시정지·재개만 받습니다. "
               "'잠금을 푼 뒤 조작'과 '항상 조작'은 공정 시작·PLC 버튼·Filament·히터·수동 조작 등도 받으며, 둘은 노트북 "
               "화면이 잠그는지만 다릅니다.",
          note="보기와 정지만: 정지(STOP·ALL STOP·부저 정지·히터 정지)와 공정 중 값 바꾸기·건너뛰기·일시정지·재개만(5-19 "
               "까지와 같다). 잠금을 푼 뒤 조작·항상 조작: 그 밖에 공정 시작·적재 레시피 시작·해제·순서 고치기·PLC "
               "버튼·Filament·히터·수동 조작도 받는다. 설정 적용·되돌리기는 웹에서만. 잠금은 노트북 화면이 하고, 장비는 이 값과 "
               "잠금 시간(LOCAL_HMI_RELOCK_MIN)을 hello 로 알린다."),
    _inst("LOCAL_HMI_RELOCK_MIN", "노트북 조작 다시 잠그기까지", I, 10, lo=1, hi=120, unit="분", safety=L,
          src="v2 (5-20 ② — 사용자 결정)",
          desc="'노트북 조작'이 '잠금을 푼 뒤 조작'일 때, 잠금을 푼 뒤 이 시간 동안 아무 조작이 없으면 노트북 화면이 다시 잠급니다. "
               "정지 버튼은 잠겨 있어도 늘 누를 수 있습니다."),
    _inst("LOCAL_DATA_DIR", "노트북 기록 폴더", S, r"C:\ChamberK_v2\data", pattern=r".+", safety=L,
          src=_V2, desc="노트북에 1초 스냅샷(SQLite)·일별 CSV·기록·블랙박스를 저장하는 폴더입니다.",
                   note="기록 = 이벤트."),
    _inst("LOCAL_HMI_STATIC_DIR", "노트북 화면 파일 폴더", S, "", pattern=r".*", safety=L, src=_V2,
          desc="노트북 화면 서버가 화면 파일을 내주는 폴더이며, 이 폴더 밖의 경로는 내주지 않습니다. 비워 두면 프로그램 위치를 기준으로 "
               "프로그램에 함께 들어 있는 화면 파일 폴더를 씁니다.",
          note="서버가 / 와 정적 파일로 내준다. 비면 이 저장소의 v2\\hmi(사용자 결정 2026-10-22). 화면 파일은 웹 W-7 "
               "에서 넣는다(127.0.0.1:LOCAL_HMI_PORT)."),
    _inst("NAS_COPY_DIR", "NAS 사본 폴더", S, r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK2",
          pattern=r".*", safety=L, src=_V2, desc="기록 파일의 사본을 둘 NAS 폴더입니다. 비워 두면 NAS 에 복사하지 않으며, 복사는 제어와 분리된 별도 스레드가 합니다."),
    _inst("KEEP_AWAKE", "절전 막기", E, "run", enum=("run", "always", "off"),
          enum_labels=(("run", "운전 중만"), ("always", "프로그램이 켜져 있는 동안"), ("off", "막지 않음")), src=_V2,
          desc="장비 노트북이 대기 모드(절전)로 들어가지 않게 막는 때이며, 기본은 공정·수동 조작·히터 운전 중에만 막습니다. "
               "덮개 닫기·전원 버튼처럼 사람이 직접 시킨 절전은 막지 못하므로 윈도우 전원 설정에서 덮개 닫기와 절전을 바꿔 두어야 합니다.",
          note="화면 꺼짐(ES_DISPLAY_REQUIRED)은 막지 않는다. SetThreadExecutionState 는 사용자 절전(덮개·전원 버튼)을 막지 못한다"
               "(Microsoft 문서) — 시작 때 전원 설정을 읽어 위험하면 POWER_SETTINGS 경고(W-25 A5, 설정은 바꾸지 않음)."),
    _inst("INVENTORY_API_URL", "재고관리 장착 타겟 주소", S, "https://vanam.synology.me/inventory/api/chamber-slots",
          pattern=r"(https?://\S+)?", safety=L, src=_V2,
          desc="재고관리 서버의 챔버 자리 목록 주소로, 연결할 때·5분마다·공정 시작 때 읽어 Gun1·Gun2 장착 타겟을 "
               "보입니다(사내망은 로그인 없이 읽음). 비우면 읽지 않습니다."),
    _inst("UPLINK_IN_SIM", "sim 에서도 웹 연결(웹 개발용)", B, False, safety=H, src=_V2, locked=True,
          desc="켜면 sim 모드에서도 웹에 연결합니다(웹 개발용). 진짜 장비 기록과 섞이지 않게 이때 '장비 ID'가 -SIM 으로 끝나야 "
               "프로그램이 시작하며, 이 설정은 웹에서 바꾸지 않습니다.",
          note="웹은 hello 의 runMode 로 sim 장비를 구분한다. EQUIPMENT_ID 가 -SIM 으로 끝나지 않으면 시작 "
               "거부."),

    # ───────────── 비밀값(secret) — 장비 로컬 파일에만, 웹에 보내지 않음 ─────────────
    _p("UPLINK_URL", "secret", "웹 수집 주소", "", S, "", scope=Scope.SECRET, apply=RESTART,
       pattern=r"(https?://.+)?", v1_key="ERP_INGEST_URL", src="lib/config_local.py (이름만)",
       desc="웹의 장비 연결 기준 주소입니다. https 는 인증서를 확인하고 http 면 시작 때 경고하며, 로그·화면에는 호스트만 "
            "보입니다.",
       note="POST {주소}/ingest, GET {주소}/stream"),
    _p("UPLINK_TOKEN", "secret", "웹 수집 토큰(장비별)", "", S, "", scope=Scope.SECRET, apply=RESTART,
       v1_key="ERP_INGEST_TOKEN", src="lib/config_local.py (이름만)",
        desc="'웹 수집 주소'로 보내는 모든 요청(상태 보내기·명령 받기)에 싣는 장비별 인증 토큰입니다. 장비 노트북 파일에만 두고 "
             "로그·화면·웹 메시지에는 쓰지 않으며, 바꾸면 재시작해야 적용됩니다.",
        note="Authorization: Bearer 헤더로 싣는다."),

    # ───────────── 1초 수집(tick) ─────────────
    _p("TICK_DEADLINE_MS", "tick", "1초 수집 마감", "ms", I, 900, 500, 950, safety=M, apply=RESTART, src=_V2,
       desc="매초 정각에 장비 읽기를 보내고, 정각에서 이 시간이 지나면 그 초의 1초 기록 한 줄을 닫습니다. 그보다 늦게 온 응답은 "
            "다음 줄에 씁니다.",
       note="정각 초 t 에 읽기를 보내고 t+이 값에 행을 닫는다. 늦은 응답은 다음 행에 씀(처음 쓰이는 행 late, 그 뒤 "
            "stale). 장비별 읽기 마감 ≤ 이 값(X12, Ion Gauge 는 예외 X25)."),
    _p("PLC_FAST_POLL_MS", "tick", "PLC 내부 폴링 주기", "ms", I, 250, 100, 500, safety=H, apply=RESTART, src=_V2,
       desc="PLC 안전 비트(MV·인터락·히터)를 읽는 주기입니다. 기본 250 ms 면 1초에 4번 읽고, 그중 정각 회차 값을 1초 "
            "기록에 씁니다.",
       v1_default=200, note="v1 200 ms(상대 타이머)"),
    _p("PLC_READ_DEADLINE_MS", "tick", "PLC 읽기 마감", "ms", I, 400, 100, 900, apply=RESTART, src=_V2,
        desc="PLC 한 번 읽기(재시도 포함)가 끝나야 하는 한도로, 넘기면 그 읽기는 무응답으로 끝납니다. 1초 수집과 내부 폴링이 모두 "
             "이 한도를 쓰며, '1초 수집 마감' 이하이고 'PLC 내부 폴링 주기'의 2배 이하여야 합니다."),
    _p("MFC_READ_DEADLINE_MS", "tick", "MFC 읽기 마감", "ms", I, 900, 200, 950, apply=RESTART, src=_V2,
        desc="MFC 1초 읽기(압력·유량·밸브 위치)가 끝나야 하는 한도로, 넘기면 그 읽기는 실패로 끝납니다. 'MFC 읽기 계획'이 "
             "auto 일 때 느린 읽기 판정도 이 한도의 비율로 하며, '1초 수집 마감' 이하여야 합니다."),
    _p("DC_READ_DEADLINE_MS", "tick", "DC 읽기 마감", "ms", I, 500, 100, 900, apply=RESTART, src=_V2,
        desc="DC 1초 측정 질의가 끝나야 하는 한도로, 넘기면 그 읽기는 실패로 끝납니다. 'DC 쓰기 뒤 질의 금지 시간' 때문에 이 "
             "한도 안에 질의를 보내지 못해도 실패가 되며, '1초 수집 마감' 이하여야 합니다."),
    _p("RFPULSE_READ_DEADLINE_MS", "tick", "RF Pulse 읽기 마감", "ms", I, 900, 200, 950, apply=RESTART, src=_V2,
        desc="RF Pulse 1초 읽기(상태·Forward·Reflected)가 끝나야 하는 한도로, 넘기면 그 읽기는 실패로 끝납니다. "
             "'RF Pulse 읽기 계획'이 auto 일 때 느린 읽기 판정도 이 한도의 비율로 하며, '1초 수집 마감' 이하여야 "
             "합니다."),
    _p("IG_RESPONSE_LIMIT_MS", "tick", "Ion Gauge E 응답 제한", "ms", I, 1200, 900, 1900, apply=RESTART, src="lib/config.py:756 (v1 실측 근거)",
       desc="Ion Gauge 상태 읽기를 보낸 뒤 응답을 기다리는 한도로, '1초 수집 마감'을 넘겨 온 값은 다음 1초 기록에 쓰고 이 "
            "시간을 넘기면 읽기 실패로 봅니다. 레인지를 찾을 때는 응답이 860 ms 까지 걸려(평소 250~300 ms) 다른 장비의 "
            "읽기 마감과 달리 '1초 수집 마감'보다 길게 둡니다.",
       note="E 응답을 tick 마감 뒤에도 이만큼까지 받아 다음 행에 late 로 쓴다(그동안 다음 E 는 보내지 않아 stale). "
            "넘기면 fail. 장비별 읽기 마감 ≤ tick 마감 규칙(X12)의 예외, tick 마감 이상·2000 ms 미만(X25).",
       v1_key="IG_RESPONSE_TIMEOUT_MS"),
    _p("PLC_RETRY_COUNT", "comm", "PLC 재시도 횟수", "회", I, 2, 0, 5, desc="PLC Modbus 트랜잭션이 실패했을 때 다시 보내는 횟수로, 처음 한 번을 더해 모두 1 + 이 횟수만큼 보냅니다. "
                                                                                  "재시도까지 'PLC 읽기 마감' 안에 끝나야 합니다."),
    _p("PLC_RETRY_DELAY_MS", "comm", "PLC 재시도 간격", "ms", I, 30, 0, 1000, safety=L,
        desc="PLC Modbus 트랜잭션이 실패했을 때 다시 보내기 전에 쉬는 시간입니다. 재시도는 'PLC 재시도 횟수'만큼 하며, 쉬는 "
             "시간도 'PLC 읽기 마감' 안에 들어갑니다."),
    _p("PLC_COMM_LOSS_ABORT_SEC", "comm", "PLC 끊김 공정 중단", "s", F, 10.0, 1.0, 10.0, safety=H,
       desc="PLC 읽기가 마지막으로 성공한 뒤 이 시간이 지나면 공정을 중단합니다. PLC 래더가 히터를 끄는 워치독 시간(10 s) "
            "이하로 둡니다.",
       note="래더 히터 워치독 TON(10 s) 이하."),
    _p("PLC_DISCONNECT_NOTIFY_SEC", "comm", "PLC 끊김 알림 유예", "s", F, 60.0, 10.0, 600.0, safety=L,
       src="device/PLC.py:192 (_DISCONNECT_GRACE_S)",
        desc="PLC 값이 이 시간 넘게 들어오지 않으면 끊김 알림(오류)을 끊길 때마다 한 번 보내고 PLC 블랙박스를 파일로 남깁니다. "
             "다시 연결되면 정보 알림을 한 번 보내며, 공정 중단과는 별개로 늘 봅니다."),
    _p("DC_COMM_LOSS_ABORT_SEC", "comm", "DC 끊김 공정 중단", "s", F, 10.0, 2.0, 30.0, safety=H,
        desc="DC 를 쓰는 공정에서 출력을 켠 뒤 DC 값이 이 시간 넘게 들어오지 않으면 DC 끄기를 보내고 공정을 중단합니다. 통신이 "
             "돌아왔을 때 공정이 없으면 DC 끄기를 다시 보냅니다."),
    _p("MFC_COMM_LOSS_ABORT_SEC", "comm", "MFC 끊김 공정 중단", "s", F, 30.0, 5.0, 120.0, safety=H, src=_V2,
       desc="공정이나 수동 조작 중에 MFC 값이 이 시간 넘게 들어오지 않으면 정지 순서로 공정을 멈춥니다. MFC 가 끊긴 채로도 파워 "
            "끄기·셔터 닫기·가스 밸브 닫기는 끝까지 합니다."),
    _p("RFPULSE_COMM_LOSS_ABORT_SEC", "comm", "RF Pulse 끊김 공정 중단", "s", F, 10.0, 2.0, 30.0,
       safety=H,
        desc="RF Pulse 를 쓰는 공정에서 RF Pulse 를 시작한 뒤 값이 이 시간 넘게 들어오지 않으면 RF Pulse 끄기를 "
             "보내고 공정을 중단합니다. 통신이 돌아왔을 때 공정이 없으면 RF Pulse 끄기를 다시 보냅니다."),
    _p("RFPULSE_COMM_REOPEN_SEC", "comm", "RF Pulse 포트 다시 열기", "s", F, 3.0, 1.0, 30.0, safety=L,
       src="lib/config.py:57 (고정값)",
        desc="지금은 쓰지 않는 설정입니다. 다시 연결은 'RF Pulse 연속 무응답 재연결 횟수'와 '재연결 첫 간격'·'재연결 최대 "
             "간격'이 정합니다.",
        note="v2 에서는 사용하지 않는다. v1 은 무응답이 이만큼 이어지면 포트를 다시 열었음."),
    _p("IG_COMM_LOSS_ABORT_SEC", "comm", "Ion Gauge 끊김 베이스 대기 중단", "s", F, 60.0, 10.0, 600.0,
       src="controller/process_controller.py:35",
        desc="베이스 압력 대기 단계 중 Ion Gauge 값이 이 시간 넘게 들어오지 않으면 공정을 중단합니다. Filament 켜기·끄기 "
             "직후의 'Filament 전환 뒤 무응답 유예' 구간은 끊김으로 세지 않습니다."),
    _p("COMM_RECONNECT_START_MS", "comm", "재연결 첫 간격", "ms", I, 1000, 100, 60000, safety=L,
        desc="장비 포트가 끊기거나 열기에 실패한 뒤 다시 연결을 시도하기까지의 첫 간격입니다. 실패할 때마다 두 배로 늘어 '재연결 최대 "
             "간격'까지 커지며, 다섯 장비에 함께 씁니다."),
    _p("COMM_RECONNECT_MAX_MS", "comm", "재연결 최대 간격", "ms", I, 60000, 1000, 600000, safety=L,
        desc="실패할 때마다 두 배로 늘어나는 재연결 간격의 상한입니다. '재연결 첫 간격'보다 작을 수 없으며, 다섯 장비에 함께 씁니다."),
    _p("COMM_LONG_OUTAGE_SEC", "comm", "장기 끊김 판정", "s", I, 600, 30, 86400, safety=L,
        desc="연결 실패가 이 시간 넘게 이어지면 장기 끊김으로 보고 알림을 한 번 냅니다. 그 뒤로는 '장기 끊김 중 시도 주기'마다 연결을 "
             "시도하고, 실패 기록은 '끊김 기록 간격'마다만 남깁니다."),
    _p("COMM_PROBE_MS", "comm", "장기 끊김 중 시도 주기", "ms", I, 60000, 1000, 600000, safety=L,
        desc="장기 끊김으로 본 뒤 연결을 다시 시도하는 간격입니다. 이때부터는 두 배로 늘리는 재연결 간격 대신 이 주기로 시도합니다."),
    _p("COMM_OUTAGE_LOG_SEC", "comm", "끊김 기록 간격", "s", I, 600, 30, 86400, safety=L, apply=LIVE,
        desc="장기 끊김 중 연결 실패 기록을 남기는 간격입니다. DC 끄기를 확인하지 못해 다시 시도할 때 늘어나는 간격('DC OFF "
             "미확인 재시도 첫 간격'에서 시작)의 상한으로도 씁니다."),

    # ───────────── PLC ─────────────
    _p("PLC_SESSION_MARK_REG", "plc", "PLC 재기동 감지 D 주소", "D번호", I, 60, 0, 9999,
       desc="PLC 재기동을 감지하려고 연결할 때 표시 값을 써 두고 계속 되읽는 PLC D 주소로, 래더가 쓰지 않는 주소여야 합니다. "
            "0 이면 재기동 감지를 하지 않습니다.",
       note="폴링마다 마커 D 를 비교, 2회 연속 다르면 재기동(S9): 히터 한계값·마커 다시 쓰기, 공정 중이면 중단. 사용 중인 D "
            "주소와 겹치면 안 됨(X09)."),
    _p("MV_INTERLOCK_ABORT_MS", "plc", "MV 인터락 해제 지속 중단", "ms", I, 1000, 200, 5000, safety=H,
       src="main.py:3839", desc="공정 중 MV 인터락 신호가 이 시간 이상 이어서 꺼져 있으면 공정을 중단합니다. 이보다 짧게 끊겼다 돌아오면 기록만 "
                                "남깁니다.",
                           note="MV_INTERLOCK 이 이만큼 연속 OFF 면 중단(S3), 순간 끊김은 기록만."),
    _p("BLACKBOX_SECONDS", "plc", "PLC 블랙박스 길이", "s", I, 60, 10, 600, safety=L,
       apply=RESTART, src="device/PLC.py:178 (deque 300 × 200 ms)",
        desc="PLC 내부 폴링 값을 최근 이 시간만큼 메모리에 담아 두는 블랙박스 길이입니다. PLC 통신 끊김 알림·재기동 감지 때 "
             "이 기록을 파일로 남기며, 길수록 사고 전 기록이 길어집니다."),

    # ───────────── MFC(경고·알림만, 공정은 계속) ─────────────
    _p("FLOW_ERROR_TOLERANCE", "mfc", "유량 이탈 허용 비율", "비율", F, 0.10, 0.01, 0.5, apply=LIVE,
       desc="MFC 실제 유량이 설정 유량에서 벗어나도 되는 비율로, 차이가 설정 × 이 비율 이하이면 정상으로 봅니다. 유량을 켤 때의 "
            "안정화 판정과 공정 중 유량 이탈 감시(경고)에 씁니다.",
       note="안정화 판정(FLOW ON 뒤)과 공정 중 감시(S38A)에 쓴다."),
    _p("FLOW_ERROR_SEC", "mfc", "유량 이탈 지속 시간", "s", F, 10.0, 1.0, 600.0, safety=L, apply=LIVE,
       v1_key="FLOW_ERROR_MAX_COUNT", v1_period=2.0, v1_default=5,
       desc="공정 중 유량이 '유량 이탈 허용 비율'을 넘어 벗어난 상태가 이 시간 이어지면 경고합니다. 경고만 하고 공정은 계속하며, "
            "유량 이탈 감시가 켜진 단계(기본 템플릿은 증착)에서만 봅니다.",
       note="v1: 5회 × MFC 폴링 2 s"),
    _p("MFC_PRESSURE_WARN_RATIO", "mfc", "압력 이탈 허용 비율", "비율", F, 0.10, 0.01, 0.5, safety=L,
       apply=LIVE,
        desc="공정 중 챔버 압력과 작업 압력의 차이가 작업 압력 × 이 비율을 넘은 채 '압력 이탈 지속 시간' 이어지면 경고합니다. "
             "경고만 하고 공정은 계속하며, 압력 이탈 감시가 켜진 단계(기본 템플릿은 증착)에서만 봅니다."),
    _p("MFC_PRESSURE_WARN_SEC", "mfc", "압력 이탈 지속 시간", "s", F, 10.0, 1.0, 600.0, safety=L, apply=LIVE,
       v1_key="MFC_PRESSURE_WARN_COUNT", v1_period=2.0, v1_default=5, desc="공정 중 챔버 압력이 작업 압력에서 '압력 이탈 허용 비율'을 넘어 벗어난 상태가 이 시간 이어지면 경고합니다. 경고만 하고 "
                                                                           "공정은 계속합니다.",
                                                                      note="v1: 5회 × MFC 폴링 2 s"),
    _p("MFC_FLOW_STABLE_TIMEOUT_SEC", "mfc", "FLOW ON 유량 안정화 한도", "s", F, 100.0, 5.0, 600.0,
       v1_key="MFC_STABILIZATION_TIMEOUT_S", v1_default=100,
       src="config_user.json · lib/config.py MFC_STABILIZATION_TIMEOUT_S(v1 563f772 — 기본 100, 허용 10~600, 시간 판정. "
           "그 전 v1 은 30회 × 1 s)",
       desc="가스 유량을 켠 뒤 실제 유량과 설정 유량의 차이가 설정 × '유량 이탈 허용 비율' 이하가 되기를 기다리는 최대 시간입니다. "
            "이 시간 안에 들어오지 않으면 공정을 중단합니다.",
       note="FLOW ON(R69 확인) 뒤 |실제 − 설정| ≤ 설정 × FLOW_ERROR_TOLERANCE 를 기다림. 넘으면 "
            "FLOW_STABILIZE_TIMEOUT(v1 '유량 안정화 시간 초과(100초)')."),
    _p("ZERO_MIN_IDLE_SEC", "mfc", "영점 전 가스 멈춤 시간", "s", F, 10.0, 0.0, 600.0, safety=M, apply=LIVE, src=_V2,
       desc="유량이 꺼지고 가스 밸브가 닫히고 MFC 밸브가 열린 상태가 이 시간 이어져야 MFC·압력 영점을 보냅니다. Ion Gauge "
            "가 연결돼 있으면 Gauge A 가 범위 아래여야 하며, 조건이 될 때까지 '영점 대기 한도'만큼 기다립니다."),
    _p("DEVICE_PARAMS_REFRESH_MIN", "mfc", "컨트롤러 값 다시 읽기 주기", "분", I, 10, 0, 1440, safety=L, apply=LIVE, src=_V2,
       desc="대기 중에 MFC 컨트롤러에 저장된 값을 이 간격으로 다시 읽고, 프로그램이 바꾸지 않았는데 값이 달라졌으면 경고를 남깁니다. "
            "공정·수동 조작이 끝난 뒤에도 한 번 읽으며, 0 이면 주기 읽기를 하지 않습니다."),
    _p("ZERO_WAIT_MAX_SEC", "mfc", "영점 대기 한도", "s", F, 30.0, 0.0, 300.0, safety=M, apply=LIVE, src=_V2,
       desc="영점 조건이 될 때까지 기다리는 가장 긴 시간입니다. 넘으면 그 영점만 건너뛰고 이유를 기록에 남긴 뒤 공정을 이어 갑니다."),
    _p("MFC_VALVE_SETTLE_SEC", "mfc", "MFC 밸브 이동 대기", "s", F, 5.0, 0.5, 60.0, safety=L,
       src="lib/config.py:799 (MFC_DELAY_MS_VALVE)", desc="MFC 밸브 열기·닫기 명령을 보낸 뒤 밸브 위치를 읽어 확인하기까지 기다리는 시간입니다.",
                                                     note="O·C 뒤 R6 로 위치를 확인하기까지."),
    _p("MFC_SP_VERIFY_TOL", "mfc", "압력 SP(SP1·2·3·5) 설정 확인 허용 오차", "장비 단위", F, 0.01, 0.0, 1.0, safety=L,
       src="lib/config.py MFC_SP_VERIFY_TOL (fa85e15, 고정값 — 예전 MFC_SP1_VERIFY_TOL 0.1)",
       desc="압력 SP 를 MFC 에 쓴 뒤 되읽은 값과 보낸 값의 차이(%FS)가 이 값 이하이면 설정 확인을 통과합니다. 장비가 소수 "
            "둘째 자리까지 저장하므로 0.01 %FS(FS 1 Torr 에서 0.1 mTorr)로 둡니다.",
       note="Sn 쓰기 뒤 R1~R3·R10 으로 되읽어 비교."),

    # ───────────── DC Power ─────────────
    _p("DC_MAX_VOLTAGE", "dc", "DC 전압 상한", "V", F, 600.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:512 (고정값)", desc="DC 출력을 켤 때 장비에 주는 전압 상한으로, 장비 정격 이하로 둡니다. 'DC 연결 때 OVP·OCP 쓰기'를 켜면 OVP "
                                                 "값으로도 씁니다.",
                                            note="APPL 전압 상한. 장비 정격 이하(X19)."),
    _p("DC_MAX_CURRENT", "dc", "DC 전류 상한", "A", F, 1.0, 0.0, 2.0, safety=H, src="lib/config.py:513 (고정값)",
        desc="DC 전류 설정의 상한으로, 정전류 제어는 전류를 이 값 위로 올리지 않습니다. 올리는 중 이 상한에 걸린 채 목표에 미치지 "
             "않으면 'DC 상한 정체 시 유지로 넘김' 시간 뒤 유지 단계로 넘어가며, 'DC 연결 때 OVP·OCP 쓰기'를 켜면 OCP "
             "값으로도 씁니다."),
    _p("DC_MAX_POWER", "dc", "DC 파워 상한", "W", F, 500.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:514 (고정값)", desc="레시피·수동 조작에서 넣을 수 있는 DC 파워의 최대값입니다. 공정 중 값을 바꿀 때도 이 값으로 검사합니다.",
                                            note="v1 은 드라이버에만 있었다."),
    _p("DC_INITIAL_VOLTAGE", "dc", "DC 시작 전압", "V", F, 450.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:510 (고정값)",
        desc="DC 출력을 켤 때 이 값은 쓰지 않고 'DC 전압 상한'으로 켭니다. 설정 검사에서 'DC 전압 상한' 이하인지만 봅니다.",
        note="v2 의 출력 켜기는 DC_MAX_VOLTAGE 로 켠다. v1 에서도 장비에 보내지 않음(X19)."),
    _p("DC_INITIAL_CURRENT", "dc", "DC 시작 전류", "A", F, 0.1, 0.0, 2.0, safety=H, src="lib/config.py:511 (고정값)",
        desc="DC 출력을 켤 때 처음 주는 전류 설정입니다. 켠 뒤에는 정전류 제어가 이 값에서부터 전류를 조절해 목표 파워를 맞추며, "
             "'DC 전류 상한'보다 크면 상한을 씁니다."),
    _p("DC_TOLERANCE_W", "dc", "DC 목표 도달 허용 오차", "W", F, 0.5, 0.1, 10.0, src="lib/config.py:515 (고정값)",
        desc="DC 파워가 목표 ± 이 값 안이면 도달로 보고, 유지 중에는 전류를 고치지 않습니다. 유지 이탈 판정 폭은 이 값과 목표 × "
             "'DC 유지 이탈 비율' 중 큰 값입니다."),
    _p("DC_POWER_ERROR_RATIO", "dc", "DC 유지 이탈 비율", "비율", F, 0.10, 0.02, 0.5, safety=H,
       desc="DC 파워가 목표에서 벗어났다고 보는 폭을 정하는 비율로, 판정 폭은 목표 × 이 비율과 'DC 목표 도달 허용 오차' 중 큰 "
            "값입니다. 이 폭을 넘은 채 'DC 유지 이탈 지속 시간' 이어지면 공정을 중단합니다.",
       note="판정 폭 = max(DC_TOLERANCE_W, 목표×비율)"),
    _p("DC_POWER_ERROR_SEC", "dc", "DC 유지 이탈 지속 시간", "s", F, 5.0, 1.0, 60.0, safety=H,
       v1_key="DC_POWER_ERROR_MAX_COUNT", v1_period=1.0, v1_default=5, desc="DC 파워 감시가 켜진 단계(기본 템플릿은 Shutter Delay·증착)에서 DC 파워가 판정 폭('DC 유지 이탈 비율' "
                                                                            "참고)을 넘어 벗어난 상태가 이 시간 이어지면 공정을 중단합니다.",
                                                                       note="v1: 5회 × DC 제어 1 s"),
    _p("DC_MIN_CURRENT_ABORT", "dc", "DC 저전류 기준", "A", F, 0.05, 0.0, 1.0, safety=H,
        desc="DC 파워 유지 중 측정 전류가 이 값 이하인 상태가 'DC 저전류 지속 시간' 이어지면 공정을 중단합니다."),
    _p("DC_MIN_CURRENT_ABORT_SEC", "dc", "DC 저전류 지속 시간", "s", F, 10.0, 1.0, 120.0, safety=H,
       v1_key="DC_MIN_CURRENT_ABORT_COUNT", v1_period=1.0, v1_default=10, desc="DC 파워 유지 중 측정 전류가 'DC 저전류 기준' 이하인 상태가 이 시간 이어지면 공정을 중단합니다.",
                                                                          note="v1: 10회 × DC 제어 1 s"),
    _p("DC_FAIL_ISET_A", "dc", "DC 무출력 판정 설정 전류", "A", F, 0.20, 0.01, 1.0, safety=H,
       src="lib/config.py:517 (고정값)",
        desc="DC 올리는 중 설정 전류가 이 값 이상인데 파워가 'DC 무출력 판정 파워' 이하인 상태가 'DC 올리는 중 무출력 허용' "
             "시간 이어지면, 출력이 나오지 않는 것으로 보고 공정을 중단합니다."),
    _p("DC_FAIL_POWER_W", "dc", "DC 무출력 판정 파워", "W", F, 1.0, 0.1, 50.0, safety=H,
       src="lib/config.py:518 (고정값)",
        desc="DC 올리는 중 설정 전류가 'DC 무출력 판정 설정 전류' 이상인데 파워가 이 값 이하인 상태가 'DC 올리는 중 무출력 "
             "허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 공정을 중단합니다."),
    _p("DC_FAIL_SEC", "dc", "DC 올리는 중 무출력 허용", "s", F, 15.0, 3.0, 120.0, safety=H,
       v1_key="DC_FAIL_MAX_TICKS", v1_period=1.0, v1_default=15,
       desc="DC 올리는 중 설정 전류가 'DC 무출력 판정 설정 전류' 이상인데 파워가 'DC 무출력 판정 파워' 이하인 상태가 이 시간 "
            "이어지면, 출력이 나오지 않는 것으로 보고 공정을 중단합니다.",
       note="Iset ≥ DC_FAIL_ISET_A 인데 P ≤ DC_FAIL_POWER_W 가 이만큼 이어지면 중단. v1: 15틱 × "
            "1 s"),
    _p("DC_POWER_DELAY_SEC", "dc", "DC 안정화 대기", "s", I, 300, 0, 3600,
       desc="레시피에서 DC 안정화 대기를 켜고 DC 를 쓸 때, 작업 압력(SP1)에 도달한 뒤 Shutter Delay 전에 이 "
            "시간만큼 기다립니다. 이 동안에는 DC 파워 이탈 판정을 하지 않으며, 0 이면 기다리지 않습니다.",
       note="SP1 도달 뒤 Shutter Delay 전 대기(레시피 use_dc_delay 일 때만). 이 동안 DC ±이탈 판정 꺼짐."),
    _p("DC_CONTROL_GAIN", "dc", "DC 전류 보정 이득", "배", F, 1.0, 0.1, 1.0, safety=H,
       desc="DC 정전류 제어에서 파워 오차를 전류 보정량으로 바꿀 때 곱하는 이득으로, 보정 전류는 이득 × 파워 오차 ÷ 전압입니다. "
            "작을수록 천천히 고칩니다.",
       note="ΔI = 이득 × ΔP / V. v1 은 검사가 없어 음수도 허용했다."),
    _p("DC_RAMP_STEP_A", "dc", "DC 올리는 중 전류 상승 상한", "A/s", F, 0.010, 0.001, 0.1, safety=H,
        desc="DC 올리는 중 1초에 올릴 수 있는 전류의 상한입니다. 작을수록 천천히 올라가 파워 도달이 늦어지며, 파워 도달 대기 한도의 "
             "예상 시간 계산에도 씁니다."),
    _p("DC_MAINTAIN_STEP_UP_A", "dc", "DC 유지 중 전류 상승 상한", "A/s", F, 0.020, 0.001, 0.1, safety=H,
        desc="DC 파워 유지 중 1초에 올릴 수 있는 전류의 상한입니다. 올리는 중에는 'DC 올리는 중 전류 상승 상한'을 씁니다."),
    _p("DC_MAINTAIN_STEP_DOWN_A", "dc", "DC 전류 하강 상한", "A/s", F, 0.050, 0.001, 0.2,
        desc="DC 전류를 1초에 내릴 수 있는 상한으로, 천천히 올리기와 유지 모두에 씁니다. 측정이 한 번 튀어도 전류가 한꺼번에 크게 "
             "떨어지지 않게 합니다."),
    _p("DC_SMALL_ERROR_RATIO", "dc", "DC 작은 오차 경계", "비율", F, 0.01, 0.0, 0.1, safety=L,
        desc="DC 파워 유지 중 오차가 목표 × 이 비율(최소 'DC 목표 도달 허용 오차') 이하이면 작은 오차로 보고 'DC 작은 오차 "
             "이득'으로 고칩니다. 0 이면 작은 오차 구간을 쓰지 않습니다."),
    _p("DC_SMALL_ERROR_GAIN", "dc", "DC 작은 오차 이득", "배", F, 0.5, 0.1, 1.0, safety=L,
        desc="DC 파워 유지 중 작은 오차 구간에서 쓰는 전류 보정 이득입니다. 1 이면 보통 보정과 같고, 작을수록 작은 오차를 천천히 "
             "고쳐 덜 흔들립니다."),
    _p("DC_LIMIT_STALL_SEC", "dc", "DC 상한 정체 시 유지로 넘김", "s", I, 10, 1, 120, safety=L,
        desc="DC 올리는 중 전류 상한이나 전압 상한 근처('DC 전압 상한' − 20 V)에 걸린 채 목표에 미치지 않는 상태가 이 시간 "
             "이어지면, 목표 미달인 채로 유지 단계로 넘깁니다. 이렇게 파워 도달 대기를 끝내고 다음 단계(기본 템플릿에서는 압력 단계)로 "
             "진행합니다."),
    _p("DC_OFF_VERIFY_TRIES", "dc", "DC OFF 확인 횟수", "회", I, 3, 1, 10, safety=H,
       src="device/DCpower.py:38", desc="DC 출력 끄기를 보낸 뒤 출력 상태를 되물어 꺼짐을 확인하는 최대 횟수입니다. 응답이 없으면 끄기를 다시 보냅니다.",
                                   note="OUTP OFF 뒤 OUTP? 확인 횟수(무응답이면 OFF 재전송)."),
    _p("DC_SET_PROTECTION", "dc", "DC 연결 때 OVP·OCP 쓰기", "", B, False, safety=H, src="v2 (사용자 결정 2026-10-15)",
       desc="켜면 DC 에 연결할 때 장비의 OVP·OCP 를 'DC 전압 상한'·'DC 전류 상한'으로 쓰고 되읽어 확인합니다. 기본은 "
            "꺼짐이며, 형명과 공정의 실제 최대 전압·전류(점화 순간 포함)를 장비에서 확인한 뒤 켭니다.",
       note="true 면 연결 때 쓰고 되읽는다(prod·sim). bench 에서 확인한 뒤 켠다. "
            "VOLT:OVP=DC_MAX_VOLTAGE, CURR:OCP=DC_MAX_CURRENT."),
    _p("DC_OFF_RETRY_START_SEC", "dc", "DC OFF 미확인 재시도 첫 간격", "s", F, 10.0, 2.0, 600.0,
       src="device/DCpower.py:32", desc="DC 출력 끄기를 확인하지 못했을 때 끄기를 다시 시도하기까지의 첫 간격입니다. 실패할 때마다 두 배로 늘어나며 '끊김 기록 "
                                        "간격'까지 커집니다.",
                                   note="이후 2배씩, 최대 COMM_OUTAGE_LOG_SEC."),

    # ───────────── RF Power(PLC DAC/ADC) ─────────────
    _p("RF_MAX_POWER", "rf", "RF DAC 풀스케일(환산 기준)", "W", F, 600.0, 1.0, 600.0, safety=H,
       src="lib/config.py:561 (고정값)",
       desc="RF 파워를 DAC 카운트로 바꾸는 환산 기준값으로, 상한으로 쓰지 않습니다. 바꾸면 같은 입력에서 출력이 달라지며, RF "
            "파워 상한은 'RF 운전 상한'입니다.",
       note="DAC 카운트 = (P ÷ 이 값) × 4000 × 이득 + 오프셋(RF_CAL_GAIN·RF_CAL_OFFSET). 파워 "
            "상한은 RF_OPERATING_MAX_W(W-22)."),
    _p("RF_OPERATING_MAX_W", "rf", "RF 운전 상한", "W", F, 600.0, 1.0, 600.0, safety=M, apply=LIVE, src=_V2,
       desc="RF 파워로 넣을 수 있는 최대값입니다. 레시피 RF 파워, 파워 켜기 단계, 공정 중 값 바꾸기, 수동 조작이 모두 이 "
            "값으로 검사합니다."),
    _p("RF_UNCOMMANDED_FWD_W", "rf", "RF 명령 없는 출력 기준", "W", F, 5.0, 0.5, 100.0, safety=M, apply=LIVE, src=_V2,
       desc="프로그램이 RF 를 켜지 않았는데 Forward 가 이 값 이상이면 '명령 없이 출력 중'으로 보고 기록을 남깁니다."),
    _p("RF_UNCOMMANDED_GRACE_SEC", "rf", "RF 끈 뒤 명령 없는 출력 유예", "s", F, 5.0, 0.0, 30.0, safety=M, apply=LIVE, src=_V2,
       desc="프로그램이 RF 를 끈 뒤 이 시간 동안은 Forward 가 남아 있어도 꺼지는 중으로 보고 '명령 없이 출력 중'으로 보지 않습니다. "
            "0 이면 끈 바로 다음 읽기부터 판정합니다.",
       note="W-25 A1(챔버12 에서 재현) — 끄는 순간 PLC Forward ADC 가 1~2 s 늦게 내려와 RF_UNCOMMANDED 경고가 잘못 뜸. "
            "rf.cmd_on 이 참이었던 마지막 PLC 읽기 시각부터 잰다(drivers/plc.py _decode)."),
    _p("RF_FWD_FULL_SCALE_W", "rf", "RF Forward ADC 풀스케일", "W", F, 594.5, 1.0, 2000.0, safety=H,
       src="lib/config.py:558 (고정값)", desc="PLC ADC 4000 카운트에 해당하는 Forward 파워(W)로, Forward 측정값을 환산하는 교정 상수입니다.",
                                            note="v1 값이며 사용자 확인을 마쳤다(2026-10-15)."),
    _p("RF_REF_FULL_SCALE_W", "rf", "RF Reflected ADC 풀스케일", "W", F, 600.0, 1.0, 2000.0, safety=H, v1_default=200.0,
       src="lib/config.py:559 (v1 200) → W-22 기본 600",
       desc="PLC ADC 4000 카운트에 해당하는 Reflected 파워(W)로, Reflected 측정값을 환산하는 교정 상수입니다. "
            "장비에 저장된 값은 설정 탭에서 바꿉니다.",
       note="2026-11-25 실제 장비 두 점(장비 6 W·화면 2 W, 장비 9 W·화면 3 W)이 200 기준의 3배라 기본값을 "
            "600 으로 바꿨다(W-22, v1 200)."),
    _p("RF_CAL_OFFSET", "rf", "RF DAC 보정 오프셋", "count", F, 6.79, -400.0, 400.0, safety=H,
       src="UI.py:440 (offset_edit 기본값)", desc="RF 목표 파워를 DAC 카운트로 바꿀 때 더하는 보정 오프셋(카운트)입니다. DAC = 이 값 + (파워 ÷ 'RF DAC "
                                                     "풀스케일(환산 기준)') × 4000 × 'RF DAC 보정 이득'입니다.",
                                                note="v1 은 화면 입력칸 값을 공정마다 썼다."),
    _p("RF_CAL_GAIN", "rf", "RF DAC 보정 이득", "배", F, 1.0395, 0.5, 1.5, safety=H,
       src="UI.py:444 (param_edit 기본값)",
        desc="RF 목표 파워를 DAC 카운트로 바꿀 때 곱하는 보정 이득입니다. DAC = 'RF DAC 보정 오프셋' + (파워 ÷ "
             "'RF DAC 풀스케일(환산 기준)') × 4000 × 이득이며, 클수록 같은 파워에 더 큰 DAC 를 냅니다.",
        note="식의 나누는 값은 RF_MAX_POWER(환산 기준, 옛 설명의 'RF 파워 상한'). 운전 상한은 "
             "RF_OPERATING_MAX_W."),
    _p("RF_RAMP_STEP_W", "rf", "RF 천천히 올리기 속도", "W/s", F, 1.0, 0.1, 20.0, safety=H, src="lib/config.py:562 (고정값)",
        desc="RF 를 천천히 올릴 때 1초마다 중간 목표를 올리는 폭입니다. 작을수록 천천히 올라가며, 공정 중 목표를 바꿀 때도 이 "
             "속도로 따라가고 파워 도달 대기 한도 계산에도 씁니다."),
    _p("RF_RAMP_DOWN_STEP_COUNTS", "rf", "RF 천천히 내리기 속도", "count/0.5s", I, 8, 1, 400,
       src="lib/config.py:563 (고정값)", note="v1 이름·주석은 W 지만 실제는 DAC 카운트(RFpower.py:212)",
        desc="RF 천천히 내리기 때 0.5 초마다 DAC 를 이 카운트씩 내립니다. 정지 순서·파워 끄기 단계·시작 때 남은 RF 정리에 "
             "쓰며, 'RF 천천히 내리기 한도' 안에 끝나지 않으면 바로 0 으로 내립니다."),
    _p("RF_TOLERANCE_W", "rf", "RF 목표 도달 허용 오차", "W", F, 1.0, 0.1, 20.0, src="lib/config.py:566 (고정값)",
        desc="RF Forward 파워가 목표 ± 이 값 안에 들어오면 도달로 보고, 유지 중 이 범위를 벗어나면 1초마다 DAC 를 1 씩 "
             "고칩니다. 유지 이탈 판정 폭은 이 값과 목표 × 'RF 유지 이탈 비율' 중 큰 값입니다."),
    _p("RF_POWER_ERROR_RATIO", "rf", "RF 유지 이탈 비율", "비율", F, 0.10, 0.02, 0.5, safety=H,
        desc="RF 파워 유지 중 Forward와 목표의 차이가 목표 × 이 비율(최소 'RF 목표 도달 허용 오차')을 넘은 채 'RF "
             "유지 이탈 지속 시간' 이어지면, RF 를 천천히 내려 끄고 공정을 중단합니다."),
    _p("RF_POWER_ERROR_SEC", "rf", "RF 유지 이탈 지속 시간", "s", F, 5.0, 1.0, 60.0, safety=H,
       v1_key="RF_POWER_ERROR_MAX_COUNT", v1_period=1.0, v1_default=5, desc="RF 파워 유지 중 Forward와 목표의 차이가 'RF 유지 이탈 비율'로 정한 폭을 넘은 상태가 이 시간 이어지면, RF "
                                                                            "를 천천히 내려 끄고 공정을 중단합니다.",
                                                                       note="v1: 5회 × RF 제어 1 s"),
    _p("RF_FAIL_DAC_COUNTS", "rf", "RF 무출력 판정 DAC", "count", I, 100, 10, 4000, safety=H,
       src="lib/config.py:567 (고정값)",
        desc="RF 올리는 중 DAC 가 이 값 이상인데 Forward가 'RF 무출력 판정 Forward' 이하인 상태가 'RF 올리는 중 "
             "무출력 허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 RF 를 내려 끄고 공정을 중단합니다."),
    _p("RF_FAIL_FWD_W", "rf", "RF 무출력 판정 Forward", "W", F, 1.0, 0.1, 50.0, safety=H,
       src="lib/config.py:568 (고정값)",
        desc="RF 올리는 중 DAC 가 'RF 무출력 판정 DAC' 이상인데 Forward가 이 값 이하인 상태가 'RF 올리는 중 무출력 "
             "허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 RF 를 내려 끄고 공정을 중단합니다."),
    _p("RF_FAIL_SEC", "rf", "RF 올리는 중 무출력 허용", "s", F, 10.0, 3.0, 120.0, safety=H,
       v1_key="RF_FAIL_MAX_TICKS", v1_period=1.0, v1_default=10, desc="RF 올리는 중 DAC 가 'RF 무출력 판정 DAC' 이상인데 Forward가 'RF 무출력 판정 Forward' 이하인 "
                                                                      "상태가 이 시간 이어지면, 출력이 나오지 않는 것으로 보고 RF 를 내려 끄고 공정을 중단합니다.",
                                                                 note="v1: 10틱 × 1 s"),
    _p("RF_REFP_ABORT_THRESHOLD", "rf", "RF Reflected 대기 기준", "W", F, 3.0, 0.5, 200.0, safety=H,
       v1_default=20.0, desc="RF Reflected가 이 값 이상이 되면 올리는 중이든 유지 중이든 DAC 조정을 멈추고 기다립니다. 그 상태가 'RF "
                             "Reflected 대기 허용' 시간 이어지면 RF 를 천천히 내려 끄고 공정을 중단합니다."),
    _p("RF_REFP_HYST_W", "rf", "RF Reflected 대기 여유 폭", "W", F, 1.0, 0.0, 50.0, safety=M, apply=LIVE, src=_V2,
       desc="RF Reflected 대기는 'RF Reflected 대기 기준'에서 이 값을 뺀 값 아래로 내려가야 풀립니다. 그 사이를 "
            "오가면 대기 시간을 이어서 셉니다."),
    _p("RF_REFP_WAIT_SEC", "rf", "RF Reflected 대기 허용", "s", I, 60, 1, 600, safety=H, v1_default=15,
        desc="RF Reflected가 'RF Reflected 대기 기준' 이상이면 올리기를 멈추고 기다리는데, 그 상태가 이 시간 "
             "이어지면 RF 를 천천히 내려 끄고 공정을 중단합니다. 파워 도달 대기 한도 계산에는 이 값의 3배를 더합니다."),
    _p("STOP_RF_RAMPDOWN_TIMEOUT_SEC", "stop", "RF 천천히 내리기 한도", "s", F, 120.0, 10.0, 600.0,
       src="controller/process_controller.py:392",
        desc="RF 천천히 내리기가 걸릴 수 있는 한도입니다. 이 시간에서 1 초를 뺀 시간 안에 DAC 가 0 이 되지 않으면 바로 0 "
             "으로 쓰고 다음 단계로 넘어갑니다."),

    # ───────────── RF Pulse(CESAR 1310) ─────────────
    _p("RFPULSE_MAX_POWER", "rfpulse", "RF Pulse 출력 상한", "W", F, 600.0, 1.0, float(dev.CESAR_MAX_POWER_W),
       safety=H,
        desc="레시피·파워 켜기 단계·공정 중 수정에서 넣을 수 있는 RF Pulse 목표 파워의 상한입니다. 이 값을 넘는 목표는 장비에도 "
             "보내지 않습니다.",
        note="드라이버도 이 값을 넘는 목표는 장비에 보내지 않는다"),
    _p("RFPULSE_PULSE_FREQ_MIN_HZ", "rfpulse", "펄스 주파수 하한", "Hz", I, 1, 1, 100,
       src="lib/config.py:613 (고정값)",
        desc="레시피·파워 켜기 단계·공정 중 수정에서 넣을 수 있는 펄스 주파수의 하한입니다. 화면에서는 kHz 로 넣고 장비에는 Hz 로 "
             "보내며, '펄스 주파수 상한'보다 작아야 합니다."),
    _p("RFPULSE_PULSE_FREQ_MAX_HZ", "rfpulse", "펄스 주파수 상한", "Hz", I, 30000, 1, 100000,
        desc="레시피·파워 켜기 단계·공정 중 수정에서 넣을 수 있는 펄스 주파수의 상한입니다. 화면에서는 kHz 로 넣고 장비에는 Hz 로 "
             "보내며, '펄스 주파수 하한'보다 커야 하고 CESAR 사양을 넘을 수 없습니다."),
    _p("RFPULSE_DUTY_MIN", "rfpulse", "펄스 듀티 하한", "%", I, 1, 1, 99, src="lib/config.py:614 (고정값)",
        desc="레시피·파워 켜기 단계·공정 중 수정에서 넣을 수 있는 펄스 듀티의 하한입니다. '펄스 듀티 상한'보다 작아야 합니다."),
    _p("RFPULSE_DUTY_MAX", "rfpulse", "펄스 듀티 상한", "%", I, 99, 1, 99, src="lib/config.py:615 (고정값)",
        desc="레시피·파워 켜기 단계·공정 중 수정에서 넣을 수 있는 펄스 듀티의 상한입니다. '펄스 듀티 하한'보다 커야 합니다."),
    _p("RFPULSE_PULSE_MIN_EDGE_US", "rfpulse", "펄스 최소 ON/OFF 시간", "µs", F, 16.0, 0.0, 1000.0,
       src="lib/config.py:617", desc="펄스 한 주기의 ON 시간과 OFF 시간이 각각 넘어야 하는 최소 시간으로, 레시피·파워 켜기 단계·공정 중 수정에서 주파수와 "
                                     "듀티로 계산해 검사합니다. 0 이면 검사하지 않습니다.",
                                note="CESAR 1310 명령 96 규칙(ON = 듀티/주파수, OFF = (100 − 듀티)/주파수 ≥ 이 값)"),
    _p("RFPULSE_FORP_TOLERANCE_PERCENT", "rfpulse", "Forward 허용 오차", "%", F, 5.0, 0.5, 50.0, safety=H,
        desc="RF Pulse 출력 중 Forward와 목표의 차이가 목표의 이 비율 이상인 상태가 'Forward 이탈 지속 시간' "
             "이어지면 RF Pulse 를 끄고 공정을 중단합니다. 공정 중 목표를 바꾼 뒤 새 목표에 들어왔는지 볼 때도 이 비율을 "
             "씁니다."),
    _p("RFPULSE_FORP_SEC", "rfpulse", "Forward 이탈 지속 시간", "s", F, 15.0, 1.0, 120.0, safety=H,
       v1_key="RFPULSE_FORP_CONSECUTIVE_LIMIT", v1_period=5.0, v1_default=3, desc="RF Pulse 출력 중 Forward와 목표의 차이가 'Forward 허용 오차' 이상인 상태가 이 시간 이어지면 RF "
                                                                                  "Pulse 를 끄고 공정을 중단합니다.",
                                                                             note="v1: 3회 × RF Pulse 폴링 5 s"),
    _p("RFPULSE_REFP_LIMIT_WATTS", "rfpulse", "Reflected 상한", "W", F, 20.0, 0.5, 200.0, safety=H,
        desc="RF Pulse Reflected 파워가 이 값 이상인 상태가 'Reflected 초과 지속 시간' 이어지면 RF Pulse "
             "를 끄고 공정을 중단합니다. RF Pulse 를 쓰는 공정 내내 봅니다."),
    _p("RFPULSE_REFP_SEC", "rfpulse", "Reflected 초과 지속 시간", "s", F, 15.0, 1.0, 120.0, safety=H,
       v1_key="RFPULSE_REFP_CONSECUTIVE_LIMIT", v1_period=5.0, v1_default=3, desc="RF Pulse Reflected 파워가 'Reflected 상한' 이상인 상태가 이 시간 이어지면 RF Pulse 를 끄고 "
                                                                                  "공정을 중단합니다.",
                                                                             note="v1: 3회 × RF Pulse 폴링 5 s"),
    _p("RFPULSE_REFP_AUTO_OFF_SEC", "rfpulse", "CESAR Reflected 자동 차단 시간(명령 33)", "s", I, 20, 1, 255, safety=H,
       src="v2 (사용자 결정 2026-10-15)",
       desc="CESAR 장비 자체의 Reflected 자동 차단 시간으로, 연결할 때 장비에 쓰고 되읽어 확인합니다. PC 가 멈췄을 때를 "
            "위한 보조 차단이므로 'Reflected 초과 지속 시간'보다 길게 둡니다.",
       note="prod 연결 때 장비에 쓰고 되읽는다(명령 33 쓰기·152 되읽기, bench 는 읽기만). 소프트웨어 "
            "판정(RFPULSE_REFP_SEC)보다 길어야 한다(X24)"),
    _p("RFPULSE_VERIFY_PULSE_CONFIG", "rfpulse", "펄스 설정 되읽기 불일치 시 중단", "", B, True,
       desc="RF Pulse 를 시작하거나 공정 중 펄스 설정을 바꾼 뒤 장비에서 되읽은 주파수·듀티가 요청과 다르면 RF Pulse 를 "
            "끄고 공정을 중단합니다. 꺼 두면 경고만 남깁니다.",
       note="false 면 경고만. 시작 뒤 193·196 되읽기 불일치 시 RFP_CONFIG_MISMATCH(S35)"),
    _p("RFPULSE_RAW_LOG", "rfpulse", "RF Pulse 프레임 원문 기록", "", B, True, safety=L, apply=LIVE,
       desc="RF Pulse 장비와 주고받은 통신 프레임 원문을 남길지 정합니다. 원문은 노트북의 파일에만 씁니다.",
       note="v1 은 NAS 공정 로그에 썼다"),
    _p("STOP_RFPULSE_OFF_TIMEOUT_SEC", "stop", "RF Pulse OFF 대기 한도", "s", F, 30.0, 5.0, 120.0,
       src="controller/process_controller.py:1980",
        desc="정지 순서에서 RF Pulse 끄기 결과를 기다리는 한도입니다. 넘으면 기다리지 않고 다음 단계(가스 유량 끄기)로 "
             "넘어갑니다."),

    # ───────────── 이온 게이지·베이스 압력(ig) ─────────────
    _p("IG_GAUGE_A_NAME", "ig", "Gauge A 이름", "", S, "게이지 A", safety=L, apply=LIVE,
       pattern=r".{1,20}", src="lib/config.py:736",
        desc="Gauge A 의 표시 이름이지만 장비 프로그램과 웹 화면에서는 쓰지 않으므로, 바꿔도 달라지는 것이 없습니다.",
        note="v2 장비 프로그램과 웹 화면에서는 사용하지 않는다(v1 에서 게이지 A 를 화면·기록에 보이던 이름)"),
    _p("IG_GAUGE_B_NAME", "ig", "Gauge B 이름", "", S, "게이지 B", safety=L, apply=LIVE,
       pattern=r".{1,20}", src="lib/config.py:737",
        desc="Gauge B 의 표시 이름이지만 장비 프로그램과 웹 화면에서는 쓰지 않으므로, 바꿔도 달라지는 것이 없습니다.",
        note="v2 장비 프로그램과 웹 화면에서는 사용하지 않는다(v1 에서 게이지 B 를 화면·기록에 보이던 이름)"),
    _p("IG_FILAMENT_SWITCH_QUIET_SEC", "ig", "Filament 전환 뒤 무응답 유예", "s", F, 12.0, 6.0, 60.0,
       safety=L, src="lib/config.py:746",
       desc="Filament 를 켜거나 끈 뒤 Ion Gauge 가 잠시 응답하지 않는 것을 감안한 유예 시간입니다. 이 시간 동안의 "
            "무응답은 통신 실패로 세지 않습니다.",
       note="A 뒤 약 5.9 s·B 뒤 약 4.9 s 무응답(실측). 이 동안 품질 suppressed, 실패로 세지 않음"),
    _p("IG_FILAMENT_ON_TIMEOUT_SEC", "ig", "Filament ON 결과 기한", "s", F, 20.0, 10.0, 120.0,
       src="lib/config.py:749",
        desc="Filament 켜기를 보낸 뒤 Filament와 Emission이 모두 확인되기를 기다리는 기한입니다. 넘으면 켜기 실패로 "
             "보며, 공정의 Filament 켜기 단계에서 실패하면 공정을 중단합니다."),
    _p("IG_FILAMENT_OFF_TIMEOUT_SEC", "ig", "Filament OFF 확인 기한", "s", F, 20.0, 10.0, 120.0,
       src="controller/process_controller.py:39 (드라이버 기한 20 s)",
        desc="Filament 끄기를 보낸 뒤 꺼짐이 확인되기를 기다리는 기한입니다. 넘으면 끄기 실패로 기록하며, 공정은 경고만 남기고 "
             "계속합니다."),
    _p("IG_SETTLE_SEC", "ig", "Emission 뒤 판정 제외 시간", "s", F, 15.0, 0.0, 120.0, src="lib/config.py:753",
        desc="Filament Emission이 처음 확인된 뒤 이 시간 동안의 Ion Gauge 압력은 베이스 압력 도달 판정에 쓰지 "
             "않습니다. 0 이면 바로 판정합니다."),
    _p("IG_VALUE_MAX_AGE_SEC", "ig", "베이스 판정에 쓰는 값 나이 상한", "s", F, 2.0, 1.0, 10.0,
       src="controller/process_controller.py:36",
        desc="베이스 압력 판정에 쓸 값의 나이 상한이지만 지금은 쓰지 않으므로, 바꿔도 달라지는 것이 없습니다. 베이스 판정은 품질이 "
             "정상·늦음인 값만 씁니다.",
        note="v2 에서는 사용하지 않는다(v1 에서는 이 나이보다 오래된 값을 베이스 판정에 쓰지 않았음)"),
    _p("IG_BASE_STABLE_SEC", "ig", "베이스 도달 지속 시간", "s", F, 5.0, 1.0, 600.0, src="lib/config.py:758",
       v1_key="IG_BASE_STABLE_COUNT", v1_period=1.0, v1_default=5, desc="베이스 압력 대기에서 Ion Gauge 압력이 목표 이하인 상태가 이 시간 이어지면 도달로 봅니다.",
                                                                   note="v1: 5회 × 1 s"),
    _p("IG_BASE_WAIT_TIMEOUT_SEC", "ig", "베이스 압력 대기 한도", "s", I, 1800, 60, 14400,
       desc="베이스 압력 대기 단계에서 이 시간 안에 Ion Gauge 압력이 목표 베이스 압력에 도달하지 못하면 공정을 중단합니다."),
    _p("IG_RELIGHT_LOCKOUT_SEC", "ig", "Filament 끈(B) 뒤 켜기(A) 금지 시간", "s", F, 5.0, 4.0, 60.0,
       src="device/IonGauge.py:377 (relight_lockout_sec)", desc="Filament 를 끈 뒤 다시 켜기까지 기다리는 최소 시간으로, 이 시간 안에 들어온 켜기는 시간이 지날 때까지 늦춰 "
                                                                "보냅니다. 장비 매뉴얼상 끈 뒤 4 초 안에는 다시 켤 수 없어 1 초 여유를 더한 값입니다.",
                                                           note="B(끄기) 뒤 이 시간 안의 A(켜기)는 그만큼 늦춘다"),
    _p("IG_MAX_RELIGHTS", "ig", "대기 중 Filament 재점등 횟수", "회", I, 2, 0, 10,
       src="controller/process_controller.py:37",
        desc="베이스 압력 대기 중 Filament가 꺼지거나 다시 켜기에 실패하면 이 횟수까지 다시 켭니다. 횟수를 다 쓰면 공정을 "
             "중단하고, 0 이면 다시 켜지 않습니다."),
    _p("IG_BASE_LOG_INTERVAL_SEC", "ig", "베이스 대기 기록 간격", "s", I, 5, 1, 600, safety=L, apply=LIVE,
       src="lib/config.py:766",
        desc="베이스 압력 대기 진행 기록을 남기는 간격이지만 지금은 쓰지 않으므로, 바꿔도 달라지는 것이 없습니다.",
        note="v2 에서는 사용하지 않는다(v1 에서 베이스 대기 진행 기록을 남기던 간격)"),
    _p("BASE_PRESSURE_MIN_TORR", "ig", "베이스 압력 입력 하한", "Torr", F, 1e-9, 1e-12, 1e-3,
       src="core/params.py:21",
        desc="레시피와 베이스 압력 대기 단계에 넣을 수 있는 목표 베이스 압력의 하한입니다. '베이스 압력 입력 상한'보다 작아야 합니다."),
    _p("BASE_PRESSURE_MAX_TORR", "ig", "베이스 압력 입력 상한", "Torr", F, 1e-3, 1e-9, 1e-2,
       src="core/params.py:22",
        desc="레시피와 베이스 압력 대기 단계에 넣을 수 있는 목표 베이스 압력의 상한입니다. '베이스 압력 입력 하한'보다 커야 합니다."),

    # ───────────── 히터 기본(heater) ─────────────
    _p("HEATER_WD_PERIOD_MS", "heater", "히터 워치독 하트비트 주기", "ms", I, 3000, 500, 3000, safety=H,
       desc="PC 가 PLC 에 하트비트를 쓰는 주기의 상한입니다. PLC 래더는 하트비트가 10 초 동안 바뀌지 않으면 히터를 끄므로 그 "
            "1/3 이하로 두며, 실제로는 1 초마다 쓰므로 이 값은 주기 상한 검사에만 씁니다.",
       note="v1 device/PLC.py:1110-1163 (3 s 주기); 래더는 하트비트(D00014)가 TON 10 s 동안 바뀌지 "
            "않으면 히터를 끈다. v1 은 이 주기(3 s)로 썼고 v2 는 매 tick(1 s)마다 쓰므로 이 값은 주기 상한 검사에만 "
            "쓴다(v2 방식 유지, 사용자 2026-10-20, X06)"),
    _p("HEATER_WD_DELAY_WARN_SEC", "heater", "하트비트 지연 경고", "s", F, 5.0, 1.0, 9.0, safety=L,
       src="device/PLC.py:119",
        desc="PC 하트비트 쓰기가 마지막으로 성공한 뒤 이 시간이 지나도록 다음 성공이 없으면 경고를 한 번 남기고, 회복되면 회복을 "
             "알립니다. 래더는 하트비트가 10 초 동안 바뀌지 않으면 히터를 끄므로 그보다 짧게 둡니다."),
    _p("HEATER_MAX_TEMP", "heater", "목표 온도 입력 상한", "°C", F, 700.0, 0.0, 1100.0, safety=H,
       v1_default=500.0, desc="공정 레시피·히터 레시피·히터 조작에서 넣을 수 있는 목표 온도의 상한입니다. PLC 래더의 목표 온도 상한 설정보다 크게 둘 "
                              "수 없습니다.",
                         note="HEATER_SV_LIMIT_C(D00013 목표 상한) 이하(X04)"),
    _p("HEATER_SOAK_TOLERANCE", "heater", "도달 판정 허용 편차", "°C", F, 2.0, 0.5, 20.0, v1_default=3.0,
        desc="공정의 히터 도달 판정에서 온도가 목표 ± 이 값 안에 들어와야 도달로 봅니다. TC2 단독 제어에서는 TC2 와 TC2 "
             "목표의 차이로, 그 밖의 제어 구조에서는 TC1 과 목표의 차이로 판정합니다."),
    _p("HEATER_SOAK_TIME_SEC", "heater", "공정 레시피 도달 후 유지", "s", I, 60, 0, 3600,
        desc="공정의 히터 도달 판정에서 '도달 판정 허용 편차' 안에 이 시간 동안 연속으로 머물러야 도달로 봅니다. 벗어나면 처음부터 "
             "다시 세며, 히터 레시피의 유지 시간과는 별개입니다.",
        note="히터 레시피의 SOAK 시간과 별개"),
    # ── 히터 제어 구조(5-15 — v1 f188f28·b0a945a·3328872 lib/config.py, controller/heater_cascade) ──
    _p("HEATER_CTRL_MODE", "heater", "히터 제어 구조", "", E, "tc2", enum=("tc2", "cascade", "tc1"), safety=H,
       src="lib/config.py(6e0685b)",
       desc="히터를 어떤 구조로 제어할지 고릅니다. tc2 는 TC2 단독 제어(기본), cascade 는 TC1 로 TC2 목표를 "
            "보정하는 2단 제어, tc1 은 TC1 직접 제어에 유지 모드를 더한 되돌림용이며, 목표 온도 숫자는 세 모드 모두 TC1 "
            "기준입니다.",
       note="tc2 = TC2 단독 제어(기본, 2026-11-02 사용자 결정): PLC PID 는 RUN 내내 TC2, SV2 = "
            "매핑(TC1 기준값 r1) 뿐. TC1 은 기록·안내만(바깥 루프·학습 보정·TC1 탈락 경보·셔터 전 정착 없음) / "
            "cascade = 2단 제어(PID 는 TC2, 파이썬이 SV2 = 매핑(r1) + 보정. 학습 ff·적분 보정·TC1 탈락 "
            "경보·셔터 전 정착) / tc1 = 예전 TC1 직접 제어 + 유지 모드(롤백용). tc2·cascade 는 유지 "
            "모드(HEATER_HOLD_*)를 쓰지 않는다. 목표 온도 숫자는 세 모드 모두 TC1 기준"),
    _p("HEATER_TC2_MAP", "heater", "TC1→TC2 기본 매핑", "°C", S,
       "25:25,50:92,100:170,150:226,200:290,250:365,300:437,400:569,500:705,600:846,700:982",
       pattern=r"\s*-?\d+(\.\d+)?\s*:\s*-?\d+(\.\d+)?\s*(,\s*-?\d+(\.\d+)?\s*:\s*-?\d+(\.\d+)?\s*)+",
       safety=H, src="lib/config.py(6e0685b)",
       desc="TC1 기준값을 TC2 목표로 바꾸는 표로, 'TC1:TC2' 쌍을 쉼표로 이어 적습니다. 점 사이는 직선으로 잇고 범위 밖은 "
            "끝 구간 기울기로 늘려 쓰며, 히터 제어 구조가 tc2·cascade 일 때 씁니다.",
       note="v1 형식 [[TC1, TC2], ...]. 2026-10-28~29 캐스케이드 3런의 평형 관계(300 °C = 수동 유지 "
            "실측 437)이다. 600 은 10-28~11-02 2단 제어 런 정상 상태 실측 중앙값 846 이고, 700 은 실측이 없어 "
            "250~600 실측 직선(기울기 1.36)으로 외삽했다(v1 6e0685b)"),
    _p("HEATER_TC2_TC1_WARN_C", "heater", "TC2 단독: TC1 편차 안내 기준", "°C", F, 15.0, 3.0, 100.0, safety=L,
       src="lib/config.py(6e0685b)",
       desc="TC2 단독 제어에서 TC2 가 목표에 자리 잡았는데도 TC1 과 목표의 차이가 이 값을 넘은 채 'TC2 단독: TC1 편차 "
            "안내 지속' 시간 이어지면 경고를 한 번 남깁니다. 기록용 안내이므로 제어와 공정에는 영향이 없습니다.",
       note="tc2 모드 기록용 안내(제어에 영향 없음): 셔터 닫힘·r1 도달·|TC2 − SV2| ≤ "
            "HEATER_SOAK_TOLERANCE 에서 |TC1 − 목표| 가 이 값을 넘은 채 "
            "HEATER_TC2_TC1_WARN_SEC 이상이면 경고·기록 1회(HEATER_TC2_TC1_DEVIATION). 공정은 "
            "계속"),
    _p("HEATER_TC2_TC1_WARN_SEC", "heater", "TC2 단독: TC1 편차 안내 지속", "s", F, 120.0, 30.0, 1800.0, safety=L,
       src="lib/config.py(6e0685b)",
        desc="TC2 단독 제어에서 셔터가 닫혀 있고 기준값이 목표에 닿았으며 TC2 가 TC2 목표 ± '도달 판정 허용 편차' 안일 때, "
             "TC1 편차가 'TC2 단독: TC1 편차 안내 기준'을 넘은 채 이 시간 이상 이어지면 경고를 한 번 남깁니다. 제어와 "
             "공정에는 영향이 없습니다."),
    _p("HEATER_TC2_HOLDBACK_C", "heater", "TC2 단독: 홀드백 폭(TC1 눈금)", "°C", F, 20.0, 2.0, 100.0,
       src="lib/config.py(6e0685b)",
       desc="TC2 단독 제어에서 TC2 가 TC2 목표보다 이 폭(TC1 눈금을 매핑 기울기로 TC2 눈금으로 바꾼 값) 넘게 처지면 "
            "기준값 올리기를 멈춥니다. PLC 래더 홀드백과는 별개이며, 너무 작으면 승온 중 자주 멈춰 승온 대기 한도를 넘을 수 "
            "있습니다.",
       note="tc2 모드에서 (SV2 − TC2) 가 매핑 기울기 × 이 값보다 크면 r1(목표 승온)을 세운다. 래더 "
            "홀드백(cascade 의 TC1 홀드백)과는 별개이다. 10 이면 램프 중 TC2 추종 지연(14~27 °C)에 자주 걸려 "
            "600 °C 도달이 승온 한도를 넘었다(v1 시뮬레이터, HEATER_HOLDBACK_C·D00021)"),
    _p("HEATER_CASCADE_TI_SEC", "heater", "바깥 루프 적분 시간", "s", F, 600.0, 60.0, 7200.0, src="lib/config.py(f188f28)",
        desc="2단 제어(cascade)에서 TC1 오차를 TC2 목표 보정으로 쌓아 가는 적분 시간입니다. 클수록 보정이 천천히 따라가며, "
             "공정 승온 대기 한도에 이 값의 2배가 더해집니다."),
    _p("HEATER_CASCADE_LAG_SEC", "heater", "TC1 비교 기준 지연", "s", F, 90.0, 0.0, 600.0, src="lib/config.py(f188f28)",
       desc="2단 제어(cascade)에서 TC1 오차를 계산할 때, 열이 TC1 까지 전해지는 지연을 감안해 지금의 TC1(30 초 "
            "평균)을 이 시간 전의 기준값과 비교합니다.",
       note="e = r1(t−LAG) − TC1(EMA 30 s)"),
    _p("HEATER_CASCADE_BIAS_RATE_MAX", "heater", "보정 최대 변화율", "°C/min", F, 3.0, 0.1, 60.0,
       src="lib/config.py(f188f28)", desc="2단 제어(cascade)에서 적분 보정이 1분 동안 바뀔 수 있는 최대 폭(TC2 눈금)입니다.",
                                     note="TC2 기준"),
    _p("HEATER_CASCADE_BIAS_LIMIT_C", "heater", "보정 크기 상한", "°C", F, 150.0, 10.0, 500.0, src="lib/config.py(f188f28)",
        desc="2단 제어(cascade)에서 적분 보정이 커질 수 있는 한도(±, TC2 눈금)입니다. 보정이 이 값에 닿으면 더 늘리지 "
             "않습니다."),
    _p("HEATER_CASCADE_BIAS_ALARM_C", "heater", "TC1 탈락 의심 보정 크기", "°C", F, 100.0, 10.0, 500.0,
       src="lib/config.py(f188f28)", desc="2단 제어(cascade)에서 보정 크기가 이 값 이상인 채 60 초 이어지면 TC1 탈락을 의심해 경보를 냅니다. 그 뒤의 "
                                          "조치는 'TC1 탈락 의심 시'를 따릅니다.",
                                     note="|보정| 이 이 값 이상으로 60 s 지속되면 TC1 탈락 의심 경보(bias = 학습 ff + 적분 b_int, "
                                          "ALARM_HOLD_SEC 60 s, HEATER_TC1_DETACHED)"),
    _p("HEATER_CASCADE_ALARM_ACTION", "heater", "TC1 탈락 의심 시", "", E, "abort", enum=("abort", "warn"), safety=H,
       src="lib/config.py(f188f28)", desc="2단 제어(cascade)에서 TC1 탈락이 의심될 때의 조치입니다. abort 이면 공정이 히터를 쓰는 중에는 공정을, 히터 "
                                          "레시피 중에는 레시피를 중단하고 수동 운전이면 경고만 남기며, warn 이면 언제나 경고만 남깁니다.",
                                     note="abort = 공정 소유면 공정 중단·히터 레시피면 레시피 중단·수동이면 경고 / warn = 경고만"),
    _p("HEATER_SHUTTER_SETTLE_TOL_C", "heater", "셔터 전 정착 허용 폭", "°C", F, 2.0, 0.5, 10.0, src="lib/config.py(3328872)",
       desc="Main 셔터를 열기 직전 TC1 이 목표 ± 이 값 안에 '셔터 전 정착 연속 시간' 동안 머물면 셔터를 엽니다. 히터 제어 "
            "구조가 cascade(2단 제어)일 때만 쓰고 tc2·tc1 에서는 쓰지 않습니다.",
       note="HEATER_SHUTTER_SETTLE_SEC, HEATER_CTRL_MODE=cascade"),
    _p("HEATER_SHUTTER_SETTLE_SEC", "heater", "셔터 전 정착 연속 시간", "s", F, 60.0, 10.0, 600.0,
       src="lib/config.py(3328872)", desc="Main 셔터를 열기 전 TC1 이 목표 ± '셔터 전 정착 허용 폭' 안에 연속으로 머물러야 하는 시간입니다. 승온 대기를 "
                                          "통과한 뒤부터 세므로, 이미 이 시간을 채웠으면 바로 통과합니다."),
    _p("HEATER_SHUTTER_SETTLE_MAX_SEC", "heater", "셔터 전 정착 대기 상한", "s", F, 900.0, 0.0, 3600.0,
       src="lib/config.py(3328872)", desc="셔터 전 히터 정착을 기다리는 최대 시간으로, 넘으면 '셔터 전 정착 상한 초과 시'에 따라 처리합니다. 0 이면 기본 "
                                          "템플릿에 이 단계를 넣지 않습니다."),
    _p("HEATER_SHUTTER_SETTLE_FAIL_ACTION", "heater", "셔터 전 정착 상한 초과 시", "", E, "proceed",
       enum=("proceed", "abort"), safety=H, src="lib/config.py(3328872)",
       desc="셔터 전 히터 정착이 '셔터 전 정착 대기 상한' 안에 끝나지 않았을 때의 처리입니다. proceed 이면 경고를 남기고 "
            "셔터를 열어 진행하고, abort 이면 알린 뒤 공정을 중단합니다."),
    _p("HEATER_WAIT_TIMEOUT_SEC", "heater", "승온 대기 최소 한도", "s", I, 5400, 600, 43200, v1_default=3600,
       desc="승온 대기 한도의 최소값입니다. 실제 한도는 이 값과 예상 승온 시간 + '승온 대기 여유' 중 큰 값이며, '승온 대기 절대 "
            "상한'을 넘지 않습니다.",
       note="실제 한도 = min(max(이 값, 예상 승온 + MARGIN), MAX)"),
    _p("HEATER_WAIT_TIMEOUT_MARGIN_SEC", "heater", "승온 대기 여유", "s", F, 1800.0, 0.0, 14400.0,
       src="controller/process_controller.py:26, heater_recipe.py:54 (중복 정의)",
        desc="승온 대기 한도를 정할 때 예상 승온 시간에 더하는 여유입니다. 한도는 '승온 대기 최소 한도'와 예상 승온 + 이 값 중 큰 "
             "값(절대 상한 이하)이며, 공정 승온 대기와 히터 레시피 단계에 씁니다."),
    _p("HEATER_WAIT_TIMEOUT_MAX_SEC", "heater", "승온 대기 절대 상한", "s", F, 43200.0, 600.0, 86400.0,
       safety=H, src="controller/process_controller.py:27, heater_recipe.py:55 (중복 정의)",
        desc="예상 승온 시간으로 계산한 승온 대기 한도의 절대 상한입니다. 한도를 넘으면 공정은 승온 대기에서 중단하고, 히터 레시피는 그 "
             "단계에서 레시피를 중단합니다.",
        note="중단 사유 HEATER_WAIT_TIMEOUT"),
    _p("HEATER_RAMP_DONE_TOL_C", "heater", "승온 완료 판정 폭", "°C", F, 0.15, 0.05, 5.0, safety=L,
       src="controller/process_controller.py:43, heater_recipe.py:59",
        desc="승온이 끝났는지 볼 때 쓰는 폭입니다. TC1 직접 제어에서는 래더 승온 중간 목표가, 2단·TC2 단독 제어에서는 기준값의 "
             "최종 목표가 목표와 이 값 이하로 차이 나야 승온 완료로 보며, 공정 도달 판정과 히터 레시피에서 승온을 마치고 유지로 "
             "넘어가는 판정에 씁니다.",
        note="히터 레시피의 RAMP → SOAK 판정"),
    _p("HEATER_TC_BAD_LIMIT_SEC", "heater", "TC 이상 지속 시 레시피 중단", "s", F, 60.0, 5.0, 600.0,
       safety=H, src="controller/heater_recipe.py:47",
        desc="히터 레시피 중 TC 이상(TC 이상 비트 또는 TC1 값 없음)이 이 시간 이어지면 레시피를 중단하고 히터를 끕니다. 공정이 "
             "같이 돌고 있으면 공정도 중단합니다."),
    _p("HEATER_OUT_DEAD_LIMIT_SEC", "heater", "출력 죽음 지속 시 레시피 중단", "s", F, 10.0, 3.0, 600.0,
       safety=H, src="controller/heater_recipe.py:50",
        desc="히터 레시피 중 히터가 켜져 있고 이상이 없는데 출력(DAC)이 '출력 0% DAC'보다 낮은 상태가 이 시간 이어지면 "
             "레시피를 중단하고 히터를 끕니다. 공정이 같이 돌고 있으면 공정도 중단합니다."),
    _p("HEATER_OUT_DEAD_GRACE_SEC", "heater", "출력 죽음 판정 유예", "s", F, 5.0, 0.0, 60.0,
       src="device/PLC.py:49",
        desc="히터를 켠 직후 이 시간 동안은 출력 죽음 경고 판정을 하지 않습니다. 히터 레시피의 출력 죽음 중단 판정에는 이 유예를 두지 "
             "않습니다."),
    _p("HEATER_OUT_DEAD_SEC", "heater", "출력 죽음 지속 시간", "s", F, 0.6, 0.25, 60.0, src="device/PLC.py:1040",
       v1_period=0.2, v1_default=3, desc="히터가 켜져 있고 이상이 없는데 출력(DAC)이 '출력 0% DAC'보다 낮은 상태가 이 시간 이어지면 출력 죽음 경고를 "
                                         "남깁니다. 히터를 켠 직후 '출력 죽음 판정 유예' 동안은 판정하지 않습니다.",
                                    note="v1: 3회 × PLC 폴링 200 ms(유예 HEATER_OUT_DEAD_GRACE_SEC 뒤)"),
    _p("HEATER_RUN_OFF_LIMIT_SEC", "heater", "외부 RUN OFF 지속 시 레시피 중단", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_recipe.py:63",
        desc="히터 레시피 중 히터 운전(RUN)이 꺼진 상태가 이 시간 이어지면 외부에서 꺼진 것으로 보고 레시피를 중단합니다. 공정이 "
             "같이 돌고 있으면 공정도 중단합니다."),
    _p("HEATER_GAS_HOLD_RELEASE_C", "heater", "냉각 보호 가스 해제 온도", "°C", F, 100.0, 30.0, 400.0,
       safety=H, desc="히터를 끌 때 히터 온도(PV)가 이 값보다 높으면 가스와 압력을 그대로 유지하고, 이 값 이하로 식으면 해제합니다. 온도를 "
                      "알 수 없으면 해제하지 않습니다."),
    _p("HEATER_STALE_SEC", "heater", "히터 값 stale 표시", "s", F, 5.0, 1.0, 60.0, safety=L, apply=LIVE,
       desc="PLC 값이 이 시간 넘게 들어오지 않으면 화면의 히터 값을 회색으로 바꾸고 히터 버튼을 잠급니다."),
    _p("HEATER_COOLDOWN_TARGET_C", "heater", "냉각 단계 기준", "°C", F, 30.0, 0.0, 100.0, safety=L,
       desc="히터 레시피의 마지막 단계 목표가 이 값 이하이면 그 단계를 냉각 단계로 보고, 도달 판정 없이 유지 시간만큼 기다립니다.",
       note="마지막 스텝 목표가 이 값 이하면 냉각 스텝(COOL, 램프 없이 soak 만큼 대기)"),
    _p("HEATER_CURRENT_MV_ZERO", "heater", "전류 추정: 출력 0 DAC", "count", I, 400, 0, 4000, safety=L,
       apply=LIVE, desc="화면에 보이는 히터 추정 전류를 계산할 때 전류가 0 이 되는 DAC 값입니다. 표시에만 쓰며 제어에는 영향이 없습니다."),
    _p("HEATER_CURRENT_MV_FULL", "heater", "전류 추정: 전출력 DAC", "count", I, 1600, 0, 4000, safety=L,
       apply=LIVE, desc="화면에 보이는 히터 추정 전류를 계산할 때 '전류 추정: 전출력 전류'가 되는 DAC 값입니다. 표시에만 쓰며 제어에는 영향이 "
                        "없습니다."),
    _p("HEATER_CURRENT_SCALE", "heater", "전류 추정: 전출력 전류", "A", F, 135.1, 1.0, 500.0, safety=L,
       apply=LIVE, desc="화면에 보이는 히터 추정 전류를 계산할 때 DAC 가 '전류 추정: 전출력 DAC'일 때의 전류입니다. 표시에만 쓰며 제어에는 "
                        "영향이 없습니다."),

    # ───────────── 히터: PLC 로 쓰는 한계값(heater_plc) — 적용 시 쓰고 되읽어 확인 ─────────────
    _p("HEATER_MV_MIN", "heater_plc", "출력 0% DAC", "count", I, 400, 320, 800, safety=H,
       desc="히터 출력 0% 에 해당하는 DAC 값입니다. PLC 래더에 정해 둔 값과 같아야 합니다.",
       note="PLC K1227 과 같아야 함(VSCD-30 0.8 V = 320)"),
    _p("HEATER_MV_ABS_MAX", "heater_plc", "DAC 절대 상한", "count", I, 1300, 420, dev.HEATER_DAC_PHYS_MAX,
       safety=H, desc="프로그램이 히터 출력(DAC)으로 넘지 않는 절대 상한입니다. 히터 출력 운전 상한은 이 값 이하로만 둘 수 있으며, 물리적 "
                      "최대는 1600(4 V)입니다.",
                 note="코드가 넘지 않는 상한. HEATER_MV_LIMIT ≤ 이 값(X01)"),
    _p("HEATER_MV_LIMIT", "heater_plc", "D00018 운전 상한", "count", I, 1200, 420, dev.HEATER_DAC_PHYS_MAX,
       safety=H, desc="PLC 래더가 쓰는 히터 출력(DAC) 운전 상한으로, 연결·재기동·설정 적용 때 PLC 에 쓰고 되읽어 확인합니다. '출력 "
                      "0% DAC'에 20 을 더한 값 이상, 'DAC 절대 상한' 이하로 둡니다.",
                 note="D00018. 범위 MV_MIN+20 ~ MV_ABS_MAX(X01)"),
    _p("HEATER_SV_LIMIT_C", "heater_plc", "D00013 목표 상한", "°C", F, 700.0, 0.0, 1100.0, safety=H,
       v1_default=500.0,
        desc="PLC 래더의 목표 온도 상한으로, 연결·재기동·설정 적용 때 PLC 에 쓰고 되읽어 확인합니다. 히터 목표 명령은 PLC 가 "
             "보고한 이 상한과 '목표 온도 입력 상한' 중 작은 값을 넘으면 거부됩니다.",
        note="PLC 주소 D00013."),
    _p("HEATER_RAMP_RATE_C_PER_MIN", "heater_plc", "D00020 승온 속도·기본 승온", "°C/min", F, 6.0, 6.0, 60.0,
       safety=H, v1_default=12.0,
       desc="PLC 래더의 승온 속도이며, 승온 속도를 비운 히터 레시피·공정 단계와 수동 목표 변경 때 쓰는 기본 승온 속도입니다. 래더 "
            "분해능은 6 °C/min(1 카운트/초)이고, 기본 6 은 장비와 맞춘 값입니다.",
       note="PLC 주소 D00020. 기본 6 출처: v1 bb2b0a3, 캐스케이드 실기 시험 조건."),
    _p("HEATER_HOLDBACK_C", "heater_plc", "D00021 홀드백", "°C", F, 10.0, 0.5, 50.0, safety=H, v1_default=2.0,
        desc="PLC 래더의 홀드백 값으로, 연결·재기동·설정 적용 때 PLC 에 쓰고 되읽어 확인합니다. 2단 제어에서는 TC1 이 지연을 "
             "감안한 기준값보다 이 값 이상 처지면 기준값 상승을 멈춥니다.",
        note="PLC 주소 D00021. 2단 제어 = cascade."),
    _p("HEATER_OT_LIMIT_C", "heater_plc", "D00026 TC1 과온 트립", "°C", F, 750.0, 50.0, 1130.0, safety=H,
       v1_default=550.0, desc="PLC 래더가 TC1 과온 트립을 거는 온도입니다. PLC 목표 상한보다 20 °C 이상 높게(권장 50 °C) 두어야 하며, "
                              "트립은 래치되므로 풀려면 이상 리셋을 해야 합니다.",
                         note="PLC 주소 D00026. 원래 설명: SV_LIMIT+20 이상으로 둔다(권장 +50) — 교차 검사 "
                              "X02(HEATER_OT_LIMIT_C ≥ HEATER_SV_LIMIT_C + 20)."),
    _p("HEATER_OT2_LIMIT_C", "heater_plc", "D00034 TC2 과온 트립", "°C", F, 1150.0, 70.0,
       dev.HEATER_OT2_LADDER_MAX_C, safety=H, desc="PLC 래더가 TC2 과온 트립을 거는 온도입니다. TC1 과온 트립 온도보다 20 °C 이상 높게 두어야 하며, 래더가 받는 "
                                                   "상한은 1150 °C 입니다.",
                                              note="PLC 주소 D00034(래더 상한 11500). 원래 설명: OT+20 이상, 래더 상한 1150 — 교차 검사 "
                                                   "X03(HEATER_OT2_LIMIT_C ≥ HEATER_OT_LIMIT_C + 20)."),
    _p("HEATER_SLOW_ZONE_C", "heater_plc", "D00027 감속 구간", "°C", F, 10.0, 10.0, 10.0, safety=L,
       note="래더가 10.0 으로 강제 — 되읽기 일치용 / PLC 주소 D00027.",
        desc="PLC 래더의 감속 구간에 쓰는 값입니다. 래더가 10.0 °C 로 고정하므로 되읽기 확인이 맞도록 10 으로만 둘 수 "
             "있습니다."),
    _p("HEATER_SLOW_RATE_C_PER_MIN", "heater_plc", "D00028 감속 속도", "°C/min", F, 60.0, 6.0, 600.0,
       v1_default=6.0, desc="PLC 래더가 감속 구간에서 쓰는 승온 속도로, 기본 60 °C/min 으로 넉넉히 열어 두어 래더가 속도를 막지 않게 "
                            "합니다. 실제 목표 직전 감속은 프로그램의 '목표 직전 감속 구간'·'도착 속도'가 정합니다.",
                       note="PLC 주소 D00028. 실제 승온은 파이썬 감속 "
                            "접근(HEATER_APPROACH_ZONE_C·HEATER_APPROACH_MIN_RATE_C_PER_MIN)이 정한다."),

    # ───────────── 히터 감속 접근(heater_ramp) ─────────────
    _p("HEATER_APPROACH_ZONE_C", "heater_ramp", "목표 직전 감속 구간", "°C", F, 20.0, 0.0, 200.0,
        desc="목표까지 남은 온도가 이 값보다 작아지면 승온 속도를 남은 거리에 비례해 줄여, 목표에 닿을 때 '도착 속도'가 되게 합니다. "
             "0 이면 감속하지 않고, 클수록 승온 시간이 길어집니다."),
    _p("HEATER_APPROACH_MIN_RATE_C_PER_MIN", "heater_ramp", "도착 속도", "°C/min", F, 1.0, 0.1, 6.0,
        desc="'목표 직전 감속 구간' 끝(목표에 닿는 순간)의 승온 속도입니다. 작을수록 목표 직전에 더 천천히 다가가 승온 시간이 "
             "길어집니다."),
    _p("HEATER_APPROACH_LEAD_C", "heater_ramp", "SV 앞서기 폭", "°C", F, 1.0, 0.2, 10.0,
        desc="TC1 직접 제어에서 프로그램이 PLC 에 쓰는 목표가 래더 승온 중간 목표보다 이 값 넘게 앞서지 않게 합니다.",
        note="PLC 주소 D00012(목표 온도)·D00019(래더 승온 중간 목표)."),

    # ───────────── 히터 유지 모드·포화(heater_hold) ─────────────
    _p("HEATER_HOLD_MODE", "heater_hold", "도달 후 유지 모드", "", E, "tc2", enum=("off", "dac", "tc2"),
       safety=H, v1_default="off", desc="히터가 목표에 도달한 뒤 쓰는 유지 모드를 정하며, '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다. dac 는 출력 "
                                        "상한을 그때 출력으로 고정하고, tc2 는 TC2 추종 제어로 바꾸며, off 는 유지 모드를 쓰지 않습니다.",
                                   note="원래 설명: dac=D00018 을 그때 출력으로 고정 / tc2=TC2 추종 제어. 2단(cascade)·TC2 "
                                        "단독(tc2) 제어 구조에서는 실제 모드가 off(PLC 에 쓰지 않음)."),
    _p("HEATER_HOLD_MV_ENTER_TOL_C", "heater_hold", "유지 진입 허용 |PV-SV|", "°C", F, 3.0, 0.5, 20.0,
        desc="유지 모드 진입을 재는 동안 TC1 과 목표의 차이가 이 값 이하여야 표본을 모으며, 벗어나면 모은 표본을 버리고 다시 "
             "잽니다. '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_MV_ENTER_SEC", "heater_hold", "유지 진입 안정 시간", "s", F, 60.0, 10.0, 600.0,
        desc="'유지 진입 허용 |PV-SV|' 범위 안의 표본을 이 시간 동안 모은 뒤 출력·온도 변화가 작으면, 그 평균 출력(DAC "
             "고정) 또는 평균 TC2(TC2 추종)로 유지 모드에 들어갑니다. '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_MV_ARRIVE_TOL_C", "heater_hold", "한 번은 들어와야 하는 범위", "°C", F, 1.0, 0.2, 10.0,
        desc="승온이 끝난 뒤 TC1 이 목표 ± 이 값 안에 한 번은 들어와야 유지 모드 측정을 시작하며, '유지 진입 허용 "
             "|PV-SV|'보다 클 수 없습니다. '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_MV_DRIFT_PV_C", "heater_hold", "PV 드리프트 하한", "°C", F, 0.5, 0.1, 10.0, safety=L,
        desc="유지 모드 측정 창의 앞 절반과 뒤 절반의 TC1 평균 차이가 허용값을 넘으면 아직 정착 전으로 보고 다시 잽니다. 허용값은 "
             "이 값과 '유지 진입 허용 |PV-SV|' × 'PV 드리프트 비율' 중 큰 값이며, TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_MV_DRIFT_MV", "heater_hold", "MV 드리프트 하한", "count", F, 15.0, 3.0, 200.0, safety=L,
        desc="유지 모드 측정 창의 앞 절반과 뒤 절반의 평균 출력(DAC) 차이가 허용값을 넘으면 아직 정착 전으로 보고 다시 잽니다. "
             "허용값은 이 값과 출력 운전 상한 × 'MV 드리프트 비율' 중 큰 값이며, TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_TC2_MARGIN_C", "heater_hold", "TC2 목표 마진(OT2 − 이 값)", "°C", F, 50.0, 20.0, 500.0,
       safety=H,
        desc="TC2 목표의 상한을 TC2 과온 트립 온도보다 이 값만큼 낮게 잡습니다. 2단·TC2 단독 제어와 TC2 추종 유지 모드 "
             "모두 TC2 목표를 이 상한 아래로 제한합니다."),
    _p("HEATER_HOLD_TC2_DRIFT_C", "heater_hold", "TC2 드리프트 하한", "°C", F, 1.0, 0.1, 20.0, safety=L,
        desc="TC2 추종 유지 모드 측정 창의 앞 절반과 뒤 절반의 TC2 평균 차이가 허용값을 넘으면 아직 정착 전으로 보고 다시 "
             "잽니다. 허용값은 이 값과 창의 TC2 평균 × 'TC2 드리프트 비율' 중 큰 값이며, TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_DRIFT_PV_REL", "heater_hold", "PV 드리프트 비율(×ENTER_TOL)", "비율", F, 0.5, 0.05, 1.0,
       safety=L, src="controller/heater_hold.py:38",
        desc="유지 모드의 TC1 드리프트 허용값을 '유지 진입 허용 |PV-SV|' × 이 비율과 'PV 드리프트 하한' 중 큰 값으로 "
             "정합니다. '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_DRIFT_MV_REL", "heater_hold", "MV 드리프트 비율(×MV_LIMIT)", "비율", F, 0.05, 0.005, 0.5,
       safety=L, src="controller/heater_hold.py:39",
        desc="유지 모드의 출력 드리프트 허용값을 출력 운전 상한 × 이 비율과 'MV 드리프트 하한' 중 큰 값으로 정합니다. '히터 제어 "
             "구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_DRIFT_TC2_REL", "heater_hold", "TC2 드리프트 비율", "비율", F, 0.02, 0.001, 0.5,
       safety=L, src="controller/heater_hold.py:40",
        desc="TC2 추종 유지 모드의 TC2 드리프트 허용값을 측정 창의 TC2 평균 × 이 비율과 'TC2 드리프트 하한' 중 큰 값으로 "
             "정합니다. '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_ENGAGE_TIMEOUT_SEC", "heater_hold", "유지 진입 단계 대기", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_hold.py:44", note="v1 주석: 래더 스캔 지연 미확인 — bench 실측",
        desc="TC2 추종 유지 모드에 들어갈 때 TC2 목표 되읽기와 래더의 TC2 제어 확인을 단계마다 기다리는 한도입니다. 넘으면 "
             "TC2 추종을 취소하고 TC1 제어로 되돌리며, TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_HOLD_REAPPLY_SEC", "heater_hold", "되돌아간 값 재적용 대기", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_hold.py:45",
        desc="유지 모드 중 PLC 에서 읽은 고정값(출력 상한 또는 TC2 목표)이 프로그램이 정한 값과 다르면 이 시간이 지난 뒤 PLC "
             "에 다시 씁니다. '히터 제어 구조'가 TC1 직접 제어일 때만 쓰는 설정입니다."),
    _p("HEATER_HOLD_WAIT_SEC", "heater_hold", "공정의 유지 진입 대기 한도", "s", F, 600.0, 0.0, 7200.0,
        desc="공정의 히터 도달·유지 대기에서 목표 도달 뒤 유지 모드(2단·TC2 단독 제어에서는 TC2 제어 진입)를 기다리는 "
             "한도입니다. 넘으면 TC1 직접 제어는 완화 TC2 추종과 DAC 고정을 차례로 시도하고, 그래도 실패하거나 2단·TC2 단독 "
             "제어이면 '유지 진입 최종 실패 시' 설정을 따릅니다."),
    _p("HEATER_HOLD_FAIL_ACTION", "heater_hold", "유지 진입 최종 실패 시", "", E, "abort",
       enum=("abort", "proceed"), safety=H, desc="공정에서 유지 모드 진입이 끝내 실패했을 때(TC1 직접 제어에서는 완화 TC2 추종과 DAC 고정까지 실패했을 때) 할 일을 "
                                                 "정합니다. abort 는 공정을 중단하고, proceed 는 경고만 남기고 공정을 이어 갑니다.",
                                            note="원래 설명: 완화 tc2 → dac 폴백을 먼저 시도한 뒤에도 실패하면. 사유 코드 HEATER_HOLD_FAILED."),
    _p("HEATER_TC1_DEV_WARN_C", "heater_hold", "TC1 편차 경고 기준", "°C", F, 120.0, 10.0, 500.0, safety=L,
       src="controller/heater_hold.py:49",
        desc="TC2 추종 유지 중 TC1 과 목표의 차이가 이 값을 넘은 채 'TC1 편차 경고 지속' 시간이 지나면 안내를 남기고 추종은 "
             "그대로 유지합니다. '히터 제어 구조'가 TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_TC1_DEV_WARN_SEC", "heater_hold", "TC1 편차 경고 지속", "s", F, 300.0, 10.0, 3600.0, safety=L,
       src="controller/heater_hold.py:50",
        desc="TC2 추종 유지 중 TC1 편차가 'TC1 편차 경고 기준'을 넘은 채 이 시간 이어지면 안내를 남깁니다. 추종은 그대로 "
             "유지하며, TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_TC1_DEV_OK_C", "heater_hold", "TC1 편차 정상 기준", "°C", F, 10.0, 1.0, 100.0, safety=L,
       src="controller/heater_hold.py:51",
        desc="TC1 편차 안내를 남긴 뒤 편차가 이 값 이하로 줄면 정상 복귀로 기록하고 다시 셀 수 있게 합니다. '히터 제어 구조'가 "
             "TC1 직접 제어일 때만 씁니다."),
    _p("HEATER_MV_SAT_REL", "heater_hold", "포화 판정 비율(×MV_LIMIT)", "비율", F, 0.98, 0.8, 1.0,
       src="controller/heater_hold.py:42, heater_saturation.py:29",
        desc="히터 출력(DAC)이 출력 상한의 이 비율 이상이면 포화로 보고, 그 상태가 'DAC 포화 판정 시간' 이어지면 출력 상한을 "
             "낮추고 경고합니다. 유지 모드 진입 때 평균 출력이 상한의 이 비율 이상이어도 DAC 고정을 하지 않습니다."),
    _p("HEATER_MV_SAT_SEC", "heater_hold", "DAC 포화 판정 시간", "s", F, 120.0, 10.0, 3600.0,
       desc="히터 출력(DAC)이 출력 상한 × '포화 판정 비율' 이상인 상태가 이 시간 이어지면 포화로 보고 출력 상한을 낮추고 "
            "경고합니다.",
       note="v1 은 10 미만이면 120 으로 되돌렸지만 v2 는 범위 밖 값을 거부한다."),
    _p("HEATER_SAT_PRE_WINDOW_SEC", "heater_hold", "포화 직전 평균 창", "s", F, 60.0, 10.0, 600.0,
       src="controller/heater_saturation.py:31",
        desc="DAC 포화로 출력 상한을 낮출 때, 포화가 시작되기 직전 이 시간 동안의 평균 출력을 새 상한으로 씁니다. 낮춘 상한은 운전 "
             "OFF·목표 변경 때 원래대로 돌아갑니다."),
    _p("HEATER_SAT_FALLBACK_REL", "heater_hold", "포화 직전 이력 없을 때 출력 상한 비율", "비율", F, 0.9, 0.5, 1.0,
       src="controller/heater_saturation.py:30",
        desc="DAC 포화로 상한을 낮출 때 포화 직전 출력 기록이 없으면, 출력 운전 상한 × 이 비율을 새 상한으로 씁니다."),

    # ───────────── 히터 레시피·히터 가스(heater_recipe / heater_gas) ─────────────
    _p("HEATER_RECIPE_MAX_STEPS", "heater_recipe", "히터 레시피 최대 단계", "개", I, 20, 1, 100, safety=L,
        desc="히터 레시피에 넣을 수 있는 단계 수의 상한입니다. 넘으면 레시피를 받지 않습니다."),
    _p("HEATER_RECIPE_MAX_REPEAT", "heater_recipe", "히터 레시피 반복 상한", "회", I, 1, 1, 100, src="controller/heater_recipe.py",
       note="v1 과 같음(사용자 확정 2026-10-20): repeat 빈칸·1 만 허용(반복 없음) / 반복 칸 = repeat.",
        desc="히터 레시피 단계의 반복 칸에 넣을 수 있는 상한입니다. 지금 프로그램은 단계를 되풀이하지 않으며, 이 값을 넘는 반복이 "
             "있으면 레시피를 받지 않습니다."),
    _p("HEATER_RECIPE_HOLD_AT_END", "heater_recipe", "레시피 끝 온도 유지", "", B, False, src="controller/heater_recipe.py",
       desc="히터 레시피가 끝난 뒤 마지막 목표 온도를 계속 유지할지 정합니다. 꺼 두면(기본) 끝날 때 히터를 끄고, 켜 두면 마지막 "
            "목표를 유지하므로 무인 가열에 주의해야 합니다.",
       note="기본 false 는 v1 과 같음(사용자 확정 2026-10-20)."),
    _p("HEATER_RECIPE_RAMP_MAX_C_PER_MIN", "heater_recipe", "레시피 승온 상한", "°C/min", F, 60.0, 6.0, 600.0,
       src="core/params.py:364 (공정 레시피 heater_ramp 60 초과 오류)",
       note="v1 히터 레시피에는 상한이 없다 — v2 는 공정 레시피와 같은 60 °C/min 을 히터 레시피에도(v2 방식 유지, "
            "2026-10-20)",
        desc="승온 속도로 넣을 수 있는 상한입니다. 히터 레시피 단계, 공정 레시피의 히터 승온 칸, 히터 승온 단계, 공정 중 승온 속도 "
             "수정에 모두 씁니다."),
    _p("HEATER_GAS_SP1_TIMEOUT_SEC", "heater_gas", "히터 가스 SP1 도달 한도", "s", F, 300.0, 30.0, 1800.0,
       src="controller/heater_atmosphere.py:55",
        desc="히터 가스를 준비할 때 챔버 압력이 정한 압력에 도달하기를 기다리는 한도입니다. 넘으면 가스 준비 실패로 보고 유량 "
             "끄기·MFC 밸브 열기·밸브 닫기를 하며 히터는 켜지 않습니다."),
    _p("HEATER_GAS_MFC_TIMEOUT_SEC", "heater_gas", "히터 가스 MFC 명령 한도", "s", F, 60.0, 5.0, 600.0,
       src="controller/heater_atmosphere.py:43",
        desc="히터 가스 준비 때 MFC 명령을 기다리는 한도로 두었던 설정이며, 지금은 이 값을 쓰지 않습니다. 히터 가스의 MFC 명령 "
             "한도는 프로그램에 정해 둔 60 s 를 씁니다.",
        note="v2 에서는 사용하지 않는다(v1 heater_atmosphere MFC_TIMEOUT_SEC → v2 "
             "process_steps.MFC_CMD_GUARD_S 60 s 가 대신)."),

    # ───────────── 공정 시퀀스(process) ─────────────
    _p("PROCESS_PAUSE_MAX_SEC", "process", "공정 일시정지 한도", "s", F, 1800.0, 0.0, 86400.0, src="v2",
       desc="공정 일시정지가 이 시간 이어지면 정지 순서를 실행합니다. 한도 5분 전에 경고하며, 0 이면 한도가 없습니다.",
       note="일시정지 명령 PROCESS_PAUSE, 5분 전 경고 PAUSE_LONG, 넘으면 사유 코드 "
            "PAUSE_TIMEOUT(5-15)."),
    _p("PRE_POWER_OFF_SETTLE_SEC", "process", "시작 전 전원 OFF 뒤 대기", "s", F, 1.0, 0.0, 30.0, safety=L,
       src="controller/process_controller.py:519",
        desc="시작 전 정리에서 DC·RF·RF Pulse 를 끈 뒤 다음 단계로 가기 전에 기다리는 시간입니다. 정수 초로 반올림하며, "
             "단계 값을 비우면 이 값을 씁니다."),
    _p("GAS_STABILIZE_SEC", "process", "SP4 ON 뒤 압력 안정화", "s", F, 60.0, 0.0, 600.0,
       src="controller/process_controller.py:577",
        desc="기본 템플릿에서 가스를 넣고 '가스 안정화 SP(SP4)'를 고른 뒤 압력이 자리 잡도록 기다리는 시간입니다(정수 초로 "
             "반올림). 이 동안 압력 도달 판정은 하지 않습니다."),
    _p("SP3_MTORR", "process", "압력 1단계(SP3)", "mTorr", F, 15.0, 1.0, 100.0,
       src="controller/process_controller.py:619",
       desc="기본 템플릿에서 파워를 켠 뒤 작업 압력(WP)으로 가기 전에 거치는 1단계 압력으로, 작업 압력이 이 값보다 낮을 때만 "
            "거칩니다. 'SP3 값 출처'가 프로그램 값일 때 장비에 쓰는 값이며, 장비 값이면 장비에서 읽은 값으로 비교합니다.",
       note="조건 WP < SP3_MTORR. 값 출처 SP3_SOURCE."),
    _p("SP2_MTORR", "process", "압력 2단계(SP2)", "mTorr", F, 5.0, 0.5, 100.0,
       src="controller/process_controller.py:623",
       desc="기본 템플릿에서 파워를 켠 뒤 작업 압력(WP)으로 가기 전에 거치는 2단계 압력으로, 작업 압력이 이 값보다 낮을 때만 "
            "거칩니다. 'SP2 값 출처'가 프로그램 값일 때 장비에 쓰는 값이며, 장비 값이면 장비에서 읽은 값으로 비교합니다.",
       note="조건 WP < SP2_MTORR. 값 출처 SP2_SOURCE."),
    _p("SP5_MTORR", "process", "압력 0단계(SP5)", "mTorr", F, 50.0, 1.0, 1000.0,
       src="controller/process_controller.py (a78b5ce — 고정값 50.0)",
       desc="기본 템플릿에서 작업 압력(WP)이 이 값보다 높을 때만 거치는 0단계 압력으로, 파워를 켠 뒤 이 압력을 고르고 'SP5 "
            "ON 뒤 고정 대기' 동안 기다린 다음 작업 압력으로 갑니다. 'SP5 값 출처'가 프로그램 값일 때 장비에 쓰는 값이며, "
            "장비 값이면 장비에서 읽은 값으로 비교합니다.",
       note="순서: SP4 → 파워 → SP5(이 값) 설정·선택 → SP5_STABILIZE_SEC 고정 대기 → SP1(WP). 조건 "
            "WP > SP5_MTORR. 값 출처 SP5_SOURCE."),
    _p("SP5_STABILIZE_SEC", "process", "SP5 ON 뒤 고정 대기", "s", F, 60.0, 0.0, 600.0,
       src="controller/process_controller.py (f5780ca — 고정값 60 s)",
       desc="0단계(SP5) 압력을 고른 뒤 압력 도달 판정 없이 기다리는 시간입니다."),
    # 압력 설정점 값 출처(5-19 ③) — 단계마다 프로그램 값(SPn_MTORR 를 장비에 쓰고 되읽음)을 쓸지, 장비에 저장된 값을 그대로 쓸지.
    # 기본값은 v1 과 같게: SP4 = 장비 값(v1 은 D4 만), SP2·SP3·SP5 = 프로그램 값(v1 이 값을 써서 확인). 장비 값이면 기본 템플릿이
    # 그 단계를 set=false·값 비움으로 만들고, 단계를 넣을지 비교(WP < SP3·WP < SP2·WP > SP5)도 장비에서 읽은 값으로 한다.
    *(_p(f"SP{n}_SOURCE", "process", f"SP{n} 값 출처", "", E, "device" if n == 4 else "program",
         enum=("program", "device"), enum_labels=(("program", "프로그램 값"), ("device", "장비 값")),
         src="controller/process_controller.py (v1 — SP4 는 D4 만, SP2·SP3·SP5 는 값을 써서 확인)" if n != 4 else
             "controller/process_controller.py (v1 — 가스 안정화 SP4 ON 은 D4 만)",
         desc=f"SP{n} 단계에 쓸 압력 값을 어디서 가져올지 정합니다. 프로그램 값은 '{_SP_LABEL[n]}' 값을 장비에 쓰고 되읽어 "
              f"확인한 뒤 고르고, 장비 값은 장비에 쓰지 않고 이미 저장된 SP{n} 를 골라 읽은 값을 목표와 비교에 쓰며, 읽지 "
              "못하면 시작을 거부합니다.",
         note="기본은 v1 과 같다" + ("(v1 은 SP4 를 쓰지 않음)" if n == 4 else "") + f"(SP{n}_MTORR).") for n in (2, 3, 4, 5)),
    _p("SP4_MTORR", "process", "가스 안정화 SP(SP4)", "mTorr", F, 30.0, 0.5, 1000.0,
       src="v2 (10/20 bench 장비 저장값 S4 3.00 %FS = 30 mTorr — v1 은 S4 를 쓰지 않음)",
       desc="기본 템플릿의 가스 안정화 단계에서 고르는 압력 SP 값입니다. 'SP4 값 출처'가 프로그램 값일 때 이 값을 장비에 쓰고 "
            "되읽은 뒤 고르며, 기본인 장비 값이면 장비에 저장된 SP4 를 그대로 고릅니다.",
       note="프로그램 값: S4 로 쓰고 R4 로 되읽은 뒤 D4 로 고른다. 기본은 장비 값(v1 과 같게 D4 만, "
            "SP4_SOURCE)."),
    _p("PRESSURE_WAIT_TOL_RATIO", "process", "압력 도달 허용 비율", "비율", F, 0.10, 0.01, 0.5,
       src="controller/process_controller.py:105",
        desc="압력 도달 판정에서 챔버 압력이 목표 ± 목표 × 이 비율 안에 '압력 도달 지속 시간' 동안 머물면 도달로 봅니다. 압력 "
             "단계의 허용 범위를 비우면 이 값을 씁니다."),
    _p("PRESSURE_WAIT_STABLE_SEC", "process", "압력 도달 지속 시간", "s", F, 6.0, 1.0, 120.0,
       src="controller/process_controller.py:106, device/MFC.py:864-900", v1_period=2.0, v1_default=3,
       desc="압력 도달 판정에서 챔버 압력이 '압력 도달 허용 비율'로 정한 범위 안에 이어서 머물러야 하는 시간입니다. 압력 단계의 지속 "
            "시간을 비우면 이 값을 씁니다.",
       note="v1: 3회 × (폴 간격 1 s + 명령 간격 1 s)."),
    _p("PRESSURE_STAGE_TIMEOUT_SEC", "process", "SP3·SP2 도달 한도(넘어도 진행)", "s", F, 180.0, 10.0, 1800.0,
       src="controller/process_controller.py:107", note="v1 은 틱 수로 세어 실제 2배 이상 걸렸다 — v2 는 실제 시각",
        desc="작업 압력(SP1)이 아닌 압력 단계가 압력 도달을 기다리는 기본 한도입니다. 넘으면 기본으로 경고를 남기고 다음으로 "
             "진행하며, 단계에 최대 대기·시간 초과 시 동작을 넣으면 그 값을 따릅니다."),
    _p("SP1_TIMEOUT_SEC", "process", "SP1(WP) 도달 한도(넘으면 중단)", "s", F, 300.0, 30.0, 3600.0,
       src="controller/process_controller.py:628",
        desc="작업 압력(SP1) 단계가 압력 도달을 기다리는 기본 한도입니다. 넘으면 기본으로 공정을 중단하며, 단계에 최대 대기·시간 "
             "초과 시 동작을 넣으면 그 값을 따릅니다."),
    _p("POWER_WAIT_TIMEOUT_SEC", "process", "파워 도달 대기 최소 한도", "s", F, 600.0, 60.0, 7200.0,
        desc="파워 켜기 단계의 최대 대기를 비웠을 때 쓰는 목표 도달 대기 한도의 최소값입니다. 실제 한도는 예상 도달 시간 × 배수 + "
             "여유와 이 값 중 큰 값('파워 도달 대기 절대 상한' 이하)이며, 넘으면 공정을 중단합니다."),
    _p("ADJUST_SETTLE_MAX_SEC", "process", "공정 중 수정 뒤 이탈 판정 쉼 상한", "s", F, 120.0, 10.0, 3600.0, src=_V2,
       desc="공정 중 수정으로 DC·RF Pulse 목표를 바꾸면 새 목표에 다시 도달할 때까지 이탈 판정을 쉽니다. 이 시간이 지나면 "
            "도달하지 못해도 이탈 판정을 다시 켜고 경고를 남깁니다.",
       note="안전 규칙 S31C·S34A, 사유 코드 ADJUST_SETTLE_TIMEOUT."),
    _p("POWER_WAIT_TIMEOUT_MAX_SEC", "process", "파워 도달 대기 절대 상한", "s", F, 7200.0, 600.0, 14400.0,
       safety=H, src="lib/config.py:676 (고정값)",
        desc="파워 도달 대기 한도의 절대 상한입니다. 예상 시간으로 계산한 한도가 이 값을 넘으면 이 값을 씁니다."),
    _p("POWER_WAIT_EST_FACTOR", "process", "파워 대기 예상 시간 배수", "배", F, 1.5, 1.0, 5.0, safety=L,
       src="controller/process_controller.py:997",
        desc="파워 도달 대기 한도를 계산할 때 예상 도달 시간에 곱하는 배수입니다. 예상 도달 시간은 DC·RF·RF Pulse 중 목표 "
             "파워까지 올리는 데 가장 오래 걸리는 장비의 예상 시간입니다."),
    _p("POWER_WAIT_EST_MARGIN_SEC", "process", "파워 대기 예상 여유", "s", F, 120.0, 0.0, 1800.0, safety=L,
       src="controller/process_controller.py:997",
        desc="파워 도달 대기 한도를 계산할 때 예상 도달 시간 × 배수에 더하는 여유입니다. 클수록 한도가 길어집니다."),

    # ───────────── 정지(stop) ─────────────
    _p("STOP_FLOW_OFF_TIMEOUT_SEC", "stop", "STOP 유량 OFF 확인 한도(채널당)", "s", F, 6.0, 2.0, 60.0,
       src="controller/process_controller.py:31",
        desc="STOP·ALL STOP 정지 순서의 가스 유량 정지 단계에서 유량 꺼짐 확인을 기다리는 한도입니다. 넘으면 기다리지 않고 "
             "다음 단계로 넘어갑니다."),
    _p("STOP_EXHAUST_WAIT_SEC", "stop", "STOP MFC 밸브 열기(배기) 대기", "s", F, 8.0, 1.0, 60.0,
       src="controller/process_controller.py:1922",
        desc="STOP·ALL STOP 정지 순서의 MFC 밸브 열기(배기) 단계에서 밸브 열림 확인을 기다리는 한도입니다. 넘으면 기다리지 "
             "않고 다음 단계로 넘어갑니다."),
    _p("STOP_STEP_WAIT_DEFAULT_SEC", "stop", "STOP·ALL STOP 단계 기본 기다림(표에 한도가 없는 단계)", "s", F, 5.0, 1.0, 60.0,
       src=_V2, desc="정지 순서에서 따로 정한 한도 설정이 없는 단계의 완료를 기다리는 시간입니다. 넘으면 다음 단계로 갑니다.",
                note="작업 번호 5-1."),

    # ───────────── 웹 연동(uplink) ─────────────
    _p("UPLINK_INTERVAL_MS", "uplink", "웹 전송 주기", "ms", I, 1000, 500, 10000, safety=L, apply=LIVE,
       src="reporter.py (_loop 1 s)",
        desc="'웹 수집 주소'로 데이터를 보내는 주기로, 매번 보관함의 미전송 메시지와 최신 상태 하나를 싣습니다. 명령 결과·경고 같은 "
             "중요한 메시지는 이 주기를 기다리지 않고 바로 보냅니다.",
        note="웹 수집 주소로 POST 를 보내는 주기."),
    _p("UPLINK_TIMEOUT_SEC", "uplink", "웹 요청 응답 제한", "s", F, 15.0, 2.0, 60.0, safety=L,
       src="reporter.py (_post timeout=15)",
        desc="웹 요청 하나(상태 보내기·명령 받기 연결)의 응답 제한 시간입니다. 넘으면 실패로 보고 다시 시도하며, 공정·안전 동작에는 "
             "영향이 없습니다."),
    _p("UPLINK_SPOOL_MAX_MB", "uplink", "미전송 보관 상한", "MB", I, 5, 1, 500, safety=L, src="reporter.py",
        desc="웹으로 아직 보내지 못한 메시지를 디스크 보관함에 쌓아 두는 상한입니다. 넘치면 중요도가 낮은 메시지부터 버리고 경고를 "
             "남깁니다.",
        note="경고 코드 UPLINK_SPOOL_FULL."),
    _p("UPLINK_SUMMARY_PERIOD_SEC", "uplink", "웹으로 보내는 스냅샷 요약 주기", "s", I, 60, 10, 600, safety=L,
       apply=LIVE, src=_V2, desc="웹으로 보내는 1초 기록 요약의 구간 길이로, 정각에 맞춘 구간마다 숫자 값의 마지막·평균·최소·최대, 장비별 정상 비율·최대 "
                                 "나이, 행 수를 요약해 보냅니다. 공정 중인 구간에만 보내며, 공정 밖 요약은 보내지 않습니다.",
                            note="원래 설명: 숫자 열의 last·mean·min·max, 장비별 ok 비율·최대 나이, 행 수. 구간 안 행에 공정 id 가 "
                                 "하나라도 있을 때만 내고 구간의 마지막 run id 를 싣는다(공정 밖 요약은 내지 않음, 사용자 결정 2026-10-22, "
                                 "snapshot_summary·proc.run_id)."),
    _p("UPLINK_CLOCK_SKEW_SEC", "uplink", "웹 명령 만료 판정 허용 시계 차", "s", F, 5.0, 0.0, 60.0, safety=L, apply=LIVE,
       src=_V2, desc="웹 명령의 만료를 판정할 때 허용하는 시계 차이입니다. 명령의 만료 시각이 장비 시계로 이 시간보다 더 지났으면 명령을 "
                     "실행하지 않고 만료 결과를 돌려줍니다.",
                note="만료 시각 = 명령의 expiresAt, 결과 코드 COMMAND_EXPIRED."),
    _p("UPLINK_DOWN_SEC", "uplink", "웹 연결 끊김 알림 기준", "s", I, 30, 5, 600, safety=L, apply=LIVE, src=_V2,
       desc="웹 보내기 실패가 이 시간을 넘게 이어지면 경고합니다. 공정·안전 동작은 그대로이고 기록·화면에만 남습니다.",
       note="경고 코드 UPLINK_DOWN."),

    # ───────────── 기록(record) ─────────────
    _p("SNAPSHOT_RETENTION_DAYS", "record", "1초 기록 보관 기간", "일", I, 180, 7, 3650, safety=L, apply=RESTART, src=_V2,
        desc="'노트북 기록 폴더'의 일별 기록 파일(1초 스냅샷과 기록, SQLite·CSV)을 이 기간만 남깁니다. 날이 바뀔 때 기간이 "
             "지난 노트북 파일을 지우며, NAS 사본은 지우지 않습니다.",
        note="일별 파일 = 1초 스냅샷·이벤트(SQLite·CSV)."),
    _p("NAS_SYNC_PERIOD_SEC", "record", "NAS 사본 주기(지난 날 파일)", "s", I, 60, 10, 3600, safety=L, apply=RESTART,
       src=_V2,
        desc="지난 날 기록 파일(.sqlite·.csv)을 'NAS 사본 폴더'로 복사하는 주기입니다. 크기가 같은 파일은 건너뛰고 "
             "실패하면 다음 회차에 다시 하며, 오늘 DB 사본은 'NAS 오늘 DB 사본 주기'를 따릅니다."),
    _p("NAS_TODAY_COPY_SEC", "record", "NAS 오늘 DB 사본 주기", "s", I, 600, 60, 86400, safety=L, apply=RESTART, src=_V2,
       desc="오늘 기록 DB 를 SQLite 백업으로 떠서 'NAS 사본 폴더'로 복사하는 주기입니다.",
       note="SQLite backup API 로 사본을 뜬다."),
)


# ───────────── 장비별 통신 묶음(5-17) ─────────────
# 포트·속도·패리티·종결 문자·응답 시간·재시도가 같은 장비끼리 한 묶음에 보이게 한다(화면 묶음 params[].group 만 바뀐다 —
# scope·적용 시점·역할은 그대로). 표에 있는데 PARAMS 에 없는 키는 그냥 지나간다.
def _RP(dev_key: str) -> tuple:
    return tuple(f"{dev_key}_READ_PLAN_{n}" for n in ("SLOW_N", "SLOW_M", "SLOW_PCT", "FAST_PCT", "RETURN_SEC", "HOLD_SEC"))


COMM_GROUPS: tuple = (
    ("comm_plc", ("PLC_PORT", "PLC_BAUD", "PLC_PARITY", "PLC_STOPBITS", "PLC_SLAVE_ID", "PLC_FAIL_STREAK",
                  "PLC_TXN_TIMEOUT_MS", "PLC_WRITE_DEADLINE_MS", "PLC_RETRY_COUNT", "PLC_RETRY_DELAY_MS",
                  "PLC_READ_DEADLINE_MS")),
    ("comm_mfc", ("MFC_PORT", "MFC_BAUD", "MFC_FAIL_STREAK", "MFC_CMD_GAP_MS", "MFC_QUERY_TIMEOUT_MS",
                  "MFC_QUERY_RETRIES", "MFC_VERIFY_RETRY_MS", "MFC_VERIFY_TRIES", "MFC_LINK_RETRY_MS", "MFC_SP_REFRESH_SEC",
                  "MFC_READ_PLAN", *_RP("MFC"), "MFC_READ_DEADLINE_MS")),
    ("comm_dc", ("DC_PORT", "DC_BAUD", "DC_DELIMITER", "DC_FAIL_STREAK", "DC_WRITE_QUERY_GAP_MS", "DC_START_GAP_MS",
                 "DC_STEP_TIMEOUT_MS", "DC_CURR_WRITE_DELAY_MS", "DC_READ_DEADLINE_MS")),
    ("comm_rfpulse", ("RFPULSE_ENABLED", "RFPULSE_PORT", "RFPULSE_BAUD", "RFPULSE_PARITY", "RFPULSE_ADDR",
                      "RFPULSE_FAIL_STREAK", "RFPULSE_CMD_GAP_MS", "RFPULSE_QUERY_TIMEOUT_MS", "RFPULSE_ACK_TIMEOUT_MS",
                      "RFPULSE_ONOFF_TIMEOUT_MS", "RFPULSE_ON_SETTLE_MS", "RFPULSE_CMD_RETRIES", "RFPULSE_READ_PLAN",
                      *_RP("RFPULSE"), "RFPULSE_READ_DEADLINE_MS", "RFPULSE_COMM_REOPEN_SEC")),
    ("comm_ig", ("IG_ENABLED", "IG_PORT", "IG_BAUD", "IG_FAIL_STREAK", "IG_CHAR_GAP_MS", "IG_FILAMENT_E_BLOCK_MS",
                 "IG_RESPONSE_LIMIT_MS")),
    ("comm", ("PORT_OPEN_TIMEOUT_SEC", "PORT_WRITE_TIMEOUT_SEC", "CONNECT_INIT_TIMEOUT_SEC", "CONNECT_RETRY_PAUSE_MS")),
)
_GROUP_OF = {k: g for g, keys in COMM_GROUPS for k in keys}
PARAMS = tuple(_dc_replace(p, group=_GROUP_OF[p.key]) if p.key in _GROUP_OF else p for p in PARAMS)

# 아무 동작도 하지 않는 설정(W-25 C2 — 코드에서 읽는 곳 없음을 다시 확인: process_states·safety_rules 의 params 목록은 설명용).
# 키는 지우지 않는다(저장된 설정이 깨지지 않게). 웹 묶음 params[].unused — 평소 보기 설정 화면·서랍에서 숨기고 상세 보기에서만 '쓰지 않음'
UNUSED_KEYS = frozenset({
    "IG_GAUGE_A_NAME", "IG_GAUGE_B_NAME", "IG_VALUE_MAX_AGE_SEC", "IG_BASE_LOG_INTERVAL_SEC",
    "HEATER_GAS_MFC_TIMEOUT_SEC", "RFPULSE_COMM_REOPEN_SEC", "DC_INITIAL_VOLTAGE",
})


# ───────────── 키 사이 규칙 ─────────────
def _r(id_, keys, desc, fn) -> CrossRule:
    return CrossRule(id=id_, keys=tuple(keys), desc=desc, check=fn)


def _need(cond: bool, msg: str) -> Optional[str]:
    return None if cond else msg


CROSS_RULES: tuple = (
    _r("X26", ("HEATER_TC2_MAP",), "TC1→TC2 매핑: 점 2개 이상, TC1·TC2 모두 증가(v1 _validate_heater_cascade_config) — "
       "tc2·cascade 두 모드가 쓰고 tc2 는 역함수(TC2 → TC1 출발점)도 쓴다",
       lambda v: _map_problem(v["HEATER_TC2_MAP"])),
    _r("X27", ("HEATER_TC2_MAP", "HEATER_SV_LIMIT_C", "HEATER_OT2_LIMIT_C", "HEATER_HOLD_TC2_MARGIN_C", "HEATER_CTRL_MODE"),
       "tc2 모드: 목표 상한(SV_LIMIT)의 매핑 SV2 가 TC2 목표 상한(OT2 − 마진) 이하 — 넘으면 보정할 바깥 루프가 없어 그 목표에 "
       "닿지 못한다(cascade·tc1 은 검사하지 않음)",
       lambda v: _tc2_cap_problem(v)),
    _r("X01", ("HEATER_MV_MIN", "HEATER_MV_LIMIT", "HEATER_MV_ABS_MAX"),
       "MV_MIN+20 ≤ MV_LIMIT ≤ MV_ABS_MAX",
       lambda v: _need(v["HEATER_MV_MIN"] + 20 <= v["HEATER_MV_LIMIT"] <= v["HEATER_MV_ABS_MAX"],
                       f"MV_LIMIT {v['HEATER_MV_LIMIT']} 은 {v['HEATER_MV_MIN'] + 20}~{v['HEATER_MV_ABS_MAX']}")),
    _r("X02", ("HEATER_OT_LIMIT_C", "HEATER_SV_LIMIT_C"), "OT ≥ SV_LIMIT + 20",
       lambda v: _need(v["HEATER_OT_LIMIT_C"] >= v["HEATER_SV_LIMIT_C"] + 20,
                       f"OT {v['HEATER_OT_LIMIT_C']} < SV_LIMIT {v['HEATER_SV_LIMIT_C']} + 20")),
    _r("X03", ("HEATER_OT2_LIMIT_C", "HEATER_OT_LIMIT_C"), "OT2 ≥ OT + 20",
       lambda v: _need(v["HEATER_OT2_LIMIT_C"] >= v["HEATER_OT_LIMIT_C"] + 20,
                       f"OT2 {v['HEATER_OT2_LIMIT_C']} < OT {v['HEATER_OT_LIMIT_C']} + 20")),
    _r("X04", ("HEATER_MAX_TEMP", "HEATER_SV_LIMIT_C"), "MAX_TEMP ≤ SV_LIMIT",
       lambda v: _need(v["HEATER_MAX_TEMP"] <= v["HEATER_SV_LIMIT_C"],
                       f"MAX_TEMP {v['HEATER_MAX_TEMP']} > SV_LIMIT {v['HEATER_SV_LIMIT_C']}")),
    _r("X05", ("HEATER_HOLD_TC2_MARGIN_C", "HEATER_OT2_LIMIT_C", "HEATER_SV_LIMIT_C"),
       "TC2 목표 상한(OT2 − 마진)이 SV_LIMIT 이상",
       lambda v: _need(v["HEATER_OT2_LIMIT_C"] - v["HEATER_HOLD_TC2_MARGIN_C"] >= v["HEATER_SV_LIMIT_C"],
                       "OT2 − TC2_MARGIN 이 SV_LIMIT 보다 낮다")),
    _r("X06", ("HEATER_WD_PERIOD_MS",), "워치독 주기 ≤ 래더 TON/3",
       lambda v: _need(v["HEATER_WD_PERIOD_MS"] * 3 <= dev.HEATER_WD_TON_SEC * 1000,
                       f"WD {v['HEATER_WD_PERIOD_MS']} ms × 3 > TON {dev.HEATER_WD_TON_SEC} s")),
    _r("X07", ("PLC_COMM_LOSS_ABORT_SEC",), "PLC 끊김 중단 ≤ 래더 워치독 TON",
       lambda v: _need(v["PLC_COMM_LOSS_ABORT_SEC"] <= dev.HEATER_WD_TON_SEC,
                       "PLC 끊김 중단이 래더 워치독보다 늦다")),
    _r("X08", ("COMM_RECONNECT_START_MS", "COMM_RECONNECT_MAX_MS"), "재연결 MAX ≥ START",
       lambda v: _need(v["COMM_RECONNECT_MAX_MS"] >= v["COMM_RECONNECT_START_MS"], "MAX < START")),
    _r("X09", ("PLC_SESSION_MARK_REG",), "마커 D 주소가 사용 중인 주소와 겹치지 않음",
       lambda v: _need(v["PLC_SESSION_MARK_REG"] == 0
                       or v["PLC_SESSION_MARK_REG"] not in dev.PLC_D_USED,
                       f"D{v['PLC_SESSION_MARK_REG']:05d} 는 사용 중")),
    _r("X10", ("HEATER_CURRENT_MV_ZERO", "HEATER_CURRENT_MV_FULL"), "전류 추정 FULL > ZERO",
       lambda v: _need(v["HEATER_CURRENT_MV_FULL"] > v["HEATER_CURRENT_MV_ZERO"], "FULL ≤ ZERO")),
    _r("X11", ("IG_FILAMENT_ON_TIMEOUT_SEC", "IG_FILAMENT_SWITCH_QUIET_SEC"), "ON 기한 > 무응답 유예",
       lambda v: _need(v["IG_FILAMENT_ON_TIMEOUT_SEC"] > v["IG_FILAMENT_SWITCH_QUIET_SEC"],
                       "필라멘트 ON 기한이 무응답 유예보다 짧다")),
    _r("X12", ("TICK_DEADLINE_MS", "PLC_READ_DEADLINE_MS", "MFC_READ_DEADLINE_MS", "DC_READ_DEADLINE_MS",
               "RFPULSE_READ_DEADLINE_MS"), "장비별 읽기 마감 ≤ tick 마감(이온 게이지는 예외 — X25)",
       lambda v: _need(all(v[k] <= v["TICK_DEADLINE_MS"] for k in
                           ("PLC_READ_DEADLINE_MS", "MFC_READ_DEADLINE_MS", "DC_READ_DEADLINE_MS",
                            "RFPULSE_READ_DEADLINE_MS")),
                       "장비 읽기 마감이 tick 마감보다 길다")),
    _r("X25", ("IG_RESPONSE_LIMIT_MS", "TICK_DEADLINE_MS"), "이온 게이지 응답 제한: tick 마감 이상, 2 tick 미만",
       lambda v: _need(v["TICK_DEADLINE_MS"] <= v["IG_RESPONSE_LIMIT_MS"] < 2000,
                       "IG 응답 제한은 tick 마감 이상·2000 ms 미만(늦은 응답이 바로 다음 행에 들어가도록)")),
    _r("X13", ("PLC_FAST_POLL_MS", "PLC_READ_DEADLINE_MS"), "PLC 내부 폴링 주기 ≥ PLC 읽기 마감의 절반",
       lambda v: _need(v["PLC_FAST_POLL_MS"] * 2 >= v["PLC_READ_DEADLINE_MS"], "폴링 주기가 너무 짧다")),
    _r("X14", ("POWER_WAIT_TIMEOUT_SEC", "POWER_WAIT_TIMEOUT_MAX_SEC"), "파워 대기 최소 ≤ 상한",
       lambda v: _need(v["POWER_WAIT_TIMEOUT_SEC"] <= v["POWER_WAIT_TIMEOUT_MAX_SEC"], "최소 > 상한")),
    _r("X15", ("HEATER_WAIT_TIMEOUT_SEC", "HEATER_WAIT_TIMEOUT_MAX_SEC"), "승온 대기 최소 ≤ 상한",
       lambda v: _need(v["HEATER_WAIT_TIMEOUT_SEC"] <= v["HEATER_WAIT_TIMEOUT_MAX_SEC"], "최소 > 상한")),
    _r("X16", ("BASE_PRESSURE_MIN_TORR", "BASE_PRESSURE_MAX_TORR"), "베이스 압력 하한 < 상한",
       lambda v: _need(v["BASE_PRESSURE_MIN_TORR"] < v["BASE_PRESSURE_MAX_TORR"], "하한 ≥ 상한")),
    _r("X17", ("RFPULSE_DUTY_MIN", "RFPULSE_DUTY_MAX", "RFPULSE_PULSE_FREQ_MIN_HZ", "RFPULSE_PULSE_FREQ_MAX_HZ"),
       "펄스 듀티·주파수 하한 < 상한",
       lambda v: _need(v["RFPULSE_DUTY_MIN"] < v["RFPULSE_DUTY_MAX"]
                       and v["RFPULSE_PULSE_FREQ_MIN_HZ"] < v["RFPULSE_PULSE_FREQ_MAX_HZ"], "하한 ≥ 상한")),
    _r("X18", ("RFPULSE_PULSE_FREQ_MAX_HZ",), "펄스 주파수 상한 ≤ CESAR 사양",
       lambda v: _need(v["RFPULSE_PULSE_FREQ_MAX_HZ"] <= dev.CESAR_PULSE_FREQ_MAX_HZ, "장비 사양 초과")),
    _r("X19", ("DC_MAX_VOLTAGE", "DC_MAX_CURRENT", "DC_INITIAL_VOLTAGE", "DC_INITIAL_CURRENT", "DC_MAX_POWER"),
       "DC 시작값 ≤ 상한, 상한 ≤ 장비 정격",
       lambda v: _need(v["DC_INITIAL_VOLTAGE"] <= v["DC_MAX_VOLTAGE"] <= dev.DC_RATED_VOLTAGE_V
                       and v["DC_INITIAL_CURRENT"] <= v["DC_MAX_CURRENT"] <= dev.DC_RATED_CURRENT_A
                       and v["DC_MAX_POWER"] <= dev.DC_RATED_POWER_W, "DC 한도 불일치")),
    _r("X20", ("HEATER_HOLD_MV_ARRIVE_TOL_C", "HEATER_HOLD_MV_ENTER_TOL_C"), "도착 범위 ≤ 진입 허용",
       lambda v: _need(v["HEATER_HOLD_MV_ARRIVE_TOL_C"] <= v["HEATER_HOLD_MV_ENTER_TOL_C"], "도착 범위가 더 넓다")),
    _r("X21", ("IG_PORT", "IG_ENABLED"), "이온 게이지를 쓰면 포트 필요",
       lambda v: _need((not v["IG_ENABLED"]) or bool(v["IG_PORT"]), "IG_ENABLED 인데 IG_PORT 가 비었다")),
    _r("X22", ("PLC_PORT", "MFC_PORT", "DC_PORT", "RFPULSE_PORT", "IG_PORT", "RFPULSE_ENABLED", "IG_ENABLED"),
       "시리얼 포트 중복 없음(꺼진 장비 RFPULSE_ENABLED·IG_ENABLED = false 의 포트는 보지 않는다)",
       lambda v: _port_clash(v)),
    _r("X42", ("DC_CURR_WRITE_DELAY_MS", "DC_WRITE_QUERY_GAP_MS"),
       "DC 전류 쓰기 지연 + 쓰기 뒤 질의 금지 시간 ≤ 1000 ms(다음 초의 측정이 금지 시간에 걸리지 않게)",
       lambda v: _need(v["DC_CURR_WRITE_DELAY_MS"] + v["DC_WRITE_QUERY_GAP_MS"] <= 1000,
                       f"DC_CURR_WRITE_DELAY_MS {v['DC_CURR_WRITE_DELAY_MS']} + DC_WRITE_QUERY_GAP_MS "
                       f"{v['DC_WRITE_QUERY_GAP_MS']} > 1000 ms")),
    _r("X24", ("RFPULSE_REFP_AUTO_OFF_SEC", "RFPULSE_REFP_SEC"), "장비 반사 자동 차단(명령 33) > 소프트웨어 반사파 판정",
       lambda v: _need(v["RFPULSE_REFP_AUTO_OFF_SEC"] > v["RFPULSE_REFP_SEC"],
                       "명령 33 시간이 소프트웨어 판정보다 짧다 — 평소에도 장비가 먼저 끈다")),
    *(_r(xid, (f"{d}_READ_PLAN_SLOW_N", f"{d}_READ_PLAN_SLOW_M", f"{d}_READ_PLAN_SLOW_PCT", f"{d}_READ_PLAN_FAST_PCT"),
         f"{name} 읽기 계획 기준: 느린 읽기 수 ≤ 세는 읽기 수, 돌아오는 기준 < 느린 읽기 기준(5-20 ①)",
         lambda v, d=d: _need(v[f"{d}_READ_PLAN_SLOW_N"] <= v[f"{d}_READ_PLAN_SLOW_M"]
                              and v[f"{d}_READ_PLAN_FAST_PCT"] < v[f"{d}_READ_PLAN_SLOW_PCT"],
                              f"{d}_READ_PLAN_SLOW_N {v[f'{d}_READ_PLAN_SLOW_N']} > SLOW_M {v[f'{d}_READ_PLAN_SLOW_M']} 이거나 "
                              f"FAST_PCT {v[f'{d}_READ_PLAN_FAST_PCT']} ≥ SLOW_PCT {v[f'{d}_READ_PLAN_SLOW_PCT']}"))
      for xid, d, name in (("X28", "MFC", "MFC"), ("X29", "RFPULSE", "RF Pulse"))),
    _r("X23", ("RUN_MODE", "UPLINK_URL"), "sim 모드는 웹 주소를 쓰지 않는다(UPLINK_IN_SIM 이 아니면 무시) — prod 는 주소 필요",
       lambda v: _need(v["RUN_MODE"] != "prod" or bool(v["UPLINK_URL"]),
                       "prod 인데 UPLINK_URL 이 비었다")),
)


def _map_problem(text) -> Optional[str]:
    from v2.chk.heater_cascade import map_problem
    p = map_problem(text)
    return f"HEATER_TC2_MAP: {p}" if p else None


def _tc2_cap_problem(v: Mapping[str, Any]) -> Optional[str]:
    if str(v["HEATER_CTRL_MODE"]) != "tc2":
        return None
    from v2.chk.heater_cascade import Tc2Map, map_problem, parse_map
    if map_problem(v["HEATER_TC2_MAP"]):
        return None                                   # X26 이 알린다
    need = Tc2Map(parse_map(v["HEATER_TC2_MAP"])).value(float(v["HEATER_SV_LIMIT_C"]))
    cap = float(v["HEATER_OT2_LIMIT_C"]) - float(v["HEATER_HOLD_TC2_MARGIN_C"])
    return _need(need <= cap, f"매핑(SV_LIMIT {v['HEATER_SV_LIMIT_C']:g}) = SV2 {need:.1f} > TC2 목표 상한 {cap:.1f}"
                              f"(OT2 − TC2_MARGIN)")


_PORT_KEYS = (("PLC_PORT", None, "PLC"), ("MFC_PORT", None, "MFC"), ("DC_PORT", None, "DC Power"),
              ("RFPULSE_PORT", "RFPULSE_ENABLED", "RF Pulse"), ("IG_PORT", "IG_ENABLED", "이온 게이지"))


def _port_clash(v: Mapping[str, Any]) -> Optional[str]:
    """같은 COM 포트를 쓰는 두 장비(켜진 것만) — 없으면 None."""
    seen: dict = {}
    for key, en, name in _PORT_KEYS:
        if en is not None and not v[en]:
            continue                                   # 꺼진 장비는 포트를 열지 않는다
        port = str(v[key]).strip().upper()
        if not port:
            continue
        if port in seen:
            return f"같은 COM 포트를 두 장비가 쓴다: {port} — {seen[port]}·{name}"
        seen[port] = name
    return None


def _ports_unique(v: Mapping[str, Any]) -> bool:
    return _port_clash(v) is None


# X23 은 RUN_MODE 기본값이 sim 이라 기본값에서 통과한다(UPLINK_URL 기본 "").


# ───────────── v2 에서 뺀 키·상수 ─────────────
DROPPED: tuple = (
    Dropped("UPLINK_STATE_HEARTBEAT_SEC", "결정 8(2026-10-20) — state 는 POST(UPLINK_INTERVAL_MS)마다 최신 하나. "
            "따로 재전송 주기가 없다", "reporter.py"),
    Dropped("_HEATER_COOLDOWN_TARGET_C", "설명 문장을 키로 넣은 것 — 쓰는 곳 없음(죽은 키)", _JSON),
    Dropped("HEATER_TEMP_SCALE", "PLC 래더 고정 사실(0.1 °C/카운트). 바꾸면 한계값 전송(×10 고정)과 단위가 "
            "어긋나고 0 이면 0 나누기 — devices.HEATER_TEMP_SCALE_C_PER_COUNT 상수로", _JSON,
            replaced_by="devices.HEATER_TEMP_SCALE_C_PER_COUNT"),
    Dropped("HEATER_PUSH_CONFIG", "v2 는 히터 한계값을 항상 쓰고 되읽어 확인한다. 끄면 PLC 에 남은 "
            "모르는 값으로 운전하게 된다", _JSON),
    Dropped("HEATER_RECIPE_DIR", "노트북 파일 대화상자 없음 — 레시피는 웹에서 온다", _JSON),
    Dropped("HEATER_LOG_ENABLED", "히터 CSV 는 1초 스냅샷(히터 필드 포함)으로 대체", _JSON,
            replaced_by="스냅샷 heater.*"),
    Dropped("HEATER_LOG_PERIOD_MS", "히터 CSV 는 1초 스냅샷으로 대체", _JSON, replaced_by="스냅샷 heater.*"),
    Dropped("IG_CSV_ENABLED", "IG CSV 는 1초 스냅샷(ig.*)으로 대체", _JSON, replaced_by="스냅샷 ig.*"),
    Dropped("HEATER_WD_FORCE_SEC", "v2 는 PLC worker 하나가 버스를 독점해 하트비트가 잠금 경합으로 밀리지 "
            "않는다 — 하트비트는 HEATER_WD_PERIOD_MS 주기로만", "lib/config.py:318"),
    Dropped("HEATER_HOLD_MV_AFTER_REACH", "옛 키(HEATER_HOLD_MODE 로 바뀜)", "lib/config.py:151",
            replaced_by="HEATER_HOLD_MODE"),
    Dropped("DEV_MODE", "RUN_MODE(sim/bench/prod) 로 대체. v1 은 파일이 없으면 운영 모드였다", "lib/paths.py",
            replaced_by="RUN_MODE"),
    Dropped("CHAT_WEBHOOK_URL", "구글챗은 웹 서버가 이벤트를 받아 보낸다", "lib/config_local.py (이름만)"),
    Dropped("CHAT_WEBHOOK_PLC_DISCONNECT_URL", "구글챗은 웹 서버가 보낸다", "lib/config_local.py (이름만)"),
    Dropped("INTERLOCK_CHECK_INTERVAL", "쓰는 곳 없음(죽은 상수)", "lib/config.py:205"),
    Dropped("MFC_SCALE_FACTORS", "쓰는 곳 없음(죽은 상수)", "lib/config.py:801"),
    Dropped("RF_MAX_ERROR_COUNT", "쓰는 곳 없음(죽은 상수)", "lib/config.py:565"),
    Dropped("RF_RAMP_DELAY", "쓰는 곳 없음(죽은 상수)", "lib/config.py:564"),
    Dropped("RFPULSE_RECV_FRAME_TIMEOUT_MS", "쓰는 곳 없음(죽은 상수)", "lib/config.py:651"),
    Dropped("RFPULSE_POST_WRITE_DELAY_MS", "쓰는 곳 없음(죽은 상수)", "lib/config.py:653"),
    Dropped("RFPULSE_ACK_FOLLOWUP_GRACE_MS", "쓰는 곳 없음(죽은 상수)", "lib/config.py:654"),
    Dropped("HEATER_COIL_AT_REQ", "오토튜닝 — import 만 되고 쓰는 곳 없음", "lib/config.py:255"),
    Dropped("RFPULSE_POLL_INTERVAL_MS", "5 s 폴링 → 1초 tick 안에서 번갈아 읽기", "lib/config.py:655",
            replaced_by="devices.TICK_PLAN"),
    Dropped("MFC_POLLING_INTERVAL_MS", "2 s 폴링(공정 중만) → 1초 tick, 공정 밖에서도 읽음", "lib/config.py:793",
            replaced_by="devices.TICK_PLAN"),
    Dropped("HEATER_SV_LOG_MIN_DELTA_C", "기록 줄이기용 — 1초 스냅샷이 SV 를 남긴다", "device/PLC.py:54"),
    Dropped("HEATER_SV_LOG_MIN_INTERVAL_SEC", "기록 줄이기용", "device/PLC.py:55"),
    Dropped("MFC_STATE_LOG_ENABLED", "v2 는 1초 스냅샷 기록이 대신함 — mfc.pressure_mtorr(R5)·mfc.valve_pct(R6)를 매초 "
            "SQLite·일별 CSV 에 남긴다(끄는 스위치 없음)", _JSON, replaced_by="스냅샷 mfc.pressure_mtorr·mfc.valve_pct"),
    Dropped("MFC_STATE_LOG_INTERVAL_S", "v2 는 1초 스냅샷 기록이 대신함 — 주기는 tick 1 s 고정", _JSON,
            replaced_by="devices.TICK_PLAN"),
    Dropped("MFC_STATE_LOG_GAP_MS", "v2 는 1초 스냅샷 기록이 대신함 — 읽기 사이 간격은 MFC_CMD_GAP_MS 하나", _JSON,
            replaced_by="MFC_CMD_GAP_MS"),
    Dropped("MFC_STATE_LOG_TIMEOUT_MS", "v2 는 1초 스냅샷 기록이 대신함 — 읽기 응답 제한은 MFC_QUERY_TIMEOUT_MS·"
            "MFC_READ_DEADLINE_MS", _JSON, replaced_by="MFC_QUERY_TIMEOUT_MS"),
    Dropped("_EXC_SUPPRESS_SEC", "미처리 예외 챗 억제 — 챗은 웹 서버로", "main.py:4751"),
)


def by_key() -> dict:
    return {p.key: p for p in PARAMS}


def json_key_map() -> dict:
    """v1 json 키 → v2 키(이름이 같거나 v1_key)."""
    m = {}
    for p in PARAMS:
        m[p.v1_key or p.key] = p.key
    return m
