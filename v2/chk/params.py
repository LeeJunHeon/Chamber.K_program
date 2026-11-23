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
          enum_labels=None) -> ParamDef:
    return _p(key, "install", label, unit, type_, default, lo, hi, safety=safety, apply=RESTART,
              scope=Scope.INSTALL, desc=desc, pattern=pattern, enum=enum, v1_key=v1_key, src=src,
              v1_default=v1_default, locked=locked, choices=choices, enum_labels=enum_labels)


# 읽기 계획 판정 기준(5-20 ① — drivers/readplan.py). auto 일 때만 쓰고, 모두 PC 쪽 설정이다(장비 쪽에 맞출 값 없음).
def _read_plan_params(dev_key: str, name: str, what: str, slow_n: int) -> tuple:
    k = f"{dev_key}_READ_PLAN"
    src = "v2 (5-20 ① — 11/23 bench: 한 번 느렸던 읽기로 끝까지 번갈아 읽음)"
    return (
        _inst(f"{k}_SLOW_N", f"{name} 번갈아로 바꾸는 느린 읽기 수", I, slow_n, lo=2, hi=60, unit="회", src=src,
              desc="매초 읽기 중 최근 읽기(세는 읽기 수) 가운데 느린 읽기가 이 횟수 이상이면 번갈아 읽기로 바꾼다. 하한 2 는 한 번 "
                   f"느렸다고 바꾸지 않기 위함이다({k}_SLOW_M)"),
        _inst(f"{k}_SLOW_M", f"{name} 느린 읽기를 세는 최근 읽기 수", I, 10, lo=2, hi=120, unit="회", src=src,
              desc="느린 읽기를 셀 때 보는 최근 읽기 수이다(1초에 한 번 읽으므로 대략 초). 번갈아로 바꾸는 느린 읽기 수 이상이어야 "
                   f"한다({k}_SLOW_N)"),
        _inst(f"{k}_SLOW_PCT", f"{name} 느린 읽기 기준", I, 60, lo=20, hi=100, unit="%", src=src,
              desc=f"{what}이 읽기 마감의 이 비율을 넘으면 느린 읽기다. 기본은 v2 처음 기준(마감의 60 %)과 같다"
                   f"({dev_key}_READ_DEADLINE_MS)"),
        _inst(f"{k}_FAST_PCT", f"{name} 매초 읽기로 돌아오는 기준", I, 45, lo=10, hi=95, unit="%", src=src,
              desc="번갈아 읽는 동안 셋 다 읽었다면 걸렸을 시간(이번 읽기 + 명령 간격 + 가장 긴 응답)이 읽기 마감의 이 비율 안이면 "
                   "빠른 읽기다. 기준 근처에서 계획이 오가지 않게 느린 읽기 기준보다 작아야 한다"
                   f"({k}_SLOW_PCT)"),
        _inst(f"{k}_RETURN_SEC", f"{name} 매초 읽기로 돌아오기까지", I, 60, lo=10, hi=3600, unit="s", src=src,
              desc="번갈아 읽는 동안 이 시간 내내 빠른 읽기만 이어지면 매초 읽기로 돌아온다(느린 읽기가 하나라도 있으면 처음부터 "
                   "다시 센다)"),
        _inst(f"{k}_HOLD_SEC", f"{name} 읽기 계획을 다시 바꾸기까지", I, 60, lo=60, hi=3600, unit="s", src=src,
              desc="읽기 계획을 바꾼 뒤 이 시간 안에는 다시 바꾸지 않는다. 하한이 60 s 라서 1분에 한 번보다 자주 바뀌지 않는다"),
    )


# 통신 설정 설명에 붙는 말(5-17) — 이 값은 PC 쪽 설정일 뿐이고 장비 쪽은 사람이 따로 맞춘다
def _pc(where: str) -> str:
    return f" PC 쪽 설정이다. 장비 쪽도 같은 값이어야 한다(장비에서 바꾸는 곳: {where})."


_AT_PLC = "XG5000 기본 파라미터 Cnet"
_AT_MFC = "장비 메뉴"
_AT_DC = "앞면 I/O 메뉴"
_AT_RFP = "앞면"
_AT_IG = "내부 DIP S1"


PARAMS: tuple = (
    # ───────────── 설치값(install) — 웹 admin + 노트북 비상 덮어쓰기, 재시작 필요 ─────────────
    _inst("EQUIPMENT_ID", "장비 ID", S, "CHK2", pattern=r"[A-Z0-9][A-Z0-9-]{1,15}", safety=H, src=_V2, locked=True,
          desc="웹에서 장비를 구분하는 ID. 개발 중에는 v1(CHK)과 섞이지 않게 CHK2 를 쓴다. 웹에서 바꾸지 않고 노트북 설정 파일로만 바꾼다"),
    _inst("RUN_MODE", "실행 모드", E, "sim", enum=("sim", "bench", "prod"), safety=H, src=_V2, locked=True,
          desc="sim=가짜 장비·외부 연결 없음 / bench=실장비 읽기 전용 / prod=운전. "
               "로컬 파일이 없으면 sim. prod 는 명시적으로만"),
    _inst("PLC_PORT", "PLC 포트", S, "COM9", pattern=_COM, safety=H,
          desc="PLC 가 물린 PC 의 COM 포트(장치 관리자에서 확인)"),
    _inst("PLC_BAUD", "PLC 통신 속도", I, 115200, choices=dev.PLC_BAUDS, unit="bps",
          src="lib/config.py:24 (고정값) · XGB 내장 Cnet 매뉴얼(사용자 확인 2026-11-13)",
          desc="XGB 내장 Cnet(Modbus RTU 서버)이 받는 속도만 고른다. 지금 장비 115200 8N1." + _pc(_AT_PLC)),
    _inst("PLC_PARITY", "PLC 패리티", E, "none", enum=dev.PLC_PARITIES,
          src="v2 (5-17 — XGB 내장 Cnet 매뉴얼, 사용자 확인 2026-11-13. v1 은 8N1 고정)",
          desc="데이터 비트는 Modbus RTU 라 8 고정. 지금 장비 none." + _pc(_AT_PLC)),
    _inst("PLC_STOPBITS", "PLC 정지 비트", I, 1, choices=dev.PLC_STOPBITS, unit="bit",
          src="v2 (5-17 — XGB 내장 Cnet 매뉴얼, 사용자 확인 2026-11-13. v1 은 8N1 고정)",
          desc="지금 장비 1." + _pc(_AT_PLC)),
    _inst("PLC_SLAVE_ID", "PLC Modbus 국번", I, 1, lo=1, hi=247, src="lib/config.py:25 (고정값)",
          desc="지금 장비 국번 1." + _pc(_AT_PLC)),
    _inst("MFC_PORT", "MFC 포트", S, "COM10", pattern=_COM, desc="MFC 컨트롤러가 물린 PC 의 COM 포트"),
    _inst("MFC_BAUD", "MFC 통신 속도", I, 9600, choices=dev.MFC_BAUDS, unit="bps", safety=L,
          src="config_user.json · [MFC p.72, p.54] (9600 기본, 9600~115200 선택 — 장비 사실 표 근거 그대로)",
          desc="GPC3000 이 받는 속도만 고른다. 데이터 8·패리티 없음·정지 1 은 장비 고정(8N1)." + _pc(_AT_MFC)),
    _inst("DC_PORT", "DC Power 포트", S, "COM11", pattern=_COM, desc="DC Power 가 물린 PC 의 COM 포트"),
    _inst("DC_BAUD", "DC Power 통신 속도", I, 9600, choices=dev.DC_BAUDS, unit="bps", safety=L,
          v1_key="DC_BAUDRATE", src="config_user.json · [DC p.38-40] (사용자 확인 2026-11-13)",
          desc="ODA EX 가 받는 속도만 고른다. 데이터 8·정지 1·패리티 없음은 장비 고정(8N1)." + _pc(_AT_DC)),
    _inst("DC_DELIMITER", "DC Power 종결 문자", E, "LF", enum=dev.DC_DELIMITERS,
          src="v2 (5-17 — [DC p.49], 출고값 LF. v1 은 LF 고정 송신)",
          desc="명령 끝에 붙이고 응답 끝으로 읽는 문자(LF·CR·CRLF). 다르면 장비가 명령이 끝난 것으로 보지 않아 "
               "무응답이 된다." + _pc(_AT_DC)),
    _inst("RFPULSE_ENABLED", "RF Pulse 사용", B, False, v1_default=True, src=_V2,
          desc="false 면 포트를 열지 않고 rfp 블록 품질을 off 로 둔다. RF Pulse 를 쓰는 레시피는 시작을 거부한다(지금 연결하지 않음, 사용자 2026-10-16, "
               "DEVICE_DISABLED)"),
    _inst("RFPULSE_PORT", "RF Pulse 포트", S, "COM13", pattern=_COM, desc="RF Pulse(CESAR)가 물린 PC 의 COM 포트"),
    _inst("RFPULSE_BAUD", "RF Pulse 통신 속도", I, 9600, choices=dev.CESAR_BAUDS, unit="bps", safety=L,
          src="config_user.json · [CESAR 4-32~4-37] (사용자 확인 2026-11-13)",
          desc="CESAR 가 받는 속도만 고른다. 8 데이터·1 정지는 장비 고정." + _pc(_AT_RFP)),
    _inst("RFPULSE_ADDR", "RF Pulse AE Bus 주소", I, 1, lo=0, hi=31,
          desc="AE Bus 주소 0~31(v1 은 32 이상이면 응답 비교가 늘 맞지 않았다). 매뉴얼 판 57023916-00B 에 "
               "'unit address is always 1' 이라고 나와 기본 1 을 그대로 쓴다." + _pc(_AT_RFP)),
    _inst("RFPULSE_PARITY", "RF Pulse 패리티", E, "odd", enum=("none", "odd", "even"),
          desc="CESAR RS-232 규격은 홀수 패리티(8O1)이고 장비는 홀수로 고정돼 있어 odd 로 둔다. 다르면 NAK 만 돌아온다."
               + _pc(_AT_RFP)),
    _inst("IG_ENABLED", "이온 게이지 사용", B, True, v1_default=False,
          desc="false 면 base_pressure 가 있는 공정은 시작을 거부한다(v1 은 경고만 하고 대기를 건너뜀)"),
    _inst("IG_PORT", "이온 게이지 포트", S, "COM12", pattern=r"(" + _COM + r")?", v1_default="",
          desc="이온 게이지가 물린 PC 의 COM 포트(MOXA UPort 경유)"),
    _inst("IG_BAUD", "이온 게이지 통신 속도", I, 9600, choices=dev.IG_BAUDS, unit="bps", safety=L,
          src="lib/config.py:735 · [IG p.15, p.18] (DIP S1-1·S1-2, 출고 9600 — 사용자 확인 2026-11-13)",
          desc="Terranova 934 가 받는 속도만 고른다. 8N1 은 장비 고정." + _pc(_AT_IG)),
    _inst("HEATER_ENABLED", "히터 기능 사용", B, True, safety=H,
          desc="false 면 히터 명령·레시피 히터 행을 거부한다. 워치독 하트비트는 항상 쓴다"),
    _inst("PLC_FAIL_STREAK", "PLC 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="v2 (3단계 worker 기본값)",
          desc="연속 무응답이 이 횟수면 포트를 닫고 백오프 재연결"),
    _inst("MFC_FAIL_STREAK", "MFC 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="v2 (v1 은 무응답 1회에 닫음)",
        desc="MFC 읽기·명령에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 백오프로 다시 연결한다. 작을수록 잠깐의 무응답에도 다시 연결하고, 클수록 끊김을 늦게 알아챈다."),
    _inst("DC_FAIL_STREAK", "DC 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="device/DCpower.py:28",
        desc="DC 질의에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 백오프로 다시 연결한다. 작을수록 잠깐의 무응답에도 다시 연결하고, 클수록 끊김을 늦게 알아챈다."),
    _inst("RFPULSE_FAIL_STREAK", "RF Pulse 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="v2 (3단계 worker 기본값)",
        desc="RF Pulse 질의·명령에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 백오프로 다시 연결한다. 작을수록 잠깐의 무응답에도 다시 연결하고, 클수록 끊김을 늦게 알아챈다."),
    _inst("IG_FAIL_STREAK", "이온 게이지 연속 무응답 재연결 횟수", I, 3, lo=1, hi=20, unit="회", src="device/IonGauge.py (3회)",
        desc="이온 게이지 질의에 연속으로 응답이 없는 횟수가 이 값에 이르면 포트를 닫고 백오프로 다시 연결한다. 필라멘트 켜기·끄기 직후 무응답 유예 동안의 무응답은 세지 않는다."),
    _inst("PLC_TXN_TIMEOUT_MS", "PLC 트랜잭션 응답 제한", I, 150, lo=30, hi=1000, unit="ms",
          src="v2 (4-1 drivers/plc.py TXN_TIMEOUT_S, v1 0.5 s)",
          desc="Modbus 트랜잭션 하나의 응답 제한이다. bench 실측(회선 + 스캔 + USB 지연)으로 맞춘다"),
    _inst("PLC_WRITE_DEADLINE_MS", "PLC 쓰기 요청 한도", I, 2000, lo=500, hi=10000, unit="ms",
          src="v2 (4-1 drivers/plc.py submit_write 의 고정값 2.0 s)",
          desc="쓰기 요청 하나(대기열 기다림 + 트랜잭션 + 재시도 전부)의 한도이다. 넘으면 그 쓰기는 실패로 끝낸다"),
    _inst("DC_WRITE_QUERY_GAP_MS", "DC 쓰기 뒤 질의 금지 시간", I, 500, lo=100, hi=3000, unit="ms",
          src="device/DCpower.py:36 (2026-09-28 실측)",
          desc="ODA EX 는 직전 명령을 처리하는 동안 들어온 질의에 답하지 않으므로, 쓰기 뒤 이만큼 질의하지 않는다"),
    _inst("DC_START_GAP_MS", "DC 출력 켜기 명령 간격", I, 120, lo=120, hi=2000, unit="ms",
          src="device/DCpower.py:829-833 (_send 의 msleep(120))",
          desc="*RST → *CLS → APPLy → OUTP ON 사이 간격. v1 실측값 120 ms 보다 짧게는 못 둔다"),
    _inst("DC_STEP_TIMEOUT_MS", "DC 켜기·끄기 단계 응답 제한", I, 1500, lo=1500, hi=10000, unit="ms",
          src="device/DCpower.py (OUTP? 1.5 s)", safety=H,
          desc="출력 켜기·OFF 확인 순서에서 단계 하나(OUTP? 질의 포함)의 한도. OFF 확인이 일찍 포기하지 않게 1500 ms 보다 "
               "짧게는 못 둔다"),
    _inst("DC_CURR_WRITE_DELAY_MS", "DC 측정 뒤 전류 쓰기까지", I, 100, lo=100, hi=400, unit="ms",
          src="v2 (사용자 결정 2026-10-15, drivers/dc.py CURR_WRITE_DELAY_S)",
          desc="1초 읽기(MEAS:ALL?)를 보낸 뒤 CURR 쓰기(와 출력 켜기 시작)까지. 이 값 + DC_WRITE_QUERY_GAP_MS 가 1000 ms 를 "
               "넘으면 다음 초의 측정이 질의 금지 시간에 걸린다(X42)"),
    _inst("MFC_CMD_GAP_MS", "MFC 명령 간격", I, 150, lo=20, hi=2000, unit="ms",
          src="v2 (사용자 결정 2026-10-15, v1 1000 ms)", desc="명령(질의·설정) 사이 최소 간격이다. bench 실측으로 맞춘다"),
    _inst("MFC_QUERY_TIMEOUT_MS", "MFC 질의 응답 제한", I, 300, lo=50, hi=2000, unit="ms",
          src="v2 (사용자 결정 2026-10-15, v1 1000 ms)", desc="bench 실측으로 맞춘다"),
    _inst("MFC_QUERY_RETRIES", "MFC 무응답 질의 다시 보내기 횟수", I, 5, lo=0, hi=10, unit="회",
          src="device/MFC.py enqueue(retries_left=5)",
          desc="설정 명령의 되읽기 질의 하나가 무응답이면 이 횟수만큼 다시 보내고, 다 쓰면 '응답 없음'으로 판정한다. PC 쪽 설정이다(장비 쪽에 맞출 값 없음)"),
    _inst("MFC_VERIFY_RETRY_MS", "MFC 되읽기 다시 하기 전 대기", I, 1000, lo=200, hi=5000, unit="ms",
          src="lib/config.py MFC_DELAY_MS(1000)",
          desc="설정 명령(유량·설정점·설정점 선택·밸브·ON/OFF) 되읽기가 다르면 이만큼 뒤 다시 확인한다. 하한 200 ms 는 명령 간격(기본 150 ms)보다 길게 두기 위함이다. "
               "PC 쪽 설정이다(장비 쪽에 맞출 값 없음, MFC_CMD_GAP_MS)"),
    _inst("MFC_VERIFY_TRIES", "MFC 되읽기 확인 횟수", I, 5, lo=1, hi=10, unit="회",
          src="device/MFC.py max_attempts=5 (FLOW_SET·SPn_SET·SPn_ON·밸브)",
          desc="PC 쪽 설정이다(장비 쪽에 맞출 값 없음). 이 횟수 안에 되읽기가 맞지 않으면 그 명령 실패(VERIFY_MISMATCH). "
               "횟수 × 대기가 공정 스텝 한도(60 s)를 넘지 않게 상한 10"),
    _inst("MFC_SP_REFRESH_SEC", "MFC 장비 설정점 값 다시 읽기 주기", I, 60, lo=0, hi=3600, unit="s", src="v2 (5-19 ③ — v1 없음)",
          desc="공정이 돌지 않는 동안 장비에 저장된 설정점 값(R1~R4·R10)을 이 주기로 다시 읽는다(한 초에 하나씩, 설정 명령·읽기 마감에 양보). 0 이면 다시 읽지 않고 연결 "
               "직후·설정점을 고를 때만 읽는다. PC 쪽 설정이다(장비 쪽에 맞출 값 없음)"),
    _inst("MFC_LINK_RETRY_MS", "MFC 재연결 대기 중 다시 내는 간격", I, 1000, lo=200, hi=5000, unit="ms",
          src="v2 (4-2 drivers/mfc.py LINK_RETRY_S, v1 은 큐에서 기다림)",
          desc="PC 쪽 설정이다(장비 쪽에 맞출 값 없음). 포트가 끊긴 동안 설정 명령을 이 간격으로 다시 낸다(공정 스텝 한도 안에서)"),
    _inst("MFC_AR_FS_SCCM", "MFC Ar 채널 풀스케일(못 읽을 때)", F, 100.0, lo=1.0, hi=100000.0, unit="sccm",
          src="v2 (사용자 결정 2026-10-15)", desc="연결 때 R70 으로 읽은 FS 를 먼저 쓰고, 읽지 못하면 이 값을 쓴다. 100 이면 %FS 와 sccm 이 같다"),
    _inst("MFC_O2_FS_SCCM", "MFC O2 채널 풀스케일(못 읽을 때)", F, 100.0, lo=1.0, hi=100000.0, unit="sccm",
          src="v2 (사용자 결정 2026-10-15)", desc="연결 때 R71 로 읽은 FS 를 우선"),
    _inst("MFC_PRESSURE_FS_TORR", "MFC 압력 센서 풀스케일", F, 1.0, lo=0.001, hi=1000.0, unit="Torr",
          src="lib/config.py:806 (MFC_PRESSURE_SCALE 0.1 = FS 1 Torr 가정)",
          desc="압력 %FS ↔ mTorr 환산. bench 에서 레인지 코드(E)로 확인"),
    _inst("MFC_READ_PLAN", "MFC 읽기 계획", E, "auto", enum=("auto", "full", "alternate"), src="v2 (사용자 결정 2026-10-15)",
          desc="full 은 매초 R5(압력)·R60(유량)·R6(밸브 위치)를 모두 읽고, alternate 는 R5 는 매초·R60 은 짝수 초·R6 은 홀수 초에 읽는다. auto 는 "
               "느린 읽기가 이어질 때만 alternate 로 바꾸고 다시 빨라지면 full 로 돌아온다(R6 무응답으로 길어진 읽기는 세지 않고, 읽기 실패로도 바꾸지 않는다). R6 는 "
               "기록용이라 설정 명령이 진행 중이거나 R6 를 기다리면 마감을 넘길 초에는 보내지 않고 양보한다(그 초의 밸브 값만 stale). 지금 계획은 스냅샷 mfc.read_plan, "
               "판정 기준은 MFC_READ_PLAN_SLOW_N·SLOW_M·SLOW_PCT·FAST_PCT·RETURN_SEC·HOLD_SEC"),
    *_read_plan_params("MFC", "MFC", "셋 다 읽은 한 번(R5·R60·R6)", 5),
    _inst("RFPULSE_CMD_GAP_MS", "RF Pulse 명령 간격", I, 50, lo=0, hi=2000, unit="ms",
          src="[CESAR p.174 / 6-4] (v1 1500 ms)", desc="응답을 받은 뒤 다음 명령까지의 간격이다(매뉴얼: 핸드셰이크 또는 50 ms)"),
    _inst("RFPULSE_QUERY_TIMEOUT_MS", "RF Pulse 질의 응답 제한", I, 250, lo=50, hi=3000, unit="ms",
          src="v2 (사용자 결정 2026-10-15, v1 4500~9000 ms)", desc="질의 하나(162·165·166 각각)의 제한"),
    _inst("RFPULSE_ACK_TIMEOUT_MS", "RF Pulse 설정 명령 응답 제한", I, 2000, lo=500, hi=10000, unit="ms",
          src="lib/config.py:649 (RFPULSE_ACK_TIMEOUT_MS 2000, 고정값)",
          desc="설정 명령(모드·셋포인트·주파수·듀티·Host 전환)의 CSR 응답을 기다리는 시간"),
    _inst("RFPULSE_ONOFF_TIMEOUT_MS", "RF Pulse RF ON·OFF 응답 제한", I, 2500, lo=2500, hi=10000, unit="ms", safety=H,
          src="device/RFpulse.py:927-956 (max(RFPULSE_ACK_TIMEOUT_MS, 2500))",
          desc="RF ON·RF OFF 의 CSR 응답을 기다리는 시간. RF OFF 를 일찍 포기하지 않게 2500 ms 보다 짧게는 못 둔다"),
    _inst("RFPULSE_ON_SETTLE_MS", "RF Pulse RF ON 뒤 되읽기까지", I, 800, lo=0, hi=5000, unit="ms",
          src="lib/config.py:657 (RFPULSE_POLL_START_DELAY_AFTER_RF_ON_MS 800, 고정값)",
          desc="RF ON 뒤 펄스 설정(193·196)을 되읽기 전에 기다리는 시간"),
    _inst("RFPULSE_CMD_RETRIES", "RF Pulse 시작 명령 재시도 횟수", I, 3, lo=1, hi=10, unit="회",
          src="device/RFpulse.py (_make_cmd(retries=3))",
          desc="시작 순서의 설정 명령 하나가 무응답·NAK·CSR ≠ 0 일 때 다시 보내는 횟수"),
    _inst("RFPULSE_READ_PLAN", "RF Pulse 읽기 계획", E, "auto", enum=("auto", "full", "alternate"),
          src="v2 (사용자 결정 2026-10-15)",
          desc="full 은 매초 162(상태)·165(순방향)·166(반사)를 모두 읽고, alternate 는 166 을 매초·165 와 162 를 번갈아 읽는다. auto 는 느린 읽기가 "
               "이어질 때만 alternate 로 바꾸고 다시 빨라지면 full 로 돌아온다. 느린 읽기는 질의 하나가 응답 제한을 다 썼거나 한 번 읽기가 마감의 기준 비율을 넘은 읽기이다. "
               "지금 계획은 스냅샷 rfp.read_plan(RFPULSE_QUERY_TIMEOUT_MS·RFPULSE_READ_PLAN_SLOW_PCT)"),
    *_read_plan_params("RFPULSE", "RF Pulse", "셋 다 읽은 한 번(162·165·166)", 3),
    _inst("IG_CHAR_GAP_MS", "이온 게이지 글자 사이 간격", I, 50, lo=dev.IG_CHAR_GAP_MIN_MS, hi=1000, unit="ms",
          src="v2 (5-17 — [IG p.52-53] 글자 사이 50 ms 이상, 초당 약 2글자보다 빠르게 보내지 않는다)",
          desc="명령 글자(E·F·A·B)를 보낸 뒤 다음 글자를 보내기까지 최소 간격. 평소에는 E 를 1초에 한 번만 보내 걸리지 않는다"),
    _inst("IG_FILAMENT_E_BLOCK_MS", "필라멘트 명령 뒤 E 금지 시간", I, 1000, lo=500, hi=5000, unit="ms",
          src="device/IonGauge.py (e_block_sec 1.0)",
          desc="A(켜기)·B(끄기)를 보낸 뒤 이만큼은 상태 질의 E 를 보내지 않는다. 초당 약 2글자 제한 때문에 500 ms 보다 짧게 둘 수 없다"),
    _inst("PORT_WRITE_TIMEOUT_SEC", "포트 쓰기 제한 시간", F, 1.0, lo=0.2, hi=10.0, unit="s",
          src="v2 (3단계 transport.SerialSettings.write_timeout 1.0)",
          desc="시리얼 포트에 바이트를 쓰는 호출 하나의 한도이다(USB-시리얼이 사라졌을 때 쓰기가 막히지 않게). 다섯 장비에 함께 쓴다"),
    _inst("CONNECT_RETRY_PAUSE_MS", "연결 초기화 다시 하기 전 쉼", I, 300, lo=100, hi=5000, unit="ms",
          src="v2 (3단계 worker.CONNECT_RETRY_PAUSE_S 0.3)",
          desc="연결 직후 첫 질의에 답이 없으면 포트를 닫지 않고 이만큼 쉰 뒤 입력을 비우고 초기화를 한 번 더 한다"),
    _inst("PORT_OPEN_TIMEOUT_SEC", "포트 열기 제한 시간", F, 2.0, lo=0.5, hi=30.0, unit="s", src="lib/serial_guard.py (v1 2 s)",
          desc="포트 열기를 별도 스레드에서 이만큼만 기다린다(2026-09-16 사고)"),
    _inst("CONNECT_INIT_TIMEOUT_SEC", "연결 직후 초기화 제한 시간", F, 5.0, lo=1.0, hi=60.0, unit="s", src="v2 (3단계 worker 상수)",
          desc="연결 직후 장비 초기화(설정 쓰기·되읽기 등) 한도"),
    _inst("LOCAL_HMI_PORT", "로컬 화면 포트", I, 8710, lo=1024, hi=65535, safety=L, src=_V2,
          desc="127.0.0.1 에서만 연다"),
    _inst("LOCAL_HMI_CONTROL", "노트북 조작", E, "unlock", enum=("view_stop", "unlock", "always"),
          enum_labels=(("view_stop", "보기와 정지만"), ("unlock", "잠금을 푼 뒤 조작"), ("always", "항상 조작")),
          src="v2 (5-20 ② — 11/23 bench 뒤 사용자 결정)",
          desc="노트북(로컬 화면)에서 받는 명령을 정한다. 보기와 정지만은 정지(STOP·ALL STOP·부저 정지·히터 정지)와 공정 중 값 바꾸기·건너뛰기·일시정지·재개만 받는다(5-19 "
               "까지와 같다). 잠금을 푼 뒤 조작과 항상 조작은 그 밖에 공정 시작·적재 레시피 시작·해제·순서 고치기·PLC 버튼·필라멘트·히터·수동 조작도 받는다(설정 적용·되돌리기는 "
               "웹에서만). 둘의 차이는 노트북 화면이 잠그는지뿐이며, 잠금은 노트북 화면이 하고 장비는 이 값과 잠금 시간을 hello 로 알린다"),
    _inst("LOCAL_HMI_RELOCK_MIN", "노트북 조작 다시 잠그기까지", I, 10, lo=1, hi=120, unit="분", safety=L,
          src="v2 (5-20 ② — 사용자 결정)",
          desc="노트북 조작이 '잠금을 푼 뒤 조작'일 때, 잠금을 푼 뒤 이 시간 동안 아무 조작이 없으면 노트북 화면이 다시 잠근다. "
               "정지 버튼은 잠겨 있어도 늘 누를 수 있다"),
    _inst("LOCAL_DATA_DIR", "로컬 기록 폴더", S, r"C:\ChamberK_v2\data", pattern=r".+", safety=L,
          src=_V2, desc="1초 스냅샷 SQLite·일별 CSV·이벤트·블랙박스"),
    _inst("LOCAL_HMI_STATIC_DIR", "로컬 화면 파일 폴더", S, "", pattern=r".*", safety=L, src=_V2,
          desc="로컬 서버가 / 와 정적 파일로 내주는 폴더이다(폴더 밖 경로는 막음). 비면 프로그램 위치 기준으로 이 저장소의 v2\\hmi 를 쓴다(사용자 결정 2026-10-22). "
               "화면 파일은 웹 W-7 에서 넣는다(127.0.0.1:LOCAL_HMI_PORT)"),
    _inst("NAS_COPY_DIR", "NAS 사본 폴더", S, r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK2",
          pattern=r".*", safety=L, src=_V2, desc="빈 값이면 NAS 에 복사하지 않는다. 복사는 제어 흐름과 분리된 스레드가 한다"),
    _inst("UPLINK_IN_SIM", "sim 에서도 웹 연결(웹 개발용)", B, False, safety=H, src=_V2, locked=True,
          desc="켜면 sim 도 웹에 연결한다(웹 단계 개발용). 진짜 장비 기록과 섞이지 않게 이때 장비 ID 가 -SIM 으로 끝나야 시작한다(아니면 시작 거부). 웹은 hello 의 "
               "runMode 로 sim 장비를 구분한다. 웹에서 바꾸지 않는다(EQUIPMENT_ID)"),

    # ───────────── 비밀값(secret) — 장비 로컬 파일에만, 웹에 보내지 않음 ─────────────
    _p("UPLINK_URL", "secret", "웹 수집 주소", "", S, "", scope=Scope.SECRET, apply=RESTART,
       pattern=r"(https?://.+)?", v1_key="ERP_INGEST_URL", src="lib/config_local.py (이름만)",
       desc="웹의 장비 연결 기준 주소이다(POST {주소}/ingest, GET {주소}/stream). https 는 인증서를 확인하고, http 면 시작 때 경고한다. 로그·화면에는 "
            "호스트만 보인다"),
    _p("UPLINK_TOKEN", "secret", "웹 수집 토큰(장비별)", "", S, "", scope=Scope.SECRET, apply=RESTART,
       v1_key="ERP_INGEST_TOKEN", src="lib/config_local.py (이름만)",
        desc="웹 수집 주소로 보내는 모든 요청(상태 보내기·명령 받기)에 Authorization: Bearer 헤더로 싣는 장비별 토큰이다. 장비 로컬 파일에만 두고 로그·화면·웹 메시지에는 "
             "쓰지 않으며, 바꾸면 재시작해야 적용된다."),

    # ───────────── 1초 수집(tick) ─────────────
    _p("TICK_DEADLINE_MS", "tick", "1초 수집 마감", "ms", I, 900, 500, 950, safety=M, apply=RESTART, src=_V2,
       desc="정각 초 t 에 읽기를 보내고 t+이 값에 행을 닫는다. 늦은 응답은 다음 행에 stale"),
    _p("PLC_FAST_POLL_MS", "tick", "PLC 내부 폴링 주기", "ms", I, 250, 100, 500, safety=H, apply=RESTART, src=_V2,
       desc="안전 비트(MV·인터록·히터)를 보는 주기. 1초에 4번 중 정각 회차를 스냅샷에 쓴다",
       v1_default=200, note="v1 200 ms(상대 타이머)"),
    _p("PLC_READ_DEADLINE_MS", "tick", "PLC 읽기 마감", "ms", I, 400, 100, 900, apply=RESTART, src=_V2,
        desc="PLC 한 번 읽기(재시도 포함)가 끝나야 하는 한도로, 넘기면 그 읽기는 무응답으로 끝난다. 1초 수집과 내부 폴링이 모두 이 한도를 쓰며, 1초 수집 마감 이하이고 내부 폴링 "
             "주기의 2배 이하여야 한다."),
    _p("MFC_READ_DEADLINE_MS", "tick", "MFC 읽기 마감", "ms", I, 900, 200, 950, apply=RESTART, src=_V2,
        desc="MFC 1초 읽기(압력·유량·밸브 위치)가 끝나야 하는 한도로, 넘기면 그 읽기는 실패로 끝난다. 읽기 계획(auto)의 느린 읽기 판정도 이 한도의 비율로 하며, 1초 수집 마감 "
             "이하여야 한다."),
    _p("DC_READ_DEADLINE_MS", "tick", "DC 읽기 마감", "ms", I, 500, 100, 900, apply=RESTART, src=_V2,
        desc="DC 1초 측정 질의가 끝나야 하는 한도로, 넘기면 그 읽기는 실패로 끝난다. 쓰기 뒤 질의 금지 시간 때문에 이 한도 안에 질의를 보내지 못해도 실패가 되며, 1초 수집 마감 "
             "이하여야 한다."),
    _p("RFPULSE_READ_DEADLINE_MS", "tick", "RF Pulse 읽기 마감", "ms", I, 900, 200, 950, apply=RESTART, src=_V2,
        desc="RF Pulse 1초 읽기(상태·순방향·반사)가 끝나야 하는 한도로, 넘기면 그 읽기는 실패로 끝난다. 읽기 계획(auto)의 느린 읽기 판정도 이 한도의 비율로 하며, 1초 수집 "
             "마감 이하여야 한다."),
    _p("IG_RESPONSE_LIMIT_MS", "tick", "이온 게이지 E 응답 제한", "ms", I, 1200, 900, 1900, apply=RESTART, src="lib/config.py:756 (v1 실측 근거)",
       desc="E 응답을 tick 마감 뒤에도 이만큼까지 받아 다음 행에 late 로 쓴다(그동안 다음 E 는 보내지 않아 stale). 넘기면 fail 이다. 평소 응답은 250~300 ms, "
            "레인지 찾기는 860 ms 라서 장비별 읽기 마감 ≤ tick 마감 규칙의 예외이다(X12)",
       v1_key="IG_RESPONSE_TIMEOUT_MS"),
    _p("PLC_RETRY_COUNT", "comm", "PLC 재시도 횟수", "회", I, 2, 0, 5, desc="트랜잭션 실패 시 재시도(총 1+N)"),
    _p("PLC_RETRY_DELAY_MS", "comm", "PLC 재시도 간격", "ms", I, 30, 0, 1000, safety=L,
        desc="PLC Modbus 트랜잭션이 실패했을 때 다시 보내기 전에 쉬는 시간이다. 재시도는 'PLC 재시도 횟수'만큼 하며, 쉬는 시간도 읽기 마감 안에 들어간다."),
    _p("PLC_COMM_LOSS_ABORT_SEC", "comm", "PLC 두절 공정 중단", "s", F, 10.0, 1.0, 10.0, safety=H,
       desc="마지막 성공 뒤 이만큼 지나면 공정 중단. 래더 히터 워치독 TON(10 s) 이하"),
    _p("PLC_DISCONNECT_NOTIFY_SEC", "comm", "PLC 끊김 알림 유예", "s", F, 60.0, 10.0, 600.0, safety=L,
       src="device/PLC.py:192 (_DISCONNECT_GRACE_S)",
        desc="PLC 값이 이 시간 넘게 들어오지 않으면 끊김 알림(오류)을 단절마다 한 번 보내고 PLC 블랙박스를 파일로 남긴다. 다시 연결되면 정보 알림을 한 번 보내며, 공정 중단과는 별개로 "
             "늘 본다."),
    _p("DC_COMM_LOSS_ABORT_SEC", "comm", "DC 두절 공정 중단", "s", F, 10.0, 2.0, 30.0, safety=H,
        desc="DC 를 쓰는 공정에서 출력을 켠 뒤 DC 값이 이 시간 넘게 들어오지 않으면 DC 끄기를 보내고 공정을 중단한다. 통신이 돌아왔을 때 공정이 없으면 DC 끄기를 다시 보낸다."),
    _p("RFPULSE_COMM_LOSS_ABORT_SEC", "comm", "RF Pulse 두절 공정 중단", "s", F, 10.0, 2.0, 30.0,
       safety=H,
        desc="RF Pulse 를 쓰는 공정에서 RF Pulse 를 시작한 뒤 값이 이 시간 넘게 들어오지 않으면 RF Pulse 끄기를 보내고 공정을 중단한다. 통신이 돌아왔을 때 공정이 없으면 "
             "RF Pulse 끄기를 다시 보낸다."),
    _p("RFPULSE_COMM_REOPEN_SEC", "comm", "RF Pulse 포트 다시 열기", "s", F, 3.0, 1.0, 30.0, safety=L,
       src="lib/config.py:57 (고정값)",
        desc="v2 에서는 사용하지 않는다. 재연결은 연속 무응답 재연결 횟수와 재연결 간격 설정이 정한다(v1 은 무응답이 이만큼 이어지면 포트를 다시 열었음)"),
    _p("IG_COMM_LOSS_ABORT_SEC", "comm", "이온 게이지 두절 베이스 대기 중단", "s", F, 60.0, 10.0, 600.0,
       src="controller/process_controller.py:35",
        desc="베이스 압력 대기 블록 중 이온 게이지 값이 이 시간 넘게 들어오지 않으면 공정을 중단한다. 필라멘트 켜기·끄기 직후 무응답 유예 구간은 두절로 세지 않는다."),
    _p("COMM_RECONNECT_START_MS", "comm", "재연결 첫 간격", "ms", I, 1000, 100, 60000, safety=L,
        desc="장비 포트가 끊기거나 열기에 실패한 뒤 다시 연결을 시도하기까지의 첫 간격이다. 실패할 때마다 두 배로 늘어 '재연결 최대 간격'까지 커지며, 다섯 장비에 함께 쓴다."),
    _p("COMM_RECONNECT_MAX_MS", "comm", "재연결 최대 간격", "ms", I, 60000, 1000, 600000, safety=L,
        desc="실패할 때마다 두 배로 늘어나는 재연결 간격의 상한이다. '재연결 첫 간격'보다 작을 수 없으며, 다섯 장비에 함께 쓴다."),
    _p("COMM_LONG_OUTAGE_SEC", "comm", "장기 두절 판정", "s", I, 600, 30, 86400, safety=L,
        desc="연결 실패가 이 시간 넘게 이어지면 장기 두절로 보고 알림을 한 번 낸다. 그 뒤로는 '장기 두절 중 시도 주기'마다 연결을 시도하고, 실패 기록은 '두절 기록 간격'마다만 남긴다."),
    _p("COMM_PROBE_MS", "comm", "장기 두절 중 시도 주기", "ms", I, 60000, 1000, 600000, safety=L,
        desc="장기 두절로 본 뒤 연결을 다시 시도하는 간격이다. 이때부터는 두 배로 늘리는 재연결 간격 대신 이 주기로 시도한다."),
    _p("COMM_OUTAGE_LOG_SEC", "comm", "두절 기록 간격", "s", I, 600, 30, 86400, safety=L, apply=LIVE,
        desc="장기 두절 중 연결 실패 기록을 남기는 간격이다. DC 끄기를 확인하지 못해 다시 시도할 때 늘어나는 간격의 상한으로도 쓴다."),

    # ───────────── PLC ─────────────
    _p("PLC_SESSION_MARK_REG", "plc", "PLC 재기동 감지 D 주소", "D번호", I, 60, 0, 9999,
       desc="래더가 쓰지 않는 D 주소이다. 0 이면 꺼짐(재기동 감지를 하지 않는다)"),
    _p("MV_INTERLOCK_ABORT_MS", "plc", "MV 인터록 해제 지속 중단", "ms", I, 1000, 200, 5000, safety=H,
       src="main.py:3839", desc="공정 중 MV_INTERLOCK 이 이만큼 연속 OFF 면 중단(순간 끊김은 기록만)"),
    _p("BLACKBOX_SECONDS", "plc", "PLC 블랙박스 길이", "s", I, 60, 10, 600, safety=L,
       apply=RESTART, src="device/PLC.py:178 (deque 300 × 200 ms)",
        desc="PLC 내부 폴링 값을 최근 이 시간만큼 메모리에 담아 두는 블랙박스 길이다. PLC 통신 두절·끊김 알림·재기동 감지 때 이 기록을 파일로 남기며, 길수록 사고 전 기록이 길어진다."),

    # ───────────── MFC(경고·알림만, 공정은 계속) ─────────────
    _p("FLOW_ERROR_TOLERANCE", "mfc", "유량 이탈 허용 비율", "비율", F, 0.10, 0.01, 0.5, apply=LIVE,
       desc="안정화 판정과 공정 중 감시에 쓴다"),
    _p("FLOW_ERROR_SEC", "mfc", "유량 이탈 지속 시간", "s", F, 10.0, 1.0, 600.0, safety=L, apply=LIVE,
       v1_key="FLOW_ERROR_MAX_COUNT", v1_period=2.0, v1_default=5,
       desc="이탈이 이만큼 이어지면 경고. v1: 5회 × MFC 폴링 2 s"),
    _p("MFC_PRESSURE_WARN_RATIO", "mfc", "압력 이탈 허용 비율", "비율", F, 0.10, 0.01, 0.5, safety=L,
       apply=LIVE,
        desc="공정 중 챔버 압력과 작업 압력의 차이가 작업 압력 × 이 비율을 넘은 채 '압력 이탈 지속 시간' 이어지면 경고한다. 경고만 하고 공정은 계속하며, 압력 이탈 감시가 켜진 블록(기본 "
             "템플릿은 메인)에서만 본다."),
    _p("MFC_PRESSURE_WARN_SEC", "mfc", "압력 이탈 지속 시간", "s", F, 10.0, 1.0, 600.0, safety=L, apply=LIVE,
       v1_key="MFC_PRESSURE_WARN_COUNT", v1_period=2.0, v1_default=5, desc="v1: 5회 × MFC 폴링 2 s"),
    _p("MFC_FLOW_STABLE_TIMEOUT_SEC", "mfc", "FLOW ON 유량 안정화 한도", "s", F, 100.0, 5.0, 600.0,
       v1_key="MFC_STABILIZATION_TIMEOUT_S", v1_default=100,
       src="config_user.json · lib/config.py MFC_STABILIZATION_TIMEOUT_S(v1 563f772 — 기본 100, 허용 10~600, 시간 판정. "
           "그 전 v1 은 30회 × 1 s)",
       desc="FLOW ON(R69 확인) 뒤 |실제 − 설정| ≤ 설정 × FLOW_ERROR_TOLERANCE 가 되기를 기다리는 시간. 넘으면 공정 중단"
            "(FLOW_STABILIZE_TIMEOUT, v1 '유량 안정화 시간 초과(100초)')"),
    _p("MFC_VALVE_SETTLE_SEC", "mfc", "MFC 밸브 이동 대기", "s", F, 5.0, 0.5, 60.0, safety=L,
       src="lib/config.py:799 (MFC_DELAY_MS_VALVE)", desc="O·C 뒤 R6 로 위치를 확인하기까지"),
    _p("MFC_SP_VERIFY_TOL", "mfc", "압력 설정점(SP1·2·3·5) 설정 확인 허용 오차", "장비 단위", F, 0.01, 0.0, 1.0, safety=L,
       src="lib/config.py MFC_SP_VERIFY_TOL (fa85e15, 고정값 — 예전 MFC_SP1_VERIFY_TOL 0.1)",
       desc="Sn 쓰기 뒤 읽은 값(R1~R3·R10, %FS)과 보낸 값의 차이가 이 값 이하면 통과한다. 장비가 소수 둘째 자리까지 저장하므로 0.01 %FS(FS 1 Torr 에서 0.1 "
            "mTorr)로 둔다"),

    # ───────────── DC Power ─────────────
    _p("DC_MAX_VOLTAGE", "dc", "DC 전압 상한", "V", F, 600.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:512 (고정값)", desc="APPL 전압 상한. 장비 정격 이하"),
    _p("DC_MAX_CURRENT", "dc", "DC 전류 상한", "A", F, 1.0, 0.0, 2.0, safety=H, src="lib/config.py:513 (고정값)",
        desc="DC 전류 설정의 상한으로, 정전류 제어는 전류를 이 값 위로 올리지 않는다. 램프업 중 이 상한에 걸린 채 목표에 미치지 않으면 'DC 상한 정체 시 유지로 넘김' 시간 뒤 유지 "
             "단계로 넘어가며, 연결 때 OVP·OCP 쓰기를 켜면 OCP 값으로도 쓴다."),
    _p("DC_MAX_POWER", "dc", "DC 파워 상한", "W", F, 500.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:514 (고정값)", desc="레시피·수동 입력 상한(v1 은 드라이버에만 있었다)"),
    _p("DC_INITIAL_VOLTAGE", "dc", "DC 시작 전압", "V", F, 450.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:510 (고정값)",
        desc="v2 의 출력 켜기는 이 값을 쓰지 않고 DC 최대 전압으로 켠다. 설정 검사에서 최대 전압 이하인지만 본다(v1 에서도 장비에 보내지 않음, X19)"),
    _p("DC_INITIAL_CURRENT", "dc", "DC 시작 전류", "A", F, 0.1, 0.0, 2.0, safety=H, src="lib/config.py:511 (고정값)",
        desc="DC 출력을 켤 때 처음 주는 전류 설정이다. 켠 뒤에는 정전류 제어가 이 값에서부터 전류를 조절해 목표 파워를 맞추며, 'DC 전류 상한'보다 크면 상한을 쓴다."),
    _p("DC_TOLERANCE_W", "dc", "DC 목표 도달 허용 오차", "W", F, 0.5, 0.1, 10.0, src="lib/config.py:515 (고정값)",
        desc="DC 파워가 목표 ± 이 값 안이면 램프업 도달로 보고, 유지 중에는 전류를 고치지 않는다. 유지 이탈 판정 폭은 이 값과 목표 × 'DC 유지 이탈 비율' 중 큰 값이다."),
    _p("DC_POWER_ERROR_RATIO", "dc", "DC 유지 이탈 비율", "비율", F, 0.10, 0.02, 0.5, safety=H,
       desc="판정 폭 = max(DC_TOLERANCE_W, 목표×비율)"),
    _p("DC_POWER_ERROR_SEC", "dc", "DC 유지 이탈 지속 시간", "s", F, 5.0, 1.0, 60.0, safety=H,
       v1_key="DC_POWER_ERROR_MAX_COUNT", v1_period=1.0, v1_default=5, desc="v1: 5회 × DC 제어 1 s"),
    _p("DC_MIN_CURRENT_ABORT", "dc", "DC 저전류 기준", "A", F, 0.05, 0.0, 1.0, safety=H,
        desc="DC 파워 유지 중 측정 전류가 이 값 이하인 상태가 'DC 저전류 지속 시간' 이어지면 공정을 중단한다."),
    _p("DC_MIN_CURRENT_ABORT_SEC", "dc", "DC 저전류 지속 시간", "s", F, 10.0, 1.0, 120.0, safety=H,
       v1_key="DC_MIN_CURRENT_ABORT_COUNT", v1_period=1.0, v1_default=10, desc="v1: 10회 × DC 제어 1 s"),
    _p("DC_FAIL_ISET_A", "dc", "DC 무출력 판정 설정 전류", "A", F, 0.20, 0.01, 1.0, safety=H,
       src="lib/config.py:517 (고정값)",
        desc="DC 램프업 중 설정 전류가 이 값 이상인데 파워가 'DC 무출력 판정 파워' 이하인 상태가 'DC 램프업 무출력 허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 공정을 "
             "중단한다."),
    _p("DC_FAIL_POWER_W", "dc", "DC 무출력 판정 파워", "W", F, 1.0, 0.1, 50.0, safety=H,
       src="lib/config.py:518 (고정값)",
        desc="DC 램프업 중 설정 전류가 'DC 무출력 판정 설정 전류' 이상인데 파워가 이 값 이하인 상태가 'DC 램프업 무출력 허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 공정을 "
             "중단한다."),
    _p("DC_FAIL_SEC", "dc", "DC 램프업 무출력 허용", "s", F, 15.0, 3.0, 120.0, safety=H,
       v1_key="DC_FAIL_MAX_TICKS", v1_period=1.0, v1_default=15,
       desc="Iset ≥ DC_FAIL_ISET_A 인데 P ≤ DC_FAIL_POWER_W 가 이만큼 이어지면 중단. v1: 15틱 × 1 s"),
    _p("DC_POWER_DELAY_SEC", "dc", "DC 안정화 대기", "s", I, 300, 0, 3600,
       desc="SP1 도달 뒤 Shutter Delay 전 대기(레시피 use_dc_delay 일 때만). 이 동안 ±이탈 판정 꺼짐"),
    _p("DC_CONTROL_GAIN", "dc", "DC 전류 보정 이득", "배", F, 1.0, 0.1, 1.0, safety=H,
       desc="ΔI = 이득 × ΔP / V. v1 은 검사가 없어 음수도 허용했다"),
    _p("DC_RAMP_STEP_A", "dc", "DC 램프업 전류 상승 상한", "A/s", F, 0.010, 0.001, 0.1, safety=H,
        desc="DC 램프업 중 1초에 올릴 수 있는 전류의 상한이다. 작을수록 천천히 올라가 파워 도달이 늦어지며, 파워 도달 대기 한도의 예상 시간 계산에도 쓴다."),
    _p("DC_MAINTAIN_STEP_UP_A", "dc", "DC 유지 중 전류 상승 상한", "A/s", F, 0.020, 0.001, 0.1, safety=H,
        desc="DC 파워 유지 중 1초에 올릴 수 있는 전류의 상한이다. 램프업 중에는 'DC 램프업 전류 상승 상한'을 쓴다."),
    _p("DC_MAINTAIN_STEP_DOWN_A", "dc", "DC 전류 하강 상한", "A/s", F, 0.050, 0.001, 0.2,
        desc="DC 전류를 1초에 내릴 수 있는 상한으로, 램프업과 유지 모두에 쓴다. 측정이 한 번 튀어도 전류가 한꺼번에 크게 떨어지지 않게 한다."),
    _p("DC_SMALL_ERROR_RATIO", "dc", "DC 작은 오차 경계", "비율", F, 0.01, 0.0, 0.1, safety=L,
        desc="DC 파워 유지 중 오차가 목표 × 이 비율(최소 'DC 목표 도달 허용 오차') 이하이면 작은 오차로 보고 'DC 작은 오차 이득'으로 고친다. 0 이면 작은 오차 구간을 쓰지 "
             "않는다."),
    _p("DC_SMALL_ERROR_GAIN", "dc", "DC 작은 오차 이득", "배", F, 0.5, 0.1, 1.0, safety=L,
        desc="DC 파워 유지 중 작은 오차 구간에서 쓰는 전류 보정 이득이다. 1 이면 보통 보정과 같고, 작을수록 작은 오차를 천천히 고쳐 덜 흔들린다."),
    _p("DC_LIMIT_STALL_SEC", "dc", "DC 상한 정체 시 유지로 넘김", "s", I, 10, 1, 120, safety=L,
        desc="DC 램프업 중 전류 상한이나 전압 상한 근처(전압 상한 − 20 V)에 걸린 채 목표에 미치지 않는 상태가 이 시간 이어지면, 목표 미달인 채로 유지 단계로 넘긴다. 이렇게 해서 "
             "파워 도달 대기를 끝내고 다음 블록(기본 템플릿에서는 압력 단계)으로 진행한다."),
    _p("DC_OFF_VERIFY_TRIES", "dc", "DC OFF 확인 횟수", "회", I, 3, 1, 10, safety=H,
       src="device/DCpower.py:38", desc="OUTP OFF 뒤 OUTP? 확인 횟수(무응답이면 OFF 재전송)"),
    _p("DC_SET_PROTECTION", "dc", "DC 연결 때 OVP·OCP 쓰기", "", B, False, safety=H, src="v2 (사용자 결정 2026-10-15)",
       desc="true 면 연결할 때 OVP·OCP 를 최대 전압·전류로 쓰고 되읽는다(prod·sim). 기본은 꺼짐이며, 형명과 공정의 실제 최대 전압·전류(점화 순간 포함)를 bench 에서 "
            "확인한 뒤 켠다(VOLT:OVP=DC_MAX_VOLTAGE, CURR:OCP=DC_MAX_CURRENT)"),
    _p("DC_OFF_RETRY_START_SEC", "dc", "DC OFF 미확인 재시도 첫 간격", "s", F, 10.0, 2.0, 600.0,
       src="device/DCpower.py:32", desc="이후 2배씩, 최대 COMM_OUTAGE_LOG_SEC"),

    # ───────────── RF Power(PLC DAC/ADC) ─────────────
    _p("RF_MAX_POWER", "rf", "RF 파워 상한(DAC 풀스케일 환산)", "W", F, 600.0, 1.0, 600.0, safety=H,
       src="lib/config.py:561 (고정값)", desc="DAC 카운트 = (P/RF_MAX_POWER)×4000×gain + offset"),
    _p("RF_FWD_FULL_SCALE_W", "rf", "RF 순방향 ADC 풀스케일", "W", F, 594.5, 1.0, 2000.0, safety=H,
       src="lib/config.py:558 (고정값)", desc="ADC 4000 카운트에 해당하는 순방향 파워(W)이다. 교정 상수로 v1 값이며 사용자 확인을 마쳤다(2026-10-15)"),
    _p("RF_REF_FULL_SCALE_W", "rf", "RF 반사 ADC 풀스케일", "W", F, 200.0, 1.0, 2000.0, safety=H,
       src="lib/config.py:559 (고정값)", desc="ADC 4000 카운트에 해당하는 반사 파워(W)이다. v1 값을 그대로 쓰며 사용자 확인을 마쳤다(2026-10-15)"),
    _p("RF_CAL_OFFSET", "rf", "RF DAC 보정 오프셋", "count", F, 6.79, -400.0, 400.0, safety=H,
       src="UI.py:440 (offset_edit 기본값)", desc="v1 은 화면 입력칸 값을 공정마다 썼다"),
    _p("RF_CAL_GAIN", "rf", "RF DAC 보정 이득", "배", F, 1.0395, 0.5, 1.5, safety=H,
       src="UI.py:444 (param_edit 기본값)",
        desc="RF 목표 파워를 DAC 카운트로 바꿀 때 곱하는 보정 이득이다. DAC = 오프셋 + (파워 ÷ RF 파워 상한) × 4000 × 이득이며, 클수록 같은 파워에 더 큰 DAC 를 "
             "낸다."),
    _p("RF_RAMP_STEP_W", "rf", "RF 램프업 속도", "W/s", F, 1.0, 0.1, 20.0, safety=H, src="lib/config.py:562 (고정값)",
        desc="RF 램프업 때 1초마다 스텝 목표를 올리는 폭이다. 작을수록 천천히 올라가며, 공정 중 목표를 바꿀 때도 이 속도로 따라가고 파워 도달 대기 한도 계산에도 쓴다."),
    _p("RF_RAMP_DOWN_STEP_COUNTS", "rf", "RF 램프다운 속도", "count/0.5s", I, 8, 1, 400,
       src="lib/config.py:563 (고정값)", note="v1 이름·주석은 W 지만 실제는 DAC 카운트(RFpower.py:212)",
        desc="RF 램프다운 때 0.5 초마다 DAC 를 이 카운트씩 내린다. 정지 순서·파워 끄기 블록·시작 때 남은 RF 정리에 쓰며, 'RF 램프다운 한도' 안에 끝나지 않으면 바로 0 으로 "
             "내린다."),
    _p("RF_TOLERANCE_W", "rf", "RF 목표 도달 허용 오차", "W", F, 1.0, 0.1, 20.0, src="lib/config.py:566 (고정값)",
        desc="RF 순방향 파워가 목표 ± 이 값 안에 들어오면 램프업 도달로 보고, 유지 중 이 범위를 벗어나면 1초마다 DAC 를 1 씩 고친다. 유지 이탈 판정 폭은 이 값과 목표 × 'RF "
             "유지 이탈 비율' 중 큰 값이다."),
    _p("RF_POWER_ERROR_RATIO", "rf", "RF 유지 이탈 비율", "비율", F, 0.10, 0.02, 0.5, safety=H,
        desc="RF 파워 유지 중 순방향과 목표의 차이가 목표 × 이 비율(최소 'RF 목표 도달 허용 오차')을 넘은 채 'RF 유지 이탈 지속 시간' 이어지면, RF 를 램프다운해 끄고 공정을 "
             "중단한다."),
    _p("RF_POWER_ERROR_SEC", "rf", "RF 유지 이탈 지속 시간", "s", F, 5.0, 1.0, 60.0, safety=H,
       v1_key="RF_POWER_ERROR_MAX_COUNT", v1_period=1.0, v1_default=5, desc="v1: 5회 × RF 제어 1 s"),
    _p("RF_FAIL_DAC_COUNTS", "rf", "RF 무출력 판정 DAC", "count", I, 100, 10, 4000, safety=H,
       src="lib/config.py:567 (고정값)",
        desc="RF 램프업 중 DAC 가 이 값 이상인데 순방향이 'RF 무출력 판정 순방향' 이하인 상태가 'RF 램프업 무출력 허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 RF 를 "
             "내려 끄고 공정을 중단한다."),
    _p("RF_FAIL_FWD_W", "rf", "RF 무출력 판정 순방향", "W", F, 1.0, 0.1, 50.0, safety=H,
       src="lib/config.py:568 (고정값)",
        desc="RF 램프업 중 DAC 가 'RF 무출력 판정 DAC' 이상인데 순방향이 이 값 이하인 상태가 'RF 램프업 무출력 허용' 시간 이어지면, 출력이 나오지 않는 것으로 보고 RF 를 "
             "내려 끄고 공정을 중단한다."),
    _p("RF_FAIL_SEC", "rf", "RF 램프업 무출력 허용", "s", F, 10.0, 3.0, 120.0, safety=H,
       v1_key="RF_FAIL_MAX_TICKS", v1_period=1.0, v1_default=10, desc="v1: 10틱 × 1 s"),
    _p("RF_REFP_ABORT_THRESHOLD", "rf", "RF 반사파 대기 기준", "W", F, 3.0, 0.5, 200.0, safety=H,
       v1_default=20.0, desc="반사파가 이 값 이상이면 대기 시작(전 구간)"),
    _p("RF_REFP_WAIT_SEC", "rf", "RF 반사파 대기 허용", "s", I, 60, 1, 600, safety=H, v1_default=15,
        desc="RF 반사파가 'RF 반사파 대기 기준' 이상이면 램프를 멈추고 기다리는데, 그 상태가 이 시간 이어지면 RF 를 램프다운해 끄고 공정을 중단한다. 파워 도달 대기 한도 계산에는 이 "
             "값의 3배를 더한다."),
    _p("STOP_RF_RAMPDOWN_TIMEOUT_SEC", "stop", "RF 램프다운 한도", "s", F, 120.0, 10.0, 600.0,
       src="controller/process_controller.py:392",
        desc="RF 램프다운이 걸릴 수 있는 한도다. 이 시간에서 1 초를 뺀 시간 안에 DAC 가 0 이 되지 않으면 바로 0 으로 쓰고 다음 단계로 넘어간다."),

    # ───────────── RF Pulse(CESAR 1310) ─────────────
    _p("RFPULSE_MAX_POWER", "rfpulse", "RF Pulse 출력 상한", "W", F, 600.0, 1.0, float(dev.CESAR_MAX_POWER_W),
       safety=H,
        desc="레시피·파워 켜기 블록·공정 중 수정에서 넣을 수 있는 RF Pulse 목표 파워의 상한이다. 드라이버도 이 값을 넘는 목표는 장비에 보내지 않는다."),
    _p("RFPULSE_PULSE_FREQ_MIN_HZ", "rfpulse", "펄스 주파수 하한", "Hz", I, 1, 1, 100,
       src="lib/config.py:613 (고정값)",
        desc="레시피·파워 켜기 블록·공정 중 수정에서 넣을 수 있는 펄스 주파수의 하한이다(화면 입력은 kHz, 장비에는 Hz). '펄스 주파수 상한'보다 작아야 한다."),
    _p("RFPULSE_PULSE_FREQ_MAX_HZ", "rfpulse", "펄스 주파수 상한", "Hz", I, 30000, 1, 100000,
        desc="레시피·파워 켜기 블록·공정 중 수정에서 넣을 수 있는 펄스 주파수의 상한이다(화면 입력은 kHz, 장비에는 Hz). 하한보다 커야 하고 CESAR 사양을 넘을 수 없다."),
    _p("RFPULSE_DUTY_MIN", "rfpulse", "펄스 듀티 하한", "%", I, 1, 1, 99, src="lib/config.py:614 (고정값)",
        desc="레시피·파워 켜기 블록·공정 중 수정에서 넣을 수 있는 펄스 듀티의 하한이다. '펄스 듀티 상한'보다 작아야 한다."),
    _p("RFPULSE_DUTY_MAX", "rfpulse", "펄스 듀티 상한", "%", I, 99, 1, 99, src="lib/config.py:615 (고정값)",
        desc="레시피·파워 켜기 블록·공정 중 수정에서 넣을 수 있는 펄스 듀티의 상한이다. '펄스 듀티 하한'보다 커야 한다."),
    _p("RFPULSE_PULSE_MIN_EDGE_US", "rfpulse", "펄스 최소 ON/OFF 시간", "µs", F, 16.0, 0.0, 1000.0,
       src="lib/config.py:617", desc="0 이면 검사하지 않는다. CESAR 1310 명령 96 규칙이다"),
    _p("RFPULSE_FORP_TOLERANCE_PERCENT", "rfpulse", "순방향 허용 오차", "%", F, 5.0, 0.5, 50.0, safety=H,
        desc="RF Pulse 출력 중 순방향과 목표의 차이가 목표의 이 비율 이상인 상태가 '순방향 이탈 지속 시간' 이어지면 RF Pulse 를 끄고 공정을 중단한다. 공정 중 목표를 바꾼 뒤 "
             "새 목표에 들어왔는지 볼 때도 이 비율을 쓴다."),
    _p("RFPULSE_FORP_SEC", "rfpulse", "순방향 이탈 지속 시간", "s", F, 15.0, 1.0, 120.0, safety=H,
       v1_key="RFPULSE_FORP_CONSECUTIVE_LIMIT", v1_period=5.0, v1_default=3, desc="v1: 3회 × RF Pulse 폴링 5 s"),
    _p("RFPULSE_REFP_LIMIT_WATTS", "rfpulse", "반사파 상한", "W", F, 20.0, 0.5, 200.0, safety=H,
        desc="RF Pulse 반사 파워가 이 값 이상인 상태가 '반사파 초과 지속 시간' 이어지면 RF Pulse 를 끄고 공정을 중단한다. RF Pulse 를 쓰는 공정 내내 본다."),
    _p("RFPULSE_REFP_SEC", "rfpulse", "반사파 초과 지속 시간", "s", F, 15.0, 1.0, 120.0, safety=H,
       v1_key="RFPULSE_REFP_CONSECUTIVE_LIMIT", v1_period=5.0, v1_default=3, desc="v1: 3회 × RF Pulse 폴링 5 s"),
    _p("RFPULSE_REFP_AUTO_OFF_SEC", "rfpulse", "CESAR 반사 전력 자동 차단 시간(명령 33)", "s", I, 20, 1, 255, safety=H,
       src="v2 (사용자 결정 2026-10-15)",
       desc="prod 연결 때 장비에 쓰고 되읽는다. PC 가 멈췄을 때의 보조 차단이므로 소프트웨어 판정보다 길게 둔다(RFPULSE_REFP_SEC)"),
    _p("RFPULSE_VERIFY_PULSE_CONFIG", "rfpulse", "펄스 설정 되읽기 불일치 시 중단", "", B, True,
       desc="false 면 경고만"),
    _p("RFPULSE_RAW_LOG", "rfpulse", "RF Pulse 프레임 원문 기록", "", B, True, safety=L, apply=LIVE,
       desc="로컬 파일에만 쓴다(v1 은 NAS 공정 로그에 썼다)"),
    _p("STOP_RFPULSE_OFF_TIMEOUT_SEC", "stop", "RF Pulse OFF 대기 한도", "s", F, 30.0, 5.0, 120.0,
       src="controller/process_controller.py:1980",
        desc="정지 순서에서 RF Pulse 끄기 결과를 기다리는 한도다. 넘으면 기다리지 않고 다음 단계(가스 유량 끄기)로 넘어간다."),

    # ───────────── 이온 게이지·베이스 압력(ig) ─────────────
    _p("IG_GAUGE_A_NAME", "ig", "게이지 A 이름", "", S, "게이지 A", safety=L, apply=LIVE,
       pattern=r".{1,20}", src="lib/config.py:736",
        desc="v2 장비 프로그램과 웹 화면에서는 사용하지 않는다(v1 에서 게이지 A 를 화면·기록에 보이던 이름)"),
    _p("IG_GAUGE_B_NAME", "ig", "게이지 B 이름", "", S, "게이지 B", safety=L, apply=LIVE,
       pattern=r".{1,20}", src="lib/config.py:737",
        desc="v2 장비 프로그램과 웹 화면에서는 사용하지 않는다(v1 에서 게이지 B 를 화면·기록에 보이던 이름)"),
    _p("IG_FILAMENT_SWITCH_QUIET_SEC", "ig", "필라멘트 전환 뒤 무응답 유예", "s", F, 12.0, 6.0, 60.0,
       safety=L, src="lib/config.py:746",
       desc="A 뒤 약 5.9 s·B 뒤 약 4.9 s 무응답(실측). 이 동안 품질 suppressed, 실패로 세지 않음"),
    _p("IG_FILAMENT_ON_TIMEOUT_SEC", "ig", "필라멘트 ON 결과 기한", "s", F, 20.0, 10.0, 120.0,
       src="lib/config.py:749",
        desc="필라멘트 켜기를 보낸 뒤 필라멘트와 방출이 모두 확인되기를 기다리는 기한이다. 넘으면 켜기 실패로 보며, 공정의 필라멘트 켜기 단계에서 실패하면 공정을 중단한다."),
    _p("IG_FILAMENT_OFF_TIMEOUT_SEC", "ig", "필라멘트 OFF 확인 기한", "s", F, 20.0, 10.0, 120.0,
       src="controller/process_controller.py:39 (드라이버 기한 20 s)",
        desc="필라멘트 끄기를 보낸 뒤 꺼짐이 확인되기를 기다리는 기한이다. 넘으면 끄기 실패로 기록하며, 공정은 경고만 남기고 계속한다."),
    _p("IG_SETTLE_SEC", "ig", "방출 뒤 판정 제외 시간", "s", F, 15.0, 0.0, 120.0, src="lib/config.py:753",
        desc="필라멘트 방출이 처음 확인된 뒤 이 시간 동안의 이온 압력은 베이스 압력 도달 판정에 쓰지 않는다. 0 이면 바로 판정한다."),
    _p("IG_VALUE_MAX_AGE_SEC", "ig", "베이스 판정에 쓰는 값 나이 상한", "s", F, 2.0, 1.0, 10.0,
       src="controller/process_controller.py:36",
        desc="v2 에서는 사용하지 않는다. 베이스 판정은 품질이 정상·늦음인 값만 쓴다(v1 에서는 이 나이보다 오래된 값을 베이스 판정에 쓰지 않았음)"),
    _p("IG_BASE_STABLE_SEC", "ig", "베이스 도달 지속 시간", "s", F, 5.0, 1.0, 600.0, src="lib/config.py:758",
       v1_key="IG_BASE_STABLE_COUNT", v1_period=1.0, v1_default=5, desc="이온 ≤ 목표가 이만큼 이어지면 도달. v1: 5회 × 1 s"),
    _p("IG_BASE_WAIT_TIMEOUT_SEC", "ig", "베이스 압력 대기 한도", "s", I, 1800, 60, 14400,
       desc="넘으면 공정 중단"),
    _p("IG_RELIGHT_LOCKOUT_SEC", "ig", "필라멘트 끈(B) 뒤 켜기(A) 금지 시간", "s", F, 5.0, 4.0, 60.0,
       src="device/IonGauge.py:377 (relight_lockout_sec)", desc="매뉴얼상 끈 뒤 4 s 안에는 다시 켤 수 없어 1 s 여유를 더했다"),
    _p("IG_MAX_RELIGHTS", "ig", "대기 중 필라멘트 재점등 횟수", "회", I, 2, 0, 10,
       src="controller/process_controller.py:37",
        desc="베이스 압력 대기 중 필라멘트가 꺼지거나 다시 켜기에 실패하면 이 횟수까지 다시 켠다. 횟수를 다 쓰면 공정을 중단하고, 0 이면 다시 켜지 않는다."),
    _p("IG_BASE_LOG_INTERVAL_SEC", "ig", "베이스 대기 기록 간격", "s", I, 5, 1, 600, safety=L, apply=LIVE,
       src="lib/config.py:766",
        desc="v2 에서는 사용하지 않는다(v1 에서 베이스 대기 진행 기록을 남기던 간격)"),
    _p("BASE_PRESSURE_MIN_TORR", "ig", "베이스 압력 입력 하한", "Torr", F, 1e-9, 1e-12, 1e-3,
       src="core/params.py:21",
        desc="레시피와 베이스 압력 대기 블록에 넣을 수 있는 목표 베이스 압력의 하한이다. '베이스 압력 입력 상한'보다 작아야 한다."),
    _p("BASE_PRESSURE_MAX_TORR", "ig", "베이스 압력 입력 상한", "Torr", F, 1e-3, 1e-9, 1e-2,
       src="core/params.py:22",
        desc="레시피와 베이스 압력 대기 블록에 넣을 수 있는 목표 베이스 압력의 상한이다. '베이스 압력 입력 하한'보다 커야 한다."),

    # ───────────── 히터 기본(heater) ─────────────
    _p("HEATER_WD_PERIOD_MS", "heater", "히터 워치독 하트비트 주기", "ms", I, 3000, 500, 3000, safety=H,
       desc="래더는 하트비트(D00014)가 TON 10 s 동안 바뀌지 않으면 히터를 끈다. TON 의 1/3 이하로 둔다. v1 은 이 주기(3 s)로 썼고 v2 는 매 tick(1 s)마다 "
            "쓰므로 이 값은 주기 상한 검사에만 쓴다(v2 방식 유지, 사용자 2026-10-20, X06)",
       note="v1 device/PLC.py:1110-1163 (3 s 주기)"),
    _p("HEATER_WD_DELAY_WARN_SEC", "heater", "하트비트 지연 경고", "s", F, 5.0, 1.0, 9.0, safety=L,
       src="device/PLC.py:119",
        desc="PC 하트비트 쓰기가 마지막으로 성공한 뒤 이 시간이 지나도록 다음 성공이 없으면 경고를 한 번 남기고, 회복되면 정보를 남긴다. 래더는 하트비트가 10 초 동안 바뀌지 않으면 히터를 "
             "끄므로 그보다 짧게 둔다."),
    _p("HEATER_MAX_TEMP", "heater", "목표 온도 입력 상한", "°C", F, 700.0, 0.0, 1100.0, safety=H,
       v1_default=500.0, desc="HEATER_SV_LIMIT_C 이하"),
    _p("HEATER_SOAK_TOLERANCE", "heater", "도달 판정 허용 편차", "°C", F, 2.0, 0.5, 20.0, v1_default=3.0,
        desc="공정의 히터 도달 판정에서 온도가 목표 ± 이 값 안에 들어와야 도달로 본다. TC2 단독 제어에서는 TC2 와 TC2 목표의 차이로, 그 밖의 제어 구조에서는 TC1 과 목표의 "
             "차이로 판정한다."),
    _p("HEATER_SOAK_TIME_SEC", "heater", "공정 레시피 도달 후 유지", "s", I, 60, 0, 3600,
        desc="공정의 히터 도달 판정에서 도달 허용 편차 안에 이 시간 동안 연속으로 머물러야 도달로 본다. 벗어나면 처음부터 다시 세며, 히터 레시피의 SOAK 시간과는 별개다."),
    # ── 히터 제어 구조(5-15 — v1 f188f28·b0a945a·3328872 lib/config.py, controller/heater_cascade) ──
    _p("HEATER_CTRL_MODE", "heater", "히터 제어 구조", "", E, "tc2", enum=("tc2", "cascade", "tc1"), safety=H,
       src="lib/config.py(6e0685b)",
       desc="tc2 = TC2 단독 제어(기본, 2026-11-02 사용자 결정): PLC PID 는 RUN 내내 TC2, SV2 = 매핑(TC1 기준값 r1) 뿐. "
            "TC1 은 기록·안내만(바깥 루프·학습 보정·TC1 탈락 경보·셔터 전 정착 없음) / "
            "cascade = 2단 제어(PID 는 TC2, 파이썬이 SV2 = 매핑(r1) + 보정. 학습 ff·적분 보정·TC1 탈락 경보·셔터 전 정착) / "
            "tc1 = 예전 TC1 직접 제어 + 유지 모드(롤백용). tc2·cascade 는 유지 모드(HEATER_HOLD_*)를 쓰지 않는다. "
            "목표 온도 숫자는 세 모드 모두 TC1 기준"),
    _p("HEATER_TC2_MAP", "heater", "TC1→TC2 기본 매핑", "°C", S,
       "25:25,50:92,100:170,150:226,200:290,250:365,300:437,400:569,500:705,600:846,700:982",
       pattern=r"\s*-?\d+(\.\d+)?\s*:\s*-?\d+(\.\d+)?\s*(,\s*-?\d+(\.\d+)?\s*:\s*-?\d+(\.\d+)?\s*)+",
       safety=H, src="lib/config.py(6e0685b)",
       desc="'TC1:TC2' 를 쉼표로 이은 것이다(v1 [[TC1, TC2], ...]). 점 사이는 선형이고 범위 밖은 끝 구간 기울기로 외삽하며, tc2·cascade 두 모드가 쓴다. "
            "2026-10-28~29 캐스케이드 3런의 평형 관계(300 °C = 수동 유지 실측 437)이다. 600 은 10-28~11-02 2단 제어 런 정상 상태 실측 중앙값 846 이고, "
            "700 은 실측이 없어 250~600 실측 직선(기울기 1.36)으로 외삽했다(v1 6e0685b)"),
    _p("HEATER_TC2_TC1_WARN_C", "heater", "TC2 단독: TC1 편차 안내 기준", "°C", F, 15.0, 3.0, 100.0, safety=L,
       src="lib/config.py(6e0685b)",
       desc="tc2 모드 기록용 안내(제어에 영향 없음): 셔터 닫힘·r1 도달·|TC2 − SV2| ≤ HEATER_SOAK_TOLERANCE 에서 "
            "|TC1 − 목표| 가 이 값을 넘은 채 HEATER_TC2_TC1_WARN_SEC 이상이면 경고·사건 1회(HEATER_TC2_TC1_DEVIATION). 공정은 계속"),
    _p("HEATER_TC2_TC1_WARN_SEC", "heater", "TC2 단독: TC1 편차 안내 지속", "s", F, 120.0, 30.0, 1800.0, safety=L,
       src="lib/config.py(6e0685b)",
        desc="TC2 단독 제어에서 셔터가 닫혀 있고 기준값이 목표에 닿았으며 TC2 가 TC2 목표 ± '도달 판정 허용 편차' 안일 때, TC1 편차가 'TC2 단독: TC1 편차 안내 기준'을 "
             "넘은 채 이 시간 이상 이어지면 경고를 한 번 남긴다. 제어와 공정에는 영향이 없다."),
    _p("HEATER_TC2_HOLDBACK_C", "heater", "TC2 단독: 홀드백 폭(TC1 눈금)", "°C", F, 20.0, 2.0, 100.0,
       src="lib/config.py(6e0685b)",
       desc="tc2 모드에서 (SV2 − TC2) 가 매핑 기울기 × 이 값보다 크면 r1(목표 램프)을 세운다. 래더 홀드백(cascade 의 TC1 홀드백)과는 별개이다. 10 이면 램프 중 "
            "TC2 추종 지연(14~27 °C)에 자주 걸려 600 °C 도달이 승온 한도를 넘었다(v1 시뮬레이터, HEATER_HOLDBACK_C·D00021)"),
    _p("HEATER_CASCADE_TI_SEC", "heater", "바깥 루프 적분 시간", "s", F, 600.0, 60.0, 7200.0, src="lib/config.py(f188f28)",
        desc="2단 제어(cascade)에서 TC1 오차를 TC2 목표 보정으로 쌓아 가는 적분 시간이다. 클수록 보정이 천천히 따라가며, 공정 승온 대기 한도에 이 값의 2배가 더해진다."),
    _p("HEATER_CASCADE_LAG_SEC", "heater", "TC1 비교 기준 지연", "s", F, 90.0, 0.0, 600.0, src="lib/config.py(f188f28)",
       desc="e = r1(t−LAG) − TC1(EMA 30 s)"),
    _p("HEATER_CASCADE_BIAS_RATE_MAX", "heater", "보정 최대 변화율", "°C/min", F, 3.0, 0.1, 60.0,
       src="lib/config.py(f188f28)", desc="TC2 기준"),
    _p("HEATER_CASCADE_BIAS_LIMIT_C", "heater", "보정 크기 상한", "°C", F, 150.0, 10.0, 500.0, src="lib/config.py(f188f28)",
        desc="2단 제어(cascade)에서 적분 보정이 커질 수 있는 한도(±, TC2 눈금)다. 보정이 이 값에 닿으면 더 늘리지 않는다."),
    _p("HEATER_CASCADE_BIAS_ALARM_C", "heater", "TC1 탈락 의심 보정 크기", "°C", F, 100.0, 10.0, 500.0,
       src="lib/config.py(f188f28)", desc="|보정| 이 이 값 이상으로 60 s 지속되면 TC1 탈락 의심 경보"),
    _p("HEATER_CASCADE_ALARM_ACTION", "heater", "TC1 탈락 의심 시", "", E, "abort", enum=("abort", "warn"), safety=H,
       src="lib/config.py(f188f28)", desc="abort = 공정 소유면 공정 중단·히터 레시피면 레시피 중단·수동이면 경고 / warn = 경고만"),
    _p("HEATER_SHUTTER_SETTLE_TOL_C", "heater", "셔터 전 정착 허용 폭", "°C", F, 2.0, 0.5, 10.0, src="lib/config.py(3328872)",
       desc="메인 셔터 열기 직전 TC1 이 목표 ±이 값 안에 정해진 시간 동안 이어지면 연다. 2단 제어에서만 쓰고 tc2·tc1 에서는 쓰지 "
            "않는다(HEATER_SHUTTER_SETTLE_SEC, HEATER_CTRL_MODE=cascade)"),
    _p("HEATER_SHUTTER_SETTLE_SEC", "heater", "셔터 전 정착 연속 시간", "s", F, 60.0, 10.0, 600.0,
       src="lib/config.py(3328872)", desc="연속 시간은 승온 대기를 통과한 뒤부터 센다. 이미 맞으면 바로 통과한다"),
    _p("HEATER_SHUTTER_SETTLE_MAX_SEC", "heater", "셔터 전 정착 대기 상한", "s", F, 900.0, 0.0, 3600.0,
       src="lib/config.py(3328872)", desc="0 이면 기본 템플릿에 이 단계를 넣지 않는다"),
    _p("HEATER_SHUTTER_SETTLE_FAIL_ACTION", "heater", "셔터 전 정착 상한 초과 시", "", E, "proceed",
       enum=("proceed", "abort"), safety=H, src="lib/config.py(3328872)",
       desc="proceed = 경고 후 셔터 열고 진행 / abort = 알리고 공정 중단"),
    _p("HEATER_WAIT_TIMEOUT_SEC", "heater", "승온 대기 최소 한도", "s", I, 5400, 600, 43200, v1_default=3600,
       desc="실제 한도 = min(max(이 값, 예상 램프 + MARGIN), MAX)"),
    _p("HEATER_WAIT_TIMEOUT_MARGIN_SEC", "heater", "승온 대기 여유", "s", F, 1800.0, 0.0, 14400.0,
       src="controller/process_controller.py:26, heater_recipe.py:54 (중복 정의)",
        desc="승온 대기 한도를 정할 때 예상 램프 시간에 더하는 여유다. 한도는 '승온 대기 최소 한도'와 예상 램프 + 이 값 중 큰 값(절대 상한 이하)이며, 공정 승온 대기와 히터 레시피 "
             "스텝에 쓴다."),
    _p("HEATER_WAIT_TIMEOUT_MAX_SEC", "heater", "승온 대기 절대 상한", "s", F, 43200.0, 600.0, 86400.0,
       safety=H, src="controller/process_controller.py:27, heater_recipe.py:55 (중복 정의)",
        desc="예상 램프 시간으로 계산한 승온 대기 한도의 절대 상한이다. 한도를 넘으면 공정은 승온 대기에서 중단하고, 히터 레시피는 그 스텝에서 레시피를 "
             "중단한다(HEATER_WAIT_TIMEOUT)."),
    _p("HEATER_RAMP_DONE_TOL_C", "heater", "램프 완료 판정 폭", "°C", F, 0.15, 0.05, 5.0, safety=L,
       src="controller/process_controller.py:43, heater_recipe.py:59",
        desc="램프가 끝났는지 볼 때 쓰는 폭이다. TC1 직접 제어에서는 래더 램프 중간 목표가, 2단·TC2 단독 제어에서는 기준값의 최종 목표가 목표와 이 값 이하로 차이 나야 램프 완료로 "
             "보며, 공정 도달 판정과 히터 레시피의 RAMP 에서 SOAK 로 넘어가는 판정에 쓴다."),
    _p("HEATER_TC_BAD_LIMIT_SEC", "heater", "TC 이상 지속 시 레시피 중단", "s", F, 60.0, 5.0, 600.0,
       safety=H, src="controller/heater_recipe.py:47",
        desc="히터 레시피 중 TC 이상(TC 이상 비트 또는 TC1 값 없음)이 이 시간 이어지면 레시피를 중단하고 히터를 끈다. 공정이 같이 돌고 있으면 공정도 중단한다."),
    _p("HEATER_OUT_DEAD_LIMIT_SEC", "heater", "출력 죽음 지속 시 레시피 중단", "s", F, 10.0, 3.0, 600.0,
       safety=H, src="controller/heater_recipe.py:50",
        desc="히터 레시피 중 히터가 켜져 있고 이상이 없는데 출력(DAC)이 '출력 0% DAC'보다 낮은 상태가 이 시간 이어지면 레시피를 중단하고 히터를 끈다. 공정이 같이 돌고 있으면 공정도 "
             "중단한다."),
    _p("HEATER_OUT_DEAD_GRACE_SEC", "heater", "출력 죽음 판정 유예", "s", F, 5.0, 0.0, 60.0,
       src="device/PLC.py:49",
        desc="히터를 켠 직후 이 시간 동안은 출력 죽음 경고 판정을 하지 않는다. 히터 레시피의 출력 죽음 중단 판정에는 이 유예를 두지 않는다."),
    _p("HEATER_OUT_DEAD_SEC", "heater", "출력 죽음 지속 시간", "s", F, 0.6, 0.25, 60.0, src="device/PLC.py:1040",
       v1_period=0.2, v1_default=3, desc="v1: 3회 × PLC 폴링 200 ms(유예 HEATER_OUT_DEAD_GRACE_SEC 뒤)"),
    _p("HEATER_RUN_OFF_LIMIT_SEC", "heater", "외부 RUN OFF 지속 시 레시피 중단", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_recipe.py:63",
        desc="히터 레시피 중 히터 운전(RUN)이 꺼진 상태가 이 시간 이어지면 외부에서 꺼진 것으로 보고 레시피를 중단한다. 공정이 같이 돌고 있으면 공정도 중단한다."),
    _p("HEATER_GAS_HOLD_RELEASE_C", "heater", "냉각 보호 가스 해제 온도", "°C", F, 100.0, 30.0, 400.0,
       safety=H, desc="히터를 끌 때 PV 가 이보다 높으면 가스·압력을 유지하고, 식으면 해제한다. PV 를 모르면 해제하지 않는다"),
    _p("HEATER_STALE_SEC", "heater", "히터 값 stale 표시", "s", F, 5.0, 1.0, 60.0, safety=L, apply=LIVE,
       desc="이만큼 PLC 값이 없으면 화면 회색·히터 버튼 잠금"),
    _p("HEATER_COOLDOWN_TARGET_C", "heater", "냉각 스텝 기준", "°C", F, 30.0, 0.0, 100.0, safety=L,
       desc="마지막 스텝 목표가 이 값 이하면 냉각 스텝(도달 판정 없이 시간만)"),
    _p("HEATER_CURRENT_MV_ZERO", "heater", "전류 추정: 출력 0 DAC", "count", I, 400, 0, 4000, safety=L,
       apply=LIVE, desc="표시 전용"),
    _p("HEATER_CURRENT_MV_FULL", "heater", "전류 추정: 전출력 DAC", "count", I, 1600, 0, 4000, safety=L,
       apply=LIVE, desc="표시 전용"),
    _p("HEATER_CURRENT_SCALE", "heater", "전류 추정: 전출력 전류", "A", F, 135.1, 1.0, 500.0, safety=L,
       apply=LIVE, desc="표시 전용"),

    # ───────────── 히터: PLC 로 쓰는 한계값(heater_plc) — 적용 시 쓰고 되읽어 확인 ─────────────
    _p("HEATER_MV_MIN", "heater_plc", "출력 0% DAC", "count", I, 400, 320, 800, safety=H,
       desc="PLC K1227 과 같아야 함(VSCD-30 0.8 V = 320)"),
    _p("HEATER_MV_ABS_MAX", "heater_plc", "DAC 절대 상한", "count", I, 1300, 420, dev.HEATER_DAC_PHYS_MAX,
       safety=H, desc="코드가 넘지 않는 상한. 물리 최대 1600(4 V)"),
    _p("HEATER_MV_LIMIT", "heater_plc", "D00018 운전 상한", "count", I, 1200, 420, dev.HEATER_DAC_PHYS_MAX,
       safety=H, desc="MV_MIN+20 ~ MV_ABS_MAX"),
    _p("HEATER_SV_LIMIT_C", "heater_plc", "D00013 목표 상한", "°C", F, 700.0, 0.0, 1100.0, safety=H,
       v1_default=500.0,
        desc="PLC 래더의 목표 온도 상한으로, 연결·재기동·설정 적용 때 쓰고 되읽어 확인한다. 히터 목표 명령은 PLC 가 보고한 이 상한과 '목표 온도 입력 상한' 중 작은 값을 넘으면 "
             "거부된다(D00013)."),
    _p("HEATER_RAMP_RATE_C_PER_MIN", "heater_plc", "D00020 램프 속도·기본 램프", "°C/min", F, 6.0, 6.0, 60.0,
       safety=H, v1_default=12.0,
       desc="래더 분해능은 6 °C/min(1 카운트/초)이다. 기본 6 은 장비와 맞춘 값이다(v1 bb2b0a3, 캐스케이드 실기 시험 조건)"),
    _p("HEATER_HOLDBACK_C", "heater_plc", "D00021 홀드백", "°C", F, 10.0, 0.5, 50.0, safety=H, v1_default=2.0,
        desc="PLC 래더의 홀드백 값으로, 연결·재기동·설정 적용 때 쓰고 되읽어 확인한다. 2단 제어(cascade)에서는 TC1 이 지연을 감안한 기준값보다 이 값 이상 처지면 기준값 상승을 "
             "멈춘다(D00021)."),
    _p("HEATER_OT_LIMIT_C", "heater_plc", "D00026 TC1 과온 트립", "°C", F, 750.0, 50.0, 1130.0, safety=H,
       v1_default=550.0, desc="SV_LIMIT+20 이상으로 둔다(권장 +50). 래치되므로 풀려면 이상 리셋을 해야 한다"),
    _p("HEATER_OT2_LIMIT_C", "heater_plc", "D00034 TC2 과온 트립", "°C", F, 1150.0, 70.0,
       dev.HEATER_OT2_LADDER_MAX_C, safety=H, desc="OT+20 이상, 래더 상한 1150"),
    _p("HEATER_SLOW_ZONE_C", "heater_plc", "D00027 감속 구간", "°C", F, 10.0, 10.0, 10.0, safety=L,
       note="래더가 10.0 으로 강제 — 되읽기 일치용",
        desc="PLC 래더의 감속 구간에 쓰는 값이다. 래더가 10.0 °C 로 고정하므로 되읽기 확인이 맞도록 10 으로만 둘 수 있다(D00027)."),
    _p("HEATER_SLOW_RATE_C_PER_MIN", "heater_plc", "D00028 감속 속도", "°C/min", F, 60.0, 6.0, 600.0,
       v1_default=6.0, desc="60 으로 열어 래더가 속도를 막지 않게 한다. 실제 램프는 파이썬 감속 접근이 정한다"),

    # ───────────── 히터 감속 접근(heater_ramp) ─────────────
    _p("HEATER_APPROACH_ZONE_C", "heater_ramp", "목표 직전 감속 구간", "°C", F, 20.0, 0.0, 200.0,
        desc="목표까지 남은 온도가 이 값보다 작아지면 승온 속도를 남은 거리에 비례해 줄여, 목표에 닿을 때 '도착 속도'가 되게 한다. 0 이면 감속하지 않고, 클수록 승온 시간이 길어진다."),
    _p("HEATER_APPROACH_MIN_RATE_C_PER_MIN", "heater_ramp", "도착 속도", "°C/min", F, 1.0, 0.1, 6.0,
        desc="목표 직전 감속 구간 끝(목표에 닿는 순간)의 승온 속도다. 작을수록 목표 직전에 더 천천히 다가가 승온 시간이 길어진다."),
    _p("HEATER_APPROACH_LEAD_C", "heater_ramp", "SV 앞서기 폭", "°C", F, 1.0, 0.2, 10.0,
        desc="TC1 직접 제어에서 프로그램이 PLC 에 쓰는 목표가 래더 램프 중간 목표보다 이 값 넘게 앞서지 않게 한다(D00012·D00019)."),

    # ───────────── 히터 유지 모드·포화(heater_hold) ─────────────
    _p("HEATER_HOLD_MODE", "heater_hold", "도달 후 유지 모드", "", E, "tc2", enum=("off", "dac", "tc2"),
       safety=H, v1_default="off", desc="dac=D00018 을 그때 출력으로 고정 / tc2=TC2 추종 제어"),
    _p("HEATER_HOLD_MV_ENTER_TOL_C", "heater_hold", "유지 진입 허용 |PV-SV|", "°C", F, 3.0, 0.5, 20.0,
        desc="유지 모드 진입을 재는 동안 TC1 과 목표의 차이가 이 값 이하여야 표본을 모으며, 벗어나면 모은 표본을 버리고 다시 잰다. 히터 제어 구조가 TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_MV_ENTER_SEC", "heater_hold", "유지 진입 안정 시간", "s", F, 60.0, 10.0, 600.0,
        desc="진입 허용 범위 안의 표본을 이 시간 동안 모은 뒤 출력·온도 변화가 작으면, 그 평균 출력(DAC 고정) 또는 평균 TC2(TC2 추종)로 유지 모드에 들어간다. 히터 제어 구조가 "
             "TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_MV_ARRIVE_TOL_C", "heater_hold", "한 번은 들어와야 하는 범위", "°C", F, 1.0, 0.2, 10.0,
        desc="램프가 끝난 뒤 TC1 이 목표 ± 이 값 안에 한 번은 들어와야 유지 모드 측정을 시작하며, 진입 허용 범위보다 클 수 없다. 히터 제어 구조가 TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_MV_DRIFT_PV_C", "heater_hold", "PV 드리프트 하한", "°C", F, 0.5, 0.1, 10.0, safety=L,
        desc="유지 모드 측정 창의 앞 절반과 뒤 절반의 TC1 평균 차이가 허용값을 넘으면 아직 정착 전으로 보고 다시 잰다. 허용값은 이 값과 진입 허용 범위 × 'PV 드리프트 비율' 중 큰 "
             "값이며, TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_MV_DRIFT_MV", "heater_hold", "MV 드리프트 하한", "count", F, 15.0, 3.0, 200.0, safety=L,
        desc="유지 모드 측정 창의 앞 절반과 뒤 절반의 평균 출력(DAC) 차이가 허용값을 넘으면 아직 정착 전으로 보고 다시 잰다. 허용값은 이 값과 출력 운전 상한 × 'MV 드리프트 비율' "
             "중 큰 값이며, TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_TC2_MARGIN_C", "heater_hold", "TC2 목표 마진(OT2 − 이 값)", "°C", F, 50.0, 20.0, 500.0,
       safety=H,
        desc="TC2 목표의 상한을 TC2 과온 트립 온도보다 이 값만큼 낮게 잡는다. 2단·TC2 단독 제어와 TC2 추종 유지 모드 모두 TC2 목표를 이 상한 아래로 제한한다."),
    _p("HEATER_HOLD_TC2_DRIFT_C", "heater_hold", "TC2 드리프트 하한", "°C", F, 1.0, 0.1, 20.0, safety=L,
        desc="TC2 추종 유지 모드 측정 창의 앞 절반과 뒤 절반의 TC2 평균 차이가 허용값을 넘으면 아직 정착 전으로 보고 다시 잰다. 허용값은 이 값과 창의 TC2 평균 × 'TC2 드리프트 "
             "비율' 중 큰 값이며, TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_DRIFT_PV_REL", "heater_hold", "PV 드리프트 비율(×ENTER_TOL)", "비율", F, 0.5, 0.05, 1.0,
       safety=L, src="controller/heater_hold.py:38",
        desc="유지 모드의 TC1 드리프트 허용값을 진입 허용 범위 × 이 비율과 'PV 드리프트 하한' 중 큰 값으로 정한다. 히터 제어 구조가 TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_DRIFT_MV_REL", "heater_hold", "MV 드리프트 비율(×MV_LIMIT)", "비율", F, 0.05, 0.005, 0.5,
       safety=L, src="controller/heater_hold.py:39",
        desc="유지 모드의 출력 드리프트 허용값을 출력 운전 상한 × 이 비율과 'MV 드리프트 하한' 중 큰 값으로 정한다. 히터 제어 구조가 TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_DRIFT_TC2_REL", "heater_hold", "TC2 드리프트 비율", "비율", F, 0.02, 0.001, 0.5,
       safety=L, src="controller/heater_hold.py:40",
        desc="TC2 추종 유지 모드의 TC2 드리프트 허용값을 측정 창의 TC2 평균 × 이 비율과 'TC2 드리프트 하한' 중 큰 값으로 정한다. 히터 제어 구조가 TC1 직접 제어일 때만 "
             "쓴다."),
    _p("HEATER_HOLD_ENGAGE_TIMEOUT_SEC", "heater_hold", "유지 진입 단계 대기", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_hold.py:44", note="v1 주석: 래더 스캔 지연 미확인 — bench 실측",
        desc="TC2 추종 유지 모드에 들어갈 때 TC2 목표 되읽기와 래더의 TC2 제어 확인을 단계마다 기다리는 한도다. 넘으면 TC2 추종을 취소하고 TC1 제어로 되돌리며, TC1 직접 "
             "제어일 때만 쓴다."),
    _p("HEATER_HOLD_REAPPLY_SEC", "heater_hold", "되돌아간 값 재적용 대기", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_hold.py:45",
        desc="유지 모드 중 PLC 에서 읽은 고정값(출력 상한 또는 TC2 목표)이 프로그램이 정한 값과 다르면 이 시간이 지난 뒤 다시 쓴다. 히터 제어 구조가 TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_HOLD_WAIT_SEC", "heater_hold", "공정의 유지 진입 대기 한도", "s", F, 600.0, 0.0, 7200.0,
        desc="공정의 히터 도달·유지 대기에서 목표 도달 뒤 유지 모드(2단·TC2 단독 제어에서는 TC2 제어 진입)를 기다리는 한도다. 넘으면 TC1 직접 제어는 완화 TC2 추종과 DAC "
             "고정을 차례로 시도하고, 그래도 안 되거나 2단·TC2 단독 제어이면 '유지 진입 최종 실패 시' 설정을 따른다."),
    _p("HEATER_HOLD_FAIL_ACTION", "heater_hold", "유지 진입 최종 실패 시", "", E, "abort",
       enum=("abort", "proceed"), safety=H, desc="완화 tc2 → dac 폴백을 먼저 시도한 뒤에도 실패하면"),
    _p("HEATER_TC1_DEV_WARN_C", "heater_hold", "TC1 편차 경고 기준", "°C", F, 120.0, 10.0, 500.0, safety=L,
       src="controller/heater_hold.py:49",
        desc="TC2 추종 유지 중 TC1 과 목표의 차이가 이 값을 넘은 채 'TC1 편차 경고 지속' 시간이 지나면 안내를 남기고 추종은 그대로 유지한다. 히터 제어 구조가 TC1 직접 제어일 "
             "때만 쓴다."),
    _p("HEATER_TC1_DEV_WARN_SEC", "heater_hold", "TC1 편차 경고 지속", "s", F, 300.0, 10.0, 3600.0, safety=L,
       src="controller/heater_hold.py:50",
        desc="TC2 추종 유지 중 TC1 편차가 'TC1 편차 경고 기준'을 넘은 채 이 시간 이어지면 안내를 남긴다. 추종은 그대로 유지하며, TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_TC1_DEV_OK_C", "heater_hold", "TC1 편차 정상 기준", "°C", F, 10.0, 1.0, 100.0, safety=L,
       src="controller/heater_hold.py:51",
        desc="TC1 편차 안내를 남긴 뒤 편차가 이 값 이하로 줄면 정상 복귀로 기록하고 다시 셀 수 있게 한다. 히터 제어 구조가 TC1 직접 제어일 때만 쓴다."),
    _p("HEATER_MV_SAT_REL", "heater_hold", "포화 판정 비율(×MV_LIMIT)", "비율", F, 0.98, 0.8, 1.0,
       src="controller/heater_hold.py:42, heater_saturation.py:29",
        desc="히터 출력(DAC)이 출력 상한의 이 비율 이상이면 포화로 보고, 그 상태가 'DAC 포화 판정 시간' 이어지면 출력 상한을 낮추고 경고한다. 유지 모드 진입 때 평균 출력이 상한의 "
             "이 비율 이상이어도 DAC 고정을 하지 않는다."),
    _p("HEATER_MV_SAT_SEC", "heater_hold", "DAC 포화 판정 시간", "s", F, 120.0, 10.0, 3600.0,
       desc="v1 은 10 미만이면 120 으로 되돌렸지만 v2 는 범위 밖 값을 거부한다"),
    _p("HEATER_SAT_PRE_WINDOW_SEC", "heater_hold", "포화 직전 평균 창", "s", F, 60.0, 10.0, 600.0,
       src="controller/heater_saturation.py:31",
        desc="DAC 포화로 출력 상한을 낮출 때, 포화가 시작되기 직전 이 시간 동안의 평균 출력을 새 상한으로 쓴다. 낮춘 상한은 운전 OFF·목표 변경 때 원래대로 돌아간다."),
    _p("HEATER_SAT_FALLBACK_REL", "heater_hold", "포화 직전 이력 없을 때 클램프 비율", "비율", F, 0.9, 0.5, 1.0,
       src="controller/heater_saturation.py:30",
        desc="DAC 포화로 상한을 낮출 때 포화 직전 출력 기록이 없으면, 출력 운전 상한 × 이 비율을 새 상한으로 쓴다."),

    # ───────────── 히터 레시피·히터 가스(heater_recipe / heater_gas) ─────────────
    _p("HEATER_RECIPE_MAX_STEPS", "heater_recipe", "히터 레시피 최대 스텝", "개", I, 20, 1, 100, safety=L,
        desc="히터 레시피에 넣을 수 있는 스텝 수의 상한이다. 넘으면 레시피를 받지 않는다."),
    _p("HEATER_RECIPE_MAX_REPEAT", "heater_recipe", "히터 레시피 반복 상한", "회", I, 1, 1, 100, src="controller/heater_recipe.py",
       note="v1 과 같음(사용자 확정 2026-10-20): repeat 빈칸·1 만 허용(반복 없음)",
        desc="히터 레시피 스텝의 반복(repeat) 칸에 넣을 수 있는 상한이다. 지금 프로그램은 스텝을 되풀이하지 않으며, 이 값을 넘는 반복이 있으면 레시피를 받지 않는다."),
    _p("HEATER_RECIPE_HOLD_AT_END", "heater_recipe", "레시피 끝 온도 유지", "", B, False, src="controller/heater_recipe.py",
       desc="false(v1 과 같음, 사용자 확정 2026-10-20): 끝나면 히터를 끈다. true 면 마지막 목표 유지(무인 가열 주의)"),
    _p("HEATER_RECIPE_RAMP_MAX_C_PER_MIN", "heater_recipe", "레시피 램프 상한", "°C/min", F, 60.0, 6.0, 600.0,
       src="core/params.py:364 (공정 레시피 heater_ramp 60 초과 오류)",
       note="v1 히터 레시피에는 상한이 없다 — v2 는 공정 레시피와 같은 60 °C/min 을 히터 레시피에도(v2 방식 유지, 2026-10-20)",
        desc="승온 속도로 넣을 수 있는 상한이다. 히터 레시피 스텝, 공정 레시피의 히터 램프 칸, 히터 승온 블록, 공정 중 승온 속도 수정에 모두 쓴다."),
    _p("HEATER_GAS_SP1_TIMEOUT_SEC", "heater_gas", "히터 가스 SP1 도달 한도", "s", F, 300.0, 30.0, 1800.0,
       src="controller/heater_atmosphere.py:55",
        desc="히터 가스를 준비할 때 챔버 압력이 정한 압력에 도달하기를 기다리는 한도다. 넘으면 가스 준비 실패로 보고 유량 끄기·MFC 밸브 열기·차단밸브 닫기를 하며 히터는 켜지 않는다."),
    _p("HEATER_GAS_MFC_TIMEOUT_SEC", "heater_gas", "히터 가스 MFC 명령 한도", "s", F, 60.0, 5.0, 600.0,
       src="controller/heater_atmosphere.py:43",
        desc="v2 에서는 사용하지 않는다. 히터 가스의 MFC 명령 한도는 프로그램에 정한 60 s 가 대신한다(v1 heater_atmosphere MFC_TIMEOUT_SEC, "
             "process_steps.MFC_CMD_GUARD_S)"),

    # ───────────── 공정 시퀀스(process) ─────────────
    _p("PROCESS_PAUSE_MAX_SEC", "process", "공정 일시정지 한도", "s", F, 1800.0, 0.0, 86400.0, src="v2",
       desc="공정 중 일시정지(PROCESS_PAUSE)가 이만큼 이어지면 정지 순서(PAUSE_TIMEOUT). 5분 전 경고. 0 = 한도 없음(5-15)"),
    _p("PRE_POWER_OFF_SETTLE_SEC", "process", "시작 전 전원 OFF 뒤 대기", "s", F, 1.0, 0.0, 30.0, safety=L,
       src="controller/process_controller.py:519",
        desc="시작 전 정리에서 DC·RF·RF Pulse 를 끈 뒤 다음 단계로 가기 전에 기다리는 시간이다. 정수 초로 반올림하며, 블록 값을 비우면 이 값을 쓴다."),
    _p("GAS_STABILIZE_SEC", "process", "SP4 ON 뒤 압력 안정화", "s", F, 60.0, 0.0, 600.0,
       src="controller/process_controller.py:577",
        desc="기본 템플릿에서 가스를 넣고 가스 안정화 설정점을 고른 뒤 압력이 자리 잡도록 기다리는 시간이다(정수 초로 반올림). 이 동안 압력 도달 판정은 하지 않는다."),
    _p("SP3_MTORR", "process", "압력 1단계(SP3)", "mTorr", F, 15.0, 1.0, 100.0,
       src="controller/process_controller.py:619",
       desc="WP < 이 값일 때만 거친다. 값 출처(SP3_SOURCE)가 프로그램 값일 때 장비에 쓰는 값(장비 값이면 장비에서 읽은 값으로 비교)"),
    _p("SP2_MTORR", "process", "압력 2단계(SP2)", "mTorr", F, 5.0, 0.5, 100.0,
       src="controller/process_controller.py:623",
       desc="WP < 이 값일 때만 거친다. 값 출처(SP2_SOURCE)가 프로그램 값일 때 장비에 쓰는 값"),
    _p("SP5_MTORR", "process", "압력 0단계(SP5)", "mTorr", F, 50.0, 1.0, 1000.0,
       src="controller/process_controller.py (a78b5ce — 고정값 50.0)",
       desc="WP > 이 값일 때만 거친다: SP4 → 파워 → SP5(이 값) 설정·선택 → SP5_STABILIZE_SEC 고정 대기 → SP1(WP). "
            "값 출처(SP5_SOURCE)가 프로그램 값일 때 장비에 쓰는 값(장비 값이면 장비에서 읽은 값으로 비교)"),
    _p("SP5_STABILIZE_SEC", "process", "SP5 ON 뒤 고정 대기", "s", F, 60.0, 0.0, 600.0,
       src="controller/process_controller.py (f5780ca — 고정값 60 s)",
       desc="SP5 단계는 압력 도달 판정 없이 이 시간만 기다린다"),
    # 압력 설정점 값 출처(5-19 ③) — 단계마다 프로그램 값(SPn_MTORR 를 장비에 쓰고 되읽음)을 쓸지, 장비에 저장된 값을 그대로 쓸지.
    # 기본값은 v1 과 같게: SP4 = 장비 값(v1 은 D4 만), SP2·SP3·SP5 = 프로그램 값(v1 이 값을 써서 확인). 장비 값이면 기본 템플릿이
    # 그 단계를 set=false·값 비움으로 만들고, 단계를 넣을지 비교(WP < SP3·WP < SP2·WP > SP5)도 장비에서 읽은 값으로 한다.
    *(_p(f"SP{n}_SOURCE", "process", f"SP{n} 값 출처", "", E, "device" if n == 4 else "program",
         enum=("program", "device"), enum_labels=(("program", "프로그램 값"), ("device", "장비 값")),
         src="controller/process_controller.py (v1 — SP4 는 D4 만, SP2·SP3·SP5 는 값을 써서 확인)" if n != 4 else
             "controller/process_controller.py (v1 — 가스 안정화 SP4 ON 은 D4 만)",
         desc=f"프로그램 값은 설정 값을 장비에 쓰고 되읽어 확인한 뒤 고른다. 장비 값은 쓰지 않고 장비에 저장된 SP{n} 을 고르며, "
              "읽은 값을 목표와 비교에 쓴다(읽지 못하면 시작 거부). 기본은 v1 과 같다"
              + ("(v1 은 SP4 를 쓰지 않음)" if n == 4 else "") + f"(SP{n}_MTORR)") for n in (2, 3, 4, 5)),
    _p("SP4_MTORR", "process", "가스 안정화 설정점(SP4)", "mTorr", F, 30.0, 0.5, 1000.0,
       src="v2 (10/20 bench 장비 저장값 S4 3.00 %FS = 30 mTorr — v1 은 S4 를 쓰지 않음)",
       desc="값 출처가 프로그램 값일 때 기본 템플릿의 가스 안정화 단계가 S4 로 쓰고 R4 로 되읽은 뒤 D4 로 고른다. 기본은 장비 값이다(v1 과 같게 D4 만, SP4_SOURCE)"),
    _p("PRESSURE_WAIT_TOL_RATIO", "process", "압력 도달 허용 비율", "비율", F, 0.10, 0.01, 0.5,
       src="controller/process_controller.py:105",
        desc="압력 도달 판정에서 챔버 압력이 목표 ± 목표 × 이 비율 안에 '압력 도달 지속 시간' 동안 머물면 도달로 본다. 압력 블록의 허용 범위를 비우면 이 값을 쓴다."),
    _p("PRESSURE_WAIT_STABLE_SEC", "process", "압력 도달 지속 시간", "s", F, 6.0, 1.0, 120.0,
       src="controller/process_controller.py:106, device/MFC.py:864-900", v1_period=2.0, v1_default=3,
       desc="v1: 3회 × (폴 간격 1 s + 명령 간격 1 s)"),
    _p("PRESSURE_STAGE_TIMEOUT_SEC", "process", "SP3·SP2 도달 한도(넘어도 진행)", "s", F, 180.0, 10.0, 1800.0,
       src="controller/process_controller.py:107", note="v1 은 틱 수로 세어 실제 2배 이상 걸렸다 — v2 는 실제 시각",
        desc="작업 압력이 아닌 단계 압력 블록이 압력 도달을 기다리는 기본 한도다. 넘으면 기본으로 경고를 남기고 다음으로 진행하며, 블록에 최대 대기·시간 초과 시 동작을 넣으면 그 값을 "
             "따른다."),
    _p("SP1_TIMEOUT_SEC", "process", "SP1(WP) 도달 한도(넘으면 중단)", "s", F, 300.0, 30.0, 3600.0,
       src="controller/process_controller.py:628",
        desc="작업 압력 블록이 압력 도달을 기다리는 기본 한도다. 넘으면 기본으로 공정을 중단하며, 블록에 최대 대기·시간 초과 시 동작을 넣으면 그 값을 따른다."),
    _p("POWER_WAIT_TIMEOUT_SEC", "process", "파워 도달 대기 최소 한도", "s", F, 600.0, 60.0, 7200.0,
        desc="파워 켜기 블록의 최대 대기를 비웠을 때 쓰는 목표 도달 대기 한도의 최소값이다. 실제 한도는 예상 도달 시간 × 배수 + 여유와 이 값 중 큰 값(절대 상한 이하)이며, 넘으면 "
             "공정을 중단한다."),
    _p("ADJUST_SETTLE_MAX_SEC", "process", "공정 중 수정 뒤 이탈 판정 쉼 상한", "s", F, 120.0, 10.0, 3600.0, src=_V2,
       desc="공정 중 수정으로 DC·RF Pulse 목표를 바꾸면 새 목표에 다시 도달할 때까지 이탈 판정을 쉰다. 이 시간이 지나면 도달하지 못해도 다시 켜고 경고를 남긴다(S31C·S34A, "
            "ADJUST_SETTLE_TIMEOUT)"),
    _p("POWER_WAIT_TIMEOUT_MAX_SEC", "process", "파워 도달 대기 절대 상한", "s", F, 7200.0, 600.0, 14400.0,
       safety=H, src="lib/config.py:676 (고정값)",
        desc="파워 도달 대기 한도의 절대 상한이다. 예상 시간으로 계산한 한도가 이 값을 넘으면 이 값을 쓴다."),
    _p("POWER_WAIT_EST_FACTOR", "process", "파워 대기 예상 시간 배수", "배", F, 1.5, 1.0, 5.0, safety=L,
       src="controller/process_controller.py:997",
        desc="파워 도달 대기 한도를 계산할 때 예상 도달 시간에 곱하는 배수다. 예상 도달 시간은 DC·RF·RF Pulse 중 가장 오래 걸리는 장비의 예상 램프 시간이다."),
    _p("POWER_WAIT_EST_MARGIN_SEC", "process", "파워 대기 예상 여유", "s", F, 120.0, 0.0, 1800.0, safety=L,
       src="controller/process_controller.py:997",
        desc="파워 도달 대기 한도를 계산할 때 예상 도달 시간 × 배수에 더하는 여유다. 클수록 한도가 길어진다."),

    # ───────────── 정지(stop) ─────────────
    _p("STOP_FLOW_OFF_TIMEOUT_SEC", "stop", "STOP 유량 OFF 확인 한도(채널당)", "s", F, 6.0, 2.0, 60.0,
       src="controller/process_controller.py:31",
        desc="STOP·ALL STOP 정지 순서의 가스 유량 끄기 단계에서 유량 꺼짐 확인을 기다리는 한도다. 넘으면 기다리지 않고 다음 단계로 넘어간다."),
    _p("STOP_EXHAUST_WAIT_SEC", "stop", "STOP MFC 밸브 열기(배기) 대기", "s", F, 8.0, 1.0, 60.0,
       src="controller/process_controller.py:1922",
        desc="STOP·ALL STOP 정지 순서의 MFC 밸브 열기(배기) 단계에서 밸브 열림 확인을 기다리는 한도다. 넘으면 기다리지 않고 다음 단계로 넘어간다."),
    _p("STOP_STEP_WAIT_DEFAULT_SEC", "stop", "STOP·ALL STOP 단계 기본 기다림(표에 한도가 없는 단계)", "s", F, 5.0, 1.0, 60.0,
       src=_V2, desc="정지 순서에서 한도 파라미터가 없는 단계의 완료를 기다리는 시간이다. 넘으면 다음 단계로 간다(5-1)"),

    # ───────────── 웹 연동(uplink) ─────────────
    _p("UPLINK_INTERVAL_MS", "uplink", "웹 전송 주기", "ms", I, 1000, 500, 10000, safety=L, apply=LIVE,
       src="reporter.py (_loop 1 s)",
        desc="웹 수집 주소로 POST 를 보내는 주기로, 매번 보관함의 미전송 메시지와 최신 상태 하나를 싣는다. 명령 결과·경고 같은 중요한 메시지는 이 주기를 기다리지 않고 바로 보낸다."),
    _p("UPLINK_TIMEOUT_SEC", "uplink", "웹 요청 응답 제한", "s", F, 15.0, 2.0, 60.0, safety=L,
       src="reporter.py (_post timeout=15)",
        desc="웹 요청 하나(상태 보내기·명령 받기 연결)의 응답 제한 시간이다. 넘으면 실패로 보고 다시 시도하며, 공정·안전 동작에는 영향이 없다."),
    _p("UPLINK_SPOOL_MAX_MB", "uplink", "미전송 보관 상한", "MB", I, 5, 1, 500, safety=L, src="reporter.py",
        desc="웹으로 아직 보내지 못한 메시지를 디스크 보관함에 쌓아 두는 상한이다. 넘치면 중요도가 낮은 메시지부터 버리고 경고를 남긴다(UPLINK_SPOOL_FULL)."),
    _p("UPLINK_SUMMARY_PERIOD_SEC", "uplink", "웹으로 보내는 스냅샷 요약 주기", "s", I, 60, 10, 600, safety=L,
       apply=LIVE, src=_V2, desc="정각에 맞춘 구간마다 숫자 열의 last·mean·min·max, 장비별 ok 비율·최대 나이, 행 수를 낸다. 공정 중에만 내며, 구간 안 "
                                 "행에 공정 id 가 하나라도 있을 때만 내고 구간의 마지막 run id 를 싣는다(공정 밖 요약은 내지 않음, 사용자 결정 2026-10-22, "
                                 "snapshot_summary·proc.run_id)"),
    _p("UPLINK_CLOCK_SKEW_SEC", "uplink", "웹 명령 만료 판정 허용 시계 차", "s", F, 5.0, 0.0, 60.0, safety=L, apply=LIVE,
       src=_V2, desc="명령의 expiresAt 이 장비 시계로 이 시간보다 더 지났으면 실행하지 않고 COMMAND_EXPIRED"),
    _p("UPLINK_DOWN_SEC", "uplink", "웹 연결 끊김 알림 기준", "s", I, 30, 5, 600, safety=L, apply=LIVE, src=_V2,
       desc="웹 보내기 실패가 이 시간을 넘게 이어지면 경고한다. 공정·안전은 그대로이고 기록·화면에만 남는다(UPLINK_DOWN)"),

    # ───────────── 기록(record) ─────────────
    _p("SNAPSHOT_RETENTION_DAYS", "record", "1초 기록 보관 기간", "일", I, 180, 7, 3650, safety=L, apply=RESTART, src=_V2,
        desc="로컬 기록 폴더의 일별 기록 파일(1초 스냅샷·이벤트, SQLite·CSV)을 이 기간만 남긴다. 날이 바뀔 때 기간이 지난 로컬 파일을 지우며, NAS 사본은 지우지 않는다."),
    _p("NAS_SYNC_PERIOD_SEC", "record", "NAS 사본 주기(지난 날 파일)", "s", I, 60, 10, 3600, safety=L, apply=RESTART,
       src=_V2,
        desc="지난 날 기록 파일(.sqlite·.csv)을 NAS 사본 폴더로 복사하는 주기다. 크기가 같은 파일은 건너뛰고 실패하면 다음 회차에 다시 하며, 오늘 DB 사본은 'NAS 오늘 DB "
             "사본 주기'를 따른다."),
    _p("NAS_TODAY_COPY_SEC", "record", "NAS 오늘 DB 사본 주기", "s", I, 600, 60, 86400, safety=L, apply=RESTART, src=_V2,
       desc="오늘 DB 를 SQLite backup 으로 떠서 NAS 로 복사하는 주기"),
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
    _r("X07", ("PLC_COMM_LOSS_ABORT_SEC",), "PLC 두절 중단 ≤ 래더 워치독 TON",
       lambda v: _need(v["PLC_COMM_LOSS_ABORT_SEC"] <= dev.HEATER_WD_TON_SEC,
                       "PLC 두절 중단이 래더 워치독보다 늦다")),
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
