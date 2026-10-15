"""챔버K 파라미터 정의 표.

출처
- v1 config_user.json 99개 항목(주석 제외) — default 는 이 운영값이다.
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
       v1_default=None, src=_JSON, pending=False, note="") -> ParamDef:
    if role is None:
        role = Role.ADMIN if (safety is H or scope is not Scope.PROCESS) else Role.OPERATOR
    return ParamDef(key=key, group=group, label=label, desc=desc or label, unit=unit, type=type_,
                    default=default, min=lo, max=hi, enum=enum, pattern=pattern, safety=safety,
                    apply=apply, scope=scope, role=role, v1_key=v1_key, v1_default=v1_default,
                    v1_source=src, pending_confirm=pending, note=note)


def _inst(key, label, type_, default, *, desc="", pattern=None, lo=None, hi=None, enum=None,
          safety=M, v1_key=None, src=_JSON, unit="", v1_default=None) -> ParamDef:
    return _p(key, "install", label, unit, type_, default, lo, hi, safety=safety, apply=RESTART,
              scope=Scope.INSTALL, desc=desc, pattern=pattern, enum=enum, v1_key=v1_key, src=src,
              v1_default=v1_default)


PARAMS: tuple = (
    # ───────────── 설치값(install) — 웹 admin + 노트북 비상 덮어쓰기, 재시작 필요 ─────────────
    _inst("EQUIPMENT_ID", "장비 ID", S, "CHK2", pattern=r"[A-Z0-9][A-Z0-9-]{1,15}", safety=H, src=_V2,
          desc="웹에서 장비를 구분하는 ID. 개발 중 CHK2 — v1(CHK)과 섞이지 않게 한다"),
    _inst("RUN_MODE", "실행 모드", E, "sim", enum=("sim", "bench", "prod"), safety=H, src=_V2,
          desc="sim=가짜 장비·외부 연결 없음 / bench=실장비 읽기 전용 / prod=운전. "
               "로컬 파일이 없으면 sim. prod 는 명시적으로만"),
    _inst("PLC_PORT", "PLC 포트", S, "COM9", pattern=_COM, safety=H),
    _inst("PLC_BAUD", "PLC 통신 속도", I, 115200, lo=9600, hi=115200, unit="bps",
          src="lib/config.py:24 (고정값)"),
    _inst("PLC_SLAVE_ID", "PLC Modbus 국번", I, 1, lo=1, hi=247, src="lib/config.py:25 (고정값)"),
    _inst("MFC_PORT", "MFC 포트", S, "COM10", pattern=_COM),
    _inst("MFC_BAUD", "MFC 통신 속도", I, 9600, lo=1200, hi=115200, unit="bps", safety=L),
    _inst("DC_PORT", "DC 전원 포트", S, "COM11", pattern=_COM),
    _inst("DC_BAUD", "DC 전원 통신 속도", I, 9600, lo=1200, hi=115200, unit="bps", safety=L,
          v1_key="DC_BAUDRATE"),
    _inst("RFPULSE_PORT", "RF Pulse 포트", S, "COM13", pattern=_COM),
    _inst("RFPULSE_BAUD", "RF Pulse 통신 속도", I, 9600, lo=1200, hi=115200, unit="bps", safety=L),
    _inst("RFPULSE_ADDR", "RF Pulse AE Bus 주소", I, 1, lo=0, hi=31,
          desc="AE Bus 주소 0~31(v1 은 32 이상이면 응답 비교가 영원히 불일치)"),
    _inst("RFPULSE_PARITY", "RF Pulse 패리티", E, "odd", enum=("none", "odd", "even"),
          desc="CESAR RS-232 규격은 홀수 패리티(8O1). 다르면 NAK 만 돌아온다"),
    _inst("IG_ENABLED", "이온 게이지 사용", B, True, v1_default=False,
          desc="false 면 base_pressure 가 있는 공정은 시작을 거부한다(v1 은 경고만 하고 대기를 건너뜀)"),
    _inst("IG_PORT", "이온 게이지 포트", S, "COM12", pattern=r"(" + _COM + r")?", v1_default=""),
    _inst("IG_BAUD", "이온 게이지 통신 속도", I, 9600, lo=1200, hi=115200, unit="bps", safety=L,
          src="lib/config.py:735"),
    _inst("HEATER_ENABLED", "히터 기능 사용", B, True, safety=H,
          desc="false 면 히터 명령·레시피 히터 행을 거부한다. 워치독 하트비트는 항상 쓴다"),
    _inst("LOCAL_HMI_PORT", "로컬 화면 포트", I, 8710, lo=1024, hi=65535, safety=L, src=_V2,
          desc="127.0.0.1 에서만 연다"),
    _inst("LOCAL_DATA_DIR", "로컬 기록 폴더", S, r"C:\ChamberK_v2\data", pattern=r".+", safety=L,
          src=_V2, desc="1초 스냅샷 SQLite·일별 CSV·이벤트·블랙박스"),
    _inst("NAS_COPY_DIR", "NAS 사본 폴더", S, r"\\VanaM_NAS\VanaM_toShare\JH_Lee\Logs\CHK2",
          pattern=r".*", safety=L, src=_V2, desc="빈 값이면 NAS 복사 안 함. 제어 흐름과 분리된 스레드가 복사"),

    # ───────────── 비밀값(secret) — 장비 로컬 파일에만, 웹에 보내지 않음 ─────────────
    _p("UPLINK_URL", "secret", "웹 수집 주소", "", S, "", scope=Scope.SECRET, apply=RESTART,
       pattern=r"(https://.+)?", v1_key="ERP_INGEST_URL", src="lib/config_local.py (이름만)"),
    _p("UPLINK_TOKEN", "secret", "웹 수집 토큰(장비별)", "", S, "", scope=Scope.SECRET, apply=RESTART,
       v1_key="ERP_INGEST_TOKEN", src="lib/config_local.py (이름만)"),

    # ───────────── 1초 수집(tick) ─────────────
    _p("TICK_DEADLINE_MS", "tick", "1초 수집 마감", "ms", I, 900, 500, 950, safety=M, src=_V2,
       desc="정각 초 t 에 읽기를 보내고 t+이 값에 행을 닫는다. 늦은 응답은 다음 행에 stale"),
    _p("PLC_FAST_POLL_MS", "tick", "PLC 내부 폴링 주기", "ms", I, 250, 100, 500, safety=H, src=_V2,
       desc="안전 비트(MV·인터록·히터)를 보는 주기. 1초에 4번 중 정각 회차를 스냅샷에 쓴다",
       v1_default=200, note="v1 200 ms(상대 타이머)"),
    _p("PLC_READ_DEADLINE_MS", "tick", "PLC 읽기 마감", "ms", I, 400, 100, 900, src=_V2),
    _p("MFC_READ_DEADLINE_MS", "tick", "MFC 읽기 마감", "ms", I, 900, 200, 950, src=_V2),
    _p("DC_READ_DEADLINE_MS", "tick", "DC 읽기 마감", "ms", I, 500, 100, 900, src=_V2),
    _p("RFPULSE_READ_DEADLINE_MS", "tick", "RF Pulse 읽기 마감", "ms", I, 900, 200, 950, src=_V2),
    _p("IG_READ_DEADLINE_MS", "tick", "이온 게이지 읽기 마감", "ms", I, 900, 400, 950, src=_V2,
       desc="E 평소 250~300 ms, 레인지 찾기 중 최대 860 ms(2026-10-14 실측)"),

    # ───────────── 통신 내성(comm) ─────────────
    _p("PLC_RETRY_COUNT", "comm", "PLC 재시도 횟수", "회", I, 2, 0, 5, desc="트랜잭션 실패 시 재시도(총 1+N)"),
    _p("PLC_RETRY_DELAY_MS", "comm", "PLC 재시도 간격", "ms", I, 30, 0, 1000, safety=L),
    _p("PLC_COMM_LOSS_ABORT_SEC", "comm", "PLC 두절 공정 중단", "s", F, 10.0, 1.0, 10.0, safety=H,
       desc="마지막 성공 뒤 이만큼 지나면 공정 중단. 래더 히터 워치독 TON(10 s) 이하"),
    _p("PLC_DISCONNECT_NOTIFY_SEC", "comm", "PLC 끊김 알림 유예", "s", F, 60.0, 10.0, 600.0, safety=L,
       src="device/PLC.py:192 (_DISCONNECT_GRACE_S)"),
    _p("DC_COMM_LOSS_ABORT_SEC", "comm", "DC 두절 공정 중단", "s", F, 10.0, 2.0, 30.0, safety=H),
    _p("RFPULSE_COMM_LOSS_ABORT_SEC", "comm", "RF Pulse 두절 공정 중단", "s", F, 10.0, 2.0, 30.0,
       safety=H),
    _p("RFPULSE_COMM_REOPEN_SEC", "comm", "RF Pulse 포트 다시 열기", "s", F, 3.0, 1.0, 30.0, safety=L,
       src="lib/config.py:57 (고정값)"),
    _p("IG_COMM_LOSS_ABORT_SEC", "comm", "이온 게이지 두절 베이스 대기 중단", "s", F, 60.0, 10.0, 600.0,
       src="controller/process_controller.py:35"),
    _p("COMM_RECONNECT_START_MS", "comm", "재연결 첫 간격", "ms", I, 1000, 100, 60000, safety=L),
    _p("COMM_RECONNECT_MAX_MS", "comm", "재연결 최대 간격", "ms", I, 60000, 1000, 600000, safety=L),
    _p("COMM_LONG_OUTAGE_SEC", "comm", "장기 두절 판정", "s", I, 600, 30, 86400, safety=L),
    _p("COMM_PROBE_MS", "comm", "장기 두절 중 시도 주기", "ms", I, 60000, 1000, 600000, safety=L),
    _p("COMM_OUTAGE_LOG_SEC", "comm", "두절 기록 간격", "s", I, 600, 30, 86400, safety=L, apply=LIVE),

    # ───────────── PLC ─────────────
    _p("PLC_SESSION_MARK_REG", "plc", "PLC 재기동 감지 D 주소", "D번호", I, 60, 0, 9999,
       desc="래더가 쓰지 않는 D 주소. 0 이면 끔(재기동 감지 안 함)"),
    _p("MV_INTERLOCK_ABORT_MS", "plc", "MV 인터록 해제 지속 중단", "ms", I, 1000, 200, 5000, safety=H,
       src="main.py:3839", desc="공정 중 MV_INTERLOCK 이 이만큼 연속 OFF 면 중단(순간 끊김은 기록만)"),
    _p("BLACKBOX_SECONDS", "plc", "PLC 블랙박스 길이", "s", I, 60, 10, 600, safety=L,
       src="device/PLC.py:178 (deque 300 × 200 ms)"),
    _p("ALL_STOP_KEEP_PUMPS", "plc", "ALL STOP 때 펌프 유지", "", B, True, safety=H, pending=True,
       src=_V2, v1_default=False,
       desc="true: 전원·가스·히터 차단, 셔터·MV·도어 닫기, 펌프 계통(Rotary·Turbo·RV·FV)은 지금 상태 그대로 "
            "— 터보가 도는 동안 FV 를 닫지 않기 위함. false: v1 처럼 PLC 코일 전부 0"),

    # ───────────── MFC(경고·알림만, 공정은 계속) ─────────────
    _p("FLOW_ERROR_TOLERANCE", "mfc", "유량 이탈 허용 비율", "비율", F, 0.10, 0.01, 0.5, apply=LIVE,
       desc="안정화 판정과 공정 중 감시에 쓴다"),
    _p("FLOW_ERROR_MAX_COUNT", "mfc", "유량 이탈 연속 횟수", "회", I, 5, 1, 100, safety=L, apply=LIVE,
       desc="이 횟수에 도달하면 경고(>=)"),
    _p("MFC_PRESSURE_WARN_RATIO", "mfc", "압력 이탈 허용 비율", "비율", F, 0.10, 0.01, 0.5, safety=L,
       apply=LIVE),
    _p("MFC_PRESSURE_WARN_COUNT", "mfc", "압력 이탈 연속 횟수", "회", I, 5, 1, 100, safety=L, apply=LIVE),
    _p("MFC_SP1_VERIFY_TOL", "mfc", "SP1 설정 확인 허용 오차", "장비 단위", F, 0.1, 0.01, 1.0, safety=L,
       src="lib/config.py:808 (고정값)"),

    # ───────────── DC 전원 ─────────────
    _p("DC_MAX_VOLTAGE", "dc", "DC 전압 상한", "V", F, 600.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:512 (고정값)", desc="APPL 전압 상한. 장비 정격 이하"),
    _p("DC_MAX_CURRENT", "dc", "DC 전류 상한", "A", F, 1.0, 0.0, 2.0, safety=H, src="lib/config.py:513 (고정값)"),
    _p("DC_MAX_POWER", "dc", "DC 파워 상한", "W", F, 500.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:514 (고정값)", desc="레시피·수동 입력 상한(v1 은 드라이버에만 있었다)"),
    _p("DC_INITIAL_VOLTAGE", "dc", "DC 시작 전압", "V", F, 450.0, 0.0, 1000.0, safety=H,
       src="lib/config.py:510 (고정값)"),
    _p("DC_INITIAL_CURRENT", "dc", "DC 시작 전류", "A", F, 0.1, 0.0, 2.0, safety=H, src="lib/config.py:511 (고정값)"),
    _p("DC_TOLERANCE_W", "dc", "DC 목표 도달 허용 오차", "W", F, 0.5, 0.1, 10.0, src="lib/config.py:515 (고정값)"),
    _p("DC_POWER_ERROR_RATIO", "dc", "DC 유지 이탈 비율", "비율", F, 0.10, 0.02, 0.5, safety=H,
       desc="판정 폭 = max(DC_TOLERANCE_W, 목표×비율)"),
    _p("DC_POWER_ERROR_MAX_COUNT", "dc", "DC 유지 이탈 연속 횟수", "회", I, 5, 1, 60, safety=H),
    _p("DC_MIN_CURRENT_ABORT", "dc", "DC 저전류 기준", "A", F, 0.05, 0.0, 1.0, safety=H),
    _p("DC_MIN_CURRENT_ABORT_COUNT", "dc", "DC 저전류 연속 횟수", "회", I, 10, 1, 120, safety=H),
    _p("DC_FAIL_ISET_A", "dc", "DC 무출력 판정 설정 전류", "A", F, 0.20, 0.01, 1.0, safety=H,
       src="lib/config.py:517 (고정값)"),
    _p("DC_FAIL_POWER_W", "dc", "DC 무출력 판정 파워", "W", F, 1.0, 0.1, 50.0, safety=H,
       src="lib/config.py:518 (고정값)"),
    _p("DC_FAIL_MAX_TICKS", "dc", "DC 램프업 무출력 허용", "s", I, 15, 3, 120, safety=H,
       desc="Iset ≥ DC_FAIL_ISET_A 인데 P ≤ DC_FAIL_POWER_W 가 이만큼 이어지면 중단"),
    _p("DC_POWER_DELAY_SEC", "dc", "DC 안정화 대기", "s", I, 300, 0, 3600,
       desc="SP1 도달 뒤 Shutter Delay 전 대기(레시피 use_dc_delay 일 때만). 이 동안 ±이탈 판정 꺼짐"),
    _p("DC_CONTROL_GAIN", "dc", "DC 전류 보정 이득", "배", F, 1.0, 0.1, 1.0, safety=H,
       desc="ΔI = 이득 × ΔP / V. v1 은 검사가 없어 음수도 허용했다"),
    _p("DC_RAMP_STEP_A", "dc", "DC 램프업 전류 상승 상한", "A/s", F, 0.010, 0.001, 0.1, safety=H),
    _p("DC_MAINTAIN_STEP_UP_A", "dc", "DC 유지 중 전류 상승 상한", "A/s", F, 0.020, 0.001, 0.1, safety=H),
    _p("DC_MAINTAIN_STEP_DOWN_A", "dc", "DC 전류 하강 상한", "A/s", F, 0.050, 0.001, 0.2),
    _p("DC_SMALL_ERROR_RATIO", "dc", "DC 작은 오차 경계", "비율", F, 0.01, 0.0, 0.1, safety=L),
    _p("DC_SMALL_ERROR_GAIN", "dc", "DC 작은 오차 이득", "배", F, 0.5, 0.1, 1.0, safety=L),
    _p("DC_LIMIT_STALL_SEC", "dc", "DC 상한 정체 시 유지로 넘김", "s", I, 10, 1, 120, safety=L),
    _p("DC_OFF_VERIFY_TRIES", "dc", "DC OFF 확인 횟수", "회", I, 3, 1, 10, safety=H,
       src="device/DCpower.py:38", desc="OUTP OFF 뒤 OUTP? 확인 횟수(무응답이면 OFF 재전송)"),
    _p("DC_OFF_RETRY_START_SEC", "dc", "DC OFF 미확인 재시도 첫 간격", "s", F, 10.0, 2.0, 600.0,
       src="device/DCpower.py:32", desc="이후 2배씩, 최대 COMM_OUTAGE_LOG_SEC"),

    # ───────────── RF Power(PLC DAC/ADC) ─────────────
    _p("RF_MAX_POWER", "rf", "RF 파워 상한(DAC 풀스케일 환산)", "W", F, 600.0, 1.0, 600.0, safety=H,
       src="lib/config.py:561 (고정값)", desc="DAC 카운트 = (P/RF_MAX_POWER)×4000×gain + offset"),
    _p("RF_FWD_FULL_SCALE_W", "rf", "RF 순방향 ADC 풀스케일", "W", F, 594.5, 1.0, 2000.0, safety=H,
       src="lib/config.py:558 (고정값)", desc="ADC 4000 카운트 = 이 값(W). 교정 상수 — 사용자 확인 완료(2026-10-15), v1 값"),
    _p("RF_REF_FULL_SCALE_W", "rf", "RF 반사 ADC 풀스케일", "W", F, 200.0, 1.0, 2000.0, safety=H,
       src="lib/config.py:559 (고정값)", desc="ADC 4000 카운트 = 이 값(W) — 사용자 확인 완료(2026-10-15), v1 값 유지"),
    _p("RF_CAL_OFFSET", "rf", "RF DAC 보정 오프셋", "count", F, 6.79, -400.0, 400.0, safety=H,
       src="UI.py:440 (offset_edit 기본값)", desc="v1 은 화면 입력칸 값을 공정마다 썼다"),
    _p("RF_CAL_GAIN", "rf", "RF DAC 보정 이득", "배", F, 1.0395, 0.5, 1.5, safety=H,
       src="UI.py:444 (param_edit 기본값)"),
    _p("RF_RAMP_STEP_W", "rf", "RF 램프업 속도", "W/s", F, 1.0, 0.1, 20.0, safety=H, src="lib/config.py:562 (고정값)"),
    _p("RF_RAMP_DOWN_STEP_COUNTS", "rf", "RF 램프다운 속도", "count/0.5s", I, 8, 1, 400,
       src="lib/config.py:563 (고정값)", note="v1 이름·주석은 W 지만 실제는 DAC 카운트(RFpower.py:212)"),
    _p("RF_TOLERANCE_W", "rf", "RF 목표 도달 허용 오차", "W", F, 1.0, 0.1, 20.0, src="lib/config.py:566 (고정값)"),
    _p("RF_POWER_ERROR_RATIO", "rf", "RF 유지 이탈 비율", "비율", F, 0.10, 0.02, 0.5, safety=H),
    _p("RF_POWER_ERROR_MAX_COUNT", "rf", "RF 유지 이탈 연속 횟수", "회", I, 5, 1, 60, safety=H),
    _p("RF_FAIL_DAC_COUNTS", "rf", "RF 무출력 판정 DAC", "count", I, 100, 10, 4000, safety=H,
       src="lib/config.py:567 (고정값)"),
    _p("RF_FAIL_FWD_W", "rf", "RF 무출력 판정 순방향", "W", F, 1.0, 0.1, 50.0, safety=H,
       src="lib/config.py:568 (고정값)"),
    _p("RF_FAIL_MAX_TICKS", "rf", "RF 램프업 무출력 허용", "s", I, 10, 3, 120, safety=H),
    _p("RF_REFP_ABORT_THRESHOLD", "rf", "RF 반사파 대기 기준", "W", F, 3.0, 0.5, 200.0, safety=H,
       v1_default=20.0, desc="반사파가 이 값 이상이면 대기 시작(전 구간)"),
    _p("RF_REFP_WAIT_SEC", "rf", "RF 반사파 대기 허용", "s", I, 60, 1, 600, safety=H, v1_default=15),
    _p("STOP_RF_RAMPDOWN_TIMEOUT_SEC", "stop", "RF 램프다운 한도", "s", F, 120.0, 10.0, 600.0,
       src="controller/process_controller.py:392"),

    # ───────────── RF Pulse(CESAR 1310) ─────────────
    _p("RFPULSE_MAX_POWER", "rfpulse", "RF Pulse 출력 상한", "W", F, 600.0, 1.0, float(dev.CESAR_MAX_POWER_W),
       safety=H),
    _p("RFPULSE_PULSE_FREQ_MIN_HZ", "rfpulse", "펄스 주파수 하한", "Hz", I, 1, 1, 100,
       src="lib/config.py:613 (고정값)"),
    _p("RFPULSE_PULSE_FREQ_MAX_HZ", "rfpulse", "펄스 주파수 상한", "Hz", I, 30000, 1, 100000),
    _p("RFPULSE_DUTY_MIN", "rfpulse", "펄스 듀티 하한", "%", I, 1, 1, 99, src="lib/config.py:614 (고정값)"),
    _p("RFPULSE_DUTY_MAX", "rfpulse", "펄스 듀티 상한", "%", I, 99, 1, 99, src="lib/config.py:615 (고정값)"),
    _p("RFPULSE_PULSE_MIN_EDGE_US", "rfpulse", "펄스 최소 ON/OFF 시간", "µs", F, 16.0, 0.0, 1000.0,
       src="lib/config.py:617", desc="0 이면 검사 안 함. CESAR 1310 명령 96 규칙"),
    _p("RFPULSE_FORP_TOLERANCE_PERCENT", "rfpulse", "순방향 허용 오차", "%", F, 5.0, 0.5, 50.0, safety=H),
    _p("RFPULSE_FORP_CONSECUTIVE_LIMIT", "rfpulse", "순방향 이탈 연속 횟수", "회", I, 3, 1, 60, safety=H),
    _p("RFPULSE_REFP_LIMIT_WATTS", "rfpulse", "반사파 상한", "W", F, 20.0, 0.5, 200.0, safety=H),
    _p("RFPULSE_REFP_CONSECUTIVE_LIMIT", "rfpulse", "반사파 초과 연속 횟수", "회", I, 3, 1, 60, safety=H),
    _p("RFPULSE_VERIFY_PULSE_CONFIG", "rfpulse", "펄스 설정 되읽기 불일치 시 중단", "", B, True,
       desc="false 면 경고만"),
    _p("RFPULSE_RAW_LOG", "rfpulse", "RF Pulse 프레임 원문 기록", "", B, True, safety=L, apply=LIVE,
       desc="로컬 파일에만 쓴다(v1 은 NAS 공정 로그에 썼다)"),
    _p("STOP_RFPULSE_OFF_TIMEOUT_SEC", "stop", "RF Pulse OFF 대기 한도", "s", F, 30.0, 5.0, 120.0,
       src="controller/process_controller.py:1980"),

    # ───────────── 이온 게이지·베이스 압력(ig) ─────────────
    _p("IG_GAUGE_A_NAME", "ig", "게이지 A 이름", "", S, "게이지 A", safety=L, apply=LIVE,
       pattern=r".{1,20}", src="lib/config.py:736"),
    _p("IG_GAUGE_B_NAME", "ig", "게이지 B 이름", "", S, "게이지 B", safety=L, apply=LIVE,
       pattern=r".{1,20}", src="lib/config.py:737"),
    _p("IG_FILAMENT_SWITCH_QUIET_SEC", "ig", "필라멘트 전환 뒤 무응답 유예", "s", F, 12.0, 6.0, 60.0,
       safety=L, src="lib/config.py:746",
       desc="A 뒤 약 5.9 s·B 뒤 약 4.9 s 무응답(실측). 이 동안 품질 suppressed, 실패로 세지 않음"),
    _p("IG_FILAMENT_ON_TIMEOUT_SEC", "ig", "필라멘트 ON 결과 기한", "s", F, 20.0, 10.0, 120.0,
       src="lib/config.py:749"),
    _p("IG_FILAMENT_OFF_TIMEOUT_SEC", "ig", "필라멘트 OFF 확인 기한", "s", F, 20.0, 10.0, 120.0,
       src="controller/process_controller.py:39 (드라이버 기한 20 s)"),
    _p("IG_SETTLE_SEC", "ig", "방출 뒤 판정 제외 시간", "s", F, 15.0, 0.0, 120.0, src="lib/config.py:753"),
    _p("IG_VALUE_MAX_AGE_SEC", "ig", "베이스 판정에 쓰는 값 나이 상한", "s", F, 2.0, 1.0, 10.0,
       src="controller/process_controller.py:36"),
    _p("IG_BASE_STABLE_COUNT", "ig", "베이스 도달 연속 횟수", "회", I, 5, 1, 60, src="lib/config.py:758"),
    _p("IG_BASE_WAIT_TIMEOUT_SEC", "ig", "베이스 압력 대기 한도", "s", I, 1800, 60, 14400,
       desc="넘으면 공정 중단"),
    _p("IG_MAX_RELIGHTS", "ig", "대기 중 필라멘트 재점등 횟수", "회", I, 2, 0, 10,
       src="controller/process_controller.py:37"),
    _p("IG_BASE_LOG_INTERVAL_SEC", "ig", "베이스 대기 기록 간격", "s", I, 5, 1, 600, safety=L, apply=LIVE,
       src="lib/config.py:766"),
    _p("BASE_PRESSURE_MIN_TORR", "ig", "베이스 압력 입력 하한", "Torr", F, 1e-9, 1e-12, 1e-3,
       src="core/params.py:21"),
    _p("BASE_PRESSURE_MAX_TORR", "ig", "베이스 압력 입력 상한", "Torr", F, 1e-3, 1e-9, 1e-2,
       src="core/params.py:22"),

    # ───────────── 히터 기본(heater) ─────────────
    _p("HEATER_WD_PERIOD_MS", "heater", "히터 워치독 하트비트 주기", "ms", I, 3000, 500, 3000, safety=H,
       desc="래더는 D00014 가 TON 10 s 동안 안 바뀌면 히터를 끈다. TON 의 1/3 이하"),
    _p("HEATER_WD_DELAY_WARN_SEC", "heater", "하트비트 지연 경고", "s", F, 5.0, 1.0, 9.0, safety=L,
       src="device/PLC.py:119"),
    _p("HEATER_MAX_TEMP", "heater", "목표 온도 입력 상한", "°C", F, 700.0, 0.0, 1100.0, safety=H,
       v1_default=500.0, desc="HEATER_SV_LIMIT_C 이하"),
    _p("HEATER_SOAK_TOLERANCE", "heater", "도달 판정 허용 편차", "°C", F, 2.0, 0.5, 20.0, v1_default=3.0),
    _p("HEATER_SOAK_TIME_SEC", "heater", "공정 레시피 도달 후 유지", "s", I, 60, 0, 3600),
    _p("HEATER_WAIT_TIMEOUT_SEC", "heater", "승온 대기 최소 한도", "s", I, 5400, 600, 43200, v1_default=3600,
       desc="실제 한도 = min(max(이 값, 예상 램프 + MARGIN), MAX)"),
    _p("HEATER_WAIT_TIMEOUT_MARGIN_SEC", "heater", "승온 대기 여유", "s", F, 1800.0, 0.0, 14400.0,
       src="controller/process_controller.py:26, heater_recipe.py:54 (중복 정의)"),
    _p("HEATER_WAIT_TIMEOUT_MAX_SEC", "heater", "승온 대기 절대 상한", "s", F, 43200.0, 600.0, 86400.0,
       safety=H, src="controller/process_controller.py:27, heater_recipe.py:55 (중복 정의)"),
    _p("HEATER_RAMP_DONE_TOL_C", "heater", "램프 완료 판정 폭", "°C", F, 0.15, 0.05, 5.0, safety=L,
       src="controller/process_controller.py:43, heater_recipe.py:59"),
    _p("HEATER_TC_BAD_LIMIT_SEC", "heater", "TC 이상 지속 시 레시피 중단", "s", F, 60.0, 5.0, 600.0,
       safety=H, src="controller/heater_recipe.py:47"),
    _p("HEATER_OUT_DEAD_LIMIT_SEC", "heater", "출력 죽음 지속 시 레시피 중단", "s", F, 10.0, 3.0, 600.0,
       safety=H, src="controller/heater_recipe.py:50"),
    _p("HEATER_OUT_DEAD_GRACE_SEC", "heater", "출력 죽음 판정 유예", "s", F, 5.0, 0.0, 60.0,
       src="device/PLC.py:49"),
    _p("HEATER_OUT_DEAD_COUNT", "heater", "출력 죽음 연속 횟수", "회", I, 3, 1, 60, src="device/PLC.py:1040"),
    _p("HEATER_RUN_OFF_LIMIT_SEC", "heater", "외부 RUN OFF 지속 시 레시피 중단", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_recipe.py:63"),
    _p("HEATER_GAS_HOLD_RELEASE_C", "heater", "냉각 보호 가스 해제 온도", "°C", F, 100.0, 30.0, 400.0,
       safety=H, desc="히터 OFF 때 PV 가 이보다 높으면 가스·압력 유지, 식으면 해제. PV 를 모르면 해제 안 함"),
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
       v1_default=500.0),
    _p("HEATER_RAMP_RATE_C_PER_MIN", "heater_plc", "D00020 램프 속도·기본 램프", "°C/min", F, 12.0, 6.0, 60.0,
       safety=H, desc="래더 분해능 6 °C/min(1 카운트/초)"),
    _p("HEATER_HOLDBACK_C", "heater_plc", "D00021 홀드백", "°C", F, 10.0, 0.5, 50.0, safety=H, v1_default=2.0),
    _p("HEATER_OT_LIMIT_C", "heater_plc", "D00026 TC1 과온 트립", "°C", F, 750.0, 50.0, 1130.0, safety=H,
       v1_default=550.0, desc="SV_LIMIT+20 이상(권장 +50). 래치 — 풀려면 이상 리셋"),
    _p("HEATER_OT2_LIMIT_C", "heater_plc", "D00034 TC2 과온 트립", "°C", F, 1150.0, 70.0,
       dev.HEATER_OT2_LADDER_MAX_C, safety=H, desc="OT+20 이상, 래더 상한 1150"),
    _p("HEATER_SLOW_ZONE_C", "heater_plc", "D00027 감속 구간", "°C", F, 10.0, 10.0, 10.0, safety=L,
       note="래더가 10.0 으로 강제 — 되읽기 일치용"),
    _p("HEATER_SLOW_RATE_C_PER_MIN", "heater_plc", "D00028 감속 속도", "°C/min", F, 60.0, 6.0, 600.0,
       v1_default=6.0, desc="60 으로 열어 래더가 속도를 막지 않게 — 실제 램프는 파이썬 감속 접근이 정한다"),

    # ───────────── 히터 감속 접근(heater_ramp) ─────────────
    _p("HEATER_APPROACH_ZONE_C", "heater_ramp", "목표 직전 감속 구간", "°C", F, 20.0, 0.0, 200.0),
    _p("HEATER_APPROACH_MIN_RATE_C_PER_MIN", "heater_ramp", "도착 속도", "°C/min", F, 1.0, 0.1, 6.0),
    _p("HEATER_APPROACH_LEAD_C", "heater_ramp", "SV 앞서기 폭", "°C", F, 1.0, 0.2, 10.0),

    # ───────────── 히터 유지 모드·포화(heater_hold) ─────────────
    _p("HEATER_HOLD_MODE", "heater_hold", "도달 후 유지 모드", "", E, "tc2", enum=("off", "dac", "tc2"),
       safety=H, v1_default="off", desc="dac=D00018 을 그때 출력으로 고정 / tc2=TC2 추종 제어"),
    _p("HEATER_HOLD_MV_ENTER_TOL_C", "heater_hold", "유지 진입 허용 |PV-SV|", "°C", F, 3.0, 0.5, 20.0),
    _p("HEATER_HOLD_MV_ENTER_SEC", "heater_hold", "유지 진입 안정 시간", "s", F, 60.0, 10.0, 600.0),
    _p("HEATER_HOLD_MV_ARRIVE_TOL_C", "heater_hold", "한 번은 들어와야 하는 범위", "°C", F, 1.0, 0.2, 10.0),
    _p("HEATER_HOLD_MV_DRIFT_PV_C", "heater_hold", "PV 드리프트 하한", "°C", F, 0.5, 0.1, 10.0, safety=L),
    _p("HEATER_HOLD_MV_DRIFT_MV", "heater_hold", "MV 드리프트 하한", "count", F, 15.0, 3.0, 200.0, safety=L),
    _p("HEATER_HOLD_TC2_MARGIN_C", "heater_hold", "TC2 목표 마진(OT2 − 이 값)", "°C", F, 50.0, 20.0, 500.0,
       safety=H),
    _p("HEATER_HOLD_TC2_DRIFT_C", "heater_hold", "TC2 드리프트 하한", "°C", F, 1.0, 0.1, 20.0, safety=L),
    _p("HEATER_HOLD_DRIFT_PV_REL", "heater_hold", "PV 드리프트 비율(×ENTER_TOL)", "비율", F, 0.5, 0.05, 1.0,
       safety=L, src="controller/heater_hold.py:38"),
    _p("HEATER_HOLD_DRIFT_MV_REL", "heater_hold", "MV 드리프트 비율(×MV_LIMIT)", "비율", F, 0.05, 0.005, 0.5,
       safety=L, src="controller/heater_hold.py:39"),
    _p("HEATER_HOLD_DRIFT_TC2_REL", "heater_hold", "TC2 드리프트 비율", "비율", F, 0.02, 0.001, 0.5,
       safety=L, src="controller/heater_hold.py:40"),
    _p("HEATER_HOLD_ENGAGE_TIMEOUT_SEC", "heater_hold", "유지 진입 단계 대기", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_hold.py:44", note="v1 주석: 래더 스캔 지연 미확인 — bench 실측"),
    _p("HEATER_HOLD_REAPPLY_SEC", "heater_hold", "되돌아간 값 재적용 대기", "s", F, 5.0, 1.0, 60.0,
       src="controller/heater_hold.py:45"),
    _p("HEATER_HOLD_WAIT_SEC", "heater_hold", "공정의 유지 진입 대기 한도", "s", F, 600.0, 0.0, 7200.0),
    _p("HEATER_HOLD_FAIL_ACTION", "heater_hold", "유지 진입 최종 실패 시", "", E, "abort",
       enum=("abort", "proceed"), safety=H, desc="완화 tc2 → dac 폴백을 먼저 시도한 뒤에도 실패하면"),
    _p("HEATER_TC1_DEV_WARN_C", "heater_hold", "TC1 편차 경고 기준", "°C", F, 120.0, 10.0, 500.0, safety=L,
       src="controller/heater_hold.py:49"),
    _p("HEATER_TC1_DEV_WARN_SEC", "heater_hold", "TC1 편차 경고 지속", "s", F, 300.0, 10.0, 3600.0, safety=L,
       src="controller/heater_hold.py:50"),
    _p("HEATER_TC1_DEV_OK_C", "heater_hold", "TC1 편차 정상 기준", "°C", F, 10.0, 1.0, 100.0, safety=L,
       src="controller/heater_hold.py:51"),
    _p("HEATER_MV_SAT_REL", "heater_hold", "포화 판정 비율(×MV_LIMIT)", "비율", F, 0.98, 0.8, 1.0,
       src="controller/heater_hold.py:42, heater_saturation.py:29"),
    _p("HEATER_MV_SAT_SEC", "heater_hold", "DAC 포화 판정 시간", "s", F, 120.0, 10.0, 3600.0,
       desc="v1 은 10 미만이면 120 으로 되돌렸다 — v2 는 범위 밖이면 거부"),
    _p("HEATER_SAT_PRE_WINDOW_SEC", "heater_hold", "포화 직전 평균 창", "s", F, 60.0, 10.0, 600.0,
       src="controller/heater_saturation.py:31"),
    _p("HEATER_SAT_FALLBACK_REL", "heater_hold", "포화 직전 이력 없을 때 클램프 비율", "비율", F, 0.9, 0.5, 1.0,
       src="controller/heater_saturation.py:30"),

    # ───────────── 히터 레시피·히터 가스(heater_recipe / heater_gas) ─────────────
    _p("HEATER_RECIPE_MAX_STEPS", "heater_recipe", "히터 레시피 최대 스텝", "개", I, 20, 1, 100, safety=L),
    _p("HEATER_RECIPE_MAX_REPEAT", "heater_recipe", "히터 레시피 반복 상한", "회", I, 1, 1, 100, src=_V2,
       pending=True, note="v1 은 repeat 빈칸·1 만 허용(반복 미지원). 반복을 쓰려면 이 값을 올린다"),
    _p("HEATER_RECIPE_HOLD_AT_END", "heater_recipe", "레시피 끝 온도 유지", "", B, False, pending=True,
       desc="true 면 끝나도 마지막 목표 유지(무인 가열 주의)"),
    _p("HEATER_RECIPE_RAMP_MAX_C_PER_MIN", "heater_recipe", "레시피 램프 상한", "°C/min", F, 60.0, 6.0, 600.0,
       src="core/params.py (60 초과 오류)"),
    _p("HEATER_GAS_SP1_TIMEOUT_SEC", "heater_gas", "히터 가스 SP1 도달 한도", "s", F, 300.0, 30.0, 1800.0,
       src="controller/heater_atmosphere.py:55"),
    _p("HEATER_GAS_MFC_TIMEOUT_SEC", "heater_gas", "히터 가스 MFC 명령 한도", "s", F, 60.0, 5.0, 600.0,
       src="controller/heater_atmosphere.py:43"),

    # ───────────── 공정 시퀀스(process) ─────────────
    _p("PRE_POWER_OFF_SETTLE_SEC", "process", "시작 전 전원 OFF 뒤 대기", "s", F, 1.0, 0.0, 30.0, safety=L,
       src="controller/process_controller.py:519"),
    _p("GAS_STABILIZE_SEC", "process", "SP4 ON 뒤 압력 안정화", "s", F, 60.0, 0.0, 600.0,
       src="controller/process_controller.py:577"),
    _p("SP3_MTORR", "process", "압력 1단계(SP3)", "mTorr", F, 15.0, 1.0, 100.0,
       src="controller/process_controller.py:619", desc="WP < 이 값일 때만 거친다"),
    _p("SP2_MTORR", "process", "압력 2단계(SP2)", "mTorr", F, 5.0, 0.5, 100.0,
       src="controller/process_controller.py:623"),
    _p("PRESSURE_WAIT_TOL_RATIO", "process", "압력 도달 허용 비율", "비율", F, 0.10, 0.01, 0.5,
       src="controller/process_controller.py:105"),
    _p("PRESSURE_WAIT_STABLE_COUNT", "process", "압력 도달 연속 횟수", "회", I, 3, 1, 30,
       src="controller/process_controller.py:106"),
    _p("PRESSURE_STAGE_TIMEOUT_SEC", "process", "SP3·SP2 도달 한도(넘어도 진행)", "s", F, 180.0, 10.0, 1800.0,
       src="controller/process_controller.py:107", note="v1 은 틱 수로 세어 실제 2배 이상 걸렸다 — v2 는 실제 시각"),
    _p("SP1_TIMEOUT_SEC", "process", "SP1(WP) 도달 한도(넘으면 중단)", "s", F, 300.0, 30.0, 3600.0,
       src="controller/process_controller.py:628"),
    _p("POWER_WAIT_TIMEOUT_SEC", "process", "파워 도달 대기 최소 한도", "s", F, 600.0, 60.0, 7200.0),
    _p("POWER_WAIT_TIMEOUT_MAX_SEC", "process", "파워 도달 대기 절대 상한", "s", F, 7200.0, 600.0, 14400.0,
       safety=H, src="lib/config.py:676 (고정값)"),
    _p("POWER_WAIT_EST_FACTOR", "process", "파워 대기 예상 시간 배수", "배", F, 1.5, 1.0, 5.0, safety=L,
       src="controller/process_controller.py:997"),
    _p("POWER_WAIT_EST_MARGIN_SEC", "process", "파워 대기 예상 여유", "s", F, 120.0, 0.0, 1800.0, safety=L,
       src="controller/process_controller.py:997"),

    # ───────────── 정지(stop) ─────────────
    _p("STOP_FLOW_OFF_TIMEOUT_SEC", "stop", "STOP 유량 OFF 확인 한도(채널당)", "s", F, 6.0, 2.0, 60.0,
       src="controller/process_controller.py:31"),
    _p("STOP_EXHAUST_WAIT_SEC", "stop", "STOP MFC 밸브 열기(배기) 대기", "s", F, 8.0, 1.0, 60.0,
       src="controller/process_controller.py:1922"),

    # ───────────── 웹 연동(uplink) ─────────────
    _p("UPLINK_INTERVAL_MS", "uplink", "웹 전송 주기", "ms", I, 1000, 500, 10000, safety=L, apply=LIVE,
       src="reporter.py (_loop 1 s)"),
    _p("UPLINK_STATE_HEARTBEAT_SEC", "uplink", "변화 없을 때 state 재전송", "s", I, 10, 2, 60, safety=L,
       apply=LIVE, src="reporter.py"),
    _p("UPLINK_TIMEOUT_SEC", "uplink", "웹 요청 응답 제한", "s", F, 15.0, 2.0, 60.0, safety=L,
       src="reporter.py (_post timeout=15)"),
    _p("UPLINK_SPOOL_MAX_MB", "uplink", "미전송 보관 상한", "MB", I, 5, 1, 500, safety=L, src="reporter.py"),
    _p("UPLINK_SUMMARY_PERIOD_SEC", "uplink", "웹으로 보내는 스냅샷 요약 주기", "s", I, 60, 10, 600, safety=L,
       apply=LIVE, src=_V2),

    # ───────────── 기록(record) ─────────────
    _p("SNAPSHOT_RETENTION_DAYS", "record", "1초 기록 보관 기간", "일", I, 180, 7, 3650, safety=L, src=_V2),
    _p("NAS_SYNC_PERIOD_SEC", "record", "NAS 사본 주기", "s", I, 60, 10, 3600, safety=L, apply=LIVE, src=_V2),
)


# ───────────── 키 사이 규칙 ─────────────
def _r(id_, keys, desc, fn) -> CrossRule:
    return CrossRule(id=id_, keys=tuple(keys), desc=desc, check=fn)


def _need(cond: bool, msg: str) -> Optional[str]:
    return None if cond else msg


CROSS_RULES: tuple = (
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
               "RFPULSE_READ_DEADLINE_MS", "IG_READ_DEADLINE_MS"), "장비별 읽기 마감 ≤ tick 마감",
       lambda v: _need(all(v[k] <= v["TICK_DEADLINE_MS"] for k in
                           ("PLC_READ_DEADLINE_MS", "MFC_READ_DEADLINE_MS", "DC_READ_DEADLINE_MS",
                            "RFPULSE_READ_DEADLINE_MS", "IG_READ_DEADLINE_MS")),
                       "장비 읽기 마감이 tick 마감보다 길다")),
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
    _r("X22", ("PLC_PORT", "MFC_PORT", "DC_PORT", "RFPULSE_PORT", "IG_PORT"), "시리얼 포트 중복 없음",
       lambda v: _need(_ports_unique(v), "같은 COM 포트를 두 장비가 쓴다")),
    _r("X23", ("RUN_MODE", "UPLINK_URL"), "sim 모드는 웹 주소를 쓰지 않는다(쓰더라도 무시) — prod 는 주소 필요",
       lambda v: _need(v["RUN_MODE"] != "prod" or bool(v["UPLINK_URL"]),
                       "prod 인데 UPLINK_URL 이 비었다")),
)


def _ports_unique(v: Mapping[str, Any]) -> bool:
    ps = [str(v[k]).strip().upper() for k in ("PLC_PORT", "MFC_PORT", "DC_PORT", "RFPULSE_PORT", "IG_PORT")]
    ps = [p for p in ps if p]
    return len(ps) == len(set(ps))


# X23 은 RUN_MODE 기본값이 sim 이라 기본값에서 통과한다(UPLINK_URL 기본 "").


# ───────────── v2 에서 뺀 키·상수 ─────────────
DROPPED: tuple = (
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
    Dropped("IG_RESPONSE_TIMEOUT_MS", "응답 제한(1.2 s)이 주기(1 s)보다 길어 공백을 만들었다 — tick 마감으로 대체",
            "lib/config.py:756", replaced_by="IG_READ_DEADLINE_MS"),
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
