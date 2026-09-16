# config.py
import math
from typing import Dict, List, Tuple
from lib.config_loader import get  # ← JSON 설정 로더 (없으면 기본값 사용)

"""
장비 시리얼 통신, 공정 파라미터, 기본 명령어 등 전역 설정 파일
- 사용자 변경 가능한 값은 config_user.json에서 관리합니다.
- config_user.json이 없거나 오류 시 이 파일의 기본값을 사용합니다.
"""

# ================================================================
# 시리얼 포트 설정 — config_user.json에서 변경 가능
# ================================================================
MFC_PORT    = get('MFC_PORT',    "COM10")
MFC_BAUD    = get('MFC_BAUD',    9600)

DC_PORT     = get('DC_PORT',     "COM11")
DC_BAUDRATE = get('DC_BAUDRATE', 9600)

PLC_PORT    = get('PLC_PORT',    "COM9")

# 아래는 고정값 (사용자 변경 불필요)
PLC_BAUD     = 115200
PLC_SLAVE_ID = 1
PLC_TIMEOUT  = 0.5   # 초

# ── PLC 통신 내성 — 한 프레임 손실로 공정이 죽지 않게 ──
#  PLC 는 래더 인터락·히터 워치독(TON 10초)으로 자체 보호되므로 짧은 단절은 견딘다.
#  2026-09-16 사고: 코일 읽기 1회 타임아웃에 즉시 "재시작" 으로 중단됐고 재시도·포트
#  재오픈이 없었다(그 뒤 PLC CPU 자체가 정지한 것으로 판명).
PLC_RETRY_COUNT         = get('PLC_RETRY_COUNT',         2)     # 트랜잭션 실패 시 재시도 횟수(총 1+N 회)
PLC_RETRY_DELAY_MS      = get('PLC_RETRY_DELAY_MS',      30)    # 재시도 사이 대기(ms)
PLC_COMM_REOPEN_SEC     = get('PLC_COMM_REOPEN_SEC',     3.0)   # 이만큼 단절이면 포트 close→open 시도(그 뒤 같은 주기로 반복)
PLC_COMM_LOSS_ABORT_SEC = get('PLC_COMM_LOSS_ABORT_SEC', 10.0)  # 이만큼 단절이면 공정 중단 — PLC 히터 워치독 TON 10초와 같게

# ── PLC 콜드 스타트(재기동/메모리 초기화) 감지용 예비 D레지스터 ──
#  접속 시 난수 마커를 써 두고 ≈1초마다 되읽는다. 값이 바뀌면 PLC 가 재기동(D영역 초기화)된
#  것이라 히터 설정을 다시 밀어 넣고 상위에 알린다. 래더가 쓰지 않는 주소여야 한다 —
#  XG5000 편집→디바이스 사용 검색으로 확인. 0 이면 기능 끔.
PLC_SESSION_MARK_REG    = get('PLC_SESSION_MARK_REG',    60)    # D00060

# ── 시리얼 장비 공통 통신 두절 정책(DC 파워 / RF 펄스) ──
#  "끊기면 재연결 시도, 예산 안에 복구 안 되면 안전정지 + 구글챗, 복구되면 OFF 재적용".
#  MFC 는 이미 백오프 재연결이 있어 이벤트 기록만 붙인다.
DC_COMM_LOSS_ABORT_SEC       = get('DC_COMM_LOSS_ABORT_SEC',       10.0)  # 공정 중 이만큼 응답이 없으면 "재시작"
DC_RECONNECT_BACKOFF_START_MS = get('DC_RECONNECT_BACKOFF_START_MS', 500)
DC_RECONNECT_BACKOFF_MAX_MS   = get('DC_RECONNECT_BACKOFF_MAX_MS',  8000)
RFPULSE_COMM_LOSS_ABORT_SEC  = get('RFPULSE_COMM_LOSS_ABORT_SEC',  10.0)  # 폴링 중 이만큼 응답이 없으면 "재시작"
RFPULSE_COMM_REOPEN_SEC      = 3.0      # 무응답이 이만큼 이어지면 포트를 닫고 다시 연다(USB 재열거 대응)


def _validate_plc_comm_config() -> None:
    """설정이 틀려도 프로그램은 떠야 한다 — 클램프/끄기만 하고 예외는 던지지 않는다."""
    global PLC_RETRY_COUNT, PLC_RETRY_DELAY_MS, PLC_COMM_REOPEN_SEC, PLC_COMM_LOSS_ABORT_SEC
    global PLC_SESSION_MARK_REG
    try:
        PLC_RETRY_COUNT = max(0, min(5, int(PLC_RETRY_COUNT)))
    except Exception:
        print(f"[Config] PLC_RETRY_COUNT {PLC_RETRY_COUNT!r} → 2"); PLC_RETRY_COUNT = 2
    try:
        PLC_RETRY_DELAY_MS = max(0, min(1000, int(PLC_RETRY_DELAY_MS)))
    except Exception:
        print(f"[Config] PLC_RETRY_DELAY_MS {PLC_RETRY_DELAY_MS!r} → 30"); PLC_RETRY_DELAY_MS = 30
    try:
        PLC_COMM_REOPEN_SEC = max(0.5, float(PLC_COMM_REOPEN_SEC))
    except Exception:
        print(f"[Config] PLC_COMM_REOPEN_SEC {PLC_COMM_REOPEN_SEC!r} → 3.0"); PLC_COMM_REOPEN_SEC = 3.0
    try:
        PLC_COMM_LOSS_ABORT_SEC = max(1.0, float(PLC_COMM_LOSS_ABORT_SEC))
    except Exception:
        print(f"[Config] PLC_COMM_LOSS_ABORT_SEC {PLC_COMM_LOSS_ABORT_SEC!r} → 10.0"); PLC_COMM_LOSS_ABORT_SEC = 10.0
    if PLC_COMM_LOSS_ABORT_SEC < PLC_COMM_REOPEN_SEC:
        print(f"[Config] PLC_COMM_LOSS_ABORT_SEC {PLC_COMM_LOSS_ABORT_SEC:g} < REOPEN {PLC_COMM_REOPEN_SEC:g} → REOPEN 으로 상향")
        PLC_COMM_LOSS_ABORT_SEC = PLC_COMM_REOPEN_SEC
    # 마커 레지스터가 사용 중인 D 주소와 겹치면 기능을 끈다
    #  D00000~3 ADC, D00010~28 히터, D00029~32 예약, D00040~43 DAC
    _used = set(range(0, 4)) | set(range(10, 29)) | set(range(29, 33)) | set(range(40, 44))
    try:
        _m = int(PLC_SESSION_MARK_REG)
    except Exception:
        print(f"[Config] PLC_SESSION_MARK_REG {PLC_SESSION_MARK_REG!r} → 0(기능 끔)"); _m = 0
    if _m < 0:
        _m = 0
    if _m in _used:
        print(f"[Config] PLC_SESSION_MARK_REG D{_m:05d} 는 사용 중인 주소(ADC/히터/DAC/예약)와 겹침 → 0(기능 끔)")
        _m = 0
    PLC_SESSION_MARK_REG = _m


_validate_plc_comm_config()

# ── 목표 온도 도달 후 DAC 출력 상한(D00018)을 그 시점 출력으로 고정 ──
#  2026-09-15 사고: 메인 셔터가 열릴 때 지그 TC 가 실제보다 낮게 읽혀 PID 가 DAC 를
#  최대(1200 ≒143A)로 45분간 밀어붙였다. 파이썬은 D00041(DAC 값)을 직접 쓸 수 없다
#  (래더가 매 스캔 덮어씀) → PID 출력 상한 D00018 로 막는다. 내리는 방향은 막지 않는다.
HEATER_HOLD_MV_AFTER_REACH = get('HEATER_HOLD_MV_AFTER_REACH', False)   # 기능 on/off
HEATER_HOLD_MV_ENTER_TOL_C = get('HEATER_HOLD_MV_ENTER_TOL_C', 3.0)     # |PV-SV| 허용 오차 [°C]
HEATER_HOLD_MV_ENTER_SEC   = get('HEATER_HOLD_MV_ENTER_SEC',   60)      # 이만큼 연속 안정돼야 캡처 [초]


def _validate_heater_hold_config() -> None:
    """설정이 틀려도 프로그램은 떠야 한다 — 클램프/끄기만 하고 예외는 던지지 않는다."""
    global HEATER_HOLD_MV_AFTER_REACH, HEATER_HOLD_MV_ENTER_TOL_C, HEATER_HOLD_MV_ENTER_SEC
    v = HEATER_HOLD_MV_AFTER_REACH
    if isinstance(v, str):
        v = v.strip().lower() in ("1", "t", "true", "y", "yes", "on")
    HEATER_HOLD_MV_AFTER_REACH = bool(v)
    try:
        t = float(HEATER_HOLD_MV_ENTER_TOL_C)
    except Exception:
        print(f"[Config] HEATER_HOLD_MV_ENTER_TOL_C {HEATER_HOLD_MV_ENTER_TOL_C!r} → 3.0"); t = 3.0
    tc = min(max(t, 0.5), 20.0)
    if tc != t:
        print(f"[Config] HEATER_HOLD_MV_ENTER_TOL_C {t:g} → {tc:g} 로 클램프 (허용 0.5~20)")
    HEATER_HOLD_MV_ENTER_TOL_C = tc
    try:
        s = float(HEATER_HOLD_MV_ENTER_SEC)
    except Exception:
        print(f"[Config] HEATER_HOLD_MV_ENTER_SEC {HEATER_HOLD_MV_ENTER_SEC!r} → 60"); s = 60.0
    sc = min(max(s, 10.0), 600.0)
    if sc != s:
        print(f"[Config] HEATER_HOLD_MV_ENTER_SEC {s:g} → {sc:g} 로 클램프 (허용 10~600)")
    HEATER_HOLD_MV_ENTER_SEC = sc


_validate_heater_hold_config()

# 인터락 기준값/공정 파라미터 등
INTERLOCK_CHECK_INTERVAL = 0.2  # sec

# === RF 피드백(ADC) ===
RF_ADC_FORWARD_ADDR = 0      # D00000 -> Holding Register 0
RF_ADC_REFLECT_ADDR = 1      # D00001 -> Holding Register 1
RF_ADC_MAX_COUNT    = 4000   # 모듈 사양

# === RF DAC ===
RF_DAC_ADDR_CH0     = 40    # D00040 -> Holding Register 64
COIL_ENABLE_DAC_CH0 = 320   # U02.02.0 -> Coil 320

# ================================================================
# 히터 (PLC 내장 PID — HEATER 스캔 프로그램)
# ================================================================
# --- 홀딩 레지스터 (D영역) ---
HEATER_REG_BLOCK_START = 10   # D00010~D00028 연속 19개 배치 읽기
HEATER_REG_BLOCK_COUNT = 19
HEATER_REG_PV       = 10      # D00010 TEMP_READ_1 (signed, -1=이상)
HEATER_REG_SV       = 12      # D00012 목표 온도        [W]
HEATER_REG_SV_LIMIT = 13      # D00013 목표 상한        [W] 접속 시 JSON 값으로 복구
HEATER_REG_WD       = 14      # D00014 워치독 카운터    [W]
HEATER_REG_CUR_SV   = 16      # D00016 램프 적용 목표   [R]
HEATER_REG_PID_ERR  = 17      # D00017 PID 에러 코드    [R]
HEATER_REG_MV       = 41      # D00041 DAC 출력 카운트  [R]

# --- 램프/한계값 (래더가 D레지스터로 노출. 접속 시 JSON 값으로 복구한다) ---
HEATER_REG_MV_LIMIT  = 18     # D00018 DAC 하드리밋      [W]
HEATER_REG_SV_RAMP   = 19     # D00019 램프 중간 목표    [R]
HEATER_REG_RAMP_RATE = 20     # D00020 램프 속도(카운트/초) [W]
HEATER_REG_HOLDBACK  = 21     # D00021 홀드백 폭(0.1°C)  [W]
HEATER_REG_OT_LIMIT  = 26     # D00026 과온 트립(0.1°C)  [W]
HEATER_REG_SLOW_ZONE = 27     # D00027 감속 구간 폭(0.1°C) [W]
HEATER_REG_SLOW_RATE = 28     # D00028 감속 구간 램프 속도 [W]

# --- 코일 (M영역, 워드×16+비트) ---
HEATER_COIL_BASE    = 64      # M00040~M00049 연속 10개
HEATER_COIL_COUNT   = 10
HEATER_COIL_RUN     = 64      # M00040 운전 요구        [W]
HEATER_COIL_ITL     = 65      # M00041 인터락 정상      [R]
HEATER_COIL_FAULT   = 66      # M00042 이상 종합        [R]
HEATER_COIL_RST     = 67      # M00043 이상 리셋        [W]
HEATER_COIL_OT      = 68      # M00044 과온 트립
HEATER_COIL_TC_ERR  = 69      # M00045 온도센서 이상
HEATER_COIL_WD_ERR  = 70      # M00046 워치독 타임아웃
HEATER_COIL_AT_REQ  = 71      # M00047 오토튜닝 요구    [W]
HEATER_COIL_AT_DONE = 72      # M00048 오토튜닝 완료
HEATER_COIL_PID_RUN = 73      # M00049 PID 동작 중

# --- DAC 출력 범위 ---
#   DAC(XBF-DV04A): 0~10V 를 0~4000 카운트로 출력  →  1 카운트 = 2.5mV
#   VSCD-30 입력 사양: 0.8~4V  →  카운트로는 320~1600 이다.
#
#   눈금:
#      400  출력 0점. PLC K1227(MV 최소값)과 반드시 같아야 한다 — 현재 양쪽 다 400.
#           다르면 화면 출력%가 틀어진다.
#     1200  현재 운전 상한 HEATER_MV_LIMIT (약 121A 환산)
#     1300  현재 절대 상한 HEATER_MV_ABS_MAX
#     1600  VSCD-30 의 4V 규격 상한. 래더가 D00018 을 여기로 클램프한다.
#
#   ※ 운전 상한 HEATER_MV_LIMIT 은 접속 시 D00018로 전송되며,
#      래더가 이를 PID 최대 조작값으로 넘긴다. 코드는 절대
#      HEATER_MV_ABS_MAX 위로는 쓰지 않는다.
#   ※ 전류 표시는 아래 heater_est_current()(위상제어식)만 쓴다.
#      카운트에 비례하는 선형식이 아니다.
HEATER_MV_MIN     = get('HEATER_MV_MIN',     400)   # PLC K1227과 일치. 출력 0% 지점
HEATER_MV_ABS_MAX = get('HEATER_MV_ABS_MAX', 1300)  # 절대 안전 상한
HEATER_MV_LIMIT   = get('HEATER_MV_LIMIT',   1200)  # D00018에 쓸 실제 운전 상한

# ---- 위상제어 전류 추정 (2026-09-01 재교정) ----
# VARITAP은 위상제어라 전류가 DAC 카운트에 비례하지 않는다.
# 점호각 α가 카운트에 선형이라고 보고 위상제어 실효값 공식을 쓴다.
#     α = π × (MV_FULL − MV) / (MV_FULL − MV_ZERO)
#     I = SCALE × √[(π − α + sin2α/2) / π]
# 실측 앵커 3점을 ±6% 안에 재현한다:
#     MV 520 → 11.5A(저온) · MV 600 → 22.4A(저온) · MV 800 → 59.8A(600°C 유지)
#     ↑ MV 800 이 600°C 유지점이었다(실측). 앵커 밖(600°C 이상)은 외삽이므로
#       그 구간은 전면 전류계를 우선한다.
# 저온 2점으로 맞춘 옛 선형식은 MV 800 에서 49.5A(17% 과소)여서 폐기했다.
HEATER_CURRENT_MV_ZERO = get('HEATER_CURRENT_MV_ZERO', 400)    # 출력 0   (점호각 180°)
HEATER_CURRENT_MV_FULL = get('HEATER_CURRENT_MV_FULL', 1600)   # 전출력   (점호각 0°)
HEATER_CURRENT_SCALE   = get('HEATER_CURRENT_SCALE',   135.1)  # 전출력 시 전류 [A]


def heater_est_current(mv) -> float:
    """DAC 카운트 → 전면 전류 추정 [A]. 추정치이며 계기 대체용이 아니다."""
    try:
        mv = float(mv)
    except (TypeError, ValueError):
        return 0.0
    span = float(HEATER_CURRENT_MV_FULL) - float(HEATER_CURRENT_MV_ZERO)
    if span <= 0:
        return 0.0
    x = (float(HEATER_CURRENT_MV_FULL) - mv) / span      # 1=완전 차단, 0=전출력
    a = min(max(x, 0.0), 1.0) * math.pi                  # 점호각 [rad]
    v = (math.pi - a + math.sin(2.0 * a) / 2.0) / math.pi
    return float(HEATER_CURRENT_SCALE) * math.sqrt(max(0.0, v))

# --- config_user.json에서 변경 가능 ---
HEATER_ENABLED          = get('HEATER_ENABLED',          True)
HEATER_TEMP_SCALE       = get('HEATER_TEMP_SCALE',       0.1)   # ★ 실측 후 확정
HEATER_WD_PERIOD_MS     = get('HEATER_WD_PERIOD_MS',     3000)  # PLC 10초의 1/3
HEATER_MAX_TEMP         = get('HEATER_MAX_TEMP',         500.0) # UI 입력 상한
HEATER_SOAK_TOLERANCE   = get('HEATER_SOAK_TOLERANCE',   3.0)   # °C
HEATER_SOAK_TIME_SEC    = get('HEATER_SOAK_TIME_SEC',    60)    # 공정 레시피 전용
HEATER_WAIT_TIMEOUT_SEC = get('HEATER_WAIT_TIMEOUT_SEC', 3600)  # RAMP 대기 최대
# 히터 OFF 후 가스·압력을 유지하는 PV 상한 [°C]. 이 아래로 식으면 해제한다.
HEATER_GAS_HOLD_RELEASE_C = get('HEATER_GAS_HOLD_RELEASE_C', 100.0)

# --- PLC로 밀어 넣는 한계값 (사람 단위. PLC raw 변환은 PLC.py가 한다) ---
HEATER_SV_LIMIT_C          = get('HEATER_SV_LIMIT_C',          500.0)  # D00013 [°C]
HEATER_RAMP_RATE_C_PER_MIN = get('HEATER_RAMP_RATE_C_PER_MIN', 12.0)   # D00020 [°C/min]
HEATER_HOLDBACK_C          = get('HEATER_HOLDBACK_C',          2.0)    # D00021 [°C]
HEATER_OT_LIMIT_C          = get('HEATER_OT_LIMIT_C',          550.0)  # D00026 [°C]
HEATER_SLOW_ZONE_C         = get('HEATER_SLOW_ZONE_C',         10.0)   # D00027 [°C]
# 마지막 스텝 목표가 이 온도 이하면 냉각 스텝으로 본다(램프 없이 대기)
HEATER_COOLDOWN_TARGET_C   = get('HEATER_COOLDOWN_TARGET_C',   30.0)   # [°C]
HEATER_SLOW_RATE_C_PER_MIN = get('HEATER_SLOW_RATE_C_PER_MIN', 6.0)    # D00028 [°C/min]
HEATER_PUSH_CONFIG         = get('HEATER_PUSH_CONFIG',         True)   # False면 PLC 쓰기 생략

# --- 감속 접근(approach) : 파이썬이 만드는 램프의 목표 직전 감속 ---
#  래더 2단 감속(D00027/D00028)은 계단식이라 전환 지점에서 오버슈트가 남는다.
#  대신 파이썬이 SV 를 밀어 올리면서 남은 거리에 비례해 속도를 줄여, 도착
#  순간의 잉여 전력을 없앤다(2026-09-10 수동 100°C 테스트: 6°C/min 으로
#  도착 → 19A 잔류 → +5°C 오버슈트).
HEATER_APPROACH_ZONE_C           = get('HEATER_APPROACH_ZONE_C', 20.0)   # 목표 직전 감속 접근 구간 [°C]. 0 이면 감속 없음
HEATER_APPROACH_MIN_RATE_C_PER_MIN = get('HEATER_APPROACH_MIN_RATE_C_PER_MIN', 1.0)  # 접근 구간 끝(도착) 속도 [°C/min]
HEATER_APPROACH_LEAD_C           = get('HEATER_APPROACH_LEAD_C', 1.0)    # 파이썬 SV 가 래더 램프(D00019)보다 앞설 수 있는 최대 폭 [°C]

# --- 히터 레시피 / 로깅 ---
HEATER_RECIPE_DIR         = get('HEATER_RECIPE_DIR', '')        # 빈 문자열이면 프로그램 폴더
HEATER_LOG_ENABLED        = get('HEATER_LOG_ENABLED', True)
HEATER_LOG_PERIOD_MS      = get('HEATER_LOG_PERIOD_MS', 3000)
HEATER_RECIPE_MAX_STEPS   = get('HEATER_RECIPE_MAX_STEPS', 20)
HEATER_RECIPE_HOLD_AT_END = get('HEATER_RECIPE_HOLD_AT_END', False)  # True면 마지막 목표 유지


def _validate_heater_config() -> None:
    """JSON 값이 위험하거나 앞뒤가 안 맞으면 안전한 쪽으로 클램프한다.
    예외는 던지지 않는다 — 설정이 틀려도 프로그램은 떠야 한다."""
    global HEATER_MV_LIMIT, HEATER_OT_LIMIT_C, HEATER_MAX_TEMP
    global HEATER_RAMP_RATE_C_PER_MIN, HEATER_SLOW_RATE_C_PER_MIN
    global HEATER_APPROACH_ZONE_C, HEATER_APPROACH_MIN_RATE_C_PER_MIN
    global HEATER_APPROACH_LEAD_C
    global HEATER_LOG_PERIOD_MS
    global HEATER_CURRENT_SCALE, HEATER_CURRENT_MV_FULL, HEATER_CURRENT_MV_ZERO

    # 1) DAC 운전 상한: 최소 지점 +20 ~ 절대 상한
    lo, hi = HEATER_MV_MIN + 20, HEATER_MV_ABS_MAX
    if not (lo <= HEATER_MV_LIMIT <= hi):
        print(f"[Config] HEATER_MV_LIMIT {HEATER_MV_LIMIT} → "
              f"{min(max(HEATER_MV_LIMIT, lo), hi)} 로 클램프 (허용 {lo}~{hi})")
        HEATER_MV_LIMIT = min(max(HEATER_MV_LIMIT, lo), hi)

    # 2) 과온 트립은 목표 상한보다 충분히 높아야 한다
    if HEATER_OT_LIMIT_C < HEATER_SV_LIMIT_C + 20:
        new = HEATER_SV_LIMIT_C + 50
        print(f"[Config] HEATER_OT_LIMIT_C {HEATER_OT_LIMIT_C} → {new} 로 상향 "
              f"(SV 상한 {HEATER_SV_LIMIT_C} 보다 최소 20°C 높아야 함)")
        HEATER_OT_LIMIT_C = new

    # 3) UI 입력 상한이 PLC 소프트 상한을 넘을 수 없다
    if HEATER_MAX_TEMP > HEATER_SV_LIMIT_C:
        print(f"[Config] HEATER_MAX_TEMP {HEATER_MAX_TEMP} → {HEATER_SV_LIMIT_C} 로 하향 "
              f"(HEATER_SV_LIMIT_C 초과 불가)")
        HEATER_MAX_TEMP = HEATER_SV_LIMIT_C

    # 4) 램프 속도 최소 1카운트/초 = 6°C/min
    if HEATER_RAMP_RATE_C_PER_MIN < 6.0:
        print(f"[Config] HEATER_RAMP_RATE_C_PER_MIN {HEATER_RAMP_RATE_C_PER_MIN} → 6.0 (최소 1카운트/초)")
        HEATER_RAMP_RATE_C_PER_MIN = 6.0
    if HEATER_SLOW_RATE_C_PER_MIN < 6.0:
        print(f"[Config] HEATER_SLOW_RATE_C_PER_MIN {HEATER_SLOW_RATE_C_PER_MIN} → 6.0 (최소 1카운트/초)")
        HEATER_SLOW_RATE_C_PER_MIN = 6.0

    # 4-1) 감속 접근 — 구간은 음수 불가, 도착 속도는 래더 최소(6°C/min)를 넘으면
    #      감속이 되지 않는다. 앞섬 폭이 너무 작으면 SV 가 래더를 못 끌고 간다.
    if HEATER_APPROACH_ZONE_C < 0:
        print(f"[Config] HEATER_APPROACH_ZONE_C {HEATER_APPROACH_ZONE_C} → 0 (음수 불가 — 감속 없음)")
        HEATER_APPROACH_ZONE_C = 0.0
    if HEATER_APPROACH_MIN_RATE_C_PER_MIN <= 0:
        print(f"[Config] HEATER_APPROACH_MIN_RATE_C_PER_MIN {HEATER_APPROACH_MIN_RATE_C_PER_MIN} → 1.0 (0 이하 불가)")
        HEATER_APPROACH_MIN_RATE_C_PER_MIN = 1.0
    elif HEATER_APPROACH_MIN_RATE_C_PER_MIN > 6.0:
        print(f"[Config] HEATER_APPROACH_MIN_RATE_C_PER_MIN {HEATER_APPROACH_MIN_RATE_C_PER_MIN} → 6.0 "
              f"(래더 최소 램프 이상이면 감속 의미가 없다)")
        HEATER_APPROACH_MIN_RATE_C_PER_MIN = 6.0
    if HEATER_APPROACH_LEAD_C < 0.2:
        print(f"[Config] HEATER_APPROACH_LEAD_C {HEATER_APPROACH_LEAD_C} → 0.2 (너무 작으면 SV 가 래더를 끌지 못한다)")
        HEATER_APPROACH_LEAD_C = 0.2

    # 5) 로깅 주기: 너무 짧으면 NAS I/O 폭주, 너무 길면 RAMP 곡선이 뭉갠다
    if not (1000 <= HEATER_LOG_PERIOD_MS <= 60000):
        new = min(max(int(HEATER_LOG_PERIOD_MS), 1000), 60000)
        print(f"[Config] HEATER_LOG_PERIOD_MS {HEATER_LOG_PERIOD_MS} → {new} 로 클램프 (1000~60000)")
        HEATER_LOG_PERIOD_MS = new

    # 6) 위상제어 전류 추정 계수
    if HEATER_CURRENT_SCALE <= 0:
        print(f"[Config] HEATER_CURRENT_SCALE {HEATER_CURRENT_SCALE} → 135.1 (0 이하 불가)")
        HEATER_CURRENT_SCALE = 135.1
    if HEATER_CURRENT_MV_FULL <= HEATER_CURRENT_MV_ZERO:
        print(f"[Config] HEATER_CURRENT_MV_FULL {HEATER_CURRENT_MV_FULL} / "
              f"MV_ZERO {HEATER_CURRENT_MV_ZERO} → 1600 / 400 "
              f"(MV_FULL 은 MV_ZERO 보다 커야 한다)")
        HEATER_CURRENT_MV_FULL = 1600
        HEATER_CURRENT_MV_ZERO = 400


_validate_heater_config()

# PLC 주소 맵핑 (고정 — 배선표 기준)
PLC_COIL_MAP: Dict[str, int] = {
    "Rotary_button":  0,   # M00000
    "RV_button":      1,   # M00001
    "FV_button":      2,   # M00002
    "MV_button":      3,   # M00003
    "Vent_button":    4,   # M00004
    "Turbo_button":   5,   # M00005
    "Doorup_button":  6,   # M00006 (Door Up)
    "Ar_Button":      7,   # M00007
    "O2_Button":      8,   # M00008
    "MS_button":      9,   # M00009
    "S1_button":     16,   # M00010
    "S2_button":     17,   # M00011
    "Doordn_button": 32,   # M00020 (Door Down)
    "BuzzStop_Button":33,  # M00021 (버저)
    "ION_button":    80,   # M00050 → P00160 → D-sub 14 (이오나이저 #1 Remote On)
}

# 센서 DI (고정)
PLC_SENSOR_BITS: Dict[str, int] = {
    "Air":   160,  # M00100
    "G1":    161,  # M00101
    "G2":    162,  # M00102
    "ATM":   163,  # M00103
    "Water": 164,  # M00104
    # --- 이오나이저 SVC-K24 (D-sub 25P 접점) ---
    "ION_RUN":  180,  # M00114 ← P0000E ← D-sub 1 (구동 상태)
    "ION_LAMP": 181,  # M00115 ← P0000F ← D-sub 2 (실제 점등)
    "ION_OT":   182,  # M00116 ← P00010 ← D-sub 6 (램프 수명 초과)
}

# 공정 시작 인터록: 메인밸브(MV)가 실제 열린 상태(MV & MV_INTERLOCK 모두 ON)일 때만 시작 허용
# M영역 → Modbus 코일 = (워드 4자리) × 16 + (비트 1자리)
PLC_MV_COIL           = 3    # M00003 (Main Valve open 명령 비트)
PLC_MV_INTERLOCK_COIL = 50   # M00032 (Main Valve interlock,  3×16+2)

# ================================================================
# DC Power 설정
# ================================================================
# 고정값
DC_INITIAL_VOLTAGE  = 450.0  # 초기 전압(V)
DC_INITIAL_CURRENT  = 0.1    # 초기 전류(A)
DC_MAX_VOLTAGE      = 600.0  # 최대 전압(V)
DC_MAX_CURRENT      = 1.0    # 최대 전류(A)
DC_MAX_POWER        = 500.0  # 최대 파워(W)
DC_TOLERANCE_WATT   = 0.5    # 목표 Power 허용 오차(W)
DC_MAX_ERROR_COUNT  = 5      # 연속 측정 실패 허용 횟수
DC_FAIL_ISET_THRESHOLD  = 0.20  # 램프업 무응답: 설정 전류 기준(A)
DC_FAIL_POWER_THRESHOLD = 1.0   # 램프업 무응답: 파워 기준(W)

# config_user.json에서 변경 가능
DC_POWER_ERROR_RATIO       = get('DC_POWER_ERROR_RATIO',       0.10)  # ±10% 허용
DC_POWER_ERROR_MAX_COUNT   = get('DC_POWER_ERROR_MAX_COUNT',   5)     # 5회 연속 시 중단
DC_MIN_CURRENT_ABORT       = get('DC_MIN_CURRENT_ABORT',       0.05)  # 저전류 기준(A)
DC_MIN_CURRENT_ABORT_COUNT = get('DC_MIN_CURRENT_ABORT_COUNT', 10)    # 10회 연속 시 중단
DC_FAIL_MAX_TICKS          = get('DC_FAIL_MAX_TICKS',          15)    # 램프업 무응답 시간(초)

# DC Power Delay: SP1 도달 후 Shutter Delay 시작 전, 파워 안정화 대기 시간(초)
# 이 구간에서는 DC ±% 이탈 abort가 비활성. 저전류/램프업 보호는 그대로 동작.
DC_POWER_DELAY_SEC = get('DC_POWER_DELAY_SEC', 300)   # 기본 5분

# --- DC 파워 제어식 (lib/dc_control.py) ---
# 서플라이는 정전류(CC) 모드라 전압 V 는 플라즈마(압력)가 정한다. P = V·I 이므로
# 목표 파워를 맞추는 데 필요한 전류 변화량은  ΔI = (P_target − P_now) / V_now  로 바로 나온다.
# 압력 단계 전환(SP4→SP3→SP2→SP1)마다 V 가 5~30% 뛰어도 그 비율만큼 즉시 따라가므로
# 목표 파워·압력이 달라져도 값을 다시 맞출 필요가 없다. 아래는 안전용 상한/이득이다.
#   2026-09-09 로그(Hf 250 W, WP 2 mTorr) 재현 결과: 기존 고정 스텝(0.001 A/s)은 SP1 후
#   +11.5% 에서 정체 → 5회 이탈 중단. 새 식은 최대 편차 +6%(1초), 3초 안에 ±0.5% 복귀.
DC_CONTROL_GAIN         = get('DC_CONTROL_GAIN',         1.0)    # ΔI 계수. 1.0 = 한 번에 맞춤
DC_RAMP_STEP_A          = get('DC_RAMP_STEP_A',          0.010)  # 램프업 1초당 전류 상승 상한(A)
DC_MAINTAIN_STEP_UP_A   = get('DC_MAINTAIN_STEP_UP_A',   0.020)  # 유지 중 1초당 상승 상한(A)
DC_MAINTAIN_STEP_DOWN_A = get('DC_MAINTAIN_STEP_DOWN_A', 0.050)  # 1초당 하강 상한(A). 글리치 1회 영향 ≤ 약 6%
# 유지 중 작은 오차는 플라즈마 노이즈일 수 있다. 매초 전량 보정하면 1초 주기로
# 상승/하강을 번갈아 하며 스스로 흔든다(2026-09-09 17:00 로그: 편차 자기상관 -0.59,
# 900초 중 473초 보정). 작은 오차 구간에서만 이득을 낮추면 전환 응답은 그대로 두고
# 흔들림만 준다(재현: std 0.82 -> 0.62 W, 보정 478 -> 360회).
DC_SMALL_ERROR_RATIO = get('DC_SMALL_ERROR_RATIO', 0.01)  # 목표의 이 비율 이하 오차는 "작은 오차"(노이즈 가능성) — 이득을 낮춰 반응
DC_SMALL_ERROR_GAIN  = get('DC_SMALL_ERROR_GAIN',  0.5)   # 작은 오차 구간 이득. 1.0 이면 기존과 동일. 0.3~0.7 권장, 1 초과 금지(코드에서 0.1~1.0 클램프)
DC_SMALL_ERROR_GAIN  = min(1.0, max(0.1, float(DC_SMALL_ERROR_GAIN)))
DC_SMALL_ERROR_RATIO = max(0.0, float(DC_SMALL_ERROR_RATIO))
# 램프업 중 전류/전압 상한에 걸린 채 목표 미달이 이 시간(초) 이어지면 유지 단계로 넘긴다.
# (상한에서는 기다려도 파워가 안 오른다. 압력 step-down 이 진행되어야 V 가 올라 도달한다.)
DC_LIMIT_STALL_SEC      = get('DC_LIMIT_STALL_SEC',      10)

# ================================================================
# RF Power 설정
# ================================================================
# 고정값
RF_FORWARD_SCALING_MAX_WATT  = 594.5   # for.p 센서 교정 상수
RF_REFLECTED_SCALING_MAX_WATT = 200.0  # ref.p 센서 교정 상수
RF_DAC_FULL_SCALE  = 4000   # PLC DAC 풀스케일
RF_MAX_POWER       = 600.0  # RF Power 장비 최대값(W)
RF_RAMP_STEP       = 1      # 램프업 스텝(W)
RF_RAMP_DOWN_STEP  = 8      # 램프다운 스텝(W)
RF_RAMP_DELAY      = 1      # 램프업 딜레이(초)
RF_MAX_ERROR_COUNT = 5      # 연속 실패 허용 횟수
RF_TOLERANCE_POWER = 1.0    # 목표 Power 허용 오차(W)
RF_FAIL_DAC_THRESHOLD  = 100  # 무응답 보호: DAC 기준
RF_FAIL_FORP_THRESHOLD = 1.0  # 무응답 보호: for.p 기준(W)

# config_user.json에서 변경 가능
RF_POWER_ERROR_RATIO    = get('RF_POWER_ERROR_RATIO',    0.10)  # ±10% 허용
RF_POWER_ERROR_MAX_COUNT= get('RF_POWER_ERROR_MAX_COUNT',5)     # 5회 연속 시 중단
RF_FAIL_MAX_TICKS       = get('RF_FAIL_MAX_TICKS',       10)    # 램프업 무응답 시간(초)
RF_REFP_ABORT_THRESHOLD = get('RF_REFP_ABORT_THRESHOLD', 20.0)  # Ref.P 대기 시작 임계값(W)
RF_REFP_WAIT_SEC        = get('RF_REFP_WAIT_SEC',        15)    # Ref.P 대기 허용 시간(초)

# ================================================================
# RF Pulse 설정 (CESAR 1310, AE Bus RS-232)
# ================================================================
# PLC DAC 로 제어하는 위 RF Power 와는 완전히 별개인 장비다.
#  시리얼 직결(COM)만 지원한다. 화면의 for.P/ref.P 칸만 공유한다.

# config_user.json에서 변경 가능
# ★ RFPULSE_PORT 는 실제 포트가 아직 확정되지 않았다.
#    장비를 연결한 뒤 config_user.json 에서 반드시 실제 COM 포트로 고칠 것.
RFPULSE_PORT      = get('RFPULSE_PORT',      "COM12")
RFPULSE_BAUD      = get('RFPULSE_BAUD',      9600)
RFPULSE_ADDR      = get('RFPULSE_ADDR',      1)       # AE Bus 주소 0~31
# ★ CESAR 매뉴얼 RS-232 규격: "Odd parity, one start bit, eight data bits, one stop bit"
#   = 9600 8O1. 2026-09-15 22:21 실기에서 8N1 로 열었더니 SET_ACTIVE_CTRL 이 4회 연속
#   즉시 NAK(체크섬 불일치) — 홀수 패리티 수신기에 패리티 없는 프레임을 보내면 1의 개수가
#   홀수인 바이트(0E, 02)가 매번 패리티 오류로 깨진다. 챔버2 는 TCP→시리얼 컨버터가
#   패리티를 담당해서 코드에 드러나지 않았다. 허용값 none / odd / even.
RFPULSE_PARITY    = get('RFPULSE_PARITY',    'odd')


def _validate_rfpulse_parity() -> None:
    global RFPULSE_PARITY
    _v = str(RFPULSE_PARITY or '').strip().lower()
    if _v not in ('none', 'odd', 'even'):
        print(f"[Config] RFPULSE_PARITY {RFPULSE_PARITY!r} → 'odd' (허용값 none/odd/even, CESAR 규격은 odd)")
        _v = 'odd'
    RFPULSE_PARITY = _v


_validate_rfpulse_parity()
RFPULSE_MAX_POWER = get('RFPULSE_MAX_POWER', 600.0)   # 장비 최대값(W)
# 펄스 주파수/듀티 시작 전 범위 검증 — CESAR 1310 매뉴얼: 펄스 주파수 1 Hz~30 kHz(3-5, 5-44),
#  듀티 1~99 %(4-75). 30 kHz 에서는 40~60 % 로 좁아지는데(5-33) 그건 막지 않는다 —
#  장비가 CSR 51 로 알려 주고 드라이버의 CSR 50/51 처리가 최후 방어선이다.
RFPULSE_PULSE_FREQ_MAX_HZ = get('RFPULSE_PULSE_FREQ_MAX_HZ', 30000)   # 다른 모델이면 config 에서 수정
RFPULSE_PULSE_FREQ_MIN_HZ = 1
RFPULSE_DUTY_MIN = 1
RFPULSE_DUTY_MAX = 99

# 프레임 단위 송수신 로그(챔버2 원본의 [RFP][RAW][TX]/[RX] 수준). 통신 문제 추적용.
#  False 면 전송 줄의 raw= 와 수신 줄 전체를 생략하고 기존 로그만 남긴다.
RFPULSE_RAW_LOG = get('RFPULSE_RAW_LOG', True)
# START 후 1회 리드백(193 주파수 / 196 듀티)이 공정이 요청한 freq/duty 와 다르거나 실패하면
#  "재시작" 으로 공정을 중단하고 드라이버가 스스로 RF OFF 한다. False 면 경고만 남긴다.
#  비워 둔(장비값 유지) 항목은 검증하지 않는다. 주파수 허용 오차 1 Hz(정수라 사실상 일치).
RFPULSE_VERIFY_PULSE_CONFIG = get('RFPULSE_VERIFY_PULSE_CONFIG', True)

# 고정값 — 원본(Chamber.Total_program) 값을 그대로 옮겼다. 바꾸지 말 것.
RFPULSE_ACK_TIMEOUT_MS        = 2000   # 쓰기(exec) CSR 대기
RFPULSE_QUERY_TIMEOUT_MS      = 4500   # 읽기(query) 데이터 프레임 대기
RFPULSE_RECV_FRAME_TIMEOUT_MS = 4000
RFPULSE_CMD_GAP_MS            = 1500   # 인터커맨드 최소 간격
RFPULSE_POST_WRITE_DELAY_MS   = 1500
RFPULSE_ACK_FOLLOWUP_GRACE_MS = 500
RFPULSE_POLL_INTERVAL_MS      = 5000   # 폴링 주기(STATUS→FWD→REF 한 바퀴)
RFPULSE_POLL_QUERY_TIMEOUT_MS = 9000
RFPULSE_POLL_START_DELAY_AFTER_RF_ON_MS = 800
RFPULSE_WATCHDOG_INTERVAL_MS       = 3000
RFPULSE_RECONNECT_BACKOFF_START_MS = 2000
RFPULSE_RECONNECT_BACKOFF_MAX_MS   = 30000

# 파워 감시 — 기본값은 원본과 같고, 현장에서 조정할 수 있게 config_user.json 경유다.
#  (타이밍/백오프는 프로토콜 타이밍이라 위처럼 고정값으로 둔다)
RFPULSE_FORP_TOLERANCE_PERCENT = get('RFPULSE_FORP_TOLERANCE_PERCENT', 5.0)   # setpoint 대비 허용 오차(%)
RFPULSE_FORP_CONSECUTIVE_LIMIT = get('RFPULSE_FORP_CONSECUTIVE_LIMIT', 3)     # 연속 이탈 허용 횟수
RFPULSE_REFP_LIMIT_WATTS       = get('RFPULSE_REFP_LIMIT_WATTS',       20.0)  # 반사파 허용 상한(W)
RFPULSE_REFP_CONSECUTIVE_LIMIT = get('RFPULSE_REFP_CONSECUTIVE_LIMIT', 3)     # 연속 초과 허용 횟수

# ── 파워 목표 도달 대기 타임아웃 [초] — "최소값" ──
#  process_controller._power_wait 이 DC / RF / RF Pulse 의 target_reached 를 기다리는
#  시간의 하한. 실제 타임아웃은 이번 공정의 목표값으로 산정한 예상 램프 시간
#  (RF 1 W/s 실측, DC 전류 램프, 펄스 START 시퀀스)의 1.5배 + 120초와 이 값 중 큰 쪽이다.
#  고정값으로 두면 RF 600W(램프만 600초) 같은 정상 공정이 타임아웃에 걸린다.
#  드라이버가 아무 신호도 못 내는 경로(펄스 포트 닫힘, 시리얼 오류 무한 재연결 등)가
#  실재하므로, DC/RF 자체의 램프업 무응답 보호(DC_FAIL_MAX_TICKS / RF_FAIL_MAX_TICKS)
#  위에 덮는 마지막 그물이다. 초과 시 "재시작" 으로 공정을 중단한다.
POWER_WAIT_TIMEOUT_SEC     = get('POWER_WAIT_TIMEOUT_SEC', 600)   # 최소 10분
POWER_WAIT_TIMEOUT_MAX_SEC = 7200                                # 산정 결과 상한(2시간)


def _validate_power_wait_config() -> None:
    """60초 미만이면 RF 램프(~200초)도 못 기다리고, 2시간을 넘기면 그물 구실을 못 한다.
    예외는 던지지 않는다 — 설정이 틀려도 프로그램은 떠야 한다."""
    global POWER_WAIT_TIMEOUT_SEC
    try:
        _v = float(POWER_WAIT_TIMEOUT_SEC)
    except Exception:
        print(f"[Config] POWER_WAIT_TIMEOUT_SEC {POWER_WAIT_TIMEOUT_SEC!r} → 600 (숫자가 아님)")
        _v = 600.0
    _c = min(max(_v, 60.0), 7200.0)
    if _c != _v:
        print(f"[Config] POWER_WAIT_TIMEOUT_SEC {_v:g} → {_c:g} 로 클램프 (허용 60~7200초)")
    POWER_WAIT_TIMEOUT_SEC = _c


_validate_power_wait_config()


def _validate_rfpulse_config() -> None:
    """감시 임계값이 말이 안 되면 안전한 쪽으로 클램프한다.
    예외는 던지지 않는다 — 설정이 틀려도 프로그램은 떠야 한다."""
    global RFPULSE_FORP_TOLERANCE_PERCENT, RFPULSE_FORP_CONSECUTIVE_LIMIT
    global RFPULSE_REFP_LIMIT_WATTS, RFPULSE_REFP_CONSECUTIVE_LIMIT
    global RFPULSE_MAX_POWER

    if not (RFPULSE_FORP_TOLERANCE_PERCENT > 0):
        print(f"[Config] RFPULSE_FORP_TOLERANCE_PERCENT {RFPULSE_FORP_TOLERANCE_PERCENT} → 5.0 (0 이하 불가)")
        RFPULSE_FORP_TOLERANCE_PERCENT = 5.0
    if not (RFPULSE_REFP_LIMIT_WATTS > 0):
        print(f"[Config] RFPULSE_REFP_LIMIT_WATTS {RFPULSE_REFP_LIMIT_WATTS} → 20.0 (0 이하 불가)")
        RFPULSE_REFP_LIMIT_WATTS = 20.0
    for _n in ("RFPULSE_FORP_CONSECUTIVE_LIMIT", "RFPULSE_REFP_CONSECUTIVE_LIMIT"):
        _v = globals()[_n]
        try:
            _iv = int(_v)
        except Exception:
            _iv = 0
        if _iv < 1:
            print(f"[Config] {_n} {_v} → 3 (1 이상의 정수여야 한다)")
            _iv = 3
        globals()[_n] = _iv
    if not (RFPULSE_MAX_POWER > 0):
        print(f"[Config] RFPULSE_MAX_POWER {RFPULSE_MAX_POWER} → 600.0 (0 이하 불가)")
        RFPULSE_MAX_POWER = 600.0


_validate_rfpulse_config()

# ================================================================
# MFC 설정
# ================================================================
# config_user.json에서 변경 가능
FLOW_ERROR_TOLERANCE  = get('FLOW_ERROR_TOLERANCE',  0.10)  # 유량 이탈 허용 비율(10%)
FLOW_ERROR_MAX_COUNT  = get('FLOW_ERROR_MAX_COUNT',  5)     # 이탈 연속 횟수 → 채팅 알림
MFC_PRESSURE_WARN_RATIO = get('MFC_PRESSURE_WARN_RATIO', 0.10)  # 압력 이탈 허용 비율(10%)
MFC_PRESSURE_WARN_COUNT = get('MFC_PRESSURE_WARN_COUNT', 5)     # 이탈 연속 횟수 → 채팅 알림

# 고정값 (타이밍/간격)
MFC_POLLING_INTERVAL_MS       = 2000   # polling 주기(ms)
MFC_STABILIZATION_INTERVAL_MS = 1000   # 안정화 확인 주기(ms)
MFC_WATCHDOG_INTERVAL_MS      = 1500   # 포트 감시 주기(ms)
MFC_RECONNECT_BACKOFF_START_MS = 500   # 재연결 첫 대기(ms)
MFC_RECONNECT_BACKOFF_MAX_MS   = 8000  # 재연결 최대 대기(ms)
MFC_TIMEOUT      = 1000   # 명령 timeout(ms)
MFC_GAP_MS       = 1000   # 인터커맨드 간격(ms)
MFC_DELAY_MS     = 1000   # 검증/재시도 지연(ms)
MFC_DELAY_MS_VALVE = 5000 # 밸브 대기(ms)

MFC_SCALE_FACTORS = {
    1: 1.0,  # Channel 1 (Ar)
    2: 1.0,  # Channel 2 (O2)
}

MFC_PRESSURE_SCALE    = 0.1   # UI ↔ HW 스케일 (UI 2.00 ↔ HW 0.20)
MFC_PRESSURE_DECIMALS = 2     # UI 표시 소수 자리
MFC_SP1_VERIFY_TOL    = 0.1   # SP1_SET 검증 허용 오차(장비 단위)

# MFC 명령어 (고정 — 장비 프로토콜)
MFC_COMMANDS = {
    'SET_ONOFF_MASK': lambda bits: f"L0{bits}",
    'FLOW_ON':        lambda channel: f"L{int(channel)} 1",
    'FLOW_OFF':       lambda channel: f"L{int(channel)} 0",
    'MFC_ZEROING':    lambda channel: f"L{4+channel} 1",
    'FLOW_SET':       lambda channel, value: f"Q{channel} {value}",
    'READ_FLOW_ALL':  "R60",
    'READ_FLOW':      lambda channel: f"R6{int(channel)}",
    'READ_MFC_ON_OFF_STATUS': "R69",
    'READ_PRESSURE':  "R5",
    'READ_SP1_VALUE': "R1",
    'READ_VALVE_POSITION': "R6",
    'READ_SYSTEM_STATUS':  "R7",
    'READ_FLOW_SET':  lambda channel: f"R6{4+int(channel)}",
    'VALVE_OPEN':     "O",
    'VALVE_CLOSE':    "C",
    'PS_ZEROING':     "Z1",
    'SP1_ON':  "D1",
    'SP2_ON':  "D2",
    'SP3_ON':  "D3",
    'SP4_ON':  "D4",
    'SP1_SET': lambda value: f"S1 {value}",
    'SP2_SET': lambda value: f"S2 {value}",
    'SP3_SET': lambda value: f"S3 {value}",
    'SP4_SET': lambda value: f"S4 {value}",
}

# === Chamber-K NAS CSV 로그 경로 (고정) ===
CHK_CSV_PATH = r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database\ChK_log.csv"

CHK_CSV_COLUMNS = [
    "Timestamp",
    "Process Name",
    "Main Shutter",
    "Shutter Delay",
    "G1 Target",
    "G2 Target",
    "Ar flow",
    "O2 flow",
    "Working Pressure",
    "Process Time",
    "Heater Temp",
    "RF: For.P",
    "RF: Ref. P",
    "DC: V",
    "DC: I",
    "DC: P",
    # ↓ RF Pulse 전용 컬럼. 반드시 맨 끝에 붙일 것 — 중간에 끼우면 기존
    #   ChK_log.csv 의 열 순서와 어긋난다(옛 파일은 tools/migrate_chk_csv.py 로 옮긴다).
    #   위 "RF: For.P"/"RF: Ref. P" 는 PLC DAC RF power 전용이다. 두 장비를 나란히
    #   비교할 수 있게 펄스는 자기 컬럼을 따로 갖는다.
    "RF Pulse: Freq[kHz]",   # 설정값(평균 아님). 빈 칸이면 장비 리드백값
    "RF Pulse: Duty[%]",     # 설정값(평균 아님). 빈 칸이면 장비 리드백값
    "RF Pulse: For.P",       # 계측 평균
    "RF Pulse: Ref. P",      # 계측 평균
]