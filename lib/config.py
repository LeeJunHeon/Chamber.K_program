# config.py
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
#   VSCD-30 입력 사양: 0.8~4V  (0.8V = 출력 0%, 4V = 100%)
#
#   실측: DAC 카운트 → 전류 A ≈ 0.1 × (카운트 − 412), 예열 후 약 8% 증가
#         400 ≈ 출력 0% / 600 ≈ 20A / 640 ≈ 24.6A(절대 상한)
#
#   ※ HEATER_MV_MIN 은 PLC K1227(MV 최소값)과 반드시 같아야 한다.
#      다르면 화면 출력%가 틀어진다.
#   ※ 운전 상한 HEATER_MV_LIMIT 은 접속 시 D00018로 전송되며,
#      래더가 이를 PID 최대 조작값으로 넘긴다. 코드는 절대
#      HEATER_MV_ABS_MAX 위로는 쓰지 않는다.
HEATER_MV_MIN     = get('HEATER_MV_MIN',     400)   # PLC K1227과 일치. 출력 0% 지점
HEATER_MV_ABS_MAX = get('HEATER_MV_ABS_MAX', 640)   # 절대 안전 상한
HEATER_MV_LIMIT   = get('HEATER_MV_LIMIT',   600)   # D00018에 쓸 실제 운전 상한

# 하위 호환: 예전 이름으로 import 하는 코드가 있을 수 있다
HEATER_MV_MAX = HEATER_MV_ABS_MAX

# --- config_user.json에서 변경 가능 ---
HEATER_ENABLED          = get('HEATER_ENABLED',          True)
HEATER_TEMP_SCALE       = get('HEATER_TEMP_SCALE',       0.1)   # ★ 실측 후 확정
HEATER_WD_PERIOD_MS     = get('HEATER_WD_PERIOD_MS',     3000)  # PLC 10초의 1/3
HEATER_MAX_TEMP         = get('HEATER_MAX_TEMP',         500.0) # UI 입력 상한
HEATER_SOAK_TOLERANCE   = get('HEATER_SOAK_TOLERANCE',   3.0)   # °C
HEATER_SOAK_TIME_SEC    = get('HEATER_SOAK_TIME_SEC',    60)    # 도달 유지 시간
HEATER_WAIT_TIMEOUT_SEC = get('HEATER_WAIT_TIMEOUT_SEC', 3600)  # 승온 대기 최대

# --- PLC로 밀어 넣는 한계값 (사람 단위. PLC raw 변환은 PLC.py가 한다) ---
HEATER_SV_LIMIT_C          = get('HEATER_SV_LIMIT_C',          500.0)  # D00013 [°C]
HEATER_RAMP_RATE_C_PER_MIN = get('HEATER_RAMP_RATE_C_PER_MIN', 12.0)   # D00020 [°C/min]
HEATER_HOLDBACK_C          = get('HEATER_HOLDBACK_C',          2.0)    # D00021 [°C]
HEATER_OT_LIMIT_C          = get('HEATER_OT_LIMIT_C',          550.0)  # D00026 [°C]
HEATER_SLOW_ZONE_C         = get('HEATER_SLOW_ZONE_C',         10.0)   # D00027 [°C]
HEATER_SLOW_RATE_C_PER_MIN = get('HEATER_SLOW_RATE_C_PER_MIN', 6.0)    # D00028 [°C/min]
HEATER_PUSH_CONFIG         = get('HEATER_PUSH_CONFIG',         True)   # False면 PLC 쓰기 생략


def _validate_heater_config() -> None:
    """JSON 값이 위험하거나 앞뒤가 안 맞으면 안전한 쪽으로 클램프한다.
    예외는 던지지 않는다 — 설정이 틀려도 프로그램은 떠야 한다."""
    global HEATER_MV_LIMIT, HEATER_MV_MAX, HEATER_OT_LIMIT_C, HEATER_MAX_TEMP
    global HEATER_RAMP_RATE_C_PER_MIN, HEATER_SLOW_RATE_C_PER_MIN

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
]