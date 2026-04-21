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
}

# 센서 DI (고정)
PLC_SENSOR_BITS: Dict[str, int] = {
    "Air":   160,  # M00100
    "G1":    161,  # M00101
    "G2":    162,  # M00102
    "ATM":   163,  # M00103
    "Water": 164,  # M00104
}

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
    "RF: For.P",
    "RF: Ref. P",
    "DC: V",
    "DC: I",
    "DC: P",
]