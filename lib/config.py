# config.py
from typing import Dict, List, Tuple

"""
장비 시리얼 통신, 공정 파라미터, 기본 명령어 등 전역 설정 파일
이 파일에서 시리얼 포트, 바우드레이트, MFC 번호 등 관리!
"""

# 시리얼 포트 설정 (포트 이름은 실제 환경에 맞게 수정)
MFC_PORT = "COM4"
MFC_BAUD = 9600

DC_PORT = "COM8"
DC_BAUDRATE = 9600

PLC_PORT = "COM3"
PLC_BAUD = 115200
PLC_SLAVE_ID = 1
PLC_TIMEOUT = 0.5   # 초
RF_ADC_MAX_COUNT = 4000  # 주소맵 사양에 맞춤(ADC/DAC 모두 0~4000 카운트)

# 인터락 기준값/공정 파라미터 등
INTERLOCK_CHECK_INTERVAL = 0.2  # sec

# === RF 피드백(ADC) ===
# 표: ADC Ch0 data = D00000, ADC Ch1 data = D00001
RF_ADC_FORWARD_ADDR = 0   # D00000 -> Holding Register 0
RF_ADC_REFLECT_ADDR = 1   # D00001 -> Holding Register 1
RF_ADC_MAX_COUNT    = 4000   # 모듈 사양(기존 값 유지)

# === RF DAC (U02/D0004x) ===
# 표: DAC Ch0 data = D00040, outen = U02.02.0
RF_DAC_ADDR_CH0     = 40   # D00040 -> Holding Register 64 (FC=6/3)
COIL_ENABLE_DAC_CH0 = 320  # U02.02.0 -> Coil 320 (FC=5/1)

# PLC 주소 맵핑
# UI 버튼 이름 -> Coil 주소 (쓰기용, FC 5/1)
# (배선표: M00000~M00020)
PLC_COIL_MAP: Dict[str, int] = {
    "Rotary_button": 0,    # M00000
    "RV_button": 1,        # M00001
    "FV_button": 2,        # M00002
    "MV_button": 3,        # M00003
    "Vent_button": 4,      # M00004
    "Turbo_button": 5,     # M00005
    "Doorup_button": 6,    # M00006 (Door Up)
    "Ar_Button": 7,        # M00007
    "O2_Button": 8,        # M00008
    "MS_button": 9,        # M00009
    "S1_button": 16,       # M00010
    "S2_button": 17,       # M00011
    "Doordn_button": 32,   # M00020 (Door Down)
    "BuzzStop_Button": 33,   # M00021 (버저)
}

# === 센서 DI (입력: FC=2) ===
# 표: Air/G1/G2/ATM/Water = [DI0]~[DI4]
SENSOR_DI_BASE = 100  # DI0가 시작점이므로 0
PLC_SENSOR_BITS: Dict[str, int] = {
    "Air":   0,  # [DI0]
    "G1":    1,  # [DI1]
    "G2":    2,  # [DI2]
    "ATM":   3,  # [DI3]
    "Water": 4,  # [DI4]
}

# --- DCpower ---
DC_INITIAL_VOLTAGE = 450.0 # 초기 전압(V)
DC_INITIAL_CURRENT = 0.1   # 초기 전류(A)
DC_MAX_VOLTAGE = 600.0     # 최대 전압(V)
DC_MAX_CURRENT = 1.0       # 최대 전류(A)
DC_MAX_POWER = 500.0       # 최대 파워
DC_TOLERANCE_WATT = 0.5    # 목표 Power 허용 오차(W)
DC_MAX_ERROR_COUNT = 5     # 연속 실패 허용 횟수

# --- RFpower ---
RF_FORWARD_SCALING_MAX_WATT  = 629.7  # for.p 센서 교정 상수
RF_REFLECTED_SCALING_MAX_WATT = 100.0 # ref.p 센서 교정 상수
RF_DAC_FULL_SCALE = 4000   # PLC DAC 풀스케일(모듈 사양에 맞게)
RF_MAX_POWER = 600.0       # RF Power 장비 최대값 (W)
RF_RAMP_STEP = 1           # 램프업 스텝 (W)
RF_RAMP_DOWN_STEP = 8      # 램프다운 스텝 (W)
RF_RAMP_DELAY = 1          # 램프업 딜레이 (초)
RF_MAX_ERROR_COUNT = 5     # 연속 실패 허용 횟수
RF_TOLERANCE_POWER = 1.0   # 목표 Power 허용 오차

# === MFC ===
FLOW_ERROR_TOLERANCE = 0.05  # 5% 오차 허용
FLOW_ERROR_MAX_COUNT = 3     # 3회 연속 불일치 시 경고

# === MFC 타이밍/간격 상수 ===
# 주기/타이머
MFC_POLLING_INTERVAL_MS       = 2000    # polling 주기
MFC_STABILIZATION_INTERVAL_MS = 1000    # 1초마다 목표 대비 실제값 확인
MFC_WATCHDOG_INTERVAL_MS      = 1500    # 포트가 닫혔는지 주기적으로 점검

# 재연결 백오프
MFC_RECONNECT_BACKOFF_START_MS = 500    # 포트 오류/타임아웃 시 첫 재연결 대기시간
MFC_RECONNECT_BACKOFF_MAX_MS   = 8000   # 지수 백오프 최대 상한

# === 전역 통일 상수 ===
MFC_TIMEOUT   = 1000         # 모든 명령 timeout
MFC_GAP_MS    = 1000         # 모든 인터커맨드 간격(gap)
MFC_DELAY_MS  = 1000         # 모든 검증/재시도 지연
MFC_DELAY_MS_VALVE = 5000    # 밸브 이동/재전송 대기(5초)

# [신규] 채널별 유량 스케일 팩터 정의
MFC_SCALE_FACTORS = {
    1: 1.0,     # Channel 1 (Ar): 1:1 스케일
    2: 1.0,     # Channel 2 (O2): 1:1 스케일
}

# UI ↔ HW 스케일 (SP1/압력 공용)
# - 장비값(HW) → UI:  ui = hw / MFC_PRESSURE_SCALE
# - UI → 장비(HW):   hw = ui * MFC_PRESSURE_SCALE
MFC_PRESSURE_SCALE = 0.1        # 예) UI 2.00 ↔ HW 0.20
MFC_PRESSURE_DECIMALS = 2       # UI 표시에 사용할 소수 자리
MFC_SP1_VERIFY_TOL = 0.1        # SP1_SET 검증 허용 오차(장비 단위)

# 명령어는 ASCII 문자로 전송해야 되며, \r으로 끝나야 함.
MFC_COMMANDS = {
    # --- MFC 쓰기(Write) 명령어 ---
    # 일괄 ON/OFF: L0 뒤에 비트마스크(예: '1010')
    'SET_ONOFF_MASK': lambda bits: f"L0{bits}",

    # (선택) 단일 채널 ON/OFF는 유지해도 되지만 내부 로직에선 쓰지 않음(폴백용)
    'FLOW_ON':  lambda channel: f"L{int(channel)} 1",
    'FLOW_OFF': lambda channel: f"L{int(channel)} 0",

    'MFC_ZEROING': lambda channel: f"L{4+channel} 1",         # 지정된 채널의 MFC를 Zeroing합니다 (Ch1=L5, Ch2=L6 ...). 
    'FLOW_SET': lambda channel, value: f"Q{channel} {value}", # 지정된 채널의 Flow 값을 설정합니다 (% of Full Scale). 

    # === 읽기 ===
    'READ_FLOW_ALL': "R60",                    # 모든 채널 유량
    'READ_FLOW':     lambda channel: f"R6{int(channel)}",  # 폴백/디버깅용
    'READ_MFC_ON_OFF_STATUS': "R69",
    'READ_PRESSURE': "R5",
    'READ_SP1_VALUE': "R1",
    'READ_VALVE_POSITION': "R6",
    'READ_SYSTEM_STATUS': "R7",
    'READ_FLOW_SET': lambda channel: f"R6{4+int(channel)}", # 지정된 채널의 Flow 설정 값을 읽습니다 (Ch1=R65, Ch2=R66 ...). 

    # --- 공통 명령어 (채널 지정 불필요) ---
    'VALVE_OPEN': "O",  # Throttle Valve를 엽니다. 
    'VALVE_CLOSE': "C", # Throttle Valve를 닫습니다. 
    'PS_ZEROING': "Z1", # 압력 센서(게이지)를 Zeroing합니다. 
    'SP4_ON': "D4",     # Set-point 4를 실행합니다. 
    'SP1_ON': "D1",     # Set-point 1을 실행합니다. 
    'SP1_SET': lambda value: f"S1 {value}", # Set-point 1의 목표 압력 값을 설정합니다. 
}
