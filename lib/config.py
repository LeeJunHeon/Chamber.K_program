# config.py
"""
장비 시리얼 통신, 공정 파라미터, 기본 명령어 등 전역 설정 파일
이 파일에서 시리얼 포트, 바우드레이트, MFC 번호 등 관리!
"""

# 시리얼 포트 설정 (포트 이름은 실제 환경에 맞게 수정)
MFC_PORT = "COM4"
DC_PORT = "COM8"
FADUINO_PORT = "COM3"

FADUINO_BAUD = 9600
MFC_BAUDRATE = 9600
DC_BAUDRATE = 9600

# 인터락 기준값/공정 파라미터 등
INTERLOCK_CHECK_INTERVAL = 0.2  # sec

# --- 출력 센서 (Digital Out) ---
# FADUINO의 포트 인덱스
FADUINO_PORT_INDEX = {
    'Rotary': 0, 
    'RV': 1, 
    'FV': 2, 
    'MV': 3, 
    'Vent': 4, 
    'Turbo': 5,
    'Door_Up': 6, 
    'Door_Down': 7, 
    'Ar': 8, 
    'O2': 9, 
    'Buzz': 10,
    'Shutter': 11, 
    'Shutter1': 12, 
    'Shutter2': 13
}

# --- 입력 센서 (Digital In) ---
# FADUINO로부터 받는 센서 데이터(blocK0)의 각 비트가 무엇을 의미하는지 정의합니다.
FADUINO_SENSOR_MAP = {
    'Air': 0,       # 압축 공기 공급 센서
    'G1': 1,        # 진공 게이지 1
    'G2': 2,        # 진공 게이지 2
    'ATM': 3,       # 대기압 센서
    'Water': 4      # 냉각수 공급 센서
}

# UI 버튼 객체와 해당하는 포트 이름을 매핑
BUTTON_TO_PORT_MAP = {
    'Rotary_button': 'Rotary',
    'RV_button': 'RV',
    'FV_button': 'FV',
    'MV_button': 'MV',
    'Vent_button': 'Vent',
    'Turbo_button': 'Turbo',
    'Ar_Button': 'Ar',
    'O2_Button': 'O2',
    'BuzzStop_Button': 'Buzz',
    'MS_button': 'Shutter',
    'S1_button': 'Shutter1',
    'S2_button': 'Shutter2',
}

# --- MFC 명령어 (MFC Command) ---
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

# --- MFC ---
FLOW_ERROR_TOLERANCE = 0.05  # 5% 오차 허용
FLOW_ERROR_MAX_COUNT = 3     # 3회 연속 불일치 시 경고

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
RF_REFLECTED_SCALING_MAX_WATT = 950.3 # ref.p 센서 교정 상수
RF_MAX_POWER = 600.0       # RF Power 장비 최대값 (W)
RF_RAMP_STEP = 1           # 램프업,다운 스텝 (W)
RF_RAMP_DELAY = 1          # 램프업 딜레이 (초)
RF_MAX_ERROR_COUNT = 5     # 연속 실패 허용 횟수
RF_TOLERANCE_POWER = 1.0   # 목표 Power 허용 오차