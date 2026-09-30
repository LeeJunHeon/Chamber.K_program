# core/params.py
# -*- coding: utf-8 -*-
"""공정 파라미터 만들기·검사 — 화면 없이 도는 순수 함수.

main.py 는 입력칸을 읽어(ManualInputs / CSV 행 + 화면 offset·param) 여기로 넘기기만 한다.
이 모듈은 PyQt6·UI·main·controller·device·reporter·lib.logger 를 import 하지 않는다(lib.config 의 상수·함수만).
로그를 직접 남기지 않는다 — 알릴 것은 ValueError 나 warn 콜백으로만.

동작은 옮기기 전 main.py(_handle_start_process 의 try 블록, _build_params_from_csv_row,
_check_rfpulse_pulse_range)와 같다: 검사 순서, 오류 문구, dict 키 순서·값·타입까지.
"""
from dataclasses import dataclass
from typing import Callable, Optional

from lib.config import (HEATER_ENABLED, HEATER_MAX_TEMP, HEATER_RAMP_RATE_C_PER_MIN,
                        RFPULSE_MAX_POWER, RFPULSE_PULSE_FREQ_MAX_HZ, RFPULSE_PULSE_FREQ_MIN_HZ,
                        RFPULSE_DUTY_MIN, RFPULSE_DUTY_MAX, rfpulse_pulse_edge_violation)


@dataclass(frozen=True)
class ManualInputs:
    """수동 입력칸 원시값 — toPlainText()/isChecked() 결과 그대로(strip 도 하지 않는다)."""
    use_ar: bool
    ar_flow_text: str
    use_o2: bool
    o2_flow_text: str
    working_pressure_text: str
    use_dc: bool
    dc_power_text: str
    use_rf: bool
    rf_power_text: str
    offset_text: str
    param_text: str
    use_rf_pulse: bool
    rfp_power_text: str
    rfp_freq_text: str
    rfp_duty_text: str
    shutter_delay_text: str
    process_time_text: str
    use_g1: bool
    g1_name_text: str
    use_g2: bool
    g2_name_text: str
    use_dc_delay: bool


def check_rfpulse_range(freq_khz, duty, where: str) -> None:
    """펄스 주파수/듀티가 장비 범위 안인지 시작 전에 확인한다. None 은 통과(장비 현재값 유지).

    CESAR 1310 매뉴얼: 펄스 주파수 1 Hz~30 kHz, 듀티 1~99 %, 그리고 최소 ON/OFF 시간 16 µs(명령 96).
    16 µs 규칙은 둘 다 주어졌을 때만 여기서 잰다(rfpulse_pulse_edge_violation). 한쪽이 None(장비값 유지)이면
    START 뒤 리드백에서 같은 함수로 경고한다 — 장비는 위반 설정도 CSR 0 으로 받아들이므로(2026-09-17 20 kHz·80 %)
    CSR 51 에 기대지 않는다.
    """
    if freq_khz is not None:
        _hz = float(freq_khz) * 1000.0
        _lo = float(RFPULSE_PULSE_FREQ_MIN_HZ)
        _hi = float(RFPULSE_PULSE_FREQ_MAX_HZ)
        if not (_lo <= _hz <= _hi):
            raise ValueError(
                f"[{where}] RF Pulse 주파수 {float(freq_khz):g}kHz 가 장비 범위 "
                f"{_lo / 1000.0:g}~{_hi / 1000.0:g}kHz(CESAR 1310, RFPULSE_PULSE_FREQ_MAX_HZ)를 벗어납니다.")
    if duty is not None:
        _d = int(duty)
        if not (int(RFPULSE_DUTY_MIN) <= _d <= int(RFPULSE_DUTY_MAX)):
            raise ValueError(
                f"[{where}] RF Pulse 듀티 {_d}% 가 범위 "
                f"{int(RFPULSE_DUTY_MIN)}~{int(RFPULSE_DUTY_MAX)}% 를 벗어납니다.")
    if freq_khz is not None and duty is not None:
        _why = rfpulse_pulse_edge_violation(int(round(float(freq_khz) * 1000.0)), int(duty))
        if _why:
            raise ValueError(f"[{where}] RF Pulse 설정 {_why} (CESAR 1310 최소 ON/OFF 시간, RFPULSE_PULSE_MIN_EDGE_US).")


def build_manual_params(inp: ManualInputs) -> dict:
    """수동 Start 의 공정 파라미터. 잘못된 입력은 ValueError(float 변환 오류 문구 포함) 그대로."""
    # --- 가스 선택: Ar / O2 를 각각 체크박스로 처리 ---
    use_ar = inp.use_ar
    use_o2 = inp.use_o2

    if not (use_ar or use_o2):
        raise ValueError("Ar 또는 O2 가스를 하나 이상 선택해야 합니다.")

    ar_flow = 0.0
    o2_flow = 0.0

    if use_ar:
        ar_text = inp.ar_flow_text.strip()
        if not ar_text:
            raise ValueError("Ar 가스 유량을 입력해야 합니다.")
        ar_flow = float(ar_text)

    if use_o2:
        o2_text = inp.o2_flow_text.strip()
        if not o2_text:
            raise ValueError("O2 가스 유량을 입력해야 합니다.")
        o2_flow = float(o2_text)

    # 기존 RF offset/param 체크 로직 그대로 유지
    offset_text = inp.offset_text.strip()
    param_text = inp.param_text.strip()

    if inp.use_rf:
        if not offset_text:
            raise ValueError("RF 파워의 Offset 값을 입력해야 합니다.")
        if not param_text:
            raise ValueError("RF 파워의 Param 값을 입력해야 합니다.")

    # --- RF Pulse: 전용 칸에서 읽는다. RF power 와 독립이다 ---
    #   파워는 필수, freq/duty 는 빈 칸 허용(= 장비 현재값 유지)
    use_rf_pulse = inp.use_rf_pulse
    rf_pulse_power = 0.0
    rf_pulse_freq = None
    rf_pulse_duty = None
    if use_rf_pulse:
        _rp = inp.rfp_power_text.strip()
        if not _rp or float(_rp) <= 0:
            # 흔한 실수: RF power 칸(RF_power_edit)에 펄스 파워를 넣는다.
            #  그 칸은 아날로그 RF 전용이라 펄스 파워는 비어 있다.
            _rf_big = inp.rf_power_text.strip()
            if (not _rp) and _rf_big and not inp.use_rf:
                raise ValueError(
                    "RF Pulse 파워는 RF Pulse 체크박스 바로 아래 칸에 입력하세요. "
                    "왼쪽 칸은 RF power(아날로그) 전용입니다.")
            raise ValueError("RF Pulse 파워를 입력해야 합니다.")
        rf_pulse_power = float(_rp)
        _fq = inp.rfp_freq_text.strip()
        _dt = inp.rfp_duty_text.strip()
        if _fq:
            rf_pulse_freq = float(_fq)          # kHz
        if _dt:
            rf_pulse_duty = int(float(_dt))     # %
        check_rfpulse_range(rf_pulse_freq, rf_pulse_duty, "수동 시작")

    # --- selected_gas / mfc_flow는 기존 코드 호환용으로 유지 ---
    if use_ar and not use_o2:
        selected_gas = "Ar"
        mfc_flow = ar_flow
    elif use_o2 and not use_ar:
        selected_gas = "O2"
        mfc_flow = o2_flow
    else:
        # 둘 다 쓰는 경우: 기본은 Ar 기준
        selected_gas = "Ar"
        mfc_flow = ar_flow

    # --- G1/G2 타겟 이름 ---
    g1_target_name = inp.g1_name_text.strip()
    g2_target_name = inp.g2_name_text.strip()

    # 값 계산(float 변환)은 아래 키 순서대로 일어난다 — 여러 칸이 잘못됐을 때 먼저 나는 오류가 이 순서로 정해진다.
    params = {
        # ▼ 새 다중 가스 파라미터
        "use_ar_gas": use_ar,
        "use_o2_gas": use_o2,
        "ar_flow": ar_flow,
        "o2_flow": o2_flow,

        # ▼ 기존 단일 가스 방식(백워드 호환용)
        "selected_gas": selected_gas,
        "mfc_flow": float(mfc_flow),

        # ▼ 나머지 기존 파라미터들 그대로 유지
        "sp1_set": float(inp.working_pressure_text.strip()),
        "dc_power": float(inp.dc_power_text.strip() or 0) if inp.use_dc else 0,
        "rf_power": float(inp.rf_power_text.strip() or 0) if inp.use_rf else 0,
        "shutter_delay": float(inp.shutter_delay_text.strip()),
        "process_time": float(inp.process_time_text.strip()),
        "rf_offset": float(offset_text or 6.79),
        "rf_param": float(param_text or 1.0395),

        # ▼ RF Pulse (CESAR). freq 는 kHz, duty 는 %. None = 장비 현재값 유지
        "use_rf_pulse": bool(use_rf_pulse),
        "rf_pulse_power": float(rf_pulse_power),
        "rf_pulse_freq": rf_pulse_freq,
        "rf_pulse_duty": rf_pulse_duty,

        # ▼ G1/G2 사용 여부
        "use_g1": inp.use_g1,
        "use_g2": inp.use_g2,

        # ▼ DC Power 안정화 대기 사용 여부 (기본 OFF)
        "use_dc_delay": inp.use_dc_delay,

        # ▼ 히터: 수동 공정은 히터를 소유하지 않는다(2026-09-17). 히터 패널의 ON/OFF·목표·유지 모드는
        #    히터 패널 몫이고, 공정은 램프/설정/대기 스텝을 만들지 않으며 종료·중단·STOP 때 히터를 끄지 않는다.
        #    (600°C ON 상태에서 공정을 시작하면 공정이 히터를 잡아 목표 재설정·110분 대기·종료 시 OFF 까지 했다)
        #    히터를 공정이 소유하는 것은 CSV 행의 use_heater/heater_temp 뿐이다.
        "use_heater": False,
        "heater_temp": 0.0,

        "g1_target_name": g1_target_name,
        "g2_target_name": g2_target_name,
    }

    if params['rf_pulse_power'] > float(RFPULSE_MAX_POWER):
        raise ValueError(
            f"RF Pulse 파워 {params['rf_pulse_power']:g}W 가 장비 상한 "
            f"{float(RFPULSE_MAX_POWER):g}W(RFPULSE_MAX_POWER)를 넘습니다.")

    if not (params['dc_power'] > 0 or params['rf_power'] > 0
            or params['rf_pulse_power'] > 0):
        raise ValueError("RF / RF Pulse / DC 파워 중 하나 이상을 입력해야 합니다.")

    # Shutter Delay / Process Time 정책:
    #   - 둘 다 0 이상
    #   - 둘 중 하나는 반드시 > 0
    #   - process_time == 0 이면 Main Shutter는 열지 않고 shutter delay만 진행 후 종료
    if params['shutter_delay'] < 0:
        raise ValueError("Shutter Delay는 0 이상이어야 합니다.")
    if params['process_time'] < 0:
        raise ValueError("Process Time은 0 이상이어야 합니다.")
    if params['shutter_delay'] <= 0 and params['process_time'] <= 0:
        raise ValueError("Shutter Delay와 Process Time 중 하나는 0보다 커야 합니다.")

    return params


def build_csv_params(row: dict, offset_text: str, param_text: str,
                     warn: Optional[Callable[[str, str], None]] = None) -> dict:
    """
    CSV 한 행의 공정 파라미터.
    - CSV 값이 비어있을 때(UI 값으로) 폴백하지 않음
    - 필요한 값이 비어있거나 형식이 잘못되면 ValueError로 즉시 중단
    - use_* 가 False(또는 비어있음)인 기능은 안전하게 OFF(0) 처리
    RF 보정값(offset/param)은 CSV 에 없어 화면 칸 값을 받는다. warn(level, msg) 은 보정 경고용(없으면 버린다).
    """

    def _s(key: str):
        v = (row or {}).get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s != "" else None

    def _b(key: str, default: bool = False) -> bool:
        s = _s(key)
        if s is None:
            return default
        return s.lower() in ("1", "y", "yes", "true", "t", "on")

    def _f(key: str, *, required: bool = False, default: float | None = None) -> float | None:
        s = _s(key)
        if s is None:
            if required:
                raise ValueError(f"CSV '{key}' 값이 비어 있습니다.")
            return default
        try:
            return float(s)
        except Exception:
            raise ValueError(f"CSV '{key}' 값이 숫자가 아닙니다: {s!r}")

    process_name = (_s("Process_name") or "").strip()

    # ---- 가스(필수) ----
    use_ar_gas = _b("Ar", False)
    use_o2_gas = _b("O2", False)
    if not (use_ar_gas or use_o2_gas):
        raise ValueError(f"[{process_name or 'STEP'}] Ar/O2 중 하나 이상을 1(ON)로 지정해야 합니다.")

    ar_flow = 0.0
    o2_flow = 0.0
    if use_ar_gas:
        ar_flow = _f("Ar_flow", required=True)  # type: ignore
        if ar_flow is None or ar_flow <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] Ar=ON 인데 Ar_flow가 비어있거나 0 이하입니다.")
    if use_o2_gas:
        o2_flow = _f("O2_flow", required=True)  # type: ignore
        if o2_flow is None or o2_flow <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] O2=ON 인데 O2_flow가 비어있거나 0 이하입니다.")

    # ---- 압력/시간(필수) ----
    work_p = _f("working_pressure", required=True)  # type: ignore
    proc_time = _f("process_time", required=True)   # type: ignore
    sh_delay = _f("shutter_delay", required=False, default=0.0)

    if work_p is None or work_p <= 0:
        raise ValueError(f"[{process_name or 'STEP'}] working_pressure가 비어있거나 0 이하입니다.")

    if proc_time is None:
        proc_time = 0.0
    if sh_delay is None:
        sh_delay = 0.0

    if proc_time < 0:
        raise ValueError(f"[{process_name or 'STEP'}] process_time은 0 이상이어야 합니다.")
    if sh_delay < 0:
        raise ValueError(f"[{process_name or 'STEP'}] shutter_delay는 0 이상이어야 합니다.")
    if proc_time <= 0 and sh_delay <= 0:
        raise ValueError(
            f"[{process_name or 'STEP'}] shutter_delay와 process_time 중 "
            f"하나는 0보다 커야 합니다."
        )

    # ---- 파워(선택) : use_*가 비어있으면 OFF로 간주 ----
    use_dc = _b("use_dc_power", False)
    use_rf = _b("use_rf_power", False)
    # ---- RF Pulse(선택) : 컬럼이 아예 없는 기존 레시피는 자동 OFF (하위 호환) ----
    #  RF power / RF Pulse / DC power 는 서로 독립이다 — 동시에 켤 수 있다.
    use_rf_pulse = _b("use_rf_pulse", False)

    # ---- 히터(선택) : 컬럼이 없으면 자동 OFF → 기존 CSV 하위 호환 ----
    use_heater = _b("use_heater", False)
    heater_temp = _f("heater_temp", required=False, default=0.0) or 0.0
    if use_heater and heater_temp <= 0:
        raise ValueError(f"[{process_name or 'STEP'}] use_heater=ON 인데 heater_temp가 0 이하입니다.")
    if use_heater and heater_temp > HEATER_MAX_TEMP:
        raise ValueError(f"[{process_name or 'STEP'}] heater_temp가 상한({HEATER_MAX_TEMP:.0f}°C)을 넘습니다.")

    # RAMP 속도(°C/min). 컬럼이 없거나 0이면 config 기본값을 쓴다.
    heater_ramp = _f("heater_ramp", required=False, default=0.0) or 0.0
    if use_heater:
        if heater_ramp <= 0:
            heater_ramp = float(HEATER_RAMP_RATE_C_PER_MIN)
        elif heater_ramp < 6.0:
            # PLC 램프는 1카운트/초 = 6°C/min 단위라 그 아래는 표현되지 않는다
            if warn is not None:
                warn("히터(경고)",
                     f"[{process_name or 'STEP'}] heater_ramp {heater_ramp:g} → 6 °C/min 으로 보정")
            heater_ramp = 6.0
        elif heater_ramp > 60.0:
            raise ValueError(
                f"[{process_name or 'STEP'}] heater_ramp가 상한(60°C/min)을 넘습니다.")

    # DC Power 안정화 대기(선택) : 컬럼이 없거나 비어 있으면 OFF
    use_dc_delay = _b("use_dc_delay", False)

    dc_power = 0.0
    rf_power = 0.0

    if use_dc:
        dc_power_val = _f("dc_power", required=True)  # type: ignore
        if dc_power_val is None or dc_power_val <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] use_dc_power=ON 인데 dc_power가 비어있거나 0 이하입니다.")
        dc_power = float(dc_power_val)

    if use_rf:
        rf_power_val = _f("rf_power", required=True)  # type: ignore
        if rf_power_val is None or rf_power_val <= 0:
            raise ValueError(f"[{process_name or 'STEP'}] use_rf_power=ON 인데 rf_power가 비어있거나 0 이하입니다.")
        rf_power = float(rf_power_val)

    # ---- Gun(선택) ----
    use_g1 = _b("gun1", False)
    use_g2 = _b("gun2", False)

    g1_target_name = (_s("G1 Target") or "").strip()
    g2_target_name = (_s("G2 Target") or "").strip()

    # selected_gas / mfc_flow (백워드 호환용)
    if use_ar_gas and not use_o2_gas:
        selected_gas = "Ar"
        mfc_flow = ar_flow
    elif use_o2_gas and not use_ar_gas:
        selected_gas = "O2"
        mfc_flow = o2_flow
    else:
        selected_gas = "Ar"
        mfc_flow = ar_flow

    # RF 보정값은 CSV에 없으므로 UI 값을 그대로 사용(단, RF를 쓰는 경우 값이 비면 오류)
    offset_text = (offset_text or "").strip()
    param_text = (param_text or "").strip()

    def _float_ui(text, default):
        try:
            return float(str(text).strip())
        except Exception:
            return default

    if use_rf and (not offset_text or not param_text):
        raise ValueError(f"[{process_name or 'STEP'}] RF 사용인데 Offset/Param 값이 UI에 비어있습니다.")

    rf_offset = _float_ui(offset_text or 6.79, 6.79)
    rf_param = _float_ui(param_text or 1.0395, 1.0395)

    # RF Pulse 값 — 파워는 사용 시 필수, freq/duty 는 비어 있으면 None(장비 현재값 유지)
    rf_pulse_power = 0.0
    rf_pulse_freq = None
    rf_pulse_duty = None
    if use_rf_pulse:
        _v = _f("rf_pulse_power", required=False, default=0.0) or 0.0
        if _v <= 0:
            raise ValueError(
                f"[{process_name or 'STEP'}] use_rf_pulse=ON 인데 "
                f"rf_pulse_power가 비어있거나 0 이하입니다.")
        if _v > float(RFPULSE_MAX_POWER):
            raise ValueError(
                f"[{process_name or 'STEP'}] rf_pulse_power {float(_v):g}W 가 "
                f"장비 상한 {float(RFPULSE_MAX_POWER):g}W(RFPULSE_MAX_POWER)를 넘습니다.")
        rf_pulse_power = float(_v)
        _fq = _f("rf_pulse_freq", required=False, default=None)
        _dt = _f("rf_pulse_duty", required=False, default=None)
        rf_pulse_freq = float(_fq) if _fq not in (None, "") else None
        rf_pulse_duty = int(float(_dt)) if _dt not in (None, "") else None
        check_rfpulse_range(rf_pulse_freq, rf_pulse_duty, process_name or "STEP")

    return {
        "use_ar_gas": use_ar_gas,
        "use_o2_gas": use_o2_gas,
        "ar_flow": float(ar_flow),
        "o2_flow": float(o2_flow),

        "selected_gas": selected_gas,
        "mfc_flow": float(mfc_flow),

        "sp1_set": float(work_p),

        "dc_power": float(dc_power),
        "rf_power": float(rf_power),
        "shutter_delay": float(sh_delay),
        "process_time": float(proc_time),

        "rf_offset": float(rf_offset),
        "rf_param": float(rf_param),

        # ▼ RF Pulse — 컬럼이 없으면 전부 미사용으로 떨어진다
        "use_rf_pulse": bool(use_rf_pulse),
        "rf_pulse_power": float(rf_pulse_power),
        "rf_pulse_freq": rf_pulse_freq,
        "rf_pulse_duty": rf_pulse_duty,

        "use_g1": bool(use_g1),
        "use_g2": bool(use_g2),

        "use_dc_delay": bool(use_dc_delay and use_dc),
        "use_heater":   bool(use_heater and HEATER_ENABLED),   # ★
        "heater_temp":  float(heater_temp),                    # ★
        "heater_ramp":  float(heater_ramp),                    # ★ °C/min

        "g1_target_name": g1_target_name,
        "g2_target_name": g2_target_name,

        "process_name": process_name,
    }
