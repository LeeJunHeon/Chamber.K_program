# lib/dc_control.py — DC 파워 제어 계산식 (Qt 의존 없음, 단독 테스트 가능)
"""DC 파워 서플라이(정전류 모드)로 목표 파워를 맞추기 위한 전류 스텝 계산.

왜 이 파일이 따로 있나
    device/DCpower.py 는 QSerialPort/QTimer 에 묶여 있어 장비 없이는 실행할 수 없다.
    제어식만 순수 함수로 떼어 두면 실제 로그 데이터를 넣어 재현·검증할 수 있고,
    DCpower.py 는 이 함수를 호출만 하므로 검증한 식이 그대로 실기에서 돈다.

제어식
    서플라이는 정전류(CC) 모드라 전류 I 를 우리가 정하고 전압 V 는 플라즈마가 정한다.
    P = V·I 이므로, 지금 전압에서 목표 파워를 내려면 필요한 전류 변화량은

        ΔI = (P_target − P_now) / V_now

    압력이 바뀌어 V 가 20~30% 뛰면 ΔI 도 같은 비율로 즉시 나오므로, 목표 파워나
    압력 조건이 달라져도 파라미터를 다시 맞출 필요가 없다. 안전을 위해 1초당 전류
    변화폭의 상한(상승/하강 각각)만 둔다.
"""
from __future__ import annotations


def power_step_current(diff_w: float, now_v: float, gain: float,
                       up_max: float, down_max: float) -> float:
    """목표 파워와의 차이를 지금 전압에서 필요한 전류 변화량(A)으로 바꾼다.

    Args:
        diff_w:   목표 − 현재 파워 (W). 양수면 파워가 부족, 음수면 과다.
        now_v:    현재 측정 전압 (V).
        gain:     ΔI 에 곱하는 계수. 1.0 이면 한 번에 맞추고, 1 미만이면 나눠서 접근.
        up_max:   1초당 전류 상승 상한 (A). 램프업과 같은 속도로 두는 것이 자연스럽다.
        down_max: 1초당 전류 하강 상한 (A). 과다 파워는 빨리 걷어내야 하므로 크게 둔다.

    Returns:
        이번 스텝의 전류 변화량 (A). 상한으로 클램프되어 있다.
    """
    up_max = abs(float(up_max))
    down_max = abs(float(down_max))
    if now_v <= 1.0:
        # 플라즈마가 꺼져 있거나 측정 이상. 전압으로 나눌 수 없으니 최소 상승 스텝만 준다.
        return min(up_max, 0.005) if diff_w > 0 else 0.0
    di = float(gain) * float(diff_w) / float(now_v)
    return max(-down_max, min(up_max, di))


def is_at_current_cap(iset: float, i_max: float, eps: float = 1e-6) -> bool:
    """설정 전류가 서플라이 상한에 붙어 있는지 (더 올릴 수 없는지) 판정한다."""
    return float(iset) >= float(i_max) - eps
