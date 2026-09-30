# integrations/erp_state.py
# -*- coding: utf-8 -*-
"""ERP 상태 스냅샷 — 1초마다 장비 상태를 dict 로 만들어 ERP 리포터에 보낸다.

옮기기 전에는 MainDialog.__init__ 안의 중첩 함수 _erp_snapshot 이었다. 상태 dict 의 키·순서·값 계산과
값을 읽는 횟수·순서는 그대로다(히터 출력 문구 → 가스 스냅샷 → 레시피 진행도 옮기기 전 순서 그대로).
화면 위젯 글자·히터·PLC 표시 등 읽을 것과 보내는 곳은 모두 src(ErpStateSource)로만 부른다.

이 모듈은 PyQt6·UI·main·lib.logger 를 import 하지 않는다.
"""
import os
from typing import Any, Optional, Protocol


class ErpStateSource(Protocol):
    """스냅샷이 읽는 값과 보내는 곳. main.py 의 _MainErpStateSource 가 구현한다."""

    def text(self, name: str) -> str:
        """이름의 위젯 글자(앞뒤 공백 제거). 위젯이 없거나 읽을 수 없으면 ""."""

    def checked(self, name: str) -> bool:
        """이름의 위젯 체크 상태. 없거나 읽을 수 없으면 False."""

    def process_running(self) -> bool:
        """공정(스텝·CSV 딜레이)이 도는가."""

    def meas(self) -> dict:
        """MFC 실측값 {"flow": {가스: 값}, "pressure": 값}."""

    def current_name(self) -> str:
        """지금 공정 이름."""

    def main_remain_sec(self) -> int:
        """메인 공정 남은 초(-1 = 메인 공정 전)."""

    def main_total_sec(self) -> int:
        """메인 공정 총 초."""

    def csv_file_path(self) -> Optional[str]:
        """적재된 레시피 파일 경로."""

    def csv_index(self) -> int:
        """실행 중인 레시피 행(0부터, -1 = 시작 전)."""

    def csv_rows(self) -> Optional[list]:
        """적재된 레시피 행 목록."""

    def csv_mode(self) -> bool:
        """CSV 리스트 공정 진행 중인가."""

    def heater_output_text(self) -> str:
        """히터 출력 문구(예: "정지 (DAC 0)")."""

    def heater_atmosphere(self) -> dict:
        """히터 전용 가스·압력 상태 스냅샷."""

    def heater_info(self) -> dict:
        """마지막 히터 폴링 값(cur_sv·pid_err·run·fault …)."""

    def heater_dev_ok(self) -> bool:
        """히터 편차가 허용 범위 안인가(LCD 편차 색)."""

    def heater_hold(self) -> Any:
        """목표 도달 후 유지 모드 객체(is_holding()·kind) — 없으면 None."""

    def heater_badge(self) -> Optional[str]:
        """히터 LCD 배지(FAULT/ITL/HOLD/RUN/STOP)."""

    def heater_recipe(self) -> Any:
        """히터 레시피 러너(is_running()·progress()) — 없으면 None."""

    def indicators(self) -> dict:
        """PLC 센서 표시등 마지막 값 {이름: bool}."""

    def plc_bits(self) -> dict:
        """PLC 비트 마지막 값 {이름: bool}."""

    def valves(self) -> dict:
        """PLC 밸브·버튼 표시 마지막 값 {이름: bool}."""

    def plc_link_up(self) -> bool:
        """PLC 링크가 살아 있는가."""

    def erp_update_state(self, state: dict) -> None:
        """상태 dict 를 ERP 리포터로 보낸다."""

    def erp_event(self, level: str, msg: str) -> None:
        """ERP 이벤트 한 건을 보낸다."""

    def snap_err_reported(self) -> bool:
        """스냅샷 수집 실패를 이미 보고했는가."""

    def set_snap_err_reported(self, v: bool) -> None:
        """스냅샷 수집 실패 보고 여부를 기록한다."""


def build_state(src: ErpStateSource) -> dict:
    """ERP 로 보낼 상태 dict. 값이 없으면 빈 글자/None/False — 예외는 부르는 쪽(tick)이 처리한다."""
    running = src.process_running()

    _stage_all = src.text("stage_monitor")
    stage = _stage_all.splitlines()[-1] if _stage_all else ""

    # MFC 실측값 (update_mfc_*_display 가 채운다).
    #  MFC 폴링은 공정 중에만 돌기 때문에, 공정 밖에서는 마지막에
    #  읽은 죽은 값이 남는다. 그래서 공정 중일 때만 실측으로 내보낸다.
    #  (setpoint 는 UI 설정값이라 공정과 무관하게 항상 유효하다)
    _meas = src.meas() if running else {}
    _flow = _meas.get("flow") or {}
    _press = _meas.get("pressure")

    # 공정 중이 아니면 계측값(PV)은 신뢰할 수 없다.
    # 위젯에는 이전 공정의 마지막 값이 남아 있으므로 대기 중에는 비운다.
    def _pv(name: str):
        return src.text(name) if running else ""

    _w = src.text

    # 계측 그룹 — value=계측값(PV), setpoint=설정값(SV)
    groups = [
        {"label": "전원", "items": [
            {"label": "DC Power", "value": _pv("Power_edit"),
             "setpoint": _w("DC_power_edit"), "unit": "W"},
            {"label": "Voltage", "value": _pv("Voltage_edit"), "unit": "V"},
            {"label": "Current", "value": _pv("Current_edit"), "unit": "A"},
        ]},
        {"label": "RF", "items": [
            {"label": "for.P", "value": _pv("for_p_edit"),
             "setpoint": _w("RF_power_edit"), "unit": "W"},
            {"label": "ref.P", "value": _pv("ref_p_edit"), "unit": "W"},
            {"label": "offset / param",
             "value": f'{_w("offset_edit")} / {_w("param_edit")}'},
        ]},
        # RF Pulse 는 별개 장비라 그룹을 따로 둔다(RF 와 나란히 비교)
        {"label": "RF Pulse", "items": [
            {"label": "for.P", "value": _pv("rfp_for_p_edit"),
             "setpoint": _w("rfp_power_edit"), "unit": "W"},
            {"label": "ref.P", "value": _pv("rfp_ref_p_edit"), "unit": "W"},
            {"label": "Freq", "value": _w("rfp_freq_edit"), "unit": "kHz"},
            {"label": "Duty", "value": _w("rfp_duty_edit"), "unit": "%"},
        ]},
        {"label": "가스", "items": [
            {"label": "Ar", "value": _flow.get("Ar"),
             "setpoint": _w("Ar_flow_edit"), "unit": "sccm"},
            {"label": "O₂", "value": _flow.get("O2"),
             "setpoint": _w("O2_flow_edit"), "unit": "sccm"},
        ]},
        {"label": "압력 · 시간", "items": [
            {"label": "챔버 압력", "value": _press, "unit": "mTorr"},
            {"label": "Working P", "setpoint": _w("working_pressure_edit"),
             "unit": "mTorr"},
            {"label": "공정 시간", "setpoint": _w("process_time_edit"), "unit": "분"},
            {"label": "셔터 딜레이", "setpoint": _w("Shutter_delay_edit"),
             "unit": "분"},
        ]},
    ]

    # 타겟(체크된 건만)
    tg = []
    if src.checked("G1_checkbox"):
        tg.append({"label": "G1", "value": _w("G1_edit") or "미입력"})
    if src.checked("G2_checkbox"):
        tg.append({"label": "G2", "value": _w("G2_edit") or "미입력"})
    if tg:
        groups.append({"label": "타겟", "items": tg})

    state = {
        "status": "running" if running else "idle",
        "stage": stage,
        "groups": groups,
        "heater": {
            "pv": _w("heater_pv_edit"),
            "sv": _w("heater_sv_edit"),
            "status": _w("heater_status_label"),
            "output": src.heater_output_text(),
            "atmosphere": src.heater_atmosphere(),
            "on": src.checked("heater_onoff_button"),
            "curSv": src.heater_info().get("cur_sv"),
            "pidErr": src.heater_info().get("pid_err"),
            "otLimit": src.heater_info().get("ot_limit"),
            "run": bool(src.heater_info().get("run")),
            "fault": bool(src.heater_info().get("fault")),
            "tcErr": bool(src.heater_info().get("tc_err")),
            "wdErr": bool(src.heater_info().get("wd_err")),
            "ot": bool(src.heater_info().get("ot")),
            # 장비 LCD 에 떠 있는 것을 텍스트 그대로 보낸다(웹은 해석하지 않는다).
            #  sv   : heater_sv_big — 운전 cur_sv / 정지 sv / TC2 추종 중 sv(TC1 목표)
            #  dev  : heater_dev_label — "Δ+1.2", 정지 중 ""
            #  tc2  : heater_pv2_label — "TC2 382.3 °C" / "TC2 --.-" / "TC2 a → b"
            #  badge: FAULT / ITL / HOLD / RUN / STOP (_update_heater_lcd 우선순위 그대로)
            "lcd": {
                "sv": _w("heater_sv_big"),
                "dev": _w("heater_dev_label"),
                "devOk": bool(src.heater_dev_ok()),
                "tc2": _w("heater_pv2_label"),
                "tc2Hold": bool(
                    src.heater_hold() is not None
                    and src.heater_hold().is_holding()
                    and src.heater_hold().kind == "tc2"),
                "badge": src.heater_badge() or "",
            },
            "recipeRunning": bool(
                getattr(src.heater_recipe(), "is_running", lambda: False)()
            ),
        },
        "heaterRecipe": (
            src.heater_recipe().progress()
            if hasattr(src.heater_recipe(), "progress")
            else None
        ),
        "csvRecipe": (
            {
                "name": os.path.basename(str(src.csv_file_path() or "")) or None,
                "stepNo": int(src.csv_index()) + 1,
                "total": len(src.csv_rows() or []),
                "active": bool(src.csv_mode()),
                "steps": [
                    str((r or {}).get("Process_name") or f"STEP{i+1}")
                    for i, r in enumerate(src.csv_rows() or [])
                ],
                # 웹 상세 표시용 스텝 파라미터(문자열 그대로, 검증은 장비가 한다)
                "rows": [
                    {k: str((r or {}).get(k, "") or "") for k in (
                        "Process_name", "Ar", "Ar_flow", "O2", "O2_flow",
                        "working_pressure", "process_time", "shutter_delay",
                        "use_rf_power", "rf_power", "use_dc_power", "dc_power",
                        "use_rf_pulse", "rf_pulse_power",
                        "rf_pulse_freq", "rf_pulse_duty",
                        "use_heater", "heater_temp", "heater_ramp",
                        "gun1", "gun2", "G1 Target", "G2 Target",
                    )}
                    for r in (src.csv_rows() or [])
                ],
            }
            if (src.csv_rows() or None)
            else None
        ),
        "ion": {
            "run": bool(src.indicators().get("ION_RUN")),
            "lamp": bool(src.indicators().get("ION_LAMP")),
            "overtime": bool(src.indicators().get("ION_OT")),
        },
        "indicators": dict(src.indicators()),
        "mvInterlock": (src.plc_bits() or {}).get("MV_INTERLOCK"),
        "valves": dict(src.valves()),
        # 링크 다운 중 indicators/valves 는 마지막 값 그대로다(거짓 OFF 보고 금지) — 이 플래그로 구분
        "plc_link": bool(src.plc_link_up()),
    }

    # 공정 진행 정보 — 계산은 장비가 하고 웹은 표시만 한다.
    #  remainSec: 메인 공정 잔여 초 (process_time_tick 원본). -1 = 아직 메인 공정 전
    #  totalSec : 메인 공정 총 초
    #  phase    : main = 메인 공정 진행 중, pre = 준비 단계(승온·안정화·셔터딜레이)
    if running:
        _remain = int(src.main_remain_sec())
        _total = int(src.main_total_sec())
        state["process"] = {
            "name": src.current_name() or "",
            "remainSec": _remain,
            "totalSec": _total,
            "phase": "main" if _remain >= 0 else "pre",
        }
    return state


class ErpStatePublisher:
    """1초 타이머가 부르는 tick — 상태를 만들어 보내고, 수집이 실패하면 처음 한 번만 ERP 이벤트로 알린다."""

    def __init__(self, src: ErpStateSource):
        self.src = src

    def tick(self) -> None:
        src = self.src
        try:
            src.erp_update_state(build_state(src))
        except Exception as e:
            # 조용한 실패 방지: 원인을 웹 이벤트로 1회만 보고한다.
            try:
                if not src.snap_err_reported():
                    src.set_snap_err_reported(True)
                    src.erp_event("error", f"스냅샷 수집 실패: {type(e).__name__}: {e}")
            except Exception:
                pass
