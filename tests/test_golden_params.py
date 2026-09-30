# -*- coding: utf-8 -*-
"""골든(동작 고정) — 공정 파라미터 만들기(수동 입력칸 / CSV 행).

리팩토링 1단계(파라미터 만들기를 화면에서 떼어내기) 전에 지금 코드의 결과를 찍어 둔다.
  20_params_manual.json : 수동 입력칸 → _handle_start_process() → 나간 params(키 순서·타입) 또는 경고창
  21_params_csv.json    : UI offset/param + CSV 행 → _build_params_from_csv_row() → dict 또는 ValueError, 그동안의 로그
갱신은 CHK_UPDATE_GOLDEN=1 일 때만(test_golden_process.check_golden).
"""
from test_main_heater import win, fresh   # noqa: F401  (픽스처 재사용)
from test_golden_process import H, check_golden, _reset_window, csv_row   # noqa: F401

# 수동 입력칸 전부 — (위젯 이름, 기준값). bool 은 setChecked, str 은 setPlainText.
MANUAL_BASE = {
    "Ar_gas_radio": True, "Ar_flow_edit": "20",
    "O2_gas_radio": False, "O2_flow_edit": "",
    "working_pressure_edit": "5",
    "dc_power_checkbox": True, "DC_power_edit": "100",
    "rf_power_checkbox": False, "RF_power_edit": "",
    "offset_edit": "6.79", "param_edit": "1.0395",
    "rf_pulse_checkbox": False, "rfp_power_edit": "", "rfp_freq_edit": "", "rfp_duty_edit": "",
    "Shutter_delay_edit": "1", "process_time_edit": "1",
    "G1_checkbox": True, "G1_edit": "CeO2",
    "G2_checkbox": False, "G2_edit": "",
    "dc_delay_checkbox": False,
}

_RFP = {"rf_pulse_checkbox": True, "dc_power_checkbox": False}

MANUAL_CASES = [
    ("기본(Ar+DC)", {}),
    ("Ar+O2", {"O2_gas_radio": True, "O2_flow_edit": "10"}),
    ("O2만", {"Ar_gas_radio": False, "O2_gas_radio": True, "O2_flow_edit": "10"}),
    ("가스 없음", {"Ar_gas_radio": False, "O2_gas_radio": False}),
    ("Ar 유량 빈칸", {"Ar_flow_edit": ""}),
    ("Ar 유량 abc", {"Ar_flow_edit": "abc"}),
    ("Ar 유량 0", {"Ar_flow_edit": "0"}),
    ("O2 유량 빈칸", {"O2_gas_radio": True, "O2_flow_edit": ""}),
    ("RF 체크 + offset 빈칸", {"rf_power_checkbox": True, "RF_power_edit": "150", "offset_edit": ""}),
    ("RF 체크 + param 빈칸", {"rf_power_checkbox": True, "RF_power_edit": "150", "param_edit": ""}),
    ("RF 체크 정상", {"rf_power_checkbox": True, "RF_power_edit": "150"}),
    ("RF 체크 + RF 파워 빈칸", {"rf_power_checkbox": True, "RF_power_edit": ""}),
    ("RF 해제 + offset·param 빈칸(기본값)", {"offset_edit": "", "param_edit": ""}),
    ("RF 해제 + offset abc", {"offset_edit": "abc"}),
    ("RF Pulse 파워 빈칸 + 왼쪽 RF 칸 값(흔한 실수)", dict(_RFP, rfp_power_edit="", RF_power_edit="300")),
    ("RF Pulse 파워 빈칸", dict(_RFP, rfp_power_edit="")),
    ("RF Pulse 파워 0", dict(_RFP, rfp_power_edit="0")),
    ("RF Pulse 파워 abc", dict(_RFP, rfp_power_edit="abc")),
    ("RF Pulse 주파수·듀티 빈칸", dict(_RFP, rfp_power_edit="100")),
    ("RF Pulse 20kHz·80%", dict(_RFP, rfp_power_edit="100", rfp_freq_edit="20", rfp_duty_edit="80")),
    ("RF Pulse 40kHz", dict(_RFP, rfp_power_edit="100", rfp_freq_edit="40")),
    ("RF Pulse 듀티 50.7", dict(_RFP, rfp_power_edit="100", rfp_duty_edit="50.7")),
    ("RF Pulse 5kHz·50%", dict(_RFP, rfp_power_edit="100", rfp_freq_edit="5", rfp_duty_edit="50")),
    ("RF Pulse 상한 초과", dict(_RFP, rfp_power_edit="601")),
    ("파워 전부 없음", {"dc_power_checkbox": False}),
    ("DC 체크 + 값 빈칸 + RF 켬", {"DC_power_edit": "", "rf_power_checkbox": True, "RF_power_edit": "150"}),
    ("DC 체크 + 값 빈칸 + 다른 파워 없음", {"DC_power_edit": ""}),
    ("압력 빈칸", {"working_pressure_edit": ""}),
    ("압력 0", {"working_pressure_edit": "0"}),
    ("shutter delay 음수", {"Shutter_delay_edit": "-1"}),
    ("process time 음수", {"process_time_edit": "-1"}),
    ("delay·time 둘 다 0", {"Shutter_delay_edit": "0", "process_time_edit": "0"}),
    ("process time 0 + delay 1", {"process_time_edit": "0", "Shutter_delay_edit": "1"}),
    ("G1·G2 이름 앞뒤 공백", {"G1_edit": "  CeO2 ", "G2_checkbox": True, "G2_edit": " Ti  "}),
    ("DC delay 체크", {"dc_delay_checkbox": True}),
    ("겹침: 압력 빈칸 + shutter delay 음수", {"working_pressure_edit": "", "Shutter_delay_edit": "-1"}),
    ("겹침: RF Pulse 범위 위반 + 압력 빈칸", dict(_RFP, rfp_power_edit="100", rfp_freq_edit="40",
                                            working_pressure_edit="")),
]


def typed(d):
    """dict → [[키, 타입, 값], …] (키 순서·int/float 구분이 그대로 남게)."""
    return [[k, type(v).__name__, v] for k, v in d.items()]


def _fill(w, fields):
    for name, v in fields.items():
        wd = getattr(w.ui, name)
        if isinstance(v, bool):
            wd.setChecked(v)
        else:
            wd.setPlainText(v)


def _calls_since(h, n0, prefix):
    return [c for c in h.sink.mock_calls[n0:] if c[0].startswith(prefix)]


def test_golden_params_manual(H):
    w = H.w
    out = []
    for name, over in MANUAL_CASES:
        _reset_window(w)
        _fill(w, dict(MANUAL_BASE, **over))
        n0 = len(H.sink.mock_calls)
        w._handle_start_process()
        H.flush()
        starts = _calls_since(H, n0, "sig.request_process_start")
        alerts = [[c[0].split(".", 1)[1], *[H.scrub(str(a)) for a in c[1]]]
                  for c in _calls_since(H, n0, "msgbox.")]
        out.append({"case": name,
                    "params": [typed(c[1][0]) for c in starts],
                    "alerts": alerts})
    _reset_window(w)
    check_golden("20_params_manual", {"cases": out})


# ───────────────────────── CSV 행 ─────────────────────────
def _row(**over):
    return csv_row("S1", **over)


def _old_row():
    r = _row()
    for k in ("use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty",
              "use_heater", "heater_temp", "heater_ramp"):
        r.pop(k)
    return r


_RF = {"use_rf_power": "1", "rf_power": "150"}
_RP = {"use_rf_pulse": "1", "rf_pulse_power": "100"}
_HT = {"use_heater": "1", "heater_temp": "300"}

# (이름, 행, UI offset, UI param)
CSV_CASES = [
    ("기본 행", _row(), "6.79", "1.0395"),
    ("Ar·O2 둘 다 0(이름 있음)", _row(Ar="0", O2="0"), "6.79", "1.0395"),
    ("Ar·O2 둘 다 0(이름 빈칸=STEP)", _row(Ar="0", O2="0", Process_name=""), "6.79", "1.0395"),
    ("Ar_flow 빈칸", _row(Ar_flow=""), "6.79", "1.0395"),
    ("Ar_flow abc", _row(Ar_flow="abc"), "6.79", "1.0395"),
    ("Ar_flow 0", _row(Ar_flow="0"), "6.79", "1.0395"),
    ("O2_flow 빈칸", _row(Ar="0", O2="1", O2_flow=""), "6.79", "1.0395"),
    ("O2_flow abc", _row(Ar="0", O2="1", O2_flow="abc"), "6.79", "1.0395"),
    ("O2_flow 0", _row(Ar="0", O2="1", O2_flow="0"), "6.79", "1.0395"),
    ("O2 정상 + Ar", _row(O2="1", O2_flow="10"), "6.79", "1.0395"),
    ("working_pressure 빈칸", _row(working_pressure=""), "6.79", "1.0395"),
    ("working_pressure 0", _row(working_pressure="0"), "6.79", "1.0395"),
    ("process_time 빈칸", _row(process_time=""), "6.79", "1.0395"),
    ("process_time·shutter_delay 둘 다 0", _row(process_time="0", shutter_delay="0"), "6.79", "1.0395"),
    ("process_time 음수", _row(process_time="-1"), "6.79", "1.0395"),
    ("shutter_delay 음수", _row(shutter_delay="-1"), "6.79", "1.0395"),
    ("process_time·shutter_delay 둘 다 음수", _row(process_time="-1", shutter_delay="-1"), "6.79", "1.0395"),
    ("shutter_delay 빈칸", _row(shutter_delay=""), "6.79", "1.0395"),
    ("use_dc_power=1 + dc_power 빈칸", _row(dc_power=""), "6.79", "1.0395"),
    ("use_dc_power=1 + dc_power 0", _row(dc_power="0"), "6.79", "1.0395"),
    ("파워 전부 없음(use_dc_power=0)", _row(use_dc_power="0"), "6.79", "1.0395"),
    ("use_rf_power=1 + UI offset 빈칸", _row(**_RF), "", "1.0395"),
    ("use_rf_power=1 정상", _row(**_RF), "6.79", "1.0395"),
    ("use_rf_power=1 + UI offset abc", _row(**_RF), "abc", "1.0395"),
    ("use_rf_power=0 + UI offset·param 빈칸", _row(), "", ""),
    ("use_rf_pulse=1 + 파워 빈칸", _row(**dict(_RP, rf_pulse_power="")), "6.79", "1.0395"),
    ("use_rf_pulse=1 + 상한 초과", _row(**dict(_RP, rf_pulse_power="601")), "6.79", "1.0395"),
    ("use_rf_pulse=1 + 주파수·듀티 빈칸", _row(**_RP), "6.79", "1.0395"),
    ("use_rf_pulse=1 + 20kHz·80%", _row(**dict(_RP, rf_pulse_freq="20", rf_pulse_duty="80")), "6.79", "1.0395"),
    ("use_rf_pulse=1 + 5kHz·50.7%", _row(**dict(_RP, rf_pulse_freq="5", rf_pulse_duty="50.7")), "6.79", "1.0395"),
    ("use_rf_pulse=1 + 주파수 abc", _row(**dict(_RP, rf_pulse_freq="abc")), "6.79", "1.0395"),
    ("use_heater=1 + temp 0", _row(**dict(_HT, heater_temp="0")), "6.79", "1.0395"),
    ("use_heater=1 + 상한 초과", _row(**dict(_HT, heater_temp="701")), "6.79", "1.0395"),
    ("use_heater=1 + ramp 0", _row(**dict(_HT, heater_ramp="0")), "6.79", "1.0395"),
    ("use_heater=1 + ramp 3", _row(**dict(_HT, heater_ramp="3")), "6.79", "1.0395"),
    ("use_heater=1 + ramp 70", _row(**dict(_HT, heater_ramp="70")), "6.79", "1.0395"),
    ("use_heater=0 + temp·ramp 값 있음", _row(use_heater="0", heater_temp="300", heater_ramp="3"), "6.79", "1.0395"),
    ("ramp 3 경고 뒤 RF Pulse 오류", _row(**dict(_HT, heater_ramp="3", use_rf_pulse="1", rf_pulse_power="")),
     "6.79", "1.0395"),
    ("참 표기 Y", _row(Ar="Y"), "6.79", "1.0395"),
    ("참 표기 true", _row(Ar="true"), "6.79", "1.0395"),
    ("참 표기 On", _row(Ar="On"), "6.79", "1.0395"),
    ("참 표기 t", _row(Ar="t"), "6.79", "1.0395"),
    ("참 표기 yes", _row(Ar="yes"), "6.79", "1.0395"),
    ("참 표기 2", _row(Ar="2"), "6.79", "1.0395"),
    ("RF Pulse·히터 컬럼이 없는 옛 행", _old_row(), "6.79", "1.0395"),
    ("use_dc_delay=1 + use_dc_power=0", _row(use_dc_delay="1", use_dc_power="0"), "6.79", "1.0395"),
    ("use_dc_delay=1 + use_dc_power=1", _row(use_dc_delay="1"), "6.79", "1.0395"),
    ("Process_name·타겟 이름 앞뒤 공백",
     _row(Process_name="  S1 ", **{"G1 Target": " CeO2 ", "G2 Target": " Ti ", "gun2": "1"}), "6.79", "1.0395"),
    ("값이 None 인 칸", _row(O2_flow=None, rf_pulse_freq=None, **{"G2 Target": None, "shutter_delay": None}),
     "6.79", "1.0395"),
]


def test_golden_params_csv(H):
    w = H.w
    out = []
    for name, row, off, par in CSV_CASES:
        w.ui.offset_edit.setPlainText(off)
        w.ui.param_edit.setPlainText(par)
        n0 = len(H.sink.mock_calls)
        entry = {"case": name}
        try:
            entry["params"] = typed(w._build_params_from_csv_row(dict(row)))
        except Exception as e:
            entry["error"] = [type(e).__name__, str(e)]
        entry["logs"] = [list(c[1]) for c in H.sink.mock_calls[n0:] if c[0] == "log"]
        out.append(entry)
    check_golden("21_params_csv", {"cases": out})
