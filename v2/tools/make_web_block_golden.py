"""블록 검사 골든 만들기(사용자 지시 5-10 ①) — 웹 W-4b 에서 TS 로 옮긴 blocks.check_blocks·expand·uses_of 가 장비와 같은지 확인용.

python -m v2.tools.make_web_block_golden   → v2/tests/golden/web_block_cases.json 을 다시 쓴다(손으로 고치지 않음).
경우마다 {name, covers(이 경우가 보는 규칙 꼬리표), blocks(입력 — 형식이 틀린 것도 그대로), cfg(검사·펼치기가 읽는 키만 — CFG_KEYS),
          igEnabled(= cfg.IG_ENABLED), rfpulsePresent(= cfg.RFPULSE_ENABLED — process_engine 과 같게),
          clean(정리된 블록 목록 또는 null), errors(오류 문구 그대로), warnings(경고 문구 그대로),
          uses(blocks.uses_of(clean) 또는 null), expanded(펼친 단계 요약 또는 null)}.
오류가 있으면 clean·uses·expanded 가 null.

펼친 단계 요약(expanded) — 블록 하나(BlockInst)가 항목 하나, 펼친 순서대로:
  {"no": "5.2#2"(블록 번호 — 반복 안은 "<반복 번호>.<안 번호>#<회차>", 중첩 회차는 "#1.2"),
   "type", "label"(빈 문자열이면 화면은 정의 label), "stage"(이 블록의 공정 상태 이름 — 비우면 정의 stage, pressure 는 PRESSURE_SP{sp}),
   "iter": "2" 또는 "1.2"(반복 안 회차, 반복 밖이면 null), "auto": true(자동 끝 정지 — 목록 끝이 end_stop 이 아니어서 덧붙임),
   "params": 펼칠 때 쓴 값(비운 값은 기본값, "cfg:키" 는 지금 설정값으로 바뀐 것),
   "watch": 이탈 감시 {dc, rf, rfp, flow, pressure}(wait·main·cond_wait 만 — 그 밖은 없음),
   "steps": [{"action", "message"(화면 문구), "params"?(드라이버 명령 [이름, 인자] 또는 COND_WAIT 인자),
              "value"?, "sec"?(DELAY 시간 s), "timer"?(shutter·process), "polling"?, "ref"?(공정 중 수정 키),
              "internal"?(END_STOP·NOP — 실행 기록·골든 비교에서 뺌), "spec"?(POWER_WAIT 목표·skippable·main·skip)}]}
  스텝의 키는 값이 있을 때만(null·""·false 는 뺀다).
숫자 문구는 파이썬 형식 그대로. 검사 전에 값을 정의 형식대로 바꾼다(float 값은 float, int 값은 정수값이면 int —
  JSON 의 5 와 5.0 이 같은 결과). 값은 repr(float 5 → "5.0", 1e-5 → "1e-05", int → "5"), 범위 끝은 :g("1e-09", "700"),
  압력 :.2f, 베이스 :.1E("5.0E-06"), 펄스 위반은 recipe.pulse_edge_violation(:g·:.1f).
시험(test_web_block_golden)이 파일 = 지금 코드로 만든 것인지, 블록 종류 18개·규칙 꼬리표·VALUE_RULES 가 다 나오는지 본다.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from v2.common.paramdef import defaults
from v2.chk import blocks as BK
from v2.chk import params as P
from v2.tools.make_web_template_golden import _Track

OUT = Path(__file__).resolve().parents[1] / "tests" / "golden" / "web_block_cases.json"

# check_blocks·expand 가 읽는 설정 키 — 시험이 실제로 읽은 키와 같은지 본다
CFG_KEYS: tuple = (
    "BASE_PRESSURE_MAX_TORR", "BASE_PRESSURE_MIN_TORR", "DC_MAX_POWER", "HEATER_ENABLED", "HEATER_MAX_TEMP",
    "HEATER_RAMP_RATE_C_PER_MIN", "HEATER_RECIPE_RAMP_MAX_C_PER_MIN", "HEATER_SOAK_TIME_SEC", "HEATER_SOAK_TOLERANCE",
    "IG_ENABLED", "MFC_AR_FS_SCCM", "MFC_O2_FS_SCCM", "PRESSURE_STAGE_TIMEOUT_SEC", "PRESSURE_WAIT_STABLE_SEC",
    "PRESSURE_WAIT_TOL_RATIO", "PRE_POWER_OFF_SETTLE_SEC", "RFPULSE_DUTY_MAX", "RFPULSE_DUTY_MIN", "RFPULSE_ENABLED",
    "RFPULSE_MAX_POWER", "RFPULSE_PULSE_FREQ_MAX_HZ", "RFPULSE_PULSE_FREQ_MIN_HZ", "RFPULSE_PULSE_MIN_EDGE_US", "RF_MAX_POWER",
    "SP1_TIMEOUT_SEC", "SP2_MTORR", "SP3_MTORR", "SP4_MTORR",
)

# 규칙 꼬리표 — 시험이 모두 한 번 이상 나오는지 본다(값 사이 규칙은 VALUE_RULES id 로 따로 본다)
TAGS: tuple = (
    "range.min", "range.max", "range.paramMin", "range.paramMax", "range.paramScale", "range.minExclusive", "required",
    "enum", "device.RFPULSE_ENABLED", "device.IG_ENABLED", "device.HEATER_ENABLED", "cond.field", "cond.op",
    "repeat.inner", "repeat.nested", "repeat.count", "repeat.empty", "stop.skip", "limits.repeatMax", "limits.maxDepth",
    "limits.maxExpandedSteps", "limits.labelMax", "rules.orderFree", "list.empty", "format.strNumber", "format.boolNumber", "format.notBool",
    "format.notInt", "format.notObject", "format.unknownKey", "format.stage", "format.list", "format.watch", "defaults",
    "autoEndStop", "warning",
)


def B(t: str, *, label: Any = None, stage: Any = None, blocks: Any = None, **params) -> Dict[str, Any]:
    b: Dict[str, Any] = {"type": t, "params": params}
    if label is not None:
        b["label"] = label
    if stage is not None:
        b["stage"] = stage
    if blocks is not None:
        b["blocks"] = blocks
    return b


RFP = {"RFPULSE_ENABLED": True}
_FULL = [
    B("pre_clean"), B("gas_prep", ar=True), B("heater_ramp", target_c=200.0, ramp_c_per_min=10.0), B("heater_wait"),
    B("base_wait", target_torr=5e-6), B("gas_intro", ar=True, ar_flow=20.0),
    B("pressure", sp=4, stage="GAS_STABILIZE", wait=False), B("wait", sec=5, stage="GAS_STABILIZE"),
    B("valve", action="open"), B("shutter", action="open", targets=["g1"]), B("power_on", dc_power=100.0),
    B("pressure", sp=1, value=3.0),
    B("repeat", count=2, blocks=[B("main", sec=10.0, watch={"flow": False}),
                                 B("cond_wait", field="mfc.pressure_mtorr", op="le", value=4.0, timeout_sec=60.0)]),
    B("power_off", dc=True), B("gas_off", ar=True), B("heater_off"), B("end_stop", skip=[1, 2]),
]

# (이름, 블록 목록, 설정 바꿈, 꼬리표)
CASES: List[tuple] = [
    # ── 통과(블록 종류마다 펼친 모양) ──
    ("full_all_types", _FULL, {}, ["repeat.inner"]),
    ("pre_clean_defaults", [B("pre_clean")], {}, ["defaults", "autoEndStop"]),
    ("pre_clean_rfpulse_on", [B("pre_clean", settle_sec=2.4)], RFP, ["defaults"]),
    ("gas_prep_both", [B("gas_prep", ar=True, o2=True)], {}, []),
    ("heater_ramp_default_rate", [B("heater_ramp", target_c=150.0), B("heater_wait")], {}, ["defaults"]),
    ("heater_ramp_zero_rate", [B("heater_ramp", target_c=150.0, ramp_c_per_min=0)], {}, ["defaults"]),
    ("heater_ramp_below_min_warn", [B("heater_ramp", target_c=150.0, ramp_c_per_min=3.0)], {}, ["warning"]),
    ("heater_wait_without_target", [B("heater_wait")], {}, []),
    ("base_wait_ok", [B("base_wait", target_torr=5e-6)], {}, []),
    ("gas_intro_both_no_zero", [B("gas_intro", ar=True, ar_flow=20.0, o2=True, o2_flow=2.5, zero=False)], {}, []),
    ("pressure_defaults", [B("pressure", sp=2), B("pressure", sp=3, label="S3"), B("pressure", sp=4, wait=False),
                           B("pressure", sp=1, set=False, wait=False)], {}, ["defaults"]),
    ("pressure_custom_abort", [B("pressure", sp=2, value=12.5, tol_ratio=0.1, stable_sec=3.0, timeout_sec=90.0,
                                 on_timeout="abort"), B("pressure", sp=1, value=3.0, on_timeout="continue")], {}, []),
    ("pressure_sp_int_as_float", [B("pressure", sp=2.0)], {}, []),
    ("valve_all", [B("valve", action="open"), B("valve", action="close"), B("valve", action="hold")], {}, []),
    ("shutter_close_all", [B("shutter", action="close", targets=["ms", "g2", "g1"])], {}, []),
    ("power_on_rf_pulse", [B("power_on", rfp_power=100.0, rfp_freq_khz=10.0, rfp_duty=50, timeout_sec=120.0)], RFP, []),
    ("power_on_dc_rf_no_wait", [B("power_on", dc_power=50.0, rf_power=80.0, wait=False)], {}, []),
    ("power_on_nothing_no_wait", [B("power_on", wait=False)], {}, []),
    ("power_on_freq_only", [B("power_on", rfp_power=60.0, rfp_freq_khz=25.0)], RFP, []),
    ("power_off_all_rfpulse_absent", [B("power_off", dc=True, rf=True, rfpulse=True)], {}, []),
    ("power_off_all_rfpulse_present", [B("power_off", dc=True, rf=True, rfpulse=True)], RFP, []),
    ("gas_off_keep_valves", [B("gas_off", ar=True, o2=True, close_valves=False)], {}, []),
    ("wait_stages", [B("wait", sec=1.5), B("wait", sec=3, stage="DC_DELAY", label="DC 대기"),
                     B("wait", sec=60, stage="SHUTTER_DELAY", watch={"dc": True}), B("wait", sec=0)], {}, []),
    ("main_close", [B("main", sec=30, close=True, watch={"rf": False, "pressure": False})], {}, []),
    ("cond_wait_ops", [B("cond_wait", field="heater.pv", op="ge", value=100.0, timeout_sec=600.0, stable_sec=5.0),
                       B("cond_wait", field="mfc.ar_sccm", op="near", value=20.0, tol=0.5, timeout_sec=30.0,
                         on_timeout="continue", label="유량 확인"),
                       B("cond_wait", field="ig.ion_torr", op="le", value=1e-5, timeout_sec=900.0)], {},
     ["cond.field", "cond.op"]),
    ("cond_wait_non_ig_ig_off", [B("cond_wait", field="dc.p", op="ge", value=50.0, timeout_sec=10.0)],
     {"IG_ENABLED": False}, ["device.IG_ENABLED"]),
    ("repeat_nested", [B("repeat", count=2, blocks=[B("wait", sec=1), B("repeat", count=2, blocks=[B("wait", sec=2)])]),
                       B("end_stop")], {}, ["repeat.nested", "repeat.inner"]),
    ("repeat_max_ok", [B("repeat", count=100, blocks=[B("valve", action="hold")])], {}, ["limits.repeatMax"]),
    ("repeat_depth_4_ok", [B("repeat", count=1, blocks=[B("repeat", count=1, blocks=[B("repeat", count=1, blocks=[
        B("repeat", count=1, blocks=[B("wait", sec=1)])])])])], {}, ["limits.maxDepth"]),
    ("end_stop_skip_all_allowed", [B("end_stop", skip=[1, 2, 3, 9, 10, 11, 12])], {}, ["stop.skip"]),
    ("end_stop_middle", [B("end_stop"), B("wait", sec=2)], {}, ["autoEndStop", "rules.orderFree"]),
    ("order_free", [B("main", sec=5), B("power_on", dc_power=20.0), B("gas_intro", o2=True, o2_flow=1.0),
                    B("heater_off"), B("pre_clean", dc=False, rf=False, rfpulse=False, settle_sec=0)], {},
     ["rules.orderFree"]),
    ("label_stage_given", [B("shutter", action="open", targets=["g2"], label="L" * BK.LABEL_MAX, stage="MAIN")], {},
     ["limits.labelMax", "format.stage"]),
    ("empty_values_are_defaults", [B("wait", sec=4, watch=None), B("pre_clean", settle_sec="  "),
                                   B("pressure", sp=2, value="", on_timeout=None)], {}, ["defaults"]),
    ("params_falsy_is_empty", [{"type": "heater_wait", "params": []}, {"type": "valve", "params": {"action": "open"}}], {},
     ["format.notObject"]),
    # ── 목록 ──
    ("err_empty_list", [], {}, ["list.empty"]),
    ("err_not_list", {"type": "wait"}, {}, ["list.empty"]),
    ("err_block_not_object", [5, B("wait", sec=1)], {}, ["format.notObject"]),
    ("err_unknown_type", [B("nope"), {"params": {}}], {}, ["format.unknownKey"]),
    ("err_unknown_block_key", [dict(B("wait", sec=1), foo=1)], {}, ["format.unknownKey"]),
    ("err_params_not_object", [{"type": "wait", "params": "sec=1"}, B("wait", sec=1, label=5)], {},
     ["format.notObject", "limits.labelMax"]),
    ("err_unknown_param", [B("wait", sec=1, secs=2)], {}, ["format.unknownKey"]),
    # ── 필수·형식 ──
    ("err_required_missing", [B("wait"), B("heater_ramp"), B("base_wait"), B("valve"), B("shutter", action="open"),
                              B("main"), B("repeat", blocks=[B("wait", sec=1)]), B("pressure")], {}, ["required"]),
    ("err_required_blank_string", [B("wait", sec="  "), B("cond_wait", field="", op="le", value=1.0, timeout_sec=1.0)], {},
     ["required"]),
    ("err_cond_wait_required", [B("cond_wait")], {}, ["required", "cond.field"]),
    ("err_str_number", [B("wait", sec="5"), B("power_on", dc_power="100")], {}, ["format.strNumber"]),
    ("err_bool_number", [B("wait", sec=True), B("repeat", count=False, blocks=[B("wait", sec=1)])], {}, ["format.boolNumber"]),
    ("err_not_bool", [B("gas_prep", ar="true", o2=1), B("main", sec=5, close=0)], {}, ["format.notBool"]),
    ("err_not_int", [B("repeat", count=2.5, blocks=[B("wait", sec=1)]), B("pressure", sp=1.5, value=3.0),
                     B("power_on", rfp_power=10.0, rfp_duty=33.3)], RFP, ["format.notInt"]),
    ("err_watch", [B("wait", sec=1, watch={"dc": 1}), B("main", sec=1, watch={"xx": True}), B("cond_wait", field="dc.p",
                   op="ge", value=1.0, timeout_sec=1.0, watch="on")], {}, ["format.watch"]),
    ("err_list", [B("shutter", action="open", targets=[]), B("shutter", action="open", targets=["g3"]),
                  B("shutter", action="close", targets=["g1", "g1"]), B("shutter", action="close", targets="g1")], {},
     ["format.list"]),
    ("err_label_too_long", [B("wait", sec=1, label="L" * (BK.LABEL_MAX + 1))], {}, ["limits.labelMax"]),
    ("err_stage", [B("wait", sec=1, stage="MAIN"), B("pressure", sp=2, stage="PRESSURE_SP9"), B("repeat", count=1,
                   stage="X", blocks=[B("wait", sec=1)])], {}, ["format.stage"]),
    ("err_blocks_on_non_repeat", [B("wait", sec=1, blocks=[B("wait", sec=1)])], {}, ["format.unknownKey"]),
    # ── 범위 ──
    ("err_range_min", [B("wait", sec=-1), B("cond_wait", field="dc.p", op="near", value=1.0, tol=-0.00001, timeout_sec=1.0),
                       B("pressure", sp=0, value=3.0), B("pressure", sp=2, tol_ratio=0.001)], {}, ["range.min"]),
    ("err_range_max", [B("wait", sec=7 * 86400 + 1), B("pressure", sp=5, value=3.0), B("pressure", sp=1, value=1000.5),
                       B("cond_wait", field="dc.p", op="ge", value=1.0, stable_sec=3601, timeout_sec=1.0),
                       B("pre_clean", settle_sec=31)], {}, ["range.max"]),
    ("err_range_min_exclusive", [B("main", sec=0), B("gas_intro", ar=True, ar_flow=0.0), B("heater_ramp", target_c=0),
                                 B("power_on", dc_power=0.0), B("cond_wait", field="dc.p", op="ge", value=1.0,
                                                                timeout_sec=0)], {}, ["range.minExclusive"]),
    ("err_range_param_max", [B("gas_intro", ar=True, ar_flow=500.0, o2=True, o2_flow=500.0),
                             B("heater_ramp", target_c=800.0, ramp_c_per_min=1000.0), B("power_on", dc_power=99999.0,
                                                                                         rf_power=99999.0)], {},
     ["range.paramMax"]),
    ("err_range_param_max_cfg", [B("heater_ramp", target_c=450.0)], {"HEATER_MAX_TEMP": 400.0}, ["range.paramMax"]),
    ("err_range_param_min", [B("base_wait", target_torr=1e-12), B("base_wait", target_torr=1.0)], {},
     ["range.paramMin", "range.paramMax"]),
    ("err_range_param_scale", [B("power_on", rfp_power=50.0, rfp_freq_khz=0.0001), B("power_on", rfp_power=50.0,
                                                                                  rfp_freq_khz=9999.0)], RFP,
     ["range.paramScale"]),
    ("err_rfp_duty_range", [B("power_on", rfp_power=50.0, rfp_duty=0), B("power_on", rfp_power=50.0, rfp_duty=100)], RFP,
     ["range.paramMin", "range.paramMax"]),
    # ── enum ──
    ("err_enum", [B("valve", action="toggle"), B("pressure", sp=2, on_timeout="retry"),
                  B("cond_wait", field="x.y", op="lt", value=1.0, timeout_sec=1.0, on_timeout=True),
                  B("shutter", action=1, targets=["g1"])], {}, ["enum", "cond.field", "cond.op"]),
    # ── 값 사이·꺼진 장비 ──
    ("err_need_flow", [B("gas_intro", ar=True), B("gas_intro", o2=True, o2_flow=None)], {}, ["required"]),
    ("err_need_sp1_value", [B("pressure", sp=1), B("pressure", sp=1, set=False)], {}, ["required"]),
    ("err_heater_disabled", [B("heater_ramp", target_c=100.0), B("heater_wait"), B("heater_off")], {"HEATER_ENABLED": False},
     ["device.HEATER_ENABLED"]),
    ("err_ig_disabled", [B("base_wait", target_torr=5e-6), B("cond_wait", field="ig.ion_torr", op="le", value=1e-5,
                                                            timeout_sec=60.0)], {"IG_ENABLED": False},
     ["device.IG_ENABLED"]),
    ("err_rfpulse_disabled", [B("power_on", rfp_power=50.0), B("pre_clean", rfpulse=True), B("power_off", rfpulse=True)],
     {}, ["device.RFPULSE_ENABLED"]),
    ("err_pulse_edge", [B("power_on", rfp_power=100.0, rfp_freq_khz=25.0, rfp_duty=10),
                        B("power_on", rfp_power=100.0, rfp_freq_khz=25.0, rfp_duty=95)], RFP, []),
    ("pulse_edge_check_off", [B("power_on", rfp_power=100.0, rfp_freq_khz=25.0, rfp_duty=10)],
     dict(RFP, RFPULSE_PULSE_MIN_EDGE_US=0.0), []),
    ("err_mixed_order_of_messages", [B("gas_intro", ar=True, ar_flow=-1.0, o2="x", zero=None, extra=1),
                                     B("heater_ramp", target_c=900.0, ramp_c_per_min=2.0)],
     {"HEATER_ENABLED": False}, ["warning", "device.HEATER_ENABLED"]),
    # ── 반복 ──
    ("err_repeat_count", [B("repeat", count=0, blocks=[B("wait", sec=1)]), B("repeat", count=101, blocks=[B("wait", sec=1)])],
     {}, ["repeat.count", "limits.repeatMax"]),
    ("err_repeat_empty", [B("repeat", count=2, blocks=[]), B("repeat", count=2), B("repeat", count=2, blocks={"type": "wait"})],
     {}, ["repeat.empty"]),
    ("err_repeat_inner_error", [B("wait", sec=1), B("repeat", count=2, blocks=[B("wait", sec=1), B("wait", sec=-5),
                                                                             B("repeat", count=2, blocks=[B("nope")])])],
     {}, ["repeat.inner", "repeat.nested"]),
    ("err_repeat_depth", [B("repeat", count=1, blocks=[B("repeat", count=1, blocks=[B("repeat", count=1, blocks=[
        B("repeat", count=1, blocks=[B("repeat", count=1, blocks=[B("wait", sec=1)])])])])])], {}, ["limits.maxDepth"]),
    ("err_expanded_steps", [B("repeat", count=100, blocks=[B("repeat", count=31, blocks=[B("wait", sec=1)])])], {},
     ["limits.maxExpandedSteps"]),
    # ── 정지 순서 번호 ──
    ("err_end_stop_skip_power", [B("end_stop", skip=[4, 13])], {}, ["stop.skip"]),
    ("err_end_stop_skip_format", [B("end_stop", skip=[99]), B("end_stop", skip=["1"]), B("end_stop", skip=[True]),
                                  B("end_stop", skip=1)], {}, ["stop.skip"]),
]


def _step(s) -> Dict[str, Any]:
    d: Dict[str, Any] = {"action": s.action, "message": s.message}
    p = s.params
    for k, v in (("params", list(p) if isinstance(p, tuple) else p), ("value", s.value), ("sec", s.duration_sec),
                 ("timer", s.timer_purpose), ("polling", s.polling), ("ref", s.ref), ("internal", s.internal),
                 ("spec", s.spec)):
        if v is not None and v != "" and v is not False:
            d[k] = v
    return d


def summary(steps: list, insts: list) -> List[Dict[str, Any]]:
    defs = BK.by_type()
    out = []
    for inst in insts:
        it = inst.no.split("#", 1)[1] if "#" in inst.no else None
        d = {"no": inst.no, "type": inst.type, "label": inst.label, "stage": inst.stage, "iter": it, "auto": inst.auto,
             "params": dict(inst.params)}
        if defs[inst.type].watch is not None:
            d["watch"] = dict(inst.watch)
        d["steps"] = [_step(s) for s in steps if s.block == inst.index]
        out.append(d)
    return out


def one(name: str, blocks: Any, over: Dict[str, Any], tags: list, track: set = None) -> Dict[str, Any]:
    full = _Track(defaults(P.PARAMS), **over)
    ig, rfp = bool(full["IG_ENABLED"]), bool(full["RFPULSE_ENABLED"])
    clean, errs, warns = BK.check_blocks(blocks, full)
    uses = expanded = None
    if clean is not None:
        uses = BK.uses_of(clean)
        expanded = summary(*BK.expand(clean, full, ig_enabled=ig, rfpulse_present=rfp))
    if track is not None:
        track |= full.read
    return {"name": name, "covers": sorted(set(tags)), "blocks": blocks,
            "cfg": {k: dict.__getitem__(full, k) for k in CFG_KEYS}, "igEnabled": ig, "rfpulsePresent": rfp,
            "clean": clean, "errors": list(errs), "warnings": list(warns), "uses": uses, "expanded": expanded}


def build(track: set = None) -> Dict[str, Any]:
    return {"_설명": "블록 검사 골든(v2.tools.make_web_block_golden 이 만든다 — 손으로 고치지 않음). 웹 W-4b TS 계산 확인용. "
                   "모양은 생성기 머리말",
            "cases": [one(n, b, o, t, track) for n, b, o, t in CASES]}


def to_json(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False, indent=1, allow_nan=False) + "\n"


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(to_json(build()), encoding="utf-8", newline="\n")
    print(f"{OUT} — {len(CASES)} 경우")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
