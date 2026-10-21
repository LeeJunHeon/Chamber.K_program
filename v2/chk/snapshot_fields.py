"""챔버K 1초 스냅샷 필드 — 장비 블록 순서와 필드.

열 이름: <블록>.<필드>. 장비 품질은 <블록>.q / <블록>.age_s.
source 의 "plc:<이름>" 은 devices.PLC_ADDRS 이름이다(시험이 존재를 확인한다).
heater 와 rf 블록은 PLC 한 번 읽기에서 나오지만 화면·요약 단위로 따로 둔다(품질은 PLC 와 같다).
"""
from __future__ import annotations

from v2.common.snapshot import DeviceBlock, FieldDef, FType, row_columns

F, I, B, S = FType.FLOAT, FType.INT, FType.BOOL, FType.STR


def _f(name, type_, unit, label, fmt="", source="", own=False, summary="last") -> FieldDef:
    return FieldDef(name=name, type=type_, unit=unit, label=label, fmt=fmt, source=source,
                    own_quality=own, summary=summary)


def _bit(name, label, addr=None) -> FieldDef:
    return _f(name, B, "", label, source=f"plc:{addr or name}", summary="any")


PLC_BLOCK = DeviceBlock("plc", "PLC", (
    _bit("rotary", "Rotary"), _bit("rv", "RV"), _bit("fv", "FV"), _bit("mv", "Main 밸브"),
    _bit("vent", "Vent"), _bit("turbo", "Turbo"), _bit("door_up", "도어 올림"), _bit("door_down", "도어 내림"),
    _bit("ar_valve", "Ar 차단밸브"), _bit("o2_valve", "O2 차단밸브"), _bit("ms", "Main 셔터"),
    _bit("s1", "Gun1 셔터"), _bit("s2", "Gun2 셔터"), _bit("buzz_stop", "부저 정지"),
    _bit("mv_interlock", "MV 인터록"),
    _bit("air", "Air"), _bit("g1", "G1"), _bit("g2", "G2"), _bit("atm", "ATM"), _bit("water", "냉각수"),
    _bit("ion_remote_on", "이오나이저 ON"), _bit("ion_run", "이오나이저 구동"), _bit("ion_lamp", "이오나이저 점등"),
    _bit("ion_ot", "이오나이저 수명 초과"),
    _f("marker_ok", B, "", "재기동 마커 일치", source="core: D(PLC_SESSION_MARK_REG) 되읽기 비교", summary="any"),
    _f("wd_counter", I, "", "워치독 카운터(마지막 쓴 값)", source="plc:heater_wd"),
))

HEATER_BLOCK = DeviceBlock("heater", "히터(PLC)", (
    _f("pv", F, "°C", "TC1", ".1f", "plc:heater_pv", summary="mean"),
    _f("pv2", F, "°C", "TC2", ".1f", "plc:heater_pv2", summary="mean"),
    _f("sv", F, "°C", "목표", ".1f", "plc:heater_sv"),
    _f("cur_sv", F, "°C", "램프 적용 목표", ".1f", "plc:heater_cur_sv"),
    _f("sv_ramp", F, "°C", "램프 중간 목표", ".1f", "plc:heater_sv_ramp"),
    _f("sv2", F, "°C", "TC2 목표", ".1f", "plc:heater_sv2"),
    _f("sv_limit", F, "°C", "목표 상한(D00013)", ".1f", "plc:heater_sv_limit"),
    _f("ot2_limit", F, "°C", "TC2 과온 한계(D00034)", ".1f", "plc:heater_ot2_limit"),
    _f("sv2_max", F, "°C", "TC2 목표 상한(D00036, 0 = 래더 미지원)", ".1f", "plc:heater_sv2_max"),
    _f("mv", I, "count", "DAC 출력", "d", "plc:heater_mv", summary="mean"),
    _f("mv_limit", I, "count", "DAC 운전 상한", "d", "plc:heater_mv_limit"),
    _f("current_est", F, "A", "추정 전류(표시용)", ".1f", "계산: HEATER_CURRENT_*", summary="mean"),
    _bit("run", "RUN", "heater_run"), _bit("itl", "인터록", "heater_itl"), _bit("fault", "이상", "heater_fault"),
    _bit("ot", "과온", "heater_ot"), _bit("tc_err", "TC 이상", "heater_tc_err"), _bit("wd_err", "워치독", "heater_wd_err"),
    _bit("pid_run", "PID 동작", "heater_pid_run"), _bit("pv_sel", "TC2 제어 요구", "heater_pv_sel"),
    _bit("pv_sel_eff", "TC2 제어 중", "heater_pv_sel_eff"),
    _f("pid_err", I, "", "PID 에러 코드", "d", "plc:heater_pid_err"),
    _f("hold", S, "", "유지 모드 상태(off/arming/holding_dac/holding_tc2)", source="core"),
))

RF_BLOCK = DeviceBlock("rf", "RF 전원(아날로그)", (
    _f("fwd_w", F, "W", "순방향", ".1f", "plc:rf_fwd_adc × RF_FWD_FULL_SCALE_W/4000", summary="mean"),
    _f("ref_w", F, "W", "반사", ".1f", "plc:rf_ref_adc × RF_REF_FULL_SCALE_W/4000", summary="max"),
    _f("dac", I, "count", "DAC 설정", "d", "plc:rf_dac"),
    _bit("dac_enable", "DAC enable", "rf_dac_enable"),
    _f("set_w", F, "W", "목표", ".1f", "core"),
))

MFC_BLOCK = DeviceBlock("mfc", "MFC(GPC3000)", (
    _f("ar_sccm", F, "sccm", "Ar 유량", ".2f", "R60 ch1 × FS/100", own=True, summary="mean"),
    _f("o2_sccm", F, "sccm", "O2 유량", ".2f", "R60 ch2 × FS/100", own=True, summary="mean"),
    _f("pressure_mtorr", F, "mTorr", "챔버 압력", ".2f", "R5 (%FS → mTorr)", summary="mean"),
    _f("ar_set", F, "sccm", "Ar 설정", ".2f", "core"),
    _f("o2_set", F, "sccm", "O2 설정", ".2f", "core"),
    _f("sp_active", I, "", "선택된 설정점(0~5)", "d", "core(마지막 D 명령)"),
    _f("sp_mtorr", F, "mTorr", "압력 설정", ".2f", "core"),
))

DC_BLOCK = DeviceBlock("dc", "DC 전원", (
    _f("v", F, "V", "전압", ".1f", "MEAS:ALL?", summary="mean"),
    _f("i", F, "A", "전류", ".4f", "MEAS:ALL?", summary="mean"),
    _f("p", F, "W", "파워(V×I)", ".1f", "계산", summary="mean"),
    _f("i_set", F, "A", "설정 전류", ".4f", "core"),
    _f("out", B, "", "출력 ON(명령 기준)", source="core", summary="any"),
    _f("set_w", F, "W", "목표", ".1f", "core"),
))

RFPULSE_BLOCK = DeviceBlock("rfp", "RF Pulse(CESAR 1310)", (
    _f("fwd_w", F, "W", "순방향", ".0f", "165", own=True, summary="mean"),
    _f("ref_w", F, "W", "반사", ".0f", "166", summary="max"),
    _f("out_on", B, "", "출력 ON", source="162 byte1 b5", own=True, summary="any"),
    _f("itl_open", B, "", "인터록 열림", source="162 byte2 b7", own=True, summary="any"),
    _f("status_raw", S, "", "상태 4바이트(hex)", source="162", own=True),
    _f("set_w", F, "W", "목표", ".0f", "core"),
    _f("freq_hz", I, "Hz", "펄스 주파수", "d", "core(시작 때 193 되읽기)"),
    _f("duty_pct", I, "%", "듀티", "d", "core(시작 때 196 되읽기)"),
))

IG_BLOCK = DeviceBlock("ig", "이온 게이지(934)", (
    _f("ion_torr", F, "Torr", "이온 압력", ".2E", "E #24", summary="min"),
    _f("a_torr", F, "Torr", "게이지 A", ".2E", "E #25", summary="mean"),
    _f("b_torr", F, "Torr", "게이지 B", ".2E", "E #26", summary="mean"),
    _f("filament", B, "", "필라멘트", source="E #2", summary="any"),
    _f("emission", B, "", "방출 OK", source="E #3", summary="any"),
    _f("degas", B, "", "디개스", source="E #1", summary="any"),
    _f("err_code", I, "", "필라멘트 에러 코드", "d", "E #5"),
))

BLOCKS: tuple = (PLC_BLOCK, HEATER_BLOCK, RF_BLOCK, MFC_BLOCK, DC_BLOCK, RFPULSE_BLOCK, IG_BLOCK)

# 장비 worker 가 아닌 core 가 채우는 행 앞부분
PROCESS_FIELDS: tuple = (
    FieldDef("proc.state", FType.STR, "", "공정 상태(process_states)"),
    FieldDef("proc.run_id", FType.STR, "", "공정 id"),
    FieldDef("proc.row", FType.INT, "", "레시피 행 번호"),
    FieldDef("proc.block", FType.STR, "", "지금 블록 번호/전체(블록 공정)"),
    FieldDef("proc.block_type", FType.STR, "", "지금 블록 종류(blocks.BLOCK_DEFS)"),
    FieldDef("proc.block_label", FType.STR, "", "지금 블록 라벨(없으면 블록 번호)"),
    FieldDef("proc.remain_s", FType.INT, "s", "지금 대기(DELAY·조건·압력 대기)의 남은 초"),
    FieldDef("proc.alarms", FType.STR, "", "활성 알람 규칙 id(쉼표)"),
)

# 블록 → 품질을 공유하는 worker
BLOCK_WORKER = {"plc": "plc", "heater": "plc", "rf": "plc", "mfc": "mfc", "dc": "dc", "rfp": "rfpulse", "ig": "ig"}


def columns() -> list:
    return row_columns(BLOCKS, PROCESS_FIELDS)
