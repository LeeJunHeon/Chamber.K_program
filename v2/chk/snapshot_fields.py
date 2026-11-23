"""챔버K 1초 스냅샷 필드 — 장비 블록 순서와 필드.

열 이름: <블록>.<필드>. 장비 품질은 <블록>.q / <블록>.age_s.
source 의 "plc:<이름>" 은 devices.PLC_ADDRS 이름이다(시험이 존재를 확인한다).
heater 와 rf 블록은 PLC 한 번 읽기에서 나오지만 화면·요약 단위로 따로 둔다(품질은 PLC 와 같다).
"""
from __future__ import annotations

from v2.common.snapshot import DeviceBlock, FieldDef, FType, row_columns
from v2.chk.drivers.ig import GAUGE_STATES, ION_STATES
from v2.chk.heater_cascade import STATE_TEXT, STATE_TEXT_TC2

F, I, B, S = FType.FLOAT, FType.INT, FType.BOOL, FType.STR


def _f(name, type_, unit, label, fmt="", source="", own=False, summary="last", detail=False, desc="",
       enum_labels=None, state="", enum_labels_by=None) -> FieldDef:
    return FieldDef(name=name, type=type_, unit=unit, label=label, fmt=fmt, source=source,
                    own_quality=own, summary=summary, detail=detail, desc=desc, enum_labels=enum_labels, state=state,
                    enum_labels_by=enum_labels_by)


def _bit(name, label, addr=None, detail=False, desc="") -> FieldDef:
    return _f(name, B, "", label, source=f"plc:{addr or name}", summary="any", detail=detail, desc=desc)

# 히터 제어 구조(HEATER_CTRL_MODE)·캐스케이드 상태(heater_cascade.STATE_TEXT) 화면용 이름
CTRL_MODE_LABELS = (("tc2", "TC2 단독 제어"), ("cascade", "2단 제어(TC2 + TC1 보정)"), ("tc1", "TC1 직접 제어"))
CASC_STATE_LABELS = tuple(STATE_TEXT.items())
# 제어 구조에 따라 바뀌는 상태 이름(5-18, v1 controller/heater_cascade.py STATE_TEXT_TC2) — ctrl_mode = tc2 이면 running·frozen 만
CASC_STATE_LABELS_BY = ("ctrl_mode", (("tc2", tuple(STATE_TEXT_TC2.items())),))

# 유지 모드(heater_engine.HeaterEngine.hold_label: off / arming / holding_<kind>, kind = dac·tc2) 화면용 이름
# 읽기 계획(5-20 ①) — drivers/readplan.py PLAN_LABELS 와 같다
READ_PLAN_LABELS = (("full", "매초 읽기"), ("alternate", "번갈아 읽기(2초마다)"))
VALVE_READ_LABELS = (("read", "읽음"), ("alternate", "번갈아 읽는 중(이번 초는 유량 차례)"),
                     ("yielded", "명령을 처리하느라 이번 읽기를 건너뜀"), ("no_reply", "응답이 없거나 형식이 다름"))
HOLD_LABELS = (("off", "꺼짐"), ("arming", "진입 대기"), ("holding_dac", "DAC 고정 유지"), ("holding_tc2", "TC2 추종 유지"))


PLC_BLOCK = DeviceBlock("plc", "PLC", (
    _bit("rotary", "Rotary"), _bit("rv", "RV"), _bit("fv", "FV"), _bit("mv", "Main 밸브"),
    _bit("vent", "Vent"), _bit("turbo", "Turbo"), _bit("door_up", "도어 올림"), _bit("door_down", "도어 내림"),
    _bit("ar_valve", "Ar 차단밸브"), _bit("o2_valve", "O₂ 차단밸브"), _bit("ms", "Main 셔터"),
    _bit("s1", "Gun1 셔터"), _bit("s2", "Gun2 셔터"), _bit("buzz_stop", "부저 정지"),
    _bit("mv_interlock", "MV 인터록"),
    _bit("air", "Air"), _bit("g1", "G1"), _bit("g2", "G2"), _bit("atm", "ATM"), _bit("water", "냉각수"),
    _bit("ion_remote_on", "이오나이저 ON"), _bit("ion_run", "이오나이저 구동"), _bit("ion_lamp", "이오나이저 점등"),
    _bit("ion_ot", "이오나이저 수명 초과"),
    _f("marker_ok", B, "", "재기동 마커 일치", source="core: D(PLC_SESSION_MARK_REG) 되읽기 비교", summary="any",
       detail=True),
    _f("wd_counter", I, "", "워치독 카운터", source="plc:heater_wd", detail=True, desc="마지막에 쓴 값"),
))

HEATER_BLOCK = DeviceBlock("heater", "히터", (
    _f("pv", F, "°C", "TC1", ".1f", "plc:heater_pv", summary="mean"),
    _f("pv2", F, "°C", "TC2", ".1f", "plc:heater_pv2", summary="mean"),
    _f("sv", F, "°C", "목표 온도", ".1f", "plc:heater_sv"),
    _f("cur_sv", F, "°C", "램프 적용 목표", ".1f", "plc:heater_cur_sv", detail=True,
       desc="D00016 × 0.1 °C(v1 과 같은 레지스터·배율). 래더가 운전(RUN=1) 중에만 갱신 — 정지 중에는 마지막 운전 때 값이 "
            "남는다(실측 10/20 825.1·10/29 513.3, v1 main.py 는 정지 중 D00012 를 보임). TC2 추종·캐스케이드 중에는 TC2 목표"),
    _f("sv_ramp", F, "°C", "램프 중간 목표", ".1f", "plc:heater_sv_ramp", detail=True),
    _f("sv2", F, "°C", "TC2 목표", ".1f", "plc:heater_sv2", desc="D00035 — 2단 제어·TC2 추종 중 PID 가 따라가는 TC2 목표(SV2)"),
    _f("sv_limit", F, "°C", "목표 상한", ".1f", "plc:heater_sv_limit", detail=True, desc="PLC D00013"),
    _f("ot2_limit", F, "°C", "TC2 과온 한계", ".1f", "plc:heater_ot2_limit", detail=True, desc="PLC D00034"),
    _f("sv2_max", F, "°C", "TC2 목표 상한", ".1f", "plc:heater_sv2_max", detail=True,
       desc="PLC D00036 — 0 이면 래더가 지원하지 않음"),
    _f("mv", I, "count", "출력(DAC)", "d", "plc:heater_mv", summary="mean"),
    _f("mv_limit", I, "count", "출력 상한(DAC)", "d", "plc:heater_mv_limit", detail=True),
    _f("current_est", F, "A", "추정 전류", ".1f", "계산: HEATER_CURRENT_*", summary="mean", desc="표시용 추정값"),
    _bit("run", "운전(RUN)", "heater_run"), _bit("itl", "인터록", "heater_itl"), _bit("fault", "이상", "heater_fault"),
    _bit("ot", "과온", "heater_ot"), _bit("tc_err", "TC 이상", "heater_tc_err"), _bit("wd_err", "워치독 이상", "heater_wd_err"),
    _bit("pid_run", "PID 동작", "heater_pid_run", detail=True), _bit("pv_sel", "TC2 제어 요청", "heater_pv_sel", detail=True),
    _bit("pv_sel_eff", "TC2 제어 중", "heater_pv_sel_eff"),
    _f("pid_err", I, "", "PID 오류 코드", "d", "plc:heater_pid_err", detail=True),
    _f("hold", S, "", "유지 모드", source="core", desc="heater_engine.hold_label — off/arming/holding_dac/holding_tc2",
       enum_labels=HOLD_LABELS),
    # 2단(캐스케이드) 제어 — 5-15(v1 7542db4·2d8146b 히터 CSV 캐스케이드 열·화면 값). 꺼져 있으면 값 없음(제어 상태는 idle)
    _f("ctrl_mode", S, "", "제어 구조", source="core", detail=True, enum_labels=CTRL_MODE_LABELS,
       desc="HEATER_CTRL_MODE — tc2(기본, TC2 단독) / cascade(2단) / tc1(롤백)"),
    _f("casc_state", S, "", "TC2 제어 상태", source="core", enum_labels=CASC_STATE_LABELS,
       enum_labels_by=CASC_STATE_LABELS_BY,
       desc="heater_cascade.HeaterCascade.state — v1 casc_state 열. tc2·cascade 두 모드 공통(상태 값은 같다). "
            "ctrl_mode = tc2 면 running 은 'TC2 제어(단독)', frozen 은 'TC2 제어(단독) · 셔터 열림(유지)'"
            "(heater_cascade.STATE_TEXT_TC2 — 묶음 enumLabelsBy)"),
    _f("r1", F, "°C", "TC1 기준값", ".1f", "core",
       desc="r1 — 감속 접근 램프(v1 r1_c). 홀드백은 cascade = TC1 기준, tc2 = TC2 기준. TC2 제어 중 화면 목표 칸"),
    _f("bias", F, "°C", "보정", ".2f", "core", desc="ff + 적분 보정(SV2 = 매핑값 + 보정, v1 bias_c). tc2 모드는 값 없음"),
    _f("bias_ff", F, "°C", "학습 보정", ".2f", "core", detail=True,
       desc="ff(r1) — 학습 매핑 곡선(v1 bias_ff_c). tc2 모드는 값 없음"),
    _f("bias_int", F, "°C", "적분 보정", ".2f", "core", detail=True,
       desc="b_int — 매 운전 0 에서(v1 bias_int_c). tc2 모드는 값 없음"),
    _f("r1_lag", F, "°C", "비교 기준", ".1f", "core", detail=True, desc="r1(t−HEATER_CASCADE_LAG_SEC)(v1 r1_lag_c)"),
    _f("tc1_ema", F, "°C", "TC1 평균", ".1f", "core", detail=True, desc="TC1 EMA 30 s(v1 tc1_ema_c)"),
    _f("sv2_map", F, "°C", "SV2 매핑값", ".1f", "core", detail=True, desc="map(r1) — HEATER_TC2_MAP(v1 sv2_map_c)"),
), desc="PLC 내장 PID")

RF_BLOCK = DeviceBlock("rf", "RF Power", (
    _f("fwd_w", F, "W", "순방향", ".1f", "plc:rf_fwd_adc × RF_FWD_FULL_SCALE_W/4000", summary="mean"),
    _f("ref_w", F, "W", "반사", ".1f", "plc:rf_ref_adc × RF_REF_FULL_SCALE_W/4000", summary="max"),
    _f("dac", I, "count", "DAC 설정값", "d", "plc:rf_dac", detail=True),
    _bit("dac_enable", "DAC 출력 허용", "rf_dac_enable", detail=True),
    _f("set_w", F, "W", "목표", ".1f", "core"),
), desc="PLC 아날로그 입출력")

MFC_BLOCK = DeviceBlock("mfc", "MFC", (
    _f("ar_sccm", F, "sccm", "Ar 유량", ".2f", "R60 ch1 × FS/100", own=True, summary="mean"),
    _f("o2_sccm", F, "sccm", "O₂ 유량", ".2f", "R60 ch2 × FS/100", own=True, summary="mean"),
    _f("pressure_mtorr", F, "mTorr", "챔버 압력", ".2f", "R5 (%FS → mTorr)", summary="mean"),
    _f("valve_pct", F, "%", "밸브 위치", ".1f", "R6 ('V' 접두어 확인)", own=True, summary="mean",
       desc="압력 조절 밸브 열림 — 0 닫힘 ~ 100 완전 열림. 기록용 읽기(설정 명령 중에는 양보해 앞 값·stale)"),
    _f("ar_set", F, "sccm", "Ar 설정", ".2f", "core"),
    _f("o2_set", F, "sccm", "O₂ 설정", ".2f", "core"),
    _f("sp_active", I, "", "사용 중 설정점", "d", "core(마지막 D 명령)",
       desc="0~5 — 0 은 밸브 직접 조작(열기·닫기·정지), 1 작업 압력, 2·3·5 단계 압력, 4 가스 안정화"),
    _f("sp_mtorr", F, "mTorr", "작업 압력 설정(SP1)", ".2f", "core", detail=True,
       desc="SP1 을 쓰고 되읽어 확인한 값(공정 중 수정·기록용 — 단계 압력 SP2~SP5 를 쓸 때는 바뀌지 않는다). "
            "지금 목표는 mfc.sp_sel_mtorr"),
    _f("sp_sel_mtorr", F, "mTorr", "압력 설정", ".2f", "core(선택된 설정점에 쓴 값)",
       desc="지금 선택된 설정점(사용 중 설정점 1~5)에 마지막으로 쓴 값 — SP1·SP2·SP3·SP5 는 되읽기 확인값, SP4 는 쓴 SP4_MTORR. "
            "선택이 없으면(밸브 직접 조작) 값 없음(5-18). 5-19: 이번 연결에서 쓴 값이 없으면 장비에서 읽은 값"),
    *(_f(f"sp{n}_dev_mtorr", F, "mTorr", f"SP{n} 장비 값", ".2f", f"R{10 if n == 5 else n}(parse_sp_reply)", detail=True,
         desc=f"장비에 저장된 SP{n} 값 — 연결 직후·선택할 때·쓰고 확인한 뒤·공정 밖 주기(MFC_SP_REFRESH_SEC)에 읽는다. "
              "못 읽었거나 형식이 다르면 값 없음(5-19)") for n in (1, 2, 3, 4, 5)),
    _f("read_plan", S, "", "읽기 계획", source="core(MFC_READ_PLAN)", detail=True, enum_labels=READ_PLAN_LABELS,
       desc="이 행을 읽은 계획 — 매초 읽기(압력·유량·밸브 위치 모두 매초) 또는 번갈아 읽기(압력 매초, 유량·밸브 위치는 "
            "2초마다 번갈아). auto 면 느린 읽기가 이어질 때 번갈아로, 다시 빨라지면 매초로 바뀐다(5-20 ①)"),
    _f("valve_read", S, "", "밸브 위치 읽기", source="core(R6)", detail=True, enum_labels=VALVE_READ_LABELS,
       desc="이번 초에 밸브 위치(R6)를 읽었는지, 읽지 않았다면 그 까닭(5-20 ①)"),
), desc="GPC3000")

DC_BLOCK = DeviceBlock("dc", "DC Power", (
    _f("v", F, "V", "전압", ".1f", "MEAS:ALL?", summary="mean"),
    _f("i", F, "A", "전류", ".4f", "MEAS:ALL?", summary="mean"),
    _f("p", F, "W", "파워", ".1f", "계산", summary="mean", desc="V×I 로 계산"),
    _f("i_set", F, "A", "설정 전류", ".4f", "core", detail=True),
    _f("out", B, "", "출력", source="core", summary="any", desc="명령 기준(장비 응답 아님)"),
    _f("set_w", F, "W", "목표", ".1f", "core"),
))

RFPULSE_BLOCK = DeviceBlock("rfp", "RF Pulse", (
    _f("fwd_w", F, "W", "순방향", ".0f", "165", own=True, summary="mean"),
    _f("ref_w", F, "W", "반사", ".0f", "166", summary="max"),
    _f("out_on", B, "", "출력", source="162 byte1 b5", own=True, summary="any"),
    _f("itl_open", B, "", "인터록 열림", source="162 byte2 b7", own=True, summary="any"),
    _f("status_raw", S, "", "상태 코드(hex)", source="162", own=True, detail=True, desc="상태 4바이트"),
    _f("set_w", F, "W", "목표", ".0f", "core"),
    _f("freq_hz", I, "Hz", "펄스 주파수", "d", "core(시작 때 193 되읽기)"),
    _f("duty_pct", I, "%", "듀티", "d", "core(시작 때 196 되읽기)"),
    _f("read_plan", S, "", "읽기 계획", source="core(RFPULSE_READ_PLAN)", detail=True, enum_labels=READ_PLAN_LABELS,
       desc="이 행을 읽은 계획 — 매초 읽기(상태·순방향·반사 모두 매초) 또는 번갈아 읽기(반사 매초, 순방향·상태는 2초마다 "
            "번갈아). auto 면 느린 읽기가 이어질 때 번갈아로, 다시 빨라지면 매초로 바뀐다(5-20 ①)"),
), desc="CESAR 1310")

IG_BLOCK = DeviceBlock("ig", "이온 게이지", (
    _f("ion_torr", F, "Torr", "이온 압력", ".2E", "E #24", summary="min", state="ion_state"),
    _f("a_torr", F, "Torr", "게이지 A", ".2E", "E #25", summary="mean", state="a_state"),
    _f("b_torr", F, "Torr", "게이지 B", ".2E", "E #26", summary="mean", state="b_state"),
    _f("filament", B, "", "필라멘트", source="E #2", summary="any"),
    _f("emission", B, "", "방출 정상", source="E #3", summary="any"),
    _f("degas", B, "", "디개스", source="E #1", summary="any"),
    _f("err_code", I, "", "필라멘트 오류 코드", "d", "E #5", detail=True),
    # 값이 빈 이유(5-14) — 값 열의 state 가 가리킨다. 열은 블록 끝에(앞 열 순서 그대로)
    _f("ion_state", S, "", "이온 압력 상태", source="E #24", detail=True, enum_labels=ION_STATES,
       desc="drivers.ig.pressure_from_pair — ok 가 아니면 이온 압력이 빈 칸"),
    _f("a_state", S, "", "게이지 A 상태", source="E #25", detail=True, enum_labels=GAUGE_STATES,
       desc="drivers.ig.pressure_from_pair — ok 가 아니면 게이지 A 가 빈 칸(below = 측정 하한 GAUGE_MIN_TORR 아래)"),
    _f("b_state", S, "", "게이지 B 상태", source="E #26", detail=True, enum_labels=GAUGE_STATES,
       desc="drivers.ig.pressure_from_pair — ok 가 아니면 게이지 B 가 빈 칸(below = 측정 하한 GAUGE_MIN_TORR 아래)"),
), desc="Terranova 934")

BLOCKS: tuple = (PLC_BLOCK, HEATER_BLOCK, RF_BLOCK, MFC_BLOCK, DC_BLOCK, RFPULSE_BLOCK, IG_BLOCK)

# 장비 worker 가 아닌 core 가 채우는 행 앞부분
PROCESS_FIELDS: tuple = (
    FieldDef("proc.state", FType.STR, "", "공정 상태", detail=True, desc="process_states 이름"),
    FieldDef("proc.run_id", FType.STR, "", "공정 id", detail=True),
    FieldDef("proc.row", FType.INT, "", "레시피 행 번호", detail=True),
    FieldDef("proc.block", FType.STR, "", "지금 블록", detail=True, desc="블록 번호/전체(블록 공정)"),
    FieldDef("proc.block_type", FType.STR, "", "지금 블록 종류", detail=True, desc="blocks.BLOCK_DEFS 의 type"),
    FieldDef("proc.block_label", FType.STR, "", "지금 블록 이름", detail=True, desc="블록 label, 없으면 블록 번호"),
    FieldDef("proc.remain_s", FType.INT, "s", "남은 시간", detail=True, desc="지금 대기(DELAY·조건·압력 대기)의 남은 초"),
    FieldDef("proc.alarms", FType.STR, "", "활성 알람", detail=True, desc="활성 알람 규칙 id(쉼표)"),
    FieldDef("proc.paused", FType.BOOL, "", "일시정지", summary="any",
             desc="PROCESS_PAUSE 로 멈춤(공정 상태 이름은 그대로 — 안전 규칙 구간이 바뀌지 않게, 5-15)"),
    FieldDef("proc.revision", FType.INT, "", "순서 판", detail=True, desc="지금 행의 판 번호(PROCESS_EDIT 마다 +1)"),
    FieldDef("proc.manual", FType.BOOL, "", "수동 조작", summary="any",
             desc="수동 조작 중(블록을 하나씩 실행하는 공정 — 5-20 ③). 공정이면 값 없음"),
)

# 블록 → 품질을 공유하는 worker
BLOCK_WORKER = {"plc": "plc", "heater": "plc", "rf": "plc", "mfc": "mfc", "dc": "dc", "rfp": "rfpulse", "ig": "ig"}


def columns() -> list:
    return row_columns(BLOCKS, PROCESS_FIELDS)
