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
    _bit("rotary", "Rotary",
        desc="Rotary 펌프 출력 코일의 상태다. 켜짐이면 PLC 가 펌프 출력을 내고 있으며, ALL STOP 때 꺼진다(M00000 → P00040)."), _bit("rv", "RV",
                                                                                                      desc="Roughing "
                                                                                                           "밸브 출력 "
                                                                                                           "코일의 상태다. "
                                                                                                           "켜짐이면 PLC "
                                                                                                           "가 이 밸브 "
                                                                                                           "출력을 내고 "
                                                                                                           "있으며, ALL "
                                                                                                           "STOP 때 "
                                                                                                           "꺼진다(M00001 "
                                                                                                           "→ "
                                                                                                           "P00041)."), _bit("fv", "FV",
                                                                                                                            desc="Foreline "
                                                                                                                                 "밸브 "
                                                                                                                                 "출력 "
                                                                                                                                 "코일의 "
                                                                                                                                 "상태다. "
                                                                                                                                 "켜짐이면 "
                                                                                                                                 "PLC "
                                                                                                                                 "가 "
                                                                                                                                 "이 "
                                                                                                                                 "밸브 "
                                                                                                                                 "출력을 "
                                                                                                                                 "내고 "
                                                                                                                                 "있으며, "
                                                                                                                                 "ALL "
                                                                                                                                 "STOP "
                                                                                                                                 "때 "
                                                                                                                                 "꺼진다(M00002 "
                                                                                                                                 "→ "
                                                                                                                                 "P00042)."), _bit("mv", "Main 밸브",
                                                                                                                                                  desc="Main "
                                                                                                                                                       "밸브 "
                                                                                                                                                       "출력 "
                                                                                                                                                       "코일의 "
                                                                                                                                                       "상태다. "
                                                                                                                                                       "공정·히터 "
                                                                                                                                                       "가스를 "
                                                                                                                                                       "시작하려면 "
                                                                                                                                                       "켜짐이어야 "
                                                                                                                                                       "하고, "
                                                                                                                                                       "공정 "
                                                                                                                                                       "중 "
                                                                                                                                                       "꺼지면 "
                                                                                                                                                       "바로 "
                                                                                                                                                       "공정을 "
                                                                                                                                                       "중단한다(M00003 "
                                                                                                                                                       "→ "
                                                                                                                                                       "P00043)."),
    _bit("vent", "Vent",
        desc="Vent 밸브 출력 코일의 상태다. 켜짐이면 PLC 가 이 밸브 출력을 내고 있으며, ALL STOP 때 꺼진다(M00004 → P00161)."), _bit("turbo", "Turbo",
                                                                                                      desc="Turbo 펌프 "
                                                                                                           "출력 코일의 "
                                                                                                           "상태다. "
                                                                                                           "켜짐이면 PLC "
                                                                                                           "가 펌프 출력을 "
                                                                                                           "내고 있으며, "
                                                                                                           "ALL STOP "
                                                                                                           "때 "
                                                                                                           "꺼진다(M00005 "
                                                                                                           "→ "
                                                                                                           "P00045)."), _bit("door_up", "도어 올림",
                                                                                                                            desc="도어 "
                                                                                                                                 "올림 "
                                                                                                                                 "출력 "
                                                                                                                                 "코일의 "
                                                                                                                                 "상태다. "
                                                                                                                                 "PLC "
                                                                                                                                 "버튼으로 "
                                                                                                                                 "켜고 "
                                                                                                                                 "끄며, "
                                                                                                                                 "ALL "
                                                                                                                                 "STOP "
                                                                                                                                 "때 "
                                                                                                                                 "꺼진다(M00006 "
                                                                                                                                 "→ "
                                                                                                                                 "P00162)."), _bit("door_down", "도어 내림",
                                                                                                                                                  desc="도어 "
                                                                                                                                                       "내림 "
                                                                                                                                                       "출력 "
                                                                                                                                                       "코일의 "
                                                                                                                                                       "상태다. "
                                                                                                                                                       "PLC "
                                                                                                                                                       "버튼으로 "
                                                                                                                                                       "켜고 "
                                                                                                                                                       "끄며, "
                                                                                                                                                       "ALL "
                                                                                                                                                       "STOP "
                                                                                                                                                       "때 "
                                                                                                                                                       "꺼진다(M00020 "
                                                                                                                                                       "→ "
                                                                                                                                                       "P00163)."),
    _bit("ar_valve", "Ar 차단밸브",
        desc="Ar 차단밸브 출력 코일의 상태다. 가스 도입 때 열고 정지 순서의 마지막 단계에서 닫는다(M00007 → P00047)."), _bit("o2_valve", "O₂ 차단밸브",
                                                                                          desc="O₂ 차단밸브 출력 코일의 상태다. "
                                                                                               "가스 도입 때 열고 정지 순서의 "
                                                                                               "마지막 단계에서 닫는다(M00008 "
                                                                                               "→ P00164)."), _bit("ms", "Main 셔터",
                                                                                                                  desc="Main "
                                                                                                                       "셔터 "
                                                                                                                       "출력 "
                                                                                                                       "코일의 "
                                                                                                                       "상태다. "
                                                                                                                       "메인 "
                                                                                                                       "블록이 "
                                                                                                                       "열고 "
                                                                                                                       "정지 "
                                                                                                                       "순서가 "
                                                                                                                       "닫으며, "
                                                                                                                       "열려 "
                                                                                                                       "있는 "
                                                                                                                       "동안 "
                                                                                                                       "2단·TC2 "
                                                                                                                       "단독 "
                                                                                                                       "히터 "
                                                                                                                       "제어는 "
                                                                                                                       "TC2 "
                                                                                                                       "목표를 "
                                                                                                                       "올리지 "
                                                                                                                       "않고 "
                                                                                                                       "내리기만 "
                                                                                                                       "한다(M00009 "
                                                                                                                       "→ "
                                                                                                                       "P00049)."),
    _bit("s1", "Gun1 셔터",
        desc="Gun 1 셔터 출력 코일의 상태다. 셔터 블록(기본 템플릿의 건 셔터 열기)이 열고 정지 순서가 닫는다(M00010 → P0004A)."), _bit("s2", "Gun2 셔터",
                                                                                                  desc="Gun 2 셔터 출력 "
                                                                                                       "코일의 상태다. 셔터 "
                                                                                                       "블록(기본 템플릿의 건 "
                                                                                                       "셔터 열기)이 열고 "
                                                                                                       "정지 순서가 "
                                                                                                       "닫는다(M00011 → "
                                                                                                       "P0004B)."), _bit("buzz_stop", "부저 정지",
                                                                                                                        desc="부저 "
                                                                                                                             "정지 "
                                                                                                                             "출력 "
                                                                                                                             "코일의 "
                                                                                                                             "상태다. "
                                                                                                                             "부저 "
                                                                                                                             "정지 "
                                                                                                                             "명령이 "
                                                                                                                             "켜짐으로 "
                                                                                                                             "쓰며, "
                                                                                                                             "ALL "
                                                                                                                             "STOP "
                                                                                                                             "때 "
                                                                                                                             "꺼진다(M00021 "
                                                                                                                             "→ "
                                                                                                                             "P00165)."),
    _bit("mv_interlock", "MV 인터록",
        desc="PLC 래더가 내는 MV 인터록 상태다. 공정 시작에는 켜짐이어야 하고, 공정 중 꺼짐이 'MV 인터록 해제 지속 중단' 시간 이상 이어지면 공정을 중단한다(M00032)."),
    _bit("air", "Air",
        desc="Air 센서 입력의 상태다. 프로그램은 표시·기록에만 쓴다(P00000 → M00100)."), _bit("g1", "G1",
                                                                        desc="G1(Gauge1) 센서 입력의 상태다. 프로그램은 표시·기록에만 "
                                                                             "쓴다(P00001 → M00101)."), _bit("g2", "G2",
                                                                                                          desc="G2(Gauge2) "
                                                                                                               "센서 "
                                                                                                               "입력의 "
                                                                                                               "상태다. "
                                                                                                               "프로그램은 "
                                                                                                               "표시·기록에만 "
                                                                                                               "쓴다(P00002 "
                                                                                                               "→ "
                                                                                                               "M00102)."), _bit("atm", "ATM",
                                                                                                                                desc="ATM(대기압) "
                                                                                                                                     "센서 "
                                                                                                                                     "입력의 "
                                                                                                                                     "상태다. "
                                                                                                                                     "켜짐이거나 "
                                                                                                                                     "값을 "
                                                                                                                                     "모르면 "
                                                                                                                                     "이온 "
                                                                                                                                     "게이지 "
                                                                                                                                     "필라멘트 "
                                                                                                                                     "켜기 "
                                                                                                                                     "명령을 "
                                                                                                                                     "거부한다(P00003 "
                                                                                                                                     "→ "
                                                                                                                                     "M00103)."), _bit("water", "냉각수",
                                                                                                                                                      desc="냉각수 "
                                                                                                                                                           "센서 "
                                                                                                                                                           "입력의 "
                                                                                                                                                           "상태다. "
                                                                                                                                                           "프로그램은 "
                                                                                                                                                           "표시·기록에만 "
                                                                                                                                                           "쓴다(P00004 "
                                                                                                                                                           "→ "
                                                                                                                                                           "M00104)."),
    _bit("ion_remote_on", "이오나이저 ON",
        desc="이오나이저 Remote On 출력 코일의 상태다. PLC 버튼 '이오나이저'로 켜고 끄며, ALL STOP 때 꺼진다(M00050 → P00160)."), _bit("ion_run", "이오나이저 구동",
                                                                                                         desc="이오나이저 "
                                                                                                              "구동 입력 "
                                                                                                              "신호의 "
                                                                                                              "상태다(P0000E "
                                                                                                              "→ "
                                                                                                              "M00114)."), _bit("ion_lamp", "이오나이저 점등",
                                                                                                                               desc="이오나이저 "
                                                                                                                                    "점등 "
                                                                                                                                    "입력 "
                                                                                                                                    "신호의 "
                                                                                                                                    "상태다(P0000F "
                                                                                                                                    "→ "
                                                                                                                                    "M00115)."),
    _bit("ion_ot", "이오나이저 수명 초과",
        desc="이오나이저 램프 수명 초과 입력 신호의 상태다(P00010 → M00116)."),
    _f("marker_ok", B, "", "재기동 마커 일치", source="core: D(PLC_SESSION_MARK_REG) 되읽기 비교", summary="any",
       detail=True,
        desc="연결 때 PLC 에 써 둔 재기동 마커 값이 그대로 읽히는지를 보인다(값이 없으면 마커를 쓰지 않은 상태). 두 번 연속 다르면 PLC 재기동으로 보고 히터 한계값과 마커를 다시 "
             "쓰며, 공정 중이면 공정을 중단한다(PLC_SESSION_MARK_REG)."),
    _f("wd_counter", I, "", "워치독 카운터", source="plc:heater_wd", detail=True, desc="마지막에 쓴 값"),
))

HEATER_BLOCK = DeviceBlock("heater", "히터", (
    _f("pv", F, "°C", "TC1", ".1f", "plc:heater_pv", summary="mean",
        desc="TC1 온도다. 센서 이상이면 값이 비며, 목표 온도 숫자는 제어 구조와 상관없이 TC1 기준이다(D00010)."),
    _f("pv2", F, "°C", "TC2", ".1f", "plc:heater_pv2", summary="mean",
        desc="TC2 온도다. 센서 이상이면 값이 비며, 2단·TC2 단독 제어에서는 PLC PID 가 이 값을 따라 제어한다(D00011)."),
    _f("sv", F, "°C", "목표 온도", ".1f", "plc:heater_sv",
        desc="PLC 에 쓴 히터 목표 온도다. 2단·TC2 단독 제어에서는 프로그램이 매 주기 TC1 기준값을 쓰고, TC1 직접 제어에서는 감속 접근으로 올라가는 목표를 쓴다(D00012)."),
    _f("cur_sv", F, "°C", "램프 적용 목표", ".1f", "plc:heater_cur_sv", detail=True,
       desc="래더가 운전(RUN=1) 중에만 갱신하는 현재 목표 온도이다. 정지 중에는 마지막 운전 때 값이 남고(실측 10/20 825.1·10/29 513.3, v1 main.py 는 정지 "
            "중 D00012 를 보임), TC2 추종·캐스케이드 중에는 TC2 목표이다(D00016 × 0.1 °C, v1 과 같은 레지스터·배율)"),
    _f("sv_ramp", F, "°C", "램프 중간 목표", ".1f", "plc:heater_sv_ramp", detail=True,
        desc="PLC 래더 램프가 지금 가 있는 중간 목표다. TC1 직접 제어의 램프 완료 판정에 쓰며, 2단·TC2 단독 제어에서는 TC2 목표 쪽 값이라 판정에 쓰지 않는다(D00019)."),
    _f("sv2", F, "°C", "TC2 목표", ".1f", "plc:heater_sv2", desc="2단 제어·TC2 추종 중 PID 가 따라가는 TC2 목표(SV2)이다(D00035)"),
    _f("sv_limit", F, "°C", "목표 상한", ".1f", "plc:heater_sv_limit", detail=True, desc="PLC D00013"),
    _f("ot2_limit", F, "°C", "TC2 과온 한계", ".1f", "plc:heater_ot2_limit", detail=True, desc="PLC D00034"),
    _f("sv2_max", F, "°C", "TC2 목표 상한", ".1f", "plc:heater_sv2_max", detail=True,
       desc="PLC 가 알려 주는 TC2 목표 상한이다. 0 이면 래더가 지원하지 않는다(D00036)"),
    _f("mv", I, "count", "출력(DAC)", "d", "plc:heater_mv", summary="mean",
        desc="PLC 래더가 히터에 내는 출력 DAC 값이다. 히터가 켜져 있는데 '출력 0% DAC'보다 낮은 상태가 이어지면 출력 죽음으로 본다(D00041)."),
    _f("mv_limit", I, "count", "출력 상한(DAC)", "d", "plc:heater_mv_limit", detail=True,
        desc="PLC 에 걸린 히터 출력 DAC 운전 상한이다. 평소에는 설정한 운전 상한이고, DAC 포화 클램프나 DAC 고정 유지 모드가 잠시 낮출 수 있다(D00018)."),
    _f("current_est", F, "A", "추정 전류", ".1f", "계산: HEATER_CURRENT_*", summary="mean", desc="표시용 추정값"),
    _bit("run", "운전(RUN)", "heater_run",
        desc="히터 운전 요구 비트다. 켜짐이면 래더가 히터를 운전하며, 히터 레시피 중 꺼짐이 이어지면 외부에서 꺼진 것으로 보고 레시피를 중단한다(M00040)."), _bit("itl", "인터록", "heater_itl",
                                                                                                          desc="PLC "
                                                                                                               "래더가 "
                                                                                                               "내는 "
                                                                                                               "히터 "
                                                                                                               "인터록 "
                                                                                                               "정상 "
                                                                                                               "비트다. "
                                                                                                               "꺼짐이면 "
                                                                                                               "히터 "
                                                                                                               "켜기와 "
                                                                                                               "히터 "
                                                                                                               "레시피 "
                                                                                                               "시작을 "
                                                                                                               "거부한다(M00041)."), _bit("fault", "이상", "heater_fault",
                                                                                                                                     desc="히터 "
                                                                                                                                          "이상 "
                                                                                                                                          "종합 "
                                                                                                                                          "비트(과온·TC "
                                                                                                                                          "이상·워치독)다. "
                                                                                                                                          "켜지면 "
                                                                                                                                          "히터를 "
                                                                                                                                          "끄고 "
                                                                                                                                          "히터를 "
                                                                                                                                          "쓰는 "
                                                                                                                                          "공정과 "
                                                                                                                                          "히터 "
                                                                                                                                          "레시피를 "
                                                                                                                                          "중단하며, "
                                                                                                                                          "히터 "
                                                                                                                                          "운전이 "
                                                                                                                                          "꺼진 "
                                                                                                                                          "뒤 "
                                                                                                                                          "이상 "
                                                                                                                                          "리셋으로 "
                                                                                                                                          "푼다(M00042)."),
    _bit("ot", "과온", "heater_ot",
        desc="과온 트립 비트다. 래치되어 히터 이상의 원인으로 기록되며, 이상 리셋으로 푼다(M00044)."), _bit("tc_err", "TC 이상", "heater_tc_err",
                                                                           desc="온도센서 이상 비트다. 히터 레시피 중 TC 이상이 'TC 이상 "
                                                                                "지속 시 레시피 중단' 시간 이어지면 레시피를 "
                                                                                "중단한다(M00045)."), _bit("wd_err", "워치독 이상", "heater_wd_err",
                                                                                                      desc="워치독 타임아웃 "
                                                                                                           "비트다. PC "
                                                                                                           "하트비트가 10 "
                                                                                                           "초 동안 바뀌지 "
                                                                                                           "않아 래더가 "
                                                                                                           "히터를 끈 경우 "
                                                                                                           "켜진다(M00046)."),
    _bit("pid_run", "PID 동작", "heater_pid_run", detail=True,
        desc="PLC PID 가 동작 중인지 보이는 비트다(M00049)."), _bit("pv_sel", "TC2 제어 요청", "heater_pv_sel", detail=True,
                                                       desc="프로그램이 래더에 TC2 로 제어하라고 요청하는 비트다(M0004A)."),
    _bit("pv_sel_eff", "TC2 제어 중", "heater_pv_sel_eff",
        desc="래더가 실제로 TC2 로 PID 제어 중인지 보이는 비트다. TC2 제어 요청, TC2 정상, 운전 중, TC2 목표 > 0 이 모두 맞아야 켜진다(M0004B)."),
    _f("pid_err", I, "", "PID 오류 코드", "d", "plc:heater_pid_err", detail=True,
        desc="PLC PID 가 보고한 오류 코드다. 프로그램은 기록용으로 읽기만 한다(D00017)."),
    _f("hold", S, "", "유지 모드", source="core", desc="히터 유지 모드 상태(꺼짐·진입 대기·DAC 고정 유지·TC2 추종 "
                                                   "유지)이다(heater_engine.hold_label)",
       enum_labels=HOLD_LABELS),
    # 2단(캐스케이드) 제어 — 5-15(v1 7542db4·2d8146b 히터 CSV 캐스케이드 열·화면 값). 꺼져 있으면 값 없음(제어 상태는 idle)
    _f("ctrl_mode", S, "", "제어 구조", source="core", detail=True, enum_labels=CTRL_MODE_LABELS,
       desc="히터 제어 구조이다. tc2 는 TC2 단독(기본), cascade 는 2단, tc1 은 TC1 직접 제어(롤백)이다(HEATER_CTRL_MODE)"),
    _f("casc_state", S, "", "TC2 제어 상태", source="core", enum_labels=CASC_STATE_LABELS,
       enum_labels_by=CASC_STATE_LABELS_BY,
       desc="TC2 제어 상태로 tc2·cascade 두 모드에 함께 쓴다(상태 값은 같다). 제어 구조가 tc2 면 running 은 'TC2 제어(단독)', frozen 은 'TC2 "
            "제어(단독) · 셔터 열림(유지)'로 보인다(v1 casc_state 열, heater_cascade.HeaterCascade.state·STATE_TEXT_TC2, 묶음 "
            "enumLabelsBy)"),
    _f("r1", F, "°C", "TC1 기준값", ".1f", "core",
       desc="감속 접근 램프 값이다. 홀드백은 cascade 면 TC1 기준, tc2 면 TC2 기준이며, TC2 제어 중 화면의 목표 칸에 쓴다(v1 r1_c)"),
    _f("bias", F, "°C", "보정", ".2f", "core", desc="ff + 적분 보정(SV2 = 매핑값 + 보정, v1 bias_c). tc2 모드는 값 없음"),
    _f("bias_ff", F, "°C", "학습 보정", ".2f", "core", detail=True,
       desc="학습 매핑 곡선 ff(r1) 값이다. tc2 모드에서는 값이 없다(v1 bias_ff_c)"),
    _f("bias_int", F, "°C", "적분 보정", ".2f", "core", detail=True,
       desc="적분 보정 b_int 로 운전마다 0 에서 시작한다. tc2 모드에서는 값이 없다(v1 bias_int_c)"),
    _f("r1_lag", F, "°C", "비교 기준", ".1f", "core", detail=True, desc="r1(t−HEATER_CASCADE_LAG_SEC)(v1 r1_lag_c)"),
    _f("tc1_ema", F, "°C", "TC1 평균", ".1f", "core", detail=True, desc="TC1 EMA 30 s(v1 tc1_ema_c)"),
    _f("sv2_map", F, "°C", "SV2 매핑값", ".1f", "core", detail=True, desc="매핑 map(r1) 값이다(HEATER_TC2_MAP, v1 "
                                                                       "sv2_map_c)"),
), desc="PLC 내장 PID")

RF_BLOCK = DeviceBlock("rf", "RF Power", (
    _f("fwd_w", F, "W", "순방향", ".1f", "plc:rf_fwd_adc × RF_FWD_FULL_SCALE_W/4000", summary="mean",
        desc="RF 순방향 파워다. PLC 가 읽은 ADC 값을 'RF 순방향 ADC 풀스케일' 기준 W 로 바꾼 값이며, RF 제어가 목표와 비교한다(D00000)."),
    _f("ref_w", F, "W", "반사", ".1f", "plc:rf_ref_adc × RF_REF_FULL_SCALE_W/4000", summary="max",
        desc="RF 반사 파워다. 'RF 반사파 대기 기준' 이상이면 램프를 멈추고 기다리며, 그 상태가 'RF 반사파 대기 허용' 시간 이어지면 공정을 중단한다(D00001)."),
    _f("dac", I, "count", "DAC 설정값", "d", "plc:rf_dac", detail=True,
        desc="PLC 가 RF 전원에 내는 DAC 설정값(0~4000)이다. 램프업 중 이 값이 'RF 무출력 판정 DAC' 이상인데 순방향이 거의 없으면 무출력으로 본다(D00040)."),
    _bit("dac_enable", "DAC 출력 허용", "rf_dac_enable", detail=True,
        desc="RF DAC 출력 허용 비트다. DAC 를 0 보다 크게 쓰기 전에 켜고, RF 를 끌 때 끈다(M00200)."),
    _f("set_w", F, "W", "목표", ".1f", "core",
        desc="RF 제어의 지금 목표 파워다. RF 를 시작하거나 목표를 바꾸면 그 값이 되고, RF 를 끄면 0 이다."),
), desc="PLC 아날로그 입출력")

MFC_BLOCK = DeviceBlock("mfc", "MFC", (
    _f("ar_sccm", F, "sccm", "Ar 유량", ".2f", "R60 ch1 × FS/100", own=True, summary="mean",
        desc="MFC 가 읽은 Ar 채널 실제 유량이다(%FS 를 풀스케일로 환산). 번갈아 읽기 중에는 2초마다 갱신된다(R60)."),
    _f("o2_sccm", F, "sccm", "O₂ 유량", ".2f", "R60 ch2 × FS/100", own=True, summary="mean",
        desc="MFC 가 읽은 O₂ 채널 실제 유량이다(%FS 를 풀스케일로 환산). 번갈아 읽기 중에는 2초마다 갱신된다(R60)."),
    _f("pressure_mtorr", F, "mTorr", "챔버 압력", ".2f", "R5 (%FS → mTorr)", summary="mean",
        desc="MFC 압력 센서로 읽은 챔버 압력이다. 압력 도달 대기와 공정 중 압력 이탈 경고가 이 값을 쓰며, 읽기 계획과 상관없이 매초 읽는다(R5)."),
    _f("valve_pct", F, "%", "밸브 위치", ".1f", "R6 ('V' 접두어 확인)", own=True, summary="mean",
       desc="압력 조절 밸브가 열린 정도로 0 은 닫힘, 100 은 완전 열림이다. 기록용 읽기라 설정 명령 중에는 양보해 앞 값이 남고 stale 로 보인다"),
    _f("ar_set", F, "sccm", "Ar 설정", ".2f", "core",
        desc="Ar 채널에 마지막으로 쓰고 되읽어 확인한 유량 설정값이다. 유량을 꺼도 설정값은 그대로 남고, FLOW ON 뒤 유량 안정화 판정은 이 값을 기준으로 한다."),
    _f("o2_set", F, "sccm", "O₂ 설정", ".2f", "core",
        desc="O₂ 채널에 마지막으로 쓰고 되읽어 확인한 유량 설정값이다. 유량을 꺼도 설정값은 그대로 남고, FLOW ON 뒤 유량 안정화 판정은 이 값을 기준으로 한다."),
    _f("sp_active", I, "", "사용 중 설정점", "d", "core(마지막 D 명령)",
       desc="지금 쓰는 설정점 번호 0~5 이다. 0 은 밸브 직접 조작(열기·닫기·정지), 1 은 작업 압력, 2·3·5 는 단계 압력, 4 는 가스 안정화이다"),
    _f("sp_mtorr", F, "mTorr", "작업 압력 설정(SP1)", ".2f", "core", detail=True,
       desc="SP1 을 쓰고 되읽어 확인한 값이다(공정 중 수정·기록용). 단계 압력 SP2~SP5 를 쓸 때는 바뀌지 않으며, 지금 목표는 압력 설정 칸에 있다(mfc.sp_sel_mtorr)"),
    _f("sp_sel_mtorr", F, "mTorr", "압력 설정", ".2f", "core(선택된 설정점에 쓴 값)",
       desc="지금 선택된 설정점(사용 중 설정점 1~5)에 마지막으로 쓴 값이다. SP1·SP2·SP3·SP5 는 되읽기 확인값, SP4 는 쓴 값이며, 이번 연결에서 쓴 값이 없으면 장비에서 "
            "읽은 값이다. 선택이 없으면(밸브 직접 조작) 값이 없다(5-18·5-19, SP4_MTORR)"),
    *(_f(f"sp{n}_dev_mtorr", F, "mTorr", f"SP{n} 장비 값", ".2f", f"R{10 if n == 5 else n}(parse_sp_reply)", detail=True,
         desc=f"장비에 저장된 SP{n} 값이다. 연결 직후·선택할 때·쓰고 확인한 뒤·공정 밖 주기에 읽으며, 읽지 못했거나 형식이 "
              "다르면 값이 없다(MFC_SP_REFRESH_SEC, 5-19)") for n in (1, 2, 3, 4, 5)),
    _f("read_plan", S, "", "읽기 계획", source="core(MFC_READ_PLAN)", detail=True, enum_labels=READ_PLAN_LABELS,
       desc="이 행을 읽은 계획이다. 매초 읽기는 압력·유량·밸브 위치를 모두 매초 읽고, 번갈아 읽기는 압력만 매초·유량과 밸브 위치는 2초마다 번갈아 읽는다. auto 면 느린 읽기가 이어질 "
            "때 번갈아로, 다시 빨라지면 매초로 바뀐다(5-20 ①)"),
    _f("valve_read", S, "", "밸브 위치 읽기", source="core(R6)", detail=True, enum_labels=VALVE_READ_LABELS,
       desc="이번 초에 밸브 위치(R6)를 읽었는지, 읽지 않았다면 그 까닭(5-20 ①)"),
), desc="GPC3000")

DC_BLOCK = DeviceBlock("dc", "DC Power", (
    _f("v", F, "V", "전압", ".1f", "MEAS:ALL?", summary="mean",
        desc="DC 전원이 측정한 출력 전압이다. 정전류 제어가 이 값으로 목표 파워에 필요한 전류 변화를 계산한다(MEAS:ALL?)."),
    _f("i", F, "A", "전류", ".4f", "MEAS:ALL?", summary="mean",
        desc="DC 전원이 측정한 출력 전류다. 파워 유지 중 이 값이 'DC 저전류 기준' 이하로 'DC 저전류 지속 시간' 이어지면 공정을 중단한다(MEAS:ALL?)."),
    _f("p", F, "W", "파워", ".1f", "계산", summary="mean", desc="V×I 로 계산"),
    _f("i_set", F, "A", "설정 전류", ".4f", "core", detail=True,
        desc="프로그램이 DC 전원에 마지막으로 보낸 전류 설정값이다(장비 응답이 아니라 명령 기준). 램프업 중 이 값이 'DC 무출력 판정 설정 전류' 이상인데 파워가 거의 없으면 무출력으로 "
             "본다."),
    _f("out", B, "", "출력", source="core", summary="any", desc="명령 기준(장비 응답 아님)"),
    _f("set_w", F, "W", "목표", ".1f", "core",
        desc="DC 정전류 제어의 지금 목표 파워다. DC 제어를 멈추면 값이 빈다."),
))

RFPULSE_BLOCK = DeviceBlock("rfp", "RF Pulse", (
    _f("fwd_w", F, "W", "순방향", ".0f", "165", own=True, summary="mean",
        desc="RF Pulse 가 보고한 순방향 파워다. 출력 중 목표와의 차이가 '순방향 허용 오차' 이상으로 이어지면 공정을 중단하며, 번갈아 읽기 중에는 2초마다 갱신된다(명령 165)."),
    _f("ref_w", F, "W", "반사", ".0f", "166", summary="max",
        desc="RF Pulse 가 보고한 반사 파워다. '반사파 상한' 이상이 '반사파 초과 지속 시간' 이어지면 RF Pulse 를 끄고 공정을 중단한다(명령 166)."),
    _f("out_on", B, "", "출력", source="162 byte1 b5", own=True, summary="any",
        desc="RF Pulse 상태에서 읽은 RF 출력 켜짐 비트다. 번갈아 읽기 중에는 2초마다 갱신된다(명령 162)."),
    _f("itl_open", B, "", "인터록 열림", source="162 byte2 b7", own=True, summary="any",
        desc="RF Pulse 상태에서 읽은 인터록 열림 비트다. RF Pulse 를 쓰는 공정 중 켜지면 바로 RF Pulse 를 끄고 공정을 중단한다(명령 162)."),
    _f("status_raw", S, "", "상태 코드(hex)", source="162", own=True, detail=True, desc="상태 4바이트"),
    _f("set_w", F, "W", "목표", ".0f", "core",
        desc="RF Pulse 의 지금 목표 파워다. RF ON 이 받아들여지면 그 값이 되고, 꺼짐을 확인하면 0 이다."),
    _f("freq_hz", I, "Hz", "펄스 주파수", "d", "core(시작 때 193 되읽기)",
        desc="RF Pulse 시작 때(공정 중 수정 때 포함) 장비에서 되읽은 펄스 주파수다. 요청한 값과 다르면 설정에 따라 공정을 중단하거나 경고만 남긴다(명령 193)."),
    _f("duty_pct", I, "%", "듀티", "d", "core(시작 때 196 되읽기)",
        desc="RF Pulse 시작 때(공정 중 수정 때 포함) 장비에서 되읽은 펄스 듀티다. 요청한 값과 다르면 설정에 따라 공정을 중단하거나 경고만 남긴다(명령 196)."),
    _f("read_plan", S, "", "읽기 계획", source="core(RFPULSE_READ_PLAN)", detail=True, enum_labels=READ_PLAN_LABELS,
       desc="이 행을 읽은 계획이다. 매초 읽기는 상태·순방향·반사를 모두 매초 읽고, 번갈아 읽기는 반사만 매초·순방향과 상태는 2초마다 번갈아 읽는다. auto 면 느린 읽기가 이어질 때 "
            "번갈아로, 다시 빨라지면 매초로 바뀐다(5-20 ①)"),
), desc="CESAR 1310")

IG_BLOCK = DeviceBlock("ig", "이온 게이지", (
    _f("ion_torr", F, "Torr", "이온 압력", ".2E", "E #24", summary="min", state="ion_state",
        desc="이온 게이지 압력이다. 필라멘트가 꺼져 있거나 측정 준비 중이면 값이 비고, 베이스 압력 대기는 이 값이 목표 이하로 이어지는지 본다(E #24)."),
    _f("a_torr", F, "Torr", "게이지 A", ".2E", "E #25", summary="mean", state="a_state",
        desc="이온 게이지 컨트롤러에 물린 게이지 A 의 압력이다. 측정 하한(1.0E-3 Torr) 아래·미연결·영점 미조정이면 값이 비고, 그 까닭은 게이지 A 상태에 보인다(E #25)."),
    _f("b_torr", F, "Torr", "게이지 B", ".2E", "E #26", summary="mean", state="b_state",
        desc="이온 게이지 컨트롤러에 물린 게이지 B 의 압력이다. 측정 하한(1.0E-3 Torr) 아래·미연결·영점 미조정이면 값이 비고, 그 까닭은 게이지 B 상태에 보인다(E #26)."),
    _f("filament", B, "", "필라멘트", source="E #2", summary="any",
        desc="이온 게이지 필라멘트가 켜짐인지 보인다. 베이스 압력 대기 중 꺼지면 '대기 중 필라멘트 재점등 횟수'까지 다시 켠다(E #2)."),
    _f("emission", B, "", "방출 정상", source="E #3", summary="any",
        desc="이온 게이지 방출이 정상인지 보인다. 필라멘트 켜기는 필라멘트와 방출이 모두 켜짐으로 보여야 성공으로 본다(E #3)."),
    _f("degas", B, "", "디개스", source="E #1", summary="any",
        desc="이온 게이지 디개스 동작 여부다. 프로그램은 디개스 명령을 보내지 않고 상태만 기록한다(E #1)."),
    _f("err_code", I, "", "필라멘트 오류 코드", "d", "E #5", detail=True,
        desc="이온 게이지가 보고한 필라멘트 오류 코드다. 필라멘트 켜기·끄기가 기한 안에 확인되지 않으면 이 코드의 뜻을 함께 기록한다(E #5)."),
    # 값이 빈 이유(5-14) — 값 열의 state 가 가리킨다. 열은 블록 끝에(앞 열 순서 그대로)
    _f("ion_state", S, "", "이온 압력 상태", source="E #24", detail=True, enum_labels=ION_STATES,
       desc="이온 압력 값의 상태이다. ok 가 아니면 이온 압력 칸이 비어 있다(drivers.ig.pressure_from_pair)"),
    _f("a_state", S, "", "게이지 A 상태", source="E #25", detail=True, enum_labels=GAUGE_STATES,
       desc="게이지 A 값의 상태이다. ok 가 아니면 게이지 A 칸이 비어 있고, below 는 측정 하한 아래라는 뜻이다(drivers.ig.pressure_from_pair, "
            "GAUGE_MIN_TORR)"),
    _f("b_state", S, "", "게이지 B 상태", source="E #26", detail=True, enum_labels=GAUGE_STATES,
       desc="게이지 B 값의 상태이다. ok 가 아니면 게이지 B 칸이 비어 있고, below 는 측정 하한 아래라는 뜻이다(drivers.ig.pressure_from_pair, "
            "GAUGE_MIN_TORR)"),
), desc="Terranova 934")

BLOCKS: tuple = (PLC_BLOCK, HEATER_BLOCK, RF_BLOCK, MFC_BLOCK, DC_BLOCK, RFPULSE_BLOCK, IG_BLOCK)

# 장비 worker 가 아닌 core 가 채우는 행 앞부분
PROCESS_FIELDS: tuple = (
    FieldDef("proc.state", FType.STR, "", "공정 상태", detail=True, desc="process_states 이름"),
    FieldDef("proc.run_id", FType.STR, "", "공정 id", detail=True,
        desc="공정을 시작할 때(레시피면 공정 행마다) 새로 만드는 공정 id 다. 공정 기록과 웹 요약을 이 id 로 묶으며, 다음 공정이 시작될 때까지 마지막 id 가 남는다."),
    FieldDef("proc.row", FType.INT, "", "레시피 행 번호", detail=True,
        desc="적재한 레시피를 돌리는 동안 지금 진행 중인 행 번호(1부터, 딜레이 행 포함)다. 레시피 없이 시작한 수동 공정이면 값이 없다."),
    FieldDef("proc.block", FType.STR, "", "지금 블록", detail=True, desc="블록 번호/전체(블록 공정)"),
    FieldDef("proc.block_type", FType.STR, "", "지금 블록 종류", detail=True, desc="blocks.BLOCK_DEFS 의 type"),
    FieldDef("proc.block_label", FType.STR, "", "지금 블록 이름", detail=True, desc="블록 label, 없으면 블록 번호"),
    FieldDef("proc.remain_s", FType.INT, "s", "남은 시간", detail=True, desc="지금 대기(DELAY·조건·압력 대기)의 남은 초"),
    FieldDef("proc.alarms", FType.STR, "", "활성 알람", detail=True, desc="활성 알람 규칙 id(쉼표)"),
    FieldDef("proc.paused", FType.BOOL, "", "일시정지", summary="any",
             desc="일시정지로 멈춘 상태이다. 안전 규칙 구간이 바뀌지 않게 공정 상태 이름은 그대로 둔다(PROCESS_PAUSE, 5-15)"),
    FieldDef("proc.revision", FType.INT, "", "순서 판", detail=True, desc="지금 행의 판 번호(PROCESS_EDIT 마다 +1)"),
    FieldDef("proc.manual", FType.BOOL, "", "수동 조작", summary="any",
             desc="수동 조작 중이면 참이다(블록을 하나씩 실행하는 공정, 5-20 ③). 공정이면 값이 없다"),
)

# 블록 → 품질을 공유하는 worker
BLOCK_WORKER = {"plc": "plc", "heater": "plc", "rf": "plc", "mfc": "mfc", "dc": "dc", "rfp": "rfpulse", "ig": "ig"}


def columns() -> list:
    return row_columns(BLOCKS, PROCESS_FIELDS)
