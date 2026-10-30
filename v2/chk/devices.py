"""챔버K 장비 사실 표 — 매뉴얼 근거(파일·PDF 쪽) + v1 코드와 다른 곳 + 1초 예산 + PC 가 죽었을 때 출력 끄기.

매뉴얼 폴더: C:\\Users\\wnsgj\\Desktop\\Sputter\\장비메뉴얼 (읽기만, 저장소로 복사하지 않음)
쪽 표기: "p.N" = PDF 쪽, "/라벨" = 인쇄 쪽 번호(있을 때). EX 매뉴얼은 인쇄 쪽 = PDF + 2.
confirmed=False 는 매뉴얼로 확정 못 한 값(한글 변환 깨짐·이미지·매뉴얼 없음) — bench 또는 원본 확인 필요.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ───────────── 매뉴얼 파일 ─────────────
MANUALS = {
    "IG": r"IG\Terranova934-Manual.pdf",
    "MFC": r"MFC\(MFC 메뉴얼)GPC3000.pdf",
    "DC": r"POWER\(Ch.k DC) EX Series Manual-Kor_x_ Ver-2.1.pdf",
    "DC_NOTE": r"POWER\Ch.K DCpower 명령어.txt",
    "RF": r"POWER\(RF Power) YSR03MF&06MF메뉴얼.pdf",
    "CESAR": r"POWER\RF Pulse Power&Match (Ch.2)\CH2 AE CESAR manual.pdf",
    "PLC_HELP": r"PLC\XG5000Help_kor.pdf",
}

MISSING_DOCS: tuple = (
    ("PLC", "XGB 내장 Cnet/Modbus 사용설명서 — 국번·지원 FC·응답 처리 시점·통신 오류 플래그"),
    ("PLC", "XGB CPU 하드웨어/명령어 설명서 — 스캔 워치독, RUN→STOP 때 출력 유지/소거"),
    ("PLC", "XBF-AD04A·XBF-DV04A 아날로그 모듈 설명서 — 레인지, CPU STOP·통신 이상 때 DAC 출력 상태"),
    ("PLC", "XBO-TC02A 열전대 모듈 설명서"),
    ("PLC", "실제 XG5000 프로젝트(.xgwx) — CPU 형명, 래더(히터 워치독·인터록), 사용 중 D 주소"),
    ("DC", "아날로그 리모트 옵션(TER1) 장착 여부와 RS-232 제어와 동시 사용 가능 여부 — ODA 문의"),
    ("RF", "YSR 매뉴얼 p.16 원본(한글 깨짐) — 핀 2·3 할당 (반사파 스케일은 사용자 확인으로 v1 값 유지, 2026-10-15)"),
    ("RF", "현재 15핀 배선도 — 4·9(RF ON), 12·15(EXT LOOP) 연결 상태"),
    ("MFC", "장비 실제 설정 — bench 설정 읽기(R33·R34·R70·R71·R74·R75·R37)로 확인. Delimiter 는 명령으로 읽을 수 없음"),
    ("CESAR", "부품번호(15핀/25핀 User Port 변형) — 시리얼 태그 확인"),
)


@dataclass(frozen=True)
class Cmd:
    cmd: str
    meaning: str
    response: str
    source: str
    v1_uses: bool = True


@dataclass(frozen=True)
class Diff:
    item: str
    v1: str
    manual: str
    action: str          # v2 에서 할 일
    source: str


@dataclass(frozen=True)
class Device:
    key: str
    name: str
    model: str
    model_confirmed: bool
    manual: Optional[str]
    serial: str
    framing: str
    commands: tuple
    timing_manual: str           # 매뉴얼상 응답 시간·간격
    errors: str
    safe_off: str
    comm_loss: str               # 통신이 끊기면 장비가 스스로 하는 일
    interlock: str               # 외부 인터록·인히빗 입력
    diffs: tuple = ()
    bench: tuple = ()            # bench 에서 실측할 것


# 시리얼 선(DTR/RTS)은 바꾸지 않는다 — 세 장비 모두 핸드셰이크 선이 없다:
#   DC RJ45 3 RX · 7 TX · 5 GND [DC p.41], MFC DB9 2·3·5 [MFC p.25 표 9], IG DB9 2·3·7 [IG p.35]
SERIAL_LINES_NOTE = "DTR/RTS 손대지 않음 — DC RJ45 3·7·5 [DC p.41], MFC 2·3·5 [MFC p.25], IG 2·3·7 [IG p.35] 핸드셰이크 선 없음"

# MFC(GPC3000) 읽기 명령 표 11 전체 [MFC p.80-88] — (R, 응답 머리, 뜻, 지원). 채널 3·4 는 이 장비에 없음(2채널).
MFC_READ_TABLE: tuple = (
    (0, "S0", "아날로그 셋포인트 %FS", True), (1, "S1", "셋포인트 1 %FS", True), (2, "S2", "셋포인트 2 %FS", True),
    (3, "S3", "셋포인트 3 %FS", True), (4, "S4", "셋포인트 4 %FS", True), (5, "P", "시스템 압력 %FS", True),
    (6, "V", "밸브 열림 %(0 닫힘·100 열림)", True), (7, "M", "Alternate status MXYZ", True),
    (10, "S5", "셋포인트 5 %FS", True), (11, "P1", "공정 한계 1 하한", True), (12, "P2", "공정 한계 1 상한", True),
    (13, "P3", "공정 한계 2 하한", True), (14, "P4", "공정 한계 2 상한", True), (23, "J", "밸브 종류", True),
    (24, "A", "아날로그 셋포인트 범위 — 현재 지원 안 함", False), (25, "T0", "아날로그 셋포인트 종류", True),
    (26, "T1", "SP1 종류", True), (27, "T2", "SP2 종류", True), (28, "T3", "SP3 종류", True), (29, "T4", "SP4 종류", True),
    (30, "T5", "SP5 종류", True), (31, "B", "밸브 위치 출력 범위 — 현재 지원 안 함", False), (32, "N", "제어 방향", True),
    (33, "E", "압력 센서 레인지 코드 [p.83 표]", True), (34, "F", "압력 단위 코드", True),
    (35, "G", "센서 전압 범위 — 현재 지원 안 함", False), (36, "U", "센서 종류 — 현재 지원 안 함", False),
    (37, "M", "시스템 상태 MXYZ(X 0 Local·1 Remote)", True), (38, "H3", "소프트웨어 버전", True),
    (41, "X1", "SP1 P.Lead", True), (42, "X2", "SP2 P.Lead", True), (43, "X3", "SP3 P.Lead", True), (44, "X4", "SP4 P.Lead", True),
    (45, "X5", "SP5 P.Lead", True), (46, "M1", "SP1 Gain", True), (47, "M2", "SP2 Gain", True), (48, "M3", "SP3 Gain", True),
    (49, "M4", "SP4 Gain", True), (50, "M5", "SP5 Gain", True), (51, "V", "자동 압력 제어 종류", True),
    (60, "Q0", "전 채널 유량 %FS", True), (61, "Q1", "ch1 유량", True), (62, "Q2", "ch2 유량", True),
    (63, "Q3", "ch3 유량 — 이 장비에 없음", False), (64, "Q4", "ch4 유량 — 이 장비에 없음", False),
    (65, "Q5", "ch1 설정", True), (66, "Q6", "ch2 설정", True), (67, "Q7", "ch3 설정 — 없음", False), (68, "Q8", "ch4 설정 — 없음", False),
    (69, "L0", "ON/OFF 전체", True), (70, "W1", "ch1 FS", True), (71, "W2", "ch2 FS", True), (72, "W3", "ch3 FS — 없음", False),
    (73, "W4", "ch4 FS — 없음", False), (74, "W5", "ch1 단위(1 SCCM, 2 SLM)", True), (75, "W6", "ch2 단위", True),
    (76, "W7", "ch3 단위 — 없음", False), (77, "W8", "ch4 단위 — 없음", False),
)
# DC(ODA EX) 사양 — 사양표 [DC p.104-105] 는 PDF 에 그림으로만 있다. 사용자가 적은 한 줄만(2026-10-16). 정격은 VOLT:OVL?·CURR:OCL? 기본값으로 확인
DC_SPEC: tuple = (("EX 600-1", 630.0, 1.1, "[DC p.105] — 사용자 제공"),
                  ("EX 600-2", 630.0, 2.1, "[DC p.105] — 10/20 bench OVL 630 V·OCL 2.1 A(기본값 = 설정 가능한 최대)와 일치"))


# ═════════════ 1. PLC — LS XGB(형명 미확인), 내장 Modbus RTU ═════════════
PLC = Device(
    key="plc", name="PLC", model="LS XGB 계열(CPU 형명 미확인 — XBF-DV04A·XBO-TC02A 확장)", model_confirmed=False,
    manual="PLC_HELP",
    serial="COM9, 115200 8N1, 국번 1, Modbus RTU (v1 lib/config.py:21-26). XG5000 기본 파라미터 'MODBUS 설정'"
           " — 국번 0~63, 7/8비트, 패리티, 속도, RTU/ASCII, DI·DO·AI·AO 시작 주소 [PLC_HELP txt p.358 / 9-6]",
    framing="Modbus RTU. 코일 = M워드(10진)×16 + 비트(16진), 레지스터 = D번호. FC1·FC3·FC5·FC6·FC15",
    commands=(
        Cmd("FC1 0..95", "코일 M00000~M0005F 한 번에(밸브·셔터·MV_INTERLOCK·히터 코일·이오나이저)", "비트",
            "v1 PLC.py _read_coils_grouped (v1 은 3~4 프레임)", v1_uses=False),
        Cmd("FC1 160..182", "센서 DI 복사 M00100~M00116", "비트", "lib/config.py:483"),
        Cmd("FC3 0..60", "D00000~D00060 한 번에(RF ADC·히터·DAC·마커)", "워드 61개",
            "v1 은 D0·D1·D10~36·D41·D60 을 따로 읽음", v1_uses=False),
        Cmd("FC5/FC6", "코일·레지스터 1개 쓰기", "에코", "minimalmodbus"),
        Cmd("FC15", "코일 여러 개 쓰기(비상 OFF)", "에코", "device/PLC.py:1416"),
    ),
    timing_manual="매뉴얼에 없음(Cnet 은 보통 스캔 END 에서 처리 — 일반 지식). 115200 bps 1문자 87 µs. "
                  "10/20 bench 응답 p50 22·p99 35 ms(600 tick 모두 ok, 놓친 tick 0). 히터 한계값 8개 = v2 기본값. "
                  "D00016(cur_sv)은 래더가 운전 중에만 갱신(정지 중 825.1 이 남아 있었음) — 화면 단계에서 v1 처럼 정지 중에는 "
                  "D00012 를 보인다(v1 main.py:2809-2826)",
    errors="Modbus 예외 코드 자료 없음. ADC/DAC 모듈 RDY·ERR 비트(U01.00.F/0, U02.00.x) — PLC 주소 맵.txt",
    safe_off="FC15 로 출력 코일 OFF, M00200(DAC enable)=0, D00040=0, 히터 M00040(RUN)=0",
    comm_loss="Modbus 통신 타임아웃 출력 차단 기능은 XG5000 Help 에 없음. 래더 히터 워치독만 있음: "
              "D00014 가 TON 10 s 동안 안 바뀌면 M00046 → 히터 OFF (v1 lib/config.py:226, 313)",
    interlock="래더 인터록 MV_INTERLOCK M00032, 히터 인터록 M00041(래더)",
    diffs=(
        Diff("트랜잭션 수", "폴링 1회 7~12 트랜잭션, timeout 0.5 s × (1+재시도 2) → 실패 시 1.56 s",
             "프레임 크기상 3 트랜잭션(FC1 2 + FC3 1)이면 충분",
             "v2: 블록 3개, timeout 150 ms·재시도 1, 250 ms 내부 폴링", "v1 PLC.py:354-408"),
        Diff("RF DAC 쓰기", "RF 스레드가 매초 enable 코일+DAC+에코 읽기 3 트랜잭션(로그 스팸)",
             "값이 바뀔 때만 FC6 1회면 됨", "v2: PLC worker 가 값 변화 때만 D00040 쓰기", "v1 PLC.py:1322-1364"),
        Diff("D00040 주석", "'Holding Register 64'", "레지스터 40(코드도 40)", "주석 오류 — v2 표에서 40",
             "v1 lib/config.py:213"),
        Diff("주소 주석", "PLC.py 57~58 '[…]는 Modbus 주소 HEX'", "대괄호는 P 출력 번호", "v2 표에 P 번호 따로",
             "v1 device/PLC.py:57"),
        Diff("히터 워치독 하트비트 주기", "HEATER_WD_PERIOD_MS(3 s)마다 D00014 증가", "래더 TON 10 s 안에 바뀌면 됨",
             "v1 과 다르지만 v2 방식 유지(사용자 2026-10-20): 매 tick(1 s). 지연 > HEATER_WD_DELAY_WARN_SEC 면 경고",
             "v1 device/PLC.py:1110-1163"),
        Diff("히터 레시피 램프 상한", "히터 레시피에는 상한 없음(공정 레시피만 60 °C/min)", "래더 분해능 6 °C/min",
             "v1 과 다르지만 v2 방식 유지(사용자 2026-10-20): 히터 레시피도 HEATER_RECIPE_RAMP_MAX_C_PER_MIN 60",
             "v1 core/params.py:364, controller/heater_recipe.py"),
        Diff("ALL STOP", "비상 정지: 버튼 코일 15개 전부 OFF, 히터 RUN=0, DAC enable OFF(D00040 은 그대로)", "—",
             "v1 과 같게(사용자 2026-10-20): 펌프·밸브 포함 전부 OFF + D00040=0 만 더함", "v1 device/PLC.py:1390-1447"),
    ),
    bench=("트랜잭션 왕복 시간 분포(스캔 포함)", "timeout 100/150 ms 때 오류율",
           "USB-시리얼 latency(FTDI 기본 16 ms) 설정", "RUN→STOP 때 DAC·출력 상태"),
)

# ═════════════ 2. MFC — ATOVAC GPC3000 ═════════════
MFC = Device(
    key="mfc", name="MFC 컨트롤러", model="ATOVAC GPC3000 (MFC 2채널 Ar/O2 + 압력 제어)", model_confirmed=True,
    manual="MFC",
    serial="RS-232 DB9 male: 2 TX, 3 RX, 5 GND [MFC p.25 표9]. 9600 기본(9600~115200 선택), 8N1 고정 [p.72, p.54]. "
           "원격 제어는 [CTRL Mode]=Remote [p.54, p.71] — R37 X=1 로 읽어 확인. 핸드셰이크 선 없음(DTR/RTS 손대지 않음)",
    framing="ASCII 명령 + CR, R 명령 외에는 파라미터 필수 [p.72]. 값은 %FS",
    commands=(
        Cmd("R60", "모든 채널 유량", "Q0+100.0+100.0+100.0+100.0 (%FS, 채널당 6바이트)", "[MFC p.86]"),
        Cmd("R5", "시스템 압력", "P <값> (%FS)", "[MFC p.80]"),
        Cmd("R1", "SP1 값", "S1+30.00 (%FS)", "[MFC p.80]"),
        Cmd("R6", "밸브 위치", "V <0 닫힘 ~ 100 열림>", "[MFC p.80]"),
        Cmd("R7", "Alternate status", "MXYZ: X=SP, Y=0 제어/2 열림/4 닫힘, Z=압력 ≤10% 또는 >10% FS", "[MFC p.81]"),
        Cmd("R65~R68", "채널 설정 유량", "Q5..Q8 <값>", "[MFC p.87]"),
        Cmd("R69", "전체 ON/OFF", "L0 bbbb", "[MFC p.87]"),
        Cmd("R70~R77", "채널 FS·단위", "W1..W8", "[MFC p.87-88]", v1_uses=False),
        Cmd("R33 / R34", "압력 레인지 코드 / 단위 코드(읽기)", "E Value / F Value", "[MFC p.83]", v1_uses=False),
        Cmd("R37", "시스템 상태 MXYZ(X 1 = Remote)", "MXYZ", "[MFC p.84]", v1_uses=False),
        Cmd("E v / F v", "!!! 설정 명령 — 압력 레인지·단위를 바꾼다(IG 의 E·F 질의와 다르다). 보내지 않음(가드 금지)", "—",
            "[MFC p.74]", v1_uses=False),
        Cmd("Q1~Q4 v", "유량 설정(플래시 기록 없음)", "응답 규정 없음", "[MFC p.78]"),
        Cmd("L0 bbbb / L1~L4 v", "전체/채널 Flow ON·OFF", "응답 규정 없음", "[MFC p.79]"),
        Cmd("L5~L8 1", "채널 영점", "응답 규정 없음", "[MFC p.79]"),
        Cmd("S1~S5 v / D1~D5", "설정점 값 / 선택", "응답 규정 없음", "[MFC p.73]"),
        Cmd("O / C / H", "밸브 열기 / 닫기 / 정지", "응답 규정 없음", "[MFC p.74]"),
        Cmd("Z1", "압력센서 영점", "응답 규정 없음", "[MFC p.75]"),
    ),
    timing_manual="매뉴얼에 없음(응답 시간·최소 명령 간격·쓰기 ACK 모두). Interface 입력은 250 ms 이상 유지 [p.23-24]. "
                  "10/20 bench 응답 p50 34·p99 62 ms. 설정값은 소수 둘째 자리까지 저장(R65 'Q5+014.00', R2 'S2+000.50'). "
                  "R37 'M006'(X=0, 매뉴얼상 Local)인데 v1 원격 제어가 됨 — R37 은 정보로만. R38 'H1.00'(형식 다름). "
                  "압력 레인지 E11(1000)·단위 F01(mTorr) → FS 1000 mTorr = 1 Torr, 유량 FS 100·단위 SCCM(R74·R75 'W51'·'W61'), "
                  "SP2·SP3·SP4 = 0.50·1.50·3.00 %FS(v1 이 5·15 mTorr 를 ×0.1 로 넣은 값 — 압력 배율 이중 확인)",
    errors="오류 코드 체계 없음. 상태는 R7(MXYZ)·R69·Interface 2 밸브 상태 핀",
    safe_off="L0 0000(전체 Flow OFF) → Q1 0/Q2 0, 밸브 O(배기) — 가스를 확실히 끊는 것은 PLC Ar/O2 차단밸브(M00007/M00008)",
    comm_loss="매뉴얼에 없음 — 마지막 유량·압력 제어를 계속한다고 본다",
    interlock="Flow-OFF 하드 입력 없음. Interface 2: 밸브 Open/Stop/Close 입력(Active Low), 릴레이 2개 [p.24]. "
              "Interface 1: 12번 압력 0~10 V 출력, 13번 밸브 위치 0~10 V 출력 [p.23]",
    diffs=(
        Diff("명령 간격", "MFC_GAP_MS 1000, TIMEOUT 1000, 폴링 2000(공정 중만)", "매뉴얼에 없음",
             "bench 에서 최소 간격 측정. 계획: 간격 100~200 ms, timeout 300 ms", "v1 lib/config.py:793-797"),
        Diff("유량 단위", "스케일 없음(MFC_SCALE_FACTORS=1.0, 죽은 상수)", "R60 값은 %FS",
             "R70/R71(FS) 을 읽어 sccm 환산 — FS 가 100 이 아니면 v1 표시가 틀림", "[MFC p.86-88]"),
        Diff("압력 스케일", "MFC_PRESSURE_SCALE 0.1 (UI 2.00 ↔ HW 0.20)", "R5 는 %FS. FS 1 Torr(E=03) 이면 %FS×10 = mTorr",
             "장비 E 레인지 코드 확인 뒤 상수 확정", "[MFC p.80, p.83]"),
        Diff("R7 이름", "READ_SYSTEM_STATUS", "Alternate status(MXYZ)", "v2 이름 정정", "[MFC p.81]"),
    ),
    bench=("R60·R5 응답 지연 분포", "무응답 0 이 되는 최소 간격(0/50/100/200 ms)", "쓰기 직후 읽기 간섭",
           "R70/R71·E·Delimiter 실제 값", "압력 풀스케일(E 레인지)로 작업 압력 상한 1000 mTorr 가정 확인"),
)

# ═════════════ 3. DC Power — ODA EX 시리즈 ═════════════
# 10/20 bench: VOLT:OVL? 630·CURR:OCL? 2.1(기본값 = 설정 가능한 최대 [p.15, p.98]) → 사양표 [DC p.105] EX 600-2(1.2 kW급).
# v1 소프트웨어 상한 DC_MAX_*(600 V·1 A·600 W)는 그대로. 그 밖에: OVP 660 V·OCP 2.2 A(사용 1), LAST-STA DISABLE, FACT:ADC 20Hz
DC_RATED_VOLTAGE_V = 630.0
DC_RATED_CURRENT_A = 2.1
DC_RATED_POWER_W = 1200.0

DC = Device(
    key="dc", name="DC Power", model="ODA EX 600-2(630 V·2.1 A, 1.2 kW급) — *IDN? 'ODA Technologies,EX-Series,2.1-1.0-1.6'"
                                    "(10/16 bench), OVL 630·OCL 2.1(10/20 bench)과 사양표 [p.105]", model_confirmed=True,
    manual="DC",
    serial="RS-232C/RS-485/TCP(옵션), 9600 기본(9600~230400), 8N1 고정 [DC p.13/15, p.36-37/38-39]. "
           "Delimiter LF/CR/CRLF 선택 [p.47/49], 'Response' 메뉴 Yes/No(공장 No) [p.49] — 명령에 응답을 돌려줄지 정하는 앞면 설정"
           "(명령으로 읽을 수 없음, v1 에서 설정 명령 무응답 → No 로 보임). RS-232 RJ45 3 RX · 7 TX · 5 GND [p.41], "
           "핸드셰이크 선 없음(DTR/RTS 손대지 않음)",
    framing="SCPI 텍스트 + Delimiter, 명령 최대 40바이트 [p.84/86]",
    commands=(
        Cmd("MEAS:ALL?", "출력 전압·전류 측정", "\"10.0000,1.0000\" (V,A)", "[DC p.90/92]"),
        Cmd("MEAS:VOLT? / MEAS:CURR?", "개별 측정", "\"11.0000\"", "[DC p.90/92]"),
        Cmd("APPLy V,I", "전압·전류 설정 — v1 과 같은 글자 'APPLy'(DCpower.py:682, 매뉴얼상 APPL 과 같은 명령)",
            "없음(APPL? → \"600.0000,0.1230\" 따옴표 포함, 10/20 bench)", "[DC p.86/88]"),
        Cmd("CURR A / CURR?", "전류 설정 / 확인", "\"4.5000\"", "[DC p.86-88]"),
        Cmd("OUTP ON|OFF / OUTP?", "출력 ON/OFF / 확인", "\"0\"/\"1\"", "[DC p.51/53, p.94/96]"),
        Cmd("*RST", "출력 OFF·0 V·OVP/OCP 기본·트립 해제", "없음", "[DC p.96/98]"),
        Cmd("*CLS", "오류 큐 지움", "없음", "[DC p.96/98]"),
        Cmd("SYST:ERR?", "오류 기록 — 최근 것부터, 최대 10개, 읽으면 지워짐, *CLS 로 전부 지움(*RST 로는 안 지워짐)",
            "코드,\"문구\"(없으면 +0,\"No error\")", "[DC p.54, p.96]", v1_uses=False),
        Cmd("VOLT:OVP / CURR:OCP", "하드 보호 설정", "없음", "[DC p.87-88]", v1_uses=False),
        Cmd("FACT:LAST-STA?", "전원 복귀 상태 — 읽기만(쓰지 않음, 2026-10-16). FULLY 면 경고", "DISABLE|SAFETY|FULLY",
            "[DC p.94, p.78]", v1_uses=False),
        Cmd("VOLT:OVP:TRIP? / CURR:OCP:TRIP?", "트립 상태 — 트립 중에는 출력이 나오지 않는다", "0|1", "[DC p.89-91]", v1_uses=False),
        Cmd("FLOW? / POL?", "바이폴라 전용 — 이 장비는 응답하지 않아 보내지 않는다", "—", "[DC p.86, p.91]", v1_uses=False),
    ),
    timing_manual="매뉴얼에 없음(사양표는 이미지). v1 실측: 쓰기 직후 질의 무응답(2026-09-28 OUTP OFF 직후 OUTP? 13/13 무응답) → 500 ms 간격. "
                  "10/20 bench 응답 p50 37·p99 45 ms",
    errors="SYST:ERR? 코드: 0, −10, −20~−34(원격 교정), −74~−77, −80~−89(메모리), −120~−124(문법 — −124 정의되지 않은 명령 "
           "[p.102]), −200~−202, −220~−223(실행) [DC p.97-100/99-102]. 기록은 최근 것부터·최대 10·읽으면 지워짐 [p.54, p.96]",
    safe_off="OUTP OFF → OUTP? 로 0 확인(3회). *RST 도 출력 OFF",
    comm_loss="통신 워치독 매뉴얼에 없음 — PC 가 죽으면 마지막 설정으로 계속 출력",
    interlock="아날로그 리모트 옵션(TER1) [p.56-63/58-65]: SW1-6 Direct = TER1 7–10 접점 닫힘 ON/개방 OFF, "
              "SW1-7 TTL Positive = TER1 8(+)–10 High ON. 옵션 장착·RS-232 동시 사용 가능 여부 미확인",
    diffs=(
        Diff("타임아웃·폴백", "MEAS:ALL? 1500 ms + 개별 질의 폴백 1200+1200 ms → 최악 3.9 s, 중첩 이벤트 루프",
             "응답 시간 미기재", "MEAS:ALL? 1회, 마감 500 ms, 폴백은 같은 tick 에 넣지 않음", "v1 DCpower.py:800-818"),
        Diff("쓰기 뒤 대기", "_send 는 120 ms 대기(+msleep)", "실측 500 ms 필요",
             "읽기 t0 → 쓰기 t0+~100 ms → 다음 읽기까지 ~900 ms", "v1 DCpower.py:821-838"),
        Diff("하드 보호", "*RST 뒤 OVP/OCP 설정 안 함", "VOLT:OVP·CURR:OCP 있음",
             "시작 때 OVP=DC_MAX_VOLTAGE, OCP=DC_MAX_CURRENT 설정·되읽기", "[DC p.87-88]"),
        Diff("종단 주석", "_write_line 주석 '\\r\\n'", "실제 송신 LF, 장비 Delimiter 설정을 따라야 함",
             "Delimiter 실제 값 확인", "v1 DCpower.py _write_line"),
        Diff("통신 정상 판정", "쓰기 성공만으로 _comm_last_ok 갱신", "—", "응답을 받았을 때만 정상", "v1 DCpower.py:833"),
        Diff("LAST-STA", "쓰지 않음", "응답은 DISABLE·SAFETY·FULLY(약어 아님)",
             "v2 가 'SAF' 와 비교하고 연결마다 쓰던 버그를 고침 — 읽기만, FULLY 면 경고", "[DC p.94]"),
    ),
    bench=("쓰기→질의 최소 간격 분포", "MEAS:ALL? 지연", "FACT:ADC? 측정 갱신 주기", "Response 설정에 따른 여분 줄",
           "*IDN?·*SN? 형명·정격", "TER1 옵션 유무"),
)

# ═════════════ 4. RF Power — YSR-06MF(추정), PLC 아날로그 제어 ═════════════
RF = Device(
    key="rf", name="RF Power(아날로그)", model="YSR-06MF 13.56 MHz 600 W(추정, 03MF=300 W) + AMN-100", model_confirmed=False,
    manual="RF",
    serial="통신 없음 — 15핀 D-sub 아날로그 I/F, PLC DAC(XBF-DV04A 0~10 V=0~4000)·ADC 로 제어",
    framing="핀 5: 출력 설정 0~10 V = 0~정격, 핀 2·3: 순방향/반사 모니터 출력, 핀 4: RF ON 입력(0 V Off/+5 V On), "
            "핀 7: RF ON 상태 출력, 핀 1: 인터록/과전류 출력, 핀 12·15: EXT LOOP [RF p.16 — 한글 깨짐, 원본 확인]",
    commands=(
        Cmd("D00040(FC6)", "DAC Ch0 = 출력 설정(핀 5)", "—", "v1 lib/config.py:213"),
        Cmd("M00200(FC5)", "DAC Ch0 출력 enable", "—", "v1 lib/config.py:214"),
        Cmd("D00000/D00001(FC3)", "ADC Ch0/Ch1 = 순방향/반사(핀 2·3)", "0~4000", "v1 lib/config.py:208"),
    ),
    timing_manual="출력 응답 10 µs 이내 [RF p.3], 매처 튜닝 2 s 미만 [RF p.7]. 10/20 bench 대기 중 순방향 0.3~0.6 W·반사 0.05~0.15 W"
                  "(ADC 잡음 2~4 카운트)",
    errors="전면 LED: EXT Loop, P.P VOLT, Interlock, OVER HEATING, LOCAL/REMOTE ON [RF p.8-9]. 보호: 최대 전력·과전류·Vpp·과열 [p.6]",
    safe_off="DAC 0(핀 5 = 0 V) → enable OFF. 하드로는 핀 4 = 0 V 또는 12–15 루프 개방",
    comm_loss="해당 없음 — PLC DAC 가 마지막 값을 유지하면 RF 도 계속 켜짐(DAC 의 CPU STOP 동작은 모듈 매뉴얼 없음)",
    interlock="EXT LOOP 12–15 [RF p.9], RF ON 입력 4(–9) [RF p.16]. v1 은 둘 다 PLC 로 구동하지 않음",
    diffs=(
        Diff("반사파 스케일", "ADC 4000 = 200 W (RF_REFLECTED_SCALING_MAX_WATT)",
             "p.16 판독(한글 깨짐)상 600 W 가능성이 있었으나 사용자 확인 완료(2026-10-15) — v1 값 유지",
             "확정: 순방향 4000 = 594.5 W, 반사 4000 = 200 W (RF_FWD/REF_FULL_SCALE_W 기본값)", "[RF p.16], 사용자 확인"),
        Diff("순방향 스케일", "4000 = 594.5 W", "0~10 V = 0~600 W", "교정값(−0.9%)으로 타당", "[RF p.16]"),
        Diff("RF ON 핀", "구동 안 함(DAC enable 만)", "핀 4 RF ON 입력 있음", "PLC 릴레이로 핀 4 또는 EXT LOOP 구동 제안", "[RF p.16]"),
        Diff("RF 끌 때 enable", "D00040 만 0 — M00200(enable)은 비상 정지에서만 OFF", "—",
             "v1 과 다르지만 v2 방식 유지(사용자 2026-10-20): RF_SAFE_OFF 는 D00040=0 뒤 M00200=0",
             "v1 device/PLC.py send_rfpower_command·on_emergency_stop"),
        Diff("RF 이상 중단", "S32·S33A·S33B 는 램프다운(8 카운트/0.5 s) 뒤 0", "—",
             "v1 과 같게: 이상 중단은 정지 순서의 램프다운, ALL STOP 만 즉시 0", "v1 device/RFpower.py:100-102·124-131·158-167·210-214"),
    ),
    bench=("PLC CPU STOP 때 DAC 출력", "현재 핀 4·9·12·15 배선"),
)

# ═════════════ 5. RF Pulse — AE CESAR 1310 ═════════════
CESAR_MAX_POWER_W = 1000      # [CESAR p.31 / 3-5] 최대 출력 1 kW
CESAR_MAX_REFLECTED_W = 200   # 공칭의 20%
CESAR_PULSE_FREQ_MAX_HZ = 30000   # 사양 1 Hz~30 kHz [CESAR p.31 / 3-5] (명령 93 은 100000 까지 받음 p.108)

RFPULSE = Device(
    key="rfpulse", name="RF Pulse", model="AE CESAR 1310 13.56 MHz 1 kW (변형 61300050/61300053 미확인)", model_confirmed=True,
    manual="CESAR",
    serial="DB9 female: 2 TXD, 3 RXD, 5 COM, 핸드셰이크 없음 [CESAR p.78 / 4-44]. 9600~115200, 홀수 패리티 8O1 "
           "[p.78-79 / 4-44~45]. 주소 1~31(0 = 브로드캐스트, 응답 없음), RS-232 는 사실상 1 [p.80 / 4-46, p.120 / 4-86]",
    framing="AE Bus: Header(bit3~7 주소, bit0~2 길이 0~6, 7 이면 길이 바이트 추가) + Command + Data(LSB 먼저) + "
            "XOR 체크섬. ACK 0x06 / NAK 0x15. 1~127 설정(CSR 응답), 128~255 읽기 [p.79-83 / 4-45~49, p.96-97 / 4-62~63]",
    commands=(
        Cmd("1 / 2", "RF OFF / ON", "CSR", "[CESAR p.97 / 4-63]"),
        Cmd("3", "조절 모드 6=Pforward (RF ON 중 변경 불가)", "CSR", "[CESAR p.97 / 4-63]"),
        Cmd("8", "셋포인트 W(2바이트)", "CSR", "[CESAR p.98 / 4-64]"),
        Cmd("14", "제어 모드 2=Host — RF ON 중 바꾸면 자동 OFF", "CSR", "[CESAR p.101 / 4-67]"),
        Cmd("27", "펄싱 0 off/1 내부/2 외부/3 외부 반전/4", "CSR", "[CESAR p.105 / 4-71]"),
        Cmd("93", "펄스 주파수 Hz(3바이트)", "CSR", "[CESAR p.108 / 4-74]"),
        Cmd("96", "듀티 1~99 %(2바이트), 최소 ON/OFF 16 µs", "CSR", "[CESAR p.109 / 4-75]"),
        Cmd("162", "상태 4바이트(출력 on b5, 인터록 열림 2번째 b7, 과열, 셋포인트 밖 …)", "4바이트", "[CESAR p.114-115 / 4-80~81]"),
        Cmd("165 / 166", "순방향 / 반사 전력 W", "2바이트", "[CESAR p.116 / 4-82]"),
        Cmd("193 / 196", "펄스 주파수 / 듀티 되읽기", "3 / 2바이트", "[CESAR p.119 / 4-85]"),
        Cmd("223", "에러 코드", "데이터", "[CESAR p.120 / 4-86]", v1_uses=False),
        Cmd("33 / 152", "반사 전력 자동 차단 시간 설정/읽기(1~255 s, 0=끔)", "CSR / 데이터", "[CESAR p.107 / 4-73]", v1_uses=False),
    ),
    timing_manual="RS-232 응답 시간 없음. 핸드셰이크(응답 받은 뒤 다음 명령)를 지키면 추가 간격 요구 없음, 아니면 50 ms 이상 "
                  "[CESAR p.174 / 6-4]. 한 트랜잭션을 끝낸 뒤 다음 시작 [p.79 / 4-45]. 여러 값 한 번에 주는 명령 없음(PROFIBUS 만)",
    errors="CSR 0 accepted, 1 wrong control mode, 2 RF on, 4 범위 밖, 5 User Port RF signal off, 7 fault, 9 바이트 수, "
           "19 레시피, 50 주파수 범위, 51 듀티 범위, 99 미구현 [p.96 / 4-62]. 에러 E01 인터록 루프 열림, E10/E11 과열 … "
           "'Error conditions always turn output off' [p.177-180 / 6-7~10]",
    safe_off="명령 1(RF OFF) → 162 로 출력 off 확인",
    comm_loss="RS-232 호스트 워치독 매뉴얼에 없음(PROFIBUS 오류 때만 출력 OFF [p.87 / 4-53]) — PC 가 죽으면 RF 계속",
    interlock="15핀 형: 별도 9핀 인터록 커넥터 1(입력+)–2(24 V) 닫힘 [p.64-68 / 4-30~34]. 25핀 형: User Port 10–23 루프 "
              "[p.52 / 4-18]. 열리면 RF OFF(E01). 점퍼 플러그는 인터록을 없앤다 [p.45 / 4-11]",
    diffs=(
        Diff("명령 간격", "RFPULSE_CMD_GAP_MS 1500", "핸드셰이크 또는 50 ms", "간격 0~50 ms, 요청당 마감 250 ms",
             "v1 lib/config.py:652"),
        Diff("질의 타임아웃", "RFPULSE_POLL_QUERY_TIMEOUT_MS 9000, 재오픈 3 s 보다 김", "근거 없음",
             "요청당 250 ms, 실패 시 그 tick 나머지 건너뜀", "v1 lib/config.py:656"),
        Diff("폴링 주기", "5 s(START 이후만)", "—", "매초 162·165·166, 공정 밖에서도", "v1 lib/config.py:655"),
        Diff("CSR 5 문구", "'interlock (model dependent)'", "'User Port RF signal is off'", "문구 정정",
             "[CESAR p.96 / 4-62]"),
        Diff("주소 0", "검사 없음(&0x1F)", "0 은 브로드캐스트 — 응답 없음", "RFPULSE_ADDR 0 은 경고(범위 0~31 유지, bench 확인)",
             "[CESAR p.80 / 4-46]"),
        Diff("인터록 열림", "162 인터록 비트를 보지 않음", "162 byte2 b7 = 인터록 열림 [p.114]",
             "v1 과 다르지만 v2 방식 유지(사용자 2026-10-20): RF Pulse 사용 공정에서 열림 즉시 중단(S49)", "[CESAR p.114 / 4-80]"),
    ),
    bench=("명령별 요청→ACK→데이터 지연(RF ON/OFF/펄싱)", "응답 직후 0 ms 송신 가능 여부", "인터록 열 때 RF OFF 시간, 닫을 때 자동 재ON 안 함",
           "명령 33 동작", "SW 버전(198/130)", "15핀/25핀 변형"),
)

# ═════════════ 6. 이온 게이지 — Duniway Terranova 934 ═════════════
IG = Device(
    key="ig", name="이온 게이지", model="Duniway Terranova 934 (MOXA UPort 1410 경유 COM12)", model_confirmed=True,
    manual="IG",
    serial="DB9 female: 2 TX(934→PC), 3 RX, 7 GND, 핸드셰이크 없음 [IG p.35]. 1200~9600(공장 9600, DIP S1-1/S1-2), "
           "8N1 [IG p.18, p.36, p.52]. 주소 없음(1:1)",
    framing="대문자 ASCII 한 글자, 종단 없음. E 응답 26항목 CR 구분(실측 LF CR). 압력 'xy z' = xy×10^z [IG p.53-54]",
    commands=(
        Cmd("E", "상태 26항목: 2 필라멘트, 3 방출, 5 에러 코드, 24 이온, 25·26 게이지 A/B", "26줄", "[IG p.53-54 표 3.4]"),
        Cmd("A / B", "필라멘트 ON / OFF", "응답 없음(실측)", "[IG p.53]"),
        Cmd("C / D", "디개스 ON / OFF", "—", "[IG p.53]", v1_uses=False),
        Cmd("F / G / H", "이온 / 게이지 A / 게이지 B 압력 한 줄(xy z = xy×10^z, 꺼짐 0, 게이지 없음 −900, 영점 안 됨 −999)",
            "한 줄", "[IG p.54]"),
        Cmd("E 15~23", "15 셋포인트 보호 해제 · 16 자동 필라멘트 · 17 자동 필라멘트 셋포인트 · 18·19 이온 SP1·SP2 · "
            "20 가스 계수(50~150 → 0.50~1.50) · 21 감도(50~150 → 5.0~15.0, 563 정상 10.0 [p.41]) · 22·23 게이지 A·B 셋포인트 "
            "— 셋포인트는 xy z = xy×10^(z−2) Torr", "E 의 일부", "[IG p.53-54]"),
    ),
    timing_manual="10/20 bench: E 응답 p50 234·p99 337·최대 390 ms(필라멘트 꺼짐), 가스 계수 1.00·감도 10.0, 자동 필라멘트 꺼짐. "
                  "응답 시간 없음. 문자 사이 50 ms 이상 [p.36, p.52], 초당 약 2문자 이하·응답 받은 뒤 송신 [p.53]. "
                  "필라멘트 끈 뒤 4 s 재점등 금지, 전원 뒤 10 s 금지 [p.42]. A/B 뒤 무응답(실측 5.9/4.9 s)은 매뉴얼에 없음",
    errors="필라멘트 에러 표 3.2: 3 방출 상실, 4 시간 안에 안 켜짐, 5 자동 시동 실패, 6 HV 없음, 7 필라멘트 전류 없음, "
           "8 과전류, 9 최대 압력 초과, 12/14/15 9.9E-3 초과 차단, 16/17 자동 모드 OFF [IG p.44]",
    safe_off="B(필라멘트 OFF)",
    comm_loss="호스트 타임아웃 없음. 자체 보호: 1 Torr 초과 차단 [p.12], S2-3 이면 9.9E-3 Torr 차단(공장 기본) [p.16, p.18]",
    interlock="릴레이 4개(게이지 A·B, 이온 SP1·SP2), 압력 < 셋포인트일 때 여자 — 전원 OFF·고압이면 풀림(페일세이프). "
              "6 A 250 VAC [IG p.11, p.32-34, p.46]",
    diffs=(
        Diff("응답 제한", "IG_RESPONSE_TIMEOUT_MS 1200 > 주기 1 s", "근거 없음", "tick 마감 900 ms, 늦으면 stale(실패 아님)",
             "v1 lib/config.py:756"),
        Diff("무응답 유예·ON 기한", "12 s / 20 s", "근거 없음(실측 기반)", "파라미터로 유지, bench 분포 측정",
             "v1 lib/config.py:746-749"),
        Diff("에러 표 번호", "—", "E 항목 5 주석이 '표 3.3' 이라 했지만 실제 표 3.2", "참고만", "[IG p.53]"),
    ),
    bench=("E 바이트 수·지연 분포(필라멘트 off/시동/안정/레인지 전환)", "A/B 뒤 무응답 분포", "시동 중('E.0 -3') 항목 24 원문",
           "릴레이 핀·24 V 점퍼", "DIP S2 값"),
)

DEVICES: tuple = (PLC, MFC, DC, RF, RFPULSE, IG)


# ═════════════ PLC 주소 (배선·래더 사실, v1 lib/config.py:208-259, 464-504 + PLC 주소 맵.txt) ═════════════
@dataclass(frozen=True)
class Addr:
    name: str
    kind: str        # coil / reg
    addr: int        # Modbus 번호
    plc: str         # PLC 주소(M/D)
    access: str      # R / W / RW
    desc: str


PLC_ADDRS: tuple = (
    Addr("rotary", "coil", 0, "M00000→P00040", "RW", "Rotary 펌프"),
    Addr("rv", "coil", 1, "M00001→P00041", "RW", "Roughing 밸브"),
    Addr("fv", "coil", 2, "M00002→P00042", "RW", "Foreline 밸브"),
    Addr("mv", "coil", 3, "M00003→P00043", "RW", "Main 밸브(공정 시작 인터록)"),
    Addr("vent", "coil", 4, "M00004→P00161", "RW", "Vent 밸브"),
    Addr("turbo", "coil", 5, "M00005→P00045", "RW", "Turbo 펌프"),
    Addr("door_up", "coil", 6, "M00006→P00162", "RW", "도어 올림(내림과 상호배타 — 소프트웨어)"),
    Addr("ar_valve", "coil", 7, "M00007→P00047", "RW", "Ar 차단밸브"),
    Addr("o2_valve", "coil", 8, "M00008→P00164", "RW", "O2 차단밸브"),
    Addr("ms", "coil", 9, "M00009→P00049", "RW", "Main 셔터"),
    Addr("s1", "coil", 16, "M00010→P0004A", "RW", "Gun 1 셔터"),
    Addr("s2", "coil", 17, "M00011→P0004B", "RW", "Gun 2 셔터"),
    Addr("door_down", "coil", 32, "M00020→P00163", "RW", "도어 내림"),
    Addr("buzz_stop", "coil", 33, "M00021→P00165", "RW", "부저 정지"),
    Addr("mv_interlock", "coil", 50, "M00032", "R", "MV 인터록(래더)"),
    Addr("heater_run", "coil", 64, "M00040", "RW", "히터 운전 요구"),
    Addr("heater_itl", "coil", 65, "M00041", "R", "히터 인터록 정상"),
    Addr("heater_fault", "coil", 66, "M00042", "R", "히터 이상 종합"),
    Addr("heater_rst", "coil", 67, "M00043", "W", "히터 이상 리셋"),
    Addr("heater_ot", "coil", 68, "M00044", "R", "과온 트립"),
    Addr("heater_tc_err", "coil", 69, "M00045", "R", "온도센서 이상"),
    Addr("heater_wd_err", "coil", 70, "M00046", "R", "워치독 타임아웃"),
    Addr("heater_pid_run", "coil", 73, "M00049", "R", "PID 동작 중"),
    Addr("heater_pv_sel", "coil", 74, "M0004A", "RW", "TC2 로 제어 요구"),
    Addr("heater_pv_sel_eff", "coil", 75, "M0004B", "R", "래더가 TC2 로 제어 중"),
    Addr("ion_remote_on", "coil", 80, "M00050→P00160", "RW", "이오나이저 Remote On"),
    Addr("air", "coil", 160, "M00100←P00000", "R", "Air 센서"),
    Addr("g1", "coil", 161, "M00101←P00001", "R", "G1 센서"),
    Addr("g2", "coil", 162, "M00102←P00002", "R", "G2 센서"),
    Addr("atm", "coil", 163, "M00103←P00003", "R", "ATM 센서"),
    Addr("water", "coil", 164, "M00104←P00004", "R", "냉각수 센서"),
    Addr("ion_run", "coil", 180, "M00114←P0000E", "R", "이오나이저 구동"),
    Addr("ion_lamp", "coil", 181, "M00115←P0000F", "R", "이오나이저 점등"),
    Addr("ion_ot", "coil", 182, "M00116←P00010", "R", "이오나이저 램프 수명 초과"),
    Addr("rf_dac_enable", "coil", 320, "M00200(U02.02.0)", "RW", "RF DAC Ch0 출력 enable"),
    Addr("rf_fwd_adc", "reg", 0, "D00000(U01.02)", "R", "RF 순방향 ADC 0~4000"),
    Addr("rf_ref_adc", "reg", 1, "D00001(U01.03)", "R", "RF 반사 ADC 0~4000"),
    Addr("heater_pv", "reg", 10, "D00010", "R", "TC1 (0.1 °C, -1=이상)"),
    Addr("heater_pv2", "reg", 11, "D00011", "R", "TC2 (0.1 °C, -1=이상)"),
    Addr("heater_sv", "reg", 12, "D00012", "RW", "목표 온도"),
    Addr("heater_sv_limit", "reg", 13, "D00013", "RW", "목표 상한"),
    Addr("heater_wd", "reg", 14, "D00014", "W", "워치독 카운터(PC 하트비트)"),
    Addr("heater_cur_sv", "reg", 16, "D00016", "R", "램프 적용 목표"),
    Addr("heater_pid_err", "reg", 17, "D00017", "R", "PID 에러 코드"),
    Addr("heater_mv_limit", "reg", 18, "D00018", "RW", "DAC 운전 상한"),
    Addr("heater_sv_ramp", "reg", 19, "D00019", "R", "램프 중간 목표"),
    Addr("heater_ramp_rate", "reg", 20, "D00020", "RW", "램프 속도(카운트/초)"),
    Addr("heater_holdback", "reg", 21, "D00021", "RW", "홀드백(0.1 °C)"),
    Addr("heater_ot_limit", "reg", 26, "D00026", "RW", "TC1 과온(0.1 °C)"),
    Addr("heater_slow_zone", "reg", 27, "D00027", "RW", "감속 구간(0.1 °C)"),
    Addr("heater_slow_rate", "reg", 28, "D00028", "RW", "감속 속도"),
    Addr("heater_pv_ctrl", "reg", 33, "D00033", "R", "PID 가 보는 PV"),
    Addr("heater_ot2_limit", "reg", 34, "D00034", "RW", "TC2 과온(0.1 °C, 래더 상한 11500)"),
    Addr("heater_sv2", "reg", 35, "D00035", "RW", "TC2 목표"),
    Addr("heater_sv2_max", "reg", 36, "D00036", "R", "TC2 목표 상한(옛 래더는 0)"),
    Addr("rf_dac", "reg", 40, "D00040(U02.03)", "RW", "RF DAC Ch0 0~4000"),
    Addr("heater_mv", "reg", 41, "D00041(U02.04)", "R", "히터 DAC 출력(래더가 매 스캔 씀)"),
)

PLC_D_USED = frozenset(set(range(0, 4)) | set(range(10, 37)) | set(range(40, 44)))   # v1 lib/config.py:97
HEATER_WD_TON_SEC = 10            # 래더 TON (v1 lib/config.py:34 주석, 226)
HEATER_TEMP_SCALE_C_PER_COUNT = 0.1   # 래더 고정 — v1 HEATER_TEMP_SCALE 을 설정에서 뺐다
HEATER_DAC_PHYS_MAX = 1600        # XBF-DV04A 4 V = VSCD-30 입력 상한(래더가 D00018 을 여기로 클램프)
HEATER_DAC_PHYS_MIN = 320         # 0.8 V
HEATER_OT2_LADDER_MAX_C = 1150.0  # 래더 상한 11500
RF_DAC_FULL_SCALE = 4000
RF_ADC_FULL_SCALE = 4000


# ═════════════ 1초 예산 ═════════════
@dataclass(frozen=True)
class TickPlan:
    device: str
    reads: tuple              # 이번 tick 에 보내는 읽기
    line_ms: float            # 회선 시간(프레임 바이트 × 문자 시간) 합
    est_ms: str               # 장비 처리 포함 예상
    fits: str                 # "yes" / "bench" / "no"
    plan: str                 # 1초 안에 안 되면 번갈아 읽기 계획
    deadline_param: str       # 마감 파라미터 키


TICK_PLAN: tuple = (
    TickPlan("plc", ("FC1 0..95", "FC1 160..182", "FC3 0..60"), 25.0,
             "트랜잭션당 10~25 ms(스캔·USB latency) → 30~75 ms", "yes",
             "250 ms 내부 폴링 4회 중 정각 회차를 스냅샷에. 실패 시 재시도 1회 후 그 회차 fail",
             "PLC_READ_DEADLINE_MS"),
    TickPlan("mfc", ("R60", "R5"), 47.0, "장비 처리 미상 — 간격 100~200 ms 면 약 300~500 ms", "bench",
             "실측에서 1초에 둘 다 안 되면: R5(압력)는 매초, R60(유량)은 짝수 초 — 유량 필드 own_quality",
             "MFC_READ_DEADLINE_MS"),
    TickPlan("dc", ("MEAS:ALL?",), 28.0, "장비 처리 미상 + 쓰기 뒤 500 ms 규칙", "bench",
             "읽기 t0, 제어 쓰기(CURR) t0+약 100 ms → 다음 읽기까지 약 900 ms. 쓰기 직후 읽기 금지",
             "DC_READ_DEADLINE_MS"),
    TickPlan("rfpulse", ("162", "165", "166"), 41.0, "장비 처리 L 미상, L=150 ms 여도 약 0.5 s", "bench",
             "L > 250 ms 이면: 166(반사)은 매초, 165 와 162 를 번갈아 — fwd·status own_quality",
             "RFPULSE_READ_DEADLINE_MS"),
    TickPlan("ig", ("E",), 160.0, "실측 250~300 ms, 레인지 찾기 860 ms", "yes",
             "응답을 IG_RESPONSE_LIMIT_MS(1200)까지 기다려 늦으면 다음 행에 late, 그동안 E 를 보내지 않음(stale). "
             "필라멘트 A/B 뒤 무응답은 suppressed. F/G/H 는 끼우지 않음",
             "IG_RESPONSE_LIMIT_MS"),
    TickPlan("rf", ("PLC FC3 D0~D1 에 포함",), 0.0, "PLC 블록에 포함 — 추가 0", "yes", "—", "PLC_READ_DEADLINE_MS"),
)


# ═════════════ PC 가 죽었을 때 출력 끄기 ═════════════
@dataclass(frozen=True)
class PcDeadMeasure:
    id: str
    target: str
    method: str
    needs_ladder: bool
    needs_wiring: bool
    needs_option: bool        # 장비 옵션·확인 필요
    priority: int             # 1 = 먼저
    source: str
    status: str = "planned"   # planned(계획대로) / hold(보류)
    note: str = ""


# 보류 결정(사용자 2026-10-15): PLC 래더(PD1~PD3)·릴레이 배선(PD4~PD6) 모두 보류, PD9 는 매뉴얼을 구할 때까지 보류.
# PD7·PD8 은 PLC 와 무관한 장비 자체 설정 — 연결할 때 prod 에서만 쓰고 되읽어 확인(보류 아님).
# 전원 3종(RF·DC·RF Pulse) 중 PLC 와 연결된 것은 RF power(아날로그, DAC·ADC)뿐이다 — DC·RF Pulse 는 시리얼만.
PLC_CONNECTED_POWER = ("rf",)
_HOLD = "보류(2026-10-15)"
_HB_NOTE = ("나중에 PC 하트비트 래더를 만들 때: v1 이 D00014 를 3 s 마다 쓰는 동안(v1·v2 병행·되돌리기 기간)은 "
            "판정 시간을 10 s 이상으로 해야 오작동하지 않는다")


PC_DEAD_PLAN: tuple = (
    PcDeadMeasure("PD1", "공통 하트비트", "히터 워치독 D00014(TON 10 s)를 'PC_ALIVE' 로 넓혀 PC 가 1초 tick 마다 증가. "
                  "래더가 약 3 s 동안 안 바뀌면 아래 PD2~PD5 를 실행. 펌프·MV 는 그대로", True, False, False, 1,
                  "v1 lib/config.py:226, 313 + 매뉴얼 4종 모두 통신 워치독 없음", "hold", f"{_HOLD}. {_HB_NOTE}"),
    PcDeadMeasure("PD2", "RF(아날로그)", "PC_ALIVE 끊김 → M00200(DAC enable)=0, D00040=0", True, False, False, 1,
                  "[RF p.16] 핀 5 = 0 V 면 출력 0", "hold", _HOLD),
    PcDeadMeasure("PD3", "가스", "PC_ALIVE 끊김 → Ar/O2 차단밸브 M00007/M00008 OFF", True, False, False, 1,
                  "PLC 주소 맵.txt (P00047, P00164)", "hold", _HOLD),
    PcDeadMeasure("PD4", "RF(아날로그) 하드", "PLC 릴레이 출력을 RF 핀 4(RF ON) 또는 EXT LOOP 12–15 에 직렬 — PLC 가 죽어도 OFF",
                  True, True, False, 2, "[RF p.9, p.16]", "hold", _HOLD),
    PcDeadMeasure("PD5", "RF Pulse", "PLC 릴레이 접점을 CESAR 인터록 루프(15핀형 9핀 커넥터 1–2 / 25핀형 10–23)에 직렬. "
                  "열리면 E01 로 RF OFF, 다시 닫혀도 자동 재ON 안 됨(bench 확인)", True, True, False, 2,
                  "[CESAR p.52 / 4-18, p.64-68 / 4-30~34, p.177 / 6-7]", "hold", _HOLD),
    PcDeadMeasure("PD6", "DC", "EX 아날로그 옵션 TER1 Direct(SW1-6, 7–10 접점) 또는 TTL Positive(SW1-7, 8–10)에 PLC 릴레이 — "
                  "개방 = 출력 OFF. 옵션 장착·RS-232 동시 사용 가능 여부를 ODA 확인", True, True, True, 3,
                  "[DC p.56-63/58-65, p.44/46]", "hold", _HOLD),
    PcDeadMeasure("PD7", "DC 보조", "시작 때 VOLT:OVP·CURR:OCP 설정(DC_SET_PROTECTION 일 때만). FACT:LAST-STA 는 읽기만 — "
                  "FULLY 면 경고(바꾸는 것은 사람이 앞면 FACTORY 메뉴에서)", False, False, False, 2,
                  "[DC p.87-88, p.78, p.94]", "planned", "OVP·OCP 는 prod 에서만 쓰고 되읽어 확인. LAST-STA 는 어떤 모드에서도 쓰지 않음"),
    PcDeadMeasure("PD8", "RF Pulse 보조", "명령 33 반사 전력 자동 차단(1~255 s) 설정 — 통신과 무관한 장비 자체 보호", False, False, False, 2,
                  "[CESAR p.107 / 4-73]", "planned", "연결할 때 prod 에서만 쓰고 되읽어 확인"),
    PcDeadMeasure("PD9", "PLC 자체", "PLC 가 STOP/전원 OFF 일 때 DAC·출력 상태 확인(모듈 매뉴얼 필요). 출력 소거가 아니면 PD4~PD6 하드 배선이 유일한 길",
                  False, False, True, 1, "MISSING_DOCS (XBF-DV04A, XGB CPU)", "hold", f"{_HOLD} — 매뉴얼을 구할 때까지"),
)


# ═════════════ 확인 안 된 데이터 — 지금은 가정값으로 진행(사용자 2026-10-15), bench 에서 확인 ═════════════
@dataclass(frozen=True)
class Assumption:
    item: str
    assumed: str
    device: str        # DEVICES 의 key
    bench_item: str    # 그 장비 bench 목록에 있어야 하는 항목(일부 문자열)


ASSUMPTIONS: tuple = (
    Assumption("MFC 유량 풀스케일", "확인(10/20 bench): FS 100·단위 SCCM(R74·R75 'W51'·'W61') — R60 %FS = sccm, v1 과 같음",
               "mfc", "R70/R71"),
    Assumption("MFC 압력 레인지·작업 압력 상한", "확인(10/20 bench): 레인지 E11(1000)·단위 F01(mTorr) → FS 1 Torr = 1000 mTorr. "
               "SP2·SP3·SP4 0.50·1.50·3.00 %FS = v1 이 5·15 mTorr 를 ×0.1 로 넣은 값(배율 이중 확인)", "mfc", "작업 압력 상한"),
    Assumption("MFC 명령 간격", "100~200 ms(10/20 bench 응답 p50 34·p99 62 ms)", "mfc", "최소 간격"),
    Assumption("MFC Remote 표시(R37)", "10/20 bench 'M006'(X=0, 매뉴얼상 Local)인데 v1 은 원격 제어가 됨 — 정보로만, 거부하지 않음",
               "mfc", "R70/R71"),
    Assumption("DC 형명·정격", "확인(10/20 bench): OVL 630 V·OCL 2.1 A → EX 600-2(630 V·2.1 A, 1.2 kW급). OVP 660·OCP 2.2(사용 1), "
               "ADC 20Hz, APPL? 응답에 따옴표. v1 소프트웨어 상한 600 V·1 A·600 W 는 그대로", "dc", "*IDN?"),
    Assumption("DC 쓰기→질의 간격", "500 ms (v1 실측)", "dc", "쓰기→질의"),
    Assumption("DC 아날로그 옵션(TER1)", "없음으로 가정", "dc", "TER1"),
    Assumption("DC Delimiter", "LF(v1 송신과 같게)", "dc", "Response 설정"),
    Assumption("DC LAST-STA 현재 값", "확인(10/20 bench): DISABLE — 연결 때 읽기만(FULLY 면 경고, 쓰지 않음)", "dc", "*IDN?"),
    Assumption("CESAR User Port 커넥터 형식", "모름(15핀/25핀) — 인터록 배선 보류라 당장 영향 없음", "rfpulse", "15핀/25핀"),
    Assumption("CESAR 응답 지연", "150 ms 이하(1초에 3질의)", "rfpulse", "지연"),
    Assumption("PLC 트랜잭션 왕복", "확인(10/20 bench): 응답 p50 22·p99 35 ms, 600 tick 모두 ok", "plc", "왕복 시간"),
    Assumption("PLC STOP 때 DAC 출력", "모름 — PD9 보류", "rf", "PLC CPU STOP"),
    Assumption("이온 게이지 E 응답 분포", "10/20 bench(필라멘트 꺼짐) p50 234·p99 337·최대 390 ms, v1 실측 최대 860 ms(레인지 찾기)",
               "ig", "E 바이트 수"),
)
