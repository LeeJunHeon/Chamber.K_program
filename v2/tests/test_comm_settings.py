"""장비 통신 설정(5-17) — 속도 허용값·패리티·정지 비트·DC 종결 문자·응답 시간이 설치값이고 화면에서 고칠 수 있다.

- 속도는 장비가 실제로 받는 값만(ParamDef.choices — 형식은 int 그대로, 묶음 params[].choices).
- 기본값으로 만든 포트 설정(SerialSettings)은 예전과 같다.
- DC_DELIMITER(LF·CR·CRLF): 드라이버 송신 종결·수신 끝·통신층 가드·시뮬레이터가 같은 값을 쓴다.
- 같은 COM 포트를 켜진 두 장비가 쓰면 거부(X22 — 꺼진 장비 제외).
실제 포트는 열지 않는다(가상 시계 + 시뮬레이터).
"""
import pytest

from v2.common.clock import VirtualClock
from v2.common.paramdef import PType, Scope, Apply, check_config, check_value, defaults, schema_dict
from v2.common.transport import SerialSettings, SimTransport
from v2.common.worker import Request, Worker
from v2.chk import cmd_guard as G
from v2.chk import devices as dev
from v2.chk import params as P
from v2.chk import web_schema as W
from v2.chk.drivers.dc import DcDriver
from v2.chk.sim.dc_sim import DcSim
from v2.chk.system import serial_settings

CFG = defaults(P.PARAMS)
BY = P.by_key()
BAUDS = {"PLC_BAUD": dev.PLC_BAUDS, "MFC_BAUD": dev.MFC_BAUDS, "DC_BAUD": dev.DC_BAUDS,
         "RFPULSE_BAUD": dev.CESAR_BAUDS, "IG_BAUD": dev.IG_BAUDS}


# ───────── 허용값 ─────────
def test_baud_lists_are_the_manual_values():
    assert dev.PLC_BAUDS == (1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200)
    assert dev.MFC_BAUDS == (9600, 19200, 38400, 57600, 115200)
    assert dev.DC_BAUDS == (9600, 19200, 38400, 57600, 115200, 230400)
    assert dev.CESAR_BAUDS == (9600, 19200, 38400, 57600, 115200)
    assert dev.IG_BAUDS == (1200, 2400, 4800, 9600)
    for k, bauds in BAUDS.items():
        p = BY[k]
        assert p.type is PType.INT and p.choices == bauds and (p.min, p.max) == (min(bauds), max(bauds))
        assert p.scope is Scope.INSTALL and p.apply is Apply.RESTART
    # 기본값 = 지금 장비
    assert [CFG[k] for k in BAUDS] == [115200, 9600, 9600, 9600, 9600]
    assert (CFG["PLC_PARITY"], CFG["PLC_STOPBITS"], CFG["DC_DELIMITER"], CFG["RFPULSE_PARITY"]) == ("none", 1, "LF", "odd")


@pytest.mark.parametrize("key,bad", [
    ("PLC_BAUD", 14400), ("PLC_BAUD", 230400), ("PLC_BAUD", 600), ("PLC_BAUD", "115200"), ("PLC_BAUD", 115200.0),
    ("MFC_BAUD", 4800), ("MFC_BAUD", 10000), ("MFC_BAUD", 230400),
    ("DC_BAUD", 4800), ("DC_BAUD", 28800), ("DC_BAUD", 460800),
    ("RFPULSE_BAUD", 4800), ("RFPULSE_BAUD", 230400), ("RFPULSE_BAUD", 56000),
    ("IG_BAUD", 19200), ("IG_BAUD", 300), ("IG_BAUD", 9601),
    ("PLC_PARITY", "N"), ("PLC_PARITY", "mark"), ("PLC_PARITY", 0),
    ("PLC_STOPBITS", 0), ("PLC_STOPBITS", 3), ("PLC_STOPBITS", 1.5), ("PLC_STOPBITS", "1"),
    ("DC_DELIMITER", "lf"), ("DC_DELIMITER", "\n"), ("DC_DELIMITER", "LFCR"),
    ("RFPULSE_PARITY", "mark"),
])
def test_values_outside_the_list_are_rejected(key, bad):
    assert check_value(BY[key], bad)
    assert check_config(P.PARAMS, P.CROSS_RULES, dict(CFG, **{key: bad}))


def test_every_listed_value_is_accepted():
    for k, bauds in BAUDS.items():
        for b in bauds:
            assert check_config(P.PARAMS, P.CROSS_RULES, dict(CFG, **{k: b})) == [], (k, b)
    for k, vals in (("PLC_PARITY", dev.PLC_PARITIES), ("PLC_STOPBITS", dev.PLC_STOPBITS), ("DC_DELIMITER", dev.DC_DELIMITERS)):
        for v in vals:
            assert check_config(P.PARAMS, P.CROSS_RULES, dict(CFG, **{k: v})) == [], (k, v)


def test_choices_only_on_numbers_and_default_is_listed():
    for p in P.PARAMS:
        if p.choices:
            assert p.type in (PType.INT, PType.FLOAT) and p.default in p.choices and p.enum is None


# ───────── 포트 설정 조립 ─────────
def test_default_serial_settings_same_as_before():
    """기본값으로 만든 SerialSettings = 5-17 전의 것(PLC 115200 8N1, MFC·DC·IG 9600 8N1, RF Pulse 9600 8O1, 쓰기 제한 1 s)."""
    st = serial_settings(CFG)
    assert st == {"plc": SerialSettings("COM9", 115200), "mfc": SerialSettings("COM10", 9600),
                  "dc": SerialSettings("COM11", 9600), "rfpulse": SerialSettings("COM13", 9600, parity="O"),
                  "ig": SerialSettings("COM12", 9600)}
    for s in st.values():
        assert (s.bytesize, s.stopbits, s.write_timeout) == (8, 1, 1.0)


def test_serial_settings_follow_the_new_keys():
    c = dict(CFG, PLC_BAUD=38400, PLC_PARITY="even", PLC_STOPBITS=2, MFC_BAUD=19200, DC_BAUD=230400, RFPULSE_BAUD=57600,
             IG_BAUD=1200, PORT_WRITE_TIMEOUT_SEC=2.5)
    st = serial_settings(c, {"dc": "COM21"})
    assert st["plc"] == SerialSettings("COM9", 38400, parity="E", stopbits=2, write_timeout=2.5)
    assert st["mfc"] == SerialSettings("COM10", 19200, write_timeout=2.5)
    assert st["dc"] == SerialSettings("COM21", 230400, write_timeout=2.5)
    assert st["rfpulse"] == SerialSettings("COM13", 57600, parity="O", write_timeout=2.5)
    assert st["ig"] == SerialSettings("COM12", 1200, write_timeout=2.5)
    assert serial_settings(dict(CFG, PLC_PARITY="odd"))["plc"].parity == "O"
    # 장비 고정 항목(DC·MFC·IG 8N1, RF Pulse 8 데이터·1 정지)은 설정 키가 없다
    for k in ("DC_PARITY", "DC_STOPBITS", "MFC_PARITY", "MFC_STOPBITS", "IG_PARITY", "IG_STOPBITS", "RFPULSE_STOPBITS",
              "PLC_BYTESIZE"):
        assert k not in BY


# ───────── DC 종결 문자 ─────────
class DcRig:
    def __init__(self, delimiter="LF", sim_delimiter=None):
        self.c = VirtualClock(start_wall=1_760_000_000.0)
        self.c.register()
        self.cfg = dict(CFG, DC_DELIMITER=delimiter)
        self.sim = DcSim(delimiter=dev.DC_DELIMITER_BYTES[sim_delimiter or delimiter])
        self.events = []
        self.guard = G.GuardedTransport(SimTransport(self.c, self.sim), "dc", "prod", self.cfg)
        self.w = Worker("dc", self.guard, fail_streak=self.cfg["DC_FAIL_STREAK"],
                        connect_init_timeout_s=self.cfg["CONNECT_INIT_TIMEOUT_SEC"])
        self.d = DcDriver(self.c, self.w, self.cfg, True, on_event=self.events.append)
        self.w.start()
        self.c.wait_until(3.0)

    def read(self):
        box = []
        self.w.submit(Request("read", lambda s: self.d.source_read(s, 0), deadline=self.c.monotonic() + 0.5,
                              on_done=lambda r: (box.append(r), self.c.notify())))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]

    def seq(self, starter):
        box = []
        starter(on_done=lambda n, r, res: (box.append(r), self.c.notify()))
        self.c.wait_until(None, lambda: bool(box))
        return box[0]


@pytest.mark.parametrize("name", ["LF", "CR", "CRLF"])
def test_dc_delimiter_send_and_receive(name):
    """송신 종결·수신 끝·가드·시뮬레이터가 모두 그 종결 문자 — 연결·측정·켜기·끄기 확인까지."""
    term = dev.DC_DELIMITER_BYTES[name]
    r = DcRig(name)
    try:
        assert r.w.connected and r.d.model.startswith("ODA")
        assert r.read().status == "ok"
        assert r.seq(lambda on_done: r.d.start_output(450.0, 0.1, on_done=on_done)) is None
        assert r.sim.out is True
        assert r.seq(lambda on_done: r.d.output_off_confirmed(on_done=on_done)) is None
        assert r.sim.out is False and "off_confirmed" in [e.kind for e in r.events]
        sent = [b for _, b in r.sim.received]
        assert sent and all(b.endswith(term) for b in sent)
        assert all(b.count(b"\r") + b.count(b"\n") == len(term) for b in sent)           # 종결 문자 말고는 줄바꿈이 없다
        assert {b[:-len(term)] for b in sent} >= {b"*IDN?", b"MEAS:ALL?", b"OUTP ON", b"OUTP OFF", b"OUTP?"}
    finally:
        r.w.stop()


@pytest.mark.parametrize("pc,device", [("LF", "CR"), ("CR", "LF"), ("CR", "CRLF")])
def test_dc_delimiter_mismatch_is_no_response(pc, device):
    """PC 쪽과 장비 쪽 종결 문자가 다르면 장비는 답하지 않는다 — 연결되지 않는다(설정 설명의 '장비 쪽도 같은 값')."""
    r = DcRig(pc, sim_delimiter=device)
    try:
        assert r.d.model is None and not r.w.ready and r.sim.received      # 보냈지만 답이 없다
    finally:
        r.w.stop()


def test_dc_guard_uses_the_configured_delimiter():
    assert G._DC_TERM == dev.DC_DELIMITER_BYTES
    for name, term in dev.DC_DELIMITER_BYTES.items():
        chk = G.checker("dc", "prod", dict(CFG, DC_DELIMITER=name))
        assert chk(b"MEAS:ALL?" + term) is None and chk(b"OUTP OFF" + term) is None
        for other in set(dev.DC_DELIMITER_BYTES.values()) - {term}:
            assert chk(b"MEAS:ALL?" + other), (name, other)                    # 다른 종결은 형식 오류
        assert G.dc_check(b"FACT:LAST-STA FULLY" + term, "prod", False, term)   # 허용 목록 밖은 어느 종결이든 거부
        bench = G.checker("dc", "bench", dict(CFG, DC_DELIMITER=name))
        assert bench(b"MEAS:ALL?" + term) is None and bench(b"OUTP OFF" + term)
    for rx, _why in G.DC_FORBIDDEN:                                               # 금지 규칙은 느슨해지지 않았다
        assert rx.pattern


def test_chk_rig_dc_sim_follows_delimiter(tmp_path):
    from v2.chk.sim.rig import ChkRig
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path, cfg={"DC_DELIMITER": "CRLF"})
    assert rig.sims["dc"].delimiter == b"\r\n" and rig.dc.term == b"\r\n"
    c2 = VirtualClock(start_wall=1_760_000_000.0)
    rig2 = ChkRig(c2, tmp_path / "b")
    assert rig2.sims["dc"].delimiter == b"\n" and rig2.dc.term == b"\n"      # 기본(sim)은 예전과 같다


# ───────── 포트 중복 ─────────
def _errs(**kw):
    return check_config(P.PARAMS, P.CROSS_RULES, dict(CFG, **kw))


def test_port_clash_rejected_except_disabled_devices():
    assert _errs() == []
    e = _errs(MFC_PORT="COM9")
    assert len(e) == 1 and e[0].startswith("[X22]") and "COM9" in e[0] and "PLC" in e[0] and "MFC" in e[0]
    assert _errs(DC_PORT="com10")                                    # 대소문자 무시
    # RF Pulse 는 기본 꺼짐 — 꺼진 장비의 포트는 겹쳐도 된다
    assert CFG["RFPULSE_ENABLED"] is False
    assert _errs(RFPULSE_PORT="COM9") == []
    assert _errs(RFPULSE_PORT="COM9", RFPULSE_ENABLED=True)
    # 이온 게이지: 켜져 있으면 거부, 끄면 통과
    assert CFG["IG_ENABLED"] is True
    assert _errs(IG_PORT="COM11")
    assert _errs(IG_PORT="COM11", IG_ENABLED=False) == []
    # 꺼진 두 장비끼리 겹치는 것도 보지 않는다
    assert _errs(IG_PORT="COM13", IG_ENABLED=False) == []
    assert _errs(IG_PORT="COM13", RFPULSE_ENABLED=True)
    x22 = next(r for r in P.CROSS_RULES if r.id == "X22")
    assert {"RFPULSE_ENABLED", "IG_ENABLED"} <= set(x22.keys)


# ───────── 통신 시간·횟수 ─────────
NEW_TIMING = {   # 키: (기본값 = 예전 코드 상수, 하한)
    "PLC_WRITE_DEADLINE_MS": (2000, 500), "DC_START_GAP_MS": (120, 120), "DC_STEP_TIMEOUT_MS": (1500, 1500),
    "DC_CURR_WRITE_DELAY_MS": (100, 100), "RFPULSE_ACK_TIMEOUT_MS": (2000, 500), "RFPULSE_ONOFF_TIMEOUT_MS": (2500, 2500),
    "RFPULSE_ON_SETTLE_MS": (800, 0), "RFPULSE_CMD_RETRIES": (3, 1), "IG_CHAR_GAP_MS": (50, 50),
    "IG_FILAMENT_E_BLOCK_MS": (1000, 500), "PORT_WRITE_TIMEOUT_SEC": (1.0, 0.2), "CONNECT_RETRY_PAUSE_MS": (300, 100),
}


def test_new_timing_params_default_to_old_constants():
    for k, (dflt, lo) in NEW_TIMING.items():
        p = BY[k]
        assert (p.default, p.min) == (dflt, lo), k
        assert p.scope is Scope.INSTALL and p.apply is Apply.RESTART and p.v1_source
    # 안전 순서·장비 보호에 걸린 값은 지금 값보다 낮출 수 없다
    for k in ("DC_START_GAP_MS", "DC_STEP_TIMEOUT_MS", "DC_CURR_WRITE_DELAY_MS", "RFPULSE_ONOFF_TIMEOUT_MS"):
        assert BY[k].min == BY[k].default
        assert check_value(BY[k], BY[k].default - 1)
    assert BY["IG_CHAR_GAP_MS"].min == dev.IG_CHAR_GAP_MIN_MS == 50                # 매뉴얼: 글자 사이 50 ms 이상
    assert BY["DC_WRITE_QUERY_GAP_MS"].min == 100 and BY["DC_WRITE_QUERY_GAP_MS"].default == 500   # 예전 그대로
    # X42: 전류 쓰기 지연 + 질의 금지 시간이 1초를 넘으면 다음 측정이 막힌다
    assert _errs(DC_CURR_WRITE_DELAY_MS=400, DC_WRITE_QUERY_GAP_MS=700)
    assert _errs(DC_CURR_WRITE_DELAY_MS=400, DC_WRITE_QUERY_GAP_MS=600) == []


def test_drivers_read_the_timing_params():
    from v2.chk.drivers.ig import IgDriver
    from v2.chk.drivers.rfpulse import RfPulseDriver
    from v2.chk.sim.cesar_sim import CesarSim
    from v2.chk.sim.ig_sim import IgSim
    c = VirtualClock(start_wall=1_760_000_000.0)
    cfg = dict(CFG, DC_START_GAP_MS=300, DC_STEP_TIMEOUT_MS=2000, DC_CURR_WRITE_DELAY_MS=200, RFPULSE_ACK_TIMEOUT_MS=3000,
               RFPULSE_ONOFF_TIMEOUT_MS=4000, RFPULSE_ON_SETTLE_MS=500, RFPULSE_CMD_RETRIES=5, IG_CHAR_GAP_MS=200,
               IG_FILAMENT_E_BLOCK_MS=2000)
    dc = DcDriver(c, Worker("dc", SimTransport(c, DcSim())), cfg, True)
    assert (dc.start_gap_s, dc.step_timeout_s, dc.curr_delay_s) == (0.3, 2.0, 0.2)
    rfp = RfPulseDriver(c, Worker("rfp", SimTransport(c, CesarSim())), cfg, True)
    assert (rfp.ack_timeout_s, rfp.onoff_timeout_s, rfp.on_settle_s, rfp.cmd_retries) == (3.0, 4.0, 0.5, 5)
    ig = IgDriver(c, Worker("ig", SimTransport(c, IgSim())), cfg, True)
    assert (ig.char_gap_s, ig.e_block_s) == (0.2, 2.0)
    w = Worker("x", SimTransport(c, DcSim()), connect_retry_pause_s=1.5)
    assert w.connect_retry_pause_s == 1.5 and Worker("y", SimTransport(c, DcSim())).connect_retry_pause_s == 0.3


def test_ig_char_gap_spaces_two_letters():
    """글자 사이 간격: 앞 글자를 보낸 뒤 IG_CHAR_GAP_MS 안에는 다음 글자를 보내지 않는다."""
    from v2.chk.drivers.ig import IgDriver
    from v2.chk.sim.ig_sim import IgSim
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    sim = IgSim()
    w = Worker("ig", SimTransport(c, sim), fail_streak=CFG["IG_FAIL_STREAK"])
    d = IgDriver(c, w, dict(CFG, IG_CHAR_GAP_MS=600), True)
    w.start()
    c.wait_until(2.0)
    try:
        box = []
        for ch in ("F", "F"):
            w.submit(Request("read", lambda s, ch=ch: d._query(s, ch), deadline=c.monotonic() + 5.0,
                             on_done=lambda r: (box.append(r), c.notify())))
        c.wait_until(None, lambda: len(box) == 2)
        assert [r.status for r in box] == ["ok", "ok"]
        assert box[1].finished - box[0].started >= 0.6
    finally:
        w.stop()


# ───────── 웹 묶음 ─────────
def test_bundle_exports_new_keys_choices_and_groups():
    b = W.bundle(CFG)
    ps = {x["key"]: x for x in b["params"]}
    for k, bauds in BAUDS.items():
        assert ps[k]["type"] == "int" and ps[k]["choices"] == list(bauds) and ps[k]["enum"] is None
        assert ps[k]["scope"] == "install" and ps[k]["apply"] == "restart"
        assert "PC 쪽 설정이다. 장비 쪽도 같은 값이어야 한다" in ps[k]["desc"]
    assert ps["PLC_STOPBITS"]["choices"] == [1, 2] and ps["PLC_STOPBITS"]["type"] == "int"
    assert ps["PLC_PARITY"]["enum"] == ["none", "even", "odd"] and ps["PLC_PARITY"]["choices"] is None
    assert ps["DC_DELIMITER"]["enum"] == ["LF", "CR", "CRLF"] and ps["DC_DELIMITER"]["default"] == "LF"
    assert ps["FLOW_ERROR_SEC"]["choices"] is None                                   # 그 밖의 키는 목록 없음
    for k in ("PLC_PARITY", "PLC_STOPBITS", "PLC_SLAVE_ID", "DC_DELIMITER", "RFPULSE_PARITY", "RFPULSE_ADDR"):
        assert "PC 쪽 설정이다. 장비 쪽도 같은 값이어야 한다" in ps[k]["desc"], k
    for k, where in (("PLC_BAUD", "XG5000 기본 파라미터 Cnet"), ("DC_BAUD", "앞면 I/O 메뉴"), ("RFPULSE_BAUD", "앞면"),
                     ("IG_BAUD", "내부 DIP S1"), ("MFC_BAUD", "장비 메뉴")):
        assert where in ps[k]["desc"], k
    assert "unit address is always 1" in ps["RFPULSE_ADDR"]["desc"] and ps["RFPULSE_ADDR"]["default"] == 1
    assert (ps["RFPULSE_ADDR"]["min"], ps["RFPULSE_ADDR"]["max"]) == (0, 31)
    for k in NEW_TIMING:
        assert k in ps, k
    # hello 정의(schema)에도 허용 숫자 목록이 나간다
    sch = {x["key"]: x for x in schema_dict(P.PARAMS, P.CROSS_RULES)["params"]}
    assert sch["DC_BAUD"]["choices"] == list(dev.DC_BAUDS) and sch["FLOW_ERROR_SEC"]["choices"] is None
    # 장비별 묶음: 포트·속도·패리티·시간이 같은 장비끼리
    groups = dict((g["key"], g["label"]) for g in b["paramGroups"])
    assert [groups[k] for k in ("comm_plc", "comm_mfc", "comm_dc", "comm_rfpulse", "comm_ig")] == \
        ["PLC 통신", "MFC 통신", "DC Power 통신", "RF Pulse 통신", "이온 게이지 통신"]
    for g, keys in P.COMM_GROUPS:
        for k in keys:
            if k in ps:
                assert ps[k]["group"] == g, k
    for dev_key, g in (("PLC", "comm_plc"), ("MFC", "comm_mfc"), ("DC", "comm_dc"), ("RFPULSE", "comm_rfpulse"),
                       ("IG", "comm_ig")):
        assert ps[f"{dev_key}_PORT"]["group"] == ps[f"{dev_key}_BAUD"]["group"] == ps[f"{dev_key}_FAIL_STREAK"]["group"] == g
    assert ps["DC_DELIMITER"]["group"] == "comm_dc" and ps["PLC_PARITY"]["group"] == ps["PLC_STOPBITS"]["group"] == "comm_plc"
    assert ps["EQUIPMENT_ID"]["group"] == "install" and ps["RUN_MODE"]["default"] == "sim"
