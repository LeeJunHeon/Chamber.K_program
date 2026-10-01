# -*- coding: utf-8 -*-
"""integrations.erp_state — Qt 없이, 가짜 src 로 상태 dict 만들기와 1회 오류 보고를 본다."""
import ast
import glob
import os

import pytest

from integrations.erp_state import ErpStatePublisher, ErpStateSource, build_state

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Hold:
    def __init__(self, holding=True, kind="tc2"):
        self._h, self.kind = holding, kind

    def is_holding(self):
        return self._h


class Recipe:
    def __init__(self, running=False, progress=None):
        self._r, self._p = running, progress or {"state": "IDLE"}

    def is_running(self):
        return self._r

    def progress(self):
        return dict(self._p)


class FakeSrc:
    """ErpStateSource 대역 — 읽은 순서를 reads 에 남긴다."""

    def __init__(self, **kw):
        self.reads = []
        self.texts, self.checks = {}, {}
        self.running = False
        self._meas = {}
        self.name = ""
        self.remain, self.total = -1, 0
        self.path, self.index, self.rows, self.mode = "", -1, [], False
        self.out, self.atm = "정지 (DAC 0)", {"state": "idle"}
        self.info, self.dev_ok, self.hold, self.badge, self.recipe = {}, False, None, None, None
        self.ind, self.bits, self.valv, self.link = {}, {}, {}, True
        self.sent, self.events, self.err = [], [], False
        self.raise_on = None
        self.__dict__.update(kw)

    def _r(self, name, *a):
        self.reads.append((name, *a))
        if self.raise_on == name:
            raise RuntimeError(f"{name} 실패(가짜)")

    def text(self, name): self._r("text", name); return self.texts.get(name, "")
    def checked(self, name): self._r("checked", name); return self.checks.get(name, False)
    def process_running(self): self._r("process_running"); return self.running
    def meas(self): self._r("meas"); return self._meas
    def current_name(self): self._r("current_name"); return self.name
    def main_remain_sec(self): self._r("main_remain_sec"); return self.remain
    def main_total_sec(self): self._r("main_total_sec"); return self.total
    def delay_active(self): self._r("delay_active"); return getattr(self, "d_active", False)      # E5
    def delay_remaining_sec(self): self._r("delay_remaining_sec"); return getattr(self, "d_remain", 0)
    def delay_total_sec(self): self._r("delay_total_sec"); return getattr(self, "d_total", 0)
    def csv_file_path(self): self._r("csv_file_path"); return self.path
    def recipe_name(self): self._r("recipe_name"); return getattr(self, "rname", "")
    def csv_index(self): self._r("csv_index"); return self.index
    def csv_rows(self): self._r("csv_rows"); return self.rows
    def csv_mode(self): self._r("csv_mode"); return self.mode
    def heater_output_text(self): self._r("heater_output_text"); return self.out
    def heater_atmosphere(self): self._r("heater_atmosphere"); return self.atm
    def heater_info(self): self._r("heater_info"); return self.info
    def heater_dev_ok(self): self._r("heater_dev_ok"); return self.dev_ok
    def heater_hold(self): self._r("heater_hold"); return self.hold
    def heater_badge(self): self._r("heater_badge"); return self.badge
    def heater_recipe(self): self._r("heater_recipe"); return self.recipe
    def indicators(self): self._r("indicators"); return self.ind
    def plc_bits(self): self._r("plc_bits"); return self.bits
    def valves(self): self._r("valves"); return self.valv
    def plc_link_up(self): self._r("plc_link_up"); return self.link
    def erp_update_state(self, state): self.sent.append(state)
    def erp_event(self, level, msg): self.events.append((level, msg))
    def snap_err_reported(self): return self.err
    def set_snap_err_reported(self, v): self.err = v

    def names(self):
        return [r[0] for r in self.reads]


def test_fake_src_covers_protocol():
    wanted = {n for n in vars(ErpStateSource) if not n.startswith("_")}
    assert wanted <= set(vars(FakeSrc))


def _items(state, group):
    return {i["label"]: i for g in state["groups"] if g["label"] == group for i in g["items"]}


PV_WIDGETS = {"Power_edit": "100.2", "Voltage_edit": "351.0", "Current_edit": "0.29", "for_p_edit": "5",
              "ref_p_edit": "1", "rfp_for_p_edit": "80", "rfp_ref_p_edit": "2"}
SV_WIDGETS = {"DC_power_edit": "100", "RF_power_edit": "0", "offset_edit": "6.79", "param_edit": "1.0395",
              "rfp_power_edit": "", "rfp_freq_edit": "5", "rfp_duty_edit": "50", "Ar_flow_edit": "20",
              "O2_flow_edit": "", "working_pressure_edit": "5", "process_time_edit": "1", "Shutter_delay_edit": "1"}


def test_idle_blanks_pv_and_skips_meas_and_process():
    src = FakeSrc(texts=dict(PV_WIDGETS, **SV_WIDGETS, stage_monitor="A\n공정 종료"),
                  _meas={"flow": {"Ar": 19.9}, "pressure": 5.0})
    st = build_state(src)
    assert st["status"] == "idle" and st["stage"] == "공정 종료"
    p = _items(st, "전원")
    assert p["DC Power"] == {"label": "DC Power", "value": "", "setpoint": "100", "unit": "W"}
    assert p["Voltage"]["value"] == "" and _items(st, "RF Pulse")["for.P"]["value"] == ""
    assert _items(st, "RF")["offset / param"]["value"] == "6.79 / 1.0395"
    assert _items(st, "가스")["Ar"] == {"label": "Ar", "value": None, "setpoint": "20", "unit": "sccm"}
    assert _items(st, "압력 · 시간")["챔버 압력"]["value"] is None
    assert "meas" not in src.names() and "process" not in st
    for n in PV_WIDGETS:                                   # 대기 중엔 계측 위젯을 읽지도 않는다
        assert ("text", n) not in src.reads


def test_running_uses_measurements_and_process_phase():
    src = FakeSrc(running=True, texts=dict(PV_WIDGETS, **SV_WIDGETS), _meas={"flow": {"Ar": 19.9, "O2": None},
                                                                              "pressure": 4.8},
                  name=None, remain=-1, total=0)
    st = build_state(src)
    assert _items(st, "전원")["Voltage"]["value"] == "351.0" and _items(st, "가스")["Ar"]["value"] == 19.9
    assert _items(st, "압력 · 시간")["챔버 압력"]["value"] == 4.8
    assert st["process"] == {"name": "", "remainSec": -1, "totalSec": 0, "phase": "pre"}
    src = FakeSrc(running=True, name="Single CHK", remain=0, total=60)
    assert build_state(src)["process"] == {"name": "Single CHK", "remainSec": 0, "totalSec": 60, "phase": "main"}
    src = FakeSrc(running=True, name="X", remain="30", total="60")
    assert build_state(src)["process"]["remainSec"] == 30            # int() 변환 그대로
    assert list(build_state(FakeSrc(running=True)))[-1] == "process"    # 맨 뒤에 붙는다


@pytest.mark.parametrize("rows", [None, []])
def test_csv_recipe_none(rows):
    assert build_state(FakeSrc(rows=rows))["csvRecipe"] is None


def test_csv_recipe_loaded_and_running():
    rows = [{"Process_name": "S1", "Ar": 1, "dc_power": None}, {"Process_name": "", "Ar_flow": "20"}]
    st = build_state(FakeSrc(rows=rows, path="C:/r/레시피.csv", index=-1, mode=False))["csvRecipe"]
    assert (st["name"], st["stepNo"], st["total"], st["active"], st["steps"]) == ("레시피.csv", 0, 2, False, ["S1", "STEP2"])
    assert st["rows"][0]["Ar"] == "1" and st["rows"][0]["dc_power"] == "" and st["rows"][1]["Process_name"] == ""
    assert len(st["rows"][0]) == 24 and list(st["rows"][0])[:2] == ["Process_name", "Ar"]
    st = build_state(FakeSrc(rows=rows, path=None, index=1, mode=True))["csvRecipe"]
    assert (st["name"], st["stepNo"], st["active"]) == (None, 2, True)


def test_target_groups():
    assert [g["label"] for g in build_state(FakeSrc())["groups"]] == ["전원", "RF", "RF Pulse", "가스", "압력 · 시간"]
    st = build_state(FakeSrc(checks={"G2_checkbox": True}))
    assert st["groups"][-1] == {"label": "타겟", "items": [{"label": "G2", "value": "미입력"}]}
    st = build_state(FakeSrc(checks={"G1_checkbox": True, "G2_checkbox": True}, texts={"G1_edit": "CeO2", "G2_edit": "Ti"}))
    assert st["groups"][-1]["items"] == [{"label": "G1", "value": "CeO2"}, {"label": "G2", "value": "Ti"}]


def test_heater_block_lcd_and_tc2_hold():
    info = {"cur_sv": 512.5, "pid_err": 3, "ot_limit": 650, "run": 1, "fault": 0, "tc_err": None, "wd_err": 1, "ot": 0}
    src = FakeSrc(info=info, dev_ok=1, hold=Hold(True, "tc2"), badge=None, recipe=Recipe(True, {"state": "RAMPING"}),
                  texts={"heater_sv_big": "600.0", "heater_pv2_label": "TC2 880.0 °C"}, checks={"heater_onoff_button": True})
    st = build_state(src)
    h = st["heater"]
    assert (h["curSv"], h["pidErr"], h["otLimit"], h["run"], h["fault"], h["tcErr"], h["wdErr"], h["ot"]) == \
        (512.5, 3, 650, True, False, False, True, False)
    assert h["lcd"] == {"sv": "600.0", "dev": "", "devOk": True, "tc2": "TC2 880.0 °C", "tc2Hold": True, "badge": ""}
    assert h["on"] is True and h["recipeRunning"] is True and st["heaterRecipe"] == {"state": "RAMPING"}
    assert src.names().count("heater_info") == 8 and src.names().count("heater_hold") == 3   # 읽는 횟수 그대로
    assert build_state(FakeSrc(hold=Hold(True, "dac")))["heater"]["lcd"]["tc2Hold"] is False
    src = FakeSrc(hold=None)
    assert build_state(src)["heater"]["lcd"]["tc2Hold"] is False and src.names().count("heater_hold") == 1
    st = build_state(FakeSrc(recipe=None))
    assert st["heater"]["recipeRunning"] is False and st["heaterRecipe"] is None


def test_read_order_output_then_atmosphere_then_recipe():
    src = FakeSrc(recipe=Recipe())
    build_state(src)
    n = src.names()
    assert n.index("heater_output_text") < n.index("heater_atmosphere") < n.index("heater_recipe")
    assert n.count("heater_output_text") == 1 and n.count("heater_atmosphere") == 1
    assert n.count("heater_recipe") == 3                  # recipeRunning 1회 + heaterRecipe(조건·호출) 2회


def test_plc_block():
    st = build_state(FakeSrc(ind={"ION_RUN": 1, "ION_OT": True, "Air": False}, bits={"MV_INTERLOCK": False},
                             valv={"MV": True}, link=0))
    assert st["ion"] == {"run": True, "lamp": False, "overtime": True}
    assert st["indicators"] == {"ION_RUN": 1, "ION_OT": True, "Air": False}
    assert st["mvInterlock"] is False and st["valves"] == {"MV": True} and st["plc_link"] is False
    assert build_state(FakeSrc(bits=None))["mvInterlock"] is None


def test_state_key_order():
    assert list(build_state(FakeSrc())) == ["status", "stage", "groups", "heater", "heaterRecipe", "csvRecipe",
                                            "ion", "indicators", "mvInterlock", "valves", "plc_link"]


# ───────────────────────── 보내기·오류 1회 보고 ─────────────────────────
def test_tick_sends_state():
    src = FakeSrc()
    ErpStatePublisher(src).tick()
    assert len(src.sent) == 1 and src.sent[0]["status"] == "idle" and src.events == []


def test_tick_reports_error_once_then_resumes():
    src = FakeSrc(raise_on="heater_output_text")
    pub = ErpStatePublisher(src)
    pub.tick(); pub.tick()
    assert src.sent == [] and src.events == [("error", "스냅샷 수집 실패: RuntimeError: heater_output_text 실패(가짜)")]
    assert src.err is True
    src.raise_on = None
    pub.tick()
    assert len(src.sent) == 1 and src.err is False                           # E6: 복구되면 표시를 내리고
    assert src.events[-1] == ("info", "스냅샷 수집 복구") and len(src.events) == 2   # 복구를 한 번 알린다


def test_tick_swallows_errors_while_reporting():
    class Boom(FakeSrc):
        def erp_event(self, level, msg):
            raise RuntimeError("리포터 죽음")
    src = Boom(raise_on="text")
    ErpStatePublisher(src).tick()                          # 밖으로 예외가 나오지 않는다


# ───────────────────────── 정적 검사 ─────────────────────────
def test_integrations_import_rules_cover_erp_state():
    files = glob.glob(os.path.join(ROOT, "integrations", "*.py"))
    assert any(f.endswith("erp_state.py") for f in files)
    bad = []
    for f in files:
        tree = ast.parse(open(f, encoding="utf-8").read(), f)
        for node in ast.walk(tree):
            mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                    else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
            for m in mods:
                if m.split(".")[0] in ("PyQt6", "UI", "main") or m == "lib.logger" or m.startswith("lib.logger."):
                    bad.append((os.path.basename(f), m))
    assert bad == []
