"""명령 인자 검사(사용자 지시 5-9 ②) — 명령 표(ArgDef) 하나로: 형식·범위 밖은 BAD_ARGS + detail(이유 문구).

- HEATER_SV·HEATER_ON value 는 min(HEATER_MAX_TEMP, PLC 보고 상한 heater.sv_limit — 0 보다 클 때)까지(v1 main.py _read_heater_sv_input)
- 변환 예외(float("abc") 등)가 INTERNAL_ERROR 로 나가지 않는다 — 모든 명령 정의를 돌며 맞는 인자·빠진 인자·틀린 형식
- base_pressure 0 = 빈 값(사용자 지시 5-9 ①)
- 엔진이 문구와 함께 거부하면 첫 문구를 detail 로(5-9 추가) — 히터 가스 준비, 히터 레시피 검사, 행 검사, 공정 중 수정
"""
import pytest

from v2.common.commands import Source
from v2.common.paramdef import defaults
from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import process_steps as PSt
from v2.chk import recipe as RC
from v2.chk.hub import MessageHub
from v2.tests.test_process_engine import FAST, R, SHORT

CFG = defaults(P.PARAMS)
K = C.by_kind()


def chk(kind, args, sv_limit=None, cfg=CFG):
    return C.check_args(K[kind], args, cfg, sv_limit)


@pytest.mark.parametrize("value", [5000, -50, "abc", None, True, float("nan"), float("inf"), [1]])
def test_heater_sv_bad_values(value):
    assert chk("HEATER_SV", {"value": value})


def test_heater_sv_detail_text_and_limits():
    assert chk("HEATER_SV", {"value": 5000}) == "value 5000 — 범위 0 ~ 700 °C (HEATER_MAX_TEMP)"
    assert chk("HEATER_SV", {"value": "abc"}) == "value 'abc' — 숫자여야 함"
    assert chk("HEATER_SV", {}) == "value — 값 필요"
    assert chk("HEATER_SV", {"value": 0}) is None and chk("HEATER_SV", {"value": 700}) is None
    assert chk("HEATER_SV", {"value": 650}, sv_limit=600) == "value 650 — 범위 0 ~ 600 °C (PLC 상한 D00013)"
    assert chk("HEATER_SV", {"value": 600}, sv_limit=600) is None
    assert chk("HEATER_SV", {"value": 700}, sv_limit=0) is None             # 0 = PLC 가 상한을 안 줌(v1)
    assert chk("HEATER_SV", {"value": 700}, sv_limit=900) is None           # 더 큰 PLC 상한은 HEATER_MAX_TEMP 를 넘지 않는다
    assert chk("HEATER_SV", {"value": 701}, sv_limit=900).endswith("(HEATER_MAX_TEMP)")
    assert chk("HEATER_ON", {"value": 650}, sv_limit=600)                   # HEATER_ON 도 같은 상한
    assert chk("HEATER_SV", {"value": 100, "extra": "x"}) is None           # 모르는 인자는 무시


def test_other_commands():
    assert chk("HEATER_ON", {"value": 100, "use_ar": True, "ar_flow": CFG["MFC_AR_FS_SCCM"] + 1}).startswith("ar_flow")
    assert chk("HEATER_ON", {"value": 100, "use_ar": True, "ar_flow": CFG["MFC_AR_FS_SCCM"]}) is None
    assert chk("HEATER_ON", {"value": 100, "use_ar": "yes"}) == "use_ar 'yes' — true/false 여야 함"
    assert chk("PLC_BUTTON", {"target": "mv", "on": True}) is None
    assert chk("PLC_BUTTON", {"target": "mv", "on": 1})
    assert chk("PLC_BUTTON", {"target": "nope", "on": True}).startswith("target 'nope' — rotary·")
    assert chk("PLC_BUTTON", {"on": True}) == "target — 값 필요"
    assert chk("IG_FILAMENT", {"on": "true"}) and chk("IG_FILAMENT", {"on": False}) is None
    assert chk("HEATER_RECIPE_HOLD", {"on": 0}) and chk("HEATER_RECIPE_HOLD", {"on": True}) is None
    ok = {"values": {}, "schemaHash": "h"}
    assert chk("CONFIG_APPLY", dict(ok, version=0)) == "version 0 — 1 이상"
    assert chk("CONFIG_APPLY", dict(ok, version=1.5)) == "version 1.5 — 정수여야 함"
    assert chk("CONFIG_APPLY", dict(ok, version=2.0)) is None and chk("CONFIG_APPLY", dict(ok, version=3)) is None
    assert chk("CONFIG_APPLY", dict(ok, version=1, values=[])) == "values — 객체(dict)여야 함"
    assert chk("RECIPE_LOAD", {"name": "a", "rows": {}}) == "rows — 목록이어야 함"
    assert chk("PROCESS_ADJUST", {"device": "dc", "item": "power", "value": "x"}) is None   # any — ADJUST_ITEMS 가 본다
    assert chk("PROCESS_ADJUST", {"device": "dc", "item": "power", "value": 1, "source": "ai"}).startswith("source")


def _good(a):
    if a.type in ("float", "int"):
        lo = CFG[a.param_min] if a.param_min else a.min
        v = lo if lo is not None else 1
        return int(v) if a.type == "int" else float(v)
    return {"bool": True, "str": "x", "enum": (a.enum or ("",))[0], "obj": {}, "rows": [], "any": 1.0}[a.type]


_WRONG = {"float": "abc", "int": "abc", "bool": "yes", "str": 5, "enum": "zz", "obj": [], "rows": {}}


def _cases():
    for c in C.COMMANDS:
        good = {a.name: _good(a) for a in c.args}
        yield c.kind, good, False
        for a in c.args:
            if a.required:
                yield c.kind, {k: v for k, v in good.items() if k != a.name}, True
            if a.type in _WRONG:
                yield c.kind, dict(good, **{a.name: _WRONG[a.type]}), True
            if a.type in ("float", "int"):
                yield c.kind, dict(good, **{a.name: True}), True


def test_check_args_table_cases():
    for kind, args, bad in _cases():
        assert bool(chk(kind, args)) == bad, (kind, args)


def test_every_command_no_internal_error(tmp_path):
    r = R(tmp_path, FAST)
    got = []

    def go():
        for kind, args, bad in _cases():
            for src in K[kind].sources:
                res = r.rig.core.command(kind, dict(args), source=src, cid=f"{kind}-{len(got)}")
                got.append((kind, src, args, bad, res))
                if r.rig.process.active:
                    r.rig.core.command("PROCESS_STOP", {})
    r.on(5, go)
    r.run(10)
    assert len(got) > 100
    assert [g for g in got if g[4].reason == "INTERNAL_ERROR"] == []
    for kind, src, args, bad, res in got:
        if bad and src != Source.INTERNAL and C.check_command(kind, src, "IDLE") is None:
            assert res.reason == "BAD_ARGS" and res.detail, (kind, args, res)
        if res.reason is None:
            assert res.detail == "", (kind, args, res)


def test_core_heater_sv_rejects_with_detail_and_uses_plc_limit(tmp_path):
    r = R(tmp_path, FAST)
    out = {}

    def go():
        core = r.rig.core
        out["5000"] = core.command("HEATER_SV", {"value": 5000}, cid="a")
        out["neg"] = core.command("HEATER_SV", {"value": -50}, cid="b")
        out["abc"] = core.command("HEATER_SV", {"value": "abc"}, cid="c")
        out["null"] = core.command("HEATER_SV", {"value": None}, cid="d")
        out["true"] = core.command("HEATER_SV", {"value": True}, cid="e")
        out["0"] = core.command("HEATER_SV", {"value": 0}, cid="f")
        out["700"] = core.command("HEATER_SV", {"value": 700}, cid="g")
        r.rig.heater.st = dict(r.rig.heater.st, sv_limit=600.0)
        out["650"] = core.command("HEATER_SV", {"value": 650}, cid="h")
        out["on650"] = core.command("HEATER_ON", {"value": 650}, cid="i")
        out["ar"] = core.command("HEATER_ON", {"value": 100, "use_ar": True, "ar_flow": 1000.0, "wp_mtorr": 5.0}, cid="j")
        out["hmi"] = core.command("PLC_BUTTON", {"target": "mv", "on": "x"}, source=Source.HMI, cid="k")
    r.on(5, go)
    r.run(8)
    for k in ("5000", "neg", "abc", "null", "true", "650", "on650", "ar"):
        assert out[k].reason == "BAD_ARGS" and out[k].detail, k
    assert out["5000"].detail == "value 5000 — 범위 0 ~ 700 °C (HEATER_MAX_TEMP)"
    assert out["650"].detail == "value 650 — 범위 0 ~ 600 °C (PLC 상한 D00013)"
    assert out["ar"].detail.startswith("ar_flow 1000 — 범위 0 ~ ")
    assert out["0"].reason != "BAD_ARGS" and out["700"].reason != "BAD_ARGS"
    assert out["hmi"].reason == "SOURCE_NOT_ALLOWED" and out["hmi"].detail == ""   # 경로·상태가 먼저
    res = {e["data"]["cmdId"]: e["data"] for e in r.rig.hub.recent if e["type"] == "cmd_result"}
    assert res["a"]["detail"] == out["5000"].detail and res["a"]["reasonCode"] == "BAD_ARGS"
    assert res["f"]["detail"] == "" or res["f"]["status"] == "rejected"


def test_cmd_result_detail_is_only_the_reason():
    class Res:
        ok, reason, detail = True, None, ""
    d = MessageHub.cmd_result_data("x", "PROCESS_STOP", Res(), Source.HMI, "local")
    assert d["detail"] == "" and d["status"] == "accepted"


# ───────────── base_pressure 0 = 빈 값(5-9 ①) ─────────────
ROW = {"process_name": "b", "ar": True, "ar_flow": 20.0, "working_pressure": 3.0, "process_time": 10.0,
       "use_dc": True, "dc_power": 100.0, "gun1": True}


@pytest.mark.parametrize("ig", [True, False])
def test_base_pressure_zero_is_empty(ig):
    cfg = dict(CFG, IG_ENABLED=ig)
    outs = []
    for v in (0, 0.0, "", None, "missing"):
        row = dict(ROW) if v == "missing" else dict(ROW, base_pressure=v)
        clean, errs, warns = RC.check_process_row(row, cfg)
        assert errs == [] and warns == [], (v, errs)
        p = PSt.row_to_params(clean, cfg)
        outs.append((clean, p, BK.template_blocks(p, cfg, ig_enabled=ig, rfpulse_present=False)))
    assert all(o == outs[0] for o in outs)
    assert "base_pressure" not in outs[0][0]
    assert not any(b["type"] == "base_wait" for b in outs[0][2])


def test_base_pressure_negative_and_out_of_range_rejected():
    for v, ig in ((-1e-6, True), (-1.0, False), (1e-10, True), (5e-3, True), (True, True), ("x", True)):
        _, errs, _ = RC.check_process_row(dict(ROW, base_pressure=v), dict(CFG, IG_ENABLED=ig))
        assert errs and errs[0].startswith("base_pressure"), (v, errs)
    clean, errs, _ = RC.check_process_row(dict(ROW, base_pressure=5e-6), CFG)
    assert errs == [] and clean["base_pressure"] == 5e-6
    _, errs, _ = RC.check_process_row(dict(ROW, base_pressure=5e-6), dict(CFG, IG_ENABLED=False))
    assert errs == ["base_pressure: 이온 게이지가 꺼져 있음(IG_ENABLED=false)"]


def test_base_pressure_zero_is_empty_exported():
    from v2.chk import web_schema as W
    cols = {c["name"]: c for c in W.definitions()["recipe"]["processColumns"]}
    assert cols["base_pressure"]["zeroIsEmpty"] is True
    assert [n for n, c in cols.items() if c["zeroIsEmpty"]] == ["base_pressure"]


def test_load_with_base_pressure_zero_and_ig_off(tmp_path):
    r = R(tmp_path, dict(FAST, IG_ENABLED=False))
    r.on(3, r.cmd("RECIPE_LOAD", {"name": "z", "rows": [dict(SHORT, base_pressure=0)]}))
    r.on(4, r.cmd("RECIPE_LOAD", {"name": "n", "rows": [dict(SHORT, base_pressure=5e-6)]}))
    r.run(6)
    assert r.results[0].ok, r.results[0]
    assert r.results[1].reason in ("RECIPE_INVALID", "IG_DISABLED")


# ───────────── 엔진 거부 이유도 detail 로(5-9 추가) ─────────────
def _open_mv(r):
    from v2.chk.devices import PLC_ADDRS as A
    r.plc.coils[A["mv"]] = True


def test_engine_reject_reasons_become_detail(tmp_path):
    r = R(tmp_path, FAST)
    out = {}

    def go():
        core = r.rig.core
        heater = r.rig.heater
        heater.st = dict(heater.st, itl=True, fault=False, run=False)
        out["ar0"] = core.command("HEATER_ON", {"value": 100, "use_ar": True, "wp_mtorr": 5.0}, cid="h1")
        out["arneg"] = core.command("HEATER_ON", {"value": 100, "use_ar": True, "ar_flow": 0.0, "wp_mtorr": 5.0}, cid="h2")
        out["o2"] = core.command("HEATER_ON", {"value": 100, "use_o2": True, "wp_mtorr": 5.0}, cid="h3")
        out["wp"] = core.command("HEATER_ON", {"value": 100, "use_ar": True, "ar_flow": 10.0}, cid="h4")
        out["hrec"] = core.command("HEATER_RECIPE_RUN", {"name": "x", "steps": []}, cid="h5")
        out["hrec2"] = core.command("HEATER_RECIPE_RUN", {"name": "x", "steps": [{"target_c": 5000}]}, cid="h6")
        out["start"] = core.command("PROCESS_START", {"row": dict(SHORT, ar=None, ar_flow=None)}, cid="p1")
        out["delay"] = core.command("PROCESS_START", {"row": {"process_name": "delay 5"}}, cid="p2")
        out["load"] = core.command("RECIPE_LOAD", {"name": "z", "rows": [dict(SHORT), dict(SHORT, dc_power=99999.0)]}, cid="l1")
        out["load_empty"] = core.command("RECIPE_LOAD", {"name": "z", "rows": []}, cid="l2")
        out["load_ig"] = core.command("RECIPE_LOAD", {"name": "z", "rows": [dict(SHORT, base_pressure=5e-6)]}, cid="l3")
        out["blocks"] = core.command("RECIPE_LOAD", {"name": "b", "blocks": [{"type": "nope"}]}, cid="b1")
    r.on(5, go)
    r.run(8)
    exp = {"ar0": ("BAD_ARGS", "Ar 유량이 필요합니다(0 보다 커야 함)"),
           "arneg": ("BAD_ARGS", "Ar 유량이 필요합니다(0 보다 커야 함)"),
           "o2": ("BAD_ARGS", "O2 유량이 필요합니다(0 보다 커야 함)"),
           "wp": ("BAD_ARGS", "압력(wp_mtorr)이 필요합니다(0 보다 커야 함)"),
           "hrec": ("RECIPE_INVALID", "스텝 없음"),
           "load_empty": ("RECIPE_INVALID", "행 목록이 비어 있음"),
           "delay": ("RECIPE_INVALID", "수동 시작은 딜레이 행을 받지 않음")}
    for k, (code, detail) in exp.items():
        assert (out[k].reason, out[k].detail) == (code, detail), (k, out[k])
    assert out["hrec2"].reason == "RECIPE_INVALID" and out["hrec2"].detail.startswith("스텝 1: target_c")
    assert out["load"].reason == "RECIPE_INVALID" and out["load"].detail.startswith("2행: dc_power: 99999")
    assert out["start"].reason == "RECIPE_INVALID" and out["start"].detail == "가스: Ar 또는 O2 중 하나 이상"
    assert out["blocks"].reason == "RECIPE_INVALID" and out["blocks"].detail
    assert out["load_ig"].reason in ("IG_DISABLED", None)
    res = {e["data"]["cmdId"]: e["data"] for e in r.rig.hub.recent if e["type"] == "cmd_result"}
    assert res["h1"]["detail"] == "Ar 유량이 필요합니다(0 보다 커야 함)" and res["l1"]["detail"] == out["load"].detail
    assert all(len(d["detail"]) <= 300 for d in res.values())


def test_engine_detail_is_capped_and_not_stale(tmp_path):
    r = R(tmp_path, FAST)
    out = {}

    def go():
        core = r.rig.core
        long_rows = [dict(SHORT, process_name="x" * 50, **{"k" * 400: 1})]
        out["long"] = core.command("RECIPE_LOAD", {"name": "z", "rows": long_rows})
        out["ok"] = core.command("RECIPE_LOAD", {"name": "z", "rows": [dict(SHORT)]})
        out["running"] = core.command("PROCESS_START", {"row": dict(SHORT)})
    r.on(5, go)
    r.run(8)
    assert out["long"].reason == "RECIPE_INVALID" and 0 < len(out["long"].detail) <= 300
    assert out["ok"].ok and out["ok"].detail == ""
    assert out["running"].reason == "RECIPE_LOADED" and out["running"].detail == ""   # 지난 거부 문구가 남지 않는다


def test_adjust_reject_reasons_become_detail(tmp_path):
    r = R(tmp_path, FAST)
    got = {}
    LONG = dict(SHORT, process_time=2.0)
    r.on(4, r.cmd("PROCESS_START", {"row": LONG}))

    def at_main():
        core = r.rig.core
        got["type"] = core.command("PROCESS_ADJUST", {"device": "dc", "item": "power", "value": "x"})
        got["item"] = core.command("PROCESS_ADJUST", {"device": "dc", "item": "volts", "value": 1.0})
        got["range"] = core.command("PROCESS_ADJUST", {"device": "dc", "item": "power", "value": 5000.0})
        got["used"] = core.command("PROCESS_ADJUST", {"device": "rf", "item": "power", "value": 50.0})
    r.when(lambda: r.rig.process.state == "MAIN", at_main)
    r.when(lambda: "used" in got, r.cmd("PROCESS_STOP"))
    r.run(160)
    assert got["type"].reason == "BAD_ARGS" and "숫자" in got["type"].detail
    assert (got["item"].reason, got["item"].detail) == ("BAD_ARGS", "dc.volts — 모르는 항목")
    assert got["range"].reason == "ADJUST_OUT_OF_RANGE" and got["range"].detail
    assert got["used"].reason == "ADJUST_NOT_USED" and got["used"].detail == ""
