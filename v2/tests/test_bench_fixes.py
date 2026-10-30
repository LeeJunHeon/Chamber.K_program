"""bench 시험(10/29)에서 나온 문제 고치기(사용자 지시 5-14) — 장비·포트 없이(sim·가상 시계·임시 폴더).

- 이온 게이지 상태 열 ig.ion_state·ig.a_state·ig.b_state: 화면 이름이 v1 문구와 같다(v1 device/IonGauge.py 는 글로만 읽음 —
  import 하지 않음), below 는 GAUGE_MIN_TORR 로 만든다, 값 열 stateColumn 짝, 행·hello·CSV·1분 요약이 깨지지 않는다
  (같은 날 열이 늘어난 버전으로 다시 시작 포함), bench_probe·inventory 문구가 같은 함수.
- 설정 파일 BOM: BOM 있는/없는 local_override.json·config_active.json 이 같은 값으로 읽힌다(쓰기는 BOM 없이).
- bench 종료: S41 이 BENCH_READ_ONLY 로 거부돼도 ERROR 가 아니다(bench_skip info), RUN=0 쓰기·기다림을 건너뛴다,
  PLC 에 쓰기 프레임 0. sim·prod 의 조치 실패는 그대로 error.
- 묶음 commands[].benchOk = core.NO_DEVICE_WRITE(정의 한 곳).
"""
import ast
import codecs
import json
import logging

from v2.common.clock import VirtualClock
from v2.common.paramdef import defaults
from v2.common.recorder import Recorder
from v2.common.safety_engine import Act, LiveRule, SafetyEngine
from v2.chk import commands as C
from v2.chk import core
from v2.chk import inventory as INV
from v2.chk import params as P
from v2.chk import snapshot_fields as SF
from v2.chk import uplink as U
from v2.chk import web_schema as W
from v2.chk.app import App, repo_root
from v2.chk.drivers import ig as IG
from v2.chk.sim.rig import ChkRig
from v2.chk.system import new_store

V1_STATES = ("off", "preparing", "below", "negative", "disconnected", "unzeroed", "invalid")
WRITE_FCS = (5, 6, 15, 16)


def _v1_state_text() -> dict:
    """v1 device/IonGauge.py 의 STATE_TEXT — 글로만 읽는다(import 하지 않음)."""
    tree = ast.parse((repo_root() / "device" / "IonGauge.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "STATE_TEXT" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError("v1 STATE_TEXT 없음")


# ───────────── 1) 이온 게이지 상태 열 ─────────────
def test_ig_state_labels_match_v1_and_below_from_gauge_min():
    v1 = _v1_state_text()
    assert set(v1) == set(V1_STATES)
    for k in V1_STATES:
        # v1 화면은 below 에만 단위를 붙인다(_with_unit: ok·below → " Torr")
        assert IG.STATE_TEXT[k] == (v1[k] + " Torr" if k == "below" else v1[k]), k
    assert IG.STATE_TEXT["below"] == f"< {IG.torr_text(IG.GAUGE_MIN_TORR)} Torr" == "< 1.0E-3 Torr"
    assert IG.torr_text(9.0e-6) == "9.0E-6" and IG.torr_text(1.0e-3) == "1.0E-3"
    assert IG.pressure_text(None, "below") == "< 1.0E-3 Torr" and IG.pressure_text(6.3e-3, "ok") == "6.3E-3 Torr"
    assert IG.pressure_text(None, "disconnected") == "미연결" and IG.pressure_text(None, "off") == "꺼짐"


def test_ig_state_columns_in_definition_and_bundle():
    cols = SF.columns()
    new = ["ig.ion_state", "ig.a_state", "ig.b_state"]
    assert cols[-3:] == new                                   # IG 블록 끝 — 앞 열 순서는 그대로
    by = {f.name: f for f in SF.IG_BLOCK.fields}
    for v, st in (("ion_torr", "ion_state"), ("a_torr", "a_state"), ("b_torr", "b_state")):
        assert by[v].state == st and by[st].type.value == "str" and by[st].summary == "last"
    assert dict(by["ion_state"].enum_labels).keys() == {"ok", "off", "preparing", "invalid"}
    assert dict(by["a_state"].enum_labels).keys() == {"ok", "below", "negative", "disconnected", "unzeroed", "invalid"}
    sn = W.bundle(defaults(P.PARAMS))["snapshot"]
    f = {x["column"]: x for b in sn["blocks"] for x in b["fields"]}
    assert f["ig.a_torr"]["stateColumn"] == "ig.a_state" and f["ig.ion_torr"]["stateColumn"] == "ig.ion_state"
    assert f["ig.a_state"]["enumLabels"]["below"] == "< 1.0E-3 Torr"
    assert f["heater.pv"]["stateColumn"] is None and f["ig.a_state"]["stateColumn"] is None
    states = {c for c, x in f.items() if x["stateColumn"]}
    assert all(f[c]["stateColumn"] in f for c in states)       # 가리키는 열이 같은 묶음에 있다
    assert sn["columns"][-3:] == new


def test_parse_status_states_reach_row():
    lines = ["0", "1", "1"] + ["0"] * 20 + ["90 -7", "0 -4", "-900"]
    r = IG.parse_status(lines)
    assert (r.ion_torr, r.a_torr, r.b_torr) == (9.0e-6, None, None)
    assert (r.ion_state, r.a_state, r.b_state) == ("ok", "below", "disconnected")


def test_rows_hello_summary_carry_state_columns(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path)
    rows = []
    real = rig.hub.on_row
    rig.hub.on_row = lambda row: (rows.append(dict(row)), real(row))[1]
    assert rig.start()
    try:
        rig.loop.run(5)
    finally:
        rig.stop()
    assert rig.hub.hello_data()["snapshotColumns"] == SF.columns()
    ok_rows = [r for r in rows if r.get("ig.q") == "ok"]
    assert ok_rows
    for r in ok_rows:
        for v, st in (("ig.ion_torr", "ig.ion_state"), ("ig.a_torr", "ig.a_state"), ("ig.b_torr", "ig.b_state")):
            assert r[st] in IG.STATE_TEXT
            assert (r[v] is None) == (r[st] != "ok"), (v, r[v], r[st])
    s = U.Summary()                                            # 1분 요약 — 글자 열은 건너뛴다(깨지지 않음)
    for k, r in enumerate(rows):
        s.add(1_760_000_040.0 + k, dict(r, **{"proc.run_id": "r1"}), 60)
    out = s.finish()
    assert out is not None and not any(k.endswith("_state") for k in out["values"])
    db = rig.recorder.db_path(rows[0]["ts"][:10].replace("-", ""))
    csv = rig.recorder.export_csv(db.name[5:13]).read_text(encoding="utf-8-sig").splitlines()
    assert csv[0].split(",")[-3:] == ["ig.ion_state", "ig.a_state", "ig.b_state"]


def test_recorder_same_day_restart_with_more_columns(tmp_path):
    """같은 날 열이 늘어난 버전으로 다시 시작 — 앞 행은 새 열이 빈 칸, CSV 는 새 열까지."""
    old = [c for c in SF.columns() if not c.endswith("_state")]
    r1 = Recorder(tmp_path, old)
    r1.start()
    r1.put_row({"ts": "2026-10-29T10:00:00", "seq": 1, "ig.a_torr": 2.0e-3})
    r1.stop()
    r2 = Recorder(tmp_path, SF.columns())
    r2.start()
    r2.put_row({"ts": "2026-10-29T10:00:01", "seq": 2, "ig.a_torr": None, "ig.a_state": "below"})
    r2.stop()
    assert not r1.errors and not r2.errors
    lines = r2.export_csv("20261029").read_text(encoding="utf-8-sig").splitlines()
    head = lines[0].split(",")
    assert head == SF.columns() and len(lines) == 3
    a = head.index("ig.a_state")
    assert lines[1].split(",")[a] == "" and lines[2].split(",")[a] == "below"


def test_inventory_and_bench_probe_use_same_gauge_text():
    assert INV.ig_meaning(25, "0       -4") == "< 1.0E-3 Torr" == IG.pressure_text(*IG.pressure_from_pair("0 -4", "conv"))
    assert INV.ig_meaning(26, "-900") == "미연결" and INV.ig_meaning(25, "-999") == "영점 미조정"
    assert INV.ig_meaning(25, "63 -4") == "6.3E-3 Torr" and INV.ig_meaning(24, "90 -7") == "9.0E-6 Torr"
    assert INV.ig_meaning(24, "0") == "꺼짐" and INV.ig_meaning(24, "abc") == "형식 다름"
    src = (repo_root() / "v2" / "tools" / "bench_probe.py").read_text(encoding="utf-8")
    assert "pressure_text(r.a_torr, r.a_state)" in src and '{"F": 24, "G": 25, "H": 26}[ch]' in src


# ───────────── 2) 설정 파일 BOM ─────────────
def test_local_override_with_or_without_bom_reads_same(tmp_path):
    vals = {"LOCAL_HMI_PORT": 18123, "EQUIPMENT_ID": "CHK2-TEST", "UPLINK_URL": "https://example.invalid/x"}
    got = []
    for name, bom in (("plain", b""), ("bom", codecs.BOM_UTF8)):
        d = tmp_path / name
        d.mkdir()
        (d / "local_override.json").write_bytes(bom + json.dumps({"values": vals}, ensure_ascii=False).encode("utf-8"))
        got.append(dict(new_store(d).load()))
    assert got[0] == got[1] and got[0]["LOCAL_HMI_PORT"] == 18123


def test_config_active_with_bom_reads_and_writes_without_bom(tmp_path):
    from v2.chk.system import param_schema_hash
    d = tmp_path / "c"
    d.mkdir()
    st = new_store(d)
    st.load()
    secrets = {p.key for p in P.PARAMS if p.scope.value == "secret"}
    st.apply(1, {k: v for k, v in defaults(P.PARAMS).items() if k not in secrets}, param_schema_hash(), busy=False)
    act = d / "config_active.json"
    raw = act.read_bytes()
    assert not raw.startswith(codecs.BOM_UTF8)                 # 쓰기는 BOM 없이
    before = dict(new_store(d).load())
    act.write_bytes(codecs.BOM_UTF8 + raw)                     # 사람이 메모장으로 열었다 저장한 경우
    s2 = new_store(d)
    assert dict(s2.load()) == before and s2.version == 1


# ───────────── 3) bench 종료 ─────────────
def _bench_rig(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    return c, ChkRig(c, tmp_path, mode="bench")


def test_bench_shutdown_no_error_no_writes(tmp_path, caplog):
    c, rig = _bench_rig(tmp_path)
    assert rig.start()
    rig.loop.run(3)
    plc = rig.sims["plc"]
    n0 = len(plc.frames)
    out = []
    app = App(system=rig, clock=c, print_fn=out.append)
    lg = logging.getLogger("chk")                              # 앞 시험의 App 로그 설정이 propagate 를 끄므로 직접 붙인다
    old = lg.level
    lg.addHandler(caplog.handler)
    lg.setLevel(logging.INFO)
    try:
        app.shutdown()
    finally:
        lg.removeHandler(caplog.handler)
        lg.setLevel(old)
    writes = [f for _, f in list(plc.frames)[n0:] if len(f) > 1 and f[1] in WRITE_FCS]
    assert writes == []                                        # 장비에 쓰기 0
    assert all(len(f) < 2 or f[1] not in WRITE_FCS for _, f in plc.frames)
    log = caplog.text
    assert "bench — 히터 쓰기 건너뜀" in log and "히터 RUN=0 쓰기" not in log
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    ev = [e for e in rig.events if type(e).__name__ == "SafetyEvent"]
    assert not [e for e in ev if e.level == "error"]
    skip = [e for e in ev if e.kind == "bench_skip"]
    assert [(e.rule, e.code, e.level, e.detail) for e in skip] == [
        ("S41", "HEATER_FORCE_OFF", "info", "bench — 보내지 않음(읽기 전용)")]


def _engine(bench: bool, err: str):
    c = VirtualClock(start_wall=1_760_000_000.0)

    class _P:
        active, state = False, "IDLE"

    ev = []
    rules = [LiveRule("S41", "maintain", "shutdown", lambda v, x: True, lambda v, x: [Act("HEATER_FORCE_OFF")]),
             LiveRule("S9", "off", "door", lambda v, x: True, lambda v, x: [Act("RF_SAFE_OFF")])]
    e = SafetyEngine(c, rules, {}, {"HEATER_FORCE_OFF": lambda a, done: done(err), "RF_SAFE_OFF": lambda a, done: done(err)},
                     _P(), ev.append)
    e.bench, e.bench_info_rules = bench, frozenset({"S41"})
    for where in ("shutdown", "door"):
        e.evaluate(where, 0.0, None, _P())
    return [(x.kind, x.rule, x.level) for x in ev if x.kind in ("error", "bench_skip")]


def test_bench_read_only_classification_only_in_bench():
    assert _engine(True, "BENCH_READ_ONLY") == [("bench_skip", "S41", "info"), ("bench_skip", "S9", "warn")]
    assert _engine(False, "BENCH_READ_ONLY") == [("error", "S41", "error"), ("error", "S9", "error")]
    assert _engine(True, "TIMEOUT") == [("error", "S41", "error"), ("error", "S9", "error")]


def test_sim_action_failure_stays_error(tmp_path):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    rig = ChkRig(c, tmp_path)
    assert rig.safety.engine.bench is False
    rig.plc.heater_run = lambda on, on_done=None, **kw: on_done and on_done(
        type("R", (), {"status": "rejected", "reason": "BENCH_READ_ONLY"})())
    rig.safety.event("shutdown")
    ev = [e for e in rig.events if type(e).__name__ == "SafetyEvent" and e.kind in ("error", "bench_skip")]
    assert [(e.kind, e.level, e.code) for e in ev] == [("error", "error", "HEATER_FORCE_OFF")]


# ───────────── 4) commands[].benchOk ─────────────
def test_bench_ok_single_definition():
    ok = {c["kind"] for c in W.bundle(defaults(P.PARAMS))["commands"] if c["benchOk"]}
    assert ok == set(core.NO_DEVICE_WRITE) == {c.kind for c in C.COMMANDS if c.bench_ok}
    assert ok == {"RECIPE_LOAD", "RECIPE_CLEAR", "CONFIG_APPLY", "CONFIG_ROLLBACK"}
    assert all(isinstance(c["benchOk"], bool) for c in W.bundle(defaults(P.PARAMS))["commands"])


def test_bench_core_rejects_by_definition(tmp_path):
    _, rig = _bench_rig(tmp_path)
    assert rig.core.command("BUZZ_STOP", {}).reason == "BENCH_READ_ONLY"
    assert rig.core.command("RECIPE_CLEAR", {}).reason != "BENCH_READ_ONLY"
