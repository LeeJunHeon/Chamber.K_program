"""노트북 조작(5-20 ②) — 설정 LOCAL_HMI_CONTROL(보기와 정지만·잠금을 푼 뒤 조작·항상 조작)에 따라 노트북(hmi) 경로가 받는 명령.

- 보기와 정지만(view_stop) = 5-19 까지의 노트북: 정지 넷 + 공정 중 값 바꾸기·건너뛰기·일시정지·재개(hmi = always)
- 잠금을 푼 뒤 조작·항상 조작: 그 밖에 HMI_OPERATE(공정 시작·적재 레시피 시작·해제·순서 고치기·PLC 버튼·필라멘트·히터)도
- 설정 적용·되돌리기·레시피 적재·내부 조치는 어느 값이든 노트북에서 받지 않는다(SOURCE_NOT_ALLOWED)
- 웹 경로는 설정과 상관없이 그대로. 가상 시계·시뮬레이터만(실제 포트 없음).
"""
import pytest

from v2.common.commands import Source
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import web_schema as W
from v2.common.paramdef import defaults
from v2.tests.test_process_engine import R, SHORT

VALUES = ("view_stop", "unlock", "always")
ALWAYS = {"PROCESS_STOP", "ALL_STOP", "BUZZ_STOP", "HEATER_STOP", "PROCESS_ADJUST", "PROCESS_SKIP", "PROCESS_PAUSE",
          "PROCESS_RESUME"}
OPERATE = {"PROCESS_START", "RECIPE_START", "RECIPE_CLEAR", "PROCESS_EDIT", "PLC_BUTTON", "IG_FILAMENT", "HEATER_SV",
           "HEATER_ON", "HEATER_OFF", "HEATER_RESET", "HEATER_GAS_RELEASE", "HEATER_RECIPE_RUN", "HEATER_RECIPE_STOP",
           "HEATER_RECIPE_HOLD", "HEATER_RECIPE_SKIP"}
PATH = {"SOURCE_NOT_ALLOWED", "HMI_CONTROL_OFF"}


def test_setting_defaults_labels_and_admin_only():
    d = P.by_key()
    c, m = d["LOCAL_HMI_CONTROL"], d["LOCAL_HMI_RELOCK_MIN"]
    assert c.default == "unlock" and c.enum == VALUES and tuple(c.enum) == C.HMI_CONTROL_VALUES
    assert dict(c.enum_labels) == {"view_stop": "보기와 정지만", "unlock": "잠금을 푼 뒤 조작", "always": "항상 조작"}
    assert (m.default, m.unit, m.min) == (10, "분", 1)
    assert c.role.value == "admin" and m.role.value == "admin"           # 노트북에 무엇을 열지는 관리자가 정한다
    assert c.group == m.group == "install"


def test_command_table_marks_notebook_paths():
    by = C.by_kind()
    assert {k for k, c in by.items() if c.hmi == "always"} == ALWAYS
    assert {k for k, c in by.items() if c.hmi == "operate"} == OPERATE
    for k, c in by.items():
        assert (Source.HMI in c.sources) == bool(c.hmi), k
        if c.hmi == "operate":
            assert c.role == "operator" and Source.WEB in c.sources, k
    for k in ("CONFIG_APPLY", "CONFIG_ROLLBACK", "RECIPE_LOAD"):
        assert by[k].hmi == "" and Source.HMI not in by[k].sources
    assert all(not c.hmi for c in C.COMMANDS if c.role == "system")
    assert "HMI_CONTROL_OFF" in C.reason_codes()
    b = {x["kind"]: x for x in W.bundle(defaults(P.PARAMS))["commands"]}
    assert b["PROCESS_STOP"]["hmi"] == "always" and b["PLC_BUTTON"]["hmi"] == "operate" and b["CONFIG_APPLY"]["hmi"] is None
    assert "hmi" in b["PLC_BUTTON"]["sources"] and "hmi" not in b["CONFIG_APPLY"]["sources"]


@pytest.mark.parametrize("value", VALUES + (None,))
def test_path_check_for_every_command_and_value(value):
    """표 판정(check_command): 모든 명령 × 세 값(+ 설정 없음 = 보기와 정지만처럼)."""
    for c in C.COMMANDS:
        for state in ("IDLE", "MAIN"):
            r = C.check_command(c.kind, Source.HMI, state, {}, value)
            if not c.hmi:
                assert r == "SOURCE_NOT_ALLOWED", (c.kind, value)
            elif c.hmi == "operate" and value in ("view_stop", None):
                assert r == "HMI_CONTROL_OFF", (c.kind, value)
            else:
                assert r not in PATH, (c.kind, value, r)
            w = C.check_command(c.kind, Source.WEB, state, {}, value)            # 웹은 설정과 상관없이 같다
            assert w == C.check_command(c.kind, Source.WEB, state, {}), c.kind
    assert C.hmi_kinds(value) == [c.kind for c in C.COMMANDS if c.hmi == "always"
                                  or (c.hmi == "operate" and value in ("unlock", "always"))]


def _args():
    row = dict(SHORT)
    return {"PROCESS_START": {"row": row}, "RECIPE_LOAD": {"name": "r", "rows": [row]}, "HEATER_SV": {"value": 50.0},
            "HEATER_ON": {"value": 50.0}, "HEATER_RECIPE_HOLD": {"on": True}, "PLC_BUTTON": {"target": "buzz_stop", "on": True},
            "IG_FILAMENT": {"on": False}, "PROCESS_ADJUST": {"device": "dc", "item": "power", "value": 60.0},
            "PROCESS_SKIP": {"no": "1", "revision": 0}, "PROCESS_PAUSE": {"no": "1", "revision": 0},
            "PROCESS_RESUME": {"revision": 0}, "PROCESS_EDIT": {"revision": 1, "blocks": []},
            "HEATER_RECIPE_RUN": {"name": "h", "steps": []}, "CONFIG_APPLY": {"version": 1, "values": {}, "schemaHash": "x"},
            "HEATER_CLAMP_MV": {"value": 500}, "PROCESS_ABORT": {"reason": "TEST"}, "START_REJECT": {"reason": "TEST"},
            "HEATER_RECIPE_ABORT": {"reason": "TEST"}, "NOTIFY": {"level": "info", "code": "T"}}


def _send_all(tmp_path, value, source):
    """조립(가상 시계 sim)에 모든 명령을 한 번씩 — (kind, 결과) 목록과 hello."""
    r = R(tmp_path, cfg={"LOCAL_HMI_CONTROL": value})
    out = []

    def go():
        for c in C.COMMANDS:
            if c.kind in ("ALL_STOP", "PROCESS_STOP"):
                continue                                                  # 끝에 따로(앞 명령 결과를 지우지 않게)
            out.append((c.kind, r.rig.core.command(c.kind, dict(_args().get(c.kind, {})), source=source)))
        for k in ("PROCESS_STOP", "ALL_STOP"):
            out.append((k, r.rig.core.command(k, {}, source=source)))
        return True
    r.on(3, go)
    r.run(8)
    return out, r.rig.hub.hello_data(), r.rig


@pytest.mark.parametrize("value", VALUES)
def test_assembled_core_follows_setting_for_every_command(tmp_path, value):
    res, hello, rig = _send_all(tmp_path, value, Source.HMI)
    by = C.by_kind()
    for kind, x in res:
        c = by[kind]
        if not c.hmi:
            assert x.reason == "SOURCE_NOT_ALLOWED", (kind, x)
        elif c.hmi == "operate" and value == "view_stop":
            assert x.reason == "HMI_CONTROL_OFF" and not x.ok, (kind, x)
        else:
            assert x.reason not in PATH, (kind, x)
    ok = {k for k, x in res if x.ok}
    assert {"PROCESS_STOP", "ALL_STOP"} <= ok
    if value == "view_stop":
        assert not ok & OPERATE
    else:
        assert {"PROCESS_START", "PLC_BUTTON", "HEATER_RECIPE_STOP"} <= ok                  # 노트북에서 실제로 실행됨
        why = dict((k, x.reason) for k, x in res)                         # 공정이 시작된 뒤라 엔진 규칙으로 거부(경로는 통과)
        assert why["RECIPE_CLEAR"] == "PROCESS_RUNNING" and why["IG_FILAMENT"] == "STATE_NOT_ALLOWED"
    assert hello["commandKinds"] == C.hmi_kinds(value)
    assert hello["hmiControl"] == value and hello["hmiRelockMin"] == 10
    # 명령 기록(cmd_result)에 경로가 남는다
    cr = [m["data"] for m in rig.hub.recent if m["type"] == "cmd_result"]
    assert cr and all(d["source"] == "hmi" and d["by"] == "local" for d in cr)
    off = [d for d in cr if d["reasonCode"] == "HMI_CONTROL_OFF"]
    assert len(off) == (len(OPERATE) if value == "view_stop" else 0)


def test_unlock_and_always_accept_the_same_and_web_is_unchanged(tmp_path):
    hmi = {v: [(k, x.ok, x.reason) for k, x in _send_all(tmp_path / f"h{v}", v, Source.HMI)[0]] for v in ("unlock", "always")}
    assert hmi["unlock"] == hmi["always"]                                # 차이는 노트북 화면의 잠금뿐
    web = {v: [(k, x.ok, x.reason) for k, x in _send_all(tmp_path / f"w{v}", v, Source.WEB)[0]] for v in VALUES}
    assert web["view_stop"] == web["unlock"] == web["always"]
    assert not any(r == "HMI_CONTROL_OFF" for _, _, r in web["view_stop"])
