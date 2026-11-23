"""화면용 이름·도움말(5-13, 묶음 v11) — 웹·노트북 화면이 같은 말을 쓰도록 장비 정의에 둔 글을 확인한다.

- 블록 값·레시피 열·공정 중 수정 항목·정지 단계(두 순서)에 label 이 있다
- enum·list 블록 값의 enumLabels 는 허용값과 똑같다 / summary 는 그 블록 값 / group 은 groups 에 / paramGroups 는 모든 group
- 스냅샷 heater.hold 의 enumLabels 는 hold_label 이 낼 수 있는 값 전부(소스에서 찾는다)
- 화면용 글에 개발 표시(설정 키·값 이름·.py·()·레지스터·cfg:·쪽수·O2)가 없다 — 그런 것은 desc 로
- eventKinds: (source, kind) 중복 없음, level 은 main/detail, 코드가 내는 사건 kind 가 모두 표에 있다(소스에서 찾는다)
"""
import re
from pathlib import Path

from v2.chk import blocks as BK
from v2.chk import commands as C
from v2.chk import params as P
from v2.chk import recipe as RC
from v2.chk import safety_rules as SR
from v2.chk import snapshot_fields as SF
from v2.chk import web_schema as W
from v2.common import commands as CC
from v2.common.snapshot import TICK_COLUMNS

V2 = Path(__file__).resolve().parents[1]
DEV_MARKS = (r"[A-Z]{2,}_[A-Z0-9_]+", r"\b[a-z]+_[a-z_]+\b", r"\.py", r"\(\)", r"D0\d{4}", r"cfg:", r"p\.\d+", r"\bO2")


def _src(rel: str) -> str:
    return (V2 / rel).read_text(encoding="utf-8")


def _screen_texts():
    """(어디, 글) — 화면에 그대로 보이는 글 전부."""
    out = []
    for d in BK.BLOCK_DEFS:
        out.append((f"block {d.type} help", d.help))
        for p in d.params:
            out += [(f"block {d.type}.{p.name} label", p.label), (f"block {d.type}.{p.name} help", p.help)]
            out += [(f"block {d.type}.{p.name} enum {k}", v) for k, v in (p.enum_labels or ())]
    out += [(f"group {k}", lab) for k, lab in BK.BLOCK_GROUPS]
    for seq, name in ((SR.STOP_SEQUENCE, "stop"), (SR.ALL_STOP_SEQUENCE, "all_stop")):
        out += [(f"{name} {s.no}", s.label) for s in seq]
    for a in BK.ADJUST_ITEMS:
        out += [(f"adjust {a.key} label", a.label), (f"adjust {a.key} help", a.help)]
    for cols, name in ((RC.PROCESS_COLUMNS, "row"), (RC.HEATER_STEP_COLUMNS, "heater_step"), (RC.HEATER_GAS_COLUMNS, "heater_gas")):
        for c in cols:
            out += [(f"{name}.{c.name} label", c.label), (f"{name}.{c.name} help", c.help), (f"{name}.{c.name} group", c.group)]
    for f in TICK_COLUMNS + SF.PROCESS_FIELDS:
        out.append((f"snapshot {f.name}", f.label))
    for b in SF.BLOCKS:
        out.append((f"snapshot block {b.name}", b.label))
        for f in b.fields:
            out.append((f"snapshot {b.name}.{f.name}", f.label))
            out += [(f"snapshot {b.name}.{f.name} enum {k}", v) for k, v in (f.enum_labels or ())]
    out += [(f"event {s}.{k}", lab) for s, k, lab, _ in W.EVENT_KINDS]
    out += [(f"paramGroup {k}", lab) for k, lab in W.PARAM_GROUPS]
    reasons = {r.code: r.label for r in C.ALL_REASONS}
    out += [(f"reason {code}", reasons[code]) for code in ("COMMAND_EXPIRED", "IG_DISABLED", "COND_WAIT_TIMEOUT",
                                                           "PRESSURE_WAIT_TIMEOUT")]
    out.append(("command PLC_OUTPUTS_OFF", next(c.label for c in C.COMMANDS if c.kind == "PLC_OUTPUTS_OFF")))
    return out


def test_every_screen_item_has_label():
    missing = [f"{d.type}.{p.name}" for d in BK.BLOCK_DEFS for p in d.params if not p.label.strip()]
    missing += [f"row.{c.name}" for cols in (RC.PROCESS_COLUMNS, RC.HEATER_STEP_COLUMNS, RC.HEATER_GAS_COLUMNS)
                for c in cols if not c.label.strip()]
    missing += [a.key for a in BK.ADJUST_ITEMS if not a.label.strip() or not a.help.strip()]
    missing += [f"stop {s.no}" for s in SR.STOP_SEQUENCE + SR.ALL_STOP_SEQUENCE if not s.label.strip()]
    missing += [d.type for d in BK.BLOCK_DEFS if not d.help.strip() or not d.group]
    assert missing == []
    assert all(c.group for c in RC.PROCESS_COLUMNS)                    # 공정 레시피 열은 모두 화면 묶음이 있다


def test_enum_labels_cover_allowed_values_exactly():
    for d in BK.BLOCK_DEFS:
        for p in d.params:
            if p.type in ("enum", "list"):
                assert p.enum_labels, (d.type, p.name)
                assert [k for k, _ in p.enum_labels] == list(p.enum), (d.type, p.name)
                assert all(v.strip() for _, v in p.enum_labels)
            else:
                assert p.enum_labels is None, (d.type, p.name)
    cw = BK.by_type()["cond_wait"]
    field = next(p for p in cw.params if p.name == "field")
    assert dict(field.enum_labels)["mfc.o2_sccm"] == "O₂ 유량"            # COND_FIELDS 이름(O2)은 그대로, 화면만 O₂
    assert dict(BK.COND_FIELDS and {f: lab for f, lab, _ in BK.COND_FIELDS})["mfc.o2_sccm"] == "O2 유량"


def test_summary_groups_and_param_groups():
    keys = [k for k, _ in BK.BLOCK_GROUPS]
    assert keys == ["prep", "gas", "heater", "power", "run", "flow"]
    for d in BK.BLOCK_DEFS:
        names = [p.name for p in d.params]
        assert all(s in names for s in d.summary), (d.type, d.summary)
        assert d.group in keys
    pg = [k for k, _ in W.PARAM_GROUPS]
    assert len(pg) == len(set(pg)) and {p.group for p in P.PARAMS} <= set(pg)
    b = W.definitions()
    assert b["blocks"]["groups"] == [{"key": k, "label": lab} for k, lab in BK.BLOCK_GROUPS]
    assert b["paramGroups"][0] == {"key": "install", "label": "설치·장비"}
    d0 = b["blocks"]["defs"][0]
    assert {"help", "group", "summary"} <= set(d0) and {"label", "help", "advanced", "enumLabels"} <= set(d0["params"][0])
    assert set(b["stopSequence"]["steps"][0]) >= {"label", "desc"} and "label" in b["allStopSequence"]["steps"][0]
    assert {"label", "help"} <= set(b["adjust"]["items"][0])
    assert {"label", "help", "group"} <= set(b["recipe"]["processColumns"][0])
    f0 = b["snapshot"]["blocks"][1]["fields"][0]
    assert {"detail", "desc", "enumLabels"} <= set(f0) and "desc" in b["snapshot"]["blocks"][1]


def test_hold_enum_labels_cover_hold_label_values():
    src = _src("chk/heater_engine.py")
    assert 'return f"holding_{self.hold.kind}"' in src and '"arming" if' in src and 'else "off"' in src
    kinds = set(re.findall(r"h\.update\(state='(?:holding|engaging_sv2)', kind='(\w+)'", src))
    assert kinds == {"dac", "tc2"}
    hold = next(f for f in SF.HEATER_BLOCK.fields if f.name == "hold")
    assert {k for k, _ in hold.enum_labels} == {"off", "arming"} | {f"holding_{k}" for k in kinds}


def test_screen_texts_have_no_dev_marks():
    bad = []
    for where, text in _screen_texts():
        for pat in DEV_MARKS:
            m = re.search(pat, text or "")
            if m:
                bad.append(f"{where}: {text!r} ← {m.group(0)}")
    assert bad == []


def test_dev_info_kept_in_desc():
    """개발용 정보는 지우지 않는다 — desc 그대로."""
    pre = BK.by_type()["pre_clean"]
    assert "RFPULSE_ENABLED" in next(p for p in pre.params if p.name == "rfpulse").desc
    assert "[MFC p.74]" in next(p for p in BK.by_type()["valve"].params if p.name == "action").desc
    hb = {f.name: f for f in SF.HEATER_BLOCK.fields}
    assert hb["sv_limit"].desc == "PLC D00013" and hb["sv_limit"].source == "plc:heater_sv_limit"
    assert SR.STOP_SEQUENCE[7].desc == "RF Pulse OFF 완료 대기"               # AWAIT 문구
    assert next(c for c in C.COMMANDS if c.kind == "PLC_OUTPUTS_OFF").desc.startswith("버튼 코일 전부")
    assert next(c for c in RC.PROCESS_COLUMNS if c.name == "use_dc").v1_name == "use_dc_power"
    assert [s[2] for s in BK.SHUTTERS] == ["Gun Shutter 1", "Gun Shutter 2", "Main Shutter"]   # 스텝 문구에 쓰인다


def _code_event_kinds() -> set:
    """코드가 내는 (source, kind) — hub.on_event 의 source 규칙대로."""
    found = set()
    chk_all = "\n".join(_src(str(p.relative_to(V2))) for p in sorted((V2 / "chk").rglob("*.py")))
    common_all = "\n".join(_src(str(p.relative_to(V2))) for p in sorted((V2 / "common").rglob("*.py")))
    for cls, source in (("ProcEvent", "process"), ("SafetyEvent", "safety"), ("HeaterEvent", "heater")):
        for k in re.findall(cls + r'\(\s*"([a-z_]+)"', chk_all + common_all):
            found.add((source, k))
    drivers = {"dc": ("chk/drivers/dc.py", "DcEvent"), "ig": ("chk/drivers/ig.py", "IgEvent"),
               "mfc": ("chk/drivers/mfc.py", "MfcEvent"), "plc": ("chk/drivers/plc.py", "PlcEvent"),
               "rfp": ("chk/drivers/rfpulse.py", "RfpEvent")}
    for source, (rel, cls) in drivers.items():
        assert cls.replace("Event", "").lower() == source            # hub: device 속성 없으면 클래스 이름
        s = _src(rel)
        for k in re.findall(r'_emit\(\s*"([a-z0-9_]+)"', s):
            found.add((source, k))
        for a, b in re.findall(r'_emit\(\s*"([a-z0-9_]+)" if \w+ else "([a-z0-9_]+)"', s):
            found |= {(source, a), (source, b)}
    workers = ("plc", "mfc", "dc", "rfpulse", "ig")
    for k in re.findall(r'_emit\(\s*"([a-z0-9_]+)"', _src("common/worker.py")):
        found |= {(w, k) for w in workers}
    for k in re.findall(r'GuardEvent\(\s*"([A-Z_]+)"', _src("chk/cmd_guard.py")):
        found |= {(w, k) for w in workers}
    return found


def test_event_kinds_table():
    pairs = [(s, k) for s, k, _, _ in W.EVENT_KINDS]
    assert len(pairs) == len(set(pairs))
    assert {lv for *_, lv in W.EVENT_KINDS} <= {"main", "detail"}
    code = _code_event_kinds()
    assert len(code) > 60
    assert sorted(code - set(pairs)) == []                            # 코드가 내는 사건 kind 는 모두 표에
    lv = {(s, k): l for s, k, _, l in W.EVENT_KINDS}
    for k in ("state", "block", "list_done", "list_cancel", "stop_rest", "adjust", "plc_manual"):
        assert lv[("process", k)] == "main", k
    assert lv[("process", "steps")] == lv[("process", "step")] == lv[("safety", "step")] == "detail"
    for p in (("safety", "fired"), ("safety", "released"), ("safety", "seq_start"), ("safety", "seq_done"),
              ("plc", "link_up"), ("plc", "link_down"), ("heater", "engaged"), ("heater", "recipe_finished")):
        assert lv[p] == "main", p
    for p in (("mfc", "verify_ok"), ("dc", "off_confirmed"), ("rfp", "off_confirmed"), ("dc", "identity"),
              ("ig", "identity_ok"), ("mfc", "fs"), ("mfc", "scale"), ("plc", "marker_written"), ("plc", "heater_config_ok"),
              ("dc", "syst_err"), ("mfc", "remote_state"), ("dc", "last_state"), ("plc", "connected")):
        assert lv[p] == "detail", p


def test_reason_and_command_labels_not_in_messages():
    """사유·명령 라벨은 화면용 — 사건 message·로그 문구에 쓰이지 않는다(라벨만 바뀌고 문구는 그대로)."""
    texts = {r.code: r.label for r in C.ALL_REASONS}
    labels = [texts[c] for c in ("COMMAND_EXPIRED", "IG_DISABLED", "COND_WAIT_TIMEOUT", "PRESSURE_WAIT_TIMEOUT")]
    labels.append(next(c.label for c in C.COMMANDS if c.kind == "PLC_OUTPUTS_OFF"))
    srcs = [p for p in list((V2 / "chk").rglob("*.py")) + list((V2 / "common").rglob("*.py"))
            if p.name not in ("commands.py",)]
    for lab in labels:
        assert not any(lab in p.read_text(encoding="utf-8") for p in srcs), lab
    assert CC.COMMON_REASONS and texts["COMMAND_EXPIRED"] == "유효 시간이 지난 웹 명령이라 실행하지 않음(장비 시계 기준)"


def test_export_version_11_hash_changed():
    assert W.EXPORT_VERSION >= 11                                          # 5-14 에서 12
    assert W.content_hash() != "702c6f0d959ddf570f6d6b38cfa9b25c738093f8ad5921884fb92cea5779a017"   # v10


def test_device_names_dc_power_rf_power():
    """5-15: 화면·기록·설정에 나가는 장비 이름은 DC Power·RF Power(RF Pulse 그대로). 일반 낱말 '전원 OFF' 는 그대로."""
    from v2.chk import devices as D
    from v2.chk import params as P
    from v2.chk import snapshot_fields as SF
    b = W.bundle()
    names = {x["name"]: x["label"] for x in b["snapshot"]["blocks"]}
    assert names["dc"] == "DC Power" and names["rf"] == "RF Power" and names["rfp"] == "RF Pulse"
    assert dict((x["key"], x["label"]) for x in b["paramGroups"])["dc"] == "DC Power"
    assert dict((x["key"], x["label"]) for x in b["paramGroups"])["rf"] == "RF Power"
    src = {x["source"]: x["label"] for x in b["eventSources"]}
    assert src["dc"] == "DC Power" and src["rfp"] == src["rfpulse"] == "RF Pulse"
    assert {s for s, _, _, _ in W.EVENT_KINDS} <= set(src)
    labels = [p.label for p in P.PARAMS] + [b_.label for b_ in SF.BLOCKS] + [d.name for d in D.DEVICES]
    assert not [x for x in labels if "DC 전원" in x or "RF 전원" in x]
    assert next(p.label for p in P.PARAMS if p.key == "DC_PORT") == "DC Power 포트"


# 5-18(묶음 16) — 제어 구조에 따라 바뀌는 히터 상태 이름(enumLabelsBy). v1 controller/heater_cascade.py STATE_TEXT_TC2 와 같은 글
def test_casc_state_labels_by_ctrl_mode():
    from v2.chk.heater_cascade import CTRL_MODES, STATE_TEXT, STATE_TEXT_TC2
    f = {x["column"]: x for b in W.snapshot()["blocks"] for x in b["fields"]}
    cs = f["heater.casc_state"]
    assert cs["enumLabels"] == STATE_TEXT                                   # 옛 웹이 보던 것은 그대로
    by = cs["enumLabelsBy"]
    assert by == {"column": "heater.ctrl_mode",
                  "labels": {"tc2": {"running": "TC2 제어(단독)", "frozen": "TC2 제어(단독) · 셔터 열림(유지)"}}}
    assert by["labels"]["tc2"] == STATE_TEXT_TC2 and by["column"] in f
    assert set(by["labels"]) <= set(CTRL_MODES) == set(f["heater.ctrl_mode"]["enumLabels"])
    assert all(set(m) <= set(STATE_TEXT) for m in by["labels"].values())   # 바뀌는 값만 — 모두 enumLabels 에 있는 값
    assert all(x["enumLabelsBy"] is None for c, x in f.items() if c != "heater.casc_state")


def test_casc_state_label_matches_engine_text():
    """화면 규칙(enumLabelsBy → enumLabels)으로 고른 이름 = 장비 HeaterCascade.state_text(로그·이벤트 문구)."""
    from v2.chk import heater_cascade as HC
    cs = {x["column"]: x for b in W.snapshot()["blocks"] for x in b["fields"]}["heater.casc_state"]

    def screen(mode, st):
        return cs["enumLabelsBy"]["labels"].get(mode, {}).get(st) or cs["enumLabels"][st]
    for mode in HC.CTRL_MODES:
        for st in HC.STATE_TEXT:
            want = HC.STATE_TEXT_TC2[st] if mode == "tc2" and st in HC.STATE_TEXT_TC2 else HC.STATE_TEXT[st]
            assert screen(mode, st) == want


# 5-18 — MFC 읽기 계획 설명이 드라이버 동작(R5·R60·R6, 번갈아 읽기, R6 양보)과 같은 말
def test_mfc_read_plan_desc_matches_driver():
    from v2.chk import devices as D
    d = P.by_key()["MFC_READ_PLAN"].desc
    for w in ("R5", "R60", "R6", "짝수 초", "홀수 초", "양보", "돌아온다", "MFC_READ_PLAN_SLOW_N"):
        assert w in d, w
    assert P.by_key()["MFC_READ_PLAN_SLOW_PCT"].default == 60            # 처음 기준(마감의 60 %)은 설정으로(5-20 ①)
    assert "R5+R60" not in d and "실패하면" not in d
    plan = [t for t in D.TICK_PLAN if t.device == "mfc"][0]
    assert plan.reads == ("R5", "R60", "R6") and plan.deadline_param == "MFC_READ_DEADLINE_MS"
    for w in ("짝수 초", "홀수 초", "양보", "마감을 넘길"):
        assert w in plan.plan, w


# 5-20 ④ — 화면 문구 규칙: '—' 로 이은 조각 문장, '안 함·안 씀·못 함·안 됨·미룸', 상태·동작의 '켬·끔' 을 쓰지 않는다.
# 예외: 블록 값 검사 문구(valueRules msg — 골든 web_block_cases 에 묶여 있음)
def _bundle_screen_texts():
    from v2.chk import web_schema as W
    from v2.common.paramdef import defaults
    b = W.bundle(defaults(P.PARAMS))
    out = []
    for p in b["params"]:
        out += [("param", p["key"], p["label"]), ("param", p["key"], p["desc"])] + [
            ("param", p["key"], v) for v in (p.get("enumLabels") or {}).values()]
    for blk in b["snapshot"]["blocks"]:
        for f in blk["fields"]:
            out += [("field", f["column"], f["label"]), ("field", f["column"], f["desc"] or "")] + [
                ("field", f["column"], v) for v in (f.get("enumLabels") or {}).values()]
    for f in b["snapshot"]["process"] + b["snapshot"]["tick"]:
        out += [("field", f["column"], f["label"]), ("field", f["column"], f["desc"] or "")]
    for c in b["commands"]:
        out += [("command", c["kind"], c["label"]), ("command", c["kind"], c["desc"] or "")] + [
            ("command", c["kind"], a.get("desc") or "") for a in c["args"]]
    out += [("reason", r["code"], r["label"]) for r in b["reasons"]]
    out += [("precondition", x["id"], x["desc"]) for x in b["preconditions"]]
    out += [("state", s["name"], s["label"]) for s in b["states"]]
    out += [("eventKind", e["kind"], e["label"]) for e in b["eventKinds"]]
    for d in b["blocks"]["defs"]:
        out += [("block", d["type"], d.get(k) or "") for k in ("label", "desc", "help")]
        out += [("block", d["type"], (d.get(k) or {}).get("text") or "") for k in ("skip", "pause")]
        for pp in d["params"]:
            out += [("blockParam", f"{d['type']}.{pp['name']}", pp.get(k) or "") for k in ("label", "desc", "help")]
    out += [("stopStep", str(s["no"]), s.get("desc") or "") for s in b["stopSequence"]["steps"] + b["allStopSequence"]["steps"]]
    return out


def test_screen_texts_follow_wording_rules():
    import re
    bad = re.compile(r"—|안 ?(함|씀|됨)(?![가-힣])|못 ?함(?![가-힣])|미룸|(?<![가-힣])(켬|끔)(?![가-힣])|[가-힣](켬|끔)(?![가-힣])")
    # v1 대조 시험(test_bench_fixes)이 v1 문구와 같기를 요구하는 이온 게이지 상태 이름은 예외
    v1_same = {("field", "ig.a_state", "음수 — 영점·오염 확인"), ("field", "ig.b_state", "음수 — 영점·오염 확인"),
               ("field", "ig.ion_state", "음수 — 영점·오염 확인")}
    hits = [(k, key, t) for k, key, t in _bundle_screen_texts() if bad.search(t) and (k, key, t) not in v1_same]
    hits += [("screen", w, t) for w, t in _screen_texts() if t and bad.search(t)      # 레시피 열·공정 중 수정 항목 등
             and not (w.startswith("snapshot ig.") and t == "음수 — 영점·오염 확인")]
    assert hits == [], hits[:10]
