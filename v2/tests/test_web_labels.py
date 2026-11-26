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
    # 웹 연결(5-22 ①) — uplink._note·_warn_once: source "uplink", kind = 코드 소문자
    up = _src("chk/uplink.py")
    for k in re.findall(r'_note\(\s*"\w+",\s*"(UPLINK_[A-Z_]+)"', up) + re.findall(r'_warn_once\(\s*"[^"]+",\s*"(UPLINK_[A-Z_]+)"', up):
        found.add(("uplink", k.lower()))
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
    assert CC.COMMON_REASONS and texts["COMMAND_EXPIRED"] == "유효 시간 지난 웹 명령"


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
    p = P.by_key()["MFC_READ_PLAN"]
    d = p.desc + " " + p.note                       # W-23 D: 명령 글자·키 이름은 note 로(상세 보기)
    for w in ("R5", "R60", "R6", "짝수 초", "홀수 초", "양보", "MFC_READ_PLAN_SLOW_N"):
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
    out += [("eventKindDesc", e["kind"], e.get("desc") or "") for e in b["eventKinds"]]
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


# 5-21 ④ — 사건 종류마다 설명(desc)이 있고, 이름에 프로그램 말이 남지 않는다
def test_event_kinds_have_desc_and_operator_names():
    from v2.chk import web_schema as W
    from v2.common.paramdef import defaults
    ek = W.bundle(defaults(P.PARAMS))["eventKinds"]
    assert all(e["desc"] for e in ek)
    for word in ("눈금", "펼친 스텝", "읽기 계획", "블록"):
        assert not any(word == e["label"] or e["label"].startswith(word) for e in ek), word
    by = {(e["source"], e["kind"]): e for e in ek}
    assert by[("mfc", "scale")]["label"] == "압력·유량 범위 확인" and by[("dc", "disconnected")]["desc"].startswith("장비 연결이 끊겼습니다")


# ───────────── 5-23 ⑤ 용어표·같은 이름·길이 ─────────────
# 용어표의 옛 말 — 화면 이름(label)·운전자용 설명(help)·값 이름(enum)·사건 설명·사유 이름/설명에 쓰지 않는다.
OLD_TERMS = ("인터록", "이온 게이지", "이오나이저", "필라멘트", "디개스", "방출", "냉각수", "도어", "차단밸브", "메인 밸브", "Main 밸브",
             "건 셔터", "Gun 1", "Gun 2", "순방향", "반사", "설정점", "끄기 순서", "끄기 단계", "감시 신호", "스텝", "램프")
# 그대로 두는 글(이유와 함께) — [어디, 글의 일부]
OLD_TERM_EXCEPT = (
    # 개발용 desc 는 이 시험이 보지 않는다(상세 보기 전용). v1 대조·골든 글은 label·help 가 아니라 여기에 걸리지 않는다
)


def _named_texts():
    """(어디, 글) — 화면 이름·설명(평소 보기)."""
    b = W.bundle()
    out = []
    for bl in b["snapshot"]["blocks"]:
        out.append((f"block {bl['name']}", bl["label"]))
        for f in bl["fields"]:
            out += [(f"field {f['column']}", f["label"]), (f"field help {f['column']}", f.get("help") or "")]
            out += [(f"field enum {f['column']}", v) for v in (f.get("enumLabels") or {}).values()]
    out += [(f"button {x['target']}", x["label"]) for x in b["plcButtons"]]
    out += [(f"command {c['kind']}", c["label"]) for c in b["commands"]]
    out += [(f"state {s['name']}", s["label"]) for s in b["states"]]
    out += [(f"event {e['source']}.{e['kind']}", e["label"]) for e in b["eventKinds"]]
    out += [(f"event desc {e['source']}.{e['kind']}", e.get("desc") or "") for e in b["eventKinds"]]
    out += [(f"source {e['source']}", e["label"]) for e in b["eventSources"]]
    out += [(f"reason {r['code']}", r["label"]) for r in b["reasons"]]
    out += [(f"reason help {r['code']}", r.get("help") or "") for r in b["reasons"]]
    out += [(f"param {p['key']}", p["label"]) for p in b["params"]]
    out += [(f"param enum {p['key']}", v) for p in b["params"] for v in (p.get("enumLabels") or {}).values()]
    out += [(f"group {g['key']}", g["label"]) for g in b["paramGroups"]]
    for d in b["blocks"]["defs"]:
        out += [(f"blockdef {d['type']}", d["label"]), (f"blockdef help {d['type']}", d.get("help") or "")]
        for p in d["params"]:
            out += [(f"blockparam {d['type']}.{p['name']}", p.get("label") or ""), (f"blockparam help {d['type']}.{p['name']}", p.get("help") or "")]
            out += [(f"blockparam enum {d['type']}.{p['name']}", v) for v in (p.get("enumLabels") or {}).values()]
    out += [(f"recipe {c['name']}", c.get("label") or "") for c in b["recipe"]["processColumns"]]
    out += [(f"recipe help {c['name']}", c.get("help") or "") for c in b["recipe"]["processColumns"]]
    out += [(f"recipe group {c['name']}", c.get("group") or "") for c in b["recipe"]["processColumns"]]
    out += [(f"adjust {a['item']}", a.get("label") or "") for a in b["adjust"]["items"]]
    out += [(f"adjust help {a['item']}", a.get("help") or "") for a in b["adjust"]["items"]]
    out += [(f"stop {s['no']}", s.get("label") or "") for s in b["stopSequence"]["steps"] + b["allStopSequence"]["steps"]]
    return out


def test_old_terms_not_in_screen_names():
    bad = []
    for where, t in _named_texts():
        for w in OLD_TERMS:
            if w in (t or "") and not any(where.startswith(a) and part in t for a, part in OLD_TERM_EXCEPT):
                bad.append((where, w, t))
    assert bad == [], bad[:10]


def test_same_thing_same_name():
    b = W.bundle()
    fields = {f["column"]: f["label"] for bl in b["snapshot"]["blocks"] for f in bl["fields"]}
    # PLC 버튼 이름 = 그 주소의 스냅샷 필드 이름
    assert [(x["target"], x["label"]) for x in b["plcButtons"] if x["label"] != fields[f"plc.{x['addr']}"]] == []
    # 수동 조작 출력 이름(process_engine.manual_outputs)은 스냅샷 필드 이름과 같다(유량·파워·압력 SPn 은 그 이름)
    src = _src("chk/process_engine.py")
    body = src[src.index("def manual_outputs"):src.index("def _plc_guard")]
    names = re.findall(r'\("(\w+)", "([^"]+)"\)', body)
    plc_names = [(k, n) for k, n in names if f"plc.{k}" in fields]
    assert plc_names and all(fields[f"plc.{k}"] == n for k, n in plc_names), plc_names
    assert 'f"압력 SP{sp}"' in body
    # 상태 이름 = 그 상태를 만드는 블록 이름
    st = {s["name"]: s["label"] for s in b["states"]}
    bl = {d["type"]: d["label"] for d in b["blocks"]["defs"]}
    for s_, t in (("PRE_CLEAN", "pre_clean"), ("GAS_PRE", "gas_prep"), ("GAS_INTRO", "gas_intro"), ("HEATER_RAMP", "heater_ramp"),
                  ("BASE_WAIT", "base_wait"), ("HEATER_SETTLE", "heater_settle"), ("VALVE", "valve"), ("SHUTTER", "shutter"),
                  ("POWER_OFF", "power_off"), ("GAS_OFF", "gas_off"), ("HEATER_OFF", "heater_off"), ("COND_WAIT", "cond_wait"),
                  ("WAIT", "wait"), ("MAIN", "main")):
        assert st[s_] == bl[t], (s_, st[s_], bl[t])
    assert st["GUN_SHUTTER"] == "Gun 셔터 열기" and st["PRESSURE_SP1"] == "작업 압력(SP1)" and st["HEATER_HOLD_WAIT"] == "히터 유지 대기"
    # 기본 템플릿의 셔터 블록 이름(골든에도 이 이름 — 스텝 문구는 그대로)
    assert 'B("shutter", "Gun 셔터 열기", "GUN_SHUTTER"' in _src("chk/blocks.py") and "건 셔터 열기" not in _src("chk/blocks.py")


def test_name_lengths_and_shapes():
    b = W.bundle()
    bad = []
    for bl in b["snapshot"]["blocks"]:
        bad += [("field", f["label"]) for f in bl["fields"] if len(f["label"]) > 16]
    bad += [("state", s["label"]) for s in b["states"] if len(s["label"]) > 14]
    bad += [("event", e["label"]) for e in b["eventKinds"] if len(e["label"]) > 18]
    bad += [("command", c["label"]) for c in b["commands"] if len(c["label"]) > 20 or "(" in c["label"]]
    bad += [("reason", r["label"]) for r in b["reasons"] if len(r["label"]) > 26 or "(" in r["label"]]
    helps = [("field help", f.get("help") or "") for bl in b["snapshot"]["blocks"] for f in bl["fields"]]
    helps += [("field help", f.get("help") or "") for f in b["snapshot"]["process"]]
    helps += [("reason help", r.get("help") or "") for r in b["reasons"]]
    bad += [h for h in helps if len(h[1]) > 70]
    named = [("field", f["label"]) for bl in b["snapshot"]["blocks"] for f in bl["fields"]] + helps
    named += [("command", c["label"]) for c in b["commands"]] + [("reason", r["label"]) for r in b["reasons"]]
    for where, t in named:
        for pat in DEV_MARKS + (r"\bv1\b", r"[MDP]0\d{3,4}"):
            if re.search(pat, t or ""):
                bad.append((where, t, pat))
    assert bad == [], bad[:10]


def test_everyday_fields_have_help():
    b = W.bundle()
    fields = [f for bl in b["snapshot"]["blocks"] for f in bl["fields"]] + list(b["snapshot"]["process"])
    miss = [(f.get("column") or f.get("name")) for f in fields
            if not f.get("detail") and not f.get("help") and (f.get("column") or f.get("name")) not in SF.FIELD_NAME_ONLY]
    assert miss == []
    # 거부·중단·실패·경고(정보 중 경고만인 것 포함) 사유는 help 가 있다 — 지금은 162개 모두
    assert all(r.get("help") for r in b["reasons"])


# ───────────── W-23 D 설정 설명(desc) 규칙 ─────────────
# desc 는 운전자·관리자가 읽는 말 1~2문장(…합니다/…입니다). 개발 메모·v1 출처·설정 키·명령 글자·규칙 번호·결정 날짜는 note 로.
DESC_BAD = (
    ("설정 키", r"\b[A-Z][A-Z0-9]*_[A-Z0-9_]+\b"),
    ("v1·v2", r"(?<![A-Za-z])[vV][12](?![0-9A-Za-z])"),
    ("작업·결정 번호", r"(?<![\d.])\d{1,2}-\d{1,2}(?![\d])|W-\d+|[①-⑳]|20\d\d-\d\d-\d\d"),
    ("파일 이름", r"\w+\.(py|json|cmd|txt)\b"),
    ("장비 명령 글자", r"(?<![A-Za-z0-9])(R\d{1,3}|MEAS:\w+\??|OUTP\??|FACT:[\w-]+\??|VOLT:\w+\??|CURR:\w+\??|SYST:\w+\??|"
                  r"\*IDN\?|\*RST|APPLy|[QSDLMXVPWZ]\d{1,3}|D0\d{4}|M0\d{4}|X\d\d|S\d\d[A-Z]?)(?![A-Za-z0-9])"),
    ("금지 문구", r"—|안 ?(함|씀|됨)(?![가-힣])|못 ?함(?![가-힣])|미룸|(?<![가-힣])(켬|끔)(?![가-힣])"),
    ("옛 말", "|".join(("인터록", "이온 게이지", "필라멘트", "디개스", "방출", "냉각수", "도어", "차단 ?밸브", "메인 밸브", "순방향",
                      "반사", "설정점", "램프", "블록", "스텝", "사건", "로컬", "시퀀스", "벤트", "로터리", "터보 펌프", "가스 도입",
                      "정상 끝 정지", "셔터 딜레이"))),
)


def desc_problems(text: str) -> list:
    """설정 desc 한 줄의 규칙 위반 이름 목록(빈 목록 = 맞음) — 웹 문구 검사(scripts/ops2-chk-wording.mjs)와 같은 패턴."""
    out = [name for name, pat in DESC_BAD if re.search(pat, text)]
    sents = [x for x in re.split(r"(?<=\.)\s+", text.strip()) if x]
    if not sents or any(not re.search(r"니다(\([^()]*\))?\.$", x) for x in sents):     # 끝 괄호 덧말은 허용
        out.append("문장 끝(…합니다/…입니다)")
    if len(sents) > 2:
        out.append("2문장 넘음")
    return out


def test_param_desc_rules():
    hits = [(p.key, desc_problems(p.desc), p.desc) for p in P.PARAMS if desc_problems(p.desc)]
    assert hits == [], hits[:5]
    assert len(P.PARAMS) >= 293


def test_param_desc_check_catches_patterns():
    """검사 자체가 패턴을 잡는다(빈 검사가 늘 통과하지 않게)."""
    for bad in ("유량 안정화 시간입니다(FLOW_STABILIZE_SEC).", "v1 과 같습니다.", "5-19 ③ 결정입니다.", "R5 로 읽습니다.",
                "PLC D00040 을 씁니다.", "켬 상태입니다.", "인터록을 겁니다.", "정지한다.", "한 줄. 두 줄입니다.",
                "하나입니다. 둘입니다. 셋입니다.", "S32 규칙입니다.", "process_engine.py 에 있습니다."):
        assert desc_problems(bad), bad
    assert desc_problems("'유량 안정화 시간' 동안 기다립니다. 바꾸면 다음 공정부터 씁니다.") == []


def test_sp_source_desc_names_pressure_steps():
    by = P.by_key()
    for n in (2, 3, 4, 5):
        assert f"'{by[f'SP{n}_MTORR'].label}'" in by[f"SP{n}_SOURCE"].desc
