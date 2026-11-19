"""압력 설정점 '값 출처'(5-19 ③) — 장비에 저장된 값을 쓸지(장비 값), 프로그램 값을 장비에 쓸지(프로그램 값) 사람이 고른다.

- 드라이버: 장비 값(R1~R4·R10, parse_sp_reply)을 ㉠ 연결 직후 ㉡ 쓴 값 없이 선택할 때 ㉢ 쓰고 확인한 뒤 ㉣ 공정 밖 주기로 기억.
  못 읽으면 '모름'(다른 값으로 채우지 않음). 스냅샷 mfc.sp{n}_dev_mtorr, mfc.sp_sel_mtorr = 쓴 값 → 없으면 장비 값.
- pressure 블록 set=false: 쓰지 않고 선택만, 도달 대기 목표 = 장비에서 읽은 값(모르면 그 단계 실패 SP_DEVICE_VALUE_UNKNOWN),
  value 는 비워야 함(검사 오류).
- 설정 SP2~SP5_SOURCE(program·device): 기본 = v1(SP4 장비 값, 나머지 프로그램 값). 기본 템플릿이 따르고, 장비 값 단계의
  넣을지 비교는 장비 값으로 — 모르면 시작 거부. 단계에 들어가 다시 읽은 값이 다르면 경고 SP_DEVICE_CHANGED.
"""
import itertools

import pytest

from v2.common.paramdef import defaults
from v2.chk import blocks as BK
from v2.chk import cmd_guard as G
from v2.chk import params as P
from v2.chk import recipe as RC
from v2.chk.drivers.mfc import SP_READ_CMD
from v2.chk.process_steps import row_to_params
from v2.tests.test_mfc import Rig

CFG = defaults(P.PARAMS)
ROW = {"process_name": "t", "ar": True, "ar_flow": 20.0, "process_time": 0.1, "shutter_delay": 0.1, "use_dc": True,
       "dc_power": 100.0, "gun1": True}


def _params(wp, cfg):
    ok, errs, _ = RC.check_process_row(dict(ROW, working_pressure=float(wp)), cfg)
    assert not errs
    return row_to_params(ok, cfg)


def _press(blocks):
    return [(b["params"]["sp"], b["params"]["set"], b["params"]["value"], b["stage"]) for b in blocks if b["type"] == "pressure"]


# ───────── 설정 ─────────
def test_source_settings_default_like_v1():
    p = P.by_key()
    assert {n: p[f"SP{n}_SOURCE"].default for n in (2, 3, 4, 5)} == {2: "program", 3: "program", 4: "device", 5: "program"}
    for n in (2, 3, 4, 5):
        d = p[f"SP{n}_SOURCE"]
        assert d.enum == ("program", "device") and dict(d.enum_labels) == {"program": "프로그램 값", "device": "장비 값"}
        assert d.group == "process"
    for n in (2, 3, 4, 5):
        assert "프로그램 값일 때" in p[f"SP{n}_MTORR"].desc


# ───────── 기본 템플릿 ─────────
def test_default_template_sp4_select_only_like_v1():
    for wp in (2.0, 5.0, 60.0):
        bl = BK.template_blocks(_params(wp, CFG), CFG, ig_enabled=True, rfpulse_present=False)
        sp4 = [x for x in _press(bl) if x[0] == 4]
        assert sp4 == [(4, False, None, "GAS_STABILIZE")]
        steps, _ = BK.expand(bl, CFG, ig_enabled=True, rfpulse_present=False)
        cmds = [s.params[0] for s in steps if s.action == "MFC_CMD"]
        assert "SP4_SET" not in cmds and "SP4_ON" in cmds                       # v1: D4 만


@pytest.mark.parametrize("srcs", list(itertools.product(("program", "device"), repeat=4)))
def test_all_source_combinations(srcs):
    cfg = dict(CFG, **{f"SP{n}_SOURCE": s for n, s in zip((2, 3, 4, 5), srcs)})
    dev = {2: 7.0, 3: 20.0, 4: 30.0, 5: 40.0}                  # 장비 값이 설정과 다르다 — 비교가 무엇을 쓰는지 보인다
    src = dict(zip((2, 3, 4, 5), srcs))
    for wp in (4.0, 6.0, 18.0, 45.0, 60.0):
        bl = BK.template_blocks(_params(wp, cfg), cfg, ig_enabled=True, rfpulse_present=False, dev_sp=dev)
        got = {x[0]: x for x in _press(bl)}
        cmp_ = {n: (dev[n] if src[n] == "device" else float(cfg[f"SP{n}_MTORR"])) for n in (2, 3, 5)}
        assert (5 in got) == (wp > cmp_[5]) and (3 in got) == (wp < cmp_[3]) and (2 in got) == (wp < cmp_[2]), (srcs, wp)
        for n, (_, set_, val, _) in got.items():
            if n == 1:
                assert set_ is True and val == wp
            elif src[n] == "device":
                assert set_ is False and val is None
            else:
                assert set_ is True and val == (float(cfg[f"SP{n}_MTORR"]) if n != 4 else None)
        clean, errs, _ = BK.check_blocks(bl, cfg)
        assert errs == [] and clean is not None                                # 템플릿은 늘 블록 검사를 통과


@pytest.mark.parametrize("n", [2, 3, 5])
def test_device_value_unknown_refuses_template(n):
    cfg = dict(CFG, **{f"SP{n}_SOURCE": "device"})
    with pytest.raises(BK.TemplateError) as e:
        BK.template_blocks(_params(10.0, cfg), cfg, ig_enabled=True, dev_sp={k: 1.0 for k in (2, 3, 4, 5) if k != n})
    assert e.value.code == "SP_DEVICE_VALUE_UNKNOWN" and e.value.detail == f"SP{n}: 장비에 저장된 값을 읽지 못함"
    # SP4 는 비교가 없어 장비 값을 몰라도 템플릿은 만든다(단계에서 고르기만)
    BK.template_blocks(_params(10.0, CFG), CFG, ig_enabled=True, dev_sp={})


# ───────── 블록 검사·펼치기 ─────────
def test_set_false_with_value_is_error_and_without_value_ok():
    _, errs, _ = BK.check_blocks([{"type": "pressure", "params": {"sp": 3, "set": False, "value": 15.0}}], CFG)
    assert errs == ["블록 1(pressure): value: 프로그램 값을 장비에 쓰지 않으면(set=false) 비워야 함 — 장비에 저장된 SP3 값을 읽어 목표로 삼음"]
    clean, errs, _ = BK.check_blocks([{"type": "pressure", "params": {"sp": 3, "set": False}}], CFG)
    assert errs == [] and clean[0]["params"]["set"] is False
    _, errs, _ = BK.check_blocks([{"type": "pressure", "params": {"sp": 1, "set": False}}], CFG)
    assert errs == []                                                          # SP1 도 장비 값을 고를 수 있다(값 필요 규칙은 set=true 만)


def test_set_false_expansion_selects_only_and_waits_on_device_value():
    bl = [{"type": "pressure", "params": {"sp": 3, "set": False, "wait": True}}]
    steps, _ = BK.expand(BK.check_blocks(bl, CFG)[0], CFG)
    cmds = [(s.params[0], s.params[1]) for s in steps if s.action == "MFC_CMD"]
    assert [c for c, _ in cmds] == ["SP3_ON", "WAIT_PRESSURE"]
    a = cmds[1][1]
    assert a["device_sp"] == 3 and a["target"] is None
    assert "target=장비 값 SP3" in [s.message for s in steps if s.params and s.params[0] == "WAIT_PRESSURE"][0]
    bl = [{"type": "pressure", "params": {"sp": 4, "set": False, "wait": False}}]
    steps, _ = BK.expand(BK.check_blocks(bl, CFG)[0], CFG)
    assert [s.params[0] for s in steps if s.action == "MFC_CMD"] == ["SP4_ON"]  # set=false·wait=false = 예전과 같은 목록(선택 한 줄)


def test_block_texts_for_set():
    from v2.chk import web_schema as W
    d = [x for x in W.definitions()["blocks"]["defs"] if x["type"] == "pressure"][0]
    s = [x for x in d["params"] if x["name"] == "set"][0]
    assert s["label"] == "프로그램 값을 장비에 쓰기" and s["help"] == "끄면 장비에 저장된 값을 그대로 쓰고, 그 값을 읽어 목표로 삼습니다"
    assert not s.get("advanced")


# ───────── 드라이버 ─────────
def test_driver_reads_device_values_at_connect_and_snapshot():
    r = Rig()
    r.sim.sp[:] = [0.2, 0.5, 1.5, 3.0, 5.0]
    r2 = Rig(sim=r.sim)
    assert r2.d.sp_device == {1: 2.0, 2: 5.0, 3: 15.0, 4: 30.0, 5: 50.0}
    row = r2.run(1)[-1]
    assert [row[f"mfc.sp{n}_dev_mtorr"] for n in (1, 2, 3, 4, 5)] == [2.0, 5.0, 15.0, 30.0, 50.0]
    assert row["mfc.sp_sel_mtorr"] is None                                     # 선택 전
    assert r2.seq(r2.d.select_sp, 4) is None and r2.d.sp_sel_mtorr() == 30.0  # 쓴 값이 없으면 장비 값
    r.done()
    r2.done()


def test_driver_unreadable_value_is_unknown_with_raw_event():
    r = Rig()
    r.sim.canned["R3"] = ["P+0.20"]                                            # 형식 다름(접두어)
    assert r.seq(r.d.select_sp, 3) is None
    assert 3 not in r.d.sp_device and r.d.sp_sel_mtorr() is None
    ev = [e for e in r.events if e.kind == "sp_device"]
    assert ev and "SP3 장비 값을 읽지 못함(선택)" in ev[-1].detail and "'P+0.20'" in ev[-1].detail
    r.done()


def test_bench_connect_reads_setpoints_as_queries_only():
    r = Rig(writes=False)
    sent = [d.decode().strip() for _, d in r.sim.received]
    for q in SP_READ_CMD.values():
        assert q in sent and G.mfc_check(f"{q}\r".encode(), "bench") is None
    assert r.sim.set_commands_seen() == [] and all(x.startswith("R") for x in sent)
    assert len(r.d.sp_device) == 5
    r.done()


def test_periodic_refresh_only_when_idle_and_yields():
    r = Rig(cfg={"MFC_SP_REFRESH_SEC": 10})
    r.sim.sp[2] = 2.5                                                          # 장비 앞면에서 SP3 를 바꿈
    t0 = r.c.monotonic()
    r.d.idle = lambda: False                                                   # 공정 중 — 갱신하지 않는다
    r.run(15)
    assert "R3" not in [d.decode().strip() for t, d in r.sim.received if t >= t0]
    r.d.idle = lambda: True
    r.run(20)
    assert r.d.sp_device[3] == 25.0
    r0 = Rig(cfg={"MFC_SP_REFRESH_SEC": 0})
    t1 = r0.c.monotonic()
    r0.run(70)
    assert not [d for t, d in r0.sim.received if t >= t1 and d.decode().strip() in SP_READ_CMD.values()]
    r.done()
    r0.done()


# ───────── 공정(가상 시계 sim) ─────────
def _proc(tmp_path, cfg, setup=None, row=None, n=200, start_at=3):
    from v2.tests.test_process_engine import FAST, R
    r = R(tmp_path, dict(FAST, **cfg))
    if setup:
        setup(r)
    r.on(start_at, r.cmd("PROCESS_START", {"row": row or dict(ROW, working_pressure=5.0)}))
    r.run(n)
    return r


def test_process_start_refused_when_device_value_unknown(tmp_path):
    def setup(r):
        r.rig.sims["mfc"].canned["R3"] = ["P+0.00"] * 3                     # 연결 때 SP3 를 읽지 못함(형식 다름)
    r = _proc(tmp_path, {"SP3_SOURCE": "device"}, setup, n=8)
    res = r.results[-1]
    assert not res.ok and res.reason == "SP_DEVICE_VALUE_UNKNOWN" and res.detail == "SP3: 장비에 저장된 값을 읽지 못함"
    assert not r.ev("run_start")


def test_process_device_stage_uses_device_value_and_warns_when_changed(tmp_path):
    """SP3 장비 값 20 mTorr → WP 5 < 20 이라 SP3 단계가 들어간다(프로그램 값 15 였어도 들어감 — 값만 다름). 시작 뒤 장비에서 18 로
    바뀌면 단계에 들어가 다시 읽은 18 이 목표(경고 SP_DEVICE_CHANGED)."""
    def setup(r):
        r.rig.sims["mfc"].sp[2] = 2.0                                         # SP3 = 2.00 %FS = 20 mTorr
    def change():
        r.rig.sims["mfc"].sp[2] = 1.8
    from v2.tests.test_process_engine import FAST, R
    r = R(tmp_path, dict(FAST, SP3_SOURCE="device"))
    setup(r)
    r.on(3, r.cmd("PROCESS_START", {"row": dict(ROW, working_pressure=5.0)}))
    r.on(6, change)
    rows = []
    r.rig.loop.sinks.append(lambda x: rows.append((r.rig.process.state, x["mfc.sp_active"], x["mfc.sp_sel_mtorr"],
                                                   x["mfc.pressure_mtorr"])))
    r.run(220)
    assert [e.data["status"] for e in r.ev("run_end")] == ["done"]
    sent = [d.decode().strip() for _, d in r.rig.sims["mfc"].received]
    assert not any(w.startswith("S3") for w in sent)                          # 장비 값 — S3 를 쓰지 않는다
    warn = [e for e in r.ev("warn") if e.code == "SP_DEVICE_CHANGED"]
    assert warn and "20.00 → 지금 18.00" in warn[0].detail
    sp3 = [x for x in rows if x[0] == "PRESSURE_SP3"]
    assert sp3[-1][1:3] == (3, 18.0) and sp3[-1][3] == pytest.approx(18.0, rel=0.1)   # 새 값을 목표로 도달
    steps = r.ev("steps")[0].data["steps"]
    assert ["SP3_ON", "WAIT_PRESSURE"] == [s["params"][0] for s in steps if s["action"] == "MFC_CMD"
                                            and s["params"][0] in ("SP3_SET", "SP3_ON", "WAIT_PRESSURE")][:2]


def test_process_device_wait_fails_when_unreadable_at_stage(tmp_path):
    """블록 행 set=false·wait=true — 단계에서 장비 값을 못 읽으면 그 단계 실패(SP_DEVICE_VALUE_UNKNOWN)."""
    from v2.tests.test_process_engine import FAST, R
    r = R(tmp_path, FAST)
    def unread():
        r.rig.mfc.sp_device.pop(2, None)
        r.rig.sims["mfc"].canned["R2"] = ["x"] * 20
    blocks = [{"type": "gas_prep", "params": {"ar": True}}, {"type": "pressure", "params": {"sp": 2, "set": False, "wait": True}}]
    r.on(2, unread)
    r.on(3, r.cmd("PROCESS_START", {"blocks": blocks, "name": "dev"}))
    r.run(60)
    ends = r.ev("run_end")
    assert ends and ends[0].data["reason"] == "SP_DEVICE_VALUE_UNKNOWN"
    assert [e for e in r.ev("warn") if e.code == "SP_DEVICE_VALUE_UNKNOWN" and "SP2: 장비에 저장된 값을 읽지 못함" in e.detail]

