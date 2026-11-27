"""W-25 — 챔버12 에서 넘어온 것. 가상 시계·시뮬레이터만(실제 포트·네트워크 없음).

A1 RF 를 명령으로 끈 직후 Forward 가 1~2 s 늦게 내려와도 '명령 없는 RF 출력' 경고가 뜨지 않는다(RF_UNCOMMANDED_GRACE_SEC)
A2 PLC 읽기가 1~2번 빠져도 RF 화면용 값(rf.cmd_on·rf.uncommanded)이 꺼짐 그대로(묶기가 마지막 값을 싣는다)
A3 PLC 끊김(PLC_COMM_LOST)으로 중단하면 PLC 꺼짐 확인을 기다리지 않고 MFC 유량 끄기·DC·RF Pulse 끄기가 판정 뒤 5 s 안,
   PLC 가 돌아오면 RF DAC 0·허용 꺼짐·셔터·가스 밸브 닫힘(S10). PLC 가 살아 있는 STOP 은 그대로
"""
import pytest

from v2.chk import params as P
from v2.chk.sim.plc_sim import A
from v2.tests.test_process_engine import FAST, R, SHORT

RF_ROW = dict(SHORT, process_time=0.2, use_dc=False, use_rf=True, rf_power=300.0)
FASTER = dict(FAST, RF_RAMP_STEP_W=20.0, RF_RAMP_DOWN_STEP_COUNTS=400,    # 300 W 까지 15 s, 내리기도 빨리(시험 시간)
              RF_REFP_ABORT_THRESHOLD=10.0,                              # 모의 반사 1 % — 300 W 면 3 W(기본 기준) 라 올림
              RF_CAL_GAIN=1.0, RF_CAL_OFFSET=0.0)                       # 모의 RF 는 보정 없이 DAC 4000 = 600 W


def _unc_events(r):
    return [m["data"]["code"] for m in r.rig.hub.recent if m["type"] == "event" and m["data"]["context"]["source"] == "plc"
            and m["data"]["context"]["kind"] in ("rf_uncommanded", "rf_uncommanded_end")]


def _rf_run(tmp_path, how, lag=1.5, cfg=None, after=None, n=200):
    """300 W RF 공정 — how: end(정상 끝)·stop(STOP)·all_stop(ALL STOP)·abort(MV 인터락 꺼짐으로 중단)."""
    r = R(tmp_path, dict(FASTER, **(cfg or {})))
    plc = r.rig.sims["plc"]
    plc.rf_off_lag_s = lag
    box = {}

    def hook(t):
        p = r.rig.process
        if "main" not in box and p.state == "MAIN" and (plc.rf_fwd_w or 0) > 250:
            box["main"] = t.seq
        if "main" in box and "done" not in box and t.seq == box["main"] + 3:
            box["done"] = True
            if how == "stop":
                r.rig.core.command("PROCESS_STOP", {})
            elif how == "all_stop":
                r.rig.core.command("ALL_STOP", {})
            elif how == "abort":
                plc.itl_fault = True
        if after and "main" in box:
            after(r, t, box)
    r.rig.loop.tick_hooks.append(hook)
    r.on(3, r.cmd("PROCESS_START", {"row": RF_ROW}))
    r.run(n)
    assert "main" in box, "RF 300 W 에 닿지 못함"
    return r


@pytest.mark.parametrize("how", ["end", "stop", "all_stop", "abort"])
def test_a1_no_uncommanded_warning_when_forward_lags(tmp_path, how):
    r = _rf_run(tmp_path, how)
    assert _unc_events(r) == []
    assert r.rig.sims["plc"].rf_fwd_w < 1.0                      # RF 는 실제로 꺼졌다


def test_a1_grace_zero_keeps_old_behaviour(tmp_path):
    r = _rf_run(tmp_path, "stop", cfg={"RF_UNCOMMANDED_GRACE_SEC": 0.0})
    assert _unc_events(r)[:1] == ["RF_UNCOMMANDED"]              # 유예가 없으면 늦게 내려오는 Forward 에 경고(예전 동작)


def test_a1_warns_once_when_forward_stays_after_grace(tmp_path):
    """유예가 지난 뒤에도 Forward 가 기준 이상이면(앞면에서 켜 둠) 경고 한 번 — 유예 동안은 없음."""
    seen = {}

    def after(r, t, box):
        p = r.rig.process
        if "off" not in seen and box.get("done") and p.state in ("IDLE", "ABORTED"):
            seen["off"] = t.seq
            r.rig.sims["plc"].rf_local_w = 50.0                     # 끈 뒤에도 앞면 출력이 남음
        if "off" in seen and "first" not in seen and _unc_events(r):
            seen["first"] = t.seq
    r = _rf_run(tmp_path, "stop", lag=0.0, after=after)
    assert _unc_events(r) == ["RF_UNCOMMANDED"]
    grace = P.by_key()["RF_UNCOMMANDED_GRACE_SEC"].default
    assert grace == 5.0 and (P.by_key()["RF_UNCOMMANDED_GRACE_SEC"].min, P.by_key()["RF_UNCOMMANDED_GRACE_SEC"].max) == (0.0, 30.0)
    assert seen["first"] - seen["off"] >= 1                       # 바로가 아니라 마지막 명령 출력에서 유예가 지난 뒤


def test_a2_missed_plc_reads_keep_rf_off(tmp_path):
    r = R(tmp_path, dict(FAST))
    rows = []

    def hook(t):
        if t.seq in (10, 20):                                       # 한 번·두 번 빠짐
            now = r.c.monotonic()
            r.rig.sims["plc"].add_fault("no_response", now, now + (0.5 if t.seq == 10 else 1.5))
    r.rig.loop.tick_hooks.append(hook)
    r.rig.loop.sinks.append(rows.append)
    r.run(30)
    missed = [x for x in rows if x["plc.q"] != "ok"]
    assert len(missed) >= 3
    assert all(x["rf.cmd_on"] is False and x["rf.uncommanded"] is False for x in rows[5:])


def _plc_drop_run(tmp_path, row, down=60.0, n=170, cfg=None):
    r = R(tmp_path, dict(FASTER, **(cfg or {})))
    box = {}

    def hook(t):
        if "t" not in box and r.rig.process.state == "MAIN":
            box["t"] = r.c.monotonic()
            r.rig.sims["plc"].add_fault("disconnect", box["t"], box["t"] + down)
    r.rig.loop.tick_hooks.append(hook)
    r.on(3, r.cmd("PROCESS_START", {"row": row}))
    r.run(n)
    ab = [e for e in r.rig.events if type(e).__name__ == "SafetyEvent" and e.kind == "abort" and e.code == "PLC_COMM_LOST"]
    assert ab, "PLC_COMM_LOST 중단이 없음"
    t_ab = ab[0].t - r.c.wall() + r.c.monotonic()                 # 사건 시각(벽시계) → 단조 시계
    return r, box, t_ab


@pytest.mark.parametrize("row", [dict(RF_ROW, process_time=3.0, rf_power=100.0),
                                 dict(RF_ROW, process_time=3.0, rf_power=100.0, use_dc=True, gun2=True)],
                         ids=["rf", "rf_dc"])
def test_a3_plc_lost_stop_does_not_wait_for_plc(tmp_path, row):
    r, box, t_ab = _plc_drop_run(tmp_path, row)
    l0 = [t for t, d in r.rig.sims["mfc"].received if d.startswith(b"L0") and t > box["t"]]
    assert l0 and l0[0] - t_ab <= 5.0, (l0[:1], t_ab)
    if row.get("use_dc"):
        off = [t for t, d in r.rig.sims["dc"].received if t > box["t"] and b"OUTP" in d.upper() and (b"OFF" in d.upper() or b" 0" in d)]
        assert off and off[0] - t_ab <= 5.0
    steps = [e for e in r.rig.events if type(e).__name__ == "SafetyEvent" and e.kind == "step" and e.rule == "abort"]
    assert any("RF_RAMP_DOWN" == e.code and "건너뜀" in e.detail for e in steps)
    # 60 s 뒤 PLC 복구 → S10: RF DAC 0·허용 꺼짐·셔터 닫힘·가스 밸브 닫힘
    plc = r.rig.sims["plc"]
    assert plc.regs[A["rf_dac"]] == 0 and not plc.coils[A["rf_dac_enable"]]
    assert not any(plc.coils[A[k]] for k in ("ms", "s1", "s2", "ar_valve", "o2_valve"))
    fired = [e for e in r.rig.events if type(e).__name__ == "SafetyEvent" and e.kind == "fired" and e.rule == "S10"]
    assert fired


def test_a3_rfpulse_off_sent_quickly(tmp_path):
    row = dict(SHORT, process_time=3.0, use_dc=False, use_rf_pulse=True, rf_pulse_power=100.0, rf_pulse_freq=10.0,
               rf_pulse_duty=50, gun1=True)
    r, box, t_ab = _plc_drop_run(tmp_path, row, cfg={"RFPULSE_ENABLED": True})
    rfp = r.rig.sims["rfpulse"]
    offs = [t for t, d in rfp.received if len(d) > 1 and d[1] == 1 and t > box["t"]]   # AE Bus 명령 1 = 출력 끄기
    l0 = [t for t, d in r.rig.sims["mfc"].received if d.startswith(b"L0") and t > box["t"]]
    assert l0 and l0[0] - t_ab <= 5.0
    assert offs and offs[0] - t_ab <= 5.0


def test_a3_stop_with_plc_alive_unchanged(tmp_path):
    """PLC 가 살아 있는 STOP — RF 천천히 내리기를 그대로 거친다(건너뛰지 않음)."""
    r = _rf_run(tmp_path, "stop", lag=0.0)
    steps = [e for e in r.rig.events if type(e).__name__ == "SafetyEvent" and e.kind == "step" and e.rule == "stop"]
    ramp = [e for e in steps if e.code == "RF_RAMP_DOWN"]
    assert ramp and "건너뜀" not in ramp[0].detail and "보내기만" not in " ".join(e.detail for e in steps)


# ───────── A5 절전 막기의 한계 — 윈도우 전원 설정 읽기(가짜 API) ─────────
from v2.chk import keep_awake as KA


def _power(info=None, err=None):
    def read():
        if err:
            raise err
        return info
    return read


@pytest.mark.parametrize("name,info,want", [
    ("정상", KA.PowerInfo(lid_ac=0, lid_dc=0, sleep_ac_s=0, on_ac=True), None),
    ("덮개 절전", KA.PowerInfo(lid_ac=1, lid_dc=1, sleep_ac_s=0, on_ac=True), "덮개를 닫으면 절전됩니다(전원 연결 시)"),
    ("배터리", KA.PowerInfo(lid_ac=0, lid_dc=0, sleep_ac_s=0, on_ac=False), "지금 배터리로 돌고 있습니다"),
    ("절전 시간", KA.PowerInfo(lid_ac=0, lid_dc=0, sleep_ac_s=1800, on_ac=True), "전원 연결 시 30분 동안 쓰지 않으면 절전됩니다"),
])
def test_a5_power_settings_warning(name, info, want):
    said, evs = [], []
    msg = KA.check_power(_power(info), said.append, evs.append)
    if want is None:
        assert msg is None and said == [] and evs == []
    else:
        assert want in msg and msg.startswith("장비 노트북 전원 설정: ") and "윈도우 전원 설정에서" in msg
        assert said == [f"[주의] {msg}"]
        assert len(evs) == 1 and evs[0].kind == "power_settings" and evs[0].reason == "POWER_SETTINGS"


def test_a5_not_windows_or_unreadable_is_quiet():
    said, evs = [], []
    assert KA.check_power(None, said.append, evs.append) is None and said == [] and evs == []     # 윈도우 아님
    assert KA.check_power(_power(err=OSError("x")), said.append, evs.append) is None
    assert said == [] and len(evs) == 1 and evs[0].kind == "power_settings_unread" and evs[0].reason is None   # 상세 기록만
    assert KA.windows_power() is None                                   # 이 시험 기계(리눅스)는 윈도우가 아님
    from v2.chk import commands as C, web_schema as W
    assert "POWER_SETTINGS" in C.REASON_HELP
    lv = {(e[0], e[1]): e[3] for e in W.EVENT_KINDS}
    assert lv[("app", "power_settings")] == "main" and lv[("app", "power_settings_unread")] == "detail"
    d = P.by_key()["KEEP_AWAKE"].desc
    assert "덮개 닫기·전원 버튼처럼 사람이 직접 시킨 절전은 막지 못하므로" in d


# ───────── A6 사유 이름·'두절' → '끊김' ─────────
def test_a6_mfc_comm_lost_and_no_dujeol_words():
    from v2.chk import commands as C, safety_rules as SR, web_schema as W
    from v2.common.paramdef import defaults
    codes = [r.code for r in C.CHK_REASONS]
    assert "MFC_COMM_LOST" in codes and "MFC_COMM_LOSS" not in codes and "MFC_COMM_LOSS" not in C.REASON_HELP
    s52 = next(r for r in SR.RULES if r.id == "S52")
    assert any(getattr(a, "args", {}).get("reason") == "MFC_COMM_LOST" for a in s52.actions)
    labels = {p.key: p.label for p in P.PARAMS}
    assert labels["PLC_COMM_LOSS_ABORT_SEC"] == "PLC 끊김 공정 중단" and labels["COMM_LONG_OUTAGE_SEC"] == "장기 끊김 판정"
    assert labels["IG_COMM_LOSS_ABORT_SEC"] == "Ion Gauge 끊김 베이스 대기 중단" and labels["COMM_OUTAGE_LOG_SEC"] == "끊김 기록 간격"
    b = W.bundle(defaults(P.PARAMS))
    import json
    assert "두절" not in json.dumps(b, ensure_ascii=False)


# ───────── C1 정상 종료 끝 표시(bye) ─────────
def test_c1_shutdown_sends_bye_last(tmp_path):
    from v2.tests.fake_web import FakeWeb
    from v2.tests.test_uplink import _start
    web = FakeWeb().up()
    try:
        app = _start(tmp_path, web)
        assert web.wait(lambda: any(p.get("code") == 200 for p in web.posts), 15)
        app.shutdown()
        posts = [p for p in web.posts if p.get("code") == 200]
        assert posts[-1]["bye"] is True and posts[-1]["n"] == 0
        assert sum(1 for p in posts if p["bye"]) == 1 and all(not p["bye"] for p in posts[:-1])
    finally:
        web.down()


def test_c1_window_close_sends_bye(tmp_path):
    from v2.tests.fake_web import FakeWeb
    from v2.tests.test_uplink import _start
    web = FakeWeb().up()
    try:
        app = _start(tmp_path, web)
        assert web.wait(lambda: any(p.get("code") == 200 for p in web.posts), 15)
        res = app.force_exit("CTRL_CLOSE")
        assert res["bye"] is True
        n = len(web.posts)
        assert web.posts[-1]["bye"] is True
        import time
        time.sleep(1.5)
        assert len(web.posts) == n                                         # 끝 표시 뒤로는 보내지 않는다
        app._teardown()
    finally:
        web.down()


# ───────── C2 아무 동작도 하지 않는 설정 ─────────
def test_c2_unused_settings_marked_not_removed():
    from v2.chk import web_schema as W
    from v2.common.paramdef import defaults
    want = {"IG_GAUGE_A_NAME", "IG_GAUGE_B_NAME", "IG_VALUE_MAX_AGE_SEC", "IG_BASE_LOG_INTERVAL_SEC",
            "HEATER_GAS_MFC_TIMEOUT_SEC", "RFPULSE_COMM_REOPEN_SEC", "DC_INITIAL_VOLTAGE"}
    assert P.UNUSED_KEYS == want and want <= set(P.by_key())               # 키는 그대로
    ps = {p["key"]: p for p in W.bundle(defaults(P.PARAMS))["params"]}
    assert {k for k, p in ps.items() if p["unused"]} == want
    # 코드에서 읽는 곳이 없다(설정 정의·설명용 목록 말고) — cfg["키"]·cfg.get("키") 꼴
    import pathlib, re
    root = pathlib.Path(__file__).resolve().parents[1]
    src = "\n".join(p.read_text(encoding="utf-8") for p in (root / "chk").rglob("*.py") if p.name != "params.py")
    for k in want:
        assert not re.search(rf"""(cfg|c|self\.cfg)\s*(\[\s*["']{k}["']|\.get\(\s*["']{k}["'])""", src), k
