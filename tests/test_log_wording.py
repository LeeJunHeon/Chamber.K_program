# -*- coding: utf-8 -*-
"""T105~ 로그 문구가 실제 동작과 일치하는지(동작 변경 없음). 2026-09-27 23:25 CeO2 #2-4 로그 기준."""
import pytest

import controller.process_controller as PC
import lib.logger as LG


# ───────── HEATER_WAIT 스텝 문구 / 대기 시작 줄 ─────────
def test_T105_heater_wait_step_text_has_no_fixed_timeout():
    import inspect
    src = inspect.getsource(PC.SputterProcessController._build_steps) if hasattr(PC.SputterProcessController, "_build_steps") \
        else inspect.getsource(PC.SputterProcessController)
    i = src.index("히터 온도 도달 대기")
    line = src[i - 200:i + 200]
    assert "timeout" not in line and "HEATER_WAIT_TIMEOUT_SEC" not in line.split("value=heater_temp")[0]
    assert "유지)" in line


class _WaitHarness:
    """_heater_wait 의 타임아웃 계산·문구만 본다(대기 루프는 즉시 끝나게)."""
    def __init__(self, monkeypatch, pv, target=600.0, rate=6.0):
        self.msgs = []
        c = PC.SputterProcessController.__new__(PC.SputterProcessController)
        c.status_message = type("S", (), {"emit": lambda _s, l, m: self.msgs.append((l, m))})()
        c.plc = type("P", (), {"get_heater_status": staticmethod(lambda: ({"pv": pv} if pv is not None else {})),
                               "update_heater_status": type("Sig", (), {"connect": staticmethod(lambda f: None),
                                                                        "disconnect": staticmethod(lambda f: None)})()})()
        c._heater_ramp_c_per_min = rate
        c._running = False; c._stop_pending = True        # 루프 직후 빠져나오게
        c._active_loops = []
        monkeypatch.setattr(c, "_exec_loop_with_timeout", lambda loop, ms, timeout_message=None: self.__dict__.update(ms=ms) or True)
        self.c = c; self.target = target

    def run(self):
        self.c._heater_wait(self.target)
        return [m for l, m in self.msgs if "승온 대기 시작" in m][0], self.ms


def test_T106_wait_timeout_reason_estimated(monkeypatch):
    h = _WaitHarness(monkeypatch, pv=29.5)               # 29.5→600, 6°C/min → 예상 ≈99분 + 여유 30분
    msg, ms = h.run()
    assert "타임아웃 129분 = 예상 승온 99분 + 여유 30분 (6°C/min 기준)" in msg
    assert ms == int(129 * 60 * 1000) or abs(ms - 129 * 60 * 1000) < 60 * 1000


def test_T107_wait_timeout_reason_default(monkeypatch):
    h = _WaitHarness(monkeypatch, pv=595.0)              # 5°C 만 올리면 되므로 기본값(90분)이 더 크다
    msg, ms = h.run()
    assert "타임아웃 90분 = 기본값 HEATER_WAIT_TIMEOUT_SEC" in msg
    assert ms == int(float(PC.HEATER_WAIT_TIMEOUT_SEC) * 1000)


def test_T108_wait_timeout_reason_cap(monkeypatch):
    h = _WaitHarness(monkeypatch, pv=0.0, target=600.0, rate=0.5)   # 아주 느린 램프 → 상한 12시간
    msg, ms = h.run()
    assert f"상한 WAIT_TIMEOUT_MAX_SEC({PC.WAIT_TIMEOUT_MAX_SEC / 60:.0f}분)" in msg
    assert ms == int(PC.WAIT_TIMEOUT_MAX_SEC * 1000)


def test_T109_wait_timeout_reason_unknown_pv(monkeypatch):
    h = _WaitHarness(monkeypatch, pv=None)
    msg, ms = h.run()
    assert "현재 온도 미상 → 기본값 HEATER_WAIT_TIMEOUT_SEC" in msg and "--.-°C" in msg
    assert ms == int(float(PC.HEATER_WAIT_TIMEOUT_SEC) * 1000)


# ───────── 로거 말머리 중복 ─────────
def test_T110_logger_strips_duplicate_heater_prefix_only_for_heater_level(tmp_path, monkeypatch):
    proc = tmp_path / "proc.txt"; heat = tmp_path / "heat.txt"
    monkeypatch.setattr(LG, "_current_log_file", proc)
    monkeypatch.setattr(LG, "_heater_log_file", heat)
    LG.log_message_to_file("히터", "[히터] 램프 완료 600.0°C")
    LG.log_message_to_file("경고", "[히터] PV=69.8 ITL=1 OT=0")
    LG.log_message_to_file("히터", "TC2 추종 시작")
    LG.log_message_to_file("정보", "공정 시작")
    pl = proc.read_text(encoding="utf-8").splitlines()
    hl = heat.read_text(encoding="utf-8").splitlines()
    assert pl[0].endswith("[히터] 램프 완료 600.0°C") and "[히터] [히터]" not in pl[0]
    assert pl[1].endswith("[경고] [히터] PV=69.8 ITL=1 OT=0")        # level 이 히터가 아니면 말머리 유지
    assert pl[2].endswith("[히터] TC2 추종 시작") and pl[3].endswith("[정보] 공정 시작")
    # 히터 로그 라우팅은 수정 전과 같다(히터 level + 메시지 말머리 둘 다)
    assert len(hl) == 3 and hl[0].endswith("[히터] 램프 완료 600.0°C") and hl[1].endswith("[히터] PV=69.8 ITL=1 OT=0")
    assert all("공정 시작" not in l for l in hl)


def test_T110b_strip_helper_rules():
    assert LG._strip_dup_prefix("히터", "[히터] X") == "X"
    assert LG._strip_dup_prefix("히터(경고)", "[히터] X") == "X"
    assert LG._strip_dup_prefix("경고", "[히터] X") == "[히터] X"
    assert LG._strip_dup_prefix("히터", "X") == "X"
    assert LG._strip_dup_prefix("히터", "[히터]X") == "[히터]X"      # 공백 없는 형태는 건드리지 않는다
