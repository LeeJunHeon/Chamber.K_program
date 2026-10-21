"""챔버K v2 실행 프로그램(사용자 지시 5-6) — python -m v2.chk [--config-dir C:\\ChamberK_v2\\config]

시작 순서
  1 설정 읽기(ConfigStore — config_active·config_prev·local_override·config_history 가 --config-dir 에). notes 는 로그·콘솔에.
    RUN_MODE 는 설정에서만(기본 sim, prod 는 local_override.json 에 명시). 설정을 쓸 수 없으면 시작 거부(종료 코드 2).
  3 로그(LOCAL_DATA_DIR\\logs 회전 로그 — 콘솔에는 시작 줄·경고·오류만)
  4 조립(system.ChkSystem — sim: 시뮬레이터, bench·prod: SerialTransport + 그 모드 가드) → worker 시작 → tick 스레드 → 서버.
    bench·prod 에서 포트가 사용 중이거나 없으면 죽지 않고 worker 재연결로 계속 시도(링크 끊김으로 보임).
버전은 .git 의 HEAD·ref 파일을 읽어서(git 명령을 실행하지 않음), 못 읽으면 unknown.

종료: Ctrl+C → 정상 종료(히터 레시피 정지 → S41(히터 RUN=0) → 결과를 짧게 기다림 → worker 정지·포트 닫기 →
기록 flush → 서버 정지).
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import RealClock
from v2.common.config_store import ConfigError
from v2.common.mode import RunMode, policy, resolve_mode
from v2.chk.system import ChkSystem, new_store, serial_transports

PROGRAM = "chk-v2"
DEFAULT_CONFIG_DIR = r"C:\ChamberK_v2\config"
SHUTDOWN_HEATER_WAIT_S = 3.0       # 정상 종료: 히터 RUN=0 결과를 기다리는 시간
LOG_MAX_BYTES = 5_000_000
LOG_BACKUPS = 10
EXIT_OK, EXIT_CONFIG, EXIT_START = 0, 2, 4


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def git_version(root: Optional[Path] = None) -> str:
    """.git 의 HEAD·ref 파일만 읽는다(git 명령 없음). 브랜치@SHA7, 분리 HEAD 면 SHA7, 못 읽으면 unknown."""
    try:
        g = (root or repo_root()) / ".git"
        if g.is_file():                                   # 작업 트리(.git 파일 = gitdir: …)
            g = (g.parent / g.read_text(encoding="utf-8").split(":", 1)[1].strip()).resolve()
        head = (g / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref:"):
            return head[:7]
        ref = head.split(":", 1)[1].strip()
        f = g / ref
        sha = f.read_text(encoding="utf-8").strip() if f.is_file() else None
        if sha is None and (g / "packed-refs").is_file():
            for line in (g / "packed-refs").read_text(encoding="utf-8").splitlines():
                if line.endswith(" " + ref):
                    sha = line.split(" ", 1)[0]
        branch = ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else ref
        return f"{branch}@{sha[:7]}" if sha else f"{branch}@unknown"
    except (OSError, IndexError, ValueError):
        return "unknown"


class App:
    def __init__(self, config_dir: str = DEFAULT_CONFIG_DIR, print_fn: Callable[[str], None] = print,
                 serial_factory: Optional[Callable] = None, clock=None, system: Optional[ChkSystem] = None) -> None:
        self.config_dir = Path(config_dir)
        self.say = print_fn
        self.serial_factory = serial_factory
        self.clock = clock or RealClock()
        self.system: Optional[ChkSystem] = system
        self.store = system.store if system is not None else None
        self.cfg: Dict[str, Any] = system.cfg if system is not None else {}
        self.log = logging.getLogger("chk")
        self._log_handlers: List[logging.Handler] = []
        self._tick_thread: Optional[threading.Thread] = None
        self._ctrl_c = 0
        self._done = threading.Event()
        self.version = system.version if system is not None else git_version()
        self.sims = None

    # ───────── 1 설정 ─────────
    def load_config(self) -> bool:
        self.store = new_store(self.config_dir)
        try:
            self.cfg = dict(self.store.load())
            resolve_mode(self.cfg.get("RUN_MODE"))
        except (ConfigError, ValueError) as e:
            self.say(f"[중지] 설정을 쓸 수 없음 — 시작 거부: {e}")
            return False
        return True

    # ───────── 3 로그 ─────────
    @property
    def data_dir(self) -> Path:
        return Path(self.cfg["LOCAL_DATA_DIR"])

    def setup_logging(self) -> None:
        logs = self.data_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        fh = logging.handlers.RotatingFileHandler(logs / "chk_v2.log", maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS,
                                                  encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        fh.setLevel(logging.INFO)
        say = self.say

        class _Console(logging.Handler):                  # 콘솔은 경고·오류만(시작 줄은 따로)
            def emit(self, rec):
                try:
                    say(f"[{rec.levelname}] {rec.getMessage()}")
                except Exception:
                    pass
        ch = _Console(logging.WARNING)
        for h in (fh, ch):
            self.log.addHandler(h)
            self._log_handlers.append(h)

    def _start_lines(self) -> List[str]:
        c, st = self.cfg, self.store
        mode = RunMode(c["RUN_MODE"])
        pol = policy(mode)
        nas = c.get("NAS_COPY_DIR") if pol.nas_copy and c.get("NAS_COPY_DIR") else "끔"
        return [f"챔버K v2 — 모드 {mode.value} · 장비 {c['EQUIPMENT_ID']} · 버전 {self.version}",
                f"설정 버전 {st.version}({st.source}) · 설정 폴더 {self.config_dir}",
                f"기록 {self.data_dir} · 로그 {self.data_dir / 'logs'} · NAS 사본 {nas}"]

    # ───────── 4 조립·시작 ─────────
    def build(self) -> ChkSystem:
        mode = RunMode(self.cfg["RUN_MODE"])
        if mode is RunMode.SIM:
            from v2.chk.sim.rig import make_sims, sim_transports
            _, self.sims = make_sims(self.clock.monotonic())
            tr = sim_transports(self.clock, self.sims)
        else:
            tr = serial_transports(self.clock, self.cfg, serial_factory=self.serial_factory)
        self.system = ChkSystem(self.clock, self.cfg, self.data_dir, tr, mode=mode.value, store=self.store,
                                nas_dir=self.cfg.get("NAS_COPY_DIR") or None, program=PROGRAM, version=self.version)
        return self.system

    def start(self) -> int:
        """1~4 단계. 0 이면 실행 중(run 으로 기다림), 아니면 종료 코드."""
        if not self.load_config():
            return EXIT_CONFIG
        try:
            self.setup_logging()
            for line in self._start_lines():
                self.say(line)
                self.log.info(line)
            for n in self.store.notes:
                self.say(f"설정: {n}")
                self.log.info("설정: %s", n)
            s = self.build()
            s.start(ready_timeout_s=float(self.cfg["CONNECT_INIT_TIMEOUT_SEC"]) + 3.0)
            down = [k for k, w in s.workers.items() if s.enabled[k] and not w.ready]
            msg = "장비 연결: " + ", ".join(f"{k} {'준비' if s.workers[k].ready else '연결 안 됨(재시도 중)'}"
                                         for k in s.workers if s.enabled[k])
            self.say(msg)
            (self.log.warning if down else self.log.info)(msg)
            self._tick_thread = self.clock.start_thread(s.loop.run, "tick")
            self.say("실행 중 — 멈추려면 Ctrl+C")
        except Exception as e:
            self.say(f"[중지] 시작 실패: {e!r}")
            self.log.exception("시작 실패")
            self._teardown()
            return EXIT_START
        return EXIT_OK

    # ───────── 실행 ─────────
    def run(self) -> int:
        code = self.start()
        if code != EXIT_OK:
            return code
        self.install_handlers()
        while not self._done.is_set():
            try:
                time.sleep(0.2)
            except KeyboardInterrupt:                      # 처리기 설치 전에 온 Ctrl+C
                self._ctrl_c += 1
            while self._ctrl_c > 0:
                self._ctrl_c -= 1
                self.shutdown()
                return EXIT_OK
        return EXIT_OK

    def install_handlers(self) -> None:
        def sig(num, frame):
            self._ctrl_c += 1
        signal.signal(signal.SIGINT, sig)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, sig)

    def shutdown(self) -> None:
        """정상 종료 — 히터 레시피 정지 → S41(히터 RUN=0) → 결과 기다림 → worker 정지·포트 닫기 → 기록 flush → 서버 정지."""
        s = self.system
        self.say("종료 중 — 히터 OFF·포트 닫기·기록 정리")
        self.log.info("정상 종료 시작")
        if s is not None:
            if s.heater.recipe_running:
                s.heater.recipe_stop("프로그램 종료")
            s.safety.event("shutdown")                     # S41 HEATER_FORCE_OFF
            if s.workers["plc"].ready:
                ok = self.clock.wait_until(self.clock.monotonic() + SHUTDOWN_HEATER_WAIT_S,
                                           lambda: bool(s.plc.last) and s.plc.last.values.get("heater.run") is False)
                self.log.info("히터 RUN=0 확인: %s", ok)
        self._teardown()
        self.say("종료")

    def _teardown(self) -> None:
        self._done.set()
        s = self.system
        if s is not None and s.started:
            s.stop()                                       # tick 멈춤 → worker 정지(포트 닫기) → 기록 flush → 메시지 끝
        if self._tick_thread is not None:
            self._tick_thread.join(3.0)
        for h in self._log_handlers:
            h.flush()
            self.log.removeHandler(h)
            h.close()
        self._log_handlers = []

def main(argv=None) -> int:
    for st in (sys.stdout, sys.stderr):
        try:
            st.reconfigure(errors="replace")
        except Exception:
            pass
    ap = argparse.ArgumentParser(prog="python -m v2.chk", description="챔버K v2 실행 프로그램")
    ap.add_argument("--config-dir", default=DEFAULT_CONFIG_DIR,
                    help="config_active.json·config_prev.json·local_override.json·config_history.jsonl 폴더")
    a = ap.parse_args(argv)
    return App(a.config_dir, print_fn=lambda s: print(s, flush=True)).run()
