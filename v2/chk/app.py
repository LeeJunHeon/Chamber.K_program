"""챔버K v2 실행 프로그램(사용자 지시 5-6) — python -m v2.chk [--config-dir C:\\ChamberK_v2\\config]

시작 순서
  1 설정 읽기(ConfigStore — config_active·config_prev·local_override·config_history 가 --config-dir 에). notes 는 로그·콘솔에.
    RUN_MODE 는 설정에서만(기본 sim, prod 는 local_override.json 에 명시). 설정을 쓸 수 없으면 시작 거부(종료 코드 2).
  2 로컬 서버 포트 바인드(127.0.0.1:LOCAL_HMI_PORT) — 실패 = 이미 실행 중 → 알리고 끝(종료 코드 3). 장비 포트를 열기 전에.
  3 로그(LOCAL_DATA_DIR\\logs 회전 로그 — 콘솔에는 시작 줄·경고·오류만)
  4 조립(system.ChkSystem — sim: 시뮬레이터, bench·prod: SerialTransport + 그 모드 가드) → worker 시작 → tick 스레드 → 서버.
    bench·prod 에서 포트가 사용 중이거나 없으면 죽지 않고 worker 재연결로 계속 시도(링크 끊김으로 보임).
버전은 .git 의 HEAD·ref 파일을 읽어서(git 명령을 실행하지 않음), 못 읽으면 unknown.

종료(v1 main.py:4977 closeEvent 와 같은 조건)
- Ctrl+C: 공정 진행·정지 순서·CSV 리스트(딜레이·행 사이)·공정 밖 정리 중이거나 히터 가스가 잡혀 있으면 거부(콘솔에 이유).
  히터 운전·히터 레시피 중이면 "종료하면 히터가 꺼집니다 — 5초 안에 Ctrl+C 를 한 번 더"(v1 확인창 대신).
  정상 종료: 히터 레시피 정지 → S41(히터 RUN=0) → 결과를 짧게 기다림 → worker 정지·포트 닫기 → 기록 flush → 서버 정지.
- 강제 종료(콘솔 창 닫기·로그오프·윈도우 종료 — SetConsoleCtrlHandler): 거부할 수 없으므로 결정 4(2026-10-20)대로 파워 출력만 끈다 —
  DC OFF, RF DAC 0·enable OFF, RF Pulse OFF, 히터 RUN=0(히터 레시피 중단). PLC 릴레이(펌프·밸브·가스 차단밸브·셔터)와 MFC 는 그대로.
  공정 중이었으면 먼저 공정을 멈추고(program_exit — runner 취소·제어 루프 멈춤·ABORTED, run_end error PROGRAM_EXIT) 그다음 끄기 명령.
  창 닫기는 약 5초 안에 끝나야 하므로 확인을 기다리지 않는 명령을 먼저 보내고 기록 flush.
  로그오프·윈도우 종료: user32.dll 이 올라온 프로세스(이 파이썬은 처음부터 올라와 있다)에는 콘솔 신호가 오지 않으므로(Windows 규칙)
  숨은 최상위 창으로 WM_ENDSESSION 을 받아 같게 처리한다. 시작 때 user32 여부를 로그에 남긴다.
- 다음 시작 때는 S51(남은 파워 OFF)·S15(남은 히터 RUN 정리)가 정리한다.
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
from v2.chk.local_server import LocalServer
from v2.chk.system import ChkSystem, new_store, serial_transports

PROGRAM = "chk-v2"
DEFAULT_CONFIG_DIR = r"C:\ChamberK_v2\config"
CONFIRM_WINDOW_S = 5.0             # 히터 운전 중 Ctrl+C 두 번째 확인 시간
SHUTDOWN_HEATER_WAIT_S = 3.0       # 정상 종료: 히터 RUN=0 결과를 기다리는 시간
SIM_APP_LOG_KEEP = 1000            # 실행 프로그램 sim: 시뮬레이터가 받은 요청 기록 상한(시험용 기록 — 며칠 켜 둬도 늘지 않게)
FORCE_DEADLINE_S = 3.5             # 강제 종료: 이 시간 안에 끝낸다(창 닫기 한도 약 5초)
LOG_MAX_BYTES = 5_000_000
LOG_BACKUPS = 10
EXIT_OK, EXIT_CONFIG, EXIT_RUNNING, EXIT_START = 0, 2, 3, 4
CTRL_C, CTRL_BREAK, CTRL_CLOSE, CTRL_LOGOFF, CTRL_SHUTDOWN = 0, 1, 2, 5, 6
CTRL_NAMES = {CTRL_CLOSE: "CTRL_CLOSE", CTRL_LOGOFF: "CTRL_LOGOFF", CTRL_SHUTDOWN: "CTRL_SHUTDOWN"}
WM_QUERYENDSESSION, WM_ENDSESSION = 0x0011, 0x0016
WM_DESTROY, WM_CLOSE = 0x0002, 0x0010
REFUSE_PROCESS = "공정 중 — 로컬 화면·웹에서 STOP 먼저"
REFUSE_GAS = "히터 가스 사용 중 — 히터 가스를 해제한 뒤 종료"
CONFIRM_HEATER = "종료하면 히터가 꺼집니다 — 5초 안에 Ctrl+C 를 한 번 더"


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


def user32_loaded() -> Optional[bool]:
    """user32.dll 이 올라와 있으면 로그오프·윈도우 종료 신호(CTRL_LOGOFF·CTRL_SHUTDOWN)를 받지 못한다."""
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        k.GetModuleHandleW.restype = ctypes.c_void_p
        return bool(k.GetModuleHandleW("user32.dll"))
    except Exception:
        return None


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
        self.server: Optional[LocalServer] = None
        self.log = logging.getLogger("chk")
        self._log_handlers: List[logging.Handler] = []
        self._tick_thread: Optional[threading.Thread] = None
        self._log_thread: Optional[threading.Thread] = None
        self._log_sub = None
        self._confirm_t: Optional[float] = None
        self._ctrl_c = 0
        self._done = threading.Event()
        self._forced = threading.Lock()
        self.forced_result: Optional[dict] = None
        self.version = system.version if system is not None else git_version()
        self._ctrl_ref = None
        self._session: Optional[threading.Thread] = None
        self._session_hwnd = None
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

    # ───────── 2 포트 바인드 ─────────
    def bind_server(self) -> bool:
        self.server = LocalServer(int(self.cfg["LOCAL_HMI_PORT"]), self.cfg.get("LOCAL_HMI_STATIC_DIR"))
        try:
            self.server.bind()
        except OSError as e:
            self.say(f"[중지] 로컬 서버 포트 127.0.0.1:{self.cfg['LOCAL_HMI_PORT']} 를 열 수 없음 — 이미 실행 중일 수 있습니다"
                     f"(장비 포트는 열지 않고 끝냅니다): {e}")
            self.server = None
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
                f"로컬 화면 http://127.0.0.1:{self.server.port if self.server else c['LOCAL_HMI_PORT']}/ · "
                f"화면 파일 {c['LOCAL_HMI_STATIC_DIR']}",
                f"기록 {self.data_dir} · 로그 {self.data_dir / 'logs'} · NAS 사본 {nas}"]

    # ───────── 4 조립·시작 ─────────
    def build(self) -> ChkSystem:
        mode = RunMode(self.cfg["RUN_MODE"])
        if mode is RunMode.SIM:
            from v2.chk.sim.rig import make_sims, sim_transports
            _, self.sims = make_sims(self.clock.monotonic(), log_keep=SIM_APP_LOG_KEEP)
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
        if not self.bind_server():
            return EXIT_RUNNING
        try:
            self.setup_logging()
            for line in self._start_lines():
                self.say(line)
                self.log.info(line)
            for n in self.store.notes:
                self.say(f"설정: {n}")
                self.log.info("설정: %s", n)
            u32 = user32_loaded()
            self.log.info("로그오프·윈도우 종료 신호 수신 가능: %s",
                          "확인 못 함" if u32 is None else ("아니오(user32 로드됨)" if u32 else "예"))
            s = self.build()
            self._start_log_consumer()
            s.start(ready_timeout_s=float(self.cfg["CONNECT_INIT_TIMEOUT_SEC"]) + 3.0)
            down = [k for k, w in s.workers.items() if s.enabled[k] and not w.ready]
            msg = "장비 연결: " + ", ".join(f"{k} {'준비' if s.workers[k].ready else '연결 안 됨(재시도 중)'}"
                                         for k in s.workers if s.enabled[k])
            self.say(msg)
            (self.log.warning if down else self.log.info)(msg)
            self._tick_thread = self.clock.start_thread(s.loop.run, "tick")
            self.server.attach(s)
            self.server.start()
            self.say("실행 중 — 멈추려면 Ctrl+C (창을 닫으면 파워 출력만 끄고 끝납니다)")
        except Exception as e:
            self.say(f"[중지] 시작 실패: {e!r}")
            self.log.exception("시작 실패")
            self._teardown()
            return EXIT_START
        return EXIT_OK

    def _start_log_consumer(self) -> None:
        """장비 메시지(state 제외)를 로그 파일로 — MessageHub 의 소비자 하나(느리면 그 소비자만 끊기고 다시 붙는다)."""
        lv = {"error": logging.ERROR, "warn": logging.WARNING, "info": logging.INFO}

        def run():
            while not self._done.is_set():
                sub = self.system.hub.subscribe(name="log")
                self._log_sub = sub
                while True:
                    env = sub.get(1.0)
                    if env is None:
                        if self._done.is_set():
                            return
                        continue
                    if not isinstance(env, dict):
                        break
                    t, d = env["type"], env["data"]
                    if t == "state":
                        continue
                    if t == "event":
                        level = lv.get(d.get("level"), logging.INFO)
                        src = (d.get("context") or {}).get("source")
                        self.log.log(level, "%s %s %s%s", src, d.get("code"), d.get("message"),
                                     f" ({d['ruleId']})" if d.get("ruleId") else "")
                    else:
                        self.log.info("%s %s", t, _short(d))
        self._log_thread = threading.Thread(target=run, name="log-consumer", daemon=True)
        self._log_thread.start()

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
                if self.on_ctrl_c() == "exit":
                    self.shutdown()
                    return EXIT_OK
        return EXIT_OK

    def install_handlers(self) -> None:
        def sig(num, frame):
            self._ctrl_c += 1
        signal.signal(signal.SIGINT, sig)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, sig)
        if sys.platform == "win32":
            try:
                import ctypes
                from ctypes import wintypes
                proto = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)

                def handler(ctrl):
                    if ctrl in CTRL_NAMES:
                        self.force_exit(CTRL_NAMES[ctrl])
                        return True
                    return False                           # Ctrl+C·Break 는 파이썬 처리기(SIGINT)로
                self._ctrl_ref = proto(handler)
                ctypes.WinDLL("kernel32").SetConsoleCtrlHandler(self._ctrl_ref, True)
            except Exception as e:
                self.log.warning("콘솔 닫기 처리기 설치 실패: %r", e)
            self.start_session_window()

    def start_session_window(self) -> Optional[int]:
        """로그오프·윈도우 종료 — user32 가 올라온 프로세스는 콘솔 신호(CTRL_LOGOFF·CTRL_SHUTDOWN) 대신 최상위 창에
        WM_QUERYENDSESSION·WM_ENDSESSION 을 받는다. 숨은 창 하나로 받아 창 닫기와 같게(force_exit) 처리한다. hwnd 를 돌려준다."""
        if sys.platform != "win32":
            return None
        ready = threading.Event()
        box: Dict[str, Any] = {}
        t = threading.Thread(target=self._session_loop, args=(ready, box), name="session-watch", daemon=True)
        t.start()
        ready.wait(3.0)
        self._session, self._session_hwnd = t, box.get("hwnd")
        return self._session_hwnd

    def stop_session_window(self) -> None:
        """숨은 창 닫기(WM_CLOSE → WM_DESTROY → 메시지 루프 끝) — 정상 종료·시험에서 스레드를 남기지 않게."""
        if self._session is None:
            return
        if self._session_hwnd:
            import ctypes
            from ctypes import wintypes
            u = ctypes.WinDLL("user32")
            u.PostMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
            u.PostMessageW(self._session_hwnd, WM_CLOSE, 0, 0)
        self._session.join(2.0)
        self._session = self._session_hwnd = None

    def _session_loop(self, ready: threading.Event, box: Dict[str, Any]) -> None:
        try:
            import ctypes
            from ctypes import wintypes
            u = ctypes.WinDLL("user32", use_last_error=True)
            k = ctypes.WinDLL("kernel32", use_last_error=True)
            LRESULT = ctypes.c_ssize_t
            WNDPROC = ctypes.WINFUNCTYPE(LRESULT, wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)

            class WNDCLASSW(ctypes.Structure):
                _fields_ = [("style", wintypes.UINT), ("lpfnWndProc", WNDPROC), ("cbClsExtra", ctypes.c_int),
                            ("cbWndExtra", ctypes.c_int), ("hInstance", wintypes.HINSTANCE), ("hIcon", wintypes.HICON),
                            ("hCursor", wintypes.HANDLE), ("hbrBackground", wintypes.HBRUSH),
                            ("lpszMenuName", wintypes.LPCWSTR), ("lpszClassName", wintypes.LPCWSTR)]
            u.DefWindowProcW.restype = LRESULT
            u.DefWindowProcW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
            u.CreateWindowExW.restype = wintypes.HWND
            u.CreateWindowExW.argtypes = [wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD, ctypes.c_int,
                                          ctypes.c_int, ctypes.c_int, ctypes.c_int, wintypes.HWND, wintypes.HMENU,
                                          wintypes.HINSTANCE, wintypes.LPVOID]
            k.GetModuleHandleW.restype = wintypes.HMODULE
            u.UnregisterClassW.argtypes = [wintypes.LPCWSTR, wintypes.HINSTANCE]

            def proc(h, msg, wp, lp):
                if msg == WM_QUERYENDSESSION:
                    self.log.warning("로그오프·윈도우 종료 요청(WM_QUERYENDSESSION) 받음")
                    return 1                               # 거부하지 않는다(결정 4 — 파워 출력만 끄고 끝)
                if msg == WM_ENDSESSION:
                    if wp:
                        self.force_exit("WM_ENDSESSION")
                    return 0
                if msg == WM_DESTROY:
                    u.PostQuitMessage(0)
                    return 0
                return u.DefWindowProcW(h, msg, wp, lp)
            self._wndproc = WNDPROC(proc)
            wc = WNDCLASSW()
            wc.lpfnWndProc, wc.lpszClassName, wc.hInstance = self._wndproc, "ChkV2Session", k.GetModuleHandleW(None)
            u.RegisterClassW(ctypes.byref(wc))
            hwnd = u.CreateWindowExW(0, "ChkV2Session", "chk-v2 session", 0, 0, 0, 0, 0, None, None, wc.hInstance, None)
            box["hwnd"] = hwnd
            ready.set()
            if not hwnd:
                self.log.warning("로그오프·윈도우 종료 감시 창을 만들지 못함(%s)", ctypes.get_last_error())
                return
            self.log.info("로그오프·윈도우 종료 감시 창(WM_ENDSESSION) 준비")
            msg = wintypes.MSG()
            while u.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
                u.TranslateMessage(ctypes.byref(msg))
                u.DispatchMessageW(ctypes.byref(msg))
            u.UnregisterClassW("ChkV2Session", wc.hInstance)   # 다음 창(시험에서 다시 만들 때)이 이 창 처리기를 쓰지 않게
        except Exception as e:
            ready.set()
            self.log.warning("로그오프·윈도우 종료 감시 실패: %r", e)

    # ───────── Ctrl+C ─────────
    def exit_check(self) -> tuple:
        """("refuse", 이유) / ("confirm", 문구) / ("ok", "") — v1 closeEvent 와 같은 조건."""
        s = self.system
        p, h = s.process, s.heater
        if (p.active or p.state == "STOPPING" or getattr(p, "list_active", False) or getattr(p, "cleaning", False)
                or s.safety.engine.stopping):
            return "refuse", REFUSE_PROCESS
        if h.atm.is_active():
            return "refuse", REFUSE_GAS
        if h.recipe_running or h.st.get("run"):
            return "confirm", CONFIRM_HEATER
        return "ok", ""

    def on_ctrl_c(self) -> str:
        """"refused" / "confirm"(한 번 더 기다림) / "exit"."""
        kind, msg = self.exit_check()
        now = self.clock.monotonic()
        if kind == "refuse":
            self._confirm_t = None
            self.say(f"[종료 거부] {msg}")
            self.log.warning("종료 거부: %s", msg)
            return "refused"
        if kind == "confirm" and (self._confirm_t is None or now - self._confirm_t > CONFIRM_WINDOW_S):
            self._confirm_t = now
            self.say(f"[확인] {msg}")
            self.log.warning("종료 확인 대기: %s", msg)
            return "confirm"
        return "exit"

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
                # 지난 읽기 값(이미 RUN=0)이 아니라 쓰기 끝을 기다린다 — 같은 큐 뒤에 하나 더 넣어 S41 쓰기까지 끝난 뒤에
                # 포트를 닫는다(먼저 닫으면 S41 쓰기가 CANCELLED)
                res: Dict[str, Any] = {}

                def fin(r):
                    res["r"] = r.status
                    self.clock.notify()
                s.plc.heater_run(False, on_done=fin)
                ok = self.clock.wait_until(self.clock.monotonic() + SHUTDOWN_HEATER_WAIT_S, lambda: "r" in res)
                self.log.info("히터 RUN=0 쓰기: %s", res["r"] if ok else "시간 초과")
        self._teardown()
        self.say("종료")

    def _teardown(self) -> None:
        self._done.set()
        s = self.system
        if s is not None and s.started:
            s.stop()                                       # tick 멈춤 → worker 정지(포트 닫기) → 기록 flush → 메시지 끝
        if self._tick_thread is not None:
            self._tick_thread.join(3.0)
        if self.server is not None:
            self.server.stop()
        if self._log_thread is not None:
            self._log_thread.join(2.0)
        self.stop_session_window()
        for h in self._log_handlers:
            h.flush()
            self.log.removeHandler(h)
            h.close()
        self._log_handlers = []

    # ───────── 강제 종료(콘솔 창 닫기·로그오프·윈도우 종료) ─────────
    def force_exit(self, why: str = "CTRL_CLOSE") -> Optional[dict]:
        """결정 4 — 파워 출력만 끈다(확인을 기다리지 않고 먼저 보냄). PLC 릴레이·MFC 는 그대로."""
        if not self._forced.acquire(blocking=False):
            return None
        s = self.system
        if s is None:
            return None
        clk = self.clock
        t0 = clk.monotonic()
        p, h = s.process, s.heater
        was = bool(getattr(p, "row_is_process", False) and (p.active or p.state == "STOPPING"))
        done: Dict[str, bool] = {}
        sent: List[str] = []

        def cb(name):
            done[name] = False
            sent.append(name)

            def fin(*a):
                done[name] = True
                clk.notify()
            return fin
        # 공정을 먼저 멈춘다(runner 취소·제어 루프 멈춤·상태 ABORTED) — 끄기 명령 뒤에 하면 그 틈에 다음 블록(파워 켜기 등)이
        # 다시 켜거나, 안전 규칙이 꺼짐을 보고 정지 순서를 낼 수 있다
        if was and hasattr(p, "program_exit"):
            p.program_exit()
        for ctl in ("dcc", "rfc"):                         # 공정 밖(남긴 출력 유지 등)이어도 제어 루프가 다시 켜지 않게
            if hasattr(p, ctl):
                getattr(p, ctl).stop()
        s.dc.output_off_confirmed(on_done=cb("DC OFF"))
        s.plc.rf_off(on_done=cb("RF DAC 0·enable OFF"))
        if s.enabled["rfpulse"]:
            s.rfp.rf_off_confirmed(on_done=cb("RF Pulse OFF"))
        if h.recipe_running:
            h.recipe_abort("PROGRAM_EXIT")
            sent.append("히터 레시피 중단")
        s.plc.heater_run(False, on_done=cb("히터 RUN=0"))
        t_sent = clk.monotonic() - t0
        s.recorder.flush(1.0)
        clk.wait_until(t0 + FORCE_DEADLINE_S, lambda: all(done.values()))
        res = {"why": why, "sent": sent, "done": [k for k, v in done.items() if v],
               "sent_s": round(t_sent, 3), "elapsed_s": round(clk.monotonic() - t0, 3), "process_was_running": was}
        self.forced_result = res
        self.log.warning("강제 종료(%s) — 보낸 명령 %s, 끝난 것 %s, %.2f s(보내기 %.3f s)%s", why, sent, res["done"],
                         res["elapsed_s"], t_sent, " — run_end error PROGRAM_EXIT" if was else "")
        s.recorder.flush(0.5)
        for hd in self._log_handlers:
            hd.flush()
        return res


def _short(d: Any, n: int = 300) -> str:
    import json
    try:
        return json.dumps(d, ensure_ascii=False, default=str)[:n]
    except Exception:
        return repr(d)[:n]


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
