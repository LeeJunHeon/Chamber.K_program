"""노트북 화면 서버 — 끊긴 연결·처리 예외가 실행 창(콘솔·stderr)에 Traceback 을 찍지 않는다(5-21 ①).

11-23 bench: 새로 고침 중 hello 응답을 쓰다 브라우저가 끊어 ConnectionAbortedError Traceback 이 실행 창에 찍혔다.
가상 장비(sim)만 — 실제 포트 없음. 응답 도중 끊는 클라이언트는 SO_LINGER 0(RST)으로 바로 닫는다.
"""
import json
import logging
import socket
import struct
import time
import urllib.request

from v2.chk.app import EXIT_OK, App
from v2.chk.local_server import DISCONNECTS, _Server
from v2.tests.test_app import _cfg_dir, _free_port


def _rude(port: int, path: str, method: str = "GET", body: bytes = b"") -> None:
    """요청을 보내고 응답을 읽지 않은 채 RST 로 끊는다."""
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    req = f"{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
    s.sendall(req)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    s.close()


def test_disconnects_and_errors_do_not_reach_console(tmp_path, capfd):
    port = _free_port()
    lines = []
    (tmp_path / "hmi").mkdir()
    (tmp_path / "hmi" / "index.html").write_text("x" * 3_000_000, encoding="utf-8")   # 큰 정적 파일 — 쓰는 도중 끊김
    app = App(str(_cfg_dir(tmp_path, port)), print_fn=lines.append)
    assert app.start() == EXIT_OK
    base = f"http://127.0.0.1:{port}"
    capfd.readouterr()
    n0 = len(lines)
    try:
        for _ in range(3):
            for path in ("/api/v1/hello", "/api/v1/schema", "/", "/api/v1/stream"):
                _rude(port, path)
            _rude(port, "/api/v1/command", "POST", json.dumps({"kind": "BUZZ_STOP", "args": {}}).encode())
        time.sleep(1.5)
        h = json.loads(urllib.request.urlopen(base + "/api/v1/hello", timeout=5).read())      # 다음 요청은 정상
        assert h["equipment"] == "CHK2"
        srv = app.server.httpd
        # 그 밖의 예외 — 로그 파일에는 Traceback, 콘솔에는 없음, 같은 종류는 1분에 한 번
        for _ in range(3):
            try:
                raise RuntimeError("처리 중 오류(시험)")
            except RuntimeError:
                srv.handle_error(None, ("127.0.0.1", 0))
        assert srv.errors == 3 and srv.disconnects >= 1                    # 끊긴 연결을 실제로 겪었다
        out = capfd.readouterr()
        assert out.err == "" and out.out == ""
        assert lines[n0:] == []                                                    # 앱 콘솔 줄도 없음
    finally:
        app.shutdown()
    log = (tmp_path / "data" / "logs" / "chk_v2.log").read_text(encoding="utf-8")
    assert log.count("로컬 화면 요청 처리 중 예외(RuntimeError)") == 1 and "Traceback" in log
    assert "ConnectionAbortedError" not in "".join(lines)


def test_handle_error_classifies_disconnects():
    srv = _Server.__new__(_Server)
    srv._err_at, srv.disconnects, srv.errors = {}, 0, 0
    for e in (ConnectionAbortedError(), ConnectionResetError(), BrokenPipeError(), socket.timeout(), TimeoutError()):
        try:
            raise e
        except DISCONNECTS:
            srv.handle_error(None, None)
    assert srv.disconnects == 5 and srv.errors == 0
    assert logging.getLogger("chk.local_server").handlers                          # NullHandler — 앱 밖에서도 stderr 로 안 샘
