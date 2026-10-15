"""v2 는 v1 모듈을 import 하지 않고, 외부 라이브러리 없이 표준 라이브러리만 쓴다(이번 단계)."""
import ast
import sys
from pathlib import Path

V2_DIR = Path(__file__).resolve().parents[1]
V1_TOP = {"lib", "core", "device", "controller", "integrations", "reporter", "main", "UI", "tools", "tests"}


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name.split(".")[0], node.lineno
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                yield node.module.split(".")[0], node.lineno


def test_no_v1_imports():
    bad = []
    for py in V2_DIR.rglob("*.py"):
        for top, line in _imports(py):
            if top in V1_TOP:
                bad.append(f"{py.relative_to(V2_DIR)}:{line} imports {top}")
    assert not bad, bad


def test_stdlib_only_except_pytest():
    allowed = set(sys.stdlib_module_names) | {"v2", "pytest", "__future__"}
    bad = []
    for py in V2_DIR.rglob("*.py"):
        for top, line in _imports(py):
            if top not in allowed:
                bad.append(f"{py.relative_to(V2_DIR)}:{line} imports {top}")
    assert not bad, bad
