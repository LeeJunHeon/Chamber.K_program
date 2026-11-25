"""웹 레시피 사본(W-22 F) — 노트북이 레시피를 고를 수 있게 장비가 웹 레시피를 읽기 전용으로 들고 있는다.

- 받는 때: 웹 연결(uplink SSE)이 붙을 때와 웹이 레시피를 저장·지울 때 보내는 알림(SSE event: recipes) — uplink.fetch_recipes 가
  GET {UPLINK_URL}/recipes?equipment= (장비 토큰만 — 사용자 로그인과 별개)로 받아 replace 한다.
- 두는 곳: LOCAL_DATA_DIR/recipe_copy.json — 이름·id·종류(process·heater)·행·가스·웹 수정 시각(updatedAt)·웹 검사 결과(check)와
  웹과 맞춘 시각(syncedAt). 웹이 꺼져 있어도 마지막 사본으로 고를 수 있다.
- 적재: RECIPE_LOAD {id, updatedAt} → 사본의 행을 장비가 다시 검사해 적재(웹 검사 결과는 보여 주기만). updatedAt 이 사본과 다르면
  RECIPE_COPY_STALE(화면이 본 것과 사본이 다름), 사본에 없으면 RECIPE_COPY_MISSING. 히터 레시피는 HEATER_RECIPE_RUN {id, updatedAt}.
- 편집은 웹에서만 — 사본을 바꾸는 명령은 없다. 공정 기록(run_start)에 레시피 이름·웹 수정 시각이 남는다.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

COPY_FILE = "recipe_copy.json"
KINDS = ("process", "heater")
MAX_RECIPES = 2000


def _clean(r: Any) -> Optional[dict]:
    """웹 한 줄 → 사본 한 줄. 모양이 다르면 None(버린다)."""
    if not isinstance(r, dict):
        return None
    rid, kind, name, upd = r.get("id"), r.get("kind"), r.get("name"), r.get("updatedAt")
    if not (isinstance(rid, str) and rid and kind in KINDS and isinstance(name, str) and name and isinstance(upd, str) and upd):
        return None
    rows = r.get("rows")
    if not isinstance(rows, list):
        return None
    chk = r.get("check") if isinstance(r.get("check"), dict) else None
    return {"id": rid, "kind": kind, "name": name[:120], "rows": rows,
            "gas": r.get("gas") if isinstance(r.get("gas"), dict) else None, "updatedAt": upd,
            "updatedBy": str(r.get("updatedBy") or "")[:120] or None,
            "check": ({"errors": int(chk.get("errors") or 0), "warnings": int(chk.get("warnings") or 0),
                       "first": (str(chk["first"])[:200] if chk.get("first") else None)} if chk else None)}


class RecipeCopy:
    def __init__(self, data_dir: Optional[Path]) -> None:
        self.path = Path(data_dir) / COPY_FILE if data_dir is not None else None
        self._lock = threading.Lock()
        self.recipes: List[dict] = []
        self.synced_at: Optional[str] = None          # 웹과 맞춘 시각(UTC ISO)
        self.base: Optional[str] = None
        self.error: Optional[str] = None              # 마지막 받기 실패(웹 꺼짐 등) — 사본은 그대로
        self._load()

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        try:
            d = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self.error = "사본 파일을 읽지 못함"
            return
        self.recipes = [x for x in (_clean(r) for r in (d.get("recipes") or [])) if x][:MAX_RECIPES]
        self.synced_at, self.base = d.get("syncedAt"), d.get("base")

    def replace(self, recipes: Any, synced_at: str, base: Optional[str] = None) -> int:
        """웹 목록으로 통째로 바꾼다 — 모양이 다른 줄은 버린다. 파일은 임시 파일에 쓰고 바꿔 넣는다(끊겨도 예전 사본이 남게)."""
        got = [x for x in (_clean(r) for r in (recipes if isinstance(recipes, list) else [])) if x][:MAX_RECIPES]
        with self._lock:
            self.recipes, self.synced_at, self.base, self.error = got, synced_at, base, None
            if self.path is not None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self.path.with_suffix(".tmp")
                tmp.write_text(json.dumps({"syncedAt": synced_at, "base": base, "recipes": got}, ensure_ascii=False),
                               encoding="utf-8")
                os.replace(tmp, self.path)
        return len(got)

    def failed(self, why: str) -> None:
        self.error = why[:200]

    def get(self, rid: Any) -> Optional[dict]:
        with self._lock:
            return next((r for r in self.recipes if r["id"] == rid), None)

    def summary(self) -> dict:
        """노트북 목록(행 내용 없이) — 이름순, 종류별."""
        with self._lock:
            rs = sorted(self.recipes, key=lambda r: (r["kind"], r["name"]))
            return {"syncedAt": self.synced_at, "error": self.error, "count": len(rs),
                    "recipes": [{"id": r["id"], "kind": r["kind"], "name": r["name"], "rowCount": len(r["rows"]),
                                 "updatedAt": r["updatedAt"], "updatedBy": r["updatedBy"], "check": r["check"]} for r in rs]}

    def state(self) -> dict:
        """state.recipeCopy — 개수·맞춘 시각·실패만(목록은 로컬 /api/v1/recipes)."""
        return {"syncedAt": self.synced_at, "count": len(self.recipes), "error": self.error}
