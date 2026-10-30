"""v1 히터 2단 제어 학습 매핑 파일을 v2 자리로 한 번 가져온다(5-15) — v1 파일은 읽기만.

    python -m v2.tools.import_heater_map "<v1 폴더>\heater_tc2_map_learned.json" [--data-dir <LOCAL_DATA_DIR>] [--force]

v1 은 config_user.json 과 같은 폴더(장비 노트북 v1 폴더, git 에 없음)에 heater_tc2_map_learned.json 을 둔다.
v2 는 LOCAL_DATA_DIR(기록 폴더) 아래 같은 이름 파일을 읽고 쓴다(heater_cascade.LEARNED_FILE — 형식 같음 {"points": [{tc1, tc2, t}]}).
--data-dir 를 빼면 설정 폴더(--config-dir, 기본 C:\ChamberK_v2\config)의 설정에서 LOCAL_DATA_DIR 를 읽는다.
v2 파일이 이미 있으면 --force 가 아니면 멈춘다(v2 가 이미 배운 것을 덮지 않게). 실행 중인 프로그램은 다음 운전 시작 때 새 파일을 읽는다.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from v2.chk import heater_cascade as HC


def main(argv=None, print_fn=print) -> int:
    ap = argparse.ArgumentParser(prog="python -m v2.tools.import_heater_map", description="v1 학습 매핑 가져오기(읽기만)")
    ap.add_argument("src", help="v1 heater_tc2_map_learned.json")
    ap.add_argument("--data-dir", default=None, help="v2 LOCAL_DATA_DIR(빼면 설정에서)")
    ap.add_argument("--config-dir", default=r"C:\ChamberK_v2\config", help="--data-dir 를 뺐을 때 설정을 읽을 폴더")
    ap.add_argument("--force", action="store_true", help="v2 파일이 있어도 덮기")
    a = ap.parse_args(argv)
    if a.data_dir:
        data = Path(a.data_dir)
    else:
        from v2.chk.system import new_store
        data = Path(new_store(Path(a.config_dir)).load()["LOCAL_DATA_DIR"])
    dst = data / HC.LEARNED_FILE
    try:
        n = HC.import_learned(Path(a.src), dst, force=a.force)
    except FileExistsError:
        print_fn(f"멈춤 — v2 학습 파일이 이미 있음: {dst} (덮으려면 --force)")
        return 1
    except (OSError, ValueError, KeyError) as e:
        print_fn(f"가져오기 실패: {e!r}")
        return 2
    print_fn(f"가져옴 — {n}점 → {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
