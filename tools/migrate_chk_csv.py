# tools/migrate_chk_csv.py
# -*- coding: utf-8 -*-
"""ChK_log.csv 를 현재 CHK_CSV_COLUMNS 스키마로 옮긴다 (1회용, 수동 실행).

[왜 필요한가]
  lib/logger.py 의 append_chk_csv_row 는 파일이 없을 때만 헤더를 쓴다.
  컬럼을 늘려도 NAS 에 이미 있는 ChK_log.csv 에는 옛 헤더가 그대로 박혀 있어서,
  logger 는 '어긋난 행을 만들지 않으려고' 파일의 옛 헤더를 따라 기록한다
  (= 새 컬럼은 기록되지 않는다). 이 스크립트로 파일 자체를 옮겨야 새 컬럼이 남는다.

[안전 장치]
  - 기본이 dry-run 이다. 실제로 바꾸려면 --apply 를 붙여야 한다.
  - 바꾸기 전에 같은 폴더에 백업을 만든다. 백업에 실패하면 중단한다.
  - 기존 파일에만 있고 새 목록에 없는 컬럼이 하나라도 있으면 중단한다.
    (데이터를 조용히 버리지 않는다 — 사람이 판단할 일이다)
  - 임시 파일에 다 쓴 뒤 os.replace 로 원자적 교체한다.
  - 행 수가 달라지면 실패로 보고 교체하지 않는다.
  - 이미 최신 스키마면 아무것도 하지 않는다(몇 번을 돌려도 안전하다).

[사용법]
    python tools\\migrate_chk_csv.py                 # dry-run (기본 대상 = CHK_CSV_PATH)
    python tools\\migrate_chk_csv.py --apply         # 실제 변환
    python tools\\migrate_chk_csv.py --path "D:\\x.csv" --apply
  Windows UNC 경로(\\\\VanaM_NAS\\...)도 그대로 쓸 수 있다.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import os
import shutil
import sys
from pathlib import Path

# 프로젝트 루트를 import 경로에 넣는다(tools/ 에서 직접 실행할 수 있게)
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lib.config import CHK_CSV_PATH, CHK_CSV_COLUMNS   # noqa: E402


def _read_header(path: Path):
    with path.open("r", newline="", encoding="utf-8") as f:
        head = next(csv.reader(f), None)
    if head:
        head[0] = head[0].lstrip("\ufeff")
    return head


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="ChK_log.csv 를 현재 CHK_CSV_COLUMNS 스키마로 옮긴다.")
    ap.add_argument("--path", default=str(CHK_CSV_PATH),
                    help="대상 CSV 경로 (기본: lib.config.CHK_CSV_PATH)")
    ap.add_argument("--apply", action="store_true",
                    help="실제로 변환한다. 없으면 dry-run(기본).")
    args = ap.parse_args(argv)

    path = Path(args.path)
    dry = not args.apply

    print("대상 파일 : %s" % path)
    print("모드      : %s" % ("실제 변환(--apply)" if args.apply else "dry-run (아무것도 바꾸지 않음)"))
    print()

    if not path.exists():
        print("[중단] 파일이 없습니다. 새로 만들어질 파일은 이미 최신 스키마로 기록됩니다.")
        return 1

    try:
        old_header = _read_header(path)
    except Exception as e:
        print("[중단] 헤더를 읽지 못했습니다: %r" % (e,))
        return 1

    if not old_header:
        print("[중단] 헤더가 비어 있습니다. 파일을 확인하세요.")
        return 1

    new_header = list(CHK_CSV_COLUMNS)

    print("기존 헤더 (%d열): %s" % (len(old_header), old_header))
    print("현재 스키마(%d열): %s" % (len(new_header), new_header))
    print()

    if old_header == new_header:
        print("이미 최신 스키마입니다 — 아무것도 하지 않습니다.")
        return 0

    # 기존에만 있고 새 목록에 없는 컬럼 → 중단
    unknown = [c for c in old_header if c not in new_header]
    if unknown:
        print("[중단] 기존 파일에만 있는 컬럼이 있습니다: %s" % unknown)
        print("       이 컬럼을 버릴지/유지할지는 사람이 판단해야 합니다.")
        print("       (CHK_CSV_COLUMNS 에 추가하거나, 이 컬럼을 정리한 뒤 다시 실행하세요)")
        return 2

    added = [c for c in new_header if c not in old_header]
    print("추가될 컬럼: %s" % (added or "(없음)"))

    # 원본 읽기
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print("원본 데이터 행: %d행" % len(rows))

    if dry:
        print()
        print("dry-run 이라 파일을 바꾸지 않았습니다.")
        print("실제로 변환하려면 --apply 를 붙여 다시 실행하세요.")
        if rows:
            sample = {k: rows[0].get(k, "") for k in new_header}
            print("변환 예시(첫 행): %r" % (sample,))
        return 0

    # 백업 (실패하면 중단)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.parent / ("ChK_log_backup_%s.csv" % stamp)
    try:
        shutil.copy2(str(path), str(backup))
    except Exception as e:
        print("[중단] 백업 생성 실패: %r" % (e,))
        return 1
    print("백업 생성 : %s" % backup)

    # 임시 파일에 새 스키마로 기록 → 원자적 교체
    tmp = path.parent / ("%s.migrate_tmp_%s" % (path.name, stamp))
    written = 0
    try:
        with tmp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=new_header, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in new_header})
                written += 1

        if written != len(rows):
            print("[중단] 행 수가 다릅니다: 원본 %d행 → 변환 %d행" % (len(rows), written))
            tmp.unlink(missing_ok=True)
            return 1

        os.replace(str(tmp), str(path))
    except Exception as e:
        print("[중단] 변환 실패: %r" % (e,))
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return 1

    # 교체 후 검증
    try:
        chk_header = _read_header(path)
        with path.open("r", newline="", encoding="utf-8") as f:
            chk_rows = sum(1 for _ in csv.DictReader(f))
    except Exception as e:
        print("[경고] 변환은 끝났으나 검증 읽기에 실패했습니다: %r" % (e,))
        chk_header, chk_rows = None, -1

    print()
    print("변환 완료 : 원본 %d행 → 변환 %d행" % (len(rows), written))
    if chk_header is not None:
        print("변환 후 헤더(%d열) 일치: %s" % (len(chk_header), chk_header == new_header))
        print("변환 후 데이터 행: %d행" % chk_rows)
        if chk_rows != len(rows):
            print("[경고] 교체 후 행 수가 원본과 다릅니다. 백업을 확인하세요: %s" % backup)
            return 1
    print("문제가 있으면 백업으로 되돌리세요: %s" % backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
