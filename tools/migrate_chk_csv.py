# tools/migrate_chk_csv.py
# -*- coding: utf-8 -*-
"""ChK_log.csv 를 현재 CHK_CSV_COLUMNS 스키마로 옮긴다 (1회용, 수동 실행).

[왜 필요한가]
  lib/logger.py 의 append_chk_csv_row 는 파일이 없을 때만 헤더를 쓴다.
  컬럼을 늘리거나 옮겨도 NAS 에 이미 있는 ChK_log.csv 에는 옛 헤더가 그대로 박혀 있어서,
  logger 는 '어긋난 행을 만들지 않으려고' 파일의 옛 헤더를 따라 이름 기준으로 기록한다
  (= 옛 헤더에 없는 열은 기록되지 않고, 열 순서도 옛 순서 그대로다). 이 스크립트로 파일 자체를 옮겨야 한다.

[★ 과거 버그 — 같은 실수를 반복하지 말 것]
  예전 구현은 csv.DictReader 로 읽었다. DictReader 는 '파일의 헤더'를 fieldnames 로
  쓰기 때문에, 헤더가 15열인 파일에 섞여 있는 16필드 행(2026-09-03 에 Heater Temp 가
  추가된 뒤의 행)을 읽으면 값이 한 칸씩 밀리고 16번째 값(DC: P)이 restkey 로 새어
  통째로 버려졌다. 실측 피해:
      원본   Heater='' RF For.P=0.000 RF Ref.P=0.000 DC V=314.768 DC I=0.635 DC P=199.951
      변환후 Heater='' RF For.P=''    RF Ref.P=0.000 DC V=0.000   DC I=314.768 DC P=0.635
  게다가 자체 검증이 '행 개수'만 봤기 때문에 그냥 통과했다.
  그래서 지금은 (1) csv.reader 로 원시 리스트를 읽고, (2) 검증을 값 기준으로 한다.

[형식 판별 — 2026-10-15 부터 '헤더 이름 목록'이 먼저다]
  2026-10-15 에 Base Pressure 를 맨 끝(21번째)에서 Process Name 다음으로 옮겼다. 옛 21열(맨 끝 Base
  Pressure)과 새 21열은 필드 수가 같아서 필드 수만으로는 가를 수 없다. 그래서
    1) 파일 헤더가 알려진 형식(HEADER_SCHEMAS) 중 정확히 어느 것인지 이름 목록으로 정한다.
       어느 것과도 맞지 않으면 추측하지 않고 중단한다.
    2) 헤더와 필드 수가 같은 행은 헤더 이름대로 읽는다.
    3) 헤더가 옛 형식인데 필드 수가 다른 행(옛 코드가 헤더를 보지 않고 쓴 행)은 그 시절의 필드 수 표
       (LEGACY_BY_COUNT)로 읽는다 — 이 표의 21필드는 '맨 끝 Base Pressure' 형식이다(새 순서 행은 옛 헤더
       파일에 생길 수 없다: logger 는 파일 헤더를 따라 이름 기준으로 쓴다). 표에 없는 필드 수면 중단.
       헤더가 현재 형식인데 필드 수가 다른 행이 있으면 중단.
    4) 모든 값은 '열 이름' 기준으로 옮기고, 검증도 이름 기준으로 모든 기존 열의 값을 대조한다.
  옛 행의 Main Shutter 는 빈칸 그대로 둔다(그때 실제로 열었는지 확인할 수 없다).

[--fill-base "YYYY-mm-dd HH:MM:SS=값" — 1회성 보정]
  마이그레이션 전에 기록돼 Base Pressure 가 빈 행 하나를 공정 로그의 실제 값으로 채운다.
  Timestamp 가 정확히 일치하는 행이 1개이고 그 행의 Base Pressure 가 빈칸일 때만 채운다
  (값은 logger 와 같은 "%.2E" 로 맞춘다 — 4.6E-06 → 4.60E-06). 0개·2개 이상·이미 값 있음·값 형식 오류면
  아무것도 바꾸지 않고 중단한다. dry-run 에서도 어떤 행이 바뀔지 보여 준다.

[안전 장치]
  - 기본이 dry-run 이다. 실제로 바꾸려면 --apply 를 붙여야 한다.
  - 바꾸기 전에 같은 폴더에 백업을 만든다. 백업에 실패하면 중단한다.
  - 알 수 없는 헤더, 알 수 없는 필드 수의 행이 하나라도 있으면 중단한다(추측하지 않는다).
  - 임시 파일에 다 쓴 뒤, 다시 읽어 모든 행을 이름 기준으로 대조하고 나서 os.replace 로 원자적 교체한다.
  - 값 검증에 하나라도 실패하면 임시 파일을 지우고 원본을 건드리지 않는다.
  - 이미 최신 스키마면(그리고 --fill-base 가 없으면) 아무것도 하지 않는다(몇 번을 돌려도 안전하다).

[사용법]
    python tools\\migrate_chk_csv.py                 # dry-run (기본 대상 = CHK_CSV_PATH)
    python tools\\migrate_chk_csv.py --apply         # 실제 변환
    python tools\\migrate_chk_csv.py --fill-base "2026-10-15 05:17:39=4.6E-06"           # dry-run(보정 포함)
    python tools\\migrate_chk_csv.py --fill-base "2026-10-15 05:17:39=4.6E-06" --apply
    python tools\\migrate_chk_csv.py --path "D:\\x.csv" --apply
    python tools\\migrate_chk_csv.py --selftest      # 회귀 테스트(실제 파일 안 건드림)
  Windows UNC 경로(\\\\VanaM_NAS\\...)도 그대로 쓸 수 있다.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import os
import shutil
import sys
import tempfile
from pathlib import Path

# 한국어 윈도우 콘솔(cp949)은 아래 메시지의 '—'/'→'/'★' 를 못 찍어 UnicodeEncodeError 로
# 죽는다(실제로 --selftest 가 통과 직전에 죽었다). 출력만 UTF-8 로 바꾸고, 그것도 안 되면
# 못 찍는 글자만 '?' 로 대체해 스크립트 자체는 끝까지 돌게 한다.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# 프로젝트 루트를 import 경로에 넣는다(tools/ 에서 직접 실행할 수 있게)
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lib.config import CHK_CSV_PATH, CHK_CSV_COLUMNS   # noqa: E402

# ===== 스키마 이력 =====
COLS_NOW = list(CHK_CSV_COLUMNS)                       # 현재(21열 — Base Pressure 가 Process Name 다음)

# 16열: Heater Temp 가 들어간 시절 (2026-09-03~)
COLS_16 = [
    "Timestamp", "Process Name", "Main Shutter", "Shutter Delay",
    "G1 Target", "G2 Target", "Ar flow", "O2 flow",
    "Working Pressure", "Process Time", "Heater Temp",
    "RF: For.P", "RF: Ref. P", "DC: V", "DC: I", "DC: P",
]
# 15열: Heater Temp 가 없던 시절
COLS_15 = [c for c in COLS_16 if c != "Heater Temp"]
# 18열: RF Pulse Freq/Duty 만 붙었던 중간 스키마 (커밋 16ad9b6)
COLS_18 = COLS_16 + ["RF Pulse: Freq[kHz]", "RF Pulse: Duty[%]"]
# 20열: RF Pulse For.P/Ref.P 까지 붙은 스키마 (Base Pressure 이전)
COLS_20 = COLS_18 + ["RF Pulse: For.P", "RF Pulse: Ref. P"]
# 21열(옛): Base Pressure 를 맨 끝에 붙였던 스키마 (2026-10-15 09:51 NAS 마이그레이션 결과)
COLS_21_END = COLS_20 + ["Base Pressure"]

# 헤더 이름 목록 → 형식. 파일 헤더는 이 중 하나와 정확히(이름·순서) 같아야 한다.
HEADER_SCHEMAS = [
    ("15열(Heater Temp 이전)", COLS_15),
    ("16열(Heater Temp 추가)", COLS_16),
    ("18열(RF Pulse Freq/Duty 추가)", COLS_18),
    ("20열(RF Pulse For/Ref 추가, Base Pressure 이전)", COLS_20),
    ("21열(옛 — Base Pressure 맨 끝)", COLS_21_END),
    ("21열(현재 — Base Pressure 가 Process Name 다음)", COLS_NOW),
]
# 옛 헤더 파일에 섞인 '헤더와 필드 수가 다른 행' — 옛 코드는 그때의 스키마를 위치대로 썼다
LEGACY_BY_COUNT = {len(c): (label, c) for label, c in HEADER_SCHEMAS if c is not COLS_NOW}

# 현재 스키마는 옛 21열과 같은 열 집합이어야 한다(열을 더 늘리면 여기에 새 형식을 추가하고 이 단정을 고친다)
assert sorted(COLS_NOW) == sorted(COLS_21_END) and COLS_NOW != COLS_21_END, \
    "CHK_CSV_COLUMNS 가 바뀌었습니다 — 이 도구에 새 형식을 추가하세요"


def _schema_of_header(header):
    for label, cols in HEADER_SCHEMAS:
        if list(header) == cols:
            return label, cols
    return None


def _read_raw(path: Path):
    """헤더 + 데이터 행(원시 리스트). 빈 행은 버린다."""
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        return None, [], 0
    header = rows[0]
    if header:
        header[0] = header[0].lstrip("\ufeff")
    data = []
    blank = 0
    for r in rows[1:]:
        if not r or all((c or "").strip() == "" for c in r):
            blank += 1
            continue
        data.append(r)
    return header, data, blank


def _row_schemas(header, data):
    """행마다 (형식 이름, 열 이름 목록). 판별할 수 없으면 (None, 중단 사유 목록)."""
    hs = _schema_of_header(header)
    if hs is None:
        return None, ["헤더가 알려진 어느 형식과도 맞지 않습니다 (%d열): %r" % (len(header), header)]
    hlabel, hcols = hs
    out, bad = [], []
    for i, r in enumerate(data, start=2):
        if len(r) == len(hcols):
            out.append((hlabel, hcols))
        elif hcols is not COLS_NOW and len(r) in LEGACY_BY_COUNT:
            out.append(LEGACY_BY_COUNT[len(r)])
        else:
            bad.append("   행 %d: %d필드 → %r" % (i, len(r), r[:4]))
    if bad:
        return None, ["헤더(%s)와 맞지 않는 필드 수의 행이 %d개 있습니다" % (hlabel, len(bad))] + bad[:12]
    return out, []


def _convert(path: Path, verbose=True):
    """(헤더, 원시행들, 행별 형식, 변환된 dict 목록, 메시지) — 판별에 실패하면 변환 목록이 None(메시지 = 중단 사유)."""
    header, data, blank = _read_raw(path)
    if header is None:
        return None, [], None, None, ["헤더가 비어 있습니다."]

    hs = _schema_of_header(header)
    if verbose:
        print("기존 헤더 (%d열): %s" % (len(header), header))
        print("헤더 형식      : %s" % (hs[0] if hs else "알 수 없음"))
        print("현재 스키마(%d열): %s" % (len(COLS_NOW), COLS_NOW))
        print("데이터 행 %d개 (빈 행 %d개는 건너뜀)" % (len(data), blank))

    schemas, msgs = _row_schemas(header, data)
    if schemas is None:
        return header, data, None, None, msgs

    counts = {}
    for label, _cols in schemas:
        counts[label] = counts.get(label, 0) + 1
    if verbose:
        for label, n in counts.items():
            print("   %s 행: %d개" % (label, n))

    warns = []
    n18 = sum(1 for _l, c in schemas if c is COLS_18)
    if n18:
        warns.append(
            "18필드 행이 %d개 있습니다 — 옛 마이그레이션을 거친 파일일 수 있습니다. "
            "2026-09-03 이후 행의 DC 값(특히 DC: P)을 눈으로 확인하세요." % n18)

    out = []
    for r, (_label, cols) in zip(data, schemas):
        d = dict(zip(cols, r))
        out.append({k: d.get(k, "") for k in COLS_NOW})
    return header, data, schemas, out, warns


def parse_fill_base(spec: str):
    """'YYYY-mm-dd HH:MM:SS=값' → (Timestamp, "%.2E" 값). 형식이 틀리면 ValueError."""
    if not spec or "=" not in spec:
        raise ValueError("--fill-base 형식은 \"YYYY-mm-dd HH:MM:SS=값\" 입니다: %r" % (spec,))
    ts, val = spec.rsplit("=", 1)
    ts, val = ts.strip(), val.strip()
    try:
        datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise ValueError("--fill-base Timestamp 형식이 다릅니다(YYYY-mm-dd HH:MM:SS): %r" % ts)
    try:
        v = float(val)
    except ValueError:
        raise ValueError("--fill-base Base Pressure 값이 숫자가 아닙니다: %r" % val)
    if not (0.0 < v < 1.0):
        raise ValueError("--fill-base Base Pressure 값이 범위 밖입니다(0 < 값 < 1 Torr): %r" % val)
    return ts, "%.2E" % v


def _apply_fill_base(out, fill):
    """fill=(ts, 값) → (채운 행 index, None) 또는 (None, 중단 사유). 중단이면 out 을 바꾸지 않는다."""
    ts, val = fill
    hits = [i for i, o in enumerate(out) if o.get("Timestamp", "") == ts]
    if not hits:
        return None, "--fill-base: Timestamp %r 와 정확히 일치하는 행이 없습니다" % ts
    if len(hits) > 1:
        return None, "--fill-base: Timestamp %r 행이 %d개입니다(행 %s) — 어느 행인지 정할 수 없습니다" % (
            ts, len(hits), [i + 2 for i in hits])
    i = hits[0]
    cur = out[i].get("Base Pressure", "")
    if (cur or "").strip() != "":
        return None, "--fill-base: 행 %d(%s)의 Base Pressure 에 이미 값 %r 가 있습니다 — 덮어쓰지 않습니다" % (
            i + 2, ts, cur)
    out[i]["Base Pressure"] = val
    return i, None


def _verify(data, schemas, out, filled=None):
    """이름 기준 값 검증. 문제 목록을 돌려준다(빈 목록 = 통과). filled = (행 index, 값) 이면 그 칸만 예외."""
    problems = []
    if len(data) != len(out) or len(data) != len(schemas):
        problems.append("행 수 불일치: 원본 %d → 변환 %d" % (len(data), len(out)))
        return problems

    for i, (raw, (_label, cols), o) in enumerate(zip(data, schemas, out)):
        n = i + 2
        is_fill = filled is not None and filled[0] == i
        if list(o.keys()) != COLS_NOW:
            problems.append("행 %d: 변환 결과 열이 현재 스키마와 다름" % n)
            continue
        src = dict(zip(cols, raw))
        # 1) 원본에 있던 모든 열 값이 이름 그대로 옮겨졌는가(DC: P·Base Pressure 포함 — 예전 버그가 DC: P 에서 터졌다)
        for k, v in src.items():
            if k == "Base Pressure" and is_fill:
                continue
            if o.get(k, "") != v:
                problems.append("행 %d: %s 값이 %r → %r 로 바뀜" % (n, k, v, o.get(k, "")))
                break
        # 2) 원본에 없던 열은 빈칸이어야 한다(다른 값이 새어 들어오지 않았는가). 옛 행의 Main Shutter 도 빈칸
        for k in COLS_NOW:
            if k in src or (k == "Base Pressure" and is_fill):
                continue
            if o.get(k, "") != "":
                problems.append("행 %d: 새 열 %s 에 값 %r 가 들어감(빈칸이어야 함)" % (n, k, o.get(k, "")))
                break
        # 3) --fill-base 대상 행: 원본은 빈칸, 결과는 지정 값
        if is_fill and ((src.get("Base Pressure", "") or "").strip() != "" or o.get("Base Pressure") != filled[1]):
            problems.append("행 %d: --fill-base 결과가 다름 — 원본 %r → %r (기대 %r)"
                            % (n, src.get("Base Pressure", ""), o.get("Base Pressure"), filled[1]))
    return problems


def _print_samples(data, schemas, out, filled_idx=None):
    """첫 행 / 형식별 첫 행 / 마지막 행 / --fill-base 행을 원본 대비 전 열 출력."""
    picks = []
    if data:
        picks.append((0, "첫 행"))
        seen_fmt = set()
        for i, (label, _c) in enumerate(schemas):
            if label not in seen_fmt:
                seen_fmt.add(label)
                picks.append((i, "%s 첫 행" % label))
        picks.append((len(data) - 1, "마지막 행"))
    if filled_idx is not None:
        picks.append((filled_idx, "--fill-base 대상"))

    seen = set()
    for idx, label in picks:
        if idx in seen:
            continue
        seen.add(idx)
        raw, (_l, cols), o = data[idx], schemas[idx], out[idx]
        src = dict(zip(cols, raw))
        print()
        print("  --- 행 %d (%s, %d필드) ---" % (idx + 2, label, len(raw)))
        print("    %-22s %-22s %s" % ("열", "변환 전", "변환 후"))
        for k in COLS_NOW:
            b = src.get(k, "(없음)")
            a = o.get(k, "")
            mark = "" if (b == a or b == "(없음)") else "   ★다름"
            print("    %-22s %-22r %r%s" % (k, b, a, mark))


def cmd_migrate(path: Path, dry: bool, fill_base=None) -> int:
    print("대상 파일 : %s" % path)
    print("모드      : %s" % ("실제 변환(--apply)" if not dry else "dry-run (아무것도 바꾸지 않음)"))
    fill = None
    if fill_base:
        try:
            fill = parse_fill_base(fill_base)
        except ValueError as e:
            print("[중단] %s" % e)
            return 2
        print("보정      : Timestamp %r 행의 Base Pressure ← %r" % fill)
    print()

    if not path.exists():
        print("[중단] 파일이 없습니다. 새로 만들어질 파일은 이미 최신 스키마로 기록됩니다.")
        return 1

    try:
        header, data, schemas, out, msgs = _convert(path)
    except Exception as e:
        print("[중단] 읽기 실패: %r" % (e,))
        return 1

    if out is None:
        print()
        for m in msgs:
            print("[중단] %s" % m)
        print("       어떤 열인지 사람이 판단해야 합니다. 추측해서 옮기지 않습니다. 원본을 건드리지 않았습니다.")
        return 2

    for w in msgs:
        print()
        print("[경고] %s" % w)

    filled = None
    if fill is not None:
        idx, err = _apply_fill_base(out, fill)
        if err:
            print()
            print("[중단] %s" % err)
            print("       원본을 건드리지 않았습니다.")
            return 2
        filled = (idx, fill[1])
        o = out[idx]
        print()
        print("Base Pressure 채움: 행 %d (Timestamp %r, Process Name %r) '' → %r"
              % (idx + 2, o["Timestamp"], o["Process Name"], fill[1]))

    latest = header == COLS_NOW and all(c is COLS_NOW for _l, c in schemas)
    if latest and filled is None:
        print()
        print("이미 최신 스키마입니다 — 아무것도 하지 않습니다.")
        return 0

    if not latest:
        added = [c for c in COLS_NOW if c not in header]
        print()
        print("추가될 열 : %s" % (added or "(없음)"))
        print("열 순서   : 현재 순서로 바뀜 (Base Pressure 가 Process Name 다음)")

    problems = _verify(data, schemas, out, filled)
    _print_samples(data, schemas, out, None if filled is None else filled[0])

    if problems:
        print()
        for p in problems[:20]:
            print("[중단] 검증 실패: %s" % p)
        print("       원본을 건드리지 않았습니다.")
        return 1
    print()
    print("값 검증 통과: %d행 전부 기존 열 값이 이름 기준으로 보존됩니다." % len(out))

    if dry:
        print()
        print("dry-run 이라 파일을 바꾸지 않았습니다.")
        print("실제로 변환하려면 --apply 를 붙여 다시 실행하세요.")
        return 0

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.parent / ("ChK_log_backup_%s.csv" % stamp)
    try:
        shutil.copy2(str(path), str(backup))
    except Exception as e:
        print("[중단] 백업 생성 실패: %r" % (e,))
        return 1
    print("백업 생성 : %s" % backup)

    tmp = path.parent / ("%s.migrate_tmp_%s" % (path.name, stamp))
    try:
        with tmp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLS_NOW, extrasaction="raise")
            w.writeheader()
            for o in out:
                w.writerow(o)
        # 쓴 결과를 다시 읽어 모든 행을 이름 기준으로 대조한 뒤에야 교체한다
        with tmp.open("r", newline="", encoding="utf-8") as f:
            back = list(csv.reader(f))
        if back[0] != COLS_NOW:
            raise ValueError("임시 파일 헤더가 다릅니다")
        body = [r for r in back[1:] if r]
        if len(body) != len(out):
            raise ValueError("임시 파일 행 수가 다릅니다: %d != %d" % (len(body), len(out)))
        for i, (r, o) in enumerate(zip(body, out), start=2):
            if len(r) != len(COLS_NOW) or dict(zip(COLS_NOW, r)) != o:
                raise ValueError("임시 파일 행 %d 이 변환 결과와 다릅니다" % i)
        os.replace(str(tmp), str(path))
    except Exception as e:
        print("[중단] 변환 실패: %r" % (e,))
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        print("       원본을 건드리지 않았습니다. 백업: %s" % backup)
        return 1

    print()
    print("변환 완료 : 원본 %d행 → 변환 %d행" % (len(data), len(out)))
    print("이 파일은 이제 현재 스키마 %d열입니다." % len(COLS_NOW))
    print("백업 경로 : %s" % backup)
    print("문제가 있으면 위 백업으로 되돌리세요.")
    return 0


# ===================== 회귀 테스트 =====================
# 실제 NAS 파일에서 확인된 값을 픽스처로 쓴다.
FIX_16A = ["2026-09-03 10:12:40", "AlN_DC", "", "5.000", "Al", "", "20.000", "0.000",
           "2.000", "10.000", "", "0.000", "0.000", "314.768", "0.635", "199.951"]
FIX_16B = ["2026-09-10 22:35:37", "Au_DC", "", "1.000", "", "Au", "20.000", "0.000",
           "5.000", "0.500", "", "0.000", "0.000", "410.085", "0.121", "49.753"]
FIX_15A = ["2026-08-20 09:00:00", "SiO2_RF", "", "2.000", "SiO2", "", "18.000", "2.000",
           "3.000", "20.000", "198.500", "1.200", "0.000", "0.000", "0.000"]
FIX_15B = ["2026-08-21 11:30:00", "Au_DC_old", "", "1.000", "", "Au", "20.000", "0.000",
           "5.000", "0.500", "0.000", "0.000", "402.100", "0.130", "52.273"]
# 20필드(Base Pressure 이전) — RF Pulse 4컬럼에 값이 있는 행
FIX_20A = ["2026-10-01 14:00:00", "SiO2_RFpulse", "", "2.000", "SiO2", "", "18.000", "2.000",
           "3.000", "20.000", "300.0", "0.000", "0.000", "0.000", "0.000", "0.000",
           "10.000", "50", "199.800", "1.250"]
# 21필드 옛 형식(Base Pressure 맨 끝) — 10/15 마이그레이션 결과 / 옛 헤더 파일에 섞인 행
FIX_21E = FIX_20A[:1] + ["Au_base"] + FIX_20A[2:] + ["3.90E-06"]
# 10/15 마지막 행처럼 Base Pressure 가 빈 옛 21필드 행
FIX_21E_BLANK = ["2026-10-15 05:17:39", "CSV 1/1 - CeO2 #5-2", "", "1.000", "CeO2", "", "20.100", "0.000",
                 "5.000", "30.000", "600.2", "0.000", "0.000", "350.000", "0.285", "100.000",
                 "", "", "", "", ""]


def _new_row(cols, raw, **over):
    """옛 형식 행 → 현재 순서 리스트(이름 기준, 없는 열은 빈칸)."""
    d = dict(zip(cols, raw))
    d.update(over)
    return [d.get(k, "") for k in COLS_NOW]


def cmd_selftest() -> int:
    fails = []

    def ck(label, cond, extra=""):
        print("  %-6s %s %s" % ("[OK]" if cond else "[FAIL]", label, extra))
        if not cond:
            fails.append(label)

    def write(p, header, rows, blank_tail=False):
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow(r)
            if blank_tail:
                f.write("\r\n")          # 파일 끝 빈 줄(실제 파일에 있다)

    def read(p):
        with p.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        return rows[0], [r for r in rows[1:] if r]

    def raw_bytes(p):
        return p.read_bytes()

    td = Path(tempfile.mkdtemp(prefix="chk_mig_selftest_"))

    # ── (c) 옛 헤더 15열 + 15/16/20/옛21 필드 행이 섞인 파일 ──
    p = td / "ChK_log.csv"
    write(p, COLS_15, [FIX_15A, FIX_15B, FIX_16A, FIX_16B, FIX_20A, FIX_21E], blank_tail=True)
    print("=== (c) 헤더 15열 / 15필드 2행 / 16필드 2행 / 20필드 1행 / 옛21필드 1행 / 끝 빈 줄 ===")
    rc = cmd_migrate(p, dry=False)
    print()
    ck("(c) --apply rc=0", rc == 0, "-> rc=%d" % rc)
    hdr, body = read(p)
    ck("(c) 헤더가 현재 스키마(%d열)" % len(COLS_NOW), hdr == COLS_NOW, "-> %r" % hdr[:4])
    ck("(c) 열 순서: Process Name 다음이 Base Pressure, 그다음 Main Shutter",
       hdr[1:4] == ["Process Name", "Base Pressure", "Main Shutter"])
    ck("(c) 데이터 6행, 모두 %d필드" % len(COLS_NOW),
       len(body) == 6 and all(len(r) == len(COLS_NOW) for r in body), "-> %r" % sorted({len(r) for r in body}))
    want = [_new_row(COLS_15, FIX_15A), _new_row(COLS_15, FIX_15B), _new_row(COLS_16, FIX_16A),
            _new_row(COLS_16, FIX_16B), _new_row(COLS_20, FIX_20A), _new_row(COLS_21_END, FIX_21E)]
    ck("(c) 모든 행이 이름 기준으로 그대로", body == want)
    recs = [dict(zip(hdr, r)) for r in body]
    ck("(c) 15필드 행: DC: P == 원본 마지막 값", recs[0]["DC: P"] == FIX_15A[-1] and recs[1]["DC: P"] == FIX_15B[-1])
    for key, wv in (("Heater Temp", ""), ("RF: For.P", "0.000"), ("RF: Ref. P", "0.000"),
                    ("DC: V", "314.768"), ("DC: I", "0.635"), ("DC: P", "199.951")):
        ck("(c) 16필드 행 %-12s == %r" % (key, wv), recs[2][key] == wv, "-> %r" % recs[2][key])
    for key, wv in (("RF Pulse: Freq[kHz]", "10.000"), ("RF Pulse: Duty[%]", "50"),
                    ("RF Pulse: For.P", "199.800"), ("RF Pulse: Ref. P", "1.250"),
                    ("Heater Temp", "300.0"), ("Base Pressure", "")):
        ck("(c) 20필드 행 %-20s == %r" % (key, wv), recs[4][key] == wv, "-> %r" % recs[4][key])
    ck("(c) 옛21필드 행 Base Pressure 3.90E-06 이 새 자리로", recs[5]["Base Pressure"] == "3.90E-06"
       and body[5][2] == "3.90E-06" and recs[5]["RF Pulse: Ref. P"] == "1.250")
    ck("(c) 옛 행의 Main Shutter 는 빈칸 그대로", all(r["Main Shutter"] == "" for r in recs))
    before = raw_bytes(p)
    ck("(c) 재실행 rc=0(이미 최신)", cmd_migrate(p, dry=False) == 0)
    ck("(c) 재실행은 파일을 바꾸지 않음", raw_bytes(p) == before)
    ck("(c) 백업 생성", len(list(td.glob("ChK_log_backup_*.csv"))) >= 1)

    # ── (a) 20열 파일 ──
    p20 = td / "a" / "ChK_log.csv"
    p20.parent.mkdir()
    write(p20, COLS_20, [FIX_20A, FIX_20A])
    print()
    print("=== (a) 20열 파일 ===")
    before = raw_bytes(p20)
    ck("(a) dry-run rc=0", cmd_migrate(p20, dry=True) == 0)
    ck("(a) dry-run 은 파일을 안 바꿈", raw_bytes(p20) == before)
    ck("(a) --apply rc=0", cmd_migrate(p20, dry=False) == 0)
    hdr, body = read(p20)
    ck("(a) 현재 헤더 + 이름 기준 행(Base Pressure·Main Shutter 빈칸)",
       hdr == COLS_NOW and body == [_new_row(COLS_20, FIX_20A)] * 2, "-> %r" % (body[:1],))
    ck("(a) 백업 생성", len(list(p20.parent.glob("ChK_log_backup_*.csv"))) == 1)

    # ── (b) 21열이지만 Base Pressure 맨 끝(10/15 결과) + --fill-base ──
    pb = td / "b" / "ChK_log.csv"
    pb.parent.mkdir()
    write(pb, COLS_21_END, [FIX_21E, FIX_21E_BLANK])
    print()
    print("=== (b) 옛 21열(Base Pressure 맨 끝) + --fill-base ===")
    FILL = "2026-10-15 05:17:39=4.6E-06"
    before = raw_bytes(pb)
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = cmd_migrate(pb, dry=True, fill_base=FILL)
    dry_out = buf.getvalue()
    print(dry_out)
    ck("(b) dry-run(+fill) rc=0", rc == 0, "-> rc=%d" % rc)
    ck("(b) dry-run 이 바뀔 행을 보여 줌",
       "Base Pressure 채움: 행 3 (Timestamp '2026-10-15 05:17:39'" in dry_out and "'4.60E-06'" in dry_out)
    ck("(b) dry-run 은 파일을 안 바꿈", raw_bytes(pb) == before)
    ck("(b) --apply(+fill) rc=0", cmd_migrate(pb, dry=False, fill_base=FILL) == 0)
    hdr, body = read(pb)
    ck("(b) 현재 헤더", hdr == COLS_NOW)
    ck("(b) 행 1 이름 기준 그대로(Base Pressure 3.90E-06 새 자리)", body[0] == _new_row(COLS_21_END, FIX_21E))
    ck("(b) 행 2 Base Pressure 만 4.60E-06 으로 채움",
       body[1] == _new_row(COLS_21_END, FIX_21E_BLANK, **{"Base Pressure": "4.60E-06"}), "-> %r" % body[1][:4])
    before = raw_bytes(pb)
    ck("(b) 재실행(fill 없음) rc=0 이미 최신", cmd_migrate(pb, dry=False) == 0)
    ck("(b) 재실행은 파일을 바꾸지 않음", raw_bytes(pb) == before)
    ck("(b) 같은 --fill-base 재실행 → 이미 값 있음 rc=2", cmd_migrate(pb, dry=False, fill_base=FILL) == 2)
    ck("(b)   파일 그대로", raw_bytes(pb) == before)

    # ── --fill-base 실패들(모두 rc=2, 원본 그대로) ──
    print()
    print("=== --fill-base 실패 ===")
    pf = td / "f" / "ChK_log.csv"
    pf.parent.mkdir()
    write(pf, COLS_21_END, [FIX_21E, FIX_21E_BLANK, FIX_21E_BLANK[:1] + ["dup"] + FIX_21E_BLANK[2:]])
    before = raw_bytes(pf)
    ck("fill 중복 Timestamp(2개) → rc=2", cmd_migrate(pf, dry=False, fill_base=FILL) == 2)
    ck("fill 일치 0개 → rc=2", cmd_migrate(pf, dry=False, fill_base="2026-10-15 05:17:40=4.6E-06") == 2)
    ck("fill 이미 값 있음 → rc=2", cmd_migrate(pf, dry=False, fill_base="2026-10-01 14:00:00=4.6E-06") == 2)
    ck("fill 형식 오류(= 없음) → rc=2", cmd_migrate(pf, dry=False, fill_base="2026-10-15 05:17:39") == 2)
    ck("fill 값 오류 → rc=2", cmd_migrate(pf, dry=False, fill_base="2026-10-15 05:17:39=abc") == 2)
    ck("fill 실패는 dry-run 에서도 rc=2", cmd_migrate(pf, dry=True, fill_base=FILL) == 2)
    ck("fill 실패: 파일 그대로·백업 없음",
       raw_bytes(pf) == before and not list(pf.parent.glob("ChK_log_backup_*.csv")))

    # 이미 최신 파일에도 --fill-base 만 적용할 수 있다
    pl = td / "l" / "ChK_log.csv"
    pl.parent.mkdir()
    blank_now = _new_row(COLS_21_END, FIX_21E_BLANK)
    write(pl, COLS_NOW, [blank_now])
    ck("최신 파일 + fill → rc=0", cmd_migrate(pl, dry=False, fill_base=FILL) == 0)
    hdr, body = read(pl)
    ck("  Base Pressure 만 채움", body == [_new_row(COLS_21_END, FIX_21E_BLANK, **{"Base Pressure": "4.60E-06"})])

    # ── 헤더가 알려진 어느 형식과도 맞지 않음 → 중단 ──
    print()
    print("=== 미지 형식 ===")
    for k, (name, header, rows) in enumerate((
            ("열 이름 하나 다름", ["Timestamp", "Process"] + COLS_20[2:], [FIX_20A]),
            ("같은 열·다른 순서", [COLS_NOW[0], COLS_NOW[1], COLS_NOW[3], COLS_NOW[2]] + COLS_NOW[4:],
             [blank_now]),
            ("21열 + 알 수 없는 열", COLS_NOW + ["X"], [blank_now + ["x"]]))):
        pu = td / ("u_%d.csv" % k)
        write(pu, header, rows)
        before = raw_bytes(pu)
        ck("미지 헤더(%s) → rc=2" % name, cmd_migrate(pu, dry=False) == 2)
        ck("  원본 그대로", raw_bytes(pu) == before)
    pn = td / "n.csv"
    write(pn, COLS_NOW, [blank_now, FIX_20A])          # 현재 헤더에 20필드 행
    ck("현재 헤더에 필드 수 다른 행 → rc=2", cmd_migrate(pn, dry=False) == 2)
    p2 = td / "w.csv"
    write(p2, COLS_15, [FIX_15A, FIX_15A[:9]])           # 9필드 = 알 수 없음
    ck("알 수 없는 필드 수 → rc=2", cmd_migrate(p2, dry=False) == 2)
    with p2.open("r", newline="", encoding="utf-8") as f:
        ck("  원본을 건드리지 않음", next(csv.reader(f)) == COLS_15)

    # ── 값 검증이 실제로 잡는가 ──
    print()
    print("=== 값 검증 ===")
    pv = td / "v.csv"
    write(pv, COLS_21_END, [FIX_21E, FIX_21E_BLANK])
    _h, data, schemas, out, _m = _convert(pv, verbose=False)
    ck("검증: 그대로면 통과", _verify(data, schemas, out) == [])
    bad = [dict(o) for o in out]
    bad[0]["DC: P"] = "0.635"
    ck("검증: DC: P 변형을 잡음", any("DC: P" in x for x in _verify(data, schemas, bad)))
    bad = [dict(o) for o in out]
    bad[0]["Base Pressure"], bad[0]["RF Pulse: Ref. P"] = bad[0]["RF Pulse: Ref. P"], bad[0]["Base Pressure"]
    ck("검증: 위치로 옮긴 값(Base Pressure ↔ 끝 열)을 잡음", _verify(data, schemas, bad) != [])
    bad = [dict(o) for o in out]
    bad[1]["Base Pressure"] = "4.60E-06"
    ck("검증: fill 표시 없이 채운 값을 잡음", _verify(data, schemas, bad) != [])
    ck("검증: fill 표시가 있으면 통과", _verify(data, schemas, bad, filled=(1, "4.60E-06")) == [])
    pv15 = td / "v15.csv"
    write(pv15, COLS_15, [FIX_15A])
    _h, data, schemas, out, _m = _convert(pv15, verbose=False)
    out[0]["Main Shutter"] = "T"
    out[0]["Heater Temp"] = "1.0"
    ck("검증: 원본에 없던 열에 새어 든 값을 잡음", any("Heater Temp" in x for x in _verify(data, schemas, out)))

    shutil.rmtree(str(td), ignore_errors=True)
    print()
    print("회귀 테스트 실패 %d건 %s" % (len(fails), fails))
    return 1 if fails else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="ChK_log.csv 를 현재 CHK_CSV_COLUMNS 스키마로 옮긴다.")
    ap.add_argument("--path", default=str(CHK_CSV_PATH),
                    help="대상 CSV 경로 (기본: lib.config.CHK_CSV_PATH)")
    ap.add_argument("--apply", action="store_true",
                    help="실제로 변환한다. 없으면 dry-run(기본).")
    ap.add_argument("--fill-base", metavar="\"YYYY-mm-dd HH:MM:SS=값\"", default=None,
                    help="1회성 보정: Timestamp 가 정확히 일치하는 행 1개의 빈 Base Pressure 를 채운다.")
    ap.add_argument("--selftest", action="store_true",
                    help="회귀 테스트를 돌린다(실제 파일을 건드리지 않는다).")
    args = ap.parse_args(argv)

    if args.selftest:
        return cmd_selftest()
    return cmd_migrate(Path(args.path), dry=not args.apply, fill_base=args.fill_base)


if __name__ == "__main__":
    raise SystemExit(main())
