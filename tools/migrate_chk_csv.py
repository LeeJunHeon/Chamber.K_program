# tools/migrate_chk_csv.py
# -*- coding: utf-8 -*-
"""ChK_log.csv 를 현재 CHK_CSV_COLUMNS 스키마로 옮긴다 (1회용, 수동 실행).

[왜 필요한가]
  lib/logger.py 의 append_chk_csv_row 는 파일이 없을 때만 헤더를 쓴다.
  컬럼을 늘려도 NAS 에 이미 있는 ChK_log.csv 에는 옛 헤더가 그대로 박혀 있어서,
  logger 는 '어긋난 행을 만들지 않으려고' 파일의 옛 헤더를 따라 기록한다
  (= 새 컬럼은 기록되지 않는다). 이 스크립트로 파일 자체를 옮겨야 새 컬럼이 남는다.

[★ 과거 버그 — 같은 실수를 반복하지 말 것]
  예전 구현은 csv.DictReader 로 읽었다. DictReader 는 '파일의 헤더'를 fieldnames 로
  쓰기 때문에, 헤더가 15열인 파일에 섞여 있는 16필드 행(2026-09-03 에 Heater Temp 가
  추가된 뒤의 행)을 읽으면 값이 한 칸씩 밀리고 16번째 값(DC: P)이 restkey 로 새어
  통째로 버려졌다. 실측 피해:
      원본   Heater='' RF For.P=0.000 RF Ref.P=0.000 DC V=314.768 DC I=0.635 DC P=199.951
      변환후 Heater='' RF For.P=''    RF Ref.P=0.000 DC V=0.000   DC I=314.768 DC P=0.635
  게다가 자체 검증이 '행 개수'만 봤기 때문에 그냥 통과했다.
  그래서 지금은 (1) csv.reader 로 원시 리스트를 읽고 행별 필드 수로 스키마를 정하고,
  (2) 검증을 값 기준으로 한다(DC: P 보존 확인). 필드 수를 모르면 추측하지 않고 중단한다.

[안전 장치]
  - 기본이 dry-run 이다. 실제로 바꾸려면 --apply 를 붙여야 한다.
  - 바꾸기 전에 같은 폴더에 백업을 만든다. 백업에 실패하면 중단한다.
  - 알 수 없는 필드 수의 행이 하나라도 있으면 중단한다(추측하지 않는다).
  - 임시 파일에 다 쓴 뒤 os.replace 로 원자적 교체한다.
  - 값 검증에 하나라도 실패하면 임시 파일을 지우고 원본을 건드리지 않는다.
  - 이미 최신 스키마면 아무것도 하지 않는다(몇 번을 돌려도 안전하다).

[사용법]
    python tools\\migrate_chk_csv.py                 # dry-run (기본 대상 = CHK_CSV_PATH)
    python tools\\migrate_chk_csv.py --apply         # 실제 변환
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
# 필드 수로 어느 시절의 행인지 가른다. 헤더만 믿으면 안 된다 —
# 실제 파일에는 헤더가 15열인데 뒤쪽 행만 16필드인 경우가 있다.
COLS_NOW = list(CHK_CSV_COLUMNS)                       # 현재(이 작업 후 20열)

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

SCHEMAS = {
    len(COLS_15): ("15열(Heater Temp 이전)", COLS_15),
    len(COLS_16): ("16열(Heater Temp 추가)", COLS_16),
    len(COLS_18): ("18열(RF Pulse Freq/Duty 추가)", COLS_18),
}
if len(COLS_NOW) not in SCHEMAS:
    SCHEMAS[len(COLS_NOW)] = ("현재 스키마", COLS_NOW)


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


def _convert(path: Path, verbose=True):
    """(헤더, 원시행들, 변환된 dict 목록, 경고목록) 또는 (.., None, ..) 에러."""
    header, data, blank = _read_raw(path)
    if header is None:
        return None, [], None, ["헤더가 비어 있습니다."]

    warns = []
    counts = {}
    for r in data:
        counts[len(r)] = counts.get(len(r), 0) + 1

    if verbose:
        print("기존 헤더 (%d열): %s" % (len(header), header))
        print("현재 스키마(%d열): %s" % (len(COLS_NOW), COLS_NOW))
        print("데이터 행 %d개 (빈 행 %d개는 건너뜀)" % (len(data), blank))
        for n in sorted(counts):
            label = SCHEMAS.get(n, ("알 수 없음", None))[0]
            print("   %d필드 행: %d개  [%s]" % (n, counts[n], label))

    unknown = [n for n in counts if n not in SCHEMAS]
    if unknown:
        msgs = ["알 수 없는 필드 수의 행이 있습니다: %s" % sorted(unknown)]
        for i, r in enumerate(data, start=2):
            if len(r) in unknown:
                msgs.append("   행 %d: %d필드 → %r" % (i, len(r), r[:4]))
                if len(msgs) > 12:
                    break
        return header, data, None, msgs

    if len(COLS_18) in counts:
        warns.append(
            "18필드 행이 %d개 있습니다 — 옛 마이그레이션을 거친 파일일 수 있습니다. "
            "2026-09-03 이후 행의 DC 값(특히 DC: P)을 눈으로 확인하세요."
            % counts[len(COLS_18)])

    out = []
    for r in data:
        cols = SCHEMAS[len(r)][1]
        d = dict(zip(cols, r))
        out.append({k: d.get(k, "") for k in COLS_NOW})
    return header, data, out, warns


def _verify(data, out):
    """값 기준 검증. 문제 목록을 돌려준다(빈 목록 = 통과)."""
    problems = []
    if len(data) != len(out):
        problems.append("행 수 불일치: 원본 %d → 변환 %d" % (len(data), len(out)))
        return problems

    for i, (raw, o) in enumerate(zip(data, out), start=2):
        if len(o) != len(COLS_NOW):
            problems.append("행 %d: 변환 결과가 %d필드 (기대 %d)"
                            % (i, len(o), len(COLS_NOW)))
            continue
        cols = SCHEMAS[len(raw)][1]
        # 1) DC: P 가 값 그대로 보존되는가 (예전 버그가 정확히 여기서 터졌다)
        if "DC: P" in cols:
            want = raw[cols.index("DC: P")]
            got = o.get("DC: P", "")
            if want != got:
                problems.append("행 %d: DC: P 소실/변형 — 원본 %r → 변환 %r"
                                % (i, want, got))
        # 2) 15/16필드 행에서는 DC: P 가 마지막 필드다. 마지막 비어있지 않은 값과 대조
        if len(raw) in (len(COLS_15), len(COLS_16)):
            last = raw[-1]
            if (last or "").strip() != "" and o.get("DC: P", "") != last:
                problems.append("행 %d: 마지막 값 %r 가 DC: P(%r)와 다름"
                                % (i, last, o.get("DC: P", "")))
        # 3) 원본에 있던 모든 컬럼 값이 그대로 옮겨졌는가
        for k, v in zip(cols, raw):
            if k in COLS_NOW and o.get(k, "") != v:
                problems.append("행 %d: %s 값이 %r → %r 로 바뀜" % (i, k, v, o.get(k, "")))
                break
    return problems


def _print_samples(data, out):
    """첫 행 / 마지막 15필드 행 / 모든 16필드 행을 원본 대비 전 컬럼 출력."""
    picks = []
    if data:
        picks.append((2, "첫 행"))
    last15 = None
    for i, r in enumerate(data, start=2):
        if len(r) == len(COLS_15):
            last15 = i
    if last15 and last15 != 2:
        picks.append((last15, "마지막 15필드 행"))
    for i, r in enumerate(data, start=2):
        if len(r) == len(COLS_16):
            picks.append((i, "16필드 행"))

    seen = set()
    for idx, label in picks:
        if idx in seen:
            continue
        seen.add(idx)
        raw = data[idx - 2]
        o = out[idx - 2]
        cols = SCHEMAS[len(raw)][1]
        src = dict(zip(cols, raw))
        print()
        print("  --- 행 %d (%s, %d필드) ---" % (idx, label, len(raw)))
        print("    %-22s %-22s %s" % ("컬럼", "변환 전", "변환 후"))
        for k in COLS_NOW:
            b = src.get(k, "(없음)")
            a = o.get(k, "")
            mark = "" if (b == a or b == "(없음)") else "   ★다름"
            print("    %-22s %-22r %r%s" % (k, b, a, mark))


def cmd_migrate(path: Path, dry: bool) -> int:
    print("대상 파일 : %s" % path)
    print("모드      : %s" % ("실제 변환(--apply)" if not dry else "dry-run (아무것도 바꾸지 않음)"))
    print()

    if not path.exists():
        print("[중단] 파일이 없습니다. 새로 만들어질 파일은 이미 최신 스키마로 기록됩니다.")
        return 1

    try:
        header, data, out, msgs = _convert(path)
    except Exception as e:
        print("[중단] 읽기 실패: %r" % (e,))
        return 1

    if out is None:
        print()
        for m in msgs:
            print("[중단] %s" % m)
        print("       어떤 컬럼인지 사람이 판단해야 합니다. 추측해서 옮기지 않습니다.")
        return 2

    for w in msgs:
        print()
        print("[경고] %s" % w)

    if header == COLS_NOW and all(len(r) == len(COLS_NOW) for r in data):
        print()
        print("이미 최신 스키마입니다 — 아무것도 하지 않습니다.")
        return 0

    added = [c for c in COLS_NOW if c not in header]
    print()
    print("추가될 컬럼: %s" % (added or "(없음)"))

    problems = _verify(data, out)
    _print_samples(data, out)

    if problems:
        print()
        for p in problems[:20]:
            print("[중단] 검증 실패: %s" % p)
        print("       원본을 건드리지 않았습니다.")
        return 1
    print()
    print("값 검증 통과: %d행 전부 DC: P 등 기존 값이 보존됩니다." % len(out))

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
            w = csv.DictWriter(f, fieldnames=COLS_NOW, extrasaction="ignore")
            w.writeheader()
            for o in out:
                w.writerow(o)
        # 쓴 결과를 다시 읽어 검증한 뒤에야 교체한다
        with tmp.open("r", newline="", encoding="utf-8") as f:
            back = list(csv.reader(f))
        if back[0] != COLS_NOW:
            raise ValueError("임시 파일 헤더가 다릅니다")
        body = [r for r in back[1:] if r]
        if len(body) != len(out):
            raise ValueError("임시 파일 행 수가 다릅니다: %d != %d" % (len(body), len(out)))
        bad = [i for i, r in enumerate(body, start=2) if len(r) != len(COLS_NOW)]
        if bad:
            raise ValueError("임시 파일에 필드 수가 다른 행: %s" % bad[:5])
        ip = COLS_NOW.index("DC: P")
        for i, (r, o) in enumerate(zip(body, out), start=2):
            if r[ip] != o.get("DC: P", ""):
                raise ValueError("행 %d DC: P 불일치: %r != %r" % (i, r[ip], o.get("DC: P", "")))
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
    print("이 파일은 이제 %d열입니다." % len(COLS_NOW))
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


def cmd_selftest() -> int:
    fails = []

    def ck(label, cond, extra=""):
        print("  %-6s %s %s" % ("[OK]" if cond else "[FAIL]", label, extra))
        if not cond:
            fails.append(label)

    td = Path(tempfile.mkdtemp(prefix="chk_mig_selftest_"))
    p = td / "ChK_log.csv"

    # 헤더 15열 + 15필드 2행 + 16필드 2행 + 끝 빈 줄
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLS_15)
        w.writerow(FIX_15A)
        w.writerow(FIX_15B)
        w.writerow(FIX_16A)
        w.writerow(FIX_16B)
        f.write("\r\n")          # 파일 끝 빈 줄(실제 파일에 있다)

    print("=== 픽스처 ===")
    print("  %s" % p)
    print("  헤더 15열 / 15필드 2행 / 16필드 2행 / 끝 빈 줄")
    print()

    rc = cmd_migrate(p, dry=False)
    print()
    print("=== 단정 ===")
    ck("--apply rc=0", rc == 0, "-> rc=%d" % rc)

    with p.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    hdr = rows[0]
    body = [r for r in rows[1:] if r]
    ck("헤더가 현재 스키마(%d열)" % len(COLS_NOW), hdr == COLS_NOW, "-> %d열" % len(hdr))
    ck("데이터 4행", len(body) == 4, "-> %d행" % len(body))
    ck("모든 행 %d필드" % len(COLS_NOW),
       all(len(r) == len(COLS_NOW) for r in body),
       "-> %r" % sorted({len(r) for r in body}))

    recs = [dict(zip(hdr, r)) for r in body]

    # 15필드 행: DC: P 가 원본 마지막 값과 같고 Heater Temp 는 ""
    for i, fix in enumerate((FIX_15A, FIX_15B)):
        r = recs[i]
        ck("15필드 행%d: DC: P == 원본 마지막 값 %r" % (i + 1, fix[-1]),
           r["DC: P"] == fix[-1], "-> %r" % r["DC: P"])
        ck("  Heater Temp == ''", r["Heater Temp"] == "", "-> %r" % r["Heater Temp"])

    # 16필드 행: 실제 값 그대로
    EXPECT16 = [
        (FIX_16A, {"Heater Temp": "", "RF: For.P": "0.000", "RF: Ref. P": "0.000",
                   "DC: V": "314.768", "DC: I": "0.635", "DC: P": "199.951"}),
        (FIX_16B, {"Heater Temp": "", "RF: For.P": "0.000", "RF: Ref. P": "0.000",
                   "DC: V": "410.085", "DC: I": "0.121", "DC: P": "49.753"}),
    ]
    for k, (fix, exp) in enumerate(EXPECT16):
        r = recs[2 + k]
        print("  --- 16필드 행 %s ---" % fix[0])
        for key, want in exp.items():
            ck("    %-12s == %r" % (key, want), r[key] == want, "-> %r" % r[key])

    # 새 4컬럼은 모두 ""
    NEW4 = ["RF Pulse: Freq[kHz]", "RF Pulse: Duty[%]",
            "RF Pulse: For.P", "RF Pulse: Ref. P"]
    ck("새 4컬럼이 모두 ''",
       all(r.get(c, None) == "" for r in recs for c in NEW4),
       "-> %r" % [{c: r.get(c) for c in NEW4} for r in recs[:1]])

    # 멱등성
    rc2 = cmd_migrate(p, dry=False)
    ck("재실행 rc=0(이미 최신)", rc2 == 0, "-> rc=%d" % rc2)

    # 알 수 없는 필드 수 → 중단
    p2 = td / "ChK_weird.csv"
    with p2.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLS_15)
        w.writerow(FIX_15A)
        w.writerow(FIX_15A + ["x", "y", "z"])       # 18? -> 아니고 18필드
        w.writerow(FIX_15A[:9])                      # 9필드 = 알 수 없음
    rc3 = cmd_migrate(p2, dry=False)
    ck("알 수 없는 필드 수 → rc=2", rc3 == 2, "-> rc=%d" % rc3)
    with p2.open("r", newline="", encoding="utf-8") as f:
        ck("  원본을 건드리지 않음", next(csv.reader(f)) == COLS_15)

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
    ap.add_argument("--selftest", action="store_true",
                    help="회귀 테스트를 돌린다(실제 파일을 건드리지 않는다).")
    args = ap.parse_args(argv)

    if args.selftest:
        return cmd_selftest()
    return cmd_migrate(Path(args.path), dry=not args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
