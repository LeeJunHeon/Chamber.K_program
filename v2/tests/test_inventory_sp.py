"""장비 설정 읽기(인벤토리, 5-19 ④) — MFC R1·R2·R3·R4·R10 응답을 드라이버와 같은 해석(parse_sp_reply)으로 풀어
'형식 맞음 / 형식 다름' 과 값(mTorr)을 보인다. bench 는 쓰기 검증을 해 볼 수 없으므로 읽은 원문만으로 해석 가능 여부를 미리 본다."""
from v2.common.paramdef import defaults
from v2.chk import params as P

CFG = defaults(P.PARAMS)


def test_inventory_shows_setpoint_readback_format_and_value():
    from v2.chk import inventory as INV
    inv = INV.Inventory()
    for q, raw in (("R33", "E11"), ("R34", "F01"), ("R1", "S1+000.50"), ("R2", " S2+000.50 "), ("R3", "P+0.20"),
                   ("R4", "S4+003.00"), ("R10", None), ("R37", "M006")):
        inv.items.append(INV.Item("mfc", q, raw, True, ""))
    INV.mfc_interpret(inv, CFG)
    sp = [x for x in inv.checks if "되읽기(" in x]
    assert sp == ["[확인] MFC SP1 되읽기(R1) 형식 맞음 — 'S1+000.50' = 0.5 %FS = 5.00 mTorr",
                  "[확인] MFC SP2 되읽기(R2) 형식 맞음 — ' S2+000.50 ' = 0.5 %FS = 5.00 mTorr",
                  "!!! MFC SP3 되읽기(R3) 형식 다름 — 'P+0.20': v2 가 'S3' 접두어 값으로 풀지 못함(쓰기 확인·장비 값 출처 실패)",
                  "[확인] MFC SP4 되읽기(R4) 형식 맞음 — 'S4+003.00' = 3 %FS = 30.00 mTorr",
                  "[미확인] MFC SP5 되읽기(R10) — 무응답"]
    assert inv.values["mfc_sp4_mtorr"] == 30.0 and "mfc_sp3_mtorr" not in inv.values


# 5-20 ⑤ — 11/23 bench 응답: R38 'H1.00'(10/20 과 같음)은 맞는 형식, R37 'M100'(X=1)·'M006'(X=0)은 정보로만, 설정점 되읽기 확인
def test_inventory_11_23_bench_replies():
    from v2.chk import devices as D
    from v2.chk import inventory as INV
    assert INV.mfc_format_ok(38, "H1.00") and INV.mfc_format_ok(38, "H3.10") and not INV.mfc_format_ok(38, "1.00")
    inv = INV.Inventory()
    for q, raw in (("R33", "E11"), ("R34", "F01"), ("R1", "S1+004.60"), ("R2", "S2+000.50"), ("R3", "S3+001.50"),
                   ("R4", "S4+003.00"), ("R10", "S5+005.00"), ("R37", "M100"), ("R38", "H1.00")):
        inv.items.append(INV.Item("mfc", q, raw, True, ""))
    INV.mfc_interpret(inv, CFG)
    assert [inv.values[f"mfc_sp{n}_mtorr"] for n in (1, 2, 3, 4, 5)] == [46.0, 5.0, 15.0, 30.0, 50.0]
    assert "[확인] MFC Remote(R37 X=1) — 원격 제어 가능 [p.84, p.54]" in inv.checks
    assert any(x.startswith("[참고] MFC R37 은 bench 날마다") and "거부하지 않음" in x for x in inv.checks)
    assert any(x.startswith("[확인] MFC 소프트웨어 버전 R38 'H1.00'") for x in inv.checks)
    a = {x.item: x.assumed for x in D.ASSUMPTIONS}
    assert "11/23 bench 'M100'" in a["MFC Remote 표시(R37)"] and "확인(11/23 bench)" in a["MFC 설정점 되읽기 형식(R1~R4·R10)"]
    assert "'H1.00'" in a["MFC 소프트웨어 버전(R38)"]
