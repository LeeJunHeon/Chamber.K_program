# lib/heater_profile.py
"""히터 램프의 '감속 접근(approach)' 프로파일 — 순수 함수만 모아 둔다.

[왜]
  목표에 정속으로 도착하면 그 순간 히터에 잉여 전력이 남아 그대로 넘어간다.
  2026-09-10 11:33 수동 100°C 테스트에서 6°C/min 으로 도착 → 19A 가 남아
  +5°C 오버슈트했다. 도착 속도를 0 에 가깝게 줄이면 이 잉여가 사라진다.

[모델] 남은 거리에 선형 비례해 속도를 줄인다.
  남은 거리 d (= target − 현재 램프 목표), 본 구간 속도 rate, 접근 구간 zone,
  도착 속도 rmin 일 때

      r(d) = rmin + (rate − rmin) · d / zone      (d < zone)
      r(d) = rate                                 (d ≥ zone)

  d = zone 에서 r = rate 로 이어지고, d = 0 에서 r = rmin 이다(연속).

[시간 적분] b ≡ (rate − rmin) / zone 로 두면 r(d) = rmin + b·d 이므로,
  거리 d 를 지나는 데 걸리는 시간은 dt = dd / r(d). 접근 구간 전체(d: Z→0)는

      ∫₀^Z dd / (rmin + b·d) = ln((rmin + b·Z) / rmin) / b

  이 적분이 없으면 예상 시간이 계속 모자란다. 접근 구간은 속도가 rate 에서
  rmin 까지 떨어지므로, 단순히 Z/rate 로 잡으면 실제의 몇 분의 1이 된다.
  (12°C/min · zone 20 · rmin 1 이면 접근 구간에만 4.52분이 걸린다)

  따라서 거리 span 전체는

      ramp_minutes(span) = (span − d₁)/rate + ln((rmin + b·d₁)/rmin)/b
                           , d₁ = min(span, zone)

  rate ≤ rmin 이면 감속할 것이 없으므로 span/rate 다(zone ≤ 0 도 마찬가지).

[역산] 레시피의 ramp_min(소요 시간 지정)은 '접근 구간까지 포함한 전체 시간'
  이다. ramp_minutes 는 rate 에 대해 단조 감소하므로, 주어진 시간을 만드는
  본 구간 속도는 이분법으로 안전하게 구할 수 있다(해석해가 없다 — 로그 항의
  b 가 rate 에 묶여 있다).

이 모듈은 config 를 읽기만 하고 PLC/Qt 에 의존하지 않는다. 예상 시간 계산
(레시피 러너·엑셀 K열)과 실행 중 속도 결정이 같은 식을 쓰게 하는 것이 목적이다.
"""

from __future__ import annotations

import math

from lib.config import (
    HEATER_APPROACH_ZONE_C,
    HEATER_APPROACH_MIN_RATE_C_PER_MIN,
)


def _zr(zone, rmin):
    """zone/rmin 이 None 이면 config 값을 쓴다."""
    z = float(HEATER_APPROACH_ZONE_C) if zone is None else float(zone)
    m = float(HEATER_APPROACH_MIN_RATE_C_PER_MIN) if rmin is None else float(rmin)
    return z, m


def approach_rate(dist: float, rate: float,
                  zone=None, rmin=None) -> float:
    """남은 거리 dist(°C)에서 지금 내야 할 속도(°C/min).

    거리에 선형 비례한다. 접근 구간 밖이면 rate 그대로.
    """
    z, m = _zr(zone, rmin)
    rate = float(rate)
    dist = float(dist)
    if rate <= m or z <= 0 or dist >= z:
        return rate
    return m + (rate - m) * max(0.0, dist) / z


def ramp_minutes(span: float, rate: float,
                 zone=None, rmin=None) -> float:
    """거리 span(°C)을 rate(°C/min)로 올리는 데 걸리는 시간(분).

    접근 구간의 감속을 시간 적분으로 반영한다(모듈 docstring 참조).
    """
    z, m = _zr(zone, rmin)
    span = abs(float(span))
    rate = float(rate)
    if rate <= 0 or span <= 0:
        return 0.0
    if z <= 0 or rate <= m:
        return span / rate
    d1 = min(span, z)
    b = (rate - m) / z
    return (span - d1) / rate + math.log((m + b * d1) / m) / b


def solve_rate_for_minutes(span: float, minutes: float,
                           zone=None, rmin=None) -> float:
    """span(°C)을 minutes(분)에 올리는 '본 구간' 속도(°C/min).

    ramp_minutes(span, r) 은 r 에 대해 단조 감소이므로 이분법으로 찾는다.
    감속 구간이 시간을 잡아먹으므로 답은 항상 산술값(span/minutes)보다 크다.
    """
    z, m = _zr(zone, rmin)
    span = abs(float(span))
    minutes = float(minutes)
    if minutes <= 0 or span <= 0:
        return 0.0
    r0 = span / minutes
    if z <= 0 or r0 <= m:
        # 감속이 걸리지 않는 영역 — 산술값이 곧 답이다
        return r0
    lo = r0                      # 여기서는 ramp_minutes >= minutes
    hi = r0 * 10.0
    while ramp_minutes(span, hi, z, m) > minutes and hi < 600.0:
        hi *= 2.0
    hi = min(hi, 600.0)
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if ramp_minutes(span, mid, z, m) > minutes:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0
