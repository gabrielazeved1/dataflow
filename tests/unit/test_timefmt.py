from datetime import date

from dashboard.timefmt import fmt, local_date, to_local


def test_utc_converts_to_brasilia_time():
    # 14:47 utc = 11:47 em brasilia (utc-3, sem horario de verao desde 2019)
    assert fmt("2026-11-07T14:47:39.123Z") == "07/11/2026 11:47:39"


def test_day_boundary_falls_on_previous_day():
    # 01h utc ainda é 22h do dia anterior em brasilia
    assert local_date("2026-11-07T01:00:00Z") == date(2026, 11, 6)


def test_accepts_explicit_offset():
    assert to_local("2026-11-07T14:47:39+00:00").hour == 11


def test_empty_value():
    assert fmt(None) == "—"
