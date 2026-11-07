import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

# banco e api ficam em utc; so a tela converte pro fuso de quem le
DISPLAY_TZ = ZoneInfo(os.getenv("DISPLAY_TZ", "America/Sao_Paulo"))


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_local(value: str) -> datetime:
    return parse_ts(value).astimezone(DISPLAY_TZ)


def fmt(value: str | None, pattern: str = "%d/%m/%Y %H:%M:%S") -> str:
    return to_local(value).strftime(pattern) if value else "—"


def local_date(value: str) -> date:
    return to_local(value).date()


def today_local() -> date:
    return datetime.now(DISPLAY_TZ).date()
