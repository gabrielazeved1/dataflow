import csv
import io
import random
import string
from datetime import date, timedelta
from typing import Any

_DATE_START = date(2020, 1, 1)
_DATE_END = date(2026, 12, 31)
_DEFAULT_NUM_MIN = 0
_DEFAULT_NUM_MAX = 1000


def _random_value(field: dict[str, Any]) -> str:
    field_type = field["tipo"]
    if field_type == "date":
        delta = (_DATE_END - _DATE_START).days
        return (_DATE_START + timedelta(days=random.randint(0, delta))).isoformat()
    if field_type == "string":
        return "".join(random.choices(string.ascii_lowercase, k=8))
    lo = field.get("min") if field.get("min") is not None else _DEFAULT_NUM_MIN
    hi = field.get("max") if field.get("max") is not None else _DEFAULT_NUM_MAX
    if field_type == "integer":
        return str(random.randint(int(lo), int(hi)))
    if field_type == "float":
        return f"{random.uniform(float(lo), float(hi)):.2f}"
    raise ValueError(f"unknown field type: {field_type}")


def _invalidate(row: dict[str, str], fields: list[dict[str, Any]]) -> dict[str, str]:
    required = [f for f in fields if f["obrigatorio"]]
    ranged = [f for f in fields if f.get("min") is not None or f.get("max") is not None]
    typed = [f for f in fields if f["tipo"] != "string"]

    choices: list[tuple[str, list[dict[str, Any]], float]] = []
    if required:
        choices.append(("null_required", required, 0.6))
    if ranged:
        choices.append(("out_of_range", ranged, 0.3))
    if typed:
        choices.append(("wrong_type", typed, 0.1))

    if not choices:
        return row

    weights = [w for _, _, w in choices]
    kind, candidates, _ = random.choices(choices, weights=weights, k=1)[0]
    field = random.choice(candidates)
    name = field["nome"]
    row = dict(row)

    if kind == "null_required":
        row[name] = ""
    elif kind == "out_of_range":
        if field.get("max") is not None:
            bad = float(field["max"]) + 100
        else:
            bad = float(field["min"]) - 100
        row[name] = str(int(bad)) if field["tipo"] == "integer" else f"{bad:.2f}"
    else:
        row[name] = "not_a_number"
    return row


def generate_csv(dataset_schema: dict[str, Any], rows: int, invalid_rate: float) -> str:
    fields = dataset_schema["campos"]
    names = [f["nome"] for f in fields]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=names)
    writer.writeheader()
    for _ in range(rows):
        row = {f["nome"]: _random_value(f) for f in fields}
        if random.random() < invalid_rate:
            row = _invalidate(row, fields)
        writer.writerow(row)
    return buf.getvalue()
