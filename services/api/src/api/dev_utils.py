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


def _random_value(campo: dict[str, Any]) -> str:
    tipo = campo["tipo"]
    if tipo == "date":
        delta = (_DATE_END - _DATE_START).days
        return (_DATE_START + timedelta(days=random.randint(0, delta))).isoformat()
    if tipo == "string":
        return "".join(random.choices(string.ascii_lowercase, k=8))
    lo = campo.get("min") if campo.get("min") is not None else _DEFAULT_NUM_MIN
    hi = campo.get("max") if campo.get("max") is not None else _DEFAULT_NUM_MAX
    if tipo == "integer":
        return str(random.randint(int(lo), int(hi)))
    if tipo == "float":
        return f"{random.uniform(float(lo), float(hi)):.2f}"
    raise ValueError(f"tipo desconhecido: {tipo}")


def _invalidate(row: dict[str, str], campos: list[dict[str, Any]]) -> dict[str, str]:
    required = [c for c in campos if c["obrigatorio"]]
    ranged = [c for c in campos if c.get("min") is not None or c.get("max") is not None]
    typed = [c for c in campos if c["tipo"] != "string"]

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
    campo = random.choice(candidates)
    nome = campo["nome"]
    row = dict(row)

    if kind == "null_required":
        row[nome] = ""
    elif kind == "out_of_range":
        if campo.get("max") is not None:
            bad = float(campo["max"]) + 100
        else:
            bad = float(campo["min"]) - 100
        row[nome] = str(int(bad)) if campo["tipo"] == "integer" else f"{bad:.2f}"
    else:
        row[nome] = "nao_eh_numero"
    return row


def generate_csv(dataset_schema: dict[str, Any], rows: int, invalid_rate: float) -> str:
    campos = dataset_schema["campos"]
    nomes = [c["nome"] for c in campos]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=nomes)
    writer.writeheader()
    for _ in range(rows):
        row = {c["nome"]: _random_value(c) for c in campos}
        if random.random() < invalid_rate:
            row = _invalidate(row, campos)
        writer.writerow(row)
    return buf.getvalue()
