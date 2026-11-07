from typing import Any

from dashboard.timefmt import DISPLAY_TZ, parse_ts

DONE = "#22c55e"
ACTIVE = "#eab308"
LOST = "#ef4444"
RECOVERED = "#f97316"
IDLE = "#3f3f46"

# no: (rotulo, formato). cylinder = onde o dado mora
_NODES = {
    "client": ("cliente", "box"),
    "api": ("api\nfastapi", "box"),
    "redis": ("redis\nfila", "cylinder"),
    "worker": ("worker\ncelery + spark", "box"),
    "minio": ("minio\ndata lake", "cylinder"),
    "postgres": ("postgres\nmetadados", "cylinder"),
    "reaper": ("reaper", "box"),
    "reconciler": ("reconciler", "box"),
}


def _dot(text: str) -> str:
    # quebra de linha no dot é o escape \n, nao o caractere
    return text.replace("\n", "\\n")


def summarize(trace: dict[str, Any]) -> str:
    # estado geral da ingestao, do ponto de vista de quem enviou
    executions = trace["executions"]
    steps = [e["step"] for e in trace["events"]]
    if any(e["layer"] == "GOLD" and e["status"] == "SUCCESS" for e in executions):
        return "success"
    if steps and steps[-1] == "enqueue_failed":
        return "lost"
    if steps and steps[-1] in ("failed", "timeout"):
        return "failed"
    return "running"


def build_dot(trace: dict[str, Any]) -> str:
    events = trace["events"]
    steps = {e["step"] for e in events}
    by_step = {e["step"]: e for e in events}
    status = summarize(trace)

    def counts() -> str:
        d = (by_step.get("spark_validated") or {}).get("details") or {}
        if not d:
            return ""
        return f"\n{d['valid']} validas / {d['rejected']} rejeitadas"

    queued = "enqueued" in steps or "requeued" in steps
    picked = "silver_started" in steps
    failed = "failed" in steps and status != "success"

    # (origem, destino, rotulo, cor, tracejado)
    edges = [
        ("client", "api", "upload csv", DONE if "received" in steps else IDLE, False),
        (
            "api",
            "postgres",
            "bronze RUNNING",
            DONE if "received" in steps else IDLE,
            False,
        ),
        ("api", "minio", "bronze", DONE if "bronze_stored" in steps else IDLE, False),
    ]
    if "enqueue_failed" in steps:
        edges.append(("api", "redis", "send_task\nPERDIDA", LOST, True))
    else:
        edges.append(
            ("api", "redis", "send_task", DONE if "enqueued" in steps else IDLE, False)
        )
    if "requeued" in steps:
        edges.append(("reconciler", "redis", "reenfileirou", RECOVERED, False))
    if "timeout" in steps:
        edges.append(("reaper", "postgres", "timeout\nFAILED", LOST, True))

    if picked:
        edges.append(("redis", "worker", "task", DONE, False))
    elif queued:
        edges.append(("redis", "worker", "na fila...", ACTIVE, True))
    else:
        edges.append(("redis", "worker", "task", IDLE, False))

    if failed:
        edges.append(("worker", "minio", "erro", LOST, True))
    else:
        silver_color = DONE if "silver_stored" in steps else ACTIVE if picked else IDLE
        edges.append(
            ("worker", "minio", "silver + _rejected" + counts(), silver_color, False)
        )
        gold_color = (
            DONE
            if "gold_stored" in steps
            else ACTIVE if "gold_started" in steps else IDLE
        )
        edges.append(("worker", "minio", "gold", gold_color, False))
        edges.append(("worker", "postgres", "metricas", gold_color, False))

    used = {n for a, b, *_ in edges for n in (a, b)}
    lines = [
        "digraph trace {",
        'rankdir=LR; bgcolor="transparent"; nodesep=0.6; ranksep=0.9;',
        'node [fontname="Helvetica" fontsize=11 style="filled" fillcolor="#111111"'
        ' color="#3f3f46" fontcolor="#fafafa"];',
        'edge [fontname="Helvetica" fontsize=9 fontcolor="#a3a3a3"];',
    ]
    for name, (label, shape) in _NODES.items():
        if name not in used:
            continue
        color = LOST if name == "worker" and failed else "#00e5ff"
        lines.append(f'{name} [label="{_dot(label)}" shape={shape} color="{color}"];')
    for src, dst, label, color, dashed in edges:
        style = "dashed" if dashed else "solid"
        attrs = f'label="{_dot(label)}" color="{color}" style={style} penwidth=2'
        lines.append(f"{src} -> {dst} [{attrs}];")
    lines.append("}")
    return "\n".join(lines)


def timeline_rows(trace: dict[str, Any]) -> list[dict[str, str]]:
    rows = []
    previous = None
    for event in trace["events"]:
        ts = parse_ts(event["created_at"])
        delta = f"+{(ts - previous).total_seconds():.2f}s" if previous else ""
        rows.append(
            {
                "hora": ts.astimezone(DISPLAY_TZ).strftime("%H:%M:%S.%f")[:-3],
                "delta": delta,
                "servico": event["service"],
                "passo": event["step"],
                "mensagem": event["message"],
            }
        )
        previous = ts
    return rows
