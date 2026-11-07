from dashboard.trace_graph import build_dot, summarize, timeline_rows


def _trace(steps, gold_status=None):
    executions = [{"layer": "BRONZE", "status": "SUCCESS"}]
    if gold_status:
        executions.append({"layer": "GOLD", "status": gold_status})
    events = [
        {
            "step": s,
            "service": "api",
            "message": s,
            "details": None,
            "created_at": f"2026-11-07T00:00:0{i}Z",
        }
        for i, s in enumerate(steps)
    ]
    return {"executions": executions, "events": events}


def test_summarize_states():
    assert summarize(_trace(["received", "enqueued"])) == "running"
    assert summarize(_trace(["received", "enqueue_failed"])) == "lost"
    assert summarize(_trace(["silver_started", "failed"])) == "failed"
    assert summarize(_trace(["gold_stored"], gold_status="SUCCESS")) == "success"


def test_lost_request_draws_red_dashed_edge():
    dot = build_dot(_trace(["received", "bronze_stored", "enqueue_failed"]))
    assert (
        'api -> redis [label="send_task\\nPERDIDA" color="#ef4444" style=dashed' in dot
    )
    assert "reconciler" not in dot


def test_requeued_request_shows_reconciler_and_queue():
    dot = build_dot(_trace(["received", "enqueue_failed", "requeued"]))
    assert "reconciler -> redis" in dot
    assert 'redis -> worker [label="na fila..."' in dot


def test_timeline_has_deltas():
    rows = timeline_rows(_trace(["received", "bronze_stored"]))
    assert [r["delta"] for r in rows] == ["", "+1.00s"]
