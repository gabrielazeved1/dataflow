from unittest.mock import MagicMock, patch

from dashboard import api_client


def _page(ids, next_cursor):
    response = MagicMock()
    response.json.return_value = {
        "items": [{"id": i} for i in ids],
        "next_cursor": next_cursor,
    }
    return response


def _fake_get(pages):
    def get(url, params, timeout):
        return pages[params.get("after")]

    return get


def test_list_executions_walks_all_pages():
    # regressao: so a primeira pagina era lida, kpis travavam em 50
    api_client.list_executions.clear()
    pages = {
        None: _page(range(0, 200), "c1"),
        "c1": _page(range(200, 400), "c2"),
        "c2": _page(range(400, 450), None),
    }
    with patch.object(api_client.requests, "get", side_effect=_fake_get(pages)):
        items = api_client.list_executions(status="SUCCESS")

    assert len(items) == 450


def test_list_executions_stops_at_limit():
    api_client.list_executions.clear()
    pages = {None: _page(range(0, 50), "c1"), "c1": _page(range(50, 100), None)}
    with patch.object(api_client.requests, "get", side_effect=_fake_get(pages)) as get:
        items = api_client.list_executions(limit=50)

    assert len(items) == 50
    assert get.call_count == 1
