import json
from urllib.parse import quote

import fakeredis
import pytest
from fastapi.testclient import TestClient

from eddy import api
from eddy.config import metrics_key, user_key
from eddy.online_store import OnlineStoreWriter, to_mapping


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def writer(redis_client):
    return OnlineStoreWriter(prefix="eddy", ttl_seconds=60, client=redis_client)


def test_to_mapping_suffixes_and_skips_nulls():
    row = {"user_id": "u1", "spend": 12.5, "event_count": 3, "cart_conversion": None}
    assert to_mapping(row, "5m") == {"spend_5m": "12.5", "event_count_5m": "3"}
    assert to_mapping({"user_id": "u1", "last_device": "ios"}, None) == {"last_device": "ios"}


def test_guarded_write_rejects_stale_windows(writer, redis_client):
    key = user_key("eddy", "u1")
    assert writer.write([{"user_id": "u1", "event_count": 5, "window_end_ms": 2000}], "5m", "window_end_ms") == (1, 0)
    # An older window arriving late must not overwrite the newer one.
    assert writer.write([{"user_id": "u1", "event_count": 1, "window_end_ms": 1000}], "5m", "window_end_ms") == (0, 1)
    assert redis_client.hget(key, "event_count_5m") == "5"
    # Same window with more data does update.
    writer.write([{"user_id": "u1", "event_count": 7, "window_end_ms": 2000}], "5m", "window_end_ms")
    assert redis_client.hget(key, "event_count_5m") == "7"
    assert 0 < redis_client.ttl(key) <= 60


def test_windows_do_not_clobber_each_other(writer, redis_client):
    writer.write([{"user_id": "u2", "spend": 10.0, "window_end_ms": 5}], "5m", "window_end_ms")
    writer.write([{"user_id": "u2", "spend": 99.0, "window_end_ms": 1}], "1h", "window_end_ms")
    h = redis_client.hgetall(user_key("eddy", "u2"))
    assert h["spend_5m"] == "10.0" and h["spend_1h"] == "99.0"


def test_batching_flushes(redis_client):
    w = OnlineStoreWriter(prefix="eddy", ttl_seconds=60, client=redis_client, flush_every=7)
    rows = [{"user_id": f"u{i}", "last_seen_ms": i} for i in range(50)]
    assert w.write(rows, None, "last_seen_ms") == (50, 0)
    assert redis_client.dbsize() == 50


@pytest.fixture
def client(redis_client, monkeypatch):
    api.app.dependency_overrides[api.get_redis] = lambda: redis_client
    yield TestClient(api.app)
    api.app.dependency_overrides.clear()


def test_api_get_features(client, writer):
    writer.write([{"user_id": "u9", "spend": 42.5, "event_count": 3, "window_end_ms": 1}], "5m", "window_end_ms")
    writer.write([{"user_id": "u9", "last_device": "web", "last_seen_ms": 1_700_000_000_000}], None, "last_seen_ms")
    body = client.get("/features/u9").json()
    assert body["found"] is True
    assert body["features"]["spend_5m"] == 42.5
    assert body["features"]["event_count_5m"] == 3
    assert body["features"]["last_device"] == "web"
    assert body["freshness_ms"] > 0


def test_api_missing_user_and_batch(client, writer):
    assert client.get("/features/nobody").status_code == 404
    writer.write([{"user_id": "a", "last_seen_ms": 1}], None, "last_seen_ms")
    resp = client.post("/features/batch", json={"user_ids": ["a", "b"]}).json()
    assert [r["found"] for r in resp] == [True, False]


def test_api_health_returns_expected_response(client):
    response = client.get("/health")

    assert response.status_code == 200, response.text
    assert response.json() == {"status": "ok"}


def test_api_stats_returns_expected_response(client, redis_client):
    redis_client.hset(
        metrics_key("eddy", "profile"),
        mapping={"input_rows_per_second": "1000.5", "raw": "{}"},
    )

    response = client.get("/stats")

    assert response.status_code == 200, response.text

    body = response.json()

    assert set(body) == {
        "online_store_keys",
        "events_per_minute",
        "queries",
        "serving",
        "server_time_ms",
    }

    assert body["online_store_keys"] == 1
    assert body["events_per_minute"] == 60030

    assert set(body["queries"]) == {"window_5m", "window_1h", "profile"}
    assert body["queries"]["window_5m"] is None
    assert body["queries"]["window_1h"] is None
    assert body["queries"]["profile"] == {"input_rows_per_second": 1000.5}

    assert set(body["serving"]) == {"samples", "p50_ms", "p95_ms"}
    assert isinstance(body["serving"]["samples"], int)

    assert isinstance(body["server_time_ms"], int)
    assert body["server_time_ms"] > 0


def test_console_served_and_fallback(client, monkeypatch, tmp_path):
    monkeypatch.setattr(api, "STATIC_DIR", tmp_path)
    assert "has not been built" in client.get("/").text
    (tmp_path / "index.html").write_text("<div id='root'></div>")
    assert client.get("/").text == "<div id='root'></div>"


def test_sample_users(client, writer):
    writer.write([{"user_id": f"u{i}", "last_seen_ms": 1} for i in range(12)], None, "last_seen_ms")
    ids = client.get("/users/sample?n=5").json()["user_ids"]
    assert len(ids) == 5 and all(i.startswith("u") for i in ids)


def test_stats_reports_serving_latency(client, writer):
    api._lookup_ms.clear()
    assert client.get("/stats").json()["serving"] == {"samples": 0, "p50_ms": None, "p95_ms": None}
    writer.write([{"user_id": "u1", "last_seen_ms": 1}], None, "last_seen_ms")
    for _ in range(3):
        client.get("/features/u1")
    serving = client.get("/stats").json()["serving"]
    assert serving["samples"] == 3 and serving["p50_ms"] >= 0


def test_contracts_diff_fields(client, monkeypatch):
    v1 = {"doc": "v1", "fields": [{"name": "user_id", "type": "string"}, {"name": "amount", "type": "double"}]}
    v2 = {"doc": "v2", "fields": v1["fields"] + [{"name": "device", "type": ["null", "string"], "default": None}]}

    class FakeRegistry:
        def __init__(self, *a, **k):
            pass

        def versions(self, subject):
            return [1, 2]

        def get_version(self, subject, n):
            return {"id": 10 + n, "schema": json.dumps(v1 if n == 1 else v2)}

        def get_compatibility(self, subject):
            return "BACKWARD"

    monkeypatch.setattr(api, "SchemaRegistry", FakeRegistry)
    monkeypatch.setattr(api, "_cache", api._TTLCache())
    body = client.get("/contracts").json()
    assert body["available"] and body["compatibility"] == "BACKWARD"
    latest = body["versions"][-1]
    assert latest["id"] == 12
    added = [(f["name"], f["added"]) for f in latest["fields"]]
    assert added == [("user_id", False), ("amount", False), ("device", True)]
    assert latest["fields"][2]["type"] == "null | string"


def test_contracts_and_dlq_degrade_gracefully(client, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("registry down")

    monkeypatch.setattr(api, "_cache", api._TTLCache())
    monkeypatch.setattr(api, "_load_contracts", boom)
    monkeypatch.setattr(api, "_load_dlq", boom)
    assert client.get("/contracts").json() == {"available": False, "error": "registry down"}
    assert client.get("/dlq").json() == {"available": False, "error": "registry down"}


# ---------------------------------------------------------------- malformed user ids
# The path route declares user_id as a plain `str` and performs no format validation, so a
# malformed id is a lookup miss: the API answers 404 with a JSON detail and must never 500.
MALFORMED_USER_IDS = [
    pytest.param(" ", id="single-space"),
    pytest.param("   ", id="whitespace-only"),
    pytest.param("\t", id="tab"),
    pytest.param("\n", id="newline"),
    pytest.param("a\x00b", id="null-byte"),
    pytest.param("!@#$%^&*()", id="punctuation"),
    pytest.param("'; OR 1=1--", id="sql-injection-ish"),
    pytest.param("<img src=x onerror=alert(1)>", id="html"),
    pytest.param("юзер", id="non-ascii"),
    pytest.param("a..b", id="path-traversal-ish"),
    pytest.param("eddy:user:u9", id="prefix-injection"),
    pytest.param("a" * 1024, id="very-long"),
]


@pytest.mark.parametrize("user_id", MALFORMED_USER_IDS)
def test_api_get_features_rejects_malformed_user_id(client, redis_client, user_id):
    resp = client.get(f"/features/{quote(user_id, safe='')}")
    assert resp.status_code == 404, resp.text
    assert resp.headers["content-type"].startswith("application/json")
    assert "no features for user" in resp.json()["detail"]
    # A rejected lookup is read-only: it must not leave an empty hash behind.
    assert redis_client.dbsize() == 0


def test_api_get_features_empty_user_id_is_not_routed(client):
    resp = client.get("/features/")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Not Found"}


def test_api_get_features_encoded_traversal_stays_on_the_features_route(client):
    # `%2e%2e` reaches the handler as the literal id "..", so it must not be normalised away
    # into the console route and served as HTML.
    resp = client.get("/features/%2e%2e")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "no features for user '..'"
    assert resp.headers["content-type"].startswith("application/json")


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"user_ids": [""]}, id="empty-string"),
        pytest.param({"user_ids": [None]}, id="null"),
        pytest.param({"user_ids": [{"id": "u1"}]}, id="object"),
        pytest.param({"user_ids": [["u1"]]}, id="nested-list"),
        pytest.param({"user_ids": ["a" * 256]}, id="over-255-chars"),
        pytest.param({"user_ids": "u1"}, id="not-a-list"),
        pytest.param({}, id="missing-key"),
    ],
)
def test_api_batch_rejects_malformed_user_ids(client, payload):
    resp = client.post("/features/batch", json=payload)
    assert resp.status_code == 422, resp.text
    errors = resp.json()["detail"]
    assert errors and list(errors[0]["loc"][:2]) == ["body", "user_ids"]


def test_api_batch_whitespace_user_id_is_a_miss_not_an_error(client, redis_client):
    # Recorded behaviour (see PR notes): "" is rejected by the model, while a whitespace-only
    # id passes validation and is echoed back verbatim as a normal miss.
    resp = client.post("/features/batch", json={"user_ids": ["u  ", "   "]})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [row["user_id"] for row in body] == ["u  ", "   "]
    assert [row["found"] for row in body] == [False, False]
    assert redis_client.dbsize() == 0
