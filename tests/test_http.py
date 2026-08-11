"""lib.http — one rate limiter and one retry policy for all four REST clients.

The rate-limiter test uses real (tiny) sleeps because spacing is the whole
point; every retry test replaces `time.sleep` and asserts on the delay that
*would* have been taken, which is where Retry-After handling lives.
"""

from __future__ import annotations

import time

import pytest
import requests

from lib.http import HttpClient, RateLimiter

BASE = "https://api.example.test"


def client(**kwargs) -> HttpClient:
    kwargs.setdefault("rps", 0)  # unlimited unless a test cares
    kwargs.setdefault("name", "example")
    return HttpClient(BASE, **kwargs)


# --- rate limiting -------------------------------------------------------


def test_rate_limiter_spaces_successive_acquires():
    limiter = RateLimiter(rps=40)  # 25ms apart

    started = time.monotonic()
    for _ in range(4):
        limiter.acquire()
    elapsed = time.monotonic() - started

    # The first acquire is free, so four calls cost three intervals.
    assert elapsed >= 0.075
    assert elapsed < 1.0


def test_rate_limiter_of_zero_rps_never_waits():
    limiter = RateLimiter(rps=0)

    started = time.monotonic()
    for _ in range(50):
        limiter.acquire()

    assert limiter.min_interval == 0.0
    assert time.monotonic() - started < 0.05


def test_rate_limiter_lets_the_first_acquire_through_free(recorded_sleep):
    limiter = RateLimiter(rps=0.5)  # 2s apart — the Hyperliquid-shaped budget

    limiter.acquire()
    assert recorded_sleep == []

    limiter.acquire()
    assert len(recorded_sleep) == 1
    assert 1.9 < recorded_sleep[0] <= 2.0


# --- URL building --------------------------------------------------------


def test_relative_paths_are_joined_to_the_base_and_absolute_ones_are_not(requests_mock):
    requests_mock.get(f"{BASE}/v2/thing", json={"ok": 1})
    requests_mock.get("https://elsewhere.test/info", json={"ok": 2})

    api = client()

    assert api.get_json("/v2/thing") == {"ok": 1}
    assert api.get_json("v2/thing") == {"ok": 1}
    # Hyperliquid passes its full endpoint through unchanged.
    assert api.get_json("https://elsewhere.test/info") == {"ok": 2}


def test_headers_and_params_are_sent(requests_mock):
    requests_mock.get(f"{BASE}/v2/user", json={"users": []})

    api = client(headers={"x-api-key": "secret"})
    api.get_json("/v2/user", params={"fids": "1,2"})

    request = requests_mock.request_history[-1]
    assert request.headers["x-api-key"] == "secret"
    assert request.qs["fids"] == ["1,2"]


def test_post_json_sends_the_payload(requests_mock):
    requests_mock.post(f"{BASE}/info", json={"cumVlm": "1"})

    assert client().post_json("/info", {"type": "userRateLimit"}) == {"cumVlm": "1"}
    assert requests_mock.request_history[-1].json() == {"type": "userRateLimit"}


def test_request_count_tracks_every_attempt(requests_mock, recorded_sleep):
    requests_mock.get(
        f"{BASE}/v2/thing",
        [{"status_code": 429, "text": "slow"}, {"status_code": 200, "json": {}}],
    )

    api = client()
    api.get_json("/v2/thing")

    # Two attempts, not one: this counter is what the pipelines report as spend.
    assert api.request_count == 2


# --- retries -------------------------------------------------------------


def test_retry_after_is_honoured_on_a_429(requests_mock, recorded_sleep):
    requests_mock.get(
        f"{BASE}/v2/thing",
        [
            {"status_code": 429, "headers": {"Retry-After": "12"}, "text": "slow down"},
            {"status_code": 200, "json": {"ok": True}},
        ],
    )

    assert client().get_json("/v2/thing") == {"ok": True}
    assert recorded_sleep == [12.0]


def test_a_junk_retry_after_falls_back_to_the_backoff(requests_mock, recorded_sleep):
    requests_mock.get(
        f"{BASE}/v2/thing",
        [
            {"status_code": 429, "headers": {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, "text": ""},
            {"status_code": 200, "json": {"ok": True}},
        ],
    )

    client().get_json("/v2/thing")

    assert recorded_sleep == [1.0]


def test_backoff_doubles_and_is_capped(requests_mock, recorded_sleep):
    responses = [{"status_code": 503, "text": "down"}] * 8 + [{"status_code": 200, "json": {}}]
    requests_mock.get(f"{BASE}/v2/thing", responses)

    client(max_retries=8).get_json("/v2/thing")

    assert recorded_sleep == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]


@pytest.mark.parametrize("status", [408, 425, 429, 500, 502, 503, 504])
def test_every_retryable_status_is_retried(status, requests_mock, recorded_sleep):
    requests_mock.get(
        f"{BASE}/v2/thing",
        [{"status_code": status, "text": "transient"}, {"status_code": 200, "json": {"ok": 1}}],
    )

    assert client().get_json("/v2/thing") == {"ok": 1}


@pytest.mark.parametrize("status", [400, 401, 403, 404, 414, 422])
def test_a_non_retryable_status_raises_at_once(status, requests_mock, recorded_sleep):
    requests_mock.get(f"{BASE}/v2/thing", status_code=status, text="nope")

    with pytest.raises(requests.HTTPError):
        client().get_json("/v2/thing")

    assert requests_mock.call_count == 1
    assert recorded_sleep == []


def test_exhausted_retries_raise_a_runtime_error_naming_the_client(requests_mock, recorded_sleep):
    requests_mock.get(f"{BASE}/v2/thing", status_code=503, text="still down")

    with pytest.raises(RuntimeError, match="neynar GET /v2/thing failed after 2 retries"):
        client(name="neynar", max_retries=2).get_json("/v2/thing")

    assert requests_mock.call_count == 3
    assert recorded_sleep == [1.0, 2.0]


def test_connection_errors_are_retried_then_surfaced(requests_mock, recorded_sleep):
    requests_mock.get(
        f"{BASE}/v2/thing",
        [
            {"exc": requests.exceptions.ConnectionError},
            {"status_code": 200, "json": {"ok": 1}},
        ],
    )

    assert client().get_json("/v2/thing") == {"ok": 1}


def test_a_persistent_connection_error_raises_runtime_error(requests_mock, recorded_sleep):
    requests_mock.get(f"{BASE}/v2/thing", exc=requests.exceptions.ConnectTimeout)

    with pytest.raises(RuntimeError, match="failed after 1 retries"):
        client(max_retries=1).get_json("/v2/thing")
