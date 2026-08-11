"""Shared HTTP behaviour for the plain-REST clients (Neynar, Clanker, Bankr, HL).

One rate limiter and one retry policy, so a 429 from any provider is handled
the same way instead of four slightly different ways.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class RateLimiter:
    """Simple thread-safe spacing limiter: at most `rps` requests per second."""

    def __init__(self, rps: float):
        self.min_interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed = now + self.min_interval


class HttpClient:
    """requests.Session plus rate limiting, retries and Retry-After handling."""

    def __init__(
        self,
        base_url: str,
        headers: dict | None = None,
        rps: float = 5.0,
        max_retries: int = 5,
        timeout: int = 60,
        name: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.name = name or self.base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.limiter = RateLimiter(rps)
        self.session = requests.Session()
        if headers:
            self.session.headers.update(headers)
        self.request_count = 0

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        delay = 1.0
        last_error = None
        for attempt in range(self.max_retries + 1):
            self.limiter.acquire()
            self.request_count += 1
            try:
                response = self.session.request(
                    method, url, timeout=self.timeout, **kwargs
                )
            except requests.RequestException as exc:
                last_error = exc
                logger.warning("%s %s %s failed (%s)", self.name, method, path, exc)
            else:
                if response.status_code < 400:
                    return response
                if response.status_code not in RETRYABLE_STATUS:
                    response.raise_for_status()
                last_error = requests.HTTPError(
                    f"{response.status_code}: {response.text[:200]}", response=response
                )
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        pass
                logger.warning(
                    "%s %s -> %s; retry %d/%d in %.1fs",
                    self.name,
                    path,
                    response.status_code,
                    attempt + 1,
                    self.max_retries,
                    delay,
                )
            if attempt < self.max_retries:
                time.sleep(delay)
                delay = min(delay * 2, 60)
        raise RuntimeError(
            f"{self.name} {method} {path} failed after {self.max_retries} retries: {last_error}"
        )

    def get_json(self, path: str, params: dict | None = None) -> dict:
        return self.request("GET", path, params=params).json()

    def post_json(self, path: str, payload: dict) -> dict:
        return self.request("POST", path, json=payload).json()
