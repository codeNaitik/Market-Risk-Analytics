"""Shared HTTP session with retries, backoff and a polite rate limiter."""

from __future__ import annotations

import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RateLimiter:
    """Allow at most `rate` calls per second across threads (SEC asks for <= 10/s)."""

    def __init__(self, rate: float):
        self.min_interval = 1.0 / rate
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            delay = self._last + self.min_interval - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._last = time.monotonic()


def build_session(user_agent: str | None = None) -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.0,                      # 1s, 2s, 4s, 8s ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=10))
    if user_agent:
        session.headers["User-Agent"] = user_agent
    session.headers["Accept-Encoding"] = "gzip, deflate"
    return session
