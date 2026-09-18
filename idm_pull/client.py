"""Thread-safe ForgeRock/PingIDM REST client with retry and throttling.

Built for the Jupyter extraction workflow: a single shared session with
connection pooling, urllib3 retry/backoff on 429/5xx, and a simple
rate limiter so concurrent threads don't trip the server's throttle.
"""

from __future__ import annotations

import logging
import threading
import time
import urllib.parse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

log = logging.getLogger(__name__)


class IdmClient:
    """Concurrent-safe IDM client.

    Parameters
    ----------
    base_url:
        e.g. ``https://idm.example.com:8443/openidm``.
    username, password:
        Passed as HTTP basic auth on every request (thread-local sessions
        are not needed — ``requests.Session`` is safe for concurrent GETs).
    rate_limit_per_second:
        Max requests/sec across ALL threads (token-bucket style).
    max_retries, backoff_factor:
        Retry 429/502/503/504 with exponential backoff.
    page_size:
        ``_pageSize`` for paged queries.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        rate_limit_per_second: float = 10.0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        page_size: int = 100,
        timeout: float = 30.0,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.page_size = page_size
        self.timeout = timeout
        self._verify_ssl = verify_ssl

        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry, pool_connections=20, pool_maxsize=20
        )
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # --- rate limiter (shared by all threads) ---
        self._min_interval = 1.0 / max(rate_limit_per_second, 0.01)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def _throttle(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
            self._next_allowed = max(now, self._next_allowed) + self._min_interval

    def _url(self, path: str, **params: object) -> str:
        """Build a URL with urllib.parse so special chars stay encoded."""
        base = f"{self.base_url}/{path.lstrip('/')}"
        if not params:
            return base
        return base + "?" + urllib.parse.urlencode(params, doseq=True)

    def _get(self, path: str, **params: object) -> requests.Response:
        self._throttle()
        resp = self.session.get(
            self._url(path, **params),
            auth=self.auth,
            timeout=self.timeout,
            verify=self._session_verify(),
        )
        return resp

    def _session_verify(self):  # kept separate for test/mocking ease
        return getattr(self, "_verify_ssl", True)

    # ------------------------------------------------------------------ API
    def read(self, resource: str, object_id: str, fields=None) -> dict:
        """Read one object: ``managed/user/<id>?_fields=a,b``."""
        params = {}
        if fields:
            params["_fields"] = ",".join(fields)
        resp = self._get(f"{resource}/{urllib.parse.quote(str(object_id), safe='')}", **params)
        resp.raise_for_status()
        return resp.json()

    def query_all(self, resource: str, query_filter: str = "true", fields=None) -> list:
        """Page through a query until the server stops returning results."""
        out, paged = [], True
        params = {"_queryFilter": query_filter, "_pageSize": self.page_size}
        if fields:
            params["_fields"] = ",".join(fields)
        while paged:
            resp = self._get(resource, **params)
            resp.raise_for_status()
            body = resp.json()
            out.extend(body.get("result", []))
            cookie = body.get("pagedResultsCookie")
            if cookie:
                params["pagedResultsCookie"] = cookie
            else:
                paged = False
        return out
