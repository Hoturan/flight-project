"""HTTP client for the OpenSky Network REST API.

Provides a thin wrapper around the `states/all` endpoint that returns
parsed JSON.  Designed to be called from within a Spark `foreachBatch`
micro-batch for Structured Streaming ingestion.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

OPENSKY_ENDPOINT = "https://opensky-network.org/api/states/all"

DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_RETRIES = 3
RETRY_BACKOFF = 2  # seconds

logger = logging.getLogger(__name__)


class OpenSkyClient:
    """Minimal client for the OpenSky `states/all` endpoint."""

    def __init__(
        self,
        endpoint: str = OPENSKY_ENDPOINT,
        timeout: int = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        bearer_token: Optional[str] = None,
    ) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self.retries = retries
        self.bearer_token = bearer_token

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    def fetch_states(self) -> Dict[str, Any]:
        """Fetch the current state-vector snapshot.

        Returns:
            Dict with keys ``time`` (int) and ``states`` (list of lists).

        Raises:
            RuntimeError: if all retry attempts fail.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(
                    self.endpoint,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, json.JSONDecodeError) as exc:
                last_exc = exc
                logger.warning(
                    "OpenSky fetch attempt %d/%d failed: %s",
                    attempt,
                    self.retries,
                    exc,
                )
                if attempt < self.retries:
                    time.sleep(RETRY_BACKOFF * attempt)

        raise RuntimeError(f"OpenSky API unreachable after {self.retries} attempts: {last_exc}")

    @staticmethod
    def flatten_states(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert the raw array-of-arrays response into a list of dicts.

        Args:
            payload: The JSON dict returned by :meth:`fetch_states`.

        Returns:
            List of dictionaries keyed by :data:`OPENSKY_STATE_COLUMNS`.
        """
        from src.utils.schema import OPENSKY_STATE_COLUMNS

        states: List[Dict[str, Any]] = []
        for row in payload.get("states") or []:
            states.append(dict(zip(OPENSKY_STATE_COLUMNS, row)))
        return states