"""Finnhub /quote client: fetch with timeout, retry transient errors, fail
fast on permanent ones (assignment's data-fetching script).

Free tier is 60 req/min, so batch callers pass a delay between requests
(config.inter_request_delay).
"""

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

from .validation import InvalidQuoteError, Quote, parse_quote

logger = logging.getLogger(__name__)

QUOTE_ENDPOINT = "https://finnhub.io/api/v1/quote"

# statuses worth retrying with backoff
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
# statuses that will never succeed on retry
_FATAL_STATUSES = frozenset({400, 401, 403})


class FinnhubError(Exception):
    """Finnhub returned an unrecoverable error response."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        super().__init__(f"Finnhub API error {status_code}: {detail}")


class FinnhubClient:
    """Client for the Finnhub quote endpoint."""

    def __init__(
        self,
        api_key: str,
        timeout: float = 10.0,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
    ) -> None:
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._session = requests.Session()

    @property
    def _key_redacted(self) -> str:
        """Everything but the last 4 chars masked, safe for log messages."""
        if len(self._api_key) <= 4:
            return "****"
        return "*" * (len(self._api_key) - 4) + self._api_key[-4:]

    def fetch_quote(self, symbol: str) -> Quote:
        """Fetch and validate one quote.

        Raises FinnhubError (permanent API error), InvalidQuoteError (symbol
        had no data), or requests.RequestException (retries exhausted).
        """
        payload = self._get_json(symbol)
        return parse_quote(symbol, payload)

    def fetch_quotes(
        self, symbols: list[str], inter_request_delay: float = 1.1
    ) -> Iterator[tuple[str, Quote | InvalidQuoteError]]:
        """Yield (symbol, result) per symbol. A symbol with no data yields
        its InvalidQuoteError instead of killing the batch; anything that
        retries can't fix (auth, network) still raises.
        """
        for index, symbol in enumerate(symbols):
            if index and inter_request_delay > 0:
                # keep under the 60 req/min free-tier ceiling
                time.sleep(inter_request_delay)
            try:
                yield symbol, self.fetch_quote(symbol)
            except InvalidQuoteError as exc:
                yield symbol, exc

    def _get_json(self, symbol: str) -> Any:
        """GET /quote for one symbol with retries. Retries cover network
        errors and 429/5xx; permanent errors raise immediately.
        """
        last_error: Exception | None = None

        for attempt in range(1, self._max_retries + 2):
            try:
                response = self._session.get(
                    QUOTE_ENDPOINT,
                    params={"symbol": symbol, "token": self._api_key},
                    timeout=self._timeout,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                logger.warning(
                    "Network error fetching %s (attempt %d/%d): %s",
                    symbol,
                    attempt,
                    self._max_retries + 1,
                    exc,
                )
                self._sleep_backoff(attempt, retry_after=None)
                continue

            if response.status_code in _FATAL_STATUSES:
                # 401/403 almost always means a missing/expired API key
                raise FinnhubError(
                    response.status_code,
                    f"request rejected for symbol {symbol!r}; if this is "
                    f"401/403, check FINNHUB_API_KEY (ends ...{self._key_redacted})",
                )

            if response.status_code in _RETRYABLE_STATUSES:
                last_error = FinnhubError(
                    response.status_code,
                    f"transient error for symbol {symbol!r}",
                )
                retry_after = response.headers.get("Retry-After")
                logger.warning(
                    "HTTP %d for %s (attempt %d/%d)%s",
                    response.status_code,
                    symbol,
                    attempt,
                    self._max_retries + 1,
                    f" (Retry-After: {retry_after}s)" if retry_after else "",
                )
                self._sleep_backoff(attempt, retry_after)
                continue

            if response.status_code != 200:
                raise FinnhubError(
                    response.status_code,
                    f"unexpected status for symbol {symbol!r}",
                )

            try:
                return response.json()
            except ValueError as exc:  # body wasn't valid JSON
                last_error = exc
                logger.warning(
                    "Malformed JSON for %s (attempt %d/%d)",
                    symbol,
                    attempt,
                    self._max_retries + 1,
                )
                self._sleep_backoff(attempt, retry_after=None)
                continue

        # every attempt failed; raise the last cause
        raise last_error  # type: ignore[misc]

    def _sleep_backoff(self, attempt: int, retry_after: str | None) -> None:
        """Sleep before the next attempt: Retry-After if the server sent one,
        else exponential backoff (retry_backoff * 2^attempt)."""
        if retry_after:
            try:
                time.sleep(min(float(retry_after), 60.0))
                return
            except ValueError:
                pass  # non-numeric header, fall through to backoff
        time.sleep(self._retry_backoff * (2 ** (attempt - 1)))
