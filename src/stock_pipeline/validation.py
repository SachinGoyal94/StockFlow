"""Parse and validate Finnhub /quote responses.

A /quote reply looks like:

    {"c": 189.84, "d": -1.31, "dp": -0.6851, "h": 191.9, "l": 189.2,
     "o": 190.79, "pc": 191.15, "t": 1725825600}

Two Finnhub quirks shape the checks below:
- unknown symbols get HTTP 200 with all fields zero, not an error status
- some fields can be null outside trading hours
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Finnhub field code -> column name (used in messages and the Quote fields).
_FIELDS = {
    "c": "current",
    "d": "change",
    "dp": "percent_change",
    "h": "high",
    "l": "low",
    "o": "open",
    "pc": "prev_close",
}

# Has to be all-zero/absent to match the "unknown symbol" response.
_PRICE_CODES = ("c", "h", "l", "o", "pc")


class InvalidQuoteError(Exception):
    """A quote response could not be turned into a usable Quote."""


@dataclass(frozen=True)
class Quote:
    """One validated quote for one symbol, ready to be upserted."""

    symbol: str
    current: float
    change: float
    percent_change: float
    high: float | None
    low: float | None
    open: float | None
    prev_close: float | None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_quote(symbol: str, payload: Mapping[str, Any]) -> Quote:
    """Build a Quote from one /quote payload.

    Raises InvalidQuoteError if the payload is not a JSON object, is the
    all-zero "unknown symbol" response, or has no current price. Any other
    missing field is kept as None and ends up NULL in the database.
    """
    if not isinstance(payload, Mapping):
        raise InvalidQuoteError(
            f"{symbol}: response is not a JSON object "
            f"(got {type(payload).__name__})"
        )

    values = {code: _to_float(payload.get(code)) for code in _FIELDS}

    if all(values[code] in (None, 0.0) for code in _PRICE_CODES):
        raise InvalidQuoteError(
            f"{symbol}: no market data available (all-zero response), "
            f"check the symbol"
        )

    if values["c"] is None:
        raise InvalidQuoteError(f"{symbol}: missing current price ('c')")

    missing = [name for code, name in _FIELDS.items() if values[code] is None]
    if missing:
        logger.warning(
            "%s: partial quote, storing NULLs for: %s",
            symbol,
            ", ".join(missing),
        )

    # d/dp are deltas, so a missing value becomes 0.0 instead of NULL
    # (also keeps the "%+.2f%%" log line in fetch_quotes numeric).
    return Quote(
        symbol=symbol,
        current=values["c"],
        change=values["d"] or 0.0,
        percent_change=values["dp"] or 0.0,
        high=values["h"],
        low=values["l"],
        open=values["o"],
        prev_close=values["pc"],
    )
