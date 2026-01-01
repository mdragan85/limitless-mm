from __future__ import annotations
from typing import Any, Mapping, Optional

def effective_ts_ms(
    rec: Mapping[str, Any],
    *,
    time_field: str = "ts_ms",
    fallback_field: str = "ts_ms",
) -> int:
    """
    Return the timestamp used for ordering/windowing a snapshot.

    Args:
        rec: Raw snapshot record (dict-like).
        time_field: Preferred timestamp field (e.g. "ts_ms" or "ob_ts_ms").
        fallback_field: Fallback if time_field is missing/null.

    Returns:
        int epoch-ms

    Raises:
        KeyError/ValueError if neither field exists or is not coercible.
    """
    v = rec.get(time_field)
    if v is None:
        v = rec.get(fallback_field)
    if v is None:
        raise KeyError(f"missing timestamp fields: {time_field!r} and {fallback_field!r}")
    try:
        return int(v)
    except Exception as e:
        raise ValueError(f"timestamp {time_field!r}/{fallback_field!r} not int-coercible: {v!r}") from e
