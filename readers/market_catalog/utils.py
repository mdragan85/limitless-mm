# readers/market_catalog/utils.py
"""
Small, boring helpers shared across parsers and catalog.

Design goals:
- Keep these functions *pure* and dependency-free.
- Helpers here should not import venue-specific code.
- Prefer "fail loud" when required fields are missing (to avoid silent corruption).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def parse_iso_to_ms(s: Optional[str]) -> Optional[int]:
    """
    Parse ISO-8601 timestamps to epoch milliseconds.

    Supports common forms seen in APIs/logs:
      - "2025-12-31T21:02:16.178Z"
      - "2025-12-31T21:02:16.178+00:00"
      - naive ISO (treated as UTC)

    Returns:
      epoch ms, or None if parsing fails.

    Note:
    We intentionally return None on failure and let callers decide whether that
    should degrade gracefully (e.g., seen_ms becomes 0) or hard-fail.
    """
    if not s:
        return None
    try:
        # Handle 'Z' suffix explicitly
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def pick_seen_ms(*candidates: Optional[int]) -> int:
    """
    Choose a single 'seen_ms' from multiple timestamp candidates.

    We use max() because "updatedAt" is generally more useful than "createdAt"
    for representing the freshest sighting in a log line.

    Returns 0 if nothing is available.
    """
    vals = [c for c in candidates if c is not None]
    return max(vals) if vals else 0


def require(rec: Dict[str, Any], keys: List[str], venue: str) -> None:
    """
    Fail fast when required fields are missing.

    Why this exists:
    - Log formats can drift over time.
    - Market logs may contain multiple record types (instrument-capable vs
      market-only summaries).
    - Silent ingestion of incomplete records will poison analysis.

    Venue parsers may choose to *skip* a record type before calling require().
    """
    missing = [k for k in keys if k not in rec or rec[k] is None]
    if missing:
        raise ValueError(
            f"{venue} record missing required keys={missing}. "
            f"present_keys={sorted(rec.keys())}"
        )

def pretty_dataclass(obj) -> str:
    """
    Pretty-print a frozen dataclass with aligned ':' for notebook / REPL use.

    Intended for __repr__ only (human-facing, non-stable).
    """
    cls = obj.__class__.__name__
    items = vars(obj)

    if not items:
        return f"{cls}()"

    key_width = max(len(k) for k in items.keys())

    lines = [f"{cls}("]
    for k, v in items.items():
        lines.append(f"  {k.ljust(key_width)} : {v!r}")
    lines.append(")")

    return "\n".join(lines)