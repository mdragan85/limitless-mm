from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def parse_iso_to_ms(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def pick_seen_ms(*candidates: Optional[int]) -> int:
    vals = [c for c in candidates if c is not None]
    return max(vals) if vals else 0


def require(rec: Dict[str, Any], keys: List[str], venue: str) -> None:
    missing = [k for k in keys if k not in rec or rec[k] is None]
    if missing:
        raise ValueError(
            f"{venue} record missing required keys={missing}. present_keys={sorted(rec.keys())}"
        )
