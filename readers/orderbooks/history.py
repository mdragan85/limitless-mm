from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from readers.orderbooks.time import effective_ts_ms


@dataclass
class OrderbookHistory:
    """
    In-memory history over a bounded time window for ONE instrument.

    Key properties:
    - Built from logged snapshots (JSONL records).
    - Uses a configurable timestamp field for ordering/windowing (default "ts_ms").
    - Supports fast refresh by reading only new snapshots since last seen time.

    This is intentionally not an index and not a database.
    """
    instrument_id: str
    snapshots: List[Dict[str, Any]]

    time_field: str = "ts_ms"
    fallback_time_field: str = "ts_ms"

    # Refresh behavior
    refresh_overlap_ms: int = 0   # use small overlap for venue-time clocks if needed
    _last_effective_ts_ms: Optional[int] = None

    def __post_init__(self) -> None:
        # Normalize: compute watermark if not supplied
        if self._last_effective_ts_ms is None and self.snapshots:
            self._last_effective_ts_ms = max(
                effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
                for s in self.snapshots
            )

    @property
    def last_effective_ts_ms(self) -> Optional[int]:
        return self._last_effective_ts_ms

    def sort_in_place(self) -> None:
        """
        Sort snapshots by effective timestamp (and then by ts_ms as tie-breaker if present).
        """
        def key(s: Mapping[str, Any]):
            t = effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
            # tie-breaker: collector ts_ms if available (keeps ordering stable)
            t2 = int(s.get("ts_ms") or t)
            return (t, t2)

        self.snapshots.sort(key=key)

    def trim_to_window(self, *, start_ms: Optional[int] = None, end_ms: Optional[int] = None) -> None:
        """
        Trim snapshots to an effective timestamp window [start_ms, end_ms].

        Note: windowing uses `time_field` (with fallback), not necessarily ts_ms.
        """
        if start_ms is None and end_ms is None:
            return

        out: List[Dict[str, Any]] = []
        for s in self.snapshots:
            t = effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
            if start_ms is not None and t < int(start_ms):
                continue
            if end_ms is not None and t > int(end_ms):
                continue
            out.append(s)

        self.snapshots = out

        # Recompute watermark after trim
        if self.snapshots:
            self._last_effective_ts_ms = max(
                effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
                for s in self.snapshots
            )
        else:
            self._last_effective_ts_ms = None

    def refresh_from(
                    self,
                    iter_new_snapshots: Iterable[Dict[str, Any]],
                ) -> int:
        """
        Append new snapshots and update watermark.

        Dedupe strategy:
        - Key = (effective_ts_ms, orderbook_hash_or_None, ts_ms)
        - `effective_ts_ms` is computed using `self.time_field` with fallback to `self.fallback_time_field`
        - For Polymarket, `orderbook.hash` makes overlap-based refresh safe.
        - For venues without a hash (e.g., Limitless), hash is None and behavior matches prior logic.

        Args:
            iter_new_snapshots:
                Iterable of raw snapshot dicts (already filtered by instrument_id).

        Returns:
            Number of records appended (post-dedupe).
        """
        seen = set()

        # Seed seen-set from existing snapshots
        for s in self.snapshots:
            t = effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
            ob = s.get("orderbook")
            h = ob.get("hash") if isinstance(ob, dict) else None
            t2 = int(s.get("ts_ms") or t)
            seen.add((t, h, t2))

        added = 0

        # Append only genuinely new snapshots
        for s in iter_new_snapshots:
            t = effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
            ob = s.get("orderbook")
            h = ob.get("hash") if isinstance(ob, dict) else None
            t2 = int(s.get("ts_ms") or t)

            k = (t, h, t2)
            if k in seen:
                continue

            self.snapshots.append(s)
            seen.add(k)
            added += 1

        # Update watermark
        if added:
            self._last_effective_ts_ms = max(
                effective_ts_ms(s, time_field=self.time_field, fallback_field=self.fallback_time_field)
                for s in self.snapshots
            )

        return added


    def to_dataframe(self):
        """
        Notebook convenience. Returns a pandas DataFrame of raw snapshot fields.

        This stays simple: it does not explode bids/asks into rows.
        You can add derived series (mid/spread/best_bid/best_ask) later.
        """
        import pandas as pd
        df = pd.DataFrame(self.snapshots)
        # Provide a consistent 't_ms' column = effective timestamp used
        if not df.empty:
            df["t_ms"] = df.apply(
                lambda r: effective_ts_ms(r, time_field=self.time_field, fallback_field=self.fallback_time_field),
                axis=1,
            )
        return df
