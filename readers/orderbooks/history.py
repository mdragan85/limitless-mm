from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from readers.orderbooks.time import effective_ts_ms
from readers.market_catalog.catalog import InstrumentMeta
from readers.orderbooks.reader import OrderbookReader

from datetime import datetime, timezone, date
from pathlib import Path

from config.settings import settings


def _ms_to_utc_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()

def _dates_between_utc(d0: date, d1: date) -> list[str]:
    if d1 < d0:
        d0, d1 = d1, d0
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur = cur.fromordinal(cur.toordinal() + 1)
    return out

@dataclass
class OrderbookHistory:
    """
    In-memory orderbook evolution for ONE instrument, plus its immutable metadata.

    Source of truth:
      - instrument: InstrumentMeta (metadata)
      - snapshots: list of raw JSONL dict records (orderbook snapshots)
    """
    instrument: InstrumentMeta
    snapshots: List[Dict[str, Any]]

    time_field: str = "ts_ms"
    fallback_time_field: str = "ts_ms"

    refresh_overlap_ms: int = 0
    _last_effective_ts_ms: Optional[int] = None

    @classmethod
    def from_instrument(
        cls,
        instrument: InstrumentMeta,
        *,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        input_dir: Optional[Path] = None,
        time_field: str = "ts_ms",
        fallback_time_field: str = "ts_ms",
        refresh_overlap_ms: Optional[int] = None,
    ) -> "OrderbookHistory":
        """
        Construct history by reading orderbook JSONL logs for one instrument.

        Defaults:
        - input_dir: settings.INPUT_DIR
        - date partitions: inferred from (start_dt/end_dt) if provided,
            else from instrument.first_seen_ms .. instrument.last_seen_ms (UTC dates)
        - time_field: "ts_ms" (collector time); can be "ob_ts_ms" for polymarket
        - refresh_overlap_ms: defaults to 10s when using "ob_ts_ms", else 0

        Notes:
        - Reader window filtering uses ts_ms (collector time). History ordering/windowing
            uses time_field (ts_ms or ob_ts_ms).
        """

        base_dir = input_dir or settings.INPUT_DIR
        reader = OrderbookReader(input_dir=Path(base_dir))

        # Determine date partitions to scan
        if start_dt is not None or end_dt is not None:
            # Use provided datetimes (treat naive as UTC to avoid surprises)
            s = start_dt or end_dt
            e = end_dt or start_dt
            if s is None or e is None:
                raise ValueError("start_dt/end_dt logic error")
            if s.tzinfo is None:
                s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None:
                e = e.replace(tzinfo=timezone.utc)
            dates = _dates_between_utc(s.date(), e.date())
            start_ms = int(s.timestamp() * 1000)
            end_ms = int(e.timestamp() * 1000)
        else:
            # Infer from metadata (UTC dates)
            d0 = _ms_to_utc_date(instrument.first_seen_ms)
            d1 = _ms_to_utc_date(instrument.last_seen_ms)
            dates = _dates_between_utc(d0, d1)
            start_ms = None
            end_ms = None

        # Reasonable default overlap when using venue-time clocks
        if refresh_overlap_ms is None:
            refresh_overlap_ms = 10_000 if time_field == "ob_ts_ms" else 0

        snaps = list(
            reader.iter_snapshots(
                [instrument.instrument_id],
                dates=dates,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        )

        hist = cls(
            instrument=instrument,
            snapshots=snaps,
            time_field=time_field,
            fallback_time_field=fallback_time_field,
            refresh_overlap_ms=int(refresh_overlap_ms),
        )
        hist.sort_in_place()
        return hist

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
