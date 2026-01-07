from __future__ import annotations

import re
import pandas as pd

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


_LEVEL_RE = re.compile(r"^(bid|ask)(\d+)_(px|sz)$")

def orderbook_flat_to_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert flat orderbook columns like:
        bid1_px, bid1_sz, ask2_px, ask2_sz
    into a MultiIndex with levels:
        (level, side, field)

    Non-orderbook columns (t_ms, t_utc, mid, spread, etc.)
    are left untouched as flat columns.
    """
    
    new_cols = []
    for col in df.columns:
        m = _LEVEL_RE.match(col)
        if m:
            side, level, field = m.groups()
            new_cols.append((int(level), side, field))
        else:
            new_cols.append(col)

    # Build MultiIndex only if we actually matched something
    if any(isinstance(c, tuple) for c in new_cols):
        df = df.copy()
        df.columns = pd.Index(new_cols, dtype=object)
        df.columns = pd.MultiIndex.from_tuples(
            [
                c if isinstance(c, tuple) else ("", c, "")
                for c in df.columns
            ],
            names=["level", "side", "field"],
        )
    return df

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

    def _normalize_book(self, snap: Mapping[str, Any]) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        """
        Canonical representation:
            bids: [(px, sz)] sorted by px DESC
            asks: [(px, sz)] sorted by px ASC
        """
        raw_bids, raw_asks = self._raw_book_sides(snap)

        bids = self._coerce_levels(raw_bids)
        asks = self._coerce_levels(raw_asks)

        bids = self._aggregate_by_price(bids)
        asks = self._aggregate_by_price(asks)

        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])

        return bids, asks

    def _raw_book_sides(self, snap: Mapping[str, Any]) -> tuple[list[Any], list[Any]]:
        """
        Return raw (bids, asks) lists from a snapshot, without assuming ordering.
        Venue-specific shape handling lives here.
        """
        venue = (snap.get("venue") or getattr(self.instrument, "venue", None) or "").lower()

        if venue == "polymarket":
            ob = snap.get("orderbook") or {}
            bids = ob.get("bids") or []
            asks = ob.get("asks") or []
            return list(bids), list(asks)

        # default: limitless-like shape
        bids = snap.get("bids") or []
        asks = snap.get("asks") or []
        return list(bids), list(asks)

    @staticmethod
    def _aggregate_by_price(levels: list[tuple[float, float]]) -> list[tuple[float, float]]:
        """
        If the same price appears multiple times, sum sizes.
        Keeps one level per price.
        """
        if not levels:
            return levels
        agg: dict[float, float] = {}
        for px, sz in levels:
            agg[px] = agg.get(px, 0.0) + sz
        return [(px, sz) for px, sz in agg.items()]

    @staticmethod
    def _coerce_levels(levels: Sequence[Any]) -> list[tuple[float, float]]:
        """
        Convert a list of {price, size} dicts (or tuples) into [(px, sz), ...] floats.
        Malformed entries are skipped. No sorting here.
        """
        out: list[tuple[float, float]] = []
        for lvl in levels:
            try:
                if isinstance(lvl, dict):
                    px = float(lvl.get("price"))
                    sz = float(lvl.get("size"))
                else:
                    # allow (px, sz) tuples if they ever appear
                    px = float(lvl[0])
                    sz = float(lvl[1])
            except Exception:
                continue

            # Skip nonsensical levels; allow 0 price if it ever happens, but not negative.
            if px < 0 or sz <= 0:
                continue

            out.append((px, sz))
        return out

    def levels_df(self, n_levels: int = 1, add_utc=True, multi=False) -> "pd.DataFrame":
        """
        Return a wide, timeseries-friendly DataFrame:
        - one row per snapshot
        - levels as columns (bid{i}_px, bid{i}_sz, ask{i}_px, ask{i}_sz)
        - t_ms based on the history's time_field semantics (effective timestamp)
        """
        import pandas as pd

        if n_levels < 1:
            raise ValueError("n_levels must be >= 1")

        rows: list[dict[str, object]] = []

        for snap in self.snapshots:
            bids, asks = self._normalize_book(snap)

            row: dict[str, object] = {
                "t_ms": effective_ts_ms(snap,
                                        time_field=self.time_field,
                                        fallback_field=self.fallback_time_field),
                "n_bid_levels": len(bids),
                "n_ask_levels": len(asks),
            }

            # observation skew between collector time and venue "as-of" time
            ts_ms = snap.get("ts_ms")
            ob_ts_ms = snap.get("ob_ts_ms")

            if ts_ms is not None and ob_ts_ms is not None:
                try:
                    row["ts_ob_lag"] = int(ts_ms) - int(ob_ts_ms)
                except Exception:
                    row["ts_ob_lag"] = None
            else:
                row["ts_ob_lag"] = None


            for i in range(1, n_levels + 1):
                # bids are best->worst
                if len(bids) >= i:
                    px, sz = bids[i - 1]
                    row[f"bid{i}_px"] = px
                    row[f"bid{i}_sz"] = sz
                else:
                    row[f"bid{i}_px"] = None
                    row[f"bid{i}_sz"] = None

                # asks are best->worst (lowest first)
                if len(asks) >= i:
                    px, sz = asks[i - 1]
                    row[f"ask{i}_px"] = px
                    row[f"ask{i}_sz"] = sz
                else:
                    row[f"ask{i}_px"] = None
                    row[f"ask{i}_sz"] = None

            # After populating bid1/ask1 columns in `row`:
            bpx = row.get("bid1_px")
            bsz = row.get("bid1_sz")
            apx = row.get("ask1_px")
            asz = row.get("ask1_sz")

            if bpx is None or apx is None:
                row["mid"] = None
                row["spread"] = None
                row["micro"] = None
            else:
                bpx_f = float(bpx)
                apx_f = float(apx)
                row["mid"] = 0.5 * (bpx_f + apx_f)
                row["spread"] = apx_f - bpx_f

                if bsz is None or asz is None:
                    row["micro"] = None
                else:
                    bsz_f = float(bsz)
                    asz_f = float(asz)
                    denom = bsz_f + asz_f
                    row["micro"] = None if denom <= 0 else (bpx_f * asz_f + apx_f * bsz_f) / denom


            rows.append(row)

        df = pd.DataFrame(rows).sort_values("t_ms", kind="mergesort").reset_index(drop=True)

        # Optional...add UTC date
        if add_utc:
            df["t_utc"] = (pd.to_datetime(df["t_ms"], unit="ms", utc=True).dt.tz_convert(None))
            df = df.set_index('t_utc')

        # Optional... convert columsn to multi index L1, L2 etc.
        if multi:
            df = orderbook_flat_to_multiindex(df)

        # return 
        return df





