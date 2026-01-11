# readers/market_catalog/instrument_query.py

from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Sequence, Tuple, List, Dict, Literal, Union

from .catalog import MarketCatalog, InstrumentMeta

PerMarket = Literal["all", "one"]


def _norm_set(vals: Sequence[str]) -> set[str]:
    return {v.strip().upper() for v in vals if v and v.strip()}


def _safe_getattr(obj: Any, name: str) -> Any:
    return getattr(obj, name, None)


def _now_ms() -> int:
    """
    Current wall-clock time in epoch milliseconds.

    Kept local to this module because:
    - InstrumentQuery is notebook-facing and should be self-contained.
    - We want to inject `now_ms` for reproducible notebooks/tests.
    """
    import time
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class InstrumentQuery:
    """
    Thin, notebook-friendly query/view over MarketCatalog.instruments.

    - No persistence
    - No orderbook reading
    - Correctness-first selection helpers
    """
    _items: Tuple[InstrumentMeta, ...]

    @classmethod
    def from_catalog(cls, cat: MarketCatalog) -> "InstrumentQuery":
        items = tuple(cat.instruments.values())
        _validate_invariants(items)
        return cls(items)

    # -----------------
    # Filters (chainable)
    # -----------------

    def venues(self, *venues: str) -> "InstrumentQuery":
        vset = {v.strip().lower() for v in venues if v and v.strip()}
        if not vset:
            return self
        return InstrumentQuery(tuple(i for i in self._items if i.venue in vset))

    def is_active(self, enabled: bool = True, *, now_ms: Optional[int] = None) -> "InstrumentQuery":
        """
        Filter instruments by *inferred* activeness based on expiration time.

        Why inference:
        - Snapshots can be stale.
        - Notebooks / long-lived processes need time-correct answers.
        - Expiry is always known (your invariant), so this is deterministic.

        Args:
            enabled:
                - True  -> keep only instruments where expiration_ms > now_ms
                - False -> keep only instruments where expiration_ms <= now_ms
            now_ms:
                Epoch milliseconds. If None, uses current wall-clock time.
                Passing now_ms makes results reproducible in notebooks/tests.

        Returns:
            A new InstrumentQuery containing only matching instruments.
        """
        now = _now_ms() if now_ms is None else int(now_ms)

        if enabled:
            return InstrumentQuery(tuple(i for i in self._items if i.expiration_ms > now))
        return InstrumentQuery(tuple(i for i in self._items if i.expiration_ms <= now))

    def active_only(self, enabled: bool = True) -> "InstrumentQuery":
        """
        Backward-compatible alias for `is_active(enabled=True)`.

        Note:
        This intentionally ignores InstrumentMeta.is_active and infers activeness
        from expiration_ms to avoid snapshot staleness.
        """
        return self.is_active(enabled=enabled)

    def expiry_between(self, min_ms: Optional[int] = None, max_ms: Optional[int] = None) -> "InstrumentQuery":
        def ok(i: InstrumentMeta) -> bool:
            x = i.expiration_ms or 0
            if x <= 0:
                return False  # unknown expiry excluded from expiry-window queries
            if min_ms is not None and x < min_ms:
                return False
            if max_ms is not None and x > max_ms:
                return False
            return True

        if min_ms is None and max_ms is None:
            return self
        return InstrumentQuery(tuple(i for i in self._items if ok(i)))

    def cadence_in(self, *cadences: str) -> "InstrumentQuery":
        cset = _norm_set(cadences)
        if not cset:
            return self
        return InstrumentQuery(tuple(i for i in self._items if (i.cadence or "").upper() in cset))

    def underlying_in(self, *underlyings: str) -> "InstrumentQuery":
        uset = _norm_set(underlyings)
        if not uset:
            return self
        return InstrumentQuery(tuple(i for i in self._items if (i.underlying or "").upper() in uset))

    def expires_before(
        self,
        cutoff_utc: Union[datetime, str],
    ):
        """
        Filter instruments expiring before a UTC cutoff.

        Parameters
        ----------
        cutoff_utc :
            - datetime (must be timezone-aware, UTC), or
            - ISO-8601 string interpreted as UTC (e.g. "2026-01-08T16:00:00Z")
        """
        if isinstance(cutoff_utc, str):
            # Accept ISO-8601 strings; force UTC
            dt = datetime.fromisoformat(cutoff_utc.replace("Z", "+00:00"))
        elif isinstance(cutoff_utc, datetime):
            dt = cutoff_utc
        else:
            raise TypeError("cutoff_utc must be datetime or ISO-8601 string")

        if dt.tzinfo is None:
            raise ValueError("cutoff_utc must be timezone-aware (UTC)")

        cutoff_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)

        return self.filter(lambda i: i.expiration_ms < cutoff_ms)

    def where(self, **attrs: Any) -> "InstrumentQuery":
        """
        Generic attribute filter:
        - tolerant: if attr missing/None -> excluded
        - supports exact match only (keep minimal; add predicates later if needed)
        """
        def ok(i: InstrumentMeta) -> bool:
            for k, v in attrs.items():
                got = _safe_getattr(i, k)
                if got is None:
                    return False
                if got != v:
                    return False
            return True

        if not attrs:
            return self
        return InstrumentQuery(tuple(i for i in self._items if ok(i)))

    def filter(self, fn: Callable[[InstrumentMeta], bool]) -> "InstrumentQuery":
        return InstrumentQuery(tuple(i for i in self._items if fn(i)))

    # -----------------
    # Selection
    # -----------------
    def _materialize_items(
        self,
        *,
        top_n: Optional[int] = None,
        sort_by: str = "expiration_ms",
        descending: bool = False,
        per_market: PerMarket = "all",
    ) -> List[InstrumentMeta]:
        items = list(self._items)

        def sort_key(i: InstrumentMeta):
            v = _safe_getattr(i, sort_by)
            if sort_by == "expiration_ms":
                return (int(v), i.instrument_id)
            return (v is None, v, i.instrument_id)

        if per_market == "one":
            chosen: Dict[Tuple[str, str], InstrumentMeta] = {}
            for i in items:
                k = (i.venue, i.market_id)
                prev = chosen.get(k)
                if prev is None or i.instrument_id < prev.instrument_id:
                    chosen[k] = i
            items = list(chosen.values())

        items.sort(key=sort_key, reverse=bool(descending))

        if top_n is not None:
            items = items[: max(0, int(top_n))]

        return items

    def select(
        self,
        *,
        top_n: Optional[int] = None,
        sort_by: str = "expiration_ms",
        descending: bool = False,
        per_market: PerMarket = "all",
        debug: bool = False,
        include_is_active: bool = True,
        now_ms: Optional[int] = None,
    ) -> Tuple[List[str], Optional[Any]]:

        # Use the shared materialization logic (dedupe + sort + slice)
        items = self._materialize_items(
            top_n=top_n,
            sort_by=sort_by,
            descending=descending,
            per_market=per_market,
        )

        ids = [i.instrument_id for i in items]

        dbg = None
        if debug:
            now = _now_ms() if now_ms is None else int(now_ms)
            dbg = []
            for i in items:
                row = {
                    "instrument_id": i.instrument_id,
                    "venue": i.venue,
                    "poll_key": i.poll_key,
                    "expiration_ms": i.expiration_ms,
                    "market_id": i.market_id,
                    "slug": i.slug,
                    "title": i.title,
                    "underlying": i.underlying,
                    "outcome": i.outcome,
                }
                if include_is_active:
                    row["is_active"] = i.expiration_ms > now
                dbg.append(row)

        return ids, dbg

    def items(
        self,
        *,
        top_n: Optional[int] = None,
        sort_by: str = "expiration_ms",
        descending: bool = False,
        per_market: PerMarket = "all",
    ) -> List[InstrumentMeta]:
        """
        Returns the selected InstrumentMeta objects (ordered, deduped, sliced).
        No filesystem I/O.
        """
        return self._materialize_items(
            top_n=top_n,
            sort_by=sort_by,
            descending=descending,
            per_market=per_market,
        )

    def df(
        self,
        *,
        top_n: Optional[int] = None,
        sort_by: str = "expiration_ms",
        descending: bool = False,
        per_market: PerMarket = "all",
        include_is_active: bool = True,
        now_ms: Optional[int] = None,
        view: str = "pretty",          # "pretty" | "raw"
        id_tail: int = 6,              # used only in pretty view
    ):
        """
        Notebook helper: returns a DataFrame view of the current selection.
        - view="pretty": human-friendly columns (default)
        - view="raw": include all debug columns from select()
        """

        _ids, rows = self.select(
            top_n=top_n,
            sort_by=sort_by,
            descending=descending,
            per_market=per_market,
            debug=True,
            include_is_active=include_is_active,
            now_ms=now_ms,
        )
        return self._rows_to_df(rows, view=view, id_tail=id_tail)

    def df_and_items(
        self,
        *,
        top_n: Optional[int] = None,
        sort_by: str = "expiration_ms",
        descending: bool = False,
        per_market: PerMarket = "all",
        include_is_active: bool = True,
        now_ms: Optional[int] = None,
        view: str = "pretty",
        id_tail: int = 6,
    ):
        """
        Notebook helper: returns (df, items) in ONE pass.
        - df is formatted like df()
        - items is the ordered List[InstrumentMeta] selection for downstream readers
        """
        items = self._materialize_items(
            top_n=top_n,
            sort_by=sort_by,
            descending=descending,
            per_market=per_market,
        )

        now = _now_ms() if now_ms is None else int(now_ms)
        rows = []
        for i in items:
            row = {
                "instrument_id": i.instrument_id,
                "venue": i.venue,
                "poll_key": i.poll_key,
                "expiration_ms": i.expiration_ms,
                "market_id": i.market_id,
                "slug": i.slug,
                "title": i.title,
                "underlying": i.underlying,
                "outcome": i.outcome,
            }
            if include_is_active:
                row["is_active"] = i.expiration_ms > now
            rows.append(row)

        df = self._rows_to_df(rows, view=view, id_tail=id_tail)
        return df, items

    def _rows_to_df(self, rows, *, view: str, id_tail: int):
        import pandas as pd

        df = pd.DataFrame(rows or [])
        if df.empty:
            return df

        if "expiration_ms" in df.columns:
            df["expiration_utc"] = df["expiration_ms"].apply(_ms_to_utc_str)

        if view == "raw":
            return df

        if "instrument_id" in df.columns:
            df["instrument_id"] = df["instrument_id"].apply(
                lambda x: _abbr(str(x), last=id_tail)
            )

        drop_cols = [c for c in ["poll_key", "expiration_ms"] if c in df.columns]
        df = df.drop(columns=drop_cols)

        preferred = [
            "venue",
            "market_id",
            "slug",
            "title",
            "underlying",
            "outcome",
            "expiration_utc",
            "is_active",
            "instrument_id",
        ]
        cols = [c for c in preferred if c in df.columns] + [
            c for c in df.columns if c not in preferred
        ]
        return df[cols]


def _validate_invariants(items: Iterable[InstrumentMeta]) -> None:
    for i in items:
        if not i.instrument_id or ":" not in i.instrument_id:
            raise ValueError(f"Bad instrument_id: {i.instrument_id!r}")
        prefix, _, _rest = i.instrument_id.partition(":")
        if prefix != i.venue:
            raise ValueError(f"instrument_id venue mismatch: {i.instrument_id} vs venue={i.venue}")
        expected = f"{i.venue}:{i.poll_key}"
        if i.instrument_id != expected:
            raise ValueError(f"instrument_id != <venue>:<poll_key>: {i.instrument_id} vs {expected}")
        if i.expiration_ms is None:
            raise ValueError(f"expiration_ms is None for {i.instrument_id}")
        if int(i.expiration_ms) < 0:
            raise ValueError(f"expiration_ms < 0 for {i.instrument_id}")

def _ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def _abbr(s: str, last: int = 6) -> str:
    if s is None:
        return ""
    return s if len(s) <= last else f"…{s[-last:]}"
