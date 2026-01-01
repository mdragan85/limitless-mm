# readers/market_catalog/instrument_query.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Sequence, Tuple, List, Dict, Literal

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

    def select(
        self,
        *,
        top_n: Optional[int] = None,
        sort_by: str = "expiration_ms",
        per_market: PerMarket = "all",
        debug: bool = False,
    ) -> Tuple[List[str], Optional[Any]]:
        items = list(self._items)

        # sort: unknown expiry goes last for expiration-based sorts
        def sort_key(i: InstrumentMeta):
            v = _safe_getattr(i, sort_by)
            if sort_by == "expiration_ms":
                x = int(v or 0)
                return (x == 0, x, i.instrument_id)  # (unknown_last, exp, tie)
            return (v is None, v, i.instrument_id)

        items.sort(key=sort_key)

        if per_market == "one":
            chosen: Dict[Tuple[str, str], InstrumentMeta] = {}
            for i in items:
                k = (i.venue, i.market_id)
                if k not in chosen:
                    chosen[k] = i
            items = list(chosen.values())
            items.sort(key=sort_key)

        if top_n is not None:
            items = items[: max(0, int(top_n))]

        ids = [i.instrument_id for i in items]

        dbg = None
        if debug:
            dbg = [
                {
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
                for i in items
            ]

        return ids, dbg


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
