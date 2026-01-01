# readers/market_catalog/catalog.py
"""
MarketCatalog: venue-agnostic metadata catalog for prediction markets.

What this IS:
- A *metadata* catalog built from markets JSONL logs.
- A bridge between Discovery/Poller and analysis or readers.
- A way to reason about "markets" and "instruments" without touching orderbooks.

What this is NOT:
- An orderbook index.
- A persistent database.
- A strategy engine.

Design principles:
- Correctness over cleverness.
- Venue-specific logic lives in parsers.
- Rebuild-from-disk is cheap and preferred over incremental mutation.
"""

from __future__ import annotations

import json
import pandas as pd
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .catalog_models import InstrumentAccum, MarketAccum
from .models import make_instrument_id
from .parsers import VenueParser

from datetime import datetime, timezone




# ---------------------------------------------------------------------------
# Frozen, query-facing metadata objects
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class InstrumentMeta:
    """
    Immutable metadata for ONE orderbook stream.

    This is the final, canonical representation after merging all sightings.
    """
    instrument_id: str
    venue: str
    poll_key: str
    market_id: str
    slug: Optional[str]

    expiration_ms: int

    title: Optional[str]
    underlying: Optional[str]
    outcome: Optional[str]
    rule: Optional[str]
    cadence: Optional[str]

    first_seen_ms: int
    last_seen_ms: int

    extra: Dict[str, Any]

    def __post_init__(self) -> None:
        """
        Validate hard invariants at object construction time.

        Why here:
        - InstrumentMeta is the canonical frozen representation.
        - Catching bad data early prevents silent corruption in queries/readers.
        """
        self._validate_invariants()

    def _validate_invariants(self) -> None:
        """
        Hard invariants for InstrumentMeta.

        These are intentionally strict because downstream code relies on them.
        If a venue parser can't satisfy them, fix the parser (don't weaken these).
        """
        # Identity / keys
        assert isinstance(self.instrument_id, str) and self.instrument_id, "instrument_id must be non-empty str"
        assert isinstance(self.venue, str) and self.venue, "venue must be non-empty str"
        assert isinstance(self.poll_key, str) and self.poll_key, "poll_key must be non-empty str"
        assert isinstance(self.market_id, str) and self.market_id, "market_id must be non-empty str"

        # Expiry is a hard invariant in your system (you explicitly confirmed this).
        assert isinstance(self.expiration_ms, int), "expiration_ms must be int epoch-ms"
        assert self.expiration_ms > 0, "expiration_ms must be a positive epoch-ms"

        # Seen timestamps should be valid epoch-ms ints and ordered.
        assert isinstance(self.first_seen_ms, int) and self.first_seen_ms > 0, "first_seen_ms must be positive int"
        assert isinstance(self.last_seen_ms, int) and self.last_seen_ms > 0, "last_seen_ms must be positive int"
        assert self.last_seen_ms >= self.first_seen_ms, "last_seen_ms must be >= first_seen_ms"

        # Extra must always be a dict (can be empty).
        assert isinstance(self.extra, dict), "extra must be a dict"



@dataclass(frozen=True)
class MarketMeta:
    """
    Immutable metadata for ONE market (group of instruments).

    A market may have:
    - one instrument (e.g. Limitless)
    - multiple instruments (e.g. Polymarket YES/NO)
    """
    venue: str
    market_id: str
    slug: Optional[str]

    instruments: Tuple[str, ...]  # instrument_ids
    expiration_ms: int

    title: Optional[str]
    underlying: Optional[str]
    rule: Optional[str]
    cadence: Optional[str]

    is_active: bool
    first_seen_ms: int
    last_seen_ms: int

    extra: Dict[str, Any]


# ---------------------------------------------------------------------------
# MarketCatalog
# ---------------------------------------------------------------------------

class MarketCatalog:
    """
    Venue-agnostic catalog built from on-disk market metadata logs.

    Typical lifecycle:
    1) Construct with output_dir + venue parsers
    2) Call refresh()
    3) Query instruments / markets for analysis or readers

    The catalog may be rebuilt at any time; it holds no mutable external state.
    """

    def __init__(
        self,
        output_dir: Path,
        venues: Sequence[str],
        parsers: Dict[str, VenueParser],
    ) -> None:
        self.output_dir = Path(output_dir)
        self.venues = list(venues)
        self.parsers = parsers

        self._instruments: Dict[str, InstrumentMeta] = {}
        self._markets: Dict[Tuple[str, str], MarketMeta] = {}

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    @property
    def instruments(self) -> Dict[str, InstrumentMeta]:
        """All known instruments keyed by instrument_id."""
        return self._instruments

    @property
    def markets(self) -> Dict[Tuple[str, str], MarketMeta]:
        """All known markets keyed by (venue, market_id)."""
        return self._markets

    # ------------------------------------------------------------------
    # Build / refresh
    # ------------------------------------------------------------------

    def refresh(
        self,
        scan_days: int = 7,
        all_time: bool = False,
        use_snapshot: bool = True,
    ) -> None:
        """
        Rebuild the catalog from disk.

        Args:
        - scan_days: number of most-recent date folders to scan
        - all_time: ignore scan_days and scan everything
        - use_snapshot: annotate is_active from active snapshot files

        This method is intentionally idempotent and destructive:
        previous catalog state is discarded.
        """

        # --------------------------------------------------------------
        # Load active instrument IDs from snapshot(s), if requested
        # --------------------------------------------------------------
        active_ids: set[str] = set()
        if use_snapshot:
            active_ids = self._load_active_ids()

        # --------------------------------------------------------------
        # Phase 1: scan market logs and build instrument accumulators
        # --------------------------------------------------------------
        inst_acc: Dict[str, InstrumentAccum] = {}

        for venue in self.venues:
            parser = self.parsers[venue]

            for path in self._iter_market_files(
                venue, scan_days=scan_days, all_time=all_time
            ):
                for rec in _iter_jsonl(path):

                    # Defensive: skip lines that declare a different venue
                    if rec.get("venue") and rec.get("venue") != venue:
                        continue

                    drafts = parser.parse_line(rec)
                    if not drafts:
                        continue

                    for d in drafts:
                        if d.instrument_id not in inst_acc:
                            inst_acc[d.instrument_id] = InstrumentAccum(
                                instrument_id=d.instrument_id,
                                venue=d.venue,
                                poll_key=d.poll_key,
                                market_id=d.market_id,
                                slug=d.slug,
                                expiration_ms=d.expiration_ms,
                                title=d.title,
                                underlying=d.underlying,
                                outcome=d.outcome,
                                rule=d.rule,
                                cadence=d.cadence,
                                first_seen_ms=d.seen_ms,
                                last_seen_ms=d.seen_ms,
                                extra=dict(d.extra),
                            )
                        else:
                            inst_acc[d.instrument_id].merge(d)

        # --------------------------------------------------------------
        # Phase 2: group instruments into market accumulators
        # --------------------------------------------------------------
        mkt_acc: Dict[Tuple[str, str], MarketAccum] = {}

        for ia in inst_acc.values():
            # Normalize market_id to str for stable keys across venues/parsers.
            key = (ia.venue, str(ia.market_id))
            if key not in mkt_acc:
                mkt_acc[key] = MarketAccum(
                    venue=ia.venue,
                    market_id=str(ia.market_id),
                )
            mkt_acc[key].absorb_instrument(ia)

        # --------------------------------------------------------------
        # Phase 3: freeze into immutable metadata objects
        # --------------------------------------------------------------
        instruments_meta: Dict[str, InstrumentMeta] = {}
        for iid, ia in inst_acc.items():
            instruments_meta[iid] = InstrumentMeta(
                instrument_id=ia.instrument_id,
                venue=ia.venue,
                poll_key=ia.poll_key,
                market_id=str(ia.market_id),
                slug=ia.slug,
                expiration_ms=ia.expiration_ms,
                title=ia.title,
                underlying=ia.underlying,
                outcome=ia.outcome,
                rule=ia.rule,
                cadence=ia.cadence,
                first_seen_ms=ia.first_seen_ms,
                last_seen_ms=ia.last_seen_ms,
                extra=ia.extra,
            )

        markets_meta: Dict[Tuple[str, str], MarketMeta] = {}
        for key, ma in mkt_acc.items():
            inst_ids = tuple(sorted(ma.instruments))
            is_active = any(iid in active_ids for iid in inst_ids)

            # Normalize key + stored market_id to str.
            v, mid = key
            skey = (v, str(mid))
            markets_meta[skey] = MarketMeta(
                venue=ma.venue,
                market_id=str(ma.market_id),
                slug=ma.slug,
                instruments=inst_ids,
                expiration_ms=ma.expiration_ms,
                title=ma.title,
                underlying=ma.underlying,
                rule=ma.rule,
                cadence=ma.cadence,
                is_active=is_active,
                first_seen_ms=ma.first_seen_ms,
                last_seen_ms=ma.last_seen_ms,
                extra=ma.extra,
            )

        self._instruments = instruments_meta
        self._markets = markets_meta

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def instruments_for_market(self, venue: str, market_id: str) -> List[str]:
        """
        Return instrument_ids belonging to a given market.
        """
        m = self._markets.get((venue, str(market_id)))
        return list(m.instruments) if m else []

    # ------------------------------------------------------------------
    # Notebook helpers (optional pandas)
    # ------------------------------------------------------------------

    def markets_df(self, *, max_rows: Optional[int] = None, max_str: int = 80):
        """Market-only view (no instrument references)."""
        try:
            import pandas as pd  # type: ignore
        except ImportError as e:
            raise ImportError("pandas is required for markets_df()") from e

        rows: List[Dict[str, Any]] = []
        for (venue, market_id), m in self._markets.items():
            rows.append(
                {
                    "venue": venue,
                    "market_id": market_id,
                    "title": (m.title or "")[:max_str],
                    "slug": (m.slug or "")[:max_str],
                    "cadence": m.cadence or "",
                    "underlying": m.underlying or "",
                    "expiration_ms": int(m.expiration_ms or 0),
                    "is_active": getattr(m, "is_active", None),
                    "first_seen_ms": int(getattr(m, "first_seen_ms", 0) or 0),
                    "last_seen_ms": int(getattr(m, "last_seen_ms", 0) or 0),
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(
                ["venue", "expiration_ms", "market_id"],
                ascending=[True, True, True],
            )
        if max_rows is not None:
            df = df.head(int(max_rows))
        return df.reset_index(drop=True)

    def instruments_df(self, *, max_rows: Optional[int] = None, max_str: int = 80):
        """Instrument-only view (no market joins)."""
        try:
            import pandas as pd  # type: ignore
        except ImportError as e:
            raise ImportError("pandas is required for instruments_df()") from e

        rows: List[Dict[str, Any]] = []
        for iid, i in self._instruments.items():
            rows.append(
                {
                    "instrument_id": (iid or "")[:max_str],
                    "venue": i.venue,
                    "market_id": i.market_id,
                    "poll_key": (i.poll_key or "")[:max_str],
                    "title": (i.title or "")[:max_str],
                    "slug": (i.slug or "")[:max_str],
                    "outcome": i.outcome or "",
                    "cadence": i.cadence or "",
                    "underlying": i.underlying or "",
                    "expiration_ms": int(i.expiration_ms or 0),
                    "is_active": getattr(i, "is_active", None),
                    "first_seen_ms": int(getattr(i, "first_seen_ms", 0) or 0),
                    "last_seen_ms": int(getattr(i, "last_seen_ms", 0) or 0),
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(
                ["venue", "expiration_ms", "instrument_id"],
                ascending=[True, True, True],
            )
        if max_rows is not None:
            df = df.head(int(max_rows))
        return df.reset_index(drop=True)

    def market_detail_df(self, venue: str, market_id: Any, *, max_str: int = 200):
        """Market-only drilldown (excludes instruments)."""
        try:
            import pandas as pd  # type: ignore
        except ImportError as e:
            raise ImportError("pandas is required for market_detail_df()") from e

        candidates: List[Tuple[str, Any]] = [(venue, market_id)]
        try:
            candidates.append((venue, int(market_id)))
        except Exception:
            pass
        candidates.append((venue, str(market_id)))

        key = next((k for k in candidates if k in self._markets), None)
        if key is None:
            raise KeyError(
                f"Market not found for venue={venue!r} market_id={market_id!r}. Tried keys={candidates}"
            )

        m = self._markets[key]

        rows: List[Dict[str, Any]] = []

        def add(field: str, value: Any) -> None:
            if isinstance(value, str):
                value = value[:max_str]
            rows.append({"field": field, "value": value})

        # Dump all attributes except instruments; flatten extra.
        if hasattr(m, "__dict__"):
            for k, v in sorted(m.__dict__.items()):
                if k in ("instruments", "extra"):
                    continue
                add(k, v)
        else:
            for k in (
                "venue",
                "market_id",
                "slug",
                "expiration_ms",
                "title",
                "underlying",
                "rule",
                "cadence",
                "is_active",
                "first_seen_ms",
                "last_seen_ms",
            ):
                add(k, getattr(m, k, None))

        extra = getattr(m, "extra", None)
        if isinstance(extra, dict):
            for k, v in sorted(extra.items()):
                add(f"extra.{k}", v)

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Disk helpers
    # ------------------------------------------------------------------

    def _iter_market_files(
        self, venue: str, scan_days: int, all_time: bool
    ) -> Iterable[Path]:
        """
        Yield JSONL market metadata files for a given venue.

        Assumes directory layout:
          <output_dir>/<venue>/markets/date=YYYY-MM-DD/*.jsonl

        This is intentionally simple; future refactors may push this behind
        a venue-specific layout abstraction.
        """
        base = self.output_dir / venue / "markets"
        if not base.exists():
            return []

        folders = sorted(
            p for p in base.iterdir()
            if p.is_dir() and p.name.startswith("date=")
        )

        if not all_time and scan_days is not None:
            folders = folders[-scan_days:]

        for d in folders:
            yield from sorted(d.glob("*.jsonl"))

    def _load_active_ids(self) -> set[str]:
        """
        Load active instrument IDs from per-venue snapshot files.

        Snapshot schema (current):
          {
            "venue": "...",
            "instruments": {
                "<poll_key>": {...},
                ...
            }
          }

        Snapshot usage is OPTIONAL and only used to annotate is_active.
        """
        active_ids: set[str] = set()

        for venue in self.venues:
            snap = self.output_dir / venue / "state" / "active_instruments.snapshot.json"
            if not snap.exists():
                continue

            obj = json.loads(snap.read_text())
            v = obj.get("venue") or venue
            instruments = obj.get("instruments") or {}

            for poll_key in instruments.keys():
                active_ids.add(make_instrument_id(v, poll_key))

        return active_ids

    def summary(self) -> dict:
        """
        Venue-agnostic inventory and quick health checks.

        Purpose:
        - Show how many instruments/markets were indexed per venue
        - Provide a simple instruments-per-market ratio to catch missing legs
        (e.g., a 2-outcome venue accidentally ingesting only one outcome).

        Ratio is reported as x1000 to keep it integer-friendly.
        """
        inst_by_venue = defaultdict(int)
        for inst in self._instruments.values():
            inst_by_venue[inst.venue] += 1

        mkt_by_venue = defaultdict(int)
        for (venue, _mid) in self._markets.keys():
            mkt_by_venue[venue] += 1

        venues = sorted(set(inst_by_venue) | set(mkt_by_venue))

        by_venue = {}
        for v in venues:
            inst = inst_by_venue.get(v, 0)
            mkt = mkt_by_venue.get(v, 0)
            by_venue[v] = {
                "instruments": inst,
                "markets": mkt,
                "instruments_per_market_x1000": int(1000 * inst / max(1, mkt)),
            }

        return {
            "instruments_total": len(self._instruments),
            "markets_total": len(self._markets),
            "by_venue": by_venue,
        }
    
    # ------------------------------------------------------------------
    # Notebook helpers (optional pandas)
    # ------------------------------------------------------------------
    def markets_df(self, *, max_rows: int | None = None, max_str: int = 80):

        rows = []
        for (venue, market_id), m in self._markets.items():
            rows.append({
                "venue": venue,
                "market_id": market_id,
                "title": (m.title or "")[:max_str],
                "slug": (m.slug or "")[:max_str],
                "cadence": m.cadence or "",
                "underlying": m.underlying or "",
                "expiration_utc": ms_to_utc(m.expiration_ms),
                "first_seen_utc": ms_to_utc(getattr(m, "first_seen_ms", None)),
                "last_seen_utc": ms_to_utc(getattr(m, "last_seen_ms", None)),
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["venue", "expiration_utc", "market_id"], ascending=[True, True, True])

        if max_rows is not None:
            df = df.head(int(max_rows))

        return df.reset_index(drop=True)

    def instruments_df(self, *, max_rows: Optional[int] = None, max_str: int = 80):
        """
        Return a compact pandas DataFrame of indexed instruments (readable columns).
        """
        rows = []
        for iid, i in self._instruments.items():
            rows.append({
                "instrument_id": iid,
                "venue": i.venue,
                "market_id": i.market_id,
                "poll_key": i.poll_key,
                "title": i.title or "",
                "slug": i.slug or "",
                "outcome": i.outcome or "",
                "cadence": i.cadence or "",
                "underlying": i.underlying or "",
                "expiration_utc": ms_to_utc(i.expiration_ms),
                "first_seen_utc": ms_to_utc(getattr(i, "first_seen_ms", None)),
                "last_seen_utc": ms_to_utc(getattr(i, "last_seen_ms", None))
            })

        df = pd.DataFrame(rows)

        if not df.empty:
            df = df.sort_values(["venue", "expiration_utc", "instrument_id"], ascending=[True, True, True])
            for c in ["title", "slug", "poll_key", "instrument_id"]:
                df[c] = df[c].astype(str).str.slice(0, max_str)

        if max_rows is not None:
            df = df.head(int(max_rows))

        return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------

def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    """Yield parsed JSON objects from a .jsonl file."""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def ms_to_utc(ms):
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except Exception:
        return None