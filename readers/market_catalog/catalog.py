from __future__ import annotations
import json

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .catalog_models import InstrumentAccum, MarketAccum
from .models import InstrumentDraft, make_instrument_id
from .parsers import VenueParser
from .utils import require


@dataclass(frozen=True)
class InstrumentMeta:
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
    is_active: bool
    first_seen_ms: int
    last_seen_ms: int
    extra: Dict[str, Any]


@dataclass(frozen=True)
class MarketMeta:
    venue: str
    market_id: str
    slug: Optional[str]
    instruments: Tuple[str, ...]
    expiration_ms: int
    title: Optional[str]
    underlying: Optional[str]
    rule: Optional[str]
    cadence: Optional[str]
    is_active: bool
    first_seen_ms: int
    last_seen_ms: int
    extra: Dict[str, Any]


class MarketCatalog:
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

    @property
    def instruments(self) -> Dict[str, InstrumentMeta]:
        return self._instruments

    @property
    def markets(self) -> Dict[Tuple[str, str], MarketMeta]:
        return self._markets

    def refresh(self, scan_days: int = 7, all_time: bool = False, use_snapshot: bool = True) -> None:
        active_ids = set()
        if use_snapshot:
            active_ids = self._load_active_ids()

        # 1) scan market jsonl files
        inst_acc: Dict[str, InstrumentAccum] = {}

        for venue in self.venues:
            parser = self.parsers[venue]
            for p in self._iter_market_files(venue, scan_days=scan_days, all_time=all_time):
                for rec in _iter_jsonl(p):
                    # Ignore wrong-venue lines if mixed files ever happen
                    if rec.get("venue") and rec.get("venue") != venue:
                        continue
                    drafts = parser.parse_line(rec)
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

        # 2) build market accums
        mkt_acc: Dict[Tuple[str, str], MarketAccum] = {}
        for iid, ia in inst_acc.items():
            key = (ia.venue, ia.market_id)
            if key not in mkt_acc:
                mkt_acc[key] = MarketAccum(venue=ia.venue, market_id=ia.market_id)
            mkt_acc[key].absorb_instrument(ia)

        # 3) freeze into metas + active flags
        instruments_meta: Dict[str, InstrumentMeta] = {}
        for iid, ia in inst_acc.items():
            instruments_meta[iid] = InstrumentMeta(
                instrument_id=ia.instrument_id,
                venue=ia.venue,
                poll_key=ia.poll_key,
                market_id=ia.market_id,
                slug=ia.slug,
                expiration_ms=ia.expiration_ms,
                title=ia.title,
                underlying=ia.underlying,
                outcome=ia.outcome,
                rule=ia.rule,
                cadence=ia.cadence,
                is_active=(iid in active_ids),
                first_seen_ms=ia.first_seen_ms,
                last_seen_ms=ia.last_seen_ms,
                extra=ia.extra,
            )

        markets_meta: Dict[Tuple[str, str], MarketMeta] = {}
        for key, ma in mkt_acc.items():
            inst_ids = tuple(sorted(ma.instruments))
            is_active = any(iid in active_ids for iid in inst_ids)
            markets_meta[key] = MarketMeta(
                venue=ma.venue,
                market_id=ma.market_id,
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

    def instruments_for_market(self, venue: str, market_id: str) -> List[str]:
        m = self._markets.get((venue, str(market_id)))
        return list(m.instruments) if m else []

    def _iter_market_files(self, venue: str, scan_days: int, all_time: bool) -> Iterable[Path]:
        base = self.output_dir / venue / "markets"
        if not base.exists():
            return []

        # folders: date=YYYY-MM-DD
        folders = sorted([p for p in base.iterdir() if p.is_dir() and p.name.startswith("date=")])
        if not all_time and scan_days is not None:
            folders = folders[-scan_days:]  # simple: assumes one folder per day in order

        for d in folders:
            yield from sorted(d.glob("*.jsonl"))

    def _load_active_ids(self) -> set[str]:
        active_ids: set[str] = set()
        for venue in self.venues:
            snap = self.output_dir / venue / "state" / "active_instruments.snapshot.json"
            if not snap.exists():
                continue
            obj = json.loads(snap.read_text())
            # schema: {venue, instruments: {poll_key: {...}}}
            v = obj.get("venue") or venue
            instruments = obj.get("instruments") or {}
            for poll_key in instruments.keys():
                active_ids.add(make_instrument_id(v, poll_key))
        return active_ids

    def summary(self) -> dict:
        """
        Venue-agnostic inventory + ratios.

        Returns counts by venue and the instruments-per-market ratio per venue (x1000)
        so you can spot venues that are missing legs (e.g., 2-outcome markets showing 1 instrument).
        """
        inst_by_venue = defaultdict(int)
        for inst in self._instruments.values():
            inst_by_venue[inst.venue] += 1

        mkt_by_venue = defaultdict(int)
        for (venue, _mid) in self._markets.keys():
            mkt_by_venue[venue] += 1

        venues = sorted(set(inst_by_venue) | set(mkt_by_venue))

        ratios_x1000 = {}
        for v in venues:
            inst = inst_by_venue.get(v, 0)
            mkt = mkt_by_venue.get(v, 0)
            ratios_x1000[v] = int(1000 * inst / max(1, mkt))

        return {
            "instruments_total": len(self._instruments),
            "markets_total": len(self._markets),
            "by_venue": {
                v: {
                    "instruments": inst_by_venue.get(v, 0),
                    "markets": mkt_by_venue.get(v, 0),
                    "instruments_per_market_x1000": ratios_x1000[v],
                }
                for v in venues
            },
        }


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

