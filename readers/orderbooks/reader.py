from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set

from config.settings import settings

@dataclass(frozen=True)
class OrderbookReader:
    """
    Filesystem reader for orderbook JSONL logs.

    Responsibilities:
      - Locate JSONL files under:
          <input_dir>/<venue>/orderbooks/date=YYYY-MM-DD/orderbooks.part-*.jsonl
      - Stream-parse JSONL lines as dicts
      - Filter by instrument_id(s) and optional [start_ms, end_ms] window

    Non-responsibilities:
      - No indexing
      - No persistence
      - No caching (beyond scanning file lists)
      - No sorting guarantees beyond file order
    """
    input_dir: Path = Path(settings.INPUT_DIR)

    def iter_snapshots(
        self,
        instrument_ids: Sequence[str],
        *,
        dates: Sequence[str],
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yield raw snapshot dicts for given instrument_ids.

        Args:
            instrument_ids:
                List of instrument_id strings to include.
            dates:
                Partition dates in YYYY-MM-DD format.
            start_ms / end_ms:
                Optional inclusive window filter. Applied to the record's `ts_ms`
                (collector timestamp). History can choose to window on another
                time_field later.

        Yields:
            Raw dict per JSONL record.
        """
        wanted: Set[str] = set(instrument_ids)

        # Group by venue inferred from instrument_id prefix "venue:..."
        by_venue: Dict[str, List[str]] = {}
        for iid in wanted:
            venue = iid.split(":", 1)[0] if ":" in iid else ""
            by_venue.setdefault(venue, []).append(iid)

        for venue, venue_ids in by_venue.items():
            yield from self._iter_venue_snapshots(
                venue,
                set(venue_ids),
                dates=dates,
                start_ms=start_ms,
                end_ms=end_ms,
            )

    def _iter_venue_snapshots(
        self,
        venue: str,
        venue_ids: Set[str],
        *,
        dates: Sequence[str],
        start_ms: Optional[int],
        end_ms: Optional[int],
    ) -> Iterator[Dict[str, Any]]:
        for d in dates:
            day_dir = self.input_dir / venue / "orderbooks" / f"date={d}"
            if not day_dir.exists():
                continue

            # Stable-ish ordering helps notebook reproducibility
            paths = sorted(day_dir.glob("orderbooks.part-*.jsonl"))
            for p in paths:
                yield from self._iter_file(p, venue_ids, start_ms=start_ms, end_ms=end_ms)

    def _iter_file(
        self,
        path: Path,
        venue_ids: Set[str],
        *,
        start_ms: Optional[int],
        end_ms: Optional[int],
    ) -> Iterator[Dict[str, Any]]:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    rec = json.loads(line)
                except Exception:
                    # Skip malformed lines rather than killing a notebook session.
                    # If you prefer strictness, raise instead.
                    continue

                iid = rec.get("instrument_id")
                if iid not in venue_ids:
                    continue

                # Window filter uses collector ts_ms (always present in your schema)
                ts = rec.get("ts_ms")
                if ts is None:
                    # If this ever happens, it's a schema bug; skip rather than crash.
                    continue

                ts_i = int(ts)
                if start_ms is not None and ts_i < int(start_ms):
                    continue
                if end_ms is not None and ts_i > int(end_ms):
                    continue

                yield rec
