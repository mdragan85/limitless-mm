from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Sequence

from readers.market_catalog.catalog import InstrumentMeta
from readers.orderbooks.reader import OrderbookReader


@dataclass(frozen=True)
class OrderbookStream:
    """
    Thin handle for ONE orderbook stream (one instrument_id).

    Purpose:
      - Bind InstrumentMeta (identity + metadata) to a reader (filesystem I/O)
      - Provide a convenient iterator for snapshots

    Non-goals:
      - No indexing
      - No caching
      - No persistence
      - No time-series logic (that belongs in OrderbookHistory)
    """
    instrument: InstrumentMeta
    reader: OrderbookReader

    @property
    def instrument_id(self) -> str:
        return self.instrument.instrument_id

    @property
    def venue(self) -> str:
        return self.instrument.venue

    def iter_snapshots(
        self,
        *,
        dates: Sequence[str],
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate raw snapshots (dicts) for this instrument.

        Args:
            dates: list of YYYY-MM-DD partitions to scan
            start_ms/end_ms: optional window filter (collector ts_ms)

        Yields:
            Raw snapshot dicts from JSONL logs.
        """
        yield from self.reader.iter_snapshots(
            [self.instrument_id],
            dates=dates,
            start_ms=start_ms,
            end_ms=end_ms,
        )
