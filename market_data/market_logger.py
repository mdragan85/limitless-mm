"""
Market logging engine for Limitless.
Discovers markets, filters them, polls snapshots, and writes logs.
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import List

from config.settings import settings
from exchanges.limitless_api import LimitlessAPI
from exchanges.limitless_market import LimitlessMarket


class MarketLogger:
    """
    Polls multiple markets across multiple underlyings and writes snapshot logs.
    """

    def __init__(self):
        self.api = LimitlessAPI()
        self.out_dir = Path(settings.OUTPUT_DIR)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Market discovery
    # -------------------------
    def discover_markets(self, underlying: str) -> List[LimitlessMarket]:
        """
        Fetch markets for one underlying and normalize them.
        Returns a filtered list of loggable markets.
        """
        raw_list = self.api.list_markets(underlying)

        # Inject the underlying symbol so LimitlessMarket sees it
        markets = [
            LimitlessMarket.from_api(m)
            for m in raw_list
        ]

        # TEMPORARY: do not filter until we define proper rules
        loggable = markets

        return loggable[: settings.MAX_MARKETS_PER_UNDERLYING]


    # -------------------------
    # Logging a single snapshot
    # -------------------------
    def log_snapshot(self, market: LimitlessMarket) -> None:
        """
        Fetch a single orderbook snapshot for a Limitless market and
        append it as one JSON line to the per-underlying log file.
        """
        try:
            orderbook = self.api.get_orderbook(market.slug)
        except Exception as exc:
            print(
                f"[WARN] Failed to fetch orderbook for "
                f"{market.market_id}/{market.slug}: {exc}"
            )
            return

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "market_id": market.market_id,
            "slug": market.slug,
            "underlying": market.underlying,
            "title": market.title,
            "orderbook": orderbook,
        }

        # Use underlying symbol in filename; fall back to UNKNOWN if empty
        underlying = market.underlying or "UNKNOWN"
        file_path = self.out_dir / f"{underlying}_orderbooks.jsonl"

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    # -------------------------
    # Main loop
    # -------------------------
    def run(self):
        """
        Main polling loop.
        Discovers markets once per iteration, logs all snapshots, sleeps, repeats.
        """
        print(f"Starting Limitless market logger. Output -> {self.out_dir}")

        while True:
            for underlying in settings.UNDERLYINGS:
                try:
                    markets = self.discover_markets(underlying)
                except Exception as exc:
                    print(f"[WARN] Market discovery failed for {underlying}: {exc}")
                    continue

                for market in markets:
                    self.log_snapshot(market)

            time.sleep(settings.POLL_INTERVAL)
