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
from market_data.market_definitions import LimitlessMarket


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
        markets = [LimitlessMarket.from_api(m) for m in raw_list]

        # Filter out expired or inactive markets
        loggable = [m for m in markets if m.is_loggable()]

        # Keep only the first N if configured
        return loggable[: settings.MAX_MARKETS_PER_UNDERLYING]

    # -------------------------
    # Logging a single snapshot
    # -------------------------
    def log_snapshot(self, market: LimitlessMarket):
        """
        Fetch orderbook snapshot, timestamp it, and write to disk.
        One JSON line per snapshot.
        """
        try:
            snapshot = self.api.get_orderbook(market.market_id)
        except Exception as exc:
            print(f"[WARN] Failed to fetch orderbook for {market.market_id}: {exc}")
            return

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "market_id": market.market_id,
            "underlying": market.underlying,
            "title": market.title,
            "snapshot": snapshot,
        }

        # Write to underlying-specific file
        file_path = self.out_dir / f"{market.underlying}_orderbooks.jsonl"
        with open(file_path, "a") as f:
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
