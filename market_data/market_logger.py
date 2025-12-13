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
    def log_snapshot(self, market: LimitlessMarket):
        """Fetch orderbook snapshot, timestamp it, and write to disk."""
        # Prefer slug from raw payload; required by /markets/{slug}/orderbook
        slug = market.raw.get("slug")
        if not slug:
            print(f"[WARN] No slug found for market {market.market_id}, skipping")
            return

        # Fetch BOTH orderbooks
        try:
            yes_book = self.api.get_orderbook(market.slug, market.yes_token)
            no_book  = self.api.get_orderbook(market.slug, market.no_token)
        except Exception as exc:
            print(f"[WARN] Failed to fetch orderbooks for {market.market_id}/{market.slug}: {exc}")
            return

        # Log them together
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "market_id": market.market_id,
            "slug": market.slug,
            "underlying": market.underlying,
            "title": market.title,
            "yes_orderbook": yes_book,
            "no_orderbook":  no_book,
        }

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
