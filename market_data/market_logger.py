"""
Market logging engine for Limitless.
Discovers markets, filters them, polls snapshots, and writes logs.
"""

import json
import time
from datetime import datetime
from pathlib import Path

from config.settings import settings
from exchanges.limitless_api import LimitlessAPI
from exchanges.limitless_market import LimitlessMarket

from market_data.jsonl_writer import JsonlRotatingWriter
from market_data.normalize_orderbook import normalize_orderbook
from market_data.active_markets import ActiveMarkets


class MarketLogger:
    """
    Polls multiple markets across multiple underlyings and writes snapshot logs.
    """

    def __init__(self, api: LimitlessAPI):
        self.api = api
        self.out_dir = Path(settings.OUTPUT_DIR)
        self.out_dir.mkdir(parents=True, exist_ok=True)


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
    # Logging helpers
    # -------------------------
    def log_markets(self, markets: list[LimitlessMarket]) -> None:
        for market in markets:
            self.log_snapshot(market)

    # -------------------------
    # Main loop
    # -------------------------
    def run(self):
        current_date = datetime.utcnow().strftime("%Y-%m-%d")

        def make_writers(date_str: str):
            markets_writer = JsonlRotatingWriter(
                self.out_dir / "markets" / f"date={date_str}",
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            books_writer = JsonlRotatingWriter(
                self.out_dir / "orderbooks" / f"date={date_str}",
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )
            return markets_writer, books_writer

        markets_writer, books_writer = make_writers(current_date)

        active = ActiveMarkets(
            self.out_dir / "state" / "active_markets.json",
            settings.EXPIRE_GRACE_SECONDS,
        )

        last_discover = 0

        while True:
            now = time.time()

            # ----- NEW: rollover writers at midnight UTC -----
            new_date = datetime.utcnow().strftime("%Y-%m-%d")
            if new_date != current_date:
                try:
                    markets_writer.close()
                    books_writer.close()
                finally:
                    current_date = new_date
                    markets_writer, books_writer = make_writers(current_date)
            # -----------------------------------------------

            if now - last_discover > settings.DISCOVER_EVERY_SECONDS:
                last_discover = now

                for u in settings.UNDERLYINGS:
                    markets = self.api.discover_markets(u)
                    active.refresh(markets)

                    for m in markets:
                        markets_writer.write({
                            "asof_ts_utc": datetime.utcnow().isoformat(),
                            "market_id": m.market_id,
                            "slug": m.slug,
                            "underlying": m.underlying,
                            "raw": m.raw,
                        })

                active.prune()
                active.save()

            for mid, info in active.active.items():
                try:
                    raw_ob = self.api.get_orderbook(info["slug"])
                except RuntimeError:
                    print(f'error fetching orderbook for {mid}')
                    continue

                snap = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "market_id": mid,
                    "slug": info["slug"],
                    "underlying": info.get("underlying"),
                    "orderbook": raw_ob,
                }

                rec = normalize_orderbook(snap, full_orderbook=settings.FULL_ORDERBOOK)
                books_writer.write(rec)

            time.sleep(settings.POLL_INTERVAL)
