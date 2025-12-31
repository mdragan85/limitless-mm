from datetime import datetime
from pathlib import Path
from config.settings import settings

from collectors.market_logger import MarketLogger
from collectors.venue_runtime import VenueRuntime

from venues.limitless.client import LimitlessVenueClient
from venues.limitless.normalizer import normalize_orderbook

from venues.polymarket.client import PolymarketClient

from config.polymarket_rules import POLYMARKET_RULES
from config.limitless_rules import LIMITLESS_RULES



limitless_client = LimitlessVenueClient()
poly_client = PolymarketClient()


def discover_polymarket():
    return poly_client.discover_instruments(POLYMARKET_RULES)


def discover_limitless():
    return limitless_client.discover_instruments(LIMITLESS_RULES)


def main():
    print("entered code")

    limitless = VenueRuntime(
        name="limitless",
        client=limitless_client,
        normalizer=normalize_orderbook,
        out_dir=Path(settings.OUTPUT_DIR) / "limitless",
        discover_fn=None,  
    )

    polymarket = VenueRuntime(
        name="polymarket",
        client=poly_client,
        normalizer=lambda rec, **kwargs: rec,
        out_dir=Path(settings.OUTPUT_DIR) / "polymarket",
        discover_fn=None,
    )

    venues = [limitless, polymarket]
    logger = MarketLogger(venues=venues)
    logger.run()


if __name__ == "__main__":
    print('hi')
    main()