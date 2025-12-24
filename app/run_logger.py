from pathlib import Path
from config.settings import settings

from collectors.market_logger import MarketLogger
from collectors.venue_runtime import VenueRuntime

from venues.limitless.client import LimitlessVenueClient
from venues.limitless.normalizer import normalize_orderbook

from venues.polymarket.client import PolymarketClient
from config.settings import POLYMARKET_RULES


limitless_client = LimitlessVenueClient()
poly_client = PolymarketClient()


def discover_polymarket():
    return poly_client.discover_markets(POLYMARKET_RULES)


def discover_limitless():
    markets = []
    for u in settings.UNDERLYINGS:
        markets.extend(limitless_client.discover_markets(u))
    return markets


def main():
    print("entered code")

    limitless = VenueRuntime(
        name="limitless",
        client=limitless_client,
        normalizer=normalize_orderbook,
        out_dir=Path(settings.OUTPUT_DIR) / "limitless",
        discover_fn=discover_limitless,  # NEW
    )

    polymarket = VenueRuntime(
        name="polymarket",
        client=poly_client,
        normalizer=lambda *args, **kwargs: None,  # TEMP
        out_dir=Path(settings.OUTPUT_DIR) / "polymarket",
        discover_fn=discover_polymarket,
    )

    logger = MarketLogger(venues=[limitless])
    logger.run()


if __name__ == "__main__":
    print('hi')
    main()