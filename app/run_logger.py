from pathlib import Path
from config.settings import settings

from collectors.market_logger import MarketLogger
from collectors.venue_runtime import VenueRuntime

from venues.limitless.client import LimitlessVenueClient
from venues.limitless.normalizer import normalize_orderbook



limitless_client = LimitlessVenueClient()

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

    logger = MarketLogger(venues=[limitless])
    logger.run()


if __name__ == "__main__":
    print('hi')
    main()