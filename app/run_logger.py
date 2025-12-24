from pathlib import Path
from config.settings import settings

from collectors.market_logger import MarketLogger
from collectors.venue_runtime import VenueRuntime

from venues.limitless.client import LimitlessVenueClient
from venues.limitless.normalizer import normalize_orderbook


def main():
    print("entered code")

    limitless = VenueRuntime(
        name="limitless",
        client=LimitlessVenueClient(),
        normalizer=normalize_orderbook,
        out_dir=Path(settings.OUTPUT_DIR) / "limitless",
    )

    logger = MarketLogger(venues=[limitless])
    logger.run()


if __name__ == "__main__":
    print('hi')
    main()