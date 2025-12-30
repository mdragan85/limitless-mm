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
    return poly_client.discover_instruments(POLYMARKET_RULES)


def discover_limitless():
    instruments = []
    for u in settings.UNDERLYINGS:
        markets = limitless_client.discover_markets(u)
        for m in markets:
            instruments.append({
                "venue": "limitless",
                "market_id": m.market_id,
                "instrument_id": "BOOK",
                "poll_key": m.slug,                    # Limitless polls by slug
                "slug": m.slug,
                "underlying": m.underlying,
                "expiration": m.raw.get("expirationTimestamp"),
                "title": getattr(m, "title", None),
                "raw": m.raw,
            })
    return instruments


def main():
    print("entered code")

    limitless = VenueRuntime(
        name="limitless",
        client=limitless_client,
        normalizer=normalize_orderbook,
        out_dir=Path(settings.OUTPUT_DIR) / "limitless",
        discover_fn=None,  # NEW
    )

    polymarket = VenueRuntime(
        name="polymarket",
        client=poly_client,
        normalizer=lambda rec, **kwargs: rec,
        out_dir=Path(settings.OUTPUT_DIR) / "polymarket",
        discover_fn=None,
    )

    logger = MarketLogger(venues=[limitless, polymarket])
    logger.run()


if __name__ == "__main__":
    print('hi')
    main()