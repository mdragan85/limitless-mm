from datetime import datetime
from pathlib import Path

from config.settings import settings, POLYMARKET_RULES

from collectors.discovery_service import DiscoveryService
from collectors.venue_runtime import VenueRuntime

from venues.limitless.client import LimitlessVenueClient
from venues.polymarket.client import PolymarketClient


limitless_client = LimitlessVenueClient()
poly_client = PolymarketClient()



def disp_boot_banner_discovery(venues):
    root = Path(settings.OUTPUT_DIR)
    print(f"[BOOT][DISCOVERY] OUTPUT_DIR={settings.OUTPUT_DIR} resolved={root.resolve()}")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    for v in venues:
        out = v.out_dir.resolve()
        snap = (out / "state" / "active_instruments.snapshot.json").resolve()
        mk_dir = (out / "markets" / f"date={today}").resolve()
        print(f"[BOOT][DISCOVERY] venue={v.name} out_dir={out}")
        print(f"[BOOT][DISCOVERY] venue={v.name} snapshot={snap}")
        print(f"[BOOT][DISCOVERY] venue={v.name} markets_dir={mk_dir}")

def discover_polymarket():
    return poly_client.discover_instruments(POLYMARKET_RULES)

def discover_limitless():
    instruments = []

    for u in settings.UNDERLYINGS:
        markets = limitless_client.discover_markets(u)
        for m in markets:
            raw = m.raw or {}

            # Only include markets that actually have an orderbook
            if raw.get("tradeType") != "clob":
                continue
            if not raw.get("tokens"):
                continue
            if raw.get("expired") is True:
                continue
            if raw.get("status") not in ("FUNDED", "ACTIVE"):  # keep FUNDED at least
                continue

            instruments.append(
                {
                    "venue": "limitless",
                    "market_id": m.market_id,
                    "instrument_id": "BOOK",
                    "poll_key": m.slug,
                    "slug": m.slug,
                    "underlying": m.underlying,
                    "expiration": raw.get("expirationTimestamp"),
                    "title": getattr(m, "title", None),
                    "raw": raw,
                }
            )

    return instruments


def main():
    limitless = VenueRuntime(
        name="limitless",
        client=limitless_client,
        normalizer=None,  # discovery doesn't need it
        out_dir=Path(settings.OUTPUT_DIR) / "limitless",
        discover_fn=discover_limitless,
    )

    polymarket = VenueRuntime(
        name="polymarket",
        client=poly_client,
        normalizer=None,
        out_dir=Path(settings.OUTPUT_DIR) / "polymarket",
        discover_fn=discover_polymarket,
    )
    
    venues = [limitless, polymarket]
    disp_boot_banner_discovery(venues)
    svc = DiscoveryService(venues=venues)
    svc.run_forever()


if __name__ == "__main__":
    main()
