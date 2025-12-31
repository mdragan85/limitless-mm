"""
Polymarket discovery diagnostic harness.

"""

from datetime import datetime
from pathlib import Path

from config.settings import settings, POLYMARKET_RULES

from collectors.discovery_service import DiscoveryService
from collectors.venue_runtime import VenueRuntime

from venues.limitless.client import LimitlessVenueClient
from venues.polymarket.client import PolymarketClient



def discover_polymarket():
    return poly_client.discover_instruments(POLYMARKET_RULES)

def discover_limitless():
    return limitless_client.discover_instruments(settings.UNDERLYINGS)


limitless_client = LimitlessVenueClient()
poly_client = PolymarketClient()

xl = discover_limitless()
#xp = discover_polymarket()


for d in xl: 
    for k, v in d.items():
        print(k, ':', v)

    print('\n---------\n')