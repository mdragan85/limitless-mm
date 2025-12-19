# scripts/peek_markets.py
from venues.limitless_api import LimitlessAPI

api = LimitlessAPI()
markets = api.list_markets()  # no filter

print(type(markets))
if isinstance(markets, dict):
    print("top-level keys:", markets.keys())
    first = next(iter(markets.values()), None)
elif isinstance(markets, list) and markets:
    first = markets[0]
    print("first market keys:", first.keys())
else:
    print("empty response:", markets)