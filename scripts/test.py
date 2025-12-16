from exchanges.limitless_api import LimitlessAPI

api = LimitlessAPI()
mkts = api.discover_markets("BTC")
print(len(mkts), mkts[0])