from exchanges.limitless_api import LimitlessAPI
from market_data.market_logger import MarketLogger

api = LimitlessAPI()
logger = MarketLogger(api)
markets = api.discover_markets("BTC")
logger.log_snapshot(markets[0])