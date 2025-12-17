from market_data.market_logger import MarketLogger
from exchanges.limitless_api import LimitlessAPI

def main():
    api = LimitlessAPI()
    logger = MarketLogger(api=api)
    logger.run()

if __name__ == "__main__":
    main()