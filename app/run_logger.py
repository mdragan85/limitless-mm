from collectors.market_logger import MarketLogger
from venues.limitless.client import LimitlessVenueClient

def main():
    print('entered code')
    client = LimitlessVenueClient()
    logger = MarketLogger(client=client)
    logger.run()


if __name__ == "__main__":
    print('hi')
    main()