"""
Entrypoint for running the Limitless market logger.
"""

import sys
import signal

from market_data.market_logger import MarketLogger


def main():
    logger = MarketLogger()

    def handle_sigint(sig, frame):
        print("\nReceived interrupt. Shutting down cleanly...")
        logger.api.close()
        sys.exit(0)

    # Graceful shutdown on Ctrl+C
    signal.signal(signal.SIGINT, handle_sigint)

    # Run the logger loop
    logger.run()


if __name__ == "__main__":
    main()
