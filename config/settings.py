"""
Application configuration for the Limitless data logger.
Reads settings from environment variables and provides defaults.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field

from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


@dataclass
class LimitlessConfig:
    """
    Settings for connecting to the Limitless API and logging behavior.
    """

    # API base URL
    BASE_URL: str = os.getenv("LIMITLESS_BASE_URL", "https://api.limitless.exchange")

    # How often (in seconds) to poll orderbooks
    POLL_INTERVAL: float = float(os.getenv("LIMITLESS_POLL_INTERVAL", "2.0"))

    # Which underlyings to log
    UNDERLYINGS: list[str] = field(
        default_factory=lambda: os.getenv(
            "LIMITLESS_UNDERLYINGS", "BTC,ETH,SOL,XRP"
        ).split(",")
    )

    # Max number of markets per underlying to log
    MAX_MARKETS_PER_UNDERLYING: int = int(
        os.getenv("LIMITLESS_MAX_MARKETS_PER_UNDERLYING", "10")
    )

    # Where to write logs
    OUTPUT_DIR: Path = Path(os.getenv("LIMITLESS_OUTPUT_DIR", "data/logs"))

    def __post_init__(self):
        # Normalize underlying symbols to uppercase
        self.UNDERLYINGS = [u.strip().upper() for u in self.UNDERLYINGS if u.strip()]


# Create a single config instance for import
settings = LimitlessConfig()
