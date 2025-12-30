from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


@dataclass
class VenueRuntime:
    """
    Lightweight container describing how to run one venue
    inside the shared MarketLogger loop.
    """
    name: str
    client: object
    normalizer: Callable
    out_dir: Path
    discover_fn: Optional[Callable] = None  
