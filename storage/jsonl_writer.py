import json
import os
import time
from pathlib import Path


class JsonlRotatingWriter:
    """
    Append-only JSONL writer with time-based file rotation and periodic fsync.

    Responsibilities:
    - Write one JSON record per line (JSONL format)
    - Rotate output files on a fixed time interval
    - Periodically fsync to balance durability and throughput

    Non-responsibilities:
    - Schema validation or record transformation
    - Atomic multi-file transactions
    - Log retention or cleanup policies

    Design notes:
    - Rotation is time-based, not size-based, to simplify downstream readers.
    - fsync is decoupled from per-write flushes to reduce I/O overhead while
      still providing bounded data-loss windows on crash.
    """

    def __init__(self, directory: Path, prefix: str, rotate_minutes: int, fsync_seconds: int):
        # Directory where JSONL files will be written
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)

        # Prefix used in file naming: <prefix>.part-XXXX.jsonl
        self.prefix = prefix

        # Rotation interval in seconds
        self.rotate_seconds = rotate_minutes * 60

        # Minimum interval between fsync calls
        self.fsync_seconds = fsync_seconds

        # Monotonically increasing file part counter
        self.part = 0

        # Timestamps used for rotation and fsync decisions
        self.opened_at = 0
        self.last_fsync = 0

        # Active file handle
        self.fh = None

        # Open the initial output file
        self._open_new()

    def _open_new(self):
        """
        Close the current file (if any) and open a new rotated file.

        This method ensures the previous file is flushed and fsynced
        before closing to minimize data loss on rotation boundaries.
        """
        if self.fh:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.fh.close()

        path = self.dir / f"{self.prefix}.part-{self.part:04d}.jsonl"
        self.part += 1

        self.fh = open(path, "a", encoding="utf-8")
        self.opened_at = time.time()
        self.last_fsync = self.opened_at

    def write(self, record: dict):
        """
        Append a single record to the current JSONL file.

        Performs:
        - Time-based file rotation
        - Line-buffered write + flush
        - Periodic fsync for durability
        """
        now = time.time()

        # Rotate file if the rotation interval has elapsed
        if now - self.opened_at > self.rotate_seconds:
            self._open_new()

        # Write one JSON object per line
        self.fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.fh.flush()

        # Force data to disk periodically (not on every write)
        if now - self.last_fsync > self.fsync_seconds:
            os.fsync(self.fh.fileno())
            self.last_fsync = now

    def close(self):
        """
        Flush, fsync, and close the active file handle.

        Safe to call multiple times.
        """
        if self.fh:
            try:
                self.fh.flush()
                os.fsync(self.fh.fileno())
            finally:
                self.fh.close()
                self.fh = None
