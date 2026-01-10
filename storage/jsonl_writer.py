import re
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
        - Ensure monotonic file part numbering across process restarts
        - Periodically fsync to balance durability and throughput

    Non-responsibilities:
        - Schema validation or record transformation
        - Atomic multi-file transactions
        - Log retention, compaction, or cleanup policies
        - Strict global ordering guarantees (ordering is by record timestamp)

    Design notes:
        - Rotation is time-based, not size-based, to simplify downstream readers.
        - On startup, the writer resumes at the next available part number to avoid
        appending to previously closed files after a restart.
        - fsync is decoupled from per-write flushes to reduce I/O overhead while still
        providing bounded data-loss windows on crash.
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

        # NEW: resume part counter from disk so restarts don't append to part-0000
        self._init_part_counter()

        # Open the initial output file
        self._open_new()

    def _init_part_counter(self) -> None:
        pat = re.compile(rf"^{re.escape(self.prefix)}\.part-(\d+)\.jsonl$")
        max_part = -1
        for p in self.dir.iterdir():
            if not p.is_file():
                continue
            m = pat.match(p.name)
            if not m:
                continue
            try:
                n = int(m.group(1))
                if n > max_part:
                    max_part = n
            except ValueError:
                pass
        self.part = max_part + 1

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
        - Buffered write (no per-record flush)
        - Periodic flush + fsync for durability (bounded loss window)

        Why this change:
        - Per-record flush is extremely expensive and caps throughput.
        - You already fsync every N seconds, which defines your durability window.
        - Flushing only when we fsync preserves safety while improving speed.
        """
        now = time.time()

        # Rotate file if the rotation interval has elapsed
        if now - self.opened_at > self.rotate_seconds:
            self._open_new()

        # Write one JSON object per line (buffered)
        self.fh.write(json.dumps(record, ensure_ascii=False) + "\n")

        # Force data to disk periodically (not on every write).
        # We flush BEFORE fsync so the OS sees the latest bytes.
        if now - self.last_fsync > self.fsync_seconds:
            self.fh.flush()
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
