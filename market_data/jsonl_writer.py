import json
import os
import time
from pathlib import Path


class JsonlRotatingWriter:
    def __init__(self, directory: Path, prefix: str, rotate_minutes: int, fsync_seconds: int):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix
        self.rotate_seconds = rotate_minutes * 60
        self.fsync_seconds = fsync_seconds

        self.part = 0
        self.opened_at = 0
        self.last_fsync = 0
        self.fh = None

        self._open_new()

    def _open_new(self):
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
        now = time.time()
        if now - self.opened_at > self.rotate_seconds:
            self._open_new()

        self.fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.fh.flush()

        if now - self.last_fsync > self.fsync_seconds:
            os.fsync(self.fh.fileno())
            self.last_fsync = now
