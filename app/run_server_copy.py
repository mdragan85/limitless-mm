import shutil
from pathlib import Path
from datetime import datetime

SOURCE_ROOT = Path("/Volumes/mm_output")
DEST_ROOT = Path("/Users/maciejdragan/Python VSCode Projects/limitless_mm/limitless-mm/.outputs/logs_server")

# ---- CONFIG ----
# Copy files modified AFTER this date
CUTOFF_DATE = datetime(2026, 1, 6)
# ----------------

cutoff_ts = CUTOFF_DATE.timestamp()

def copy_filtered_tree(src_root: Path, dst_root: Path, cutoff_ts: float):
    for src_path in src_root.rglob("*"):
        rel_path = src_path.relative_to(src_root)
        dst_path = dst_root / rel_path

        if src_path.is_dir():
            dst_path.mkdir(parents=True, exist_ok=True)
            continue

        stat = src_path.stat()
        if stat.st_mtime < cutoff_ts:
            continue

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Copying: {rel_path}")
        shutil.copy2(src_path, dst_path)

if __name__ == "__main__":
    if not SOURCE_ROOT.exists():
        raise RuntimeError(f"Source does not exist: {SOURCE_ROOT}")

    DEST_ROOT.mkdir(parents=True, exist_ok=True)
    copy_filtered_tree(SOURCE_ROOT, DEST_ROOT, cutoff_ts)

    print("Done.")
