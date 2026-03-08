"""
TTB Wine Production Pipeline — Stage 1: Download and Cache Source Files
========================================================================

This module fetches all 5 source files from the TTB (Alcohol and Tobacco Tax
and Trade Bureau) website and caches them locally in data/raw/.

Key design decisions:
- ETag/Last-Modified headers are stored in a JSON sidecar file so we can skip
  re-downloading unchanged files on subsequent runs.
- Files are saved with a datestamp suffix (e.g. wine_monthly_20240815.csv) so
  we preserve a historical record of downloads.
- A "latest" symlink-style copy (no date suffix) is also maintained so
  downstream stages can always read from a stable filename.
"""

import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configure logging — we want to see download progress in the console
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source file registry
# Each entry maps a logical name to its URL and the local filename base.
# The TTB publishes these files at stable URLs that get updated in-place,
# which is why ETag/Last-Modified checking is essential — the URL doesn't
# change, but the content does.
# ---------------------------------------------------------------------------
SOURCE_FILES = {
    "wine_monthly": {
        "url": "https://www.ttb.gov/system/files/2024-08/Wine_monthly_data_csv.csv",
        "filename": "wine_monthly.csv",
    },
    "wine_yearly": {
        "url": "https://www.ttb.gov/system/files/2024-08/Wine_yearly_data_csv.csv",
        "filename": "wine_yearly.csv",
    },
    "wine_state": {
        "url": "https://www.ttb.gov/system/files/2024-04/Wine_State_Report.xlsx",
        "filename": "wine_state.xlsx",
    },
    "wine_json": {
        "url": "https://www.ttb.gov/system/files/2024-08/Wine_Data.json",
        "filename": "wine_data.json",
    },
    "wine_mapping": {
        "url": "https://www.ttb.gov/system/files/images/wine_mapping_2022.xlsx",
        "filename": "wine_mapping.xlsx",
    },
}

# ---------------------------------------------------------------------------
# Path to the metadata sidecar file that stores ETags and Last-Modified
# timestamps for each downloaded file.
# ---------------------------------------------------------------------------
METADATA_FILE = "download_metadata.json"


def _get_raw_dir() -> Path:
    """Return the path to data/raw/, creating it if needed."""
    raw_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def _load_metadata(raw_dir: Path) -> dict:
    """
    Load the download metadata sidecar file.
    This file tracks ETags and Last-Modified headers so we can skip
    re-downloading files that haven't changed on the server.
    Returns an empty dict if no metadata exists yet (first run).
    """
    meta_path = raw_dir / METADATA_FILE
    if meta_path.exists():
        with open(meta_path, "r") as f:
            return json.load(f)
    return {}


def _save_metadata(raw_dir: Path, metadata: dict) -> None:
    """Persist the download metadata back to the sidecar file."""
    meta_path = raw_dir / METADATA_FILE
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)


def download_file(
    name: str,
    url: str,
    filename: str,
    raw_dir: Path,
    metadata: dict,
    force: bool = False,
) -> bool:
    """
    Download a single file from TTB, with ETag/Last-Modified change detection.

    How the caching works:
    1. We send a HEAD request first to get the current ETag and Last-Modified.
    2. If they match what we stored from the last download, we skip (304-like).
    3. If they differ (or we have no prior record), we download the full file.
    4. We save both a timestamped copy and a "latest" copy for downstream use.

    Args:
        name: Logical name for logging (e.g. "wine_monthly")
        url: The TTB download URL
        filename: Local filename base (e.g. "wine_monthly.csv")
        raw_dir: Path to data/raw/
        metadata: The metadata dict (mutated in place with new ETags)
        force: If True, skip the ETag check and always re-download

    Returns:
        True if the file was downloaded, False if skipped (unchanged)
    """
    logger.info(f"Checking {name}: {url}")

    # -----------------------------------------------------------------------
    # Step 1: Check current server headers via HEAD request
    # We use HEAD instead of GET to avoid downloading the full file just to
    # check if it changed. TTB's server supports ETag and Last-Modified.
    # -----------------------------------------------------------------------
    headers = {}
    prior = metadata.get(name, {})

    if not force and prior:
        # Send conditional headers so the server can tell us "not modified"
        if prior.get("etag"):
            headers["If-None-Match"] = prior["etag"]
        if prior.get("last_modified"):
            headers["If-Modified-Since"] = prior["last_modified"]

    try:
        # First try a HEAD request to check headers without downloading
        head_resp = requests.head(url, headers=headers, timeout=30, allow_redirects=True)

        # Some servers respond to conditional HEAD with 304
        if head_resp.status_code == 304:
            logger.info(f"  ✓ {name} unchanged (304 Not Modified), skipping download")
            return False

        # Compare ETags manually if the server didn't give us a 304
        server_etag = head_resp.headers.get("ETag", "")
        server_modified = head_resp.headers.get("Last-Modified", "")

        if not force and prior:
            if server_etag and server_etag == prior.get("etag"):
                logger.info(f"  ✓ {name} unchanged (ETag match), skipping download")
                return False
            if server_modified and server_modified == prior.get("last_modified"):
                logger.info(f"  ✓ {name} unchanged (Last-Modified match), skipping download")
                return False

    except requests.RequestException as e:
        # If HEAD fails, fall through to GET — some servers don't support HEAD
        logger.warning(f"  HEAD request failed for {name}: {e}, will try GET")
        server_etag = ""
        server_modified = ""

    # -----------------------------------------------------------------------
    # Step 2: Download the full file via GET
    # -----------------------------------------------------------------------
    try:
        logger.info(f"  Downloading {name}...")
        resp = requests.get(url, timeout=120, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"  ✗ Failed to download {name}: {e}")
        return False

    # Update ETags from the GET response (more reliable than HEAD)
    server_etag = resp.headers.get("ETag", server_etag)
    server_modified = resp.headers.get("Last-Modified", server_modified)

    # -----------------------------------------------------------------------
    # Step 3: Save the file with both a timestamped name and a "latest" name
    # Timestamped: wine_monthly_20240815.csv  (historical record)
    # Latest:      wine_monthly.csv           (stable path for downstream)
    # -----------------------------------------------------------------------
    today = datetime.now().strftime("%Y%m%d")
    stem, ext = os.path.splitext(filename)
    dated_filename = f"{stem}_{today}{ext}"

    # Write the latest (stable) copy — this is what parse.py reads
    latest_path = raw_dir / filename
    with open(latest_path, "wb") as f:
        f.write(resp.content)

    # Write the dated copy — for historical tracking
    dated_path = raw_dir / dated_filename
    shutil.copy2(latest_path, dated_path)

    file_size = len(resp.content)
    logger.info(f"  ✓ Downloaded {name}: {file_size:,} bytes -> {filename}")

    # -----------------------------------------------------------------------
    # Step 4: Update metadata for next run's change detection
    # -----------------------------------------------------------------------
    metadata[name] = {
        "etag": server_etag,
        "last_modified": server_modified,
        "downloaded_at": datetime.now().isoformat(),
        "file_size": file_size,
        "filename": filename,
        "dated_filename": dated_filename,
    }

    return True


def run(force: bool = False) -> dict:
    """
    Download all TTB source files, skipping unchanged ones.

    This is the main entry point for Stage 1. It iterates over all registered
    source files, checks for changes via ETag/Last-Modified, and downloads
    any that are new or updated.

    Args:
        force: If True, re-download everything regardless of cache state

    Returns:
        Dict with download results: {name: {"downloaded": bool, "path": str}}
    """
    raw_dir = _get_raw_dir()
    metadata = _load_metadata(raw_dir)
    results = {}

    logger.info("=" * 60)
    logger.info("Stage 1: Downloading TTB source files")
    logger.info("=" * 60)

    for name, info in SOURCE_FILES.items():
        downloaded = download_file(
            name=name,
            url=info["url"],
            filename=info["filename"],
            raw_dir=raw_dir,
            metadata=metadata,
            force=force,
        )
        results[name] = {
            "downloaded": downloaded,
            "path": str(raw_dir / info["filename"]),
        }

    # Persist metadata so next run can check for changes
    _save_metadata(raw_dir, metadata)

    # Summary
    downloaded_count = sum(1 for r in results.values() if r["downloaded"])
    skipped_count = len(results) - downloaded_count
    logger.info(f"Download complete: {downloaded_count} downloaded, {skipped_count} skipped (unchanged)")

    return results


if __name__ == "__main__":
    run()
