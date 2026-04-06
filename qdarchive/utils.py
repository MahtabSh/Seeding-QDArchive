"""
Pure utility functions: file classification, path sanitisation, checksums.
No project-level dependencies — safe to import from anywhere.
"""
import hashlib
import logging
import random
import time
from pathlib import Path

log = logging.getLogger(__name__)

# ── Request counter for periodic long pauses ─────────────────────────────────
_request_counter = 0

# ── QDA software file extensions ─────────────────────────────────────────────
QDA_EXTENSIONS: set[str] = {
    ".qdpx", ".qdc",
    ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
    ".mx24", ".mx24bac", ".mc24", ".mex24",
    ".mx22", ".mex22", ".mx20", ".mx18", ".mx12",
    ".mx11", ".mx5", ".mx4", ".mx3", ".mx2", ".m2k",
    ".loa", ".sea", ".mtr", ".mod",
    ".nvp", ".nvpx",
    ".atlasproj", ".hpr7",
    ".ppj", ".pprj", ".qlt",
    ".f4p", ".qpd",
}

# ── Primary data format extensions ───────────────────────────────────────────
PRIMARY_EXTENSIONS: set[str] = {
    ".pdf", ".txt", ".doc", ".docx", ".rtf", ".odt",
    ".csv", ".xls", ".xlsx", ".ods",
    ".mp3", ".mp4", ".wav", ".m4a", ".ogg", ".avi", ".mov",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".zip", ".tar", ".gz", ".7z",
}


def human_delay(min_s: float = 2.0, max_s: float = 6.0, label: str = "") -> None:
    """
    Sleep for a random duration in [min_s, max_s] to mimic human browsing pace.

    Every 30–50 requests also inserts a longer "reading break" (20–45 s) so
    the request pattern does not look like a bot with a fixed interval.
    """
    global _request_counter
    _request_counter += 1

    # Periodic long pause every 30–50 requests
    if _request_counter % random.randint(30, 50) == 0:
        pause = random.uniform(20, 45)
        log.debug(f"human_delay: long reading pause {pause:.1f}s (request #{_request_counter})")
        time.sleep(pause)
        return

    delay = random.uniform(min_s, max_s)
    if label:
        log.debug(f"human_delay: {delay:.1f}s ({label})")
    time.sleep(delay)


def classify_file(filename: str) -> str:
    """Return 'analysis', 'primary', or 'additional' based on file extension."""
    ext = Path(filename).suffix.lower().strip()
    if ext in QDA_EXTENSIONS:
        return "analysis"
    if ext in PRIMARY_EXTENSIONS:
        return "primary"
    return "additional"


def safe_folder(s: str) -> str:
    """Sanitise a string for use as a directory name."""
    return s.replace("/", "_").replace(":", "_")


def year_from_date(d: str) -> str:
    """Extract the 4-digit year from an ISO date string."""
    return d[:4] if d and len(d) >= 4 else ""


def md5(path: Path) -> str:
    """Compute MD5 hex digest of a local file."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
