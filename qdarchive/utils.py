"""
Pure utility functions: file classification, path sanitisation, checksums.
No project-level dependencies — safe to import from anywhere.
"""
import hashlib
from pathlib import Path

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
