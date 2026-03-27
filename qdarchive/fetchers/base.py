"""
Shared utilities for platform fetchers.
"""
import logging

log = logging.getLogger(__name__)


def _insert_file_row(db, seen_urls: set, record: dict) -> bool:
    """Insert a record into the legacy files table only if its URL is new."""
    url = record.get("url", "")
    if url and url not in seen_urls:
        seen_urls.add(url)
        db.insert(record)
        return True
    return False
