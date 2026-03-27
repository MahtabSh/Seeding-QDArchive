"""
Platform fetchers package.

Exports:
  _insert_file_row      — dedup-safe legacy DB insert
  PLATFORM_DOWNLOADERS  — maps DOI prefix → fetcher function
"""
from qdarchive.fetchers.base import _insert_file_row
from qdarchive.fetchers.zenodo import fetch_zenodo
from qdarchive.fetchers.osf import fetch_osf
from qdarchive.fetchers.figshare import fetch_figshare
from qdarchive.fetchers.icpsr import fetch_icpsr
from qdarchive.fetchers.dryad import fetch_dryad

# Maps DOI prefix → downloader function.
# Add new platforms here; key must match a NON_DATAVERSE_PREFIXES entry in config.
PLATFORM_DOWNLOADERS: dict = {
    "10.5281":  fetch_zenodo,
    "10.17605": fetch_osf,
    "10.6084":  fetch_figshare,
    "10.3886":  fetch_icpsr,
    "10.5061":  fetch_dryad,
}

__all__ = [
    "_insert_file_row",
    "PLATFORM_DOWNLOADERS",
    "fetch_zenodo", "fetch_osf", "fetch_figshare", "fetch_icpsr", "fetch_dryad",
]
