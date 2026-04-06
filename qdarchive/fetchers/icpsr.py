"""
ICPSR / openICPSR platform fetcher.

openICPSR (doi:10.3886/ENNNNNVN) — API blocked (403); records Harvard source URL as inaccessible.

Regular ICPSR (doi:10.3886/ICPSRNNNNNN) — custom system at icpsr.umich.edu;
  no public file API, so only metadata is recorded.
"""
import json
import logging

from qdarchive.utils import year_from_date
from qdarchive.fetchers.base import _insert_file_row

log = logging.getLogger(__name__)


def fetch_icpsr(global_id, item, output_dir, db, seen_urls, download, q):
    doi      = f"https://doi.org/{global_id.replace('doi:', '')}"
    local_id = global_id.replace("doi:10.3886/", "").replace("doi:", "")

    is_open_icpsr = local_id.upper().startswith("E")

    if is_open_icpsr:
        # openICPSR blocks all API access (403) regardless of credentials.
        # Record the Harvard source URL immediately without attempting any API call.
        harvard_url = item.get("url", doi)
        source_url  = f"https://www.openicpsr.org/openicpsr/project/{local_id}"
        log.info(f"openICPSR: {global_id} — API inaccessible, recording Harvard source URL")
        _insert_file_row(db, seen_urls, {
            "url": harvard_url, "doi": doi, "local_dir": "",
            "local_filename": "", "file_type": "inaccessible",
            "file_extension": "", "file_size_bytes": None,
            "checksum_md5": "", "download_timestamp": "",
            "source_name": "openICPSR", "source_url": source_url,
            "title": item.get("name", ""),
            "description": (item.get("description", "") or "")[:500],
            "year": year_from_date(item.get("published_at", "")),
            "keywords": "; ".join(item.get("keywords", [])),
            "language": "", "author": "; ".join(item.get("authors", [])),
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 1,
            "access_note": "openICPSR API blocked (403) — requires institutional access at openicpsr.org",
        })

    else:
        # Regular ICPSR — no public file API
        source_url = item.get("url", doi)
        log.info(f"ICPSR: {global_id} — no public API, recording metadata only")
        _insert_file_row(db, seen_urls, {
            "url": source_url, "doi": doi, "local_dir": "",
            "local_filename": "", "file_type": "inaccessible",
            "file_extension": "", "file_size_bytes": None,
            "checksum_md5": "", "download_timestamp": "",
            "source_name": "ICPSR", "source_url": source_url,
            "title": item.get("name", ""),
            "description": (item.get("description", "") or "")[:500],
            "year": year_from_date(item.get("published_at", "")),
            "keywords": "; ".join(item.get("keywords", [])),
            "language": "", "author": "; ".join(item.get("authors", [])),
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 1,
            "access_note": "ICPSR: requires free account registration and per-study terms acceptance at icpsr.umich.edu",
        })
