"""
Zenodo platform fetcher.

DOI format : doi:10.5281/zenodo.NNNNNN
API        : GET https://zenodo.org/api/records/{record_id}
Files      : .files[].links.self  (direct download URL)
Auth       : none required for public records
"""
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from qdarchive.http_client import get_json, download_file
from qdarchive.utils import classify_file, safe_folder, year_from_date, md5
from qdarchive.fetchers.base import _insert_file_row

log = logging.getLogger(__name__)


def fetch_zenodo(global_id, item, output_dir, db, seen_urls, download, q):
    record_id  = global_id.replace("doi:", "").split("zenodo.")[-1].split(".")[0]
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    source_url = f"https://zenodo.org/records/{record_id}"

    meta = get_json(f"https://zenodo.org/api/records/{record_id}")
    if not meta:
        log.warning(f"Zenodo: could not fetch record {record_id}")
        return

    title       = meta.get("metadata", {}).get("title", item.get("name", ""))
    description = (meta.get("metadata", {}).get("description", "") or "")[:500]
    pub_date    = meta.get("metadata", {}).get("publication_date", "")
    creators    = meta.get("metadata", {}).get("creators", [])
    author      = "; ".join(c.get("name", "") for c in creators)
    keywords    = "; ".join(meta.get("metadata", {}).get("keywords", []))
    license_str = (meta.get("metadata", {}).get("license", {}) or {}).get("id", "")
    license_url = f"https://creativecommons.org/licenses/{license_str}" if license_str.startswith("cc") else ""

    files = meta.get("files", [])
    if not files:
        log.info(f"Zenodo: no files for {record_id}")
        return

    log.info(f"Zenodo: record {record_id} — {len(files)} file(s)")
    safe_id   = safe_folder(global_id)
    local_dir = f"zenodo/{safe_id}"

    for f in files:
        fname = f.get("key", f.get("filename", ""))
        furl  = f.get("links", {}).get("self", "")
        fsize = f.get("size", 0)
        ftype = classify_file(fname)
        fext  = Path(fname).suffix.lower()

        if not furl:
            continue

        checksum = ""
        ts = ""

        if download:
            dest = output_dir / "zenodo" / safe_id / fname
            if download_file(furl, dest):
                checksum = md5(dest)
                ts = datetime.now(timezone.utc).isoformat()
            time.sleep(1)

        _insert_file_row(db, seen_urls, {
            "url": furl, "doi": doi,
            "local_dir": local_dir if download else "",
            "local_filename": fname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "source_name": "Zenodo", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": keywords,
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": license_str, "license_url": license_url,
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0,
            "access_note": "Zenodo API error — record may be private or deleted",
        })
