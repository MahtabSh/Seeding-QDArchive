"""
Dryad platform fetcher.

DOI format : doi:10.5061/dryad.XXXXXXX
API        : GET https://datadryad.org/api/v2/datasets/{encoded_doi}
             GET https://datadryad.org/api/v2/datasets/{encoded_doi}/files
Download   : GET https://datadryad.org/api/v2/files/{file_id}/download
Auth       : none required for public datasets
"""
import json
import logging
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from qdarchive.http_client import get_json, download_file
from qdarchive.utils import classify_file, safe_folder, year_from_date, md5
from qdarchive.fetchers.base import _insert_file_row

log = logging.getLogger(__name__)

_BASE = "https://datadryad.org/api/v2"


def fetch_dryad(global_id, item, output_dir, db, seen_urls, download, q):
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    doi_plain  = global_id.replace("doi:", "")
    enc_doi    = urllib.parse.quote(doi, safe="")
    source_url = item.get("url", doi)

    meta = get_json(f"{_BASE}/datasets/{enc_doi}", retries=2)
    if not meta:
        log.warning(f"Dryad: could not fetch {global_id}")
        _insert_file_row(db, seen_urls, {
            "url": source_url, "doi": doi, "local_dir": "",
            "local_filename": "", "file_type": "inaccessible",
            "file_extension": "", "file_size_bytes": None,
            "checksum_md5": "", "download_timestamp": "",
            "source_name": "Dryad", "source_url": source_url,
            "title": item.get("name", ""),
            "description": (item.get("description", "") or "")[:500],
            "year": year_from_date(item.get("published_at", "")),
            "keywords": "; ".join(item.get("keywords", [])),
            "language": "", "author": "; ".join(item.get("authors", [])),
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0,
            "access_note": "Dryad API error — dataset may be embargoed or unavailable",
        })
        return

    title       = meta.get("title", item.get("name", ""))
    description = (meta.get("abstract", "") or "")[:500]
    pub_date    = meta.get("publicationDate", "")
    author      = "; ".join(
        f"{a.get('lastName','')}, {a.get('firstName','')}".strip(", ")
        for a in meta.get("authors", [])
    )
    keywords    = "; ".join(s.get("subject", "") for s in meta.get("subjects", []))
    license_str = (meta.get("license", "") or "")
    license_url = (
        f"https://creativecommons.org/licenses/{license_str.lower()}"
        if license_str.startswith("CC") else ""
    )

    files_data = get_json(f"{_BASE}/datasets/{enc_doi}/files", retries=2)
    files = (files_data or {}).get("_embedded", {}).get("stash:files", [])

    if not files:
        log.info(f"Dryad: no files for {global_id}")
        return

    log.info(f"Dryad: {global_id} — {len(files)} file(s)")
    safe_id   = safe_folder(global_id)
    local_dir = f"dryad/{safe_id}"

    for f in files:
        fname = f.get("path", f.get("_links", {}).get("self", {}).get("href", ""))
        fname = fname.split("/")[-1]
        fsize = f.get("size", 0)
        fid   = f.get("_links", {}).get("stash:file-download", {}).get("href", "")
        if not fid:
            self_href = f.get("_links", {}).get("self", {}).get("href", "")
            fid = self_href.replace("/api/v2/files/", "").split("/")[0] if self_href else ""
        furl = f"https://datadryad.org/api/v2/files/{fid}/download" if fid else ""
        if not furl:
            continue

        ftype    = classify_file(fname)
        fext     = Path(fname).suffix.lower()
        checksum = ""
        ts       = ""

        if download:
            dest = output_dir / "dryad" / safe_id / fname
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
            "source_name": "Dryad", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": keywords,
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": license_str, "license_url": license_url,
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0, "access_note": "",
        })
