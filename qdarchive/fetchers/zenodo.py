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

ZENODO_REPO_ID  = 1   # repository ID for Zenodo in the projects table
ZENODO_REPO_URL = "https://zenodo.org"

log = logging.getLogger(__name__)


def fetch_zenodo(global_id, item, output_dir, db, seen_urls, download, q):
    record_id  = global_id.replace("doi:", "").split("zenodo.")[-1].split(".")[0]
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    source_url = f"https://zenodo.org/records/{record_id}"

    # Fast path: crawl_zenodo passes the full search-result record in item["_record"].
    # Use it directly to avoid a redundant API call.  Fall back to a detail call
    # only when invoked from other crawlers (e.g. Harvard) that don't supply it.
    preloaded = item.get("_record") if item else None
    if preloaded:
        meta = preloaded
        meta_section = preloaded.get("metadata", {})
    else:
        meta = get_json(f"https://zenodo.org/api/records/{record_id}")
        if not meta:
            log.warning(f"Zenodo: could not fetch record {record_id}")
            return
        meta_section = meta.get("metadata", {})

    title       = meta_section.get("title", item.get("name", ""))
    description = (meta_section.get("description", "") or "")[:500]
    pub_date    = meta_section.get("publication_date", "")
    creators    = meta_section.get("creators", [])
    author      = "; ".join(c.get("name", "") for c in creators)
    keywords    = "; ".join(meta_section.get("keywords", []))
    license_str = (meta_section.get("license", {}) or {}).get("id", "")
    license_url = f"https://creativecommons.org/licenses/{license_str}" if license_str.startswith("cc") else ""
    access_right = meta_section.get("access_right", "open")

    files = meta.get("files", [])

    safe_id   = safe_folder(global_id)
    local_dir = f"zenodo/{safe_id}"

    common = {
        "doi": doi, "source_name": "Zenodo", "source_url": source_url,
        "title": title, "description": description,
        "year": year_from_date(pub_date), "keywords": keywords,
        "language": "", "author": author,
        "uploader_name": "", "uploader_email": "",
        "license": license_str, "license_url": license_url,
        "matched_query": json.dumps([q], ensure_ascii=False),
        "manual_download": 0,
    }

    # ── Write normalised v2 project row ──────────────────────────────────────
    proj_id = db.insert_project({
        "query_string":               json.dumps([q], ensure_ascii=False),
        "repository_id":              ZENODO_REPO_ID,
        "repository_url":             ZENODO_REPO_URL,
        "project_url":                source_url,
        "version":                    "",
        "title":                      title,
        "description":                description,
        "language":                   "",
        "doi":                        doi,
        "upload_date":                pub_date[:10] if pub_date else "",
        "download_date":              datetime.now(timezone.utc).isoformat(),
        "download_repository_folder": "zenodo",
        "download_project_folder":    safe_id,
        "download_version_folder":    "",
        "download_method":            "API",
    })
    for kw in [k.strip() for k in keywords.split(";") if k.strip()]:
        db.insert_keyword(proj_id, kw)
    for person in [p.strip() for p in author.split(";") if p.strip()]:
        db.insert_person_role(proj_id, person, "AUTHOR")
    if license_str:
        db.insert_license(proj_id, license_str)

    if not files:
        note = (
            "files restricted or embargoed"
            if access_right in ("restricted", "embargoed", "closed")
            else "no files in API response"
        )
        log.info(f"Zenodo: record {record_id} — {note}, saving metadata only")
        _insert_file_row(db, seen_urls, {
            **common,
            "url": source_url,
            "local_dir": "", "local_filename": "",
            "file_type": "no_files", "file_extension": "",
            "file_size_bytes": None, "checksum_md5": "",
            "download_timestamp": "",
            "access_note": note,
        })
        db.insert_project_file(proj_id, "", "", "NO_FILES")
        return

    log.info(f"Zenodo: record {record_id} — {len(files)} file(s)")

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
            **common,
            "url": furl,
            "local_dir": local_dir,
            "local_filename": fname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "access_note": "",
        })
        db.insert_project_file(proj_id, fname, fext, "SUCCEEDED")
