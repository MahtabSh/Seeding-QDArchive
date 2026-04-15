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


def fetch_zenodo(
    global_id, item, output_dir, db, seen_urls, download, q,
    allowed_extensions: set[str] | None = None,
    max_file_size_bytes: int | None = None,
    gdrive_service=None,
    gdrive_folder_cache: dict | None = None,
    gdrive_root_id: str | None = None,
):
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
        # No project_file row — absence of rows signals no files available
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

        # ── Extension and size filters ────────────────────────────────────────
        if download:
            if allowed_extensions and fext not in allowed_extensions:
                _insert_file_row(db, seen_urls, {
                    **common, "url": furl, "local_dir": "", "local_filename": fname,
                    "file_type": ftype, "file_extension": fext,
                    "file_size_bytes": fsize, "checksum_md5": "",
                    "download_timestamp": "", "access_note": "extension filtered",
                })
                db.insert_project_file(proj_id, fname, fext, "SUCCEEDED", file_size_bytes=fsize)
                continue
            if max_file_size_bytes and fsize and fsize > max_file_size_bytes:
                log.info(f"Zenodo: skipping {fname} ({fsize:,} bytes > limit)")
                _insert_file_row(db, seen_urls, {
                    **common, "url": furl, "local_dir": "", "local_filename": fname,
                    "file_type": ftype, "file_extension": fext,
                    "file_size_bytes": fsize, "checksum_md5": "",
                    "download_timestamp": "", "access_note": "file too large",
                })
                db.insert_project_file(proj_id, fname, fext, "FAILED_TOO_LARGE", file_size_bytes=fsize)
                continue

        checksum   = ""
        ts         = ""
        gdrive_id  = None
        gdrive_url = ""
        saved_local_dir  = local_dir
        saved_local_name = fname

        if download:
            if gdrive_service and gdrive_root_id:
                # Skip Drive API entirely if this file was already uploaded in a previous run
                _already = db.conn.execute(
                    "SELECT gdrive_file_id FROM files "
                    "WHERE url=? AND gdrive_file_id IS NOT NULL AND gdrive_file_id != ''",
                    (furl,)
                ).fetchone()
                if _already:
                    log.debug(f"Zenodo: '{fname}' already on Drive ({_already[0]}) — skipping")
                    continue
                # ── Stream directly to Google Drive — no local disk write ──────
                from qdarchive.gdrive import _get_or_create_folder, file_url, folder_url, make_public, stream_url_to_drive
                cache = gdrive_folder_cache if gdrive_folder_cache is not None else {}
                # repo folder
                repo_key = "/zenodo"
                if repo_key not in cache:
                    cache[repo_key] = _get_or_create_folder(gdrive_service, "zenodo", gdrive_root_id)
                repo_folder_id = cache[repo_key]
                # project folder
                proj_key = f"/zenodo/{safe_id}"
                if proj_key not in cache:
                    fid = _get_or_create_folder(gdrive_service, safe_id, repo_folder_id)
                    make_public(gdrive_service, fid)
                    cache[proj_key] = fid
                proj_folder_id = cache[proj_key]

                gdrive_id = stream_url_to_drive(gdrive_service, furl, fname, proj_folder_id)
                if gdrive_id:
                    gdrive_url       = file_url(gdrive_id)
                    ts               = datetime.now(timezone.utc).isoformat()
                    saved_local_dir  = ""   # not saved locally
                    saved_local_name = ""
                    proj_folder_url  = folder_url(proj_folder_id)
                    db.conn.execute("""
                        UPDATE projects SET gdrive_folder_id=?, gdrive_folder_url=?
                        WHERE download_project_folder=?
                          AND (gdrive_folder_id IS NULL OR gdrive_folder_id='')
                    """, (proj_folder_id, proj_folder_url, safe_id))
                else:
                    log.warning(f"Zenodo: Drive upload failed for '{fname}' after 3 attempts — metadata saved, file skipped")
                time.sleep(0.5)
            else:
                # ── Normal download to local disk ─────────────────────────────
                dest = output_dir / "zenodo" / safe_id / fname
                if download_file(furl, dest):
                    checksum = md5(dest)
                    ts = datetime.now(timezone.utc).isoformat()
                time.sleep(1)

        _insert_file_row(db, seen_urls, {
            **common,
            "url": furl,
            "local_dir": saved_local_dir,
            "local_filename": saved_local_name, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "gdrive_file_id": gdrive_id or "",
            "gdrive_url": gdrive_url,
            "access_note": "",
        })
        pf_id = db.insert_project_file(proj_id, fname, fext, "SUCCEEDED", file_size_bytes=fsize)
        if gdrive_id:
            db.conn.execute(
                "UPDATE project_files SET gdrive_file_id=?, gdrive_url=? WHERE id=?",
                (gdrive_id, gdrive_url, pf_id),
            )
