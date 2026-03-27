#!/usr/bin/env python3
"""
QDArchive Seeding Pipeline  v6
================================
Harvests qualitative research data from Harvard Dataverse and stores
metadata in an SQLite database.

By default only metadata is recorded (filenames, URLs, etc.).
Pass --download to actually fetch files to disk.

Usage:
  # metadata scan only (fast, no files downloaded)
  python qdarchive_pipeline.py

  # full download
  python qdarchive_pipeline.py --download

  # limit records collected
  python qdarchive_pipeline.py --max-records 200

  # custom output paths
  python qdarchive_pipeline.py --download --output-dir /data/qda --db /data/qda/metadata.sqlite
"""

import argparse
import hashlib
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# Harvard: https://dataverse.harvard.edu → top-right menu → API Token
# Borealis: https://borealisdata.ca      → top-right menu → API Token
# ─────────────────────────────────────────────────────────────────────────────
HARVARD_TOKEN  = "583ab5c1-8557-4202-9c45-a9798cbe95c2"
BOREALIS_TOKEN = "4c0e5038-62ce-4ee4-a320-646cb2d25ef8"
# openICPSR uses Basic Auth (email + password) instead of an API token.
# Enter the email and password you use to log in to openicpsr.org
OPENICPSR_USER = "mahtab.shahsavan@fau.de"
OPENICPSR_PASS = "Mahtabshahsavan@123"

# ─────────────────────────────────────────────────────────────────────────────
# DATAVERSE HOST REGISTRY
# Maps each known Dataverse installation to its base URL and API token.
# Key = substring matched against name_of_dataverse (lowercased) in search results.
# Add new institutions here as needed.
# ─────────────────────────────────────────────────────────────────────────────
HARVARD_BASE = "https://dataverse.harvard.edu"
HARVARD_API  = "https://dataverse.harvard.edu/api"

DATAVERSE_REGISTRY = {
    "harvard":      ("https://dataverse.harvard.edu",  HARVARD_TOKEN),
    "borealis":     ("https://borealisdata.ca",         BOREALIS_TOKEN),
    "dans":         ("https://ssh.datastations.nl",     ""),
    "dataverse.no": ("https://dataverse.no",            ""),
    "dataverseno":  ("https://dataverse.no",            ""),
    "dataverse.nl": ("https://dataverse.nl",            ""),
    "dataverseni":  ("https://dataverse.nl",            ""),
    "openicpsr":    ("https://www.openicpsr.org",       ""),  # Basic Auth — see fetch_icpsr
    "icpsr":        ("https://www.openicpsr.org",       ""),  # Basic Auth — see fetch_icpsr
}

# Maps DOI prefixes to their platform name so the right downloader is called.
# Add new platforms here — the platform name must match a key in PLATFORM_DOWNLOADERS.
NON_DATAVERSE_PREFIXES = {
    "10.3886":  "icpsr",    # ICPSR / openICPSR
    "10.5281":  "zenodo",   # Zenodo
    "10.17605": "osf",      # OSF (Open Science Framework)
    "10.6084":  "figshare", # figshare
    "10.5061":  "dryad",    # Dryad — not Dataverse, has its own API
}

# Add DOI prefixes here to skip silently (no DB row, no download attempt).
SILENT_SKIP_PREFIXES: set = set()

# ─────────────────────────────────────────────────────────────────────────────
# NON-DATAVERSE PLATFORM DOWNLOADERS
# Each function signature:
#   fetch_<platform>(global_id, item, output_dir, db, seen_urls, download, q)
# Returns list of db record dicts inserted (already inserted inside function).
# ─────────────────────────────────────────────────────────────────────────────

def _insert_file_row(db, seen_urls, record: dict):
    """Insert a record only if its URL hasn't been seen before."""
    url = record.get("url", "")
    if url and url not in seen_urls:
        seen_urls.add(url)
        db.insert(record)
        return True
    return False


def fetch_zenodo(global_id, item, output_dir, db, seen_urls, download, q):
    """
    Zenodo REST API  — completely open, no token needed.
    DOI format: doi:10.5281/zenodo.NNNNNN
    API:  GET https://zenodo.org/api/records/{record_id}
    Files: .files[].links.self  (direct download URL)
    """
    record_id = global_id.replace("doi:", "").split("zenodo.")[-1].split(".")[0]
    doi       = f"https://doi.org/{global_id.replace('doi:', '')}"
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
        fname    = f.get("key", f.get("filename", ""))
        furl     = f.get("links", {}).get("self", "")
        fsize    = f.get("size", 0)
        ftype    = classify_file(fname)
        fext     = Path(fname).suffix.lower()

        if not furl:
            continue

        lfname = fname
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
            "local_filename": lfname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "source_name": "Zenodo", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": keywords,
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": license_str, "license_url": license_url,
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0, "access_note": 'Zenodo API error — record may be private or deleted',
        })


def fetch_osf(global_id, item, output_dir, db, seen_urls, download, q):
    """
    OSF REST API — public, no token needed for public projects.
    DOI format:  doi:10.17605/OSF.IO/XXXXX
    API:  GET https://api.osf.io/v2/registrations/{id}/files/
          GET https://api.osf.io/v2/nodes/{id}/files/osfstorage/
    """
    # Extract OSF project ID from DOI
    osf_id    = global_id.replace("doi:", "").split("OSF.IO/")[-1].split("/")[0].lower()
    doi       = f"https://doi.org/{global_id.replace('doi:', '')}"
    source_url = f"https://osf.io/{osf_id}/"

    title       = item.get("name", "")
    description = (item.get("description", "") or "")[:500]
    pub_date    = item.get("published_at", "")
    author      = "; ".join(item.get("authors", []))

    # Try nodes endpoint first, then registrations
    files_data = None
    for endpoint in [f"https://api.osf.io/v2/nodes/{osf_id}/files/osfstorage/",
                     f"https://api.osf.io/v2/registrations/{osf_id}/files/osfstorage/"]:
        resp = get_json(endpoint, retries=2)
        if resp and resp.get("data"):
            files_data = resp["data"]
            break

    if not files_data:
        log.warning(f"OSF: could not list files for {osf_id}")
        _insert_file_row(db, seen_urls, {
            "url": source_url, "doi": doi, "local_dir": "",
            "local_filename": "", "file_type": "inaccessible",
            "file_extension": "", "file_size_bytes": None,
            "checksum_md5": "", "download_timestamp": "",
            "source_name": "OSF", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": "",
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 1, "access_note": 'OSF project may be private or require login',
        })
        return

    log.info(f"OSF: project {osf_id} — {len(files_data)} item(s)")
    safe_id   = safe_folder(global_id)
    local_dir = f"osf/{safe_id}"

    for f in files_data:
        attrs = f.get("attributes", {})
        fname = attrs.get("name", "")
        fsize = attrs.get("size", 0)
        furl  = attrs.get("extra", {}).get("hashes", {})
        # direct download link
        furl  = f.get("links", {}).get("download", "")
        if not furl:
            continue
        ftype = classify_file(fname)
        fext  = Path(fname).suffix.lower()

        lfname = fname
        checksum = ""
        ts = ""

        if download:
            dest = output_dir / "osf" / safe_id / fname
            if download_file(furl, dest):
                checksum = md5(dest)
                ts = datetime.now(timezone.utc).isoformat()
            time.sleep(1)

        _insert_file_row(db, seen_urls, {
            "url": furl, "doi": doi,
            "local_dir": local_dir if download else "",
            "local_filename": lfname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "source_name": "OSF", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": "",
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 1, "access_note": 'OSF project may be private or require login',
        })


def fetch_figshare(global_id, item, output_dir, db, seen_urls, download, q):
    """
    figshare REST API — public articles need no token.
    DOI format: doi:10.6084/m9.figshare.NNNNNNN[.vN]

    Uses DOI search endpoint instead of extracting a numeric ID — more robust
    and handles all DOI variants correctly.
    API: GET https://api.figshare.com/v2/articles?doi=<doi>
         GET https://api.figshare.com/v2/articles/{article_id}
    Files: .files[].download_url
    """
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    doi_plain  = global_id.replace("doi:", "")
    source_url = item.get("url", doi)

    # Step 1: resolve DOI to article_id via figshare search
    article_id = None
    search     = get_json(
        "https://api.figshare.com/v2/articles",
        params={"doi": doi_plain},
        retries=2,
    )
    if search and isinstance(search, list) and len(search) > 0:
        article_id = search[0].get("id")
        source_url = search[0].get("url_public_html", source_url)

    if not article_id:
        # Fallback: try extracting numeric ID directly from DOI
        try:
            article_id = int(global_id.replace("doi:", "").split("figshare.")[-1].split(".")[0])
        except Exception:
            article_id = None

    # Sanity-check: article IDs < 100 are almost certainly bad DOI parses
    if article_id is not None and article_id < 100:
        log.warning(f"figshare: suspicious article ID {article_id} for {global_id} — skipping")
        article_id = None

    if not article_id:
        log.warning(f"figshare: could not resolve article ID for {global_id}")
        _insert_file_row(db, seen_urls, {
            "url": source_url, "doi": doi, "local_dir": "",
            "local_filename": "", "file_type": "inaccessible",
            "file_extension": "", "file_size_bytes": None,
            "checksum_md5": "", "download_timestamp": "",
            "source_name": "figshare", "source_url": source_url,
            "title": item.get("name", ""),
            "description": (item.get("description", "") or "")[:500],
            "year": year_from_date(item.get("published_at", "")),
            "keywords": "", "language": "",
            "author": "; ".join(item.get("authors", [])),
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0, "access_note": "figshare DOI could not be resolved to an article ID",
        })
        return

    # Step 2: fetch full article metadata
    meta = get_json(f"https://api.figshare.com/v2/articles/{article_id}", retries=2)
    if not meta:
        log.warning(f"figshare: could not fetch article {article_id}")
        _insert_file_row(db, seen_urls, {
            "url": source_url, "doi": doi, "local_dir": "",
            "local_filename": "", "file_type": "inaccessible",
            "file_extension": "", "file_size_bytes": None,
            "checksum_md5": "", "download_timestamp": "",
            "source_name": "figshare", "source_url": source_url,
            "title": item.get("name", ""), "description": (item.get("description","") or "")[:500],
            "year": year_from_date(item.get("published_at","")), "keywords": "",
            "language": "", "author": "; ".join(item.get("authors",[])),
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0, "access_note": "figshare API error — article may be private or deleted",
        })
        return

    title       = meta.get("title", item.get("name", ""))
    description = (meta.get("description", "") or "")[:500]
    pub_date    = meta.get("published_date", "")
    authors     = meta.get("authors", [])
    author      = "; ".join(a.get("full_name", "") for a in authors)
    license_str = (meta.get("license", {}) or {}).get("name", "")
    license_url = (meta.get("license", {}) or {}).get("url",  "")
    raw_tags = meta.get("tags", [])
    keywords = "; ".join(
        t if isinstance(t, str) else t.get("title", t.get("value", ""))
        for t in raw_tags
    )

    files = meta.get("files", [])
    if not files:
        log.info(f"figshare: no files for article {article_id}")
        return

    log.info(f"figshare: article {article_id} — {len(files)} file(s)")
    safe_id   = safe_folder(global_id)
    local_dir = f"figshare/{safe_id}"

    for f in files:
        fname = f.get("name", "")
        furl  = f.get("download_url", "")
        fsize = f.get("size", 0)
        if not furl:
            continue
        ftype = classify_file(fname)
        fext  = Path(fname).suffix.lower()

        lfname = fname
        checksum = ""
        ts = ""

        if download:
            dest = output_dir / "figshare" / safe_id / fname
            if download_file(furl, dest):
                checksum = md5(dest)
                ts = datetime.now(timezone.utc).isoformat()
            time.sleep(1)

        _insert_file_row(db, seen_urls, {
            "url": furl, "doi": doi,
            "local_dir": local_dir if download else "",
            "local_filename": lfname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "source_name": "figshare", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": keywords,
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": license_str, "license_url": license_url,
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0, "access_note": 'figshare API error — article may be private or deleted',
        })


def fetch_icpsr(global_id, item, output_dir, db, seen_urls, download, q):
    """
    Handles both ICPSR variants:

    openICPSR (doi:10.3886/ENNNNNVN) — runs Dataverse software at
      https://www.openicpsr.org — full Dataverse API, uses Basic Auth (email+password)
      for public datasets.

    Regular ICPSR (doi:10.3886/ICPSRNNNNNN) — custom system at icpsr.umich.edu,
      no public file API. Records metadata + landing page only.
    """
    doi      = f"https://doi.org/{global_id.replace('doi:', '')}"
    local_id = global_id.replace("doi:10.3886/", "").replace("doi:", "")

    # openICPSR IDs start with E (e.g. E101440V1)
    is_open_icpsr = local_id.upper().startswith("E")

    if is_open_icpsr:
        # Treat as a standard Dataverse installation
        log.info(f"openICPSR (Dataverse): {global_id}")
        synthetic = {
            "global_id":         global_id,
            "type":              "dataset",
            "name":              item.get("name", ""),
            "description":       item.get("description", ""),
            "published_at":      item.get("published_at", ""),
            "url":               doi,   # DOI redirect → resolve_host will find openicpsr.org
            "name_of_dataverse": "openICPSR",
            "keywords":          item.get("keywords", []),
            "authors":           item.get("authors", []),
        }
        import base64
        item_host  = "https://www.openicpsr.org"
        item_api   = f"{item_host}/api"
        _b64       = base64.b64encode(f"{OPENICPSR_USER}:{OPENICPSR_PASS}".encode()).decode()
        item_auth  = {"Authorization": f"Basic {_b64}"}
        source_url = f"{item_host}/dataset.xhtml?persistentId={global_id}"

        detail = get_json(
            f"{item_api}/datasets/:persistentId/",
            params={"persistentId": global_id},
            headers=item_auth,
            retries=2,
        )
        if not detail or detail.get("status") != "OK":
            log.warning(f"openICPSR: could not fetch {global_id} — recording as inaccessible")
            _insert_file_row(db, seen_urls, {
                "url": source_url, "doi": doi, "local_dir": "",
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
                "manual_download": 1, "access_note": "openICPSR API error — set OPENICPSR_USER and OPENICPSR_PASS",
            })
            return

        ds     = detail["data"]
        latest = ds.get("latestVersion", {})
        files  = latest.get("files", [])
        if not files:
            log.info(f"openICPSR: no files for {global_id}")
            return

        log.info(f"openICPSR: {global_id} — {len(files)} file(s)")
        safe_id   = safe_folder(global_id)
        local_dir = f"openicpsr/{safe_id}"

        # Extract citation metadata
        citation_fields = (
            latest.get("metadataBlocks", {}).get("citation", {}).get("fields", [])
        )
        fbn    = {f["typeName"]: f for f in citation_fields}
        author = "; ".join(
            v.get("authorName", {}).get("value", "")
            for v in (fbn.get("author", {}).get("value") or [])
        )
        keywords = "; ".join(
            v.get("keywordValue", {}).get("value", "")
            for v in (fbn.get("keyword", {}).get("value") or [])
        )
        lic_raw     = latest.get("license", {})
        license_str = lic_raw.get("name", "") if isinstance(lic_raw, dict) else str(lic_raw or "")
        license_url = lic_raw.get("uri",  "") if isinstance(lic_raw, dict) else ""

        for fi in files:
            df    = fi.get("dataFile", {})
            fname = df.get("filename", "")
            fid   = df.get("id", "")
            fsize = df.get("filesize", 0)
            if not fid or fi.get("restricted", False):
                continue
            ftype = classify_file(fname)
            fext  = Path(fname).suffix.lower()
            furl  = f"{item_api}/access/datafile/{fid}"

            lfname = fname
            checksum = ""
            ts = ""

            if download:
                dest = output_dir / "openicpsr" / safe_id / fname
                if download_file(furl, dest, extra_headers=item_auth):
                    checksum = md5(dest)
                    ts = datetime.now(timezone.utc).isoformat()
                time.sleep(1)

            _insert_file_row(db, seen_urls, {
                "url": furl, "doi": doi,
                "local_dir": local_dir if download else "",
                "local_filename": lfname, "file_type": ftype,
                "file_extension": fext, "file_size_bytes": fsize,
                "checksum_md5": checksum, "download_timestamp": ts,
                "source_name": "openICPSR", "source_url": source_url,
                "title": item.get("name", ds.get("persistentUrl", "")),
                "description": (item.get("description", "") or "")[:500],
                "year": year_from_date(item.get("published_at", "")),
                "keywords": keywords, "language": "",
                "author": author, "uploader_name": "", "uploader_email": "",
                "license": license_str, "license_url": license_url,
                "matched_query": json.dumps([q], ensure_ascii=False),
                "manual_download": 1, "access_note": 'openICPSR API error — set OPENICPSR_USER and OPENICPSR_PASS',
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
            "manual_download": 1, "access_note": "ICPSR: requires free account registration and per-study terms acceptance at icpsr.umich.edu",
        })


def fetch_dryad(global_id, item, output_dir, db, seen_urls, download, q):
    """
    Dryad REST API (v2) — public datasets are freely downloadable.
    DOI format: doi:10.5061/dryad.XXXXXXX
    API:  GET https://datadryad.org/api/v2/datasets/{encoded_doi}
    Files: GET https://datadryad.org/api/v2/datasets/{encoded_doi}/files
    Download: GET https://datadryad.org/api/v2/files/{file_id}/download
    """
    import urllib.parse
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    doi_plain  = global_id.replace("doi:", "")
    enc_doi    = urllib.parse.quote(doi, safe="")
    source_url = item.get("url", doi)
    base       = "https://datadryad.org/api/v2"

    meta = get_json(f"{base}/datasets/{enc_doi}", retries=2)
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
    authors     = meta.get("authors", [])
    author      = "; ".join(
        f"{a.get('lastName','')}, {a.get('firstName','')}".strip(", ")
        for a in authors
    )
    keywords    = "; ".join(
        s.get("subject", "") for s in meta.get("subjects", [])
    )
    license_str = (meta.get("license", "") or "")
    license_url = f"https://creativecommons.org/licenses/{license_str.lower()}" if license_str.startswith("CC") else ""

    # Fetch file list
    files_data = get_json(f"{base}/datasets/{enc_doi}/files", retries=2)
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
            # fallback: construct from self link
            self_href = f.get("_links", {}).get("self", {}).get("href", "")
            fid = self_href.replace("/api/v2/files/", "").split("/")[0] if self_href else ""
        furl  = f"https://datadryad.org/api/v2/files/{fid}/download" if fid else ""
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


# Routing table: DOI prefix → downloader function
PLATFORM_DOWNLOADERS = {
    "10.5281":  fetch_zenodo,
    "10.17605": fetch_osf,
    "10.6084":  fetch_figshare,
    "10.3886":  fetch_icpsr,
    "10.5061":  fetch_dryad,
}


# Cache of resolved DOI hosts so we don't HEAD the same DOI twice
_DOI_HOST_CACHE: dict = {}


def resolve_host(item: dict) -> tuple:
    """
    Return (base_url, api_token) for a Dataverse search result item.

    Resolution order:
      1. item["url"] is a direct Dataverse URL  → extract host from it
      2. name_of_dataverse matches DATAVERSE_REGISTRY → use registry entry
      3. item["url"] is a DOI redirect           → follow redirect (HEAD),
         extract real host, check registry for a token
      4. Unknown host                            → use host with no token
         (public Dataverse nodes work without a token)
    """
    from urllib.parse import urlparse

    item_url = item.get("url", "")
    parsed   = urlparse(item_url) if item_url else None
    url_host = f"{parsed.scheme}://{parsed.netloc}" if parsed and parsed.netloc else ""

    # ── 1. Direct Dataverse URL ────────────────────────────────────────────
    if url_host and "doi.org" not in url_host:
        for base_url, token in DATAVERSE_REGISTRY.values():
            if url_host.rstrip("/") == base_url.rstrip("/"):
                return base_url, token
        return url_host, ""   # unknown host, try without token

    # ── 2. Match by name_of_dataverse before hitting the network ──────────
    dv_name = (item.get("name_of_dataverse", "") or "").lower().strip()
    for key, (base_url, token) in DATAVERSE_REGISTRY.items():
        if key in dv_name:
            return base_url, token

    # ── 3. Follow DOI redirect to find real host ───────────────────────────
    if item_url and "doi.org" in url_host:
        if item_url in _DOI_HOST_CACHE:
            real_host = _DOI_HOST_CACHE[item_url]
        else:
            try:
                r = SESSION.head(item_url, allow_redirects=True, timeout=10)
                rp = urlparse(r.url)
                real_host = f"{rp.scheme}://{rp.netloc}"
                _DOI_HOST_CACHE[item_url] = real_host
                log.debug(f"DOI {item_url} resolved to {real_host}")
            except Exception as e:
                log.debug(f"Could not follow DOI redirect {item_url}: {e}")
                real_host = ""

        if real_host:
            # Check registry for a matching token
            for base_url, token in DATAVERSE_REGISTRY.values():
                if real_host.rstrip("/") == base_url.rstrip("/"):
                    return base_url, token
            return real_host, ""   # known host, no token in registry

    # ── 4. Give up — unknown institution, no token ────────────────────────
    return HARVARD_BASE, ""

# ─────────────────────────────────────────────────────────────────────────────
# SEARCH QUERIES
# Three tiers: file extensions → software names → methodology keywords
# The matched_query column records which query found each dataset.
# ─────────────────────────────────────────────────────────────────────────────

    # ".qdpx", ".qdc",
    # ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
    # ".mx24", ".mx24bac", ".mc24", ".mex24",
    # ".mx22", ".mex22", ".mx20", ".mx18", ".mx12",
    # ".mx11", ".mx5", ".mx4", ".mx3", ".mx2", ".m2k",
    # ".loa", ".sea", 
        # ── Tier 1: QDA file extensions ───────────────────────────
    # ".mtr", ".mod",
    # ".nvp", ".nvpx",
    # ".atlasproj", ".hpr7",
    # ".ppj", ".pprj", ".qlt",
    # ".f4p", ".qpd",
    # # ── Tier 2: QDA software names ────────────────────────────
    # "NVivo",
    # "MAXQDA",
    # "ATLAS.ti",
    # "Dedoose",
    # "QDA Miner",
    #     "f4analyse",
    # "Quirkos",
    # "CAQDAS",
    # "REFI-QDA",
    # # ── Tier 3: methodology keywords ──────────────────────────
    # "qdpx",
    # "interview study",
    # "qualitative research data",
    # "qualitative data analysis",
    # "thematic analysis",
    # "grounded theory",
    # "interview transcripts",
    # "focus group transcripts",
    # "qualitative interviews",
    # "semi-structured interviews",
QDA_QUERIES = [
    "oral history",
    "ethnographic data",
    "field notes qualitative",
    "coding qualitative",
    "qualitative data archive",

    # ── Tier 4: data collection methods ───────────────────────
    "in-depth interviews",
    "narrative inquiry",
    "life history interviews",
    "biographical interviews",
    "expert interviews",
    "participant observation",
    "ethnographic fieldwork",
    "discourse analysis data",
    "conversation analysis data",
    "content analysis qualitative",
    "photovoice",
    "diary studies qualitative",

    # ── Tier 5: transcripts and raw data ──────────────────────
    "interview transcript",
    "verbatim transcript",
    "audio recording interview",
    "video recording interview",
    "coded transcript",
    "research transcript",
    "focus group data",
    "group interview data",

    # ── Tier 6: discipline-specific qualitative data ───────────
    "qualitative health research",
    "qualitative nursing research",
    "qualitative psychology data",
    "qualitative sociology data",
    "qualitative education research",
    "qualitative social work",
    "qualitative political science",
    "phenomenological study data",
    "interpretive phenomenological analysis",
    "IPA qualitative",
    "case study qualitative",
    "action research qualitative",

    # ── Tier 7: archiving and sharing terms ────────────────────
    "qualitative data sharing",
    "qualitative data reuse",
    "secondary analysis qualitative",
    "archived interviews",
    "deposited interviews",
    "UK Data Archive qualitative",
    "UKDA qualitative",
    "Qualidata",
    "CESSDA qualitative",
    "data replication qualitative",

    # ── Tier 8: mixed-methods (qualitative component) ──────────
    "mixed methods qualitative",
    "qual strand",
    "qualitative component",
    "interview data mixed methods",

    # ── Tier 9: language-specific (common in European repos) ───
    "qualitative Forschung",        # German: qualitative research
    "Interviewdaten",               # German: interview data
    "Leitfadeninterview",           # German: semi-structured interview
    "données qualitatives",         # French: qualitative data
    "entretiens qualitatifs",       # French: qualitative interviews
    "investigación cualitativa",    # Spanish: qualitative research
    "pesquisa qualitativa",         # Portuguese: qualitative research
]

# Extension queries (.qdpx, .nvpx …) — Dataverse searches filenames so these
# are already precise. Searched as bare terms.
QDA_EXTENSION_QUERIES = [q for q in QDA_QUERIES if q.startswith(".")]

# Keyword queries — searched with field scoping to reduce noise:
#   title:"<term>"   surfaces datasets whose TITLE mentions the term
#   keyword:"<term>" surfaces datasets tagged with the term in subject/keyword fields
# Both are searched for every keyword query and results are deduplicated.
QDA_KEYWORD_QUERIES = [q for q in QDA_QUERIES if not q.startswith(".")]

# ─────────────────────────────────────────────────────────────────────────────
# FILE EXTENSION CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────────────
QDA_EXTENSIONS = {
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

PRIMARY_EXTENSIONS = {
    ".pdf", ".txt", ".doc", ".docx", ".rtf", ".odt",
    ".csv", ".xls", ".xlsx", ".ods",
    ".mp3", ".mp4", ".wav", ".m4a", ".ogg", ".avi", ".mov",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".zip", ".tar", ".gz", ".7z",
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "QDArchivePipeline/6.0 (research; oss.cs.fau.de)",
})


# ─────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_json(url, params=None, headers=None, retries=5, delay=3):
    """GET a URL and return parsed JSON. Handles 403 rate-limits with long waits."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=(10, 60))

            if r.status_code == 403:
                wait = 30 * (attempt + 1)
                log.warning(f"GET {url} -> 403, waiting {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.HTTPError as e:
            wait = delay * (2 ** attempt)
            log.warning(f"GET {url} attempt {attempt + 1} failed: {e} - retry in {wait}s")
            time.sleep(wait)
        except Exception as e:
            wait = delay * (2 ** attempt)
            log.warning(f"GET {url} attempt {attempt + 1} failed: {e} - retry in {wait}s")
            time.sleep(wait)

    log.error(f"get_json: gave up after {retries} attempts: {url}")
    return None


def download_file(url: str, dest: Path, retries=5, extra_headers: dict = None) -> bool:
    """Stream a remote file to dest. Returns True on success."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.debug(f"Already downloaded: {dest}")
        return True

    for attempt in range(retries):
        try:
            r = SESSION.get(
                url, stream=True, timeout=(10, 120),
                headers=extra_headers or {}, allow_redirects=True,
            )

            if r.status_code == 403:
                wait = 30 * (attempt + 1)
                log.warning(f"Download {url} -> 403, waiting {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
            log.info(f"  -> {dest.name}  ({dest.stat().st_size:,} bytes)")
            return True

        except requests.exceptions.HTTPError as e:
            wait = 2 * (2 ** attempt)
            log.warning(f"Download {url} attempt {attempt + 1} failed: {e} - retry in {wait}s")
            time.sleep(wait)
        except Exception as e:
            wait = 2 * (2 ** attempt)
            log.warning(f"Download {url} attempt {attempt + 1} failed: {e} - retry in {wait}s")
            time.sleep(wait)

    log.error(f"download_file: gave up after {retries} attempts: {url}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────
def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def classify_file(filename: str) -> str:
    ext = Path(filename).suffix.lower().strip()
    if ext in QDA_EXTENSIONS:
        return "analysis"
    if ext in PRIMARY_EXTENSIONS:
        return "primary"
    return "additional"


def safe_folder(s: str) -> str:
    return s.replace("/", "_").replace(":", "_")


def year_from_date(d: str) -> str:
    return d[:4] if d and len(d) >= 4 else ""


# ─────────────────────────────────────────────────────────────────────────────
# SQLITE DATABASE
# ─────────────────────────────────────────────────────────────────────────────
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS files (
    id                 INTEGER  PRIMARY KEY AUTOINCREMENT,

    -- Identity
    url                TEXT NOT NULL,   -- direct download URL; deduplication key
    doi                TEXT,            -- persistent dataset identifier

    -- File
    local_dir          TEXT,            -- e.g. "harvard_dataverse/doi_10_..."
                                        -- empty when --download is NOT used
    local_filename     TEXT,            -- filename (set even in metadata-only mode)
    file_type          TEXT,            -- "analysis" | "primary" | "additional"
    file_extension     TEXT,            -- lowercase, e.g. ".qdpx"
    file_size_bytes    INTEGER,
    checksum_md5       TEXT,            -- empty until file is downloaded

    -- Download
    download_timestamp TEXT,            -- ISO-8601 UTC; empty if not yet downloaded
    source_name        TEXT,            -- "Harvard Dataverse"
    source_url         TEXT,            -- dataset landing page URL

    -- Dataset
    title              TEXT,
    description        TEXT,            -- truncated to 500 chars
    year               TEXT,            -- YYYY
    keywords           TEXT,            -- semicolon-separated
    language           TEXT,

    -- People
    author             TEXT,            -- semicolon-separated credited authors
    uploader_name      TEXT,            -- depositor name
    uploader_email     TEXT,            -- depositor contact email

    -- Legal
    license            TEXT,            -- SPDX id or short name (all datasets collected)
    license_url        TEXT,

    -- Discovery
    matched_query      TEXT,            -- JSON array of queries that surfaced this dataset e.g. '[".qdpx","NVivo"]'

    -- Access
    manual_download    INTEGER DEFAULT 0,  -- 1 = cannot be auto-downloaded, needs manual action
    access_note        TEXT               -- reason: e.g. "requires ICPSR account and terms acceptance"
);
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_url           ON files(url);
CREATE INDEX IF NOT EXISTS idx_doi           ON files(doi);
CREATE INDEX IF NOT EXISTS idx_file_type     ON files(file_type);
CREATE INDEX IF NOT EXISTS idx_matched_query  ON files(matched_query);
CREATE INDEX IF NOT EXISTS idx_manual_download ON files(manual_download);
"""

# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZED SCHEMA (v2) — 5-table design from schema spreadsheet 2026-03-13
# PROJECTS  → one row per dataset/project
# project_files → one row per file within a project (avoids name clash with legacy 'files')
# keywords  → one row per keyword tag
# person_role → one row per person×role pair
# licenses  → one row per license entry
# ─────────────────────────────────────────────────────────────────────────────
_CREATE_PROJECTS_TABLE = """
CREATE TABLE IF NOT EXISTS projects (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string                TEXT,
    repository_id               INTEGER,
    repository_url              TEXT    NOT NULL,
    project_url                 TEXT    NOT NULL,
    version                     TEXT,
    title                       TEXT    NOT NULL,
    description                 TEXT,
    language                    TEXT,
    doi                         TEXT,
    upload_date                 TEXT,
    download_date               TEXT    NOT NULL,
    download_repository_folder  TEXT    NOT NULL,
    download_project_folder     TEXT    NOT NULL,
    download_version_folder     TEXT,
    download_method             TEXT    NOT NULL
);
"""

_CREATE_PROJECT_FILES_TABLE = """
CREATE TABLE IF NOT EXISTS project_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    file_name   TEXT    NOT NULL,
    file_type   TEXT    NOT NULL,
    status      TEXT    NOT NULL
);
"""

_CREATE_KEYWORDS_TABLE = """
CREATE TABLE IF NOT EXISTS keywords (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    keyword     TEXT    NOT NULL
);
"""

_CREATE_PERSON_ROLE_TABLE = """
CREATE TABLE IF NOT EXISTS person_role (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    name        TEXT    NOT NULL,
    role        TEXT    NOT NULL
);
"""

_CREATE_LICENSES_TABLE = """
CREATE TABLE IF NOT EXISTS licenses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    license     TEXT    NOT NULL
);
"""

_CREATE_V2_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_projects_doi         ON projects(doi);
CREATE INDEX IF NOT EXISTS idx_projects_url         ON projects(project_url);
CREATE INDEX IF NOT EXISTS idx_projects_repo        ON projects(repository_id);
CREATE INDEX IF NOT EXISTS idx_pfiles_project       ON project_files(project_id);
CREATE INDEX IF NOT EXISTS idx_keywords_project     ON keywords(project_id);
CREATE INDEX IF NOT EXISTS idx_person_role_project  ON person_role(project_id);
CREATE INDEX IF NOT EXISTS idx_licenses_project     ON licenses(project_id);
"""

_COLUMNS = [
    "url", "doi",
    "local_dir", "local_filename", "file_type", "file_extension",
    "file_size_bytes", "checksum_md5",
    "download_timestamp", "source_name", "source_url",
    "title", "description", "year", "keywords", "language",
    "author", "uploader_name", "uploader_email",
    "license", "license_url",
    "matched_query",
    "manual_download", "access_note",
]


class MetadataDB:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        # Legacy single-table schema
        self.conn.executescript(_CREATE_TABLE + _CREATE_INDEXES)
        # Normalized v2 schema
        self.conn.executescript(
            _CREATE_PROJECTS_TABLE
            + _CREATE_PROJECT_FILES_TABLE
            + _CREATE_KEYWORDS_TABLE
            + _CREATE_PERSON_ROLE_TABLE
            + _CREATE_LICENSES_TABLE
            + _CREATE_V2_INDEXES
        )
        # Migrate existing databases — add new columns if they don't exist yet
        for col, definition in [
            ("manual_download", "INTEGER DEFAULT 0"),
            ("access_note",     "TEXT"),
        ]:
            try:
                self.conn.execute(f"ALTER TABLE files ADD COLUMN {col} {definition}")
                self.conn.commit()
                log.info(f"DB migration: added column '{col}'")
            except Exception:
                pass  # column already exists
        self.conn.commit()
        log.info(f"Database ready: {path}")

    def seen_urls(self) -> set:
        return {r["url"] for r in self.conn.execute("SELECT url FROM files").fetchall()}

    def seen_dois(self) -> set:
        """Return all dataset DOIs already recorded (for cross-query dedup)."""
        return {r["doi"] for r in self.conn.execute(
            "SELECT DISTINCT doi FROM files WHERE doi IS NOT NULL AND doi != ''"
        ).fetchall()}

    def append_query(self, doi: str, new_query: str):
        """
        Add new_query to the matched_query JSON array for all rows with this DOI.
        e.g.  '[".qdpx"]'  ->  '[".qdpx", "NVivo"]'
        """
        import json as _json
        rows = self.conn.execute(
            "SELECT id, matched_query FROM files WHERE doi = ?", (doi,)
        ).fetchall()
        for row in rows:
            raw = row["matched_query"] or "[]"
            try:
                parts = _json.loads(raw)
                if not isinstance(parts, list):
                    parts = [raw]   # migrate old semicolon value
            except Exception:
                # migrate old semicolon-separated value
                parts = [p.strip() for p in raw.split(";") if p.strip()]
            if new_query not in parts:
                parts.append(new_query)
                self.conn.execute(
                    "UPDATE files SET matched_query = ? WHERE id = ?",
                    (_json.dumps(parts, ensure_ascii=False), row["id"]),
                )
        self.conn.commit()

    def insert(self, record: dict):
        values = [record.get(c) for c in _COLUMNS]
        ph = ", ".join("?" for _ in _COLUMNS)
        self.conn.execute(
            f"INSERT INTO files ({', '.join(_COLUMNS)}) VALUES ({ph})",
            values,
        )
        self.conn.commit()

    # ── Normalized v2 insert helpers ─────────────────────────────────────────

    def seen_project_urls(self) -> set:
        return {r["project_url"] for r in self.conn.execute("SELECT project_url FROM projects").fetchall()}

    def insert_project(self, record: dict) -> int:
        """Insert a row into projects and return its new id."""
        cols = list(record.keys())
        ph   = ", ".join("?" for _ in cols)
        cur  = self.conn.execute(
            f"INSERT INTO projects ({', '.join(cols)}) VALUES ({ph})",
            [record[c] for c in cols],
        )
        self.conn.commit()
        return cur.lastrowid

    def insert_project_file(self, project_id: int, file_name: str, file_type: str, status: str):
        self.conn.execute(
            "INSERT INTO project_files (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
            (project_id, file_name, file_type, status),
        )
        self.conn.commit()

    def insert_keyword(self, project_id: int, keyword: str):
        self.conn.execute(
            "INSERT INTO keywords (project_id, keyword) VALUES (?, ?)",
            (project_id, keyword),
        )
        self.conn.commit()

    def insert_person_role(self, project_id: int, name: str, role: str):
        self.conn.execute(
            "INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)",
            (project_id, name, role),
        )
        self.conn.commit()

    def insert_license(self, project_id: int, license_str: str):
        self.conn.execute(
            "INSERT INTO licenses (project_id, license) VALUES (?, ?)",
            (project_id, license_str),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# PROGRESS STATE — survives interrupted runs
# ─────────────────────────────────────────────────────────────────────────────
class ProgressState:
    def __init__(self, path: Path):
        self.path   = path
        self._state: dict = {}
        if path.exists():
            try:
                with open(path) as f:
                    self._state = json.load(f)
                log.info(f"Resuming from progress file: {path}")
            except Exception:
                pass

    def get(self, query: str, default: int = 0) -> int:
        v = self._state.get(query, default)
        return default if v == "done" else int(v)

    def save(self, query: str, start: int):
        self._state[query] = start
        self._flush()

    def mark_done(self, query: str):
        self._state[query] = "done"
        self._flush()

    def is_done(self, query: str) -> bool:
        return self._state.get(query) == "done"

    def _flush(self):
        with open(self.path, "w") as f:
            json.dump(self._state, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# HARVARD DATAVERSE CRAWLER
#
#   Flow:
#     1. Search type=dataset  →  list of dataset DOIs matching the query
#     2. Fetch full dataset record regardless of hosting institution
#     3. Fetch full dataset record  →  latestVersion.files[]
#     4. Download EVERY file in the dataset regardless of extension
#        (skip only files where restricted=true or canDownloadFile=false)
#
#   Auth:   X-Dataverse-key on all requests including downloads
#   License: recorded for every dataset; no filtering applied
# ─────────────────────────────────────────────────────────────────────────────
def crawl_harvard(
    output_dir: Path,
    db: MetadataDB,
    max_records: int = 100000,
    progress: ProgressState = None,
    download: bool = False,
):
    mode = "DOWNLOAD" if download else "METADATA ONLY"
    log.info(f"=== Harvard Dataverse  [{mode}] ===")
    log.info(f"Token: {'set' if HARVARD_TOKEN and HARVARD_TOKEN != 'your-harvard-api-token-here' else 'NOT SET - public access only'}")

    auth_headers   = {"X-Dataverse-key": HARVARD_TOKEN}
    seen_urls      = db.seen_urls()
    total_datasets = 0
    per_page       = 20

    max_records = 1000000

    # Build the full search agenda:
    # Extension queries (.qdpx …) are searched as-is — Dataverse matches filenames,
    # so bare ".qdpx" is already precise.
    #
    # Keyword queries are searched in THREE scoped forms to maximise recall while
    # avoiding noise from irrelevant description-only matches:
    #   1. title:"<term>"    — dataset title contains the term
    #   2. keyword:"<term>"  — dataset is tagged with the term in subject/keyword field
    #   3. <term>            — bare full-text search (catches remaining matches)
    # All three share a progress key so resuming works correctly.
    # Results are deduplicated by dataset persistent ID (seen_ds_ids).

    search_agenda = []  # list of (progress_key, query_string) pairs

    for q in QDA_EXTENSION_QUERIES:
        search_agenda.append((q, q))   # key == query string

    for q in QDA_KEYWORD_QUERIES:
        search_agenda.append((f"title:{q}",   f'title:"{q}"'))
        search_agenda.append((f"keyword:{q}", f'keyword:"{q}"'))
        search_agenda.append((q,              q))

    for prog_key, q_str in search_agenda:
        if progress and progress.is_done(prog_key):
            log.info(f"Harvard: '{prog_key}' already complete, skipping.")
            continue

        start = progress.get(prog_key) if progress else 0
        log.info(f"Harvard: query='{q_str}' starting at offset {start}")

        # DOIs already in DB from previous queries — we won't re-download
        # but will append this query to their matched_query field.
        seen_ds_ids = db.seen_dois()
        # DOIs processed within THIS query run — avoids duplicate pages
        seen_ds_this_query: set = set()

        while total_datasets < max_records:

            # ── 1. Search (all types: dataset + file) ─────────────────────
            print('q_str', q_str)
            data = get_json(
                f"{HARVARD_API}/search",
                params={
                    "q":        q_str,
                    "per_page": per_page,
                    "start":    start,
                },
                headers=auth_headers,
            )

            if not data or data.get("status") != "OK":
                log.warning(f"Harvard: no OK response for q='{q_str}' start={start}")
                if progress:
                    progress.mark_done(prog_key)
                break

            items = data.get("data", {}).get("items", [])
            if not items:
                log.info(f"Harvard: query '{q_str}' exhausted at offset {start}.")
                if progress:
                    progress.mark_done(prog_key)
                break

            for item in items:
                clean_q   = prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key
                item_type = item.get("type", "dataset")

                # For file-type results, the parent dataset DOI is in
                # dataset_persistent_id. We process the parent dataset
                # (all its files) rather than just the one matched file.
                if item_type == "file":
                    parent_pid = item.get("dataset_persistent_id", "")
                    if not parent_pid:
                        continue
                    if parent_pid in seen_ds_this_query:
                        continue
                    doi_key = f"https://doi.org/{parent_pid.replace('doi:', '')}"
                    if parent_pid in seen_ds_ids:
                        db.append_query(doi_key, clean_q)
                        continue
                    # Build a synthetic dataset item. For resolve_host to work
                    # correctly we use the dataset DOI URL (not the file download
                    # URL) so the host is derived from the right place.
                    doi_url = f"https://doi.org/{parent_pid.replace('doi:', '')}"
                    item = {
                        "global_id":         parent_pid,
                        "type":              "dataset",
                        "name":              item.get("dataset_name", ""),
                        "description":       item.get("description", ""),
                        "published_at":      item.get("published_at", ""),
                        "url":               doi_url,
                        "name_of_dataverse": item.get("name_of_dataverse", ""),
                        "keywords":          item.get("keywords", []),
                        "authors":           item.get("authors", []),
                    }

                global_id = item.get("global_id", "")
                if not global_id:
                    continue

                # Already processed in this query page — skip entirely
                if global_id in seen_ds_this_query:
                    continue
                seen_ds_this_query.add(global_id)

                # Already in DB from a previous query — just append this query
                doi_key = f"https://doi.org/{global_id.replace('doi:', '')}"
                if global_id in seen_ds_ids:
                    db.append_query(doi_key, clean_q)
                    continue

                seen_ds_ids.add(global_id)

                # ── 2. Check DOI prefix — skip non-Dataverse systems ───────
                # Extract prefix from e.g. "doi:10.3886/ICPSR03076.V1"
                doi_prefix = global_id.replace("doi:", "").split("/")[0]

                if doi_prefix in SILENT_SKIP_PREFIXES:
                    continue

                if doi_prefix in NON_DATAVERSE_PREFIXES:
                    # Route to the right platform downloader
                    downloader = PLATFORM_DOWNLOADERS.get(doi_prefix)
                    if downloader:
                        log.info(f"Non-Dataverse DOI {global_id} ({doi_prefix}) — using platform downloader")
                        downloader(global_id, item, output_dir, db, seen_urls, download, prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key)
                    else:
                        log.warning(f"No downloader for DOI prefix {doi_prefix} ({global_id}) — recording as inaccessible")
                        _insert_file_row(db, seen_urls, {
                            "url":                item.get("url", f"https://doi.org/{global_id.replace('doi:', '')}"),
                            "doi":                f"https://doi.org/{global_id.replace('doi:', '')}",
                            "local_dir":          "", "local_filename":     "",
                            "file_type":          "inaccessible", "file_extension":     "",
                            "file_size_bytes":    None, "checksum_md5":       "",
                            "download_timestamp": "",
                            "source_name":        item.get("name_of_dataverse", "Unknown"),
                            "source_url":         item.get("url", ""),
                            "title":              item.get("name", ""),
                            "description":        (item.get("description", "") or "")[:500],
                            "year":               year_from_date(item.get("published_at", "")),
                            "keywords":           "; ".join(item.get("keywords", [])),
                            "language":           "", "author": "; ".join(item.get("authors", [])),
                            "uploader_name":      "", "uploader_email":     "",
                            "license":            "", "license_url":        "",
                            "matched_query": json.dumps([prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key], ensure_ascii=False),
                            "manual_download":    1,
                            "access_note":        f"No downloader for DOI prefix {doi_prefix} — add platform support",
                        })
                    continue

                # ── 3. Resolve host and token for this Dataverse institution
                item_host, item_token = resolve_host(item)
                item_api   = f"{item_host}/api"
                item_auth  = {"X-Dataverse-key": item_token} if item_token else {}
                source_url = f"{item_host}/dataset.xhtml?persistentId={global_id}"
                doi        = f"https://doi.org/{global_id.replace('doi:', '')}"

                # ── 3. Fetch full dataset record from the correct host ──────
                # retries=1: 401 means wrong/missing token — no point retrying.
                detail = get_json(
                    f"{item_api}/datasets/:persistentId/",
                    params={"persistentId": global_id},
                    headers=item_auth,
                    retries=1,
                )
                if not detail or detail.get("status") != "OK":
                    log.warning(
                        f"Inaccessible: {global_id} from {item_host} "
                        f"— recording in DB (add token to DATAVERSE_REGISTRY to unlock)"
                    )
                    # Record the dataset in the DB so the link is not lost.
                    # file_type="inaccessible" makes these easy to query later.
                    if source_url not in seen_urls:
                        seen_urls.add(source_url)
                        db.insert({
                            "url":                source_url,
                            "doi":                doi,
                            "local_dir":          "",
                            "local_filename":     "",
                            "file_type":          "inaccessible",
                            "file_extension":     "",
                            "file_size_bytes":    None,
                            "checksum_md5":       "",
                            "download_timestamp": "",
                            "source_name":        item.get("name_of_dataverse", "Unknown Dataverse"),
                            "source_url":         source_url,
                            "title":              item.get("name", ""),
                            "description":        (item.get("description", "") or "")[:500],
                            "year":               year_from_date(item.get("published_at", "")),
                            "keywords":           "; ".join(item.get("keywords", [])),
                            "language":           "",
                            "author":             "; ".join(item.get("authors", [])),
                            "uploader_name":      "",
                            "uploader_email":     "",
                            "license":            "",
                            "license_url":        "",
                            "matched_query": json.dumps([prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key], ensure_ascii=False),
                            "manual_download":    0,
                            "access_note":        f"Dataverse API 401/403 from {item_host} — add token to DATAVERSE_REGISTRY",
                        })
                    time.sleep(0.3)
                    continue

                ds     = detail["data"]
                latest = ds.get("latestVersion", {})

                # ── 4. Dataset metadata ────────────────────────────────────
                title      = item.get("name", "")
                pub_date   = item.get("published_at", "")

                citation_fields = (
                    latest.get("metadataBlocks", {})
                          .get("citation", {})
                          .get("fields", [])
                )
                fbn = {f["typeName"]: f for f in citation_fields}

                description = "; ".join(
                    v.get("value", "") if isinstance(v, dict) else str(v)
                    for v in (fbn.get("dsDescription", {}).get("value") or [])
                    if isinstance(v, dict) and v.get("dsDescriptionValue", {}).get("value")
                ) or (item.get("description", "") or "")
                description = description[:500]

                author = "; ".join(
                    v.get("authorName", {}).get("value", "")
                    for v in (fbn.get("author", {}).get("value") or [])
                )
                contacts       = fbn.get("datasetContact", {}).get("value") or []
                uploader_name  = contacts[0].get("datasetContactName",  {}).get("value", "") if contacts else ""
                uploader_email = contacts[0].get("datasetContactEmail", {}).get("value", "") if contacts else ""
                keywords       = "; ".join(
                    v.get("keywordValue", {}).get("value", "")
                    for v in (fbn.get("keyword", {}).get("value") or [])
                )
                language = "; ".join(
                    v.get("value", "") if isinstance(v, dict) else str(v)
                    for v in (fbn.get("language", {}).get("value") or [])
                )

                lic_raw = latest.get("license", ds.get("license", ""))
                if isinstance(lic_raw, dict):
                    license_str = lic_raw.get("name", "") or lic_raw.get("shortDescription", "")
                    license_url = lic_raw.get("uri", "")
                else:
                    license_str = str(lic_raw) if lic_raw else ""
                    license_url = ""

                # ── 4. Record each file in the database ───────────────────
                files = latest.get("files", [])
                if not files:
                    log.info(f"Harvard: no files in {global_id}")
                    continue

                log.info(f"Harvard: dataset {global_id} — {len(files)} file(s)")
                safe_id   = safe_folder(global_id)
                local_dir = f"harvard_dataverse/{safe_id}"

                for fi in files:
                    df    = fi.get("dataFile", {})
                    fname = df.get("filename", "")
                    fid   = df.get("id", "")
                    fsize = df.get("filesize", 0)
                    ftype = classify_file(fname)
                    fext  = Path(fname).suffix.lower()

                    if not fid:
                        continue

                    furl = f"{item_api}/access/datafile/{fid}"
                    if furl in seen_urls:
                        continue
                    seen_urls.add(furl)

                    db.insert({
                        "url":                furl,
                        "doi":                doi,
                        "local_dir":          "",       # filled in after zip download
                        "local_filename":     fname,
                        "file_type":          ftype,
                        "file_extension":     fext,
                        "file_size_bytes":    fsize,
                        "checksum_md5":       "",
                        "download_timestamp": "",
                        "source_name":        "Harvard Dataverse",
                        "source_url":         source_url,
                        "title":              title,
                        "description":        description,
                        "year":               year_from_date(pub_date),
                        "keywords":           keywords,
                        "language":           language,
                        "author":             author,
                        "uploader_name":      uploader_name,
                        "uploader_email":     uploader_email,
                        "license":            license_str,
                        "license_url":        license_url,
                        "matched_query": json.dumps([prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key], ensure_ascii=False),
                    })

                # ── 5. Write to normalized v2 tables ──────────────────────
                clean_q_str = prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key
                proj_id = db.insert_project({
                    "query_string":               clean_q_str,
                    "repository_id":              10,
                    "repository_url":             HARVARD_BASE,
                    "project_url":                source_url,
                    "version":                    latest.get("versionNumber", ""),
                    "title":                      title,
                    "description":                description,
                    "language":                   language,
                    "doi":                        doi,
                    "upload_date":                pub_date[:10] if pub_date else "",
                    "download_date":              datetime.now(timezone.utc).isoformat(),
                    "download_repository_folder": "harvard_dataverse",
                    "download_project_folder":    safe_id,
                    "download_version_folder":    "",
                    "download_method":            "API",
                })
                for kw in [k.strip() for k in keywords.split(";") if k.strip()]:
                    db.insert_keyword(proj_id, kw)
                for person in [p.strip() for p in author.split(";") if p.strip()]:
                    db.insert_person_role(proj_id, person, "AUTHOR")
                if uploader_name:
                    db.insert_person_role(proj_id, uploader_name, "UPLOADER")
                if license_str:
                    db.insert_license(proj_id, license_str)
                for fi in files:
                    df     = fi.get("dataFile", {})
                    fname  = df.get("filename", "")
                    fext   = Path(fname).suffix.lower()
                    status = "FAILED_LOGIN" if fi.get("restricted", False) else "SUCCEEDED"
                    if fname:
                        db.insert_project_file(proj_id, fname, fext, status)

                # ── 6. Download entire dataset as one zip ──────────────────
                # API: GET /api/access/dataset/:persistentId/?persistentId=doi:...
                # Returns a zip with all files, preserving folder structure.
                # Much simpler and more reliable than downloading file by file.
                if download:
                    zip_url  = f"{item_api}/access/dataset/:persistentId/?persistentId={global_id}"
                    zip_dest = output_dir / "harvard_dataverse" / safe_id / f"{safe_id}.zip"

                    if not zip_dest.exists():
                        log.info(f"Harvard: downloading zip for {global_id} -> {zip_dest}")
                        if download_file(zip_url, zip_dest, extra_headers=item_auth):
                            ts = datetime.now(timezone.utc).isoformat()
                            log.info(f"Harvard: zip saved ({zip_dest.stat().st_size:,} bytes)")

                            # Extract zip and update DB rows with timestamps
                            import zipfile
                            extract_dir = output_dir / "harvard_dataverse" / safe_id
                            try:
                                with zipfile.ZipFile(zip_dest) as zf:
                                    zf.extractall(extract_dir)
                                log.info(f"Harvard: extracted {global_id} to {extract_dir}")
                                zip_dest.unlink()   # remove zip after extraction

                                # Mark all files in this dataset as downloaded
                                db.conn.execute(
                                    "UPDATE files SET download_timestamp=?, local_dir=? WHERE doi=? AND download_timestamp=''",
                                    (ts, local_dir, doi)
                                )
                                db.conn.commit()
                            except zipfile.BadZipFile:
                                log.warning(f"Harvard: bad zip for {global_id} — keeping zip file")
                        else:
                            log.warning(f"Harvard: zip download failed for {global_id}")
                    else:
                        log.info(f"Harvard: zip already exists for {global_id}, skipping.")

                total_datasets += 1
                time.sleep(0.5)

            # ── Pagination ─────────────────────────────────────────────────
            total_count = data.get("data", {}).get("total_count", 0)
            start += per_page
            if start >= total_count:
                log.info(f"Harvard: query '{q}' exhausted ({total_count} total).")
                if progress:
                    progress.mark_done(q)
                break

            if progress:
                progress.save(prog_key, start)

            log.info(
                f"Harvard: '{q}' offset {start}/{total_count} "
                f"| datasets processed={total_datasets}"
            )
            time.sleep(1)

    log.info(f"Harvard Dataverse complete: {total_datasets} datasets processed.")


# ─────────────────────────────────────────────────────────────────────────────
# COLUMBIA ORAL HISTORY ARCHIVE CRAWLER  (repository #19)
#
#   Source: Columbia Digital Collections (Blacklight / Solr JSON:API)
#   Base:   https://digitalcollections.columbia.edu/catalog.json
#
#   Flow:
#     1. Search with QDA_QUERIES + oral-history-specific terms
#     2. For each result item: record metadata in PROJECTS table
#     3. Attempt IIIF manifest download to discover file-level URLs
#     4. Write files to project_files, keywords, person_role, licenses tables
#     5. Also write a summary row to legacy `files` table for backward compat
#
#   Note: many Columbia items are restricted; access_note records the reason.
# ─────────────────────────────────────────────────────────────────────────────
COLUMBIA_REPOSITORY_ID  = 19
COLUMBIA_REPOSITORY_URL = "https://guides.library.columbia.edu/"
COLUMBIA_SEARCH_BASE    = "https://digitalcollections.columbia.edu"
COLUMBIA_SEARCH_URL     = f"{COLUMBIA_SEARCH_BASE}/catalog.json"

# Oral-history-specific terms layered on top of the shared QDA_QUERIES
COLUMBIA_EXTRA_QUERIES = [
    "oral history interview",
    "oral history transcript",
    "oral history recording",
    "qualitative interview transcript",
    "interview audio recording",
]

def _columbia_attr(attrs: dict, key: str) -> str:
    """Extract a Blacklight attribute value as a semicolon-joined string."""
    val = attrs.get(key, [])
    if isinstance(val, list):
        return "; ".join(str(v) for v in val if v)
    return str(val) if val else ""


def crawl_columbia(
    output_dir: Path,
    db: MetadataDB,
    max_records: int = 100_000,
    progress: ProgressState = None,
    download: bool = False,
):
    """
    Harvest qualitative / oral-history records from Columbia Digital Collections.
    Writes to the normalized v2 schema tables AND the legacy `files` table.
    """
    mode = "DOWNLOAD" if download else "METADATA ONLY"
    log.info(f"=== Columbia Oral History Archive  [{mode}] ===")

    seen_urls  = db.seen_urls()
    seen_proj  = db.seen_project_urls()
    total      = 0
    per_page   = 25

    all_queries = COLUMBIA_EXTRA_QUERIES + QDA_KEYWORD_QUERIES

    for q in all_queries:
        prog_key = f"columbia:{q}"
        if progress and progress.is_done(prog_key):
            log.info(f"Columbia: '{q}' already complete, skipping.")
            continue

        # Progress stores page number (1-based), default 1
        page  = max(1, progress.get(prog_key, 1)) if progress else 1
        log.info(f"Columbia: query='{q}' starting at page {page}")

        while total < max_records:
            data = get_json(
                COLUMBIA_SEARCH_URL,
                params={"q": q, "per_page": per_page, "page": page},
            )

            if not data:
                log.warning(f"Columbia: no response for q='{q}' page={page}")
                if progress:
                    progress.mark_done(prog_key)
                break

            items = data.get("data", [])
            if not items:
                log.info(f"Columbia: query '{q}' exhausted at page {page}.")
                if progress:
                    progress.mark_done(prog_key)
                break

            for item in items:
                item_id = item.get("id", "")
                if not item_id:
                    continue

                project_url = f"{COLUMBIA_SEARCH_BASE}/catalog/{item_id}"
                if project_url in seen_proj:
                    continue
                seen_proj.add(project_url)

                attrs = item.get("attributes", {})

                title       = _columbia_attr(attrs, "title") or item_id
                description = (_columbia_attr(attrs, "description") or "")[:500]
                language    = _columbia_attr(attrs, "language_ssim") or _columbia_attr(attrs, "language")
                date_str    = _columbia_attr(attrs, "date_ssim") or _columbia_attr(attrs, "date")
                doi         = _columbia_attr(attrs, "identifier_doi_ssim") or ""
                rights      = _columbia_attr(attrs, "rights_ssim") or _columbia_attr(attrs, "rights") or ""
                subjects    = attrs.get("subject_ssim", attrs.get("subject", []))
                creators    = attrs.get("creator_ssim", attrs.get("creator", []))
                upload_date = date_str[:10] if date_str else ""

                # ── Write to normalized PROJECTS table ────────────────────
                project_id = db.insert_project({
                    "query_string":               q,
                    "repository_id":              COLUMBIA_REPOSITORY_ID,
                    "repository_url":             COLUMBIA_REPOSITORY_URL,
                    "project_url":                project_url,
                    "version":                    "",
                    "title":                      title,
                    "description":                description,
                    "language":                   language,
                    "doi":                        doi,
                    "upload_date":                upload_date,
                    "download_date":              datetime.now(timezone.utc).isoformat(),
                    "download_repository_folder": "columbia-oral-history-archive",
                    "download_project_folder":    item_id,
                    "download_version_folder":    "",
                    "download_method":            "SCRAPING",
                })

                # Keywords
                for kw in (subjects if isinstance(subjects, list) else [subjects]):
                    if kw:
                        db.insert_keyword(project_id, str(kw))

                # Persons — role is UNKNOWN for Columbia catalogue records
                for creator in (creators if isinstance(creators, list) else [creators]):
                    if creator:
                        db.insert_person_role(project_id, str(creator), "UNKNOWN")

                # License
                if rights:
                    db.insert_license(project_id, rights)

                # ── Discover downloadable files via IIIF manifest ─────────
                files_written = False
                iiif_url = f"{COLUMBIA_SEARCH_BASE}/iiif/3/{item_id}/manifest"
                iiif_data = get_json(iiif_url, retries=1)

                if iiif_data:
                    # IIIF Presentation 3 — items are in .items[].items[].items[]
                    for canvas in iiif_data.get("items", []):
                        for anno_page in canvas.get("items", []):
                            for anno in anno_page.get("items", []):
                                body = anno.get("body", {})
                                # body may be a list (multiple representations)
                                bodies = body if isinstance(body, list) else [body]
                                for b in bodies:
                                    furl = b.get("id", "")
                                    if not furl:
                                        continue
                                    fname  = furl.split("/")[-1].split("?")[0] or f"{item_id}.bin"
                                    fext   = Path(fname).suffix.lower()
                                    status = "SUCCEEDED"

                                    if download:
                                        dest = output_dir / "columbia-oral-history-archive" / item_id / fname
                                        ok   = download_file(furl, dest)
                                        status = "SUCCEEDED" if ok else "FAILED_SERVER"
                                        time.sleep(0.5)

                                    db.insert_project_file(project_id, fname, fext, status)
                                    files_written = True

                if not files_written:
                    # No IIIF or no downloadable items — record access status
                    db.insert_project_file(project_id, "", "", "FAILED_LOGIN")

                # ── Legacy `files` row for backward compatibility ─────────
                legacy_url = project_url
                _insert_file_row(db, seen_urls, {
                    "url":             legacy_url,
                    "doi":             doi,
                    "local_dir":       "",
                    "local_filename":  "",
                    "file_type":       "inaccessible" if not files_written else "primary",
                    "file_extension":  "",
                    "file_size_bytes": None,
                    "checksum_md5":    "",
                    "download_timestamp": "",
                    "source_name":     "Columbia Oral History Archive",
                    "source_url":      project_url,
                    "title":           title,
                    "description":     description,
                    "year":            date_str[:4] if date_str else "",
                    "keywords":        "; ".join(str(s) for s in (subjects if isinstance(subjects, list) else [subjects]) if s),
                    "language":        language,
                    "author":          "; ".join(str(c) for c in (creators if isinstance(creators, list) else [creators]) if c),
                    "uploader_name":   "",
                    "uploader_email":  "",
                    "license":         rights,
                    "license_url":     "",
                    "matched_query":   json.dumps([q], ensure_ascii=False),
                    "manual_download": 0 if files_written else 1,
                    "access_note":     "" if files_written else "Columbia Digital Collections: item may require in-person or authenticated access",
                })

                total += 1
                time.sleep(0.5)

            # Pagination
            pages_meta   = data.get("meta", {}).get("pages", {})
            total_pages  = pages_meta.get("total_pages", 1)
            total_count  = pages_meta.get("total_count", 0)

            if page >= total_pages:
                log.info(f"Columbia: '{q}' exhausted ({total_count} items).")
                if progress:
                    progress.mark_done(prog_key)
                break

            page += 1
            if progress:
                progress.save(prog_key, page)
            log.info(f"Columbia: '{q}' page {page}/{total_pages} | total={total}")
            time.sleep(1)

    log.info(f"Columbia Oral History Archive complete: {total} records processed.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
CRAWLERS = {
    "harvard":  crawl_harvard,
    "columbia": crawl_columbia,
}

ALL_SOURCES = list(CRAWLERS.keys())


def main():
    parser = argparse.ArgumentParser(
        description="QDArchive Seeding Pipeline v7 — Harvard Dataverse (#10) + Columbia Oral History Archive (#19)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Metadata scan only — all sources
  python qdarchive_pipeline.py

  # Specific sources only
  python qdarchive_pipeline.py --sources harvard columbia

  # Full download
  python qdarchive_pipeline.py --download

  # Custom record limit and paths
  python qdarchive_pipeline.py --max-records 1000 --download \\
      --output-dir /data/qdarchive --db /data/qdarchive/metadata.sqlite
        """,
    )
    parser.add_argument(
        "--output-dir", default="./archive",
        help="Root directory for downloaded files (default: ./archive)",
    )
    parser.add_argument(
        "--db", default="./metadata.sqlite",
        help="SQLite database path (default: ./metadata.sqlite)",
    )
    parser.add_argument(
        "--max-records", type=int, default=500,
        help="Max records to collect per source (default: 500)",
    )
    parser.add_argument(
        "--download", action="store_true", default=False,
        help="Download files to disk. Without this flag only metadata is saved.",
    )
    parser.add_argument(
        "--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES,
        help=f"Which repositories to crawl (default: all). Choices: {', '.join(ALL_SOURCES)}",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    db       = MetadataDB(Path(args.db))
    progress = ProgressState(Path(args.db).with_suffix(".progress.json"))

    log.info(f"Pipeline start | sources={args.sources} | max_records={args.max_records} | download={args.download}")

    for source in args.sources:
        crawler = CRAWLERS[source]
        try:
            crawler(
                output_dir  = output_dir,
                db          = db,
                max_records = args.max_records,
                progress    = progress,
                download    = args.download,
            )
        except Exception as e:
            log.error(f"{source} crawler crashed: {e}", exc_info=True)

    db.close()
    log.info(f"Pipeline complete. Database -> {args.db}")


if __name__ == "__main__":
    main()
