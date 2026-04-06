"""
figshare platform fetcher.

DOI format : doi:10.6084/m9.figshare.NNNNNNN[.vN]
API        : GET https://api.figshare.com/v2/articles?doi=<doi>
             GET https://api.figshare.com/v2/articles/{article_id}
Files      : .files[].download_url
Auth       : none required for public articles
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


def fetch_figshare(global_id, item, output_dir, db, seen_urls, download, q):
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    doi_plain  = global_id.replace("doi:", "")
    source_url = item.get("url", doi)

    # Step 1: resolve DOI → article_id via figshare search
    article_id = None
    search = get_json(
        "https://api.figshare.com/v2/articles",
        params={"doi": doi_plain},
        retries=2,
    )
    if search and isinstance(search, list) and len(search) > 0:
        article_id = search[0].get("id")
        source_url = search[0].get("url_public_html", source_url)

    if not article_id:
        try:
            article_id = int(global_id.replace("doi:", "").split("figshare.")[-1].split(".")[0])
        except Exception:
            article_id = None

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
            "manual_download": 0,
            "access_note": "figshare DOI could not be resolved to an article ID",
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
            "title": item.get("name", ""),
            "description": (item.get("description", "") or "")[:500],
            "year": year_from_date(item.get("published_at", "")),
            "keywords": "", "language": "",
            "author": "; ".join(item.get("authors", [])),
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0,
            "access_note": "figshare API error — article may be private or deleted",
        })
        return

    title       = meta.get("title", item.get("name", ""))
    description = (meta.get("description", "") or "")[:500]
    pub_date    = meta.get("published_date", "")
    author      = "; ".join(a.get("full_name", "") for a in meta.get("authors", []))
    license_str = (meta.get("license", {}) or {}).get("name", "")
    license_url = (meta.get("license", {}) or {}).get("url",  "")
    raw_tags    = meta.get("tags", [])
    keywords    = "; ".join(
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
            "local_dir": local_dir,
            "local_filename": fname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "source_name": "figshare", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": keywords,
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": license_str, "license_url": license_url,
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 0,
            "access_note": "figshare API error — article may be private or deleted",
        })
