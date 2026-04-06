"""
Open Science Framework (OSF) platform fetcher.

DOI format : doi:10.17605/OSF.IO/XXXXX
API        : GET https://api.osf.io/v2/nodes/{id}/files/osfstorage/
             GET https://api.osf.io/v2/registrations/{id}/files/osfstorage/
Auth       : none required for public projects
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


def fetch_osf(global_id, item, output_dir, db, seen_urls, download, q):
    osf_id     = global_id.replace("doi:", "").split("OSF.IO/")[-1].split("/")[0].lower()
    doi        = f"https://doi.org/{global_id.replace('doi:', '')}"
    source_url = f"https://osf.io/{osf_id}/"

    title       = item.get("name", "")
    description = (item.get("description", "") or "")[:500]
    pub_date    = item.get("published_at", "")
    author      = "; ".join(item.get("authors", []))

    files_data = None
    for endpoint in [
        f"https://api.osf.io/v2/nodes/{osf_id}/files/osfstorage/",
        f"https://api.osf.io/v2/registrations/{osf_id}/files/osfstorage/",
    ]:
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
            "manual_download": 1,
            "access_note": "OSF project may be private or require login",
        })
        return

    log.info(f"OSF: project {osf_id} — {len(files_data)} item(s)")
    safe_id   = safe_folder(global_id)
    local_dir = f"osf/{safe_id}"

    for f in files_data:
        attrs = f.get("attributes", {})
        fname = attrs.get("name", "")
        fsize = attrs.get("size", 0)
        furl  = f.get("links", {}).get("download", "")
        if not furl:
            continue
        ftype = classify_file(fname)
        fext  = Path(fname).suffix.lower()

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
            "local_dir": local_dir,
            "local_filename": fname, "file_type": ftype,
            "file_extension": fext, "file_size_bytes": fsize,
            "checksum_md5": checksum, "download_timestamp": ts,
            "source_name": "OSF", "source_url": source_url,
            "title": title, "description": description,
            "year": year_from_date(pub_date), "keywords": "",
            "language": "", "author": author,
            "uploader_name": "", "uploader_email": "",
            "license": "", "license_url": "",
            "matched_query": json.dumps([q], ensure_ascii=False),
            "manual_download": 1,
            "access_note": "OSF project may be private or require login",
        })
