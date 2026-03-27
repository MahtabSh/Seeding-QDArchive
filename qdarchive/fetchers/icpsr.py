"""
ICPSR / openICPSR platform fetcher.

openICPSR (doi:10.3886/ENNNNNVN) — runs Dataverse software at
  https://www.openicpsr.org — full Dataverse API, uses Basic Auth.

Regular ICPSR (doi:10.3886/ICPSRNNNNNN) — custom system at icpsr.umich.edu;
  no public file API, so only metadata is recorded.
"""
import base64
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from qdarchive.config import OPENICPSR_USER, OPENICPSR_PASS
from qdarchive.http_client import get_json, download_file
from qdarchive.utils import classify_file, safe_folder, year_from_date, md5
from qdarchive.fetchers.base import _insert_file_row

log = logging.getLogger(__name__)


def fetch_icpsr(global_id, item, output_dir, db, seen_urls, download, q):
    doi      = f"https://doi.org/{global_id.replace('doi:', '')}"
    local_id = global_id.replace("doi:10.3886/", "").replace("doi:", "")

    is_open_icpsr = local_id.upper().startswith("E")

    if is_open_icpsr:
        log.info(f"openICPSR (Dataverse): {global_id}")
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
                "manual_download": 1,
                "access_note": "openICPSR API error — set OPENICPSR_USER and OPENICPSR_PASS",
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
                "local_filename": fname, "file_type": ftype,
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
                "manual_download": 1,
                "access_note": "openICPSR API error — set OPENICPSR_USER and OPENICPSR_PASS",
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
