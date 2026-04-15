"""
Columbia Oral History Archive crawler  (repository #19).

Source : Columbia Digital Collections — JSON:API
Base   : https://dlc.library.columbia.edu/catalog.json

Changes vs. original:
  - Domain moved from digitalcollections.columbia.edu to dlc.library.columbia.edu
  - q= text search is non-functional in the new API; replaced with a single
    facet filter: f[lib_project_short_ssim][]=Oral History Collections - General
  - Attribute values are now nested document_value objects (not flat lists)
  - IIIF manifests return 404 for collection-level records; handled gracefully

Flow:
  1. Paginate through all items in the Oral History Collections facet.
  2. For each record: write project metadata to the normalised v2 PROJECTS table.
  3. Attempt IIIF v3 manifest discovery to enumerate downloadable files.
  4. Write files to project_files; write keywords, persons, licenses.
  5. Also write a summary row to the legacy `files` table for backward compat.

Note: many Columbia items are access-restricted; access_note records the reason.
"""
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from qdarchive.db import MetadataDB
from qdarchive.fetchers.base import _insert_file_row
from qdarchive.http_client import get_json, download_file
from qdarchive.progress import ProgressState

log = logging.getLogger(__name__)

REPOSITORY_ID  = 19
REPOSITORY_URL = "https://guides.library.columbia.edu/"
SEARCH_BASE    = "https://dlc.library.columbia.edu"
SEARCH_URL     = f"{SEARCH_BASE}/catalog.json"

# Single facet filter — covers the full Oral History Collections project
ORAL_HISTORY_FACET = "Oral History Collections - General"
PROGRESS_KEY       = "columbia:oral_history_collections"


def _attr(attrs: dict, *keys: str) -> str:
    """
    Extract an attribute value as a semicolon-joined string.

    Handles both:
      - plain values: str, list[str]
      - nested document_value objects: {"type":"document_value","attributes":{"value":...}}
    Tries each key in order and returns the first non-empty result.
    """
    for key in keys:
        val = attrs.get(key)
        if val is None:
            continue
        # Nested document_value object
        if isinstance(val, dict) and val.get("type") == "document_value":
            inner = val.get("attributes", {}).get("value", "")
            if isinstance(inner, list):
                result = "; ".join(str(v) for v in inner if v)
            else:
                result = str(inner) if inner else ""
            if result:
                return result
        elif isinstance(val, list):
            result = "; ".join(str(v) for v in val if v)
            if result:
                return result
        elif val:
            return str(val)
    return ""


def crawl_columbia(
    output_dir: Path,
    db: MetadataDB,
    max_records: int = 100_000,
    progress: ProgressState = None,
    download: bool = False,
    allowed_extensions: set[str] | None = None,
    max_file_size_bytes: int | None = None,
    gdrive_service=None,
    gdrive_folder_cache: dict | None = None,
    gdrive_root_id: str | None = None,
):
    mode = "DOWNLOAD" if download else "METADATA ONLY"
    log.info(f"=== Columbia Oral History Archive  [{mode}] ===")

    # ── Pre-flight: confirm endpoint is reachable ─────────────────────────────
    _preflight = get_json(
        SEARCH_URL,
        params={"per_page": 1, "page": 1, "f[lib_project_short_ssim][]": ORAL_HISTORY_FACET},
        retries=1,
    )
    if _preflight is None:
        log.error("Columbia Digital Collections endpoint unreachable — skipping.")
        return
    total_available = _preflight.get("meta", {}).get("pages", {}).get("total_count", 0)
    log.info(f"Columbia: {total_available} oral-history records available.")

    if progress and progress.is_done(PROGRESS_KEY):
        log.info("Columbia: already complete, skipping.")
        return

    seen_urls = db.seen_urls()
    seen_proj = db.seen_project_urls()
    total    = 0
    per_page = 25
    _limit   = max_records if max_records > 0 else float("inf")
    page     = max(1, progress.get(PROGRESS_KEY, 1)) if progress else 1
    log.info(f"Columbia: starting at page {page}")

    while total < _limit:
        data = get_json(
            SEARCH_URL,
            params={
                "per_page": per_page,
                "page":     page,
                "f[lib_project_short_ssim][]": ORAL_HISTORY_FACET,
            },
        )

        if not data:
            log.warning(f"Columbia: no response at page {page} — stopping.")
            break

        items = data.get("data", [])
        if not items:
            log.info(f"Columbia: exhausted at page {page}.")
            if progress:
                progress.mark_done(PROGRESS_KEY)
            break

        for item in items:
            item_id = item.get("id", "")
            if not item_id:
                continue

            project_url = f"{SEARCH_BASE}/catalog/{item_id}"
            if project_url in seen_proj:
                continue
            seen_proj.add(project_url)

            attrs = item.get("attributes", {})

            title       = _attr(attrs, "title") or item_id
            description = (_attr(attrs, "description") or "")[:500]
            language    = _attr(attrs, "language_language_term_text_ssim", "language_ssim", "language")
            date_str    = _attr(attrs, "lib_date_textual_ssm", "date_ssim", "date")
            doi         = _attr(attrs, "identifier_doi_ssim", "doi") or ""
            rights      = _attr(attrs, "rights_ssim", "rights") or ""
            collection  = _attr(attrs, "lib_collection_ssm") or ""
            subjects_raw = attrs.get("subject_ssim", attrs.get("subject", []))
            creators_raw = attrs.get(
                "primary_name_ssm",        # new nested format
                attrs.get("creator_ssim", attrs.get("creator", [])),
            )

            # Normalise creators from both flat-list and document_value forms
            if isinstance(creators_raw, dict) and creators_raw.get("type") == "document_value":
                inner = creators_raw.get("attributes", {}).get("value", [])
                creators = inner if isinstance(inner, list) else [inner]
            elif isinstance(creators_raw, list):
                creators = creators_raw
            else:
                creators = [creators_raw] if creators_raw else []

            subjects = subjects_raw if isinstance(subjects_raw, list) else ([subjects_raw] if subjects_raw else [])
            upload_date = date_str[:10] if date_str else ""

            # ── Normalised v2 tables ──────────────────────────────────────────
            project_id = db.insert_project({
                "query_string":               ORAL_HISTORY_FACET,
                "repository_id":              REPOSITORY_ID,
                "repository_url":             REPOSITORY_URL,
                "project_url":                project_url,
                "version":                    "",
                "title":                      title,
                "description":                description or collection[:500],
                "language":                   language,
                "doi":                        doi,
                "upload_date":                upload_date,
                "download_date":              datetime.now(timezone.utc).isoformat(),
                "download_repository_folder": "columbia-oral-history-archive",
                "download_project_folder":    item_id,
                "download_version_folder":    "",
                "download_method":            "SCRAPING",
            })

            for kw in subjects:
                if kw:
                    db.insert_keyword(project_id, str(kw))

            for creator in creators:
                if creator:
                    db.insert_person_role(project_id, str(creator), "UNKNOWN")

            if rights:
                db.insert_license(project_id, rights)

            # ── Discover files via IIIF v3 manifest ───────────────────────────
            files_written = False
            iiif_url  = f"{SEARCH_BASE}/iiif/3/{item_id}/manifest"
            iiif_data = get_json(iiif_url, retries=1)

            if iiif_data:
                for canvas in iiif_data.get("items", []):
                    for anno_page in canvas.get("items", []):
                        for anno in anno_page.get("items", []):
                            body   = anno.get("body", {})
                            bodies = body if isinstance(body, list) else [body]
                            for b in bodies:
                                furl = b.get("id", "")
                                if not furl:
                                    continue
                                fname  = furl.split("/")[-1].split("?")[0] or f"{item_id}.bin"
                                fext   = Path(fname).suffix.lower()
                                status = "SUCCEEDED"

                                if download:
                                    dest   = output_dir / "columbia-oral-history-archive" / item_id / fname
                                    ok     = download_file(furl, dest)
                                    status = "SUCCEEDED" if ok else "FAILED_SERVER"
                                    time.sleep(0.5)

                                db.insert_project_file(project_id, fname, fext, status, file_size_bytes=None)
                                files_written = True

            if not files_written:
                db.insert_project_file(project_id, "", "", "FAILED_LOGIN", file_size_bytes=None)

            # ── Legacy `files` row ────────────────────────────────────────────
            _insert_file_row(db, seen_urls, {
                "url":             project_url,
                "doi":             doi,
                "local_dir":       "",
                "local_filename":  "",
                "file_type":       "primary" if files_written else "inaccessible",
                "file_extension":  "",
                "file_size_bytes": None,
                "checksum_md5":    "",
                "download_timestamp": "",
                "source_name":     "Columbia Oral History Archive",
                "source_url":      project_url,
                "title":           title,
                "description":     description or collection[:500],
                "year":            date_str[:4] if date_str else "",
                "keywords":        "; ".join(str(s) for s in subjects if s),
                "language":        language,
                "author":          "; ".join(str(c) for c in creators if c),
                "uploader_name":   "",
                "uploader_email":  "",
                "license":         rights,
                "license_url":     "",
                "matched_query":   json.dumps([ORAL_HISTORY_FACET], ensure_ascii=False),
                "manual_download": 0 if files_written else 1,
                "access_note":     "" if files_written else "Columbia Digital Collections: item may require in-person or authenticated access",
            })

            total += 1
            time.sleep(1.0)   # Columbia WAF blocks aggressive scrapers — be polite

        # ── Pagination ────────────────────────────────────────────────────────
        pages_meta  = data.get("meta", {}).get("pages", {})
        total_pages = pages_meta.get("total_pages", 1)
        total_count = pages_meta.get("total_count", 0)

        if page >= total_pages:
            log.info(f"Columbia: all {total_count} records exhausted.")
            if progress:
                progress.mark_done(PROGRESS_KEY)
            break

        page += 1
        if progress:
            progress.save(PROGRESS_KEY, page)
        log.info(f"Columbia: page {page}/{total_pages} | processed={total}")
        time.sleep(3)   # polite inter-page delay

    log.info(f"Columbia Oral History Archive complete: {total} records processed.")
