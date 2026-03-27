"""
Harvard Dataverse — OAI-PMH bulk harvester  (alternative to search API).

Instead of querying with keywords (which triggers IP blocks), this crawler:
  1. Streams ALL Harvard Dataverse records via OAI-PMH (Dublin Core).
     114k+ records, 10 per page, no authentication required.
  2. Filters each record locally against QDA keywords / software names / extensions.
  3. For matched records, fetches full dataset detail via the Dataverse API
     to get file listings and rich metadata.
  4. Writes matched records to both the legacy `files` table and the v2 tables.

OAI-PMH is the official bulk-access protocol — far less rate-limited than
the search API and designed for exactly this kind of harvest.

Resume: progress key "harvard_oai:<resumption_token_or_offset>" is saved after
each page so interrupted runs continue from the last position.
"""
import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from qdarchive.config import HARVARD_TOKEN, HARVARD_BASE
from qdarchive.db import MetadataDB
from qdarchive.fetchers.base import _insert_file_row
from qdarchive.http_client import SESSION
from qdarchive.progress import ProgressState
from qdarchive.queries import QDA_EXTENSION_QUERIES, QDA_SOFTWARE_QUERIES, QDA_KEYWORD_QUERIES
from qdarchive.utils import safe_folder

log = logging.getLogger(__name__)

OAI_BASE      = "https://dataverse.harvard.edu/oai"
OAI_NS        = "http://www.openarchives.org/OAI/2.0/"
DC_NS         = "http://purl.org/dc/elements/1.1/"
PROGRESS_KEY  = "harvard_oai"

# ── Build a fast local filter from all QDA terms ─────────────────────────────
_QDA_TERMS = set()
for q in QDA_EXTENSION_QUERIES:
    _QDA_TERMS.add(q.lower().lstrip("."))   # "qdpx", "nvp", …
for q in QDA_SOFTWARE_QUERIES:
    _QDA_TERMS.add(q.lower())               # "nvivo", "maxqda", …
for q in QDA_KEYWORD_QUERIES:
    _QDA_TERMS.add(q.lower())               # "oral history", …


def _is_qda_relevant(title: str, description: str, subjects: list[str]) -> bool:
    """Return True if any QDA term appears in the record's text fields."""
    haystack = " ".join([title, description] + subjects).lower()
    return any(term in haystack for term in _QDA_TERMS)


def _oai_text(record, tag: str, ns: str = DC_NS) -> str:
    """Return semicolon-joined text of all matching elements in a DC record."""
    vals = [el.text.strip() for el in record.findall(f".//{{{ns}}}{tag}") if el.text]
    return "; ".join(vals)


def _fetch_oai_page(resumption_token: str | None) -> tuple[list, str | None, int]:
    """
    Fetch one OAI-PMH page.
    Returns (records_xml_list, next_resumption_token, complete_list_size).
    Returns ([], None, 0) on unrecoverable errors so the caller stops cleanly.
    """
    if resumption_token:
        params = {"verb": "ListRecords", "resumptionToken": resumption_token}
    else:
        params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}

    try:
        r = SESSION.get(OAI_BASE, params=params, timeout=(15, 60))
        if r.status_code == 500:
            log.error(
                f"OAI-PMH 500 Internal Server Error at token={resumption_token!r} — "
                "Harvard's OAI server hit a bad offset. Stopping harvest. "
                "Delete the progress entry for 'harvard_oai' and re-run to restart."
            )
            return [], None, 0   # None token → caller exits cleanly
        r.raise_for_status()
        root = ET.fromstring(r.text)
    except Exception as e:
        log.warning(f"OAI-PMH fetch failed: {e}")
        return [], resumption_token, 0   # keep token so transient errors retry

    records = root.findall(f".//{{{OAI_NS}}}record")

    token_el   = root.find(f".//{{{OAI_NS}}}resumptionToken")
    next_token = token_el.text.strip() if (token_el is not None and token_el.text) else None
    total      = int(token_el.get("completeListSize", 0)) if token_el is not None else len(records)

    return records, next_token, total


def crawl_harvard_oai(
    output_dir: Path,
    db: MetadataDB,
    max_records: int = 0,
    progress: ProgressState = None,
    download: bool = False,
):
    mode = "DOWNLOAD" if download else "METADATA ONLY"
    log.info(f"=== Harvard Dataverse OAI-PMH  [{mode}] ===")

    if progress and progress.is_done(PROGRESS_KEY):
        log.info("Harvard OAI: already complete, skipping.")
        return

    auth_headers  = {"X-Dataverse-key": HARVARD_TOKEN} if HARVARD_TOKEN else {}
    seen_urls     = db.seen_urls()
    seen_ds_ids   = db.seen_dois()
    _limit        = max_records if max_records > 0 else float("inf")

    # Resume from saved token or start fresh
    resumption_token = progress.get_raw(PROGRESS_KEY) if progress else None
    if resumption_token in (None, 0, "0"):
        resumption_token = None

    total_seen        = 0
    total_matched     = 0
    page              = 0
    consecutive_empty = 0

    while True:
        records, next_token, complete_size = _fetch_oai_page(resumption_token)

        if not records:
            if next_token is None:
                log.info(f"Harvard OAI: exhausted all records (seen={total_seen} matched={total_matched}).")
                if progress:
                    progress.mark_done(PROGRESS_KEY)
                break
            # Transient empty page — allow a few retries before giving up
            consecutive_empty += 1
            if consecutive_empty >= 3:
                log.error(
                    f"Harvard OAI: {consecutive_empty} consecutive empty pages — stopping. "
                    "Resume by re-running; progress is saved."
                )
                break
            log.warning(f"Harvard OAI: empty page (attempt {consecutive_empty}/3), retrying in 10s...")
            time.sleep(10)
            continue

        consecutive_empty = 0

        page += 1
        log.info(
            f"Harvard OAI: page {page} — {len(records)} records "
            f"| seen={total_seen}/{complete_size} matched={total_matched}"
        )

        for record in records:
            # Skip deleted records
            header = record.find(f"{{{OAI_NS}}}header")
            if header is not None and header.get("status") == "deleted":
                continue

            identifier_el = record.find(f".//{{{OAI_NS}}}identifier")
            global_id = identifier_el.text.strip() if identifier_el is not None else ""
            if not global_id or not global_id.startswith("doi:"):
                continue

            total_seen += 1

            # ── Local QDA filter ──────────────────────────────────────────────
            metadata = record.find(f".//{{{OAI_NS}}}metadata")
            if metadata is None:
                continue

            title       = _oai_text(metadata, "title")
            description = _oai_text(metadata, "description")
            subjects    = [
                el.text.strip()
                for el in metadata.findall(f".//{{{DC_NS}}}subject") if el.text
            ]
            date_str    = _oai_text(metadata, "date")
            doi         = _oai_text(metadata, "identifier")
            if doi and not doi.startswith("http"):
                doi = f"https://doi.org/{global_id.replace('doi:', '')}"

            if not _is_qda_relevant(title, description, subjects):
                continue

            # Already in DB?
            if global_id in seen_ds_ids:
                continue
            seen_ds_ids.add(global_id)

            total_matched += 1
            log.info(f"Harvard OAI: QDA match — {global_id} | {title[:60]}")

            if total_matched > _limit:
                log.info(f"Harvard OAI: reached limit of {max_records}.")
                if progress:
                    progress.mark_done(PROGRESS_KEY)
                return

            # ── Write metadata from OAI record (no extra API call needed) ───────
            author     = _oai_text(metadata, "creator")
            keywords   = "; ".join(subjects)
            source_url = f"{HARVARD_BASE}/dataset.xhtml?persistentId={global_id}"
            safe_id    = safe_folder(global_id)

            # ── Legacy files table — one summary row per dataset ──────────────
            if doi not in seen_urls:
                seen_urls.add(doi)
                db.insert({
                    "url":             doi, "doi": doi,
                    "local_dir": "", "local_filename": "",
                    "file_type": "inaccessible", "file_extension": "",
                    "file_size_bytes": None, "checksum_md5": "",
                    "download_timestamp": "",
                    "source_name":    "Harvard Dataverse",
                    "source_url":     source_url,
                    "title":          title,
                    "description":    description[:500],
                    "year":           date_str[:4] if date_str else "",
                    "keywords":       keywords,
                    "language":       _oai_text(metadata, "language"),
                    "author":         author,
                    "uploader_name":  "", "uploader_email": "",
                    "license":        _oai_text(metadata, "rights"),
                    "license_url":    "",
                    "matched_query":  json.dumps(["oai_bulk"], ensure_ascii=False),
                    "manual_download": 1,
                    "access_note":    "OAI-PMH metadata only — file list not fetched",
                })

            # ── Normalised v2 tables ──────────────────────────────────────────
            proj_id = db.insert_project({
                "query_string":               "oai_bulk",
                "repository_id":              10,
                "repository_url":             HARVARD_BASE,
                "project_url":                source_url,
                "version":                    "",
                "title":                      title,
                "description":                description[:500],
                "language":                   _oai_text(metadata, "language"),
                "doi":                        doi,
                "upload_date":                date_str[:10] if date_str else "",
                "download_date":              datetime.now(timezone.utc).isoformat(),
                "download_repository_folder": "harvard_dataverse",
                "download_project_folder":    safe_id,
                "download_version_folder":    "",
                "download_method":            "OAI-PMH",
            })
            for kw in [k.strip() for k in keywords.split(";") if k.strip()]:
                db.insert_keyword(proj_id, kw)
            for person in [p.strip() for p in author.split(";") if p.strip()]:
                db.insert_person_role(proj_id, person, "AUTHOR")
            rights = _oai_text(metadata, "rights")
            if rights:
                db.insert_license(proj_id, rights)
            # File list unknown at OAI level — record a placeholder
            db.insert_project_file(proj_id, "", "", "PENDING")

            db.flush()

        # ── Advance pagination ────────────────────────────────────────────────
        if next_token is None:
            log.info("Harvard OAI: no more pages.")
            if progress:
                progress.mark_done(PROGRESS_KEY)
            break

        resumption_token = next_token
        if progress:
            progress.save_raw(PROGRESS_KEY, resumption_token)

        time.sleep(1)

    log.info(f"Harvard OAI complete: scanned={total_seen} matched={total_matched}")
