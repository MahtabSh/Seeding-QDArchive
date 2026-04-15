"""
Zenodo direct crawler.

Queries Zenodo's public REST search API with all QDA query tiers,
paginates through results, and passes each record to fetch_zenodo.

API reference:
  GET https://zenodo.org/api/records
      ?q=<query>
      &type=dataset          (or omit to include software/other)
      &page=1&size=100
      &access_token=<token>  (optional — unlocks restricted records)

Response shape:
  {"hits": {"hits": [...], "total": N}, ...}

Each hit already contains full metadata, so fetch_zenodo only needs
to make one extra call (for file listings) per record.
"""
import logging
import os
import time
from pathlib import Path

from qdarchive.db import MetadataDB
from qdarchive.fetchers.zenodo import fetch_zenodo
from qdarchive.http_client import get_json
from qdarchive.progress import ProgressState
from qdarchive.queries import (
    QDA_EXTENSION_QUERIES,
    QDA_SOFTWARE_QUERIES,
    QDA_KEYWORD_QUERIES,
)
from qdarchive.utils import human_delay

log = logging.getLogger(__name__)

ZENODO_API         = "https://zenodo.org/api/records"
ZENODO_MAX_OFFSET  = 10_000   # Elasticsearch hard limit (max_result_window = 10000)
ZENODO_PAGE_MAX    = ZENODO_MAX_OFFSET // 25  # = 400 pages of 25
ZENODO_PAGE_LIMIT  = 50       # soft cap per query — queries with more results
                               # are logged so they can be resumed later
ZENODO_TOKEN = os.environ.get("ZENODO_TOKEN", "")   # optional; increases rate limit



def crawl_zenodo(
    output_dir: Path,
    db: MetadataDB,
    max_records: int = 0,
    progress: ProgressState = None,
    download: bool = False,
    allowed_extensions: set[str] | None = None,
    max_file_size_bytes: int | None = None,
    gdrive_service=None,
    gdrive_folder_cache: dict | None = None,
    gdrive_root_id: str | None = None,
):
    mode = "DOWNLOAD" if download else "METADATA ONLY"
    log.info(f"=== Zenodo [{mode}] ===")
    log.info(f"Token: {'set' if ZENODO_TOKEN else 'NOT SET — public rate limit applies'}")

    # Pass token in Authorization header (not URL param — keeps token out of logs)
    auth_headers = {"Authorization": f"Bearer {ZENODO_TOKEN}"} if ZENODO_TOKEN else {}

    seen_urls   = db.seen_urls()
    seen_dois   = db.seen_dois()
    total       = 0
    page_size   = 25   # Zenodo API max is 25 (size=100 returns 400)
    _limit      = max_records if max_records > 0 else float("inf")

    # Build search agenda: (progress_key, query_string)
    # Tier 1 — file extensions, Tier 2 — software names, Tier 3 — keywords
    #
    # Zenodo uses Elasticsearch syntax: a bare ".qdpx" is parsed as a field
    # reference and returns 400. Strip the leading dot so "qdpx" is sent
    # instead — Zenodo indexes filenames in full-text so it still matches.
    search_agenda: list[tuple[str, str]] = []
    for q in QDA_EXTENSION_QUERIES:
        search_agenda.append((f"zenodo:{q}", q.lstrip(".")))
    for q in QDA_SOFTWARE_QUERIES:
        search_agenda.append((f"zenodo:sw:{q}", q))
    for q in QDA_KEYWORD_QUERIES:
        search_agenda.append((f"zenodo:kw:{q}", q))

    for prog_key, q_str in search_agenda:
        if total >= _limit:
            break

        if progress and progress.is_done(prog_key):
            log.info(f"Zenodo: '{prog_key}' already complete, skipping.")
            continue

        start_page = progress.get(prog_key) if progress else 1
        if start_page == 0:
            start_page = 1
        log.info(f"Zenodo: query='{q_str}' starting at page {start_page}")

        page = start_page

        while total < _limit:
            # Guard against a stale progress file that saved a page beyond the cap
            if (page - 1) * page_size >= ZENODO_MAX_OFFSET:
                log.info(f"Zenodo: '{q_str}' — saved page {page} is beyond the 10 000-result cap, marking done.")
                if progress:
                    progress.mark_done(prog_key)
                break

            params = {
                "q":    q_str,
                "page": page,
                "size": page_size,
                "sort": "mostrecent",
            }

            data = get_json(ZENODO_API, params=params, headers=auth_headers)

            if data is None:
                log.warning(f"Zenodo: no response for query='{q_str}' page={page} — waiting 120s before retrying")
                time.sleep(120)
                data = get_json(ZENODO_API, params=params, headers=auth_headers)
            if data is None:
                log.error(f"Zenodo: API still unreachable for query='{q_str}' page={page} — skipping query")
                break

            hits      = data.get("hits", {})
            records   = hits.get("hits", [])
            total_cnt = hits.get("total", 0)

            if not records:
                log.info(f"Zenodo: query '{q_str}' exhausted at page {page} ({total_cnt} total).")
                if progress:
                    progress.mark_done(prog_key)
                break

            for record in records:
                if total >= _limit:
                    break

                rec_id = record.get("id")
                doi    = record.get("doi", f"10.5281/zenodo.{rec_id}")
                # Normalise to the same format fetch_zenodo stores in the DB
                doi_url   = f"https://doi.org/{doi}"
                global_id = f"doi:{doi}"

                # Deduplicate by normalised DOI URL (matches what fetch_zenodo stores)
                if doi_url in seen_dois:
                    continue
                seen_dois.add(doi_url)

                # Pass the full search result so fetch_zenodo can skip its
                # redundant detail API call when files are already present
                item = {
                    "name":         record.get("metadata", {}).get("title", ""),
                    "description":  (record.get("metadata", {}).get("description", "") or "")[:500],
                    "published_at": record.get("metadata", {}).get("publication_date", ""),
                    "_record":      record,   # full search result, used as fast path
                }

                fetch_zenodo(
                    global_id, item, output_dir, db, seen_urls, download, q_str,
                    allowed_extensions=allowed_extensions,
                    max_file_size_bytes=max_file_size_bytes,
                    gdrive_service=gdrive_service,
                    gdrive_folder_cache=gdrive_folder_cache,
                    gdrive_root_id=gdrive_root_id,
                )
                total += 1
                human_delay(1.0, 3.5, "between records")

            # Pagination checks (evaluated in priority order)
            total_pages = -(-total_cnt // page_size)  # ceiling division

            if page >= ZENODO_PAGE_LIMIT:
                # Soft cap reached — log remaining pages for future runs
                remaining_pages = total_pages - page
                remaining_records = total_cnt - (page * page_size)
                log.warning(
                    f"Zenodo: query '{q_str}' paused at page {page}/{total_pages} "
                    f"({remaining_records} records on {remaining_pages} remaining pages). "
                    f"Re-run to continue from page {page + 1}."
                )
                if progress:
                    progress.save(prog_key, page + 1)
                break

            if page * page_size >= min(total_cnt, ZENODO_MAX_OFFSET):
                if total_cnt > ZENODO_MAX_OFFSET:
                    log.info(
                        f"Zenodo: query '{q_str}' hit Elasticsearch 10 000-result cap "
                        f"({total_cnt} total)."
                    )
                else:
                    log.info(f"Zenodo: query '{q_str}' complete ({total_cnt} total records).")
                if progress:
                    progress.mark_done(prog_key)
                break

            page += 1
            if progress:
                progress.save(prog_key, page)
            log.info(
                f"Zenodo: '{q_str}' page {page}/{total_pages} "
                f"| records processed={total}"
            )
            human_delay(2.0, 6.0, "between pages")

    log.info(f"Zenodo crawl complete: {total} records processed.")
