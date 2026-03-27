"""
Harvard Dataverse crawler  (repository #10).

Also handles any Dataverse installation whose datasets appear in Harvard's
search results (Borealis, DANS, dataverse.no, openICPSR, etc.) by resolving
the correct API base URL and token via resolve_host().

Flow:
  1. Search Harvard's API with QDA_QUERIES in title/keyword/full-text forms.
  2. For each dataset DOI: resolve the hosting institution.
  3. Fetch the full dataset record from that institution's Dataverse API.
  4. Write every file to the legacy `files` table AND the normalised v2 tables.
  5. Optionally download the dataset as a zip.
"""
import json
import logging
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from qdarchive.config import (
    HARVARD_TOKEN, HARVARD_BASE, HARVARD_API,
    DATAVERSE_REGISTRY, NON_DATAVERSE_PREFIXES, SILENT_SKIP_PREFIXES,
)
from qdarchive.db import MetadataDB
from qdarchive.fetchers import _insert_file_row, PLATFORM_DOWNLOADERS
from qdarchive.http_client import get_json, download_file, SESSION
from qdarchive.progress import ProgressState
from qdarchive.queries import QDA_EXTENSION_QUERIES, QDA_SOFTWARE_QUERIES, QDA_KEYWORD_QUERIES
from qdarchive.utils import classify_file, safe_folder, year_from_date

log = logging.getLogger(__name__)

# Cache of resolved DOI hosts so we don't HEAD the same DOI twice
_DOI_HOST_CACHE: dict = {}


def _fetch_detail(global_id: str, item_host: str, item_token: str) -> tuple:
    """Fetch full dataset detail from a Dataverse API. Returns (detail_json, item_api, item_auth)."""
    item_api  = f"{item_host}/api"
    item_auth = {"X-Dataverse-key": item_token} if item_token else {}
    detail = get_json(
        f"{item_api}/datasets/:persistentId/",
        params={"persistentId": global_id},
        headers=item_auth,
        retries=5,   # renews Tor circuit on each empty/blocked response
    )
    return detail, item_api, item_auth


def resolve_host(item: dict) -> tuple[str, str]:
    """
    Return (base_url, api_token) for a Dataverse search result item.

    Resolution order:
      1. item["url"] is a direct Dataverse URL  → extract host from it
      2. name_of_dataverse matches DATAVERSE_REGISTRY → use registry entry
      3. item["url"] is a DOI redirect           → follow redirect (HEAD)
      4. Unknown host                            → use host with no token
    """
    item_url = item.get("url", "")
    parsed   = urlparse(item_url) if item_url else None
    url_host = f"{parsed.scheme}://{parsed.netloc}" if parsed and parsed.netloc else ""

    # ── 1. Direct Dataverse URL ───────────────────────────────────────────────
    if url_host and "doi.org" not in url_host:
        for base_url, token in DATAVERSE_REGISTRY.values():
            if url_host.rstrip("/") == base_url.rstrip("/"):
                return base_url, token
        return url_host, ""

    # ── 2. Match by name_of_dataverse ─────────────────────────────────────────
    dv_name = (item.get("name_of_dataverse", "") or "").lower().strip()
    for key, (base_url, token) in DATAVERSE_REGISTRY.items():
        if key in dv_name:
            return base_url, token

    # ── 3. Follow DOI redirect ────────────────────────────────────────────────
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
            for base_url, token in DATAVERSE_REGISTRY.values():
                if real_host.rstrip("/") == base_url.rstrip("/"):
                    return base_url, token
            return real_host, ""

    # ── 4. Give up ────────────────────────────────────────────────────────────
    return HARVARD_BASE, ""


def crawl_harvard(
    output_dir: Path,
    db: MetadataDB,
    max_records: int = 100_000,
    progress: ProgressState = None,
    download: bool = False,
):
    mode = "DOWNLOAD" if download else "METADATA ONLY"
    log.info(f"=== Harvard Dataverse  [{mode}] ===")
    log.info(f"Token: {'set' if HARVARD_TOKEN and HARVARD_TOKEN != 'your-harvard-api-token-here' else 'NOT SET - public access only'}")

    # ── Pre-flight: confirm search endpoint is reachable before burning hours ──
    _preflight = get_json(
        f"{HARVARD_API}/search",
        params={"q": "test", "type": "dataset", "per_page": 1},
        headers={"X-Dataverse-key": HARVARD_TOKEN},
        retries=10,   # keep renewing Tor circuits until one works
    )
    if _preflight is None:
        log.error(
            "Harvard Dataverse search endpoint is unreachable (blocked or down). "
            "Your IP may be temporarily rate-limited — wait a few hours and retry. "
            "Skipping Harvard crawl entirely."
        )
        return

    auth_headers   = {"X-Dataverse-key": HARVARD_TOKEN}
    seen_urls      = db.seen_urls()
    seen_ds_ids    = db.seen_dois()   # loaded once; updated incrementally
    total_datasets = 0
    per_page       = 100
    _limit         = max_records if max_records > 0 else float("inf")

    # Build full search agenda: (progress_key, query_string) pairs.
    #
    # Priority order:
    #   1. File extensions (.qdpx …) — bare term, Dataverse matches filenames → zero noise
    #   2. Software names (NVivo …)  — title/keyword scoped → high precision
    #   3. Methodology keywords      — title/keyword/full-text → broadest recall
    search_agenda = []
    for q in QDA_EXTENSION_QUERIES:
        search_agenda.append((q, q))
    for q in QDA_SOFTWARE_QUERIES:
        search_agenda.append((f"title:{q}",   f'title:"{q}"'))
        search_agenda.append((f"keyword:{q}", f'keyword:"{q}"'))
    for q in QDA_KEYWORD_QUERIES:
        search_agenda.append((f"title:{q}",   f'title:"{q}"'))
        search_agenda.append((f"keyword:{q}", f'keyword:"{q}"'))
        search_agenda.append((q,              q))

    endpoint_blocked = False   # set True on any None response → abort entire crawl

    for prog_key, q_str in search_agenda:
        if endpoint_blocked:
            break

        if progress and progress.is_done(prog_key):
            log.info(f"Harvard: '{prog_key}' already complete, skipping.")
            continue

        start = progress.get(prog_key) if progress else 0
        log.info(f"Harvard: query='{q_str}' starting at offset {start}")

        seen_ds_this_query: set = set()

        while total_datasets < _limit:
            data = get_json(
                f"{HARVARD_API}/search",
                params={"q": q_str, "type": "dataset", "per_page": per_page, "start": start},
                headers=auth_headers,
            )

            if data is None:
                # None means the endpoint is blocked (403 HTML / connection error)
                # — all subsequent queries will fail too, abort the whole crawl.
                log.error(
                    "Harvard search endpoint blocked (403/connection error). "
                    "Stopping crawl — resume after the IP block lifts."
                )
                endpoint_blocked = True
                break

            if data.get("status") != "OK":
                log.warning(f"Harvard: non-OK response for q='{q_str}' start={start}: {data.get('message','')}")
                if progress:
                    progress.mark_done(prog_key)
                break

            items = data.get("data", {}).get("items", [])
            if not items:
                log.info(f"Harvard: query '{q_str}' exhausted at offset {start}.")
                if progress:
                    progress.mark_done(prog_key)
                break

            clean_q = prog_key.split(":", 1)[-1].strip('"') if ":" in prog_key else prog_key

            # ── Pass 1: classify each item, handle non-Dataverse DOIs inline ──
            # Collect Dataverse datasets that need a slow API detail fetch.
            pending: list[tuple[str, dict, str, str]] = []  # (global_id, item, item_host, item_token)

            for item in items:
                item_type = item.get("type", "dataset")

                if item_type == "file":
                    parent_pid = item.get("dataset_persistent_id", "")
                    if not parent_pid or parent_pid in seen_ds_this_query:
                        continue
                    doi_key = f"https://doi.org/{parent_pid.replace('doi:', '')}"
                    if parent_pid in seen_ds_ids:
                        db.append_query(doi_key, clean_q)
                        continue
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
                if not global_id or global_id in seen_ds_this_query:
                    continue
                seen_ds_this_query.add(global_id)

                doi_key = f"https://doi.org/{global_id.replace('doi:', '')}"
                if global_id in seen_ds_ids:
                    db.append_query(doi_key, clean_q)
                    continue
                seen_ds_ids.add(global_id)

                doi_prefix = global_id.replace("doi:", "").split("/")[0]

                if doi_prefix in SILENT_SKIP_PREFIXES:
                    continue

                if doi_prefix in NON_DATAVERSE_PREFIXES:
                    downloader = PLATFORM_DOWNLOADERS.get(doi_prefix)
                    if downloader:
                        log.info(f"Non-Dataverse DOI {global_id} ({doi_prefix}) — using platform downloader")
                        downloader(global_id, item, output_dir, db, seen_urls, download, clean_q)
                    else:
                        log.warning(f"No downloader for DOI prefix {doi_prefix} ({global_id})")
                        _insert_file_row(db, seen_urls, {
                            "url":             item.get("url", f"https://doi.org/{global_id.replace('doi:', '')}"),
                            "doi":             f"https://doi.org/{global_id.replace('doi:', '')}",
                            "local_dir": "", "local_filename": "",
                            "file_type": "inaccessible", "file_extension": "",
                            "file_size_bytes": None, "checksum_md5": "",
                            "download_timestamp": "",
                            "source_name":     item.get("name_of_dataverse", "Unknown"),
                            "source_url":      item.get("url", ""),
                            "title":           item.get("name", ""),
                            "description":     (item.get("description", "") or "")[:500],
                            "year":            year_from_date(item.get("published_at", "")),
                            "keywords":        "; ".join(item.get("keywords", [])),
                            "language": "", "author": "; ".join(item.get("authors", [])),
                            "uploader_name": "", "uploader_email": "",
                            "license": "", "license_url": "",
                            "matched_query":   json.dumps([clean_q], ensure_ascii=False),
                            "manual_download": 1,
                            "access_note":     f"No downloader for DOI prefix {doi_prefix}",
                        })
                    continue

                item_host, item_token = resolve_host(item)
                pending.append((global_id, item, item_host, item_token))

            # ── Pass 2: fetch dataset details in parallel ─────────────────────
            # HTTP calls are parallelised; DB writes happen serially afterwards.
            futures_map = {}
            with ThreadPoolExecutor(max_workers=5) as executor:
                for global_id, item, item_host, item_token in pending:
                    fut = executor.submit(_fetch_detail, global_id, item_host, item_token)
                    futures_map[fut] = (global_id, item, item_host)

            # ── Pass 3: process results and write to DB (serial) ──────────────
            for fut, (global_id, item, item_host) in futures_map.items():
                detail, item_api, item_auth = fut.result()
                source_url = f"{item_host}/dataset.xhtml?persistentId={global_id}"
                doi        = f"https://doi.org/{global_id.replace('doi:', '')}"

                if not detail or detail.get("status") != "OK":
                    log.warning(f"Inaccessible: {global_id} from {item_host}")
                    if source_url not in seen_urls:
                        seen_urls.add(source_url)
                        db.insert({
                            "url": source_url, "doi": doi,
                            "local_dir": "", "local_filename": "",
                            "file_type": "inaccessible", "file_extension": "",
                            "file_size_bytes": None, "checksum_md5": "",
                            "download_timestamp": "",
                            "source_name":  item.get("name_of_dataverse", "Unknown Dataverse"),
                            "source_url":   source_url,
                            "title":        item.get("name", ""),
                            "description":  (item.get("description", "") or "")[:500],
                            "year":         year_from_date(item.get("published_at", "")),
                            "keywords":     "; ".join(item.get("keywords", [])),
                            "language": "", "author": "; ".join(item.get("authors", [])),
                            "uploader_name": "", "uploader_email": "",
                            "license": "", "license_url": "",
                            "matched_query": json.dumps([clean_q], ensure_ascii=False),
                            "manual_download": 0,
                            "access_note":  f"Dataverse API 401/403 from {item_host} — add token to DATAVERSE_REGISTRY",
                        })
                    continue

                ds     = detail["data"]
                latest = ds.get("latestVersion", {})

                # ── Dataset metadata ──────────────────────────────────────────
                title    = item.get("name", "")
                pub_date = item.get("published_at", "")

                citation_fields = (
                    latest.get("metadataBlocks", {}).get("citation", {}).get("fields", [])
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

                files = latest.get("files", [])
                if not files:
                    log.info(f"Harvard: no files in {global_id}")
                    continue

                log.info(f"Harvard: dataset {global_id} — {len(files)} file(s)")
                safe_id   = safe_folder(global_id)
                local_dir = f"harvard_dataverse/{safe_id}"

                # ── Write legacy files table ──────────────────────────────────
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
                        "local_dir":          "",
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
                        "matched_query":      json.dumps([clean_q], ensure_ascii=False),
                    })

                # ── Write normalised v2 tables ────────────────────────────────
                proj_id = db.insert_project({
                    "query_string":               clean_q,
                    "repository_id":              10,
                    "repository_url":             HARVARD_BASE,
                    "project_url":                source_url,
                    "version":                    str(latest.get("versionNumber", "")),
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

                # ── Download entire dataset as zip ────────────────────────────
                if download:
                    zip_url  = f"{item_api}/access/dataset/:persistentId/?persistentId={global_id}"
                    zip_dest = output_dir / "harvard_dataverse" / safe_id / f"{safe_id}.zip"

                    if not zip_dest.exists():
                        log.info(f"Harvard: downloading zip for {global_id} -> {zip_dest}")
                        if download_file(zip_url, zip_dest, extra_headers=item_auth):
                            ts = datetime.now(timezone.utc).isoformat()
                            log.info(f"Harvard: zip saved ({zip_dest.stat().st_size:,} bytes)")
                            extract_dir = output_dir / "harvard_dataverse" / safe_id
                            try:
                                with zipfile.ZipFile(zip_dest) as zf:
                                    zf.extractall(extract_dir)
                                log.info(f"Harvard: extracted {global_id} to {extract_dir}")
                                zip_dest.unlink()
                                db.conn.execute(
                                    "UPDATE files SET download_timestamp=?, local_dir=? WHERE doi=? AND download_timestamp=''",
                                    (ts, local_dir, doi),
                                )
                                db.conn.commit()
                            except zipfile.BadZipFile:
                                log.warning(f"Harvard: bad zip for {global_id} — keeping zip file")
                        else:
                            log.warning(f"Harvard: zip download failed for {global_id}")
                    else:
                        log.info(f"Harvard: zip already exists for {global_id}, skipping.")

                db.flush()
                total_datasets += 1
                time.sleep(0.5)

            # ── Pagination ────────────────────────────────────────────────────
            total_count = data.get("data", {}).get("total_count", 0)
            start += per_page
            if start >= total_count:
                log.info(f"Harvard: query '{q_str}' exhausted ({total_count} total).")
                if progress:
                    progress.mark_done(prog_key)
                break

            if progress:
                progress.save(prog_key, start)
            log.info(
                f"Harvard: '{q_str}' offset {start}/{total_count} "
                f"| datasets processed={total_datasets}"
            )
            time.sleep(1)

    log.info(f"Harvard Dataverse complete: {total_datasets} datasets processed.")
